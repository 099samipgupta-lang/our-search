from dataclasses import dataclass
from time import monotonic, perf_counter
from typing import Any, Protocol


@dataclass(frozen=True)
class DomainDiscoveryContext:
    """
    Context supplied to an independent domain-discovery source.

    Unlike page-level DiscoveryContext, this context does not
    represent a crawled Web page. It represents the execution
    environment for discovering previously unknown domains.
    """

    seed_domain: str | None = None
    metadata: dict[str, Any] | None = None


@dataclass(frozen=True)
class DomainCandidate:
    """
    A domain discovered independently of the existing Web graph.

    The candidate intentionally preserves provenance so later
    stages can distinguish independently discovered domains from
    domains discovered through links, feeds, or sitemaps.
    """

    hostname: str
    url: str
    source: str
    evidence: str | None = None
    discovered_at: float | None = None
    metadata: dict[str, Any] | None = None

    def __hash__(self) -> int:
        """
        Hash candidates by stable identity/provenance fields.

        Metadata is intentionally excluded because dictionaries
        are unhashable and metadata is descriptive rather than
        candidate identity.
        """
        return hash(
            (
                self.hostname,
                self.url,
                self.source,
                self.evidence,
            )
        )


class DomainDiscoverySource(Protocol):
    """
    Protocol implemented by independent domain-discovery sources.
    """

    name: str

    def can_discover(
        self,
        context: DomainDiscoveryContext,
    ) -> bool:
        ...

    def discover(
        self,
        context: DomainDiscoveryContext,
    ) -> set[DomainCandidate]:
        ...


class DomainDiscoverySourceRegistry:
    """
    Registry for independent new-domain discovery sources.

    This registry is intentionally separate from
    DiscoverySourceRegistry.

    DiscoverySourceRegistry discovers URLs from already observed
    Web resources.

    DomainDiscoverySourceRegistry discovers domain candidates
    independently of those already observed Web resources.
    """

    def __init__(
        self,
        max_failure_rate: float = 0.50,
        degraded_failure_rate: float = 0.20,
        max_average_latency_seconds: float = 5.0,
        adaptive_failure_threshold: int = 3,
        adaptive_latency_threshold: int = 3,
        adaptive_cooldown_seconds: float = 30.0,
    ):
        if not 0.0 <= max_failure_rate <= 1.0:
            raise ValueError(
                "max_failure_rate must be between 0 and 1"
            )

        if not 0.0 <= degraded_failure_rate <= 1.0:
            raise ValueError(
                "degraded_failure_rate must be between 0 and 1"
            )

        if degraded_failure_rate > max_failure_rate:
            raise ValueError(
                "degraded_failure_rate cannot exceed max_failure_rate"
            )

        if adaptive_failure_threshold < 1:
            raise ValueError(
                "adaptive_failure_threshold must be at least 1"
            )

        if adaptive_latency_threshold < 1:
            raise ValueError(
                "adaptive_latency_threshold must be at least 1"
            )

        if adaptive_cooldown_seconds < 0:
            raise ValueError(
                "adaptive_cooldown_seconds must not be negative"
            )

        if max_average_latency_seconds < 0:
            raise ValueError(
                "max_average_latency_seconds must not be negative"
            )

        self._sources: dict[
            str,
            DomainDiscoverySource
        ] = {}

        self.execution_metrics: dict[
            str,
            dict[str, Any]
        ] = {}

        self.adaptive_control: dict[
            str,
            dict[str, Any]
        ] = {}

        self.max_failure_rate = max_failure_rate
        self.degraded_failure_rate = degraded_failure_rate

        # Kept under this public name for consistency with the
        # existing discovery-source architecture.
        #
        # Adaptive control treats this as the latency threshold
        # for an individual successful execution.
        self.max_average_latency_seconds = (
            max_average_latency_seconds
        )

        self.adaptive_failure_threshold = (
            adaptive_failure_threshold
        )

        self.adaptive_latency_threshold = (
            adaptive_latency_threshold
        )

        self.adaptive_cooldown_seconds = (
            adaptive_cooldown_seconds
        )

    # ============================================================
    # METRICS
    # ============================================================

    def _metrics_for(
        self,
        name: str,
    ) -> dict[str, Any]:
        return self.execution_metrics.setdefault(
            name,
            {
                "executions": 0,
                "skipped": 0,
                "failures": 0,
                "items_produced": 0,
                "successful_executions": 0,
                "empty_results": 0,

                "total_latency_seconds": 0.0,
                "average_latency_seconds": 0.0,
                "min_latency_seconds": None,
                "max_latency_seconds": None,

                "failure_rate": 0.0,
                "success_rate": 0.0,
                "health": "healthy",
            },
        )

    def _adaptive_state_for(
        self,
        name: str,
    ) -> dict[str, Any]:
        return self.adaptive_control.setdefault(
            name,
            {
                "enabled": True,
                "failure_streak": 0,
                "latency_streak": 0,
                "cooldown_until": 0.0,
                "control_action": "active",
                "control_reason": None,
            },
        )

    def _calculate_health(
        self,
        metrics: dict[str, Any],
    ) -> str:
        executions = metrics["executions"]
        failures = metrics["failures"]

        if executions <= 0:
            return "healthy"

        failure_rate = failures / executions

        if failure_rate >= self.max_failure_rate:
            return "unhealthy"

        if failure_rate >= self.degraded_failure_rate:
            return "degraded"

        return "healthy"

    def _update_health_metrics(
        self,
        metrics: dict[str, Any],
    ) -> None:
        executions = metrics["executions"]
        failures = metrics["failures"]

        if executions > 0:
            metrics["failure_rate"] = (
                failures / executions
            )

            metrics["success_rate"] = (
                metrics["successful_executions"]
                / executions
            )
        else:
            metrics["failure_rate"] = 0.0
            metrics["success_rate"] = 0.0

        metrics["health"] = (
            self._calculate_health(metrics)
        )

    @staticmethod
    def _update_latency_metrics(
        metrics: dict[str, Any],
        latency_seconds: float,
    ) -> None:
        metrics["total_latency_seconds"] += (
            latency_seconds
        )

        successful_executions = (
            metrics["successful_executions"] + 1
        )

        metrics["average_latency_seconds"] = (
            metrics["total_latency_seconds"]
            / successful_executions
        )

        current_min = metrics["min_latency_seconds"]

        if (
            current_min is None
            or latency_seconds < current_min
        ):
            metrics["min_latency_seconds"] = (
                latency_seconds
            )

        current_max = metrics["max_latency_seconds"]

        if (
            current_max is None
            or latency_seconds > current_max
        ):
            metrics["max_latency_seconds"] = (
                latency_seconds
            )

    # ============================================================
    # ADAPTIVE CONTROL
    # ============================================================

    def _update_adaptive_state_after_success(
        self,
        name: str,
        latency_seconds: float,
    ) -> None:
        state = self._adaptive_state_for(name)

        state["failure_streak"] = 0

        if (
            latency_seconds
            > self.max_average_latency_seconds
        ):
            state["latency_streak"] += 1
        else:
            state["latency_streak"] = 0

        state["enabled"] = True
        state["cooldown_until"] = 0.0
        state["control_action"] = "active"
        state["control_reason"] = None

        if (
            state["latency_streak"]
            >= self.adaptive_latency_threshold
        ):
            state["enabled"] = False
            state["cooldown_until"] = (
                monotonic()
                + self.adaptive_cooldown_seconds
            )
            state["control_action"] = "cooldown"
            state["control_reason"] = (
                "persistent_high_latency"
            )

    def _update_adaptive_state_after_failure(
        self,
        name: str,
    ) -> None:
        state = self._adaptive_state_for(name)

        state["failure_streak"] += 1
        state["latency_streak"] = 0

        if (
            state["control_action"]
            == "recovery_probe"
        ):
            state["enabled"] = False
            state["cooldown_until"] = (
                monotonic()
                + self.adaptive_cooldown_seconds
            )
            state["control_action"] = "cooldown"
            state["control_reason"] = (
                "recovery_probe_failed"
            )

            return

        if (
            state["failure_streak"]
            >= self.adaptive_failure_threshold
        ):
            state["enabled"] = False
            state["cooldown_until"] = (
                monotonic()
                + self.adaptive_cooldown_seconds
            )
            state["control_action"] = "cooldown"
            state["control_reason"] = (
                "persistent_failures"
            )

    def _adaptive_should_execute(
        self,
        name: str,
    ) -> bool:
        state = self._adaptive_state_for(name)

        if state["enabled"]:
            return True

        now = monotonic()

        if now >= state["cooldown_until"]:
            state["enabled"] = True
            state["failure_streak"] = 0
            state["latency_streak"] = 0
            state["control_action"] = (
                "recovery_probe"
            )
            state["control_reason"] = (
                "cooldown_expired"
            )

            return True

        return False

    def get_adaptive_state(
        self,
        name: str,
    ) -> dict[str, Any] | None:
        state = self.adaptive_control.get(name)

        if state is None:
            return None

        return dict(state)

    # ============================================================
    # REGISTRATION
    # ============================================================

    def register(
        self,
        source: DomainDiscoverySource,
    ) -> None:
        name = getattr(source, "name", None)

        if (
            not isinstance(name, str)
            or not name.strip()
        ):
            raise ValueError(
                "Domain discovery source must have "
                "a non-empty name"
            )

        name = name.strip()

        if name in self._sources:
            raise ValueError(
                "Domain discovery source already "
                f"registered: {name}"
            )

        self._sources[name] = source

        self._metrics_for(name)
        self._adaptive_state_for(name)

    def unregister(
        self,
        name: str,
    ) -> bool:
        if not isinstance(name, str):
            return False

        removed = (
            self._sources.pop(name, None)
            is not None
        )

        if removed:
            self.execution_metrics.pop(
                name,
                None
            )

            self.adaptive_control.pop(
                name,
                None
            )

        return removed

    def get(
        self,
        name: str,
    ):
        if not isinstance(name, str):
            return None

        return self._sources.get(name)

    def names(self) -> tuple[str, ...]:
        return tuple(self._sources.keys())

    # ============================================================
    # DISCOVERY
    # ============================================================

    def discover(
        self,
        context: DomainDiscoveryContext | None = None,
    ) -> set[DomainCandidate]:
        if context is None:
            context = DomainDiscoveryContext()

        results: set[DomainCandidate] = set()

        for source in self._sources.values():

            name = getattr(
                source,
                "name",
                "unknown",
            )

            metrics = self._metrics_for(name)

            # ----------------------------------------------------
            # Adaptive source control
            # ----------------------------------------------------

            if not self._adaptive_should_execute(name):
                metrics["skipped"] += 1

                self._update_health_metrics(
                    metrics
                )

                continue

            try:
                can_discover = getattr(
                    source,
                    "can_discover",
                    None,
                )

                if can_discover is not None:
                    if not can_discover(context):
                        metrics["skipped"] += 1

                        self._update_health_metrics(
                            metrics
                        )

                        continue

                metrics["executions"] += 1

                start_time = perf_counter()

                discovered = source.discover(
                    context
                )

                latency_seconds = (
                    perf_counter()
                    - start_time
                )

                self._update_latency_metrics(
                    metrics,
                    latency_seconds,
                )

            except Exception:
                metrics["failures"] += 1

                self._update_adaptive_state_after_failure(
                    name
                )

                self._update_health_metrics(
                    metrics
                )

                continue

            metrics["successful_executions"] += 1

            self._update_adaptive_state_after_success(
                name,
                latency_seconds,
            )

            if not discovered:
                metrics["empty_results"] += 1

                self._update_health_metrics(
                    metrics
                )

                continue

            produced = 0

            for item in discovered:
                if isinstance(
                    item,
                    DomainCandidate,
                ):
                    results.add(item)
                    produced += 1

            metrics["items_produced"] += produced

            if produced == 0:
                metrics["empty_results"] += 1

            self._update_health_metrics(
                metrics
            )

        return results

    def metrics(
        self,
        name: str | None = None,
    ):
        if name is None:
            return {
                source_name: dict(source_metrics)
                for source_name, source_metrics
                in self.execution_metrics.items()
            }

        source_metrics = self.execution_metrics.get(
            name
        )

        if source_metrics is None:
            return None

        return dict(source_metrics)
