from dataclasses import dataclass
from time import perf_counter, monotonic
from typing import Any, Protocol


@dataclass(frozen=True)
class DiscoveryContext:
    url: str
    body: bytes | None = None
    content_type: str | None = None


@dataclass(frozen=True)
class DiscoveryItem:
    url: str
    source: str
    source_url: str | None = None
    depth: int = 0
    metadata: dict[str, Any] | None = None


class DiscoverySource(Protocol):
    name: str

    def can_discover(
        self,
        context: DiscoveryContext
    ) -> bool:
        ...

    def discover(
        self,
        url: str,
        body: bytes | None = None,
    ) -> set[DiscoveryItem]:
        ...


class DiscoverySourceRegistry:

    def __init__(
        self,
        max_failure_rate: float = 0.50,
        degraded_failure_rate: float = 0.20,
        max_average_latency_seconds: float = 5.0,
        adaptive_failure_threshold: int = 3,
        adaptive_latency_threshold: int = 3,
        adaptive_cooldown_seconds: float = 30.0,
    ):
        self._sources: dict[str, DiscoverySource] = {}

        # ---------------------------------------------------------
        # Discovery Source Metrics
        # ---------------------------------------------------------

        self.execution_metrics: dict[str, dict[str, Any]] = {}

        # ---------------------------------------------------------
        # Adaptive-control configuration
        # ---------------------------------------------------------

        if not 0.0 <= max_failure_rate <= 1.0:
            raise ValueError("max_failure_rate must be between 0 and 1")

        if not 0.0 <= degraded_failure_rate <= 1.0:
            raise ValueError(
                "degraded_failure_rate must be between 0 and 1"
            )

        if degraded_failure_rate > max_failure_rate:
            raise ValueError(
                "degraded_failure_rate cannot exceed max_failure_rate"
            )

        self.max_failure_rate = max_failure_rate
        self.degraded_failure_rate = degraded_failure_rate

        # Kept under the existing public name for compatibility.
        # Adaptive control treats this as the per-execution latency
        # threshold, not the calculated average.
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

        # ---------------------------------------------------------
        # Adaptive-control state
        # ---------------------------------------------------------

        self.adaptive_control: dict[str, dict[str, Any]] = {}

    def _metrics_for(self, name):
        return self.execution_metrics.setdefault(
            name,
            {
                "executions": 0,
                "skipped": 0,
                "failures": 0,
                "items_produced": 0,
                "successful_executions": 0,
                "empty_results": 0,

                # Performance / latency observability

                "total_latency_seconds": 0.0,
                "average_latency_seconds": 0.0,
                "min_latency_seconds": None,
                "max_latency_seconds": None,
            }
        )

    def _adaptive_state_for(self, name):
        return self.adaptive_control.setdefault(
            name,
            {
                "enabled": True,
                "failure_streak": 0,
                "latency_streak": 0,
                "cooldown_until": 0.0,
                "control_action": "active",
                "control_reason": None,
            }
        )

    def _calculate_health(self, metrics):
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

    def _update_health_metrics(self, metrics):
        executions = metrics["executions"]
        failures = metrics["failures"]

        if executions > 0:
            metrics["failure_rate"] = failures / executions
            metrics["success_rate"] = (
                metrics["successful_executions"]
                / executions
            )
        else:
            metrics["failure_rate"] = 0.0
            metrics["success_rate"] = 0.0

        metrics["health"] = self._calculate_health(
            metrics
        )

    @staticmethod
    def _update_latency_metrics(
        metrics,
        latency_seconds
    ):
        metrics["total_latency_seconds"] += latency_seconds

        # Failed executions are intentionally excluded from latency
        # metrics. Therefore the denominator is the number of
        # successful executions including the current one.
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
            metrics["min_latency_seconds"] = latency_seconds

        current_max = metrics["max_latency_seconds"]

        if (
            current_max is None
            or latency_seconds > current_max
        ):
            metrics["max_latency_seconds"] = latency_seconds

    def _update_adaptive_state_after_success(
        self,
        name,
        latency_seconds,
    ):
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
        name,
    ):
        state = self._adaptive_state_for(name)

        state["failure_streak"] += 1
        state["latency_streak"] = 0

        # ---------------------------------------------------------
        # A failed recovery probe immediately returns the source
        # to cooldown. A recovery probe exists specifically to
        # verify that a previously unhealthy source is safe to
        # resume.
        # ---------------------------------------------------------

        if state["control_action"] == "recovery_probe":
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

    def _adaptive_should_execute(self, name):
        state = self._adaptive_state_for(name)

        if state["enabled"]:
            return True

        now = monotonic()

        if now >= state["cooldown_until"]:
            # Automatic recovery probe.

            state["enabled"] = True
            state["failure_streak"] = 0
            state["latency_streak"] = 0
            state["control_action"] = "recovery_probe"
            state["control_reason"] = (
                "cooldown_expired"
            )

            return True

        return False

    def get_adaptive_state(self, name):
        state = self.adaptive_control.get(name)

        if state is None:
            return None

        return dict(state)

    def register(self, source):
        name = getattr(source, "name", None)

        if not isinstance(name, str) or not name.strip():
            raise ValueError(
                "Discovery source must have a non-empty name"
            )

        name = name.strip()

        if name in self._sources:
            raise ValueError(
                f"Discovery source already registered: {name}"
            )

        self._sources[name] = source

        self._metrics_for(name)
        self._adaptive_state_for(name)

    def unregister(self, name):
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

    def get(self, name):
        if not isinstance(name, str):
            return None

        return self._sources.get(name)

    def names(self):
        return tuple(self._sources.keys())

    def discover(
        self,
        url,
        body=None,
        content_type=None,
    ):
        context = DiscoveryContext(
            url=url,
            body=body,
            content_type=content_type,
        )

        results = set()

        for source in self._sources.values():

            name = getattr(
                source,
                "name",
                "unknown"
            )

            metrics = self._metrics_for(name)

            # -----------------------------------------------------
            # Adaptive source control
            # -----------------------------------------------------

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

                # Context-aware filtering

                if (
                    can_discover is not None
                    and content_type is not None
                ):
                    if not can_discover(context):
                        metrics["skipped"] += 1

                        self._update_health_metrics(
                            metrics
                        )

                        continue

                # -------------------------------------------------
                # Source execution
                # -------------------------------------------------

                metrics["executions"] += 1

                start_time = perf_counter()

                discovered = source.discover(
                    url,
                    body,
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

            # -----------------------------------------------------
            # Successful execution
            # -----------------------------------------------------

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
                    DiscoveryItem
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
