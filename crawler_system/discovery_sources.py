from dataclasses import dataclass
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

    def __init__(self):
        self._sources: dict[str, DiscoverySource] = {}

        # ---------------------------------------------------------
        # Discovery Source Execution Metrics
        # ---------------------------------------------------------

        self.execution_metrics: dict[str, dict[str, int]] = {}

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
            }
        )

    @staticmethod
    def _calculate_health(metrics):
        executions = metrics["executions"]
        failures = metrics["failures"]

        if executions <= 0:
            return "healthy"

        failure_rate = failures / executions

        if failure_rate >= 0.50:
            return "unhealthy"

        if failure_rate >= 0.20:
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

            try:
                can_discover = getattr(
                    source,
                    "can_discover",
                    None,
                )

                # -------------------------------------------------
                # Context-aware filtering
                #
                # When content_type is explicitly supplied,
                # use can_discover().
                #
                # When content_type is omitted, preserve the
                # original registry behavior for compatibility.
                # -------------------------------------------------

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

                discovered = source.discover(
                    url,
                    body,
                )

            except Exception:
                metrics["failures"] += 1

                self._update_health_metrics(
                    metrics
                )

                continue

            # -----------------------------------------------------
            # Successful execution
            # -----------------------------------------------------

            metrics["successful_executions"] += 1

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
