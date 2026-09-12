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
            }
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

        removed = self._sources.pop(name, None) is not None

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
                # If content_type is explicitly supplied, use the
                # source's can_discover() decision.
                #
                # If content_type is not supplied, preserve the
                # original registry behavior and execute the source.
                #
                # This keeps older callers compatible while allowing
                # newer callers to provide precise context.
                # -------------------------------------------------

                if (
                    can_discover is not None
                    and content_type is not None
                ):
                    if not can_discover(context):
                        metrics["skipped"] += 1
                        continue

                metrics["executions"] += 1

                discovered = source.discover(
                    url,
                    body,
                )

            except Exception:
                metrics["failures"] += 1
                continue

            if not discovered:
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

        return results
