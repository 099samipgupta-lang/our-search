from dataclasses import dataclass
from urllib.parse import urlsplit

from crawler_system.url_normalizer import URLNormalizer
from crawler_system.url_crawl_intelligence import (
    URLCrawlSpaceIntelligence
)


@dataclass
class DiscoveryDecision:
    url: str
    accepted: bool
    reason: str
    source: str
    depth: int
    crawl_action: str = "crawl_now"
    crawl_score: float = 100.0


class DiscoveryControl:

    def __init__(
        self,
        normalizer=None,
        crawl_space_intelligence=None,
        max_depth=20,
        max_url_length=4096,
        max_query_length=2048,
        max_path_length=4096
    ):
        self.normalizer = (
            normalizer
            if normalizer is not None
            else URLNormalizer()
        )

        self.crawl_space_intelligence = (
            crawl_space_intelligence
            if crawl_space_intelligence is not None
            else URLCrawlSpaceIntelligence()
        )

        self.max_depth = max(
            0,
            int(max_depth)
        )

        self.max_url_length = max(
            256,
            int(max_url_length)
        )

        self.max_query_length = max(
            256,
            int(max_query_length)
        )

        self.max_path_length = max(
            256,
            int(max_path_length)
        )

    def evaluate(
        self,
        url,
        *,
        source="discovery",
        depth=0,
        seed=False
    ):
        if not isinstance(url, str):
            return DiscoveryDecision(
                url="",
                accepted=False,
                reason="invalid_type",
                source=source,
                depth=depth
            )

        depth = max(
            0,
            int(depth)
        )

        normalized = self.normalizer.normalize(
            url
        )

        if normalized is None:
            return DiscoveryDecision(
                url=url,
                accepted=False,
                reason="invalid_url",
                source=source,
                depth=depth
            )

        if len(normalized) > self.max_url_length:
            return DiscoveryDecision(
                url=normalized,
                accepted=False,
                reason="url_too_long",
                source=source,
                depth=depth
            )

        if (
            depth > self.max_depth
            and not seed
        ):
            return DiscoveryDecision(
                url=normalized,
                accepted=False,
                reason="depth_limit",
                source=source,
                depth=depth
            )

        try:
            parsed = urlsplit(
                normalized
            )
        except Exception:
            return DiscoveryDecision(
                url=normalized,
                accepted=False,
                reason="parse_failed",
                source=source,
                depth=depth
            )

        if len(parsed.path) > self.max_path_length:
            return DiscoveryDecision(
                url=normalized,
                accepted=False,
                reason="path_too_long",
                source=source,
                depth=depth
            )

        if len(parsed.query) > self.max_query_length:
            return DiscoveryDecision(
                url=normalized,
                accepted=False,
                reason="query_too_long",
                source=source,
                depth=depth
            )

        intelligence = (
            self.crawl_space_intelligence.evaluate(
                normalized
            )
        )

        if intelligence.action == "reject":
            return DiscoveryDecision(
                url=normalized,
                accepted=False,
                reason="crawl_space_rejected",
                source=source,
                depth=depth,
                crawl_action=intelligence.action,
                crawl_score=intelligence.score
            )

        reason = "accepted"

        if intelligence.reasons:
            reason = (
                "accepted:"
                + ",".join(
                    intelligence.reasons
                )
            )

        return DiscoveryDecision(
            url=normalized,
            accepted=True,
            reason=reason,
            source=source,
            depth=depth,
            crawl_action=intelligence.action,
            crawl_score=intelligence.score
        )
