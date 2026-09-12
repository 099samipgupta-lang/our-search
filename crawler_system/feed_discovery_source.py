from crawler_system.discovery_sources import (
    DiscoveryContext,
    DiscoveryItem,
)
from crawler_system.feed_discovery import FeedDiscovery


class FeedDiscoverySource:
    name = "feed"

    def __init__(self, discovery=None):
        self.discovery = (
            discovery
            if discovery is not None
            else FeedDiscovery()
        )

    def can_discover(self, context: DiscoveryContext):
        if not context.body:
            return False

        content_type = (context.content_type or "").lower()

        return (
            "rss" in content_type
            or "atom" in content_type
            or "feed" in content_type
        )

    def discover(self, url, body=None):
        if not body:
            return set()

        urls = self.discovery.discover(
            url,
            body,
        )

        return {
            DiscoveryItem(
                url=discovered_url,
                source=self.name,
                source_url=url,
            )
            for discovered_url in urls
        }
