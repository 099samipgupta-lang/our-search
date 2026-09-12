from crawler_system.discovery_sources import (
    DiscoveryContext,
    DiscoveryItem,
)
from crawler_system.feed_endpoint_discovery import (
    FeedEndpointDiscovery,
)


class FeedEndpointDiscoverySource:
    name = "feed_endpoint"

    def __init__(self, discovery=None):
        self.discovery = (
            discovery
            if discovery is not None
            else FeedEndpointDiscovery()
        )

    def can_discover(self, context: DiscoveryContext):
        if not context.body:
            return False

        content_type = context.content_type or ""

        return "html" in content_type.lower()

    def discover(self, url, body=None):
        if not body:
            return set()

        feeds = self.discovery.discover(
            url,
            body,
        )

        return {
            DiscoveryItem(
                url=feed_url,
                source=self.name,
                source_url=url,
            )
            for feed_url in feeds
        }
