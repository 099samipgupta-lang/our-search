from crawler_system.discovery import WebDiscovery
from crawler_system.discovery_sources import (
    DiscoveryContext,
    DiscoveryItem,
)


class LinkDiscoverySource:
    name = "link"

    def __init__(self, discovery=None):
        self.discovery = (
            discovery
            if discovery is not None
            else WebDiscovery()
        )

    def can_discover(self, context: DiscoveryContext):
        if not context.body:
            return False

        content_type = context.content_type or ""

        return "html" in content_type.lower()

    def discover(self, url, body=None):
        if not body:
            return set()

        urls = self.discovery.discover(url, body)

        return {
            DiscoveryItem(
                url=discovered_url,
                source=self.name,
                source_url=url,
            )
            for discovered_url in urls
        }
