from crawler_system.sitemap import SitemapDiscovery
from crawler_system.discovery_sources import DiscoveryItem


class SitemapDiscoverySource:

    name = "sitemap"

    def __init__(self, discovery=None):
        self.discovery = (
            discovery
            if discovery is not None
            else SitemapDiscovery()
        )

    def discover(self, url, body=None):
        urls = self.discovery.discover(url)

        return {
            DiscoveryItem(
                url=discovered_url,
                source=self.name,
                source_url=url
            )
            for discovered_url in urls
        }
