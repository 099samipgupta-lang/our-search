from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser


class CrawlPolicy:

    def __init__(self, user_agent="OurSearchBot/1.0"):
        self.user_agent = user_agent
        self.robots_cache = {}

    def is_valid_url(self, url):
        parsed = urlparse(url)

        return (
            parsed.scheme in ("http", "https")
            and bool(parsed.netloc)
        )

    def allowed_by_robots(self, url):
        parsed = urlparse(url)

        robots_url = (
            parsed.scheme
            + "://"
            + parsed.netloc
            + "/robots.txt"
        )

        if robots_url not in self.robots_cache:
            parser = RobotFileParser()
            parser.set_url(robots_url)

            try:
                parser.read()
            except Exception:
                return False

            self.robots_cache[robots_url] = parser

        return self.robots_cache[robots_url].can_fetch(
            self.user_agent,
            url
        )

    def allow(self, url):
        if not self.is_valid_url(url):
            return False

        return self.allowed_by_robots(url)
