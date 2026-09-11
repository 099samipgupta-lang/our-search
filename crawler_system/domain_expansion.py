from dataclasses import dataclass
from threading import Lock
from urllib.parse import urlsplit


@dataclass(frozen=True)
class DomainExpansionEvent:
    hostname: str
    url: str
    source_hostname: str | None = None

    def to_dict(self):
        return {
            "hostname": self.hostname,
            "url": self.url,
            "source_hostname": self.source_hostname,
        }


class DomainExpansionDetector:

    def __init__(self):
        self._lock = Lock()
        self._known_hosts = set()

    @staticmethod
    def hostname(url):
        if not isinstance(url, str):
            return None

        try:
            parsed = urlsplit(url)
        except Exception:
            return None

        hostname = parsed.hostname

        if not hostname:
            return None

        return hostname.rstrip(".").lower()

    def register(self, url):
        host = self.hostname(url)

        if not host:
            return False

        with self._lock:
            already_known = host in self._known_hosts
            self._known_hosts.add(host)

        return not already_known

    def evaluate(self, url, source_url=None):
        host = self.hostname(url)

        if not host:
            return None

        source_host = self.hostname(source_url)

        with self._lock:
            if host in self._known_hosts:
                return None

            self._known_hosts.add(host)

        return DomainExpansionEvent(
            hostname=host,
            url=url,
            source_hostname=source_host,
        )

    def known_hosts(self):
        with self._lock:
            return set(self._known_hosts)

    def count(self):
        with self._lock:
            return len(self._known_hosts)
