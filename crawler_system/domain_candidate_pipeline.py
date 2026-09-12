from dataclasses import replace
from urllib.parse import urlsplit


class DomainCandidateNormalizer:
    """Canonicalize domain-discovery candidates."""

    @staticmethod
    def normalize_hostname(hostname):
        if not isinstance(hostname, str):
            return None

        hostname = hostname.strip().lower()

        if hostname.startswith("*."):
            hostname = hostname[2:]

        hostname = hostname.rstrip(".")

        if not hostname:
            return None

        # A domain candidate must be a hostname, not a URL.
        if "://" in hostname or "/" in hostname:
            return None

        try:
            hostname = hostname.encode("idna").decode("ascii")
        except Exception:
            return None

        return hostname

    @classmethod
    def normalize_url(cls, url, hostname=None):
        if not isinstance(url, str):
            return None

        url = url.strip()

        if not url:
            return None

        try:
            parsed = urlsplit(url)
        except Exception:
            return None

        if parsed.scheme.lower() not in ("http", "https"):
            return None

        parsed_hostname = cls.normalize_hostname(
            parsed.hostname
        )

        if parsed_hostname is None:
            return None

        if hostname is not None:
            normalized_hostname = cls.normalize_hostname(
                hostname
            )

            if normalized_hostname is None:
                return None

            if parsed_hostname != normalized_hostname:
                return None

        scheme = parsed.scheme.lower()

        # Domain discovery candidates represent the domain root.
        return f"{scheme}://{parsed_hostname}/"

    @classmethod
    def normalize_candidate(cls, candidate):
        if candidate is None:
            return None

        hostname = cls.normalize_hostname(
            getattr(candidate, "hostname", None)
        )

        if hostname is None:
            return None

        url = cls.normalize_url(
            getattr(candidate, "url", None),
            hostname=hostname,
        )

        if url is None:
            return None

        return replace(
            candidate,
            hostname=hostname,
            url=url,
        )


class DomainCandidateValidator:
    """Validate normalized domain-discovery candidates."""

    @staticmethod
    def is_valid_hostname(hostname):
        if not isinstance(hostname, str):
            return False

        if not hostname:
            return False

        if len(hostname) > 253:
            return False

        labels = hostname.split(".")

        # We require an actual domain-like hostname.
        if len(labels) < 2:
            return False

        for label in labels:
            if not label or len(label) > 63:
                return False

            if label.startswith("-") or label.endswith("-"):
                return False

            if not all(
                character.isalnum() or character == "-"
                for character in label
            ):
                return False

        # Avoid treating an IP-like value as a discovered domain.
        final_label = labels[-1]

        if final_label.isdigit():
            return False

        return True

    @classmethod
    def validate(cls, candidate):
        if candidate is None:
            return False

        hostname = getattr(candidate, "hostname", None)
        url = getattr(candidate, "url", None)
        source = getattr(candidate, "source", None)

        if not cls.is_valid_hostname(hostname):
            return False

        if not isinstance(url, str) or not url:
            return False

        if not isinstance(source, str) or not source.strip():
            return False

        try:
            parsed = urlsplit(url)
        except Exception:
            return False

        if parsed.scheme.lower() not in ("http", "https"):
            return False

        if parsed.hostname != hostname:
            return False

        if parsed.path not in ("", "/"):
            return False

        if parsed.query or parsed.fragment:
            return False

        return True


class DomainCandidateDeduplicator:
    """Deduplicate candidates by canonical hostname."""

    def __init__(self):
        self._seen = set()

    def is_new(self, candidate):
        if candidate is None:
            return False

        hostname = getattr(candidate, "hostname", None)

        if not hostname:
            return False

        if hostname in self._seen:
            return False

        self._seen.add(hostname)
        return True

    def add(self, candidate):
        return self.is_new(candidate)

    def contains(self, hostname):
        return hostname in self._seen

    def count(self):
        return len(self._seen)

    def get_state(self):
        return sorted(self._seen)

    def load_state(self, state):
        if not state:
            return

        self._seen = set(state)


class DomainCandidatePipeline:
    """
    Normalize, validate, and deduplicate domain candidates.

    This component is intentionally independent from the crawler's
    URLDeduplicator and persistent URL state.
    """

    def __init__(
        self,
        normalizer=None,
        validator=None,
        deduplicator=None,
    ):
        self.normalizer = (
            normalizer
            if normalizer is not None
            else DomainCandidateNormalizer()
        )

        self.validator = (
            validator
            if validator is not None
            else DomainCandidateValidator()
        )

        self.deduplicator = (
            deduplicator
            if deduplicator is not None
            else DomainCandidateDeduplicator()
        )

    def process(self, candidate):
        normalized = self.normalizer.normalize_candidate(
            candidate
        )

        if normalized is None:
            return None

        if not self.validator.validate(normalized):
            return None

        if not self.deduplicator.is_new(normalized):
            return None

        return normalized

    def process_many(self, candidates):
        if candidates is None:
            return set()

        results = set()

        for candidate in candidates:
            processed = self.process(candidate)

            if processed is not None:
                results.add(processed)

        return results

    def count(self):
        return self.deduplicator.count()
