from dataclasses import replace
from urllib.parse import urlsplit


class DomainCandidateNormalizer:
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

        if "://" in hostname or "/" in hostname:
            return None

        try:
            hostname = hostname.encode(
                "idna"
            ).decode("ascii").lower()
        except (UnicodeError, ValueError):
            return None

        return hostname

    @classmethod
    def normalize_url(cls, url, hostname):
        if not isinstance(url, str):
            return None

        url = url.strip()

        if not url:
            return None

        try:
            parsed = urlsplit(url)
        except Exception:
            return None

        if parsed.scheme.lower() not in (
            "http",
            "https",
        ):
            return None

        parsed_hostname = parsed.hostname

        if not parsed_hostname:
            return None

        parsed_hostname = cls.normalize_hostname(
            parsed_hostname
        )

        if parsed_hostname != hostname:
            return None

        return f"{parsed.scheme.lower()}://{hostname}/"

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
            hostname,
        )

        if url is None:
            return None

        try:
            return replace(
                candidate,
                hostname=hostname,
                url=url,
            )
        except (TypeError, ValueError):
            return None


class DomainCandidateValidator:
    @staticmethod
    def validate_hostname(hostname):
        if not isinstance(hostname, str):
            return False

        if not hostname:
            return False

        if len(hostname) > 253:
            return False

        labels = hostname.split(".")

        if len(labels) < 2:
            return False

        for label in labels:
            if not label:
                return False

            if len(label) > 63:
                return False

            if label.startswith("-"):
                return False

            if label.endswith("-"):
                return False

            for character in label:
                if not (
                    character.isalnum()
                    or character == "-"
                ):
                    return False

        if labels[-1].isdigit():
            return False

        return True

    @classmethod
    def validate(cls, candidate):
        if candidate is None:
            return False

        hostname = getattr(
            candidate,
            "hostname",
            None,
        )

        if not cls.validate_hostname(hostname):
            return False

        url = getattr(candidate, "url", None)

        if not isinstance(url, str):
            return False

        try:
            parsed = urlsplit(url)
        except Exception:
            return False

        if parsed.scheme.lower() not in (
            "http",
            "https",
        ):
            return False

        if parsed.hostname != hostname:
            return False

        if parsed.path not in ("", "/"):
            return False

        if parsed.query:
            return False

        if parsed.fragment:
            return False

        source = getattr(
            candidate,
            "source",
            None,
        )

        if not isinstance(source, str):
            return False

        if not source.strip():
            return False

        return True


class DomainCandidateDeduplicator:
    def __init__(self):
        self._seen = set()

    def is_new(self, hostname):
        if not isinstance(hostname, str):
            return False

        return hostname not in self._seen

    def add(self, hostname):
        if not isinstance(hostname, str):
            return False

        if hostname in self._seen:
            return False

        self._seen.add(hostname)
        return True

    def contains(self, hostname):
        return hostname in self._seen

    def count(self):
        return len(self._seen)

    def get_state(self):
        return set(self._seen)

    def load_state(self, values):
        if values is None:
            return

        self._seen = {
            value
            for value in values
            if isinstance(value, str)
        }


class DomainCandidatePipeline:
    """
    Normalizes, validates, and deduplicates independently
    discovered domain candidates.

    When multiple discovery sources report the same hostname,
    process_many() deterministically selects the strongest
    candidate instead of depending on set iteration order.
    """

    SOURCE_PRIORITY = {
        "certificate_transparency": 100,
        "registry": 90,
        "public_registry": 90,
        "feed": 70,
        "sitemap": 60,
        "link": 50,
    }

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

    @classmethod
    def _candidate_rank(cls, candidate):
        source = getattr(
            candidate,
            "source",
            "",
        )

        source = (
            source.strip().lower()
            if isinstance(source, str)
            else ""
        )

        source_score = cls.SOURCE_PRIORITY.get(
            source,
            0,
        )

        evidence = getattr(
            candidate,
            "evidence",
            None,
        )

        evidence_score = (
            1
            if isinstance(evidence, str)
            and evidence.strip()
            else 0
        )

        metadata = getattr(
            candidate,
            "metadata",
            None,
        )

        metadata_score = (
            1
            if isinstance(metadata, dict)
            and metadata
            else 0
        )

        # Higher values win.
        #
        # Source strength is dominant. Evidence and metadata
        # are secondary. Source name is a stable final
        # deterministic tie-breaker.
        return (
            source_score,
            evidence_score,
            metadata_score,
            source,
        )

    def process(self, candidate):
        normalized = self.normalizer.normalize_candidate(
            candidate
        )

        if normalized is None:
            return None

        if not self.validator.validate(
            normalized
        ):
            return None

        if not self.deduplicator.is_new(
            normalized.hostname
        ):
            return None

        self.deduplicator.add(
            normalized.hostname
        )

        return normalized

    def process_many(self, candidates):
        if candidates is None:
            return set()

        normalized_candidates = []

        for candidate in candidates:
            normalized = (
                self.normalizer.normalize_candidate(
                    candidate
                )
            )

            if normalized is None:
                continue

            if not self.validator.validate(
                normalized
            ):
                continue

            normalized_candidates.append(
                normalized
            )

        # Group by normalized hostname so duplicate reports
        # from different discovery sources can be resolved
        # deterministically.
        grouped = {}

        for candidate in normalized_candidates:
            hostname = candidate.hostname

            existing = grouped.get(hostname)

            if existing is None:
                grouped[hostname] = candidate
                continue

            if self._candidate_rank(
                candidate
            ) > self._candidate_rank(existing):
                grouped[hostname] = candidate

        accepted = set()

        # Stable hostname ordering makes processing deterministic.
        for hostname in sorted(grouped):
            candidate = grouped[hostname]

            if not self.deduplicator.is_new(
                hostname
            ):
                continue

            self.deduplicator.add(hostname)
            accepted.add(candidate)

        return accepted

    def count(self):
        return self.deduplicator.count()
