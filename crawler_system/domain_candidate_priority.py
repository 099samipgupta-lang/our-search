from dataclasses import dataclass
from urllib.parse import urlsplit


@dataclass(frozen=True)
class DomainCandidatePriority:
    """
    Deterministic priority score for independently discovered
    domain candidates.

    Higher scores mean the candidate should be considered earlier
    by future discovery/activation queues.
    """

    default_score: float = 50.0

    def score(self, candidate) -> float:
        if candidate is None:
            return 0.0

        hostname = getattr(candidate, "hostname", None)
        url = getattr(candidate, "url", None)
        source = getattr(candidate, "source", None)
        evidence = getattr(candidate, "evidence", None)

        if not isinstance(hostname, str):
            return 0.0

        hostname = hostname.strip().lower().rstrip(".")

        if not hostname:
            return 0.0

        if not isinstance(url, str):
            return 0.0

        if not isinstance(source, str) or not source.strip():
            return 0.0

        score = self.default_score

        source_name = source.strip().lower()

        # Stronger independent discovery sources receive
        # higher starting confidence.
        source_scores = {
            "certificate_transparency": 20.0,
            "registry": 15.0,
            "public_registry": 15.0,
            "feed": 10.0,
            "sitemap": 5.0,
            "link": 5.0,
        }

        score += source_scores.get(source_name, 0.0)

        # Explicit evidence means the candidate can be traced
        # back to a concrete discovery record.
        if isinstance(evidence, str) and evidence.strip():
            score += 10.0

        try:
            parsed = urlsplit(url)

            if parsed.scheme.lower() == "https":
                score += 5.0

            if parsed.path in ("", "/"):
                score += 5.0

            if not parsed.query:
                score += 2.0

            if not parsed.fragment:
                score += 1.0

        except Exception:
            return 0.0

        # Prefer normal public-looking hostnames over very deep
        # subdomain chains. This is only a small adjustment;
        # it must never dominate source/evidence quality.
        labels = hostname.split(".")

        if len(labels) == 2:
            score += 5.0
        elif len(labels) == 3:
            score += 2.0
        elif len(labels) > 5:
            score -= min((len(labels) - 5) * 2.0, 6.0)

        return max(1.0, min(100.0, score))
