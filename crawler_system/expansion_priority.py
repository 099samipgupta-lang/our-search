from urllib.parse import urlsplit


class ExpansionPriority:

    def __init__(self):
        self.default_score = 50.0

    def score(
        self,
        url,
        *,
        source_url=None,
        source="link",
        discovery_depth=0,
    ):
        if not isinstance(url, str):
            return 0.0

        try:
            parsed = urlsplit(url)
        except Exception:
            return 0.0

        if parsed.scheme not in ("http", "https"):
            return 0.0

        if not parsed.hostname:
            return 0.0

        score = self.default_score

        # Direct links are the strongest initial expansion signal.
        if source == "link":
            score += 20

        elif source == "sitemap":
            score += 15

        elif source == "seed":
            score += 30

        # Prefer shallow discovery paths.
        score -= min(
            max(int(discovery_depth), 0) * 3,
            20
        )

        # Prefer HTTPS.
        if parsed.scheme == "https":
            score += 5

        # A simple URL is generally a better expansion entry point.
        path = parsed.path or "/"

        if path == "/":
            score += 10

        # Avoid giving highly parameterized entry URLs high priority.
        if parsed.query:
            score -= 10

        score = max(
            1.0,
            min(score, 100.0)
        )

        return score
