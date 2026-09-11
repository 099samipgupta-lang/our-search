import re
from urllib.parse import urlsplit, parse_qsl


class CrawlSpaceDecision:

    def __init__(
        self,
        url,
        score,
        action,
        reasons
    ):
        self.url = url
        self.score = float(score)
        self.action = action
        self.reasons = list(reasons)

    def to_dict(self):
        return {
            "url": self.url,
            "score": self.score,
            "action": self.action,
            "reasons": self.reasons
        }


class URLCrawlSpaceIntelligence:

    QUERY_SPACE_PATTERNS = {
        "sort",
        "order",
        "filter",
        "filters",
        "facet",
        "facets",
        "category",
        "categories",
        "tag",
        "tags",
        "page",
        "offset",
        "limit",
        "start",
        "cursor",
        "price",
        "min_price",
        "max_price",
        "rating",
        "color",
        "size",
        "brand",
        "brands",
        "search",
        "query",
        "q"
    }

    PATH_SPACE_PATTERNS = (
        re.compile(r"/page/\d+(?:/|$)", re.I),
        re.compile(r"/p/\d+(?:/|$)", re.I),
        re.compile(r"/offset/\d+(?:/|$)", re.I),
        re.compile(r"/start/\d+(?:/|$)", re.I),
        re.compile(r"/search(?:/|$)", re.I),
        re.compile(r"/filter(?:/|$)", re.I),
        re.compile(r"/filters?(?:/|$)", re.I),
        re.compile(r"/facet(?:/|$)", re.I),
        re.compile(r"/facets?(?:/|$)", re.I)
    )

    STATIC_EXTENSIONS = {
        ".jpg",
        ".jpeg",
        ".png",
        ".gif",
        ".webp",
        ".svg",
        ".ico",
        ".mp3",
        ".wav",
        ".ogg",
        ".mp4",
        ".webm",
        ".avi",
        ".mov",
        ".zip",
        ".gz",
        ".tar",
        ".rar",
        ".7z",
        ".exe",
        ".dmg",
        ".iso"
    }

    def __init__(
        self,
        *,
        max_path_segments=20,
        max_query_parameters=12
    ):
        self.max_path_segments = max(
            1,
            int(max_path_segments)
        )

        self.max_query_parameters = max(
            1,
            int(max_query_parameters)
        )

    def evaluate(self, url):

        if not isinstance(url, str):
            return CrawlSpaceDecision(
                url="",
                score=0,
                action="reject",
                reasons=["invalid_type"]
            )

        try:
            parsed = urlsplit(url)
        except Exception:
            return CrawlSpaceDecision(
                url=url,
                score=0,
                action="reject",
                reasons=["parse_failed"]
            )

        score = 100.0
        reasons = []

        path = parsed.path or "/"
        query = parsed.query or ""

        parameters = parse_qsl(
            query,
            keep_blank_values=True
        )

        segments = [
            segment
            for segment in path.split("/")
            if segment
        ]

        # ---------------------------------------------------------
        # Static resources
        # ---------------------------------------------------------

        lower_path = path.lower()

        for extension in self.STATIC_EXTENSIONS:

            if lower_path.endswith(extension):

                return CrawlSpaceDecision(
                    url=url,
                    score=0,
                    action="reject",
                    reasons=["static_asset"]
                )

        # ---------------------------------------------------------
        # Query-space signals
        # ---------------------------------------------------------

        if parameters:

            score -= min(
                len(parameters) * 3,
                18
            )

            reasons.append(
                "query_parameters"
            )

        query_space_count = 0

        for key, _ in parameters:

            if key.lower() in self.QUERY_SPACE_PATTERNS:
                query_space_count += 1

        if query_space_count:

            score -= min(
                query_space_count * 7,
                28
            )

            reasons.append(
                "query_space_signal"
            )

        if len(parameters) > self.max_query_parameters:

            score -= 20

            reasons.append(
                "many_query_parameters"
            )

        # ---------------------------------------------------------
        # Known enumerable path patterns
        # ---------------------------------------------------------

        crawl_space_path = False

        for pattern in self.PATH_SPACE_PATTERNS:

            if pattern.search(path):

                crawl_space_path = True

                score -= 30

                reasons.append(
                    "crawl_space_path_pattern"
                )

                break

        # ---------------------------------------------------------
        # Numeric path segments
        #
        # Numeric segments alone are NOT considered dangerous.
        # Dates, IDs and version numbers are common legitimate
        # Web content structures.
        # ---------------------------------------------------------

        numeric_segments = sum(
            1
            for segment in segments
            if segment.isdigit()
        )

        if numeric_segments >= 3 and not crawl_space_path:

            score -= 8

            reasons.append(
                "multiple_numeric_segments"
            )

        # ---------------------------------------------------------
        # Repeated structural segments
        # ---------------------------------------------------------

        if len(segments) >= 5:

            normalized = [
                re.sub(
                    r"\d+",
                    "{n}",
                    segment.lower()
                )
                for segment in segments
            ]

            repeated_count = max(
                (
                    normalized.count(segment)
                    for segment in set(normalized)
                ),
                default=0
            )

            if repeated_count >= 3:

                score -= 15

                reasons.append(
                    "repeated_path_structure"
                )

        # ---------------------------------------------------------
        # Deep paths
        # ---------------------------------------------------------

        if len(segments) > self.max_path_segments:

            score -= 15

            reasons.append(
                "deep_path"
            )

        # ---------------------------------------------------------
        # Final classification
        # ---------------------------------------------------------

        score = max(
            0.0,
            min(score, 100.0)
        )

        if crawl_space_path and score < 75:

            action = "low_priority"

        elif score >= 75:

            action = "crawl_now"

        elif score >= 45:

            action = "crawl_later"

        elif score > 0:

            action = "low_priority"

        else:

            action = "reject"

        return CrawlSpaceDecision(
            url=url,
            score=score,
            action=action,
            reasons=reasons
        )
