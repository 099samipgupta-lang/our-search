import time


class CrawlPriority:

    def __init__(self):

        self.default_priority = 50

    def score(
        self,
        *,
        source="discovery",
        freshness=0,
        recrawl_due=False,
        depth=0,
        failure_count=0,
        seed=False
    ):

        score = self.default_priority

        if seed:
            score += 50

        if source == "sitemap":
            score += 20

        elif source == "link":
            score += 10

        elif source == "recrawl":
            score += 30

        if recrawl_due:
            score += 25

        score += min(
            max(freshness, 0),
            20
        )

        score -= min(
            depth * 2,
            20
        )

        score -= min(
            failure_count * 10,
            40
        )

        return max(
            1,
            min(
                score,
                200
            )
        )

    def recrawl_priority(
        self,
        age_seconds,
        expected_change_rate=0.0
    ):

        score = 30

        if expected_change_rate > 0:
            score += min(
                expected_change_rate * 50,
                40
            )

        if age_seconds > 86400:
            score += 20

        if age_seconds > 604800:
            score += 30

        if age_seconds > 2592000:
            score += 40

        return min(
            score,
            200
        )
