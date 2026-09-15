from exploration.relationships import RelationshipEngine


class ExplorationExperience:
    EXPERIENCE_VERSION = "stage9.v1"

    def __init__(self, relationship_engine=None):
        self.relationships = (
            relationship_engine
            if relationship_engine is not None
            else RelationshipEngine()
        )

    def build(
        self,
        query,
        results,
        related_limit=10,
    ):
        if not isinstance(query, str):
            raise TypeError("query must be a string")

        if not isinstance(results, list):
            raise TypeError("results must be a list")

        primary = [
            dict(item)
            for item in results
            if isinstance(item, dict)
        ]

        related = []

        if primary:
            related = self.relationships.related_results(
                primary[0],
                primary,
                limit=related_limit,
            )

        return {
            "experience_version": self.EXPERIENCE_VERSION,
            "query": query,
            "results": primary,
            "related": related,
        }
