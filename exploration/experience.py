from exploration.brain import OurSearchBrain


class ExplorationExperience:
    """
    Main exploration layer of OUR SEARCH.

    Search remains responsible for retrieving documents.
    The OUR SEARCH brain operates on those results and builds
    the richer conversational exploration response.
    """

    EXPERIENCE_VERSION = "stage9.brain.v2"

    def __init__(self, brain=None):
        self.brain = (
            brain
            if brain is not None
            else OurSearchBrain()
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

        brain_result = self.brain.think(
            query=query,
            results=primary,
        )

        related = brain_result.relationships[
            :max(0, int(related_limit))
        ]

        return {
            "experience_version": self.EXPERIENCE_VERSION,
            "brain_version": self.brain.VERSION,
            "query": query,

            # Existing search results remain unchanged.
            "results": primary,

            # Existing related-results interface remains.
            "related": related,

            # New OUR SEARCH brain output.
            "brain": {
                "understanding": brain_result.understanding,
                "knowledge": brain_result.knowledge,
                "reasoning": brain_result.reasoning,
                "verification": brain_result.verification,
                "explanation": brain_result.explanation,
                "conversation": brain_result.conversation,
                "concepts": brain_result.concepts,
            },
        }
