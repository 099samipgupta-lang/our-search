from crawler_system.discovery_priority_feedback import (
    DiscoveryPriorityFeedback,
)


class DiscoveryPriorityFeedbackPolicy:
    """
    Converts discovery-source feedback into a bounded priority
    adjustment.
    """

    def __init__(
        self,
        feedback=None,
        multiplier=1.0,
        max_adjustment=10.0,
    ):
        self.feedback = (
            feedback
            if feedback is not None
            else DiscoveryPriorityFeedback()
        )
        self.multiplier = float(multiplier)
        self.max_adjustment = float(max_adjustment)

        if self.multiplier < 0:
            raise ValueError("multiplier must be >= 0")

        if self.max_adjustment < 0:
            raise ValueError("max_adjustment must be >= 0")

    def adjustment(self, source):
        value = self.feedback.score(source)
        value *= self.multiplier

        return max(
            -self.max_adjustment,
            min(value, self.max_adjustment),
        )

    def apply(self, base_priority, source):
        try:
            base = float(base_priority)
        except (TypeError, ValueError):
            return 0.0

        adjusted = base + self.adjustment(source)

        return max(0.0, min(adjusted, 100.0))
