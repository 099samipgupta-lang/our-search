from collections import defaultdict
from threading import Lock


class DiscoveryPriorityFeedback:
    """
    Tracks deterministic discovery-source outcomes and exposes
    bounded feedback for future priority decisions.

    Positive outcomes increase feedback.
    Negative outcomes decrease feedback.
    Feedback is bounded so one source cannot dominate forever.
    """

    def __init__(
        self,
        positive_weight=1.0,
        negative_weight=1.0,
        max_feedback=20.0,
    ):
        self.positive_weight = float(positive_weight)
        self.negative_weight = float(negative_weight)
        self.max_feedback = float(max_feedback)

        if self.positive_weight < 0:
            raise ValueError("positive_weight must be >= 0")

        if self.negative_weight < 0:
            raise ValueError("negative_weight must be >= 0")

        if self.max_feedback < 0:
            raise ValueError("max_feedback must be >= 0")

        self._feedback = defaultdict(float)
        self._lock = Lock()

    def record(self, source, *, success):
        if not isinstance(source, str) or not source.strip():
            return False

        source = source.strip()

        if not isinstance(success, bool):
            return False

        weight = (
            self.positive_weight
            if success
            else -self.negative_weight
        )

        with self._lock:
            current = self._feedback[source]
            updated = current + weight

            updated = max(
                -self.max_feedback,
                min(updated, self.max_feedback),
            )

            self._feedback[source] = updated

        return True

    def score(self, source):
        if not isinstance(source, str) or not source.strip():
            return 0.0

        with self._lock:
            return float(self._feedback[source.strip()])

    def snapshot(self):
        with self._lock:
            return dict(self._feedback)

    def reset(self, source=None):
        with self._lock:
            if source is None:
                self._feedback.clear()
                return True

            if not isinstance(source, str) or not source.strip():
                return False

            self._feedback.pop(source.strip(), None)
            return True
