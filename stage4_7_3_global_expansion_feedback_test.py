import tempfile
from pathlib import Path

from crawler_system.global_expansion_feedback import (
    GlobalExpansionFeedback,
)


def main():
    with tempfile.TemporaryDirectory() as temp:
        database = Path(temp) / "feedback.db"

        feedback = GlobalExpansionFeedback(database)

        first = feedback.record_outcome(
            "alpha.example",
            "source_a",
            "crawl_success",
        )

        assert first["recorded"] is True
        assert first["duplicate"] is False

        duplicate = feedback.record_outcome(
            "alpha.example",
            "source_a",
            "crawl_success",
        )

        assert duplicate["recorded"] is False
        assert duplicate["duplicate"] is True

        feedback.record_outcome(
            "beta.example",
            "source_a",
            "crawl_success",
        )

        feedback.record_outcome(
            "gamma.example",
            "source_a",
            "crawl_failed",
        )

        feedback.record_outcome(
            "delta.example",
            "source_b",
            "crawl_failed",
        )

        source_a = feedback.source_stats("source_a")
        source_b = feedback.source_stats("source_b")

        assert source_a is not None
        assert source_b is not None

        assert source_a["successes"] == 2
        assert source_a["failures"] == 1
        assert source_a["total_events"] == 3

        assert source_b["successes"] == 0
        assert source_b["failures"] == 1

        assert source_a["score"] > source_b["score"]

        adapted_a = feedback.adapt_priority(
            50,
            "source_a",
        )

        adapted_b = feedback.adapt_priority(
            50,
            "source_b",
        )

        assert adapted_a > adapted_b

        restarted = GlobalExpansionFeedback(database)

        assert restarted.feedback_count() == 4

        restarted_a = restarted.source_stats("source_a")

        assert restarted_a["successes"] == 2
        assert restarted_a["failures"] == 1

        print("RESULT: PASS")


if __name__ == "__main__":
    main()
