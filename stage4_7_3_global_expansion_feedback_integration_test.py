import os
import tempfile

from crawler_system.domain_candidate_store import DomainCandidateStore
from crawler_system.expansion_store import ExpansionCandidateStore
from crawler_system.expansion_queue import ExpansionQueue
from crawler_system.domain_candidate_activation import DomainCandidateActivator
from crawler_system.domain_discovery_sources import DomainCandidate
from crawler_system.global_expansion_feedback import GlobalExpansionFeedback
from crawler_system.global_expansion_feedback_controller import (
    GlobalExpansionFeedbackController,
)
from crawler_system.continuous_domain_discovery_loop import (
    ContinuousDomainDiscoveryLoop,
)


class FakeDiscoveryPipeline:
    def __init__(self, domain_store):
        self.domain_store = domain_store

    def discover_and_store(self, context):
        return {
            "discovered": 0,
            "accepted": 0,
            "stored": 0,
            "duplicates": 0,
        }

    def stored_candidates(self, status="discovered", limit=100):
        return self.domain_store.list_candidates(
            status=status,
            limit=limit,
        )


def add_domain(
    store,
    hostname,
    source,
    priority,
):
    candidate = DomainCandidate(
        hostname=hostname,
        url=f"https://{hostname}/",
        source=source,
    )

    store.add(candidate)

    if priority != 50.0:
        store.update_priority(hostname, priority)


def main():
    with tempfile.TemporaryDirectory() as temp_dir:
        domain_db = os.path.join(
            temp_dir,
            "domains.sqlite3",
        )
        expansion_db = os.path.join(
            temp_dir,
            "expansion.sqlite3",
        )
        feedback_db = os.path.join(
            temp_dir,
            "feedback.sqlite3",
        )

        domain_store = DomainCandidateStore(
            database_path=domain_db,
        )

        expansion_store = ExpansionCandidateStore(
            database_path=expansion_db,
        )

        expansion_queue = ExpansionQueue(
            database_path=expansion_db,
            lease_timeout=60.0,
        )

        feedback = GlobalExpansionFeedback(
            database_path=feedback_db,
        )

        controller = GlobalExpansionFeedbackController(
            domain_store=domain_store,
            expansion_queue=expansion_queue,
            expansion_store=expansion_store,
            feedback=feedback,
        )

        activator = DomainCandidateActivator(
            domain_store=domain_store,
            expansion_store=expansion_store,
            expansion_queue=expansion_queue,
        )

        # ---------------------------------------------------------
        # 1. ESTABLISH DIFFERENT HISTORIES
        # ---------------------------------------------------------

        add_domain(
            domain_store,
            "good-source.example",
            "good_source",
            50.0,
        )

        add_domain(
            domain_store,
            "bad-source.example",
            "bad_source",
            50.0,
        )

        for index in range(4):
            feedback.record_outcome(
                hostname=f"good-{index}.example",
                source="good_source",
                outcome="activation_success",
            )

        for index in range(4):
            feedback.record_outcome(
                hostname=f"bad-{index}.example",
                source="bad_source",
                outcome="activation_failed",
            )

        good_multiplier = controller.adaptation_multiplier(
            "good_source"
        )

        bad_multiplier = controller.adaptation_multiplier(
            "bad_source"
        )

        assert good_multiplier > 1.0
        assert bad_multiplier < 1.0

        print("GOOD SOURCE ADAPTATION: PASS")
        print("BAD SOURCE ADAPTATION: PASS")
        print("SOURCE EFFECTIVENESS DIFFERENTIATION: PASS")

        # ---------------------------------------------------------
        # 2. VERIFY PRIORITY ADAPTATION
        # ---------------------------------------------------------

        good_record = domain_store.get(
            "good-source.example"
        )
        bad_record = domain_store.get(
            "bad-source.example"
        )

        good_priority = controller.adapt_candidate_priority(
            good_record
        )
        bad_priority = controller.adapt_candidate_priority(
            bad_record
        )

        assert good_priority > 50.0
        assert bad_priority < 50.0
        assert good_priority > bad_priority

        print("GOOD SOURCE PRIORITY BOOST: PASS")
        print("BAD SOURCE PRIORITY REDUCTION: PASS")
        print("ADAPTED PRIORITY ORDERING: PASS")

        # ---------------------------------------------------------
        # 3. VERIFY REAL ACTIVATOR RECEIVES ADAPTED PRIORITY
        # ---------------------------------------------------------

        class RecordingActivator:
            def __init__(self, real_activator):
                self.real = real_activator
                self.calls = []

            def activate(self, hostname, priority=None):
                self.calls.append(
                    {
                        "hostname": hostname,
                        "priority": priority,
                    }
                )
                return self.real.activate(
                    hostname,
                    priority=priority,
                )

        recording = RecordingActivator(activator)

        loop = ContinuousDomainDiscoveryLoop(
            discovery_pipeline=FakeDiscoveryPipeline(
                domain_store
            ),
            activator=recording,
            contexts=[],
            activation_batch_size=100,
            feedback_controller=controller,
        )

        result = loop.activate_once()

        assert result["requested"] == 2
        assert result["activated"] == 2
        assert len(recording.calls) == 2

        calls = {
            item["hostname"]: item["priority"]
            for item in recording.calls
        }

        assert calls["good-source.example"] == good_priority
        assert calls["bad-source.example"] == bad_priority
        assert calls["good-source.example"] > calls[
            "bad-source.example"
        ]

        print("REAL ACTIVATOR ADAPTATION: PASS")
        print("GOOD CANDIDATE QUEUE PRIORITY: PASS")
        print("BAD CANDIDATE QUEUE PRIORITY: PASS")

        # ---------------------------------------------------------
        # 4. VERIFY DURABLE QUEUE STATE
        # ---------------------------------------------------------

        good_queue = None
        bad_queue = None

        with expansion_queue._lock:
            connection = expansion_queue._connect()

            try:
                good_row = connection.execute(
                    """
                    SELECT hostname, priority
                    FROM expansion_queue
                    WHERE hostname = ?
                    """,
                    ("good-source.example",),
                ).fetchone()

                bad_row = connection.execute(
                    """
                    SELECT hostname, priority
                    FROM expansion_queue
                    WHERE hostname = ?
                    """,
                    ("bad-source.example",),
                ).fetchone()

                if good_row is not None:
                    good_queue = dict(good_row)

                if bad_row is not None:
                    bad_queue = dict(bad_row)

            finally:
                connection.close()

        assert good_queue is not None
        assert bad_queue is not None
        assert good_queue["priority"] == good_priority
        assert bad_queue["priority"] == bad_priority
        assert good_queue["priority"] > bad_queue["priority"]

        print("QUEUE PERSISTED ADAPTED PRIORITY: PASS")
        print("QUEUE PRIORITY ORDER: PASS")

        # ---------------------------------------------------------
        # 5. FEEDBACK RESTART PERSISTENCE
        # ---------------------------------------------------------

        feedback.close()

        restarted_feedback = GlobalExpansionFeedback(
            database_path=feedback_db,
        )

        restarted_controller = GlobalExpansionFeedbackController(
            domain_store=domain_store,
            expansion_queue=expansion_queue,
            expansion_store=expansion_store,
            feedback=restarted_feedback,
        )

        assert (
            restarted_controller.adaptation_multiplier(
                "good_source"
            )
            == good_multiplier
        )

        assert (
            restarted_controller.adaptation_multiplier(
                "bad_source"
            )
            == bad_multiplier
        )

        print("FEEDBACK RESTART PERSISTENCE: PASS")

        # ---------------------------------------------------------
        # 6. UNKNOWN SOURCE REMAINS NEUTRAL
        # ---------------------------------------------------------

        neutral_record = {
            "hostname": "neutral.example",
            "priority": 50.0,
            "source": "unknown_source",
        }

        assert (
            restarted_controller.adapt_candidate_priority(
                neutral_record
            )
            == 50.0
        )

        print("UNKNOWN SOURCE NEUTRALITY: PASS")

        print("RESULT: PASS")


if __name__ == "__main__":
    main()
