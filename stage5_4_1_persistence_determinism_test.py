import json
import os
import tempfile

from crawler_system.worker_ownership import (
    WorkerNode,
    WorkerOwnership,
)


def main():
    print("=" * 72)
    print("STAGE 5.4.1 — PERSISTENCE & DETERMINISM TEST")
    print("=" * 72)

    nodes = [
        WorkerNode("worker-0", 4),
        WorkerNode("worker-1", 4),
        WorkerNode("worker-2", 4),
        WorkerNode("worker-3", 4),
    ]

    original = WorkerOwnership(
        partition_count=512,
        nodes=nodes,
        cluster_id="our-search-persistence-test",
    )

    original_map = original.ownership_map()

    with tempfile.TemporaryDirectory(
        prefix="our_search_stage5_4_1_"
    ) as workspace:

        path = os.path.join(
            workspace,
            "ownership.json",
        )

        original.save_configuration(path)

        assert os.path.exists(path)
        print(
            "PASS — ownership configuration persisted"
        )

        loaded = WorkerOwnership.load_configuration(
            path
        )

        assert loaded.configuration() == (
            original.configuration()
        )

        print(
            "PASS — persisted configuration restored"
        )

        assert loaded.ownership_map() == original_map

        print(
            "PASS — persisted ownership map is identical"
        )

        raw = json.loads(
            open(
                path,
                "r",
                encoding="utf-8",
            ).read()
        )

        assert raw["schema_version"] == 1
        assert raw["partition_count"] == 512
        assert raw["cluster_id"] == (
            "our-search-persistence-test"
        )

        print(
            "PASS — persisted schema validated"
        )

    # Independently constructed objects must produce
    # exactly the same mapping.
    independent = WorkerOwnership(
        partition_count=512,
        nodes=[
            WorkerNode("worker-3", 4),
            WorkerNode("worker-1", 4),
            WorkerNode("worker-0", 4),
            WorkerNode("worker-2", 4),
        ],
        cluster_id="our-search-persistence-test",
    )

    assert independent.ownership_map() == original_map

    print(
        "PASS — independent construction is deterministic"
    )

    # Repeated calculations must remain identical.
    repeated = [
        original.ownership_map()
        for _ in range(5)
    ]

    assert all(
        mapping == original_map
        for mapping in repeated
    )

    print(
        "PASS — repeated ownership calculations are stable"
    )

    print("=" * 72)
    print("RESULT: PASS")
    print("STAGE 5.4.1 PERSISTENCE/DETERMINISM: PASS")
    print("=" * 72)


if __name__ == "__main__":
    main()
