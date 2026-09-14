from crawler_system.worker_ownership import (
    WorkerNode,
    WorkerOwnership,
)


def main():
    print("=" * 72)
    print("STAGE 5.4.1 — MULTI-NODE OWNERSHIP TEST")
    print("=" * 72)

    nodes = [
        WorkerNode("node-a", 8),
        WorkerNode("node-b", 8),
        WorkerNode("node-c", 8),
        WorkerNode("node-d", 8),
    ]

    ownership = WorkerOwnership(
        partition_count=1000,
        nodes=nodes,
        cluster_id="our-search-production",
    )

    mapping = ownership.ownership_map()

    assert len(mapping) == 1000
    assert ownership.validate_complete_ownership()

    print(
        "PASS — 1000 partitions assigned exactly once"
    )

    counts = {}

    for node_id in ownership.node_ids():
        partitions = ownership.partitions_for_node(
            node_id
        )

        counts[node_id] = len(partitions)

        assert all(
            ownership.owner_for_partition(partition_id)
            == node_id
            for partition_id in partitions
        )

    print(
        "PASS — ownership enumeration verified for all nodes"
    )

    assert sum(counts.values()) == 1000
    assert all(
        count > 0
        for count in counts.values()
    )

    print(
        "PASS — all 4 nodes receive owned partitions"
    )

    print(
        "PARTITION DISTRIBUTION:",
        counts,
    )

    # Determinism across independently constructed instances.
    second = WorkerOwnership(
        partition_count=1000,
        nodes=[
            WorkerNode("node-d", 8),
            WorkerNode("node-b", 8),
            WorkerNode("node-a", 8),
            WorkerNode("node-c", 8),
        ],
        cluster_id="our-search-production",
    )

    assert mapping == second.ownership_map()

    print(
        "PASS — node ordering does not affect ownership"
    )

    # Worker count changes must not affect partition ownership.
    third = WorkerOwnership(
        partition_count=1000,
        nodes=[
            WorkerNode("node-a", 32),
            WorkerNode("node-b", 16),
            WorkerNode("node-c", 4),
            WorkerNode("node-d", 2),
        ],
        cluster_id="our-search-production",
    )

    assert mapping == third.ownership_map()

    print(
        "PASS — local worker counts do not affect partition ownership"
    )

    print("=" * 72)
    print("RESULT: PASS")
    print("STAGE 5.4.1 MULTI-NODE OWNERSHIP: PASS")
    print("=" * 72)


if __name__ == "__main__":
    main()
