from crawler_system.worker_ownership import (
    WorkerNode,
    WorkerOwnership,
)


def main():
    print("=" * 72)
    print("STAGE 5.4.1 — DISTRIBUTED WORKER ARCHITECTURE TEST")
    print("=" * 72)

    nodes = [
        WorkerNode("node-0", 4),
        WorkerNode("node-1", 4),
        WorkerNode("node-2", 4),
        WorkerNode("node-3", 4),
    ]

    ownership = WorkerOwnership(
        partition_count=64,
        nodes=nodes,
        cluster_id="our-search-stage5",
    )

    assert ownership.partition_count == 64
    assert ownership.node_ids() == [
        "node-0",
        "node-1",
        "node-2",
        "node-3",
    ]
    print("PASS — worker-node configuration validated")

    assert ownership.validate_complete_ownership()
    print("PASS — every partition has exactly one owner")

    ownership_map = ownership.ownership_map()

    assert len(ownership_map) == 64
    assert set(ownership_map.values()).issubset(
        set(ownership.node_ids())
    )
    print("PASS — complete partition ownership map")

    for partition_id, owner in ownership_map.items():
        assert ownership.owns_partition(
            owner,
            partition_id,
        )

    print("PASS — partition → node ownership validation")

    for node_id in ownership.node_ids():
        owned = ownership.partitions_for_node(
            node_id
        )

        for partition_id in owned:
            assert ownership.owner_for_partition(
                partition_id
            ) == node_id

    print("PASS — node ownership enumeration")

    try:
        ownership.owner_for_partition(64)
        raise AssertionError(
            "invalid partition accepted"
        )
    except ValueError:
        pass

    print("PASS — invalid partition rejected")

    try:
        ownership.partitions_for_node(
            "unknown-node"
        )
        raise AssertionError(
            "unknown node accepted"
        )
    except ValueError:
        pass

    print("PASS — unknown node rejected")

    print("=" * 72)
    print("RESULT: PASS")
    print("STAGE 5.4.1 ARCHITECTURE: PASS")
    print("=" * 72)


if __name__ == "__main__":
    main()
