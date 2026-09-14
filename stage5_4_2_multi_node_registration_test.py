from crawler_system.worker_registry import WorkerRegistry


def main():
    registry = WorkerRegistry(cluster_id="multi-node-test")

    expected = {
        "node-a": 4,
        "node-b": 8,
        "node-c": 16,
        "node-d": 32,
    }

    for index, (node_id, workers) in enumerate(expected.items()):
        registry.register(node_id, workers, now=float(index))

    assert registry.node_ids() == tuple(sorted(expected))
    assert registry.count() == 4
    assert registry.total_worker_capacity() == sum(expected.values())

    snapshot = registry.snapshot()
    assert snapshot["schema_version"] == 1
    assert snapshot["cluster_id"] == "multi-node-test"
    assert len(snapshot["nodes"]) == 4

    # Registration enumeration is deterministic regardless of insertion order.
    registry2 = WorkerRegistry(cluster_id="multi-node-test")
    for index, (node_id, workers) in enumerate(reversed(list(expected.items()))):
        registry2.register(node_id, workers, now=float(index))

    assert registry.node_ids() == registry2.node_ids()
    assert [n.node_id for n in registry.all_nodes()] == [
        n.node_id for n in registry2.all_nodes()
    ]

    # Inactive nodes are excluded from active capacity.
    registry.unregister("node-c")

    assert registry.count() == 3
    assert registry.total_worker_capacity() == 44
    assert registry.count(active_only=False) == 4

    print("STAGE 5.4.2 MULTI-NODE REGISTRATION TEST: PASS")


if __name__ == "__main__":
    main()
