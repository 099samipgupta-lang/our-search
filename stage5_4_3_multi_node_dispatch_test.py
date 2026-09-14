from crawler_system.distributed_dispatcher import DistributedDispatcher
from crawler_system.task import CrawlTask
from crawler_system.worker_ownership import WorkerNode, WorkerOwnership
from crawler_system.worker_registry import WorkerRegistry


def main():
    nodes = [
        WorkerNode("node-a", 4),
        WorkerNode("node-b", 8),
        WorkerNode("node-c", 16),
        WorkerNode("node-d", 32),
    ]

    ownership = WorkerOwnership(
        partition_count=1000,
        nodes=nodes,
        cluster_id="multi-dispatch-test",
    )

    registry = WorkerRegistry(cluster_id="multi-dispatch-test")

    for index, node in enumerate(nodes):
        registry.register(
            node.node_id,
            node.worker_count,
            now=float(index),
        )

    dispatcher = DistributedDispatcher(ownership, registry)

    tasks = [
        CrawlTask(
            url=f"https://host-{index}.example/page",
            document_id=f"doc-{index}",
        )
        for index in range(1000)
    ]

    decisions = dispatcher.dispatch_many(tasks)

    assert len(decisions) == 1000
    assert dispatcher.in_flight_count() == 1000

    seen_task_ids = set()
    seen_pairs = set()

    for decision in decisions:
        assert decision.task_id not in seen_task_ids
        seen_task_ids.add(decision.task_id)

        expected_partition = ownership.partition_for_url(
            decision.url
        )
        expected_node = ownership.owner_node_for_partition(
            expected_partition
        )

        assert decision.partition_id == expected_partition
        assert decision.node_id == expected_node

        pair = (decision.partition_id, decision.node_id)
        assert pair not in seen_pairs
        seen_pairs.add(pair)

        dispatcher.validate_decision(decision)

    assert len(seen_task_ids) == 1000
    assert dispatcher.validate_all()

    loads = dispatcher.node_load()

    assert sum(loads.values()) == 1000
    assert all(node_id in loads for node_id in registry.node_ids())

    print("STAGE 5.4.3 MULTI-NODE DISPATCH TEST: PASS")


if __name__ == "__main__":
    main()
