from crawler_system.distributed_dispatcher import DistributedDispatcher
from crawler_system.task import CrawlTask
from crawler_system.worker_ownership import WorkerNode, WorkerOwnership
from crawler_system.worker_registry import WorkerRegistry


def main():
    nodes = [
        WorkerNode("node-a", 4),
        WorkerNode("node-b", 8),
        WorkerNode("node-c", 16),
    ]

    ownership = WorkerOwnership(
        partition_count=32,
        nodes=nodes,
        cluster_id="dispatch-test",
    )

    registry = WorkerRegistry(cluster_id="dispatch-test")

    for node in nodes:
        registry.register(node.node_id, node.worker_count, now=1.0)

    dispatcher = DistributedDispatcher(ownership, registry)

    task = CrawlTask(
        url="https://example.com/page",
        document_id="doc-1",
        priority=50.0,
    )

    decision = dispatcher.decide(task)

    assert decision.task_id == "doc-1"
    assert decision.url == task.url
    assert decision.partition_id == ownership.partition_for_url(task.url)
    assert decision.node_id == ownership.owner_node_for_partition(
        decision.partition_id
    )
    assert decision.worker_count == registry.get(
        decision.node_id
    ).worker_count

    dispatcher.validate_decision(decision)

    dispatched = dispatcher.dispatch(task)

    assert dispatched == decision
    assert dispatcher.is_in_flight("doc-1")
    assert dispatcher.in_flight_count() == 1

    try:
        dispatcher.dispatch(task)
    except RuntimeError:
        pass
    else:
        raise AssertionError("duplicate in-flight task accepted")

    completed = dispatcher.complete("doc-1")

    assert completed == decision
    assert dispatcher.is_completed("doc-1")
    assert dispatcher.completed_count() == 1
    assert dispatcher.in_flight_count() == 0

    try:
        dispatcher.dispatch(task)
    except RuntimeError:
        pass
    else:
        raise AssertionError("completed task was dispatched again")

    print("STAGE 5.4.3 DISPATCH TEST: PASS")


if __name__ == "__main__":
    main()
