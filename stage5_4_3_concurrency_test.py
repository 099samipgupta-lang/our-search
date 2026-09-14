from concurrent.futures import ThreadPoolExecutor

from crawler_system.distributed_dispatcher import DistributedDispatcher
from crawler_system.task import CrawlTask
from crawler_system.worker_ownership import WorkerNode, WorkerOwnership
from crawler_system.worker_registry import WorkerRegistry


def main():
    nodes = [
        WorkerNode("node-a", 8),
        WorkerNode("node-b", 8),
        WorkerNode("node-c", 8),
        WorkerNode("node-d", 8),
    ]

    ownership = WorkerOwnership(
        partition_count=512,
        nodes=nodes,
        cluster_id="concurrency-dispatch-test",
    )

    registry = WorkerRegistry(cluster_id="concurrency-dispatch-test")

    for index, node in enumerate(nodes):
        registry.register(
            node.node_id,
            node.worker_count,
            now=float(index),
        )

    dispatcher = DistributedDispatcher(ownership, registry)

    tasks = [
        CrawlTask(
            url=f"https://site-{index}.example/page/{index}",
            document_id=f"doc-{index}",
        )
        for index in range(2000)
    ]

    def dispatch_one(task):
        return dispatcher.dispatch(task)

    with ThreadPoolExecutor(max_workers=32) as executor:
        decisions = list(executor.map(dispatch_one, tasks))

    assert len(decisions) == 2000
    assert dispatcher.in_flight_count() == 2000
    assert dispatcher.validate_all()

    # Complete every task concurrently.
    def complete_one(task):
        return dispatcher.complete(task.document_id)

    with ThreadPoolExecutor(max_workers=32) as executor:
        completed = list(executor.map(complete_one, tasks))

    assert len(completed) == 2000
    assert dispatcher.in_flight_count() == 0
    assert dispatcher.completed_count() == 2000
    assert dispatcher.validate_all()

    print("STAGE 5.4.3 CONCURRENCY TEST: PASS")


if __name__ == "__main__":
    main()
