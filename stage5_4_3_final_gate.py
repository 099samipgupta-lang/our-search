import py_compile
from pathlib import Path

from crawler_system.distributed_dispatcher import DistributedDispatcher
from crawler_system.task import CrawlTask
from crawler_system.worker_ownership import WorkerNode, WorkerOwnership
from crawler_system.worker_registry import WorkerRegistry


FILES = [
    "crawler_system/distributed_dispatcher.py",
    "stage5_4_3_dispatch_test.py",
    "stage5_4_3_multi_node_dispatch_test.py",
    "stage5_4_3_concurrency_test.py",
    "stage5_4_3_final_gate.py",
]


def compilation_gate():
    for file in FILES:
        py_compile.compile(file, doraise=True)

    print("TEST 1 — Python compilation: PASS")


def architecture_gate():
    nodes = [
        WorkerNode("node-a", 4),
        WorkerNode("node-b", 8),
        WorkerNode("node-c", 16),
    ]

    ownership = WorkerOwnership(
        partition_count=128,
        nodes=nodes,
        cluster_id="final-dispatch-test",
    )

    registry = WorkerRegistry(cluster_id="final-dispatch-test")

    for index, node in enumerate(nodes):
        registry.register(
            node.node_id,
            node.worker_count,
            now=float(index),
        )

    dispatcher = DistributedDispatcher(ownership, registry)

    for index in range(128):
        task = CrawlTask(
            url=f"https://host-{index}.example/page",
            document_id=f"architecture-{index}",
        )

        decision = dispatcher.decide(task)

        assert decision.partition_id == dispatcher.partition_for_task(
            task
        )
        assert decision.node_id == ownership.owner_for_partition(
            decision.partition_id
        )
        assert dispatcher.validate_decision(decision)

    print("TEST 2 — Deterministic ownership dispatch: PASS")


def lifecycle_gate():
    ownership = WorkerOwnership(
        partition_count=32,
        nodes=[
            WorkerNode("node-a", 4),
            WorkerNode("node-b", 4),
        ],
        cluster_id="lifecycle-test",
    )

    registry = WorkerRegistry(cluster_id="lifecycle-test")
    registry.register("node-a", 4, now=1.0)
    registry.register("node-b", 4, now=2.0)

    dispatcher = DistributedDispatcher(ownership, registry)

    task = CrawlTask(
        url="https://lifecycle.example/",
        document_id="lifecycle-doc",
    )

    decision = dispatcher.dispatch(task)

    assert dispatcher.is_in_flight("lifecycle-doc")

    released = dispatcher.release("lifecycle-doc")

    assert released == decision
    assert not dispatcher.is_in_flight("lifecycle-doc")

    decision = dispatcher.dispatch(task)
    completed = dispatcher.complete("lifecycle-doc")

    assert completed == decision
    assert dispatcher.is_completed("lifecycle-doc")

    print("TEST 3 — Dispatch lifecycle and duplicate protection: PASS")


def inactive_node_gate():
    ownership = WorkerOwnership(
        partition_count=1,
        nodes=[
            WorkerNode("node-a", 4),
        ],
        cluster_id="inactive-test",
    )

    registry = WorkerRegistry(cluster_id="inactive-test")
    registry.register("node-a", 4, now=1.0)
    registry.unregister("node-a")

    dispatcher = DistributedDispatcher(ownership, registry)

    task = CrawlTask(
        url="https://inactive.example/",
        document_id="inactive-doc",
    )

    try:
        dispatcher.dispatch(task)
    except RuntimeError:
        pass
    else:
        raise AssertionError(
            "inactive owning node accepted a task"
        )

    print("TEST 4 — Inactive-node protection: PASS")


def concurrency_gate():
    from concurrent.futures import ThreadPoolExecutor

    ownership = WorkerOwnership(
        partition_count=512,
        nodes=[
            WorkerNode("node-a", 8),
            WorkerNode("node-b", 8),
            WorkerNode("node-c", 8),
            WorkerNode("node-d", 8),
        ],
        cluster_id="final-concurrency-test",
    )

    registry = WorkerRegistry(
        cluster_id="final-concurrency-test"
    )

    for index, node in enumerate(
        (
            WorkerNode("node-a", 8),
            WorkerNode("node-b", 8),
            WorkerNode("node-c", 8),
            WorkerNode("node-d", 8),
        )
    ):
        registry.register(
            node.node_id,
            node.worker_count,
            now=float(index),
        )

    dispatcher = DistributedDispatcher(ownership, registry)

    tasks = [
        CrawlTask(
            url=f"https://concurrent-{index}.example/{index}",
            document_id=f"concurrent-{index}",
        )
        for index in range(2000)
    ]

    with ThreadPoolExecutor(max_workers=32) as executor:
        decisions = list(
            executor.map(dispatcher.dispatch, tasks)
        )

    assert len(decisions) == 2000
    assert dispatcher.in_flight_count() == 2000
    assert dispatcher.validate_all()

    print(
        "TEST 5 — Concurrent distributed dispatch "
        "(2000 tasks / 32 threads): PASS"
    )


def main():
    compilation_gate()
    architecture_gate()
    lifecycle_gate()
    inactive_node_gate()
    concurrency_gate()

    print()
    print("RESULT: PASS")
    print("STAGE 5.4.3 DISTRIBUTED TASK DISPATCH: 100% COMPLETE")


if __name__ == "__main__":
    main()
