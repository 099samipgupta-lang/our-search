from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from crawler_system.communication_protocol import CommunicationProtocol
from crawler_system.distributed_dispatcher import DistributedDispatcher
from crawler_system.message_delivery import MessageDeliveryTracker
from crawler_system.partition_router import PartitionRouter
from crawler_system.task import CrawlTask
from crawler_system.worker_ownership import WorkerNode, WorkerOwnership
from crawler_system.worker_registry import WorkerRegistry


ROOT = Path(__file__).resolve().parent
CLUSTER_ID = "stage5-4-5-final"
PARTITION_COUNT = 32


def run_gate(script: str) -> None:
    path = ROOT / script

    if not path.exists():
        raise AssertionError(f"required gate is missing: {script}")

    result = subprocess.run(
        [sys.executable, str(path)],
        cwd=ROOT,
        text=True,
    )

    if result.returncode != 0:
        raise AssertionError(
            f"sub-gate failed: {script} "
            f"(exit={result.returncode})"
        )


def test_cross_component_consistency() -> None:
    nodes = [
        WorkerNode("node-a", 4),
        WorkerNode("node-b", 4),
        WorkerNode("node-c", 4),
    ]

    ownership = WorkerOwnership(
        partition_count=PARTITION_COUNT,
        nodes=nodes,
        cluster_id=CLUSTER_ID,
    )

    registry = WorkerRegistry(cluster_id=CLUSTER_ID)

    for node in nodes:
        registry.register(
            node_id=node.node_id,
            worker_count=node.worker_count,
        )

    router = PartitionRouter(
        partition_count=PARTITION_COUNT,
        database_root="crawler_storage/stage5_4_5_final",
    )

    dispatcher = DistributedDispatcher(
        ownership=ownership,
        registry=registry,
        router=router,
    )

    protocol = CommunicationProtocol(CLUSTER_ID)
    delivery = MessageDeliveryTracker(timeout=30.0)

    seen_tasks = set()
    seen_partitions = {}

    for index in range(500):
        task = CrawlTask(
            url=f"https://final-{index}.example.com/page",
            document_id=f"final-task-{index}",
        )

        partition_id = router.partition_for(task.url)
        owner = ownership.owner_for_partition(partition_id)

        decision = dispatcher.dispatch(task)

        assert decision.task_id == task.document_id
        assert decision.url == task.url
        assert decision.partition_id == partition_id
        assert decision.node_id == owner

        registration = registry.get(owner)

        assert registration is not None
        assert registration.active
        assert registration.worker_count > 0

        message = protocol.task_dispatch(
            task=task,
            sender_node_id="node-a",
            receiver_node_id=owner,
            message_id=f"final-message-{index}",
        )

        encoded = protocol.encode(message)
        decoded = protocol.decode(encoded)

        extracted = protocol.extract_task(decoded)

        assert extracted.document_id == task.document_id
        assert extracted.url == task.url

        delivery.register(
            message_id=message.message_id,
            sender_node_id="node-a",
            receiver_node_id=owner,
        )

        assert delivery.is_acknowledged(message.message_id) is False
        assert delivery.is_completed(message.message_id) is False

        delivery.acknowledge(message.message_id)
        assert delivery.is_acknowledged(message.message_id)

        delivery.complete(message.message_id)
        assert delivery.is_completed(message.message_id)

        dispatcher.complete(task.document_id)

        assert dispatcher.is_completed(task.document_id)
        assert dispatcher.is_in_flight(task.document_id) is False

        seen_tasks.add(task.document_id)
        seen_partitions.setdefault(partition_id, set()).add(owner)

    assert len(seen_tasks) == 500

    for partition_id, owners in seen_partitions.items():
        assert len(owners) == 1, (
            f"partition {partition_id} has multiple owners: {owners}"
        )

    assert ownership.validate_complete_ownership()
    assert registry.count() == 3
    assert registry.total_worker_capacity() == 12
    assert dispatcher.in_flight_count() == 0
    assert dispatcher.completed_count() == 500
    assert delivery.count() == 500
    assert len(delivery.completed()) == 500
    assert delivery.clear_completed() == 500
    assert delivery.count() == 0


def main() -> None:
    print("STAGE 5.4.5 DISTRIBUTED WORKERS FINAL GATE")
    print()

    # ------------------------------------------------------------
    # TEST 1 — Production compilation
    # ------------------------------------------------------------
    modules = [
        "crawler_system/worker_ownership.py",
        "crawler_system/worker_registry.py",
        "crawler_system/distributed_dispatcher.py",
        "crawler_system/communication_protocol.py",
        "crawler_system/communication_transport.py",
        "crawler_system/message_delivery.py",
        "crawler_system/distributed_communication.py",
        "crawler_system/worker_pool.py",
    ]

    compile_result = subprocess.run(
        [sys.executable, "-m", "py_compile", *modules],
        cwd=ROOT,
        text=True,
    )

    assert compile_result.returncode == 0

    print("TEST 1 — Distributed worker production compilation: PASS")

    # ------------------------------------------------------------
    # TEST 2 — Architecture and ownership
    # ------------------------------------------------------------
    run_gate("stage5_4_1_final_gate.py")

    print("TEST 2 — Worker ownership architecture: PASS")

    # ------------------------------------------------------------
    # TEST 3 — Registration and coordination
    # ------------------------------------------------------------
    run_gate("stage5_4_2_final_gate.py")

    print("TEST 3 — Worker registration and coordination: PASS")

    # ------------------------------------------------------------
    # TEST 4 — Distributed task dispatch
    # ------------------------------------------------------------
    run_gate("stage5_4_3_final_gate.py")

    print("TEST 4 — Distributed task dispatch: PASS")

    # ------------------------------------------------------------
    # TEST 5 — Communication protocol
    # ------------------------------------------------------------
    run_gate("stage5_4_4_1_protocol_gate.py")

    print("TEST 5 — Communication protocol: PASS")

    # ------------------------------------------------------------
    # TEST 6 — TCP transport
    # ------------------------------------------------------------
    run_gate("stage5_4_4_2_transport_gate.py")

    print("TEST 6 — TCP transport: PASS")

    # ------------------------------------------------------------
    # TEST 7 — Delivery semantics
    # ------------------------------------------------------------
    run_gate("stage5_4_4_3_delivery_gate.py")

    print("TEST 7 — Message delivery semantics: PASS")

    # ------------------------------------------------------------
    # TEST 8 — Full distributed integration
    # ------------------------------------------------------------
    run_gate("stage5_4_4_4_integration_gate.py")

    print("TEST 8 — Full distributed integration: PASS")

    # ------------------------------------------------------------
    # TEST 9 — Cross-component consistency
    # ------------------------------------------------------------
    test_cross_component_consistency()

    print(
        "TEST 9 — Cross-component ownership/dispatch/"
        "protocol/delivery consistency: PASS"
    )

    # ------------------------------------------------------------
    # TEST 10 — Final invariants
    # ------------------------------------------------------------
    assert PARTITION_COUNT == 32
    assert len({node.node_id for node in [
        WorkerNode("node-a", 4),
        WorkerNode("node-b", 4),
        WorkerNode("node-c", 4),
    ]}) == 3

    print("TEST 10 — Final distributed-worker invariants: PASS")

    print()
    print("RESULT: PASS")
    print("STAGE 5.4 DISTRIBUTED WORKERS: 100% COMPLETE")


if __name__ == "__main__":
    main()
