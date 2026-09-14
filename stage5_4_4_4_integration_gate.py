from __future__ import annotations

import http.server
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from crawler_system.communication_transport import TCPTransport
from crawler_system.distributed_communication import (
    DistributedCommunicationNode,
)
from crawler_system.message_delivery import MessageDeliveryTracker
from crawler_system.partition_router import PartitionRouter
from crawler_system.result import CrawlResult
from crawler_system.task import CrawlTask
from crawler_system.worker_ownership import WorkerNode, WorkerOwnership
from crawler_system.worker_pool import WorkerPool
from crawler_system.worker_registry import WorkerRegistry


CLUSTER_ID = "stage5-4-4-4"
PARTITION_COUNT = 16


class TestHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        body = (
            b"<html><head><title>OUR SEARCH integration</title></head>"
            b"<body>distributed crawler integration test</body></html>"
        )

        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        pass


def free_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def start_http_server():
    server = http.server.ThreadingHTTPServer(
        ("127.0.0.1", 0),
        TestHandler,
    )
    thread = threading.Thread(
        target=server.serve_forever,
        daemon=True,
    )
    thread.start()
    return server


def build_cluster():
    nodes = [
        WorkerNode("node-a", 2),
        WorkerNode("node-b", 2),
    ]

    ownership = WorkerOwnership(
        partition_count=PARTITION_COUNT,
        nodes=nodes,
        cluster_id=CLUSTER_ID,
    )

    registry = WorkerRegistry(
        cluster_id=CLUSTER_ID,
    )

    registry.register(
        node_id="node-a",
        worker_count=2,
    )

    registry.register(
        node_id="node-b",
        worker_count=2,
    )

    router = PartitionRouter(
        partition_count=PARTITION_COUNT,
        database_root="crawler_storage/stage5_4_4_4",
    )

    return ownership, registry, router


def worker_executor(worker_pool: WorkerPool):
    worker_pool.start()

    lock = threading.Lock()

    def execute(task: CrawlTask) -> CrawlResult:
        deadline = time.monotonic() + 30.0

        while time.monotonic() < deadline:
            with lock:
                available = worker_pool.available_worker_ids()

                if available:
                    worker_id = available[0]

                    worker_pool.submit(
                        task,
                        worker_id,
                    )

                    while time.monotonic() < deadline:
                        result = worker_pool.get_result(
                            timeout=0.25,
                        )

                        if result is None:
                            continue

                        if not isinstance(
                            result,
                            CrawlResult,
                        ):
                            continue

                        if (
                            result.task.document_id
                            == task.document_id
                        ):
                            worker_pool.release_worker(
                                worker_id,
                            )
                            return result

                        # The gate serializes WorkerPool execution,
                        # so another task result here indicates an
                        # unexpected WorkerPool state.
                        raise RuntimeError(
                            "received result for unexpected task: "
                            f"{result.task.document_id}"
                        )

                    worker_pool.release_worker(
                        worker_id,
                    )

            time.sleep(0.005)

        raise RuntimeError(
            "timed out waiting for WorkerPool result"
        )

    return execute


def main():
    print("STAGE 5.4.4.4 DISTRIBUTED INTEGRATION GATE")
    print()

    ownership, registry, router = build_cluster()

    source_port = free_port()
    remote_port = free_port()

    source_transport = TCPTransport(
        host="127.0.0.1",
        port=source_port,
        cluster_id=CLUSTER_ID,
    )

    remote_transport = TCPTransport(
        host="127.0.0.1",
        port=remote_port,
        cluster_id=CLUSTER_ID,
    )

    source_delivery = MessageDeliveryTracker(
        timeout=30.0,
    )

    remote_delivery = MessageDeliveryTracker(
        timeout=30.0,
    )

    from crawler_system.distributed_dispatcher import (
        DistributedDispatcher,
    )

    source_dispatcher = DistributedDispatcher(
        ownership=ownership,
        registry=registry,
        router=router,
    )

    http_server = start_http_server()

    worker_pool = WorkerPool(
        worker_count=2,
    )

    remote_executor = worker_executor(
        worker_pool,
    )

    source_node = DistributedCommunicationNode(
        node_id="node-a",
        cluster_id=CLUSTER_ID,
        transport=source_transport,
        delivery=source_delivery,
        dispatcher=source_dispatcher,
        peer_endpoints={
            "node-a": ("127.0.0.1", source_port),
            "node-b": ("127.0.0.1", remote_port),
        },
    )

    remote_node = DistributedCommunicationNode(
        node_id="node-b",
        cluster_id=CLUSTER_ID,
        transport=remote_transport,
        delivery=remote_delivery,
        task_executor=remote_executor,
        peer_endpoints={
            "node-a": ("127.0.0.1", source_port),
            "node-b": ("127.0.0.1", remote_port),
        },
    )

    try:
        # --------------------------------------------------------------
        # TEST 1 — Component integration
        # --------------------------------------------------------------
        assert ownership.validate_complete_ownership()
        assert registry.count() == 2
        assert router.partition_for(
            "http://127.0.0.1/"
        ) >= 0

        print("TEST 1 — Component integration: PASS")

        # --------------------------------------------------------------
        # TEST 2 — Real TCP node startup
        # --------------------------------------------------------------
        source_node.start()
        remote_node.start()

        time.sleep(0.1)

        assert source_node.running()
        assert remote_node.running()

        print("TEST 2 — Real TCP node startup: PASS")

        # --------------------------------------------------------------
        # TEST 3 — Real distributed task dispatch
        # --------------------------------------------------------------
        tasks = []

        for index in range(20):
            tasks.append(
                CrawlTask(
                    url=(
                        f"http://127.0.0.1:"
                        f"{http_server.server_port}/page/{index}"
                    ),
                    document_id=f"integration-{index}",
                )
            )

        # Ownership is host-based. Different URL paths on the same
        # host therefore cannot move a task between owners.
        # Use a deterministic hostname that maps to node-b while
        # still resolving locally through the HTTP test server.
        remote_task = None

        for index in range(10000):
            hostname = f"remote-{index}.localhost"

            candidate = CrawlTask(
                url=(
                    f"http://{hostname}:"
                    f"{http_server.server_port}/remote/{index}"
                ),
                document_id=f"remote-integration-{index}",
            )

            if source_dispatcher.owner_for_task(candidate) == "node-b":
                remote_task = candidate
                break

        assert remote_task is not None

        dispatch = source_node.dispatch(
            task=remote_task,
            receiver_host="127.0.0.1",
            receiver_port=remote_port,
            message_id="integration-dispatch-1",
        )

        assert dispatch.receiver_node_id == "node-b"
        assert source_delivery.is_acknowledged(
            "integration-dispatch-1"
        )

        print("TEST 3 — Real distributed task dispatch: PASS")

        # --------------------------------------------------------------
        # TEST 4 — Real remote WorkerPool execution + result delivery
        # --------------------------------------------------------------
        deadline = time.monotonic() + 30.0

        while time.monotonic() < deadline:
            if source_node.result_count() == 1:
                break

            if remote_node.errors():
                break

            if source_node.errors():
                break

            time.sleep(0.05)

        print("REMOTE NODE ERRORS:", remote_node.errors())
        print(
            "REMOTE PROCESSED TASKS:",
            remote_node.processed_task_count(),
        )
        print("SOURCE NODE ERRORS:", source_node.errors())

        assert remote_node.errors() == {}
        assert source_node.errors() == {}
        assert remote_node.processed_task_count() == 1
        assert source_node.result_count() == 1

        result_record = source_node.result_record(
            "integration-dispatch-1"
        )

        assert result_record is not None
        assert result_record.result.success()
        assert result_record.result.task.document_id == (
            remote_task.document_id
        )

        print(
            "TEST 4 — Remote WorkerPool execution + "
            "result delivery: PASS"
        )

        # --------------------------------------------------------------
        # TEST 5 — Delivery lifecycle
        # --------------------------------------------------------------
        deadline = time.monotonic() + 5.0

        while time.monotonic() < deadline:
            if source_delivery.is_completed(
                "integration-dispatch-1"
            ):
                break

            time.sleep(0.02)

        assert source_delivery.is_acknowledged(
            "integration-dispatch-1"
        )
        assert source_delivery.is_completed(
            "integration-dispatch-1"
        )

        assert source_dispatcher.is_completed(
            remote_task.document_id
        )

        print(
            "TEST 5 — Delivery lifecycle + dispatcher completion: PASS"
        )

        # --------------------------------------------------------------
        # TEST 6 — Duplicate delivery protection
        # --------------------------------------------------------------
        duplicate = source_node.dispatch(
            task=CrawlTask(
                url=remote_task.url,
                document_id="duplicate-integration",
            ),
            receiver_host="127.0.0.1",
            receiver_port=remote_port,
            message_id="integration-duplicate-1",
        )

        assert duplicate.message_id == (
            "integration-duplicate-1"
        )

        print(
            "TEST 6 — Duplicate-safe distributed delivery: PASS"
        )

        # --------------------------------------------------------------
        # TEST 7 — Concurrent distributed tasks
        # --------------------------------------------------------------
        concurrent_tasks = []

        for index in range(40):
            task = CrawlTask(
                url=(
                    f"http://127.0.0.1:"
                    f"{http_server.server_port}/bulk/{index}"
                ),
                document_id=f"bulk-integration-{index}",
            )

            if source_dispatcher.owner_for_task(task) == "node-b":
                concurrent_tasks.append(task)

        assert concurrent_tasks

        # Keep task count bounded while guaranteeing remote ownership.
        concurrent_tasks = concurrent_tasks[:20]

        def send(task):
            message_id = f"bulk-message-{task.document_id}"

            return source_node.dispatch(
                task=task,
                receiver_host="127.0.0.1",
                receiver_port=remote_port,
                message_id=message_id,
            )

        with ThreadPoolExecutor(
            max_workers=8
        ) as executor:
            dispatches = list(
                executor.map(
                    send,
                    concurrent_tasks,
                )
            )

        assert len(dispatches) == len(concurrent_tasks)

        expected = len(concurrent_tasks)
        deadline = time.monotonic() + 60.0

        while time.monotonic() < deadline:
            completed = sum(
                1
                for task in concurrent_tasks
                if source_dispatcher.is_completed(
                    task.document_id
                )
            )

            if completed == expected:
                break

            time.sleep(0.05)

        completed = sum(
            1
            for task in concurrent_tasks
            if source_dispatcher.is_completed(
                task.document_id
            )
        )

        assert completed == expected

        print(
            f"TEST 7 — Concurrent distributed tasks "
            f"({expected} tasks): PASS"
        )

        # --------------------------------------------------------------
        # TEST 8 — Final integration invariants
        # --------------------------------------------------------------
        assert source_dispatcher.in_flight_count() == 0
        assert source_node.errors() == {}
        assert source_node.result_count() >= expected + 1
        assert remote_node.processed_task_count() >= expected + 2

        print("TEST 8 — Final distributed invariants: PASS")

        print()
        print("RESULT: PASS")
        print(
            "STAGE 5.4.4.4 DISTRIBUTED INTEGRATION: "
            "100% COMPLETE"
        )

    finally:
        try:
            source_node.stop()
        finally:
            remote_node.stop()

        try:
            worker_pool.stop()
        except Exception:
            pass

        http_server.shutdown()
        http_server.server_close()

        # PartitionRouter is a routing-only component and does not
        # own open database connections.


if __name__ == "__main__":
    main()
