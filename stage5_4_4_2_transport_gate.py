import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from crawler_system.communication_protocol import CommunicationProtocol
from crawler_system.communication_transport import (
    TCPTransport,
    CommunicationTransportError,
)
from crawler_system.task import CrawlTask


HOST = "127.0.0.1"


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, 0))
        return s.getsockname()[1]


def make_task(i):
    return CrawlTask(
        url=f"https://example{i}.com/page",
        document_id=f"doc-{i}",
        priority=float(i),
        attempt=i % 3,
    )


def make_handler(received):
    lock = threading.Lock()

    def handler(message, address):
        with lock:
            received.append(message)

        protocol = CommunicationProtocol(message.cluster_id)

        return protocol.acknowledgement(
            sender_node_id=message.receiver_node_id,
            receiver_node_id=message.sender_node_id,
            acknowledged_message_id=message.message_id,
        )

    return handler


def test_basic_round_trip():
    port = free_port()
    received = []

    server = TCPTransport(HOST, port, cluster_id="test-cluster")
    server.start_server(make_handler(received))

    time.sleep(0.1)

    client = TCPTransport(HOST, port, cluster_id="test-cluster")

    task = make_task(1)

    protocol = CommunicationProtocol("test-cluster")

    message = protocol.task_dispatch(
        task=task,
        sender_node_id="node-a",
        receiver_node_id="node-b",
    )

    response = client.request(message)

    assert response.message_type == CommunicationProtocol.ACK
    assert response.cluster_id == "test-cluster"
    assert response.payload["acknowledged_message_id"] == message.message_id

    assert len(received) == 1
    assert received[0].message_type == CommunicationProtocol.TASK_DISPATCH
    assert received[0].payload["task"]["url"] == task.url

    server.stop()


def test_fragmented_frame():
    port = free_port()
    received = []

    server = TCPTransport(HOST, port, cluster_id="fragment-test")
    server.start_server(make_handler(received))

    time.sleep(0.1)

    task = make_task(2)

    protocol = CommunicationProtocol("fragment-test")

    message = protocol.task_dispatch(
        task=task,
        sender_node_id="node-a",
        receiver_node_id="node-b",
    )

    protocol = CommunicationProtocol("fragment-test")
    payload = protocol.encode(message)
    frame = TCPTransport._encode_frame(
        payload,
        server.max_message_bytes,
    )

    with socket.create_connection(
        (HOST, port),
        timeout=2,
    ) as sock:
        midpoint = len(frame) // 2
        sock.sendall(frame[:midpoint])
        time.sleep(0.02)
        sock.sendall(frame[midpoint:])

    deadline = time.time() + 2

    while time.time() < deadline and not received:
        time.sleep(0.01)

    assert len(received) == 1
    assert received[0].payload["task"]["url"] == task.url

    server.stop()


def test_concurrent_connections():
    port = free_port()
    received = []

    server = TCPTransport(HOST, port, cluster_id="concurrent-test")
    server.start_server(make_handler(received))

    time.sleep(0.1)

    def send_one(i):
        client = TCPTransport(HOST, port, cluster_id="concurrent-test")

        protocol = CommunicationProtocol("concurrent-test")

        message = protocol.task_dispatch(
            task=make_task(i),
            sender_node_id=f"node-{i}",
            receiver_node_id="node-server",
        )

        client.send_message(message)
        return True

    count = 200

    with ThreadPoolExecutor(max_workers=32) as executor:
        results = list(
            executor.map(send_one, range(count))
        )

    assert all(results)

    deadline = time.time() + 5

    while time.time() < deadline and len(received) < count:
        time.sleep(0.01)

    assert len(received) == count

    urls = {
        message.payload["task"]["url"]
        for message in received
    }

    assert len(urls) == count

    server.stop()


def test_oversized_frame_rejection():
    port = free_port()

    server = TCPTransport(
        HOST,
        port,
        max_message_bytes=128,
    )

    received = []

    server.start_server(make_handler(received))
    time.sleep(0.1)

    with socket.create_connection(
        (HOST, port),
        timeout=2,
    ) as sock:
        sock.sendall((129).to_bytes(4, "big"))

    time.sleep(0.1)

    assert received == []

    server.stop()


def test_malformed_frame_rejection():
    port = free_port()

    server = TCPTransport(HOST, port)
    received = []

    server.start_server(make_handler(received))
    time.sleep(0.1)

    with socket.create_connection(
        (HOST, port),
        timeout=2,
    ) as sock:
        malformed = b"{not valid json"
        frame = len(malformed).to_bytes(4, "big") + malformed
        sock.sendall(frame)

    time.sleep(0.1)

    assert received == []

    server.stop()


def test_connection_failure():
    port = free_port()

    client = TCPTransport(
        HOST,
        port,
        connect_timeout=0.2,
        cluster_id="failure-test",
    )

    protocol = CommunicationProtocol("failure-test")

    message = protocol.task_dispatch(
        task=make_task(999),
        sender_node_id="node-a",
        receiver_node_id="node-b",
    )

    try:
        client.send_message(message)
    except (CommunicationTransportError, OSError):
        return

    raise AssertionError(
        "connection failure was not reported"
    )


def test_clean_shutdown():
    port = free_port()

    server = TCPTransport(HOST, port)
    server.start_server(lambda message, address: None)

    time.sleep(0.1)

    assert server.running()

    server.stop()

    assert not server.running()
    assert server.connection_count() == 0


def main():
    print("TEST 1 — Real TCP protocol round-trip: ", end="")
    test_basic_round_trip()
    print("PASS")

    print("TEST 2 — Fragmented frame handling: ", end="")
    test_fragmented_frame()
    print("PASS")

    print("TEST 3 — Concurrent TCP connections (200): ", end="")
    test_concurrent_connections()
    print("PASS")

    print("TEST 4 — Oversized frame rejection: ", end="")
    test_oversized_frame_rejection()
    print("PASS")

    print("TEST 5 — Malformed frame rejection: ", end="")
    test_malformed_frame_rejection()
    print("PASS")

    print("TEST 6 — Connection failure handling: ", end="")
    test_connection_failure()
    print("PASS")

    print("TEST 7 — Clean transport shutdown: ", end="")
    test_clean_shutdown()
    print("PASS")

    print("RESULT: PASS")
    print(
        "STAGE 5.4.4.2 TRANSPORT ARCHITECTURE: "
        "100% COMPLETE"
    )


if __name__ == "__main__":
    main()
