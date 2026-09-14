from concurrent.futures import ThreadPoolExecutor

from crawler_system.communication_protocol import (
    CommunicationMessage,
    CommunicationProtocol,
    CommunicationProtocolError,
)
from crawler_system.result import CrawlResult
from crawler_system.task import CrawlTask


def make_task(index=1):
    return CrawlTask(
        url=f"https://example-{index}.com/page",
        document_id=f"task-{index}",
        priority=42.5,
        attempt=2,
        etag='"etag-value"',
        last_modified="Wed, 01 Jan 2025 00:00:00 GMT",
    )


def make_result(index=1):
    task = make_task(index)

    return CrawlResult(
        task=task,
        response={
            "url": task.url,
            "requested_url": task.url,
            "status": 200,
            "status_type": "success",
            "content_type": "text/html",
            "headers": {
                "content-type": "text/html",
            },
            "body": b"ignored-by-wire-test",
            "redirect_chain": [],
            "final_url": task.url,
            "retries": task.attempt,
            "storage_path": None,
            "worker_id": 3,
        },
    )


def task_roundtrip_gate():
    protocol = CommunicationProtocol("protocol-test")

    task = make_task()

    message = protocol.task_dispatch(
        task,
        sender_node_id="node-a",
        receiver_node_id="node-b",
        message_id="task-message-1",
    )

    wire = protocol.encode(message)
    restored = protocol.decode(wire)
    restored_task = protocol.extract_task(restored)

    assert restored.message_type == protocol.TASK_DISPATCH
    assert restored.message_id == "task-message-1"
    assert restored.sender_node_id == "node-a"
    assert restored.receiver_node_id == "node-b"
    assert restored_task.to_dict() == task.to_dict()

    print("TEST 1 — Task message round-trip: PASS")


def result_roundtrip_gate():
    protocol = CommunicationProtocol("protocol-test")

    result = make_result()

    message = protocol.task_result(
        result,
        sender_node_id="node-b",
        receiver_node_id="node-a",
        message_id="result-message-1",
    )

    wire = protocol.encode(message)
    restored = protocol.decode(wire)
    restored_result = protocol.extract_result(restored)

    assert restored.message_type == protocol.TASK_RESULT
    assert restored.message_id == "result-message-1"
    assert restored.sender_node_id == "node-b"
    assert restored.receiver_node_id == "node-a"

    expected = result.to_dict()
    actual = restored_result.to_dict()

    # JSON cannot preserve bytes directly, so compare all wire-safe fields.
    expected["response"]["body"] = ""
    actual["response"]["body"] = ""

    assert actual == expected

    print("TEST 2 — Result message round-trip: PASS")


def control_message_gate():
    protocol = CommunicationProtocol("protocol-test")

    ack = protocol.acknowledgement(
        sender_node_id="node-b",
        receiver_node_id="node-a",
        acknowledged_message_id="task-message-1",
    )

    decoded_ack = protocol.decode(
        protocol.encode(ack)
    )

    assert decoded_ack.message_type == protocol.ACK
    assert (
        decoded_ack.payload["acknowledged_message_id"]
        == "task-message-1"
    )

    error = protocol.error(
        sender_node_id="node-b",
        receiver_node_id="node-a",
        error="worker unavailable",
        failed_message_id="task-message-2",
    )

    decoded_error = protocol.decode(
        protocol.encode(error)
    )

    assert decoded_error.message_type == protocol.ERROR
    assert decoded_error.payload["error"] == "worker unavailable"
    assert (
        decoded_error.payload["failed_message_id"]
        == "task-message-2"
    )

    print("TEST 3 — ACK and error messages: PASS")


def deterministic_encoding_gate():
    protocol = CommunicationProtocol("protocol-test")

    message = protocol.task_dispatch(
        make_task(),
        sender_node_id="node-a",
        receiver_node_id="node-b",
        message_id="deterministic-1",
    )

    wire_a = protocol.encode(message)
    wire_b = protocol.encode(message)

    assert wire_a == wire_b

    restored = CommunicationMessage.from_bytes(wire_a)

    assert restored.to_bytes() == wire_a

    print("TEST 4 — Deterministic wire encoding: PASS")


def malformed_message_gate():
    protocol = CommunicationProtocol("protocol-test")

    malformed_messages = [
        b"",
        b"not-json",
        b"[]",
        b'{"cluster_id":"protocol-test"}',
    ]

    for wire in malformed_messages:
        try:
            protocol.decode(wire)
        except CommunicationProtocolError:
            pass
        else:
            raise AssertionError(
                "malformed message was accepted"
            )

    unknown = CommunicationMessage(
        protocol_version=1,
        cluster_id="protocol-test",
        message_id="unknown-1",
        message_type="unknown-message",
        sender_node_id="node-a",
        receiver_node_id="node-b",
        payload={},
    )

    try:
        protocol.decode(unknown.to_bytes())
    except CommunicationProtocolError:
        pass
    else:
        raise AssertionError(
            "unknown message type was accepted"
        )

    print("TEST 5 — Malformed and unknown-message rejection: PASS")


def cluster_isolation_gate():
    protocol_a = CommunicationProtocol("cluster-a")
    protocol_b = CommunicationProtocol("cluster-b")

    message = protocol_a.task_dispatch(
        make_task(),
        sender_node_id="node-a",
        receiver_node_id="node-b",
    )

    wire = protocol_a.encode(message)

    try:
        protocol_b.decode(wire)
    except CommunicationProtocolError:
        pass
    else:
        raise AssertionError(
            "cross-cluster message was accepted"
        )

    print("TEST 6 — Cluster isolation: PASS")


def concurrent_gate():
    protocol = CommunicationProtocol("concurrent-test")

    def roundtrip(index):
        task = make_task(index)

        message = protocol.task_dispatch(
            task,
            sender_node_id="node-a",
            receiver_node_id="node-b",
            message_id=f"concurrent-{index}",
        )

        restored = protocol.decode(
            protocol.encode(message)
        )

        restored_task = protocol.extract_task(
            restored
        )

        assert restored_task.to_dict() == task.to_dict()
        return restored.message_id

    with ThreadPoolExecutor(
        max_workers=32
    ) as executor:
        results = list(
            executor.map(
                roundtrip,
                range(2000),
            )
        )

    assert len(results) == 2000
    assert len(set(results)) == 2000

    print(
        "TEST 7 — Concurrent protocol communication "
        "(2000 messages / 32 threads): PASS"
    )


def main():
    task_roundtrip_gate()
    result_roundtrip_gate()
    control_message_gate()
    deterministic_encoding_gate()
    malformed_message_gate()
    cluster_isolation_gate()
    concurrent_gate()

    print()
    print("RESULT: PASS")
    print(
        "STAGE 5.4.4.1 COMMUNICATION PROTOCOL: "
        "100% COMPLETE"
    )


if __name__ == "__main__":
    main()
