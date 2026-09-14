import concurrent.futures
import threading
import time

from crawler_system.message_delivery import (
    MessageDeliveryError,
    MessageDeliveryTracker,
)


def test_registration_and_inflight():
    tracker = MessageDeliveryTracker(timeout=10.0)

    record = tracker.register("msg-1", "node-a", "node-b")

    assert record.message_id == "msg-1"
    assert record.sender_node_id == "node-a"
    assert record.receiver_node_id == "node-b"
    assert record.attempts == 1
    assert not record.acknowledged
    assert not record.completed_at
    assert tracker.get("msg-1") is not None

    print("TEST 1 — Registration and in-flight state: PASS")


def test_ack_transition():
    tracker = MessageDeliveryTracker(timeout=10.0)
    tracker.register("msg-1", "node-a", "node-b")

    record = tracker.acknowledge("msg-1")

    assert record.acknowledged
    assert tracker.is_acknowledged("msg-1")
    assert not tracker.is_completed("msg-1")

    print("TEST 2 — ACK state transition: PASS")


def test_completion_requires_ack():
    tracker = MessageDeliveryTracker(timeout=10.0)
    tracker.register("msg-1", "node-a", "node-b")

    try:
        tracker.complete("msg-1")
    except MessageDeliveryError:
        pass
    else:
        raise AssertionError("Completion must require ACK")

    assert not tracker.is_completed("msg-1")

    print("TEST 3 — Completion requires ACK: PASS")


def test_duplicate_registration_rejected():
    tracker = MessageDeliveryTracker(timeout=10.0)
    tracker.register("msg-1", "node-a", "node-b")

    try:
        tracker.register("msg-1", "node-a", "node-b")
    except MessageDeliveryError:
        pass
    else:
        raise AssertionError("Duplicate registration was accepted")

    print("TEST 4 — Duplicate registration rejection: PASS")


def test_duplicate_ack_idempotent():
    tracker = MessageDeliveryTracker(timeout=10.0)
    tracker.register("msg-1", "node-a", "node-b")

    first = tracker.acknowledge("msg-1")
    second = tracker.acknowledge("msg-1")

    assert first.acknowledged
    assert second.acknowledged
    assert tracker.is_acknowledged("msg-1")

    print("TEST 5 — Duplicate ACK idempotency: PASS")


def test_duplicate_completion_idempotent():
    tracker = MessageDeliveryTracker(timeout=10.0)
    tracker.register("msg-1", "node-a", "node-b")
    tracker.acknowledge("msg-1")

    first = tracker.complete("msg-1")
    second = tracker.complete("msg-1")

    assert first.completed_at is not None
    assert second.completed_at == first.completed_at

    print("TEST 6 — Duplicate completion idempotency: PASS")


def test_retry_attempts():
    tracker = MessageDeliveryTracker(timeout=10.0)
    tracker.register("msg-1", "node-a", "node-b")

    record = tracker.retry("msg-1")
    assert record.attempts == 2

    record = tracker.retry("msg-1")
    assert record.attempts == 3

    print("TEST 7 — Retry attempt accounting: PASS")


def test_completed_message_cannot_retry():
    tracker = MessageDeliveryTracker(timeout=10.0)
    tracker.register("msg-1", "node-a", "node-b")
    tracker.acknowledge("msg-1")
    tracker.complete("msg-1")

    try:
        tracker.retry("msg-1")
    except MessageDeliveryError:
        pass
    else:
        raise AssertionError("Completed message was retried")

    assert tracker.get("msg-1").attempts == 1

    print("TEST 8 — Completed-message retry protection: PASS")


def test_expiration():
    tracker = MessageDeliveryTracker(timeout=0.05)
    tracker.register("msg-1", "node-a", "node-b")

    assert not tracker.is_expired("msg-1")

    time.sleep(0.08)

    assert tracker.is_expired("msg-1")

    print("TEST 9 — Expiration behavior: PASS")


def test_unknown_message_rejection():
    tracker = MessageDeliveryTracker(timeout=10.0)

    operations = (
        lambda: tracker.acknowledge("unknown"),
        lambda: tracker.complete("unknown"),
        lambda: tracker.retry("unknown"),
    )

    for operation in operations:
        try:
            operation()
        except MessageDeliveryError:
            pass
        else:
            raise AssertionError("Unknown message was accepted")

    print("TEST 10 — Unknown-message rejection: PASS")


def test_identity_integrity():
    tracker = MessageDeliveryTracker(timeout=10.0)

    tracker.register("msg-1", "node-a", "node-b")

    record = tracker.get("msg-1")

    assert record.sender_node_id == "node-a"
    assert record.receiver_node_id == "node-b"

    print("TEST 11 — Message identity integrity: PASS")


def test_concurrent_delivery_state():
    tracker = MessageDeliveryTracker(timeout=30.0)

    message_count = 2000
    lock = threading.Lock()
    failures = []

    def process(index):
        message_id = f"msg-{index}"
        try:
            tracker.register(
                message_id,
                f"node-{index % 8}",
                f"node-{(index + 1) % 8}",
            )

            tracker.retry(message_id)

            tracker.acknowledge(message_id)
            tracker.complete(message_id)

            assert tracker.is_acknowledged(message_id)
            assert tracker.is_completed(message_id)

        except Exception as exc:
            with lock:
                failures.append((index, repr(exc)))

    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
        list(executor.map(process, range(message_count)))

    assert not failures, failures[:10]
    assert tracker.count() == message_count
    assert len(tracker.pending()) == 0
    assert len(tracker.acknowledged()) == 0
    assert len(tracker.completed()) == message_count
    assert tracker.count() == message_count

    print(
        f"TEST 12 — Concurrent delivery semantics "
        f"({message_count} messages / 32 threads): PASS"
    )


def test_completed_cleanup():
    tracker = MessageDeliveryTracker(timeout=10.0)

    for index in range(100):
        message_id = f"msg-{index}"
        tracker.register(message_id, "node-a", "node-b")
        tracker.acknowledge(message_id)
        tracker.complete(message_id)

    assert tracker.count() == 100

    removed = tracker.clear_completed()

    assert removed == 100
    assert tracker.count() == 0
    assert not tracker.completed()

    print("TEST 13 — Completed-message cleanup: PASS")


def main():
    print("STAGE 5.4.4.3 DELIVERY SEMANTICS GATE")
    print("=" * 60)

    test_registration_and_inflight()
    test_ack_transition()
    test_completion_requires_ack()
    test_duplicate_registration_rejected()
    test_duplicate_ack_idempotent()
    test_duplicate_completion_idempotent()
    test_retry_attempts()
    test_completed_message_cannot_retry()
    test_expiration()
    test_unknown_message_rejection()
    test_identity_integrity()
    test_concurrent_delivery_state()
    test_completed_cleanup()

    print("=" * 60)
    print("RESULT: PASS")
    print("STAGE 5.4.4.3 DELIVERY SEMANTICS: 100% COMPLETE")


if __name__ == "__main__":
    main()
