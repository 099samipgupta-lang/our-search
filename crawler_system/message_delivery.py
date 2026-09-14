import threading
import time
from dataclasses import dataclass
from typing import Optional


class MessageDeliveryError(Exception):
    """Raised for message-delivery state violations."""


@dataclass(frozen=True)
class DeliveryRecord:
    message_id: str
    sender_node_id: str
    receiver_node_id: str
    created_at: float
    attempts: int = 0
    acknowledged: bool = False
    completed_at: Optional[float] = None


class MessageDeliveryTracker:
    """
    Thread-safe delivery state machine.

    A message progresses:

        registered
            ↓
        in-flight
            ↓
        acknowledged
            ↓
        completed

    Duplicate registration is rejected.
    Duplicate ACKs are harmless.
    Duplicate completion is harmless.
    Completed messages cannot be retried.
    """

    def __init__(self, timeout: float = 10.0):
        if timeout <= 0:
            raise ValueError("timeout must be positive")

        self.timeout = float(timeout)
        self._records: dict[str, DeliveryRecord] = {}
        self._lock = threading.RLock()

    @staticmethod
    def _validate_message_id(message_id: str) -> str:
        message_id = str(message_id).strip()

        if not message_id:
            raise ValueError("message_id must be non-empty")

        return message_id

    @staticmethod
    def _validate_node_id(node_id: str, field: str) -> str:
        node_id = str(node_id).strip()

        if not node_id:
            raise ValueError(f"{field} must be non-empty")

        return node_id

    def register(
        self,
        message_id: str,
        sender_node_id: str,
        receiver_node_id: str,
    ) -> DeliveryRecord:
        message_id = self._validate_message_id(message_id)
        sender_node_id = self._validate_node_id(
            sender_node_id,
            "sender_node_id",
        )
        receiver_node_id = self._validate_node_id(
            receiver_node_id,
            "receiver_node_id",
        )

        with self._lock:
            if message_id in self._records:
                raise MessageDeliveryError(
                    f"message already registered: {message_id}"
                )

            record = DeliveryRecord(
                message_id=message_id,
                sender_node_id=sender_node_id,
                receiver_node_id=receiver_node_id,
                created_at=time.monotonic(),
                attempts=1,
            )

            self._records[message_id] = record
            return record

    def acknowledge(self, message_id: str) -> DeliveryRecord:
        message_id = self._validate_message_id(message_id)

        with self._lock:
            record = self._records.get(message_id)

            if record is None:
                raise MessageDeliveryError(
                    f"unknown message: {message_id}"
                )

            if record.acknowledged:
                return record

            updated = DeliveryRecord(
                message_id=record.message_id,
                sender_node_id=record.sender_node_id,
                receiver_node_id=record.receiver_node_id,
                created_at=record.created_at,
                attempts=record.attempts,
                acknowledged=True,
                completed_at=record.completed_at,
            )

            self._records[message_id] = updated
            return updated

    def complete(self, message_id: str) -> DeliveryRecord:
        message_id = self._validate_message_id(message_id)

        with self._lock:
            record = self._records.get(message_id)

            if record is None:
                raise MessageDeliveryError(
                    f"unknown message: {message_id}"
                )

            if not record.acknowledged:
                raise MessageDeliveryError(
                    f"message is not acknowledged: {message_id}"
                )

            if record.completed_at is not None:
                return record

            updated = DeliveryRecord(
                message_id=record.message_id,
                sender_node_id=record.sender_node_id,
                receiver_node_id=record.receiver_node_id,
                created_at=record.created_at,
                attempts=record.attempts,
                acknowledged=True,
                completed_at=time.monotonic(),
            )

            self._records[message_id] = updated
            return updated

    def retry(self, message_id: str) -> DeliveryRecord:
        message_id = self._validate_message_id(message_id)

        with self._lock:
            record = self._records.get(message_id)

            if record is None:
                raise MessageDeliveryError(
                    f"unknown message: {message_id}"
                )

            if record.completed_at is not None:
                raise MessageDeliveryError(
                    f"completed message cannot be retried: {message_id}"
                )

            updated = DeliveryRecord(
                message_id=record.message_id,
                sender_node_id=record.sender_node_id,
                receiver_node_id=record.receiver_node_id,
                created_at=record.created_at,
                attempts=record.attempts + 1,
                acknowledged=record.acknowledged,
                completed_at=record.completed_at,
            )

            self._records[message_id] = updated
            return updated

    def get(self, message_id: str) -> Optional[DeliveryRecord]:
        message_id = self._validate_message_id(message_id)

        with self._lock:
            return self._records.get(message_id)

    def is_acknowledged(self, message_id: str) -> bool:
        record = self.get(message_id)
        return record is not None and record.acknowledged

    def is_completed(self, message_id: str) -> bool:
        record = self.get(message_id)
        return (
            record is not None
            and record.completed_at is not None
        )

    def is_expired(self, message_id: str) -> bool:
        record = self.get(message_id)

        if record is None:
            raise MessageDeliveryError(
                f"unknown message: {message_id}"
            )

        if record.acknowledged:
            return False

        return (
            time.monotonic() - record.created_at
            >= self.timeout
        )

    def pending(self) -> list[DeliveryRecord]:
        with self._lock:
            return [
                record
                for record in self._records.values()
                if not record.acknowledged
            ]

    def acknowledged(self) -> list[DeliveryRecord]:
        with self._lock:
            return [
                record
                for record in self._records.values()
                if record.acknowledged
                and record.completed_at is None
            ]

    def completed(self) -> list[DeliveryRecord]:
        with self._lock:
            return [
                record
                for record in self._records.values()
                if record.completed_at is not None
            ]

    def count(self) -> int:
        with self._lock:
            return len(self._records)

    def clear_completed(self) -> int:
        with self._lock:
            completed_ids = [
                message_id
                for message_id, record in self._records.items()
                if record.completed_at is not None
            ]

            for message_id in completed_ids:
                del self._records[message_id]

            return len(completed_ids)
