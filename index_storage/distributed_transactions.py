import os
import threading
import uuid

from index_storage.backend import IndexStorageBackend
from index_storage.transactions import TransactionLog, TransactionalIndexStorage


class DistributedTransaction:
    ACTIVE = "active"
    COMMITTED = "committed"
    ABORTED = "aborted"

    def __init__(self, manager):
        self.manager = manager
        self.transaction_id = uuid.uuid4().hex
        self.state = self.ACTIVE
        self._operations = []
        self._overlay = {}

    def _check_active(self):
        if self.state != self.ACTIVE:
            raise RuntimeError("transaction is no longer active")

    def put(self, key, data):
        self._check_active()

        if not isinstance(key, str) or not key:
            raise ValueError("key must be a non-empty string")

        if not isinstance(data, bytes):
            raise TypeError("data must be bytes")

        self._operations.append(("put", key, data))
        self._overlay[key] = data

    def get(self, key):
        self._check_active()

        if key in self._overlay:
            return self._overlay[key]

        return self.manager.storage.get(key)

    def exists(self, key):
        return self.get(key) is not None

    def delete(self, key):
        self._check_active()

        if not isinstance(key, str) or not key:
            raise ValueError("key must be a non-empty string")

        self._operations.append(("delete", key, b""))
        self._overlay[key] = None

    def commit(self):
        self._check_active()
        self.manager._commit(self)
        self.state = self.COMMITTED

    def rollback(self):
        self._check_active()
        self.manager._rollback(self)
        self.state = self.ABORTED


class DistributedTransactionalIndexStorage(IndexStorageBackend):
    """
    Distributed transaction coordinator.

    The supplied storage is normally a ShardedIndexStorage whose shard
    backends are independently transactional. The coordinator establishes
    a durable commit point before applying participant transactions.

    Recovery rule:
      - no durable commit record -> transaction is discarded
      - durable commit record -> all recorded operations are replayed
    """

    def __init__(self, storage, wal_path=None):
        if not isinstance(storage, IndexStorageBackend):
            raise TypeError(
                "storage must implement IndexStorageBackend"
            )

        self.storage = storage
        self._lock = threading.RLock()

        if wal_path is None:
            root = getattr(storage, "root", None)

            if root is None:
                raise ValueError(
                    "wal_path is required for this storage backend"
                )

            wal_path = os.path.join(
                root,
                "distributed_transactions.wal",
            )

        self.wal = TransactionLog(wal_path)

        self._participants = self._build_participants(storage)

        self._recover_distributed_transactions()

    @staticmethod
    def _build_participants(storage):
        """
        Build transactional participants around each physical shard.

        For a sharded backend, each shard becomes one participant.
        For a non-sharded backend, the backend itself becomes one participant.
        """
        backends = getattr(storage, "backends", None)

        if isinstance(backends, list) and backends:
            return [
                backend
                if isinstance(backend, TransactionalIndexStorage)
                else TransactionalIndexStorage(backend)
                for backend in backends
            ]

        return [
            storage
            if isinstance(storage, TransactionalIndexStorage)
            else TransactionalIndexStorage(storage)
        ]

    def begin_transaction(self):
        return DistributedTransaction(self)

    def _participant_index(self, key):
        shard_index = getattr(self.storage, "shard_for_key", None)

        if callable(shard_index):
            return shard_index(key)

        return 0

    def _commit(self, transaction):
        with self._lock:
            grouped = {}

            for operation, key, data in transaction._operations:
                participant_index = self._participant_index(key)

                if not 0 <= participant_index < len(self._participants):
                    raise RuntimeError(
                        f"invalid participant index: {participant_index}"
                    )

                grouped.setdefault(participant_index, []).append(
                    (operation, key, data)
                )

            participant_transactions = []

            try:
                for participant_index in sorted(grouped):
                    participant = self._participants[participant_index]
                    participant_tx = participant.begin_transaction()

                    for operation, key, data in grouped[participant_index]:
                        if operation == "put":
                            participant_tx.put(key, data)
                        elif operation == "delete":
                            participant_tx.delete(key)
                        else:
                            raise ValueError(
                                f"unsupported operation: {operation}"
                            )

                    participant_transactions.append(participant_tx)

                # Durable distributed commit point.
                self.wal.append(
                    {
                        "type": "commit",
                        "transaction_id": transaction.transaction_id,
                        "operations": [
                            {
                                "operation": operation,
                                "key": key,
                                "data": data.hex(),
                            }
                            for operation, key, data
                            in transaction._operations
                        ],
                    }
                )

                for participant_tx in participant_transactions:
                    participant_tx.commit()

                self.wal.clear()

            except Exception:
                # Once the distributed commit record exists, recovery will
                # replay the complete transaction. Do not erase that record.
                if self._wal_contains_commit(transaction.transaction_id):
                    raise

                for participant_tx in participant_transactions:
                    try:
                        participant_tx.rollback()
                    except Exception:
                        pass

                raise

    def _rollback(self, transaction):
        with self._lock:
            self.wal.append(
                {
                    "type": "abort",
                    "transaction_id": transaction.transaction_id,
                }
            )
            self.wal.clear()

    def _wal_contains_commit(self, transaction_id):
        for record in self.wal.recover():
            if (
                record.get("type") == "commit"
                and record.get("transaction_id") == transaction_id
            ):
                return True

        return False

    def _apply_recovered_transaction(self, record):
        operations = record.get("operations", [])

        grouped = {}

        for item in operations:
            operation = item["operation"]
            key = item["key"]
            data = bytes.fromhex(item.get("data", ""))

            participant_index = self._participant_index(key)

            grouped.setdefault(participant_index, []).append(
                (operation, key, data)
            )

        for participant_index in sorted(grouped):
            participant = self._participants[participant_index]
            participant_tx = participant.begin_transaction()

            try:
                for operation, key, data in grouped[participant_index]:
                    if operation == "put":
                        participant_tx.put(key, data)
                    elif operation == "delete":
                        participant_tx.delete(key)
                    else:
                        raise ValueError(
                            f"unsupported recovered operation: {operation}"
                        )

                participant_tx.commit()

            except Exception:
                try:
                    participant_tx.rollback()
                except Exception:
                    pass
                raise

    def _recover_distributed_transactions(self):
        with self._lock:
            records = self.wal.recover()

            commits = {}

            for record in records:
                record_type = record.get("type")
                transaction_id = record.get("transaction_id")

                if not transaction_id:
                    raise ValueError(
                        "distributed transaction record missing transaction_id"
                    )

                if record_type == "commit":
                    commits[transaction_id] = record

                elif record_type == "abort":
                    commits.pop(transaction_id, None)

                else:
                    raise ValueError(
                        f"unknown distributed transaction record: {record_type}"
                    )

            for record in commits.values():
                self._apply_recovered_transaction(record)

            self.wal.clear()

    def put(self, key, data):
        transaction = self.begin_transaction()
        transaction.put(key, data)
        transaction.commit()

    def get(self, key):
        return self.storage.get(key)

    def exists(self, key):
        return self.storage.exists(key)

    def delete(self, key):
        transaction = self.begin_transaction()
        transaction.delete(key)
        transaction.commit()

    def list_keys(self, prefix=""):
        return self.storage.list_keys(prefix)
