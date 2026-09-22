import hashlib
import json
import os
import threading
import uuid

from index_storage.backend import IndexStorageBackend


class TransactionLog:
    MAGIC = b"OURSEARCHTX1"
    HEADER_SIZE = 8

    def __init__(self, path):
        if not isinstance(path, str):
            raise TypeError("path must be a string")

        self.path = os.path.abspath(path)

        directory = os.path.dirname(self.path)
        if directory:
            os.makedirs(directory, exist_ok=True)

    @staticmethod
    def _checksum(payload):
        return hashlib.sha256(payload).hexdigest()

    def append(self, record):
        if not isinstance(record, dict):
            raise TypeError("record must be a dictionary")

        payload = json.dumps(
            record,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")

        envelope = {
            "checksum": self._checksum(payload),
            "record": record,
        }

        encoded = json.dumps(
            envelope,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")

        with open(self.path, "ab") as file:
            file.write(self.MAGIC)
            file.write(len(encoded).to_bytes(8, "big"))
            file.write(encoded)
            file.flush()
            os.fsync(file.fileno())

    def recover(self):
        if not os.path.exists(self.path):
            return []

        records = []

        with open(self.path, "rb") as file:
            while True:
                record_start = file.tell()

                magic = file.read(len(self.MAGIC))

                if not magic:
                    break

                if len(magic) != len(self.MAGIC):
                    self._truncate(record_start)
                    break

                if magic != self.MAGIC:
                    raise ValueError(
                        "invalid transaction WAL magic"
                    )

                header = file.read(self.HEADER_SIZE)

                if len(header) != self.HEADER_SIZE:
                    self._truncate(record_start)
                    break

                length = int.from_bytes(header, "big")

                payload = file.read(length)

                if len(payload) != length:
                    self._truncate(record_start)
                    break

                envelope = json.loads(
                    payload.decode("utf-8")
                )

                record = envelope["record"]
                checksum = envelope["checksum"]

                canonical = json.dumps(
                    record,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")

                if self._checksum(canonical) != checksum:
                    raise ValueError(
                        "transaction WAL checksum mismatch"
                    )

                records.append(record)

        return records

    def _truncate(self, size):
        with open(self.path, "r+b") as file:
            file.truncate(size)
            file.flush()
            os.fsync(file.fileno())

    def clear(self):
        directory = os.path.dirname(self.path)
        temporary_path = self.path + ".tmp"

        with open(temporary_path, "wb") as file:
            file.flush()
            os.fsync(file.fileno())

        os.replace(
            temporary_path,
            self.path,
        )

        if directory:
            directory_fd = os.open(
                directory,
                os.O_DIRECTORY,
            )

            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)


class Transaction:
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
            raise RuntimeError(
                "transaction is no longer active"
            )

    def put(self, key, data):
        self._check_active()

        if not isinstance(key, str) or not key:
            raise ValueError(
                "key must be a non-empty string"
            )

        if not isinstance(data, bytes):
            raise TypeError(
                "data must be bytes"
            )

        self._operations.append(
            ("put", key, data)
        )

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
            raise ValueError(
                "key must be a non-empty string"
            )

        self._operations.append(
            ("delete", key, b"")
        )

        self._overlay[key] = None

    def commit(self):
        self._check_active()

        self.manager._commit(self)

        self.state = self.COMMITTED

    def rollback(self):
        self._check_active()

        self.manager._rollback(self)

        self.state = self.ABORTED


class TransactionalIndexStorage(IndexStorageBackend):

    def __init__(self, storage, wal_path=None):
        if not isinstance(
            storage,
            IndexStorageBackend,
        ):
            raise TypeError(
                "storage must implement IndexStorageBackend"
            )

        self.storage = storage

        if wal_path is None:
            root = getattr(
                storage,
                "root",
                None,
            )

            if root is None:
                raise ValueError(
                    "wal_path is required for this storage backend"
                )

            wal_path = os.path.join(
                root,
                "transactions.wal",
            )

        self.wal = TransactionLog(wal_path)

        self._lock = threading.RLock()

        self._recover_transactions()

    def begin_transaction(self):
        return Transaction(self)

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

    def _commit(self, transaction):
        with self._lock:
            txid = transaction.transaction_id

            self.wal.append({
                "type": "begin",
                "transaction": txid,
            })

            for operation, key, data in transaction._operations:
                record = {
                    "type": operation,
                    "transaction": txid,
                    "key": key,
                }

                if operation == "put":
                    record["data"] = data.hex()

                self.wal.append(record)

            # The durable COMMIT record is the atomicity boundary.
            self.wal.append({
                "type": "commit",
                "transaction": txid,
            })

            self._apply_operations(
                transaction._operations
            )

            self.wal.clear()

    def _rollback(self, transaction):
        with self._lock:
            self.wal.append({
                "type": "abort",
                "transaction": transaction.transaction_id,
            })

            self.wal.clear()

    def _apply_operations(self, operations):
        for operation, key, data in operations:
            if operation == "put":
                self.storage.put(key, data)

            elif operation == "delete":
                self.storage.delete(key)

            else:
                raise ValueError(
                    f"unsupported transaction operation: {operation}"
                )

    def _recover_transactions(self):
        records = self.wal.recover()

        if not records:
            return

        transactions = {}
        committed = set()
        aborted = set()

        for record in records:
            txid = record.get("transaction")
            operation = record.get("type")

            if not txid:
                raise ValueError(
                    "transaction WAL record missing transaction id"
                )

            if operation not in {
                "begin",
                "put",
                "delete",
                "commit",
                "abort",
            }:
                raise ValueError(
                    "invalid transaction WAL operation"
                )

            transactions.setdefault(
                txid,
                [],
            )

            if operation == "put":
                data = bytes.fromhex(
                    record["data"]
                )

                transactions[txid].append(
                    (
                        "put",
                        record["key"],
                        data,
                    )
                )

            elif operation == "delete":
                transactions[txid].append(
                    (
                        "delete",
                        record["key"],
                        b"",
                    )
                )

            elif operation == "commit":
                committed.add(txid)

            elif operation == "abort":
                aborted.add(txid)

        if committed & aborted:
            raise ValueError(
                "transaction cannot be both committed and aborted"
            )

        for txid in committed:
            self._apply_operations(
                transactions.get(txid, [])
            )

        self.wal.clear()
