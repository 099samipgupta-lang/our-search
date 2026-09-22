import json
import os
import threading


class StorageCatalog:

    SCHEMA_VERSION = 1
    FILENAME = "storage.catalog"
    JOURNAL_FILENAME = "storage.catalog.log"

    def __init__(self, root):
        if not isinstance(root, str):
            raise TypeError("root must be a string")

        self.root = os.path.abspath(root)

        self.path = os.path.join(
            self.root,
            self.FILENAME,
        )

        self.journal_path = os.path.join(
            self.root,
            self.JOURNAL_FILENAME,
        )

        self._lock = threading.RLock()
        self._records = {}

        os.makedirs(self.root, exist_ok=True)

        self._load()
        self._replay_journal()

    def _load(self):
        if not os.path.exists(self.path):
            return

        with open(self.path, "rb") as file:
            data = file.read()

        if not data:
            return

        record = json.loads(data.decode("utf-8"))

        if record.get("schema_version") != self.SCHEMA_VERSION:
            raise ValueError(
                "unsupported catalog schema version"
            )

        records = record.get("records", {})

        if not isinstance(records, dict):
            raise ValueError(
                "catalog records must be a dictionary"
            )

        self._records = records

    def _apply(self, operation, key, record=None):
        if operation == "put":
            self._records[key] = dict(record or {})
            return

        if operation == "delete":
            self._records.pop(key, None)
            return

        raise ValueError(
            f"unsupported catalog operation: {operation}"
        )

    def _append_journal(self, operation, key, record=None):
        entry = {
            "operation": operation,
            "key": key,
        }

        if operation == "put":
            entry["record"] = dict(record or {})

        encoded = (
            json.dumps(
                entry,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            + b"\n"
        )

        with open(self.journal_path, "ab") as file:
            file.write(encoded)
            file.flush()
            os.fsync(file.fileno())

    def _replay_journal(self):
        if not os.path.exists(self.journal_path):
            return

        with open(self.journal_path, "rb") as file:
            data = file.read()

        if not data:
            return

        lines = data.split(b"\n")
        valid_end = 0

        for index, raw_line in enumerate(lines):
            # The final element after split() is empty when the file
            # ends with a newline.
            is_final = index == len(lines) - 1

            if is_final and raw_line == b"":
                valid_end = len(data)
                break

            # A non-empty final fragment without a newline is a
            # crash-partial journal record. Ignore and truncate it.
            if is_final:
                break

            line = raw_line.strip()

            if not line:
                valid_end += len(raw_line) + 1
                continue

            try:
                entry = json.loads(line.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValueError(
                    "corrupted catalog journal entry"
                ) from exc

            operation = entry.get("operation")
            key = entry.get("key")

            if (
                not isinstance(operation, str)
                or not isinstance(key, str)
                or not key
            ):
                raise ValueError(
                    "invalid catalog journal entry"
                )

            if operation == "put":
                record = entry.get("record", {})

                if not isinstance(record, dict):
                    raise ValueError(
                        "catalog journal record must be a dictionary"
                    )

                self._apply(
                    "put",
                    key,
                    record,
                )

            elif operation == "delete":
                self._apply(
                    "delete",
                    key,
                )

            else:
                raise ValueError(
                    "unsupported catalog journal operation "
                    f"{operation!r}"
                )

            valid_end += len(raw_line) + 1

        # Remove only an incomplete final record. All complete records
        # have already been applied above.
        if valid_end < len(data):
            with open(self.journal_path, "r+b") as file:
                file.truncate(valid_end)
                file.flush()
                os.fsync(file.fileno())

    def _save_snapshot(self):
        payload = {
            "schema_version": self.SCHEMA_VERSION,
            "records": self._records,
        }

        encoded = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")

        temporary_path = self.path + ".tmp"

        with open(temporary_path, "wb") as file:
            file.write(encoded)
            file.flush()
            os.fsync(file.fileno())

        os.replace(
            temporary_path,
            self.path,
        )

    def compact(self):
        with self._lock:
            self._save_snapshot()

            temporary_path = self.journal_path + ".tmp"

            with open(temporary_path, "wb") as file:
                file.flush()
                os.fsync(file.fileno())

            os.replace(
                temporary_path,
                self.journal_path,
            )

    def put(self, key, record=None):
        if not isinstance(key, str) or not key:
            raise ValueError(
                "key must be a non-empty string"
            )

        if record is None:
            record = {}

        if not isinstance(record, dict):
            raise TypeError(
                "record must be a dictionary"
            )

        with self._lock:
            self._append_journal(
                "put",
                key,
                record,
            )

            self._apply(
                "put",
                key,
                record,
            )

    def get(self, key):
        with self._lock:
            record = self._records.get(key)

            if record is None:
                return None

            return dict(record)

    def exists(self, key):
        with self._lock:
            return key in self._records

    def delete(self, key):
        with self._lock:
            if key not in self._records:
                return False

            self._append_journal(
                "delete",
                key,
            )

            self._apply(
                "delete",
                key,
            )

            return True

    def list_keys(self, prefix=""):
        prefix = prefix or ""

        with self._lock:
            return sorted(
                key
                for key in self._records
                if key.startswith(prefix)
            )

    def count(self):
        with self._lock:
            return len(self._records)
