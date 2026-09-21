import hashlib
import json
import os
import struct


class WriteAheadLog:

    MAGIC = b"OURSEARCHWAL1"
    HEADER = struct.Struct(">I")

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

    def append(self, operation, key, data=b""):
        if operation not in {"put", "delete"}:
            raise ValueError("unsupported WAL operation")

        if not isinstance(key, str) or not key:
            raise ValueError("key must be a non-empty string")

        if not isinstance(data, bytes):
            raise TypeError("data must be bytes")

        record = {
            "operation": operation,
            "key": key,
            "data": data.hex(),
        }

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
            file.write(self.HEADER.pack(len(encoded)))
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
                    file.close()

                    with open(self.path, "r+b") as repair_file:
                        repair_file.truncate(record_start)

                    break

                if magic != self.MAGIC:
                    raise ValueError("invalid WAL magic")

                header = file.read(self.HEADER.size)

                if len(header) != self.HEADER.size:
                    file.close()

                    with open(self.path, "r+b") as repair_file:
                        repair_file.truncate(record_start)

                    break

                (length,) = self.HEADER.unpack(header)

                payload = file.read(length)

                if len(payload) != length:
                    file.close()

                    with open(self.path, "r+b") as repair_file:
                        repair_file.truncate(record_start)

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

                if not hashlib.sha256(
                    canonical
                ).hexdigest() == checksum:
                    raise ValueError("WAL checksum mismatch")

                record["data"] = bytes.fromhex(
                    record["data"]
                )

                records.append(record)

        return records

    def clear(self):
        directory = os.path.dirname(self.path)

        temporary_path = self.path + ".tmp"

        with open(temporary_path, "wb") as file:
            file.flush()
            os.fsync(file.fileno())

        os.replace(
            temporary_path,
            self.path
        )

        if directory:
            directory_fd = os.open(
                directory,
                os.O_DIRECTORY
            )

            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
