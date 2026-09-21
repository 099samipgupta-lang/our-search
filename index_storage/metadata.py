import json
import time


class StorageMetadata:

    SCHEMA_VERSION = 1

    def __init__(
        self,
        version,
        size,
        checksum,
        created_at,
        updated_at,
    ):
        self.version = int(version)
        self.size = int(size)
        self.checksum = str(checksum)
        self.created_at = float(created_at)
        self.updated_at = float(updated_at)

    @classmethod
    def create(cls, version, data, checksum):
        now = time.time()

        return cls(
            version=version,
            size=len(data),
            checksum=checksum,
            created_at=now,
            updated_at=now,
        )

    def to_dict(self):
        return {
            "schema_version": self.SCHEMA_VERSION,
            "version": self.version,
            "size": self.size,
            "checksum": self.checksum,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    def to_bytes(self):
        return json.dumps(
            self.to_dict(),
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")

    @classmethod
    def from_bytes(cls, data):
        if not isinstance(data, bytes):
            raise TypeError("data must be bytes")

        record = json.loads(data.decode("utf-8"))

        if record.get("schema_version") != cls.SCHEMA_VERSION:
            raise ValueError("unsupported metadata schema version")

        return cls(
            version=record["version"],
            size=record["size"],
            checksum=record["checksum"],
            created_at=record["created_at"],
            updated_at=record["updated_at"],
        )
