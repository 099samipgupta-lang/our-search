import json


class ChunkManifest:

    SCHEMA_VERSION = 1

    def __init__(
        self,
        key,
        size,
        chunk_size,
        chunk_count,
        checksum,
    ):
        if not isinstance(key, str) or not key:
            raise ValueError(
                "key must be a non-empty string"
            )

        if int(size) < 0:
            raise ValueError(
                "size must be non-negative"
            )

        if int(chunk_size) <= 0:
            raise ValueError(
                "chunk_size must be greater than zero"
            )

        if int(chunk_count) < 0:
            raise ValueError(
                "chunk_count must be non-negative"
            )

        if not isinstance(checksum, str) or not checksum:
            raise ValueError(
                "checksum must be a non-empty string"
            )

        self.key = key
        self.size = int(size)
        self.chunk_size = int(chunk_size)
        self.chunk_count = int(chunk_count)
        self.checksum = checksum

    def to_dict(self):
        return {
            "schema_version": self.SCHEMA_VERSION,
            "key": self.key,
            "size": self.size,
            "chunk_size": self.chunk_size,
            "chunk_count": self.chunk_count,
            "checksum": self.checksum,
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
            raise TypeError(
                "data must be bytes"
            )

        record = json.loads(
            data.decode("utf-8")
        )

        if (
            record.get("schema_version")
            != cls.SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported chunk manifest schema version"
            )

        return cls(
            key=record["key"],
            size=record["size"],
            chunk_size=record["chunk_size"],
            chunk_count=record["chunk_count"],
            checksum=record["checksum"],
        )
