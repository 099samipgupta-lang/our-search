import hashlib


class StorageIntegrity:

    ALGORITHM = "sha256"

    @classmethod
    def checksum(cls, data):
        if not isinstance(data, bytes):
            raise TypeError("data must be bytes")

        return hashlib.sha256(data).hexdigest()

    @classmethod
    def verify(cls, data, expected_checksum):
        if not isinstance(expected_checksum, str):
            raise TypeError(
                "expected_checksum must be a string"
            )

        actual_checksum = cls.checksum(data)

        return actual_checksum == expected_checksum
