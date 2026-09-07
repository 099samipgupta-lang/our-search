import json
import os
import tempfile
import hashlib


class IndexSegment:

    FORMAT_VERSION = 1

    def __init__(self, path):

        self.path = os.path.abspath(path)

        self.data = {
            "format_version": self.FORMAT_VERSION,
            "segment_id": None,
            "terms": {},
            "documents": {}
        }

    def build(
        self,
        segment_id,
        index_state
    ):

        if not segment_id:
            raise ValueError(
                "segment_id is required"
            )

        if not isinstance(index_state, dict):
            raise TypeError(
                "index_state must be a dictionary"
            )

        self.data = {
            "format_version":
                self.FORMAT_VERSION,

            "segment_id":
                str(segment_id),

            "terms":
                dict(
                    index_state.get(
                        "index",
                        {}
                    )
                ),

            "documents":
                dict(
                    index_state.get(
                        "documents",
                        {}
                    )
                )
        }

    def save(self):

        directory = os.path.dirname(
            self.path
        )

        os.makedirs(
            directory,
            exist_ok=True
        )

        encoded = json.dumps(
            self.data,
            ensure_ascii=False,
            separators=(",", ":")
        ).encode("utf-8")

        checksum = hashlib.sha256(
            encoded
        ).hexdigest()

        envelope = {
            "checksum": checksum,
            "data": self.data
        }

        fd, temporary_path = tempfile.mkstemp(
            prefix=".segment_",
            suffix=".tmp",
            dir=directory
        )

        try:

            with os.fdopen(
                fd,
                "wb"
            ) as file:

                payload = json.dumps(
                    envelope,
                    ensure_ascii=False,
                    separators=(",", ":")
                ).encode("utf-8")

                file.write(payload)

                file.flush()

                os.fsync(
                    file.fileno()
                )

            os.replace(
                temporary_path,
                self.path
            )

        except Exception:

            try:
                os.unlink(
                    temporary_path
                )
            except OSError:
                pass

            raise

    def to_bytes(self):

        encoded = json.dumps(
            self.data,
            ensure_ascii=False,
            separators=(",", ":")
        ).encode("utf-8")

        checksum = hashlib.sha256(
            encoded
        ).hexdigest()

        envelope = {
            "checksum": checksum,
            "data": self.data
        }

        return json.dumps(
            envelope,
            ensure_ascii=False,
            separators=(",", ":")
        ).encode("utf-8")

    def load_bytes(self, payload):

        if not isinstance(payload, bytes):
            raise TypeError(
                "payload must be bytes"
            )

        envelope = json.loads(
            payload.decode("utf-8")
        )

        data = envelope.get(
            "data"
        )

        stored_checksum = envelope.get(
            "checksum"
        )

        if data is None:
            raise ValueError(
                "Invalid segment: missing data"
            )

        encoded = json.dumps(
            data,
            ensure_ascii=False,
            separators=(",", ":")
        ).encode("utf-8")

        actual_checksum = hashlib.sha256(
            encoded
        ).hexdigest()

        if actual_checksum != stored_checksum:
            raise ValueError(
                "Segment checksum mismatch"
            )

        version = data.get(
            "format_version"
        )

        if version != self.FORMAT_VERSION:
            raise ValueError(
                f"Unsupported segment format: {version}"
            )

        self.data = data

        return self.data

    def save_to_storage(
        self,
        storage,
        key
    ):

        storage.put(
            key,
            self.to_bytes()
        )

    def load_from_storage(
        self,
        storage,
        key
    ):

        payload = storage.get(
            key
        )

        if payload is None:
            raise FileNotFoundError(
                key
            )

        return self.load_bytes(
            payload
        )

    def load(self):

        if not os.path.exists(
            self.path
        ):
            raise FileNotFoundError(
                self.path
            )

        with open(
            self.path,
            "rb"
        ) as file:

            payload = file.read()

        envelope = json.loads(
            payload.decode("utf-8")
        )

        data = envelope.get(
            "data"
        )

        stored_checksum = envelope.get(
            "checksum"
        )

        if data is None:
            raise ValueError(
                "Invalid segment: missing data"
            )

        encoded = json.dumps(
            data,
            ensure_ascii=False,
            separators=(",", ":")
        ).encode("utf-8")

        actual_checksum = hashlib.sha256(
            encoded
        ).hexdigest()

        if actual_checksum != stored_checksum:
            raise ValueError(
                "Segment checksum mismatch"
            )

        version = data.get(
            "format_version"
        )

        if version != self.FORMAT_VERSION:
            raise ValueError(
                f"Unsupported segment format: {version}"
            )

        self.data = data

        return self.data

    def get_postings(
        self,
        term
    ):

        return dict(
            self.data.get(
                "terms",
                {}
            ).get(
                term,
                {}
            )
        )

    def get_document_info(
        self,
        document_id
    ):

        return self.data.get(
            "documents",
            {}
        ).get(
            document_id
        )

    def vocabulary_size(self):

        return len(
            self.data.get(
                "terms",
                {}
            )
        )

    def document_count(self):

        return len(
            self.data.get(
                "documents",
                {}
            )
        )

    def exists(self):

        return os.path.exists(
            self.path
        )

    def delete(self):

        if not self.exists():
            return False

        os.remove(
            self.path
        )

        return True
