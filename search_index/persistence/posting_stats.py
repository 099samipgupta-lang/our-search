import json
import os
import tempfile


class PersistentPostingStats:

    FORMAT_VERSION = 1

    def __init__(self, path):

        self.path = os.path.abspath(path)

        self.stats = {}

    def record(
        self,
        segment_id,
        term,
        document_frequency
    ):

        segment = self.stats.setdefault(
            segment_id,
            {}
        )

        segment[term] = int(
            document_frequency
        )

    def get(
        self,
        segment_id,
        term
    ):

        return self.stats.get(
            segment_id,
            {}
        ).get(
            term
        )

    def segment_stats(
        self,
        segment_id
    ):

        return dict(
            self.stats.get(
                segment_id,
                {}
            )
        )

    def save(self):

        directory = os.path.dirname(
            self.path
        )

        os.makedirs(
            directory,
            exist_ok=True
        )

        data = {
            "format_version":
                self.FORMAT_VERSION,

            "stats":
                self.stats
        }

        fd, temporary_path = tempfile.mkstemp(
            prefix=".posting_stats_",
            suffix=".tmp",
            dir=directory
        )

        try:

            with os.fdopen(
                fd,
                "w",
                encoding="utf-8"
            ) as file:

                json.dump(
                    data,
                    file,
                    ensure_ascii=False,
                    separators=(",", ":")
                )

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

        data = {
            "format_version":
                self.FORMAT_VERSION,

            "stats":
                self.stats
        }

        return json.dumps(
            data,
            ensure_ascii=False,
            separators=(",", ":")
        ).encode("utf-8")

    def load_bytes(
        self,
        payload
    ):

        if not isinstance(
            payload,
            bytes
        ):
            raise TypeError(
                "payload must be bytes"
            )

        data = json.loads(
            payload.decode("utf-8")
        )

        if data.get(
            "format_version"
        ) != self.FORMAT_VERSION:

            raise ValueError(
                "Unsupported posting stats format"
            )

        self.stats = dict(
            data.get(
                "stats",
                {}
            )
        )

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
            return False

        self.load_bytes(
            payload
        )

        return True

    def load(self):

        if not os.path.exists(
            self.path
        ):
            return
            return

        with open(
            self.path,
            "r",
            encoding="utf-8"
        ) as file:

            data = json.load(
                file
            )

        if data.get(
            "format_version"
        ) != self.FORMAT_VERSION:

            raise ValueError(
                "Unsupported posting stats format"
            )

        self.stats = dict(
            data.get(
                "stats",
                {}
            )
        )

    def remove_segment(
        self,
        segment_id
    ):

        self.stats.pop(
            segment_id,
            None
        )

    def rename_segment(
        self,
        old_segment_id,
        new_segment_id
    ):

        values = self.stats.pop(
            old_segment_id,
            {}
        )

        self.stats[
            new_segment_id
        ] = values

    def segment_count(self):

        return len(
            self.stats
        )
