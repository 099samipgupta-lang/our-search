import json
import os
import tempfile


class Stage8SignalStore:
    FORMAT_VERSION = 1

    def __init__(self, path):
        self.path = os.path.abspath(path)
        os.makedirs(
            os.path.dirname(self.path),
            exist_ok=True,
        )
        self.records = {}
        self.load()

    def put(self, document_id, signals):
        self.records[str(document_id)] = dict(signals)

    def get(self, document_id):
        record = self.records.get(str(document_id))
        if record is None:
            return None
        return dict(record)

    def delete(self, document_id):
        self.records.pop(str(document_id), None)

    def count(self):
        return len(self.records)

    def save(self):
        directory = os.path.dirname(self.path)

        fd, temporary_path = tempfile.mkstemp(
            prefix=".stage8_signals_",
            suffix=".tmp",
            dir=directory,
        )

        try:
            with os.fdopen(
                fd,
                "w",
                encoding="utf-8",
            ) as file:
                json.dump(
                    {
                        "format_version": self.FORMAT_VERSION,
                        "records": self.records,
                    },
                    file,
                    ensure_ascii=False,
                    indent=2,
                )

                file.flush()
                os.fsync(file.fileno())

            os.replace(
                temporary_path,
                self.path,
            )

        except Exception:
            try:
                os.unlink(temporary_path)
            except OSError:
                pass
            raise

    def load(self):
        if not os.path.exists(self.path):
            return

        with open(
            self.path,
            "r",
            encoding="utf-8",
        ) as file:
            data = json.load(file)

        if isinstance(data, dict) and "records" in data:
            self.records = dict(data["records"])
        elif isinstance(data, dict):
            # Backward-safe handling of a raw record dictionary.
            self.records = dict(data)
