import json
import os
import tempfile


class FrontierStorage:

    def __init__(
        self,
        path="crawler_storage/frontier/state.json"
    ):

        self.path = os.path.abspath(path)

        os.makedirs(
            os.path.dirname(self.path),
            exist_ok=True
        )

    def save(self, state):

        fd, temporary_path = tempfile.mkstemp(
            prefix=".frontier_",
            suffix=".tmp",
            dir=os.path.dirname(self.path)
        )

        try:

            with os.fdopen(
                fd,
                "w",
                encoding="utf-8"
            ) as file:

                json.dump(
                    state,
                    file,
                    ensure_ascii=False,
                    indent=2
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

    def load(self):

        if not os.path.exists(
            self.path
        ):
            return None

        with open(
            self.path,
            "r",
            encoding="utf-8"
        ) as file:

            return json.load(file)

    def exists(self):

        return os.path.isfile(
            self.path
        )
