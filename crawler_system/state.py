import json
import os
import tempfile


class CrawlState:

    def __init__(
        self,
        path="crawler_state.json"
    ):

        self.path = path

    def save(self, state):

        directory = os.path.dirname(
            os.path.abspath(self.path)
        )

        os.makedirs(
            directory,
            exist_ok=True
        )

        fd, temporary_path = (
            tempfile.mkstemp(
                prefix=".crawler_state_",
                suffix=".tmp",
                dir=directory
            )
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

        return os.path.exists(
            self.path
        )

    def clear(self):

        if os.path.exists(
            self.path
        ):

            os.remove(
                self.path
            )
