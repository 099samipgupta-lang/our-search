import json
import os
import tempfile


class CrawlerStateStorage:

    def __init__(self, root):
        self.root = root
        self.path = os.path.join(
            root,
            "crawler_state.json"
        )

    def load(self):
        if not os.path.exists(self.path):
            return {}

        with open(
            self.path,
            "r",
            encoding="utf-8"
        ) as file:
            return json.load(file)

    def save(
        self,
        url_dedup,
        content_dedup,
        change_tracker
    ):
        state = {
            "url_dedup": url_dedup.get_state(),
            "content_dedup": content_dedup.get_state(),
            "change_tracker": change_tracker.get_state()
        }

        os.makedirs(
            self.root,
            exist_ok=True
        )

        directory = os.path.dirname(
            self.path
        )

        fd, temp_path = tempfile.mkstemp(
            dir=directory,
            prefix=".crawler_state_",
            suffix=".tmp"
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
                    ensure_ascii=False
                )
                file.flush()
                os.fsync(file.fileno())

            os.replace(
                temp_path,
                self.path
            )

        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)
