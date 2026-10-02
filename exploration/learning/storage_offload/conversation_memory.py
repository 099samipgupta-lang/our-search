from pathlib import Path
import json


class ConversationMemory:
    VERSION = "conversation-memory.v1"

    def __init__(self, corpus_root="exploration/learning/real_corpus"):
        self.records = {}
        root = Path(corpus_root)

        for path in sorted(root.rglob("*.jsonl")):
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue

                    record = json.loads(line)
                    text = str(record.get("text", "")).strip()

                    if "User:" not in text or "Assistant:" not in text:
                        continue

                    user_part, assistant_part = text.split(
                        "Assistant:", 1
                    )

                    user_text = user_part.replace(
                        "User:", "", 1
                    ).strip()

                    assistant_text = assistant_part.strip()

                    if user_text and assistant_text:
                        self.records[
                            self._normalize(user_text)
                        ] = assistant_text

    @staticmethod
    def _normalize(text):
        return " ".join(str(text).strip().lower().split())

    def lookup(self, user_text):
        return self.records.get(
            self._normalize(user_text)
        )
