from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterator, List


class LanguageTrainingDataset:
    VERSION = "language-training-dataset.v1"

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def add(
        self,
        user: str,
        assistant: str,
        context: str = "",
    ) -> None:
        record = {
            "user": str(user),
            "assistant": str(assistant),
            "context": str(context),
        }

        with self.path.open(
            "a",
            encoding="utf-8",
        ) as handle:
            handle.write(
                json.dumps(
                    record,
                    ensure_ascii=False,
                )
                + "\n"
            )

    def add_batch(
        self,
        records: List[Dict[str, str]],
    ) -> None:
        for record in records:
            self.add(
                user=record.get("user", ""),
                assistant=record.get("assistant", ""),
                context=record.get("context", ""),
            )

    def read(self) -> Iterator[Dict[str, str]]:
        if not self.path.exists():
            return

        with self.path.open(
            "r",
            encoding="utf-8",
        ) as handle:
            for line in handle:
                line = line.strip()

                if not line:
                    continue

                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue

                if not isinstance(record, dict):
                    continue

                yield {
                    "user": str(record.get("user", "")),
                    "assistant": str(
                        record.get("assistant", "")
                    ),
                    "context": str(
                        record.get("context", "")
                    ),
                }

    def count(self) -> int:
        if not self.path.exists():
            return 0

        return sum(
            1
            for _ in self.read()
        )


__all__ = [
    "LanguageTrainingDataset",
]
