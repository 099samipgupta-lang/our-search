from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass
class LanguageTrainingConfig:
    VERSION: str = "language-training.v1"

    dataset_path: Path = Path(
        "exploration/language_core/data/training.jsonl"
    )

    checkpoint_dir: Path = Path(
        "exploration/language_core/checkpoints"
    )

    sequence_length: int = 256
    batch_size: int = 1
    gradient_accumulation_steps: int = 8

    learning_rate: float = 3e-4
    epochs: int = 1

    save_every_steps: int = 100
    log_every_steps: int = 10

    seed: int = 42

    def prepare(self) -> None:
        self.dataset_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.checkpoint_dir.mkdir(
            parents=True,
            exist_ok=True,
        )


__all__ = [
    "LanguageTrainingConfig",
]
