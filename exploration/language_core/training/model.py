from __future__ import annotations

import math

try:
    import torch
    import torch.nn as nn
except ImportError as exc:
    raise RuntimeError(
        "PyTorch is required for the training model."
    ) from exc


class OurSearchLanguageModel(nn.Module):
    """
    Small causal language model foundation.

    This is intentionally compact so we can experiment with
    CPU training before scaling the architecture.
    """

    VERSION = "our-search-language-model.v1"

    def __init__(
        self,
        vocab_size: int,
        embedding_size: int = 128,
        hidden_size: int = 256,
        layers: int = 2,
        heads: int = 4,
        max_sequence_length: int = 256,
    ):
        super().__init__()

        self.vocab_size = vocab_size
        self.max_sequence_length = max_sequence_length

        self.token_embedding = nn.Embedding(
            vocab_size,
            embedding_size,
        )

        self.position_embedding = nn.Embedding(
            max_sequence_length,
            embedding_size,
        )

        layer = nn.TransformerEncoderLayer(
            d_model=embedding_size,
            nhead=heads,
            dim_feedforward=hidden_size,
            batch_first=True,
            activation="gelu",
        )

        self.transformer = nn.TransformerEncoder(
            layer,
            num_layers=layers,
        )

        self.output = nn.Linear(
            embedding_size,
            vocab_size,
        )

    def forward(self, input_ids):
        batch_size, sequence_length = input_ids.shape

        if sequence_length > self.max_sequence_length:
            raise ValueError(
                "Sequence length exceeds model limit."
            )

        positions = torch.arange(
            sequence_length,
            device=input_ids.device,
        ).unsqueeze(0)

        hidden = (
            self.token_embedding(input_ids)
            + self.position_embedding(positions)
        )

        mask = torch.triu(
            torch.ones(
                sequence_length,
                sequence_length,
                device=input_ids.device,
            ),
            diagonal=1,
        ).bool()

        hidden = self.transformer(
            hidden,
            mask=mask,
        )

        return self.output(hidden)


__all__ = [
    "OurSearchLanguageModel",
]
