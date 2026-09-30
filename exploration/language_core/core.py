from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List

from .llama_runtime import LlamaRuntime


@dataclass
class LanguageUnderstanding:
    text: str
    intent: str
    tokens: List[str] = field(default_factory=list)
    entities: List[str] = field(default_factory=list)
    context: Dict[str, Any] = field(default_factory=dict)


@dataclass
class LanguageResponse:
    text: str
    confidence: float
    understanding: LanguageUnderstanding
    metadata: Dict[str, Any] = field(default_factory=dict)


class LanguageBrain:
    VERSION = "language-core.v2"

    def __init__(self, runtime=None):
        self.runtime = runtime if runtime is not None else LlamaRuntime()

    def understand(self, text, context=None):
        normalized = " ".join(str(text).strip().split())
        lowered = normalized.lower()
        tokens = lowered.split()

        if lowered in {"hi", "hello", "hey", "hi bro", "hello bro", "hey bro"}:
            intent = "greeting"
        elif lowered.endswith("?"):
            intent = "question"
        else:
            intent = "statement"

        return LanguageUnderstanding(
            text=normalized,
            intent=intent,
            tokens=tokens,
            context=context or {},
        )

    def generate(self, understanding):
        prompt = (
            "You are the conversational language brain of OUR SEARCH. "
            "Respond naturally, clearly and helpfully.\n\n"
            f"User: {understanding.text}\n"
            "Assistant:"
        )

        answer = self.runtime.generate(prompt, max_tokens=128)

        return LanguageResponse(
            text=answer,
            confidence=0.8,
            understanding=understanding,
            metadata={
                "version": self.VERSION,
                "learned_model": True,
                "runtime": self.runtime.VERSION,
            },
        )

    def respond(self, text, context=None):
        understanding = self.understand(text, context=context)
        return self.generate(understanding)
