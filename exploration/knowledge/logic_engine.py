from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class LogicResult:
    statement: str
    conclusion: str
    premises: List[str]
    success: bool
    rule: str = ""
    error: str = ""


class LogicCapabilityEngine:
    VERSION = "logic-capability.v1"

    _implication = re.compile(
        r"^\s*(.+?)\s+(?:implies|means|leads to)\s+(.+?)\s*$",
        re.IGNORECASE,
    )

    _if_then = re.compile(
        r"^\s*if\s+(.+?)\s+then\s+(.+?)\s*$",
        re.IGNORECASE,
    )

    def _normalize(self, text: str) -> str:
        return " ".join(
            text.strip().lower().split()
        )

    def _parse_implication(
        self,
        statement: str,
    ) -> Optional[tuple[str, str]]:
        match = self._if_then.match(statement)

        if match:
            return (
                self._normalize(match.group(1)),
                self._normalize(match.group(2)),
            )

        match = self._implication.match(statement)

        if match:
            return (
                self._normalize(match.group(1)),
                self._normalize(match.group(2)),
            )

        return None

    def modus_ponens(
        self,
        premise: str,
        implication: str,
    ) -> LogicResult:
        normalized_premise = self._normalize(
            premise
        )

        parsed = self._parse_implication(
            implication
        )

        if parsed is None:
            return LogicResult(
                statement=implication,
                conclusion="",
                premises=[premise],
                success=False,
                error="Could not parse implication.",
            )

        condition, consequence = parsed

        if normalized_premise != condition:
            return LogicResult(
                statement=implication,
                conclusion="",
                premises=[premise],
                success=False,
                error=(
                    "Premise does not satisfy "
                    "the implication condition."
                ),
            )

        return LogicResult(
            statement=implication,
            conclusion=consequence,
            premises=[
                premise,
                implication,
            ],
            success=True,
            rule="modus ponens",
        )

    def chain(
        self,
        statements: List[str],
    ) -> LogicResult:
        normalized = [
            self._normalize(statement)
            for statement in statements
            if statement.strip()
        ]

        if len(normalized) < 2:
            return LogicResult(
                statement="",
                conclusion="",
                premises=normalized,
                success=False,
                error=(
                    "At least two statements "
                    "are required."
                ),
            )

        first = normalized[0]
        current = first

        for implication in normalized[1:]:
            parsed = self._parse_implication(
                implication
            )

            if parsed is None:
                return LogicResult(
                    statement="",
                    conclusion="",
                    premises=normalized,
                    success=False,
                    error=(
                        "Could not parse implication."
                    ),
                )

            condition, consequence = parsed

            if current != condition:
                return LogicResult(
                    statement="",
                    conclusion="",
                    premises=normalized,
                    success=False,
                    error=(
                        "The logical chain is "
                        "not connected."
                    ),
                )

            current = consequence

        return LogicResult(
            statement=" → ".join(normalized),
            conclusion=current,
            premises=normalized,
            success=True,
            rule="implication chain",
        )

    def can_handle(
        self,
        query: str,
    ) -> bool:
        text = self._normalize(query)

        return (
            ("if " in text and " then " in text)
            or " implies " in text
            or " means " in text
            or "leads to" in text
        )
