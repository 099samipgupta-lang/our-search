from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class ValidationFinding:
    status: str
    subject: str
    predicate: str
    value: str
    reason: str
    confidence: float


@dataclass(frozen=True)
class ValidationResult:
    accepted: list[dict]
    rejected: list[dict]
    findings: list[ValidationFinding]


class KnowledgeValidationEngine:
    """
    Validates candidate knowledge before it becomes trusted
    internal knowledge.

    This layer does not claim that a fact is objectively true.
    It performs structural consistency checks and identifies
    conflicts that require stronger evidence.
    """

    VERSION = "knowledge-validation.v1"

    def __init__(self):
        self._exclusive_predicates = {
            "type",
            "definition",
            "located in",
            "birth date",
            "death date",
            "capital",
        }

    @staticmethod
    def _clean(value):
        return " ".join(
            str(value or "").split()
        ).strip()

    @staticmethod
    def _key(value):
        return " ".join(
            str(value or "").casefold().split()
        )

    def _validate_shape(self, fact):
        if not isinstance(fact, dict):
            return (
                False,
                "fact is not a dictionary",
            )

        subject = self._clean(
            fact.get("subject", "")
        )
        predicate = self._clean(
            fact.get("predicate", "")
        )
        value = self._clean(
            fact.get("value", "")
        )

        if not subject:
            return False, "missing subject"

        if not predicate:
            return False, "missing predicate"

        if not value:
            return False, "missing value"

        if len(subject) > 300:
            return False, "subject is too long"

        if len(predicate) > 200:
            return False, "predicate is too long"

        if len(value) > 2000:
            return False, "value is too long"

        confidence = fact.get(
            "confidence",
            0.0,
        )

        try:
            confidence = float(confidence)
        except (TypeError, ValueError):
            return False, "invalid confidence"

        if not 0.0 <= confidence <= 1.0:
            return False, "confidence outside valid range"

        return True, ""

    def validate(
        self,
        facts: Iterable[dict],
        existing_facts: Iterable[dict] = (),
    ):
        accepted = []
        rejected = []
        findings = []

        existing = {}

        for fact in existing_facts:
            if not isinstance(fact, dict):
                continue

            key = (
                self._key(fact.get("subject")),
                self._key(fact.get("predicate")),
            )

            existing.setdefault(
                key,
                [],
            ).append(fact)

        for fact in facts:
            valid, reason = self._validate_shape(
                fact
            )

            subject = self._clean(
                fact.get("subject", "")
                if isinstance(fact, dict)
                else ""
            )

            predicate = self._clean(
                fact.get("predicate", "")
                if isinstance(fact, dict)
                else ""
            )

            value = self._clean(
                fact.get("value", "")
                if isinstance(fact, dict)
                else ""
            )

            confidence = 0.0

            if isinstance(fact, dict):
                try:
                    confidence = float(
                        fact.get(
                            "confidence",
                            0.0,
                        )
                    )
                except (TypeError, ValueError):
                    confidence = 0.0

            if not valid:
                rejected.append(
                    fact
                )

                findings.append(
                    ValidationFinding(
                        status="rejected",
                        subject=subject,
                        predicate=predicate,
                        value=value,
                        reason=reason,
                        confidence=confidence,
                    )
                )

                continue

            key = (
                self._key(subject),
                self._key(predicate),
            )

            duplicate = False
            conflict = False

            for previous in existing.get(key, []):
                previous_value = self._key(
                    previous.get("value", "")
                )

                if previous_value == self._key(value):
                    duplicate = True
                    break

                if (
                    self._key(predicate)
                    in self._exclusive_predicates
                ):
                    conflict = True

            if duplicate:
                findings.append(
                    ValidationFinding(
                        status="duplicate",
                        subject=subject,
                        predicate=predicate,
                        value=value,
                        reason="matching knowledge already exists",
                        confidence=confidence,
                    )
                )

                continue

            if conflict:
                rejected.append(
                    fact
                )

                findings.append(
                    ValidationFinding(
                        status="conflict",
                        subject=subject,
                        predicate=predicate,
                        value=value,
                        reason=(
                            "existing knowledge has a different "
                            "value for an exclusive predicate"
                        ),
                        confidence=confidence,
                    )
                )

                continue

            accepted.append(
                fact
            )

            findings.append(
                ValidationFinding(
                    status="accepted",
                    subject=subject,
                    predicate=predicate,
                    value=value,
                    reason="passed structural validation",
                    confidence=confidence,
                )
            )

            existing.setdefault(
                key,
                [],
            ).append(fact)

        return ValidationResult(
            accepted=accepted,
            rejected=rejected,
            findings=findings,
        )


__all__ = [
    "ValidationFinding",
    "ValidationResult",
    "KnowledgeValidationEngine",
]
