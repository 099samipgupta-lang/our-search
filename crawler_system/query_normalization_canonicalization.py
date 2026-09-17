"""
OUR SEARCH
Phase 11.2 — Query Normalization & Canonicalization

Version:
    query-normalization-canonicalization.v1

Purpose:
    Convert the Phase 11.1 understood query representation into a stable,
    deterministic canonical representation suitable for distributed
    retrieval planning.

Scale target:
    Billions -> potentially trillions of publicly accessible Web resources.

Design target:
    Google-scale general Web-search capability without using Google
    technology or infrastructure.

Architecture:

    PHASE 11.1 CANONICAL QUERY
              |
              v
       NORMALIZATION
              |
       +------+------+
       |             |
       v             v
    TOKEN         PHRASE
    NORMALIZATION  NORMALIZATION
       |             |
       +------+------+
              |
              v
       FIELD / OPERATOR
       CANONICALIZATION
              |
              v
       DUPLICATE / ORDER
       NORMALIZATION
              |
              v
       QUERY SEMANTIC
       FINGERPRINT
              |
              v
       CANONICAL QUERY
              |
              v
       PHASE 11.3+
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from enum import Enum
import hashlib
import re
import time
import unicodedata
from typing import Any, Dict, List, Mapping, Optional, Protocol, Sequence, Tuple


ARCHITECTURE_VERSION = "query-normalization-canonicalization.v1"
SCALE_TARGET = "billions_to_trillions_of_public_web_resources"
GOOGLE_SCALE_CAPABILITY_TARGET = True
GOOGLE_TECHNOLOGY_DEPENDENCY = False


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _now() -> float:
    return time.time()


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _new_id(prefix: str) -> str:
    return f"{prefix}-{_hash(f'{prefix}:{time.time_ns()}')[:24]}"


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------

class NormalizationState(str, Enum):
    RECEIVED = "received"
    VALIDATING = "validating"
    NORMALIZING = "normalizing"
    CANONICALIZING = "canonicalizing"
    COMPLETED = "completed"
    REJECTED = "rejected"


class CanonicalTokenKind(str, Enum):
    TERM = "term"
    NUMBER = "number"
    URL = "url"
    EMAIL = "email"
    PHRASE = "phrase"
    SYMBOL = "symbol"
    OPERATOR = "operator"


class CanonicalField(str, Enum):
    ANY = "any"
    TITLE = "title"
    BODY = "body"
    URL = "url"
    DOMAIN = "domain"
    ANCHOR = "anchor"
    METADATA = "metadata"


class CanonicalOperator(str, Enum):
    AND = "and"
    OR = "or"
    NOT = "not"
    PHRASE = "phrase"
    FIELD = "field"


class NormalizationEventType(str, Enum):
    NORMALIZATION_RECEIVED = "normalization_received"
    NORMALIZATION_STARTED = "normalization_started"
    TOKEN_NORMALIZED = "token_normalized"
    PHRASE_NORMALIZED = "phrase_normalized"
    FIELD_CANONICALIZED = "field_canonicalized"
    DUPLICATES_REMOVED = "duplicates_removed"
    ORDER_CANONICALIZED = "order_canonicalized"
    FINGERPRINT_CREATED = "fingerprint_created"
    NORMALIZATION_COMPLETED = "normalization_completed"
    NORMALIZATION_REJECTED = "normalization_rejected"
    CHECKPOINT_CREATED = "checkpoint_created"


# ---------------------------------------------------------------------------
# Input contract
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class QueryNormalizationRequest:
    query: str
    query_id: Optional[str] = None
    language: str = "und"
    script: str = "Unknown"
    region_hint: Optional[str] = None
    preserve_term_order: bool = False


@dataclass(frozen=True)
class RawQueryToken:
    position: int
    text: str
    normalized_text: str
    kind: str
    field: str = "any"


@dataclass(frozen=True)
class RawQueryPhrase:
    text: str
    normalized_text: str
    start_position: int
    end_position: int


# ---------------------------------------------------------------------------
# Canonical token structures
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CanonicalToken:
    ordinal: int
    source_position: int
    text: str
    normalized_text: str
    kind: CanonicalTokenKind
    field: CanonicalField = CanonicalField.ANY


@dataclass(frozen=True)
class CanonicalPhrase:
    ordinal: int
    text: str
    normalized_text: str
    tokens: Tuple[str, ...]


@dataclass(frozen=True)
class CanonicalFieldConstraint:
    field: CanonicalField
    value: str
    normalized_value: str
    operator: CanonicalOperator = CanonicalOperator.FIELD


@dataclass(frozen=True)
class CanonicalOperatorToken:
    ordinal: int
    operator: CanonicalOperator
    value: Optional[str] = None


# ---------------------------------------------------------------------------
# Canonical query
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class QueryCanonicalIdentity:
    query_id: str
    source_query_id: Optional[str]
    source_query_hash: str
    canonical_hash: str
    semantic_fingerprint: str
    created_at: float
    architecture_version: str = ARCHITECTURE_VERSION


@dataclass(frozen=True)
class CanonicalQueryRepresentation:
    identity: QueryCanonicalIdentity

    raw_query: str
    normalized_query: str

    language: str
    script: str
    region_hint: Optional[str]

    tokens: Tuple[CanonicalToken, ...]
    phrases: Tuple[CanonicalPhrase, ...]

    terms: Tuple[str, ...]
    unique_terms: Tuple[str, ...]

    field_constraints: Tuple[CanonicalFieldConstraint, ...]

    operators: Tuple[CanonicalOperatorToken, ...]

    ordered_terms: Tuple[str, ...]
    canonical_terms: Tuple[str, ...]

    exact_terms: Tuple[str, ...]

    token_count: int
    unique_term_count: int
    phrase_count: int

    version: str = ARCHITECTURE_VERSION


@dataclass(frozen=True)
class QueryNormalizationResult:
    state: NormalizationState
    canonical_query: Optional[CanonicalQueryRepresentation]
    errors: Tuple[str, ...] = ()


# ---------------------------------------------------------------------------
# Checkpoint / events
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class QueryNormalizationCheckpoint:
    checkpoint_id: str
    epoch: int
    created_at: float
    processed_queries: int
    architecture_version: str = ARCHITECTURE_VERSION


@dataclass(frozen=True)
class QueryNormalizationEvent:
    event_id: str
    event_type: NormalizationEventType
    query_id: Optional[str]
    epoch: int
    created_at: float
    payload: Mapping[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Backend contract
# ---------------------------------------------------------------------------

class QueryNormalizationBackend(Protocol):
    """
    Replaceable metadata/event backend.

    Production deployments can connect this contract to distributed
    infrastructure. The reference implementation below is intentionally
    in-memory and does not represent the global production storage layer.
    """

    def record_event(self, event: QueryNormalizationEvent) -> None:
        ...

    def save_checkpoint(
        self,
        checkpoint: QueryNormalizationCheckpoint,
    ) -> None:
        ...

    def get_epoch(self) -> int:
        ...

    def advance_epoch(self) -> int:
        ...


# ---------------------------------------------------------------------------
# Reference backend
# ---------------------------------------------------------------------------

class InMemoryQueryNormalizationMetadata:
    def __init__(self) -> None:
        self.events: List[QueryNormalizationEvent] = []
        self.checkpoints: List[QueryNormalizationCheckpoint] = []
        self._epoch = 0

    def record_event(self, event: QueryNormalizationEvent) -> None:
        self.events.append(event)

    def save_checkpoint(
        self,
        checkpoint: QueryNormalizationCheckpoint,
    ) -> None:
        self.checkpoints.append(checkpoint)

    def get_epoch(self) -> int:
        return self._epoch

    def advance_epoch(self) -> int:
        self._epoch += 1
        return self._epoch


# ---------------------------------------------------------------------------
# Policy
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class QueryNormalizationPolicy:
    """
    Deterministic normalization policy.

    Important:
        Normalization must not silently destroy meaning.

    Therefore:
        - URLs remain URL-like.
        - Emails remain email-like.
        - quoted phrases remain phrases.
        - field restrictions remain explicit.
        - order can be preserved when downstream logic requires it.
    """

    max_query_length: int = 32_768
    max_tokens: int = 4_096
    max_phrases: int = 512
    max_field_constraints: int = 512

    unicode_form: str = "NFKC"

    collapse_whitespace: bool = True
    casefold_terms: bool = True

    remove_duplicate_terms: bool = True
    remove_duplicate_phrases: bool = True
    remove_duplicate_constraints: bool = True

    canonicalize_term_order: bool = True

    preserve_numbers: bool = True
    preserve_urls: bool = True
    preserve_emails: bool = True
    preserve_symbols: bool = True

    checkpoint_interval_queries: int = 1


# ---------------------------------------------------------------------------
# Query normalization and canonicalization
# ---------------------------------------------------------------------------

class QueryNormalizationCanonicalization:
    """
    Phase 11.2.

    Responsibilities:
      * validate query input
      * normalize Unicode
      * normalize whitespace
      * normalize tokens
      * normalize phrases
      * canonicalize field constraints
      * canonicalize operators
      * remove semantically redundant duplicates
      * produce deterministic term ordering
      * create stable query hashes/fingerprints
      * emit distributed metadata events
      * create checkpoints

    This layer does NOT:
      * retrieve documents
      * rank documents
      * expand queries semantically
      * access Google's systems
      * modify the index
      * serve website requests
    """

    FIELD_ALIASES: Mapping[str, CanonicalField] = {
        "title": CanonicalField.TITLE,
        "intitle": CanonicalField.TITLE,
        "url": CanonicalField.URL,
        "inurl": CanonicalField.URL,
        "domain": CanonicalField.DOMAIN,
        "site": CanonicalField.DOMAIN,
        "body": CanonicalField.BODY,
        "text": CanonicalField.BODY,
        "anchor": CanonicalField.ANCHOR,
        "metadata": CanonicalField.METADATA,
    }

    FIELD_RE = re.compile(
        r"^(title|intitle|url|inurl|domain|site|body|text|anchor|metadata)"
        r":(.+)$",
        re.IGNORECASE,
    )

    QUOTED_RE = re.compile(r'"([^"]*)"')

    URL_RE = re.compile(
        r"^(?:https?://|www\.)[^\s]+$",
        re.IGNORECASE,
    )

    EMAIL_RE = re.compile(
        r"^[^@\s]+@[^@\s]+\.[^@\s]+$",
        re.IGNORECASE,
    )

    OPERATOR_RE = re.compile(
        r"^(AND|OR|NOT)$",
        re.IGNORECASE,
    )

    def __init__(
        self,
        backend: Optional[QueryNormalizationBackend] = None,
        policy: Optional[QueryNormalizationPolicy] = None,
    ) -> None:
        self.backend = backend or InMemoryQueryNormalizationMetadata()
        self.policy = policy or QueryNormalizationPolicy()
        self.processed_queries = 0

    # ------------------------------------------------------------------
    # Epoch
    # ------------------------------------------------------------------

    @property
    def epoch(self) -> int:
        return self.backend.get_epoch()

    def advance_epoch(self) -> int:
        return self.backend.advance_epoch()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def normalize(
        self,
        request: QueryNormalizationRequest | str,
    ) -> QueryNormalizationResult:
        if isinstance(request, str):
            request = QueryNormalizationRequest(query=request)

        query_id = request.query_id or _new_id("query-normalization")

        self._emit(
            NormalizationEventType.NORMALIZATION_RECEIVED,
            query_id,
            {
                "query_length": len(request.query),
            },
        )

        errors = self._validate(request.query)

        if errors:
            self._emit(
                NormalizationEventType.NORMALIZATION_REJECTED,
                query_id,
                {"errors": errors},
            )

            return QueryNormalizationResult(
                state=NormalizationState.REJECTED,
                canonical_query=None,
                errors=tuple(errors),
            )

        self._emit(
            NormalizationEventType.NORMALIZATION_STARTED,
            query_id,
            {},
        )

        normalized_query = self._normalize_query(request.query)

        raw_tokens = self._tokenize(
            normalized_query,
            request.preserve_term_order,
        )

        canonical_tokens = self._normalize_tokens(raw_tokens)

        phrases = self._normalize_phrases(normalized_query)

        constraints = self._canonicalize_field_constraints(
            canonical_tokens
        )

        operators = self._canonicalize_operators(
            canonical_tokens
        )

        unique_tokens = self._remove_duplicate_tokens(
            canonical_tokens
        )

        unique_phrases = self._remove_duplicate_phrases(
            phrases
        )

        unique_constraints = self._remove_duplicate_constraints(
            constraints
        )

        ordered_terms = self._ordered_terms(
            unique_tokens,
            preserve_order=request.preserve_term_order,
        )

        canonical_terms = self._canonical_terms(
            unique_tokens
        )

        exact_terms = self._exact_terms(unique_tokens)

        semantic_fingerprint = self._semantic_fingerprint(
            normalized_query=normalized_query,
            language=request.language,
            script=request.script,
            tokens=unique_tokens,
            phrases=unique_phrases,
            constraints=unique_constraints,
            operators=operators,
        )

        canonical_hash = self._canonical_hash(
            normalized_query=normalized_query,
            language=request.language,
            script=request.script,
            region_hint=request.region_hint,
            tokens=unique_tokens,
            phrases=unique_phrases,
            constraints=unique_constraints,
            operators=operators,
            ordered_terms=ordered_terms,
        )

        identity = QueryCanonicalIdentity(
            query_id=_new_id("canonical-query"),
            source_query_id=request.query_id,
            source_query_hash=_hash(request.query),
            canonical_hash=canonical_hash,
            semantic_fingerprint=semantic_fingerprint,
            created_at=_now(),
        )

        canonical = CanonicalQueryRepresentation(
            identity=identity,
            raw_query=request.query,
            normalized_query=normalized_query,
            language=request.language,
            script=request.script,
            region_hint=request.region_hint,
            tokens=tuple(unique_tokens),
            phrases=tuple(unique_phrases),
            terms=tuple(
                token.normalized_text
                for token in unique_tokens
                if token.normalized_text
            ),
            unique_terms=tuple(
                dict.fromkeys(
                    token.normalized_text
                    for token in unique_tokens
                    if token.normalized_text
                )
            ),
            field_constraints=tuple(unique_constraints),
            operators=tuple(operators),
            ordered_terms=tuple(ordered_terms),
            canonical_terms=tuple(canonical_terms),
            exact_terms=tuple(exact_terms),
            token_count=len(unique_tokens),
            unique_term_count=len(
                set(
                    token.normalized_text
                    for token in unique_tokens
                    if token.normalized_text
                )
            ),
            phrase_count=len(unique_phrases),
        )

        self.processed_queries += 1

        self._emit(
            NormalizationEventType.FINGERPRINT_CREATED,
            query_id,
            {
                "canonical_hash": canonical_hash,
                "semantic_fingerprint": semantic_fingerprint,
            },
        )

        self._emit(
            NormalizationEventType.NORMALIZATION_COMPLETED,
            query_id,
            {
                "token_count": canonical.token_count,
                "unique_term_count": canonical.unique_term_count,
                "phrase_count": canonical.phrase_count,
            },
        )

        self._maybe_checkpoint()

        return QueryNormalizationResult(
            state=NormalizationState.COMPLETED,
            canonical_query=canonical,
        )

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate(self, query: Any) -> List[str]:
        errors: List[str] = []

        if query is None:
            errors.append("query_is_null")
            return errors

        if not isinstance(query, str):
            errors.append("query_must_be_text")
            return errors

        if not query.strip():
            errors.append("query_is_empty")

        if len(query) > self.policy.max_query_length:
            errors.append("query_exceeds_max_length")

        return errors

    # ------------------------------------------------------------------
    # Query normalization
    # ------------------------------------------------------------------

    def _normalize_query(self, query: str) -> str:
        value = unicodedata.normalize(
            self.policy.unicode_form,
            query,
        )

        if self.policy.collapse_whitespace:
            value = re.sub(r"\s+", " ", value)

        return value.strip()

    # ------------------------------------------------------------------
    # Tokenization
    # ------------------------------------------------------------------

    def _tokenize(
        self,
        query: str,
        preserve_term_order: bool,
    ) -> List[RawQueryToken]:
        raw_parts = re.findall(
            r'https?://[^\s]+|www\.[^\s]+|[^\s]+',
            query,
        )

        tokens: List[RawQueryToken] = []

        for position, part in enumerate(raw_parts):
            cleaned = part.strip(".,;!?()[]{}")

            if not cleaned:
                continue

            if self.URL_RE.match(cleaned):
                kind = CanonicalTokenKind.URL.value
            elif self.EMAIL_RE.match(cleaned):
                kind = CanonicalTokenKind.EMAIL.value
            elif cleaned.isdigit():
                kind = CanonicalTokenKind.NUMBER.value
            elif self.OPERATOR_RE.match(cleaned):
                kind = CanonicalTokenKind.OPERATOR.value
            elif any(not char.isalnum() for char in cleaned):
                kind = CanonicalTokenKind.SYMBOL.value
            else:
                kind = CanonicalTokenKind.TERM.value

            tokens.append(
                RawQueryToken(
                    position=position,
                    text=cleaned,
                    normalized_text=cleaned,
                    kind=kind,
                )
            )

            if len(tokens) >= self.policy.max_tokens:
                break

        return tokens

    # ------------------------------------------------------------------
    # Token normalization
    # ------------------------------------------------------------------

    def _normalize_tokens(
        self,
        tokens: Sequence[RawQueryToken],
    ) -> List[CanonicalToken]:
        result: List[CanonicalToken] = []

        for ordinal, token in enumerate(tokens):
            normalized = self._normalize_token_value(
                token.text,
                token.kind,
            )

            field = self._field_for_token(token.text)

            canonical_kind = CanonicalTokenKind(
                token.kind
            )

            result.append(
                CanonicalToken(
                    ordinal=ordinal,
                    source_position=token.position,
                    text=token.text,
                    normalized_text=normalized,
                    kind=canonical_kind,
                    field=field,
                )
            )

            self._emit(
                NormalizationEventType.TOKEN_NORMALIZED,
                None,
                {
                    "source_position": token.position,
                    "kind": canonical_kind.value,
                },
            )

        return result

    def _normalize_token_value(
        self,
        value: str,
        kind: str,
    ) -> str:
        value = unicodedata.normalize(
            self.policy.unicode_form,
            value.strip(),
        )

        if kind == CanonicalTokenKind.URL.value:
            return self._normalize_url(value)

        if kind == CanonicalTokenKind.EMAIL.value:
            return value.casefold()

        if kind == CanonicalTokenKind.NUMBER.value:
            return value

        if kind == CanonicalTokenKind.OPERATOR.value:
            return value.casefold()

        if self.policy.casefold_terms:
            return value.casefold()

        return value

    def _normalize_url(self, value: str) -> str:
        value = value.strip()

        if not self.policy.preserve_urls:
            return value.casefold()

        if value.lower().startswith("www."):
            return value.casefold()

        return value

    # ------------------------------------------------------------------
    # Phrase normalization
    # ------------------------------------------------------------------

    def _normalize_phrases(
        self,
        query: str,
    ) -> List[CanonicalPhrase]:
        if not self.QUOTED_RE:
            return []

        phrases: List[CanonicalPhrase] = []

        for ordinal, match in enumerate(
            self.QUOTED_RE.finditer(query)
        ):
            phrase_text = match.group(1).strip()

            if not phrase_text:
                continue

            normalized = unicodedata.normalize(
                self.policy.unicode_form,
                phrase_text,
            )

            normalized = re.sub(
                r"\s+",
                " ",
                normalized,
            ).strip()

            if self.policy.casefold_terms:
                normalized = normalized.casefold()

            phrase_tokens = tuple(
                part
                for part in normalized.split(" ")
                if part
            )

            phrases.append(
                CanonicalPhrase(
                    ordinal=ordinal,
                    text=phrase_text,
                    normalized_text=normalized,
                    tokens=phrase_tokens,
                )
            )

            self._emit(
                NormalizationEventType.PHRASE_NORMALIZED,
                None,
                {
                    "phrase": normalized,
                },
            )

            if len(phrases) >= self.policy.max_phrases:
                break

        return phrases

    # ------------------------------------------------------------------
    # Field constraints
    # ------------------------------------------------------------------

    def _field_for_token(
        self,
        token: str,
    ) -> CanonicalField:
        match = self.FIELD_RE.match(token)

        if not match:
            return CanonicalField.ANY

        field_name = match.group(1).casefold()

        return self.FIELD_ALIASES.get(
            field_name,
            CanonicalField.ANY,
        )

    def _canonicalize_field_constraints(
        self,
        tokens: Sequence[CanonicalToken],
    ) -> List[CanonicalFieldConstraint]:
        constraints: List[CanonicalFieldConstraint] = []

        for token in tokens:
            match = self.FIELD_RE.match(token.text)

            if not match:
                continue

            field_name = match.group(1).casefold()
            value = match.group(2).strip()

            field = self.FIELD_ALIASES.get(
                field_name,
                CanonicalField.ANY,
            )

            normalized_value = unicodedata.normalize(
                self.policy.unicode_form,
                value,
            ).strip()

            if self.policy.casefold_terms:
                normalized_value = normalized_value.casefold()

            constraints.append(
                CanonicalFieldConstraint(
                    field=field,
                    value=value,
                    normalized_value=normalized_value,
                )
            )

            self._emit(
                NormalizationEventType.FIELD_CANONICALIZED,
                None,
                {
                    "field": field.value,
                    "value": normalized_value,
                },
            )

            if (
                len(constraints)
                >= self.policy.max_field_constraints
            ):
                break

        return constraints

    # ------------------------------------------------------------------
    # Operators
    # ------------------------------------------------------------------

    def _canonicalize_operators(
        self,
        tokens: Sequence[CanonicalToken],
    ) -> List[CanonicalOperatorToken]:
        operators: List[CanonicalOperatorToken] = []

        for ordinal, token in enumerate(tokens):
            if token.kind != CanonicalTokenKind.OPERATOR:
                continue

            operator_name = token.normalized_text.casefold()

            mapping = {
                "and": CanonicalOperator.AND,
                "or": CanonicalOperator.OR,
                "not": CanonicalOperator.NOT,
            }

            operator = mapping.get(
                operator_name,
                CanonicalOperator.AND,
            )

            operators.append(
                CanonicalOperatorToken(
                    ordinal=ordinal,
                    operator=operator,
                    value=None,
                )
            )

        return operators

    # ------------------------------------------------------------------
    # Duplicate handling
    # ------------------------------------------------------------------

    def _remove_duplicate_tokens(
        self,
        tokens: Sequence[CanonicalToken],
    ) -> List[CanonicalToken]:
        if not self.policy.remove_duplicate_terms:
            return list(tokens)

        seen: set[Tuple[str, str, str]] = set()
        result: List[CanonicalToken] = []

        removed = 0

        for token in tokens:
            key = (
                token.kind.value,
                token.normalized_text,
                token.field.value,
            )

            if key in seen:
                removed += 1
                continue

            seen.add(key)
            result.append(token)

        if removed:
            self._emit(
                NormalizationEventType.DUPLICATES_REMOVED,
                None,
                {
                    "token_duplicates_removed": removed,
                },
            )

        return self._reordinal_tokens(result)

    def _remove_duplicate_phrases(
        self,
        phrases: Sequence[CanonicalPhrase],
    ) -> List[CanonicalPhrase]:
        if not self.policy.remove_duplicate_phrases:
            return list(phrases)

        seen: set[str] = set()
        result: List[CanonicalPhrase] = []

        for phrase in phrases:
            if phrase.normalized_text in seen:
                continue

            seen.add(phrase.normalized_text)
            result.append(phrase)

        return self._reordinal_phrases(result)

    def _remove_duplicate_constraints(
        self,
        constraints: Sequence[CanonicalFieldConstraint],
    ) -> List[CanonicalFieldConstraint]:
        if not self.policy.remove_duplicate_constraints:
            return list(constraints)

        seen: set[Tuple[str, str]] = set()
        result: List[CanonicalFieldConstraint] = []

        for constraint in constraints:
            key = (
                constraint.field.value,
                constraint.normalized_value,
            )

            if key in seen:
                continue

            seen.add(key)
            result.append(constraint)

        return result

    def _reordinal_tokens(
        self,
        tokens: Sequence[CanonicalToken],
    ) -> List[CanonicalToken]:
        return [
            CanonicalToken(
                ordinal=index,
                source_position=token.source_position,
                text=token.text,
                normalized_text=token.normalized_text,
                kind=token.kind,
                field=token.field,
            )
            for index, token in enumerate(tokens)
        ]

    def _reordinal_phrases(
        self,
        phrases: Sequence[CanonicalPhrase],
    ) -> List[CanonicalPhrase]:
        return [
            CanonicalPhrase(
                ordinal=index,
                text=phrase.text,
                normalized_text=phrase.normalized_text,
                tokens=phrase.tokens,
            )
            for index, phrase in enumerate(phrases)
        ]

    # ------------------------------------------------------------------
    # Term ordering
    # ------------------------------------------------------------------

    def _ordered_terms(
        self,
        tokens: Sequence[CanonicalToken],
        preserve_order: bool,
    ) -> List[str]:
        values = [
            token.normalized_text
            for token in tokens
            if token.kind
            in {
                CanonicalTokenKind.TERM,
                CanonicalTokenKind.NUMBER,
            }
            and token.normalized_text
        ]

        if preserve_order or not self.policy.canonicalize_term_order:
            return list(values)

        return sorted(values)

    def _canonical_terms(
        self,
        tokens: Sequence[CanonicalToken],
    ) -> List[str]:
        values = [
            token.normalized_text
            for token in tokens
            if token.normalized_text
            and token.kind
            not in {
                CanonicalTokenKind.SYMBOL,
                CanonicalTokenKind.OPERATOR,
            }
        ]

        return sorted(set(values))

    def _exact_terms(
        self,
        tokens: Sequence[CanonicalToken],
    ) -> List[str]:
        return [
            token.normalized_text
            for token in tokens
            if token.kind
            in {
                CanonicalTokenKind.URL,
                CanonicalTokenKind.EMAIL,
            }
        ]

    # ------------------------------------------------------------------
    # Fingerprints
    # ------------------------------------------------------------------

    def _canonical_hash(
        self,
        normalized_query: str,
        language: str,
        script: str,
        region_hint: Optional[str],
        tokens: Sequence[CanonicalToken],
        phrases: Sequence[CanonicalPhrase],
        constraints: Sequence[CanonicalFieldConstraint],
        operators: Sequence[CanonicalOperatorToken],
        ordered_terms: Sequence[str],
    ) -> str:
        payload = {
            "normalized_query": normalized_query,
            "language": language,
            "script": script,
            "region_hint": region_hint,
            "tokens": [
                (
                    token.kind.value,
                    token.normalized_text,
                    token.field.value,
                )
                for token in tokens
            ],
            "phrases": [
                phrase.normalized_text
                for phrase in phrases
            ],
            "constraints": [
                (
                    constraint.field.value,
                    constraint.normalized_value,
                )
                for constraint in constraints
            ],
            "operators": [
                operator.operator.value
                for operator in operators
            ],
            "ordered_terms": list(ordered_terms),
        }

        return _hash(repr(payload))

    def _semantic_fingerprint(
        self,
        normalized_query: str,
        language: str,
        script: str,
        tokens: Sequence[CanonicalToken],
        phrases: Sequence[CanonicalPhrase],
        constraints: Sequence[CanonicalFieldConstraint],
        operators: Sequence[CanonicalOperatorToken],
    ) -> str:
        """
        Fingerprint intended to identify equivalent normalized retrieval
        structures.

        This is deliberately not a semantic embedding.

        Semantic expansion belongs to Phase 11.4.
        """

        term_values = sorted(
            {
                token.normalized_text
                for token in tokens
                if token.normalized_text
            }
        )

        phrase_values = sorted(
            {
                phrase.normalized_text
                for phrase in phrases
            }
        )

        constraint_values = sorted(
            (
                constraint.field.value,
                constraint.normalized_value,
            )
            for constraint in constraints
        )

        operator_values = [
            operator.operator.value
            for operator in operators
        ]

        payload = {
            "language": language,
            "script": script,
            "terms": term_values,
            "phrases": phrase_values,
            "constraints": constraint_values,
            "operators": operator_values,
        }

        return _hash(repr(payload))

    # ------------------------------------------------------------------
    # Checkpoints
    # ------------------------------------------------------------------

    def _maybe_checkpoint(self) -> None:
        interval = self.policy.checkpoint_interval_queries

        if interval <= 0:
            return

        if (
            self.processed_queries % interval
            != 0
        ):
            return

        checkpoint = QueryNormalizationCheckpoint(
            checkpoint_id=_new_id(
                "query-normalization-checkpoint"
            ),
            epoch=self.epoch,
            created_at=_now(),
            processed_queries=self.processed_queries,
        )

        self.backend.save_checkpoint(checkpoint)

        self._emit(
            NormalizationEventType.CHECKPOINT_CREATED,
            None,
            {
                "checkpoint_id": checkpoint.checkpoint_id,
                "processed_queries": self.processed_queries,
            },
        )

    # ------------------------------------------------------------------
    # Events
    # ------------------------------------------------------------------

    def _emit(
        self,
        event_type: NormalizationEventType,
        query_id: Optional[str],
        payload: Mapping[str, Any],
    ) -> None:
        event = QueryNormalizationEvent(
            event_id=_new_id("query-normalization-event"),
            event_type=event_type,
            query_id=query_id,
            epoch=self.epoch,
            created_at=_now(),
            payload=dict(payload),
        )

        self.backend.record_event(event)

    # ------------------------------------------------------------------
    # Architecture description
    # ------------------------------------------------------------------

    def architecture(self) -> Dict[str, Any]:
        return {
            "name": "Query Normalization and Canonicalization",
            "phase": "11.2",
            "version": ARCHITECTURE_VERSION,

            "scale_target": SCALE_TARGET,
            "google_scale_capability_target": GOOGLE_SCALE_CAPABILITY_TARGET,
            "google_technology_dependency": GOOGLE_TECHNOLOGY_DEPENDENCY,

            "input": (
                "Phase 11.1 query representation or raw query "
                "normalization request"
            ),

            "pipeline": [
                "input_validation",
                "unicode_normalization",
                "whitespace_normalization",
                "token_normalization",
                "url_preservation",
                "email_preservation",
                "number_preservation",
                "phrase_normalization",
                "field_constraint_canonicalization",
                "operator_canonicalization",
                "duplicate_elimination",
                "deterministic_term_ordering",
                "canonical_term_generation",
                "exact_term_generation",
                "canonical_hash_generation",
                "semantic_fingerprint_generation",
                "checkpointing",
            ],

            "outputs": [
                "canonical_query_identity",
                "normalized_query",
                "canonical_tokens",
                "canonical_phrases",
                "unique_terms",
                "field_constraints",
                "canonical_operators",
                "ordered_terms",
                "canonical_terms",
                "exact_terms",
                "canonical_hash",
                "semantic_fingerprint",
            ],

            "downstream": [
                "phase_11_3_query_type_intent_analysis",
                "phase_11_4_term_field_expansion",
                "phase_11_5_distributed_retrieval_planning",
            ],

            "distributed_design": {
                "deterministic": True,
                "stateless_query_transformation": True,
                "single_global_normalizer_required": False,
                "single_global_query_database_required": False,
                "single_global_query_file_required": False,
                "fixed_global_query_capacity": False,
                "horizontal_query_processing": True,
                "replaceable_backend": True,
                "epoch_aware": True,
            },

            "semantic_boundary": {
                "does_not_perform_semantic_expansion": True,
                "does_not_generate_embeddings": True,
                "does_not_retrieve_documents": True,
                "does_not_rank_documents": True,
                "semantic_expansion_phase": "11.4",
                "retrieval_planning_phase": "11.5",
            },

            "protected_components": [
                "website_server.py",
                "search_service/server.py",
            ],
        }


# ---------------------------------------------------------------------------
# Stable aliases
# ---------------------------------------------------------------------------

QueryNormalizationArchitecture = QueryNormalizationCanonicalization
GlobalQueryNormalization = QueryNormalizationCanonicalization
Phase11_2QueryNormalization = QueryNormalizationCanonicalization


__all__ = [
    "ARCHITECTURE_VERSION",
    "SCALE_TARGET",
    "GOOGLE_SCALE_CAPABILITY_TARGET",
    "GOOGLE_TECHNOLOGY_DEPENDENCY",

    "NormalizationState",
    "CanonicalTokenKind",
    "CanonicalField",
    "CanonicalOperator",
    "NormalizationEventType",

    "QueryNormalizationRequest",
    "RawQueryToken",
    "RawQueryPhrase",

    "CanonicalToken",
    "CanonicalPhrase",
    "CanonicalFieldConstraint",
    "CanonicalOperatorToken",

    "QueryCanonicalIdentity",
    "CanonicalQueryRepresentation",
    "QueryNormalizationResult",

    "QueryNormalizationCheckpoint",
    "QueryNormalizationEvent",

    "QueryNormalizationBackend",
    "InMemoryQueryNormalizationMetadata",
    "QueryNormalizationPolicy",

    "QueryNormalizationCanonicalization",
    "QueryNormalizationArchitecture",
    "GlobalQueryNormalization",
    "Phase11_2QueryNormalization",
]
