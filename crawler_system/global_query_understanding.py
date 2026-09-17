"""
OUR SEARCH
Phase 11.1 — Global Query Understanding Architecture

Version:
    global-query-understanding.v1

Purpose:
    Convert a raw human search query into a deterministic, distributed-
    retrieval-friendly canonical representation.

Scale target:
    Billions -> potentially trillions of publicly accessible Web resources.

Design target:
    Google-scale general Web-search capability, without using Google
    technology or infrastructure.

Important boundary:
    This module understands and structures queries.
    It does NOT retrieve documents, rank documents, or modify the
    protected search service / website server.

Architecture:

    RAW QUERY
        |
        v
    INPUT VALIDATION
        |
        v
    SCRIPT / LANGUAGE SIGNALS
        |
        v
    TOKENIZATION
        |
        v
    NORMALIZATION
        |
        v
    TERM + PHRASE EXTRACTION
        |
        v
    QUERY TYPE / INTENT SIGNALS
        |
        v
    ENTITY / FIELD SIGNALS
        |
        v
    TEMPORAL / FRESHNESS SIGNALS
        |
        v
    RETRIEVAL REQUIREMENTS
        |
        v
    CANONICAL QUERY REPRESENTATION
        |
        +----> Phase 11.2 Query Normalization
        +----> Phase 11.3 Intent Analysis
        +----> Phase 11.4 Expansion
        +----> Phase 11.5 Retrieval Planning
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from enum import Enum
import hashlib
import re
import time
import unicodedata
from typing import Any, Dict, Iterable, List, Mapping, Optional, Protocol, Sequence, Tuple


ARCHITECTURE_VERSION = "global-query-understanding.v1"
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


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------

class QueryState(str, Enum):
    RECEIVED = "received"
    VALIDATED = "validated"
    NORMALIZED = "normalized"
    UNDERSTOOD = "understood"
    CANONICALIZED = "canonicalized"
    REJECTED = "rejected"


class QueryType(str, Enum):
    UNKNOWN = "unknown"
    NAVIGATIONAL = "navigational"
    INFORMATIONAL = "informational"
    TRANSACTIONAL = "transactional"
    LOCAL = "local"
    MIXED = "mixed"


class RetrievalIntent(str, Enum):
    EXACT = "exact"
    BROAD = "broad"
    EXPLORATORY = "exploratory"
    FRESH = "fresh"
    HISTORICAL = "historical"
    LOCAL = "local"
    NAVIGATIONAL = "navigational"
    UNKNOWN = "unknown"


class TokenKind(str, Enum):
    TERM = "term"
    NUMBER = "number"
    URL = "url"
    EMAIL = "email"
    PHRASE = "phrase"
    SYMBOL = "symbol"


class FieldTarget(str, Enum):
    ANY = "any"
    TITLE = "title"
    BODY = "body"
    URL = "url"
    DOMAIN = "domain"
    ANCHOR = "anchor"
    METADATA = "metadata"


class FreshnessRequirement(str, Enum):
    NONE = "none"
    NORMAL = "normal"
    RECENT = "recent"
    VERY_RECENT = "very_recent"
    REAL_TIME = "real_time"


class QuerySignalType(str, Enum):
    LANGUAGE = "language"
    SCRIPT = "script"
    TERM = "term"
    PHRASE = "phrase"
    ENTITY = "entity"
    FIELD = "field"
    TIME = "time"
    FRESHNESS = "freshness"
    URL = "url"
    DOMAIN = "domain"
    LOCATION = "location"
    QUERY_TYPE = "query_type"


class UnderstandingEventType(str, Enum):
    QUERY_RECEIVED = "query_received"
    QUERY_VALIDATED = "query_validated"
    QUERY_NORMALIZED = "query_normalized"
    QUERY_TOKENIZED = "query_tokenized"
    QUERY_CLASSIFIED = "query_classified"
    QUERY_CANONICALIZED = "query_canonicalized"
    QUERY_REJECTED = "query_rejected"
    CHECKPOINT_CREATED = "checkpoint_created"


# ---------------------------------------------------------------------------
# Query identity and signals
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class QueryIdentity:
    query_id: str
    raw_query_hash: str
    canonical_query_hash: str
    created_at: float
    architecture_version: str = ARCHITECTURE_VERSION


@dataclass(frozen=True)
class QueryToken:
    position: int
    text: str
    normalized_text: str
    kind: TokenKind
    field: FieldTarget = FieldTarget.ANY


@dataclass(frozen=True)
class QueryPhrase:
    text: str
    normalized_text: str
    start_position: int
    end_position: int


@dataclass(frozen=True)
class QuerySignal:
    signal_id: str
    signal_type: QuerySignalType
    value: str
    confidence: float
    source: str = "deterministic-rule-layer"


@dataclass(frozen=True)
class QueryIntentProfile:
    query_type: QueryType
    retrieval_intent: RetrievalIntent
    freshness_requirement: FreshnessRequirement
    requires_exact_matching: bool
    requires_broad_matching: bool
    requires_local_signals: bool
    requires_url_signals: bool
    confidence: float


@dataclass(frozen=True)
class QueryFieldConstraint:
    field: FieldTarget
    value: str
    normalized_value: str
    operator: str = "contains"


@dataclass(frozen=True)
class QueryRetrievalRequirement:
    required_terms: Tuple[str, ...]
    optional_terms: Tuple[str, ...]
    required_phrases: Tuple[str, ...]
    field_constraints: Tuple[QueryFieldConstraint, ...]
    exact_terms: Tuple[str, ...]
    freshness: FreshnessRequirement
    fanout_allowed: bool
    broad_recall_required: bool


# ---------------------------------------------------------------------------
# Canonical query representation
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CanonicalQuery:
    identity: QueryIdentity

    raw_query: str
    normalized_query: str

    language: str
    script: str

    tokens: Tuple[QueryToken, ...]
    phrases: Tuple[QueryPhrase, ...]

    terms: Tuple[str, ...]
    normalized_terms: Tuple[str, ...]

    signals: Tuple[QuerySignal, ...]

    intent: QueryIntentProfile

    field_constraints: Tuple[QueryFieldConstraint, ...]

    retrieval: QueryRetrievalRequirement

    source_query_ids: Tuple[str, ...] = ()

    version: str = ARCHITECTURE_VERSION


@dataclass(frozen=True)
class QueryUnderstandingRequest:
    query: str
    request_id: Optional[str] = None
    language_hint: Optional[str] = None
    region_hint: Optional[str] = None
    client_metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class QueryUnderstandingResult:
    state: QueryState
    canonical_query: Optional[CanonicalQuery]
    errors: Tuple[str, ...] = ()


# ---------------------------------------------------------------------------
# Checkpoint / events
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class QueryUnderstandingCheckpoint:
    checkpoint_id: str
    epoch: int
    created_at: float
    processed_queries: int
    architecture_version: str = ARCHITECTURE_VERSION


@dataclass(frozen=True)
class QueryUnderstandingEvent:
    event_id: str
    event_type: UnderstandingEventType
    query_id: Optional[str]
    epoch: int
    created_at: float
    payload: Mapping[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Backend contract
# ---------------------------------------------------------------------------

class GlobalQueryUnderstandingBackend(Protocol):
    """
    Replaceable persistence/event backend.

    The query understanding layer must not depend on one local database,
    one machine, or one global file.
    """

    def record_event(self, event: QueryUnderstandingEvent) -> None:
        ...

    def save_checkpoint(self, checkpoint: QueryUnderstandingCheckpoint) -> None:
        ...

    def get_epoch(self) -> int:
        ...

    def advance_epoch(self) -> int:
        ...


# ---------------------------------------------------------------------------
# Reference backend
# ---------------------------------------------------------------------------

class InMemoryGlobalQueryUnderstandingMetadata:
    """
    Reference metadata backend only.

    This is intentionally not the global production storage system.
    The production implementation can be backed by the distributed
    infrastructure created in Phase 9 and Phase 10.
    """

    def __init__(self) -> None:
        self.events: List[QueryUnderstandingEvent] = []
        self.checkpoints: List[QueryUnderstandingCheckpoint] = []
        self._epoch = 0

    def record_event(self, event: QueryUnderstandingEvent) -> None:
        self.events.append(event)

    def save_checkpoint(self, checkpoint: QueryUnderstandingCheckpoint) -> None:
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
class GlobalQueryUnderstandingPolicy:
    """
    Policy controls for the query-understanding front door.

    These are architecture defaults, not claims about Google's internals.
    """

    max_query_length: int = 32_768
    max_token_count: int = 4_096
    max_phrase_count: int = 512
    max_signal_count: int = 4_096

    min_confidence: float = 0.0

    enable_unicode_normalization: bool = True
    enable_whitespace_normalization: bool = True
    enable_phrase_detection: bool = True
    enable_url_detection: bool = True
    enable_email_detection: bool = True
    enable_field_detection: bool = True
    enable_basic_intent_detection: bool = True
    enable_freshness_detection: bool = True

    checkpoint_interval_epochs: int = 1


# ---------------------------------------------------------------------------
# Global Query Understanding
# ---------------------------------------------------------------------------

class GlobalQueryUnderstanding:
    """
    Phase 11.1 control-plane component.

    Responsibilities:
      * accept raw search queries
      * validate them
      * normalize Unicode and whitespace
      * detect obvious URLs/emails
      * tokenize without assuming one language
      * preserve quoted phrases
      * create deterministic query signals
      * create basic intent/freshness profiles
      * produce a stable canonical query contract
      * emit architecture events
      * create checkpoints

    Non-responsibilities:
      * document retrieval
      * ranking
      * final result ordering
      * document storage
      * index mutation
      * website serving
      * LLM answer generation
    """

    URL_RE = re.compile(
        r"^(?:https?://|www\.)[^\s]+$",
        re.IGNORECASE,
    )

    EMAIL_RE = re.compile(
        r"^[^@\s]+@[^@\s]+\.[^@\s]+$",
        re.IGNORECASE,
    )

    FIELD_RE = re.compile(
        r"^(title|url|domain|site|inurl|intitle):(.+)$",
        re.IGNORECASE,
    )

    QUOTED_PHRASE_RE = re.compile(r'"([^"]+)"')

    RECENT_PATTERNS = (
        "latest",
        "today",
        "now",
        "current",
        "recent",
        "recently",
        "this week",
        "this month",
        "2026",
    )

    HISTORICAL_PATTERNS = (
        "history",
        "historical",
        "ancient",
        "old",
        "past",
        "in 19",
        "in 20",
    )

    LOCAL_PATTERNS = (
        "near me",
        "nearby",
        "near",
        "local",
        "in my area",
    )

    NAVIGATIONAL_PREFIXES = (
        "www.",
        "http://",
        "https://",
    )

    def __init__(
        self,
        backend: Optional[GlobalQueryUnderstandingBackend] = None,
        policy: Optional[GlobalQueryUnderstandingPolicy] = None,
    ) -> None:
        self.backend = backend or InMemoryGlobalQueryUnderstandingMetadata()
        self.policy = policy or GlobalQueryUnderstandingPolicy()
        self.processed_queries = 0

    # ------------------------------------------------------------------
    # Public lifecycle
    # ------------------------------------------------------------------

    @property
    def epoch(self) -> int:
        return self.backend.get_epoch()

    def advance_epoch(self) -> int:
        return self.backend.advance_epoch()

    def understand(
        self,
        request: QueryUnderstandingRequest | str,
    ) -> QueryUnderstandingResult:
        if isinstance(request, str):
            request = QueryUnderstandingRequest(query=request)

        query_id = request.request_id or _new_id("query")

        self._emit(
            UnderstandingEventType.QUERY_RECEIVED,
            query_id,
            {"query_length": len(request.query)},
        )

        errors = self._validate(request.query)

        if errors:
            self._emit(
                UnderstandingEventType.QUERY_REJECTED,
                query_id,
                {"errors": errors},
            )
            return QueryUnderstandingResult(
                state=QueryState.REJECTED,
                canonical_query=None,
                errors=tuple(errors),
            )

        self._emit(
            UnderstandingEventType.QUERY_VALIDATED,
            query_id,
            {},
        )

        normalized = self.normalize_query(request.query)

        self._emit(
            UnderstandingEventType.QUERY_NORMALIZED,
            query_id,
            {"normalized_query_hash": _hash(normalized)},
        )

        language = self.detect_language(normalized, request.language_hint)
        script = self.detect_script(normalized)

        tokens, phrases = self.tokenize(normalized)

        self._emit(
            UnderstandingEventType.QUERY_TOKENIZED,
            query_id,
            {
                "token_count": len(tokens),
                "phrase_count": len(phrases),
            },
        )

        signals = self.extract_signals(
            normalized_query=normalized,
            tokens=tokens,
            phrases=phrases,
            language=language,
            script=script,
            region_hint=request.region_hint,
        )

        intent = self.classify_intent(
            normalized_query=normalized,
            tokens=tokens,
            signals=signals,
        )

        self._emit(
            UnderstandingEventType.QUERY_CLASSIFIED,
            query_id,
            {
                "query_type": intent.query_type.value,
                "retrieval_intent": intent.retrieval_intent.value,
                "freshness": intent.freshness_requirement.value,
            },
        )

        constraints = self.extract_field_constraints(
            normalized,
            tokens,
        )

        retrieval = self.build_retrieval_requirements(
            tokens=tokens,
            phrases=phrases,
            intent=intent,
            constraints=constraints,
        )

        canonical_hash = self._canonical_hash(
            normalized,
            language,
            script,
            tokens,
            phrases,
            intent,
            constraints,
        )

        identity = QueryIdentity(
            query_id=query_id,
            raw_query_hash=_hash(request.query),
            canonical_query_hash=canonical_hash,
            created_at=_now(),
        )

        canonical = CanonicalQuery(
            identity=identity,
            raw_query=request.query,
            normalized_query=normalized,
            language=language,
            script=script,
            tokens=tuple(tokens),
            phrases=tuple(phrases),
            terms=tuple(token.text for token in tokens),
            normalized_terms=tuple(
                token.normalized_text
                for token in tokens
                if token.normalized_text
            ),
            signals=tuple(signals[: self.policy.max_signal_count]),
            intent=intent,
            field_constraints=tuple(constraints),
            retrieval=retrieval,
        )

        self.processed_queries += 1

        self._emit(
            UnderstandingEventType.QUERY_CANONICALIZED,
            query_id,
            {
                "canonical_query_hash": canonical_hash,
                "processed_queries": self.processed_queries,
            },
        )

        self._maybe_checkpoint()

        return QueryUnderstandingResult(
            state=QueryState.CANONICALIZED,
            canonical_query=canonical,
        )

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate(self, query: str) -> List[str]:
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
    # Normalization
    # ------------------------------------------------------------------

    def normalize_query(self, query: str) -> str:
        value = query

        if self.policy.enable_unicode_normalization:
            value = unicodedata.normalize("NFKC", value)

        if self.policy.enable_whitespace_normalization:
            value = re.sub(r"\s+", " ", value).strip()

        return value

    # ------------------------------------------------------------------
    # Language / script
    # ------------------------------------------------------------------

    def detect_language(
        self,
        query: str,
        language_hint: Optional[str] = None,
    ) -> str:
        if language_hint:
            return language_hint.lower()

        # 11.1 deliberately uses a conservative signal rather than
        # pretending that this small deterministic layer is a complete
        # multilingual language-identification model.
        #
        # Later versions can plug in distributed language identification.
        return "und"

    def detect_script(self, query: str) -> str:
        counts: Dict[str, int] = {}

        for char in query:
            if not char.isalpha():
                continue

            name = unicodedata.name(char, "")
            script = "Latin"

            if "CYRILLIC" in name:
                script = "Cyrillic"
            elif "GREEK" in name:
                script = "Greek"
            elif "DEVANAGARI" in name:
                script = "Devanagari"
            elif "BENGALI" in name:
                script = "Bengali"
            elif "ARABIC" in name:
                script = "Arabic"
            elif "HEBREW" in name:
                script = "Hebrew"
            elif "HIRAGANA" in name or "KATAKANA" in name:
                script = "Japanese"
            elif "HANGUL" in name:
                script = "Korean"
            elif "CJK" in name or "IDEOGRAPH" in name:
                script = "Han"

            counts[script] = counts.get(script, 0) + 1

        if not counts:
            return "Unknown"

        return max(counts, key=counts.get)

    # ------------------------------------------------------------------
    # Tokenization
    # ------------------------------------------------------------------

    def tokenize(
        self,
        query: str,
    ) -> Tuple[List[QueryToken], List[QueryPhrase]]:
        phrases: List[QueryPhrase] = []

        if self.policy.enable_phrase_detection:
            for match in self.QUOTED_PHRASE_RE.finditer(query):
                phrase_text = match.group(1).strip()

                if phrase_text:
                    phrases.append(
                        QueryPhrase(
                            text=phrase_text,
                            normalized_text=self.normalize_query(phrase_text).lower(),
                            start_position=match.start(),
                            end_position=match.end(),
                        )
                    )

        # Preserve URLs/emails as units.
        raw_parts = re.findall(
            r'https?://[^\s]+|www\.[^\s]+|[^\s]+',
            query,
        )

        tokens: List[QueryToken] = []

        for position, part in enumerate(raw_parts):
            cleaned = part.strip(".,;!?()[]{}")

            if not cleaned:
                continue

            kind = TokenKind.TERM

            if self.policy.enable_url_detection and self.URL_RE.match(cleaned):
                kind = TokenKind.URL
            elif self.policy.enable_email_detection and self.EMAIL_RE.match(cleaned):
                kind = TokenKind.EMAIL
            elif cleaned.isdigit():
                kind = TokenKind.NUMBER
            elif any(not ch.isalnum() for ch in cleaned):
                if len(cleaned) <= 3:
                    kind = TokenKind.SYMBOL

            normalized = self._normalize_token(cleaned)

            tokens.append(
                QueryToken(
                    position=position,
                    text=cleaned,
                    normalized_text=normalized,
                    kind=kind,
                )
            )

            if len(tokens) >= self.policy.max_token_count:
                break

        return tokens, phrases[: self.policy.max_phrase_count]

    def _normalize_token(self, token: str) -> str:
        token = unicodedata.normalize("NFKC", token)
        token = token.strip()

        if not self.URL_RE.match(token) and not self.EMAIL_RE.match(token):
            token = token.casefold()

        return token

    # ------------------------------------------------------------------
    # Signal extraction
    # ------------------------------------------------------------------

    def extract_signals(
        self,
        normalized_query: str,
        tokens: Sequence[QueryToken],
        phrases: Sequence[QueryPhrase],
        language: str,
        script: str,
        region_hint: Optional[str],
    ) -> List[QuerySignal]:
        signals: List[QuerySignal] = []

        signals.append(
            QuerySignal(
                signal_id=_new_id("signal"),
                signal_type=QuerySignalType.LANGUAGE,
                value=language,
                confidence=1.0 if language != "und" else 0.0,
            )
        )

        signals.append(
            QuerySignal(
                signal_id=_new_id("signal"),
                signal_type=QuerySignalType.SCRIPT,
                value=script,
                confidence=1.0 if script != "Unknown" else 0.0,
            )
        )

        for token in tokens:
            signals.append(
                QuerySignal(
                    signal_id=_new_id("signal"),
                    signal_type=(
                        QuerySignalType.URL
                        if token.kind == TokenKind.URL
                        else QuerySignalType.TERM
                    ),
                    value=token.normalized_text,
                    confidence=1.0,
                )
            )

        for phrase in phrases:
            signals.append(
                QuerySignal(
                    signal_id=_new_id("signal"),
                    signal_type=QuerySignalType.PHRASE,
                    value=phrase.normalized_text,
                    confidence=1.0,
                )
            )

        if region_hint:
            signals.append(
                QuerySignal(
                    signal_id=_new_id("signal"),
                    signal_type=QuerySignalType.LOCATION,
                    value=region_hint,
                    confidence=1.0,
                    source="request-region-hint",
                )
            )

        if self._looks_like_url(normalized_query):
            signals.append(
                QuerySignal(
                    signal_id=_new_id("signal"),
                    signal_type=QuerySignalType.URL,
                    value=normalized_query,
                    confidence=1.0,
                )
            )

        return signals

    # ------------------------------------------------------------------
    # Intent
    # ------------------------------------------------------------------

    def classify_intent(
        self,
        normalized_query: str,
        tokens: Sequence[QueryToken],
        signals: Sequence[QuerySignal],
    ) -> QueryIntentProfile:
        lowered = normalized_query.casefold()

        has_url = any(
            token.kind == TokenKind.URL
            for token in tokens
        )

        has_local = any(
            pattern in lowered
            for pattern in self.LOCAL_PATTERNS
        )

        has_recent = any(
            pattern in lowered
            for pattern in self.RECENT_PATTERNS
        )

        has_historical = any(
            pattern in lowered
            for pattern in self.HISTORICAL_PATTERNS
        )

        if has_url:
            query_type = QueryType.NAVIGATIONAL
            retrieval_intent = RetrievalIntent.NAVIGATIONAL
        elif has_local:
            query_type = QueryType.LOCAL
            retrieval_intent = RetrievalIntent.LOCAL
        elif has_recent:
            query_type = QueryType.INFORMATIONAL
            retrieval_intent = RetrievalIntent.FRESH
        elif has_historical:
            query_type = QueryType.INFORMATIONAL
            retrieval_intent = RetrievalIntent.HISTORICAL
        elif len(tokens) <= 2:
            query_type = QueryType.INFORMATIONAL
            retrieval_intent = RetrievalIntent.BROAD
        else:
            query_type = QueryType.INFORMATIONAL
            retrieval_intent = RetrievalIntent.EXPLORATORY

        freshness = FreshnessRequirement.NORMAL

        if has_recent:
            freshness = FreshnessRequirement.RECENT

        if "right now" in lowered or "live" in lowered:
            freshness = FreshnessRequirement.REAL_TIME

        exact = (
            has_url
            or bool(self.QUOTED_PHRASE_RE.search(normalized_query))
        )

        broad = not exact

        confidence = 0.75

        return QueryIntentProfile(
            query_type=query_type,
            retrieval_intent=retrieval_intent,
            freshness_requirement=freshness,
            requires_exact_matching=exact,
            requires_broad_matching=broad,
            requires_local_signals=has_local,
            requires_url_signals=has_url,
            confidence=confidence,
        )

    # ------------------------------------------------------------------
    # Field constraints
    # ------------------------------------------------------------------

    def extract_field_constraints(
        self,
        normalized_query: str,
        tokens: Sequence[QueryToken],
    ) -> List[QueryFieldConstraint]:
        constraints: List[QueryFieldConstraint] = []

        for token in tokens:
            match = self.FIELD_RE.match(token.text)

            if not match:
                continue

            raw_field = match.group(1).casefold()
            value = match.group(2).strip()

            field_map = {
                "title": FieldTarget.TITLE,
                "intitle": FieldTarget.TITLE,
                "url": FieldTarget.URL,
                "inurl": FieldTarget.URL,
                "domain": FieldTarget.DOMAIN,
                "site": FieldTarget.DOMAIN,
            }

            target = field_map.get(raw_field)

            if target is None:
                continue

            constraints.append(
                QueryFieldConstraint(
                    field=target,
                    value=value,
                    normalized_value=self._normalize_token(value),
                )
            )

        return constraints

    # ------------------------------------------------------------------
    # Retrieval contract
    # ------------------------------------------------------------------

    def build_retrieval_requirements(
        self,
        tokens: Sequence[QueryToken],
        phrases: Sequence[QueryPhrase],
        intent: QueryIntentProfile,
        constraints: Sequence[QueryFieldConstraint],
    ) -> QueryRetrievalRequirement:
        normalized_terms = tuple(
            token.normalized_text
            for token in tokens
            if token.normalized_text
            and token.kind not in {
                TokenKind.SYMBOL,
                TokenKind.URL,
                TokenKind.EMAIL,
            }
        )

        exact_terms = tuple(
            token.normalized_text
            for token in tokens
            if token.normalized_text
            and token.kind in {
                TokenKind.URL,
                TokenKind.EMAIL,
            }
        )

        phrase_terms = tuple(
            phrase.normalized_text
            for phrase in phrases
        )

        if intent.requires_exact_matching:
            required_terms = normalized_terms
            optional_terms: Tuple[str, ...] = ()
        else:
            required_terms = ()
            optional_terms = normalized_terms

        return QueryRetrievalRequirement(
            required_terms=required_terms,
            optional_terms=optional_terms,
            required_phrases=phrase_terms,
            field_constraints=tuple(constraints),
            exact_terms=exact_terms,
            freshness=intent.freshness_requirement,
            fanout_allowed=True,
            broad_recall_required=intent.requires_broad_matching,
        )

    # ------------------------------------------------------------------
    # Canonical identity
    # ------------------------------------------------------------------

    def _canonical_hash(
        self,
        normalized: str,
        language: str,
        script: str,
        tokens: Sequence[QueryToken],
        phrases: Sequence[QueryPhrase],
        intent: QueryIntentProfile,
        constraints: Sequence[QueryFieldConstraint],
    ) -> str:
        payload = {
            "normalized": normalized,
            "language": language,
            "script": script,
            "tokens": [
                {
                    "text": token.text,
                    "normalized": token.normalized_text,
                    "kind": token.kind.value,
                }
                for token in tokens
            ],
            "phrases": [
                phrase.normalized_text
                for phrase in phrases
            ],
            "query_type": intent.query_type.value,
            "retrieval_intent": intent.retrieval_intent.value,
            "freshness": intent.freshness_requirement.value,
            "constraints": [
                {
                    "field": constraint.field.value,
                    "value": constraint.normalized_value,
                }
                for constraint in constraints
            ],
        }

        canonical = repr(payload)
        return _hash(canonical)

    # ------------------------------------------------------------------
    # Checkpointing
    # ------------------------------------------------------------------

    def _maybe_checkpoint(self) -> None:
        interval = self.policy.checkpoint_interval_epochs

        if interval <= 0:
            return

        if self.processed_queries == 0:
            return

        if self.processed_queries % interval != 0:
            return

        checkpoint = QueryUnderstandingCheckpoint(
            checkpoint_id=_new_id("query-checkpoint"),
            epoch=self.epoch,
            created_at=_now(),
            processed_queries=self.processed_queries,
        )

        self.backend.save_checkpoint(checkpoint)

        self._emit(
            UnderstandingEventType.CHECKPOINT_CREATED,
            None,
            {
                "checkpoint_id": checkpoint.checkpoint_id,
                "processed_queries": self.processed_queries,
            },
        )

    # ------------------------------------------------------------------
    # Event emission
    # ------------------------------------------------------------------

    def _emit(
        self,
        event_type: UnderstandingEventType,
        query_id: Optional[str],
        payload: Mapping[str, Any],
    ) -> None:
        event = QueryUnderstandingEvent(
            event_id=_new_id("query-event"),
            event_type=event_type,
            query_id=query_id,
            epoch=self.epoch,
            created_at=_now(),
            payload=dict(payload),
        )

        self.backend.record_event(event)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _looks_like_url(self, query: str) -> bool:
        return bool(self.URL_RE.match(query))

    def architecture(self) -> Dict[str, Any]:
        return {
            "name": "Global Query Understanding",
            "version": ARCHITECTURE_VERSION,
            "phase": "11.1",
            "scale_target": SCALE_TARGET,
            "google_scale_capability_target": GOOGLE_SCALE_CAPABILITY_TARGET,
            "google_technology_dependency": GOOGLE_TECHNOLOGY_DEPENDENCY,

            "input": "raw human search query",

            "pipeline": [
                "input_validation",
                "unicode_and_whitespace_normalization",
                "language_signal_detection",
                "script_detection",
                "tokenization",
                "phrase_extraction",
                "query_signal_extraction",
                "intent_classification",
                "field_constraint_extraction",
                "freshness_requirement_detection",
                "canonical_query_generation",
            ],

            "outputs": [
                "stable query identity",
                "normalized query",
                "language signal",
                "script signal",
                "tokens",
                "phrases",
                "query signals",
                "intent profile",
                "field constraints",
                "retrieval requirements",
                "canonical query hash",
            ],

            "downstream": [
                "phase_11_2_query_normalization",
                "phase_11_3_intent_analysis",
                "phase_11_4_query_expansion",
                "phase_11_5_distributed_retrieval_planning",
                "phase_11_6_candidate_retrieval",
            ],

            "distributed_design": {
                "single_global_query_processor_required": False,
                "single_global_query_database_required": False,
                "single_global_query_state_file_required": False,
                "fixed_global_query_limit": False,
                "horizontal_query_processing": True,
                "replaceable_backend": True,
                "epoch_aware": True,
            },

            "protected_components": [
                "website_server.py",
                "search_service/server.py",
            ],
        }


# ---------------------------------------------------------------------------
# Stable aliases
# ---------------------------------------------------------------------------

GlobalQueryUnderstandingArchitecture = GlobalQueryUnderstanding
QueryUnderstandingArchitecture = GlobalQueryUnderstanding
Phase11_1GlobalQueryUnderstanding = GlobalQueryUnderstanding


__all__ = [
    "ARCHITECTURE_VERSION",
    "SCALE_TARGET",
    "GOOGLE_SCALE_CAPABILITY_TARGET",
    "GOOGLE_TECHNOLOGY_DEPENDENCY",

    "QueryState",
    "QueryType",
    "RetrievalIntent",
    "TokenKind",
    "FieldTarget",
    "FreshnessRequirement",
    "QuerySignalType",
    "UnderstandingEventType",

    "QueryIdentity",
    "QueryToken",
    "QueryPhrase",
    "QuerySignal",
    "QueryIntentProfile",
    "QueryFieldConstraint",
    "QueryRetrievalRequirement",
    "CanonicalQuery",
    "QueryUnderstandingRequest",
    "QueryUnderstandingResult",

    "QueryUnderstandingCheckpoint",
    "QueryUnderstandingEvent",

    "GlobalQueryUnderstandingBackend",
    "InMemoryGlobalQueryUnderstandingMetadata",
    "GlobalQueryUnderstandingPolicy",

    "GlobalQueryUnderstanding",
    "GlobalQueryUnderstandingArchitecture",
    "QueryUnderstandingArchitecture",
    "Phase11_1GlobalQueryUnderstanding",
]
