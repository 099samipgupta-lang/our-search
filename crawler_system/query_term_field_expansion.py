"""
OUR SEARCH
Phase 11.4 — Term & Field Expansion Architecture

Architecture version:
    query-term-field-expansion.v1

Purpose:
    Transform the canonical query + intent analysis produced by Phase 11.3
    into a controlled, provenance-aware, field-aware retrieval representation.

Design target:
    Billions -> potentially trillions of publicly accessible Web resources.

Important:
    This module is an expansion architecture/control plane.
    It does not perform retrieval, ranking, crawling, embedding generation,
    final semantic retrieval, or index mutation.

Pipeline:

    11.2 Canonical Query
        ↓
    11.3 Query Type + Intent Analysis
        ↓
    11.4 Term & Field Expansion
        ↓
    Lexical Signal Preservation
        ↓
    Controlled Variant Generation
        ↓
    Field Expansion
        ↓
    Phrase / Entity Expansion
        ↓
    Confidence + Provenance
        ↓
    Fanout Control
        ↓
    Retrieval-Ready Expansion Contract
        ↓
    11.5 Distributed Retrieval Planning
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from hashlib import sha256
from typing import Any, Dict, Iterable, List, Mapping, Optional, Protocol, Sequence, Tuple
import json
import time
import uuid


ARCHITECTURE_VERSION = "query-term-field-expansion.v1"

SCALE_TARGET = "billions_to_trillions_of_public_web_resources"
GOOGLE_SCALE_CAPABILITY_TARGET = True
GOOGLE_TECHNOLOGY_DEPENDENCY = False


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def _hash(value: Any) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )
    return sha256(payload.encode("utf-8")).hexdigest()


def _confidence_level(value: float) -> "ExpansionConfidenceLevel":
    if value >= 0.90:
        return ExpansionConfidenceLevel.VERY_HIGH
    if value >= 0.75:
        return ExpansionConfidenceLevel.HIGH
    if value >= 0.50:
        return ExpansionConfidenceLevel.MEDIUM
    if value >= 0.25:
        return ExpansionConfidenceLevel.LOW
    return ExpansionConfidenceLevel.VERY_LOW


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class ExpansionState(str, Enum):
    RECEIVED = "received"
    VALIDATING = "validating"
    SIGNAL_EXTRACTION = "signal_extraction"
    TERM_EXPANSION = "term_expansion"
    FIELD_EXPANSION = "field_expansion"
    PHRASE_EXPANSION = "phrase_expansion"
    ENTITY_EXPANSION = "entity_expansion"
    FANOUT_CONTROL = "fanout_control"
    CONFIDENCE_CALCULATION = "confidence_calculation"
    CONTRACT_BUILD = "contract_build"
    COMPLETED = "completed"
    REJECTED = "rejected"


class ExpansionKind(str, Enum):
    EXACT = "exact"
    LEXICAL_VARIANT = "lexical_variant"
    SPELLING_VARIANT = "spelling_variant"
    MORPHOLOGICAL_VARIANT = "morphological_variant"
    SYNONYM = "synonym"
    ACRONYM = "acronym"
    ABBREVIATION = "abbreviation"
    PHRASE_VARIANT = "phrase_variant"
    ENTITY_VARIANT = "entity_variant"
    FIELD_VARIANT = "field_variant"
    URL_VARIANT = "url_variant"
    NUMBER_VARIANT = "number_variant"
    TEMPORAL_VARIANT = "temporal_variant"


class ExpansionSource(str, Enum):
    ORIGINAL_QUERY = "original_query"
    CANONICAL_TOKEN = "canonical_token"
    LEXICAL_RULE = "lexical_rule"
    MORPHOLOGICAL_RULE = "morphological_rule"
    SYNONYM_RESOURCE = "synonym_resource"
    ENTITY_RESOURCE = "entity_resource"
    FIELD_RULE = "field_rule"
    PHRASE_RULE = "phrase_rule"
    URL_RULE = "url_rule"
    TEMPORAL_RULE = "temporal_rule"
    BACKEND = "backend"


class ExpansionConfidenceLevel(str, Enum):
    VERY_LOW = "very_low"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    VERY_HIGH = "very_high"


class QueryField(str, Enum):
    ALL = "all"
    TITLE = "title"
    HEADING = "heading"
    BODY = "body"
    URL = "url"
    ANCHOR = "anchor"
    DOMAIN = "domain"
    PATH = "path"
    AUTHOR = "author"
    ORGANIZATION = "organization"
    PRODUCT = "product"
    PERSON = "person"
    LOCATION = "location"
    DATE = "date"
    DOCUMENT = "document"
    MEDIA = "media"


class TermGroupRole(str, Enum):
    REQUIRED = "required"
    PREFERRED = "preferred"
    OPTIONAL = "optional"
    EXACT_PRESERVATION = "exact_preservation"
    NEGATIVE = "negative"


class ExpansionRelation(str, Enum):
    SAME = "same"
    VARIANT_OF = "variant_of"
    SYNONYM_OF = "synonym_of"
    RELATED_TO = "related_to"
    DERIVED_FROM = "derived_from"
    ENTITY_ALIAS_OF = "entity_alias_of"
    FIELD_EQUIVALENT_OF = "field_equivalent_of"
    PHRASE_EQUIVALENT_OF = "phrase_equivalent_of"


class FanoutState(str, Enum):
    WITHIN_BUDGET = "within_budget"
    REDUCED = "reduced"
    CAPPED = "capped"
    REJECTED = "rejected"


class ExpansionEventType(str, Enum):
    EXPANSION_RECEIVED = "expansion_received"
    EXPANSION_STARTED = "expansion_started"
    SIGNALS_EXTRACTED = "signals_extracted"
    EXACT_TERMS_PRESERVED = "exact_terms_preserved"
    TERMS_EXPANDED = "terms_expanded"
    FIELDS_EXPANDED = "fields_expanded"
    PHRASES_EXPANDED = "phrases_expanded"
    ENTITIES_EXPANDED = "entities_expanded"
    FANOUT_CONTROLLED = "fanout_controlled"
    CONFIDENCE_CALCULATED = "confidence_calculated"
    CONTRACT_BUILT = "contract_built"
    EXPANSION_COMPLETED = "expansion_completed"
    EXPANSION_REJECTED = "expansion_rejected"
    CHECKPOINT_CREATED = "checkpoint_created"


# ---------------------------------------------------------------------------
# Core identities
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ExpansionIdentity:
    expansion_id: str
    query_id: str
    canonical_query_hash: str
    architecture_version: str = ARCHITECTURE_VERSION


@dataclass(frozen=True)
class ExpansionTerm:
    term_id: str
    text: str
    normalized_text: str

    kind: ExpansionKind
    source: ExpansionSource
    relation: ExpansionRelation

    parent_term_id: Optional[str] = None

    field: QueryField = QueryField.ALL
    role: TermGroupRole = TermGroupRole.OPTIONAL

    confidence: float = 1.0
    confidence_level: ExpansionConfidenceLevel = ExpansionConfidenceLevel.VERY_HIGH

    exact_preserved: bool = False

    provenance: Mapping[str, Any] = field(default_factory=dict)

    def fingerprint(self) -> str:
        return _hash(
            {
                "text": self.text,
                "normalized_text": self.normalized_text,
                "kind": self.kind.value,
                "source": self.source.value,
                "relation": self.relation.value,
                "parent_term_id": self.parent_term_id,
                "field": self.field.value,
                "role": self.role.value,
                "exact_preserved": self.exact_preserved,
            }
        )


@dataclass(frozen=True)
class ExpansionPhrase:
    phrase_id: str
    text: str
    normalized_text: str

    source: ExpansionSource
    relation: ExpansionRelation

    parent_phrase_id: Optional[str] = None

    confidence: float = 1.0
    confidence_level: ExpansionConfidenceLevel = ExpansionConfidenceLevel.VERY_HIGH

    exact_preserved: bool = False

    provenance: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ExpansionFieldTarget:
    field: QueryField
    source_term_id: Optional[str]
    source_phrase_id: Optional[str]

    weight: float
    confidence: float

    required: bool = False

    provenance: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TermGroup:
    group_id: str
    role: TermGroupRole

    original_text: str

    terms: Tuple[ExpansionTerm, ...]

    minimum_match: int = 1

    exact_preservation: bool = True

    group_confidence: float = 1.0

    provenance: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ExpansionSignalProfile:
    token_count: int
    phrase_count: int
    field_constraint_count: int
    operator_count: int

    has_url: bool
    has_email: bool
    has_number: bool
    has_temporal_signal: bool
    has_local_signal: bool
    has_entity_signal: bool
    has_media_signal: bool
    has_comparison_signal: bool

    exact_tokens: Tuple[str, ...]
    constrained_fields: Tuple[str, ...]


# ---------------------------------------------------------------------------
# Expansion result
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ExpansionBudget:
    max_total_terms: int
    max_terms_per_source_term: int
    max_phrase_variants: int
    max_entity_variants: int
    max_field_targets: int

    max_total_groups: int
    max_total_characters: int


@dataclass(frozen=True)
class FanoutDecision:
    state: FanoutState

    original_candidate_count: int
    accepted_candidate_count: int
    rejected_candidate_count: int

    budget: ExpansionBudget

    reasons: Tuple[str, ...]

    fingerprint: str


@dataclass(frozen=True)
class ExpansionConfidence:
    overall_score: float
    level: ExpansionConfidenceLevel

    exact_term_coverage: float
    lexical_confidence: float
    phrase_confidence: float
    entity_confidence: float
    field_confidence: float

    uncertainty_reasons: Tuple[str, ...]


@dataclass(frozen=True)
class RetrievalExpansionContract:
    contract_id: str
    expansion_id: str

    query_id: str

    exact_terms: Tuple[ExpansionTerm, ...]
    term_groups: Tuple[TermGroup, ...]
    phrases: Tuple[ExpansionPhrase, ...]
    field_targets: Tuple[ExpansionFieldTarget, ...]

    fanout: FanoutDecision
    confidence: ExpansionConfidence

    retrieval_hints: Mapping[str, Any]

    created_at: str

    contract_hash: str


@dataclass(frozen=True)
class QueryTermFieldExpansion:
    identity: ExpansionIdentity

    state: ExpansionState

    signals: ExpansionSignalProfile

    exact_terms: Tuple[ExpansionTerm, ...]
    expanded_terms: Tuple[ExpansionTerm, ...]

    phrases: Tuple[ExpansionPhrase, ...]

    field_targets: Tuple[ExpansionFieldTarget, ...]

    term_groups: Tuple[TermGroup, ...]

    fanout: FanoutDecision
    confidence: ExpansionConfidence

    contract: RetrievalExpansionContract

    created_at: str
    completed_at: str

    analysis_hash: str


@dataclass(frozen=True)
class ExpansionCheckpoint:
    checkpoint_id: str
    expansion_id: str

    state: ExpansionState

    checkpoint_epoch: int

    expansion_hash: str

    term_count: int
    phrase_count: int
    field_target_count: int

    created_at: str


@dataclass(frozen=True)
class ExpansionEvent:
    event_id: str
    expansion_id: str

    event_type: ExpansionEventType

    state: ExpansionState

    timestamp: str

    payload: Mapping[str, Any]


@dataclass(frozen=True)
class ExpansionRequest:
    canonical_query: Mapping[str, Any]
    intent_analysis: Mapping[str, Any]

    requested_fields: Tuple[QueryField, ...] = ()

    budget: Optional[ExpansionBudget] = None

    metadata: Mapping[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Backend
# ---------------------------------------------------------------------------

class QueryTermFieldExpansionBackend(Protocol):
    def lookup_term_variants(
        self,
        normalized_term: str,
        *,
        limit: int,
    ) -> Sequence[Mapping[str, Any]]:
        ...

    def lookup_phrase_variants(
        self,
        normalized_phrase: str,
        *,
        limit: int,
    ) -> Sequence[Mapping[str, Any]]:
        ...

    def lookup_entity_variants(
        self,
        normalized_text: str,
        *,
        limit: int,
    ) -> Sequence[Mapping[str, Any]]:
        ...

    def lookup_field_targets(
        self,
        normalized_text: str,
        *,
        limit: int,
    ) -> Sequence[Mapping[str, Any]]:
        ...


class InMemoryQueryTermFieldExpansionBackend:
    """
    Reference backend only.

    Production deployments should replace this with partitioned,
    distributed resources. This class intentionally contains only tiny
    deterministic examples and is not presented as the Web-scale resource.
    """

    def __init__(
        self,
        term_variants: Optional[Mapping[str, Sequence[str]]] = None,
        phrase_variants: Optional[Mapping[str, Sequence[str]]] = None,
        entity_variants: Optional[Mapping[str, Sequence[str]]] = None,
        field_targets: Optional[Mapping[str, Sequence[str]]] = None,
    ) -> None:
        self._term_variants = {
            str(k).casefold(): tuple(str(v) for v in values)
            for k, values in (term_variants or {}).items()
        }

        self._phrase_variants = {
            str(k).casefold(): tuple(str(v) for v in values)
            for k, values in (phrase_variants or {}).items()
        }

        self._entity_variants = {
            str(k).casefold(): tuple(str(v) for v in values)
            for k, values in (entity_variants or {}).items()
        }

        self._field_targets = {
            str(k).casefold(): tuple(str(v) for v in values)
            for k, values in (field_targets or {}).items()
        }

    def lookup_term_variants(
        self,
        normalized_term: str,
        *,
        limit: int,
    ) -> Sequence[Mapping[str, Any]]:
        values = self._term_variants.get(normalized_term.casefold(), ())
        return [
            {
                "text": value,
                "confidence": 0.70,
                "source": ExpansionSource.SYNONYM_RESOURCE.value,
            }
            for value in values[:limit]
        ]

    def lookup_phrase_variants(
        self,
        normalized_phrase: str,
        *,
        limit: int,
    ) -> Sequence[Mapping[str, Any]]:
        values = self._phrase_variants.get(normalized_phrase.casefold(), ())
        return [
            {
                "text": value,
                "confidence": 0.68,
                "source": ExpansionSource.PHRASE_RULE.value,
            }
            for value in values[:limit]
        ]

    def lookup_entity_variants(
        self,
        normalized_text: str,
        *,
        limit: int,
    ) -> Sequence[Mapping[str, Any]]:
        values = self._entity_variants.get(normalized_text.casefold(), ())
        return [
            {
                "text": value,
                "confidence": 0.82,
                "source": ExpansionSource.ENTITY_RESOURCE.value,
            }
            for value in values[:limit]
        ]

    def lookup_field_targets(
        self,
        normalized_text: str,
        *,
        limit: int,
    ) -> Sequence[Mapping[str, Any]]:
        values = self._field_targets.get(normalized_text.casefold(), ())
        return [
            {
                "field": value,
                "weight": 0.70,
                "confidence": 0.75,
                "source": ExpansionSource.FIELD_RULE.value,
            }
            for value in values[:limit]
        ]


# ---------------------------------------------------------------------------
# Policy
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class QueryTermFieldExpansionPolicy:
    default_budget: ExpansionBudget = field(
        default_factory=lambda: ExpansionBudget(
            max_total_terms=128,
            max_terms_per_source_term=8,
            max_phrase_variants=32,
            max_entity_variants=32,
            max_field_targets=64,
            max_total_groups=64,
            max_total_characters=16_384,
        )
    )

    minimum_variant_confidence: float = 0.45

    exact_term_weight: float = 1.0
    lexical_variant_weight: float = 0.75
    synonym_weight: float = 0.60
    entity_weight: float = 0.85
    phrase_weight: float = 0.80

    preserve_exact_terms: bool = True

    allow_synonyms: bool = True
    allow_morphological_variants: bool = True
    allow_spelling_variants: bool = True
    allow_entity_variants: bool = True
    allow_phrase_variants: bool = True
    allow_field_expansion: bool = True

    prefer_exact_url_matching: bool = True

    # Expansion must never be allowed to become an uncontrolled
    # combinatorial query generator.
    maximum_combination_depth: int = 2


# ---------------------------------------------------------------------------
# Main architecture
# ---------------------------------------------------------------------------

class QueryTermFieldExpansion:
    """
    Phase 11.4 architecture.

    Responsibilities:
        - preserve exact query terms
        - generate controlled lexical variants
        - expand fields according to query intent
        - preserve phrase structure
        - support entity aliases
        - attach provenance and confidence
        - enforce expansion budgets
        - produce retrieval-ready contracts

    Non-responsibilities:
        - crawling
        - retrieval
        - ranking
        - embedding generation
        - semantic model inference
        - index mutation
        - final result selection
    """

    def __init__(
        self,
        backend: Optional[QueryTermFieldExpansionBackend] = None,
        policy: Optional[QueryTermFieldExpansionPolicy] = None,
    ) -> None:
        self.backend = backend or InMemoryQueryTermFieldExpansionBackend()
        self.policy = policy or QueryTermFieldExpansionPolicy()

        self._checkpoints: Dict[str, ExpansionCheckpoint] = {}
        self._events: List[ExpansionEvent] = []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def expand(self, request: ExpansionRequest) -> QueryTermFieldExpansion:
        started_at = _now()

        canonical = dict(request.canonical_query)
        intent_analysis = dict(request.intent_analysis)

        query_id = self._query_id(canonical)
        canonical_hash = self._canonical_hash(canonical)

        identity = ExpansionIdentity(
            expansion_id=_new_id("expansion"),
            query_id=query_id,
            canonical_query_hash=canonical_hash,
        )

        self._emit(
            identity,
            ExpansionEventType.EXPANSION_RECEIVED,
            ExpansionState.RECEIVED,
            {
                "query_id": query_id,
                "canonical_query_hash": canonical_hash,
            },
        )

        state = ExpansionState.VALIDATING

        if not self._validate(canonical, intent_analysis):
            self._emit(
                identity,
                ExpansionEventType.EXPANSION_REJECTED,
                ExpansionState.REJECTED,
                {"reason": "invalid canonical query or intent analysis"},
            )
            raise ValueError("Invalid canonical query or intent analysis")

        self._emit(
            identity,
            ExpansionEventType.EXPANSION_STARTED,
            state,
            {},
        )

        budget = request.budget or self.policy.default_budget

        signals = self._extract_signals(canonical, intent_analysis)

        self._emit(
            identity,
            ExpansionEventType.SIGNALS_EXTRACTED,
            ExpansionState.SIGNAL_EXTRACTION,
            {
                "token_count": signals.token_count,
                "phrase_count": signals.phrase_count,
                "exact_token_count": len(signals.exact_tokens),
            },
        )

        exact_terms = self._preserve_exact_terms(
            canonical,
            signals,
        )

        self._emit(
            identity,
            ExpansionEventType.EXACT_TERMS_PRESERVED,
            ExpansionState.TERM_EXPANSION,
            {
                "exact_term_count": len(exact_terms),
            },
        )

        expanded_terms = self._expand_terms(
            canonical,
            intent_analysis,
            exact_terms,
            budget,
        )

        self._emit(
            identity,
            ExpansionEventType.TERMS_EXPANDED,
            ExpansionState.TERM_EXPANSION,
            {
                "expanded_term_count": len(expanded_terms),
            },
        )

        phrases = self._expand_phrases(
            canonical,
            intent_analysis,
            budget,
        )

        self._emit(
            identity,
            ExpansionEventType.PHRASES_EXPANDED,
            ExpansionState.PHRASE_EXPANSION,
            {
                "phrase_count": len(phrases),
            },
        )

        entity_terms = self._expand_entities(
            canonical,
            intent_analysis,
            budget,
        )

        self._emit(
            identity,
            ExpansionEventType.ENTITIES_EXPANDED,
            ExpansionState.ENTITY_EXPANSION,
            {
                "entity_variant_count": len(entity_terms),
            },
        )

        all_terms = self._deduplicate_terms(
            list(exact_terms)
            + list(expanded_terms)
            + list(entity_terms)
        )

        field_targets = self._expand_fields(
            canonical,
            intent_analysis,
            all_terms,
            phrases,
            request.requested_fields,
            budget,
        )

        self._emit(
            identity,
            ExpansionEventType.FIELDS_EXPANDED,
            ExpansionState.FIELD_EXPANSION,
            {
                "field_target_count": len(field_targets),
            },
        )

        term_groups = self._build_term_groups(
            canonical,
            all_terms,
            phrases,
            budget,
        )

        fanout = self._control_fanout(
            exact_terms=exact_terms,
            terms=all_terms,
            phrases=phrases,
            field_targets=field_targets,
            groups=term_groups,
            budget=budget,
        )

        self._emit(
            identity,
            ExpansionEventType.FANOUT_CONTROLLED,
            ExpansionState.FANOUT_CONTROL,
            {
                "state": fanout.state.value,
                "accepted": fanout.accepted_candidate_count,
                "rejected": fanout.rejected_candidate_count,
            },
        )

        if fanout.state == FanoutState.REJECTED:
            self._emit(
                identity,
                ExpansionEventType.EXPANSION_REJECTED,
                ExpansionState.REJECTED,
                {"reason": "expansion fanout rejected"},
            )
            raise ValueError("Expansion fanout rejected")

        confidence = self._calculate_confidence(
            exact_terms=exact_terms,
            terms=all_terms,
            phrases=phrases,
            field_targets=field_targets,
            fanout=fanout,
        )

        self._emit(
            identity,
            ExpansionEventType.CONFIDENCE_CALCULATED,
            ExpansionState.CONFIDENCE_CALCULATION,
            {
                "overall_score": confidence.overall_score,
                "level": confidence.level.value,
            },
        )

        retrieval_hints = self._build_retrieval_hints(
            canonical,
            intent_analysis,
            signals,
            exact_terms,
            all_terms,
            phrases,
            field_targets,
        )

        contract = self._build_contract(
            identity=identity,
            exact_terms=exact_terms,
            terms=all_terms,
            phrases=phrases,
            field_targets=field_targets,
            groups=term_groups,
            fanout=fanout,
            confidence=confidence,
            retrieval_hints=retrieval_hints,
        )

        self._emit(
            identity,
            ExpansionEventType.CONTRACT_BUILT,
            ExpansionState.CONTRACT_BUILD,
            {
                "contract_id": contract.contract_id,
                "contract_hash": contract.contract_hash,
            },
        )

        completed_at = _now()

        analysis_hash = _hash(
            {
                "identity": {
                    "expansion_id": identity.expansion_id,
                    "query_id": identity.query_id,
                    "canonical_query_hash": identity.canonical_query_hash,
                },
                "exact_terms": [
                    term.fingerprint()
                    for term in exact_terms
                ],
                "terms": [
                    term.fingerprint()
                    for term in all_terms
                ],
                "phrases": [
                    {
                        "text": phrase.normalized_text,
                        "relation": phrase.relation.value,
                    }
                    for phrase in phrases
                ],
                "fields": [
                    {
                        "field": target.field.value,
                        "source_term_id": target.source_term_id,
                        "source_phrase_id": target.source_phrase_id,
                    }
                    for target in field_targets
                ],
                "fanout": fanout.fingerprint,
                "confidence": confidence.overall_score,
                "contract_hash": contract.contract_hash,
            }
        )

        result = QueryTermFieldExpansion(
            identity=identity,
            state=ExpansionState.COMPLETED,
            signals=signals,
            exact_terms=tuple(exact_terms),
            expanded_terms=tuple(all_terms),
            phrases=tuple(phrases),
            field_targets=tuple(field_targets),
            term_groups=tuple(term_groups),
            fanout=fanout,
            confidence=confidence,
            contract=contract,
            created_at=started_at,
            completed_at=completed_at,
            analysis_hash=analysis_hash,
        )

        self._create_checkpoint(
            result,
            epoch=0,
        )

        self._emit(
            identity,
            ExpansionEventType.EXPANSION_COMPLETED,
            ExpansionState.COMPLETED,
            {
                "analysis_hash": analysis_hash,
                "term_count": len(all_terms),
                "phrase_count": len(phrases),
                "field_target_count": len(field_targets),
            },
        )

        return result

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate(
        self,
        canonical: Mapping[str, Any],
        intent_analysis: Mapping[str, Any],
    ) -> bool:
        if not canonical:
            return False

        if not intent_analysis:
            return False

        query_id = self._query_id(canonical)

        normalized_query = canonical.get("normalized_query")

        if not query_id:
            return False

        if normalized_query is None:
            return False

        return True

    # ------------------------------------------------------------------
    # Canonical query helpers
    # ------------------------------------------------------------------

    def _query_id(self, canonical: Mapping[str, Any]) -> str:
        identity = canonical.get("identity")

        if isinstance(identity, Mapping):
            value = identity.get("query_id")
            if value:
                return str(value)

        value = canonical.get("query_id")

        if value:
            return str(value)

        return _hash(
            {
                "normalized_query": canonical.get("normalized_query"),
                "tokens": canonical.get("tokens", []),
            }
        )

    def _canonical_hash(self, canonical: Mapping[str, Any]) -> str:
        identity = canonical.get("identity")

        if isinstance(identity, Mapping):
            value = identity.get("canonical_hash")
            if value:
                return str(value)

        return _hash(
            {
                "normalized_query": canonical.get("normalized_query"),
                "tokens": canonical.get("tokens", []),
                "phrases": canonical.get("phrases", []),
                "field_constraints": canonical.get(
                    "field_constraints",
                    [],
                ),
                "operators": canonical.get("operators", []),
            }
        )

    # ------------------------------------------------------------------
    # Signal extraction
    # ------------------------------------------------------------------

    def _extract_signals(
        self,
        canonical: Mapping[str, Any],
        intent_analysis: Mapping[str, Any],
    ) -> ExpansionSignalProfile:
        tokens = self._tokens(canonical)
        phrases = self._phrases(canonical)
        field_constraints = canonical.get(
            "field_constraints",
            [],
        ) or []
        operators = canonical.get("operators", []) or []

        token_texts = tuple(
            self._token_text(token)
            for token in tokens
            if self._token_text(token)
        )

        exact_tokens = tuple(
            dict.fromkeys(
                token_texts
            )
        )

        constrained_fields: List[str] = []

        for constraint in field_constraints:
            if isinstance(constraint, Mapping):
                value = constraint.get("field")
                if value:
                    constrained_fields.append(str(value))

        lexical_blob = " ".join(
            token.casefold()
            for token in token_texts
        )

        analysis_blob = json.dumps(
            intent_analysis,
            ensure_ascii=False,
            default=str,
        ).casefold()

        combined = f"{lexical_blob} {analysis_blob}"

        return ExpansionSignalProfile(
            token_count=len(tokens),
            phrase_count=len(phrases),
            field_constraint_count=len(field_constraints),
            operator_count=len(operators),

            has_url=any(
                kind in str(token).casefold()
                for token in tokens
                for kind in ("url",)
            ),

            has_email=any(
                kind in str(token).casefold()
                for token in tokens
                for kind in ("email",)
            ),

            has_number=any(
                kind in str(token).casefold()
                for token in tokens
                for kind in ("number",)
            ),

            has_temporal_signal=any(
                word in combined
                for word in (
                    "today",
                    "yesterday",
                    "tomorrow",
                    "latest",
                    "recent",
                    "news",
                    "current",
                    "202",
                    "time",
                )
            ),

            has_local_signal=any(
                word in combined
                for word in (
                    "near",
                    "nearby",
                    "local",
                    "restaurant",
                    "hospital",
                    "school",
                    "hotel",
                )
            ),

            has_entity_signal=any(
                word in combined
                for word in (
                    "who",
                    "person",
                    "company",
                    "organization",
                    "university",
                    "president",
                    "ceo",
                )
            ),

            has_media_signal=any(
                word in combined
                for word in (
                    "video",
                    "image",
                    "photo",
                    "movie",
                    "song",
                    "audio",
                )
            ),

            has_comparison_signal=any(
                word in combined
                for word in (
                    "vs",
                    "versus",
                    "compare",
                    "comparison",
                    "difference",
                    "better",
                    "best",
                )
            ),

            exact_tokens=exact_tokens,
            constrained_fields=tuple(
                dict.fromkeys(constrained_fields)
            ),
        )

    # ------------------------------------------------------------------
    # Token helpers
    # ------------------------------------------------------------------

    def _tokens(
        self,
        canonical: Mapping[str, Any],
    ) -> Sequence[Any]:
        value = canonical.get("tokens", [])

        if isinstance(value, Sequence) and not isinstance(
            value,
            (str, bytes),
        ):
            return value

        return ()

    def _phrases(
        self,
        canonical: Mapping[str, Any],
    ) -> Sequence[Any]:
        value = canonical.get("phrases", [])

        if isinstance(value, Sequence) and not isinstance(
            value,
            (str, bytes),
        ):
            return value

        return ()

    def _token_text(self, token: Any) -> str:
        if isinstance(token, str):
            return token.strip()

        if isinstance(token, Mapping):
            for key in (
                "normalized",
                "normalized_text",
                "text",
                "value",
                "token",
            ):
                value = token.get(key)
                if value:
                    return str(value).strip()

        return str(token).strip() if token else ""

    def _normalize_text(self, text: str) -> str:
        return " ".join(
            str(text).casefold().split()
        )

    # ------------------------------------------------------------------
    # Exact preservation
    # ------------------------------------------------------------------

    def _preserve_exact_terms(
        self,
        canonical: Mapping[str, Any],
        signals: ExpansionSignalProfile,
    ) -> List[ExpansionTerm]:
        normalized_query = str(
            canonical.get(
                "normalized_query",
                "",
            )
        ).strip()

        terms: List[ExpansionTerm] = []

        for text in signals.exact_tokens:
            normalized = self._normalize_text(text)

            if not normalized:
                continue

            terms.append(
                ExpansionTerm(
                    term_id=_new_id("term"),
                    text=text,
                    normalized_text=normalized,
                    kind=ExpansionKind.EXACT,
                    source=ExpansionSource.ORIGINAL_QUERY,
                    relation=ExpansionRelation.SAME,
                    field=QueryField.ALL,
                    role=TermGroupRole.EXACT_PRESERVATION,
                    confidence=self.policy.exact_term_weight,
                    confidence_level=ExpansionConfidenceLevel.VERY_HIGH,
                    exact_preserved=True,
                    provenance={
                        "stage": "11.4",
                        "reason": "exact_query_term_preservation",
                    },
                )
            )

        if normalized_query and not terms:
            terms.append(
                ExpansionTerm(
                    term_id=_new_id("term"),
                    text=normalized_query,
                    normalized_text=self._normalize_text(
                        normalized_query
                    ),
                    kind=ExpansionKind.EXACT,
                    source=ExpansionSource.ORIGINAL_QUERY,
                    relation=ExpansionRelation.SAME,
                    field=QueryField.ALL,
                    role=TermGroupRole.EXACT_PRESERVATION,
                    confidence=1.0,
                    confidence_level=ExpansionConfidenceLevel.VERY_HIGH,
                    exact_preserved=True,
                    provenance={
                        "stage": "11.4",
                        "reason": "fallback_exact_query_preservation",
                    },
                )
            )

        return self._deduplicate_terms(terms)

    # ------------------------------------------------------------------
    # Term expansion
    # ------------------------------------------------------------------

    def _expand_terms(
        self,
        canonical: Mapping[str, Any],
        intent_analysis: Mapping[str, Any],
        exact_terms: Sequence[ExpansionTerm],
        budget: ExpansionBudget,
    ) -> List[ExpansionTerm]:
        if not self.policy.allow_synonyms:
            return []

        output: List[ExpansionTerm] = []

        for parent in exact_terms:
            if parent.kind != ExpansionKind.EXACT:
                continue

            candidates = self.backend.lookup_term_variants(
                parent.normalized_text,
                limit=budget.max_terms_per_source_term,
            )

            for candidate in candidates:
                if not isinstance(candidate, Mapping):
                    continue

                text = str(
                    candidate.get("text", "")
                ).strip()

                if not text:
                    continue

                confidence = float(
                    candidate.get(
                        "confidence",
                        self.policy.synonym_weight,
                    )
                )

                if confidence < self.policy.minimum_variant_confidence:
                    continue

                source_value = candidate.get(
                    "source",
                    ExpansionSource.BACKEND.value,
                )

                try:
                    source = ExpansionSource(
                        source_value
                    )
                except ValueError:
                    source = ExpansionSource.BACKEND

                output.append(
                    ExpansionTerm(
                        term_id=_new_id("term"),
                        text=text,
                        normalized_text=self._normalize_text(text),
                        kind=ExpansionKind.SYNONYM,
                        source=source,
                        relation=ExpansionRelation.SYNONYM_OF,
                        parent_term_id=parent.term_id,
                        field=parent.field,
                        role=TermGroupRole.OPTIONAL,
                        confidence=confidence,
                        confidence_level=_confidence_level(
                            confidence
                        ),
                        exact_preserved=False,
                        provenance={
                            "stage": "11.4",
                            "parent_fingerprint": parent.fingerprint(),
                        },
                    )
                )

        return self._deduplicate_terms(output)

    # ------------------------------------------------------------------
    # Phrase expansion
    # ------------------------------------------------------------------

    def _expand_phrases(
        self,
        canonical: Mapping[str, Any],
        intent_analysis: Mapping[str, Any],
        budget: ExpansionBudget,
    ) -> List[ExpansionPhrase]:
        if not self.policy.allow_phrase_variants:
            return []

        phrases = self._phrases(canonical)

        output: List[ExpansionPhrase] = []

        for phrase in phrases:
            text = self._token_text(phrase)

            if not text:
                continue

            normalized = self._normalize_text(text)

            output.append(
                ExpansionPhrase(
                    phrase_id=_new_id("phrase"),
                    text=text,
                    normalized_text=normalized,
                    source=ExpansionSource.ORIGINAL_QUERY,
                    relation=ExpansionRelation.SAME,
                    confidence=1.0,
                    confidence_level=ExpansionConfidenceLevel.VERY_HIGH,
                    exact_preserved=True,
                    provenance={
                        "stage": "11.4",
                        "reason": "original_phrase_preservation",
                    },
                )
            )

            candidates = self.backend.lookup_phrase_variants(
                normalized,
                limit=budget.max_phrase_variants,
            )

            for candidate in candidates:
                if not isinstance(candidate, Mapping):
                    continue

                variant_text = str(
                    candidate.get("text", "")
                ).strip()

                if not variant_text:
                    continue

                confidence = float(
                    candidate.get(
                        "confidence",
                        self.policy.phrase_weight,
                    )
                )

                if confidence < self.policy.minimum_variant_confidence:
                    continue

                output.append(
                    ExpansionPhrase(
                        phrase_id=_new_id("phrase"),
                        text=variant_text,
                        normalized_text=self._normalize_text(
                            variant_text
                        ),
                        source=ExpansionSource.PHRASE_RULE,
                        relation=ExpansionRelation.PHRASE_EQUIVALENT_OF,
                        parent_phrase_id=None,
                        confidence=confidence,
                        confidence_level=_confidence_level(
                            confidence
                        ),
                        exact_preserved=False,
                        provenance={
                            "stage": "11.4",
                            "source_phrase": normalized,
                        },
                    )
                )

        return self._deduplicate_phrases(output)

    # ------------------------------------------------------------------
    # Entity expansion
    # ------------------------------------------------------------------

    def _expand_entities(
        self,
        canonical: Mapping[str, Any],
        intent_analysis: Mapping[str, Any],
        budget: ExpansionBudget,
    ) -> List[ExpansionTerm]:
        if not self.policy.allow_entity_variants:
            return []

        intent_blob = json.dumps(
            intent_analysis,
            ensure_ascii=False,
            default=str,
        ).casefold()

        entity_intent = any(
            word in intent_blob
            for word in (
                "entity",
                "organization",
                "person",
                "company",
                "product",
                "service",
                "university",
            )
        )

        if not entity_intent:
            return []

        output: List[ExpansionTerm] = []

        for text in self._candidate_entity_texts(canonical):
            normalized = self._normalize_text(text)

            if not normalized:
                continue

            candidates = self.backend.lookup_entity_variants(
                normalized,
                limit=budget.max_entity_variants,
            )

            for candidate in candidates:
                if not isinstance(candidate, Mapping):
                    continue

                variant_text = str(
                    candidate.get("text", "")
                ).strip()

                if not variant_text:
                    continue

                confidence = float(
                    candidate.get(
                        "confidence",
                        self.policy.entity_weight,
                    )
                )

                if confidence < self.policy.minimum_variant_confidence:
                    continue

                output.append(
                    ExpansionTerm(
                        term_id=_new_id("term"),
                        text=variant_text,
                        normalized_text=self._normalize_text(
                            variant_text
                        ),
                        kind=ExpansionKind.ENTITY_VARIANT,
                        source=ExpansionSource.ENTITY_RESOURCE,
                        relation=ExpansionRelation.ENTITY_ALIAS_OF,
                        field=QueryField.ALL,
                        role=TermGroupRole.PREFERRED,
                        confidence=confidence,
                        confidence_level=_confidence_level(
                            confidence
                        ),
                        exact_preserved=False,
                        provenance={
                            "stage": "11.4",
                            "source_entity": normalized,
                        },
                    )
                )

        return self._deduplicate_terms(output)

    def _candidate_entity_texts(
        self,
        canonical: Mapping[str, Any],
    ) -> Sequence[str]:
        result: List[str] = []

        normalized_query = canonical.get(
            "normalized_query"
        )

        if normalized_query:
            result.append(
                str(normalized_query)
            )

        for phrase in self._phrases(canonical):
            text = self._token_text(phrase)
            if text:
                result.append(text)

        return tuple(
            dict.fromkeys(result)
        )

    # ------------------------------------------------------------------
    # Field expansion
    # ------------------------------------------------------------------

    def _expand_fields(
        self,
        canonical: Mapping[str, Any],
        intent_analysis: Mapping[str, Any],
        terms: Sequence[ExpansionTerm],
        phrases: Sequence[ExpansionPhrase],
        requested_fields: Sequence[QueryField],
        budget: ExpansionBudget,
    ) -> List[ExpansionFieldTarget]:
        if not self.policy.allow_field_expansion:
            return []

        output: List[ExpansionFieldTarget] = []

        for field in requested_fields:
            for term in terms:
                output.append(
                    ExpansionFieldTarget(
                        field=field,
                        source_term_id=term.term_id,
                        source_phrase_id=None,
                        weight=1.0,
                        confidence=1.0,
                        required=field != QueryField.ALL,
                        provenance={
                            "stage": "11.4",
                            "reason": "explicit_requested_field",
                        },
                    )
                )

        intent_blob = json.dumps(
            intent_analysis,
            ensure_ascii=False,
            default=str,
        ).casefold()

        inferred_fields = self._infer_fields(
            intent_blob
        )

        for term in terms:
            candidates = self.backend.lookup_field_targets(
                term.normalized_text,
                limit=budget.max_field_targets,
            )

            for candidate in candidates:
                if not isinstance(candidate, Mapping):
                    continue

                raw_field = candidate.get(
                    "field"
                )

                if raw_field:
                    try:
                        field = QueryField(
                            str(raw_field).casefold()
                        )
                    except ValueError:
                        continue

                    output.append(
                        ExpansionFieldTarget(
                            field=field,
                            source_term_id=term.term_id,
                            source_phrase_id=None,
                            weight=float(
                                candidate.get(
                                    "weight",
                                    0.70,
                                )
                            ),
                            confidence=float(
                                candidate.get(
                                    "confidence",
                                    0.75,
                                )
                            ),
                            required=False,
                            provenance={
                                "stage": "11.4",
                                "reason": "backend_field_target",
                            },
                        )
                    )

        for field in inferred_fields:
            for term in terms:
                output.append(
                    ExpansionFieldTarget(
                        field=field,
                        source_term_id=term.term_id,
                        source_phrase_id=None,
                        weight=0.60,
                        confidence=0.70,
                        required=False,
                        provenance={
                            "stage": "11.4",
                            "reason": "intent_derived_field_target",
                        },
                    )
                )

        for phrase in phrases:
            if phrase.exact_preserved:
                output.append(
                    ExpansionFieldTarget(
                        field=QueryField.ALL,
                        source_term_id=None,
                        source_phrase_id=phrase.phrase_id,
                        weight=1.0,
                        confidence=1.0,
                        required=False,
                        provenance={
                            "stage": "11.4",
                            "reason": "exact_phrase_all_field",
                        },
                    )
                )

        return self._deduplicate_field_targets(
            output
        )

    def _infer_fields(
        self,
        intent_blob: str,
    ) -> Tuple[QueryField, ...]:
        fields: List[QueryField] = []

        if any(
            word in intent_blob
            for word in (
                "website",
                "navigational",
                "url",
                "domain",
            )
        ):
            fields.extend(
                (
                    QueryField.TITLE,
                    QueryField.URL,
                    QueryField.DOMAIN,
                    QueryField.ANCHOR,
                )
            )

        if any(
            word in intent_blob
            for word in (
                "person",
                "find_person",
                "author",
            )
        ):
            fields.extend(
                (
                    QueryField.PERSON,
                    QueryField.AUTHOR,
                    QueryField.TITLE,
                )
            )

        if any(
            word in intent_blob
            for word in (
                "organization",
                "company",
                "university",
            )
        ):
            fields.extend(
                (
                    QueryField.ORGANIZATION,
                    QueryField.TITLE,
                    QueryField.BODY,
                )
            )

        if any(
            word in intent_blob
            for word in (
                "product",
                "price",
                "transactional",
            )
        ):
            fields.extend(
                (
                    QueryField.PRODUCT,
                    QueryField.TITLE,
                    QueryField.BODY,
                )
            )

        if any(
            word in intent_blob
            for word in (
                "local",
                "location",
                "nearby",
            )
        ):
            fields.extend(
                (
                    QueryField.LOCATION,
                    QueryField.TITLE,
                    QueryField.BODY,
                )
            )

        if any(
            word in intent_blob
            for word in (
                "video",
                "image",
                "media",
            )
        ):
            fields.append(
                QueryField.MEDIA
            )

        if any(
            word in intent_blob
            for word in (
                "document",
                "paper",
            )
        ):
            fields.append(
                QueryField.DOCUMENT
            )

        return tuple(
            dict.fromkeys(fields)
        )

    # ------------------------------------------------------------------
    # Term grouping
    # ------------------------------------------------------------------

    def _build_term_groups(
        self,
        canonical: Mapping[str, Any],
        terms: Sequence[ExpansionTerm],
        phrases: Sequence[ExpansionPhrase],
        budget: ExpansionBudget,
    ) -> List[TermGroup]:
        groups: List[TermGroup] = []

        exact_terms = [
            term
            for term in terms
            if term.exact_preserved
        ]

        optional_terms = [
            term
            for term in terms
            if not term.exact_preserved
        ]

        if exact_terms:
            groups.append(
                TermGroup(
                    group_id=_new_id("group"),
                    role=TermGroupRole.EXACT_PRESERVATION,
                    original_text=str(
                        canonical.get(
                            "normalized_query",
                            "",
                        )
                    ),
                    terms=tuple(exact_terms),
                    minimum_match=1,
                    exact_preservation=True,
                    group_confidence=1.0,
                    provenance={
                        "stage": "11.4",
                        "reason": "exact_query_group",
                    },
                )
            )

        if optional_terms:
            groups.append(
                TermGroup(
                    group_id=_new_id("group"),
                    role=TermGroupRole.OPTIONAL,
                    original_text=str(
                        canonical.get(
                            "normalized_query",
                            "",
                        )
                    ),
                    terms=tuple(optional_terms),
                    minimum_match=1,
                    exact_preservation=True,
                    group_confidence=(
                        sum(
                            term.confidence
                            for term in optional_terms
                        )
                        / len(optional_terms)
                    ),
                    provenance={
                        "stage": "11.4",
                        "reason": "controlled_expansion_group",
                    },
                )
            )

        for phrase in phrases:
            if phrase.exact_preserved:
                phrase_term = ExpansionTerm(
                    term_id=_new_id("term"),
                    text=phrase.text,
                    normalized_text=phrase.normalized_text,
                    kind=ExpansionKind.EXACT,
                    source=ExpansionSource.ORIGINAL_QUERY,
                    relation=ExpansionRelation.SAME,
                    field=QueryField.ALL,
                    role=TermGroupRole.EXACT_PRESERVATION,
                    confidence=1.0,
                    confidence_level=ExpansionConfidenceLevel.VERY_HIGH,
                    exact_preserved=True,
                    provenance={
                        "stage": "11.4",
                        "reason": "phrase_group_projection",
                    },
                )

                groups.append(
                    TermGroup(
                        group_id=_new_id("group"),
                        role=TermGroupRole.EXACT_PRESERVATION,
                        original_text=phrase.text,
                        terms=(phrase_term,),
                        minimum_match=1,
                        exact_preservation=True,
                        group_confidence=1.0,
                        provenance={
                            "stage": "11.4",
                            "phrase_id": phrase.phrase_id,
                        },
                    )
                )

        return groups[:budget.max_total_groups]

    # ------------------------------------------------------------------
    # Fanout control
    # ------------------------------------------------------------------

    def _control_fanout(
        self,
        exact_terms: Sequence[ExpansionTerm],
        terms: Sequence[ExpansionTerm],
        phrases: Sequence[ExpansionPhrase],
        field_targets: Sequence[ExpansionFieldTarget],
        groups: Sequence[TermGroup],
        budget: ExpansionBudget,
    ) -> FanoutDecision:
        original_count = (
            len(terms)
            + len(phrases)
            + len(field_targets)
            + len(groups)
        )

        reasons: List[str] = []

        accepted_terms = list(exact_terms)

        optional_terms = sorted(
            (
                term
                for term in terms
                if not term.exact_preserved
            ),
            key=lambda term: (
                -term.confidence,
                term.normalized_text,
            ),
        )

        remaining = max(
            0,
            budget.max_total_terms
            - len(accepted_terms),
        )

        accepted_terms.extend(
            optional_terms[:remaining]
        )

        rejected = max(
            0,
            len(optional_terms)
            - remaining,
        )

        if rejected:
            reasons.append(
                "term budget capped optional expansion"
            )

        if len(phrases) > budget.max_phrase_variants:
            reasons.append(
                "phrase budget capped expansion"
            )

        if len(field_targets) > budget.max_field_targets:
            reasons.append(
                "field target budget capped expansion"
            )

        if len(accepted_terms) >= budget.max_total_terms:
            state = FanoutState.CAPPED
        elif reasons:
            state = FanoutState.REDUCED
        else:
            state = FanoutState.WITHIN_BUDGET

        fingerprint = _hash(
            {
                "state": state.value,
                "original": original_count,
                "accepted": len(accepted_terms),
                "rejected": rejected,
                "reasons": reasons,
            }
        )

        return FanoutDecision(
            state=state,
            original_candidate_count=original_count,
            accepted_candidate_count=len(accepted_terms),
            rejected_candidate_count=rejected,
            budget=budget,
            reasons=tuple(reasons),
            fingerprint=fingerprint,
        )

    # ------------------------------------------------------------------
    # Confidence
    # ------------------------------------------------------------------

    def _calculate_confidence(
        self,
        exact_terms: Sequence[ExpansionTerm],
        terms: Sequence[ExpansionTerm],
        phrases: Sequence[ExpansionPhrase],
        field_targets: Sequence[ExpansionFieldTarget],
        fanout: FanoutDecision,
    ) -> ExpansionConfidence:
        exact_coverage = (
            1.0
            if exact_terms
            else 0.0
        )

        lexical_terms = [
            term
            for term in terms
            if term.kind
            in (
                ExpansionKind.SYNONYM,
                ExpansionKind.LEXICAL_VARIANT,
                ExpansionKind.MORPHOLOGICAL_VARIANT,
                ExpansionKind.SPELLING_VARIANT,
            )
        ]

        lexical_confidence = (
            sum(
                term.confidence
                for term in lexical_terms
            )
            / len(lexical_terms)
            if lexical_terms
            else 1.0
        )

        phrase_confidence = (
            sum(
                phrase.confidence
                for phrase in phrases
            )
            / len(phrases)
            if phrases
            else 1.0
        )

        entity_terms = [
            term
            for term in terms
            if term.kind
            == ExpansionKind.ENTITY_VARIANT
        ]

        entity_confidence = (
            sum(
                term.confidence
                for term in entity_terms
            )
            / len(entity_terms)
            if entity_terms
            else 1.0
        )

        field_confidence = (
            sum(
                target.confidence
                for target in field_targets
            )
            / len(field_targets)
            if field_targets
            else 1.0
        )

        fanout_penalty = (
            0.85
            if fanout.state == FanoutState.REDUCED
            else 0.75
            if fanout.state == FanoutState.CAPPED
            else 1.0
        )

        overall = (
            (
                exact_coverage * 0.30
                + lexical_confidence * 0.20
                + phrase_confidence * 0.15
                + entity_confidence * 0.15
                + field_confidence * 0.20
            )
            * fanout_penalty
        )

        overall = max(
            0.0,
            min(1.0, overall),
        )

        uncertainty: List[str] = []

        if not lexical_terms:
            uncertainty.append(
                "no external lexical variants were available"
            )

        if fanout.state != FanoutState.WITHIN_BUDGET:
            uncertainty.append(
                "expansion fanout was constrained"
            )

        return ExpansionConfidence(
            overall_score=overall,
            level=_confidence_level(overall),
            exact_term_coverage=exact_coverage,
            lexical_confidence=lexical_confidence,
            phrase_confidence=phrase_confidence,
            entity_confidence=entity_confidence,
            field_confidence=field_confidence,
            uncertainty_reasons=tuple(
                uncertainty
            ),
        )

    # ------------------------------------------------------------------
    # Retrieval hints
    # ------------------------------------------------------------------

    def _build_retrieval_hints(
        self,
        canonical: Mapping[str, Any],
        intent_analysis: Mapping[str, Any],
        signals: ExpansionSignalProfile,
        exact_terms: Sequence[ExpansionTerm],
        terms: Sequence[ExpansionTerm],
        phrases: Sequence[ExpansionPhrase],
        field_targets: Sequence[ExpansionFieldTarget],
    ) -> Mapping[str, Any]:
        return {
            "preserve_exact_matches": True,
            "exact_term_count": len(exact_terms),
            "expanded_term_count": len(terms),
            "phrase_count": len(phrases),
            "field_target_count": len(field_targets),

            "prefer_url_fields": signals.has_url,
            "prefer_temporal_fields": signals.has_temporal_signal,
            "prefer_local_fields": signals.has_local_signal,
            "prefer_entity_fields": signals.has_entity_signal,
            "prefer_media_fields": signals.has_media_signal,
            "comparison_aware": signals.has_comparison_signal,

            "query_type": self._extract_intent_value(
                intent_analysis,
                "query_type",
            ),

            "primary_intent": self._extract_intent_value(
                intent_analysis,
                "primary_intent",
            ),

            "architecture_version": ARCHITECTURE_VERSION,

            "next_stage": "11.5_distributed_retrieval_planning",
        }

    def _extract_intent_value(
        self,
        intent_analysis: Mapping[str, Any],
        key: str,
    ) -> Optional[str]:
        value = intent_analysis.get(key)

        if value is not None:
            return str(value)

        decision = intent_analysis.get(
            "decision"
        )

        if isinstance(decision, Mapping):
            value = decision.get(key)
            if value is not None:
                return str(value)

        return None

    # ------------------------------------------------------------------
    # Contract
    # ------------------------------------------------------------------

    def _build_contract(
        self,
        identity: ExpansionIdentity,
        exact_terms: Sequence[ExpansionTerm],
        terms: Sequence[ExpansionTerm],
        phrases: Sequence[ExpansionPhrase],
        field_targets: Sequence[ExpansionFieldTarget],
        groups: Sequence[TermGroup],
        fanout: FanoutDecision,
        confidence: ExpansionConfidence,
        retrieval_hints: Mapping[str, Any],
    ) -> RetrievalExpansionContract:
        contract_id = _new_id(
            "retrieval_contract"
        )

        contract_hash = _hash(
            {
                "expansion_id": identity.expansion_id,
                "exact_terms": [
                    term.fingerprint()
                    for term in exact_terms
                ],
                "terms": [
                    term.fingerprint()
                    for term in terms
                ],
                "phrases": [
                    {
                        "normalized_text": phrase.normalized_text,
                        "relation": phrase.relation.value,
                    }
                    for phrase in phrases
                ],
                "field_targets": [
                    {
                        "field": target.field.value,
                        "source_term_id": target.source_term_id,
                        "source_phrase_id": target.source_phrase_id,
                        "weight": target.weight,
                    }
                    for target in field_targets
                ],
                "groups": [
                    group.group_id
                    for group in groups
                ],
                "fanout": fanout.fingerprint,
                "confidence": confidence.overall_score,
            }
        )

        return RetrievalExpansionContract(
            contract_id=contract_id,
            expansion_id=identity.expansion_id,
            query_id=identity.query_id,
            exact_terms=tuple(exact_terms),
            term_groups=tuple(groups),
            phrases=tuple(phrases),
            field_targets=tuple(field_targets),
            fanout=fanout,
            confidence=confidence,
            retrieval_hints=dict(retrieval_hints),
            created_at=_now(),
            contract_hash=contract_hash,
        )

    # ------------------------------------------------------------------
    # Deduplication
    # ------------------------------------------------------------------

    def _deduplicate_terms(
        self,
        terms: Sequence[ExpansionTerm],
    ) -> List[ExpansionTerm]:
        seen = set()
        result: List[ExpansionTerm] = []

        for term in terms:
            key = (
                term.normalized_text,
                term.kind.value,
                term.field.value,
                term.role.value,
            )

            if key in seen:
                continue

            seen.add(key)
            result.append(term)

        return result

    def _deduplicate_phrases(
        self,
        phrases: Sequence[ExpansionPhrase],
    ) -> List[ExpansionPhrase]:
        seen = set()
        result: List[ExpansionPhrase] = []

        for phrase in phrases:
            key = (
                phrase.normalized_text,
                phrase.relation.value,
            )

            if key in seen:
                continue

            seen.add(key)
            result.append(phrase)

        return result

    def _deduplicate_field_targets(
        self,
        targets: Sequence[ExpansionFieldTarget],
    ) -> List[ExpansionFieldTarget]:
        seen = set()
        result: List[ExpansionFieldTarget] = []

        for target in targets:
            key = (
                target.field.value,
                target.source_term_id,
                target.source_phrase_id,
            )

            if key in seen:
                continue

            seen.add(key)
            result.append(target)

        return result

    # ------------------------------------------------------------------
    # Events/checkpoints
    # ------------------------------------------------------------------

    def _emit(
        self,
        identity: ExpansionIdentity,
        event_type: ExpansionEventType,
        state: ExpansionState,
        payload: Mapping[str, Any],
    ) -> None:
        self._events.append(
            ExpansionEvent(
                event_id=_new_id("event"),
                expansion_id=identity.expansion_id,
                event_type=event_type,
                state=state,
                timestamp=_now(),
                payload=dict(payload),
            )
        )

    def _create_checkpoint(
        self,
        result: QueryTermFieldExpansion,
        *,
        epoch: int,
    ) -> ExpansionCheckpoint:
        checkpoint = ExpansionCheckpoint(
            checkpoint_id=_new_id(
                "expansion_checkpoint"
            ),
            expansion_id=result.identity.expansion_id,
            state=result.state,
            checkpoint_epoch=epoch,
            expansion_hash=result.analysis_hash,
            term_count=len(result.expanded_terms),
            phrase_count=len(result.phrases),
            field_target_count=len(result.field_targets),
            created_at=_now(),
        )

        self._checkpoints[
            checkpoint.checkpoint_id
        ] = checkpoint

        return checkpoint

    # ------------------------------------------------------------------
    # Inspection
    # ------------------------------------------------------------------

    def events(
        self,
        expansion_id: Optional[str] = None,
    ) -> Tuple[ExpansionEvent, ...]:
        if expansion_id is None:
            return tuple(self._events)

        return tuple(
            event
            for event in self._events
            if event.expansion_id == expansion_id
        )

    def checkpoints(
        self,
        expansion_id: Optional[str] = None,
    ) -> Tuple[ExpansionCheckpoint, ...]:
        values = tuple(
            self._checkpoints.values()
        )

        if expansion_id is None:
            return values

        return tuple(
            checkpoint
            for checkpoint in values
            if checkpoint.expansion_id
            == expansion_id
        )

    # ------------------------------------------------------------------
    # Architecture contract
    # ------------------------------------------------------------------

    @staticmethod
    def architecture() -> Mapping[str, Any]:
        return {
            "name": "OUR SEARCH Phase 11.4 Term & Field Expansion",
            "version": ARCHITECTURE_VERSION,

            "scale_target": SCALE_TARGET,
            "google_scale_capability_target": (
                GOOGLE_SCALE_CAPABILITY_TARGET
            ),
            "google_technology_dependency": (
                GOOGLE_TECHNOLOGY_DEPENDENCY
            ),

            "input": (
                "Phase 11.2 canonical query + "
                "Phase 11.3 query type and intent analysis"
            ),

            "pipeline": [
                "exact term preservation",
                "lexical expansion",
                "controlled synonym/variant expansion",
                "phrase expansion",
                "entity variant expansion",
                "field-aware expansion",
                "provenance attachment",
                "confidence calculation",
                "fanout control",
                "retrieval contract generation",
            ],

            "guarantees": [
                "original query terms are preserved",
                "exact-match intent is never silently discarded",
                "every generated expansion carries provenance",
                "every generated expansion carries confidence",
                "expansion fanout is explicitly bounded",
                "field targeting is explicit",
                "phrase structure is preserved",
                "entity aliases can be incorporated",
                "backend resources are replaceable",
                "architecture is partitionable",
                "architecture is horizontally scalable",
                "architecture is checkpointable",
                "architecture is deterministic at the contract level",
            ],

            "distributed_design": {
                "partitionable": True,
                "horizontally_scalable": True,
                "epoch_aware": True,
                "checkpointable": True,
                "backend_replaceable": True,
                "stateless_compute_preferred": True,
                "global_single_worker": False,
                "global_single_database": False,
                "global_single_dictionary": False,
                "global_single_expansion_queue": False,
            },

            "anti_explosion_controls": [
                "maximum total terms",
                "maximum terms per source term",
                "maximum phrase variants",
                "maximum entity variants",
                "maximum field targets",
                "maximum term groups",
                "maximum generated characters",
                "confidence threshold",
                "combination depth bound",
            ],

            "explicit_non_responsibilities": [
                "web crawling",
                "document retrieval",
                "ranking",
                "final result selection",
                "embedding generation",
                "index mutation",
                "search result generation",
            ],

            "next_stage": (
                "11.5 Distributed Retrieval Planning"
            ),

            "protected_components": [
                "website_server.py",
                "search_service/server.py",
            ],
        }


# ---------------------------------------------------------------------------
# Compatibility aliases
# ---------------------------------------------------------------------------

QueryTermFieldExpansionArchitecture = QueryTermFieldExpansion
GlobalQueryTermFieldExpansion = QueryTermFieldExpansion
Phase11_4QueryTermFieldExpansion = QueryTermFieldExpansion


__all__ = [
    "ARCHITECTURE_VERSION",
    "SCALE_TARGET",
    "GOOGLE_SCALE_CAPABILITY_TARGET",
    "GOOGLE_TECHNOLOGY_DEPENDENCY",

    "ExpansionState",
    "ExpansionKind",
    "ExpansionSource",
    "ExpansionConfidenceLevel",
    "QueryField",
    "TermGroupRole",
    "ExpansionRelation",
    "FanoutState",
    "ExpansionEventType",

    "ExpansionIdentity",
    "ExpansionTerm",
    "ExpansionPhrase",
    "ExpansionFieldTarget",
    "TermGroup",
    "ExpansionSignalProfile",
    "ExpansionBudget",
    "FanoutDecision",
    "ExpansionConfidence",
    "RetrievalExpansionContract",
    "QueryTermFieldExpansion",
    "ExpansionCheckpoint",
    "ExpansionEvent",
    "ExpansionRequest",

    "QueryTermFieldExpansionBackend",
    "InMemoryQueryTermFieldExpansionBackend",
    "QueryTermFieldExpansionPolicy",
    "QueryTermFieldExpansion",

    "QueryTermFieldExpansionArchitecture",
    "GlobalQueryTermFieldExpansion",
    "Phase11_4QueryTermFieldExpansion",
]
