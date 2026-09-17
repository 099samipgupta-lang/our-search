"""
OUR SEARCH
Phase 12.7 — Multi-Signal Ranking Feature Fusion

Purpose
-------
Fuse independent ranking-signal families produced by Phase 12 into a
structured ranking-feature representation for downstream ranking.

This stage does NOT:
- perform final ranking
- select final search results
- crawl the Web
- mutate the index
- classify spam
- execute a ranking model
- replace Phase 11 retrieval

It prepares a deterministic feature vector / feature bundle that later
ranking stages can consume.

Scale target
------------
Designed directly for:

    billions -> potentially trillions of public-Web resources

The architecture assumes:
- distributed candidate processing
- partition-local computation
- horizontally scalable workers
- deterministic feature generation
- checkpointable processing
- backend-replaceable persistence
- partial-input handling

Google dependency
-----------------
No Google Search API.
No Google index.
No Google crawler.
No Google infrastructure.
No Google ranking implementation.
"""

from __future__ import annotations

import hashlib
import math
import uuid

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import (
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    Tuple,
)


ARCHITECTURE_VERSION = (
    "multi-signal-ranking-feature-fusion.v1"
)

SCALE_TARGET = (
    "billions_to_trillions_of_public_web_resources"
)

GOOGLE_SCALE_CAPABILITY_TARGET = True
GOOGLE_TECHNOLOGY_DEPENDENCY = False


# ============================================================================
# ENUMS
# ============================================================================


class FeatureFusionState(str, Enum):
    RECEIVED = "received"
    VALIDATING = "validating"
    SIGNAL_NORMALIZATION = "signal_normalization"
    FEATURE_CONSTRUCTION = "feature_construction"
    FEATURE_INTERACTION = "feature_interaction"
    FEATURE_AGGREGATION = "feature_aggregation"
    CONFIDENCE_CALCULATION = "confidence_calculation"
    COMPLETED = "completed"
    PARTIAL = "partial"
    REJECTED = "rejected"
    FAILED = "failed"


class RankingSignalFamily(str, Enum):
    TEXT_RELEVANCE = "text_relevance"
    SEMANTIC_RELEVANCE = "semantic_relevance"
    QUERY_ALIGNMENT = "query_alignment"

    QUALITY = "quality"
    AUTHORITY = "authority"
    TRUST = "trust"

    FRESHNESS = "freshness"
    TEMPORAL_RELEVANCE = "temporal_relevance"

    LINK_AUTHORITY = "link_authority"
    GRAPH_AUTHORITY = "graph_authority"

    SOURCE_DIVERSITY = "source_diversity"
    EVIDENCE_DIVERSITY = "evidence_diversity"

    DOCUMENT_STRUCTURE = "document_structure"
    FIELD_MATCH = "field_match"
    PHRASE_MATCH = "phrase_match"
    ENTITY_MATCH = "entity_match"

    INTENT_ALIGNMENT = "intent_alignment"
    QUERY_TYPE_ALIGNMENT = "query_type_alignment"

    QUERY_SPECIFIC_AUTHORITY = "query_specific_authority"

    QUERY_DOCUMENT_COHERENCE = "query_document_coherence"

    AUTHORITY_RELEVANCE_BALANCE = (
        "authority_relevance_balance"
    )


class FeatureValueType(str, Enum):
    CONTINUOUS = "continuous"
    BINARY = "binary"
    COUNT = "count"
    ORDINAL = "ordinal"


class FeatureInteractionType(str, Enum):
    PRODUCT = "product"
    GEOMETRIC_MEAN = "geometric_mean"
    MINIMUM = "minimum"
    MAXIMUM = "maximum"
    HARMONIC_MEAN = "harmonic_mean"


class FeatureStrength(str, Enum):
    NONE = "none"
    WEAK = "weak"
    MODERATE = "moderate"
    STRONG = "strong"
    VERY_STRONG = "very_strong"


class FeatureFusionEventType(str, Enum):
    REQUEST_RECEIVED = "request_received"
    VALIDATION_STARTED = "validation_started"

    SIGNALS_NORMALIZATION_STARTED = (
        "signals_normalization_started"
    )

    FEATURES_CONSTRUCTED = "features_constructed"

    FEATURE_INTERACTIONS_CONSTRUCTED = (
        "feature_interactions_constructed"
    )

    FEATURES_AGGREGATED = "features_aggregated"

    CONFIDENCE_CALCULATED = (
        "confidence_calculated"
    )

    PARTIAL_INPUT_DETECTED = (
        "partial_input_detected"
    )

    CHECKPOINT_CREATED = "checkpoint_created"

    FUSION_COMPLETED = "fusion_completed"

    FUSION_REJECTED = "fusion_rejected"

    FUSION_FAILED = "fusion_failed"


# ============================================================================
# IDENTITY / LINEAGE
# ============================================================================


@dataclass(frozen=True)
class FeatureFusionIdentity:
    request_id: str
    query_id: str
    document_id: str
    canonical_url: str

    def stable_key(self) -> str:
        payload = "|".join(
            [
                self.query_id,
                self.document_id,
                self.canonical_url,
            ]
        )

        return hashlib.sha256(
            payload.encode("utf-8")
        ).hexdigest()


@dataclass(frozen=True)
class FeatureFusionLineage:
    query_stage: str
    query_version: str

    signal_stage: str
    signal_version: str

    authority_stage: str
    authority_version: str

    freshness_stage: str
    freshness_version: str

    graph_stage: str
    graph_version: str

    observed_at: str

    def fingerprint(self) -> str:

        payload = "|".join(
            [
                self.query_stage,
                self.query_version,
                self.signal_stage,
                self.signal_version,
                self.authority_stage,
                self.authority_version,
                self.freshness_stage,
                self.freshness_version,
                self.graph_stage,
                self.graph_version,
                self.observed_at,
            ]
        )

        return hashlib.sha256(
            payload.encode("utf-8")
        ).hexdigest()


# ============================================================================
# INPUT SIGNAL
# ============================================================================


@dataclass(frozen=True)
class RankingSignal:

    family: RankingSignalFamily

    value: float

    confidence: float = 1.0

    evidence_count: int = 1

    source: str = ""

    partial: bool = False

    metadata: Mapping[str, object] = field(
        default_factory=dict
    )


# ============================================================================
# INPUT REPRESENTATION
# ============================================================================


@dataclass(frozen=True)
class RankingSignalInput:

    query_id: str
    document_id: str

    canonical_url: str

    signals: Tuple[
        RankingSignal,
        ...
    ]

    partial: bool = False

    query_version: str = (
        "phase11-retrieval-architecture"
    )

    observed_at: Optional[str] = None

    partition_id: str = ""

    shard_id: str = ""


# ============================================================================
# NORMALIZED FEATURE
# ============================================================================


@dataclass(frozen=True)
class RankingFeature:

    feature_id: str

    family: RankingSignalFamily

    value_type: FeatureValueType

    raw_value: float

    normalized_value: float

    confidence: float

    strength: FeatureStrength

    evidence_count: int

    source_signal_count: int

    partial: bool

    metadata: Mapping[str, object] = field(
        default_factory=dict
    )


# ============================================================================
# FEATURE INTERACTION
# ============================================================================


@dataclass(frozen=True)
class RankingFeatureInteraction:

    interaction_id: str

    name: str

    interaction_type: FeatureInteractionType

    input_features: Tuple[str, ...]

    value: float

    confidence: float

    partial: bool


# ============================================================================
# FEATURE BUNDLE
# ============================================================================


@dataclass(frozen=True)
class RankingFeatureBundle:

    query_id: str
    document_id: str

    features: Tuple[
        RankingFeature,
        ...
    ]

    interactions: Tuple[
        RankingFeatureInteraction,
        ...
    ]

    relevance_feature_score: float

    authority_feature_score: float

    quality_feature_score: float

    freshness_feature_score: float

    graph_feature_score: float

    diversity_feature_score: float

    query_specific_score: float

    interaction_score: float

    aggregate_feature_score: float

    confidence: float

    partial: bool

    feature_count: int
    interaction_count: int


# ============================================================================
# RESULT
# ============================================================================


@dataclass(frozen=True)
class FeatureFusionResult:

    identity: FeatureFusionIdentity

    lineage: FeatureFusionLineage

    bundle: RankingFeatureBundle

    state: FeatureFusionState

    completed_at: str

    architecture_version: str = (
        ARCHITECTURE_VERSION
    )


# ============================================================================
# CHECKPOINT
# ============================================================================


@dataclass(frozen=True)
class FeatureFusionCheckpoint:

    checkpoint_id: str

    request_id: str

    query_id: str
    document_id: str

    state: FeatureFusionState

    processed_signals: int
    constructed_features: int
    constructed_interactions: int

    created_at: str


# ============================================================================
# EVENTS
# ============================================================================


@dataclass(frozen=True)
class FeatureFusionEvent:

    event_id: str

    request_id: str

    query_id: str
    document_id: str

    event_type: FeatureFusionEventType

    state: FeatureFusionState

    timestamp: str

    payload: Mapping[str, object] = field(
        default_factory=dict
    )


# ============================================================================
# BACKEND
# ============================================================================


class FeatureFusionBackend(Protocol):

    def persist_event(
        self,
        event: FeatureFusionEvent,
    ) -> None:
        ...

    def persist_checkpoint(
        self,
        checkpoint: FeatureFusionCheckpoint,
    ) -> None:
        ...

    def persist_result(
        self,
        result: FeatureFusionResult,
    ) -> None:
        ...

    def get_result(
        self,
        query_id: str,
        document_id: str,
    ) -> Optional[FeatureFusionResult]:
        ...


class InMemoryFeatureFusionMetadata:

    def __init__(self) -> None:

        self._events: List[
            FeatureFusionEvent
        ] = []

        self._checkpoints: List[
            FeatureFusionCheckpoint
        ] = []

        self._results: Dict[
            Tuple[str, str],
            FeatureFusionResult,
        ] = {}

    def persist_event(
        self,
        event: FeatureFusionEvent,
    ) -> None:

        self._events.append(event)

    def persist_checkpoint(
        self,
        checkpoint: FeatureFusionCheckpoint,
    ) -> None:

        self._checkpoints.append(checkpoint)

    def persist_result(
        self,
        result: FeatureFusionResult,
    ) -> None:

        self._results[
            (
                result.identity.query_id,
                result.identity.document_id,
            )
        ] = result

    def get_result(
        self,
        query_id: str,
        document_id: str,
    ) -> Optional[FeatureFusionResult]:

        return self._results.get(
            (
                query_id,
                document_id,
            )
        )

    def events(
        self,
    ) -> Tuple[
        FeatureFusionEvent,
        ...
    ]:

        return tuple(self._events)

    def checkpoints(
        self,
    ) -> Tuple[
        FeatureFusionCheckpoint,
        ...
    ]:

        return tuple(self._checkpoints)


# ============================================================================
# POLICY
# ============================================================================


@dataclass(frozen=True)
class FeatureFusionPolicy:

    max_input_signals: int = 512
    max_features: int = 512
    max_interactions: int = 256

    minimum_confidence: float = 0.10

    allow_partial: bool = True

    # Family weights.
    relevance_weight: float = 1.50
    authority_weight: float = 1.25
    quality_weight: float = 1.00
    freshness_weight: float = 1.00
    graph_weight: float = 1.00
    diversity_weight: float = 0.50
    query_specific_weight: float = 1.50

    interaction_weight: float = 1.00


# ============================================================================
# MAIN ARCHITECTURE
# ============================================================================


class MultiSignalRankingFeatureFusionArchitecture:

    def __init__(
        self,
        backend: Optional[
            FeatureFusionBackend
        ] = None,
        policy: Optional[
            FeatureFusionPolicy
        ] = None,
    ) -> None:

        self.backend = (
            backend
            or InMemoryFeatureFusionMetadata()
        )

        self.policy = (
            policy
            or FeatureFusionPolicy()
        )

    # ------------------------------------------------------------------
    # UTILITIES
    # ------------------------------------------------------------------

    @staticmethod
    def _now() -> str:

        return datetime.now(
            timezone.utc
        ).isoformat()

    @staticmethod
    def _clamp(
        value: float,
    ) -> float:

        return max(
            0.0,
            min(
                1.0,
                float(value),
            ),
        )

    @staticmethod
    def _strength(
        value: float,
    ) -> FeatureStrength:

        value = max(
            0.0,
            min(
                1.0,
                value,
            ),
        )

        if value <= 0.0:
            return FeatureStrength.NONE

        if value < 0.25:
            return FeatureStrength.WEAK

        if value < 0.50:
            return FeatureStrength.MODERATE

        if value < 0.80:
            return FeatureStrength.STRONG

        return FeatureStrength.VERY_STRONG

    @staticmethod
    def _safe_log(
        value: float,
    ) -> float:

        if value <= 0.0:
            return 0.0

        return min(
            1.0,
            math.log1p(value)
            / math.log1p(1000.0),
        )

    # ------------------------------------------------------------------
    # EVENTS
    # ------------------------------------------------------------------

    def _event(
        self,
        request_id: str,
        query_id: str,
        document_id: str,
        event_type: FeatureFusionEventType,
        state: FeatureFusionState,
        payload: Optional[
            Mapping[str, object]
        ] = None,
    ) -> FeatureFusionEvent:

        event = FeatureFusionEvent(
            event_id=str(uuid.uuid4()),
            request_id=request_id,
            query_id=query_id,
            document_id=document_id,
            event_type=event_type,
            state=state,
            timestamp=self._now(),
            payload=dict(payload or {}),
        )

        self.backend.persist_event(event)

        return event

    # ------------------------------------------------------------------
    # SIGNAL NORMALIZATION
    # ------------------------------------------------------------------

    def _normalize_signal(
        self,
        signal: RankingSignal,
        index: int,
    ) -> RankingFeature:

        normalized = self._clamp(
            signal.value
        )

        confidence = self._clamp(
            signal.confidence
        )

        return RankingFeature(
            feature_id=(
                f"feature-{index}-"
                f"{signal.family.value}"
            ),

            family=signal.family,

            value_type=FeatureValueType.CONTINUOUS,

            raw_value=float(
                signal.value
            ),

            normalized_value=normalized,

            confidence=confidence,

            strength=self._strength(
                normalized
            ),

            evidence_count=max(
                0,
                signal.evidence_count,
            ),

            source_signal_count=1,

            partial=signal.partial,

            metadata=dict(
                signal.metadata
            ),
        )

    def _normalize_signals(
        self,
        signals: Sequence[
            RankingSignal
        ],
    ) -> Tuple[
        RankingFeature,
        ...
    ]:

        features: List[
            RankingFeature
        ] = []

        for index, signal in enumerate(
            signals[
                : self.policy.max_input_signals
            ]
        ):

            features.append(
                self._normalize_signal(
                    signal,
                    index,
                )
            )

        return tuple(
            features[
                : self.policy.max_features
            ]
        )

    # ------------------------------------------------------------------
    # FEATURE LOOKUP
    # ------------------------------------------------------------------

    @staticmethod
    def _feature_value(
        features: Sequence[
            RankingFeature
        ],
        family: RankingSignalFamily,
    ) -> float:

        values = [
            feature.normalized_value
            for feature in features
            if feature.family == family
        ]

        if not values:
            return 0.0

        return (
            sum(values)
            / len(values)
        )

    @staticmethod
    def _feature_confidence(
        features: Sequence[
            RankingFeature
        ],
        family: RankingSignalFamily,
    ) -> float:

        values = [
            feature.confidence
            for feature in features
            if feature.family == family
        ]

        if not values:
            return 0.0

        return (
            sum(values)
            / len(values)
        )

    # ------------------------------------------------------------------
    # FEATURE INTERACTIONS
    # ------------------------------------------------------------------

    def _interaction(
        self,
        name: str,
        interaction_type: FeatureInteractionType,
        feature_values: Sequence[float],
        feature_confidences: Sequence[float],
        partial: bool,
    ) -> RankingFeatureInteraction:

        values = [
            self._clamp(value)
            for value in feature_values
        ]

        if not values:
            result = 0.0

        elif interaction_type == (
            FeatureInteractionType.PRODUCT
        ):

            result = 1.0

            for value in values:
                result *= value

        elif interaction_type == (
            FeatureInteractionType.GEOMETRIC_MEAN
        ):

            positive_values = [
                max(
                    value,
                    1e-12,
                )
                for value in values
            ]

            result = (
                math.prod(
                    positive_values
                )
                ** (
                    1.0
                    / len(
                        positive_values
                    )
                )
            )

        elif interaction_type == (
            FeatureInteractionType.MINIMUM
        ):

            result = min(values)

        elif interaction_type == (
            FeatureInteractionType.MAXIMUM
        ):

            result = max(values)

        elif interaction_type == (
            FeatureInteractionType.HARMONIC_MEAN
        ):

            denominator = sum(
                1.0 / max(
                    value,
                    1e-12,
                )
                for value in values
            )

            result = (
                len(values)
                / denominator
                if denominator > 0
                else 0.0
            )

        else:
            result = 0.0

        confidence = (
            sum(
                self._clamp(value)
                for value
                in feature_confidences
            )
            / len(
                feature_confidences
            )
            if feature_confidences
            else 0.0
        )

        return RankingFeatureInteraction(
            interaction_id=str(
                uuid.uuid4()
            ),

            name=name,

            interaction_type=(
                interaction_type
            ),

            input_features=tuple(
                name
                for name in [
                    "relevance",
                    "authority",
                    "quality",
                    "freshness",
                    "graph",
                ]
            )[
                : len(values)
            ],

            value=self._clamp(
                result
            ),

            confidence=self._clamp(
                confidence
            ),

            partial=partial,
        )

    def _build_interactions(
        self,
        features: Sequence[
            RankingFeature
        ],
        partial: bool,
    ) -> Tuple[
        RankingFeatureInteraction,
        ...
    ]:

        relevance = self._feature_value(
            features,
            RankingSignalFamily
            .QUERY_DOCUMENT_COHERENCE,
        )

        if relevance <= 0.0:
            relevance = self._feature_value(
                features,
                RankingSignalFamily
                .QUERY_ALIGNMENT,
            )

        if relevance <= 0.0:
            relevance = self._feature_value(
                features,
                RankingSignalFamily
                .TEXT_RELEVANCE,
            )

        authority = self._feature_value(
            features,
            RankingSignalFamily
            .QUERY_SPECIFIC_AUTHORITY,
        )

        if authority <= 0.0:
            authority = self._feature_value(
                features,
                RankingSignalFamily
                .AUTHORITY,
            )

        quality = self._feature_value(
            features,
            RankingSignalFamily
            .QUALITY,
        )

        trust = self._feature_value(
            features,
            RankingSignalFamily
            .TRUST,
        )

        freshness = self._feature_value(
            features,
            RankingSignalFamily
            .FRESHNESS,
        )

        temporal = self._feature_value(
            features,
            RankingSignalFamily
            .TEMPORAL_RELEVANCE,
        )

        graph = self._feature_value(
            features,
            RankingSignalFamily
            .GRAPH_AUTHORITY,
        )

        if graph <= 0.0:
            graph = self._feature_value(
                features,
                RankingSignalFamily
                .LINK_AUTHORITY,
            )

        interactions: List[
            RankingFeatureInteraction
        ] = []

        interactions.append(
            self._interaction(
                name="relevance_authority",
                interaction_type=(
                    FeatureInteractionType
                    .GEOMETRIC_MEAN
                ),
                feature_values=[
                    relevance,
                    authority,
                ],
                feature_confidences=[
                    self._feature_confidence(
                        features,
                        RankingSignalFamily
                        .QUERY_DOCUMENT_COHERENCE,
                    ),
                    self._feature_confidence(
                        features,
                        RankingSignalFamily
                        .QUERY_SPECIFIC_AUTHORITY,
                    ),
                ],
                partial=partial,
            )
        )

        interactions.append(
            self._interaction(
                name="quality_trust",
                interaction_type=(
                    FeatureInteractionType
                    .GEOMETRIC_MEAN
                ),
                feature_values=[
                    quality,
                    trust,
                ],
                feature_confidences=[
                    self._feature_confidence(
                        features,
                        RankingSignalFamily
                        .QUALITY,
                    ),
                    self._feature_confidence(
                        features,
                        RankingSignalFamily
                        .TRUST,
                    ),
                ],
                partial=partial,
            )
        )

        interactions.append(
            self._interaction(
                name="freshness_temporal",
                interaction_type=(
                    FeatureInteractionType
                    .GEOMETRIC_MEAN
                ),
                feature_values=[
                    freshness,
                    temporal,
                ],
                feature_confidences=[
                    self._feature_confidence(
                        features,
                        RankingSignalFamily
                        .FRESHNESS,
                    ),
                    self._feature_confidence(
                        features,
                        RankingSignalFamily
                        .TEMPORAL_RELEVANCE,
                    ),
                ],
                partial=partial,
            )
        )

        interactions.append(
            self._interaction(
                name="authority_graph",
                interaction_type=(
                    FeatureInteractionType
                    .GEOMETRIC_MEAN
                ),
                feature_values=[
                    authority,
                    graph,
                ],
                feature_confidences=[
                    self._feature_confidence(
                        features,
                        RankingSignalFamily
                        .QUERY_SPECIFIC_AUTHORITY,
                    ),
                    self._feature_confidence(
                        features,
                        RankingSignalFamily
                        .GRAPH_AUTHORITY,
                    ),
                ],
                partial=partial,
            )
        )

        interactions.append(
            self._interaction(
                name="relevance_quality",
                interaction_type=(
                    FeatureInteractionType
                    .GEOMETRIC_MEAN
                ),
                feature_values=[
                    relevance,
                    quality,
                ],
                feature_confidences=[
                    self._feature_confidence(
                        features,
                        RankingSignalFamily
                        .QUERY_DOCUMENT_COHERENCE,
                    ),
                    self._feature_confidence(
                        features,
                        RankingSignalFamily
                        .QUALITY,
                    ),
                ],
                partial=partial,
            )
        )

        return tuple(
            interactions[
                : self.policy.max_interactions
            ]
        )

    # ------------------------------------------------------------------
    # FAMILY SCORES
    # ------------------------------------------------------------------

    def _relevance_score(
        self,
        features: Sequence[
            RankingFeature
        ],
    ) -> float:

        families = (
            RankingSignalFamily.TEXT_RELEVANCE,
            RankingSignalFamily.SEMANTIC_RELEVANCE,
            RankingSignalFamily.QUERY_ALIGNMENT,
            RankingSignalFamily.FIELD_MATCH,
            RankingSignalFamily.PHRASE_MATCH,
            RankingSignalFamily.ENTITY_MATCH,
            RankingSignalFamily.INTENT_ALIGNMENT,
            RankingSignalFamily.QUERY_TYPE_ALIGNMENT,
            RankingSignalFamily.QUERY_DOCUMENT_COHERENCE,
        )

        values = [
            self._feature_value(
                features,
                family,
            )
            for family in families
        ]

        values = [
            value
            for value in values
            if value > 0.0
        ]

        if not values:
            return 0.0

        return self._clamp(
            sum(values)
            / len(values)
        )

    def _authority_score(
        self,
        features: Sequence[
            RankingFeature
        ],
    ) -> float:

        families = (
            RankingSignalFamily.AUTHORITY,
            RankingSignalFamily.DOMAIN_AUTHORITY,
            RankingSignalFamily.DOCUMENT_STRUCTURE,
            RankingSignalFamily.LINK_AUTHORITY,
            RankingSignalFamily.GRAPH_AUTHORITY,
            RankingSignalFamily.QUERY_SPECIFIC_AUTHORITY,
            RankingSignalFamily.TOPICAL_AUTHORITY,
        )

        values = [
            self._feature_value(
                features,
                family,
            )
            for family in families
        ]

        values = [
            value
            for value in values
            if value > 0.0
        ]

        if not values:
            return 0.0

        return self._clamp(
            sum(values)
            / len(values)
        )

    def _quality_score(
        self,
        features: Sequence[
            RankingFeature
        ],
    ) -> float:

        families = (
            RankingSignalFamily.QUALITY,
            RankingSignalFamily.TRUST,
        )

        values = [
            self._feature_value(
                features,
                family,
            )
            for family in families
        ]

        return self._clamp(
            sum(values)
            / len(values)
        ) if values else 0.0

    def _freshness_score(
        self,
        features: Sequence[
            RankingFeature
        ],
    ) -> float:

        values = [
            self._feature_value(
                features,
                RankingSignalFamily.FRESHNESS,
            ),
            self._feature_value(
                features,
                RankingSignalFamily
                .TEMPORAL_RELEVANCE,
            ),
        ]

        return self._clamp(
            sum(values)
            / len(values)
        )

    def _graph_score(
        self,
        features: Sequence[
            RankingFeature
        ],
    ) -> float:

        values = [
            self._feature_value(
                features,
                RankingSignalFamily
                .LINK_AUTHORITY,
            ),
            self._feature_value(
                features,
                RankingSignalFamily
                .GRAPH_AUTHORITY,
            ),
        ]

        return self._clamp(
            sum(values)
            / len(values)
        )

    def _diversity_score(
        self,
        features: Sequence[
            RankingFeature
        ],
    ) -> float:

        values = [
            self._feature_value(
                features,
                RankingSignalFamily
                .SOURCE_DIVERSITY,
            ),
            self._feature_value(
                features,
                RankingSignalFamily
                .EVIDENCE_DIVERSITY,
            ),
        ]

        return self._clamp(
            sum(values)
            / len(values)
        )

    # ------------------------------------------------------------------
    # AGGREGATION
    # ------------------------------------------------------------------

    def _aggregate(
        self,
        relevance: float,
        authority: float,
        quality: float,
        freshness: float,
        graph: float,
        diversity: float,
        query_specific: float,
        interaction: float,
    ) -> float:

        values = [
            (
                relevance,
                self.policy.relevance_weight,
            ),
            (
                authority,
                self.policy.authority_weight,
            ),
            (
                quality,
                self.policy.quality_weight,
            ),
            (
                freshness,
                self.policy.freshness_weight,
            ),
            (
                graph,
                self.policy.graph_weight,
            ),
            (
                diversity,
                self.policy.diversity_weight,
            ),
            (
                query_specific,
                self.policy.query_specific_weight,
            ),
            (
                interaction,
                self.policy.interaction_weight,
            ),
        ]

        numerator = sum(
            value * weight
            for value, weight
            in values
        )

        denominator = sum(
            weight
            for _, weight
            in values
        )

        if denominator <= 0.0:
            return 0.0

        return self._clamp(
            numerator / denominator
        )

    # ------------------------------------------------------------------
    # BUNDLE
    # ------------------------------------------------------------------

    def _build_bundle(
        self,
        query: RankingSignalInput,
        features: Sequence[
            RankingFeature
        ],
    ) -> RankingFeatureBundle:

        partial = (
            query.partial
            or any(
                feature.partial
                for feature in features
            )
        )

        interactions = self._build_interactions(
            features,
            partial,
        )

        relevance = self._relevance_score(
            features
        )

        authority = self._authority_score(
            features
        )

        quality = self._quality_score(
            features
        )

        freshness = self._freshness_score(
            features
        )

        graph = self._graph_score(
            features
        )

        diversity = self._diversity_score(
            features
        )

        query_specific = (
            self._feature_value(
                features,
                RankingSignalFamily
                .QUERY_SPECIFIC_AUTHORITY,
            )
        )

        interaction_score = (
            sum(
                interaction.value
                for interaction
                in interactions
            )
            / len(interactions)
            if interactions
            else 0.0
        )

        aggregate = self._aggregate(
            relevance=relevance,
            authority=authority,
            quality=quality,
            freshness=freshness,
            graph=graph,
            diversity=diversity,
            query_specific=query_specific,
            interaction=interaction_score,
        )

        confidence_values = [
            feature.confidence
            for feature in features
        ]

        confidence_values.extend(
            interaction.confidence
            for interaction
            in interactions
        )

        confidence = (
            sum(confidence_values)
            / len(confidence_values)
            if confidence_values
            else 0.0
        )

        return RankingFeatureBundle(
            query_id=query.query_id,
            document_id=query.document_id,

            features=tuple(features),

            interactions=tuple(
                interactions
            ),

            relevance_feature_score=relevance,

            authority_feature_score=authority,

            quality_feature_score=quality,

            freshness_feature_score=freshness,

            graph_feature_score=graph,

            diversity_feature_score=diversity,

            query_specific_score=query_specific,

            interaction_score=self._clamp(
                interaction_score
            ),

            aggregate_feature_score=aggregate,

            confidence=self._clamp(
                confidence
            ),

            partial=partial,

            feature_count=len(features),

            interaction_count=len(
                interactions
            ),
        )

    # ------------------------------------------------------------------
    # PUBLIC API
    # ------------------------------------------------------------------

    def fuse(
        self,
        input_data: RankingSignalInput,
        request_id: Optional[str] = None,
    ) -> FeatureFusionResult:

        request_id = (
            request_id
            or str(uuid.uuid4())
        )

        identity = FeatureFusionIdentity(
            request_id=request_id,
            query_id=input_data.query_id,
            document_id=input_data.document_id,
            canonical_url=input_data.canonical_url,
        )

        lineage = FeatureFusionLineage(
            query_stage="phase11",
            query_version=input_data.query_version,

            signal_stage="phase12",
            signal_version=(
                "phase12-ranking-signal-stack"
            ),

            authority_stage="phase12.3",
            authority_version=(
                "document-quality-authority-signals.v1"
            ),

            freshness_stage="phase12.4",
            freshness_version=(
                "freshness-temporal-relevance-signals.v1"
            ),

            graph_stage="phase12.5",
            graph_version=(
                "link-graph-authority-signals.v1"
            ),

            observed_at=(
                input_data.observed_at
                or self._now()
            ),
        )

        self._event(
            request_id,
            input_data.query_id,
            input_data.document_id,
            FeatureFusionEventType.REQUEST_RECEIVED,
            FeatureFusionState.RECEIVED,
        )

        self._event(
            request_id,
            input_data.query_id,
            input_data.document_id,
            FeatureFusionEventType.VALIDATION_STARTED,
            FeatureFusionState.VALIDATING,
        )

        if not input_data.query_id:

            self._event(
                request_id,
                input_data.query_id,
                input_data.document_id,
                FeatureFusionEventType.FUSION_REJECTED,
                FeatureFusionState.REJECTED,
                {
                    "reason":
                        "missing_query_id"
                },
            )

            raise ValueError(
                "query_id is required"
            )

        if not input_data.document_id:

            self._event(
                request_id,
                input_data.query_id,
                input_data.document_id,
                FeatureFusionEventType.FUSION_REJECTED,
                FeatureFusionState.REJECTED,
                {
                    "reason":
                        "missing_document_id"
                },
            )

            raise ValueError(
                "document_id is required"
            )

        self._event(
            request_id,
            input_data.query_id,
            input_data.document_id,
            FeatureFusionEventType
            .SIGNALS_NORMALIZATION_STARTED,
            FeatureFusionState
            .SIGNAL_NORMALIZATION,
            {
                "input_signal_count":
                    len(input_data.signals)
            },
        )

        features = self._normalize_signals(
            input_data.signals
        )

        self._event(
            request_id,
            input_data.query_id,
            input_data.document_id,
            FeatureFusionEventType
            .FEATURES_CONSTRUCTED,
            FeatureFusionState
            .FEATURE_CONSTRUCTION,
            {
                "feature_count":
                    len(features)
            },
        )

        self._event(
            request_id,
            input_data.query_id,
            input_data.document_id,
            FeatureFusionEventType
            .FEATURE_INTERACTIONS_CONSTRUCTED,
            FeatureFusionState
            .FEATURE_INTERACTION,
        )

        bundle = self._build_bundle(
            input_data,
            features,
        )

        self._event(
            request_id,
            input_data.query_id,
            input_data.document_id,
            FeatureFusionEventType
            .FEATURES_AGGREGATED,
            FeatureFusionState
            .FEATURE_AGGREGATION,
            {
                "aggregate_feature_score":
                    bundle.aggregate_feature_score
            },
        )

        self._event(
            request_id,
            input_data.query_id,
            input_data.document_id,
            FeatureFusionEventType
            .CONFIDENCE_CALCULATED,
            FeatureFusionState
            .CONFIDENCE_CALCULATION,
            {
                "confidence":
                    bundle.confidence
            },
        )

        state = (
            FeatureFusionState.PARTIAL
            if bundle.partial
            else FeatureFusionState.COMPLETED
        )

        if bundle.partial:

            self._event(
                request_id,
                input_data.query_id,
                input_data.document_id,
                FeatureFusionEventType
                .PARTIAL_INPUT_DETECTED,
                FeatureFusionState.PARTIAL,
            )

        result = FeatureFusionResult(
            identity=identity,
            lineage=lineage,
            bundle=bundle,
            state=state,
            completed_at=self._now(),
        )

        checkpoint = FeatureFusionCheckpoint(
            checkpoint_id=str(uuid.uuid4()),

            request_id=request_id,

            query_id=input_data.query_id,
            document_id=input_data.document_id,

            state=state,

            processed_signals=len(
                input_data.signals[
                    : self.policy.max_input_signals
                ]
            ),

            constructed_features=len(
                features
            ),

            constructed_interactions=len(
                bundle.interactions
            ),

            created_at=self._now(),
        )

        self.backend.persist_checkpoint(
            checkpoint
        )

        self._event(
            request_id,
            input_data.query_id,
            input_data.document_id,
            FeatureFusionEventType
            .CHECKPOINT_CREATED,
            state,
            {
                "checkpoint_id":
                    checkpoint.checkpoint_id
            },
        )

        self.backend.persist_result(
            result
        )

        self._event(
            request_id,
            input_data.query_id,
            input_data.document_id,
            FeatureFusionEventType
            .FUSION_COMPLETED,
            state,
        )

        return result

    # ------------------------------------------------------------------
    # BATCH
    # ------------------------------------------------------------------

    def fuse_many(
        self,
        inputs: Iterable[
            RankingSignalInput
        ],
    ) -> Tuple[
        FeatureFusionResult,
        ...
    ]:

        return tuple(
            self.fuse(
                input_data
            )
            for input_data in inputs
        )

    # ------------------------------------------------------------------
    # OBSERVABILITY
    # ------------------------------------------------------------------

    def events(
        self,
    ) -> Tuple[
        FeatureFusionEvent,
        ...
    ]:

        if hasattr(
            self.backend,
            "events",
        ):
            return self.backend.events()  # type: ignore[attr-defined]

        return ()

    def checkpoints(
        self,
    ) -> Tuple[
        FeatureFusionCheckpoint,
        ...
    ]:

        if hasattr(
            self.backend,
            "checkpoints",
        ):
            return self.backend.checkpoints()  # type: ignore[attr-defined]

        return ()

    # ------------------------------------------------------------------
    # ARCHITECTURE CONTRACT
    # ------------------------------------------------------------------

    @staticmethod
    def architecture() -> Mapping[str, object]:

        return {
            "architecture_version":
                ARCHITECTURE_VERSION,

            "stage":
                "12.7",

            "name":
                "Multi-Signal Ranking Feature Fusion",

            "scale_target":
                SCALE_TARGET,

            "google_scale_capability_target":
                GOOGLE_SCALE_CAPABILITY_TARGET,

            "google_technology_dependency":
                GOOGLE_TECHNOLOGY_DEPENDENCY,

            "purpose": (
                "Fuse independent ranking signals into a "
                "deterministic structured feature representation "
                "for downstream ranking."
            ),

            "pipeline_position": (
                "Phase 11 retrieval -> "
                "Phase 12 ranking signals -> "
                "feature fusion -> "
                "downstream ranking"
            ),

            "inputs": [
                "text relevance",
                "semantic relevance",
                "query alignment",

                "query term match",
                "field match",
                "phrase match",
                "entity match",
                "intent alignment",
                "query type alignment",

                "quality",
                "authority",
                "trust",

                "freshness",
                "temporal relevance",

                "link authority",
                "graph authority",

                "query-specific authority",
                "query-document coherence",
                "authority-relevance balance",

                "source diversity",
                "evidence diversity",
            ],

            "outputs": [
                "normalized ranking features",
                "feature interactions",
                "relevance feature score",
                "authority feature score",
                "quality feature score",
                "freshness feature score",
                "graph feature score",
                "diversity feature score",
                "query-specific feature score",
                "interaction score",
                "aggregate feature representation",
                "confidence",
                "partial-state metadata",
                "lineage",
                "checkpoint metadata",
            ],

            "distributed_design":
                True,

            "partition_local_computation":
                True,

            "query_partitionable":
                True,

            "document_partitionable":
                True,

            "feature_generation_parallelizable":
                True,

            "incremental_feature_updates_supported":
                True,

            "partial_input_supported":
                True,

            "checkpointable":
                True,

            "provenance_preserving":
                True,

            "backend_replaceable":
                True,

            "deterministic":
                True,

            "fixed_global_document_limit":
                False,

            "fixed_global_query_limit":
                False,

            "fixed_global_feature_limit":
                False,

            "fixed_global_worker_limit":
                False,

            "fixed_global_partition_limit":
                False,

            "final_ranking":
                False,

            "ranking_model_execution":
                False,

            "crawler_execution":
                False,

            "index_mutation":
                False,

            "spam_classification":
                False,

            "google_api_dependency":
                False,

            "next_stage":
                "12.8 Learning-to-Rank / Ranking Model Architecture",
        }


# ============================================================================
# PUBLIC ALIASES
# ============================================================================


MultiSignalRankingFeatureFusion = (
    MultiSignalRankingFeatureFusionArchitecture
)

GlobalMultiSignalRankingFeatureFusion = (
    MultiSignalRankingFeatureFusionArchitecture
)

Phase12_7MultiSignalRankingFeatureFusion = (
    MultiSignalRankingFeatureFusionArchitecture
)
