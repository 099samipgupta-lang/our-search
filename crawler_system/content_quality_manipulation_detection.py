"""
OUR SEARCH
Phase 14.2 — Content Quality & Manipulation Detection

Purpose
-------
Analyze Web documents for content-quality problems and manipulation patterns
that may be relevant to downstream spam, abuse, and quality decision systems.

This stage consumes document/content evidence and produces structured quality
and manipulation evidence.

It does NOT:
- make the final spam decision
- permanently classify a resource as spam
- remove documents
- delete URLs
- mutate the search index
- perform final ranking
- execute crawling
- perform HTTP requests
- assign crawler workers
- enforce penalties
- quarantine resources directly
- replace Phase 14.1
- replace security systems
- depend on Google Search
- depend on Google's index
- depend on Google's crawler
- depend on Google's infrastructure
- depend on Google's ranking technology

Scale target
------------
Designed directly for billions to trillions of publicly accessible Web
resources.

The architecture avoids imposing a fixed global ceiling on:
- resources
- URLs
- documents
- hosts
- domains
- partitions
- shards
- workers
- observations
- quality signals
- manipulation signals
- evidence records
- histories

Per-request safety limits exist only to bound individual processing units.

Design principles
-----------------
1. Separate content quality evidence from final enforcement.
2. Detect manipulation patterns rather than relying on one opaque score.
3. Preserve independent evidence and provenance.
4. Support document, URL, host, domain, and cross-resource analysis.
5. Detect both isolated and repeated manipulation patterns.
6. Preserve temporal evidence.
7. Support partial observations.
8. Be deterministic where possible.
9. Support checkpointing and restartability.
10. Remain distributed and partition-aware.
11. Keep final spam decisions in later stages.
12. Keep ranking independent from spam/quality detection.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import hashlib
import math
from typing import Any, Dict, Iterable, List, Mapping, Optional, Protocol, Sequence, Tuple


# ============================================================================
# ARCHITECTURE METADATA
# ============================================================================

SCALE_TARGET = "billions_to_trillions_of_public_web_resources"
GOOGLE_SCALE_CAPABILITY_TARGET = True
GOOGLE_TECHNOLOGY_DEPENDENCY = False

ARCHITECTURE_VERSION = "content-quality-manipulation-detection.v1"
PHASE = "14.2"
PREVIOUS_STAGE = "14.1"
NEXT_STAGE = "14.3"

PHASE_NAME = "Content Quality & Manipulation Detection"
NEXT_STAGE_NAME = "Link Spam / Graph Abuse Detection"


# ============================================================================
# ENUMERATIONS
# ============================================================================


class ContentQualityState(str, Enum):
    RECEIVED = "received"
    VALIDATING = "validating"
    NORMALIZING = "normalizing"
    CONTENT_ANALYSIS = "content_analysis"
    QUALITY_ANALYSIS = "quality_analysis"
    MANIPULATION_ANALYSIS = "manipulation_analysis"
    REPETITION_ANALYSIS = "repetition_analysis"
    CROSS_RESOURCE_ANALYSIS = "cross_resource_analysis"
    TEMPORAL_ANALYSIS = "temporal_analysis"
    EVIDENCE_AGGREGATION = "evidence_aggregation"
    CONFIDENCE_CALCULATION = "confidence_calculation"
    DECISION_READY = "decision_ready"
    COMPLETED = "completed"
    PARTIAL = "partial"
    DEFERRED = "deferred"
    REJECTED = "rejected"
    FAILED = "failed"


class ContentQualitySignalFamily(str, Enum):
    CONTENT_COMPLETENESS = "content_completeness"
    CONTENT_DEPTH = "content_depth"
    CONTENT_CLARITY = "content_clarity"
    CONTENT_STRUCTURE = "content_structure"
    CONTENT_INFORMATION_DENSITY = "content_information_density"
    CONTENT_UNIQUENESS = "content_uniqueness"
    CONTENT_REPETITION = "content_repetition"
    CONTENT_DUPLICATION = "content_duplication"
    CONTENT_TEMPLATE = "content_template"
    CONTENT_GENERATION = "content_generation"
    CONTENT_COHERENCE = "content_coherence"
    CONTENT_CONSISTENCY = "content_consistency"

    KEYWORD_MANIPULATION = "keyword_manipulation"
    PHRASE_MANIPULATION = "phrase_manipulation"
    TITLE_MANIPULATION = "title_manipulation"
    HEADING_MANIPULATION = "heading_manipulation"
    METADATA_MANIPULATION = "metadata_manipulation"

    DOORWAY_PATTERN = "doorway_pattern"
    THIN_CONTENT = "thin_content"
    LOW_VALUE_CONTENT = "low_value_content"
    SEARCH_ENGINE_MANIPULATION = "search_engine_manipulation"

    REDIRECT_MANIPULATION = "redirect_manipulation"
    CLOAKING_PATTERN = "cloaking_pattern"
    HIDDEN_CONTENT = "hidden_content"

    ADVERTISEMENT_MANIPULATION = "advertisement_manipulation"
    COMMERCIAL_MANIPULATION = "commercial_manipulation"

    DOCUMENT_BEHAVIOR = "document_behavior"
    URL_BEHAVIOR = "url_behavior"
    HOST_BEHAVIOR = "host_behavior"
    DOMAIN_BEHAVIOR = "domain_behavior"

    CROSS_RESOURCE = "cross_resource"
    CROSS_DOMAIN = "cross_domain"

    TEMPORAL = "temporal"
    HISTORICAL = "historical"
    SOURCE_REPUTATION = "source_reputation"
    IDENTITY = "identity"
    TECHNICAL = "technical"


class ContentQualitySignalType(str, Enum):
    LOW_CONTENT_LENGTH = "low_content_length"
    LOW_INFORMATION_DENSITY = "low_information_density"
    INCOMPLETE_CONTENT = "incomplete_content"
    SHALLOW_CONTENT = "shallow_content"
    LOW_CONTENT_CLARITY = "low_content_clarity"
    POOR_CONTENT_STRUCTURE = "poor_content_structure"
    HIGH_CONTENT_REPETITION = "high_content_repetition"
    LOW_CONTENT_UNIQUENESS = "low_content_uniqueness"
    NEAR_DUPLICATE_CONTENT = "near_duplicate_content"
    MASS_DUPLICATE_CONTENT = "mass_duplicate_content"
    TEMPLATE_HEAVY_CONTENT = "template_heavy_content"
    GENERATED_CONTENT_PATTERN = "generated_content_pattern"
    CONTENT_INCOHERENCE = "content_incoherence"
    CONTENT_INCONSISTENCY = "content_inconsistency"

    KEYWORD_STUFFING = "keyword_stuffing"
    PHRASE_STUFFING = "phrase_stuffing"
    TITLE_KEYWORD_STUFFING = "title_keyword_stuffing"
    MISLEADING_TITLE = "misleading_title"
    HEADING_KEYWORD_STUFFING = "heading_keyword_stuffing"
    METADATA_KEYWORD_STUFFING = "metadata_keyword_stuffing"

    DOORWAY_PAGE = "doorway_page"
    THIN_PAGE = "thin_page"
    LOW_VALUE_PAGE = "low_value_page"
    SEARCH_ENGINE_TARGETING = "search_engine_targeting"

    SUSPICIOUS_REDIRECT = "suspicious_redirect"
    REDIRECT_CHAIN = "redirect_chain"
    CLOAKING = "cloaking"
    HIDDEN_TEXT = "hidden_text"
    HIDDEN_LINK = "hidden_link"

    AD_DENSITY_ANOMALY = "ad_density_anomaly"
    COMMERCIAL_CONTENT_ANOMALY = "commercial_content_anomaly"

    RAPID_DOCUMENT_GENERATION = "rapid_document_generation"
    RAPID_URL_GENERATION = "rapid_url_generation"
    RAPID_TEMPLATE_GENERATION = "rapid_template_generation"

    CROSS_RESOURCE_REUSE = "cross_resource_reuse"
    CROSS_DOMAIN_REUSE = "cross_domain_reuse"
    SHARED_MANIPULATION_PATTERN = "shared_manipulation_pattern"

    TEMPORAL_CONTENT_ANOMALY = "temporal_content_anomaly"
    HISTORICAL_QUALITY_PATTERN = "historical_quality_pattern"

    SOURCE_IDENTITY_INCONSISTENCY = "source_identity_inconsistency"
    TECHNICAL_CONTENT_ANOMALY = "technical_content_anomaly"


class ContentQualityEvidenceStrength(str, Enum):
    NONE = "none"
    VERY_WEAK = "very_weak"
    WEAK = "weak"
    MODERATE = "moderate"
    STRONG = "strong"
    VERY_STRONG = "very_strong"


class ContentQualityEvidenceDirection(str, Enum):
    QUALITY_POSITIVE = "quality_positive"
    NEUTRAL = "neutral"
    MANIPULATION_POSITIVE = "manipulation_positive"


class ContentQualityScope(str, Enum):
    DOCUMENT = "document"
    URL = "url"
    HOST = "host"
    DOMAIN = "domain"
    SUBDOMAIN = "subdomain"
    RESOURCE_GROUP = "resource_group"
    PARTITION = "partition"
    SHARD = "shard"
    GLOBAL = "global"


class ContentQualityBand(str, Enum):
    UNKNOWN = "unknown"
    VERY_LOW = "very_low"
    LOW = "low"
    MODERATE = "moderate"
    HIGH = "high"
    VERY_HIGH = "very_high"


class ManipulationBand(str, Enum):
    UNKNOWN = "unknown"
    MINIMAL = "minimal"
    LOW = "low"
    MODERATE = "moderate"
    HIGH = "high"
    VERY_HIGH = "very_high"
    EXTREME = "extreme"


class ContentTemporalState(str, Enum):
    UNKNOWN = "unknown"
    NEW = "new"
    STABLE = "stable"
    IMPROVING = "improving"
    DEGRADING = "degrading"
    RECENT_SPIKE = "recent_spike"
    PERSISTENT = "persistent"


class ContentQualityEventType(str, Enum):
    REQUEST_RECEIVED = "request_received"
    VALIDATION_STARTED = "validation_started"
    INPUT_NORMALIZED = "input_normalized"
    CONTENT_ANALYSIS_STARTED = "content_analysis_started"
    CONTENT_ANALYSIS_COMPLETED = "content_analysis_completed"
    QUALITY_ANALYSIS_STARTED = "quality_analysis_started"
    QUALITY_ANALYSIS_COMPLETED = "quality_analysis_completed"
    MANIPULATION_ANALYSIS_STARTED = "manipulation_analysis_started"
    MANIPULATION_ANALYSIS_COMPLETED = "manipulation_analysis_completed"
    REPETITION_ANALYSIS_STARTED = "repetition_analysis_started"
    REPETITION_ANALYSIS_COMPLETED = "repetition_analysis_completed"
    CROSS_RESOURCE_ANALYSIS_STARTED = "cross_resource_analysis_started"
    CROSS_RESOURCE_ANALYSIS_COMPLETED = "cross_resource_analysis_completed"
    TEMPORAL_ANALYSIS_STARTED = "temporal_analysis_started"
    TEMPORAL_ANALYSIS_COMPLETED = "temporal_analysis_completed"
    EVIDENCE_AGGREGATED = "evidence_aggregated"
    CONFIDENCE_CALCULATION_STARTED = "confidence_calculation_started"
    CONFIDENCE_CALCULATED = "confidence_calculated"
    PARTIAL_INPUT_DETECTED = "partial_input_detected"
    CHECKPOINT_CREATED = "checkpoint_created"
    ANALYSIS_COMPLETED = "analysis_completed"
    ANALYSIS_DEFERRED = "analysis_deferred"
    ANALYSIS_REJECTED = "analysis_rejected"
    ANALYSIS_FAILED = "analysis_failed"


class ContentQualityCheckpointType(str, Enum):
    INPUT_ACCEPTED = "input_accepted"
    INPUT_NORMALIZED = "input_normalized"
    CONTENT_ANALYZED = "content_analyzed"
    QUALITY_ANALYZED = "quality_analyzed"
    MANIPULATION_ANALYZED = "manipulation_analyzed"
    REPETITION_ANALYZED = "repetition_analyzed"
    CROSS_RESOURCE_ANALYZED = "cross_resource_analyzed"
    TEMPORAL_ANALYZED = "temporal_analyzed"
    EVIDENCE_AGGREGATED = "evidence_aggregated"
    CONFIDENCE_CALCULATED = "confidence_calculated"
    COMPLETED = "completed"


# ============================================================================
# IDENTITY / LINEAGE
# ============================================================================


@dataclass(frozen=True)
class ContentQualityIdentity:
    resource_id: str
    document_id: str = ""
    url: str = ""
    host_id: str = ""
    domain_id: str = ""
    partition_id: str = ""
    shard_id: str = ""
    region_id: str = ""

    def key(self) -> str:
        return ":".join(
            [
                self.resource_id,
                self.document_id,
                self.host_id,
                self.domain_id,
                self.partition_id,
                self.shard_id,
                self.region_id,
            ]
        )


@dataclass
class ContentQualityLineage:
    resource_id: str

    previous_stage: str = PREVIOUS_STAGE
    current_stage: str = PHASE

    source_document_version: str = ""
    source_content_version: str = ""
    source_crawl_version: str = ""
    source_freshness_version: str = ""
    source_spam_signal_version: str = ""

    parent_resource_ids: List[str] = field(default_factory=list)
    parent_signal_ids: List[str] = field(default_factory=list)

    lineage_metadata: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# CONTENT OBSERVATION
# ============================================================================


@dataclass
class ContentQualityObservation:
    signal_id: str

    signal_type: ContentQualitySignalType
    family: ContentQualitySignalFamily
    scope: ContentQualityScope

    value: float = 0.0
    confidence: float = 0.0

    evidence_strength: ContentQualityEvidenceStrength = (
        ContentQualityEvidenceStrength.NONE
    )

    direction: ContentQualityEvidenceDirection = (
        ContentQualityEvidenceDirection.NEUTRAL
    )

    observation_count: int = 1

    observed_at: str = ""

    source_id: str = ""
    source_version: str = ""

    feature_metadata: Dict[str, Any] = field(default_factory=dict)

    partial: bool = False


# ============================================================================
# DOCUMENT CONTENT PROFILE
# ============================================================================


@dataclass
class DocumentContentProfile:
    text_length: int = 0
    word_count: int = 0
    sentence_count: int = 0

    paragraph_count: int = 0
    heading_count: int = 0
    list_count: int = 0

    title_length: int = 0
    metadata_length: int = 0

    unique_term_count: int = 0
    repeated_term_count: int = 0

    repeated_phrase_count: int = 0

    internal_link_count: int = 0
    external_link_count: int = 0

    image_count: int = 0
    media_count: int = 0

    advertisement_count: int = 0

    hidden_element_count: int = 0
    suspicious_element_count: int = 0

    template_token_count: int = 0
    boilerplate_token_count: int = 0

    duplicate_similarity: float = 0.0
    template_similarity: float = 0.0

    semantic_coherence: float = 0.0
    semantic_consistency: float = 0.0

    information_density: float = 0.0
    structural_quality: float = 0.0
    content_uniqueness: float = 0.0

    partial: bool = False


# ============================================================================
# CONTENT HISTORY
# ============================================================================


@dataclass
class ContentQualityHistory:
    observation_count: int = 0

    previous_quality_score: float = 0.0
    average_quality_score: float = 0.0
    minimum_quality_score: float = 0.0
    maximum_quality_score: float = 0.0

    previous_manipulation_score: float = 0.0
    average_manipulation_score: float = 0.0
    maximum_manipulation_score: float = 0.0

    quality_improvement_count: int = 0
    quality_degradation_count: int = 0

    manipulation_observation_count: int = 0
    persistent_manipulation_count: int = 0
    recent_manipulation_spike_count: int = 0

    duplicate_observation_count: int = 0
    template_observation_count: int = 0

    first_observed_timestamp: str = ""
    last_observed_timestamp: str = ""
    last_change_timestamp: str = ""

    historical_stability: float = 0.0
    historical_volatility: float = 0.0

    freshness_accuracy: float = 0.0

    history_version: str = ""

    partial: bool = False


# ============================================================================
# TEMPORAL EVIDENCE
# ============================================================================


@dataclass
class ContentQualityTemporalEvidence:
    recent_change: float = 0.0
    recent_quality_change: float = 0.0
    recent_manipulation_change: float = 0.0

    acceleration: float = 0.0
    persistence: float = 0.0

    volatility: float = 0.0
    stability: float = 0.0

    temporal_state: ContentTemporalState = (
        ContentTemporalState.UNKNOWN
    )

    confidence: float = 0.0
    partial: bool = False


# ============================================================================
# CROSS-RESOURCE EVIDENCE
# ============================================================================


@dataclass
class CrossResourceContentEvidence:
    related_resource_count: int = 0
    related_document_count: int = 0
    related_host_count: int = 0
    related_domain_count: int = 0

    shared_content_score: float = 0.0
    shared_template_score: float = 0.0
    shared_metadata_score: float = 0.0
    shared_manipulation_score: float = 0.0

    cross_domain_reuse_score: float = 0.0
    cross_resource_concentration: float = 0.0
    cross_resource_diversity: float = 0.0

    confidence: float = 0.0
    partial: bool = False


# ============================================================================
# EVIDENCE BUNDLES
# ============================================================================


@dataclass
class ContentQualityEvidenceBundle:
    family: ContentQualitySignalFamily

    quality_score: float = 0.0
    manipulation_score: float = 0.0

    confidence: float = 0.0

    observation_count: int = 0
    supporting_signal_count: int = 0

    strongest_signal_type: Optional[ContentQualitySignalType] = None
    strongest_signal_value: float = 0.0

    quality_positive_evidence: float = 0.0
    manipulation_positive_evidence: float = 0.0

    temporal_state: ContentTemporalState = (
        ContentTemporalState.UNKNOWN
    )

    partial: bool = False


# ============================================================================
# POLICY
# ============================================================================


@dataclass
class ContentQualityManipulationPolicy:
    max_observations_per_resource: int = 512

    max_related_resources: int = 100000
    max_related_documents: int = 100000
    max_related_hosts: int = 100000
    max_related_domains: int = 100000

    max_families: int = 128
    max_signals_per_family: int = 128

    minimum_confidence: float = 0.10

    allow_partial: bool = True
    deterministic: bool = True
    checkpoint_enabled: bool = True

    content_quality_weight: float = 1.00
    content_structure_weight: float = 0.90
    content_uniqueness_weight: float = 1.10
    content_coherence_weight: float = 1.00

    repetition_weight: float = 1.10
    duplication_weight: float = 1.15
    template_weight: float = 1.00
    generation_pattern_weight: float = 1.15

    keyword_manipulation_weight: float = 1.25
    title_manipulation_weight: float = 1.00
    metadata_manipulation_weight: float = 0.90

    doorway_weight: float = 1.25
    cloaking_weight: float = 1.30
    hidden_content_weight: float = 1.20
    redirect_weight: float = 1.15

    commercial_manipulation_weight: float = 0.80
    behavioral_weight: float = 0.90

    cross_resource_weight: float = 1.15
    cross_domain_weight: float = 1.20

    temporal_weight: float = 0.90
    historical_weight: float = 1.00
    technical_weight: float = 0.70

    partial_penalty: float = 0.05

    quality_low_threshold: float = 0.25
    quality_moderate_threshold: float = 0.50
    quality_high_threshold: float = 0.75
    quality_very_high_threshold: float = 0.90

    manipulation_low_threshold: float = 0.15
    manipulation_moderate_threshold: float = 0.40
    manipulation_high_threshold: float = 0.70
    manipulation_very_high_threshold: float = 0.90


# ============================================================================
# INPUT
# ============================================================================


@dataclass
class ContentQualityManipulationInput:
    identity: ContentQualityIdentity

    content_profile: Optional[DocumentContentProfile] = None

    observations: List[ContentQualityObservation] = field(
        default_factory=list
    )

    history: Optional[ContentQualityHistory] = None

    temporal_evidence: Optional[ContentQualityTemporalEvidence] = None

    cross_resource_evidence: Optional[CrossResourceContentEvidence] = None

    evidence_sources: List[str] = field(default_factory=list)

    lineage: Optional[ContentQualityLineage] = None

    signal_version: str = ARCHITECTURE_VERSION

    confidence: float = 0.0
    partial: bool = False

    metadata: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# RESULT / CHECKPOINT / EVENT
# ============================================================================


@dataclass
class ContentQualityManipulationResult:
    result_id: str

    identity: ContentQualityIdentity

    state: ContentQualityState

    content_quality_score: float = 0.0
    manipulation_score: float = 0.0

    quality_band: ContentQualityBand = ContentQualityBand.UNKNOWN
    manipulation_band: ManipulationBand = ManipulationBand.UNKNOWN

    observations: List[ContentQualityObservation] = field(
        default_factory=list
    )

    evidence_bundles: List[ContentQualityEvidenceBundle] = field(
        default_factory=list
    )

    content_profile: Optional[DocumentContentProfile] = None

    temporal_evidence: Optional[ContentQualityTemporalEvidence] = None

    cross_resource_evidence: Optional[CrossResourceContentEvidence] = None

    aggregate_confidence: float = 0.0

    strongest_quality_family: Optional[ContentQualitySignalFamily] = None
    strongest_manipulation_family: Optional[ContentQualitySignalFamily] = None

    strongest_signal_type: Optional[ContentQualitySignalType] = None

    partial: bool = False

    lineage: Optional[ContentQualityLineage] = None

    evidence_sources: List[str] = field(default_factory=list)

    created_at: str = ""

    architecture_version: str = ARCHITECTURE_VERSION


@dataclass
class ContentQualityManipulationCheckpoint:
    checkpoint_id: str

    identity: ContentQualityIdentity

    checkpoint_type: ContentQualityCheckpointType

    state: ContentQualityState

    observation_count: int = 0
    family_count: int = 0

    quality_score: float = 0.0
    manipulation_score: float = 0.0
    confidence: float = 0.0

    partial: bool = False

    created_at: str = ""

    architecture_version: str = ARCHITECTURE_VERSION


@dataclass
class ContentQualityManipulationEvent:
    event_id: str

    identity: ContentQualityIdentity

    event_type: ContentQualityEventType

    state: ContentQualityState

    timestamp: str

    metadata: Dict[str, Any] = field(default_factory=dict)

    architecture_version: str = ARCHITECTURE_VERSION


# ============================================================================
# BACKEND
# ============================================================================


class ContentQualityManipulationBackend(Protocol):
    def persist_event(
        self,
        event: ContentQualityManipulationEvent,
    ) -> None:
        ...

    def persist_checkpoint(
        self,
        checkpoint: ContentQualityManipulationCheckpoint,
    ) -> None:
        ...

    def persist_result(
        self,
        result: ContentQualityManipulationResult,
    ) -> None:
        ...

    def get_result(
        self,
        result_id: str,
    ) -> Optional[ContentQualityManipulationResult]:
        ...


class InMemoryContentQualityManipulationMetadata:
    """
    Replaceable reference backend.

    Production deployments can replace this backend with distributed durable
    storage without changing the architecture contract.
    """

    def __init__(self) -> None:
        self._events: Dict[str, ContentQualityManipulationEvent] = {}
        self._checkpoints: Dict[
            str,
            ContentQualityManipulationCheckpoint,
        ] = {}
        self._results: Dict[
            str,
            ContentQualityManipulationResult,
        ] = {}

    def persist_event(
        self,
        event: ContentQualityManipulationEvent,
    ) -> None:
        self._events[event.event_id] = event

    def persist_checkpoint(
        self,
        checkpoint: ContentQualityManipulationCheckpoint,
    ) -> None:
        self._checkpoints[checkpoint.checkpoint_id] = checkpoint

    def persist_result(
        self,
        result: ContentQualityManipulationResult,
    ) -> None:
        self._results[result.result_id] = result

    def get_result(
        self,
        result_id: str,
    ) -> Optional[ContentQualityManipulationResult]:
        return self._results.get(result_id)

    def events(self) -> List[ContentQualityManipulationEvent]:
        return list(self._events.values())

    def checkpoints(
        self,
    ) -> List[ContentQualityManipulationCheckpoint]:
        return list(self._checkpoints.values())

    def results(
        self,
    ) -> List[ContentQualityManipulationResult]:
        return list(self._results.values())


# ============================================================================
# ARCHITECTURE
# ============================================================================


class ContentQualityManipulationDetectionArchitecture:
    """
    Phase 14.2 architecture.

    Generates content-quality and manipulation evidence for downstream
    spam/abuse/quality decision systems.

    This class deliberately does not perform final enforcement.
    """

    def __init__(
        self,
        backend: Optional[ContentQualityManipulationBackend] = None,
        policy: Optional[ContentQualityManipulationPolicy] = None,
    ) -> None:
        self.backend = (
            backend
            or InMemoryContentQualityManipulationMetadata()
        )

        self.policy = (
            policy
            or ContentQualityManipulationPolicy()
        )

    # ------------------------------------------------------------------------
    # BASIC UTILITIES
    # ------------------------------------------------------------------------

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _clamp(
        value: float,
        minimum: float = 0.0,
        maximum: float = 1.0,
    ) -> float:

        try:
            value = float(value)
        except (TypeError, ValueError):
            return minimum

        if math.isnan(value) or math.isinf(value):
            return minimum

        return max(
            minimum,
            min(maximum, value),
        )

    @staticmethod
    def _safe_int(
        value: Any,
        default: int = 0,
    ) -> int:

        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _digest_payload(
        payload: str,
    ) -> str:

        return hashlib.sha256(
            payload.encode("utf-8")
        ).hexdigest()

    @staticmethod
    def _deterministic_unit(
        value: str,
    ) -> float:

        digest = hashlib.sha256(
            value.encode("utf-8")
        ).hexdigest()

        integer = int(
            digest[:16],
            16,
        )

        return integer / float(
            0xFFFFFFFFFFFFFFFF
        )

    # ------------------------------------------------------------------------
    # EVENT / CHECKPOINT
    # ------------------------------------------------------------------------

    def _event(
        self,
        identity: ContentQualityIdentity,
        event_type: ContentQualityEventType,
        state: ContentQualityState,
        **metadata: Any,
    ) -> ContentQualityManipulationEvent:

        timestamp = self._now()

        event_id = self._digest_payload(
            "|".join(
                [
                    identity.key(),
                    event_type.value,
                    state.value,
                    timestamp,
                ]
            )
        )

        event = ContentQualityManipulationEvent(
            event_id=event_id,
            identity=identity,
            event_type=event_type,
            state=state,
            timestamp=timestamp,
            metadata=metadata,
        )

        self.backend.persist_event(event)

        return event

    def _checkpoint(
        self,
        identity: ContentQualityIdentity,
        checkpoint_type: ContentQualityCheckpointType,
        state: ContentQualityState,
        observation_count: int,
        family_count: int,
        quality_score: float,
        manipulation_score: float,
        confidence: float,
        partial: bool,
    ) -> ContentQualityManipulationCheckpoint:

        checkpoint_id = self._digest_payload(
            "|".join(
                [
                    identity.key(),
                    checkpoint_type.value,
                    str(observation_count),
                    str(family_count),
                    str(quality_score),
                    str(manipulation_score),
                    ARCHITECTURE_VERSION,
                ]
            )
        )

        checkpoint = ContentQualityManipulationCheckpoint(
            checkpoint_id=checkpoint_id,
            identity=identity,
            checkpoint_type=checkpoint_type,
            state=state,
            observation_count=observation_count,
            family_count=family_count,
            quality_score=self._clamp(quality_score),
            manipulation_score=self._clamp(manipulation_score),
            confidence=self._clamp(confidence),
            partial=partial,
            created_at=self._now(),
        )

        if self.policy.checkpoint_enabled:
            self.backend.persist_checkpoint(checkpoint)

        return checkpoint

    # ------------------------------------------------------------------------
    # NORMALIZATION
    # ------------------------------------------------------------------------

    def _strength(
        self,
        value: float,
    ) -> ContentQualityEvidenceStrength:

        value = self._clamp(value)

        if value >= 0.90:
            return ContentQualityEvidenceStrength.VERY_STRONG

        if value >= 0.70:
            return ContentQualityEvidenceStrength.STRONG

        if value >= 0.40:
            return ContentQualityEvidenceStrength.MODERATE

        if value >= 0.15:
            return ContentQualityEvidenceStrength.WEAK

        if value > 0.0:
            return ContentQualityEvidenceStrength.VERY_WEAK

        return ContentQualityEvidenceStrength.NONE

    def _normalize_observation(
        self,
        observation: ContentQualityObservation,
    ) -> ContentQualityObservation:

        return ContentQualityObservation(
            signal_id=observation.signal_id.strip(),
            signal_type=observation.signal_type,
            family=observation.family,
            scope=observation.scope,
            value=self._clamp(observation.value),
            confidence=self._clamp(observation.confidence),
            evidence_strength=self._strength(
                observation.value
            ),
            direction=observation.direction,
            observation_count=max(
                1,
                self._safe_int(
                    observation.observation_count,
                    1,
                ),
            ),
            observed_at=(
                observation.observed_at
                or self._now()
            ),
            source_id=observation.source_id,
            source_version=observation.source_version,
            feature_metadata=dict(
                observation.feature_metadata
            ),
            partial=bool(
                observation.partial
            ),
        )

    def _normalize_observations(
        self,
        observations: Sequence[ContentQualityObservation],
    ) -> List[ContentQualityObservation]:

        normalized: List[
            ContentQualityObservation
        ] = []

        for observation in observations[
            : self.policy.max_observations_per_resource
        ]:
            normalized.append(
                self._normalize_observation(
                    observation
                )
            )

        return normalized

    def _normalize_profile(
        self,
        profile: Optional[DocumentContentProfile],
    ) -> DocumentContentProfile:

        if profile is None:
            return DocumentContentProfile(
                partial=True
            )

        integer_fields = {
            "text_length",
            "word_count",
            "sentence_count",
            "paragraph_count",
            "heading_count",
            "list_count",
            "title_length",
            "metadata_length",
            "unique_term_count",
            "repeated_term_count",
            "repeated_phrase_count",
            "internal_link_count",
            "external_link_count",
            "image_count",
            "media_count",
            "advertisement_count",
            "hidden_element_count",
            "suspicious_element_count",
            "template_token_count",
            "boilerplate_token_count",
        }

        values: Dict[str, Any] = {}

        for name in integer_fields:
            values[name] = max(
                0,
                self._safe_int(
                    getattr(profile, name),
                    0,
                ),
            )

        float_fields = {
            "duplicate_similarity",
            "template_similarity",
            "semantic_coherence",
            "semantic_consistency",
            "information_density",
            "structural_quality",
            "content_uniqueness",
        }

        for name in float_fields:
            values[name] = self._clamp(
                getattr(profile, name)
            )

        values["partial"] = bool(
            profile.partial
        )

        return DocumentContentProfile(
            **values
        )

    def _normalize_history(
        self,
        history: Optional[ContentQualityHistory],
    ) -> ContentQualityHistory:

        if history is None:
            return ContentQualityHistory(
                partial=True
            )

        return ContentQualityHistory(
            observation_count=max(
                0,
                self._safe_int(
                    history.observation_count
                ),
            ),
            previous_quality_score=self._clamp(
                history.previous_quality_score
            ),
            average_quality_score=self._clamp(
                history.average_quality_score
            ),
            minimum_quality_score=self._clamp(
                history.minimum_quality_score
            ),
            maximum_quality_score=self._clamp(
                history.maximum_quality_score
            ),
            previous_manipulation_score=self._clamp(
                history.previous_manipulation_score
            ),
            average_manipulation_score=self._clamp(
                history.average_manipulation_score
            ),
            maximum_manipulation_score=self._clamp(
                history.maximum_manipulation_score
            ),
            quality_improvement_count=max(
                0,
                self._safe_int(
                    history.quality_improvement_count
                ),
            ),
            quality_degradation_count=max(
                0,
                self._safe_int(
                    history.quality_degradation_count
                ),
            ),
            manipulation_observation_count=max(
                0,
                self._safe_int(
                    history.manipulation_observation_count
                ),
            ),
            persistent_manipulation_count=max(
                0,
                self._safe_int(
                    history.persistent_manipulation_count
                ),
            ),
            recent_manipulation_spike_count=max(
                0,
                self._safe_int(
                    history.recent_manipulation_spike_count
                ),
            ),
            duplicate_observation_count=max(
                0,
                self._safe_int(
                    history.duplicate_observation_count
                ),
            ),
            template_observation_count=max(
                0,
                self._safe_int(
                    history.template_observation_count
                ),
            ),
            first_observed_timestamp=(
                history.first_observed_timestamp
            ),
            last_observed_timestamp=(
                history.last_observed_timestamp
            ),
            last_change_timestamp=(
                history.last_change_timestamp
            ),
            historical_stability=self._clamp(
                history.historical_stability
            ),
            historical_volatility=self._clamp(
                history.historical_volatility
            ),
            freshness_accuracy=self._clamp(
                history.freshness_accuracy
            ),
            history_version=history.history_version,
            partial=bool(history.partial),
        )

    def _normalize_temporal(
        self,
        temporal: Optional[ContentQualityTemporalEvidence],
    ) -> ContentQualityTemporalEvidence:

        if temporal is None:
            return ContentQualityTemporalEvidence(
                partial=True
            )

        return ContentQualityTemporalEvidence(
            recent_change=self._clamp(
                temporal.recent_change
            ),
            recent_quality_change=self._clamp(
                temporal.recent_quality_change
            ),
            recent_manipulation_change=self._clamp(
                temporal.recent_manipulation_change
            ),
            acceleration=self._clamp(
                temporal.acceleration
            ),
            persistence=self._clamp(
                temporal.persistence
            ),
            volatility=self._clamp(
                temporal.volatility
            ),
            stability=self._clamp(
                temporal.stability
            ),
            temporal_state=temporal.temporal_state,
            confidence=self._clamp(
                temporal.confidence
            ),
            partial=bool(
                temporal.partial
            ),
        )

    def _normalize_cross_resource(
        self,
        evidence: Optional[CrossResourceContentEvidence],
    ) -> CrossResourceContentEvidence:

        if evidence is None:
            return CrossResourceContentEvidence(
                partial=True
            )

        return CrossResourceContentEvidence(
            related_resource_count=max(
                0,
                self._safe_int(
                    evidence.related_resource_count
                ),
            ),
            related_document_count=max(
                0,
                self._safe_int(
                    evidence.related_document_count
                ),
            ),
            related_host_count=max(
                0,
                self._safe_int(
                    evidence.related_host_count
                ),
            ),
            related_domain_count=max(
                0,
                self._safe_int(
                    evidence.related_domain_count
                ),
            ),
            shared_content_score=self._clamp(
                evidence.shared_content_score
            ),
            shared_template_score=self._clamp(
                evidence.shared_template_score
            ),
            shared_metadata_score=self._clamp(
                evidence.shared_metadata_score
            ),
            shared_manipulation_score=self._clamp(
                evidence.shared_manipulation_score
            ),
            cross_domain_reuse_score=self._clamp(
                evidence.cross_domain_reuse_score
            ),
            cross_resource_concentration=self._clamp(
                evidence.cross_resource_concentration
            ),
            cross_resource_diversity=self._clamp(
                evidence.cross_resource_diversity
            ),
            confidence=self._clamp(
                evidence.confidence
            ),
            partial=bool(
                evidence.partial
            ),
        )

    def _normalize_input(
        self,
        request: ContentQualityManipulationInput,
    ) -> ContentQualityManipulationInput:

        profile = self._normalize_profile(
            request.content_profile
        )

        observations = self._normalize_observations(
            request.observations
        )

        history = self._normalize_history(
            request.history
        )

        temporal = self._normalize_temporal(
            request.temporal_evidence
        )

        cross_resource = self._normalize_cross_resource(
            request.cross_resource_evidence
        )

        partial = bool(
            request.partial
            or profile.partial
            or history.partial
            or temporal.partial
            or cross_resource.partial
            or any(
                observation.partial
                for observation in observations
            )
        )

        return ContentQualityManipulationInput(
            identity=request.identity,
            content_profile=profile,
            observations=observations,
            history=history,
            temporal_evidence=temporal,
            cross_resource_evidence=cross_resource,
            evidence_sources=list(
                request.evidence_sources
            ),
            lineage=request.lineage
            or ContentQualityLineage(
                resource_id=request.identity.resource_id
            ),
            signal_version=(
                request.signal_version
                or ARCHITECTURE_VERSION
            ),
            confidence=self._clamp(
                request.confidence
            ),
            partial=partial,
            metadata=dict(request.metadata),
        )

    # ------------------------------------------------------------------------
    # VALIDATION
    # ------------------------------------------------------------------------

    def validate(
        self,
        request: ContentQualityManipulationInput,
    ) -> Tuple[bool, List[str]]:

        errors: List[str] = []

        if not request.identity.resource_id.strip():
            errors.append(
                "resource_id is required"
            )

        if (
            len(request.observations)
            > self.policy.max_observations_per_resource
        ):
            errors.append(
                "observation count exceeds per-resource safety limit"
            )

        for observation in request.observations[
            : self.policy.max_observations_per_resource
        ]:
            if not observation.signal_id:
                errors.append(
                    "signal_id is required"
                )

            if not isinstance(
                observation.signal_type,
                ContentQualitySignalType,
            ):
                errors.append(
                    "invalid signal_type"
                )

            if not isinstance(
                observation.family,
                ContentQualitySignalFamily,
            ):
                errors.append(
                    "invalid signal family"
                )

            if not isinstance(
                observation.scope,
                ContentQualityScope,
            ):
                errors.append(
                    "invalid signal scope"
                )

        if (
            request.partial
            and not self.policy.allow_partial
        ):
            errors.append(
                "partial input is disabled by policy"
            )

        return not errors, errors

    # ------------------------------------------------------------------------
    # FAMILY WEIGHTS
    # ------------------------------------------------------------------------

    def _family_weight(
        self,
        family: ContentQualitySignalFamily,
    ) -> float:

        if family in {
            ContentQualitySignalFamily.CONTENT_COMPLETENESS,
            ContentQualitySignalFamily.CONTENT_DEPTH,
            ContentQualitySignalFamily.CONTENT_INFORMATION_DENSITY,
            ContentQualitySignalFamily.CONTENT_CLARITY,
        }:
            return self.policy.content_quality_weight

        if family in {
            ContentQualitySignalFamily.CONTENT_STRUCTURE,
        }:
            return self.policy.content_structure_weight

        if family in {
            ContentQualitySignalFamily.CONTENT_UNIQUENESS,
        }:
            return self.policy.content_uniqueness_weight

        if family in {
            ContentQualitySignalFamily.CONTENT_COHERENCE,
            ContentQualitySignalFamily.CONTENT_CONSISTENCY,
        }:
            return self.policy.content_coherence_weight

        if family in {
            ContentQualitySignalFamily.CONTENT_REPETITION,
        }:
            return self.policy.repetition_weight

        if family in {
            ContentQualitySignalFamily.CONTENT_DUPLICATION,
        }:
            return self.policy.duplication_weight

        if family in {
            ContentQualitySignalFamily.CONTENT_TEMPLATE,
        }:
            return self.policy.template_weight

        if family in {
            ContentQualitySignalFamily.CONTENT_GENERATION,
        }:
            return self.policy.generation_pattern_weight

        if family in {
            ContentQualitySignalFamily.KEYWORD_MANIPULATION,
            ContentQualitySignalFamily.PHRASE_MANIPULATION,
        }:
            return self.policy.keyword_manipulation_weight

        if family in {
            ContentQualitySignalFamily.TITLE_MANIPULATION,
            ContentQualitySignalFamily.HEADING_MANIPULATION,
        }:
            return self.policy.title_manipulation_weight

        if family in {
            ContentQualitySignalFamily.METADATA_MANIPULATION,
        }:
            return self.policy.metadata_manipulation_weight

        if family in {
            ContentQualitySignalFamily.DOORWAY_PATTERN,
        }:
            return self.policy.doorway_weight

        if family in {
            ContentQualitySignalFamily.CLOAKING_PATTERN,
        }:
            return self.policy.cloaking_weight

        if family in {
            ContentQualitySignalFamily.HIDDEN_CONTENT,
        }:
            return self.policy.hidden_content_weight

        if family in {
            ContentQualitySignalFamily.REDIRECT_MANIPULATION,
        }:
            return self.policy.redirect_weight

        if family in {
            ContentQualitySignalFamily.ADVERTISEMENT_MANIPULATION,
            ContentQualitySignalFamily.COMMERCIAL_MANIPULATION,
        }:
            return self.policy.commercial_manipulation_weight

        if family in {
            ContentQualitySignalFamily.DOCUMENT_BEHAVIOR,
            ContentQualitySignalFamily.URL_BEHAVIOR,
            ContentQualitySignalFamily.HOST_BEHAVIOR,
            ContentQualitySignalFamily.DOMAIN_BEHAVIOR,
        }:
            return self.policy.behavioral_weight

        if family == ContentQualitySignalFamily.CROSS_RESOURCE:
            return self.policy.cross_resource_weight

        if family == ContentQualitySignalFamily.CROSS_DOMAIN:
            return self.policy.cross_domain_weight

        if family == ContentQualitySignalFamily.TEMPORAL:
            return self.policy.temporal_weight

        if family == ContentQualitySignalFamily.HISTORICAL:
            return self.policy.historical_weight

        if family == ContentQualitySignalFamily.TECHNICAL:
            return self.policy.technical_weight

        if family in {
            ContentQualitySignalFamily.THIN_CONTENT,
            ContentQualitySignalFamily.LOW_VALUE_CONTENT,
            ContentQualitySignalFamily.SEARCH_ENGINE_MANIPULATION,
        }:
            return self.policy.content_quality_weight

        return 1.0

    # ------------------------------------------------------------------------
    # PROFILE-BASED SIGNALS
    # ------------------------------------------------------------------------

    def _profile_quality_score(
        self,
        profile: DocumentContentProfile,
    ) -> float:

        completeness = 1.0

        if profile.word_count <= 0:
            completeness = 0.0
        elif profile.sentence_count <= 0:
            completeness = 0.25
        elif profile.paragraph_count <= 0:
            completeness = 0.50

        density = self._clamp(
            profile.information_density
        )

        structure = self._clamp(
            profile.structural_quality
        )

        coherence = self._clamp(
            profile.semantic_coherence
        )

        consistency = self._clamp(
            profile.semantic_consistency
        )

        uniqueness = self._clamp(
            profile.content_uniqueness
        )

        return self._clamp(
            completeness * 0.15
            + density * 0.20
            + structure * 0.15
            + coherence * 0.20
            + consistency * 0.15
            + uniqueness * 0.15
        )

    def _profile_manipulation_score(
        self,
        profile: DocumentContentProfile,
    ) -> float:

        text = max(
            1,
            profile.word_count,
        )

        repetition_ratio = self._clamp(
            profile.repeated_term_count
            / float(text)
        )

        phrase_ratio = self._clamp(
            profile.repeated_phrase_count
            / float(max(1, profile.sentence_count))
        )

        hidden_ratio = self._clamp(
            profile.hidden_element_count
            / float(max(1, profile.word_count))
        )

        suspicious_ratio = self._clamp(
            profile.suspicious_element_count
            / float(max(1, profile.word_count))
        )

        template_ratio = self._clamp(
            profile.template_token_count
            / float(max(1, profile.word_count))
        )

        duplicate_score = self._clamp(
            profile.duplicate_similarity
        )

        return self._clamp(
            repetition_ratio * 0.15
            + phrase_ratio * 0.10
            + hidden_ratio * 0.20
            + suspicious_ratio * 0.20
            + template_ratio * 0.10
            + duplicate_score * 0.25
        )

    # ------------------------------------------------------------------------
    # FAMILY AGGREGATION
    # ------------------------------------------------------------------------

    def _aggregate_family(
        self,
        family: ContentQualitySignalFamily,
        observations: Sequence[ContentQualityObservation],
        history: ContentQualityHistory,
    ) -> ContentQualityEvidenceBundle:

        family_observations = [
            observation
            for observation in observations
            if observation.family == family
        ]

        if not family_observations:
            return ContentQualityEvidenceBundle(
                family=family,
                partial=True,
            )

        quality_values: List[float] = []
        manipulation_values: List[float] = []

        quality_positive = 0.0
        manipulation_positive = 0.0

        confidence_values: List[float] = []

        strongest: Optional[
            ContentQualityObservation
        ] = None

        for observation in family_observations[
            : self.policy.max_signals_per_family
        ]:
            value = self._clamp(
                observation.value
            )

            confidence = self._clamp(
                observation.confidence
            )

            confidence_values.append(
                confidence
            )

            if (
                observation.direction
                == ContentQualityEvidenceDirection.QUALITY_POSITIVE
            ):
                quality_values.append(
                    value
                )
                quality_positive += value

            elif (
                observation.direction
                == ContentQualityEvidenceDirection.MANIPULATION_POSITIVE
            ):
                manipulation_values.append(
                    value
                )
                manipulation_positive += value

            else:
                quality_values.append(
                    value * 0.50
                )

            if strongest is None:
                strongest = observation
            elif value > strongest.value:
                strongest = observation

        quality_score = (
            sum(quality_values)
            / len(quality_values)
            if quality_values
            else 0.0
        )

        manipulation_score = (
            sum(manipulation_values)
            / len(manipulation_values)
            if manipulation_values
            else 0.0
        )

        confidence = (
            sum(confidence_values)
            / len(confidence_values)
            if confidence_values
            else 0.0
        )

        if (
            family
            == ContentQualitySignalFamily.CONTENT_DUPLICATION
        ):
            manipulation_score = max(
                manipulation_score,
                self._clamp(
                    history.duplicate_observation_count
                    / float(
                        max(
                            1,
                            history.observation_count,
                        )
                    )
                ),
            )

        if (
            family
            == ContentQualitySignalFamily.CONTENT_TEMPLATE
        ):
            manipulation_score = max(
                manipulation_score,
                self._clamp(
                    history.template_observation_count
                    / float(
                        max(
                            1,
                            history.observation_count,
                        )
                    )
                ),
            )

        temporal_state = (
            ContentTemporalState.PERSISTENT
            if history.persistent_manipulation_count > 0
            else (
                ContentTemporalState.RECENT_SPIKE
                if history.recent_manipulation_spike_count > 0
                else ContentTemporalState.UNKNOWN
            )
        )

        partial = any(
            observation.partial
            for observation in family_observations
        )

        return ContentQualityEvidenceBundle(
            family=family,
            quality_score=self._clamp(
                quality_score
            ),
            manipulation_score=self._clamp(
                manipulation_score
            ),
            confidence=self._clamp(
                confidence
            ),
            observation_count=sum(
                observation.observation_count
                for observation in family_observations
            ),
            supporting_signal_count=len(
                family_observations
            ),
            strongest_signal_type=(
                strongest.signal_type
                if strongest
                else None
            ),
            strongest_signal_value=(
                self._clamp(
                    strongest.value
                )
                if strongest
                else 0.0
            ),
            quality_positive_evidence=self._clamp(
                quality_positive
            ),
            manipulation_positive_evidence=self._clamp(
                manipulation_positive
            ),
            temporal_state=temporal_state,
            partial=partial,
        )

    # ------------------------------------------------------------------------
    # TEMPORAL ANALYSIS
    # ------------------------------------------------------------------------

    def _temporal_quality_score(
        self,
        temporal: ContentQualityTemporalEvidence,
        history: ContentQualityHistory,
    ) -> float:

        return self._clamp(
            temporal.stability * 0.25
            + (1.0 - temporal.volatility) * 0.15
            + (1.0 - temporal.recent_manipulation_change) * 0.20
            + (1.0 - history.historical_volatility) * 0.15
            + history.historical_stability * 0.15
            + history.freshness_accuracy * 0.10
        )

    def _temporal_manipulation_score(
        self,
        temporal: ContentQualityTemporalEvidence,
        history: ContentQualityHistory,
    ) -> float:

        persistence = self._clamp(
            temporal.persistence
        )

        volatility = self._clamp(
            temporal.volatility
        )

        recent_change = self._clamp(
            temporal.recent_manipulation_change
        )

        acceleration = self._clamp(
            temporal.acceleration
        )

        historical = self._clamp(
            history.historical_volatility
        )

        persistent_pattern = self._clamp(
            history.persistent_manipulation_count
            / float(
                max(
                    1,
                    history.observation_count,
                )
            )
        )

        return self._clamp(
            persistence * 0.25
            + volatility * 0.15
            + recent_change * 0.25
            + acceleration * 0.15
            + historical * 0.10
            + persistent_pattern * 0.10
        )

    # ------------------------------------------------------------------------
    # CROSS-RESOURCE ANALYSIS
    # ------------------------------------------------------------------------

    def _cross_resource_manipulation_score(
        self,
        evidence: CrossResourceContentEvidence,
    ) -> float:

        return self._clamp(
            evidence.shared_content_score * 0.20
            + evidence.shared_template_score * 0.15
            + evidence.shared_metadata_score * 0.10
            + evidence.shared_manipulation_score * 0.25
            + evidence.cross_domain_reuse_score * 0.15
            + evidence.cross_resource_concentration * 0.15
        )

    def _cross_resource_quality_score(
        self,
        evidence: CrossResourceContentEvidence,
    ) -> float:

        diversity = self._clamp(
            evidence.cross_resource_diversity
        )

        return self._clamp(
            diversity * 0.50
            + (1.0 - evidence.cross_domain_reuse_score) * 0.20
            + (1.0 - evidence.cross_resource_concentration) * 0.15
            + (1.0 - evidence.shared_manipulation_score) * 0.15
        )

    # ------------------------------------------------------------------------
    # GLOBAL AGGREGATION
    # ------------------------------------------------------------------------

    def _aggregate_quality_score(
        self,
        bundles: Sequence[ContentQualityEvidenceBundle],
        profile: DocumentContentProfile,
        temporal: ContentQualityTemporalEvidence,
        cross_resource: CrossResourceContentEvidence,
        history: ContentQualityHistory,
    ) -> float:

        weighted_values: List[Tuple[float, float]] = []

        for bundle in bundles:
            if bundle.supporting_signal_count <= 0:
                continue

            weight = max(
                0.01,
                self._family_weight(
                    bundle.family
                ),
            )

            weighted_values.append(
                (
                    self._clamp(
                        bundle.quality_score
                    ),
                    weight,
                )
            )

        family_score = (
            sum(
                value * weight
                for value, weight in weighted_values
            )
            / sum(
                weight
                for _, weight in weighted_values
            )
            if weighted_values
            else 0.0
        )

        profile_score = self._profile_quality_score(
            profile
        )

        temporal_score = self._temporal_quality_score(
            temporal,
            history,
        )

        cross_resource_score = (
            self._cross_resource_quality_score(
                cross_resource
            )
        )

        historical_score = self._clamp(
            history.average_quality_score * 0.60
            + history.maximum_quality_score * 0.20
            + history.historical_stability * 0.20
        )

        return self._clamp(
            family_score * 0.40
            + profile_score * 0.25
            + temporal_score * 0.10
            + cross_resource_score * 0.10
            + historical_score * 0.15
        )

    def _aggregate_manipulation_score(
        self,
        bundles: Sequence[ContentQualityEvidenceBundle],
        profile: DocumentContentProfile,
        temporal: ContentQualityTemporalEvidence,
        cross_resource: CrossResourceContentEvidence,
        history: ContentQualityHistory,
    ) -> float:

        weighted_values: List[Tuple[float, float]] = []

        for bundle in bundles:
            if bundle.supporting_signal_count <= 0:
                continue

            weight = max(
                0.01,
                self._family_weight(
                    bundle.family
                ),
            )

            weighted_values.append(
                (
                    self._clamp(
                        bundle.manipulation_score
                    ),
                    weight,
                )
            )

        family_score = (
            sum(
                value * weight
                for value, weight in weighted_values
            )
            / sum(
                weight
                for _, weight in weighted_values
            )
            if weighted_values
            else 0.0
        )

        profile_score = self._profile_manipulation_score(
            profile
        )

        temporal_score = self._temporal_manipulation_score(
            temporal,
            history,
        )

        cross_resource_score = (
            self._cross_resource_manipulation_score(
                cross_resource
            )
        )

        historical_score = self._clamp(
            history.average_manipulation_score * 0.55
            + history.maximum_manipulation_score * 0.20
            + history.historical_volatility * 0.25
        )

        return self._clamp(
            family_score * 0.40
            + profile_score * 0.20
            + temporal_score * 0.15
            + cross_resource_score * 0.15
            + historical_score * 0.10
        )

    # ------------------------------------------------------------------------
    # BANDS
    # ------------------------------------------------------------------------

    def _quality_band(
        self,
        score: float,
    ) -> ContentQualityBand:

        score = self._clamp(score)

        if score >= self.policy.quality_very_high_threshold:
            return ContentQualityBand.VERY_HIGH

        if score >= self.policy.quality_high_threshold:
            return ContentQualityBand.HIGH

        if score >= self.policy.quality_moderate_threshold:
            return ContentQualityBand.MODERATE

        if score >= self.policy.quality_low_threshold:
            return ContentQualityBand.LOW

        if score > 0.0:
            return ContentQualityBand.VERY_LOW

        return ContentQualityBand.UNKNOWN

    def _manipulation_band(
        self,
        score: float,
    ) -> ManipulationBand:

        score = self._clamp(score)

        if score >= self.policy.manipulation_very_high_threshold:
            return ManipulationBand.EXTREME

        if score >= self.policy.manipulation_high_threshold:
            return ManipulationBand.VERY_HIGH

        if score >= self.policy.manipulation_moderate_threshold:
            return ManipulationBand.HIGH

        if score >= self.policy.manipulation_low_threshold:
            return ManipulationBand.MODERATE

        if score > 0.0:
            return ManipulationBand.LOW

        return ManipulationBand.MINIMAL

    # ------------------------------------------------------------------------
    # CONFIDENCE
    # ------------------------------------------------------------------------

    def _aggregate_confidence(
        self,
        observations: Sequence[ContentQualityObservation],
        bundles: Sequence[ContentQualityEvidenceBundle],
        temporal: ContentQualityTemporalEvidence,
        cross_resource: CrossResourceContentEvidence,
        history: ContentQualityHistory,
        partial: bool,
    ) -> float:

        values: List[float] = []

        values.extend(
            self._clamp(
                observation.confidence
            )
            for observation in observations
        )

        values.extend(
            self._clamp(
                bundle.confidence
            )
            for bundle in bundles
            if bundle.supporting_signal_count > 0
        )

        values.append(
            self._clamp(
                temporal.confidence
            )
        )

        values.append(
            self._clamp(
                cross_resource.confidence
            )
        )

        if history.observation_count > 0:
            values.append(
                self._clamp(
                    history.freshness_accuracy
                )
            )

        if not values:
            confidence = 0.0
        else:
            confidence = (
                sum(values)
                / len(values)
            )

        if partial:
            confidence = self._clamp(
                confidence
                - self.policy.partial_penalty
            )

        return self._clamp(
            confidence
        )

    # ------------------------------------------------------------------------
    # RESULT ID
    # ------------------------------------------------------------------------

    def _result_id(
        self,
        identity: ContentQualityIdentity,
        observations: Sequence[ContentQualityObservation],
    ) -> str:

        payload = "|".join(
            [
                identity.key(),
                ARCHITECTURE_VERSION,
                "|".join(
                    sorted(
                        observation.signal_id
                        for observation in observations
                    )
                ),
            ]
        )

        return self._digest_payload(
            payload
        )

    # ------------------------------------------------------------------------
    # MAIN ANALYSIS
    # ------------------------------------------------------------------------

    def analyze(
        self,
        request: ContentQualityManipulationInput,
    ) -> ContentQualityManipulationResult:

        identity = request.identity

        self._event(
            identity,
            ContentQualityEventType.REQUEST_RECEIVED,
            ContentQualityState.RECEIVED,
        )

        valid, errors = self.validate(
            request
        )

        self._event(
            identity,
            ContentQualityEventType.VALIDATION_STARTED,
            ContentQualityState.VALIDATING,
            valid=valid,
            errors=errors,
        )

        if not valid:
            result = ContentQualityManipulationResult(
                result_id=self._result_id(
                    identity,
                    request.observations,
                ),
                identity=identity,
                state=ContentQualityState.REJECTED,
                partial=request.partial,
                lineage=request.lineage,
                evidence_sources=list(
                    request.evidence_sources
                ),
                created_at=self._now(),
            )

            self._event(
                identity,
                ContentQualityEventType.ANALYSIS_REJECTED,
                ContentQualityState.REJECTED,
                errors=errors,
            )

            self.backend.persist_result(
                result
            )

            return result

        normalized = self._normalize_input(
            request
        )

        self._event(
            identity,
            ContentQualityEventType.INPUT_NORMALIZED,
            ContentQualityState.NORMALIZING,
            observation_count=len(
                normalized.observations
            ),
            partial=normalized.partial,
        )

        if normalized.partial:
            self._event(
                identity,
                ContentQualityEventType.PARTIAL_INPUT_DETECTED,
                ContentQualityState.PARTIAL,
            )

        self._checkpoint(
            identity,
            ContentQualityCheckpointType.INPUT_NORMALIZED,
            ContentQualityState.NORMALIZING,
            len(normalized.observations),
            0,
            0.0,
            0.0,
            normalized.confidence,
            normalized.partial,
        )

        profile = (
            normalized.content_profile
            or DocumentContentProfile(
                partial=True
            )
        )

        history = (
            normalized.history
            or ContentQualityHistory(
                partial=True
            )
        )

        temporal = (
            normalized.temporal_evidence
            or ContentQualityTemporalEvidence(
                partial=True
            )
        )

        cross_resource = (
            normalized.cross_resource_evidence
            or CrossResourceContentEvidence(
                partial=True
            )
        )

        # --------------------------------------------------------------------
        # CONTENT ANALYSIS
        # --------------------------------------------------------------------

        self._event(
            identity,
            ContentQualityEventType.CONTENT_ANALYSIS_STARTED,
            ContentQualityState.CONTENT_ANALYSIS,
        )

        profile_quality = (
            self._profile_quality_score(
                profile
            )
        )

        profile_manipulation = (
            self._profile_manipulation_score(
                profile
            )
        )

        self._event(
            identity,
            ContentQualityEventType.CONTENT_ANALYSIS_COMPLETED,
            ContentQualityState.CONTENT_ANALYSIS,
            profile_quality=profile_quality,
            profile_manipulation=profile_manipulation,
        )

        self._checkpoint(
            identity,
            ContentQualityCheckpointType.CONTENT_ANALYZED,
            ContentQualityState.CONTENT_ANALYSIS,
            len(normalized.observations),
            0,
            profile_quality,
            profile_manipulation,
            normalized.confidence,
            normalized.partial,
        )

        # --------------------------------------------------------------------
        # FAMILY ANALYSIS
        # --------------------------------------------------------------------

        self._event(
            identity,
            ContentQualityEventType.QUALITY_ANALYSIS_STARTED,
            ContentQualityState.QUALITY_ANALYSIS,
        )

        families = sorted(
            {
                observation.family
                for observation in normalized.observations
            },
            key=lambda family: family.value,
        )

        bundles: List[
            ContentQualityEvidenceBundle
        ] = []

        for family in families[
            : self.policy.max_families
        ]:
            bundles.append(
                self._aggregate_family(
                    family,
                    normalized.observations,
                    history,
                )
            )

        self._event(
            identity,
            ContentQualityEventType.QUALITY_ANALYSIS_COMPLETED,
            ContentQualityState.QUALITY_ANALYSIS,
            family_count=len(bundles),
        )

        self._checkpoint(
            identity,
            ContentQualityCheckpointType.QUALITY_ANALYZED,
            ContentQualityState.QUALITY_ANALYSIS,
            len(normalized.observations),
            len(bundles),
            0.0,
            0.0,
            normalized.confidence,
            normalized.partial,
        )

        # --------------------------------------------------------------------
        # MANIPULATION ANALYSIS
        # --------------------------------------------------------------------

        self._event(
            identity,
            ContentQualityEventType.MANIPULATION_ANALYSIS_STARTED,
            ContentQualityState.MANIPULATION_ANALYSIS,
        )

        manipulation_signal_score = self._clamp(
            sum(
                bundle.manipulation_score
                for bundle in bundles
            )
            / float(
                max(
                    1,
                    len(
                        [
                            bundle
                            for bundle in bundles
                            if bundle.supporting_signal_count > 0
                        ]
                    ),
                )
            )
        )

        self._event(
            identity,
            ContentQualityEventType.MANIPULATION_ANALYSIS_COMPLETED,
            ContentQualityState.MANIPULATION_ANALYSIS,
            manipulation_signal_score=(
                manipulation_signal_score
            ),
        )

        self._checkpoint(
            identity,
            ContentQualityCheckpointType.MANIPULATION_ANALYZED,
            ContentQualityState.MANIPULATION_ANALYSIS,
            len(normalized.observations),
            len(bundles),
            0.0,
            manipulation_signal_score,
            normalized.confidence,
            normalized.partial,
        )

        # --------------------------------------------------------------------
        # REPETITION / DUPLICATION
        # --------------------------------------------------------------------

        self._event(
            identity,
            ContentQualityEventType.REPETITION_ANALYSIS_STARTED,
            ContentQualityState.REPETITION_ANALYSIS,
        )

        repetition_score = self._clamp(
            profile.duplicate_similarity * 0.40
            + profile.template_similarity * 0.30
            + self._clamp(
                profile.repeated_term_count
                / float(
                    max(
                        1,
                        profile.word_count,
                    )
                )
            ) * 0.15
            + self._clamp(
                profile.repeated_phrase_count
                / float(
                    max(
                        1,
                        profile.sentence_count,
                    )
                )
            ) * 0.15
        )

        self._event(
            identity,
            ContentQualityEventType.REPETITION_ANALYSIS_COMPLETED,
            ContentQualityState.REPETITION_ANALYSIS,
            repetition_score=repetition_score,
        )

        self._checkpoint(
            identity,
            ContentQualityCheckpointType.REPETITION_ANALYZED,
            ContentQualityState.REPETITION_ANALYSIS,
            len(normalized.observations),
            len(bundles),
            0.0,
            repetition_score,
            normalized.confidence,
            normalized.partial,
        )

        # --------------------------------------------------------------------
        # CROSS-RESOURCE ANALYSIS
        # --------------------------------------------------------------------

        self._event(
            identity,
            ContentQualityEventType.CROSS_RESOURCE_ANALYSIS_STARTED,
            ContentQualityState.CROSS_RESOURCE_ANALYSIS,
        )

        cross_resource_manipulation = (
            self._cross_resource_manipulation_score(
                cross_resource
            )
        )

        cross_resource_quality = (
            self._cross_resource_quality_score(
                cross_resource
            )
        )

        self._event(
            identity,
            ContentQualityEventType.CROSS_RESOURCE_ANALYSIS_COMPLETED,
            ContentQualityState.CROSS_RESOURCE_ANALYSIS,
            cross_resource_manipulation=(
                cross_resource_manipulation
            ),
            cross_resource_quality=(
                cross_resource_quality
            ),
        )

        self._checkpoint(
            identity,
            ContentQualityCheckpointType.CROSS_RESOURCE_ANALYZED,
            ContentQualityState.CROSS_RESOURCE_ANALYSIS,
            len(normalized.observations),
            len(bundles),
            cross_resource_quality,
            cross_resource_manipulation,
            cross_resource.confidence,
            normalized.partial,
        )

        # --------------------------------------------------------------------
        # TEMPORAL ANALYSIS
        # --------------------------------------------------------------------

        self._event(
            identity,
            ContentQualityEventType.TEMPORAL_ANALYSIS_STARTED,
            ContentQualityState.TEMPORAL_ANALYSIS,
        )

        temporal_quality = (
            self._temporal_quality_score(
                temporal,
                history,
            )
        )

        temporal_manipulation = (
            self._temporal_manipulation_score(
                temporal,
                history,
            )
        )

        self._event(
            identity,
            ContentQualityEventType.TEMPORAL_ANALYSIS_COMPLETED,
            ContentQualityState.TEMPORAL_ANALYSIS,
            temporal_quality=temporal_quality,
            temporal_manipulation=(
                temporal_manipulation
            ),
        )

        self._checkpoint(
            identity,
            ContentQualityCheckpointType.TEMPORAL_ANALYZED,
            ContentQualityState.TEMPORAL_ANALYSIS,
            len(normalized.observations),
            len(bundles),
            temporal_quality,
            temporal_manipulation,
            temporal.confidence,
            normalized.partial,
        )

        # --------------------------------------------------------------------
        # EVIDENCE AGGREGATION
        # --------------------------------------------------------------------

        self._event(
            identity,
            ContentQualityEventType.EVIDENCE_AGGREGATED,
            ContentQualityState.EVIDENCE_AGGREGATION,
        )

        quality_score = (
            self._aggregate_quality_score(
                bundles,
                profile,
                temporal,
                cross_resource,
                history,
            )
        )

        manipulation_score = (
            self._aggregate_manipulation_score(
                bundles,
                profile,
                temporal,
                cross_resource,
                history,
            )
        )

        # Explicitly preserve direct profile evidence without allowing it
        # to replace the independent evidence pipeline.
        quality_score = self._clamp(
            quality_score * 0.85
            + profile_quality * 0.15
        )

        manipulation_score = self._clamp(
            manipulation_score * 0.85
            + (
                profile_manipulation
                + repetition_score
            ) * 0.075
        )

        self._checkpoint(
            identity,
            ContentQualityCheckpointType.EVIDENCE_AGGREGATED,
            ContentQualityState.EVIDENCE_AGGREGATION,
            len(normalized.observations),
            len(bundles),
            quality_score,
            manipulation_score,
            normalized.confidence,
            normalized.partial,
        )

        # --------------------------------------------------------------------
        # CONFIDENCE
        # --------------------------------------------------------------------

        self._event(
            identity,
            ContentQualityEventType.CONFIDENCE_CALCULATION_STARTED,
            ContentQualityState.CONFIDENCE_CALCULATION,
        )

        aggregate_confidence = (
            self._aggregate_confidence(
                normalized.observations,
                bundles,
                temporal,
                cross_resource,
                history,
                normalized.partial,
            )
        )

        self._event(
            identity,
            ContentQualityEventType.CONFIDENCE_CALCULATED,
            ContentQualityState.CONFIDENCE_CALCULATION,
            confidence=aggregate_confidence,
        )

        self._checkpoint(
            identity,
            ContentQualityCheckpointType.CONFIDENCE_CALCULATED,
            ContentQualityState.CONFIDENCE_CALCULATION,
            len(normalized.observations),
            len(bundles),
            quality_score,
            manipulation_score,
            aggregate_confidence,
            normalized.partial,
        )

        # --------------------------------------------------------------------
        # STRONGEST EVIDENCE
        # --------------------------------------------------------------------

        strongest_quality_family: Optional[
            ContentQualitySignalFamily
        ] = None

        strongest_manipulation_family: Optional[
            ContentQualitySignalFamily
        ] = None

        strongest_signal_type: Optional[
            ContentQualitySignalType
        ] = None

        strongest_quality_value = 0.0
        strongest_manipulation_value = 0.0
        strongest_signal_value = 0.0

        for bundle in bundles:

            if (
                bundle.quality_score
                > strongest_quality_value
            ):
                strongest_quality_value = (
                    bundle.quality_score
                )
                strongest_quality_family = (
                    bundle.family
                )

            if (
                bundle.manipulation_score
                > strongest_manipulation_value
            ):
                strongest_manipulation_value = (
                    bundle.manipulation_score
                )
                strongest_manipulation_family = (
                    bundle.family
                )

            if (
                bundle.strongest_signal_value
                > strongest_signal_value
            ):
                strongest_signal_value = (
                    bundle.strongest_signal_value
                )
                strongest_signal_type = (
                    bundle.strongest_signal_type
                )

        # --------------------------------------------------------------------
        # FINAL STATE
        # --------------------------------------------------------------------

        state = (
            ContentQualityState.PARTIAL
            if normalized.partial
            else ContentQualityState.DECISION_READY
        )

        result = ContentQualityManipulationResult(
            result_id=self._result_id(
                identity,
                normalized.observations,
            ),
            identity=identity,
            state=state,
            content_quality_score=quality_score,
            manipulation_score=manipulation_score,
            quality_band=self._quality_band(
                quality_score
            ),
            manipulation_band=self._manipulation_band(
                manipulation_score
            ),
            observations=list(
                normalized.observations
            ),
            evidence_bundles=bundles,
            content_profile=profile,
            temporal_evidence=temporal,
            cross_resource_evidence=cross_resource,
            aggregate_confidence=aggregate_confidence,
            strongest_quality_family=(
                strongest_quality_family
            ),
            strongest_manipulation_family=(
                strongest_manipulation_family
            ),
            strongest_signal_type=(
                strongest_signal_type
            ),
            partial=normalized.partial,
            lineage=normalized.lineage,
            evidence_sources=list(
                normalized.evidence_sources
            ),
            created_at=self._now(),
        )

        self._checkpoint(
            identity,
            ContentQualityCheckpointType.COMPLETED,
            state,
            len(normalized.observations),
            len(bundles),
            quality_score,
            manipulation_score,
            aggregate_confidence,
            normalized.partial,
        )

        self._event(
            identity,
            ContentQualityEventType.ANALYSIS_COMPLETED,
            state,
            quality_score=quality_score,
            manipulation_score=manipulation_score,
            confidence=aggregate_confidence,
            family_count=len(bundles),
        )

        self.backend.persist_result(
            result
        )

        return result

    # ------------------------------------------------------------------------
    # BATCH PROCESSING
    # ------------------------------------------------------------------------

    def analyze_many(
        self,
        requests: Iterable[
            ContentQualityManipulationInput
        ],
    ) -> List[ContentQualityManipulationResult]:

        results: List[
            ContentQualityManipulationResult
        ] = []

        for request in requests:

            if (
                len(results)
                >= self.policy.max_related_resources
            ):
                break

            results.append(
                self.analyze(
                    request
                )
            )

        return results

    # ------------------------------------------------------------------------
    # RESULT ACCESS
    # ------------------------------------------------------------------------

    def result(
        self,
        result_id: str,
    ) -> Optional[
        ContentQualityManipulationResult
    ]:

        return self.backend.get_result(
            result_id
        )

    def events(
        self,
    ) -> List[ContentQualityManipulationEvent]:

        backend = self.backend

        if hasattr(
            backend,
            "events",
        ):
            return list(
                getattr(
                    backend,
                    "events",
                )()
            )

        return []

    def checkpoints(
        self,
    ) -> List[
        ContentQualityManipulationCheckpoint
    ]:

        backend = self.backend

        if hasattr(
            backend,
            "checkpoints",
        ):
            return list(
                getattr(
                    backend,
                    "checkpoints",
                )()
            )

        return []

    # ------------------------------------------------------------------------
    # ARCHITECTURE DESCRIPTION
    # ------------------------------------------------------------------------

    def architecture(
        self,
    ) -> Dict[str, Any]:

        return {
            "phase": PHASE,
            "phase_name": PHASE_NAME,
            "architecture_version": ARCHITECTURE_VERSION,

            "scale_target": SCALE_TARGET,
            "google_scale_capability_target":
                GOOGLE_SCALE_CAPABILITY_TARGET,
            "google_technology_dependency":
                GOOGLE_TECHNOLOGY_DEPENDENCY,

            "purpose": (
                "Analyze Web content quality and manipulation patterns "
                "and produce structured evidence for downstream "
                "spam, abuse, security, and quality systems."
            ),

            "pipeline_role": (
                "content_quality_and_manipulation_evidence_generation"
            ),

            "inputs": [
                "document_content_profile",
                "content_quality_observations",
                "historical_quality_evidence",
                "temporal_evidence",
                "cross_resource_evidence",
                "provenance",
                "lineage",
            ],

            "outputs": [
                "content_quality_score",
                "manipulation_score",
                "quality_band",
                "manipulation_band",
                "content_quality_evidence_bundles",
                "temporal_evidence",
                "cross_resource_evidence",
                "confidence",
                "provenance",
                "lineage",
            ],

            "signal_families": [
                family.value
                for family in ContentQualitySignalFamily
            ],

            "signal_types": [
                signal.value
                for signal in ContentQualitySignalType
            ],

            "does_not": [
                "final_spam_classification",
                "final_quality_enforcement",
                "document_deletion",
                "url_deletion",
                "index_mutation",
                "index_removal",
                "penalty_application",
                "quarantine_enforcement",
                "crawler_execution",
                "http_fetching",
                "worker_assignment",
                "final_ranking",
                "retrieval",
                "search_result_selection",
                "security_enforcement",
                "malware_blocking",
                "phishing_enforcement",
                "google_search_api",
                "google_index",
                "google_crawler",
                "google_infrastructure",
                "google_ranking_technology",
            ],

            "distributed": True,
            "partition_aware": True,
            "shard_aware": True,
            "resource_partitionable": True,
            "document_partitionable": True,
            "host_partitionable": True,
            "domain_partitionable": True,
            "cross_resource_analysis": True,
            "cross_domain_analysis": True,
            "horizontally_scalable": True,

            "supports_partial_input":
                self.policy.allow_partial,

            "deterministic_normalization":
                self.policy.deterministic,

            "checkpointable":
                self.policy.checkpoint_enabled,

            "restartable": True,

            "incremental_observations": True,
            "historical_analysis": True,
            "temporal_analysis": True,
            "content_profile_analysis": True,
            "repetition_analysis": True,
            "duplication_analysis": True,
            "template_analysis": True,
            "cross_resource_analysis_enabled": True,
            "cross_domain_analysis_enabled": True,

            "provenance_preserving": True,
            "lineage_preserving": True,
            "backend_replaceable": True,

            "global_fixed_limits": {
                "resources": None,
                "urls": None,
                "documents": None,
                "hosts": None,
                "domains": None,
                "partitions": None,
                "shards": None,
                "workers": None,
                "observations": None,
                "quality_signals": None,
                "manipulation_signals": None,
                "evidence_records": None,
                "histories": None,
            },

            "per_processing_limits": {
                "max_observations_per_resource":
                    self.policy.max_observations_per_resource,
                "max_related_resources":
                    self.policy.max_related_resources,
                "max_related_documents":
                    self.policy.max_related_documents,
                "max_related_hosts":
                    self.policy.max_related_hosts,
                "max_related_domains":
                    self.policy.max_related_domains,
                "max_families":
                    self.policy.max_families,
                "max_signals_per_family":
                    self.policy.max_signals_per_family,
            },

            "phase_14_boundaries": {
                "14.1": (
                    "Generate broad spam/manipulation signal "
                    "and evidence foundations."
                ),
                "14.2": (
                    "Analyze content quality, duplication, repetition, "
                    "content structure, and manipulation patterns."
                ),
                "14.3": (
                    "Analyze link spam and graph abuse patterns."
                ),
            },

            "downstream_contract": {
                "14.3": (
                    "Consumes relevant content/manipulation evidence "
                    "where link and graph context intersects."
                ),
                "later_phase": (
                    "Final spam, abuse, security, quarantine, and "
                    "quality enforcement systems consume this evidence."
                ),
                "ranking": (
                    "Phase 12 remains responsible for ranking; "
                    "this architecture does not rank documents."
                ),
                "freshness": (
                    "Phase 13 remains responsible for freshness and "
                    "recrawling; this architecture may consume temporal "
                    "evidence without replacing freshness control."
                ),
            },

            "phase_14_complete": False,

            "next_stage": NEXT_STAGE,
            "next_stage_name": NEXT_STAGE_NAME,
        }


# ============================================================================
# COMPATIBILITY ALIASES
# ============================================================================


ContentQualityManipulationDetection = (
    ContentQualityManipulationDetectionArchitecture
)

GlobalContentQualityManipulationDetection = (
    ContentQualityManipulationDetectionArchitecture
)

Phase14_2ContentQualityManipulationDetection = (
    ContentQualityManipulationDetectionArchitecture
)

ContentQualityDetectionArchitecture = (
    ContentQualityManipulationDetectionArchitecture
)


# ============================================================================
# PUBLIC EXPORTS
# ============================================================================


__all__ = [
    "SCALE_TARGET",
    "GOOGLE_SCALE_CAPABILITY_TARGET",
    "GOOGLE_TECHNOLOGY_DEPENDENCY",
    "ARCHITECTURE_VERSION",
    "PHASE",
    "PREVIOUS_STAGE",
    "NEXT_STAGE",
    "PHASE_NAME",
    "NEXT_STAGE_NAME",

    "ContentQualityState",
    "ContentQualitySignalFamily",
    "ContentQualitySignalType",
    "ContentQualityEvidenceStrength",
    "ContentQualityEvidenceDirection",
    "ContentQualityScope",
    "ContentQualityBand",
    "ManipulationBand",
    "ContentTemporalState",
    "ContentQualityEventType",
    "ContentQualityCheckpointType",

    "ContentQualityIdentity",
    "ContentQualityLineage",
    "ContentQualityObservation",
    "DocumentContentProfile",
    "ContentQualityHistory",
    "ContentQualityTemporalEvidence",
    "CrossResourceContentEvidence",
    "ContentQualityEvidenceBundle",

    "ContentQualityManipulationPolicy",
    "ContentQualityManipulationInput",
    "ContentQualityManipulationResult",
    "ContentQualityManipulationCheckpoint",
    "ContentQualityManipulationEvent",

    "ContentQualityManipulationBackend",
    "InMemoryContentQualityManipulationMetadata",

    "ContentQualityManipulationDetectionArchitecture",

    "ContentQualityManipulationDetection",
    "GlobalContentQualityManipulationDetection",
    "Phase14_2ContentQualityManipulationDetection",
    "ContentQualityDetectionArchitecture",
]
