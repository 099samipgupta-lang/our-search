"""
OUR SEARCH
Phase 13.2 — Change Detection / Content Change Signals

Production architecture for detecting and representing Web-resource
content-change evidence at enormous public-Web scale.

Scale target:
    billions -> trillions of publicly accessible Web resources

This module is independent of:
    - Google Search API
    - Google index
    - Google crawler
    - Google infrastructure
    - Google ranking technology

Responsibilities:
    - Represent observed resource versions.
    - Compare successive resource observations.
    - Detect content and structural changes.
    - Estimate change magnitude.
    - Estimate change frequency and volatility.
    - Distinguish meaningful changes from stable observations.
    - Preserve change lineage and provenance.
    - Produce signals consumable by freshness and recrawl systems.
    - Support distributed and partition-local processing.
    - Support checkpointing and restartability.
    - Keep persistence backend-replaceable.

This module does NOT:
    - perform Web crawling
    - schedule recrawls
    - allocate crawler workers
    - mutate the search index
    - perform final ranking
    - classify spam
    - execute a specific ML model
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from math import exp, isfinite
from typing import Dict, List, Optional, Protocol, Sequence, Tuple
from uuid import uuid4


# ---------------------------------------------------------------------------
# Architecture constants
# ---------------------------------------------------------------------------

SCALE_TARGET = "billions_to_trillions_of_public_web_resources"
GOOGLE_SCALE_CAPABILITY_TARGET = True
GOOGLE_TECHNOLOGY_DEPENDENCY = False

ARCHITECTURE_VERSION = "content-change-detection-signals.v1"
PHASE = "13.2"
PREVIOUS_STAGE = "13.1"
NEXT_STAGE = "13.3"


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class ChangeDetectionState(str, Enum):
    RECEIVED = "received"
    VALIDATING = "validating"
    OBSERVATION_NORMALIZATION = "observation_normalization"
    VERSION_COMPARISON = "version_comparison"
    CHANGE_EXTRACTION = "change_extraction"
    SIGNAL_CALCULATION = "signal_calculation"
    VOLATILITY_ESTIMATION = "volatility_estimation"
    COMPLETED = "completed"
    PARTIAL = "partial"
    UNCHANGED = "unchanged"
    REJECTED = "rejected"
    FAILED = "failed"


class ChangeEvidenceType(str, Enum):
    CONTENT_CHANGE = "content_change"
    TEXT_CHANGE = "text_change"
    STRUCTURAL_CHANGE = "structural_change"
    METADATA_CHANGE = "metadata_change"
    TITLE_CHANGE = "title_change"
    DESCRIPTION_CHANGE = "description_change"
    LINK_CHANGE = "link_change"
    IMAGE_CHANGE = "image_change"
    MEDIA_CHANGE = "media_change"
    SCRIPT_CHANGE = "script_change"
    STYLE_CHANGE = "style_change"
    RESOURCE_CHANGE = "resource_change"
    URL_CHANGE = "url_change"
    SEMANTIC_CHANGE = "semantic_change"
    SECTION_CHANGE = "section_change"
    ENTITY_CHANGE = "entity_change"
    TIMESTAMP_CHANGE = "timestamp_change"
    HTTP_VALIDATOR_CHANGE = "http_validator_change"
    FEED_CHANGE = "feed_change"
    SITEMAP_CHANGE = "sitemap_change"


class ChangeMagnitude(str, Enum):
    NONE = "none"
    TINY = "tiny"
    SMALL = "small"
    MODERATE = "moderate"
    LARGE = "large"
    VERY_LARGE = "very_large"
    MAJOR = "major"


class ChangeImportance(str, Enum):
    NONE = "none"
    LOW = "low"
    MODERATE = "moderate"
    HIGH = "high"
    VERY_HIGH = "very_high"
    CRITICAL = "critical"


class ChangeVolatilityBand(str, Enum):
    STABLE = "stable"
    LOW = "low"
    MODERATE = "moderate"
    HIGH = "high"
    VERY_HIGH = "very_high"
    EXTREME = "extreme"


class ChangeEventType(str, Enum):
    REQUEST_RECEIVED = "request_received"
    VALIDATION_STARTED = "validation_started"
    OBSERVATION_NORMALIZATION_STARTED = (
        "observation_normalization_started"
    )
    VERSION_COMPARISON_STARTED = "version_comparison_started"
    CHANGE_DETECTED = "change_detected"
    NO_CHANGE_DETECTED = "no_change_detected"
    CHANGE_SIGNALS_CALCULATED = (
        "change_signals_calculated"
    )
    VOLATILITY_ESTIMATED = "volatility_estimated"
    PARTIAL_INPUT_DETECTED = "partial_input_detected"
    CHECKPOINT_CREATED = "checkpoint_created"
    DETECTION_COMPLETED = "detection_completed"
    DETECTION_REJECTED = "detection_rejected"
    DETECTION_FAILED = "detection_failed"


class ChangeCheckpointType(str, Enum):
    INPUT_ACCEPTED = "input_accepted"
    OBSERVATIONS_NORMALIZED = "observations_normalized"
    VERSIONS_COMPARED = "versions_compared"
    SIGNALS_CALCULATED = "signals_calculated"
    VOLATILITY_CALCULATED = "volatility_calculated"
    COMPLETED = "completed"


# ---------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ChangeDetectionIdentity:
    request_id: str
    resource_id: str
    canonical_url: str
    hostname: str
    partition_id: Optional[str] = None
    shard_id: Optional[str] = None
    version: str = ARCHITECTURE_VERSION


# ---------------------------------------------------------------------------
# Lineage
# ---------------------------------------------------------------------------


@dataclass
class ChangeDetectionLineage:
    source_system: str = (
        "phase13.2-content-change-detection-signals"
    )
    source_version: str = ARCHITECTURE_VERSION
    previous_stage: str = (
        "phase13.1-crawl-freshness-prioritization"
    )
    observation_ids: List[str] = field(default_factory=list)
    parent_request_ids: List[str] = field(default_factory=list)
    calculation_timestamp: str = ""
    provenance_preserved: bool = True


# ---------------------------------------------------------------------------
# Resource observation
# ---------------------------------------------------------------------------


@dataclass
class ResourceObservation:
    observation_id: str
    resource_id: str
    canonical_url: str

    observed_at: str

    content_hash: Optional[str] = None
    text_hash: Optional[str] = None
    structure_hash: Optional[str] = None
    metadata_hash: Optional[str] = None

    title_hash: Optional[str] = None
    description_hash: Optional[str] = None
    links_hash: Optional[str] = None
    media_hash: Optional[str] = None

    content_size_bytes: Optional[int] = None
    text_size_bytes: Optional[int] = None

    status_code: Optional[int] = None
    etag: Optional[str] = None
    last_modified: Optional[str] = None

    section_count: Optional[int] = None
    link_count: Optional[int] = None
    image_count: Optional[int] = None
    media_count: Optional[int] = None

    content_fingerprint: Optional[str] = None
    semantic_fingerprint: Optional[str] = None

    partial: bool = False

    metadata: Dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Explicit change evidence
# ---------------------------------------------------------------------------


@dataclass
class ChangeEvidence:
    evidence_type: ChangeEvidenceType

    changed: bool
    magnitude: float
    confidence: float = 1.0

    old_value: Optional[str] = None
    new_value: Optional[str] = None

    observed_at: Optional[str] = None

    source: str = "observation_comparison"

    partial: bool = False

    metadata: Dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Change signal bundle
# ---------------------------------------------------------------------------


@dataclass
class ContentChangeSignalBundle:
    resource_id: str

    content_change_score: float
    text_change_score: float
    structural_change_score: float
    metadata_change_score: float
    link_change_score: float
    media_change_score: float
    semantic_change_score: float

    change_magnitude: ChangeMagnitude
    change_importance: ChangeImportance

    change_frequency: float
    change_volatility: float

    confidence: float

    unchanged: bool
    partial: bool

    evidence: List[ChangeEvidence] = field(
        default_factory=list
    )


# ---------------------------------------------------------------------------
# Historical change state
# ---------------------------------------------------------------------------


@dataclass
class ResourceChangeHistory:
    resource_id: str

    observation_count: int = 0
    change_count: int = 0

    first_observed_at: Optional[str] = None
    last_observed_at: Optional[str] = None
    last_changed_at: Optional[str] = None

    total_content_change: float = 0.0
    total_text_change: float = 0.0
    total_structural_change: float = 0.0
    total_semantic_change: float = 0.0

    recent_change_count: int = 0

    average_change_magnitude: float = 0.0
    average_change_interval_days: Optional[float] = None

    historical_change_rate: float = 0.0
    historical_volatility: float = 0.0

    partial: bool = False


# ---------------------------------------------------------------------------
# Policy
# ---------------------------------------------------------------------------


@dataclass
class ContentChangeDetectionPolicy:
    """
    Per-resource and per-batch controls.

    These are local processing limits only. They are not global Web-scale
    ceilings.
    """

    max_observations_per_resource: int = 256
    max_evidence_per_comparison: int = 128
    max_resources_per_batch: int = 100_000

    minimum_confidence: float = 0.10

    allow_partial: bool = True
    deterministic: bool = True
    checkpoint_enabled: bool = True

    # Evidence weights.
    content_weight: float = 1.50
    text_weight: float = 1.25
    structural_weight: float = 1.00
    metadata_weight: float = 0.50
    link_weight: float = 0.80
    media_weight: float = 0.70
    semantic_weight: float = 1.40

    # Historical behavior.
    recent_change_window_days: float = 30.0
    volatility_window_days: float = 365.0

    # Magnitude thresholds.
    tiny_threshold: float = 0.02
    small_threshold: float = 0.10
    moderate_threshold: float = 0.30
    large_threshold: float = 0.55
    very_large_threshold: float = 0.80
    major_threshold: float = 0.93

    # Importance thresholds.
    importance_low_threshold: float = 0.15
    importance_moderate_threshold: float = 0.35
    importance_high_threshold: float = 0.60
    importance_very_high_threshold: float = 0.80
    importance_critical_threshold: float = 0.93


# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------


@dataclass
class ContentChangeDetectionResult:
    identity: ChangeDetectionIdentity
    lineage: ChangeDetectionLineage

    state: ChangeDetectionState

    previous_observation_id: Optional[str]
    current_observation_id: str

    signals: ContentChangeSignalBundle

    created_at: str
    completed_at: Optional[str] = None

    error: Optional[str] = None


@dataclass
class ChangeDetectionCheckpoint:
    checkpoint_id: str
    request_id: str
    resource_id: str

    checkpoint_type: ChangeCheckpointType
    state: ChangeDetectionState

    created_at: str

    observation_id: Optional[str] = None

    content_change_score: Optional[float] = None
    semantic_change_score: Optional[float] = None
    volatility_score: Optional[float] = None

    metadata: Dict[str, str] = field(
        default_factory=dict
    )


@dataclass
class ChangeDetectionEvent:
    event_id: str
    request_id: str
    resource_id: str

    event_type: ChangeEventType
    timestamp: str

    state: ChangeDetectionState

    metadata: Dict[str, str] = field(
        default_factory=dict
    )


# ---------------------------------------------------------------------------
# Backend
# ---------------------------------------------------------------------------


class ContentChangeDetectionBackend(Protocol):
    def persist_event(
        self,
        event: ChangeDetectionEvent,
    ) -> None:
        ...

    def persist_checkpoint(
        self,
        checkpoint: ChangeDetectionCheckpoint,
    ) -> None:
        ...

    def persist_result(
        self,
        result: ContentChangeDetectionResult,
    ) -> None:
        ...

    def get_result(
        self,
        request_id: str,
    ) -> Optional[ContentChangeDetectionResult]:
        ...


class InMemoryContentChangeDetectionMetadata:
    """
    Reference metadata backend.

    Production deployments can replace this implementation with distributed
    durable storage without changing the architecture.
    """

    def __init__(self) -> None:
        self._events: Dict[
            str,
            ChangeDetectionEvent,
        ] = {}

        self._checkpoints: Dict[
            str,
            ChangeDetectionCheckpoint,
        ] = {}

        self._results: Dict[
            str,
            ContentChangeDetectionResult,
        ] = {}

    def persist_event(
        self,
        event: ChangeDetectionEvent,
    ) -> None:
        self._events[event.event_id] = event

    def persist_checkpoint(
        self,
        checkpoint: ChangeDetectionCheckpoint,
    ) -> None:
        self._checkpoints[
            checkpoint.checkpoint_id
        ] = checkpoint

    def persist_result(
        self,
        result: ContentChangeDetectionResult,
    ) -> None:
        self._results[
            result.identity.request_id
        ] = result

    def get_result(
        self,
        request_id: str,
    ) -> Optional[ContentChangeDetectionResult]:
        return self._results.get(request_id)

    def events(self) -> List[ChangeDetectionEvent]:
        return list(self._events.values())

    def checkpoints(
        self,
    ) -> List[ChangeDetectionCheckpoint]:
        return list(self._checkpoints.values())

    def results(
        self,
    ) -> List[ContentChangeDetectionResult]:
        return list(self._results.values())


# ---------------------------------------------------------------------------
# Main architecture
# ---------------------------------------------------------------------------


class ContentChangeDetectionSignalsArchitecture:
    """
    Phase 13.2 architecture.

    Flow:

        observations
             |
             v
        normalization
             |
             v
        version comparison
             |
             v
        change evidence
             |
             v
        change magnitude
             |
             v
        semantic/content signals
             |
             v
        historical frequency
             |
             v
        volatility
             |
             v
        Phase 13.3 recrawl scheduling
    """

    def __init__(
        self,
        backend: Optional[
            ContentChangeDetectionBackend
        ] = None,
        policy: Optional[
            ContentChangeDetectionPolicy
        ] = None,
    ) -> None:
        self.backend = (
            backend
            if backend is not None
            else InMemoryContentChangeDetectionMetadata()
        )

        self.policy = (
            policy
            if policy is not None
            else ContentChangeDetectionPolicy()
        )

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _clamp(
        value: float,
        minimum: float = 0.0,
        maximum: float = 1.0,
    ) -> float:
        if not isfinite(value):
            return minimum

        return max(
            minimum,
            min(maximum, value),
        )

    @staticmethod
    def _safe_float(
        value: object,
        default: float = 0.0,
    ) -> float:
        try:
            result = float(value)

            if not isfinite(result):
                return default

            return result

        except (TypeError, ValueError):
            return default

    @staticmethod
    def _parse_timestamp(
        value: Optional[str],
    ) -> Optional[datetime]:
        if not value:
            return None

        try:
            normalized = value

            if normalized.endswith("Z"):
                normalized = (
                    normalized[:-1] + "+00:00"
                )

            parsed = datetime.fromisoformat(
                normalized
            )

            if parsed.tzinfo is None:
                parsed = parsed.replace(
                    tzinfo=timezone.utc
                )

            return parsed.astimezone(
                timezone.utc
            )

        except (TypeError, ValueError):
            return None

    @classmethod
    def _interval_days(
        cls,
        older: Optional[str],
        newer: Optional[str],
    ) -> Optional[float]:
        older_dt = cls._parse_timestamp(older)
        newer_dt = cls._parse_timestamp(newer)

        if older_dt is None or newer_dt is None:
            return None

        return max(
            0.0,
            (
                newer_dt - older_dt
            ).total_seconds()
            / 86_400.0,
        )

    # ------------------------------------------------------------------
    # Events
    # ------------------------------------------------------------------

    def _event(
        self,
        identity: ChangeDetectionIdentity,
        event_type: ChangeEventType,
        state: ChangeDetectionState,
        metadata: Optional[Dict[str, str]] = None,
    ) -> ChangeDetectionEvent:
        event = ChangeDetectionEvent(
            event_id=str(uuid4()),
            request_id=identity.request_id,
            resource_id=identity.resource_id,
            event_type=event_type,
            timestamp=self._now(),
            state=state,
            metadata=metadata or {},
        )

        self.backend.persist_event(event)

        return event

    # ------------------------------------------------------------------
    # Checkpoints
    # ------------------------------------------------------------------

    def _checkpoint(
        self,
        identity: ChangeDetectionIdentity,
        checkpoint_type: ChangeCheckpointType,
        state: ChangeDetectionState,
        observation_id: Optional[str] = None,
        signals: Optional[
            ContentChangeSignalBundle
        ] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> ChangeDetectionCheckpoint:
        checkpoint = ChangeDetectionCheckpoint(
            checkpoint_id=str(uuid4()),
            request_id=identity.request_id,
            resource_id=identity.resource_id,
            checkpoint_type=checkpoint_type,
            state=state,
            created_at=self._now(),
            observation_id=observation_id,
            content_change_score=(
                signals.content_change_score
                if signals is not None
                else None
            ),
            semantic_change_score=(
                signals.semantic_change_score
                if signals is not None
                else None
            ),
            volatility_score=(
                signals.change_volatility
                if signals is not None
                else None
            ),
            metadata=metadata or {},
        )

        self.backend.persist_checkpoint(
            checkpoint
        )

        return checkpoint

    # ------------------------------------------------------------------
    # Observation normalization
    # ------------------------------------------------------------------

    def _normalize_observation(
        self,
        observation: ResourceObservation,
    ) -> ResourceObservation:
        content_size = (
            observation.content_size_bytes
            if observation.content_size_bytes is None
            else max(
                0,
                int(observation.content_size_bytes),
            )
        )

        text_size = (
            observation.text_size_bytes
            if observation.text_size_bytes is None
            else max(
                0,
                int(observation.text_size_bytes),
            )
        )

        section_count = (
            observation.section_count
            if observation.section_count is None
            else max(
                0,
                int(observation.section_count),
            )
        )

        link_count = (
            observation.link_count
            if observation.link_count is None
            else max(
                0,
                int(observation.link_count),
            )
        )

        image_count = (
            observation.image_count
            if observation.image_count is None
            else max(
                0,
                int(observation.image_count),
            )
        )

        media_count = (
            observation.media_count
            if observation.media_count is None
            else max(
                0,
                int(observation.media_count),
            )
        )

        return ResourceObservation(
            observation_id=observation.observation_id,
            resource_id=observation.resource_id,
            canonical_url=observation.canonical_url,
            observed_at=observation.observed_at,
            content_hash=observation.content_hash,
            text_hash=observation.text_hash,
            structure_hash=observation.structure_hash,
            metadata_hash=observation.metadata_hash,
            title_hash=observation.title_hash,
            description_hash=observation.description_hash,
            links_hash=observation.links_hash,
            media_hash=observation.media_hash,
            content_size_bytes=content_size,
            text_size_bytes=text_size,
            status_code=observation.status_code,
            etag=observation.etag,
            last_modified=observation.last_modified,
            section_count=section_count,
            link_count=link_count,
            image_count=image_count,
            media_count=media_count,
            content_fingerprint=observation.content_fingerprint,
            semantic_fingerprint=observation.semantic_fingerprint,
            partial=observation.partial,
            metadata=dict(observation.metadata),
        )

    # ------------------------------------------------------------------
    # Hash comparison
    # ------------------------------------------------------------------

    @staticmethod
    def _hash_changed(
        old: Optional[str],
        new: Optional[str],
    ) -> Optional[bool]:
        if old is None or new is None:
            return None

        return old != new

    # ------------------------------------------------------------------
    # Numeric change calculation
    # ------------------------------------------------------------------

    @classmethod
    def _relative_change(
        cls,
        old: Optional[int],
        new: Optional[int],
    ) -> float:
        if old is None or new is None:
            return 0.0

        old_value = max(
            0,
            int(old),
        )

        new_value = max(
            0,
            int(new),
        )

        denominator = max(
            1,
            old_value,
            new_value,
        )

        return cls._clamp(
            abs(new_value - old_value)
            / float(denominator)
        )

    # ------------------------------------------------------------------
    # Evidence construction
    # ------------------------------------------------------------------

    def _compare_observations(
        self,
        previous: ResourceObservation,
        current: ResourceObservation,
    ) -> List[ChangeEvidence]:
        evidence: List[ChangeEvidence] = []

        def add_hash_evidence(
            evidence_type: ChangeEvidenceType,
            old_hash: Optional[str],
            new_hash: Optional[str],
        ) -> None:
            changed = self._hash_changed(
                old_hash,
                new_hash,
            )

            if changed is None:
                return

            evidence.append(
                ChangeEvidence(
                    evidence_type=evidence_type,
                    changed=changed,
                    magnitude=(
                        1.0
                        if changed
                        else 0.0
                    ),
                    confidence=1.0,
                    old_value=old_hash,
                    new_value=new_hash,
                )
            )

        add_hash_evidence(
            ChangeEvidenceType.CONTENT_CHANGE,
            previous.content_hash,
            current.content_hash,
        )

        add_hash_evidence(
            ChangeEvidenceType.TEXT_CHANGE,
            previous.text_hash,
            current.text_hash,
        )

        add_hash_evidence(
            ChangeEvidenceType.STRUCTURAL_CHANGE,
            previous.structure_hash,
            current.structure_hash,
        )

        add_hash_evidence(
            ChangeEvidenceType.METADATA_CHANGE,
            previous.metadata_hash,
            current.metadata_hash,
        )

        add_hash_evidence(
            ChangeEvidenceType.TITLE_CHANGE,
            previous.title_hash,
            current.title_hash,
        )

        add_hash_evidence(
            ChangeEvidenceType.DESCRIPTION_CHANGE,
            previous.description_hash,
            current.description_hash,
        )

        add_hash_evidence(
            ChangeEvidenceType.LINK_CHANGE,
            previous.links_hash,
            current.links_hash,
        )

        add_hash_evidence(
            ChangeEvidenceType.MEDIA_CHANGE,
            previous.media_hash,
            current.media_hash,
        )

        add_hash_evidence(
            ChangeEvidenceType.SEMANTIC_CHANGE,
            previous.semantic_fingerprint,
            current.semantic_fingerprint,
        )

        add_hash_evidence(
            ChangeEvidenceType.HTTP_VALIDATOR_CHANGE,
            previous.etag,
            current.etag,
        )

        # Size and structural deltas provide magnitude even when the
        # corresponding hash is unavailable.
        content_delta = self._relative_change(
            previous.content_size_bytes,
            current.content_size_bytes,
        )

        if content_delta > 0.0:
            evidence.append(
                ChangeEvidence(
                    evidence_type=(
                        ChangeEvidenceType.CONTENT_CHANGE
                    ),
                    changed=True,
                    magnitude=content_delta,
                    confidence=0.80,
                    source="content_size_delta",
                )
            )

        text_delta = self._relative_change(
            previous.text_size_bytes,
            current.text_size_bytes,
        )

        if text_delta > 0.0:
            evidence.append(
                ChangeEvidence(
                    evidence_type=(
                        ChangeEvidenceType.TEXT_CHANGE
                    ),
                    changed=True,
                    magnitude=text_delta,
                    confidence=0.80,
                    source="text_size_delta",
                )
            )

        section_delta = self._relative_change(
            previous.section_count,
            current.section_count,
        )

        if section_delta > 0.0:
            evidence.append(
                ChangeEvidence(
                    evidence_type=(
                        ChangeEvidenceType.SECTION_CHANGE
                    ),
                    changed=True,
                    magnitude=section_delta,
                    confidence=0.75,
                    source="section_count_delta",
                )
            )

        link_delta = self._relative_change(
            previous.link_count,
            current.link_count,
        )

        if link_delta > 0.0:
            evidence.append(
                ChangeEvidence(
                    evidence_type=(
                        ChangeEvidenceType.LINK_CHANGE
                    ),
                    changed=True,
                    magnitude=link_delta,
                    confidence=0.75,
                    source="link_count_delta",
                )
            )

        image_delta = self._relative_change(
            previous.image_count,
            current.image_count,
        )

        if image_delta > 0.0:
            evidence.append(
                ChangeEvidence(
                    evidence_type=(
                        ChangeEvidenceType.IMAGE_CHANGE
                    ),
                    changed=True,
                    magnitude=image_delta,
                    confidence=0.70,
                    source="image_count_delta",
                )
            )

        media_delta = self._relative_change(
            previous.media_count,
            current.media_count,
        )

        if media_delta > 0.0:
            evidence.append(
                ChangeEvidence(
                    evidence_type=(
                        ChangeEvidenceType.MEDIA_CHANGE
                    ),
                    changed=True,
                    magnitude=media_delta,
                    confidence=0.70,
                    source="media_count_delta",
                )
            )

        if (
            previous.last_modified
            and current.last_modified
            and previous.last_modified
            != current.last_modified
        ):
            evidence.append(
                ChangeEvidence(
                    evidence_type=(
                        ChangeEvidenceType.TIMESTAMP_CHANGE
                    ),
                    changed=True,
                    magnitude=1.0,
                    confidence=0.85,
                    old_value=previous.last_modified,
                    new_value=current.last_modified,
                )
            )

        if (
            previous.canonical_url
            != current.canonical_url
        ):
            evidence.append(
                ChangeEvidence(
                    evidence_type=(
                        ChangeEvidenceType.URL_CHANGE
                    ),
                    changed=True,
                    magnitude=1.0,
                    confidence=1.0,
                    old_value=previous.canonical_url,
                    new_value=current.canonical_url,
                )
            )

        return evidence[
            : self.policy.max_evidence_per_comparison
        ]

    # ------------------------------------------------------------------
    # Evidence aggregation
    # ------------------------------------------------------------------

    @staticmethod
    def _best_magnitude(
        evidence: Sequence[ChangeEvidence],
        evidence_type: ChangeEvidenceType,
    ) -> float:
        matching = [
            item
            for item in evidence
            if item.evidence_type == evidence_type
        ]

        if not matching:
            return 0.0

        return max(
            item.magnitude
            for item in matching
        )

    def _aggregate_signals(
        self,
        evidence: Sequence[ChangeEvidence],
    ) -> Dict[str, float]:
        content = max(
            self._best_magnitude(
                evidence,
                ChangeEvidenceType.CONTENT_CHANGE,
            ),
            self._best_magnitude(
                evidence,
                ChangeEvidenceType.TEXT_CHANGE,
            ),
        )

        text = self._best_magnitude(
            evidence,
            ChangeEvidenceType.TEXT_CHANGE,
        )

        structural = max(
            self._best_magnitude(
                evidence,
                ChangeEvidenceType.STRUCTURAL_CHANGE,
            ),
            self._best_magnitude(
                evidence,
                ChangeEvidenceType.SECTION_CHANGE,
            ),
        )

        metadata = max(
            self._best_magnitude(
                evidence,
                ChangeEvidenceType.METADATA_CHANGE,
            ),
            self._best_magnitude(
                evidence,
                ChangeEvidenceType.TITLE_CHANGE,
            ),
            self._best_magnitude(
                evidence,
                ChangeEvidenceType.DESCRIPTION_CHANGE,
            ),
        )

        link = self._best_magnitude(
            evidence,
            ChangeEvidenceType.LINK_CHANGE,
        )

        media = max(
            self._best_magnitude(
                evidence,
                ChangeEvidenceType.MEDIA_CHANGE,
            ),
            self._best_magnitude(
                evidence,
                ChangeEvidenceType.IMAGE_CHANGE,
            ),
        )

        semantic = max(
            self._best_magnitude(
                evidence,
                ChangeEvidenceType.SEMANTIC_CHANGE,
            ),
            self._best_magnitude(
                evidence,
                ChangeEvidenceType.ENTITY_CHANGE,
            ),
        )

        return {
            "content": self._clamp(content),
            "text": self._clamp(text),
            "structural": self._clamp(structural),
            "metadata": self._clamp(metadata),
            "link": self._clamp(link),
            "media": self._clamp(media),
            "semantic": self._clamp(semantic),
        }

    # ------------------------------------------------------------------
    # Change magnitude
    # ------------------------------------------------------------------

    def _change_magnitude(
        self,
        score: float,
    ) -> ChangeMagnitude:
        if score >= self.policy.major_threshold:
            return ChangeMagnitude.MAJOR

        if score >= self.policy.very_large_threshold:
            return ChangeMagnitude.VERY_LARGE

        if score >= self.policy.large_threshold:
            return ChangeMagnitude.LARGE

        if score >= self.policy.moderate_threshold:
            return ChangeMagnitude.MODERATE

        if score >= self.policy.small_threshold:
            return ChangeMagnitude.SMALL

        if score >= self.policy.tiny_threshold:
            return ChangeMagnitude.TINY

        return ChangeMagnitude.NONE

    # ------------------------------------------------------------------
    # Change importance
    # ------------------------------------------------------------------

    def _change_importance(
        self,
        content_score: float,
        semantic_score: float,
        structural_score: float,
        link_score: float,
    ) -> ChangeImportance:
        importance = (
            0.35 * content_score
            + 0.35 * semantic_score
            + 0.20 * structural_score
            + 0.10 * link_score
        )

        importance = self._clamp(
            importance
        )

        if (
            importance
            >= self.policy.importance_critical_threshold
        ):
            return ChangeImportance.CRITICAL

        if (
            importance
            >= self.policy.importance_very_high_threshold
        ):
            return ChangeImportance.VERY_HIGH

        if (
            importance
            >= self.policy.importance_high_threshold
        ):
            return ChangeImportance.HIGH

        if (
            importance
            >= self.policy.importance_moderate_threshold
        ):
            return ChangeImportance.MODERATE

        if (
            importance
            >= self.policy.importance_low_threshold
        ):
            return ChangeImportance.LOW

        return ChangeImportance.NONE

    # ------------------------------------------------------------------
    # Historical frequency
    # ------------------------------------------------------------------

    def _change_frequency(
        self,
        history: ResourceChangeHistory,
    ) -> float:
        if history.observation_count <= 1:
            return self._clamp(
                history.historical_change_rate
            )

        rate = (
            history.change_count
            / float(
                max(
                    1,
                    history.observation_count - 1,
                )
            )
        )

        return self._clamp(
            max(
                rate,
                history.historical_change_rate,
            )
        )

    # ------------------------------------------------------------------
    # Volatility
    # ------------------------------------------------------------------

    def _volatility(
        self,
        history: ResourceChangeHistory,
        current_change: float,
        observation_interval_days: Optional[float],
    ) -> float:
        frequency = self._change_frequency(
            history
        )

        historical = self._clamp(
            history.historical_volatility
        )

        if (
            observation_interval_days is not None
            and observation_interval_days > 0.0
        ):
            normalized_rate = self._clamp(
                current_change
                / min(
                    1.0,
                    observation_interval_days
                    / self.policy.volatility_window_days,
                )
            )
        else:
            normalized_rate = current_change

        volatility = (
            0.40 * frequency
            + 0.35 * historical
            + 0.25 * normalized_rate
        )

        return self._clamp(
            volatility
        )

    # ------------------------------------------------------------------
    # Confidence
    # ------------------------------------------------------------------

    def _confidence(
        self,
        evidence: Sequence[ChangeEvidence],
        history: ResourceChangeHistory,
        previous: Optional[ResourceObservation],
        current: ResourceObservation,
    ) -> float:
        if not evidence:
            return 0.0

        evidence_confidence = (
            sum(
                self._clamp(
                    item.confidence
                )
                for item in evidence
            )
            / float(len(evidence))
        )

        historical_confidence = self._clamp(
            history.observation_count / 10.0
        )

        observation_completeness = 1.0

        if current.partial:
            observation_completeness *= 0.70

        if previous is not None and previous.partial:
            observation_completeness *= 0.85

        confidence = (
            0.65 * evidence_confidence
            + 0.20 * historical_confidence
            + 0.15 * observation_completeness
        )

        return self._clamp(
            confidence
        )

    # ------------------------------------------------------------------
    # Build final signal bundle
    # ------------------------------------------------------------------

    def _build_signal_bundle(
        self,
        evidence: Sequence[ChangeEvidence],
        history: ResourceChangeHistory,
        previous: Optional[ResourceObservation],
        current: ResourceObservation,
    ) -> ContentChangeSignalBundle:
        signals = self._aggregate_signals(
            evidence
        )

        content_score = signals["content"]
        text_score = signals["text"]
        structural_score = signals["structural"]
        metadata_score = signals["metadata"]
        link_score = signals["link"]
        media_score = signals["media"]
        semantic_score = signals["semantic"]

        weighted_total = (
            content_score
            * self.policy.content_weight
            + text_score
            * self.policy.text_weight
            + structural_score
            * self.policy.structural_weight
            + metadata_score
            * self.policy.metadata_weight
            + link_score
            * self.policy.link_weight
            + media_score
            * self.policy.media_weight
            + semantic_score
            * self.policy.semantic_weight
        )

        weight_total = (
            self.policy.content_weight
            + self.policy.text_weight
            + self.policy.structural_weight
            + self.policy.metadata_weight
            + self.policy.link_weight
            + self.policy.media_weight
            + self.policy.semantic_weight
        )

        overall_change = self._clamp(
            weighted_total
            / max(
                0.001,
                weight_total,
            )
        )

        interval_days = None

        if previous is not None:
            interval_days = self._interval_days(
                previous.observed_at,
                current.observed_at,
            )

        frequency = self._change_frequency(
            history
        )

        volatility = self._volatility(
            history,
            overall_change,
            interval_days,
        )

        confidence = self._confidence(
            evidence,
            history,
            previous,
            current,
        )

        unchanged = (
            overall_change
            <= self.policy.tiny_threshold
        )

        magnitude = self._change_magnitude(
            overall_change
        )

        importance = self._change_importance(
            content_score,
            semantic_score,
            structural_score,
            link_score,
        )

        partial = (
            current.partial
            or history.partial
            or any(
                item.partial
                for item in evidence
            )
        )

        return ContentChangeSignalBundle(
            resource_id=current.resource_id,
            content_change_score=content_score,
            text_change_score=text_score,
            structural_change_score=structural_score,
            metadata_change_score=metadata_score,
            link_change_score=link_score,
            media_change_score=media_score,
            semantic_change_score=semantic_score,
            change_magnitude=magnitude,
            change_importance=importance,
            change_frequency=frequency,
            change_volatility=volatility,
            confidence=confidence,
            unchanged=unchanged,
            partial=partial,
            evidence=list(evidence),
        )

    # ------------------------------------------------------------------
    # Historical update
    # ------------------------------------------------------------------

    def update_history(
        self,
        history: ResourceChangeHistory,
        current: ResourceObservation,
        changed: bool,
        change_magnitude: float,
        content_change: float,
        text_change: float,
        structural_change: float,
        semantic_change: float,
    ) -> ResourceChangeHistory:
        observation_count = (
            history.observation_count + 1
        )

        change_count = (
            history.change_count
            + (1 if changed else 0)
        )

        recent_change_count = (
            history.recent_change_count
            + (1 if changed else 0)
        )

        average_magnitude = (
            (
                history.average_change_magnitude
                * history.change_count
                + change_magnitude
            )
            / float(
                max(
                    1,
                    change_count,
                )
            )
        )

        historical_rate = self._clamp(
            change_count
            / float(
                max(
                    1,
                    observation_count - 1,
                )
            )
        )

        historical_volatility = self._clamp(
            0.50 * historical_rate
            + 0.30
            * self._clamp(
                average_magnitude
            )
            + 0.20
            * self._clamp(
                history.historical_volatility
            )
        )

        return ResourceChangeHistory(
            resource_id=history.resource_id,
            observation_count=observation_count,
            change_count=change_count,
            first_observed_at=(
                history.first_observed_at
                or current.observed_at
            ),
            last_observed_at=current.observed_at,
            last_changed_at=(
                current.observed_at
                if changed
                else history.last_changed_at
            ),
            total_content_change=(
                history.total_content_change
                + content_change
            ),
            total_text_change=(
                history.total_text_change
                + text_change
            ),
            total_structural_change=(
                history.total_structural_change
                + structural_change
            ),
            total_semantic_change=(
                history.total_semantic_change
                + semantic_change
            ),
            recent_change_count=recent_change_count,
            average_change_magnitude=(
                average_magnitude
            ),
            average_change_interval_days=(
                history.average_change_interval_days
            ),
            historical_change_rate=(
                historical_rate
            ),
            historical_volatility=(
                historical_volatility
            ),
            partial=(
                history.partial
                or current.partial
            ),
        )

    # ------------------------------------------------------------------
    # Main detection method
    # ------------------------------------------------------------------

    def detect(
        self,
        identity: ChangeDetectionIdentity,
        current: ResourceObservation,
        history: ResourceChangeHistory,
        previous: Optional[ResourceObservation] = None,
    ) -> ContentChangeDetectionResult:
        created_at = self._now()

        try:
            self._event(
                identity,
                ChangeEventType.REQUEST_RECEIVED,
                ChangeDetectionState.RECEIVED,
            )

            # ----------------------------------------------------------
            # Validation
            # ----------------------------------------------------------

            self._event(
                identity,
                ChangeEventType.VALIDATION_STARTED,
                ChangeDetectionState.VALIDATING,
            )

            if identity.resource_id != current.resource_id:
                raise ValueError(
                    "identity.resource_id must match current.resource_id"
                )

            if history.resource_id != current.resource_id:
                raise ValueError(
                    "history.resource_id must match current.resource_id"
                )

            if previous is not None:
                if (
                    previous.resource_id
                    != current.resource_id
                ):
                    raise ValueError(
                        "previous and current observations "
                        "must reference the same resource"
                    )

            if not identity.canonical_url:
                raise ValueError(
                    "canonical_url must not be empty"
                )

            if not identity.hostname:
                raise ValueError(
                    "hostname must not be empty"
                )

            normalized_current = (
                self._normalize_observation(
                    current
                )
            )

            normalized_previous = (
                self._normalize_observation(
                    previous
                )
                if previous is not None
                else None
            )

            if (
                normalized_current.partial
                and not self.policy.allow_partial
            ):
                raise ValueError(
                    "partial observations are disabled"
                )

            self._event(
                identity,
                ChangeEventType.OBSERVATION_NORMALIZATION_STARTED,
                ChangeDetectionState.OBSERVATION_NORMALIZATION,
                metadata={
                    "current_observation_id": (
                        normalized_current.observation_id
                    ),
                    "previous_available": str(
                        normalized_previous is not None
                    ),
                },
            )

            self._checkpoint(
                identity,
                ChangeCheckpointType.OBSERVATIONS_NORMALIZED,
                ChangeDetectionState.OBSERVATION_NORMALIZATION,
                observation_id=(
                    normalized_current.observation_id
                ),
            )

            # ----------------------------------------------------------
            # First observation
            # ----------------------------------------------------------

            if normalized_previous is None:
                evidence: List[ChangeEvidence] = []

                signals = ContentChangeSignalBundle(
                    resource_id=(
                        normalized_current.resource_id
                    ),
                    content_change_score=0.0,
                    text_change_score=0.0,
                    structural_change_score=0.0,
                    metadata_change_score=0.0,
                    link_change_score=0.0,
                    media_change_score=0.0,
                    semantic_change_score=0.0,
                    change_magnitude=(
                        ChangeMagnitude.NONE
                    ),
                    change_importance=(
                        ChangeImportance.NONE
                    ),
                    change_frequency=(
                        self._change_frequency(
                            history
                        )
                    ),
                    change_volatility=(
                        self._clamp(
                            history.historical_volatility
                        )
                    ),
                    confidence=0.25,
                    unchanged=True,
                    partial=(
                        normalized_current.partial
                    ),
                    evidence=evidence,
                )

                state = (
                    ChangeDetectionState.PARTIAL
                    if normalized_current.partial
                    else ChangeDetectionState.UNCHANGED
                )

                lineage = ChangeDetectionLineage(
                    observation_ids=[
                        normalized_current.observation_id
                    ],
                    parent_request_ids=[
                        identity.request_id
                    ],
                    calculation_timestamp=self._now(),
                )

                result = ContentChangeDetectionResult(
                    identity=identity,
                    lineage=lineage,
                    state=state,
                    previous_observation_id=None,
                    current_observation_id=(
                        normalized_current.observation_id
                    ),
                    signals=signals,
                    created_at=created_at,
                    completed_at=self._now(),
                )

                self.backend.persist_result(
                    result
                )

                self._event(
                    identity,
                    ChangeEventType.DETECTION_COMPLETED,
                    state,
                    metadata={
                        "first_observation": "true",
                    },
                )

                return result

            # ----------------------------------------------------------
            # Version comparison
            # ----------------------------------------------------------

            self._event(
                identity,
                ChangeEventType.VERSION_COMPARISON_STARTED,
                ChangeDetectionState.VERSION_COMPARISON,
            )

            evidence = self._compare_observations(
                normalized_previous,
                normalized_current,
            )

            self._checkpoint(
                identity,
                ChangeCheckpointType.VERSIONS_COMPARED,
                ChangeDetectionState.VERSION_COMPARISON,
                observation_id=(
                    normalized_current.observation_id
                ),
                metadata={
                    "evidence_count": str(
                        len(evidence)
                    )
                },
            )

            changed = any(
                item.changed
                and item.magnitude
                > self.policy.tiny_threshold
                for item in evidence
            )

            if changed:
                self._event(
                    identity,
                    ChangeEventType.CHANGE_DETECTED,
                    ChangeDetectionState.CHANGE_EXTRACTION,
                    metadata={
                        "evidence_count": str(
                            len(evidence)
                        )
                    },
                )
            else:
                self._event(
                    identity,
                    ChangeEventType.NO_CHANGE_DETECTED,
                    ChangeDetectionState.UNCHANGED,
                )

            # ----------------------------------------------------------
            # Signal calculation
            # ----------------------------------------------------------

            signals = self._build_signal_bundle(
                evidence,
                history,
                normalized_previous,
                normalized_current,
            )

            self._event(
                identity,
                ChangeEventType.CHANGE_SIGNALS_CALCULATED,
                ChangeDetectionState.SIGNAL_CALCULATION,
                metadata={
                    "content_change_score": (
                        f"{signals.content_change_score:.6f}"
                    ),
                    "semantic_change_score": (
                        f"{signals.semantic_change_score:.6f}"
                    ),
                    "change_magnitude": (
                        signals.change_magnitude.value
                    ),
                },
            )

            self._checkpoint(
                identity,
                ChangeCheckpointType.SIGNALS_CALCULATED,
                ChangeDetectionState.SIGNAL_CALCULATION,
                observation_id=(
                    normalized_current.observation_id
                ),
                signals=signals,
            )

            # ----------------------------------------------------------
            # Historical state update
            # ----------------------------------------------------------

            updated_history = self.update_history(
                history=history,
                current=normalized_current,
                changed=changed,
                change_magnitude=(
                    signals.content_change_score
                ),
                content_change=(
                    signals.content_change_score
                ),
                text_change=(
                    signals.text_change_score
                ),
                structural_change=(
                    signals.structural_change_score
                ),
                semantic_change=(
                    signals.semantic_change_score
                ),
            )

            # Recalculate historical/frequency/volatility
            # using the updated state.
            signals.change_frequency = (
                self._change_frequency(
                    updated_history
                )
            )

            interval_days = self._interval_days(
                normalized_previous.observed_at,
                normalized_current.observed_at,
            )

            signals.change_volatility = (
                self._volatility(
                    updated_history,
                    signals.content_change_score,
                    interval_days,
                )
            )

            signals.confidence = (
                self._confidence(
                    evidence,
                    updated_history,
                    normalized_previous,
                    normalized_current,
                )
            )

            self._event(
                identity,
                ChangeEventType.VOLATILITY_ESTIMATED,
                ChangeDetectionState.VOLATILITY_ESTIMATION,
                metadata={
                    "change_frequency": (
                        f"{signals.change_frequency:.6f}"
                    ),
                    "change_volatility": (
                        f"{signals.change_volatility:.6f}"
                    ),
                },
            )

            self._checkpoint(
                identity,
                ChangeCheckpointType.VOLATILITY_CALCULATED,
                ChangeDetectionState.VOLATILITY_ESTIMATION,
                observation_id=(
                    normalized_current.observation_id
                ),
                signals=signals,
            )

            # ----------------------------------------------------------
            # Partial state
            # ----------------------------------------------------------

            final_state = (
                ChangeDetectionState.PARTIAL
                if signals.partial
                else (
                    ChangeDetectionState.UNCHANGED
                    if signals.unchanged
                    else ChangeDetectionState.COMPLETED
                )
            )

            if (
                signals.partial
                and self.policy.allow_partial
            ):
                self._event(
                    identity,
                    ChangeEventType.PARTIAL_INPUT_DETECTED,
                    ChangeDetectionState.PARTIAL,
                )

            # ----------------------------------------------------------
            # Lineage
            # ----------------------------------------------------------

            lineage = ChangeDetectionLineage(
                source_system=(
                    "phase13.2-content-change-detection-signals"
                ),
                source_version=ARCHITECTURE_VERSION,
                previous_stage=(
                    "phase13.1-crawl-freshness-prioritization"
                ),
                observation_ids=[
                    normalized_previous.observation_id,
                    normalized_current.observation_id,
                ],
                parent_request_ids=[
                    identity.request_id
                ],
                calculation_timestamp=self._now(),
                provenance_preserved=True,
            )

            result = ContentChangeDetectionResult(
                identity=identity,
                lineage=lineage,
                state=final_state,
                previous_observation_id=(
                    normalized_previous.observation_id
                ),
                current_observation_id=(
                    normalized_current.observation_id
                ),
                signals=signals,
                created_at=created_at,
                completed_at=self._now(),
            )

            self.backend.persist_result(
                result
            )

            self._checkpoint(
                identity,
                ChangeCheckpointType.COMPLETED,
                final_state,
                observation_id=(
                    normalized_current.observation_id
                ),
                signals=signals,
            )

            self._event(
                identity,
                ChangeEventType.DETECTION_COMPLETED,
                final_state,
                metadata={
                    "changed": str(changed),
                    "content_change_score": (
                        f"{signals.content_change_score:.6f}"
                    ),
                    "volatility": (
                        f"{signals.change_volatility:.6f}"
                    ),
                },
            )

            return result

        except Exception as exc:
            self._event(
                identity,
                ChangeEventType.DETECTION_FAILED,
                ChangeDetectionState.FAILED,
                metadata={
                    "error": str(exc)
                },
            )

            lineage = ChangeDetectionLineage(
                observation_ids=[
                    current.observation_id
                ],
                parent_request_ids=[
                    identity.request_id
                ],
                calculation_timestamp=self._now(),
            )

            failure_signals = (
                ContentChangeSignalBundle(
                    resource_id=(
                        identity.resource_id
                    ),
                    content_change_score=0.0,
                    text_change_score=0.0,
                    structural_change_score=0.0,
                    metadata_change_score=0.0,
                    link_change_score=0.0,
                    media_change_score=0.0,
                    semantic_change_score=0.0,
                    change_magnitude=(
                        ChangeMagnitude.NONE
                    ),
                    change_importance=(
                        ChangeImportance.NONE
                    ),
                    change_frequency=0.0,
                    change_volatility=0.0,
                    confidence=0.0,
                    unchanged=True,
                    partial=True,
                )
            )

            result = ContentChangeDetectionResult(
                identity=identity,
                lineage=lineage,
                state=ChangeDetectionState.FAILED,
                previous_observation_id=(
                    previous.observation_id
                    if previous is not None
                    else None
                ),
                current_observation_id=(
                    current.observation_id
                ),
                signals=failure_signals,
                created_at=created_at,
                completed_at=self._now(),
                error=str(exc),
            )

            self.backend.persist_result(
                result
            )

            return result

    # ------------------------------------------------------------------
    # Batch detection
    # ------------------------------------------------------------------

    def detect_many(
        self,
        requests: Sequence[
            Tuple[
                ChangeDetectionIdentity,
                ResourceObservation,
                ResourceChangeHistory,
                Optional[ResourceObservation],
            ]
        ],
    ) -> List[ContentChangeDetectionResult]:
        if (
            len(requests)
            > self.policy.max_resources_per_batch
        ):
            raise ValueError(
                "batch exceeds per-resource processing limit"
            )

        results: List[
            ContentChangeDetectionResult
        ] = []

        for (
            identity,
            current,
            history,
            previous,
        ) in requests:
            results.append(
                self.detect(
                    identity=identity,
                    current=current,
                    history=history,
                    previous=previous,
                )
            )

        return results

    # ------------------------------------------------------------------
    # Architecture description
    # ------------------------------------------------------------------

    def architecture(self) -> Dict[str, object]:
        return {
            "phase": "13",
            "stage": "13.2",
            "name": (
                "Change Detection / Content Change Signals"
            ),
            "version": ARCHITECTURE_VERSION,

            "scale_target": SCALE_TARGET,

            "google_scale_capability_target": (
                GOOGLE_SCALE_CAPABILITY_TARGET
            ),

            "google_technology_dependency": (
                GOOGLE_TECHNOLOGY_DEPENDENCY
            ),

            "purpose": (
                "Detect and represent content-change evidence, "
                "change magnitude, frequency, and volatility "
                "for enormous public-Web resources."
            ),

            "inputs": [
                "current resource observation",
                "previous resource observation",
                "resource change history",
                "content fingerprints",
                "text fingerprints",
                "structural fingerprints",
                "semantic fingerprints",
                "metadata fingerprints",
                "HTTP validators",
                "resource-size observations",
                "section-count observations",
                "link-count observations",
                "media observations",
            ],

            "outputs": [
                "content change score",
                "text change score",
                "structural change score",
                "metadata change score",
                "link change score",
                "media change score",
                "semantic change score",
                "change magnitude",
                "change importance",
                "change frequency",
                "change volatility",
                "confidence",
                "change evidence",
                "version lineage",
                "checkpoint state",
            ],

            "change_detection_pipeline": [
                "observation ingestion",
                "observation normalization",
                "version comparison",
                "hash/fingerprint comparison",
                "numeric delta analysis",
                "change evidence extraction",
                "signal aggregation",
                "change magnitude estimation",
                "change importance estimation",
                "historical frequency calculation",
                "volatility estimation",
                "confidence calculation",
                "lineage preservation",
                "checkpoint persistence",
                "result persistence",
            ],

            "distributed_execution": True,
            "partition_local_computation": True,
            "resource_partitionable": True,
            "horizontally_scalable": True,

            "deterministic": True,
            "checkpointable": True,
            "restartable": True,

            "partial_input_supported": (
                self.policy.allow_partial
            ),

            "incremental_history_supported": True,
            "incremental_observation_supported": True,

            "provenance_preserved": True,
            "version_lineage_preserved": True,

            "backend_replaceable": True,

            "no_global_resource_ceiling": True,
            "no_global_document_ceiling": True,
            "no_global_url_ceiling": True,
            "no_global_observation_ceiling": True,
            "no_global_partition_ceiling": True,
            "no_global_change_history_ceiling": True,

            "no_google_api_dependency": True,
            "no_google_index_dependency": True,
            "no_google_crawler_dependency": True,
            "no_google_infrastructure_dependency": True,
            "no_google_ranking_dependency": True,

            "does_not_execute_crawl": True,
            "does_not_schedule_recrawl": True,
            "does_not_allocate_crawler_workers": True,
            "does_not_mutate_search_index": True,
            "does_not_perform_final_ranking": True,
            "does_not_classify_spam": True,

            "integration": {
                "phase13_1": (
                    "Consumes freshness-prioritization "
                    "context where available."
                ),
                "phase13_3": (
                    "Provides change-frequency and "
                    "volatility signals to recrawl scheduling."
                ),
                "phase12_4": (
                    "Provides temporal change evidence "
                    "that can complement freshness signals."
                ),
            },

            "next_stage": NEXT_STAGE,
            "next_stage_name": (
                "Recrawl Scheduling"
            ),
        }


# ---------------------------------------------------------------------------
# Aliases
# ---------------------------------------------------------------------------


ContentChangeDetectionSignals = (
    ContentChangeDetectionSignalsArchitecture
)

GlobalContentChangeDetectionSignals = (
    ContentChangeDetectionSignalsArchitecture
)

Phase13_2ContentChangeDetectionSignals = (
    ContentChangeDetectionSignalsArchitecture
)

ChangeDetection = (
    ContentChangeDetectionSignalsArchitecture
)


# ---------------------------------------------------------------------------
# Public exports
# ---------------------------------------------------------------------------


__all__ = [
    "SCALE_TARGET",
    "GOOGLE_SCALE_CAPABILITY_TARGET",
    "GOOGLE_TECHNOLOGY_DEPENDENCY",
    "ARCHITECTURE_VERSION",
    "PHASE",
    "PREVIOUS_STAGE",
    "NEXT_STAGE",

    "ChangeDetectionState",
    "ChangeEvidenceType",
    "ChangeMagnitude",
    "ChangeImportance",
    "ChangeVolatilityBand",
    "ChangeEventType",
    "ChangeCheckpointType",

    "ChangeDetectionIdentity",
    "ChangeDetectionLineage",

    "ResourceObservation",
    "ChangeEvidence",
    "ContentChangeSignalBundle",
    "ResourceChangeHistory",

    "ContentChangeDetectionPolicy",

    "ContentChangeDetectionResult",
    "ChangeDetectionCheckpoint",
    "ChangeDetectionEvent",

    "ContentChangeDetectionBackend",
    "InMemoryContentChangeDetectionMetadata",

    "ContentChangeDetectionSignalsArchitecture",
    "ContentChangeDetectionSignals",
    "GlobalContentChangeDetectionSignals",
    "Phase13_2ContentChangeDetectionSignals",
    "ChangeDetection",
]
