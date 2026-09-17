"""
OUR SEARCH — Phase 14.9
Final Spam / Abuse / Security / Quality Architecture

Purpose
-------
Final integrated control-plane architecture for OUR SEARCH's complete
Spam / Abuse / Security / Quality Systems.

This stage integrates evidence and planning outputs from:

    14.1 Spam Detection Signal Architecture
    14.2 Content Quality & Manipulation Detection
    14.3 Link Spam / Graph Abuse Detection
    14.4 Malware / Phishing / Harmful Resource Detection
    14.5 Crawl / Index Abuse & Resource Protection
    14.6 Distributed Abuse Detection & Enforcement
    14.7 Security / Trust / Safe Browsing Signals
    14.8 Global Quality Control / Quarantine / Recovery

The architecture produces a final, auditable, deterministic control-plane
decision describing monitoring, protection, review, quarantine planning,
recovery planning, restoration planning, or deferral.

IMPORTANT
---------
This module does NOT:

- execute malware
- execute payloads
- execute exploits
- attack external systems
- actively scan arbitrary systems
- perform HTTP requests
- perform crawling
- assign crawler workers
- mutate the search index directly
- delete resources
- block resources directly
- execute quarantine directly
- execute restoration directly
- modify external systems
- rank search results
- replace the crawler
- replace the index
- replace ranking
- replace freshness systems
- use Google Search APIs
- use Google's index
- use Google's crawler
- use Google's infrastructure
- use Google's ranking technology

All enforcement, quarantine, recovery, and restoration outputs are plans or
control-plane intents for downstream infrastructure.

Scale target
------------
billions -> trillions of publicly accessible Web resources.

Design principles
-----------------
- deterministic
- distributed
- partition-aware
- shard-aware
- region-aware
- domain-aware
- host-aware
- cross-resource aware
- temporal
- historical
- provenance-preserving
- partial-evidence aware
- auditable
- idempotent
- failure-tolerant
- policy-driven
- Google-independent
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Iterable, List, Mapping, Optional, Protocol, Sequence, Tuple


# ============================================================================
# GLOBAL ARCHITECTURE METADATA
# ============================================================================

SCALE_TARGET = "billions_to_trillions_of_public_web_resources"
GOOGLE_SCALE_CAPABILITY_TARGET = True
GOOGLE_TECHNOLOGY_DEPENDENCY = False

ARCHITECTURE_VERSION = "final-spam-abuse-security-quality-architecture.v1"

PHASE = "14.9"
PREVIOUS_STAGE = "14.8"
NEXT_PHASE = "15"

PHASE_NAME = "Final Spam / Abuse / Security / Quality Architecture"
NEXT_PHASE_NAME = "Full Scale-Proof Program"


# ============================================================================
# ENUMS
# ============================================================================


class FinalQualityState(str, Enum):
    RECEIVED = "received"
    VALIDATING = "validating"
    NORMALIZING = "normalizing"
    EVIDENCE_AGGREGATION = "evidence_aggregation"

    SPAM_ANALYSIS = "spam_analysis"
    CONTENT_QUALITY_ANALYSIS = "content_quality_analysis"
    MANIPULATION_ANALYSIS = "manipulation_analysis"
    LINK_ABUSE_ANALYSIS = "link_abuse_analysis"

    MALWARE_ANALYSIS = "malware_analysis"
    PHISHING_ANALYSIS = "phishing_analysis"
    HARMFUL_RESOURCE_ANALYSIS = "harmful_resource_analysis"

    CRAWL_ABUSE_ANALYSIS = "crawl_abuse_analysis"
    INDEX_ABUSE_ANALYSIS = "index_abuse_analysis"
    RESOURCE_PROTECTION_ANALYSIS = "resource_protection_analysis"

    SECURITY_ANALYSIS = "security_analysis"
    TRUST_ANALYSIS = "trust_analysis"
    REPUTATION_ANALYSIS = "reputation_analysis"

    DISTRIBUTED_ABUSE_ANALYSIS = "distributed_abuse_analysis"
    CROSS_RESOURCE_ANALYSIS = "cross_resource_analysis"
    TEMPORAL_ANALYSIS = "temporal_analysis"
    HISTORICAL_ANALYSIS = "historical_analysis"

    RECOVERY_ANALYSIS = "recovery_analysis"
    RESTORATION_ANALYSIS = "restoration_analysis"

    CORRELATION = "correlation"
    RISK_CALCULATION = "risk_calculation"
    CONFIDENCE_CALCULATION = "confidence_calculation"

    POLICY_EVALUATION = "policy_evaluation"
    CONTROL_PLANNING = "control_planning"
    QUARANTINE_PLANNING = "quarantine_planning"
    RECOVERY_PLANNING = "recovery_planning"
    RESTORATION_PLANNING = "restoration_planning"
    REVIEW_PLANNING = "review_planning"

    IDEMPOTENCY_CHECK = "idempotency_check"
    CHECKPOINTING = "checkpointing"
    DECISION_READY = "decision_ready"

    COMPLETED = "completed"
    PARTIAL = "partial"
    DEFERRED = "deferred"
    REJECTED = "rejected"
    FAILED = "failed"


class FinalQualitySignalFamily(str, Enum):
    SPAM = "spam"
    CONTENT_QUALITY = "content_quality"
    MANIPULATION = "manipulation"
    LINK_ABUSE = "link_abuse"

    MALWARE = "malware"
    PHISHING = "phishing"
    HARMFUL_RESOURCE = "harmful_resource"

    CRAWL_ABUSE = "crawl_abuse"
    INDEX_ABUSE = "index_abuse"
    RESOURCE_PROTECTION = "resource_protection"

    SECURITY = "security"
    TRUST = "trust"
    REPUTATION = "reputation"

    DISTRIBUTED_ABUSE = "distributed_abuse"
    CROSS_RESOURCE = "cross_resource"
    TEMPORAL = "temporal"
    HISTORICAL = "historical"

    POLICY = "policy"
    SOURCE = "source"
    TECHNICAL = "technical"

    RECOVERY = "recovery"
    RESTORATION = "restoration"


class FinalQualitySignalType(str, Enum):
    SPAM_RISK = "spam_risk"
    CONTENT_QUALITY_RISK = "content_quality_risk"
    MANIPULATION_RISK = "manipulation_risk"
    LINK_ABUSE_RISK = "link_abuse_risk"

    MALWARE_RISK = "malware_risk"
    PHISHING_RISK = "phishing_risk"
    HARMFUL_RESOURCE_RISK = "harmful_resource_risk"

    CRAWL_ABUSE_RISK = "crawl_abuse_risk"
    INDEX_ABUSE_RISK = "index_abuse_risk"
    RESOURCE_PROTECTION_RISK = "resource_protection_risk"

    SECURITY_RISK = "security_risk"
    TRUST_RISK = "trust_risk"
    REPUTATION_RISK = "reputation_risk"

    DISTRIBUTED_ABUSE_RISK = "distributed_abuse_risk"
    CROSS_RESOURCE_CLUSTER_RISK = "cross_resource_cluster_risk"
    TEMPORAL_RISK = "temporal_risk"
    HISTORICAL_RISK = "historical_risk"

    POLICY_BYPASS_RISK = "policy_bypass_risk"
    SOURCE_RELIABILITY_RISK = "source_reliability_risk"
    TECHNICAL_RISK = "technical_risk"

    RECOVERY_ELIGIBILITY = "recovery_eligibility"
    RESTORATION_ELIGIBILITY = "restoration_eligibility"


class FinalQualityEvidenceStrength(str, Enum):
    NONE = "none"
    WEAK = "weak"
    MODERATE = "moderate"
    STRONG = "strong"
    VERY_STRONG = "very_strong"


class FinalRiskBand(str, Enum):
    NONE = "none"
    LOW = "low"
    MODERATE = "moderate"
    HIGH = "high"
    VERY_HIGH = "very_high"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


class FinalQualityAction(str, Enum):
    NONE = "none"
    MONITOR = "monitor"
    PROTECT = "protect"
    DEFER = "defer"
    REQUIRE_REVIEW = "require_review"

    QUARANTINE_RESOURCE = "quarantine_resource"
    QUARANTINE_CLUSTER = "quarantine_cluster"
    QUARANTINE_DOMAIN = "quarantine_domain"
    QUARANTINE_HOST = "quarantine_host"

    SUSPEND_PROCESSING = "suspend_processing"

    RECOVERY_REVIEW = "recovery_review"
    RESTORE_RESOURCE = "restore_resource"
    RESTORE_CLUSTER = "restore_cluster"
    RESTORE_DOMAIN = "restore_domain"
    HOLD_RESTORATION = "hold_restoration"


class FinalQualityScope(str, Enum):
    RESOURCE = "resource"
    DOCUMENT = "document"
    URL_VARIANT = "url_variant"
    HOST = "host"
    DOMAIN = "domain"
    CLUSTER = "cluster"
    PARTITION = "partition"
    SHARD = "shard"
    REGION = "region"
    GLOBAL = "global"


class FinalQualityDecision(str, Enum):
    NO_ACTION = "no_action"
    MONITOR = "monitor"
    PROTECT = "protect"
    DEFERRED = "deferred"
    REVIEW_REQUIRED = "review_required"

    QUARANTINE_PLANNED = "quarantine_planned"
    RECOVERY_PLANNED = "recovery_planned"
    RESTORATION_PLANNED = "restoration_planned"

    PARTIAL_EVIDENCE = "partial_evidence"
    REJECTED_INPUT = "rejected_input"


class FinalQualityConfidence(str, Enum):
    UNKNOWN = "unknown"
    LOW = "low"
    MODERATE = "moderate"
    HIGH = "high"
    VERY_HIGH = "very_high"


class FinalQualityEvidenceKind(str, Enum):
    SPAM = "spam"
    CONTENT_QUALITY = "content_quality"
    MANIPULATION = "manipulation"
    LINK_ABUSE = "link_abuse"

    MALWARE = "malware"
    PHISHING = "phishing"
    HARMFUL = "harmful"

    CRAWL_ABUSE = "crawl_abuse"
    INDEX_ABUSE = "index_abuse"
    RESOURCE_PROTECTION = "resource_protection"

    SECURITY = "security"
    TRUST = "trust"
    REPUTATION = "reputation"

    DISTRIBUTED = "distributed"
    CROSS_RESOURCE = "cross_resource"
    TEMPORAL = "temporal"
    HISTORICAL = "historical"

    POLICY = "policy"
    SOURCE = "source"
    TECHNICAL = "technical"

    RECOVERY = "recovery"
    RESTORATION = "restoration"


class FinalRecoveryState(str, Enum):
    NOT_ELIGIBLE = "not_eligible"
    ELIGIBILITY_REVIEW = "eligibility_review"
    EVIDENCE_REQUIRED = "evidence_required"
    READY = "ready"
    HOLD = "hold"
    DEFERRED = "deferred"
    PLANNED = "planned"


class FinalRestorationState(str, Enum):
    NOT_ELIGIBLE = "not_eligible"
    ELIGIBILITY_REVIEW = "eligibility_review"
    EVIDENCE_REQUIRED = "evidence_required"
    READY = "ready"
    HOLD = "hold"
    DEFERRED = "deferred"
    PLANNED = "planned"


class FinalQualityEventType(str, Enum):
    ANALYSIS_STARTED = "analysis_started"
    INPUT_ACCEPTED = "input_accepted"
    INPUT_NORMALIZED = "input_normalized"

    EVIDENCE_ACCEPTED = "evidence_accepted"
    SIGNAL_DETECTED = "signal_detected"

    GLOBAL_CORRELATION = "global_correlation"
    RISK_CALCULATED = "risk_calculated"
    CONFIDENCE_CALCULATED = "confidence_calculated"

    POLICY_EVALUATED = "policy_evaluated"

    CONTROL_PLANNED = "control_planned"
    QUARANTINE_PLANNED = "quarantine_planned"

    RECOVERY_ANALYZED = "recovery_analyzed"
    RESTORATION_ANALYZED = "restoration_analyzed"

    RECOVERY_PLANNED = "recovery_planned"
    RESTORATION_PLANNED = "restoration_planned"
    REVIEW_PLANNED = "review_planned"

    IDEMPOTENCY_CHECKED = "idempotency_checked"
    CHECKPOINT_CREATED = "checkpoint_created"
    DECISION_PREPARED = "decision_prepared"

    ANALYSIS_COMPLETED = "analysis_completed"
    ANALYSIS_PARTIAL = "analysis_partial"
    ANALYSIS_DEFERRED = "analysis_deferred"
    ANALYSIS_REJECTED = "analysis_rejected"
    ANALYSIS_FAILED = "analysis_failed"


class FinalQualityCheckpointType(str, Enum):
    INPUT_ACCEPTED = "input_accepted"
    NORMALIZATION_COMPLETE = "normalization_complete"
    EVIDENCE_COMPLETE = "evidence_complete"
    CORRELATION_COMPLETE = "correlation_complete"
    RISK_COMPLETE = "risk_complete"
    CONFIDENCE_COMPLETE = "confidence_complete"
    POLICY_COMPLETE = "policy_complete"

    CONTROL_PLAN_COMPLETE = "control_plan_complete"
    QUARANTINE_PLAN_COMPLETE = "quarantine_plan_complete"
    RECOVERY_PLAN_COMPLETE = "recovery_plan_complete"
    RESTORATION_PLAN_COMPLETE = "restoration_plan_complete"

    RESULT_COMPLETE = "result_complete"


# ============================================================================
# DATA MODELS
# ============================================================================


@dataclass(frozen=True)
class FinalQualityIdentity:
    resource_id: str = ""
    document_id: str = ""
    url: str = ""

    host_id: str = ""
    domain_id: str = ""

    cluster_id: str = ""
    partition_id: str = ""
    shard_id: str = ""
    region_id: str = ""

    version: str = "1"


@dataclass(frozen=True)
class FinalQualityLineage:
    resource_id: str = ""
    current_stage: str = PHASE
    previous_stage: str = PREVIOUS_STAGE
    previous_stage_version: str = ""

    source_evidence_ids: Tuple[str, ...] = ()
    source_evidence_versions: Tuple[str, ...] = ()

    source_signal_ids: Tuple[str, ...] = ()
    source_signal_versions: Tuple[str, ...] = ()

    parent_resource_ids: Tuple[str, ...] = ()
    parent_document_ids: Tuple[str, ...] = ()

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FinalQualityInput:
    identity: FinalQualityIdentity = field(
        default_factory=FinalQualityIdentity
    )

    lineage: FinalQualityLineage = field(
        default_factory=FinalQualityLineage
    )

    # Earlier-stage evidence
    spam_risk: float = 0.0
    content_quality_risk: float = 0.0
    manipulation_risk: float = 0.0
    link_abuse_risk: float = 0.0

    malware_risk: float = 0.0
    phishing_risk: float = 0.0
    harmful_resource_risk: float = 0.0

    crawl_abuse_risk: float = 0.0
    index_abuse_risk: float = 0.0
    resource_protection_risk: float = 0.0

    security_risk: float = 0.0
    trust_risk: float = 0.0
    reputation_risk: float = 0.0

    distributed_abuse_risk: float = 0.0
    cross_resource_risk: float = 0.0

    temporal_risk: float = 0.0
    historical_risk: float = 0.0

    policy_bypass_risk: float = 0.0
    source_reliability_risk: float = 0.0
    technical_risk: float = 0.0

    # Prior control-plane state
    prior_quality_risk: float = 0.0
    prior_action: str = ""
    prior_scope: str = ""
    prior_decision: str = ""

    # Prior quarantine / recovery / restoration evidence
    prior_quarantine_count: int = 0
    prior_recovery_count: int = 0
    prior_restoration_count: int = 0

    recovery_evidence_score: float = 0.0
    restoration_evidence_score: float = 0.0

    recent_clean_observation_count: int = 0
    recent_negative_observation_count: int = 0

    # Distributed / cross-resource information
    active_affected_resource_count: int = 0
    affected_cluster_size: int = 0
    affected_domain_count: int = 0
    affected_host_count: int = 0

    repeated_violation_count: int = 0
    historical_violation_count: int = 0

    independent_source_count: int = 0
    supporting_evidence_count: int = 0

    source_confidence: float = 1.0

    observed_at: str = ""

    partial: bool = False
    missing_fields: Tuple[str, ...] = ()

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FinalQualityHistory:
    previous_decision_count: int = 0
    previous_quarantine_count: int = 0
    previous_recovery_count: int = 0
    previous_restoration_count: int = 0
    previous_review_count: int = 0

    previous_risk_values: Tuple[float, ...] = ()
    previous_actions: Tuple[str, ...] = ()
    previous_scopes: Tuple[str, ...] = ()
    previous_decision_ids: Tuple[str, ...] = ()

    false_positive_count: int = 0
    rollback_count: int = 0

    recurring_issue_count: int = 0
    historical_issue_count: int = 0

    last_decision_timestamp: str = ""

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FinalQualitySignal:
    signal_id: str
    family: FinalQualitySignalFamily
    signal_type: FinalQualitySignalType

    value: float
    confidence: float

    evidence_strength: FinalQualityEvidenceStrength

    resource_id: str
    version: str

    source: str = ""
    reason_code: str = ""

    observed_at: str = ""

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FinalQualityEvidence:
    evidence_id: str
    kind: FinalQualityEvidenceKind

    resource_id: str
    version: str

    signal_ids: Tuple[str, ...]

    strength: FinalQualityEvidenceStrength
    confidence: float

    risk_contribution: float

    source_count: int
    independent_source_count: int

    partial: bool = False

    provenance: Mapping[str, Any] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FinalQualityPolicy:
    max_batch_size: int = 100_000
    max_signals_per_resource: int = 2048
    max_evidence_per_resource: int = 2048

    minimum_confidence: float = 0.20

    monitor_threshold: float = 0.20
    protect_threshold: float = 0.40
    review_threshold: float = 0.60

    quarantine_threshold: float = 0.75
    critical_threshold: float = 0.93

    restoration_threshold: float = 0.20
    recovery_threshold: float = 0.30

    high_confidence_threshold: float = 0.75
    very_high_confidence_threshold: float = 0.93

    minimum_independent_sources_for_quarantine: int = 2
    minimum_cluster_size_for_cluster_action: int = 25

    minimum_clean_observations_for_restoration: int = 3

    allow_partial: bool = True
    allow_resource_quarantine: bool = True
    allow_host_quarantine: bool = True
    allow_domain_quarantine: bool = True
    allow_cluster_quarantine: bool = True

    allow_restoration_planning: bool = True
    allow_recovery_planning: bool = True

    require_multiple_sources_for_quarantine: bool = True
    require_high_confidence_for_quarantine: bool = True

    require_clean_evidence_for_restoration: bool = True
    require_no_strong_current_risk_for_restoration: bool = True

    deterministic: bool = True

    allowed_scopes: Tuple[str, ...] = (
        FinalQualityScope.RESOURCE.value,
        FinalQualityScope.DOCUMENT.value,
        FinalQualityScope.URL_VARIANT.value,
        FinalQualityScope.HOST.value,
        FinalQualityScope.DOMAIN.value,
        FinalQualityScope.CLUSTER.value,
        FinalQualityScope.PARTITION.value,
        FinalQualityScope.SHARD.value,
        FinalQualityScope.REGION.value,
    )


@dataclass(frozen=True)
class FinalQualityControlPlan:
    plan_id: str

    action: FinalQualityAction
    scope: FinalQualityScope

    resource_ids: Tuple[str, ...]
    document_ids: Tuple[str, ...]
    host_ids: Tuple[str, ...]
    domain_ids: Tuple[str, ...]
    cluster_ids: Tuple[str, ...]

    risk: float
    confidence: float

    reason_codes: Tuple[str, ...]

    evidence_ids: Tuple[str, ...]
    signal_ids: Tuple[str, ...]

    quarantine_state: str = ""
    recovery_state: str = ""
    restoration_state: str = ""

    execution_intent: str = "control_plane_plan_only"

    idempotency_key: str = ""

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FinalQuarantinePlan:
    eligible: bool
    state: str

    action: FinalQualityAction
    scope: FinalQualityScope

    risk: float
    confidence: float

    reason_codes: Tuple[str, ...]

    evidence_ids: Tuple[str, ...]

    plan_id: str = ""

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FinalRecoveryPlan:
    state: FinalRecoveryState

    eligible: bool

    risk: float
    confidence: float

    reason_codes: Tuple[str, ...]

    evidence_ids: Tuple[str, ...]

    plan_id: str = ""

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FinalRestorationPlan:
    state: FinalRestorationState

    eligible: bool

    risk: float
    confidence: float

    reason_codes: Tuple[str, ...]

    evidence_ids: Tuple[str, ...]

    plan_id: str = ""

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FinalQualityCheckpoint:
    checkpoint_id: str
    checkpoint_type: FinalQualityCheckpointType

    resource_id: str
    state: FinalQualityState

    timestamp: str

    risk: float
    confidence: float

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FinalQualityEvent:
    event_id: str
    event_type: FinalQualityEventType

    resource_id: str
    state: FinalQualityState

    timestamp: str

    risk: float = 0.0
    confidence: float = 0.0

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FinalQualityResult:
    resource_id: str
    document_id: str

    state: FinalQualityState

    risk: float
    risk_band: FinalRiskBand

    confidence: float
    confidence_band: FinalQualityConfidence

    decision: FinalQualityDecision

    action: FinalQualityAction
    scope: FinalQualityScope

    signals: Tuple[FinalQualitySignal, ...]
    evidence: Tuple[FinalQualityEvidence, ...]

    control_plan: Optional[FinalQualityControlPlan]

    quarantine_plan: Optional[FinalQuarantinePlan]
    recovery_plan: Optional[FinalRecoveryPlan]
    restoration_plan: Optional[FinalRestorationPlan]

    reason_codes: Tuple[str, ...]

    idempotency_key: str

    lineage: FinalQualityLineage

    partial: bool
    missing_fields: Tuple[str, ...]

    timestamp: str

    metadata: Mapping[str, Any] = field(default_factory=dict)


# ============================================================================
# BACKEND
# ============================================================================


class FinalQualityBackend(Protocol):
    def put_result(self, result: FinalQualityResult) -> None:
        ...

    def get_result(self, resource_id: str) -> Optional[FinalQualityResult]:
        ...

    def put_event(self, event: FinalQualityEvent) -> None:
        ...

    def put_checkpoint(self, checkpoint: FinalQualityCheckpoint) -> None:
        ...


class InMemoryFinalQualityBackend:
    """
    Deterministic in-memory metadata backend.

    This backend is intentionally simple and framework-neutral.

    A production deployment can replace it with distributed durable storage.
    """

    def __init__(self) -> None:
        self.results: Dict[str, FinalQualityResult] = {}
        self.events: Dict[str, FinalQualityEvent] = {}
        self.checkpoints: Dict[str, FinalQualityCheckpoint] = {}

    def put_result(self, result: FinalQualityResult) -> None:
        self.results[result.resource_id] = result

    def get_result(self, resource_id: str) -> Optional[FinalQualityResult]:
        return self.results.get(resource_id)

    def put_event(self, event: FinalQualityEvent) -> None:
        self.events[event.event_id] = event

    def put_checkpoint(self, checkpoint: FinalQualityCheckpoint) -> None:
        self.checkpoints[checkpoint.checkpoint_id] = checkpoint


# ============================================================================
# MAIN ARCHITECTURE
# ============================================================================


class FinalSpamAbuseSecurityQualityArchitecture:
    """
    Final integrated Spam / Abuse / Security / Quality control-plane layer.

    This class integrates earlier-stage evidence and produces final
    control-plane plans.

    It deliberately does not perform direct external enforcement.
    """

    def __init__(
        self,
        policy: Optional[FinalQualityPolicy] = None,
        backend: Optional[FinalQualityBackend] = None,
        clock: Optional[Any] = None,
    ) -> None:
        self.policy = policy or FinalQualityPolicy()
        self.backend = backend or InMemoryFinalQualityBackend()
        self.clock = clock

    # ------------------------------------------------------------------
    # SAFE HELPERS
    # ------------------------------------------------------------------

    def _now(self) -> str:
        if self.clock is not None:
            value = self.clock()
            if isinstance(value, datetime):
                return value.astimezone(timezone.utc).isoformat()
            return str(value)

        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
        try:
            value = float(value)
        except (TypeError, ValueError):
            return low

        if not math.isfinite(value):
            return low

        return max(low, min(high, value))

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            result = float(value)
            if math.isfinite(result):
                return result
        except (TypeError, ValueError):
            pass

        return default

    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _digest(value: Any) -> str:
        payload = json.dumps(
            value,
            sort_keys=True,
            default=str,
            separators=(",", ":"),
        ).encode("utf-8")

        return hashlib.sha256(payload).hexdigest()

    def _signal_id(
        self,
        resource_id: str,
        signal_type: FinalQualitySignalType,
        value: float,
    ) -> str:
        raw = (
            f"{ARCHITECTURE_VERSION}|"
            f"{resource_id}|"
            f"{signal_type.value}|"
            f"{self._clamp(value):.8f}"
        )

        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]

    def _plan_id(
        self,
        resource_id: str,
        action: FinalQualityAction,
        scope: FinalQualityScope,
        risk: float,
        confidence: float,
    ) -> str:
        raw = (
            f"{ARCHITECTURE_VERSION}|"
            f"{resource_id}|"
            f"{action.value}|"
            f"{scope.value}|"
            f"{self._clamp(risk):.8f}|"
            f"{self._clamp(confidence):.8f}"
        )

        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]

    def _idempotency_key(
        self,
        item: FinalQualityInput,
    ) -> str:
        payload = {
            "architecture": ARCHITECTURE_VERSION,
            "phase": PHASE,
            "resource_id": item.identity.resource_id,
            "document_id": item.identity.document_id,
            "version": item.identity.version,
            "spam": round(self._clamp(item.spam_risk), 8),
            "content_quality": round(
                self._clamp(item.content_quality_risk), 8
            ),
            "manipulation": round(
                self._clamp(item.manipulation_risk), 8
            ),
            "link_abuse": round(
                self._clamp(item.link_abuse_risk), 8
            ),
            "malware": round(self._clamp(item.malware_risk), 8),
            "phishing": round(self._clamp(item.phishing_risk), 8),
            "harmful": round(
                self._clamp(item.harmful_resource_risk), 8
            ),
            "crawl_abuse": round(
                self._clamp(item.crawl_abuse_risk), 8
            ),
            "index_abuse": round(
                self._clamp(item.index_abuse_risk), 8
            ),
            "security": round(self._clamp(item.security_risk), 8),
            "trust": round(self._clamp(item.trust_risk), 8),
            "distributed": round(
                self._clamp(item.distributed_abuse_risk), 8
            ),
            "cross_resource": round(
                self._clamp(item.cross_resource_risk), 8
            ),
            "temporal": round(
                self._clamp(item.temporal_risk), 8
            ),
            "historical": round(
                self._clamp(item.historical_risk), 8
            ),
            "recovery": round(
                self._clamp(item.recovery_evidence_score), 8
            ),
            "restoration": round(
                self._clamp(item.restoration_evidence_score), 8
            ),
            "partial": item.partial,
            "missing_fields": sorted(item.missing_fields),
        }

        return self._digest(payload)

    # ------------------------------------------------------------------
    # EVENTS / CHECKPOINTS
    # ------------------------------------------------------------------

    def _event(
        self,
        event_type: FinalQualityEventType,
        resource_id: str,
        state: FinalQualityState,
        risk: float = 0.0,
        confidence: float = 0.0,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> FinalQualityEvent:
        timestamp = self._now()

        raw = (
            f"{event_type.value}|"
            f"{resource_id}|"
            f"{state.value}|"
            f"{timestamp}|"
            f"{risk:.8f}|"
            f"{confidence:.8f}"
        )

        event_id = hashlib.sha256(
            raw.encode("utf-8")
        ).hexdigest()[:32]

        event = FinalQualityEvent(
            event_id=event_id,
            event_type=event_type,
            resource_id=resource_id,
            state=state,
            timestamp=timestamp,
            risk=self._clamp(risk),
            confidence=self._clamp(confidence),
            metadata=dict(metadata or {}),
        )

        self.backend.put_event(event)

        return event

    def _checkpoint(
        self,
        checkpoint_type: FinalQualityCheckpointType,
        resource_id: str,
        state: FinalQualityState,
        risk: float = 0.0,
        confidence: float = 0.0,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> FinalQualityCheckpoint:
        timestamp = self._now()

        raw = (
            f"{checkpoint_type.value}|"
            f"{resource_id}|"
            f"{state.value}|"
            f"{timestamp}"
        )

        checkpoint_id = hashlib.sha256(
            raw.encode("utf-8")
        ).hexdigest()[:32]

        checkpoint = FinalQualityCheckpoint(
            checkpoint_id=checkpoint_id,
            checkpoint_type=checkpoint_type,
            resource_id=resource_id,
            state=state,
            timestamp=timestamp,
            risk=self._clamp(risk),
            confidence=self._clamp(confidence),
            metadata=dict(metadata or {}),
        )

        self.backend.put_checkpoint(checkpoint)

        self._event(
            FinalQualityEventType.CHECKPOINT_CREATED,
            resource_id,
            state,
            risk,
            confidence,
            {"checkpoint_id": checkpoint_id},
        )

        return checkpoint

    # ------------------------------------------------------------------
    # VALIDATION
    # ------------------------------------------------------------------

    def validate_input(
        self,
        item: FinalQualityInput,
    ) -> Tuple[bool, Tuple[str, ...]]:
        errors: List[str] = []

        if not item.identity.resource_id:
            errors.append("missing_resource_id")

        if not item.identity.url and not item.identity.document_id:
            errors.append("missing_resource_reference")

        numeric_fields = (
            "spam_risk",
            "content_quality_risk",
            "manipulation_risk",
            "link_abuse_risk",
            "malware_risk",
            "phishing_risk",
            "harmful_resource_risk",
            "crawl_abuse_risk",
            "index_abuse_risk",
            "resource_protection_risk",
            "security_risk",
            "trust_risk",
            "reputation_risk",
            "distributed_abuse_risk",
            "cross_resource_risk",
            "temporal_risk",
            "historical_risk",
            "policy_bypass_risk",
            "source_reliability_risk",
            "technical_risk",
            "prior_quality_risk",
            "recovery_evidence_score",
            "restoration_evidence_score",
            "source_confidence",
        )

        for name in numeric_fields:
            value = self._safe_float(getattr(item, name), -1.0)

            if value < 0.0 or value > 1.0:
                errors.append(f"invalid_{name}")

        count_fields = (
            "prior_quarantine_count",
            "prior_recovery_count",
            "prior_restoration_count",
            "recent_clean_observation_count",
            "recent_negative_observation_count",
            "active_affected_resource_count",
            "affected_cluster_size",
            "affected_domain_count",
            "affected_host_count",
            "repeated_violation_count",
            "historical_violation_count",
            "independent_source_count",
            "supporting_evidence_count",
        )

        for name in count_fields:
            if self._safe_int(getattr(item, name), -1) < 0:
                errors.append(f"invalid_{name}")

        return not errors, tuple(errors)

    # ------------------------------------------------------------------
    # NORMALIZATION
    # ------------------------------------------------------------------

    def normalize_input(
        self,
        item: FinalQualityInput,
    ) -> FinalQualityInput:
        identity = FinalQualityIdentity(
            resource_id=str(item.identity.resource_id or "").strip(),
            document_id=str(item.identity.document_id or "").strip(),
            url=str(item.identity.url or "").strip(),
            host_id=str(item.identity.host_id or "").strip(),
            domain_id=str(item.identity.domain_id or "").strip(),
            cluster_id=str(item.identity.cluster_id or "").strip(),
            partition_id=str(item.identity.partition_id or "").strip(),
            shard_id=str(item.identity.shard_id or "").strip(),
            region_id=str(item.identity.region_id or "").strip(),
            version=str(item.identity.version or "1"),
        )

        numeric_fields = (
            "spam_risk",
            "content_quality_risk",
            "manipulation_risk",
            "link_abuse_risk",
            "malware_risk",
            "phishing_risk",
            "harmful_resource_risk",
            "crawl_abuse_risk",
            "index_abuse_risk",
            "resource_protection_risk",
            "security_risk",
            "trust_risk",
            "reputation_risk",
            "distributed_abuse_risk",
            "cross_resource_risk",
            "temporal_risk",
            "historical_risk",
            "policy_bypass_risk",
            "source_reliability_risk",
            "technical_risk",
            "prior_quality_risk",
            "recovery_evidence_score",
            "restoration_evidence_score",
            "source_confidence",
        )

        values: Dict[str, Any] = {
            name: self._clamp(
                self._safe_float(getattr(item, name), 0.0)
            )
            for name in numeric_fields
        }

        count_fields = (
            "prior_quarantine_count",
            "prior_recovery_count",
            "prior_restoration_count",
            "recent_clean_observation_count",
            "recent_negative_observation_count",
            "active_affected_resource_count",
            "affected_cluster_size",
            "affected_domain_count",
            "affected_host_count",
            "repeated_violation_count",
            "historical_violation_count",
            "independent_source_count",
            "supporting_evidence_count",
        )

        for name in count_fields:
            values[name] = max(
                0,
                self._safe_int(getattr(item, name), 0),
            )

        values["identity"] = identity

        values["missing_fields"] = tuple(
            sorted(set(item.missing_fields))
        )

        return FinalQualityInput(
            identity=identity,
            lineage=item.lineage,
            **{
                field_name: getattr(item, field_name)
                for field_name in (
                    set(
                        FinalQualityInput.__dataclass_fields__.keys()
                    )
                    - {
                        "identity",
                        "lineage",
                        *numeric_fields,
                        *count_fields,
                        "missing_fields",
                    }
                )
            },
            **values,
        )

    # ------------------------------------------------------------------
    # SIGNAL CREATION
    # ------------------------------------------------------------------

    def _make_signal(
        self,
        item: FinalQualityInput,
        family: FinalQualitySignalFamily,
        signal_type: FinalQualitySignalType,
        value: float,
        confidence: float,
        reason_code: str,
        source: str = "phase_14_integrated_evidence",
    ) -> FinalQualitySignal:
        value = self._clamp(value)
        confidence = self._clamp(confidence)

        if confidence >= self.policy.very_high_confidence_threshold:
            strength = FinalQualityEvidenceStrength.VERY_STRONG
        elif confidence >= self.policy.high_confidence_threshold:
            strength = FinalQualityEvidenceStrength.STRONG
        elif confidence >= 0.50:
            strength = FinalQualityEvidenceStrength.MODERATE
        elif confidence > 0.0:
            strength = FinalQualityEvidenceStrength.WEAK
        else:
            strength = FinalQualityEvidenceStrength.NONE

        signal_id = self._signal_id(
            item.identity.resource_id,
            signal_type,
            value,
        )

        return FinalQualitySignal(
            signal_id=signal_id,
            family=family,
            signal_type=signal_type,
            value=value,
            confidence=confidence,
            evidence_strength=strength,
            resource_id=item.identity.resource_id,
            version=item.identity.version,
            source=source,
            reason_code=reason_code,
            observed_at=item.observed_at or self._now(),
            metadata={},
        )

    # ------------------------------------------------------------------
    # QUALITY SIGNAL EXTRACTION
    # ------------------------------------------------------------------

    def extract_quality_signals(
        self,
        item: FinalQualityInput,
    ) -> List[FinalQualitySignal]:
        signals: List[FinalQualitySignal] = []

        quality_inputs = (
            (
                FinalQualitySignalFamily.SPAM,
                FinalQualitySignalType.SPAM_RISK,
                item.spam_risk,
                "spam_risk_evidence",
            ),
            (
                FinalQualitySignalFamily.CONTENT_QUALITY,
                FinalQualitySignalType.CONTENT_QUALITY_RISK,
                item.content_quality_risk,
                "content_quality_risk_evidence",
            ),
            (
                FinalQualitySignalFamily.MANIPULATION,
                FinalQualitySignalType.MANIPULATION_RISK,
                item.manipulation_risk,
                "manipulation_risk_evidence",
            ),
            (
                FinalQualitySignalFamily.LINK_ABUSE,
                FinalQualitySignalType.LINK_ABUSE_RISK,
                item.link_abuse_risk,
                "link_abuse_risk_evidence",
            ),
            (
                FinalQualitySignalFamily.MALWARE,
                FinalQualitySignalType.MALWARE_RISK,
                item.malware_risk,
                "malware_risk_evidence",
            ),
            (
                FinalQualitySignalFamily.PHISHING,
                FinalQualitySignalType.PHISHING_RISK,
                item.phishing_risk,
                "phishing_risk_evidence",
            ),
            (
                FinalQualitySignalFamily.HARMFUL_RESOURCE,
                FinalQualitySignalType.HARMFUL_RESOURCE_RISK,
                item.harmful_resource_risk,
                "harmful_resource_risk_evidence",
            ),
            (
                FinalQualitySignalFamily.CRAWL_ABUSE,
                FinalQualitySignalType.CRAWL_ABUSE_RISK,
                item.crawl_abuse_risk,
                "crawl_abuse_risk_evidence",
            ),
            (
                FinalQualitySignalFamily.INDEX_ABUSE,
                FinalQualitySignalType.INDEX_ABUSE_RISK,
                item.index_abuse_risk,
                "index_abuse_risk_evidence",
            ),
            (
                FinalQualitySignalFamily.RESOURCE_PROTECTION,
                FinalQualitySignalType.RESOURCE_PROTECTION_RISK,
                item.resource_protection_risk,
                "resource_protection_risk_evidence",
            ),
            (
                FinalQualitySignalFamily.SECURITY,
                FinalQualitySignalType.SECURITY_RISK,
                item.security_risk,
                "security_risk_evidence",
            ),
            (
                FinalQualitySignalFamily.TRUST,
                FinalQualitySignalType.TRUST_RISK,
                item.trust_risk,
                "trust_risk_evidence",
            ),
            (
                FinalQualitySignalFamily.REPUTATION,
                FinalQualitySignalType.REPUTATION_RISK,
                item.reputation_risk,
                "reputation_risk_evidence",
            ),
        )

        for family, signal_type, value, reason in quality_inputs:
            if value <= 0.0:
                continue

            signals.append(
                self._make_signal(
                    item,
                    family,
                    signal_type,
                    value,
                    item.source_confidence,
                    reason,
                )
            )

        return signals

    # ------------------------------------------------------------------
    # GLOBAL / DISTRIBUTED SIGNAL EXTRACTION
    # ------------------------------------------------------------------

    def extract_global_signals(
        self,
        item: FinalQualityInput,
    ) -> List[FinalQualitySignal]:
        signals: List[FinalQualitySignal] = []

        distributed_inputs = (
            (
                FinalQualitySignalFamily.DISTRIBUTED_ABUSE,
                FinalQualitySignalType.DISTRIBUTED_ABUSE_RISK,
                item.distributed_abuse_risk,
                "distributed_abuse_risk",
            ),
            (
                FinalQualitySignalFamily.CROSS_RESOURCE,
                FinalQualitySignalType.CROSS_RESOURCE_CLUSTER_RISK,
                item.cross_resource_risk,
                "cross_resource_cluster_risk",
            ),
            (
                FinalQualitySignalFamily.TEMPORAL,
                FinalQualitySignalType.TEMPORAL_RISK,
                item.temporal_risk,
                "temporal_risk",
            ),
            (
                FinalQualitySignalFamily.HISTORICAL,
                FinalQualitySignalType.HISTORICAL_RISK,
                item.historical_risk,
                "historical_risk",
            ),
            (
                FinalQualitySignalFamily.POLICY,
                FinalQualitySignalType.POLICY_BYPASS_RISK,
                item.policy_bypass_risk,
                "policy_bypass_risk",
            ),
            (
                FinalQualitySignalFamily.SOURCE,
                FinalQualitySignalType.SOURCE_RELIABILITY_RISK,
                item.source_reliability_risk,
                "source_reliability_risk",
            ),
            (
                FinalQualitySignalFamily.TECHNICAL,
                FinalQualitySignalType.TECHNICAL_RISK,
                item.technical_risk,
                "technical_risk",
            ),
        )

        for family, signal_type, value, reason in distributed_inputs:
            if value <= 0.0:
                continue

            signals.append(
                self._make_signal(
                    item,
                    family,
                    signal_type,
                    value,
                    item.source_confidence,
                    reason,
                    source="distributed_phase_14_evidence",
                )
            )

        return signals

    # ------------------------------------------------------------------
    # RECOVERY / RESTORATION SIGNALS
    # ------------------------------------------------------------------

    def extract_recovery_signals(
        self,
        item: FinalQualityInput,
    ) -> List[FinalQualitySignal]:
        signals: List[FinalQualitySignal] = []

        if item.recovery_evidence_score > 0.0:
            signals.append(
                self._make_signal(
                    item,
                    FinalQualitySignalFamily.RECOVERY,
                    FinalQualitySignalType.RECOVERY_ELIGIBILITY,
                    item.recovery_evidence_score,
                    item.source_confidence,
                    "recovery_evidence_available",
                    source="recovery_evidence",
                )
            )

        if item.restoration_evidence_score > 0.0:
            signals.append(
                self._make_signal(
                    item,
                    FinalQualitySignalFamily.RESTORATION,
                    FinalQualitySignalType.RESTORATION_ELIGIBILITY,
                    item.restoration_evidence_score,
                    item.source_confidence,
                    "restoration_evidence_available",
                    source="restoration_evidence",
                )
            )

        return signals

    # ------------------------------------------------------------------
    # SCORE CALCULATION
    # ------------------------------------------------------------------

    @staticmethod
    def _saturating_score(values: Iterable[float]) -> float:
        """
        Combines independent risk signals without allowing simple addition
        to exceed 1.0.

        This is intentionally deterministic.
        """

        score = 0.0

        for raw_value in values:
            value = max(0.0, min(1.0, float(raw_value)))

            score = 1.0 - ((1.0 - score) * (1.0 - value))

        return max(0.0, min(1.0, score))

    def aggregate_risk(
        self,
        item: FinalQualityInput,
        signals: Sequence[FinalQualitySignal],
    ) -> float:
        signal_values = [
            signal.value * signal.confidence
            for signal in signals
            if signal.value > 0.0
        ]

        score = self._saturating_score(signal_values)

        # Historical / recurring issues increase persistence awareness.
        recurrence_factor = self._clamp(
            item.repeated_violation_count / 10.0
        )

        historical_factor = self._clamp(
            item.historical_violation_count / 20.0
        )

        cluster_factor = self._clamp(
            item.affected_cluster_size / 100.0
        )

        distributed_factor = self._clamp(
            item.active_affected_resource_count / 1000.0
        )

        score = self._saturating_score(
            (
                score,
                recurrence_factor * 0.20,
                historical_factor * 0.15,
                cluster_factor * 0.10,
                distributed_factor * 0.10,
            )
        )

        return self._clamp(score)

    # ------------------------------------------------------------------
    # CONFIDENCE
    # ------------------------------------------------------------------

    def calculate_confidence(
        self,
        item: FinalQualityInput,
        signals: Sequence[FinalQualitySignal],
    ) -> float:
        if not signals:
            return 0.0

        signal_confidence = sum(
            signal.confidence for signal in signals
        ) / len(signals)

        source_factor = self._clamp(
            item.independent_source_count / 4.0
        )

        evidence_factor = self._clamp(
            item.supporting_evidence_count / 10.0
        )

        confidence = (
            signal_confidence * 0.55
            + source_factor * 0.25
            + evidence_factor * 0.20
        )

        if item.partial:
            confidence *= 0.75

        return self._clamp(confidence)

    def confidence_band(
        self,
        confidence: float,
    ) -> FinalQualityConfidence:
        confidence = self._clamp(confidence)

        if confidence >= self.policy.very_high_confidence_threshold:
            return FinalQualityConfidence.VERY_HIGH

        if confidence >= self.policy.high_confidence_threshold:
            return FinalQualityConfidence.HIGH

        if confidence >= 0.50:
            return FinalQualityConfidence.MODERATE

        if confidence >= self.policy.minimum_confidence:
            return FinalQualityConfidence.LOW

        return FinalQualityConfidence.UNKNOWN

    # ------------------------------------------------------------------
    # RISK BAND
    # ------------------------------------------------------------------

    def risk_band(
        self,
        risk: float,
    ) -> FinalRiskBand:
        risk = self._clamp(risk)

        if risk <= 0.0:
            return FinalRiskBand.NONE

        if risk < self.policy.monitor_threshold:
            return FinalRiskBand.LOW

        if risk < self.policy.protect_threshold:
            return FinalRiskBand.MODERATE

        if risk < self.policy.review_threshold:
            return FinalRiskBand.HIGH

        if risk < self.policy.quarantine_threshold:
            return FinalRiskBand.VERY_HIGH

        if risk < self.policy.critical_threshold:
            return FinalRiskBand.VERY_HIGH

        return FinalRiskBand.CRITICAL

    # ------------------------------------------------------------------
    # SCOPE SELECTION
    # ------------------------------------------------------------------

    def select_scope(
        self,
        item: FinalQualityInput,
        risk: float,
    ) -> FinalQualityScope:
        if (
            item.affected_cluster_size
            >= self.policy.minimum_cluster_size_for_cluster_action
        ):
            if item.identity.cluster_id:
                return FinalQualityScope.CLUSTER

        if item.affected_domain_count >= 2 and item.identity.domain_id:
            return FinalQualityScope.DOMAIN

        if item.affected_host_count >= 2 and item.identity.host_id:
            return FinalQualityScope.HOST

        if item.identity.resource_id:
            return FinalQualityScope.RESOURCE

        if item.identity.document_id:
            return FinalQualityScope.DOCUMENT

        return FinalQualityScope.RESOURCE

    # ------------------------------------------------------------------
    # QUARANTINE ELIGIBILITY
    # ------------------------------------------------------------------

    def quarantine_eligibility(
        self,
        item: FinalQualityInput,
        risk: float,
        confidence: float,
        signals: Sequence[FinalQualitySignal],
        evidence_ids: Sequence[str],
    ) -> FinalQuarantinePlan:
        reasons: List[str] = []

        if risk < self.policy.quarantine_threshold:
            reasons.append("risk_below_quarantine_threshold")

        if (
            self.policy.require_high_confidence_for_quarantine
            and confidence < self.policy.high_confidence_threshold
        ):
            reasons.append("confidence_below_quarantine_threshold")

        if (
            self.policy.require_multiple_sources_for_quarantine
            and item.independent_source_count
            < self.policy.minimum_independent_sources_for_quarantine
        ):
            reasons.append("insufficient_independent_sources")

        if item.partial and not self.policy.allow_partial:
            reasons.append("partial_evidence_not_allowed")

        strong_current_risk = any(
            signal.value >= self.policy.quarantine_threshold
            and signal.confidence >= self.policy.high_confidence_threshold
            for signal in signals
        )

        if not strong_current_risk:
            reasons.append("no_strong_current_risk_signal")

        eligible = not reasons

        scope = self.select_scope(item, risk)

        action = FinalQualityAction.QUARANTINE_RESOURCE

        if scope == FinalQualityScope.CLUSTER:
            if self.policy.allow_cluster_quarantine:
                action = FinalQualityAction.QUARANTINE_CLUSTER
            else:
                eligible = False
                reasons.append("cluster_quarantine_not_allowed")

        elif scope == FinalQualityScope.DOMAIN:
            if self.policy.allow_domain_quarantine:
                action = FinalQualityAction.QUARANTINE_DOMAIN
            else:
                eligible = False
                reasons.append("domain_quarantine_not_allowed")

        elif scope == FinalQualityScope.HOST:
            if self.policy.allow_host_quarantine:
                action = FinalQualityAction.QUARANTINE_HOST
            else:
                eligible = False
                reasons.append("host_quarantine_not_allowed")

        elif scope == FinalQualityScope.RESOURCE:
            if not self.policy.allow_resource_quarantine:
                eligible = False
                reasons.append("resource_quarantine_not_allowed")

        state = "eligible" if eligible else "not_eligible"

        return FinalQuarantinePlan(
            eligible=eligible,
            state=state,
            action=action,
            scope=scope,
            risk=self._clamp(risk),
            confidence=self._clamp(confidence),
            reason_codes=tuple(sorted(set(reasons))),
            evidence_ids=tuple(evidence_ids),
            metadata={
                "execution": "plan_only",
                "actual_quarantine_executed": False,
            },
        )

    # ------------------------------------------------------------------
    # RECOVERY ELIGIBILITY
    # ------------------------------------------------------------------

    def recovery_eligibility(
        self,
        item: FinalQualityInput,
        risk: float,
        confidence: float,
        evidence_ids: Sequence[str],
    ) -> FinalRecoveryPlan:
        reasons: List[str] = []

        if not self.policy.allow_recovery_planning:
            reasons.append("recovery_planning_not_allowed")

        if risk > self.policy.recovery_threshold:
            reasons.append("current_risk_too_high")

        if (
            item.recovery_evidence_score
            < self.policy.recovery_threshold
        ):
            reasons.append("insufficient_recovery_evidence")

        if item.recent_negative_observation_count > 0:
            reasons.append("recent_negative_observations_present")

        if confidence < self.policy.minimum_confidence:
            reasons.append("insufficient_confidence")

        eligible = not reasons

        if eligible:
            state = FinalRecoveryState.READY
        elif risk > self.policy.recovery_threshold:
            state = FinalRecoveryState.HOLD
        elif item.partial:
            state = FinalRecoveryState.DEFERRED
        else:
            state = FinalRecoveryState.EVIDENCE_REQUIRED

        return FinalRecoveryPlan(
            state=state,
            eligible=eligible,
            risk=self._clamp(risk),
            confidence=self._clamp(confidence),
            reason_codes=tuple(sorted(set(reasons))),
            evidence_ids=tuple(evidence_ids),
            metadata={
                "execution": "plan_only",
                "actual_recovery_executed": False,
            },
        )

    # ------------------------------------------------------------------
    # RESTORATION ELIGIBILITY
    # ------------------------------------------------------------------

    def restoration_eligibility(
        self,
        item: FinalQualityInput,
        risk: float,
        confidence: float,
        signals: Sequence[FinalQualitySignal],
        evidence_ids: Sequence[str],
    ) -> FinalRestorationPlan:
        reasons: List[str] = []

        if not self.policy.allow_restoration_planning:
            reasons.append("restoration_planning_not_allowed")

        if risk > self.policy.restoration_threshold:
            reasons.append("risk_above_restoration_threshold")

        if (
            item.restoration_evidence_score
            < self.policy.restoration_threshold
        ):
            reasons.append("insufficient_restoration_evidence")

        if (
            self.policy.require_clean_evidence_for_restoration
            and item.recent_clean_observation_count
            < self.policy.minimum_clean_observations_for_restoration
        ):
            reasons.append("insufficient_clean_observations")

        if (
            self.policy.require_no_strong_current_risk_for_restoration
            and any(
                signal.value >= self.policy.protect_threshold
                and signal.confidence
                >= self.policy.high_confidence_threshold
                for signal in signals
            )
        ):
            reasons.append("strong_current_risk_present")

        if confidence < self.policy.minimum_confidence:
            reasons.append("insufficient_confidence")

        eligible = not reasons

        if eligible:
            state = FinalRestorationState.READY
        elif "strong_current_risk_present" in reasons:
            state = FinalRestorationState.HOLD
        elif item.partial:
            state = FinalRestorationState.DEFERRED
        else:
            state = FinalRestorationState.EVIDENCE_REQUIRED

        return FinalRestorationPlan(
            state=state,
            eligible=eligible,
            risk=self._clamp(risk),
            confidence=self._clamp(confidence),
            reason_codes=tuple(sorted(set(reasons))),
            evidence_ids=tuple(evidence_ids),
            metadata={
                "execution": "plan_only",
                "actual_restoration_executed": False,
            },
        )

    # ------------------------------------------------------------------
    # ACTION SELECTION
    # ------------------------------------------------------------------

    def choose_action(
        self,
        item: FinalQualityInput,
        risk: float,
        confidence: float,
        quarantine_plan: FinalQuarantinePlan,
        recovery_plan: FinalRecoveryPlan,
        restoration_plan: FinalRestorationPlan,
    ) -> FinalQualityAction:
        # Strong current risk takes precedence over recovery/restoration.
        if quarantine_plan.eligible:
            return quarantine_plan.action

        if risk >= self.policy.review_threshold:
            return FinalQualityAction.REQUIRE_REVIEW

        if risk >= self.policy.protect_threshold:
            return FinalQualityAction.PROTECT

        if risk >= self.policy.monitor_threshold:
            return FinalQualityAction.MONITOR

        if restoration_plan.eligible:
            return FinalQualityAction.RESTORE_RESOURCE

        if recovery_plan.eligible:
            return FinalQualityAction.RECOVERY_REVIEW

        if item.partial:
            return FinalQualityAction.DEFER

        if confidence < self.policy.minimum_confidence:
            return FinalQualityAction.DEFER

        return FinalQualityAction.NONE

    # ------------------------------------------------------------------
    # CONTROL PLAN
    # ------------------------------------------------------------------

    def build_control_plan(
        self,
        item: FinalQualityInput,
        risk: float,
        confidence: float,
        action: FinalQualityAction,
        scope: FinalQualityScope,
        signals: Sequence[FinalQualitySignal],
        evidence: Sequence[FinalQualityEvidence],
        quarantine_plan: FinalQuarantinePlan,
        recovery_plan: FinalRecoveryPlan,
        restoration_plan: FinalRestorationPlan,
        reason_codes: Sequence[str],
    ) -> Optional[FinalQualityControlPlan]:
        if action == FinalQualityAction.NONE:
            return None

        plan_id = self._plan_id(
            item.identity.resource_id,
            action,
            scope,
            risk,
            confidence,
        )

        idempotency_key = self._idempotency_key(item)

        resource_ids = (
            (item.identity.resource_id,)
            if item.identity.resource_id
            else ()
        )

        document_ids = (
            (item.identity.document_id,)
            if item.identity.document_id
            else ()
        )

        host_ids = (
            (item.identity.host_id,)
            if item.identity.host_id
            else ()
        )

        domain_ids = (
            (item.identity.domain_id,)
            if item.identity.domain_id
            else ()
        )

        cluster_ids = (
            (item.identity.cluster_id,)
            if item.identity.cluster_id
            else ()
        )

        return FinalQualityControlPlan(
            plan_id=plan_id,
            action=action,
            scope=scope,
            resource_ids=resource_ids,
            document_ids=document_ids,
            host_ids=host_ids,
            domain_ids=domain_ids,
            cluster_ids=cluster_ids,
            risk=self._clamp(risk),
            confidence=self._clamp(confidence),
            reason_codes=tuple(sorted(set(reason_codes))),
            evidence_ids=tuple(
                evidence_item.evidence_id
                for evidence_item in evidence
            ),
            signal_ids=tuple(
                signal.signal_id
                for signal in signals
            ),
            quarantine_state=quarantine_plan.state,
            recovery_state=recovery_plan.state.value,
            restoration_state=restoration_plan.state.value,
            execution_intent="control_plane_plan_only",
            idempotency_key=idempotency_key,
            metadata={
                "actual_enforcement_executed": False,
                "actual_quarantine_executed": False,
                "actual_recovery_executed": False,
                "actual_restoration_executed": False,
            },
        )

    # ------------------------------------------------------------------
    # EVIDENCE BUILDING
    # ------------------------------------------------------------------

    def build_evidence(
        self,
        item: FinalQualityInput,
        signals: Sequence[FinalQualitySignal],
    ) -> List[FinalQualityEvidence]:
        grouped: Dict[
            FinalQualityEvidenceKind,
            List[FinalQualitySignal]
        ] = {}

        family_to_kind = {
            FinalQualitySignalFamily.SPAM:
                FinalQualityEvidenceKind.SPAM,

            FinalQualitySignalFamily.CONTENT_QUALITY:
                FinalQualityEvidenceKind.CONTENT_QUALITY,

            FinalQualitySignalFamily.MANIPULATION:
                FinalQualityEvidenceKind.MANIPULATION,

            FinalQualitySignalFamily.LINK_ABUSE:
                FinalQualityEvidenceKind.LINK_ABUSE,

            FinalQualitySignalFamily.MALWARE:
                FinalQualityEvidenceKind.MALWARE,

            FinalQualitySignalFamily.PHISHING:
                FinalQualityEvidenceKind.PHISHING,

            FinalQualitySignalFamily.HARMFUL_RESOURCE:
                FinalQualityEvidenceKind.HARMFUL,

            FinalQualitySignalFamily.CRAWL_ABUSE:
                FinalQualityEvidenceKind.CRAWL_ABUSE,

            FinalQualitySignalFamily.INDEX_ABUSE:
                FinalQualityEvidenceKind.INDEX_ABUSE,

            FinalQualitySignalFamily.RESOURCE_PROTECTION:
                FinalQualityEvidenceKind.RESOURCE_PROTECTION,

            FinalQualitySignalFamily.SECURITY:
                FinalQualityEvidenceKind.SECURITY,

            FinalQualitySignalFamily.TRUST:
                FinalQualityEvidenceKind.TRUST,

            FinalQualitySignalFamily.REPUTATION:
                FinalQualityEvidenceKind.REPUTATION,

            FinalQualitySignalFamily.DISTRIBUTED_ABUSE:
                FinalQualityEvidenceKind.DISTRIBUTED,

            FinalQualitySignalFamily.CROSS_RESOURCE:
                FinalQualityEvidenceKind.CROSS_RESOURCE,

            FinalQualitySignalFamily.TEMPORAL:
                FinalQualityEvidenceKind.TEMPORAL,

            FinalQualitySignalFamily.HISTORICAL:
                FinalQualityEvidenceKind.HISTORICAL,

            FinalQualitySignalFamily.POLICY:
                FinalQualityEvidenceKind.POLICY,

            FinalQualitySignalFamily.SOURCE:
                FinalQualityEvidenceKind.SOURCE,

            FinalQualitySignalFamily.TECHNICAL:
                FinalQualityEvidenceKind.TECHNICAL,

            FinalQualitySignalFamily.RECOVERY:
                FinalQualityEvidenceKind.RECOVERY,

            FinalQualitySignalFamily.RESTORATION:
                FinalQualityEvidenceKind.RESTORATION,
        }

        for signal in signals:
            kind = family_to_kind.get(signal.family)

            if kind is None:
                continue

            grouped.setdefault(kind, []).append(signal)

        evidence: List[FinalQualityEvidence] = []

        for kind, grouped_signals in sorted(
            grouped.items(),
            key=lambda pair: pair[0].value,
        ):
            confidence = (
                sum(
                    signal.confidence
                    for signal in grouped_signals
                )
                / len(grouped_signals)
            )

            contribution = self._saturating_score(
                signal.value * signal.confidence
                for signal in grouped_signals
            )

            source_count = len(
                set(
                    signal.source
                    for signal in grouped_signals
                    if signal.source
                )
            )

            independent_source_count = max(
                item.independent_source_count,
                source_count,
            )

            if confidence >= self.policy.very_high_confidence_threshold:
                strength = FinalQualityEvidenceStrength.VERY_STRONG
            elif confidence >= self.policy.high_confidence_threshold:
                strength = FinalQualityEvidenceStrength.STRONG
            elif confidence >= 0.50:
                strength = FinalQualityEvidenceStrength.MODERATE
            elif confidence > 0.0:
                strength = FinalQualityEvidenceStrength.WEAK
            else:
                strength = FinalQualityEvidenceStrength.NONE

            raw_id = (
                f"{item.identity.resource_id}|"
                f"{item.identity.version}|"
                f"{kind.value}|"
                f"{','.join(signal.signal_id for signal in grouped_signals)}"
            )

            evidence_id = hashlib.sha256(
                raw_id.encode("utf-8")
            ).hexdigest()[:32]

            evidence.append(
                FinalQualityEvidence(
                    evidence_id=evidence_id,
                    kind=kind,
                    resource_id=item.identity.resource_id,
                    version=item.identity.version,
                    signal_ids=tuple(
                        signal.signal_id
                        for signal in grouped_signals
                    ),
                    strength=strength,
                    confidence=self._clamp(confidence),
                    risk_contribution=self._clamp(contribution),
                    source_count=source_count,
                    independent_source_count=independent_source_count,
                    partial=item.partial,
                    provenance={
                        "phase": PHASE,
                        "previous_stage": PREVIOUS_STAGE,
                    },
                    metadata={},
                )
            )

        return evidence

    # ------------------------------------------------------------------
    # REASONS
    # ------------------------------------------------------------------

    def build_reasons(
        self,
        item: FinalQualityInput,
        risk: float,
        confidence: float,
        action: FinalQualityAction,
        quarantine_plan: FinalQuarantinePlan,
        recovery_plan: FinalRecoveryPlan,
        restoration_plan: FinalRestorationPlan,
    ) -> Tuple[str, ...]:
        reasons: List[str] = []

        if item.spam_risk > 0.0:
            reasons.append("spam_evidence")

        if item.content_quality_risk > 0.0:
            reasons.append("content_quality_evidence")

        if item.manipulation_risk > 0.0:
            reasons.append("manipulation_evidence")

        if item.link_abuse_risk > 0.0:
            reasons.append("link_abuse_evidence")

        if item.malware_risk > 0.0:
            reasons.append("malware_evidence")

        if item.phishing_risk > 0.0:
            reasons.append("phishing_evidence")

        if item.harmful_resource_risk > 0.0:
            reasons.append("harmful_resource_evidence")

        if item.crawl_abuse_risk > 0.0:
            reasons.append("crawl_abuse_evidence")

        if item.index_abuse_risk > 0.0:
            reasons.append("index_abuse_evidence")

        if item.resource_protection_risk > 0.0:
            reasons.append("resource_protection_evidence")

        if item.security_risk > 0.0:
            reasons.append("security_evidence")

        if item.trust_risk > 0.0:
            reasons.append("trust_evidence")

        if item.reputation_risk > 0.0:
            reasons.append("reputation_evidence")

        if item.distributed_abuse_risk > 0.0:
            reasons.append("distributed_abuse_evidence")

        if item.cross_resource_risk > 0.0:
            reasons.append("cross_resource_evidence")

        if item.temporal_risk > 0.0:
            reasons.append("temporal_evidence")

        if item.historical_risk > 0.0:
            reasons.append("historical_evidence")

        if item.policy_bypass_risk > 0.0:
            reasons.append("policy_bypass_evidence")

        if item.partial:
            reasons.append("partial_evidence")

        if action == FinalQualityAction.REQUIRE_REVIEW:
            reasons.append("review_threshold_reached")

        if action in (
            FinalQualityAction.QUARANTINE_RESOURCE,
            FinalQualityAction.QUARANTINE_HOST,
            FinalQualityAction.QUARANTINE_DOMAIN,
            FinalQualityAction.QUARANTINE_CLUSTER,
        ):
            reasons.append("quarantine_eligible")

        if recovery_plan.eligible:
            reasons.append("recovery_eligible")

        if restoration_plan.eligible:
            reasons.append("restoration_eligible")

        reasons.extend(
            quarantine_plan.reason_codes
        )

        reasons.extend(
            recovery_plan.reason_codes
        )

        reasons.extend(
            restoration_plan.reason_codes
        )

        if confidence < self.policy.minimum_confidence:
            reasons.append("low_confidence")

        if risk >= self.policy.critical_threshold:
            reasons.append("critical_risk")

        return tuple(sorted(set(reasons)))

    # ------------------------------------------------------------------
    # DECISION
    # ------------------------------------------------------------------

    def decision(
        self,
        item: FinalQualityInput,
        risk: float,
        confidence: float,
        action: FinalQualityAction,
        quarantine_plan: FinalQuarantinePlan,
        recovery_plan: FinalRecoveryPlan,
        restoration_plan: FinalRestorationPlan,
    ) -> FinalQualityDecision:
        if action in (
            FinalQualityAction.QUARANTINE_RESOURCE,
            FinalQualityAction.QUARANTINE_HOST,
            FinalQualityAction.QUARANTINE_DOMAIN,
            FinalQualityAction.QUARANTINE_CLUSTER,
        ):
            return FinalQualityDecision.QUARANTINE_PLANNED

        if action == FinalQualityAction.REQUIRE_REVIEW:
            return FinalQualityDecision.REVIEW_REQUIRED

        if action == FinalQualityAction.PROTECT:
            return FinalQualityDecision.PROTECT

        if action == FinalQualityAction.MONITOR:
            return FinalQualityDecision.MONITOR

        if action == FinalQualityAction.DEFER:
            return FinalQualityDecision.DEFERRED

        if restoration_plan.eligible:
            return FinalQualityDecision.RESTORATION_PLANNED

        if recovery_plan.eligible:
            return FinalQualityDecision.RECOVERY_PLANNED

        if item.partial:
            return FinalQualityDecision.PARTIAL_EVIDENCE

        if confidence < self.policy.minimum_confidence:
            return FinalQualityDecision.PARTIAL_EVIDENCE

        return FinalQualityDecision.NO_ACTION

    # ------------------------------------------------------------------
    # FINAL ANALYSIS
    # ------------------------------------------------------------------

    def analyze(
        self,
        item: FinalQualityInput,
    ) -> FinalQualityResult:
        resource_id = item.identity.resource_id

        self._event(
            FinalQualityEventType.ANALYSIS_STARTED,
            resource_id,
            FinalQualityState.RECEIVED,
        )

        valid, errors = self.validate_input(item)

        if not valid:
            result = FinalQualityResult(
                resource_id=resource_id,
                document_id=item.identity.document_id,
                state=FinalQualityState.REJECTED,
                risk=0.0,
                risk_band=FinalRiskBand.UNKNOWN,
                confidence=0.0,
                confidence_band=FinalQualityConfidence.UNKNOWN,
                decision=FinalQualityDecision.REJECTED_INPUT,
                action=FinalQualityAction.NONE,
                scope=FinalQualityScope.RESOURCE,
                signals=(),
                evidence=(),
                control_plan=None,
                quarantine_plan=None,
                recovery_plan=None,
                restoration_plan=None,
                reason_codes=tuple(
                    sorted(set(errors))
                ),
                idempotency_key=self._idempotency_key(item),
                lineage=item.lineage,
                partial=True,
                missing_fields=tuple(
                    sorted(set(errors))
                ),
                timestamp=self._now(),
                metadata={
                    "validation_errors": errors,
                },
            )

            self.backend.put_result(result)

            self._event(
                FinalQualityEventType.ANALYSIS_REJECTED,
                resource_id,
                FinalQualityState.REJECTED,
            )

            return result

        normalized = self.normalize_input(item)

        self._event(
            FinalQualityEventType.INPUT_ACCEPTED,
            resource_id,
            FinalQualityState.VALIDATING,
        )

        self._event(
            FinalQualityEventType.INPUT_NORMALIZED,
            resource_id,
            FinalQualityState.NORMALIZING,
        )

        self._checkpoint(
            FinalQualityCheckpointType.NORMALIZATION_COMPLETE,
            resource_id,
            FinalQualityState.EVIDENCE_AGGREGATION,
        )

        signals = self.extract_quality_signals(
            normalized
        )

        signals.extend(
            self.extract_global_signals(normalized)
        )

        signals.extend(
            self.extract_recovery_signals(normalized)
        )

        # Deterministic ordering.
        signals = sorted(
            signals,
            key=lambda signal: (
                signal.family.value,
                signal.signal_type.value,
                signal.signal_id,
            ),
        )

        if len(signals) > self.policy.max_signals_per_resource:
            signals = signals[
                : self.policy.max_signals_per_resource
            ]

        for signal in signals:
            self._event(
                FinalQualityEventType.SIGNAL_DETECTED,
                resource_id,
                FinalQualityState.EVIDENCE_AGGREGATION,
                signal.value,
                signal.confidence,
                {
                    "signal_id": signal.signal_id,
                    "signal_type": signal.signal_type.value,
                },
            )

        self._checkpoint(
            FinalQualityCheckpointType.EVIDENCE_COMPLETE,
            resource_id,
            FinalQualityState.CORRELATION,
            metadata={
                "signal_count": len(signals),
            },
        )

        evidence = self.build_evidence(
            normalized,
            signals,
        )

        if len(evidence) > self.policy.max_evidence_per_resource:
            evidence = evidence[
                : self.policy.max_evidence_per_resource
            ]

        risk = self.aggregate_risk(
            normalized,
            signals,
        )

        confidence = self.calculate_confidence(
            normalized,
            signals,
        )

        self._event(
            FinalQualityEventType.GLOBAL_CORRELATION,
            resource_id,
            FinalQualityState.CORRELATION,
            risk,
            confidence,
        )

        self._event(
            FinalQualityEventType.RISK_CALCULATED,
            resource_id,
            FinalQualityState.RISK_CALCULATION,
            risk,
            confidence,
        )

        self._event(
            FinalQualityEventType.CONFIDENCE_CALCULATED,
            resource_id,
            FinalQualityState.CONFIDENCE_CALCULATION,
            risk,
            confidence,
        )

        self._checkpoint(
            FinalQualityCheckpointType.CORRELATION_COMPLETE,
            resource_id,
            FinalQualityState.RISK_CALCULATION,
            risk,
            confidence,
        )

        self._checkpoint(
            FinalQualityCheckpointType.RISK_COMPLETE,
            resource_id,
            FinalQualityState.CONFIDENCE_CALCULATION,
            risk,
            confidence,
        )

        self._checkpoint(
            FinalQualityCheckpointType.CONFIDENCE_COMPLETE,
            resource_id,
            FinalQualityState.POLICY_EVALUATION,
            risk,
            confidence,
        )

        quarantine_plan = self.quarantine_eligibility(
            normalized,
            risk,
            confidence,
            signals,
            [
                evidence_item.evidence_id
                for evidence_item in evidence
            ],
        )

        recovery_plan = self.recovery_eligibility(
            normalized,
            risk,
            confidence,
            [
                evidence_item.evidence_id
                for evidence_item in evidence
            ],
        )

        restoration_plan = self.restoration_eligibility(
            normalized,
            risk,
            confidence,
            signals,
            [
                evidence_item.evidence_id
                for evidence_item in evidence
            ],
        )

        self._event(
            FinalQualityEventType.POLICY_EVALUATED,
            resource_id,
            FinalQualityState.POLICY_EVALUATION,
            risk,
            confidence,
        )

        scope = self.select_scope(
            normalized,
            risk,
        )

        action = self.choose_action(
            normalized,
            risk,
            confidence,
            quarantine_plan,
            recovery_plan,
            restoration_plan,
        )

        reasons = self.build_reasons(
            normalized,
            risk,
            confidence,
            action,
            quarantine_plan,
            recovery_plan,
            restoration_plan,
        )

        control_plan = self.build_control_plan(
            normalized,
            risk,
            confidence,
            action,
            scope,
            signals,
            evidence,
            quarantine_plan,
            recovery_plan,
            restoration_plan,
            reasons,
        )

        if control_plan is not None:
            self._event(
                FinalQualityEventType.CONTROL_PLANNED,
                resource_id,
                FinalQualityState.CONTROL_PLANNING,
                risk,
                confidence,
                {
                    "plan_id": control_plan.plan_id,
                    "action": action.value,
                    "scope": scope.value,
                },
            )

        if quarantine_plan.eligible:
            self._event(
                FinalQualityEventType.QUARANTINE_PLANNED,
                resource_id,
                FinalQualityState.QUARANTINE_PLANNING,
                risk,
                confidence,
            )

        if recovery_plan.eligible:
            self._event(
                FinalQualityEventType.RECOVERY_PLANNED,
                resource_id,
                FinalQualityState.RECOVERY_PLANNING,
                risk,
                confidence,
            )

        if restoration_plan.eligible:
            self._event(
                FinalQualityEventType.RESTORATION_PLANNED,
                resource_id,
                FinalQualityState.RESTORATION_PLANNING,
                risk,
                confidence,
            )

        final_decision = self.decision(
            normalized,
            risk,
            confidence,
            action,
            quarantine_plan,
            recovery_plan,
            restoration_plan,
        )

        final_state = FinalQualityState.COMPLETED

        if normalized.partial:
            final_state = FinalQualityState.PARTIAL

        if action == FinalQualityAction.DEFER:
            final_state = FinalQualityState.DEFERRED

        self._event(
            FinalQualityEventType.DECISION_PREPARED,
            resource_id,
            FinalQualityState.DECISION_READY,
            risk,
            confidence,
            {
                "decision": final_decision.value,
                "action": action.value,
                "scope": scope.value,
            },
        )

        self._checkpoint(
            FinalQualityCheckpointType.POLICY_COMPLETE,
            resource_id,
            FinalQualityState.CONTROL_PLANNING,
            risk,
            confidence,
        )

        self._checkpoint(
            FinalQualityCheckpointType.CONTROL_PLAN_COMPLETE,
            resource_id,
            FinalQualityState.CONTROL_PLANNING,
            risk,
            confidence,
        )

        if quarantine_plan.eligible:
            self._checkpoint(
                FinalQualityCheckpointType.QUARANTINE_PLAN_COMPLETE,
                resource_id,
                FinalQualityState.QUARANTINE_PLANNING,
                risk,
                confidence,
            )

        if recovery_plan.eligible:
            self._checkpoint(
                FinalQualityCheckpointType.RECOVERY_PLAN_COMPLETE,
                resource_id,
                FinalQualityState.RECOVERY_PLANNING,
                risk,
                confidence,
            )

        if restoration_plan.eligible:
            self._checkpoint(
                FinalQualityCheckpointType.RESTORATION_PLAN_COMPLETE,
                resource_id,
                FinalQualityState.RESTORATION_PLANNING,
                risk,
                confidence,
            )

        idempotency_key = self._idempotency_key(
            normalized
        )

        self._event(
            FinalQualityEventType.IDEMPOTENCY_CHECKED,
            resource_id,
            FinalQualityState.IDEMPOTENCY_CHECK,
            risk,
            confidence,
            {
                "idempotency_key": idempotency_key,
            },
        )

        result = FinalQualityResult(
            resource_id=resource_id,
            document_id=normalized.identity.document_id,
            state=final_state,
            risk=self._clamp(risk),
            risk_band=self.risk_band(risk),
            confidence=self._clamp(confidence),
            confidence_band=self.confidence_band(confidence),
            decision=final_decision,
            action=action,
            scope=scope,
            signals=tuple(signals),
            evidence=tuple(evidence),
            control_plan=control_plan,
            quarantine_plan=quarantine_plan,
            recovery_plan=recovery_plan,
            restoration_plan=restoration_plan,
            reason_codes=reasons,
            idempotency_key=idempotency_key,
            lineage=normalized.lineage,
            partial=normalized.partial,
            missing_fields=normalized.missing_fields,
            timestamp=self._now(),
            metadata={
                "architecture_version": ARCHITECTURE_VERSION,
                "phase": PHASE,
                "previous_stage": PREVIOUS_STAGE,
                "next_phase": NEXT_PHASE,
                "scale_target": SCALE_TARGET,
                "google_scale_capability_target":
                    GOOGLE_SCALE_CAPABILITY_TARGET,
                "google_technology_dependency":
                    GOOGLE_TECHNOLOGY_DEPENDENCY,
                "actual_external_enforcement": False,
                "actual_quarantine": False,
                "actual_recovery": False,
                "actual_restoration": False,
            },
        )

        self.backend.put_result(result)

        self._checkpoint(
            FinalQualityCheckpointType.RESULT_COMPLETE,
            resource_id,
            final_state,
            risk,
            confidence,
        )

        if final_state == FinalQualityState.PARTIAL:
            event_type = FinalQualityEventType.ANALYSIS_PARTIAL
        elif final_state == FinalQualityState.DEFERRED:
            event_type = FinalQualityEventType.ANALYSIS_DEFERRED
        else:
            event_type = FinalQualityEventType.ANALYSIS_COMPLETED

        self._event(
            event_type,
            resource_id,
            final_state,
            risk,
            confidence,
            {
                "decision": final_decision.value,
                "action": action.value,
                "scope": scope.value,
            },
        )

        return result

    # ------------------------------------------------------------------
    # BATCH ANALYSIS
    # ------------------------------------------------------------------

    def analyze_many(
        self,
        items: Sequence[FinalQualityInput],
    ) -> List[FinalQualityResult]:
        if len(items) > self.policy.max_batch_size:
            raise ValueError(
                "batch exceeds per-request safety limit"
            )

        return [
            self.analyze(item)
            for item in items
        ]

    # ------------------------------------------------------------------
    # RESULT / METADATA ACCESS
    # ------------------------------------------------------------------

    def get_result(
        self,
        resource_id: str,
    ) -> Optional[FinalQualityResult]:
        return self.backend.get_result(resource_id)

    def result_to_dict(
        self,
        result: FinalQualityResult,
    ) -> Dict[str, Any]:
        return asdict(result)

    # ------------------------------------------------------------------
    # ARCHITECTURE METADATA
    # ------------------------------------------------------------------

    def architecture(self) -> Dict[str, Any]:
        return {
            "architecture_version": ARCHITECTURE_VERSION,
            "phase": PHASE,
            "phase_name": PHASE_NAME,
            "previous_stage": PREVIOUS_STAGE,
            "next_phase": NEXT_PHASE,
            "next_phase_name": NEXT_PHASE_NAME,
            "scale_target": SCALE_TARGET,
            "google_scale_capability_target":
                GOOGLE_SCALE_CAPABILITY_TARGET,
            "google_technology_dependency":
                GOOGLE_TECHNOLOGY_DEPENDENCY,

            "control_plane_only": True,

            "direct_execution": {
                "crawl": False,
                "http": False,
                "worker_assignment": False,
                "index_mutation": False,
                "ranking": False,
                "malware_execution": False,
                "exploit_execution": False,
                "active_scanning": False,
                "external_enforcement": False,
                "quarantine_execution": False,
                "recovery_execution": False,
                "restoration_execution": False,
            },

            "integrated_stages": [
                "14.1",
                "14.2",
                "14.3",
                "14.4",
                "14.5",
                "14.6",
                "14.7",
                "14.8",
            ],

            "outputs": [
                "final_risk",
                "final_confidence",
                "final_decision",
                "control_plan",
                "quarantine_plan",
                "recovery_plan",
                "restoration_plan",
                "reason_codes",
                "lineage",
                "idempotency_key",
                "audit_events",
                "checkpoints",
            ],

            "properties": [
                "deterministic",
                "distributed",
                "partition_aware",
                "shard_aware",
                "region_aware",
                "domain_aware",
                "host_aware",
                "cross_resource",
                "temporal",
                "historical",
                "partial_evidence_aware",
                "provenance_preserving",
                "auditable",
                "idempotent",
                "policy_driven",
                "google_independent",
            ],
        }


# ============================================================================
# ALIASES
# ============================================================================


FinalSpamAbuseSecurityQuality = (
    FinalSpamAbuseSecurityQualityArchitecture
)

GlobalSpamAbuseSecurityQualityArchitecture = (
    FinalSpamAbuseSecurityQualityArchitecture
)

Phase14_9FinalSpamAbuseSecurityQuality = (
    FinalSpamAbuseSecurityQualityArchitecture
)

FinalQualityArchitecture = (
    FinalSpamAbuseSecurityQualityArchitecture
)


# ============================================================================
# MODULE EXPORTS
# ============================================================================


__all__ = [
    "SCALE_TARGET",
    "GOOGLE_SCALE_CAPABILITY_TARGET",
    "GOOGLE_TECHNOLOGY_DEPENDENCY",

    "ARCHITECTURE_VERSION",
    "PHASE",
    "PREVIOUS_STAGE",
    "NEXT_PHASE",

    "FinalQualityState",
    "FinalQualitySignalFamily",
    "FinalQualitySignalType",
    "FinalQualityEvidenceStrength",
    "FinalRiskBand",
    "FinalQualityAction",
    "FinalQualityScope",
    "FinalQualityDecision",
    "FinalQualityConfidence",
    "FinalQualityEvidenceKind",
    "FinalRecoveryState",
    "FinalRestorationState",
    "FinalQualityEventType",
    "FinalQualityCheckpointType",

    "FinalQualityIdentity",
    "FinalQualityLineage",
    "FinalQualityInput",
    "FinalQualityHistory",
    "FinalQualitySignal",
    "FinalQualityEvidence",
    "FinalQualityPolicy",
    "FinalQualityControlPlan",
    "FinalQuarantinePlan",
    "FinalRecoveryPlan",
    "FinalRestorationPlan",
    "FinalQualityCheckpoint",
    "FinalQualityEvent",
    "FinalQualityResult",

    "FinalQualityBackend",
    "InMemoryFinalQualityBackend",

    "FinalSpamAbuseSecurityQualityArchitecture",
    "FinalSpamAbuseSecurityQuality",
    "GlobalSpamAbuseSecurityQualityArchitecture",
    "Phase14_9FinalSpamAbuseSecurityQuality",
    "FinalQualityArchitecture",
]
