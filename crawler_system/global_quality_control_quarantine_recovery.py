"""
OUR SEARCH
Phase 14.8 — Global Quality Control / Quarantine / Recovery

Purpose
-------
Provide the global control-plane architecture for quality control,
quarantine, rehabilitation, and recovery across enormous-scale public-Web
search infrastructure.

This stage integrates security/trust signals, distributed abuse signals,
content-quality signals, historical evidence, and operational health signals
into globally coordinated quality-control outcomes.

Scale target
------------
billions -> trillions of publicly accessible Web resources

Google independence
-------------------
This architecture does not use Google Search, Google indexing, Google's
crawler, Google's ranking systems, Google's infrastructure, or Google APIs.

Safety boundary
---------------
This module:

- DOES aggregate externally supplied quality/security/abuse evidence.
- DOES calculate global quality-control risk.
- DOES determine quarantine/review/recovery intents.
- DOES coordinate resource, cluster, partition, shard, region, and global
  quality-control scopes.
- DOES preserve provenance, lineage, checkpoints, and recovery history.
- DOES support deterministic idempotent control-plane decisions.

This module DOES NOT:

- execute malware
- execute exploits
- perform active network scanning
- perform HTTP requests
- crawl the Web
- assign crawler workers
- directly mutate the search index
- directly delete Web resources
- directly modify external resources
- directly execute network blocking
- execute quarantine infrastructure actions
- replace Phase 14.4 security detection
- replace Phase 14.5 resource protection
- replace Phase 14.6 distributed abuse enforcement
- replace Phase 14.7 security/trust signals
- depend on Google technology

The output is a globally coordinated control-plane intent that can be
consumed by later infrastructure responsible for controlled execution.
"""

from __future__ import annotations

import hashlib
import json
import math
import time
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Dict, Iterable, List, Mapping, Optional, Protocol, Sequence, Tuple


# ============================================================================
# ARCHITECTURE METADATA
# ============================================================================

SCALE_TARGET = "billions_to_trillions_of_public_web_resources"
GOOGLE_SCALE_CAPABILITY_TARGET = True
GOOGLE_TECHNOLOGY_DEPENDENCY = False

ARCHITECTURE_VERSION = "global-quality-control-quarantine-recovery.v1"

PHASE = "14.8"
PREVIOUS_STAGE = "14.7"
NEXT_STAGE = "14.9"

PHASE_NAME = "Global Quality Control / Quarantine / Recovery"
NEXT_STAGE_NAME = "Final Spam / Abuse / Security / Quality Architecture"


# ============================================================================
# ENUMERATIONS
# ============================================================================


class GlobalQualityState(str, Enum):
    RECEIVED = "received"
    VALIDATING = "validating"
    NORMALIZING = "normalizing"

    QUALITY_ANALYSIS = "quality_analysis"
    SECURITY_ANALYSIS = "security_analysis"
    ABUSE_ANALYSIS = "abuse_analysis"
    TRUST_ANALYSIS = "trust_analysis"

    RESOURCE_ANALYSIS = "resource_analysis"
    CLUSTER_ANALYSIS = "cluster_analysis"
    PARTITION_ANALYSIS = "partition_analysis"
    SHARD_ANALYSIS = "shard_analysis"
    REGION_ANALYSIS = "region_analysis"
    GLOBAL_ANALYSIS = "global_analysis"

    CORRELATION = "correlation"
    RISK_CALCULATION = "risk_calculation"
    CONFIDENCE_CALCULATION = "confidence_calculation"

    QUALITY_CONTROL_PLANNING = "quality_control_planning"
    QUARANTINE_PLANNING = "quarantine_planning"
    REVIEW_PLANNING = "review_planning"

    RECOVERY_ANALYSIS = "recovery_analysis"
    REHABILITATION_ANALYSIS = "rehabilitation_analysis"

    IDEMPOTENCY_CHECK = "idempotency_check"
    CHECKPOINTING = "checkpointing"

    DECISION_READY = "decision_ready"
    COMPLETED = "completed"
    PARTIAL = "partial"
    DEFERRED = "deferred"
    REJECTED = "rejected"
    FAILED = "failed"


class QualitySignalFamily(str, Enum):
    CONTENT = "content"
    QUALITY = "quality"
    MANIPULATION = "manipulation"

    SECURITY = "security"
    TRUST = "trust"
    SAFE_BROWSING = "safe_browsing"

    SPAM = "spam"
    ABUSE = "abuse"

    MALWARE = "malware"
    PHISHING = "phishing"
    HARMFUL = "harmful"

    REPUTATION = "reputation"
    HISTORY = "history"
    TEMPORAL = "temporal"

    CROSS_RESOURCE = "cross_resource"
    CROSS_DOMAIN = "cross_domain"
    INFRASTRUCTURE = "infrastructure"

    RESOURCE = "resource"
    CLUSTER = "cluster"
    PARTITION = "partition"
    SHARD = "shard"
    REGION = "region"
    GLOBAL = "global"

    SOURCE = "source"
    POLICY = "policy"
    TECHNICAL = "technical"


class QualitySignalType(str, Enum):
    LOW_CONTENT_QUALITY = "low_content_quality"
    CONTENT_MANIPULATION = "content_manipulation"
    THIN_CONTENT = "thin_content"
    DUPLICATED_CONTENT = "duplicated_content"
    GENERATED_CONTENT_PATTERN = "generated_content_pattern"

    KEYWORD_STUFFING = "keyword_stuffing"
    DOORWAY_PATTERN = "doorway_pattern"
    CLOAKING_PATTERN = "cloaking_pattern"
    HIDDEN_CONTENT_PATTERN = "hidden_content_pattern"

    MALWARE_RISK = "malware_risk"
    PHISHING_RISK = "phishing_risk"
    HARMFUL_RESOURCE_RISK = "harmful_resource_risk"
    EXPLOITATION_RISK = "exploitation_risk"

    SECURITY_TRUST_DEGRADATION = "security_trust_degradation"
    LOW_SECURITY_TRUST = "low_security_trust"
    REPUTATION_DEGRADATION = "reputation_degradation"

    ABUSE_RISK = "abuse_risk"
    RECURRING_ABUSE = "recurring_abuse"
    POLICY_BYPASS = "policy_bypass"

    HISTORICAL_SECURITY_RISK = "historical_security_risk"
    HISTORICAL_ABUSE_RISK = "historical_abuse_risk"

    TEMPORAL_RISK_SPIKE = "temporal_risk_spike"
    RAPID_RISK_ESCALATION = "rapid_risk_escalation"

    CROSS_RESOURCE_RISK = "cross_resource_risk"
    CROSS_DOMAIN_RISK = "cross_domain_risk"
    INFRASTRUCTURE_REUSE_RISK = "infrastructure_reuse_risk"

    RESOURCE_RISK = "resource_risk"
    CLUSTER_RISK = "cluster_risk"
    PARTITION_RISK = "partition_risk"
    SHARD_RISK = "shard_risk"
    REGION_RISK = "region_risk"
    GLOBAL_RISK = "global_risk"

    SOURCE_CONFLICT = "source_conflict"
    LOW_SOURCE_RELIABILITY = "low_source_reliability"

    TECHNICAL_RISK = "technical_risk"
    POLICY_RISK = "policy_risk"


class QualityEvidenceStrength(str, Enum):
    NONE = "none"
    WEAK = "weak"
    MODERATE = "moderate"
    STRONG = "strong"
    VERY_STRONG = "very_strong"


class QualityRiskBand(str, Enum):
    NONE = "none"
    LOW = "low"
    MODERATE = "moderate"
    HIGH = "high"
    VERY_HIGH = "very_high"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


class QualityTrustBand(str, Enum):
    UNKNOWN = "unknown"
    VERY_LOW = "very_low"
    LOW = "low"
    MODERATE = "moderate"
    HIGH = "high"
    VERY_HIGH = "very_high"


class QualityControlAction(str, Enum):
    NONE = "none"
    MONITOR = "monitor"
    REQUIRE_REVIEW = "require_review"

    REDUCE_PROCESSING_PRIORITY = "reduce_processing_priority"
    DEFER_PROCESSING = "defer_processing"

    QUARANTINE_RESOURCE = "quarantine_resource"
    QUARANTINE_DOCUMENT = "quarantine_document"
    QUARANTINE_CLUSTER = "quarantine_cluster"
    QUARANTINE_PARTITION = "quarantine_partition"
    QUARANTINE_SHARD = "quarantine_shard"
    QUARANTINE_REGION = "quarantine_region"

    SUSPEND_PROCESSING = "suspend_processing"

    RECOVERY_REVIEW = "recovery_review"
    REHABILITATION_REVIEW = "rehabilitation_review"

    RESTORE_RESOURCE = "restore_resource"
    RESTORE_CLUSTER = "restore_cluster"


class QualityControlScope(str, Enum):
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


class QualityControlDecision(str, Enum):
    NO_ACTION = "no_action"
    MONITOR = "monitor"
    REVIEW_REQUIRED = "review_required"

    PROTECTION_REQUIRED = "protection_required"
    QUARANTINE_REQUIRED = "quarantine_required"

    RECOVERY_REQUIRED = "recovery_required"
    REHABILITATION_REQUIRED = "rehabilitation_required"

    DEFERRED = "deferred"
    PARTIAL_EVIDENCE = "partial_evidence"
    REJECTED_INPUT = "rejected_input"


class RecoveryState(str, Enum):
    NOT_REQUIRED = "not_required"
    ELIGIBLE = "eligible"
    UNDER_REVIEW = "under_review"
    READY = "ready"
    BLOCKED = "blocked"
    RESTORE_INTENT = "restore_intent"
    REHABILITATION_INTENT = "rehabilitation_intent"


class RecoveryDecision(str, Enum):
    NO_RECOVERY = "no_recovery"
    REVIEW_RECOVERY = "review_recovery"
    REHABILITATION_REVIEW = "rehabilitation_review"
    RESTORE = "restore"
    DEFER = "defer"


class QuarantineState(str, Enum):
    NOT_REQUIRED = "not_required"
    PROPOSED = "proposed"
    READY = "ready"
    ACTIVE_INTENT = "active_intent"
    RELEASE_REVIEW = "release_review"
    RELEASE_INTENT = "release_intent"


class EvidenceKind(str, Enum):
    QUALITY = "quality"
    MANIPULATION = "manipulation"
    SECURITY = "security"
    TRUST = "trust"
    SAFE_BROWSING = "safe_browsing"
    SPAM = "spam"
    ABUSE = "abuse"
    MALWARE = "malware"
    PHISHING = "phishing"
    HARMFUL = "harmful"
    REPUTATION = "reputation"
    HISTORY = "history"
    TEMPORAL = "temporal"
    CROSS_RESOURCE = "cross_resource"
    CROSS_DOMAIN = "cross_domain"
    INFRASTRUCTURE = "infrastructure"
    RESOURCE = "resource"
    CLUSTER = "cluster"
    PARTITION = "partition"
    SHARD = "shard"
    REGION = "region"
    GLOBAL = "global"
    SOURCE = "source"
    POLICY = "policy"
    TECHNICAL = "technical"


class QualityEventType(str, Enum):
    ANALYSIS_STARTED = "analysis_started"
    INPUT_ACCEPTED = "input_accepted"
    INPUT_NORMALIZED = "input_normalized"

    EVIDENCE_ACCEPTED = "evidence_accepted"
    SIGNAL_DETECTED = "signal_detected"

    DISTRIBUTED_CORRELATION = "distributed_correlation"
    RISK_CALCULATED = "risk_calculated"
    TRUST_CALCULATED = "trust_calculated"
    CONFIDENCE_CALCULATED = "confidence_calculated"

    QUALITY_CONTROL_PLANNED = "quality_control_planned"
    QUARANTINE_PLANNED = "quarantine_planned"
    RECOVERY_PLANNED = "recovery_planned"

    IDEMPOTENCY_CHECKED = "idempotency_checked"
    CHECKPOINT_CREATED = "checkpoint_created"

    DECISION_PREPARED = "decision_prepared"

    ANALYSIS_COMPLETED = "analysis_completed"
    ANALYSIS_PARTIAL = "analysis_partial"
    ANALYSIS_DEFERRED = "analysis_deferred"
    ANALYSIS_REJECTED = "analysis_rejected"
    ANALYSIS_FAILED = "analysis_failed"


class QualityCheckpointType(str, Enum):
    INPUT_ACCEPTED = "input_accepted"
    NORMALIZATION_COMPLETE = "normalization_complete"

    SIGNAL_EXTRACTION_COMPLETE = "signal_extraction_complete"
    CORRELATION_COMPLETE = "correlation_complete"

    RISK_COMPLETE = "risk_complete"
    TRUST_COMPLETE = "trust_complete"
    CONFIDENCE_COMPLETE = "confidence_complete"

    QUALITY_CONTROL_COMPLETE = "quality_control_complete"
    QUARANTINE_COMPLETE = "quarantine_complete"
    RECOVERY_COMPLETE = "recovery_complete"

    RESULT_COMPLETE = "result_complete"


# ============================================================================
# DATA MODELS
# ============================================================================


@dataclass(frozen=True)
class GlobalQualityIdentity:
    resource_id: str

    document_id: str = ""
    url: str = ""
    canonical_url: str = ""

    host_id: str = ""
    domain_id: str = ""
    organization_id: str = ""

    cluster_id: str = ""
    partition_id: str = ""
    shard_id: str = ""
    region_id: str = ""

    version: str = "1"


@dataclass(frozen=True)
class GlobalQualityLineage:
    resource_id: str

    current_stage: str = PHASE
    previous_stage: str = PREVIOUS_STAGE
    previous_stage_version: str = ""

    source_evidence_ids: Tuple[str, ...] = ()
    source_evidence_versions: Tuple[str, ...] = ()

    parent_resource_id: str = ""
    parent_document_id: str = ""

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass
class GlobalQualityInput:
    identity: GlobalQualityIdentity

    lineage: GlobalQualityLineage = field(
        default_factory=lambda: GlobalQualityLineage(resource_id="")
    )

    timestamp: float = 0.0

    # ------------------------------------------------------------------------
    # Quality
    # ------------------------------------------------------------------------

    content_quality_risk: float = 0.0
    content_manipulation_risk: float = 0.0
    spam_risk: float = 0.0

    thin_content: bool = False
    duplicated_content: bool = False
    generated_content_pattern: bool = False

    keyword_stuffing: bool = False
    doorway_pattern: bool = False
    cloaking_pattern: bool = False
    hidden_content_pattern: bool = False

    # ------------------------------------------------------------------------
    # Security / trust
    # ------------------------------------------------------------------------

    security_risk: float = 0.0
    trust_risk: float = 0.0
    safe_browsing_risk: float = 0.0

    malware_risk: float = 0.0
    phishing_risk: float = 0.0
    harmful_resource_risk: float = 0.0
    exploitation_risk: float = 0.0

    reputation_risk: float = 0.0

    # ------------------------------------------------------------------------
    # Abuse
    # ------------------------------------------------------------------------

    abuse_risk: float = 0.0
    recurring_abuse: bool = False
    policy_bypass: bool = False

    # ------------------------------------------------------------------------
    # History / temporal
    # ------------------------------------------------------------------------

    historical_security_risk: float = 0.0
    historical_abuse_risk: float = 0.0

    temporal_risk: float = 0.0
    recent_risk_spike: bool = False
    rapid_risk_escalation: bool = False

    # ------------------------------------------------------------------------
    # Distributed
    # ------------------------------------------------------------------------

    cross_resource_risk: float = 0.0
    cross_domain_risk: float = 0.0
    infrastructure_risk: float = 0.0

    affected_resource_count: int = 0
    affected_domain_count: int = 0
    infrastructure_reuse_count: int = 0

    # ------------------------------------------------------------------------
    # Scope pressure
    # ------------------------------------------------------------------------

    resource_risk: float = 0.0
    cluster_risk: float = 0.0
    partition_risk: float = 0.0
    shard_risk: float = 0.0
    region_risk: float = 0.0
    global_risk: float = 0.0

    # ------------------------------------------------------------------------
    # Source / technical
    # ------------------------------------------------------------------------

    source_risk: float = 0.0
    source_confidence: float = 0.0
    source_reliability: float = 0.0

    conflicting_source_count: int = 0
    trusted_source_count: int = 0
    source_count: int = 0

    technical_risk: float = 0.0
    policy_risk: float = 0.0

    # ------------------------------------------------------------------------
    # Incoming evidence
    # ------------------------------------------------------------------------

    evidence_ids: Tuple[str, ...] = ()
    evidence_versions: Tuple[str, ...] = ()

    partial: bool = False
    missing_fields: Tuple[str, ...] = ()

    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class GlobalQualityHistory:
    previous_risk_score: float = 0.0
    previous_trust_score: float = 0.0

    previous_risk_band: str = QualityRiskBand.UNKNOWN.value
    previous_trust_band: str = QualityTrustBand.UNKNOWN.value

    previous_decision: str = ""
    previous_decision_id: str = ""

    previous_quarantine_count: int = 0
    previous_review_count: int = 0
    previous_recovery_count: int = 0

    previous_security_incidents: int = 0
    previous_abuse_incidents: int = 0

    historical_false_positive_count: int = 0
    historical_false_negative_count: int = 0

    rehabilitation_success_count: int = 0
    rehabilitation_failure_count: int = 0

    last_quarantine_timestamp: float = 0.0
    last_recovery_timestamp: float = 0.0
    last_analysis_timestamp: float = 0.0

    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class GlobalQualitySignal:
    signal_id: str
    resource_id: str

    family: QualitySignalFamily
    signal_type: QualitySignalType

    value: float
    confidence: float

    evidence_strength: QualityEvidenceStrength

    source: str = ""
    timestamp: float = 0.0

    evidence_ids: Tuple[str, ...] = ()

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class GlobalQualityEvidence:
    evidence_id: str
    resource_id: str

    kind: EvidenceKind

    confidence: float
    strength: QualityEvidenceStrength

    signal_ids: Tuple[str, ...] = ()

    source: str = ""
    timestamp: float = 0.0

    provenance: Mapping[str, Any] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass
class GlobalQualityPolicy:
    max_signals_per_resource: int = 512
    max_evidence_per_resource: int = 512

    minimum_confidence: float = 0.20

    low_risk_threshold: float = 0.20
    moderate_risk_threshold: float = 0.40
    high_risk_threshold: float = 0.65
    very_high_risk_threshold: float = 0.82
    critical_risk_threshold: float = 0.93

    very_low_trust_threshold: float = 0.20
    low_trust_threshold: float = 0.40
    moderate_trust_threshold: float = 0.60
    high_trust_threshold: float = 0.80

    minimum_sources_for_strong_evidence: int = 2
    minimum_sources_for_very_strong_evidence: int = 3

    minimum_cluster_size: int = 5
    large_cluster_size: int = 25
    very_large_cluster_size: int = 100

    allow_partial: bool = True
    deterministic: bool = True

    # Quarantine safeguards.
    require_multi_source_for_critical_quarantine: bool = True
    require_high_confidence_for_quarantine: bool = True

    quarantine_confidence_threshold: float = 0.75
    critical_quarantine_confidence_threshold: float = 0.90

    # Recovery safeguards.
    recovery_min_confidence: float = 0.75
    recovery_minimum_clean_observations: int = 2

    # Large-scope actions require stronger evidence.
    regional_action_threshold: float = 0.90
    global_action_threshold: float = 0.97

    # Prevent a single weak source from causing large-scope action.
    minimum_sources_for_large_scope: int = 3


@dataclass(frozen=True)
class QualityControlPlan:
    plan_id: str
    resource_id: str

    action: QualityControlAction
    scope: QualityControlScope

    confidence: float
    risk_score: float

    reason_codes: Tuple[str, ...] = ()

    requires_review: bool = False
    requires_multiple_sources: bool = False

    idempotency_key: str = ""

    timestamp: float = 0.0

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class QuarantinePlan:
    quarantine_id: str
    resource_id: str

    state: QuarantineState
    scope: QualityControlScope

    action: QualityControlAction

    risk_score: float
    confidence: float

    reason_codes: Tuple[str, ...] = ()

    requires_review: bool = True
    reversible: bool = True

    idempotency_key: str = ""

    timestamp: float = 0.0

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RecoveryPlan:
    recovery_id: str
    resource_id: str

    state: RecoveryState
    decision: RecoveryDecision

    scope: QualityControlScope

    confidence: float
    current_risk: float

    reason_codes: Tuple[str, ...] = ()

    requires_review: bool = True
    reversible: bool = True

    idempotency_key: str = ""

    timestamp: float = 0.0

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class GlobalQualityCheckpoint:
    checkpoint_id: str
    resource_id: str

    checkpoint_type: QualityCheckpointType

    state: GlobalQualityState
    timestamp: float

    signal_count: int = 0
    evidence_count: int = 0

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class GlobalQualityEvent:
    event_id: str
    resource_id: str

    event_type: QualityEventType
    state: GlobalQualityState

    timestamp: float

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass
class GlobalQualityResult:
    resource_id: str

    state: GlobalQualityState
    decision: QualityControlDecision

    risk_score: float
    trust_score: float
    confidence: float

    risk_band: QualityRiskBand
    trust_band: QualityTrustBand

    evidence_strength: QualityEvidenceStrength

    quarantine_state: QuarantineState
    recovery_state: RecoveryState

    signals: List[GlobalQualitySignal] = field(default_factory=list)
    evidence: List[GlobalQualityEvidence] = field(default_factory=list)

    quality_control_plan: Optional[QualityControlPlan] = None
    quarantine_plan: Optional[QuarantinePlan] = None
    recovery_plan: Optional[RecoveryPlan] = None

    reasons: List[str] = field(default_factory=list)

    partial: bool = False
    missing_fields: List[str] = field(default_factory=list)

    lineage: Optional[GlobalQualityLineage] = None

    timestamp: float = 0.0

    metadata: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# BACKEND
# ============================================================================


class GlobalQualityBackend(Protocol):
    def save_signal(self, signal: GlobalQualitySignal) -> None:
        ...

    def save_evidence(self, evidence: GlobalQualityEvidence) -> None:
        ...

    def save_event(self, event: GlobalQualityEvent) -> None:
        ...

    def save_checkpoint(self, checkpoint: GlobalQualityCheckpoint) -> None:
        ...

    def save_result(self, result: GlobalQualityResult) -> None:
        ...

    def get_result(self, resource_id: str) -> Optional[GlobalQualityResult]:
        ...


class InMemoryGlobalQualityBackend:
    """
    Framework-neutral backend for architecture development.

    Production deployments can replace this with a distributed durable
    metadata system.
    """

    def __init__(self) -> None:
        self.signals: Dict[str, GlobalQualitySignal] = {}
        self.evidence: Dict[str, GlobalQualityEvidence] = {}
        self.events: Dict[str, GlobalQualityEvent] = {}
        self.checkpoints: Dict[str, GlobalQualityCheckpoint] = {}
        self.results: Dict[str, GlobalQualityResult] = {}

    def save_signal(self, signal: GlobalQualitySignal) -> None:
        self.signals[signal.signal_id] = signal

    def save_evidence(self, evidence: GlobalQualityEvidence) -> None:
        self.evidence[evidence.evidence_id] = evidence

    def save_event(self, event: GlobalQualityEvent) -> None:
        self.events[event.event_id] = event

    def save_checkpoint(self, checkpoint: GlobalQualityCheckpoint) -> None:
        self.checkpoints[checkpoint.checkpoint_id] = checkpoint

    def save_result(self, result: GlobalQualityResult) -> None:
        self.results[result.resource_id] = result

    def get_result(self, resource_id: str) -> Optional[GlobalQualityResult]:
        return self.results.get(resource_id)


# ============================================================================
# MAIN ARCHITECTURE
# ============================================================================


class GlobalQualityControlQuarantineRecoveryArchitecture:
    """
    Phase 14.8 integrated global quality-control architecture.

    Pipeline:

        Phase 14.1 spam evidence
                  |
        Phase 14.2 content-quality evidence
                  |
        Phase 14.3 link/graph abuse evidence
                  |
        Phase 14.4 malware/phishing/harmful evidence
                  |
        Phase 14.5 crawl/index protection evidence
                  |
        Phase 14.6 distributed abuse evidence
                  |
        Phase 14.7 security/trust signals
                  |
             correlation
                  |
             risk/trust
                  |
          global quality control
             /          \
       quarantine      recovery
             \          /
             review / rehabilitation
                  |
           downstream execution
    """

    def __init__(
        self,
        policy: Optional[GlobalQualityPolicy] = None,
        backend: Optional[GlobalQualityBackend] = None,
    ) -> None:
        self.policy = policy or GlobalQualityPolicy()
        self.backend = backend or InMemoryGlobalQualityBackend()

    # ========================================================================
    # SAFE HELPERS
    # ========================================================================

    @staticmethod
    def _now() -> float:
        return time.time()

    @staticmethod
    def _clamp(
        value: Any,
        low: float = 0.0,
        high: float = 1.0,
    ) -> float:
        try:
            number = float(value)
        except (TypeError, ValueError):
            return low

        if not math.isfinite(number):
            return low

        return max(low, min(high, number))

    @staticmethod
    def _safe_int(
        value: Any,
        minimum: int = 0,
    ) -> int:
        try:
            number = int(value)
        except (TypeError, ValueError):
            return minimum

        return max(minimum, number)

    @staticmethod
    def _safe_text(value: Any) -> str:
        if value is None:
            return ""
        return str(value).strip()

    @staticmethod
    def _digest(value: Any) -> str:
        encoded = json.dumps(
            value,
            sort_keys=True,
            default=str,
            separators=(",", ":"),
        ).encode("utf-8")

        return hashlib.sha256(encoded).hexdigest()

    def _signal_id(
        self,
        resource_id: str,
        family: QualitySignalFamily,
        signal_type: QualitySignalType,
        value: float,
    ) -> str:
        payload = {
            "resource_id": resource_id,
            "family": family.value,
            "signal_type": signal_type.value,
            "value": round(value, 8),
            "architecture": ARCHITECTURE_VERSION,
        }

        return "gqs_" + self._digest(payload)[:32]

    def _evidence_id(
        self,
        resource_id: str,
        kind: EvidenceKind,
        signal_ids: Sequence[str],
    ) -> str:
        payload = {
            "resource_id": resource_id,
            "kind": kind.value,
            "signal_ids": sorted(signal_ids),
            "architecture": ARCHITECTURE_VERSION,
        }

        return "gqe_" + self._digest(payload)[:32]

    def _plan_id(
        self,
        resource_id: str,
        action: QualityControlAction,
        scope: QualityControlScope,
        risk_score: float,
        confidence: float,
    ) -> str:
        payload = {
            "resource_id": resource_id,
            "action": action.value,
            "scope": scope.value,
            "risk": round(risk_score, 8),
            "confidence": round(confidence, 8),
            "architecture": ARCHITECTURE_VERSION,
        }

        return "gqp_" + self._digest(payload)[:32]

    def _quarantine_id(
        self,
        resource_id: str,
        scope: QualityControlScope,
        risk_score: float,
    ) -> str:
        payload = {
            "resource_id": resource_id,
            "scope": scope.value,
            "risk": round(risk_score, 8),
            "architecture": ARCHITECTURE_VERSION,
        }

        return "gqq_" + self._digest(payload)[:32]

    def _recovery_id(
        self,
        resource_id: str,
        decision: RecoveryDecision,
        risk_score: float,
    ) -> str:
        payload = {
            "resource_id": resource_id,
            "decision": decision.value,
            "risk": round(risk_score, 8),
            "architecture": ARCHITECTURE_VERSION,
        }

        return "gqr_" + self._digest(payload)[:32]

    def _idempotency_key(
        self,
        resource_id: str,
        action: str,
        scope: str,
    ) -> str:
        payload = {
            "resource_id": resource_id,
            "action": action,
            "scope": scope,
            "architecture": ARCHITECTURE_VERSION,
        }

        return "idem_" + self._digest(payload)[:32]

    # ========================================================================
    # EVENTS / CHECKPOINTS
    # ========================================================================

    def _event(
        self,
        resource_id: str,
        event_type: QualityEventType,
        state: GlobalQualityState,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> GlobalQualityEvent:
        timestamp = self._now()

        event = GlobalQualityEvent(
            event_id="evt_" + self._digest(
                {
                    "resource_id": resource_id,
                    "event_type": event_type.value,
                    "timestamp": round(timestamp, 6),
                }
            )[:32],
            resource_id=resource_id,
            event_type=event_type,
            state=state,
            timestamp=timestamp,
            metadata=dict(metadata or {}),
        )

        self.backend.save_event(event)
        return event

    def _checkpoint(
        self,
        resource_id: str,
        checkpoint_type: QualityCheckpointType,
        state: GlobalQualityState,
        signal_count: int = 0,
        evidence_count: int = 0,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> GlobalQualityCheckpoint:
        checkpoint = GlobalQualityCheckpoint(
            checkpoint_id="chk_" + self._digest(
                {
                    "resource_id": resource_id,
                    "checkpoint_type": checkpoint_type.value,
                    "architecture": ARCHITECTURE_VERSION,
                }
            )[:32],
            resource_id=resource_id,
            checkpoint_type=checkpoint_type,
            state=state,
            timestamp=self._now(),
            signal_count=signal_count,
            evidence_count=evidence_count,
            metadata=dict(metadata or {}),
        )

        self.backend.save_checkpoint(checkpoint)

        self._event(
            resource_id,
            QualityEventType.CHECKPOINT_CREATED,
            state,
            {
                "checkpoint_type": checkpoint_type.value,
                "signal_count": signal_count,
                "evidence_count": evidence_count,
            },
        )

        return checkpoint

    # ========================================================================
    # VALIDATION
    # ========================================================================

    def validate_input(
        self,
        value: GlobalQualityInput,
    ) -> Tuple[bool, List[str]]:
        errors: List[str] = []

        if not isinstance(value, GlobalQualityInput):
            errors.append("input must be GlobalQualityInput")
            return False, errors

        if not value.identity.resource_id:
            errors.append("resource_id is required")

        risk_fields = [
            "content_quality_risk",
            "content_manipulation_risk",
            "spam_risk",
            "security_risk",
            "trust_risk",
            "safe_browsing_risk",
            "malware_risk",
            "phishing_risk",
            "harmful_resource_risk",
            "exploitation_risk",
            "reputation_risk",
            "abuse_risk",
            "historical_security_risk",
            "historical_abuse_risk",
            "temporal_risk",
            "cross_resource_risk",
            "cross_domain_risk",
            "infrastructure_risk",
            "resource_risk",
            "cluster_risk",
            "partition_risk",
            "shard_risk",
            "region_risk",
            "global_risk",
            "source_risk",
            "source_confidence",
            "source_reliability",
            "technical_risk",
            "policy_risk",
        ]

        for name in risk_fields:
            try:
                number = float(getattr(value, name))
            except (TypeError, ValueError):
                errors.append(f"{name} must be numeric")
                continue

            if not math.isfinite(number):
                errors.append(f"{name} must be finite")

        count_fields = [
            "affected_resource_count",
            "affected_domain_count",
            "infrastructure_reuse_count",
            "conflicting_source_count",
            "trusted_source_count",
            "source_count",
        ]

        for name in count_fields:
            try:
                if int(getattr(value, name)) < 0:
                    errors.append(f"{name} cannot be negative")
            except (TypeError, ValueError):
                errors.append(f"{name} must be an integer")

        return not errors, errors

    # ========================================================================
    # NORMALIZATION
    # ========================================================================

    def normalize_input(
        self,
        value: GlobalQualityInput,
    ) -> GlobalQualityInput:
        identity = GlobalQualityIdentity(
            resource_id=self._safe_text(
                value.identity.resource_id
            ),
            document_id=self._safe_text(
                value.identity.document_id
            ),
            url=self._safe_text(
                value.identity.url
            ),
            canonical_url=self._safe_text(
                value.identity.canonical_url
            ),
            host_id=self._safe_text(
                value.identity.host_id
            ),
            domain_id=self._safe_text(
                value.identity.domain_id
            ),
            organization_id=self._safe_text(
                value.identity.organization_id
            ),
            cluster_id=self._safe_text(
                value.identity.cluster_id
            ),
            partition_id=self._safe_text(
                value.identity.partition_id
            ),
            shard_id=self._safe_text(
                value.identity.shard_id
            ),
            region_id=self._safe_text(
                value.identity.region_id
            ),
            version=self._safe_text(
                value.identity.version
            ) or "1",
        )

        return GlobalQualityInput(
            identity=identity,
            lineage=value.lineage,
            timestamp=float(value.timestamp or self._now()),

            content_quality_risk=self._clamp(
                value.content_quality_risk
            ),
            content_manipulation_risk=self._clamp(
                value.content_manipulation_risk
            ),
            spam_risk=self._clamp(value.spam_risk),

            thin_content=bool(value.thin_content),
            duplicated_content=bool(value.duplicated_content),
            generated_content_pattern=bool(
                value.generated_content_pattern
            ),

            keyword_stuffing=bool(value.keyword_stuffing),
            doorway_pattern=bool(value.doorway_pattern),
            cloaking_pattern=bool(value.cloaking_pattern),
            hidden_content_pattern=bool(
                value.hidden_content_pattern
            ),

            security_risk=self._clamp(value.security_risk),
            trust_risk=self._clamp(value.trust_risk),
            safe_browsing_risk=self._clamp(
                value.safe_browsing_risk
            ),

            malware_risk=self._clamp(value.malware_risk),
            phishing_risk=self._clamp(value.phishing_risk),
            harmful_resource_risk=self._clamp(
                value.harmful_resource_risk
            ),
            exploitation_risk=self._clamp(
                value.exploitation_risk
            ),

            reputation_risk=self._clamp(
                value.reputation_risk
            ),

            abuse_risk=self._clamp(value.abuse_risk),
            recurring_abuse=bool(value.recurring_abuse),
            policy_bypass=bool(value.policy_bypass),

            historical_security_risk=self._clamp(
                value.historical_security_risk
            ),
            historical_abuse_risk=self._clamp(
                value.historical_abuse_risk
            ),

            temporal_risk=self._clamp(value.temporal_risk),
            recent_risk_spike=bool(value.recent_risk_spike),
            rapid_risk_escalation=bool(
                value.rapid_risk_escalation
            ),

            cross_resource_risk=self._clamp(
                value.cross_resource_risk
            ),
            cross_domain_risk=self._clamp(
                value.cross_domain_risk
            ),
            infrastructure_risk=self._clamp(
                value.infrastructure_risk
            ),

            affected_resource_count=self._safe_int(
                value.affected_resource_count
            ),
            affected_domain_count=self._safe_int(
                value.affected_domain_count
            ),
            infrastructure_reuse_count=self._safe_int(
                value.infrastructure_reuse_count
            ),

            resource_risk=self._clamp(value.resource_risk),
            cluster_risk=self._clamp(value.cluster_risk),
            partition_risk=self._clamp(value.partition_risk),
            shard_risk=self._clamp(value.shard_risk),
            region_risk=self._clamp(value.region_risk),
            global_risk=self._clamp(value.global_risk),

            source_risk=self._clamp(value.source_risk),
            source_confidence=self._clamp(
                value.source_confidence
            ),
            source_reliability=self._clamp(
                value.source_reliability
            ),

            conflicting_source_count=self._safe_int(
                value.conflicting_source_count
            ),
            trusted_source_count=self._safe_int(
                value.trusted_source_count
            ),
            source_count=self._safe_int(
                value.source_count
            ),

            technical_risk=self._clamp(
                value.technical_risk
            ),
            policy_risk=self._clamp(value.policy_risk),

            evidence_ids=tuple(
                self._safe_text(item)
                for item in value.evidence_ids
                if self._safe_text(item)
            ),
            evidence_versions=tuple(
                self._safe_text(item)
                for item in value.evidence_versions
                if self._safe_text(item)
            ),

            partial=bool(value.partial),
            missing_fields=tuple(
                self._safe_text(item)
                for item in value.missing_fields
                if self._safe_text(item)
            ),

            metadata=dict(value.metadata or {}),
        )

    # ========================================================================
    # SIGNAL HELPERS
    # ========================================================================

    def _strength_from_confidence(
        self,
        confidence: float,
    ) -> QualityEvidenceStrength:
        confidence = self._clamp(confidence)

        if confidence <= 0.0:
            return QualityEvidenceStrength.NONE

        if confidence < 0.40:
            return QualityEvidenceStrength.WEAK

        if confidence < 0.65:
            return QualityEvidenceStrength.MODERATE

        if confidence < 0.85:
            return QualityEvidenceStrength.STRONG

        return QualityEvidenceStrength.VERY_STRONG

    def _make_signal(
        self,
        value: GlobalQualityInput,
        family: QualitySignalFamily,
        signal_type: QualitySignalType,
        score: float,
        confidence: Optional[float] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> GlobalQualitySignal:
        score = self._clamp(score)

        if confidence is None:
            confidence = value.source_confidence

        confidence = self._clamp(confidence)

        signal = GlobalQualitySignal(
            signal_id=self._signal_id(
                value.identity.resource_id,
                family,
                signal_type,
                score,
            ),
            resource_id=value.identity.resource_id,
            family=family,
            signal_type=signal_type,
            value=score,
            confidence=confidence,
            evidence_strength=self._strength_from_confidence(
                confidence
            ),
            source="phase14.8",
            timestamp=value.timestamp,
            evidence_ids=tuple(value.evidence_ids),
            metadata=dict(metadata or {}),
        )

        self.backend.save_signal(signal)

        self._event(
            value.identity.resource_id,
            QualityEventType.SIGNAL_DETECTED,
            GlobalQualityState.QUALITY_ANALYSIS,
            {
                "signal_id": signal.signal_id,
                "family": family.value,
                "signal_type": signal_type.value,
                "value": score,
                "confidence": confidence,
            },
        )

        return signal

    # ========================================================================
    # SIGNAL EXTRACTION
    # ========================================================================

    def extract_signals(
        self,
        value: GlobalQualityInput,
    ) -> List[GlobalQualitySignal]:
        signals: List[GlobalQualitySignal] = []

        def add(
            family: QualitySignalFamily,
            signal_type: QualitySignalType,
            score: float,
            confidence: Optional[float] = None,
            metadata: Optional[Mapping[str, Any]] = None,
        ) -> None:
            signals.append(
                self._make_signal(
                    value,
                    family,
                    signal_type,
                    score,
                    confidence,
                    metadata,
                )
            )

        # --------------------------------------------------------------------
        # Quality
        # --------------------------------------------------------------------

        if value.content_quality_risk > 0.0:
            add(
                QualitySignalFamily.QUALITY,
                QualitySignalType.LOW_CONTENT_QUALITY,
                value.content_quality_risk,
            )

        if value.content_manipulation_risk > 0.0:
            add(
                QualitySignalFamily.MANIPULATION,
                QualitySignalType.CONTENT_MANIPULATION,
                value.content_manipulation_risk,
            )

        if value.thin_content:
            add(
                QualitySignalFamily.CONTENT,
                QualitySignalType.THIN_CONTENT,
                max(value.content_quality_risk, 0.65),
            )

        if value.duplicated_content:
            add(
                QualitySignalFamily.CONTENT,
                QualitySignalType.DUPLICATED_CONTENT,
                max(value.content_quality_risk, 0.60),
            )

        if value.generated_content_pattern:
            add(
                QualitySignalFamily.CONTENT,
                QualitySignalType.GENERATED_CONTENT_PATTERN,
                max(value.content_quality_risk, 0.50),
            )

        if value.keyword_stuffing:
            add(
                QualitySignalFamily.MANIPULATION,
                QualitySignalType.KEYWORD_STUFFING,
                max(value.content_manipulation_risk, 0.75),
            )

        if value.doorway_pattern:
            add(
                QualitySignalFamily.MANIPULATION,
                QualitySignalType.DOORWAY_PATTERN,
                max(value.content_manipulation_risk, 0.80),
            )

        if value.cloaking_pattern:
            add(
                QualitySignalFamily.MANIPULATION,
                QualitySignalType.CLOAKING_PATTERN,
                max(value.content_manipulation_risk, 0.85),
            )

        if value.hidden_content_pattern:
            add(
                QualitySignalFamily.MANIPULATION,
                QualitySignalType.HIDDEN_CONTENT_PATTERN,
                max(value.content_manipulation_risk, 0.80),
            )

        if value.spam_risk > 0.0:
            add(
                QualitySignalFamily.SPAM,
                QualitySignalType.ABUSE_RISK,
                value.spam_risk,
            )

        # --------------------------------------------------------------------
        # Security
        # --------------------------------------------------------------------

        if value.security_risk > 0.0:
            add(
                QualitySignalFamily.SECURITY,
                QualitySignalType.SECURITY_TRUST_DEGRADATION,
                value.security_risk,
            )

        if value.trust_risk > 0.0:
            add(
                QualitySignalFamily.TRUST,
                QualitySignalType.LOW_SECURITY_TRUST,
                value.trust_risk,
            )

        if value.safe_browsing_risk > 0.0:
            add(
                QualitySignalFamily.SAFE_BROWSING,
                QualitySignalType.SECURITY_TRUST_DEGRADATION,
                value.safe_browsing_risk,
            )

        if value.malware_risk > 0.0:
            add(
                QualitySignalFamily.MALWARE,
                QualitySignalType.MALWARE_RISK,
                value.malware_risk,
            )

        if value.phishing_risk > 0.0:
            add(
                QualitySignalFamily.PHISHING,
                QualitySignalType.PHISHING_RISK,
                value.phishing_risk,
            )

        if value.harmful_resource_risk > 0.0:
            add(
                QualitySignalFamily.HARMFUL,
                QualitySignalType.HARMFUL_RESOURCE_RISK,
                value.harmful_resource_risk,
            )

        if value.exploitation_risk > 0.0:
            add(
                QualitySignalFamily.SECURITY,
                QualitySignalType.EXPLOITATION_RISK,
                value.exploitation_risk,
            )

        if value.reputation_risk > 0.0:
            add(
                QualitySignalFamily.REPUTATION,
                QualitySignalType.REPUTATION_DEGRADATION,
                value.reputation_risk,
            )

        # --------------------------------------------------------------------
        # Abuse
        # --------------------------------------------------------------------

        if value.abuse_risk > 0.0:
            add(
                QualitySignalFamily.ABUSE,
                QualitySignalType.ABUSE_RISK,
                value.abuse_risk,
            )

        if value.recurring_abuse:
            add(
                QualitySignalFamily.ABUSE,
                QualitySignalType.RECURRING_ABUSE,
                max(value.abuse_risk, 0.75),
            )

        if value.policy_bypass:
            add(
                QualitySignalFamily.ABUSE,
                QualitySignalType.POLICY_BYPASS,
                max(value.abuse_risk, 0.80),
            )

        # --------------------------------------------------------------------
        # History
        # --------------------------------------------------------------------

        if value.historical_security_risk > 0.0:
            add(
                QualitySignalFamily.HISTORY,
                QualitySignalType.HISTORICAL_SECURITY_RISK,
                value.historical_security_risk,
            )

        if value.historical_abuse_risk > 0.0:
            add(
                QualitySignalFamily.HISTORY,
                QualitySignalType.HISTORICAL_ABUSE_RISK,
                value.historical_abuse_risk,
            )

        # --------------------------------------------------------------------
        # Temporal
        # --------------------------------------------------------------------

        if value.recent_risk_spike:
            add(
                QualitySignalFamily.TEMPORAL,
                QualitySignalType.TEMPORAL_RISK_SPIKE,
                max(value.temporal_risk, 0.75),
            )

        if value.rapid_risk_escalation:
            add(
                QualitySignalFamily.TEMPORAL,
                QualitySignalType.RAPID_RISK_ESCALATION,
                max(value.temporal_risk, 0.80),
            )

        if value.temporal_risk > 0.0:
            add(
                QualitySignalFamily.TEMPORAL,
                QualitySignalType.TEMPORAL_RISK_SPIKE,
                value.temporal_risk,
            )

        # --------------------------------------------------------------------
        # Distributed
        # --------------------------------------------------------------------

        if value.cross_resource_risk > 0.0:
            add(
                QualitySignalFamily.CROSS_RESOURCE,
                QualitySignalType.CROSS_RESOURCE_RISK,
                value.cross_resource_risk,
                metadata={
                    "affected_resource_count":
                        value.affected_resource_count
                },
            )

        if value.cross_domain_risk > 0.0:
            add(
                QualitySignalFamily.CROSS_DOMAIN,
                QualitySignalType.CROSS_DOMAIN_RISK,
                value.cross_domain_risk,
                metadata={
                    "affected_domain_count":
                        value.affected_domain_count
                },
            )

        if value.infrastructure_risk > 0.0:
            add(
                QualitySignalFamily.INFRASTRUCTURE,
                QualitySignalType.INFRASTRUCTURE_REUSE_RISK,
                value.infrastructure_risk,
                metadata={
                    "infrastructure_reuse_count":
                        value.infrastructure_reuse_count
                },
            )

        # --------------------------------------------------------------------
        # Scope
        # --------------------------------------------------------------------

        scope_values = [
            (
                QualitySignalFamily.RESOURCE,
                QualitySignalType.RESOURCE_RISK,
                value.resource_risk,
            ),
            (
                QualitySignalFamily.CLUSTER,
                QualitySignalType.CLUSTER_RISK,
                value.cluster_risk,
            ),
            (
                QualitySignalFamily.PARTITION,
                QualitySignalType.PARTITION_RISK,
                value.partition_risk,
            ),
            (
                QualitySignalFamily.SHARD,
                QualitySignalType.SHARD_RISK,
                value.shard_risk,
            ),
            (
                QualitySignalFamily.REGION,
                QualitySignalType.REGION_RISK,
                value.region_risk,
            ),
            (
                QualitySignalFamily.GLOBAL,
                QualitySignalType.GLOBAL_RISK,
                value.global_risk,
            ),
        ]

        for family, signal_type, score in scope_values:
            if score > 0.0:
                add(
                    family,
                    signal_type,
                    score,
                )

        # --------------------------------------------------------------------
        # Sources
        # --------------------------------------------------------------------

        if value.conflicting_source_count > 0:
            conflict_ratio = (
                value.conflicting_source_count
                / max(1, value.source_count)
            )

            add(
                QualitySignalFamily.SOURCE,
                QualitySignalType.SOURCE_CONFLICT,
                self._clamp(conflict_ratio),
            )

        if value.source_reliability < 0.50:
            add(
                QualitySignalFamily.SOURCE,
                QualitySignalType.LOW_SOURCE_RELIABILITY,
                1.0 - value.source_reliability,
            )

        # --------------------------------------------------------------------
        # Technical/policy
        # --------------------------------------------------------------------

        if value.technical_risk > 0.0:
            add(
                QualitySignalFamily.TECHNICAL,
                QualitySignalType.TECHNICAL_RISK,
                value.technical_risk,
            )

        if value.policy_risk > 0.0:
            add(
                QualitySignalFamily.POLICY,
                QualitySignalType.POLICY_RISK,
                value.policy_risk,
            )

        return self._deduplicate_signals(signals)

    # ========================================================================
    # DISTRIBUTED CORRELATION
    # ========================================================================

    def correlate_signals(
        self,
        value: GlobalQualityInput,
        signals: Sequence[GlobalQualitySignal],
    ) -> List[GlobalQualitySignal]:
        result = list(signals)

        families = {
            signal.family.value
            for signal in signals
        }

        if len(families) >= 4:
            result.append(
                self._make_signal(
                    value,
                    QualitySignalFamily.GLOBAL,
                    QualitySignalType.GLOBAL_RISK,
                    min(
                        1.0,
                        0.40 + 0.10 * len(families),
                    ),
                    confidence=min(
                        1.0,
                        0.50 + 0.08 * len(families),
                    ),
                    metadata={
                        "independent_signal_families":
                            len(families)
                    },
                )
            )

        if (
            value.affected_resource_count
            >= self.policy.minimum_cluster_size
        ):
            result.append(
                self._make_signal(
                    value,
                    QualitySignalFamily.CROSS_RESOURCE,
                    QualitySignalType.CROSS_RESOURCE_RISK,
                    self._cluster_score(
                        value.affected_resource_count
                    ),
                    metadata={
                        "affected_resource_count":
                            value.affected_resource_count
                    },
                )
            )

        if (
            value.affected_domain_count
            >= self.policy.minimum_cluster_size
        ):
            result.append(
                self._make_signal(
                    value,
                    QualitySignalFamily.CROSS_DOMAIN,
                    QualitySignalType.CROSS_DOMAIN_RISK,
                    self._cluster_score(
                        value.affected_domain_count
                    ),
                    metadata={
                        "affected_domain_count":
                            value.affected_domain_count
                    },
                )
            )

        if (
            value.infrastructure_reuse_count
            >= self.policy.minimum_cluster_size
        ):
            result.append(
                self._make_signal(
                    value,
                    QualitySignalFamily.INFRASTRUCTURE,
                    QualitySignalType.INFRASTRUCTURE_REUSE_RISK,
                    self._cluster_score(
                        value.infrastructure_reuse_count
                    ),
                    metadata={
                        "infrastructure_reuse_count":
                            value.infrastructure_reuse_count
                    },
                )
            )

        self._event(
            value.identity.resource_id,
            QualityEventType.DISTRIBUTED_CORRELATION,
            GlobalQualityState.CORRELATION,
            {
                "input_signal_count": len(signals),
                "correlated_signal_count": len(result),
                "family_count": len(families),
            },
        )

        return self._deduplicate_signals(result)

    # ========================================================================
    # DEDUPLICATION
    # ========================================================================

    def _deduplicate_signals(
        self,
        signals: Sequence[GlobalQualitySignal],
    ) -> List[GlobalQualitySignal]:
        result: Dict[str, GlobalQualitySignal] = {}

        for signal in signals:
            existing = result.get(signal.signal_id)

            if existing is None:
                result[signal.signal_id] = signal
                continue

            if signal.confidence > existing.confidence:
                result[signal.signal_id] = signal

            elif (
                signal.confidence == existing.confidence
                and signal.value > existing.value
            ):
                result[signal.signal_id] = signal

        return list(result.values())[
            : self.policy.max_signals_per_resource
        ]

    # ========================================================================
    # CLUSTER SCORE
    # ========================================================================

    def _cluster_score(
        self,
        count: int,
    ) -> float:
        count = self._safe_int(count)

        if count <= 0:
            return 0.0

        if count >= self.policy.very_large_cluster_size:
            return 0.95

        if count >= self.policy.large_cluster_size:
            return 0.85

        if count >= self.policy.minimum_cluster_size:
            return 0.65

        return min(
            0.60,
            0.25 + 0.05 * count,
        )

    # ========================================================================
    # RISK / TRUST / CONFIDENCE
    # ========================================================================

    def calculate_risk(
        self,
        signals: Sequence[GlobalQualitySignal],
    ) -> float:
        if not signals:
            return 0.0

        product = 1.0

        for signal in signals:
            contribution = self._clamp(
                signal.value * signal.confidence
            )

            product *= 1.0 - contribution

        return self._clamp(1.0 - product)

    def calculate_trust(
        self,
        value: GlobalQualityInput,
        risk: float,
        history: Optional[GlobalQualityHistory] = None,
    ) -> float:
        trust = 1.0 - self._clamp(risk)

        source_quality = (
            0.50 * value.source_confidence
            + 0.50 * value.source_reliability
        )

        trust = (
            0.75 * trust
            + 0.25 * source_quality
        )

        if history is not None:
            if history.previous_risk_score > 0.0:
                trust -= 0.10 * self._clamp(
                    history.previous_risk_score
                )

            if history.previous_quarantine_count > 0:
                trust -= min(
                    0.15,
                    0.02 * history.previous_quarantine_count,
                )

        return self._clamp(trust)

    def calculate_confidence(
        self,
        value: GlobalQualityInput,
        signals: Sequence[GlobalQualitySignal],
    ) -> float:
        if not signals:
            return self._clamp(
                0.50 * value.source_confidence
                + 0.50 * value.source_reliability
            )

        signal_confidence = (
            sum(
                signal.confidence
                for signal in signals
            )
            / max(1, len(signals))
        )

        source_count = max(
            value.source_count,
            value.trusted_source_count,
        )

        corroboration = min(
            1.0,
            source_count
            / max(
                1,
                self.policy.minimum_sources_for_strong_evidence,
            ),
        )

        conflict_penalty = min(
            0.35,
            value.conflicting_source_count * 0.05,
        )

        completeness = 1.0

        if value.partial:
            completeness -= 0.25

        if value.missing_fields:
            completeness -= min(
                0.25,
                len(value.missing_fields) * 0.02,
            )

        confidence = (
            0.45 * signal_confidence
            + 0.25 * value.source_confidence
            + 0.15 * value.source_reliability
            + 0.15 * corroboration
        )

        confidence *= self._clamp(completeness)
        confidence -= conflict_penalty

        return self._clamp(confidence)

    # ========================================================================
    # BANDS
    # ========================================================================

    def risk_band(
        self,
        risk: float,
    ) -> QualityRiskBand:
        risk = self._clamp(risk)

        if risk < self.policy.low_risk_threshold:
            return QualityRiskBand.NONE

        if risk < self.policy.moderate_risk_threshold:
            return QualityRiskBand.LOW

        if risk < self.policy.high_risk_threshold:
            return QualityRiskBand.MODERATE

        if risk < self.policy.very_high_risk_threshold:
            return QualityRiskBand.HIGH

        if risk < self.policy.critical_risk_threshold:
            return QualityRiskBand.VERY_HIGH

        return QualityRiskBand.CRITICAL

    def trust_band(
        self,
        trust: float,
    ) -> QualityTrustBand:
        trust = self._clamp(trust)

        if trust < self.policy.very_low_trust_threshold:
            return QualityTrustBand.VERY_LOW

        if trust < self.policy.low_trust_threshold:
            return QualityTrustBand.LOW

        if trust < self.policy.moderate_trust_threshold:
            return QualityTrustBand.MODERATE

        if trust < self.policy.high_trust_threshold:
            return QualityTrustBand.HIGH

        return QualityTrustBand.VERY_HIGH

    def evidence_strength(
        self,
        value: GlobalQualityInput,
        confidence: float,
    ) -> QualityEvidenceStrength:
        source_count = max(
            value.source_count,
            value.trusted_source_count,
        )

        if (
            source_count
            >= self.policy.minimum_sources_for_very_strong_evidence
        ):
            return QualityEvidenceStrength.VERY_STRONG

        if (
            source_count
            >= self.policy.minimum_sources_for_strong_evidence
        ):
            return QualityEvidenceStrength.STRONG

        return self._strength_from_confidence(
            confidence
        )

    # ========================================================================
    # SCOPE SELECTION
    # ========================================================================

    def select_scope(
        self,
        value: GlobalQualityInput,
        risk: float,
        confidence: float,
    ) -> QualityControlScope:
        risk = self._clamp(risk)
        confidence = self._clamp(confidence)

        if (
            value.global_risk >= self.policy.global_action_threshold
            and confidence >= self.policy.critical_quarantine_confidence_threshold
            and value.source_count
            >= self.policy.minimum_sources_for_large_scope
        ):
            return QualityControlScope.GLOBAL

        if (
            value.region_risk
            >= self.policy.regional_action_threshold
            and confidence
            >= self.policy.quarantine_confidence_threshold
        ):
            return QualityControlScope.REGION

        if (
            value.shard_risk
            >= self.policy.high_risk_threshold
        ):
            return QualityControlScope.SHARD

        if (
            value.partition_risk
            >= self.policy.high_risk_threshold
        ):
            return QualityControlScope.PARTITION

        if (
            value.cluster_risk
            >= self.policy.high_risk_threshold
        ):
            return QualityControlScope.CLUSTER

        if value.identity.domain_id:
            if value.cross_domain_risk >= self.policy.high_risk_threshold:
                return QualityControlScope.DOMAIN

        if value.identity.host_id:
            if value.resource_risk >= self.policy.high_risk_threshold:
                return QualityControlScope.HOST

        if value.identity.document_id:
            return QualityControlScope.DOCUMENT

        return QualityControlScope.RESOURCE

    # ========================================================================
    # QUALITY CONTROL ACTION
    # ========================================================================

    def choose_action(
        self,
        value: GlobalQualityInput,
        risk: float,
        confidence: float,
        scope: QualityControlScope,
        history: Optional[GlobalQualityHistory] = None,
    ) -> QualityControlAction:
        risk = self._clamp(risk)
        confidence = self._clamp(confidence)

        if confidence < self.policy.minimum_confidence:
            return QualityControlAction.REQUIRE_REVIEW

        if value.partial and not self.policy.allow_partial:
            return QualityControlAction.REQUIRE_REVIEW

        # Recovery gets precedence when current risk has fallen sufficiently.
        if history is not None:
            if (
                history.previous_quarantine_count > 0
                and risk < self.policy.moderate_risk_threshold
                and confidence >= self.policy.recovery_min_confidence
            ):
                return QualityControlAction.RECOVERY_REVIEW

        if risk >= self.policy.critical_risk_threshold:
            if scope == QualityControlScope.GLOBAL:
                return QualityControlAction.REQUIRE_REVIEW

            if scope == QualityControlScope.REGION:
                return QualityControlAction.REQUIRE_REVIEW

            if scope == QualityControlScope.SHARD:
                return QualityControlAction.QUARANTINE_SHARD

            if scope == QualityControlScope.PARTITION:
                return QualityControlAction.QUARANTINE_PARTITION

            if scope == QualityControlScope.CLUSTER:
                return QualityControlAction.QUARANTINE_CLUSTER

            if scope == QualityControlScope.DOMAIN:
                return QualityControlAction.QUARANTINE_CLUSTER

            if scope == QualityControlScope.DOCUMENT:
                return QualityControlAction.QUARANTINE_DOCUMENT

            return QualityControlAction.QUARANTINE_RESOURCE

        if risk >= self.policy.very_high_risk_threshold:
            if scope == QualityControlScope.SHARD:
                return QualityControlAction.QUARANTINE_SHARD

            if scope == QualityControlScope.PARTITION:
                return QualityControlAction.QUARANTINE_PARTITION

            if scope == QualityControlScope.CLUSTER:
                return QualityControlAction.QUARANTINE_CLUSTER

            if scope == QualityControlScope.REGION:
                return QualityControlAction.REQUIRE_REVIEW

            return QualityControlAction.QUARANTINE_RESOURCE

        if risk >= self.policy.high_risk_threshold:
            return QualityControlAction.REQUIRE_REVIEW

        if risk >= self.policy.moderate_risk_threshold:
            return QualityControlAction.REDUCE_PROCESSING_PRIORITY

        if risk >= self.policy.low_risk_threshold:
            return QualityControlAction.MONITOR

        return QualityControlAction.NONE

    # ========================================================================
    # QUALITY CONTROL PLAN
    # ========================================================================

    def build_quality_control_plan(
        self,
        value: GlobalQualityInput,
        risk: float,
        confidence: float,
        scope: QualityControlScope,
        action: QualityControlAction,
        signals: Sequence[GlobalQualitySignal],
    ) -> QualityControlPlan:
        reason_codes = tuple(
            signal.signal_type.value
            for signal in sorted(
                signals,
                key=lambda item: (
                    item.value * item.confidence,
                    item.signal_type.value,
                ),
                reverse=True,
            )[:12]
        )

        requires_multiple_sources = (
            scope
            in {
                QualityControlScope.REGION,
                QualityControlScope.GLOBAL,
            }
        )

        requires_review = (
            action
            in {
                QualityControlAction.REQUIRE_REVIEW,
                QualityControlAction.QUARANTINE_RESOURCE,
                QualityControlAction.QUARANTINE_DOCUMENT,
                QualityControlAction.QUARANTINE_CLUSTER,
                QualityControlAction.QUARANTINE_PARTITION,
                QualityControlAction.QUARANTINE_SHARD,
                QualityControlAction.QUARANTINE_REGION,
                QualityControlAction.RECOVERY_REVIEW,
                QualityControlAction.REHABILITATION_REVIEW,
            }
        )

        plan = QualityControlPlan(
            plan_id=self._plan_id(
                value.identity.resource_id,
                action,
                scope,
                risk,
                confidence,
            ),
            resource_id=value.identity.resource_id,
            action=action,
            scope=scope,
            confidence=confidence,
            risk_score=risk,
            reason_codes=reason_codes,
            requires_review=requires_review,
            requires_multiple_sources=requires_multiple_sources,
            idempotency_key=self._idempotency_key(
                value.identity.resource_id,
                action.value,
                scope.value,
            ),
            timestamp=self._now(),
            metadata={
                "architecture_version":
                    ARCHITECTURE_VERSION,
                "phase": PHASE,
            },
        )

        self._event(
            value.identity.resource_id,
            QualityEventType.QUALITY_CONTROL_PLANNED,
            GlobalQualityState.QUALITY_CONTROL_PLANNING,
            {
                "action": action.value,
                "scope": scope.value,
                "risk": risk,
                "confidence": confidence,
            },
        )

        return plan

    # ========================================================================
    # QUARANTINE
    # ========================================================================

    def build_quarantine_plan(
        self,
        value: GlobalQualityInput,
        risk: float,
        confidence: float,
        scope: QualityControlScope,
        action: QualityControlAction,
        signals: Sequence[GlobalQualitySignal],
    ) -> Optional[QuarantinePlan]:
        quarantine_actions = {
            QualityControlAction.QUARANTINE_RESOURCE,
            QualityControlAction.QUARANTINE_DOCUMENT,
            QualityControlAction.QUARANTINE_CLUSTER,
            QualityControlAction.QUARANTINE_PARTITION,
            QualityControlAction.QUARANTINE_SHARD,
            QualityControlAction.QUARANTINE_REGION,
        }

        if action not in quarantine_actions:
            return None

        requires_multiple_sources = (
            value.source_count
            < self.policy.minimum_sources_for_large_scope
        )

        if (
            self.policy.require_high_confidence_for_quarantine
            and confidence
            < self.policy.quarantine_confidence_threshold
        ):
            return QuarantinePlan(
                quarantine_id=self._quarantine_id(
                    value.identity.resource_id,
                    scope,
                    risk,
                ),
                resource_id=value.identity.resource_id,
                state=QuarantineState.PROPOSED,
                scope=scope,
                action=QualityControlAction.REQUIRE_REVIEW,
                risk_score=risk,
                confidence=confidence,
                reason_codes=(
                    "insufficient_quarantine_confidence",
                ),
                requires_review=True,
                reversible=True,
                idempotency_key=self._idempotency_key(
                    value.identity.resource_id,
                    "quarantine_review",
                    scope.value,
                ),
                timestamp=self._now(),
                metadata={
                    "original_action": action.value,
                },
            )

        if (
            scope
            in {
                QualityControlScope.REGION,
                QualityControlScope.GLOBAL,
            }
            and requires_multiple_sources
        ):
            return QuarantinePlan(
                quarantine_id=self._quarantine_id(
                    value.identity.resource_id,
                    scope,
                    risk,
                ),
                resource_id=value.identity.resource_id,
                state=QuarantineState.PROPOSED,
                scope=scope,
                action=QualityControlAction.REQUIRE_REVIEW,
                risk_score=risk,
                confidence=confidence,
                reason_codes=(
                    "insufficient_large_scope_source_corroboration",
                ),
                requires_review=True,
                reversible=True,
                idempotency_key=self._idempotency_key(
                    value.identity.resource_id,
                    "large_scope_review",
                    scope.value,
                ),
                timestamp=self._now(),
                metadata={
                    "source_count":
                        value.source_count,
                },
            )

        reason_codes = tuple(
            signal.signal_type.value
            for signal in sorted(
                signals,
                key=lambda item: (
                    item.value * item.confidence,
                    item.signal_type.value,
                ),
                reverse=True,
            )[:10]
        )

        plan = QuarantinePlan(
            quarantine_id=self._quarantine_id(
                value.identity.resource_id,
                scope,
                risk,
            ),
            resource_id=value.identity.resource_id,
            state=QuarantineState.READY,
            scope=scope,
            action=action,
            risk_score=risk,
            confidence=confidence,
            reason_codes=reason_codes,
            requires_review=True,
            reversible=True,
            idempotency_key=self._idempotency_key(
                value.identity.resource_id,
                action.value,
                scope.value,
            ),
            timestamp=self._now(),
            metadata={
                "execution_boundary":
                    "downstream_control_plane",
                "direct_enforcement":
                    False,
            },
        )

        self._event(
            value.identity.resource_id,
            QualityEventType.QUARANTINE_PLANNED,
            GlobalQualityState.QUARANTINE_PLANNING,
            {
                "action": action.value,
                "scope": scope.value,
                "risk": risk,
                "confidence": confidence,
            },
        )

        return plan

    # ========================================================================
    # RECOVERY
    # ========================================================================

    def build_recovery_plan(
        self,
        value: GlobalQualityInput,
        history: Optional[GlobalQualityHistory],
        risk: float,
        confidence: float,
        scope: QualityControlScope,
    ) -> RecoveryPlan:
        history = history or GlobalQualityHistory()

        if history.previous_quarantine_count <= 0:
            return RecoveryPlan(
                recovery_id=self._recovery_id(
                    value.identity.resource_id,
                    RecoveryDecision.NO_RECOVERY,
                    risk,
                ),
                resource_id=value.identity.resource_id,
                state=RecoveryState.NOT_REQUIRED,
                decision=RecoveryDecision.NO_RECOVERY,
                scope=scope,
                confidence=confidence,
                current_risk=risk,
                reason_codes=(
                    "no_previous_quarantine",
                ),
                requires_review=False,
                reversible=True,
                idempotency_key=self._idempotency_key(
                    value.identity.resource_id,
                    "no_recovery",
                    scope.value,
                ),
                timestamp=self._now(),
            )

        if confidence < self.policy.recovery_min_confidence:
            return RecoveryPlan(
                recovery_id=self._recovery_id(
                    value.identity.resource_id,
                    RecoveryDecision.DEFER,
                    risk,
                ),
                resource_id=value.identity.resource_id,
                state=RecoveryState.BLOCKED,
                decision=RecoveryDecision.DEFER,
                scope=scope,
                confidence=confidence,
                current_risk=risk,
                reason_codes=(
                    "insufficient_recovery_confidence",
                ),
                requires_review=True,
                reversible=True,
                idempotency_key=self._idempotency_key(
                    value.identity.resource_id,
                    "defer_recovery",
                    scope.value,
                ),
                timestamp=self._now(),
            )

        if risk >= self.policy.moderate_risk_threshold:
            return RecoveryPlan(
                recovery_id=self._recovery_id(
                    value.identity.resource_id,
                    RecoveryDecision.NO_RECOVERY,
                    risk,
                ),
                resource_id=value.identity.resource_id,
                state=RecoveryState.BLOCKED,
                decision=RecoveryDecision.NO_RECOVERY,
                scope=scope,
                confidence=confidence,
                current_risk=risk,
                reason_codes=(
                    "risk_remains_above_recovery_threshold",
                ),
                requires_review=True,
                reversible=True,
                idempotency_key=self._idempotency_key(
                    value.identity.resource_id,
                    "recovery_blocked",
                    scope.value,
                ),
                timestamp=self._now(),
            )

        if (
            history.rehabilitation_failure_count
            > history.rehabilitation_success_count
        ):
            return RecoveryPlan(
                recovery_id=self._recovery_id(
                    value.identity.resource_id,
                    RecoveryDecision.REHABILITATION_REVIEW,
                    risk,
                ),
                resource_id=value.identity.resource_id,
                state=RecoveryState.UNDER_REVIEW,
                decision=RecoveryDecision.REHABILITATION_REVIEW,
                scope=scope,
                confidence=confidence,
                current_risk=risk,
                reason_codes=(
                    "previous_rehabilitation_failures",
                ),
                requires_review=True,
                reversible=True,
                idempotency_key=self._idempotency_key(
                    value.identity.resource_id,
                    "rehabilitation_review",
                    scope.value,
                ),
                timestamp=self._now(),
            )

        if (
            history.historical_false_positive_count
            > history.historical_false_negative_count
        ):
            decision = RecoveryDecision.REVIEW_RECOVERY
            state = RecoveryState.UNDER_REVIEW
        else:
            decision = RecoveryDecision.REHABILITATION_REVIEW
            state = RecoveryState.REHABILITATION_INTENT

        plan = RecoveryPlan(
            recovery_id=self._recovery_id(
                value.identity.resource_id,
                decision,
                risk,
            ),
            resource_id=value.identity.resource_id,
            state=state,
            decision=decision,
            scope=scope,
            confidence=confidence,
            current_risk=risk,
            reason_codes=(
                "risk_reduced_after_previous_quarantine",
                "recovery_requires_downstream_review",
            ),
            requires_review=True,
            reversible=True,
            idempotency_key=self._idempotency_key(
                value.identity.resource_id,
                decision.value,
                scope.value,
            ),
            timestamp=self._now(),
        )

        self._event(
            value.identity.resource_id,
            QualityEventType.RECOVERY_PLANNED,
            GlobalQualityState.RECOVERY_ANALYSIS,
            {
                "decision": decision.value,
                "scope": scope.value,
                "risk": risk,
                "confidence": confidence,
            },
        )

        return plan

    # ========================================================================
    # DECISION
    # ========================================================================

    def decision(
        self,
        value: GlobalQualityInput,
        risk: float,
        confidence: float,
        action: QualityControlAction,
        recovery: RecoveryPlan,
    ) -> QualityControlDecision:
        if confidence < self.policy.minimum_confidence:
            return QualityControlDecision.PARTIAL_EVIDENCE

        if value.partial and not self.policy.allow_partial:
            return QualityControlDecision.DEFERRED

        if recovery.decision in {
            RecoveryDecision.RESTORE,
            RecoveryDecision.REVIEW_RECOVERY,
        }:
            return QualityControlDecision.RECOVERY_REQUIRED

        if recovery.decision == RecoveryDecision.REHABILITATION_REVIEW:
            return QualityControlDecision.REHABILITATION_REQUIRED

        if action in {
            QualityControlAction.QUARANTINE_RESOURCE,
            QualityControlAction.QUARANTINE_DOCUMENT,
            QualityControlAction.QUARANTINE_CLUSTER,
            QualityControlAction.QUARANTINE_PARTITION,
            QualityControlAction.QUARANTINE_SHARD,
            QualityControlAction.QUARANTINE_REGION,
        }:
            return QualityControlDecision.QUARANTINE_REQUIRED

        if action == QualityControlAction.REQUIRE_REVIEW:
            return QualityControlDecision.REVIEW_REQUIRED

        if action in {
            QualityControlAction.REDUCE_PROCESSING_PRIORITY,
            QualityControlAction.DEFER_PROCESSING,
        }:
            return QualityControlDecision.PROTECTION_REQUIRED

        if action == QualityControlAction.MONITOR:
            return QualityControlDecision.MONITOR

        return QualityControlDecision.NO_ACTION

    # ========================================================================
    # REASONS
    # ========================================================================

    def build_reasons(
        self,
        value: GlobalQualityInput,
        signals: Sequence[GlobalQualitySignal],
        risk_band: QualityRiskBand,
        trust_band: QualityTrustBand,
        decision: QualityControlDecision,
    ) -> List[str]:
        reasons: List[str] = []

        high_value_signals = sorted(
            signals,
            key=lambda signal: (
                signal.value * signal.confidence,
                signal.signal_type.value,
            ),
            reverse=True,
        )

        for signal in high_value_signals[:10]:
            reasons.append(
                f"{signal.signal_type.value}={signal.value:.3f}"
                f"/confidence={signal.confidence:.3f}"
            )

        if value.partial:
            reasons.append(
                "Analysis contains partial evidence."
            )

        if value.conflicting_source_count > 0:
            reasons.append(
                "Conflicting external evidence sources were supplied."
            )

        reasons.append(
            f"Risk band: {risk_band.value}."
        )

        reasons.append(
            f"Trust band: {trust_band.value}."
        )

        reasons.append(
            f"Global quality-control decision: {decision.value}."
        )

        return reasons

    # ========================================================================
    # ANALYZE
    # ========================================================================

    def analyze(
        self,
        value: GlobalQualityInput,
        history: Optional[GlobalQualityHistory] = None,
    ) -> GlobalQualityResult:
        resource_id = (
            value.identity.resource_id
            if isinstance(value, GlobalQualityInput)
            else ""
        )

        self._event(
            resource_id,
            QualityEventType.ANALYSIS_STARTED,
            GlobalQualityState.RECEIVED,
        )

        valid, errors = self.validate_input(value)

        if not valid:
            self._event(
                resource_id,
                QualityEventType.ANALYSIS_REJECTED,
                GlobalQualityState.REJECTED,
                {"errors": errors},
            )

            return GlobalQualityResult(
                resource_id=resource_id,
                state=GlobalQualityState.REJECTED,
                decision=QualityControlDecision.REJECTED_INPUT,
                risk_score=0.0,
                trust_score=0.0,
                confidence=0.0,
                risk_band=QualityRiskBand.UNKNOWN,
                trust_band=QualityTrustBand.UNKNOWN,
                evidence_strength=QualityEvidenceStrength.NONE,
                quarantine_state=QuarantineState.NOT_REQUIRED,
                recovery_state=RecoveryState.NOT_REQUIRED,
                reasons=errors,
                timestamp=self._now(),
            )

        normalized = self.normalize_input(value)

        self._event(
            normalized.identity.resource_id,
            QualityEventType.INPUT_ACCEPTED,
            GlobalQualityState.VALIDATING,
        )

        self._event(
            normalized.identity.resource_id,
            QualityEventType.INPUT_NORMALIZED,
            GlobalQualityState.NORMALIZING,
        )

        self._checkpoint(
            normalized.identity.resource_id,
            QualityCheckpointType.NORMALIZATION_COMPLETE,
            GlobalQualityState.NORMALIZING,
        )

        signals = self.extract_signals(normalized)

        self._checkpoint(
            normalized.identity.resource_id,
            QualityCheckpointType.SIGNAL_EXTRACTION_COMPLETE,
            GlobalQualityState.QUALITY_ANALYSIS,
            signal_count=len(signals),
        )

        signals = self.correlate_signals(
            normalized,
            signals,
        )

        self._checkpoint(
            normalized.identity.resource_id,
            QualityCheckpointType.CORRELATION_COMPLETE,
            GlobalQualityState.CORRELATION,
            signal_count=len(signals),
        )

        risk = self.calculate_risk(signals)

        self._event(
            normalized.identity.resource_id,
            QualityEventType.RISK_CALCULATED,
            GlobalQualityState.RISK_CALCULATION,
            {"risk_score": risk},
        )

        self._checkpoint(
            normalized.identity.resource_id,
            QualityCheckpointType.RISK_COMPLETE,
            GlobalQualityState.RISK_CALCULATION,
            signal_count=len(signals),
        )

        trust = self.calculate_trust(
            normalized,
            risk,
            history,
        )

        self._event(
            normalized.identity.resource_id,
            QualityEventType.TRUST_CALCULATED,
            GlobalQualityState.TRUST_ANALYSIS,
            {"trust_score": trust},
        )

        self._checkpoint(
            normalized.identity.resource_id,
            QualityCheckpointType.TRUST_COMPLETE,
            GlobalQualityState.TRUST_ANALYSIS,
            signal_count=len(signals),
        )

        confidence = self.calculate_confidence(
            normalized,
            signals,
        )

        self._event(
            normalized.identity.resource_id,
            QualityEventType.CONFIDENCE_CALCULATED,
            GlobalQualityState.CONFIDENCE_CALCULATION,
            {"confidence": confidence},
        )

        self._checkpoint(
            normalized.identity.resource_id,
            QualityCheckpointType.CONFIDENCE_COMPLETE,
            GlobalQualityState.CONFIDENCE_CALCULATION,
            signal_count=len(signals),
        )

        risk_band = self.risk_band(risk)
        trust_band = self.trust_band(trust)

        evidence_strength = self.evidence_strength(
            normalized,
            confidence,
        )

        scope = self.select_scope(
            normalized,
            risk,
            confidence,
        )

        action = self.choose_action(
            normalized,
            risk,
            confidence,
            scope,
            history,
        )

        quality_control_plan = self.build_quality_control_plan(
            normalized,
            risk,
            confidence,
            scope,
            action,
            signals,
        )

        quarantine_plan = self.build_quarantine_plan(
            normalized,
            risk,
            confidence,
            scope,
            action,
            signals,
        )

        recovery_plan = self.build_recovery_plan(
            normalized,
            history,
            risk,
            confidence,
            scope,
        )

        decision = self.decision(
            normalized,
            risk,
            confidence,
            action,
            recovery_plan,
        )

        evidence_grouped: Dict[
            EvidenceKind,
            List[GlobalQualitySignal],
        ] = {}

        family_to_kind = {
            QualitySignalFamily.CONTENT:
                EvidenceKind.QUALITY,
            QualitySignalFamily.QUALITY:
                EvidenceKind.QUALITY,
            QualitySignalFamily.MANIPULATION:
                EvidenceKind.MANIPULATION,
            QualitySignalFamily.SECURITY:
                EvidenceKind.SECURITY,
            QualitySignalFamily.TRUST:
                EvidenceKind.TRUST,
            QualitySignalFamily.SAFE_BROWSING:
                EvidenceKind.SAFE_BROWSING,
            QualitySignalFamily.SPAM:
                EvidenceKind.SPAM,
            QualitySignalFamily.ABUSE:
                EvidenceKind.ABUSE,
            QualitySignalFamily.MALWARE:
                EvidenceKind.MALWARE,
            QualitySignalFamily.PHISHING:
                EvidenceKind.PHISHING,
            QualitySignalFamily.HARMFUL:
                EvidenceKind.HARMFUL,
            QualitySignalFamily.REPUTATION:
                EvidenceKind.REPUTATION,
            QualitySignalFamily.HISTORY:
                EvidenceKind.HISTORY,
            QualitySignalFamily.TEMPORAL:
                EvidenceKind.TEMPORAL,
            QualitySignalFamily.CROSS_RESOURCE:
                EvidenceKind.CROSS_RESOURCE,
            QualitySignalFamily.CROSS_DOMAIN:
                EvidenceKind.CROSS_DOMAIN,
            QualitySignalFamily.INFRASTRUCTURE:
                EvidenceKind.INFRASTRUCTURE,
            QualitySignalFamily.RESOURCE:
                EvidenceKind.RESOURCE,
            QualitySignalFamily.CLUSTER:
                EvidenceKind.CLUSTER,
            QualitySignalFamily.PARTITION:
                EvidenceKind.PARTITION,
            QualitySignalFamily.SHARD:
                EvidenceKind.SHARD,
            QualitySignalFamily.REGION:
                EvidenceKind.REGION,
            QualitySignalFamily.GLOBAL:
                EvidenceKind.GLOBAL,
            QualitySignalFamily.SOURCE:
                EvidenceKind.SOURCE,
            QualitySignalFamily.POLICY:
                EvidenceKind.POLICY,
            QualitySignalFamily.TECHNICAL:
                EvidenceKind.TECHNICAL,
        }

        for signal in signals:
            kind = family_to_kind.get(
                signal.family,
                EvidenceKind.TECHNICAL,
            )

            evidence_grouped.setdefault(
                kind,
                [],
            ).append(signal)

        evidence: List[GlobalQualityEvidence] = []

        for kind, grouped_signals in evidence_grouped.items():
            signal_ids = tuple(
                sorted(
                    signal.signal_id
                    for signal in grouped_signals
                )
            )

            group_confidence = self._clamp(
                sum(
                    signal.confidence
                    for signal in grouped_signals
                )
                / max(
                    1,
                    len(grouped_signals),
                )
            )

            evidence_item = GlobalQualityEvidence(
                evidence_id=self._evidence_id(
                    normalized.identity.resource_id,
                    kind,
                    signal_ids,
                ),
                resource_id=normalized.identity.resource_id,
                kind=kind,
                confidence=group_confidence,
                strength=self._strength_from_confidence(
                    group_confidence
                ),
                signal_ids=signal_ids,
                source="phase14.8",
                timestamp=normalized.timestamp,
                provenance={
                    "architecture_version":
                        ARCHITECTURE_VERSION,
                    "phase": PHASE,
                    "previous_stage":
                        PREVIOUS_STAGE,
                    "source_evidence_ids":
                        list(normalized.evidence_ids),
                },
                metadata={
                    "signal_count":
                        len(grouped_signals),
                },
            )

            self.backend.save_evidence(evidence_item)
            evidence.append(evidence_item)

        self._checkpoint(
            normalized.identity.resource_id,
            QualityCheckpointType.QUALITY_CONTROL_COMPLETE,
            GlobalQualityState.QUALITY_CONTROL_PLANNING,
            signal_count=len(signals),
            evidence_count=len(evidence),
        )

        quarantine_state = (
            quarantine_plan.state
            if quarantine_plan is not None
            else QuarantineState.NOT_REQUIRED
        )

        recovery_state = recovery_plan.state

        partial = bool(
            normalized.partial
            or normalized.missing_fields
            or confidence < self.policy.minimum_confidence
        )

        state = (
            GlobalQualityState.PARTIAL
            if partial
            else GlobalQualityState.COMPLETED
        )

        reasons = self.build_reasons(
            normalized,
            signals,
            risk_band,
            trust_band,
            decision,
        )

        result = GlobalQualityResult(
            resource_id=normalized.identity.resource_id,
            state=state,
            decision=decision,
            risk_score=risk,
            trust_score=trust,
            confidence=confidence,
            risk_band=risk_band,
            trust_band=trust_band,
            evidence_strength=evidence_strength,
            quarantine_state=quarantine_state,
            recovery_state=recovery_state,
            signals=list(signals),
            evidence=evidence,
            quality_control_plan=quality_control_plan,
            quarantine_plan=quarantine_plan,
            recovery_plan=recovery_plan,
            reasons=reasons,
            partial=partial,
            missing_fields=list(
                normalized.missing_fields
            ),
            lineage=normalized.lineage,
            timestamp=self._now(),
            metadata={
                "architecture_version":
                    ARCHITECTURE_VERSION,
                "phase": PHASE,
                "previous_stage":
                    PREVIOUS_STAGE,
                "next_stage":
                    NEXT_STAGE,
                "scale_target":
                    SCALE_TARGET,
                "google_scale_capability_target":
                    GOOGLE_SCALE_CAPABILITY_TARGET,
                "google_technology_dependency":
                    GOOGLE_TECHNOLOGY_DEPENDENCY,
                "scope":
                    scope.value,
                "action":
                    action.value,
                "signal_count":
                    len(signals),
                "evidence_count":
                    len(evidence),
                "direct_enforcement":
                    False,
            },
        )

        self.backend.save_result(result)

        self._event(
            normalized.identity.resource_id,
            (
                QualityEventType.ANALYSIS_PARTIAL
                if partial
                else QualityEventType.ANALYSIS_COMPLETED
            ),
            state,
            {
                "decision": decision.value,
                "risk_score": risk,
                "trust_score": trust,
                "confidence": confidence,
                "scope": scope.value,
                "action": action.value,
            },
        )

        self._checkpoint(
            normalized.identity.resource_id,
            QualityCheckpointType.QUARANTINE_COMPLETE,
            state,
            signal_count=len(signals),
            evidence_count=len(evidence),
        )

        self._checkpoint(
            normalized.identity.resource_id,
            QualityCheckpointType.RECOVERY_COMPLETE,
            state,
            signal_count=len(signals),
            evidence_count=len(evidence),
        )

        self._checkpoint(
            normalized.identity.resource_id,
            QualityCheckpointType.RESULT_COMPLETE,
            state,
            signal_count=len(signals),
            evidence_count=len(evidence),
        )

        return result

    # ========================================================================
    # BATCH
    # ========================================================================

    def analyze_many(
        self,
        values: Iterable[GlobalQualityInput],
        histories: Optional[
            Mapping[str, GlobalQualityHistory]
        ] = None,
    ) -> List[GlobalQualityResult]:
        items = list(values)

        if len(items) > 100_000:
            raise ValueError(
                "batch exceeds the per-request architecture safety limit"
            )

        history_map = histories or {}

        results: List[GlobalQualityResult] = []

        for value in items:
            results.append(
                self.analyze(
                    value,
                    history_map.get(
                        value.identity.resource_id
                    ),
                )
            )

        return results

    # ========================================================================
    # RESULT
    # ========================================================================

    def get_result(
        self,
        resource_id: str,
    ) -> Optional[GlobalQualityResult]:
        return self.backend.get_result(resource_id)

    def result_to_dict(
        self,
        result: GlobalQualityResult,
    ) -> Dict[str, Any]:
        return asdict(result)

    # ========================================================================
    # ARCHITECTURE METADATA
    # ========================================================================

    def architecture(self) -> Dict[str, Any]:
        return {
            "architecture":
                "GlobalQualityControlQuarantineRecoveryArchitecture",

            "architecture_version":
                ARCHITECTURE_VERSION,

            "phase":
                PHASE,

            "phase_name":
                PHASE_NAME,

            "previous_stage":
                PREVIOUS_STAGE,

            "next_stage":
                NEXT_STAGE,

            "next_stage_name":
                NEXT_STAGE_NAME,

            "scale_target":
                SCALE_TARGET,

            "google_scale_capability_target":
                GOOGLE_SCALE_CAPABILITY_TARGET,

            "google_technology_dependency":
                GOOGLE_TECHNOLOGY_DEPENDENCY,

            "purpose": (
                "Global quality-control, quarantine, recovery, and "
                "rehabilitation control-plane architecture for "
                "enormous-scale public-Web search infrastructure."
            ),

            "integrated_inputs": [
                "spam evidence",
                "content quality evidence",
                "content manipulation evidence",
                "link and graph abuse evidence",
                "malware evidence",
                "phishing evidence",
                "harmful-resource evidence",
                "crawl and index protection evidence",
                "distributed abuse evidence",
                "security trust signals",
                "reputation signals",
                "historical signals",
                "temporal signals",
                "cross-resource signals",
                "infrastructure signals",
                "source reliability signals",
            ],

            "pipeline": [
                "external evidence intake",
                "normalization",
                "quality signal extraction",
                "security signal integration",
                "distributed correlation",
                "risk calculation",
                "trust calculation",
                "confidence calculation",
                "scope selection",
                "global quality-control planning",
                "quarantine planning",
                "recovery analysis",
                "rehabilitation analysis",
                "idempotency control",
                "downstream execution intent",
            ],

            "quarantine_scopes": [
                "resource",
                "document",
                "cluster",
                "partition",
                "shard",
                "region",
                "global",
            ],

            "recovery_scopes": [
                "resource",
                "document",
                "cluster",
                "partition",
                "shard",
                "region",
                "global",
            ],

            "safety_properties": [
                "reversible quarantine intents",
                "review-aware decisions",
                "multi-source safeguards",
                "confidence thresholds",
                "large-scope safeguards",
                "idempotent plans",
                "provenance preservation",
                "lineage preservation",
                "checkpointing",
                "partial-evidence awareness",
                "deterministic architecture",
            ],

            "does_not_execute": [
                "malware",
                "exploits",
                "network scans",
                "HTTP requests",
                "crawler workers",
                "search ranking",
                "search-index mutation",
                "resource deletion",
                "external blocking",
                "external quarantine",
                "external recovery actions",
            ],

            "downstream_stage":
                NEXT_STAGE_NAME,
        }


# ============================================================================
# COMPATIBILITY ALIASES
# ============================================================================


GlobalQualityControlQuarantineRecovery = (
    GlobalQualityControlQuarantineRecoveryArchitecture
)

GlobalQualityControlArchitecture = (
    GlobalQualityControlQuarantineRecoveryArchitecture
)

GlobalQuarantineRecoveryArchitecture = (
    GlobalQualityControlQuarantineRecoveryArchitecture
)

Phase14_8GlobalQualityControlQuarantineRecovery = (
    GlobalQualityControlQuarantineRecoveryArchitecture
)

GlobalQualityQuarantineRecovery = (
    GlobalQualityControlQuarantineRecoveryArchitecture
)


# ============================================================================
# PUBLIC EXPORTS
# ============================================================================


__all__ = [
    # Metadata
    "SCALE_TARGET",
    "GOOGLE_SCALE_CAPABILITY_TARGET",
    "GOOGLE_TECHNOLOGY_DEPENDENCY",
    "ARCHITECTURE_VERSION",
    "PHASE",
    "PREVIOUS_STAGE",
    "NEXT_STAGE",
    "PHASE_NAME",
    "NEXT_STAGE_NAME",

    # Enums
    "GlobalQualityState",
    "QualitySignalFamily",
    "QualitySignalType",
    "QualityEvidenceStrength",
    "QualityRiskBand",
    "QualityTrustBand",
    "QualityControlAction",
    "QualityControlScope",
    "QualityControlDecision",
    "RecoveryState",
    "RecoveryDecision",
    "QuarantineState",
    "EvidenceKind",
    "QualityEventType",
    "QualityCheckpointType",

    # Dataclasses
    "GlobalQualityIdentity",
    "GlobalQualityLineage",
    "GlobalQualityInput",
    "GlobalQualityHistory",
    "GlobalQualitySignal",
    "GlobalQualityEvidence",
    "GlobalQualityPolicy",
    "QualityControlPlan",
    "QuarantinePlan",
    "RecoveryPlan",
    "GlobalQualityCheckpoint",
    "GlobalQualityEvent",
    "GlobalQualityResult",

    # Backend
    "GlobalQualityBackend",
    "InMemoryGlobalQualityBackend",

    # Architecture
    "GlobalQualityControlQuarantineRecoveryArchitecture",
    "GlobalQualityControlQuarantineRecovery",
    "GlobalQualityControlArchitecture",
    "GlobalQuarantineRecoveryArchitecture",
    "Phase14_8GlobalQualityControlQuarantineRecovery",
    "GlobalQualityQuarantineRecovery",
]
