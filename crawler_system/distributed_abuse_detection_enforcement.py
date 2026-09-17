"""
OUR SEARCH
Phase 14.6 — Distributed Abuse Detection & Enforcement

Purpose
-------
Coordinate abuse detection evidence and controlled enforcement decisions
across the distributed OUR SEARCH infrastructure.

This stage consumes structured evidence from earlier security/quality
stages and produces distributed enforcement plans and execution intents.

It is designed directly for:
    billions -> trillions of publicly accessible Web resources

This architecture is:
- distributed
- partition-aware
- shard-aware
- region-aware
- deterministic
- provenance-preserving
- checkpointable
- restart-safe
- idempotency-aware
- failure-aware
- partial-evidence-aware
- temporal
- auditable
- policy-driven
- framework-neutral
- standard-library-only

IMPORTANT SAFETY / ARCHITECTURE BOUNDARY
-----------------------------------------
This module does not perform offensive security operations.

It does NOT:
- execute malware
- execute payloads
- exploit systems
- attack external systems
- perform credential theft
- perform active vulnerability scanning
- perform arbitrary network scanning
- crawl the Web
- issue HTTP requests
- assign crawler workers
- mutate the search index directly
- delete external resources
- compromise websites
- bypass authentication
- generate attack payloads

The enforcement layer operates on internally supplied evidence and emits
controlled enforcement intents for downstream infrastructure.

Enforcement decisions remain policy-controlled, auditable, reversible where
supported, and scoped to OUR SEARCH resources and infrastructure.

Google independence
-------------------
This architecture does not depend on:
- Google Search
- Google's index
- Google's crawler
- Google's ranking systems
- Google's infrastructure
- Google Search APIs
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
# Architecture constants
# ============================================================================

SCALE_TARGET = "billions_to_trillions_of_public_web_resources"
GOOGLE_SCALE_CAPABILITY_TARGET = True
GOOGLE_TECHNOLOGY_DEPENDENCY = False

ARCHITECTURE_VERSION = "distributed-abuse-detection-enforcement.v1"

PHASE = "14.6"
PREVIOUS_STAGE = "14.5"
NEXT_STAGE = "14.7"

PHASE_NAME = "Distributed Abuse Detection & Enforcement"
NEXT_STAGE_NAME = "Security / Trust / Safe Browsing Signals"


# ============================================================================
# Enums
# ============================================================================


class DistributedAbuseState(str, Enum):
    RECEIVED = "received"
    VALIDATING = "validating"
    NORMALIZING = "normalizing"
    EVIDENCE_AGGREGATION = "evidence_aggregation"
    DISTRIBUTION_ANALYSIS = "distribution_analysis"
    PARTITION_ANALYSIS = "partition_analysis"
    SHARD_ANALYSIS = "shard_analysis"
    REGION_ANALYSIS = "region_analysis"
    DOMAIN_ANALYSIS = "domain_analysis"
    HOST_ANALYSIS = "host_analysis"
    RESOURCE_ANALYSIS = "resource_analysis"
    TEMPORAL_ANALYSIS = "temporal_analysis"
    CORRELATION = "correlation"
    ABUSE_RISK_CALCULATION = "abuse_risk_calculation"
    POLICY_EVALUATION = "policy_evaluation"
    ENFORCEMENT_PLANNING = "enforcement_planning"
    ENFORCEMENT_SCOPING = "enforcement_scoping"
    IDEMPOTENCY_CHECK = "idempotency_check"
    CHECKPOINTING = "checkpointing"
    DECISION_READY = "decision_ready"
    COMPLETED = "completed"
    PARTIAL = "partial"
    DEFERRED = "deferred"
    REJECTED = "rejected"
    FAILED = "failed"


class AbuseSignalFamily(str, Enum):
    RESOURCE = "resource"
    URL = "url"
    CRAWL = "crawl"
    INDEX = "index"
    DUPLICATION = "duplication"
    VARIANT = "variant"
    LOOP = "loop"
    RESOURCE_COST = "resource_cost"
    HOST = "host"
    DOMAIN = "domain"
    PARTITION = "partition"
    SHARD = "shard"
    REGION = "region"
    CROSS_RESOURCE = "cross_resource"
    TEMPORAL = "temporal"
    HISTORICAL = "historical"
    SECURITY = "security"
    POLICY = "policy"
    SOURCE = "source"
    TECHNICAL = "technical"


class AbuseSignalType(str, Enum):
    EXCESSIVE_CRAWL_WORK = "excessive_crawl_work"
    EXCESSIVE_INDEX_WORK = "excessive_index_work"
    EXCESSIVE_URL_VARIANTS = "excessive_url_variants"
    EXCESSIVE_PARAMETER_VARIANTS = "excessive_parameter_variants"
    DUPLICATE_WORK = "duplicate_work"
    DUPLICATE_DISCOVERY = "duplicate_discovery"
    DUPLICATE_INDEXING = "duplicate_indexing"

    CRAWL_LOOP = "crawl_loop"
    REDIRECT_LOOP = "redirect_loop"

    FRONTIER_PRESSURE = "frontier_pressure"
    QUEUE_PRESSURE = "queue_pressure"

    RESOURCE_COST_SPIKE = "resource_cost_spike"
    BANDWIDTH_PRESSURE = "bandwidth_pressure"
    COMPUTE_PRESSURE = "compute_pressure"
    STORAGE_PRESSURE = "storage_pressure"

    HOST_PRESSURE = "host_pressure"
    DOMAIN_PRESSURE = "domain_pressure"
    PARTITION_PRESSURE = "partition_pressure"
    SHARD_PRESSURE = "shard_pressure"
    REGION_PRESSURE = "region_pressure"

    CROSS_RESOURCE_CLUSTER = "cross_resource_cluster"
    CROSS_DOMAIN_CLUSTER = "cross_domain_cluster"
    INFRASTRUCTURE_REUSE = "infrastructure_reuse"

    TEMPORAL_ABUSE_SPIKE = "temporal_abuse_spike"
    RECURRING_ABUSE = "recurring_abuse"
    HISTORICAL_ABUSE = "historical_abuse"

    POLICY_BYPASS = "policy_bypass"
    RESOURCE_PROTECTION_BYPASS = "resource_protection_bypass"

    SECURITY_CORRELATION = "security_correlation"
    TECHNICAL_ANOMALY = "technical_anomaly"
    SOURCE_RELIABILITY_ANOMALY = "source_reliability_anomaly"


class AbuseEvidenceStrength(str, Enum):
    NONE = "none"
    WEAK = "weak"
    MODERATE = "moderate"
    STRONG = "strong"
    VERY_STRONG = "very_strong"


class AbuseRiskBand(str, Enum):
    NONE = "none"
    LOW = "low"
    MODERATE = "moderate"
    HIGH = "high"
    VERY_HIGH = "very_high"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


class EnforcementAction(str, Enum):
    NONE = "none"
    MONITOR = "monitor"
    REDUCE_PRIORITY = "reduce_priority"
    DEFER_RESOURCE = "defer_resource"
    RATE_LIMIT_INTERNAL_WORK = "rate_limit_internal_work"
    DEDUPLICATE_WORK = "deduplicate_work"
    QUARANTINE_RESOURCE = "quarantine_resource"
    QUARANTINE_CLUSTER = "quarantine_cluster"
    SUSPEND_PROCESSING = "suspend_processing"
    REQUIRE_REVIEW = "require_review"


class EnforcementScope(str, Enum):
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


class EnforcementDecision(str, Enum):
    NO_ACTION = "no_action"
    MONITOR = "monitor"
    PROTECT = "protect"
    QUARANTINE = "quarantine"
    REVIEW_REQUIRED = "review_required"
    DEFERRED = "deferred"
    PARTIAL_EVIDENCE = "partial_evidence"
    REJECTED_INPUT = "rejected_input"


class EnforcementConfidence(str, Enum):
    UNKNOWN = "unknown"
    LOW = "low"
    MODERATE = "moderate"
    HIGH = "high"
    VERY_HIGH = "very_high"


class EvidenceKind(str, Enum):
    CRAWL = "crawl"
    INDEX = "index"
    RESOURCE_COST = "resource_cost"
    DUPLICATION = "duplication"
    VARIANT = "variant"
    LOOP = "loop"
    HOST = "host"
    DOMAIN = "domain"
    PARTITION = "partition"
    SHARD = "shard"
    REGION = "region"
    CROSS_RESOURCE = "cross_resource"
    TEMPORAL = "temporal"
    HISTORICAL = "historical"
    SECURITY = "security"
    POLICY = "policy"
    TECHNICAL = "technical"
    SOURCE = "source"


class EnforcementEventType(str, Enum):
    ANALYSIS_STARTED = "analysis_started"
    INPUT_ACCEPTED = "input_accepted"
    INPUT_NORMALIZED = "input_normalized"
    EVIDENCE_ACCEPTED = "evidence_accepted"
    SIGNAL_DETECTED = "signal_detected"
    DISTRIBUTED_CORRELATION = "distributed_correlation"
    RISK_CALCULATED = "risk_calculated"
    POLICY_EVALUATED = "policy_evaluated"
    ENFORCEMENT_PLANNED = "enforcement_planned"
    ENFORCEMENT_SCOPED = "enforcement_scoped"
    IDEMPOTENCY_CHECKED = "idempotency_checked"
    CHECKPOINT_CREATED = "checkpoint_created"
    DECISION_PREPARED = "decision_prepared"
    ANALYSIS_COMPLETED = "analysis_completed"
    ANALYSIS_PARTIAL = "analysis_partial"
    ANALYSIS_DEFERRED = "analysis_deferred"
    ANALYSIS_REJECTED = "analysis_rejected"
    ANALYSIS_FAILED = "analysis_failed"


class EnforcementCheckpointType(str, Enum):
    INPUT_ACCEPTED = "input_accepted"
    NORMALIZATION_COMPLETE = "normalization_complete"
    EVIDENCE_COMPLETE = "evidence_complete"
    CORRELATION_COMPLETE = "correlation_complete"
    RISK_COMPLETE = "risk_complete"
    POLICY_COMPLETE = "policy_complete"
    ENFORCEMENT_PLAN_COMPLETE = "enforcement_plan_complete"
    RESULT_COMPLETE = "result_complete"


# ============================================================================
# Core identity / lineage
# ============================================================================


@dataclass(frozen=True)
class DistributedAbuseIdentity:
    resource_id: str

    document_id: str = ""
    url: str = ""

    host_id: str = ""
    domain_id: str = ""

    partition_id: str = ""
    shard_id: str = ""
    region_id: str = ""

    cluster_id: str = ""
    version: str = "1"


@dataclass(frozen=True)
class DistributedAbuseLineage:
    resource_id: str

    current_stage: str = PHASE
    previous_stage: str = PREVIOUS_STAGE

    previous_stage_version: str = ""

    source_evidence_ids: Tuple[str, ...] = ()
    source_evidence_versions: Tuple[str, ...] = ()

    parent_resource_ids: Tuple[str, ...] = ()
    parent_document_ids: Tuple[str, ...] = ()

    lineage_metadata: Mapping[str, Any] = field(default_factory=dict)


# ============================================================================
# Evidence input
# ============================================================================


@dataclass
class DistributedAbuseInput:
    identity: DistributedAbuseIdentity

    evidence_ids: Tuple[str, ...] = ()
    evidence_versions: Tuple[str, ...] = ()

    crawl_abuse_risk: float = 0.0
    index_abuse_risk: float = 0.0
    resource_cost_risk: float = 0.0
    duplicate_work_risk: float = 0.0
    variant_explosion_risk: float = 0.0
    loop_risk: float = 0.0
    cross_resource_risk: float = 0.0
    temporal_risk: float = 0.0
    security_correlation_risk: float = 0.0

    prior_enforcement_risk: float = 0.0

    host_pressure: float = 0.0
    domain_pressure: float = 0.0
    partition_pressure: float = 0.0
    shard_pressure: float = 0.0
    region_pressure: float = 0.0

    policy_bypass_indicators: int = 0

    active_resource_count: int = 0
    affected_resource_count: int = 0

    cluster_size: int = 0
    domain_cluster_size: int = 0

    repeated_abuse_count: int = 0
    historical_abuse_count: int = 0

    source_confidence: float = 1.0

    observation_timestamp: Optional[str] = None

    partial: bool = False
    missing_fields: Tuple[str, ...] = ()

    metadata: Mapping[str, Any] = field(default_factory=dict)


# ============================================================================
# History
# ============================================================================


@dataclass
class DistributedAbuseHistory:
    previous_analysis_count: int = 0

    previous_protection_count: int = 0
    previous_quarantine_count: int = 0
    previous_review_count: int = 0

    previous_risk_values: Tuple[float, ...] = ()

    previous_actions: Tuple[str, ...] = ()

    previous_scope_values: Tuple[str, ...] = ()

    previous_decision_ids: Tuple[str, ...] = ()

    historical_abuse_count: int = 0
    recurring_abuse_count: int = 0

    last_decision_timestamp: Optional[str] = None

    metadata: Mapping[str, Any] = field(default_factory=dict)


# ============================================================================
# Signal
# ============================================================================


@dataclass(frozen=True)
class DistributedAbuseSignal:
    signal_id: str
    resource_id: str

    family: AbuseSignalFamily
    signal_type: AbuseSignalType

    evidence_kind: EvidenceKind

    strength: AbuseEvidenceStrength

    value: float
    confidence: float

    description: str

    source: str = "phase14.6"
    source_version: str = ARCHITECTURE_VERSION

    observed_at: Optional[str] = None

    partial: bool = False

    metadata: Mapping[str, Any] = field(default_factory=dict)


# ============================================================================
# Evidence
# ============================================================================


@dataclass(frozen=True)
class DistributedAbuseEvidence:
    evidence_id: str
    resource_id: str

    kind: EvidenceKind
    strength: AbuseEvidenceStrength

    score: float
    confidence: float

    signal_ids: Tuple[str, ...] = ()

    explanation: str = ""

    provenance: Mapping[str, Any] = field(default_factory=dict)


# ============================================================================
# Policy
# ============================================================================


@dataclass(frozen=True)
class DistributedEnforcementPolicy:
    max_batch_size: int = 100_000
    max_signals_per_resource: int = 512
    max_evidence_items_per_resource: int = 512

    minimum_confidence: float = 0.20

    monitor_threshold: float = 0.20
    protect_threshold: float = 0.40
    quarantine_threshold: float = 0.75
    critical_threshold: float = 0.93

    high_distribution_pressure: float = 0.75
    critical_distribution_pressure: float = 0.93

    minimum_cluster_size_for_cluster_action: int = 25

    allow_partial: bool = True

    require_multiple_evidence_sources_for_quarantine: bool = True

    require_high_confidence_for_quarantine: bool = True

    allow_cluster_scope: bool = True
    allow_domain_scope: bool = True
    allow_host_scope: bool = True
    allow_partition_scope: bool = True
    allow_shard_scope: bool = True
    allow_region_scope: bool = True

    deterministic: bool = True

    metadata: Mapping[str, Any] = field(default_factory=dict)


# ============================================================================
# Enforcement plan
# ============================================================================


@dataclass(frozen=True)
class EnforcementPlan:
    plan_id: str
    resource_id: str

    action: EnforcementAction
    scope: EnforcementScope

    risk_score: float
    confidence: float

    reversible: bool = True
    requires_review: bool = False

    idempotency_key: str = ""

    reason_codes: Tuple[str, ...] = ()

    source_signal_ids: Tuple[str, ...] = ()

    created_at: str = ""

    metadata: Mapping[str, Any] = field(default_factory=dict)


# ============================================================================
# Checkpoint
# ============================================================================


@dataclass(frozen=True)
class DistributedAbuseCheckpoint:
    checkpoint_id: str
    resource_id: str

    checkpoint_type: EnforcementCheckpointType

    created_at: str

    signal_count: int = 0
    evidence_count: int = 0
    plan_count: int = 0

    partial: bool = False

    metadata: Mapping[str, Any] = field(default_factory=dict)


# ============================================================================
# Event
# ============================================================================


@dataclass(frozen=True)
class DistributedAbuseEvent:
    event_id: str
    resource_id: str

    event_type: EnforcementEventType

    state: DistributedAbuseState

    timestamp: str

    message: str = ""

    signal_id: str = ""
    checkpoint_id: str = ""
    plan_id: str = ""

    metadata: Mapping[str, Any] = field(default_factory=dict)


# ============================================================================
# Result
# ============================================================================


@dataclass(frozen=True)
class DistributedAbuseResult:
    identity: DistributedAbuseIdentity
    lineage: DistributedAbuseLineage

    crawl_abuse_risk: float
    index_abuse_risk: float
    resource_cost_risk: float
    duplicate_work_risk: float
    variant_explosion_risk: float
    loop_risk: float
    cross_resource_risk: float
    temporal_risk: float
    security_correlation_risk: float

    distributed_pressure_risk: float
    overall_abuse_risk: float

    risk_band: AbuseRiskBand
    confidence_band: EnforcementConfidence

    confidence: float

    signals: Tuple[DistributedAbuseSignal, ...]
    evidence: Tuple[DistributedAbuseEvidence, ...]

    enforcement_plans: Tuple[EnforcementPlan, ...]

    decision: EnforcementDecision

    reasons: Tuple[str, ...]

    partial: bool
    missing_fields: Tuple[str, ...]

    created_at: str

    metadata: Mapping[str, Any] = field(default_factory=dict)


# ============================================================================
# Backend protocol
# ============================================================================


class DistributedAbuseBackend(Protocol):
    def save_result(self, result: DistributedAbuseResult) -> None:
        ...

    def save_event(self, event: DistributedAbuseEvent) -> None:
        ...

    def save_checkpoint(
        self,
        checkpoint: DistributedAbuseCheckpoint,
    ) -> None:
        ...

    def save_plan(self, plan: EnforcementPlan) -> None:
        ...

    def get_result(
        self,
        resource_id: str,
    ) -> Optional[DistributedAbuseResult]:
        ...

    def get_events(
        self,
        resource_id: str,
    ) -> Sequence[DistributedAbuseEvent]:
        ...

    def get_checkpoints(
        self,
        resource_id: str,
    ) -> Sequence[DistributedAbuseCheckpoint]:
        ...

    def get_plans(
        self,
        resource_id: str,
    ) -> Sequence[EnforcementPlan]:
        ...


class InMemoryDistributedAbuseBackend:
    """
    Reference backend.

    Production deployments can replace this with a distributed durable
    backend without changing the detection/enforcement architecture.
    """

    def __init__(self) -> None:
        self._results: Dict[str, DistributedAbuseResult] = {}
        self._events: Dict[str, List[DistributedAbuseEvent]] = {}
        self._checkpoints: Dict[str, List[DistributedAbuseCheckpoint]] = {}
        self._plans: Dict[str, List[EnforcementPlan]] = {}

    def save_result(self, result: DistributedAbuseResult) -> None:
        self._results[result.identity.resource_id] = result

    def save_event(self, event: DistributedAbuseEvent) -> None:
        self._events.setdefault(event.resource_id, []).append(event)

    def save_checkpoint(
        self,
        checkpoint: DistributedAbuseCheckpoint,
    ) -> None:
        self._checkpoints.setdefault(
            checkpoint.resource_id,
            [],
        ).append(checkpoint)

    def save_plan(self, plan: EnforcementPlan) -> None:
        plans = self._plans.setdefault(plan.resource_id, [])

        existing = {
            item.idempotency_key
            for item in plans
            if item.idempotency_key
        }

        if plan.idempotency_key not in existing:
            plans.append(plan)

    def get_result(
        self,
        resource_id: str,
    ) -> Optional[DistributedAbuseResult]:
        return self._results.get(resource_id)

    def get_events(
        self,
        resource_id: str,
    ) -> Sequence[DistributedAbuseEvent]:
        return tuple(self._events.get(resource_id, ()))

    def get_checkpoints(
        self,
        resource_id: str,
    ) -> Sequence[DistributedAbuseCheckpoint]:
        return tuple(self._checkpoints.get(resource_id, ()))

    def get_plans(
        self,
        resource_id: str,
    ) -> Sequence[EnforcementPlan]:
        return tuple(self._plans.get(resource_id, ()))


# ============================================================================
# Main architecture
# ============================================================================


class DistributedAbuseDetectionEnforcementArchitecture:
    """
    Phase 14.6.

    Distributed abuse detection and controlled enforcement planning.

    This component turns previously generated evidence into distributed
    protection/enforcement intents.

    It does not directly operate external systems.
    """

    def __init__(
        self,
        policy: Optional[DistributedEnforcementPolicy] = None,
        backend: Optional[DistributedAbuseBackend] = None,
    ) -> None:
        self.policy = policy or DistributedEnforcementPolicy()
        self.backend = backend or InMemoryDistributedAbuseBackend()

    # ------------------------------------------------------------------
    # Safe helpers
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
        try:
            value = float(value)
        except (TypeError, ValueError):
            return minimum

        if math.isnan(value) or math.isinf(value):
            return minimum

        return max(minimum, min(maximum, value))

    @staticmethod
    def _safe_float(
        value: Any,
        default: float = 0.0,
    ) -> float:
        try:
            value = float(value)

            if math.isnan(value) or math.isinf(value):
                return default

            return value
        except (TypeError, ValueError):
            return default

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
    def _digest(
        payload: Mapping[str, Any],
    ) -> str:
        encoded = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")

        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _signal_id(
        resource_id: str,
        signal_type: AbuseSignalType,
        value: float,
    ) -> str:
        raw = (
            f"{resource_id}|"
            f"{signal_type.value}|"
            f"{value:.8f}"
        )

        return hashlib.sha256(
            raw.encode("utf-8")
        ).hexdigest()[:32]

    @staticmethod
    def _plan_id(
        resource_id: str,
        action: EnforcementAction,
        scope: EnforcementScope,
        risk: float,
    ) -> str:
        raw = (
            f"{resource_id}|"
            f"{action.value}|"
            f"{scope.value}|"
            f"{risk:.8f}"
        )

        return hashlib.sha256(
            raw.encode("utf-8")
        ).hexdigest()[:32]

    @staticmethod
    def _idempotency_key(
        resource_id: str,
        action: EnforcementAction,
        scope: EnforcementScope,
    ) -> str:
        raw = (
            f"our-search|"
            f"phase14.6|"
            f"{resource_id}|"
            f"{action.value}|"
            f"{scope.value}"
        )

        return hashlib.sha256(
            raw.encode("utf-8")
        ).hexdigest()

    # ------------------------------------------------------------------
    # Events / checkpoints
    # ------------------------------------------------------------------

    def _event(
        self,
        resource_id: str,
        event_type: EnforcementEventType,
        state: DistributedAbuseState,
        message: str = "",
        signal_id: str = "",
        checkpoint_id: str = "",
        plan_id: str = "",
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> DistributedAbuseEvent:
        event_id = hashlib.sha256(
            (
                f"{resource_id}|"
                f"{event_type.value}|"
                f"{state.value}|"
                f"{message}"
            ).encode("utf-8")
        ).hexdigest()[:32]

        event = DistributedAbuseEvent(
            event_id=event_id,
            resource_id=resource_id,
            event_type=event_type,
            state=state,
            timestamp=self._now(),
            message=message,
            signal_id=signal_id,
            checkpoint_id=checkpoint_id,
            plan_id=plan_id,
            metadata=dict(metadata or {}),
        )

        self.backend.save_event(event)

        return event

    def _checkpoint(
        self,
        resource_id: str,
        checkpoint_type: EnforcementCheckpointType,
        signal_count: int,
        evidence_count: int,
        plan_count: int,
        partial: bool,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> DistributedAbuseCheckpoint:
        checkpoint_id = hashlib.sha256(
            (
                f"{resource_id}|"
                f"{checkpoint_type.value}|"
                f"{signal_count}|"
                f"{evidence_count}|"
                f"{plan_count}"
            ).encode("utf-8")
        ).hexdigest()[:32]

        checkpoint = DistributedAbuseCheckpoint(
            checkpoint_id=checkpoint_id,
            resource_id=resource_id,
            checkpoint_type=checkpoint_type,
            created_at=self._now(),
            signal_count=signal_count,
            evidence_count=evidence_count,
            plan_count=plan_count,
            partial=partial,
            metadata=dict(metadata or {}),
        )

        self.backend.save_checkpoint(checkpoint)

        self._event(
            resource_id,
            EnforcementEventType.CHECKPOINT_CREATED,
            DistributedAbuseState.CHECKPOINTING,
            "Distributed abuse checkpoint created.",
            checkpoint_id=checkpoint_id,
        )

        return checkpoint

    # ------------------------------------------------------------------
    # Validation / normalization
    # ------------------------------------------------------------------

    def validate_input(
        self,
        item: DistributedAbuseInput,
    ) -> Tuple[bool, Tuple[str, ...]]:
        errors: List[str] = []

        if not item.identity.resource_id:
            errors.append("missing_resource_id")

        risk_fields = (
            "crawl_abuse_risk",
            "index_abuse_risk",
            "resource_cost_risk",
            "duplicate_work_risk",
            "variant_explosion_risk",
            "loop_risk",
            "cross_resource_risk",
            "temporal_risk",
            "security_correlation_risk",
            "prior_enforcement_risk",
            "host_pressure",
            "domain_pressure",
            "partition_pressure",
            "shard_pressure",
            "region_pressure",
            "source_confidence",
        )

        for field_name in risk_fields:
            value = self._safe_float(
                getattr(item, field_name),
                0.0,
            )

            if value < 0:
                errors.append(
                    f"negative_{field_name}"
                )

        count_fields = (
            "policy_bypass_indicators",
            "active_resource_count",
            "affected_resource_count",
            "cluster_size",
            "domain_cluster_size",
            "repeated_abuse_count",
            "historical_abuse_count",
        )

        for field_name in count_fields:
            if self._safe_int(
                getattr(item, field_name),
                0,
            ) < 0:
                errors.append(
                    f"negative_{field_name}"
                )

        return not errors, tuple(errors)

    def normalize_input(
        self,
        item: DistributedAbuseInput,
    ) -> DistributedAbuseInput:
        identity = item.identity

        item.identity = DistributedAbuseIdentity(
            resource_id=str(identity.resource_id or ""),
            document_id=str(identity.document_id or ""),
            url=str(identity.url or "").strip(),
            host_id=str(identity.host_id or ""),
            domain_id=str(identity.domain_id or ""),
            partition_id=str(identity.partition_id or ""),
            shard_id=str(identity.shard_id or ""),
            region_id=str(identity.region_id or ""),
            cluster_id=str(identity.cluster_id or ""),
            version=str(identity.version or "1"),
        )

        risk_fields = (
            "crawl_abuse_risk",
            "index_abuse_risk",
            "resource_cost_risk",
            "duplicate_work_risk",
            "variant_explosion_risk",
            "loop_risk",
            "cross_resource_risk",
            "temporal_risk",
            "security_correlation_risk",
            "prior_enforcement_risk",
            "host_pressure",
            "domain_pressure",
            "partition_pressure",
            "shard_pressure",
            "region_pressure",
        )

        for field_name in risk_fields:
            setattr(
                item,
                field_name,
                self._clamp(
                    self._safe_float(
                        getattr(item, field_name),
                        0.0,
                    )
                ),
            )

        item.source_confidence = self._clamp(
            self._safe_float(
                item.source_confidence,
                1.0,
            )
        )

        count_fields = (
            "policy_bypass_indicators",
            "active_resource_count",
            "affected_resource_count",
            "cluster_size",
            "domain_cluster_size",
            "repeated_abuse_count",
            "historical_abuse_count",
        )

        for field_name in count_fields:
            setattr(
                item,
                field_name,
                max(
                    0,
                    self._safe_int(
                        getattr(item, field_name),
                        0,
                    ),
                ),
            )

        return item

    # ------------------------------------------------------------------
    # Signal construction
    # ------------------------------------------------------------------

    def _make_signal(
        self,
        item: DistributedAbuseInput,
        family: AbuseSignalFamily,
        signal_type: AbuseSignalType,
        evidence_kind: EvidenceKind,
        value: float,
        confidence: float,
        description: str,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> DistributedAbuseSignal:
        value = self._clamp(value)
        confidence = self._clamp(confidence)

        if value <= 0.0:
            strength = AbuseEvidenceStrength.NONE
        elif value < 0.25:
            strength = AbuseEvidenceStrength.WEAK
        elif value < 0.50:
            strength = AbuseEvidenceStrength.MODERATE
        elif value < 0.80:
            strength = AbuseEvidenceStrength.STRONG
        else:
            strength = AbuseEvidenceStrength.VERY_STRONG

        return DistributedAbuseSignal(
            signal_id=self._signal_id(
                item.identity.resource_id,
                signal_type,
                value,
            ),
            resource_id=item.identity.resource_id,
            family=family,
            signal_type=signal_type,
            evidence_kind=evidence_kind,
            strength=strength,
            value=value,
            confidence=confidence,
            description=description,
            observed_at=item.observation_timestamp,
            partial=item.partial,
            metadata=dict(metadata or {}),
        )

    # ------------------------------------------------------------------
    # Resource-level evidence
    # ------------------------------------------------------------------

    def extract_resource_signals(
        self,
        item: DistributedAbuseInput,
    ) -> List[DistributedAbuseSignal]:
        signals: List[DistributedAbuseSignal] = []

        risk_map = (
            (
                AbuseSignalFamily.CRAWL,
                AbuseSignalType.EXCESSIVE_CRAWL_WORK,
                EvidenceKind.CRAWL,
                item.crawl_abuse_risk,
                "Crawl-abuse evidence indicates elevated protection pressure.",
            ),
            (
                AbuseSignalFamily.INDEX,
                AbuseSignalType.EXCESSIVE_INDEX_WORK,
                EvidenceKind.INDEX,
                item.index_abuse_risk,
                "Index-abuse evidence indicates elevated indexing pressure.",
            ),
            (
                AbuseSignalFamily.RESOURCE_COST,
                AbuseSignalType.RESOURCE_COST_SPIKE,
                EvidenceKind.RESOURCE_COST,
                item.resource_cost_risk,
                "Resource-cost evidence indicates elevated infrastructure pressure.",
            ),
            (
                AbuseSignalFamily.DUPLICATION,
                AbuseSignalType.DUPLICATE_WORK,
                EvidenceKind.DUPLICATION,
                item.duplicate_work_risk,
                "Duplicate-work evidence indicates repeated infrastructure work.",
            ),
            (
                AbuseSignalFamily.VARIANT,
                AbuseSignalType.EXCESSIVE_URL_VARIANTS,
                EvidenceKind.VARIANT,
                item.variant_explosion_risk,
                "Variant evidence indicates unusually large resource-space expansion.",
            ),
            (
                AbuseSignalFamily.LOOP,
                AbuseSignalType.CRAWL_LOOP,
                EvidenceKind.LOOP,
                item.loop_risk,
                "Loop evidence indicates repeated traversal or redirect pressure.",
            ),
            (
                AbuseSignalFamily.CROSS_RESOURCE,
                AbuseSignalType.CROSS_RESOURCE_CLUSTER,
                EvidenceKind.CROSS_RESOURCE,
                item.cross_resource_risk,
                "Cross-resource evidence indicates correlated abuse patterns.",
            ),
            (
                AbuseSignalFamily.TEMPORAL,
                AbuseSignalType.TEMPORAL_ABUSE_SPIKE,
                EvidenceKind.TEMPORAL,
                item.temporal_risk,
                "Temporal evidence indicates abnormal abuse activity over time.",
            ),
            (
                AbuseSignalFamily.SECURITY,
                AbuseSignalType.SECURITY_CORRELATION,
                EvidenceKind.SECURITY,
                item.security_correlation_risk,
                "Security evidence correlates with distributed abuse indicators.",
            ),
        )

        for (
            family,
            signal_type,
            evidence_kind,
            value,
            description,
        ) in risk_map:
            if value <= 0.0:
                continue

            signals.append(
                self._make_signal(
                    item,
                    family,
                    signal_type,
                    evidence_kind,
                    value,
                    item.source_confidence,
                    description,
                )
            )

        return signals

    # ------------------------------------------------------------------
    # Distributed pressure
    # ------------------------------------------------------------------

    def extract_distributed_signals(
        self,
        item: DistributedAbuseInput,
    ) -> List[DistributedAbuseSignal]:
        signals: List[DistributedAbuseSignal] = []

        pressure_map = (
            (
                AbuseSignalFamily.HOST,
                AbuseSignalType.HOST_PRESSURE,
                EvidenceKind.HOST,
                item.host_pressure,
                "Observed host-level infrastructure pressure is elevated.",
            ),
            (
                AbuseSignalFamily.DOMAIN,
                AbuseSignalType.DOMAIN_PRESSURE,
                EvidenceKind.DOMAIN,
                item.domain_pressure,
                "Observed domain-level infrastructure pressure is elevated.",
            ),
            (
                AbuseSignalFamily.PARTITION,
                AbuseSignalType.PARTITION_PRESSURE,
                EvidenceKind.PARTITION,
                item.partition_pressure,
                "Observed partition-level infrastructure pressure is elevated.",
            ),
            (
                AbuseSignalFamily.SHARD,
                AbuseSignalType.SHARD_PRESSURE,
                EvidenceKind.SHARD,
                item.shard_pressure,
                "Observed shard-level infrastructure pressure is elevated.",
            ),
            (
                AbuseSignalFamily.REGION,
                AbuseSignalType.REGION_PRESSURE,
                EvidenceKind.REGION,
                item.region_pressure,
                "Observed region-level infrastructure pressure is elevated.",
            ),
        )

        for (
            family,
            signal_type,
            evidence_kind,
            value,
            description,
        ) in pressure_map:
            if value < self.policy.high_distribution_pressure:
                continue

            signals.append(
                self._make_signal(
                    item,
                    family,
                    signal_type,
                    evidence_kind,
                    value,
                    item.source_confidence,
                    description,
                )
            )

        return signals

    # ------------------------------------------------------------------
    # Cluster / temporal / policy signals
    # ------------------------------------------------------------------

    def extract_correlation_signals(
        self,
        item: DistributedAbuseInput,
        history: Optional[DistributedAbuseHistory],
    ) -> List[DistributedAbuseSignal]:
        signals: List[DistributedAbuseSignal] = []

        if item.cluster_size >= self.policy.minimum_cluster_size_for_cluster_action:
            value = self._clamp(
                math.log1p(item.cluster_size)
                / math.log1p(
                    max(
                        1000,
                        self.policy.minimum_cluster_size_for_cluster_action,
                    )
                )
            )

            signals.append(
                self._make_signal(
                    item,
                    AbuseSignalFamily.CROSS_RESOURCE,
                    AbuseSignalType.CROSS_RESOURCE_CLUSTER,
                    EvidenceKind.CROSS_RESOURCE,
                    value,
                    item.source_confidence,
                    "The resource belongs to a substantial correlated abuse cluster.",
                )
            )

        if item.domain_cluster_size >= self.policy.minimum_cluster_size_for_cluster_action:
            value = self._clamp(
                math.log1p(item.domain_cluster_size)
                / math.log1p(
                    max(
                        1000,
                        self.policy.minimum_cluster_size_for_cluster_action,
                    )
                )
            )

            signals.append(
                self._make_signal(
                    item,
                    AbuseSignalFamily.DOMAIN,
                    AbuseSignalType.CROSS_DOMAIN_CLUSTER,
                    EvidenceKind.DOMAIN,
                    value,
                    item.source_confidence,
                    "The resource belongs to a substantial cross-domain cluster.",
                )
            )

        if item.repeated_abuse_count > 0:
            value = self._clamp(
                math.log1p(item.repeated_abuse_count)
                / math.log1p(100.0)
            )

            signals.append(
                self._make_signal(
                    item,
                    AbuseSignalFamily.HISTORICAL,
                    AbuseSignalType.RECURRING_ABUSE,
                    EvidenceKind.HISTORICAL,
                    value,
                    item.source_confidence,
                    "Historical evidence indicates recurring abuse patterns.",
                )
            )

        if item.historical_abuse_count > 0:
            value = self._clamp(
                math.log1p(item.historical_abuse_count)
                / math.log1p(100.0)
            )

            signals.append(
                self._make_signal(
                    item,
                    AbuseSignalFamily.HISTORICAL,
                    AbuseSignalType.HISTORICAL_ABUSE,
                    EvidenceKind.HISTORICAL,
                    value,
                    item.source_confidence,
                    "Historical records contain prior abuse evidence.",
                )
            )

        if item.policy_bypass_indicators > 0:
            value = self._clamp(
                math.log1p(item.policy_bypass_indicators)
                / math.log1p(100.0)
            )

            signals.append(
                self._make_signal(
                    item,
                    AbuseSignalFamily.POLICY,
                    AbuseSignalType.POLICY_BYPASS,
                    EvidenceKind.POLICY,
                    value,
                    item.source_confidence,
                    "Externally supplied evidence contains policy-bypass indicators.",
                )
            )

        if history is not None:
            if history.previous_quarantine_count > 0:
                value = self._clamp(
                    history.previous_quarantine_count
                    / max(
                        1.0,
                        float(history.previous_analysis_count),
                    )
                )

                if value > 0:
                    signals.append(
                        self._make_signal(
                            item,
                            AbuseSignalFamily.HISTORICAL,
                            AbuseSignalType.HISTORICAL_ABUSE,
                            EvidenceKind.HISTORICAL,
                            value,
                            item.source_confidence,
                            "Previous analysis history contains quarantine-related evidence.",
                        )
                    )

        return signals

    # ------------------------------------------------------------------
    # Aggregate risk
    # ------------------------------------------------------------------

    @staticmethod
    def _saturating_score(
        signals: Sequence[DistributedAbuseSignal],
    ) -> float:
        if not signals:
            return 0.0

        remaining = 1.0

        for signal in signals:
            evidence = max(
                0.0,
                min(
                    1.0,
                    signal.value * signal.confidence,
                ),
            )

            remaining *= 1.0 - evidence

        return max(
            0.0,
            min(
                1.0,
                1.0 - remaining,
            ),
        )

    def aggregate_risk(
        self,
        item: DistributedAbuseInput,
        signals: Sequence[DistributedAbuseSignal],
    ) -> Dict[str, float]:
        crawl = self._saturating_score(
            [
                signal
                for signal in signals
                if signal.family == AbuseSignalFamily.CRAWL
            ]
        )

        index = self._saturating_score(
            [
                signal
                for signal in signals
                if signal.family == AbuseSignalFamily.INDEX
            ]
        )

        resource_cost = self._saturating_score(
            [
                signal
                for signal in signals
                if signal.family in {
                    AbuseSignalFamily.RESOURCE_COST,
                    AbuseSignalFamily.HOST,
                    AbuseSignalFamily.DOMAIN,
                    AbuseSignalFamily.PARTITION,
                    AbuseSignalFamily.SHARD,
                    AbuseSignalFamily.REGION,
                }
            ]
        )

        duplicate = self._saturating_score(
            [
                signal
                for signal in signals
                if signal.family == AbuseSignalFamily.DUPLICATION
            ]
        )

        variants = self._saturating_score(
            [
                signal
                for signal in signals
                if signal.family == AbuseSignalFamily.VARIANT
            ]
        )

        loops = self._saturating_score(
            [
                signal
                for signal in signals
                if signal.family == AbuseSignalFamily.LOOP
            ]
        )

        cross_resource = self._saturating_score(
            [
                signal
                for signal in signals
                if signal.family in {
                    AbuseSignalFamily.CROSS_RESOURCE,
                    AbuseSignalFamily.DOMAIN,
                }
            ]
        )

        temporal = self._saturating_score(
            [
                signal
                for signal in signals
                if signal.family in {
                    AbuseSignalFamily.TEMPORAL,
                    AbuseSignalFamily.HISTORICAL,
                }
            ]
        )

        security = self._saturating_score(
            [
                signal
                for signal in signals
                if signal.family == AbuseSignalFamily.SECURITY
            ]
        )

        distributed_pressure = max(
            item.host_pressure,
            item.domain_pressure,
            item.partition_pressure,
            item.shard_pressure,
            item.region_pressure,
        )

        overall = self._clamp(
            0.18 * crawl
            + 0.16 * index
            + 0.17 * resource_cost
            + 0.10 * duplicate
            + 0.10 * variants
            + 0.07 * loops
            + 0.09 * cross_resource
            + 0.05 * temporal
            + 0.05 * security
            + 0.03 * distributed_pressure
        )

        return {
            "crawl_abuse_risk": crawl,
            "index_abuse_risk": index,
            "resource_cost_risk": resource_cost,
            "duplicate_work_risk": duplicate,
            "variant_explosion_risk": variants,
            "loop_risk": loops,
            "cross_resource_risk": cross_resource,
            "temporal_risk": temporal,
            "security_correlation_risk": security,
            "distributed_pressure_risk": self._clamp(
                distributed_pressure
            ),
            "overall_abuse_risk": overall,
        }

    # ------------------------------------------------------------------
    # Confidence
    # ------------------------------------------------------------------

    def calculate_confidence(
        self,
        item: DistributedAbuseInput,
        signals: Sequence[DistributedAbuseSignal],
    ) -> float:
        if not signals:
            return self._clamp(
                item.source_confidence
                * (
                    0.60
                    if item.partial
                    else 0.75
                )
            )

        average_signal_confidence = (
            sum(
                signal.confidence
                for signal in signals
            )
            / len(signals)
        )

        evidence_coverage = self._clamp(
            len(signals) / 12.0
        )

        source_component = item.source_confidence

        completeness = (
            0.70
            if item.partial
            else 1.0
        )

        confidence = (
            0.55 * average_signal_confidence
            + 0.25 * source_component
            + 0.20 * evidence_coverage
        )

        return self._clamp(
            confidence * completeness
        )

    def confidence_band(
        self,
        confidence: float,
    ) -> EnforcementConfidence:
        confidence = self._clamp(confidence)

        if confidence < 0.20:
            return EnforcementConfidence.UNKNOWN

        if confidence < 0.40:
            return EnforcementConfidence.LOW

        if confidence < 0.65:
            return EnforcementConfidence.MODERATE

        if confidence < 0.85:
            return EnforcementConfidence.HIGH

        return EnforcementConfidence.VERY_HIGH

    # ------------------------------------------------------------------
    # Risk band
    # ------------------------------------------------------------------

    def risk_band(
        self,
        risk: float,
    ) -> AbuseRiskBand:
        risk = self._clamp(risk)

        if risk < self.policy.monitor_threshold:
            return AbuseRiskBand.NONE

        if risk < self.policy.protect_threshold:
            return AbuseRiskBand.LOW

        if risk < self.policy.quarantine_threshold:
            return AbuseRiskBand.MODERATE

        if risk < self.policy.critical_threshold:
            return AbuseRiskBand.VERY_HIGH

        return AbuseRiskBand.CRITICAL

    # ------------------------------------------------------------------
    # Enforcement policy
    # ------------------------------------------------------------------

    def select_scope(
        self,
        item: DistributedAbuseInput,
        risk: float,
    ) -> EnforcementScope:
        if (
            self.policy.allow_region_scope
            and item.region_id
            and item.region_pressure
            >= self.policy.critical_distribution_pressure
        ):
            return EnforcementScope.REGION

        if (
            self.policy.allow_shard_scope
            and item.shard_id
            and item.shard_pressure
            >= self.policy.critical_distribution_pressure
        ):
            return EnforcementScope.SHARD

        if (
            self.policy.allow_partition_scope
            and item.partition_id
            and item.partition_pressure
            >= self.policy.critical_distribution_pressure
        ):
            return EnforcementScope.PARTITION

        if (
            self.policy.allow_cluster_scope
            and item.cluster_size
            >= self.policy.minimum_cluster_size_for_cluster_action
            and risk >= self.policy.quarantine_threshold
        ):
            return EnforcementScope.CLUSTER

        if (
            self.policy.allow_domain_scope
            and item.domain_id
            and item.domain_pressure
            >= self.policy.protect_threshold
        ):
            return EnforcementScope.DOMAIN

        if (
            self.policy.allow_host_scope
            and item.host_id
            and item.host_pressure
            >= self.policy.protect_threshold
        ):
            return EnforcementScope.HOST

        return EnforcementScope.RESOURCE

    def choose_action(
        self,
        risk: float,
        confidence: float,
        partial: bool,
        signal_count: int,
    ) -> EnforcementAction:
        if confidence < self.policy.minimum_confidence:
            return EnforcementAction.REQUIRE_REVIEW

        if partial:
            if risk >= self.policy.protect_threshold:
                return EnforcementAction.MONITOR

            return EnforcementAction.NONE

        if risk < self.policy.monitor_threshold:
            return EnforcementAction.NONE

        if risk < self.policy.protect_threshold:
            return EnforcementAction.MONITOR

        if risk < self.policy.quarantine_threshold:
            if signal_count >= 2:
                return EnforcementAction.PROTECT

            return EnforcementAction.REDUCE_PRIORITY

        if (
            self.policy.require_multiple_evidence_sources_for_quarantine
            and signal_count < 2
        ):
            return EnforcementAction.REQUIRE_REVIEW

        if (
            self.policy.require_high_confidence_for_quarantine
            and confidence < 0.75
        ):
            return EnforcementAction.REQUIRE_REVIEW

        return EnforcementAction.QUARANTINE_RESOURCE

    # ------------------------------------------------------------------
    # Plan construction
    # ------------------------------------------------------------------

    def build_plan(
        self,
        item: DistributedAbuseInput,
        risk: float,
        confidence: float,
        signals: Sequence[DistributedAbuseSignal],
    ) -> Optional[EnforcementPlan]:
        action = self.choose_action(
            risk=risk,
            confidence=confidence,
            partial=item.partial,
            signal_count=len(signals),
        )

        if action == EnforcementAction.NONE:
            return None

        scope = self.select_scope(
            item=item,
            risk=risk,
        )

        if (
            action == EnforcementAction.QUARANTINE_RESOURCE
            and scope == EnforcementScope.REGION
        ):
            action = EnforcementAction.REQUIRE_REVIEW

        if (
            action == EnforcementAction.QUARANTINE_RESOURCE
            and scope == EnforcementScope.SHARD
            and item.shard_pressure
            < self.policy.critical_distribution_pressure
        ):
            action = EnforcementAction.REQUIRE_REVIEW

        reversible = action not in {
            EnforcementAction.NONE,
        }

        requires_review = action in {
            EnforcementAction.REQUIRE_REVIEW,
            EnforcementAction.QUARANTINE_CLUSTER,
        }

        plan_id = self._plan_id(
            item.identity.resource_id,
            action,
            scope,
            risk,
        )

        idempotency_key = self._idempotency_key(
            item.identity.resource_id,
            action,
            scope,
        )

        reason_codes = tuple(
            signal.signal_type.value
            for signal in sorted(
                signals,
                key=lambda signal: (
                    signal.value * signal.confidence,
                    signal.signal_type.value,
                ),
                reverse=True,
            )[:8]
        )

        plan = EnforcementPlan(
            plan_id=plan_id,
            resource_id=item.identity.resource_id,
            action=action,
            scope=scope,
            risk_score=self._clamp(risk),
            confidence=self._clamp(confidence),
            reversible=reversible,
            requires_review=requires_review,
            idempotency_key=idempotency_key,
            reason_codes=reason_codes,
            source_signal_ids=tuple(
                signal.signal_id
                for signal in signals
            ),
            created_at=self._now(),
            metadata={
                "phase": PHASE,
                "architecture_version": ARCHITECTURE_VERSION,
                "execution_performed": False,
                "external_system_contacted": False,
                "search_index_mutated": False,
            },
        )

        self.backend.save_plan(plan)

        return plan

    # ------------------------------------------------------------------
    # Evidence construction
    # ------------------------------------------------------------------

    def build_evidence(
        self,
        item: DistributedAbuseInput,
        signals: Sequence[DistributedAbuseSignal],
    ) -> Tuple[DistributedAbuseEvidence, ...]:
        groups: Dict[
            EvidenceKind,
            List[DistributedAbuseSignal],
        ] = {}

        for signal in signals:
            groups.setdefault(
                signal.evidence_kind,
                [],
            ).append(signal)

        evidence: List[DistributedAbuseEvidence] = []

        for kind, grouped in sorted(
            groups.items(),
            key=lambda pair: pair[0].value,
        ):
            remaining = 1.0

            for signal in grouped:
                remaining *= 1.0 - self._clamp(
                    signal.value
                    * signal.confidence
                )

            score = self._clamp(
                1.0 - remaining
            )

            confidence = self._clamp(
                sum(
                    signal.confidence
                    for signal in grouped
                )
                / max(
                    1,
                    len(grouped),
                )
            )

            if score < 0.15:
                strength = AbuseEvidenceStrength.WEAK
            elif score < 0.35:
                strength = AbuseEvidenceStrength.MODERATE
            elif score < 0.65:
                strength = AbuseEvidenceStrength.STRONG
            else:
                strength = AbuseEvidenceStrength.VERY_STRONG

            evidence.append(
                DistributedAbuseEvidence(
                    evidence_id=hashlib.sha256(
                        (
                            f"{item.identity.resource_id}|"
                            f"{kind.value}|"
                            f"{score:.8f}"
                        ).encode("utf-8")
                    ).hexdigest()[:32],
                    resource_id=item.identity.resource_id,
                    kind=kind,
                    strength=strength,
                    score=score,
                    confidence=confidence,
                    signal_ids=tuple(
                        signal.signal_id
                        for signal in grouped
                    ),
                    explanation=(
                        f"{len(grouped)} signal(s) support "
                        f"{kind.value} protection evidence."
                    ),
                    provenance={
                        "architecture_version": ARCHITECTURE_VERSION,
                        "phase": PHASE,
                        "source_confidence": item.source_confidence,
                    },
                )
            )

        if len(evidence) > self.policy.max_evidence_items_per_resource:
            evidence = evidence[
                : self.policy.max_evidence_items_per_resource
            ]

        return tuple(evidence)

    # ------------------------------------------------------------------
    # Decision
    # ------------------------------------------------------------------

    def decision(
        self,
        risk: float,
        confidence: float,
        partial: bool,
        valid: bool,
        plans: Sequence[EnforcementPlan],
    ) -> EnforcementDecision:
        if not valid:
            return EnforcementDecision.REJECTED_INPUT

        if confidence < self.policy.minimum_confidence:
            return EnforcementDecision.DEFERRED

        if partial:
            return EnforcementDecision.PARTIAL_EVIDENCE

        if not plans:
            if risk < self.policy.monitor_threshold:
                return EnforcementDecision.NO_ACTION

            return EnforcementDecision.MONITOR

        if any(
            plan.action == EnforcementAction.REQUIRE_REVIEW
            for plan in plans
        ):
            return EnforcementDecision.REVIEW_REQUIRED

        if any(
            plan.action == EnforcementAction.QUARANTINE_RESOURCE
            for plan in plans
        ):
            return EnforcementDecision.QUARANTINE

        return EnforcementDecision.PROTECT

    # ------------------------------------------------------------------
    # Reasons
    # ------------------------------------------------------------------

    def build_reasons(
        self,
        signals: Sequence[DistributedAbuseSignal],
    ) -> Tuple[str, ...]:
        ordered = sorted(
            signals,
            key=lambda signal: (
                signal.value * signal.confidence,
                signal.signal_type.value,
            ),
            reverse=True,
        )

        reasons: List[str] = []
        seen = set()

        for signal in ordered:
            if signal.value <= 0:
                continue

            text = signal.description.strip()

            if not text or text in seen:
                continue

            reasons.append(text)
            seen.add(text)

            if len(reasons) >= 8:
                break

        return tuple(reasons)

    # ------------------------------------------------------------------
    # Main analysis
    # ------------------------------------------------------------------

    def analyze(
        self,
        item: DistributedAbuseInput,
        history: Optional[DistributedAbuseHistory] = None,
    ) -> DistributedAbuseResult:
        resource_id = item.identity.resource_id

        self._event(
            resource_id,
            EnforcementEventType.ANALYSIS_STARTED,
            DistributedAbuseState.RECEIVED,
            "Phase 14.6 distributed abuse analysis started.",
        )

        valid, errors = self.validate_input(item)

        if not valid:
            self._event(
                resource_id,
                EnforcementEventType.ANALYSIS_REJECTED,
                DistributedAbuseState.REJECTED,
                "Input validation failed.",
                metadata={
                    "errors": errors,
                },
            )

            lineage = DistributedAbuseLineage(
                resource_id=resource_id,
                current_stage=PHASE,
                previous_stage=PREVIOUS_STAGE,
            )

            result = DistributedAbuseResult(
                identity=item.identity,
                lineage=lineage,

                crawl_abuse_risk=0.0,
                index_abuse_risk=0.0,
                resource_cost_risk=0.0,
                duplicate_work_risk=0.0,
                variant_explosion_risk=0.0,
                loop_risk=0.0,
                cross_resource_risk=0.0,
                temporal_risk=0.0,
                security_correlation_risk=0.0,

                distributed_pressure_risk=0.0,
                overall_abuse_risk=0.0,

                risk_band=AbuseRiskBand.UNKNOWN,
                confidence_band=EnforcementConfidence.UNKNOWN,
                confidence=0.0,

                signals=(),
                evidence=(),
                enforcement_plans=(),

                decision=EnforcementDecision.REJECTED_INPUT,

                reasons=(
                    "Input validation failed.",
                ),

                partial=True,
                missing_fields=errors,

                created_at=self._now(),

                metadata={
                    "architecture_version": ARCHITECTURE_VERSION,
                    "phase": PHASE,
                },
            )

            self.backend.save_result(result)

            return result

        item = self.normalize_input(item)

        self._event(
            resource_id,
            EnforcementEventType.INPUT_NORMALIZED,
            DistributedAbuseState.NORMALIZING,
            "Input normalization completed.",
        )

        self._checkpoint(
            resource_id,
            EnforcementCheckpointType.NORMALIZATION_COMPLETE,
            signal_count=0,
            evidence_count=0,
            plan_count=0,
            partial=item.partial,
        )

        signals: List[DistributedAbuseSignal] = []

        extraction_steps = (
            (
                DistributedAbuseState.EVIDENCE_AGGREGATION,
                lambda: self.extract_resource_signals(item),
            ),
            (
                DistributedAbuseState.DISTRIBUTION_ANALYSIS,
                lambda: self.extract_distributed_signals(item),
            ),
            (
                DistributedAbuseState.CORRELATION,
                lambda: self.extract_correlation_signals(
                    item,
                    history,
                ),
            ),
        )

        for state, extractor in extraction_steps:
            try:
                extracted = extractor()

                for signal in extracted:
                    if signal.value <= 0:
                        continue

                    signals.append(signal)

                    self._event(
                        resource_id,
                        EnforcementEventType.SIGNAL_DETECTED,
                        state,
                        signal.description,
                        signal_id=signal.signal_id,
                        metadata={
                            "family": signal.family.value,
                            "signal_type": signal.signal_type.value,
                            "value": signal.value,
                            "confidence": signal.confidence,
                        },
                    )

            except Exception as exc:
                if not self.policy.allow_partial:
                    raise

                item.partial = True

                item.missing_fields = tuple(
                    sorted(
                        set(item.missing_fields)
                        | {
                            f"analysis:{state.value}"
                        }
                    )
                )

                self._event(
                    resource_id,
                    EnforcementEventType.ANALYSIS_PARTIAL,
                    DistributedAbuseState.PARTIAL,
                    f"Partial failure during {state.value}.",
                    metadata={
                        "error": type(exc).__name__,
                    },
                )

        if len(signals) > self.policy.max_signals_per_resource:
            signals = signals[
                : self.policy.max_signals_per_resource
            ]

            item.partial = True

            item.missing_fields = tuple(
                sorted(
                    set(item.missing_fields)
                    | {"signal_limit_applied"}
                )
            )

        self._checkpoint(
            resource_id,
            EnforcementCheckpointType.EVIDENCE_COMPLETE,
            signal_count=len(signals),
            evidence_count=0,
            plan_count=0,
            partial=item.partial,
        )

        scores = self.aggregate_risk(
            item,
            signals,
        )

        self._event(
            resource_id,
            EnforcementEventType.RISK_CALCULATED,
            DistributedAbuseState.ABUSE_RISK_CALCULATION,
            "Distributed abuse risk calculated.",
            metadata=scores,
        )

        confidence = self.calculate_confidence(
            item,
            signals,
        )

        confidence_band = self.confidence_band(
            confidence
        )

        risk_band = self.risk_band(
            scores["overall_abuse_risk"]
        )

        self._checkpoint(
            resource_id,
            EnforcementCheckpointType.RISK_COMPLETE,
            signal_count=len(signals),
            evidence_count=0,
            plan_count=0,
            partial=item.partial,
        )

        evidence = self.build_evidence(
            item,
            signals,
        )

        self._event(
            resource_id,
            EnforcementEventType.DISTRIBUTED_CORRELATION,
            DistributedAbuseState.CORRELATION,
            "Distributed evidence correlation completed.",
            metadata={
                "evidence_count": len(evidence),
            },
        )

        self._checkpoint(
            resource_id,
            EnforcementCheckpointType.CORRELATION_COMPLETE,
            signal_count=len(signals),
            evidence_count=len(evidence),
            plan_count=0,
            partial=item.partial,
        )

        plans: List[EnforcementPlan] = []

        plan = self.build_plan(
            item=item,
            risk=scores["overall_abuse_risk"],
            confidence=confidence,
            signals=signals,
        )

        if plan is not None:
            plans.append(plan)

            self._event(
                resource_id,
                EnforcementEventType.ENFORCEMENT_PLANNED,
                DistributedAbuseState.ENFORCEMENT_PLANNING,
                "Controlled enforcement plan prepared.",
                plan_id=plan.plan_id,
                metadata={
                    "action": plan.action.value,
                    "scope": plan.scope.value,
                    "risk": plan.risk_score,
                    "confidence": plan.confidence,
                },
            )

            self._event(
                resource_id,
                EnforcementEventType.ENFORCEMENT_SCOPED,
                DistributedAbuseState.ENFORCEMENT_SCOPING,
                "Enforcement scope prepared.",
                plan_id=plan.plan_id,
                metadata={
                    "scope": plan.scope.value,
                },
            )

            self._event(
                resource_id,
                EnforcementEventType.IDEMPOTENCY_CHECKED,
                DistributedAbuseState.IDEMPOTENCY_CHECK,
                "Enforcement plan received deterministic idempotency key.",
                plan_id=plan.plan_id,
                metadata={
                    "idempotency_key": plan.idempotency_key,
                },
            )

        self._checkpoint(
            resource_id,
            EnforcementCheckpointType.POLICY_COMPLETE,
            signal_count=len(signals),
            evidence_count=len(evidence),
            plan_count=len(plans),
            partial=item.partial,
        )

        self._checkpoint(
            resource_id,
            EnforcementCheckpointType.ENFORCEMENT_PLAN_COMPLETE,
            signal_count=len(signals),
            evidence_count=len(evidence),
            plan_count=len(plans),
            partial=item.partial,
        )

        decision = self.decision(
            risk=scores["overall_abuse_risk"],
            confidence=confidence,
            partial=item.partial,
            valid=True,
            plans=plans,
        )

        reasons = self.build_reasons(
            signals
        )

        lineage = DistributedAbuseLineage(
            resource_id=resource_id,
            current_stage=PHASE,
            previous_stage=PREVIOUS_STAGE,
            previous_stage_version=str(
                item.metadata.get(
                    "previous_stage_version",
                    "",
                )
            ),
            source_evidence_ids=tuple(
                str(value)
                for value in item.evidence_ids
            ),
            source_evidence_versions=tuple(
                str(value)
                for value in item.evidence_versions
            ),
            parent_resource_ids=tuple(
                str(value)
                for value in item.metadata.get(
                    "parent_resource_ids",
                    (),
                )
            ),
            parent_document_ids=tuple(
                str(value)
                for value in item.metadata.get(
                    "parent_document_ids",
                    (),
                )
            ),
            lineage_metadata={
                "partition_id": item.identity.partition_id,
                "shard_id": item.identity.shard_id,
                "region_id": item.identity.region_id,
                "cluster_id": item.identity.cluster_id,
            },
        )

        result = DistributedAbuseResult(
            identity=item.identity,
            lineage=lineage,

            crawl_abuse_risk=scores[
                "crawl_abuse_risk"
            ],
            index_abuse_risk=scores[
                "index_abuse_risk"
            ],
            resource_cost_risk=scores[
                "resource_cost_risk"
            ],
            duplicate_work_risk=scores[
                "duplicate_work_risk"
            ],
            variant_explosion_risk=scores[
                "variant_explosion_risk"
            ],
            loop_risk=scores[
                "loop_risk"
            ],
            cross_resource_risk=scores[
                "cross_resource_risk"
            ],
            temporal_risk=scores[
                "temporal_risk"
            ],
            security_correlation_risk=scores[
                "security_correlation_risk"
            ],

            distributed_pressure_risk=scores[
                "distributed_pressure_risk"
            ],
            overall_abuse_risk=scores[
                "overall_abuse_risk"
            ],

            risk_band=risk_band,
            confidence_band=confidence_band,
            confidence=confidence,

            signals=tuple(signals),
            evidence=evidence,

            enforcement_plans=tuple(plans),

            decision=decision,

            reasons=reasons,

            partial=item.partial,
            missing_fields=item.missing_fields,

            created_at=self._now(),

            metadata={
                "architecture_version": ARCHITECTURE_VERSION,
                "phase": PHASE,
                "previous_stage": PREVIOUS_STAGE,
                "next_stage": NEXT_STAGE,
                "scale_target": SCALE_TARGET,
                "google_scale_capability_target": (
                    GOOGLE_SCALE_CAPABILITY_TARGET
                ),
                "google_technology_dependency": (
                    GOOGLE_TECHNOLOGY_DEPENDENCY
                ),

                "signal_count": len(signals),
                "evidence_count": len(evidence),
                "plan_count": len(plans),

                "execution_performed": False,
                "external_system_contacted": False,
                "crawl_performed": False,
                "http_requests_performed": False,
                "search_index_mutated": False,
                "external_resource_modified": False,

                "policy_deterministic": (
                    self.policy.deterministic
                ),
            },
        )

        self._checkpoint(
            resource_id,
            EnforcementCheckpointType.RESULT_COMPLETE,
            signal_count=len(signals),
            evidence_count=len(evidence),
            plan_count=len(plans),
            partial=item.partial,
        )

        self._event(
            resource_id,
            EnforcementEventType.DECISION_PREPARED,
            DistributedAbuseState.DECISION_READY,
            "Distributed enforcement decision prepared.",
            metadata={
                "decision": decision.value,
                "risk_band": risk_band.value,
                "confidence_band": confidence_band.value,
            },
        )

        self._event(
            resource_id,
            (
                EnforcementEventType.ANALYSIS_PARTIAL
                if item.partial
                else EnforcementEventType.ANALYSIS_COMPLETED
            ),
            (
                DistributedAbuseState.PARTIAL
                if item.partial
                else DistributedAbuseState.COMPLETED
            ),
            "Phase 14.6 analysis completed.",
            metadata={
                "overall_abuse_risk": scores[
                    "overall_abuse_risk"
                ],
                "confidence": confidence,
            },
        )

        self.backend.save_result(result)

        return result

    # ------------------------------------------------------------------
    # Batch analysis
    # ------------------------------------------------------------------

    def analyze_many(
        self,
        items: Sequence[DistributedAbuseInput],
        histories: Optional[
            Mapping[
                str,
                DistributedAbuseHistory,
            ]
        ] = None,
    ) -> Tuple[DistributedAbuseResult, ...]:
        if len(items) > self.policy.max_batch_size:
            raise ValueError(
                "Batch exceeds the configured per-request "
                f"safety limit of {self.policy.max_batch_size}."
            )

        histories = histories or {}

        results: List[DistributedAbuseResult] = []

        for item in items:
            history = histories.get(
                item.identity.resource_id
            )

            results.append(
                self.analyze(
                    item,
                    history,
                )
            )

        return tuple(results)

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    def get_result(
        self,
        resource_id: str,
    ) -> Optional[DistributedAbuseResult]:
        return self.backend.get_result(
            resource_id
        )

    def get_events(
        self,
        resource_id: str,
    ) -> Sequence[DistributedAbuseEvent]:
        return self.backend.get_events(
            resource_id
        )

    def get_checkpoints(
        self,
        resource_id: str,
    ) -> Sequence[DistributedAbuseCheckpoint]:
        return self.backend.get_checkpoints(
            resource_id
        )

    def get_plans(
        self,
        resource_id: str,
    ) -> Sequence[EnforcementPlan]:
        return self.backend.get_plans(
            resource_id
        )

    # ------------------------------------------------------------------
    # Architecture metadata
    # ------------------------------------------------------------------

    def architecture(self) -> Dict[str, Any]:
        return {
            "architecture_version": ARCHITECTURE_VERSION,
            "phase": PHASE,
            "phase_name": PHASE_NAME,
            "previous_stage": PREVIOUS_STAGE,
            "next_stage": NEXT_STAGE,
            "next_stage_name": NEXT_STAGE_NAME,

            "scale_target": SCALE_TARGET,

            "google_scale_capability_target": (
                GOOGLE_SCALE_CAPABILITY_TARGET
            ),

            "google_technology_dependency": (
                GOOGLE_TECHNOLOGY_DEPENDENCY
            ),

            "purpose": (
                "Distributed abuse detection and controlled "
                "resource-protection enforcement planning."
            ),

            "pipeline_position": (
                "Consumes Phase 14.5 protection evidence and "
                "prepares distributed enforcement intents before "
                "later security/trust systems."
            ),

            "inputs": [
                "crawl abuse evidence",
                "index abuse evidence",
                "resource cost evidence",
                "duplicate-work evidence",
                "URL variant evidence",
                "loop evidence",
                "cross-resource evidence",
                "temporal evidence",
                "security correlation evidence",
                "host pressure",
                "domain pressure",
                "partition pressure",
                "shard pressure",
                "region pressure",
                "historical evidence",
                "policy-bypass evidence",
                "source confidence",
            ],

            "outputs": [
                "distributed abuse signals",
                "correlated evidence",
                "resource protection risk",
                "distributed pressure risk",
                "overall abuse risk",
                "risk band",
                "confidence",
                "enforcement plan",
                "enforcement scope",
                "idempotency key",
                "auditable decision",
                "checkpoint lineage",
            ],

            "enforcement_actions_supported": [
                action.value
                for action in EnforcementAction
            ],

            "enforcement_scopes_supported": [
                scope.value
                for scope in EnforcementScope
            ],

            "does_not_execute": [
                "malware",
                "payloads",
                "exploits",
                "active security scans",
                "network attacks",
                "credential collection",
                "HTTP requests",
                "crawling",
                "crawler worker assignment",
                "external website modification",
                "external blocking",
                "search-index mutation",
                "direct quarantine execution",
                "direct domain bans",
            ],

            "execution_boundary": (
                "This architecture produces controlled, auditable "
                "enforcement intents. A separate authorized runtime "
                "must execute any permitted internal action."
            ),

            "distributed_properties": [
                "partition aware",
                "shard aware",
                "region aware",
                "cluster aware",
                "idempotency aware",
                "checkpointable",
                "restart safe",
                "partial evidence aware",
                "temporal",
                "provenance preserving",
            ],

            "deterministic": self.policy.deterministic,

            "partial_evidence_supported": (
                self.policy.allow_partial
            ),

            "production_backend_required": True,

            "external_execution_performed": False,
        }

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    @staticmethod
    def result_to_dict(
        result: DistributedAbuseResult,
    ) -> Dict[str, Any]:
        return asdict(result)


# ============================================================================
# Public aliases
# ============================================================================


DistributedAbuseDetectionEnforcement = (
    DistributedAbuseDetectionEnforcementArchitecture
)

GlobalDistributedAbuseDetectionEnforcement = (
    DistributedAbuseDetectionEnforcementArchitecture
)

Phase14_6DistributedAbuseDetectionEnforcement = (
    DistributedAbuseDetectionEnforcementArchitecture
)

DistributedAbuseEnforcementArchitecture = (
    DistributedAbuseDetectionEnforcementArchitecture
)


# ============================================================================
# Public exports
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

    "DistributedAbuseState",
    "AbuseSignalFamily",
    "AbuseSignalType",
    "AbuseEvidenceStrength",
    "AbuseRiskBand",
    "EnforcementAction",
    "EnforcementScope",
    "EnforcementDecision",
    "EnforcementConfidence",
    "EvidenceKind",
    "EnforcementEventType",
    "EnforcementCheckpointType",

    "DistributedAbuseIdentity",
    "DistributedAbuseLineage",
    "DistributedAbuseInput",
    "DistributedAbuseHistory",
    "DistributedAbuseSignal",
    "DistributedAbuseEvidence",
    "DistributedEnforcementPolicy",
    "EnforcementPlan",
    "DistributedAbuseCheckpoint",
    "DistributedAbuseEvent",
    "DistributedAbuseResult",

    "DistributedAbuseBackend",
    "InMemoryDistributedAbuseBackend",

    "DistributedAbuseDetectionEnforcementArchitecture",
    "DistributedAbuseDetectionEnforcement",
    "GlobalDistributedAbuseDetectionEnforcement",
    "Phase14_6DistributedAbuseDetectionEnforcement",
    "DistributedAbuseEnforcementArchitecture",
]
