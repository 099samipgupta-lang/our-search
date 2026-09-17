"""
OUR SEARCH
Phase 14.5 — Crawl / Index Abuse & Resource Protection

Purpose
-------
Analyze observable crawl, indexing, resource-consumption, and abuse
indicators so OUR SEARCH can identify resources or behaviors that may
consume disproportionate infrastructure resources, attempt to manipulate
crawl/index systems, create duplicate work, generate excessive variants,
or otherwise abuse search-engine infrastructure.

This stage is an evidence and protection-signal architecture.

It does NOT:
- perform crawling
- perform HTTP requests
- execute remote code
- perform active exploitation
- attack or scan arbitrary systems
- execute malware
- collect credentials
- make final enforcement decisions
- permanently classify resources
- block resources directly
- delete URLs
- mutate the search index
- assign crawler workers
- execute crawler jobs
- directly throttle external websites
- directly quarantine resources
- directly ban domains or hosts
- replace Phase 14.1
- replace Phase 14.2
- replace Phase 14.3
- replace Phase 14.4
- replace later distributed enforcement/security stages
- depend on Google Search
- depend on Google's index
- depend on Google's crawler
- depend on Google's infrastructure
- depend on Google's ranking technology

Scale target
------------
Designed directly for billions to trillions of publicly accessible Web
resources.

The architecture is intentionally:
- distributed-system aware
- partition aware
- shard aware
- deterministic
- provenance preserving
- explainable
- partial-evidence aware
- temporal
- history aware
- cross-resource aware
- resource-cost aware
- duplicate-work aware
- failure aware
- checkpointable
- restart safe
- framework neutral
- standard-library only

Important boundary
------------------
Risk values produced here are evidence aggregates for downstream
protection systems. They are NOT permanent classifications and are NOT
direct blocking or enforcement decisions.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Iterable, List, Mapping, Optional, Protocol, Sequence, Tuple


# ---------------------------------------------------------------------------
# Architecture constants
# ---------------------------------------------------------------------------

SCALE_TARGET = "billions_to_trillions_of_public_web_resources"
GOOGLE_SCALE_CAPABILITY_TARGET = True
GOOGLE_TECHNOLOGY_DEPENDENCY = False

ARCHITECTURE_VERSION = "crawl-index-abuse-resource-protection.v1"
PHASE = "14.5"
PREVIOUS_STAGE = "14.4"
NEXT_STAGE = "14.6"

PHASE_NAME = "Crawl / Index Abuse & Resource Protection"
NEXT_STAGE_NAME = "Distributed Abuse Detection & Enforcement"


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class ResourceProtectionState(str, Enum):
    RECEIVED = "received"
    VALIDATING = "validating"
    NORMALIZING = "normalizing"
    URL_ANALYSIS = "url_analysis"
    REQUEST_PATTERN_ANALYSIS = "request_pattern_analysis"
    CRAWL_BEHAVIOR_ANALYSIS = "crawl_behavior_analysis"
    DUPLICATE_WORK_ANALYSIS = "duplicate_work_analysis"
    URL_VARIANT_ANALYSIS = "url_variant_analysis"
    PARAMETER_ANALYSIS = "parameter_analysis"
    DISCOVERY_ABUSE_ANALYSIS = "discovery_abuse_analysis"
    CRAWL_LOOP_ANALYSIS = "crawl_loop_analysis"
    REDIRECT_LOOP_ANALYSIS = "redirect_loop_analysis"
    RESOURCE_COST_ANALYSIS = "resource_cost_analysis"
    INDEX_ABUSE_ANALYSIS = "index_abuse_analysis"
    INDEX_VARIANT_ANALYSIS = "index_variant_analysis"
    DUPLICATE_INDEX_ANALYSIS = "duplicate_index_analysis"
    HOST_RESOURCE_ANALYSIS = "host_resource_analysis"
    DOMAIN_RESOURCE_ANALYSIS = "domain_resource_analysis"
    CROSS_RESOURCE_ANALYSIS = "cross_resource_analysis"
    TEMPORAL_ANALYSIS = "temporal_analysis"
    HISTORY_ANALYSIS = "history_analysis"
    SIGNAL_AGGREGATION = "signal_aggregation"
    CONFIDENCE_CALCULATION = "confidence_calculation"
    DECISION_READY = "decision_ready"
    COMPLETED = "completed"
    PARTIAL = "partial"
    DEFERRED = "deferred"
    REJECTED = "rejected"
    FAILED = "failed"


class ProtectionSignalFamily(str, Enum):
    URL_STRUCTURE = "url_structure"
    URL_VARIANT = "url_variant"
    QUERY_PARAMETER = "query_parameter"
    CRAWL_BEHAVIOR = "crawl_behavior"
    REQUEST_PATTERN = "request_pattern"
    DUPLICATE_WORK = "duplicate_work"
    DISCOVERY = "discovery"
    LOOP = "loop"
    REDIRECT = "redirect"
    RESOURCE_COST = "resource_cost"
    BANDWIDTH = "bandwidth"
    COMPUTE = "compute"
    STORAGE = "storage"
    INDEX = "index"
    INDEX_VARIANT = "index_variant"
    DUPLICATION = "duplication"
    HOST = "host"
    DOMAIN = "domain"
    CROSS_RESOURCE = "cross_resource"
    TEMPORAL = "temporal"
    HISTORICAL = "historical"
    TECHNICAL = "technical"
    SOURCE = "source"
    IDENTITY = "identity"


class ProtectionSignalType(str, Enum):
    EXCESSIVE_URL_VARIANTS = "excessive_url_variants"
    HIGH_QUERY_PARAMETER_CARDINALITY = "high_query_parameter_cardinality"
    HIGH_PATH_VARIANT_CARDINALITY = "high_path_variant_cardinality"
    HIGH_FRAGMENT_VARIATION = "high_fragment_variation"

    DUPLICATE_URL_WORK = "duplicate_url_work"
    DUPLICATE_FETCH_WORK = "duplicate_fetch_work"
    DUPLICATE_DISCOVERY_WORK = "duplicate_discovery_work"

    CRAWL_LOOP_PATTERN = "crawl_loop_pattern"
    REDIRECT_LOOP_PATTERN = "redirect_loop_pattern"
    DISCOVERY_LOOP_PATTERN = "discovery_loop_pattern"

    EXCESSIVE_REDIRECT_VARIANTS = "excessive_redirect_variants"
    REDIRECT_DESTINATION_VARIATION = "redirect_destination_variation"

    EXCESSIVE_DISCOVERY_OUTPUT = "excessive_discovery_output"
    EXCESSIVE_URL_GENERATION = "excessive_url_generation"
    URL_GENERATION_BURST = "url_generation_burst"

    CRAWL_FRONTIER_PRESSURE = "crawl_frontier_pressure"
    CRAWL_QUEUE_PRESSURE = "crawl_queue_pressure"
    REPEATED_CRAWL_PRESSURE = "repeated_crawl_pressure"

    EXCESSIVE_RESPONSE_SIZE = "excessive_response_size"
    EXCESSIVE_BANDWIDTH_COST = "excessive_bandwidth_cost"
    EXCESSIVE_COMPUTE_COST = "excessive_compute_cost"
    EXCESSIVE_STORAGE_COST = "excessive_storage_cost"

    RESOURCE_COST_ANOMALY = "resource_cost_anomaly"
    RESOURCE_COST_SPIKE = "resource_cost_spike"

    INDEX_VARIANT_EXPLOSION = "index_variant_explosion"
    INDEX_DUPLICATE_EXPLOSION = "index_duplicate_explosion"
    INDEXABLE_VARIANT_ANOMALY = "indexable_variant_anomaly"

    DUPLICATE_CONTENT_WORK = "duplicate_content_work"
    DUPLICATE_DOCUMENT_WORK = "duplicate_document_work"
    NEAR_DUPLICATE_WORK = "near_duplicate_work"

    PARAMETER_COMBINATION_EXPLOSION = "parameter_combination_explosion"
    PARAMETER_VARIANT_ANOMALY = "parameter_variant_anomaly"

    HOST_RESOURCE_PRESSURE = "host_resource_pressure"
    DOMAIN_RESOURCE_PRESSURE = "domain_resource_pressure"
    SHARD_RESOURCE_PRESSURE = "shard_resource_pressure"

    CROSS_RESOURCE_ABUSE_CLUSTER = "cross_resource_abuse_cluster"
    CROSS_DOMAIN_ABUSE_CLUSTER = "cross_domain_abuse_cluster"
    INFRASTRUCTURE_REUSE_PATTERN = "infrastructure_reuse_pattern"

    TEMPORAL_ABUSE_SPIKE = "temporal_abuse_spike"
    TEMPORAL_RESOURCE_ANOMALY = "temporal_resource_anomaly"
    HISTORICAL_ABUSE_PATTERN = "historical_abuse_pattern"
    RECURRING_ABUSE_PATTERN = "recurring_abuse_pattern"

    SOURCE_RELIABILITY_ANOMALY = "source_reliability_anomaly"
    IDENTITY_INCONSISTENCY = "identity_inconsistency"
    TECHNICAL_RESOURCE_ANOMALY = "technical_resource_anomaly"

    CRAWL_POLICY_BYPASS_INDICATOR = "crawl_policy_bypass_indicator"
    INDEX_POLICY_BYPASS_INDICATOR = "index_policy_bypass_indicator"
    RESOURCE_PROTECTION_BYPASS_INDICATOR = "resource_protection_bypass_indicator"


class ProtectionEvidenceStrength(str, Enum):
    NONE = "none"
    WEAK = "weak"
    MODERATE = "moderate"
    STRONG = "strong"
    VERY_STRONG = "very_strong"


class ProtectionRiskBand(str, Enum):
    NONE = "none"
    LOW = "low"
    MODERATE = "moderate"
    HIGH = "high"
    VERY_HIGH = "very_high"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


class ProtectionDecision(str, Enum):
    EVIDENCE_READY = "evidence_ready"
    REVIEW_REQUIRED = "review_required"
    PARTIAL_EVIDENCE = "partial_evidence"
    DEFERRED = "deferred"
    REJECTED_INPUT = "rejected_input"


class ProtectionEvidenceKind(str, Enum):
    URL = "url"
    CRAWL = "crawl"
    REQUEST = "request"
    DISCOVERY = "discovery"
    DUPLICATION = "duplication"
    LOOP = "loop"
    RESOURCE_COST = "resource_cost"
    INDEX = "index"
    HOST = "host"
    DOMAIN = "domain"
    CROSS_RESOURCE = "cross_resource"
    TEMPORAL = "temporal"
    HISTORICAL = "historical"
    TECHNICAL = "technical"
    SOURCE = "source"
    IDENTITY = "identity"


class ProtectionEventType(str, Enum):
    ANALYSIS_STARTED = "analysis_started"
    INPUT_NORMALIZED = "input_normalized"
    SIGNAL_DETECTED = "signal_detected"
    SIGNAL_AGGREGATED = "signal_aggregated"
    PARTIAL_EVIDENCE = "partial_evidence"
    CONFIDENCE_CALCULATED = "confidence_calculated"
    DECISION_PREPARED = "decision_prepared"
    ANALYSIS_COMPLETED = "analysis_completed"
    ANALYSIS_DEFERRED = "analysis_deferred"
    ANALYSIS_REJECTED = "analysis_rejected"
    ANALYSIS_FAILED = "analysis_failed"


class ProtectionCheckpointType(str, Enum):
    INPUT_ACCEPTED = "input_accepted"
    NORMALIZATION_COMPLETE = "normalization_complete"
    SIGNAL_EXTRACTION_COMPLETE = "signal_extraction_complete"
    AGGREGATION_COMPLETE = "aggregation_complete"
    CONFIDENCE_COMPLETE = "confidence_complete"
    RESULT_COMPLETE = "result_complete"


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ResourceProtectionIdentity:
    resource_id: str
    document_id: str = ""
    url: str = ""
    host_id: str = ""
    domain_id: str = ""
    partition_id: str = ""
    shard_id: str = ""
    region_id: str = ""
    version: str = "1"


@dataclass(frozen=True)
class ResourceProtectionLineage:
    resource_id: str
    current_stage: str = PHASE
    previous_stage: str = PREVIOUS_STAGE
    previous_stage_version: str = ""
    source_evidence_ids: Tuple[str, ...] = ()
    source_evidence_versions: Tuple[str, ...] = ()
    parent_resource_ids: Tuple[str, ...] = ()
    parent_document_ids: Tuple[str, ...] = ()
    lineage_metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass
class ResourceProtectionInput:
    identity: ResourceProtectionIdentity

    title: str = ""
    content_hash: str = ""
    canonical_url: str = ""
    domain: str = ""
    host: str = ""

    path_depth: int = 0
    query_parameter_count: int = 0
    unique_query_parameter_count: int = 0
    query_value_cardinality: int = 0
    path_variant_count: int = 0
    url_variant_count: int = 0
    fragment_variant_count: int = 0

    redirect_count: int = 0
    redirect_variant_count: int = 0
    redirect_loop_detected: bool = False

    discovered_url_count: int = 0
    generated_url_count: int = 0
    crawl_attempt_count: int = 0
    repeated_crawl_count: int = 0
    duplicate_work_count: int = 0
    duplicate_fetch_count: int = 0
    duplicate_discovery_count: int = 0

    response_size_bytes: int = 0
    bandwidth_cost_bytes: int = 0
    compute_cost_units: float = 0.0
    storage_cost_bytes: int = 0

    index_variant_count: int = 0
    duplicate_document_count: int = 0
    near_duplicate_document_count: int = 0
    indexable_variant_count: int = 0

    parameter_combination_count: int = 0

    host_resource_pressure: float = 0.0
    domain_resource_pressure: float = 0.0
    shard_resource_pressure: float = 0.0

    crawl_frontier_pressure: float = 0.0
    crawl_queue_pressure: float = 0.0

    source_confidence: float = 1.0

    policy_bypass_indicators: int = 0
    technical_anomaly_indicators: int = 0

    cross_resource_cluster_size: int = 0
    cross_domain_cluster_size: int = 0
    infrastructure_reuse_count: int = 0

    observation_timestamp: Optional[str] = None

    partial: bool = False
    missing_fields: Tuple[str, ...] = ()

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass
class ResourceProtectionHistory:
    previous_analysis_count: int = 0
    previous_abuse_evidence_count: int = 0
    previous_high_risk_count: int = 0

    historical_variant_counts: Tuple[int, ...] = ()
    historical_crawl_counts: Tuple[int, ...] = ()
    historical_resource_costs: Tuple[float, ...] = ()
    historical_duplicate_counts: Tuple[int, ...] = ()

    previous_decisions: Tuple[str, ...] = ()
    change_intervals_seconds: Tuple[float, ...] = ()

    recurring_pattern_count: int = 0
    last_observation_timestamp: Optional[str] = None

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ProtectionSignal:
    signal_id: str
    resource_id: str

    family: ProtectionSignalFamily
    signal_type: ProtectionSignalType

    strength: ProtectionEvidenceStrength
    value: float
    confidence: float

    evidence_kind: ProtectionEvidenceKind

    description: str = ""
    source: str = ""
    source_version: str = ""

    observed_at: Optional[str] = None

    partial: bool = False
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ProtectionEvidence:
    evidence_id: str
    resource_id: str

    kind: ProtectionEvidenceKind
    strength: ProtectionEvidenceStrength

    phishing_relevant: bool = False
    malware_relevant: bool = False
    abuse_relevant: bool = True
    resource_protection_relevant: bool = True

    signal_ids: Tuple[str, ...] = ()

    confidence: float = 0.0
    score: float = 0.0

    explanation: str = ""
    provenance: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ResourceProtectionPolicy:
    max_batch_size: int = 100_000
    max_signals_per_resource: int = 512

    minimum_confidence: float = 0.10

    low_risk_threshold: float = 0.15
    moderate_risk_threshold: float = 0.35
    high_risk_threshold: float = 0.60
    very_high_risk_threshold: float = 0.80
    critical_risk_threshold: float = 0.93

    excessive_url_variant_threshold: int = 10_000
    excessive_parameter_threshold: int = 1_000
    excessive_discovery_threshold: int = 10_000
    excessive_duplicate_work_threshold: int = 1_000
    excessive_redirect_threshold: int = 25

    excessive_response_size_bytes: int = 50_000_000
    excessive_bandwidth_bytes: int = 100_000_000
    excessive_storage_bytes: int = 100_000_000

    excessive_compute_units: float = 10_000.0

    excessive_index_variants: int = 10_000
    excessive_duplicate_documents: int = 5_000

    high_pressure_threshold: float = 0.75
    critical_pressure_threshold: float = 0.93

    cross_resource_cluster_threshold: int = 100
    cross_domain_cluster_threshold: int = 25
    infrastructure_reuse_threshold: int = 100

    temporal_spike_multiplier: float = 5.0

    allow_partial: bool = True
    deterministic: bool = True

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ProtectionCheckpoint:
    checkpoint_id: str
    resource_id: str

    checkpoint_type: ProtectionCheckpointType

    created_at: str

    signal_count: int = 0
    evidence_count: int = 0

    partial: bool = False

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ProtectionEvent:
    event_id: str
    resource_id: str

    event_type: ProtectionEventType

    timestamp: str

    state: ResourceProtectionState

    message: str = ""

    signal_id: str = ""
    checkpoint_id: str = ""

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ResourceProtectionResult:
    identity: ResourceProtectionIdentity
    lineage: ResourceProtectionLineage

    resource_protection_risk: float
    crawl_abuse_risk: float
    index_abuse_risk: float
    resource_cost_risk: float
    duplicate_work_risk: float
    variant_explosion_risk: float
    loop_risk: float
    cross_resource_risk: float
    temporal_risk: float

    overall_protection_risk: float

    risk_band: ProtectionRiskBand
    confidence: float

    evidence: Tuple[ProtectionEvidence, ...]
    signals: Tuple[ProtectionSignal, ...]

    decision: ProtectionDecision

    reasons: Tuple[str, ...]

    partial: bool
    missing_fields: Tuple[str, ...]

    created_at: str

    metadata: Mapping[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Backend
# ---------------------------------------------------------------------------


class ResourceProtectionBackend(Protocol):
    def save_result(self, result: ResourceProtectionResult) -> None:
        ...

    def save_event(self, event: ProtectionEvent) -> None:
        ...

    def save_checkpoint(self, checkpoint: ProtectionCheckpoint) -> None:
        ...

    def get_result(self, resource_id: str) -> Optional[ResourceProtectionResult]:
        ...

    def get_events(self, resource_id: str) -> Sequence[ProtectionEvent]:
        ...

    def get_checkpoints(self, resource_id: str) -> Sequence[ProtectionCheckpoint]:
        ...


class InMemoryResourceProtectionBackend:
    """
    Small reference backend.

    This is intentionally an interface-compatible reference implementation.
    Production deployments can replace it with distributed durable storage
    without changing the detection architecture.
    """

    def __init__(self) -> None:
        self._results: Dict[str, ResourceProtectionResult] = {}
        self._events: Dict[str, List[ProtectionEvent]] = {}
        self._checkpoints: Dict[str, List[ProtectionCheckpoint]] = {}

    def save_result(self, result: ResourceProtectionResult) -> None:
        self._results[result.identity.resource_id] = result

    def save_event(self, event: ProtectionEvent) -> None:
        self._events.setdefault(event.resource_id, []).append(event)

    def save_checkpoint(self, checkpoint: ProtectionCheckpoint) -> None:
        self._checkpoints.setdefault(checkpoint.resource_id, []).append(checkpoint)

    def get_result(self, resource_id: str) -> Optional[ResourceProtectionResult]:
        return self._results.get(resource_id)

    def get_events(self, resource_id: str) -> Sequence[ProtectionEvent]:
        return tuple(self._events.get(resource_id, ()))

    def get_checkpoints(self, resource_id: str) -> Sequence[ProtectionCheckpoint]:
        return tuple(self._checkpoints.get(resource_id, ()))


# ---------------------------------------------------------------------------
# Main architecture
# ---------------------------------------------------------------------------


class CrawlIndexAbuseResourceProtectionArchitecture:
    """
    Phase 14.5 — Crawl / Index Abuse & Resource Protection.

    This class converts externally supplied crawl/index/resource observations
    into structured protection evidence.

    It is deliberately not a crawler, indexer, enforcement engine, firewall,
    or malware execution environment.
    """

    def __init__(
        self,
        policy: Optional[ResourceProtectionPolicy] = None,
        backend: Optional[ResourceProtectionBackend] = None,
    ) -> None:
        self.policy = policy or ResourceProtectionPolicy()
        self.backend = backend or InMemoryResourceProtectionBackend()

    # ------------------------------------------------------------------
    # Safe helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _clamp(value: float, minimum: float = 0.0, maximum: float = 1.0) -> float:
        try:
            value = float(value)
        except (TypeError, ValueError):
            return minimum

        if math.isnan(value) or math.isinf(value):
            return minimum

        return max(minimum, min(maximum, value))

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            parsed = float(value)
            if math.isnan(parsed) or math.isinf(parsed):
                return default
            return parsed
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _parse_timestamp(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None

        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)

        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _deterministic_unit(value: str) -> float:
        digest = hashlib.sha256(value.encode("utf-8")).digest()
        integer = int.from_bytes(digest[:8], "big")
        return integer / float(2**64 - 1)

    @staticmethod
    def _digest_payload(payload: Mapping[str, Any]) -> str:
        serialized = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    @staticmethod
    def _signal_id(
        resource_id: str,
        signal_type: ProtectionSignalType,
        value: float,
    ) -> str:
        raw = f"{resource_id}|{signal_type.value}|{value:.8f}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]

    @staticmethod
    def _evidence_id(
        resource_id: str,
        kind: ProtectionEvidenceKind,
        score: float,
    ) -> str:
        raw = f"{resource_id}|{kind.value}|{score:.8f}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]

    def _event(
        self,
        resource_id: str,
        event_type: ProtectionEventType,
        state: ResourceProtectionState,
        message: str = "",
        signal_id: str = "",
        checkpoint_id: str = "",
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> ProtectionEvent:
        event = ProtectionEvent(
            event_id=hashlib.sha256(
                f"{resource_id}|{event_type.value}|{state.value}|{message}".encode(
                    "utf-8"
                )
            ).hexdigest()[:32],
            resource_id=resource_id,
            event_type=event_type,
            timestamp=self._now(),
            state=state,
            message=message,
            signal_id=signal_id,
            checkpoint_id=checkpoint_id,
            metadata=dict(metadata or {}),
        )
        self.backend.save_event(event)
        return event

    def _checkpoint(
        self,
        resource_id: str,
        checkpoint_type: ProtectionCheckpointType,
        signal_count: int,
        evidence_count: int,
        partial: bool,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> ProtectionCheckpoint:
        checkpoint = ProtectionCheckpoint(
            checkpoint_id=hashlib.sha256(
                f"{resource_id}|{checkpoint_type.value}|{signal_count}|{evidence_count}".encode(
                    "utf-8"
                )
            ).hexdigest()[:32],
            resource_id=resource_id,
            checkpoint_type=checkpoint_type,
            created_at=self._now(),
            signal_count=signal_count,
            evidence_count=evidence_count,
            partial=partial,
            metadata=dict(metadata or {}),
        )
        self.backend.save_checkpoint(checkpoint)
        return checkpoint

    # ------------------------------------------------------------------
    # Input normalization and validation
    # ------------------------------------------------------------------

    def validate_input(self, item: ResourceProtectionInput) -> Tuple[bool, Tuple[str, ...]]:
        errors: List[str] = []

        if not item.identity.resource_id:
            errors.append("missing_resource_id")

        if item.path_depth < 0:
            errors.append("negative_path_depth")

        numeric_fields = (
            "query_parameter_count",
            "unique_query_parameter_count",
            "query_value_cardinality",
            "path_variant_count",
            "url_variant_count",
            "fragment_variant_count",
            "redirect_count",
            "redirect_variant_count",
            "discovered_url_count",
            "generated_url_count",
            "crawl_attempt_count",
            "repeated_crawl_count",
            "duplicate_work_count",
            "duplicate_fetch_count",
            "duplicate_discovery_count",
            "response_size_bytes",
            "bandwidth_cost_bytes",
            "storage_cost_bytes",
            "index_variant_count",
            "duplicate_document_count",
            "near_duplicate_document_count",
            "indexable_variant_count",
            "parameter_combination_count",
            "cross_resource_cluster_size",
            "cross_domain_cluster_size",
            "infrastructure_reuse_count",
        )

        for field_name in numeric_fields:
            if getattr(item, field_name) < 0:
                errors.append(f"negative_{field_name}")

        float_fields = (
            "compute_cost_units",
            "host_resource_pressure",
            "domain_resource_pressure",
            "shard_resource_pressure",
            "crawl_frontier_pressure",
            "crawl_queue_pressure",
            "source_confidence",
        )

        for field_name in float_fields:
            value = self._safe_float(getattr(item, field_name), 0.0)
            if value < 0:
                errors.append(f"negative_{field_name}")

        return not errors, tuple(errors)

    def normalize_input(self, item: ResourceProtectionInput) -> ResourceProtectionInput:
        identity = item.identity

        item.identity = ResourceProtectionIdentity(
            resource_id=str(identity.resource_id or ""),
            document_id=str(identity.document_id or ""),
            url=str(identity.url or "").strip(),
            host_id=str(identity.host_id or ""),
            domain_id=str(identity.domain_id or ""),
            partition_id=str(identity.partition_id or ""),
            shard_id=str(identity.shard_id or ""),
            region_id=str(identity.region_id or ""),
            version=str(identity.version or "1"),
        )

        item.title = str(item.title or "").strip()
        item.content_hash = str(item.content_hash or "").strip()
        item.canonical_url = str(item.canonical_url or "").strip()
        item.domain = str(item.domain or "").strip().lower()
        item.host = str(item.host or "").strip().lower()

        integer_fields = (
            "path_depth",
            "query_parameter_count",
            "unique_query_parameter_count",
            "query_value_cardinality",
            "path_variant_count",
            "url_variant_count",
            "fragment_variant_count",
            "redirect_count",
            "redirect_variant_count",
            "discovered_url_count",
            "generated_url_count",
            "crawl_attempt_count",
            "repeated_crawl_count",
            "duplicate_work_count",
            "duplicate_fetch_count",
            "duplicate_discovery_count",
            "response_size_bytes",
            "bandwidth_cost_bytes",
            "storage_cost_bytes",
            "index_variant_count",
            "duplicate_document_count",
            "near_duplicate_document_count",
            "indexable_variant_count",
            "parameter_combination_count",
            "policy_bypass_indicators",
            "technical_anomaly_indicators",
            "cross_resource_cluster_size",
            "cross_domain_cluster_size",
            "infrastructure_reuse_count",
        )

        for field_name in integer_fields:
            setattr(item, field_name, max(0, self._safe_int(getattr(item, field_name))))

        float_fields = (
            "compute_cost_units",
            "host_resource_pressure",
            "domain_resource_pressure",
            "shard_resource_pressure",
            "crawl_frontier_pressure",
            "crawl_queue_pressure",
        )

        for field_name in float_fields:
            setattr(
                item,
                field_name,
                max(0.0, self._safe_float(getattr(item, field_name))),
            )

        item.source_confidence = self._clamp(
            self._safe_float(item.source_confidence, 1.0)
        )

        return item

    # ------------------------------------------------------------------
    # Signal construction
    # ------------------------------------------------------------------

    def _make_signal(
        self,
        item: ResourceProtectionInput,
        family: ProtectionSignalFamily,
        signal_type: ProtectionSignalType,
        value: float,
        confidence: float,
        evidence_kind: ProtectionEvidenceKind,
        description: str,
        source: str = "phase14.5",
        partial: Optional[bool] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> ProtectionSignal:
        value = self._clamp(value)
        confidence = self._clamp(confidence)

        if value <= 0.0:
            strength = ProtectionEvidenceStrength.NONE
        elif value < 0.25:
            strength = ProtectionEvidenceStrength.WEAK
        elif value < 0.50:
            strength = ProtectionEvidenceStrength.MODERATE
        elif value < 0.80:
            strength = ProtectionEvidenceStrength.STRONG
        else:
            strength = ProtectionEvidenceStrength.VERY_STRONG

        signal = ProtectionSignal(
            signal_id=self._signal_id(
                item.identity.resource_id,
                signal_type,
                value,
            ),
            resource_id=item.identity.resource_id,
            family=family,
            signal_type=signal_type,
            strength=strength,
            value=value,
            confidence=confidence,
            evidence_kind=evidence_kind,
            description=description,
            source=source,
            source_version=ARCHITECTURE_VERSION,
            observed_at=item.observation_timestamp,
            partial=item.partial if partial is None else partial,
            metadata=dict(metadata or {}),
        )

        return signal

    # ------------------------------------------------------------------
    # URL / variant analysis
    # ------------------------------------------------------------------

    def analyze_url_structure(
        self,
        item: ResourceProtectionInput,
    ) -> List[ProtectionSignal]:
        signals: List[ProtectionSignal] = []

        if item.url_variant_count > self.policy.excessive_url_variant_threshold:
            ratio = item.url_variant_count / max(
                1.0,
                float(self.policy.excessive_url_variant_threshold),
            )

            value = self._clamp(math.log1p(ratio) / math.log1p(20.0))

            signals.append(
                self._make_signal(
                    item,
                    ProtectionSignalFamily.URL_VARIANT,
                    ProtectionSignalType.EXCESSIVE_URL_VARIANTS,
                    value,
                    item.source_confidence,
                    ProtectionEvidenceKind.URL,
                    "Observed URL variant cardinality is unusually high.",
                )
            )

        if item.path_variant_count > self.policy.excessive_url_variant_threshold:
            ratio = item.path_variant_count / max(
                1.0,
                float(self.policy.excessive_url_variant_threshold),
            )

            value = self._clamp(math.log1p(ratio) / math.log1p(20.0))

            signals.append(
                self._make_signal(
                    item,
                    ProtectionSignalFamily.URL_VARIANT,
                    ProtectionSignalType.HIGH_PATH_VARIANT_CARDINALITY,
                    value,
                    item.source_confidence,
                    ProtectionEvidenceKind.URL,
                    "Observed path variant cardinality is unusually high.",
                )
            )

        if item.fragment_variant_count > self.policy.excessive_url_variant_threshold:
            ratio = item.fragment_variant_count / max(
                1.0,
                float(self.policy.excessive_url_variant_threshold),
            )

            value = self._clamp(math.log1p(ratio) / math.log1p(20.0))

            signals.append(
                self._make_signal(
                    item,
                    ProtectionSignalFamily.URL_VARIANT,
                    ProtectionSignalType.HIGH_FRAGMENT_VARIATION,
                    value,
                    item.source_confidence,
                    ProtectionEvidenceKind.URL,
                    "Observed fragment variation is unusually high.",
                )
            )

        return signals

    def analyze_parameters(
        self,
        item: ResourceProtectionInput,
    ) -> List[ProtectionSignal]:
        signals: List[ProtectionSignal] = []

        if item.query_parameter_count > self.policy.excessive_parameter_threshold:
            ratio = item.query_parameter_count / max(
                1.0,
                float(self.policy.excessive_parameter_threshold),
            )

            value = self._clamp(math.log1p(ratio) / math.log1p(20.0))

            signals.append(
                self._make_signal(
                    item,
                    ProtectionSignalFamily.QUERY_PARAMETER,
                    ProtectionSignalType.HIGH_QUERY_PARAMETER_CARDINALITY,
                    value,
                    item.source_confidence,
                    ProtectionEvidenceKind.URL,
                    "Observed query-parameter cardinality is unusually high.",
                )
            )

        if item.parameter_combination_count > self.policy.excessive_parameter_threshold:
            ratio = item.parameter_combination_count / max(
                1.0,
                float(self.policy.excessive_parameter_threshold),
            )

            value = self._clamp(math.log1p(ratio) / math.log1p(20.0))

            signals.append(
                self._make_signal(
                    item,
                    ProtectionSignalFamily.QUERY_PARAMETER,
                    ProtectionSignalType.PARAMETER_COMBINATION_EXPLOSION,
                    value,
                    item.source_confidence,
                    ProtectionEvidenceKind.URL,
                    "Observed parameter combinations create unusually large URL-space cardinality.",
                )
            )

        if (
            item.query_value_cardinality
            > self.policy.excessive_parameter_threshold
        ):
            ratio = item.query_value_cardinality / max(
                1.0,
                float(self.policy.excessive_parameter_threshold),
            )

            value = self._clamp(math.log1p(ratio) / math.log1p(20.0))

            signals.append(
                self._make_signal(
                    item,
                    ProtectionSignalFamily.QUERY_PARAMETER,
                    ProtectionSignalType.PARAMETER_VARIANT_ANOMALY,
                    value,
                    item.source_confidence,
                    ProtectionEvidenceKind.URL,
                    "Observed parameter value cardinality is unusually high.",
                )
            )

        return signals

    # ------------------------------------------------------------------
    # Crawl / request behavior analysis
    # ------------------------------------------------------------------

    def analyze_crawl_behavior(
        self,
        item: ResourceProtectionInput,
    ) -> List[ProtectionSignal]:
        signals: List[ProtectionSignal] = []

        if item.duplicate_work_count > self.policy.excessive_duplicate_work_threshold:
            ratio = item.duplicate_work_count / max(
                1.0,
                float(self.policy.excessive_duplicate_work_threshold),
            )

            value = self._clamp(math.log1p(ratio) / math.log1p(20.0))

            signals.append(
                self._make_signal(
                    item,
                    ProtectionSignalFamily.DUPLICATE_WORK,
                    ProtectionSignalType.DUPLICATE_WORK,
                    value,
                    item.source_confidence,
                    ProtectionEvidenceKind.DUPLICATION,
                    "Observed duplicate crawl work is unusually high.",
                )
            )

        if item.duplicate_fetch_count > self.policy.excessive_duplicate_work_threshold:
            ratio = item.duplicate_fetch_count / max(
                1.0,
                float(self.policy.excessive_duplicate_work_threshold),
            )

            value = self._clamp(math.log1p(ratio) / math.log1p(20.0))

            signals.append(
                self._make_signal(
                    item,
                    ProtectionSignalFamily.DUPLICATE_WORK,
                    ProtectionSignalType.DUPLICATE_FETCH_WORK,
                    value,
                    item.source_confidence,
                    ProtectionEvidenceKind.DUPLICATION,
                    "Observed duplicate fetch work is unusually high.",
                )
            )

        if (
            item.duplicate_discovery_count
            > self.policy.excessive_duplicate_work_threshold
        ):
            ratio = item.duplicate_discovery_count / max(
                1.0,
                float(self.policy.excessive_duplicate_work_threshold),
            )

            value = self._clamp(math.log1p(ratio) / math.log1p(20.0))

            signals.append(
                self._make_signal(
                    item,
                    ProtectionSignalFamily.DUPLICATE_WORK,
                    ProtectionSignalType.DUPLICATE_DISCOVERY_WORK,
                    value,
                    item.source_confidence,
                    ProtectionEvidenceKind.DUPLICATION,
                    "Observed duplicate discovery work is unusually high.",
                )
            )

        if item.discovered_url_count > self.policy.excessive_discovery_threshold:
            ratio = item.discovered_url_count / max(
                1.0,
                float(self.policy.excessive_discovery_threshold),
            )

            value = self._clamp(math.log1p(ratio) / math.log1p(20.0))

            signals.append(
                self._make_signal(
                    item,
                    ProtectionSignalFamily.DISCOVERY,
                    ProtectionSignalType.EXCESSIVE_DISCOVERY_OUTPUT,
                    value,
                    item.source_confidence,
                    ProtectionEvidenceKind.DISCOVERY,
                    "Observed discovery output is unusually large.",
                )
            )

        if item.generated_url_count > self.policy.excessive_discovery_threshold:
            ratio = item.generated_url_count / max(
                1.0,
                float(self.policy.excessive_discovery_threshold),
            )

            value = self._clamp(math.log1p(ratio) / math.log1p(20.0))

            signals.append(
                self._make_signal(
                    item,
                    ProtectionSignalFamily.DISCOVERY,
                    ProtectionSignalType.EXCESSIVE_URL_GENERATION,
                    value,
                    item.source_confidence,
                    ProtectionEvidenceKind.DISCOVERY,
                    "Observed URL-generation output is unusually large.",
                )
            )

        if item.generated_url_count > 0 and item.crawl_attempt_count > 0:
            ratio = item.generated_url_count / max(
                1.0,
                float(item.crawl_attempt_count),
            )

            value = self._clamp(
                math.log1p(ratio) / math.log1p(1000.0)
            )

            if value >= 0.35:
                signals.append(
                    self._make_signal(
                        item,
                        ProtectionSignalFamily.DISCOVERY,
                        ProtectionSignalType.URL_GENERATION_BURST,
                        value,
                        item.source_confidence,
                        ProtectionEvidenceKind.DISCOVERY,
                        "Observed URL generation is disproportionately large relative to crawl activity.",
                    )
                )

        if item.repeated_crawl_count > 0:
            ratio = item.repeated_crawl_count / max(
                1.0,
                float(item.crawl_attempt_count),
            )

            value = self._clamp(ratio)

            if value >= 0.50:
                signals.append(
                    self._make_signal(
                        item,
                        ProtectionSignalFamily.CRAWL_BEHAVIOR,
                        ProtectionSignalType.REPEATED_CRAWL_PRESSURE,
                        value,
                        item.source_confidence,
                        ProtectionEvidenceKind.CRAWL,
                        "Repeated crawl activity represents substantial duplicate pressure.",
                    )
                )

        return signals

    # ------------------------------------------------------------------
    # Loop analysis
    # ------------------------------------------------------------------

    def analyze_loops(
        self,
        item: ResourceProtectionInput,
    ) -> List[ProtectionSignal]:
        signals: List[ProtectionSignal] = []

        if item.redirect_loop_detected:
            signals.append(
                self._make_signal(
                    item,
                    ProtectionSignalFamily.LOOP,
                    ProtectionSignalType.REDIRECT_LOOP_PATTERN,
                    1.0,
                    item.source_confidence,
                    ProtectionEvidenceKind.LOOP,
                    "Externally supplied observations indicate a redirect loop pattern.",
                )
            )

        if item.redirect_count > self.policy.excessive_redirect_threshold:
            ratio = item.redirect_count / max(
                1.0,
                float(self.policy.excessive_redirect_threshold),
            )

            value = self._clamp(math.log1p(ratio) / math.log1p(20.0))

            signals.append(
                self._make_signal(
                    item,
                    ProtectionSignalFamily.REDIRECT,
                    ProtectionSignalType.EXCESSIVE_REDIRECT_VARIANTS,
                    value,
                    item.source_confidence,
                    ProtectionEvidenceKind.REDIRECT if hasattr(
                        ProtectionEvidenceKind,
                        "REDIRECT",
                    )
                    else ProtectionEvidenceKind.LOOP,
                    "Observed redirect-chain length is unusually high.",
                )
            )

        if item.redirect_variant_count > self.policy.excessive_redirect_threshold:
            ratio = item.redirect_variant_count / max(
                1.0,
                float(self.policy.excessive_redirect_threshold),
            )

            value = self._clamp(math.log1p(ratio) / math.log1p(20.0))

            signals.append(
                self._make_signal(
                    item,
                    ProtectionSignalFamily.REDIRECT,
                    ProtectionSignalType.REDIRECT_DESTINATION_VARIATION,
                    value,
                    item.source_confidence,
                    ProtectionEvidenceKind.LOOP,
                    "Observed redirect-destination variation is unusually high.",
                )
            )

        return signals

    # ------------------------------------------------------------------
    # Resource-cost analysis
    # ------------------------------------------------------------------

    def analyze_resource_cost(
        self,
        item: ResourceProtectionInput,
    ) -> List[ProtectionSignal]:
        signals: List[ProtectionSignal] = []

        if item.response_size_bytes > self.policy.excessive_response_size_bytes:
            ratio = item.response_size_bytes / max(
                1.0,
                float(self.policy.excessive_response_size_bytes),
            )

            value = self._clamp(math.log1p(ratio) / math.log1p(20.0))

            signals.append(
                self._make_signal(
                    item,
                    ProtectionSignalFamily.RESOURCE_COST,
                    ProtectionSignalType.EXCESSIVE_RESPONSE_SIZE,
                    value,
                    item.source_confidence,
                    ProtectionEvidenceKind.RESOURCE_COST,
                    "Observed response size is unusually large.",
                )
            )

        if item.bandwidth_cost_bytes > self.policy.excessive_bandwidth_bytes:
            ratio = item.bandwidth_cost_bytes / max(
                1.0,
                float(self.policy.excessive_bandwidth_bytes),
            )

            value = self._clamp(math.log1p(ratio) / math.log1p(20.0))

            signals.append(
                self._make_signal(
                    item,
                    ProtectionSignalFamily.BANDWIDTH,
                    ProtectionSignalType.EXCESSIVE_BANDWIDTH_COST,
                    value,
                    item.source_confidence,
                    ProtectionEvidenceKind.RESOURCE_COST,
                    "Observed bandwidth cost is unusually large.",
                )
            )

        if item.compute_cost_units > self.policy.excessive_compute_units:
            ratio = item.compute_cost_units / max(
                1.0,
                float(self.policy.excessive_compute_units),
            )

            value = self._clamp(math.log1p(ratio) / math.log1p(20.0))

            signals.append(
                self._make_signal(
                    item,
                    ProtectionSignalFamily.COMPUTE,
                    ProtectionSignalType.EXCESSIVE_COMPUTE_COST,
                    value,
                    item.source_confidence,
                    ProtectionEvidenceKind.RESOURCE_COST,
                    "Observed compute cost is unusually large.",
                )
            )

        if item.storage_cost_bytes > self.policy.excessive_storage_bytes:
            ratio = item.storage_cost_bytes / max(
                1.0,
                float(self.policy.excessive_storage_bytes),
            )

            value = self._clamp(math.log1p(ratio) / math.log1p(20.0))

            signals.append(
                self._make_signal(
                    item,
                    ProtectionSignalFamily.STORAGE,
                    ProtectionSignalType.EXCESSIVE_STORAGE_COST,
                    value,
                    item.source_confidence,
                    ProtectionEvidenceKind.RESOURCE_COST,
                    "Observed storage cost is unusually large.",
                )
            )

        pressures = (
            item.host_resource_pressure,
            item.domain_resource_pressure,
            item.shard_resource_pressure,
            item.crawl_frontier_pressure,
            item.crawl_queue_pressure,
        )

        pressure = max(pressures)

        if pressure >= self.policy.high_pressure_threshold:
            signal_type = (
                ProtectionSignalType.RESOURCE_COST_SPIKE
                if pressure >= self.policy.critical_pressure_threshold
                else ProtectionSignalType.RESOURCE_COST_ANOMALY
            )

            signals.append(
                self._make_signal(
                    item,
                    ProtectionSignalFamily.RESOURCE_COST,
                    signal_type,
                    self._clamp(pressure),
                    item.source_confidence,
                    ProtectionEvidenceKind.RESOURCE_COST,
                    "Observed infrastructure resource pressure is unusually high.",
                )
            )

        return signals

    # ------------------------------------------------------------------
    # Queue/frontier pressure
    # ------------------------------------------------------------------

    def analyze_frontier_pressure(
        self,
        item: ResourceProtectionInput,
    ) -> List[ProtectionSignal]:
        signals: List[ProtectionSignal] = []

        if item.crawl_frontier_pressure >= self.policy.high_pressure_threshold:
            signals.append(
                self._make_signal(
                    item,
                    ProtectionSignalFamily.CRAWL_BEHAVIOR,
                    ProtectionSignalType.CRAWL_FRONTIER_PRESSURE,
                    self._clamp(item.crawl_frontier_pressure),
                    item.source_confidence,
                    ProtectionEvidenceKind.RESOURCE_COST,
                    "Observed crawl-frontier pressure is high.",
                )
            )

        if item.crawl_queue_pressure >= self.policy.high_pressure_threshold:
            signals.append(
                self._make_signal(
                    item,
                    ProtectionSignalFamily.CRAWL_BEHAVIOR,
                    ProtectionSignalType.CRAWL_QUEUE_PRESSURE,
                    self._clamp(item.crawl_queue_pressure),
                    item.source_confidence,
                    ProtectionEvidenceKind.RESOURCE_COST,
                    "Observed crawl-queue pressure is high.",
                )
            )

        if item.host_resource_pressure >= self.policy.high_pressure_threshold:
            signals.append(
                self._make_signal(
                    item,
                    ProtectionSignalFamily.HOST,
                    ProtectionSignalType.HOST_RESOURCE_PRESSURE,
                    self._clamp(item.host_resource_pressure),
                    item.source_confidence,
                    ProtectionEvidenceKind.HOST,
                    "Observed host-level resource pressure is high.",
                )
            )

        if item.domain_resource_pressure >= self.policy.high_pressure_threshold:
            signals.append(
                self._make_signal(
                    item,
                    ProtectionSignalFamily.DOMAIN,
                    ProtectionSignalType.DOMAIN_RESOURCE_PRESSURE,
                    self._clamp(item.domain_resource_pressure),
                    item.source_confidence,
                    ProtectionEvidenceKind.DOMAIN,
                    "Observed domain-level resource pressure is high.",
                )
            )

        if item.shard_resource_pressure >= self.policy.high_pressure_threshold:
            signals.append(
                self._make_signal(
                    item,
                    ProtectionSignalFamily.HOST,
                    ProtectionSignalType.SHARD_RESOURCE_PRESSURE,
                    self._clamp(item.shard_resource_pressure),
                    item.source_confidence,
                    ProtectionEvidenceKind.RESOURCE_COST,
                    "Observed shard-level resource pressure is high.",
                )
            )

        return signals

    # ------------------------------------------------------------------
    # Index abuse analysis
    # ------------------------------------------------------------------

    def analyze_index_abuse(
        self,
        item: ResourceProtectionInput,
    ) -> List[ProtectionSignal]:
        signals: List[ProtectionSignal] = []

        if item.index_variant_count > self.policy.excessive_index_variants:
            ratio = item.index_variant_count / max(
                1.0,
                float(self.policy.excessive_index_variants),
            )

            value = self._clamp(math.log1p(ratio) / math.log1p(20.0))

            signals.append(
                self._make_signal(
                    item,
                    ProtectionSignalFamily.INDEX_VARIANT,
                    ProtectionSignalType.INDEX_VARIANT_EXPLOSION,
                    value,
                    item.source_confidence,
                    ProtectionEvidenceKind.INDEX,
                    "Observed indexable URL/document variants are unusually numerous.",
                )
            )

        if item.duplicate_document_count > self.policy.excessive_duplicate_documents:
            ratio = item.duplicate_document_count / max(
                1.0,
                float(self.policy.excessive_duplicate_documents),
            )

            value = self._clamp(math.log1p(ratio) / math.log1p(20.0))

            signals.append(
                self._make_signal(
                    item,
                    ProtectionSignalFamily.INDEX,
                    ProtectionSignalType.INDEX_DUPLICATE_EXPLOSION,
                    value,
                    item.source_confidence,
                    ProtectionEvidenceKind.INDEX,
                    "Observed duplicate documents are unusually numerous.",
                )
            )

        if item.indexable_variant_count > self.policy.excessive_index_variants:
            ratio = item.indexable_variant_count / max(
                1.0,
                float(self.policy.excessive_index_variants),
            )

            value = self._clamp(math.log1p(ratio) / math.log1p(20.0))

            signals.append(
                self._make_signal(
                    item,
                    ProtectionSignalFamily.INDEX_VARIANT,
                    ProtectionSignalType.INDEXABLE_VARIANT_ANOMALY,
                    value,
                    item.source_confidence,
                    ProtectionEvidenceKind.INDEX,
                    "Observed indexable variants create unusually high indexing pressure.",
                )
            )

        if item.duplicate_document_count > 0:
            ratio = item.duplicate_document_count / max(
                1.0,
                float(item.indexable_variant_count),
            )

            value = self._clamp(ratio)

            if value >= 0.50:
                signals.append(
                    self._make_signal(
                        item,
                        ProtectionSignalFamily.DUPLICATION,
                        ProtectionSignalType.DUPLICATE_DOCUMENT_WORK,
                        value,
                        item.source_confidence,
                        ProtectionEvidenceKind.DUPLICATION,
                        "A large share of observed indexable variants correspond to duplicate documents.",
                    )
                )

        if item.near_duplicate_document_count > 0:
            ratio = item.near_duplicate_document_count / max(
                1.0,
                float(item.indexable_variant_count),
            )

            value = self._clamp(ratio)

            if value >= 0.50:
                signals.append(
                    self._make_signal(
                        item,
                        ProtectionSignalFamily.DUPLICATION,
                        ProtectionSignalType.NEAR_DUPLICATE_WORK,
                        value,
                        item.source_confidence,
                        ProtectionEvidenceKind.DUPLICATION,
                        "A large share of observed indexable variants correspond to near-duplicate documents.",
                    )
                )

        return signals

    # ------------------------------------------------------------------
    # Cross-resource analysis
    # ------------------------------------------------------------------

    def analyze_cross_resource(
        self,
        item: ResourceProtectionInput,
    ) -> List[ProtectionSignal]:
        signals: List[ProtectionSignal] = []

        if (
            item.cross_resource_cluster_size
            >= self.policy.cross_resource_cluster_threshold
        ):
            ratio = item.cross_resource_cluster_size / max(
                1.0,
                float(self.policy.cross_resource_cluster_threshold),
            )

            value = self._clamp(math.log1p(ratio) / math.log1p(20.0))

            signals.append(
                self._make_signal(
                    item,
                    ProtectionSignalFamily.CROSS_RESOURCE,
                    ProtectionSignalType.CROSS_RESOURCE_ABUSE_CLUSTER,
                    value,
                    item.source_confidence,
                    ProtectionEvidenceKind.CROSS_RESOURCE,
                    "The resource is associated with a large cross-resource protection-evidence cluster.",
                )
            )

        if (
            item.cross_domain_cluster_size
            >= self.policy.cross_domain_cluster_threshold
        ):
            ratio = item.cross_domain_cluster_size / max(
                1.0,
                float(self.policy.cross_domain_cluster_threshold),
            )

            value = self._clamp(math.log1p(ratio) / math.log1p(20.0))

            signals.append(
                self._make_signal(
                    item,
                    ProtectionSignalFamily.CROSS_RESOURCE,
                    ProtectionSignalType.CROSS_DOMAIN_ABUSE_CLUSTER,
                    value,
                    item.source_confidence,
                    ProtectionEvidenceKind.CROSS_RESOURCE,
                    "The resource is associated with a large cross-domain protection-evidence cluster.",
                )
            )

        if item.infrastructure_reuse_count >= self.policy.infrastructure_reuse_threshold:
            ratio = item.infrastructure_reuse_count / max(
                1.0,
                float(self.policy.infrastructure_reuse_threshold),
            )

            value = self._clamp(math.log1p(ratio) / math.log1p(20.0))

            signals.append(
                self._make_signal(
                    item,
                    ProtectionSignalFamily.CROSS_RESOURCE,
                    ProtectionSignalType.INFRASTRUCTURE_REUSE_PATTERN,
                    value,
                    item.source_confidence,
                    ProtectionEvidenceKind.CROSS_RESOURCE,
                    "Observed infrastructure reuse is unusually widespread.",
                )
            )

        return signals

    # ------------------------------------------------------------------
    # Temporal / historical analysis
    # ------------------------------------------------------------------

    def analyze_temporal_history(
        self,
        item: ResourceProtectionInput,
        history: Optional[ResourceProtectionHistory],
    ) -> List[ProtectionSignal]:
        signals: List[ProtectionSignal] = []

        if history is None:
            return signals

        current_variants = float(item.url_variant_count)
        historical_variants = tuple(
            float(value) for value in history.historical_variant_counts
        )

        if historical_variants:
            baseline = sum(historical_variants) / len(historical_variants)

            if baseline > 0 and current_variants >= baseline * self.policy.temporal_spike_multiplier:
                ratio = current_variants / baseline

                value = self._clamp(
                    math.log1p(ratio) / math.log1p(50.0)
                )

                signals.append(
                    self._make_signal(
                        item,
                        ProtectionSignalFamily.TEMPORAL,
                        ProtectionSignalType.TEMPORAL_ABUSE_SPIKE,
                        value,
                        item.source_confidence,
                        ProtectionEvidenceKind.TEMPORAL,
                        "Current URL-variant volume is substantially above historical baseline.",
                    )
                )

        current_cost = max(
            float(item.bandwidth_cost_bytes),
            float(item.compute_cost_units),
            float(item.storage_cost_bytes),
        )

        historical_costs = tuple(
            float(value) for value in history.historical_resource_costs
        )

        if historical_costs:
            baseline_cost = sum(historical_costs) / len(historical_costs)

            if baseline_cost > 0 and current_cost >= baseline_cost * self.policy.temporal_spike_multiplier:
                ratio = current_cost / baseline_cost

                value = self._clamp(
                    math.log1p(ratio) / math.log1p(50.0)
                )

                signals.append(
                    self._make_signal(
                        item,
                        ProtectionSignalFamily.TEMPORAL,
                        ProtectionSignalType.TEMPORAL_RESOURCE_ANOMALY,
                        value,
                        item.source_confidence,
                        ProtectionEvidenceKind.TEMPORAL,
                        "Current resource consumption is substantially above historical baseline.",
                    )
                )

        if history.previous_abuse_evidence_count > 0:
            recurrence = self._clamp(
                history.recurring_pattern_count
                / max(1.0, float(history.previous_analysis_count))
            )

            if recurrence > 0:
                signals.append(
                    self._make_signal(
                        item,
                        ProtectionSignalFamily.HISTORICAL,
                        ProtectionSignalType.RECURRING_ABUSE_PATTERN,
                        recurrence,
                        item.source_confidence,
                        ProtectionEvidenceKind.HISTORICAL,
                        "Historical observations contain recurring resource-protection evidence.",
                    )
                )

        if history.previous_high_risk_count > 0:
            historical_ratio = self._clamp(
                history.previous_high_risk_count
                / max(1.0, float(history.previous_analysis_count))
            )

            if historical_ratio >= 0.50:
                signals.append(
                    self._make_signal(
                        item,
                        ProtectionSignalFamily.HISTORICAL,
                        ProtectionSignalType.HISTORICAL_ABUSE_PATTERN,
                        historical_ratio,
                        item.source_confidence,
                        ProtectionEvidenceKind.HISTORICAL,
                        "Historical observations contain repeated high-risk protection evidence.",
                    )
                )

        return signals

    # ------------------------------------------------------------------
    # Technical / policy-bypass analysis
    # ------------------------------------------------------------------

    def analyze_technical_protection(
        self,
        item: ResourceProtectionInput,
    ) -> List[ProtectionSignal]:
        signals: List[ProtectionSignal] = []

        if item.policy_bypass_indicators > 0:
            value = self._clamp(
                math.log1p(item.policy_bypass_indicators)
                / math.log1p(100.0)
            )

            signals.append(
                self._make_signal(
                    item,
                    ProtectionSignalFamily.TECHNICAL,
                    ProtectionSignalType.RESOURCE_PROTECTION_BYPASS_INDICATOR,
                    value,
                    item.source_confidence,
                    ProtectionEvidenceKind.TECHNICAL,
                    "Externally supplied observations contain resource-protection policy-bypass indicators.",
                )
            )

            if item.crawl_attempt_count > 0:
                signals.append(
                    self._make_signal(
                        item,
                        ProtectionSignalFamily.CRAWL_BEHAVIOR,
                        ProtectionSignalType.CRAWL_POLICY_BYPASS_INDICATOR,
                        value,
                        item.source_confidence,
                        ProtectionEvidenceKind.TECHNICAL,
                        "Externally supplied observations contain crawl-policy bypass indicators.",
                    )
                )

            if item.indexable_variant_count > 0:
                signals.append(
                    self._make_signal(
                        item,
                        ProtectionSignalFamily.INDEX,
                        ProtectionSignalType.INDEX_POLICY_BYPASS_INDICATOR,
                        value,
                        item.source_confidence,
                        ProtectionEvidenceKind.TECHNICAL,
                        "Externally supplied observations contain index-policy bypass indicators.",
                    )
                )

        if item.technical_anomaly_indicators > 0:
            value = self._clamp(
                math.log1p(item.technical_anomaly_indicators)
                / math.log1p(100.0)
            )

            signals.append(
                self._make_signal(
                    item,
                    ProtectionSignalFamily.TECHNICAL,
                    ProtectionSignalType.TECHNICAL_RESOURCE_ANOMALY,
                    value,
                    item.source_confidence,
                    ProtectionEvidenceKind.TECHNICAL,
                    "Externally supplied observations contain technical resource anomalies.",
                )
            )

        return signals

    # ------------------------------------------------------------------
    # Signal aggregation
    # ------------------------------------------------------------------

    @staticmethod
    def _family_score(
        signals: Sequence[ProtectionSignal],
        families: Iterable[ProtectionSignalFamily],
    ) -> float:
        family_set = set(families)

        weighted_values = [
            signal.value * signal.confidence
            for signal in signals
            if signal.family in family_set
        ]

        if not weighted_values:
            return 0.0

        # Saturating union-like aggregation:
        # 1 - product(1 - evidence_i)
        remaining = 1.0

        for value in weighted_values:
            value = max(0.0, min(1.0, value))
            remaining *= 1.0 - value

        return max(0.0, min(1.0, 1.0 - remaining))

    def aggregate_scores(
        self,
        signals: Sequence[ProtectionSignal],
    ) -> Dict[str, float]:
        crawl_abuse_risk = self._family_score(
            signals,
            (
                ProtectionSignalFamily.CRAWL_BEHAVIOR,
                ProtectionSignalFamily.REQUEST_PATTERN,
                ProtectionSignalFamily.DISCOVERY,
            ),
        )

        index_abuse_risk = self._family_score(
            signals,
            (
                ProtectionSignalFamily.INDEX,
                ProtectionSignalFamily.INDEX_VARIANT,
            ),
        )

        resource_cost_risk = self._family_score(
            signals,
            (
                ProtectionSignalFamily.RESOURCE_COST,
                ProtectionSignalFamily.BANDWIDTH,
                ProtectionSignalFamily.COMPUTE,
                ProtectionSignalFamily.STORAGE,
                ProtectionSignalFamily.HOST,
                ProtectionSignalFamily.DOMAIN,
            ),
        )

        duplicate_work_risk = self._family_score(
            signals,
            (
                ProtectionSignalFamily.DUPLICATE_WORK,
                ProtectionSignalFamily.DUPLICATION,
            ),
        )

        variant_explosion_risk = self._family_score(
            signals,
            (
                ProtectionSignalFamily.URL_VARIANT,
                ProtectionSignalFamily.QUERY_PARAMETER,
                ProtectionSignalFamily.INDEX_VARIANT,
            ),
        )

        loop_risk = self._family_score(
            signals,
            (
                ProtectionSignalFamily.LOOP,
                ProtectionSignalFamily.REDIRECT,
            ),
        )

        cross_resource_risk = self._family_score(
            signals,
            (
                ProtectionSignalFamily.CROSS_RESOURCE,
            ),
        )

        temporal_risk = self._family_score(
            signals,
            (
                ProtectionSignalFamily.TEMPORAL,
                ProtectionSignalFamily.HISTORICAL,
            ),
        )

        technical_risk = self._family_score(
            signals,
            (
                ProtectionSignalFamily.TECHNICAL,
            ),
        )

        overall = self._clamp(
            0.24 * crawl_abuse_risk
            + 0.20 * index_abuse_risk
            + 0.18 * resource_cost_risk
            + 0.12 * duplicate_work_risk
            + 0.10 * variant_explosion_risk
            + 0.06 * loop_risk
            + 0.05 * cross_resource_risk
            + 0.03 * temporal_risk
            + 0.02 * technical_risk
        )

        return {
            "crawl_abuse_risk": crawl_abuse_risk,
            "index_abuse_risk": index_abuse_risk,
            "resource_cost_risk": resource_cost_risk,
            "duplicate_work_risk": duplicate_work_risk,
            "variant_explosion_risk": variant_explosion_risk,
            "loop_risk": loop_risk,
            "cross_resource_risk": cross_resource_risk,
            "temporal_risk": temporal_risk,
            "technical_risk": technical_risk,
            "overall_protection_risk": overall,
        }

    # ------------------------------------------------------------------
    # Confidence
    # ------------------------------------------------------------------

    def calculate_confidence(
        self,
        item: ResourceProtectionInput,
        signals: Sequence[ProtectionSignal],
    ) -> float:
        if not signals:
            return self._clamp(
                item.source_confidence * (0.65 if item.partial else 0.80)
            )

        signal_confidence = sum(
            signal.confidence for signal in signals
        ) / len(signals)

        evidence_coverage = self._clamp(
            len(signals) / 10.0
        )

        completeness = 0.70 if item.partial else 1.0

        confidence = (
            0.55 * signal_confidence
            + 0.25 * item.source_confidence
            + 0.20 * evidence_coverage
        )

        return self._clamp(confidence * completeness)

    # ------------------------------------------------------------------
    # Risk band
    # ------------------------------------------------------------------

    def risk_band(self, score: float) -> ProtectionRiskBand:
        score = self._clamp(score)

        if score < self.policy.low_risk_threshold:
            return ProtectionRiskBand.NONE

        if score < self.policy.moderate_risk_threshold:
            return ProtectionRiskBand.LOW

        if score < self.policy.high_risk_threshold:
            return ProtectionRiskBand.MODERATE

        if score < self.policy.very_high_risk_threshold:
            return ProtectionRiskBand.HIGH

        if score < self.policy.critical_risk_threshold:
            return ProtectionRiskBand.VERY_HIGH

        return ProtectionRiskBand.CRITICAL

    # ------------------------------------------------------------------
    # Reasons
    # ------------------------------------------------------------------

    def build_reasons(
        self,
        scores: Mapping[str, float],
        signals: Sequence[ProtectionSignal],
    ) -> Tuple[str, ...]:
        reasons: List[str] = []

        ordered = sorted(
            signals,
            key=lambda signal: (
                signal.value * signal.confidence,
                signal.signal_type.value,
            ),
            reverse=True,
        )

        seen = set()

        for signal in ordered:
            if signal.value <= 0:
                continue

            reason = signal.description.strip()

            if reason and reason not in seen:
                reasons.append(reason)
                seen.add(reason)

            if len(reasons) >= 8:
                break

        if not reasons and scores.get("overall_protection_risk", 0.0) > 0:
            reasons.append(
                "Protection evidence was observed but did not produce a dominant single signal family."
            )

        return tuple(reasons)

    # ------------------------------------------------------------------
    # Evidence construction
    # ------------------------------------------------------------------

    def build_evidence(
        self,
        item: ResourceProtectionInput,
        signals: Sequence[ProtectionSignal],
    ) -> Tuple[ProtectionEvidence, ...]:
        groups: Dict[ProtectionEvidenceKind, List[ProtectionSignal]] = {}

        for signal in signals:
            groups.setdefault(signal.evidence_kind, []).append(signal)

        evidence: List[ProtectionEvidence] = []

        for kind, grouped in sorted(
            groups.items(),
            key=lambda pair: pair[0].value,
        ):
            score = self._clamp(
                1.0
                - math.prod(
                    1.0
                    - self._clamp(
                        signal.value * signal.confidence
                    )
                    for signal in grouped
                )
            )

            confidence = self._clamp(
                sum(signal.confidence for signal in grouped)
                / max(1, len(grouped))
            )

            if score < 0.15:
                strength = ProtectionEvidenceStrength.WEAK
            elif score < 0.35:
                strength = ProtectionEvidenceStrength.MODERATE
            elif score < 0.65:
                strength = ProtectionEvidenceStrength.STRONG
            else:
                strength = ProtectionEvidenceStrength.VERY_STRONG

            evidence.append(
                ProtectionEvidence(
                    evidence_id=self._evidence_id(
                        item.identity.resource_id,
                        kind,
                        score,
                    ),
                    resource_id=item.identity.resource_id,
                    kind=kind,
                    strength=strength,
                    abuse_relevant=True,
                    resource_protection_relevant=True,
                    signal_ids=tuple(signal.signal_id for signal in grouped),
                    confidence=confidence,
                    score=score,
                    explanation=(
                        f"{len(grouped)} protection signal(s) support "
                        f"{kind.value} evidence."
                    ),
                    provenance={
                        "architecture_version": ARCHITECTURE_VERSION,
                        "phase": PHASE,
                        "source_confidence": item.source_confidence,
                    },
                )
            )

        return tuple(evidence)

    # ------------------------------------------------------------------
    # Decision
    # ------------------------------------------------------------------

    def decision(
        self,
        overall_risk: float,
        confidence: float,
        partial: bool,
        valid: bool,
    ) -> ProtectionDecision:
        if not valid:
            return ProtectionDecision.REJECTED_INPUT

        if confidence < self.policy.minimum_confidence:
            return ProtectionDecision.DEFERRED

        if partial:
            return ProtectionDecision.PARTIAL_EVIDENCE

        if overall_risk >= self.policy.high_risk_threshold:
            return ProtectionDecision.REVIEW_REQUIRED

        return ProtectionDecision.EVIDENCE_READY

    # ------------------------------------------------------------------
    # Main analysis
    # ------------------------------------------------------------------

    def analyze(
        self,
        item: ResourceProtectionInput,
        history: Optional[ResourceProtectionHistory] = None,
    ) -> ResourceProtectionResult:
        resource_id = item.identity.resource_id

        self._event(
            resource_id,
            ProtectionEventType.ANALYSIS_STARTED,
            ResourceProtectionState.RECEIVED,
            "Phase 14.5 resource-protection analysis started.",
        )

        valid, validation_errors = self.validate_input(item)

        if not valid:
            self._event(
                resource_id,
                ProtectionEventType.ANALYSIS_REJECTED,
                ResourceProtectionState.REJECTED,
                "Input validation failed.",
                metadata={"errors": validation_errors},
            )

            lineage = ResourceProtectionLineage(
                resource_id=resource_id,
                previous_stage=PREVIOUS_STAGE,
                current_stage=PHASE,
            )

            result = ResourceProtectionResult(
                identity=item.identity,
                lineage=lineage,
                resource_protection_risk=0.0,
                crawl_abuse_risk=0.0,
                index_abuse_risk=0.0,
                resource_cost_risk=0.0,
                duplicate_work_risk=0.0,
                variant_explosion_risk=0.0,
                loop_risk=0.0,
                cross_resource_risk=0.0,
                temporal_risk=0.0,
                overall_protection_risk=0.0,
                risk_band=ProtectionRiskBand.UNKNOWN,
                confidence=0.0,
                evidence=(),
                signals=(),
                decision=ProtectionDecision.REJECTED_INPUT,
                reasons=("Input validation failed.",),
                partial=True,
                missing_fields=tuple(validation_errors),
                created_at=self._now(),
                metadata={
                    "architecture_version": ARCHITECTURE_VERSION,
                    "phase": PHASE,
                },
            )

            self.backend.save_result(result)
            return result

        self._event(
            resource_id,
            ProtectionEventType.INPUT_NORMALIZED,
            ResourceProtectionState.NORMALIZING,
            "Input normalization completed.",
        )

        item = self.normalize_input(item)

        self._checkpoint(
            resource_id,
            ProtectionCheckpointType.NORMALIZATION_COMPLETE,
            signal_count=0,
            evidence_count=0,
            partial=item.partial,
        )

        signals: List[ProtectionSignal] = []

        state_methods = (
            (
                ResourceProtectionState.URL_ANALYSIS,
                self.analyze_url_structure,
            ),
            (
                ResourceProtectionState.PARAMETER_ANALYSIS,
                self.analyze_parameters,
            ),
            (
                ResourceProtectionState.CRAWL_BEHAVIOR_ANALYSIS,
                self.analyze_crawl_behavior,
            ),
            (
                ResourceProtectionState.LOOP_ANALYSIS
                if hasattr(ResourceProtectionState, "LOOP_ANALYSIS")
                else ResourceProtectionState.CRAWL_LOOP_ANALYSIS,
                self.analyze_loops,
            ),
            (
                ResourceProtectionState.RESOURCE_COST_ANALYSIS,
                self.analyze_resource_cost,
            ),
            (
                ResourceProtectionState.HOST_RESOURCE_ANALYSIS,
                self.analyze_frontier_pressure,
            ),
            (
                ResourceProtectionState.INDEX_ABUSE_ANALYSIS,
                self.analyze_index_abuse,
            ),
            (
                ResourceProtectionState.CROSS_RESOURCE_ANALYSIS,
                self.analyze_cross_resource,
            ),
            (
                ResourceProtectionState.TEMPORAL_ANALYSIS,
                lambda x: self.analyze_temporal_history(x, history),
            ),
            (
                ResourceProtectionState.TECHNICAL
                if hasattr(ResourceProtectionState, "TECHNICAL")
                else ResourceProtectionState.HISTORY_ANALYSIS,
                self.analyze_technical_protection,
            ),
        )

        for state, method in state_methods:
            self._event(
                resource_id,
                ProtectionEventType.SIGNAL_AGGREGATED,
                state,
                f"Entering {state.value}.",
            )

            try:
                extracted = method(item)

                for signal in extracted:
                    if signal.value <= 0:
                        continue

                    signals.append(signal)

                    self._event(
                        resource_id,
                        ProtectionEventType.SIGNAL_DETECTED,
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
                self._event(
                    resource_id,
                    ProtectionEventType.ANALYSIS_FAILED,
                    ResourceProtectionState.FAILED,
                    f"Signal extraction failed in {state.value}.",
                    metadata={"error": type(exc).__name__},
                )

                if not self.policy.allow_partial:
                    raise

                item.partial = True
                item.missing_fields = tuple(
                    sorted(
                        set(item.missing_fields)
                        | {f"analysis:{state.value}"}
                    )
                )

        # Per-resource safety bound.
        if len(signals) > self.policy.max_signals_per_resource:
            signals = signals[: self.policy.max_signals_per_resource]
            item.partial = True
            item.missing_fields = tuple(
                sorted(
                    set(item.missing_fields)
                    | {"signal_limit_applied"}
                )
            )

        self._checkpoint(
            resource_id,
            ProtectionCheckpointType.SIGNAL_EXTRACTION_COMPLETE,
            signal_count=len(signals),
            evidence_count=0,
            partial=item.partial,
        )

        scores = self.aggregate_scores(signals)

        resource_protection_risk = self._clamp(
            scores["overall_protection_risk"]
        )

        confidence = self.calculate_confidence(item, signals)

        evidence = self.build_evidence(item, signals)

        self._checkpoint(
            resource_id,
            ProtectionCheckpointType.AGGREGATION_COMPLETE,
            signal_count=len(signals),
            evidence_count=len(evidence),
            partial=item.partial,
        )

        self._event(
            resource_id,
            ProtectionEventType.SIGNAL_AGGREGATED,
            ResourceProtectionState.SIGNAL_AGGREGATION,
            "Protection signal aggregation completed.",
        )

        self._event(
            resource_id,
            ProtectionEventType.CONFIDENCE_CALCULATED,
            ResourceProtectionState.CONFIDENCE_CALCULATION,
            "Protection evidence confidence calculated.",
            metadata={"confidence": confidence},
        )

        self._checkpoint(
            resource_id,
            ProtectionCheckpointType.CONFIDENCE_COMPLETE,
            signal_count=len(signals),
            evidence_count=len(evidence),
            partial=item.partial,
        )

        band = self.risk_band(resource_protection_risk)

        decision = self.decision(
            overall_risk=resource_protection_risk,
            confidence=confidence,
            partial=item.partial,
            valid=True,
        )

        reasons = self.build_reasons(scores, signals)

        lineage = ResourceProtectionLineage(
            resource_id=resource_id,
            previous_stage=PREVIOUS_STAGE,
            current_stage=PHASE,
            previous_stage_version=str(
                item.metadata.get("previous_stage_version", "")
            ),
            source_evidence_ids=tuple(
                str(value)
                for value in item.metadata.get("source_evidence_ids", ())
            ),
            source_evidence_versions=tuple(
                str(value)
                for value in item.metadata.get("source_evidence_versions", ())
            ),
            parent_resource_ids=tuple(
                str(value)
                for value in item.metadata.get("parent_resource_ids", ())
            ),
            parent_document_ids=tuple(
                str(value)
                for value in item.metadata.get("parent_document_ids", ())
            ),
            lineage_metadata={
                "partition_id": item.identity.partition_id,
                "shard_id": item.identity.shard_id,
                "region_id": item.identity.region_id,
            },
        )

        result = ResourceProtectionResult(
            identity=item.identity,
            lineage=lineage,

            resource_protection_risk=resource_protection_risk,
            crawl_abuse_risk=scores["crawl_abuse_risk"],
            index_abuse_risk=scores["index_abuse_risk"],
            resource_cost_risk=scores["resource_cost_risk"],
            duplicate_work_risk=scores["duplicate_work_risk"],
            variant_explosion_risk=scores["variant_explosion_risk"],
            loop_risk=scores["loop_risk"],
            cross_resource_risk=scores["cross_resource_risk"],
            temporal_risk=scores["temporal_risk"],

            overall_protection_risk=resource_protection_risk,

            risk_band=band,
            confidence=confidence,

            evidence=evidence,
            signals=tuple(signals),

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
                "google_scale_capability_target": GOOGLE_SCALE_CAPABILITY_TARGET,
                "google_technology_dependency": GOOGLE_TECHNOLOGY_DEPENDENCY,
                "signal_count": len(signals),
                "evidence_count": len(evidence),
                "policy_deterministic": self.policy.deterministic,
                "enforcement_performed": False,
                "crawl_performed": False,
                "http_requests_performed": False,
                "index_mutation_performed": False,
            },
        )

        self._checkpoint(
            resource_id,
            ProtectionCheckpointType.RESULT_COMPLETE,
            signal_count=len(signals),
            evidence_count=len(evidence),
            partial=item.partial,
        )

        self._event(
            resource_id,
            ProtectionEventType.DECISION_PREPARED,
            ResourceProtectionState.DECISION_READY,
            "Protection evidence decision state prepared.",
            metadata={
                "decision": decision.value,
                "risk_band": band.value,
            },
        )

        self._event(
            resource_id,
            ProtectionEventType.ANALYSIS_COMPLETED,
            (
                ResourceProtectionState.PARTIAL
                if item.partial
                else ResourceProtectionState.COMPLETED
            ),
            "Phase 14.5 resource-protection analysis completed.",
            metadata={
                "overall_protection_risk": resource_protection_risk,
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
        items: Sequence[ResourceProtectionInput],
        histories: Optional[Mapping[str, ResourceProtectionHistory]] = None,
    ) -> Tuple[ResourceProtectionResult, ...]:
        if len(items) > self.policy.max_batch_size:
            raise ValueError(
                f"Batch exceeds per-request safety limit of "
                f"{self.policy.max_batch_size}."
            )

        histories = histories or {}

        results: List[ResourceProtectionResult] = []

        for item in items:
            history = histories.get(item.identity.resource_id)
            results.append(self.analyze(item, history))

        return tuple(results)

    # ------------------------------------------------------------------
    # Result/event/checkpoint access
    # ------------------------------------------------------------------

    def get_result(
        self,
        resource_id: str,
    ) -> Optional[ResourceProtectionResult]:
        return self.backend.get_result(resource_id)

    def get_events(
        self,
        resource_id: str,
    ) -> Sequence[ProtectionEvent]:
        return self.backend.get_events(resource_id)

    def get_checkpoints(
        self,
        resource_id: str,
    ) -> Sequence[ProtectionCheckpoint]:
        return self.backend.get_checkpoints(resource_id)

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
            "google_scale_capability_target": GOOGLE_SCALE_CAPABILITY_TARGET,
            "google_technology_dependency": GOOGLE_TECHNOLOGY_DEPENDENCY,
            "purpose": (
                "Detect crawl/index abuse and infrastructure resource "
                "protection indicators at enormous Web scale."
            ),
            "pipeline_position": (
                "Phase 14 security/quality evidence layer between "
                "malware/phishing/harmful-resource detection and "
                "distributed abuse detection/enforcement."
            ),
            "inputs": [
                "URL structure observations",
                "URL variant observations",
                "query parameter observations",
                "crawl behavior observations",
                "duplicate-work observations",
                "discovery observations",
                "redirect observations",
                "resource-cost observations",
                "index-variant observations",
                "cross-resource observations",
                "temporal observations",
                "historical observations",
                "technical observations",
                "source confidence",
            ],
            "outputs": [
                "structured protection signals",
                "protection evidence",
                "crawl-abuse risk",
                "index-abuse risk",
                "resource-cost risk",
                "duplicate-work risk",
                "variant-explosion risk",
                "loop risk",
                "cross-resource risk",
                "temporal risk",
                "overall protection evidence risk",
                "confidence",
                "review/defer/evidence state",
            ],
            "does_not_execute": [
                "HTTP requests",
                "crawling",
                "crawler workers",
                "malware",
                "payloads",
                "exploits",
                "active scans",
                "credential collection",
                "index mutation",
                "blocking",
                "deletion",
                "quarantine enforcement",
                "domain bans",
                "direct external throttling",
            ],
            "deterministic": self.policy.deterministic,
            "partial_evidence_supported": self.policy.allow_partial,
            "distributed_storage_backend_required_for_production": True,
            "enforcement_performed": False,
        }

    # ------------------------------------------------------------------
    # Serialization helper
    # ------------------------------------------------------------------

    @staticmethod
    def result_to_dict(
        result: ResourceProtectionResult,
    ) -> Dict[str, Any]:
        return asdict(result)


# ---------------------------------------------------------------------------
# Public aliases
# ---------------------------------------------------------------------------

CrawlIndexAbuseResourceProtection = (
    CrawlIndexAbuseResourceProtectionArchitecture
)

GlobalCrawlIndexAbuseResourceProtection = (
    CrawlIndexAbuseResourceProtectionArchitecture
)

Phase14_5CrawlIndexAbuseResourceProtection = (
    CrawlIndexAbuseResourceProtectionArchitecture
)

ResourceProtectionArchitecture = (
    CrawlIndexAbuseResourceProtectionArchitecture
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
    "PHASE_NAME",
    "NEXT_STAGE_NAME",

    "ResourceProtectionState",
    "ProtectionSignalFamily",
    "ProtectionSignalType",
    "ProtectionEvidenceStrength",
    "ProtectionRiskBand",
    "ProtectionDecision",
    "ProtectionEvidenceKind",
    "ProtectionEventType",
    "ProtectionCheckpointType",

    "ResourceProtectionIdentity",
    "ResourceProtectionLineage",
    "ResourceProtectionInput",
    "ResourceProtectionHistory",
    "ProtectionSignal",
    "ProtectionEvidence",
    "ResourceProtectionPolicy",
    "ProtectionCheckpoint",
    "ProtectionEvent",
    "ResourceProtectionResult",

    "ResourceProtectionBackend",
    "InMemoryResourceProtectionBackend",

    "CrawlIndexAbuseResourceProtectionArchitecture",
    "CrawlIndexAbuseResourceProtection",
    "GlobalCrawlIndexAbuseResourceProtection",
    "Phase14_5CrawlIndexAbuseResourceProtection",
    "ResourceProtectionArchitecture",
]
