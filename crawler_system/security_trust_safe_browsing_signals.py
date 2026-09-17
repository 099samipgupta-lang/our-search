"""
OUR SEARCH
Phase 14.7 — Security / Trust / Safe Browsing Signals

Purpose
-------
Provide a distributed, provenance-preserving security/trust signal architecture
for enormous-scale public-Web search infrastructure.

This stage consumes security, abuse, quality, identity, reputation, historical,
temporal, and cross-resource observations and produces structured trust/safety
signals for downstream systems.

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

- DOES analyze externally supplied security/trust observations.
- DOES produce structured security/trust signals.
- DOES calculate confidence and trust/risk bands.
- DOES preserve provenance and lineage.
- DOES correlate distributed security evidence.
- DOES support safe-browsing style classifications.
- DOES support historical and temporal trust analysis.
- DOES support explainable reasons.

This module DOES NOT:

- execute malware
- execute payloads
- execute exploits
- actively scan arbitrary systems
- perform network requests
- perform HTTP crawling
- assign crawler workers
- mutate the search index
- rank search results
- directly block or delete resources
- directly quarantine resources
- directly enforce penalties
- modify external systems
- replace Phase 14.4 malware/phishing detection
- replace Phase 14.5 crawl/index abuse protection
- replace Phase 14.6 distributed abuse enforcement
- depend on Google technology

The output of this stage is security/trust evidence and downstream-ready
decision metadata, not direct enforcement.
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

ARCHITECTURE_VERSION = "security-trust-safe-browsing-signals.v1"

PHASE = "14.7"
PREVIOUS_STAGE = "14.6"
NEXT_STAGE = "14.8"

PHASE_NAME = "Security / Trust / Safe Browsing Signals"
NEXT_STAGE_NAME = "Global Quality Control / Quarantine / Recovery"


# ============================================================================
# ENUMERATIONS
# ============================================================================


class SecurityTrustState(str, Enum):
    RECEIVED = "received"
    VALIDATING = "validating"
    NORMALIZING = "normalizing"

    IDENTITY_ANALYSIS = "identity_analysis"
    DOMAIN_TRUST_ANALYSIS = "domain_trust_analysis"
    HOST_TRUST_ANALYSIS = "host_trust_analysis"
    URL_TRUST_ANALYSIS = "url_trust_analysis"

    CERTIFICATE_ANALYSIS = "certificate_analysis"
    REDIRECT_TRUST_ANALYSIS = "redirect_trust_analysis"

    MALWARE_TRUST_ANALYSIS = "malware_trust_analysis"
    PHISHING_TRUST_ANALYSIS = "phishing_trust_analysis"
    HARMFUL_RESOURCE_ANALYSIS = "harmful_resource_analysis"

    ABUSE_HISTORY_ANALYSIS = "abuse_history_analysis"
    SECURITY_HISTORY_ANALYSIS = "security_history_analysis"
    TEMPORAL_ANALYSIS = "temporal_analysis"

    REPUTATION_ANALYSIS = "reputation_analysis"
    SOURCE_RELIABILITY_ANALYSIS = "source_reliability_analysis"

    CROSS_RESOURCE_ANALYSIS = "cross_resource_analysis"
    CROSS_DOMAIN_ANALYSIS = "cross_domain_analysis"
    INFRASTRUCTURE_ANALYSIS = "infrastructure_analysis"

    USER_REPORT_ANALYSIS = "user_report_analysis"
    SAFE_BROWSING_ANALYSIS = "safe_browsing_analysis"

    EVIDENCE_AGGREGATION = "evidence_aggregation"
    TRUST_CALCULATION = "trust_calculation"
    RISK_CALCULATION = "risk_calculation"
    CONFIDENCE_CALCULATION = "confidence_calculation"

    DECISION_READY = "decision_ready"
    COMPLETED = "completed"
    PARTIAL = "partial"
    DEFERRED = "deferred"
    REJECTED = "rejected"
    FAILED = "failed"


class SecurityTrustSignalFamily(str, Enum):
    URL = "url"
    DOMAIN = "domain"
    HOST = "host"
    IDENTITY = "identity"
    CERTIFICATE = "certificate"
    REDIRECT = "redirect"

    MALWARE = "malware"
    PHISHING = "phishing"
    HARMFUL_RESOURCE = "harmful_resource"
    EXPLOITATION = "exploitation"
    DOWNLOAD = "download"
    SCRIPT = "script"
    DECEPTION = "deception"

    ABUSE = "abuse"
    SECURITY_HISTORY = "security_history"
    TEMPORAL = "temporal"

    REPUTATION = "reputation"
    SOURCE = "source"

    CROSS_RESOURCE = "cross_resource"
    CROSS_DOMAIN = "cross_domain"
    INFRASTRUCTURE = "infrastructure"

    SAFE_BROWSING = "safe_browsing"
    USER_REPORT = "user_report"
    TECHNICAL = "technical"
    POLICY = "policy"


class SecurityTrustSignalType(str, Enum):
    # URL
    SUSPICIOUS_URL_STRUCTURE = "suspicious_url_structure"
    URL_ENCODING_ANOMALY = "url_encoding_anomaly"
    URL_REDIRECT_ANOMALY = "url_redirect_anomaly"
    URL_IDENTITY_MISMATCH = "url_identity_mismatch"
    URL_LOOKALIKE_PATTERN = "url_lookalike_pattern"

    # Domain
    LOOKALIKE_DOMAIN = "lookalike_domain"
    HOMOGRAPH_DOMAIN = "homograph_domain"
    BRAND_IMPERSONATION = "brand_impersonation"
    DOMAIN_IDENTITY_MISMATCH = "domain_identity_mismatch"
    DOMAIN_REPUTATION_ANOMALY = "domain_reputation_anomaly"
    DOMAIN_SECURITY_HISTORY = "domain_security_history"

    # Host
    HOST_IDENTITY_MISMATCH = "host_identity_mismatch"
    HOST_REPUTATION_ANOMALY = "host_reputation_anomaly"
    HOST_SECURITY_HISTORY = "host_security_history"
    SHARED_HOST_RISK = "shared_host_risk"

    # Identity
    IDENTITY_INCONSISTENCY = "identity_inconsistency"
    ORGANIZATION_IDENTITY_MISMATCH = "organization_identity_mismatch"
    CERTIFICATE_IDENTITY_MISMATCH = "certificate_identity_mismatch"
    BRAND_IDENTITY_MISMATCH = "brand_identity_mismatch"

    # Certificate
    CERTIFICATE_ANOMALY = "certificate_anomaly"
    CERTIFICATE_MISMATCH = "certificate_mismatch"
    CERTIFICATE_EXPIRATION_RISK = "certificate_expiration_risk"
    CERTIFICATE_TRUST_ANOMALY = "certificate_trust_anomaly"

    # Redirect
    REDIRECT_CHAIN_ANOMALY = "redirect_chain_anomaly"
    REDIRECT_DESTINATION_MISMATCH = "redirect_destination_mismatch"
    REDIRECT_IDENTITY_CHANGE = "redirect_identity_change"
    REDIRECT_TRUST_DEGRADATION = "redirect_trust_degradation"

    # Malware
    MALWARE_HASH_MATCH = "malware_hash_match"
    MALWARE_SIGNATURE_MATCH = "malware_signature_match"
    MALWARE_BEHAVIOR_EVIDENCE = "malware_behavior_evidence"
    MALWARE_REPUTATION_MATCH = "malware_reputation_match"
    MALWARE_DISTRIBUTION_PATTERN = "malware_distribution_pattern"

    # Phishing
    PHISHING_PATTERN = "phishing_pattern"
    CREDENTIAL_COLLECTION_PATTERN = "credential_collection_pattern"
    LOGIN_IMPERSONATION_PATTERN = "login_impersonation_pattern"
    PAYMENT_DATA_COLLECTION_PATTERN = "payment_data_collection_pattern"
    SOCIAL_ENGINEERING_PATTERN = "social_engineering_pattern"

    # Harmful resource
    HARMFUL_RESOURCE_PATTERN = "harmful_resource_pattern"
    HARMFUL_DOWNLOAD_PATTERN = "harmful_download_pattern"
    HARMFUL_CONTENT_INDICATOR = "harmful_content_indicator"

    # Exploitation
    EXPLOIT_DELIVERY_INDICATOR = "exploit_delivery_indicator"
    EXPLOITATION_REPUTATION_MATCH = "exploitation_reputation_match"
    EXPLOITATION_HISTORY = "exploitation_history"

    # Downloads
    SUSPICIOUS_DOWNLOAD_PATTERN = "suspicious_download_pattern"
    EXECUTABLE_DOWNLOAD_INDICATOR = "executable_download_indicator"
    SUSPICIOUS_ARCHIVE_INDICATOR = "suspicious_archive_indicator"

    # Scripts
    SCRIPT_OBFUSCATION_INDICATOR = "script_obfuscation_indicator"
    SCRIPT_DELIVERY_ANOMALY = "script_delivery_anomaly"
    SCRIPT_REPUTATION_ANOMALY = "script_reputation_anomaly"

    # Deception
    URGENCY_DECEPTION_PATTERN = "urgency_deception_pattern"
    DECEPTIVE_INTERFACE_PATTERN = "deceptive_interface_pattern"
    HIDDEN_ACTION_PATTERN = "hidden_action_pattern"
    MISLEADING_IDENTITY_PATTERN = "misleading_identity_pattern"

    # Abuse
    RECURRING_ABUSE_HISTORY = "recurring_abuse_history"
    DISTRIBUTED_ABUSE_HISTORY = "distributed_abuse_history"
    RESOURCE_PROTECTION_BYPASS = "resource_protection_bypass"
    POLICY_BYPASS_HISTORY = "policy_bypass_history"

    # Security history
    REPEATED_SECURITY_INCIDENTS = "repeated_security_incidents"
    SECURITY_INCIDENT_ESCALATION = "security_incident_escalation"
    LONG_TERM_SECURITY_RISK = "long_term_security_risk"

    # Temporal
    TEMPORAL_SECURITY_ANOMALY = "temporal_security_anomaly"
    RECENT_SECURITY_SPIKE = "recent_security_spike"
    RAPID_TRUST_DEGRADATION = "rapid_trust_degradation"
    RAPID_TRUST_RECOVERY = "rapid_trust_recovery"

    # Reputation
    REPUTATION_DEGRADATION = "reputation_degradation"
    REPUTATION_CONFLICT = "reputation_conflict"
    REPUTATION_IMPROVEMENT = "reputation_improvement"
    LOW_REPUTATION_CONFIDENCE = "low_reputation_confidence"

    # Source
    SOURCE_RELIABILITY_ANOMALY = "source_reliability_anomaly"
    SOURCE_CONFLICT = "source_conflict"
    SOURCE_LOW_CONFIDENCE = "source_low_confidence"

    # Cross-resource
    CROSS_RESOURCE_SECURITY_CLUSTER = "cross_resource_security_cluster"
    CROSS_DOMAIN_SECURITY_CLUSTER = "cross_domain_security_cluster"
    INFRASTRUCTURE_REUSE_PATTERN = "infrastructure_reuse_pattern"
    SHARED_SECURITY_INDICATOR = "shared_security_indicator"

    # Safe browsing
    SAFE_BROWSING_MALWARE = "safe_browsing_malware"
    SAFE_BROWSING_PHISHING = "safe_browsing_phishing"
    SAFE_BROWSING_HARMFUL = "safe_browsing_harmful"
    SAFE_BROWSING_DECEPTIVE = "safe_browsing_deceptive"
    SAFE_BROWSING_UNKNOWN = "safe_browsing_unknown"

    # User reports
    USER_REPORT_CLUSTER = "user_report_cluster"
    USER_REPORT_SPIKE = "user_report_spike"
    USER_REPORT_CONFLICT = "user_report_conflict"

    # Technical
    TECHNICAL_SECURITY_ANOMALY = "technical_security_anomaly"
    SECURITY_CONFIGURATION_ANOMALY = "security_configuration_anomaly"


class SecurityEvidenceStrength(str, Enum):
    NONE = "none"
    WEAK = "weak"
    MODERATE = "moderate"
    STRONG = "strong"
    VERY_STRONG = "very_strong"


class SecurityRiskBand(str, Enum):
    NONE = "none"
    LOW = "low"
    MODERATE = "moderate"
    HIGH = "high"
    VERY_HIGH = "very_high"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


class SecurityTrustBand(str, Enum):
    UNKNOWN = "unknown"
    VERY_LOW = "very_low"
    LOW = "low"
    MODERATE = "moderate"
    HIGH = "high"
    VERY_HIGH = "very_high"


class SecurityDecision(str, Enum):
    EVIDENCE_READY = "evidence_ready"
    TRUST_SIGNAL_READY = "trust_signal_ready"
    REVIEW_REQUIRED = "review_required"
    PROTECTION_SIGNAL = "protection_signal"
    HIGH_RISK_SIGNAL = "high_risk_signal"
    CRITICAL_RISK_SIGNAL = "critical_risk_signal"
    PARTIAL_EVIDENCE = "partial_evidence"
    DEFERRED = "deferred"
    REJECTED_INPUT = "rejected_input"


class SecurityEvidenceKind(str, Enum):
    URL = "url"
    DOMAIN = "domain"
    HOST = "host"
    IDENTITY = "identity"
    CERTIFICATE = "certificate"
    REDIRECT = "redirect"
    MALWARE = "malware"
    PHISHING = "phishing"
    HARMFUL = "harmful"
    EXPLOITATION = "exploitation"
    DOWNLOAD = "download"
    SCRIPT = "script"
    DECEPTION = "deception"
    ABUSE = "abuse"
    HISTORY = "history"
    TEMPORAL = "temporal"
    REPUTATION = "reputation"
    SOURCE = "source"
    CROSS_RESOURCE = "cross_resource"
    INFRASTRUCTURE = "infrastructure"
    SAFE_BROWSING = "safe_browsing"
    USER_REPORT = "user_report"
    TECHNICAL = "technical"
    POLICY = "policy"


class SecurityEventType(str, Enum):
    ANALYSIS_STARTED = "analysis_started"
    INPUT_ACCEPTED = "input_accepted"
    INPUT_NORMALIZED = "input_normalized"
    EVIDENCE_ACCEPTED = "evidence_accepted"
    SIGNAL_DETECTED = "signal_detected"
    TRUST_CALCULATED = "trust_calculated"
    RISK_CALCULATED = "risk_calculated"
    SOURCE_CORRELATED = "source_correlated"
    CROSS_RESOURCE_CORRELATED = "cross_resource_correlated"
    DECISION_PREPARED = "decision_prepared"
    CHECKPOINT_CREATED = "checkpoint_created"
    ANALYSIS_COMPLETED = "analysis_completed"
    ANALYSIS_PARTIAL = "analysis_partial"
    ANALYSIS_DEFERRED = "analysis_deferred"
    ANALYSIS_REJECTED = "analysis_rejected"
    ANALYSIS_FAILED = "analysis_failed"


class SecurityCheckpointType(str, Enum):
    INPUT_ACCEPTED = "input_accepted"
    NORMALIZATION_COMPLETE = "normalization_complete"
    SIGNAL_EXTRACTION_COMPLETE = "signal_extraction_complete"
    CORRELATION_COMPLETE = "correlation_complete"
    TRUST_COMPLETE = "trust_complete"
    RISK_COMPLETE = "risk_complete"
    CONFIDENCE_COMPLETE = "confidence_complete"
    RESULT_COMPLETE = "result_complete"


# ============================================================================
# DATA MODELS
# ============================================================================


@dataclass(frozen=True)
class SecurityTrustIdentity:
    resource_id: str
    document_id: str = ""
    url: str = ""
    canonical_url: str = ""

    domain: str = ""
    host: str = ""

    partition_id: str = ""
    shard_id: str = ""
    region_id: str = ""
    cluster_id: str = ""

    organization_id: str = ""
    certificate_id: str = ""

    version: str = "1"


@dataclass(frozen=True)
class SecurityTrustLineage:
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
class SecurityTrustInput:
    identity: SecurityTrustIdentity
    lineage: SecurityTrustLineage = field(
        default_factory=lambda: SecurityTrustLineage(resource_id="")
    )

    timestamp: float = 0.0

    # Direct observations from external systems.
    url_risk: float = 0.0
    domain_risk: float = 0.0
    host_risk: float = 0.0

    identity_risk: float = 0.0
    certificate_risk: float = 0.0
    redirect_risk: float = 0.0

    malware_risk: float = 0.0
    phishing_risk: float = 0.0
    harmful_resource_risk: float = 0.0
    exploitation_risk: float = 0.0

    download_risk: float = 0.0
    script_risk: float = 0.0
    deception_risk: float = 0.0

    abuse_risk: float = 0.0
    security_history_risk: float = 0.0
    temporal_security_risk: float = 0.0

    reputation_risk: float = 0.0
    source_risk: float = 0.0

    cross_resource_risk: float = 0.0
    cross_domain_risk: float = 0.0
    infrastructure_risk: float = 0.0

    safe_browsing_risk: float = 0.0
    user_report_risk: float = 0.0
    technical_security_risk: float = 0.0
    policy_risk: float = 0.0

    # Boolean observations.
    suspicious_url_structure: bool = False
    lookalike_domain: bool = False
    homograph_domain: bool = False
    brand_impersonation: bool = False
    credential_collection: bool = False
    login_impersonation: bool = False
    payment_data_collection: bool = False
    social_engineering: bool = False

    certificate_mismatch: bool = False
    redirect_anomaly: bool = False
    redirect_identity_change: bool = False

    malware_hash_match: bool = False
    malware_signature_match: bool = False
    malware_behavior_evidence: bool = False
    malware_reputation_match: bool = False

    exploit_delivery_indicator: bool = False
    suspicious_download: bool = False
    executable_download: bool = False
    suspicious_archive: bool = False

    script_obfuscation: bool = False
    script_delivery_anomaly: bool = False

    deceptive_interface: bool = False
    hidden_action_pattern: bool = False
    urgency_deception: bool = False

    recurring_abuse: bool = False
    policy_bypass: bool = False
    resource_protection_bypass: bool = False

    recent_security_spike: bool = False
    rapid_trust_degradation: bool = False

    cross_resource_cluster: bool = False
    cross_domain_cluster: bool = False
    infrastructure_reuse: bool = False

    safe_browsing_malware: bool = False
    safe_browsing_phishing: bool = False
    safe_browsing_harmful: bool = False
    safe_browsing_deceptive: bool = False

    user_report_cluster: bool = False
    user_report_spike: bool = False

    # Counts and scale-aware evidence.
    security_incident_count: int = 0
    malware_incident_count: int = 0
    phishing_incident_count: int = 0
    abuse_incident_count: int = 0

    affected_resource_count: int = 0
    affected_domain_count: int = 0
    infrastructure_reuse_count: int = 0

    trusted_source_count: int = 0
    conflicting_source_count: int = 0
    source_count: int = 0

    # External evidence references.
    evidence_ids: Tuple[str, ...] = ()
    evidence_versions: Tuple[str, ...] = ()

    # Source and confidence information.
    source_confidence: float = 0.0
    source_reliability: float = 0.0

    partial: bool = False
    missing_fields: Tuple[str, ...] = ()

    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SecurityTrustHistory:
    previous_trust_score: float = 0.0
    previous_risk_score: float = 0.0

    previous_trust_band: str = SecurityTrustBand.UNKNOWN.value
    previous_risk_band: str = SecurityRiskBand.UNKNOWN.value

    previous_decision: str = ""
    previous_decision_id: str = ""

    previous_security_incidents: int = 0
    previous_malware_incidents: int = 0
    previous_phishing_incidents: int = 0
    previous_abuse_incidents: int = 0

    recurring_security_events: int = 0
    historical_security_events: int = 0

    previous_report_count: int = 0
    previous_review_count: int = 0

    last_analysis_timestamp: float = 0.0

    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SecurityTrustSignal:
    signal_id: str
    resource_id: str

    family: SecurityTrustSignalFamily
    signal_type: SecurityTrustSignalType

    value: float
    confidence: float

    evidence_strength: SecurityEvidenceStrength

    source: str = ""
    timestamp: float = 0.0

    evidence_ids: Tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SecurityTrustEvidence:
    evidence_id: str
    resource_id: str

    kind: SecurityEvidenceKind

    strength: SecurityEvidenceStrength
    confidence: float

    signal_ids: Tuple[str, ...] = ()

    source: str = ""
    timestamp: float = 0.0

    provenance: Mapping[str, Any] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass
class SecurityTrustPolicy:
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

    # A critical security conclusion should generally require corroboration
    # unless the externally supplied evidence itself is very strong.
    require_multi_source_for_critical: bool = True

    # Signals considered high-value security evidence.
    critical_signal_types: Tuple[str, ...] = (
        SecurityTrustSignalType.MALWARE_HASH_MATCH.value,
        SecurityTrustSignalType.MALWARE_SIGNATURE_MATCH.value,
        SecurityTrustSignalType.SAFE_BROWSING_MALWARE.value,
        SecurityTrustSignalType.SAFE_BROWSING_PHISHING.value,
        SecurityTrustSignalType.CREDENTIAL_COLLECTION_PATTERN.value,
        SecurityTrustSignalType.EXPLOIT_DELIVERY_INDICATOR.value,
    )


@dataclass(frozen=True)
class SecurityTrustCheckpoint:
    checkpoint_id: str
    resource_id: str

    checkpoint_type: SecurityCheckpointType

    timestamp: float
    state: SecurityTrustState

    signal_count: int = 0
    evidence_count: int = 0

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SecurityTrustEvent:
    event_id: str
    resource_id: str

    event_type: SecurityEventType
    state: SecurityTrustState

    timestamp: float

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass
class SecurityTrustResult:
    resource_id: str

    state: SecurityTrustState
    decision: SecurityDecision

    trust_score: float
    risk_score: float
    confidence: float

    trust_band: SecurityTrustBand
    risk_band: SecurityRiskBand
    evidence_strength: SecurityEvidenceStrength

    signals: List[SecurityTrustSignal] = field(default_factory=list)
    evidence: List[SecurityTrustEvidence] = field(default_factory=list)

    reasons: List[str] = field(default_factory=list)

    partial: bool = False
    missing_fields: List[str] = field(default_factory=list)

    lineage: Optional[SecurityTrustLineage] = None

    timestamp: float = 0.0

    metadata: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# BACKEND
# ============================================================================


class SecurityTrustBackend(Protocol):
    def save_signal(self, signal: SecurityTrustSignal) -> None:
        ...

    def save_evidence(self, evidence: SecurityTrustEvidence) -> None:
        ...

    def save_event(self, event: SecurityTrustEvent) -> None:
        ...

    def save_checkpoint(self, checkpoint: SecurityTrustCheckpoint) -> None:
        ...

    def save_result(self, result: SecurityTrustResult) -> None:
        ...

    def get_result(self, resource_id: str) -> Optional[SecurityTrustResult]:
        ...


class InMemorySecurityTrustBackend:
    """
    Framework-neutral metadata backend.

    Production deployments can replace this with a distributed durable backend
    without changing the analysis architecture.
    """

    def __init__(self) -> None:
        self.signals: Dict[str, SecurityTrustSignal] = {}
        self.evidence: Dict[str, SecurityTrustEvidence] = {}
        self.events: Dict[str, SecurityTrustEvent] = {}
        self.checkpoints: Dict[str, SecurityTrustCheckpoint] = {}
        self.results: Dict[str, SecurityTrustResult] = {}

    def save_signal(self, signal: SecurityTrustSignal) -> None:
        self.signals[signal.signal_id] = signal

    def save_evidence(self, evidence: SecurityTrustEvidence) -> None:
        self.evidence[evidence.evidence_id] = evidence

    def save_event(self, event: SecurityTrustEvent) -> None:
        self.events[event.event_id] = event

    def save_checkpoint(self, checkpoint: SecurityTrustCheckpoint) -> None:
        self.checkpoints[checkpoint.checkpoint_id] = checkpoint

    def save_result(self, result: SecurityTrustResult) -> None:
        self.results[result.resource_id] = result

    def get_result(self, resource_id: str) -> Optional[SecurityTrustResult]:
        return self.results.get(resource_id)


# ============================================================================
# MAIN ARCHITECTURE
# ============================================================================


class SecurityTrustSafeBrowsingSignalsArchitecture:
    """
    Phase 14.7 integrated security/trust/safe-browsing signal architecture.

    The architecture is deliberately control-plane oriented.

    It converts supplied observations into:

        observations
            -> normalized security signals
            -> evidence
            -> distributed correlation
            -> trust/risk calculation
            -> confidence
            -> explainable security decision metadata

    It does not perform network activity or enforcement.
    """

    def __init__(
        self,
        policy: Optional[SecurityTrustPolicy] = None,
        backend: Optional[SecurityTrustBackend] = None,
    ) -> None:
        self.policy = policy or SecurityTrustPolicy()
        self.backend = backend or InMemorySecurityTrustBackend()

    # ========================================================================
    # SAFE HELPERS
    # ========================================================================

    @staticmethod
    def _now() -> float:
        return time.time()

    @staticmethod
    def _clamp(value: Any, low: float = 0.0, high: float = 1.0) -> float:
        try:
            number = float(value)
        except (TypeError, ValueError):
            return low

        if not math.isfinite(number):
            return low

        return max(low, min(high, number))

    @staticmethod
    def _safe_int(value: Any, minimum: int = 0) -> int:
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
    def _digest(payload: Any) -> str:
        encoded = json.dumps(
            payload,
            sort_keys=True,
            default=str,
            separators=(",", ":"),
        ).encode("utf-8")

        return hashlib.sha256(encoded).hexdigest()

    def _signal_id(
        self,
        resource_id: str,
        family: SecurityTrustSignalFamily,
        signal_type: SecurityTrustSignalType,
        value: float,
    ) -> str:
        raw = {
            "resource_id": resource_id,
            "family": family.value,
            "signal_type": signal_type.value,
            "value": round(value, 8),
            "architecture": ARCHITECTURE_VERSION,
        }

        return "sts_" + self._digest(raw)[:32]

    def _evidence_id(
        self,
        resource_id: str,
        kind: SecurityEvidenceKind,
        signal_ids: Sequence[str],
    ) -> str:
        raw = {
            "resource_id": resource_id,
            "kind": kind.value,
            "signal_ids": sorted(signal_ids),
            "architecture": ARCHITECTURE_VERSION,
        }

        return "ste_" + self._digest(raw)[:32]

    def _event_id(
        self,
        resource_id: str,
        event_type: SecurityEventType,
        timestamp: float,
    ) -> str:
        raw = {
            "resource_id": resource_id,
            "event_type": event_type.value,
            "timestamp": round(timestamp, 6),
        }

        return "evt_" + self._digest(raw)[:32]

    def _checkpoint_id(
        self,
        resource_id: str,
        checkpoint_type: SecurityCheckpointType,
    ) -> str:
        raw = {
            "resource_id": resource_id,
            "checkpoint_type": checkpoint_type.value,
            "architecture": ARCHITECTURE_VERSION,
        }

        return "chk_" + self._digest(raw)[:32]

    # ========================================================================
    # EVENTS / CHECKPOINTS
    # ========================================================================

    def _event(
        self,
        resource_id: str,
        event_type: SecurityEventType,
        state: SecurityTrustState,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> SecurityTrustEvent:
        timestamp = self._now()

        event = SecurityTrustEvent(
            event_id=self._event_id(resource_id, event_type, timestamp),
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
        checkpoint_type: SecurityCheckpointType,
        state: SecurityTrustState,
        signal_count: int = 0,
        evidence_count: int = 0,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> SecurityTrustCheckpoint:
        checkpoint = SecurityTrustCheckpoint(
            checkpoint_id=self._checkpoint_id(resource_id, checkpoint_type),
            resource_id=resource_id,
            checkpoint_type=checkpoint_type,
            timestamp=self._now(),
            state=state,
            signal_count=signal_count,
            evidence_count=evidence_count,
            metadata=dict(metadata or {}),
        )

        self.backend.save_checkpoint(checkpoint)

        self._event(
            resource_id,
            SecurityEventType.CHECKPOINT_CREATED,
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

    def validate_input(self, value: SecurityTrustInput) -> Tuple[bool, List[str]]:
        errors: List[str] = []

        if not isinstance(value, SecurityTrustInput):
            errors.append("input must be SecurityTrustInput")
            return False, errors

        if not value.identity.resource_id:
            errors.append("resource_id is required")

        numeric_fields = [
            "url_risk",
            "domain_risk",
            "host_risk",
            "identity_risk",
            "certificate_risk",
            "redirect_risk",
            "malware_risk",
            "phishing_risk",
            "harmful_resource_risk",
            "exploitation_risk",
            "download_risk",
            "script_risk",
            "deception_risk",
            "abuse_risk",
            "security_history_risk",
            "temporal_security_risk",
            "reputation_risk",
            "source_risk",
            "cross_resource_risk",
            "cross_domain_risk",
            "infrastructure_risk",
            "safe_browsing_risk",
            "user_report_risk",
            "technical_security_risk",
            "policy_risk",
            "source_confidence",
            "source_reliability",
        ]

        for name in numeric_fields:
            number = getattr(value, name, 0.0)

            try:
                number = float(number)
            except (TypeError, ValueError):
                errors.append(f"{name} must be numeric")
                continue

            if not math.isfinite(number):
                errors.append(f"{name} must be finite")

        count_fields = [
            "security_incident_count",
            "malware_incident_count",
            "phishing_incident_count",
            "abuse_incident_count",
            "affected_resource_count",
            "affected_domain_count",
            "infrastructure_reuse_count",
            "trusted_source_count",
            "conflicting_source_count",
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

    def normalize_input(self, value: SecurityTrustInput) -> SecurityTrustInput:
        identity = SecurityTrustIdentity(
            resource_id=self._safe_text(value.identity.resource_id),
            document_id=self._safe_text(value.identity.document_id),
            url=self._safe_text(value.identity.url),
            canonical_url=self._safe_text(value.identity.canonical_url),
            domain=self._safe_text(value.identity.domain).lower(),
            host=self._safe_text(value.identity.host).lower(),
            partition_id=self._safe_text(value.identity.partition_id),
            shard_id=self._safe_text(value.identity.shard_id),
            region_id=self._safe_text(value.identity.region_id),
            cluster_id=self._safe_text(value.identity.cluster_id),
            organization_id=self._safe_text(value.identity.organization_id),
            certificate_id=self._safe_text(value.identity.certificate_id),
            version=self._safe_text(value.identity.version) or "1",
        )

        normalized = SecurityTrustInput(
            identity=identity,
            lineage=value.lineage,
            timestamp=float(value.timestamp or self._now()),

            url_risk=self._clamp(value.url_risk),
            domain_risk=self._clamp(value.domain_risk),
            host_risk=self._clamp(value.host_risk),

            identity_risk=self._clamp(value.identity_risk),
            certificate_risk=self._clamp(value.certificate_risk),
            redirect_risk=self._clamp(value.redirect_risk),

            malware_risk=self._clamp(value.malware_risk),
            phishing_risk=self._clamp(value.phishing_risk),
            harmful_resource_risk=self._clamp(value.harmful_resource_risk),
            exploitation_risk=self._clamp(value.exploitation_risk),

            download_risk=self._clamp(value.download_risk),
            script_risk=self._clamp(value.script_risk),
            deception_risk=self._clamp(value.deception_risk),

            abuse_risk=self._clamp(value.abuse_risk),
            security_history_risk=self._clamp(value.security_history_risk),
            temporal_security_risk=self._clamp(value.temporal_security_risk),

            reputation_risk=self._clamp(value.reputation_risk),
            source_risk=self._clamp(value.source_risk),

            cross_resource_risk=self._clamp(value.cross_resource_risk),
            cross_domain_risk=self._clamp(value.cross_domain_risk),
            infrastructure_risk=self._clamp(value.infrastructure_risk),

            safe_browsing_risk=self._clamp(value.safe_browsing_risk),
            user_report_risk=self._clamp(value.user_report_risk),
            technical_security_risk=self._clamp(value.technical_security_risk),
            policy_risk=self._clamp(value.policy_risk),

            suspicious_url_structure=bool(value.suspicious_url_structure),
            lookalike_domain=bool(value.lookalike_domain),
            homograph_domain=bool(value.homograph_domain),
            brand_impersonation=bool(value.brand_impersonation),
            credential_collection=bool(value.credential_collection),
            login_impersonation=bool(value.login_impersonation),
            payment_data_collection=bool(value.payment_data_collection),
            social_engineering=bool(value.social_engineering),

            certificate_mismatch=bool(value.certificate_mismatch),
            redirect_anomaly=bool(value.redirect_anomaly),
            redirect_identity_change=bool(value.redirect_identity_change),

            malware_hash_match=bool(value.malware_hash_match),
            malware_signature_match=bool(value.malware_signature_match),
            malware_behavior_evidence=bool(value.malware_behavior_evidence),
            malware_reputation_match=bool(value.malware_reputation_match),

            exploit_delivery_indicator=bool(value.exploit_delivery_indicator),
            suspicious_download=bool(value.suspicious_download),
            executable_download=bool(value.executable_download),
            suspicious_archive=bool(value.suspicious_archive),

            script_obfuscation=bool(value.script_obfuscation),
            script_delivery_anomaly=bool(value.script_delivery_anomaly),

            deceptive_interface=bool(value.deceptive_interface),
            hidden_action_pattern=bool(value.hidden_action_pattern),
            urgency_deception=bool(value.urgency_deception),

            recurring_abuse=bool(value.recurring_abuse),
            policy_bypass=bool(value.policy_bypass),
            resource_protection_bypass=bool(value.resource_protection_bypass),

            recent_security_spike=bool(value.recent_security_spike),
            rapid_trust_degradation=bool(value.rapid_trust_degradation),

            cross_resource_cluster=bool(value.cross_resource_cluster),
            cross_domain_cluster=bool(value.cross_domain_cluster),
            infrastructure_reuse=bool(value.infrastructure_reuse),

            safe_browsing_malware=bool(value.safe_browsing_malware),
            safe_browsing_phishing=bool(value.safe_browsing_phishing),
            safe_browsing_harmful=bool(value.safe_browsing_harmful),
            safe_browsing_deceptive=bool(value.safe_browsing_deceptive),

            user_report_cluster=bool(value.user_report_cluster),
            user_report_spike=bool(value.user_report_spike),

            security_incident_count=self._safe_int(
                value.security_incident_count
            ),
            malware_incident_count=self._safe_int(
                value.malware_incident_count
            ),
            phishing_incident_count=self._safe_int(
                value.phishing_incident_count
            ),
            abuse_incident_count=self._safe_int(
                value.abuse_incident_count
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

            trusted_source_count=self._safe_int(
                value.trusted_source_count
            ),
            conflicting_source_count=self._safe_int(
                value.conflicting_source_count
            ),
            source_count=self._safe_int(value.source_count),

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

            source_confidence=self._clamp(value.source_confidence),
            source_reliability=self._clamp(value.source_reliability),

            partial=bool(value.partial),
            missing_fields=tuple(
                self._safe_text(item)
                for item in value.missing_fields
                if self._safe_text(item)
            ),

            metadata=dict(value.metadata or {}),
        )

        return normalized

    # ========================================================================
    # SIGNAL CREATION
    # ========================================================================

    def _make_signal(
        self,
        value: SecurityTrustInput,
        family: SecurityTrustSignalFamily,
        signal_type: SecurityTrustSignalType,
        score: float,
        confidence: Optional[float] = None,
        source: str = "phase14.7",
        evidence_ids: Sequence[str] = (),
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> SecurityTrustSignal:
        score = self._clamp(score)

        if confidence is None:
            confidence = value.source_confidence

        confidence = self._clamp(confidence)

        signal = SecurityTrustSignal(
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
            source=source,
            timestamp=value.timestamp,
            evidence_ids=tuple(evidence_ids),
            metadata=dict(metadata or {}),
        )

        self.backend.save_signal(signal)

        self._event(
            value.identity.resource_id,
            SecurityEventType.SIGNAL_DETECTED,
            SecurityTrustState.EVIDENCE_AGGREGATION,
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
    # EVIDENCE STRENGTH
    # ========================================================================

    def _strength_from_confidence(
        self,
        confidence: float,
    ) -> SecurityEvidenceStrength:
        confidence = self._clamp(confidence)

        if confidence <= 0.0:
            return SecurityEvidenceStrength.NONE

        if confidence < 0.40:
            return SecurityEvidenceStrength.WEAK

        if confidence < 0.65:
            return SecurityEvidenceStrength.MODERATE

        if confidence < 0.85:
            return SecurityEvidenceStrength.STRONG

        return SecurityEvidenceStrength.VERY_STRONG

    # ========================================================================
    # SIGNAL EXTRACTION
    # ========================================================================

    def extract_signals(
        self,
        value: SecurityTrustInput,
    ) -> List[SecurityTrustSignal]:
        signals: List[SecurityTrustSignal] = []

        def add(
            family: SecurityTrustSignalFamily,
            signal_type: SecurityTrustSignalType,
            score: float,
            confidence: Optional[float] = None,
            metadata: Optional[Mapping[str, Any]] = None,
        ) -> None:
            signals.append(
                self._make_signal(
                    value=value,
                    family=family,
                    signal_type=signal_type,
                    score=score,
                    confidence=confidence,
                    metadata=metadata,
                )
            )

        # --------------------------------------------------------------------
        # URL
        # --------------------------------------------------------------------

        if value.suspicious_url_structure:
            add(
                SecurityTrustSignalFamily.URL,
                SecurityTrustSignalType.SUSPICIOUS_URL_STRUCTURE,
                max(value.url_risk, 0.65),
            )

        if value.url_risk > 0.0:
            add(
                SecurityTrustSignalFamily.URL,
                SecurityTrustSignalType.SUSPICIOUS_URL_STRUCTURE,
                value.url_risk,
            )

        if value.lookalike_domain:
            add(
                SecurityTrustSignalFamily.DOMAIN,
                SecurityTrustSignalType.LOOKALIKE_DOMAIN,
                max(value.domain_risk, 0.75),
            )

        if value.homograph_domain:
            add(
                SecurityTrustSignalFamily.DOMAIN,
                SecurityTrustSignalType.HOMOGRAPH_DOMAIN,
                max(value.domain_risk, 0.80),
            )

        if value.brand_impersonation:
            add(
                SecurityTrustSignalFamily.DOMAIN,
                SecurityTrustSignalType.BRAND_IMPERSONATION,
                max(value.domain_risk, 0.78),
            )

        if value.domain_risk > 0.0:
            add(
                SecurityTrustSignalFamily.DOMAIN,
                SecurityTrustSignalType.DOMAIN_REPUTATION_ANOMALY,
                value.domain_risk,
            )

        if value.host_risk > 0.0:
            add(
                SecurityTrustSignalFamily.HOST,
                SecurityTrustSignalType.HOST_REPUTATION_ANOMALY,
                value.host_risk,
            )

        # --------------------------------------------------------------------
        # Identity
        # --------------------------------------------------------------------

        if value.identity_risk > 0.0:
            add(
                SecurityTrustSignalFamily.IDENTITY,
                SecurityTrustSignalType.IDENTITY_INCONSISTENCY,
                value.identity_risk,
            )

        if value.brand_impersonation:
            add(
                SecurityTrustSignalFamily.IDENTITY,
                SecurityTrustSignalType.BRAND_IDENTITY_MISMATCH,
                max(value.identity_risk, 0.70),
            )

        # --------------------------------------------------------------------
        # Certificate
        # --------------------------------------------------------------------

        if value.certificate_mismatch:
            add(
                SecurityTrustSignalFamily.CERTIFICATE,
                SecurityTrustSignalType.CERTIFICATE_MISMATCH,
                max(value.certificate_risk, 0.70),
            )

        if value.certificate_risk > 0.0:
            add(
                SecurityTrustSignalFamily.CERTIFICATE,
                SecurityTrustSignalType.CERTIFICATE_ANOMALY,
                value.certificate_risk,
            )

        # --------------------------------------------------------------------
        # Redirects
        # --------------------------------------------------------------------

        if value.redirect_anomaly:
            add(
                SecurityTrustSignalFamily.REDIRECT,
                SecurityTrustSignalType.REDIRECT_CHAIN_ANOMALY,
                max(value.redirect_risk, 0.70),
            )

        if value.redirect_identity_change:
            add(
                SecurityTrustSignalFamily.REDIRECT,
                SecurityTrustSignalType.REDIRECT_IDENTITY_CHANGE,
                max(value.redirect_risk, 0.75),
            )

        if value.redirect_risk > 0.0:
            add(
                SecurityTrustSignalFamily.REDIRECT,
                SecurityTrustSignalType.REDIRECT_TRUST_DEGRADATION,
                value.redirect_risk,
            )

        # --------------------------------------------------------------------
        # Malware
        # --------------------------------------------------------------------

        if value.malware_hash_match:
            add(
                SecurityTrustSignalFamily.MALWARE,
                SecurityTrustSignalType.MALWARE_HASH_MATCH,
                max(value.malware_risk, 0.95),
            )

        if value.malware_signature_match:
            add(
                SecurityTrustSignalFamily.MALWARE,
                SecurityTrustSignalType.MALWARE_SIGNATURE_MATCH,
                max(value.malware_risk, 0.93),
            )

        if value.malware_behavior_evidence:
            add(
                SecurityTrustSignalFamily.MALWARE,
                SecurityTrustSignalType.MALWARE_BEHAVIOR_EVIDENCE,
                max(value.malware_risk, 0.90),
            )

        if value.malware_reputation_match:
            add(
                SecurityTrustSignalFamily.MALWARE,
                SecurityTrustSignalType.MALWARE_REPUTATION_MATCH,
                max(value.malware_risk, 0.85),
            )

        if value.malware_risk > 0.0:
            add(
                SecurityTrustSignalFamily.MALWARE,
                SecurityTrustSignalType.MALWARE_DISTRIBUTION_PATTERN,
                value.malware_risk,
            )

        # --------------------------------------------------------------------
        # Phishing
        # --------------------------------------------------------------------

        if value.credential_collection:
            add(
                SecurityTrustSignalFamily.PHISHING,
                SecurityTrustSignalType.CREDENTIAL_COLLECTION_PATTERN,
                max(value.phishing_risk, 0.90),
            )

        if value.login_impersonation:
            add(
                SecurityTrustSignalFamily.PHISHING,
                SecurityTrustSignalType.LOGIN_IMPERSONATION_PATTERN,
                max(value.phishing_risk, 0.85),
            )

        if value.payment_data_collection:
            add(
                SecurityTrustSignalFamily.PHISHING,
                SecurityTrustSignalType.PAYMENT_DATA_COLLECTION_PATTERN,
                max(value.phishing_risk, 0.90),
            )

        if value.social_engineering:
            add(
                SecurityTrustSignalFamily.PHISHING,
                SecurityTrustSignalType.SOCIAL_ENGINEERING_PATTERN,
                max(value.phishing_risk, 0.80),
            )

        if value.phishing_risk > 0.0:
            add(
                SecurityTrustSignalFamily.PHISHING,
                SecurityTrustSignalType.PHISHING_PATTERN,
                value.phishing_risk,
            )

        # --------------------------------------------------------------------
        # Harmful resources
        # --------------------------------------------------------------------

        if value.harmful_resource_risk > 0.0:
            add(
                SecurityTrustSignalFamily.HARMFUL_RESOURCE,
                SecurityTrustSignalType.HARMFUL_RESOURCE_PATTERN,
                value.harmful_resource_risk,
            )

        if value.exploitation_risk > 0.0:
            add(
                SecurityTrustSignalFamily.EXPLOITATION,
                SecurityTrustSignalType.EXPLOITATION_REPUTATION_MATCH,
                value.exploitation_risk,
            )

        if value.exploit_delivery_indicator:
            add(
                SecurityTrustSignalFamily.EXPLOITATION,
                SecurityTrustSignalType.EXPLOIT_DELIVERY_INDICATOR,
                max(value.exploitation_risk, 0.90),
            )

        # --------------------------------------------------------------------
        # Downloads
        # --------------------------------------------------------------------

        if value.suspicious_download:
            add(
                SecurityTrustSignalFamily.DOWNLOAD,
                SecurityTrustSignalType.SUSPICIOUS_DOWNLOAD_PATTERN,
                max(value.download_risk, 0.70),
            )

        if value.executable_download:
            add(
                SecurityTrustSignalFamily.DOWNLOAD,
                SecurityTrustSignalType.EXECUTABLE_DOWNLOAD_INDICATOR,
                max(value.download_risk, 0.75),
            )

        if value.suspicious_archive:
            add(
                SecurityTrustSignalFamily.DOWNLOAD,
                SecurityTrustSignalType.SUSPICIOUS_ARCHIVE_INDICATOR,
                max(value.download_risk, 0.70),
            )

        # --------------------------------------------------------------------
        # Scripts
        # --------------------------------------------------------------------

        if value.script_obfuscation:
            add(
                SecurityTrustSignalFamily.SCRIPT,
                SecurityTrustSignalType.SCRIPT_OBFUSCATION_INDICATOR,
                max(value.script_risk, 0.70),
            )

        if value.script_delivery_anomaly:
            add(
                SecurityTrustSignalFamily.SCRIPT,
                SecurityTrustSignalType.SCRIPT_DELIVERY_ANOMALY,
                max(value.script_risk, 0.70),
            )

        if value.script_risk > 0.0:
            add(
                SecurityTrustSignalFamily.SCRIPT,
                SecurityTrustSignalType.SCRIPT_REPUTATION_ANOMALY,
                value.script_risk,
            )

        # --------------------------------------------------------------------
        # Deception
        # --------------------------------------------------------------------

        if value.deceptive_interface:
            add(
                SecurityTrustSignalFamily.DECEPTION,
                SecurityTrustSignalType.DECEPTIVE_INTERFACE_PATTERN,
                max(value.deception_risk, 0.70),
            )

        if value.hidden_action_pattern:
            add(
                SecurityTrustSignalFamily.DECEPTION,
                SecurityTrustSignalType.HIDDEN_ACTION_PATTERN,
                max(value.deception_risk, 0.75),
            )

        if value.urgency_deception:
            add(
                SecurityTrustSignalFamily.DECEPTION,
                SecurityTrustSignalType.URGENCY_DECEPTION_PATTERN,
                max(value.deception_risk, 0.70),
            )

        # --------------------------------------------------------------------
        # Abuse/history
        # --------------------------------------------------------------------

        if value.recurring_abuse:
            add(
                SecurityTrustSignalFamily.ABUSE,
                SecurityTrustSignalType.RECURRING_ABUSE_HISTORY,
                max(value.abuse_risk, 0.70),
            )

        if value.policy_bypass:
            add(
                SecurityTrustSignalFamily.ABUSE,
                SecurityTrustSignalType.POLICY_BYPASS_HISTORY,
                max(value.abuse_risk, 0.75),
            )

        if value.resource_protection_bypass:
            add(
                SecurityTrustSignalFamily.ABUSE,
                SecurityTrustSignalType.RESOURCE_PROTECTION_BYPASS,
                max(value.abuse_risk, 0.80),
            )

        if value.security_history_risk > 0.0:
            add(
                SecurityTrustSignalFamily.SECURITY_HISTORY,
                SecurityTrustSignalType.REPEATED_SECURITY_INCIDENTS,
                value.security_history_risk,
            )

        if value.security_incident_count > 0:
            historical_score = self._saturating_count(
                value.security_incident_count
            )

            add(
                SecurityTrustSignalFamily.SECURITY_HISTORY,
                SecurityTrustSignalType.LONG_TERM_SECURITY_RISK,
                historical_score,
                metadata={
                    "security_incident_count":
                        value.security_incident_count
                },
            )

        # --------------------------------------------------------------------
        # Temporal
        # --------------------------------------------------------------------

        if value.recent_security_spike:
            add(
                SecurityTrustSignalFamily.TEMPORAL,
                SecurityTrustSignalType.RECENT_SECURITY_SPIKE,
                max(value.temporal_security_risk, 0.75),
            )

        if value.rapid_trust_degradation:
            add(
                SecurityTrustSignalFamily.TEMPORAL,
                SecurityTrustSignalType.RAPID_TRUST_DEGRADATION,
                max(value.temporal_security_risk, 0.80),
            )

        if value.temporal_security_risk > 0.0:
            add(
                SecurityTrustSignalFamily.TEMPORAL,
                SecurityTrustSignalType.TEMPORAL_SECURITY_ANOMALY,
                value.temporal_security_risk,
            )

        # --------------------------------------------------------------------
        # Reputation
        # --------------------------------------------------------------------

        if value.reputation_risk > 0.0:
            add(
                SecurityTrustSignalFamily.REPUTATION,
                SecurityTrustSignalType.REPUTATION_DEGRADATION,
                value.reputation_risk,
            )

        if value.conflicting_source_count > 0:
            add(
                SecurityTrustSignalFamily.REPUTATION,
                SecurityTrustSignalType.REPUTATION_CONFLICT,
                self._saturating_count(
                    value.conflicting_source_count
                ),
            )

        # --------------------------------------------------------------------
        # Source
        # --------------------------------------------------------------------

        if value.source_count > 0:
            source_conflict = (
                value.conflicting_source_count
                / max(1, value.source_count)
            )

            if source_conflict > 0.0:
                add(
                    SecurityTrustSignalFamily.SOURCE,
                    SecurityTrustSignalType.SOURCE_CONFLICT,
                    self._clamp(source_conflict),
                )

        if value.source_reliability < 0.50:
            add(
                SecurityTrustSignalFamily.SOURCE,
                SecurityTrustSignalType.SOURCE_LOW_CONFIDENCE,
                1.0 - value.source_reliability,
            )

        # --------------------------------------------------------------------
        # Distributed/cross-resource
        # --------------------------------------------------------------------

        if value.cross_resource_cluster:
            score = self._cluster_score(
                value.affected_resource_count
            )

            add(
                SecurityTrustSignalFamily.CROSS_RESOURCE,
                SecurityTrustSignalType.CROSS_RESOURCE_SECURITY_CLUSTER,
                max(value.cross_resource_risk, score),
                metadata={
                    "affected_resource_count":
                        value.affected_resource_count
                },
            )

        if value.cross_domain_cluster:
            score = self._cluster_score(
                value.affected_domain_count
            )

            add(
                SecurityTrustSignalFamily.CROSS_DOMAIN,
                SecurityTrustSignalType.CROSS_DOMAIN_SECURITY_CLUSTER,
                max(value.cross_domain_risk, score),
                metadata={
                    "affected_domain_count":
                        value.affected_domain_count
                },
            )

        if value.infrastructure_reuse:
            score = self._cluster_score(
                value.infrastructure_reuse_count
            )

            add(
                SecurityTrustSignalFamily.INFRASTRUCTURE,
                SecurityTrustSignalType.INFRASTRUCTURE_REUSE_PATTERN,
                max(value.infrastructure_risk, score),
                metadata={
                    "infrastructure_reuse_count":
                        value.infrastructure_reuse_count
                },
            )

        # --------------------------------------------------------------------
        # Safe browsing
        # --------------------------------------------------------------------

        if value.safe_browsing_malware:
            add(
                SecurityTrustSignalFamily.SAFE_BROWSING,
                SecurityTrustSignalType.SAFE_BROWSING_MALWARE,
                max(value.safe_browsing_risk, 0.95),
            )

        if value.safe_browsing_phishing:
            add(
                SecurityTrustSignalFamily.SAFE_BROWSING,
                SecurityTrustSignalType.SAFE_BROWSING_PHISHING,
                max(value.safe_browsing_risk, 0.95),
            )

        if value.safe_browsing_harmful:
            add(
                SecurityTrustSignalFamily.SAFE_BROWSING,
                SecurityTrustSignalType.SAFE_BROWSING_HARMFUL,
                max(value.safe_browsing_risk, 0.90),
            )

        if value.safe_browsing_deceptive:
            add(
                SecurityTrustSignalFamily.SAFE_BROWSING,
                SecurityTrustSignalType.SAFE_BROWSING_DECEPTIVE,
                max(value.safe_browsing_risk, 0.80),
            )

        # --------------------------------------------------------------------
        # User reports
        # --------------------------------------------------------------------

        if value.user_report_cluster:
            add(
                SecurityTrustSignalFamily.USER_REPORT,
                SecurityTrustSignalType.USER_REPORT_CLUSTER,
                max(value.user_report_risk, 0.65),
            )

        if value.user_report_spike:
            add(
                SecurityTrustSignalFamily.USER_REPORT,
                SecurityTrustSignalType.USER_REPORT_SPIKE,
                max(value.user_report_risk, 0.75),
            )

        # --------------------------------------------------------------------
        # Technical/policy
        # --------------------------------------------------------------------

        if value.technical_security_risk > 0.0:
            add(
                SecurityTrustSignalFamily.TECHNICAL,
                SecurityTrustSignalType.TECHNICAL_SECURITY_ANOMALY,
                value.technical_security_risk,
            )

        if value.policy_risk > 0.0:
            add(
                SecurityTrustSignalFamily.POLICY,
                SecurityTrustSignalType.SECURITY_CONFIGURATION_ANOMALY,
                value.policy_risk,
            )

        return self._deduplicate_signals(signals)

    # ========================================================================
    # DISTRIBUTED CORRELATION
    # ========================================================================

    def correlate_signals(
        self,
        value: SecurityTrustInput,
        signals: Sequence[SecurityTrustSignal],
    ) -> List[SecurityTrustSignal]:
        correlated = list(signals)

        family_count: Dict[str, int] = {}

        for signal in signals:
            family_count[signal.family.value] = (
                family_count.get(signal.family.value, 0) + 1
            )

        # Multiple independent security families increase corroboration.
        independent_family_count = sum(
            1
            for count in family_count.values()
            if count > 0
        )

        if independent_family_count >= 3:
            correlated.append(
                self._make_signal(
                    value,
                    SecurityTrustSignalFamily.CROSS_RESOURCE,
                    SecurityTrustSignalType.SHARED_SECURITY_INDICATOR,
                    min(
                        1.0,
                        0.45
                        + 0.10 * independent_family_count,
                    ),
                    confidence=min(
                        1.0,
                        0.50
                        + 0.10 * independent_family_count,
                    ),
                    metadata={
                        "independent_security_families":
                            independent_family_count,
                    },
                )
            )

        # Distributed resource correlation.
        if value.affected_resource_count >= self.policy.minimum_cluster_size:
            correlated.append(
                self._make_signal(
                    value,
                    SecurityTrustSignalFamily.CROSS_RESOURCE,
                    SecurityTrustSignalType.CROSS_RESOURCE_SECURITY_CLUSTER,
                    self._cluster_score(
                        value.affected_resource_count
                    ),
                    metadata={
                        "affected_resource_count":
                            value.affected_resource_count
                    },
                )
            )

        if value.affected_domain_count >= self.policy.minimum_cluster_size:
            correlated.append(
                self._make_signal(
                    value,
                    SecurityTrustSignalFamily.CROSS_DOMAIN,
                    SecurityTrustSignalType.CROSS_DOMAIN_SECURITY_CLUSTER,
                    self._cluster_score(
                        value.affected_domain_count
                    ),
                    metadata={
                        "affected_domain_count":
                            value.affected_domain_count
                    },
                )
            )

        if value.infrastructure_reuse_count >= self.policy.minimum_cluster_size:
            correlated.append(
                self._make_signal(
                    value,
                    SecurityTrustSignalFamily.INFRASTRUCTURE,
                    SecurityTrustSignalType.INFRASTRUCTURE_REUSE_PATTERN,
                    self._cluster_score(
                        value.infrastructure_reuse_count
                    ),
                    metadata={
                        "infrastructure_reuse_count":
                            value.infrastructure_reuse_count
                    },
                )
            )

        if (
            value.source_count >= self.policy.minimum_sources_for_strong_evidence
            and value.conflicting_source_count == 0
        ):
            correlated.append(
                self._make_signal(
                    value,
                    SecurityTrustSignalFamily.SOURCE,
                    SecurityTrustSignalType.SOURCE_RELIABILITY_ANOMALY,
                    0.0,
                    confidence=min(
                        1.0,
                        0.50 + 0.10 * value.source_count,
                    ),
                    metadata={
                        "corroborating_sources":
                            value.source_count
                    },
                )
            )

        self._event(
            value.identity.resource_id,
            SecurityEventType.SOURCE_CORRELATED,
            SecurityTrustState.CROSS_RESOURCE_ANALYSIS,
            {
                "input_signal_count": len(signals),
                "correlated_signal_count": len(correlated),
            },
        )

        return self._deduplicate_signals(correlated)

    # ========================================================================
    # SCORE HELPERS
    # ========================================================================

    @staticmethod
    def _saturating_count(
        count: int,
        scale: float = 5.0,
    ) -> float:
        count = max(0, int(count))

        if count <= 0:
            return 0.0

        return min(
            1.0,
            math.log1p(count) / math.log1p(scale * 10.0),
        )

    def _cluster_score(self, count: int) -> float:
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
    # SIGNAL DEDUPLICATION
    # ========================================================================

    def _deduplicate_signals(
        self,
        signals: Sequence[SecurityTrustSignal],
    ) -> List[SecurityTrustSignal]:
        result: Dict[str, SecurityTrustSignal] = {}

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
    # RISK CALCULATION
    # ========================================================================

    def calculate_risk(
        self,
        signals: Sequence[SecurityTrustSignal],
    ) -> float:
        if not signals:
            return 0.0

        weighted_values: List[float] = []

        for signal in signals:
            weighted_values.append(
                self._clamp(
                    signal.value * signal.confidence
                )
            )

        # No simple arithmetic mean: use a bounded accumulation model.
        product = 1.0

        for value in weighted_values:
            product *= 1.0 - value

        risk = 1.0 - product

        return self._clamp(risk)

    # ========================================================================
    # TRUST CALCULATION
    # ========================================================================

    def calculate_trust(
        self,
        value: SecurityTrustInput,
        signals: Sequence[SecurityTrustSignal],
        history: Optional[SecurityTrustHistory] = None,
    ) -> float:
        risk = self.calculate_risk(signals)

        # Start from inverse observed risk.
        trust = 1.0 - risk

        # Strong external source reliability should preserve trust when
        # security observations are sparse.
        source_quality = (
            0.50 * value.source_confidence
            + 0.50 * value.source_reliability
        )

        trust = (
            0.75 * trust
            + 0.25 * source_quality
        )

        # Historical degradation.
        if history is not None:
            if history.previous_risk_score > 0.0:
                trust -= 0.10 * self._clamp(
                    history.previous_risk_score
                )

            if history.recurring_security_events > 0:
                trust -= min(
                    0.20,
                    0.02 * history.recurring_security_events,
                )

        # Explicit safe-browsing signals are strong negative trust evidence.
        if value.safe_browsing_malware:
            trust -= 0.50

        if value.safe_browsing_phishing:
            trust -= 0.50

        if value.safe_browsing_harmful:
            trust -= 0.40

        return self._clamp(trust)

    # ========================================================================
    # CONFIDENCE
    # ========================================================================

    def calculate_confidence(
        self,
        value: SecurityTrustInput,
        signals: Sequence[SecurityTrustSignal],
    ) -> float:
        if not signals:
            return self._clamp(
                0.50 * value.source_confidence
                + 0.50 * value.source_reliability
            )

        signal_confidence = sum(
            signal.confidence for signal in signals
        ) / max(1, len(signals))

        corroboration = min(
            1.0,
            value.trusted_source_count / max(
                1,
                self.policy.minimum_sources_for_strong_evidence,
            ),
        )

        conflict_penalty = min(
            0.40,
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
    ) -> SecurityRiskBand:
        risk = self._clamp(risk)

        if risk < self.policy.low_risk_threshold:
            return SecurityRiskBand.NONE

        if risk < self.policy.moderate_risk_threshold:
            return SecurityRiskBand.LOW

        if risk < self.policy.high_risk_threshold:
            return SecurityRiskBand.MODERATE

        if risk < self.policy.very_high_risk_threshold:
            return SecurityRiskBand.HIGH

        if risk < self.policy.critical_risk_threshold:
            return SecurityRiskBand.VERY_HIGH

        return SecurityRiskBand.CRITICAL

    def trust_band(
        self,
        trust: float,
    ) -> SecurityTrustBand:
        trust = self._clamp(trust)

        if trust < self.policy.very_low_trust_threshold:
            return SecurityTrustBand.VERY_LOW

        if trust < self.policy.low_trust_threshold:
            return SecurityTrustBand.LOW

        if trust < self.policy.moderate_trust_threshold:
            return SecurityTrustBand.MODERATE

        if trust < self.policy.high_trust_threshold:
            return SecurityTrustBand.HIGH

        return SecurityTrustBand.VERY_HIGH

    def evidence_strength(
        self,
        value: SecurityTrustInput,
        confidence: float,
    ) -> SecurityEvidenceStrength:
        source_count = max(
            value.source_count,
            value.trusted_source_count,
        )

        if source_count >= self.policy.minimum_sources_for_very_strong_evidence:
            return SecurityEvidenceStrength.VERY_STRONG

        if source_count >= self.policy.minimum_sources_for_strong_evidence:
            return SecurityEvidenceStrength.STRONG

        return self._strength_from_confidence(confidence)

    # ========================================================================
    # EVIDENCE BUILDING
    # ========================================================================

    def build_evidence(
        self,
        value: SecurityTrustInput,
        signals: Sequence[SecurityTrustSignal],
        confidence: float,
    ) -> List[SecurityTrustEvidence]:
        grouped: Dict[
            SecurityEvidenceKind,
            List[SecurityTrustSignal],
        ] = {}

        family_to_kind = {
            SecurityTrustSignalFamily.URL:
                SecurityEvidenceKind.URL,

            SecurityTrustSignalFamily.DOMAIN:
                SecurityEvidenceKind.DOMAIN,

            SecurityTrustSignalFamily.HOST:
                SecurityEvidenceKind.HOST,

            SecurityTrustSignalFamily.IDENTITY:
                SecurityEvidenceKind.IDENTITY,

            SecurityTrustSignalFamily.CERTIFICATE:
                SecurityEvidenceKind.CERTIFICATE,

            SecurityTrustSignalFamily.REDIRECT:
                SecurityEvidenceKind.REDIRECT,

            SecurityTrustSignalFamily.MALWARE:
                SecurityEvidenceKind.MALWARE,

            SecurityTrustSignalFamily.PHISHING:
                SecurityEvidenceKind.PHISHING,

            SecurityTrustSignalFamily.HARMFUL_RESOURCE:
                SecurityEvidenceKind.HARMFUL,

            SecurityTrustSignalFamily.EXPLOITATION:
                SecurityEvidenceKind.EXPLOITATION,

            SecurityTrustSignalFamily.DOWNLOAD:
                SecurityEvidenceKind.DOWNLOAD,

            SecurityTrustSignalFamily.SCRIPT:
                SecurityEvidenceKind.SCRIPT,

            SecurityTrustSignalFamily.DECEPTION:
                SecurityEvidenceKind.DECEPTION,

            SecurityTrustSignalFamily.ABUSE:
                SecurityEvidenceKind.ABUSE,

            SecurityTrustSignalFamily.SECURITY_HISTORY:
                SecurityEvidenceKind.HISTORY,

            SecurityTrustSignalFamily.TEMPORAL:
                SecurityEvidenceKind.TEMPORAL,

            SecurityTrustSignalFamily.REPUTATION:
                SecurityEvidenceKind.REPUTATION,

            SecurityTrustSignalFamily.SOURCE:
                SecurityEvidenceKind.SOURCE,

            SecurityTrustSignalFamily.CROSS_RESOURCE:
                SecurityEvidenceKind.CROSS_RESOURCE,

            SecurityTrustSignalFamily.CROSS_DOMAIN:
                SecurityEvidenceKind.CROSS_RESOURCE,

            SecurityTrustSignalFamily.INFRASTRUCTURE:
                SecurityEvidenceKind.INFRASTRUCTURE,

            SecurityTrustSignalFamily.SAFE_BROWSING:
                SecurityEvidenceKind.SAFE_BROWSING,

            SecurityTrustSignalFamily.USER_REPORT:
                SecurityEvidenceKind.USER_REPORT,

            SecurityTrustSignalFamily.TECHNICAL:
                SecurityEvidenceKind.TECHNICAL,

            SecurityTrustSignalFamily.POLICY:
                SecurityEvidenceKind.POLICY,
        }

        for signal in signals:
            kind = family_to_kind.get(
                signal.family,
                SecurityEvidenceKind.TECHNICAL,
            )

            grouped.setdefault(kind, []).append(signal)

        result: List[SecurityTrustEvidence] = []

        for kind, group in grouped.items():
            signal_ids = tuple(
                sorted(signal.signal_id for signal in group)
            )

            evidence_confidence = (
                sum(signal.confidence for signal in group)
                / max(1, len(group))
            )

            evidence_confidence = self._clamp(
                max(evidence_confidence, confidence * 0.50)
            )

            evidence_id = self._evidence_id(
                value.identity.resource_id,
                kind,
                signal_ids,
            )

            evidence = SecurityTrustEvidence(
                evidence_id=evidence_id,
                resource_id=value.identity.resource_id,
                kind=kind,
                strength=self._strength_from_confidence(
                    evidence_confidence
                ),
                confidence=evidence_confidence,
                signal_ids=signal_ids,
                source="phase14.7",
                timestamp=value.timestamp,
                provenance={
                    "architecture_version":
                        ARCHITECTURE_VERSION,
                    "phase": PHASE,
                    "previous_stage":
                        PREVIOUS_STAGE,
                    "source_evidence_ids":
                        list(value.evidence_ids),
                },
                metadata={
                    "signal_count": len(group),
                },
            )

            self.backend.save_evidence(evidence)
            result.append(evidence)

        return result[
            : self.policy.max_evidence_per_resource
        ]

    # ========================================================================
    # DECISION
    # ========================================================================

    def decision(
        self,
        value: SecurityTrustInput,
        risk: float,
        confidence: float,
        signals: Sequence[SecurityTrustSignal],
    ) -> SecurityDecision:
        risk = self._clamp(risk)
        confidence = self._clamp(confidence)

        if confidence < self.policy.minimum_confidence:
            return SecurityDecision.PARTIAL_EVIDENCE

        if value.partial and not self.policy.allow_partial:
            return SecurityDecision.DEFERRED

        critical_signal_present = any(
            signal.signal_type.value
            in self.policy.critical_signal_types
            for signal in signals
        )

        if (
            risk >= self.policy.critical_risk_threshold
            and critical_signal_present
        ):
            if (
                self.policy.require_multi_source_for_critical
                and value.source_count
                < self.policy.minimum_sources_for_strong_evidence
                and confidence < 0.90
            ):
                return SecurityDecision.REVIEW_REQUIRED

            return SecurityDecision.CRITICAL_RISK_SIGNAL

        if risk >= self.policy.very_high_risk_threshold:
            return SecurityDecision.HIGH_RISK_SIGNAL

        if risk >= self.policy.high_risk_threshold:
            return SecurityDecision.PROTECTION_SIGNAL

        if risk >= self.policy.moderate_risk_threshold:
            return SecurityDecision.REVIEW_REQUIRED

        return SecurityDecision.TRUST_SIGNAL_READY

    # ========================================================================
    # EXPLAINABLE REASONS
    # ========================================================================

    def build_reasons(
        self,
        value: SecurityTrustInput,
        signals: Sequence[SecurityTrustSignal],
        risk_band: SecurityRiskBand,
        trust_band: SecurityTrustBand,
        decision: SecurityDecision,
    ) -> List[str]:
        reasons: List[str] = []

        if value.malware_hash_match:
            reasons.append(
                "External malware-hash evidence was supplied."
            )

        if value.malware_signature_match:
            reasons.append(
                "External malware-signature evidence was supplied."
            )

        if value.malware_behavior_evidence:
            reasons.append(
                "External malware-behavior evidence was supplied."
            )

        if value.safe_browsing_malware:
            reasons.append(
                "An external safe-browsing malware signal was supplied."
            )

        if value.safe_browsing_phishing:
            reasons.append(
                "An external safe-browsing phishing signal was supplied."
            )

        if value.credential_collection:
            reasons.append(
                "Credential-collection evidence was supplied."
            )

        if value.lookalike_domain or value.homograph_domain:
            reasons.append(
                "Domain-identity similarity evidence was supplied."
            )

        if value.brand_impersonation:
            reasons.append(
                "Brand-identity impersonation evidence was supplied."
            )

        if value.redirect_anomaly:
            reasons.append(
                "Redirect-chain anomaly evidence was supplied."
            )

        if value.recurring_abuse:
            reasons.append(
                "Historical recurring-abuse evidence was supplied."
            )

        if value.recent_security_spike:
            reasons.append(
                "Recent security-activity spike evidence was supplied."
            )

        if value.cross_resource_cluster:
            reasons.append(
                "Cross-resource security clustering evidence was supplied."
            )

        if value.cross_domain_cluster:
            reasons.append(
                "Cross-domain security clustering evidence was supplied."
            )

        if value.infrastructure_reuse:
            reasons.append(
                "Infrastructure-reuse evidence was supplied."
            )

        if value.user_report_spike:
            reasons.append(
                "A user-report spike was supplied as external evidence."
            )

        if value.partial:
            reasons.append(
                "The analysis is partial because some evidence fields are missing."
            )

        if not reasons:
            reasons.append(
                "No strong security-risk signal was observed in the supplied evidence."
            )

        reasons.append(
            f"Computed risk band: {risk_band.value}."
        )

        reasons.append(
            f"Computed trust band: {trust_band.value}."
        )

        reasons.append(
            f"Security decision metadata: {decision.value}."
        )

        return reasons

    # ========================================================================
    # ANALYSIS
    # ========================================================================

    def analyze(
        self,
        value: SecurityTrustInput,
        history: Optional[SecurityTrustHistory] = None,
    ) -> SecurityTrustResult:
        resource_id = (
            value.identity.resource_id
            if isinstance(value, SecurityTrustInput)
            else ""
        )

        self._event(
            resource_id,
            SecurityEventType.ANALYSIS_STARTED,
            SecurityTrustState.RECEIVED,
        )

        valid, errors = self.validate_input(value)

        if not valid:
            self._event(
                resource_id,
                SecurityEventType.ANALYSIS_REJECTED,
                SecurityTrustState.REJECTED,
                {"errors": errors},
            )

            return SecurityTrustResult(
                resource_id=resource_id,
                state=SecurityTrustState.REJECTED,
                decision=SecurityDecision.REJECTED_INPUT,
                trust_score=0.0,
                risk_score=0.0,
                confidence=0.0,
                trust_band=SecurityTrustBand.UNKNOWN,
                risk_band=SecurityRiskBand.UNKNOWN,
                evidence_strength=SecurityEvidenceStrength.NONE,
                reasons=errors,
                timestamp=self._now(),
            )

        normalized = self.normalize_input(value)

        self._event(
            normalized.identity.resource_id,
            SecurityEventType.INPUT_ACCEPTED,
            SecurityTrustState.VALIDATING,
        )

        self._event(
            normalized.identity.resource_id,
            SecurityEventType.INPUT_NORMALIZED,
            SecurityTrustState.NORMALIZING,
        )

        self._checkpoint(
            normalized.identity.resource_id,
            SecurityCheckpointType.NORMALIZATION_COMPLETE,
            SecurityTrustState.NORMALIZING,
        )

        signals = self.extract_signals(normalized)

        self._checkpoint(
            normalized.identity.resource_id,
            SecurityCheckpointType.SIGNAL_EXTRACTION_COMPLETE,
            SecurityTrustState.EVIDENCE_AGGREGATION,
            signal_count=len(signals),
        )

        signals = self.correlate_signals(
            normalized,
            signals,
        )

        self._event(
            normalized.identity.resource_id,
            SecurityEventType.CROSS_RESOURCE_CORRELATED,
            SecurityTrustState.CROSS_RESOURCE_ANALYSIS,
            {
                "signal_count": len(signals),
            },
        )

        self._checkpoint(
            normalized.identity.resource_id,
            SecurityCheckpointType.CORRELATION_COMPLETE,
            SecurityTrustState.CROSS_RESOURCE_ANALYSIS,
            signal_count=len(signals),
        )

        risk = self.calculate_risk(signals)

        self._event(
            normalized.identity.resource_id,
            SecurityEventType.RISK_CALCULATED,
            SecurityTrustState.RISK_CALCULATION,
            {"risk_score": risk},
        )

        self._checkpoint(
            normalized.identity.resource_id,
            SecurityCheckpointType.RISK_COMPLETE,
            SecurityTrustState.RISK_CALCULATION,
            signal_count=len(signals),
        )

        trust = self.calculate_trust(
            normalized,
            signals,
            history,
        )

        self._event(
            normalized.identity.resource_id,
            SecurityEventType.TRUST_CALCULATED,
            SecurityTrustState.TRUST_CALCULATION,
            {"trust_score": trust},
        )

        self._checkpoint(
            normalized.identity.resource_id,
            SecurityCheckpointType.TRUST_COMPLETE,
            SecurityTrustState.TRUST_CALCULATION,
            signal_count=len(signals),
        )

        confidence = self.calculate_confidence(
            normalized,
            signals,
        )

        self._checkpoint(
            normalized.identity.resource_id,
            SecurityCheckpointType.CONFIDENCE_COMPLETE,
            SecurityTrustState.CONFIDENCE_CALCULATION,
            signal_count=len(signals),
        )

        risk_band = self.risk_band(risk)
        trust_band = self.trust_band(trust)

        evidence_strength = self.evidence_strength(
            normalized,
            confidence,
        )

        evidence = self.build_evidence(
            normalized,
            signals,
            confidence,
        )

        decision = self.decision(
            normalized,
            risk,
            confidence,
            signals,
        )

        reasons = self.build_reasons(
            normalized,
            signals,
            risk_band,
            trust_band,
            decision,
        )

        partial = bool(
            normalized.partial
            or normalized.missing_fields
            or confidence < self.policy.minimum_confidence
        )

        state = (
            SecurityTrustState.PARTIAL
            if partial
            else SecurityTrustState.COMPLETED
        )

        result = SecurityTrustResult(
            resource_id=normalized.identity.resource_id,
            state=state,
            decision=decision,
            trust_score=trust,
            risk_score=risk,
            confidence=confidence,
            trust_band=trust_band,
            risk_band=risk_band,
            evidence_strength=evidence_strength,
            signals=list(signals),
            evidence=list(evidence),
            reasons=reasons,
            partial=partial,
            missing_fields=list(normalized.missing_fields),
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
                "signal_count":
                    len(signals),
                "evidence_count":
                    len(evidence),
            },
        )

        self.backend.save_result(result)

        self._event(
            normalized.identity.resource_id,
            (
                SecurityEventType.ANALYSIS_PARTIAL
                if partial
                else SecurityEventType.ANALYSIS_COMPLETED
            ),
            state,
            {
                "decision": decision.value,
                "risk_score": risk,
                "trust_score": trust,
                "confidence": confidence,
            },
        )

        self._checkpoint(
            normalized.identity.resource_id,
            SecurityCheckpointType.RESULT_COMPLETE,
            state,
            signal_count=len(signals),
            evidence_count=len(evidence),
        )

        return result

    # ========================================================================
    # BATCH ANALYSIS
    # ========================================================================

    def analyze_many(
        self,
        values: Iterable[SecurityTrustInput],
        histories: Optional[
            Mapping[str, SecurityTrustHistory]
        ] = None,
    ) -> List[SecurityTrustResult]:
        items = list(values)

        if len(items) > 100_000:
            raise ValueError(
                "batch exceeds the per-request architecture safety limit"
            )

        history_map = histories or {}

        results: List[SecurityTrustResult] = []

        for value in items:
            resource_id = value.identity.resource_id

            results.append(
                self.analyze(
                    value,
                    history_map.get(resource_id),
                )
            )

        return results

    # ========================================================================
    # RESULT / METADATA
    # ========================================================================

    def get_result(
        self,
        resource_id: str,
    ) -> Optional[SecurityTrustResult]:
        return self.backend.get_result(resource_id)

    def result_to_dict(
        self,
        result: SecurityTrustResult,
    ) -> Dict[str, Any]:
        return asdict(result)

    def architecture(self) -> Dict[str, Any]:
        return {
            "architecture": (
                "SecurityTrustSafeBrowsingSignalsArchitecture"
            ),
            "architecture_version": ARCHITECTURE_VERSION,

            "phase": PHASE,
            "phase_name": PHASE_NAME,

            "previous_stage": PREVIOUS_STAGE,
            "next_stage": NEXT_STAGE,
            "next_stage_name": NEXT_STAGE_NAME,

            "scale_target": SCALE_TARGET,
            "google_scale_capability_target":
                GOOGLE_SCALE_CAPABILITY_TARGET,
            "google_technology_dependency":
                GOOGLE_TECHNOLOGY_DEPENDENCY,

            "purpose": (
                "Distributed security, trust, reputation, and "
                "safe-browsing signal generation for enormous-scale "
                "public-Web search infrastructure."
            ),

            "pipeline": [
                "external security observations",
                "identity analysis",
                "domain and host trust analysis",
                "certificate analysis",
                "redirect analysis",
                "malware/phishing/harmful-resource evidence",
                "abuse and security history",
                "temporal security analysis",
                "reputation analysis",
                "source reliability analysis",
                "cross-resource correlation",
                "infrastructure correlation",
                "safe-browsing signal aggregation",
                "trust calculation",
                "risk calculation",
                "confidence calculation",
                "explainable security decision metadata",
            ],

            "does_not_execute": [
                "malware",
                "payloads",
                "exploits",
                "network scans",
                "HTTP requests",
                "crawler workers",
                "search ranking",
                "search-index mutation",
                "external enforcement",
                "external resource modification",
            ],

            "downstream_stage": NEXT_STAGE_NAME,
        }


# ============================================================================
# COMPATIBILITY ALIASES
# ============================================================================


SecurityTrustSafeBrowsingSignals = (
    SecurityTrustSafeBrowsingSignalsArchitecture
)

GlobalSecurityTrustSafeBrowsingSignals = (
    SecurityTrustSafeBrowsingSignalsArchitecture
)

Phase14_7SecurityTrustSafeBrowsingSignals = (
    SecurityTrustSafeBrowsingSignalsArchitecture
)

SecurityTrustArchitecture = (
    SecurityTrustSafeBrowsingSignalsArchitecture
)

SafeBrowsingSignalsArchitecture = (
    SecurityTrustSafeBrowsingSignalsArchitecture
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
    "SecurityTrustState",
    "SecurityTrustSignalFamily",
    "SecurityTrustSignalType",
    "SecurityEvidenceStrength",
    "SecurityRiskBand",
    "SecurityTrustBand",
    "SecurityDecision",
    "SecurityEvidenceKind",
    "SecurityEventType",
    "SecurityCheckpointType",

    # Dataclasses
    "SecurityTrustIdentity",
    "SecurityTrustLineage",
    "SecurityTrustInput",
    "SecurityTrustHistory",
    "SecurityTrustSignal",
    "SecurityTrustEvidence",
    "SecurityTrustPolicy",
    "SecurityTrustCheckpoint",
    "SecurityTrustEvent",
    "SecurityTrustResult",

    # Backend
    "SecurityTrustBackend",
    "InMemorySecurityTrustBackend",

    # Architecture
    "SecurityTrustSafeBrowsingSignalsArchitecture",
    "SecurityTrustSafeBrowsingSignals",
    "GlobalSecurityTrustSafeBrowsingSignals",
    "Phase14_7SecurityTrustSafeBrowsingSignals",
    "SecurityTrustArchitecture",
    "SafeBrowsingSignalsArchitecture",
]
