"""
OUR SEARCH
Phase 14.3 — Link Spam / Graph Abuse Detection

Purpose
-------
Analyze Web link-graph evidence for spam, manipulation, artificial authority,
link-farm behavior, reciprocal-link abuse, anchor-text manipulation, and other
graph-level abuse patterns.

This stage consumes link and graph evidence and produces structured link-spam
and graph-abuse evidence for downstream quality, security, quarantine, and
enforcement systems.

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
- replace Phase 14.2
- replace malware/security systems
- replace final quality-control systems
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
- links
- edges
- nodes
- graph partitions
- graph shards
- workers
- observations
- link signals
- graph signals
- evidence records
- histories

Per-request safety limits exist only to bound individual processing units.

Design principles
-----------------
1. Treat link abuse as graph evidence, not a single opaque score.
2. Preserve independent link and graph signals.
3. Detect both local and cross-resource manipulation.
4. Detect coordinated graph structures.
5. Preserve temporal evidence.
6. Preserve provenance and lineage.
7. Support partial observations.
8. Support distributed graph partitions and shards.
9. Remain deterministic where possible.
10. Avoid assuming that a single suspicious graph pattern proves abuse.
11. Keep final enforcement outside this stage.
12. Keep ranking independent from abuse detection.
13. Support checkpointing and restartability.
14. Support billions-to-trillions scale without fixed global ceilings.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import hashlib
import math
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    Tuple,
)


# ============================================================================
# ARCHITECTURE METADATA
# ============================================================================

SCALE_TARGET = "billions_to_trillions_of_public_web_resources"
GOOGLE_SCALE_CAPABILITY_TARGET = True
GOOGLE_TECHNOLOGY_DEPENDENCY = False

ARCHITECTURE_VERSION = "link-spam-graph-abuse-detection.v1"
PHASE = "14.3"
PREVIOUS_STAGE = "14.2"
NEXT_STAGE = "14.4"

PHASE_NAME = "Link Spam / Graph Abuse Detection"
NEXT_STAGE_NAME = "Malware / Phishing / Harmful Resource Detection"


# ============================================================================
# ENUMERATIONS
# ============================================================================


class LinkGraphState(str, Enum):
    RECEIVED = "received"
    VALIDATING = "validating"
    NORMALIZING = "normalizing"
    LINK_ANALYSIS = "link_analysis"
    EDGE_ANALYSIS = "edge_analysis"
    NODE_ANALYSIS = "node_analysis"
    GRAPH_ANALYSIS = "graph_analysis"
    PATTERN_ANALYSIS = "pattern_analysis"
    ANCHOR_ANALYSIS = "anchor_analysis"
    RECIPROCITY_ANALYSIS = "reciprocity_analysis"
    TEMPORAL_ANALYSIS = "temporal_analysis"
    CROSS_RESOURCE_ANALYSIS = "cross_resource_analysis"
    EVIDENCE_AGGREGATION = "evidence_aggregation"
    CONFIDENCE_CALCULATION = "confidence_calculation"
    DECISION_READY = "decision_ready"
    COMPLETED = "completed"
    PARTIAL = "partial"
    DEFERRED = "deferred"
    REJECTED = "rejected"
    FAILED = "failed"


class LinkGraphSignalFamily(str, Enum):
    LINK_VOLUME = "link_volume"
    LINK_DENSITY = "link_density"
    LINK_DIVERSITY = "link_diversity"
    LINK_SOURCE_DIVERSITY = "link_source_diversity"
    LINK_TARGET_DIVERSITY = "link_target_diversity"

    ANCHOR_TEXT = "anchor_text"
    ANCHOR_DIVERSITY = "anchor_diversity"
    ANCHOR_CONCENTRATION = "anchor_concentration"
    ANCHOR_MANIPULATION = "anchor_manipulation"

    RECIPROCAL_LINKING = "reciprocal_linking"
    MUTUAL_LINKING = "mutual_linking"
    LINK_EXCHANGE = "link_exchange"

    LINK_FARM = "link_farm"
    LINK_NETWORK = "link_network"
    SATELLITE_NETWORK = "satellite_network"
    PRIVATE_LINK_NETWORK = "private_link_network"

    GRAPH_DENSITY = "graph_density"
    GRAPH_CLUSTERING = "graph_clustering"
    GRAPH_COMPONENT = "graph_component"
    GRAPH_CENTRALITY = "graph_centrality"
    GRAPH_TOPOLOGY = "graph_topology"

    MANIPULATIVE_LINK_PATTERN = "manipulative_link_pattern"
    ARTIFICIAL_AUTHORITY = "artificial_authority"
    AUTHORITY_CONCENTRATION = "authority_concentration"

    CROSS_DOMAIN = "cross_domain"
    CROSS_HOST = "cross_host"
    CROSS_RESOURCE = "cross_resource"

    TEMPORAL = "temporal"
    HISTORICAL = "historical"
    VELOCITY = "velocity"

    LINK_CONTEXT = "link_context"
    LINK_PLACEMENT = "link_placement"
    LINK_ATTRIBUTE = "link_attribute"

    LOW_QUALITY_SOURCE = "low_quality_source"
    SOURCE_REPUTATION = "source_reputation"

    IDENTITY = "identity"
    TECHNICAL = "technical"


class LinkGraphSignalType(str, Enum):
    LOW_SOURCE_DIVERSITY = "low_source_diversity"
    LOW_TARGET_DIVERSITY = "low_target_diversity"
    ABNORMAL_LINK_VOLUME = "abnormal_link_volume"
    ABNORMAL_LINK_DENSITY = "abnormal_link_density"

    ANCHOR_TEXT_CONCENTRATION = "anchor_text_concentration"
    ANCHOR_TEXT_REPETITION = "anchor_text_repetition"
    ANCHOR_KEYWORD_MANIPULATION = "anchor_keyword_manipulation"
    ANCHOR_DISTRIBUTION_ANOMALY = "anchor_distribution_anomaly"

    HIGH_RECIPROCITY = "high_reciprocity"
    RECIPROCAL_CLUSTER = "reciprocal_cluster"
    LINK_EXCHANGE_PATTERN = "link_exchange_pattern"

    LINK_FARM_PATTERN = "link_farm_pattern"
    LINK_NETWORK_PATTERN = "link_network_pattern"
    SATELLITE_NETWORK_PATTERN = "satellite_network_pattern"
    PRIVATE_LINK_NETWORK_PATTERN = "private_link_network_pattern"

    GRAPH_CLUSTER_ANOMALY = "graph_cluster_anomaly"
    GRAPH_DENSITY_ANOMALY = "graph_density_anomaly"
    GRAPH_CENTRALITY_ANOMALY = "graph_centrality_anomaly"
    GRAPH_TOPOLOGY_ANOMALY = "graph_topology_anomaly"

    ARTIFICIAL_AUTHORITY_PATTERN = "artificial_authority_pattern"
    AUTHORITY_CONCENTRATION_PATTERN = "authority_concentration_pattern"

    CROSS_DOMAIN_LINK_PATTERN = "cross_domain_link_pattern"
    CROSS_HOST_LINK_PATTERN = "cross_host_link_pattern"
    CROSS_RESOURCE_LINK_PATTERN = "cross_resource_link_pattern"

    LINK_VELOCITY_ANOMALY = "link_velocity_anomaly"
    TEMPORAL_CLUSTER_ANOMALY = "temporal_cluster_anomaly"
    HISTORICAL_REPETITION = "historical_repetition"

    CONTEXTUAL_LINK_MANIPULATION = "contextual_link_manipulation"
    LINK_PLACEMENT_ANOMALY = "link_placement_anomaly"
    LINK_ATTRIBUTE_ANOMALY = "link_attribute_anomaly"

    LOW_QUALITY_SOURCE_PATTERN = "low_quality_source_pattern"
    SOURCE_REPUTATION_ANOMALY = "source_reputation_anomaly"

    IDENTITY_GRAPH_ANOMALY = "identity_graph_anomaly"
    TECHNICAL_GRAPH_ANOMALY = "technical_graph_anomaly"

    CROSS_RESOURCE_DUPLICATED_LINK_PATTERN = (
        "cross_resource_duplicated_link_pattern"
    )


class LinkGraphEvidenceKind(str, Enum):
    LINK = "link"
    EDGE = "edge"
    NODE = "node"
    GRAPH = "graph"
    ANCHOR = "anchor"
    RECIPROCITY = "reciprocity"
    NETWORK = "network"
    TEMPORAL = "temporal"
    HISTORICAL = "historical"
    CROSS_RESOURCE = "cross_resource"
    IDENTITY = "identity"
    TECHNICAL = "technical"


class LinkGraphEvidenceStrength(str, Enum):
    NONE = "none"
    WEAK = "weak"
    MODERATE = "moderate"
    STRONG = "strong"
    VERY_STRONG = "very_strong"


class LinkGraphRiskBand(str, Enum):
    NONE = "none"
    LOW = "low"
    MODERATE = "moderate"
    HIGH = "high"
    VERY_HIGH = "very_high"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


class LinkGraphDecision(str, Enum):
    EVIDENCE_READY = "evidence_ready"
    REVIEW_REQUIRED = "review_required"
    PARTIAL_EVIDENCE = "partial_evidence"
    DEFERRED = "deferred"
    REJECTED_INPUT = "rejected_input"


class LinkGraphEventType(str, Enum):
    ANALYSIS_STARTED = "analysis_started"
    INPUT_NORMALIZED = "input_normalized"
    VALIDATION_COMPLETED = "validation_completed"
    LINK_SIGNAL_DETECTED = "link_signal_detected"
    GRAPH_SIGNAL_DETECTED = "graph_signal_detected"
    PATTERN_DETECTED = "pattern_detected"
    TEMPORAL_SIGNAL_DETECTED = "temporal_signal_detected"
    EVIDENCE_AGGREGATED = "evidence_aggregated"
    CONFIDENCE_CALCULATED = "confidence_calculated"
    ANALYSIS_COMPLETED = "analysis_completed"
    ANALYSIS_PARTIAL = "analysis_partial"
    ANALYSIS_DEFERRED = "analysis_deferred"
    ANALYSIS_REJECTED = "analysis_rejected"
    CHECKPOINT_CREATED = "checkpoint_created"


class LinkGraphCheckpointType(str, Enum):
    INPUT_NORMALIZED = "input_normalized"
    SIGNALS_EXTRACTED = "signals_extracted"
    PATTERNS_ANALYZED = "patterns_analyzed"
    EVIDENCE_AGGREGATED = "evidence_aggregated"
    RESULT_PERSISTED = "result_persisted"


# ============================================================================
# DATA MODELS
# ============================================================================


@dataclass(frozen=True)
class LinkGraphIdentity:
    resource_id: str
    document_id: Optional[str] = None
    url: Optional[str] = None
    host_id: Optional[str] = None
    domain_id: Optional[str] = None
    partition_id: Optional[str] = None
    shard_id: Optional[str] = None
    region_id: Optional[str] = None
    version: Optional[str] = None


@dataclass(frozen=True)
class LinkGraphLineage:
    resource_id: str
    previous_stage: str = PREVIOUS_STAGE
    current_stage: str = PHASE
    source_evidence_ids: Tuple[str, ...] = ()
    parent_result_ids: Tuple[str, ...] = ()
    lineage_metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class LinkGraphNodeInput:
    node_id: str
    host_id: Optional[str] = None
    domain_id: Optional[str] = None
    resource_type: Optional[str] = None

    inbound_links: int = 0
    outbound_links: int = 0

    unique_inbound_domains: int = 0
    unique_outbound_domains: int = 0
    unique_inbound_hosts: int = 0
    unique_outbound_hosts: int = 0

    graph_degree: float = 0.0
    graph_centrality: float = 0.0
    clustering_coefficient: float = 0.0

    source_quality: float = 0.5
    source_reputation: float = 0.5

    partial: bool = False


@dataclass(frozen=True)
class LinkGraphEdgeInput:
    source_node_id: str
    target_node_id: str

    source_host_id: Optional[str] = None
    target_host_id: Optional[str] = None

    source_domain_id: Optional[str] = None
    target_domain_id: Optional[str] = None

    anchor_text: Optional[str] = None

    anchor_keyword_density: float = 0.0
    anchor_repetition: float = 0.0

    contextual_relevance: float = 0.5
    placement_quality: float = 0.5

    sponsored: bool = False
    nofollow: bool = False
    ugc: bool = False

    link_age_days: Optional[float] = None
    created_at: Optional[str] = None

    confidence: float = 1.0
    partial: bool = False


@dataclass(frozen=True)
class LinkGraphAnchorEvidence:
    total_anchor_links: int = 0
    unique_anchors: int = 0

    exact_match_ratio: float = 0.0
    keyword_anchor_ratio: float = 0.0
    repeated_anchor_ratio: float = 0.0
    commercial_anchor_ratio: float = 0.0

    anchor_entropy: float = 1.0
    anchor_concentration: float = 0.0

    partial: bool = False


@dataclass(frozen=True)
class LinkGraphReciprocityEvidence:
    outbound_pairs: int = 0
    reciprocal_pairs: int = 0

    reciprocal_ratio: float = 0.0
    mutual_cluster_ratio: float = 0.0
    exchange_pattern_score: float = 0.0

    partial: bool = False


@dataclass(frozen=True)
class LinkGraphNetworkEvidence:
    cluster_size: int = 0
    internal_edges: int = 0
    external_edges: int = 0

    internal_density: float = 0.0
    external_link_ratio: float = 0.0

    hub_concentration: float = 0.0
    satellite_ratio: float = 0.0
    shared_template_ratio: float = 0.0
    shared_identity_ratio: float = 0.0

    network_coordination_score: float = 0.0

    partial: bool = False


@dataclass(frozen=True)
class LinkGraphTemporalEvidence:
    links_observed: int = 0
    new_links: int = 0

    observation_window_days: float = 0.0
    link_velocity: float = 0.0
    velocity_anomaly: float = 0.0

    temporal_cluster_score: float = 0.0
    historical_repetition_score: float = 0.0

    partial: bool = False


@dataclass(frozen=True)
class LinkGraphCrossResourceEvidence:
    duplicated_link_pattern_score: float = 0.0
    cross_domain_coordination_score: float = 0.0
    cross_host_coordination_score: float = 0.0
    common_anchor_pattern_score: float = 0.0
    common_target_pattern_score: float = 0.0

    related_resource_count: int = 0
    coordinated_resource_count: int = 0

    partial: bool = False


@dataclass(frozen=True)
class LinkGraphDocumentInput:
    identity: LinkGraphIdentity

    total_inbound_links: int = 0
    total_outbound_links: int = 0

    unique_inbound_domains: int = 0
    unique_outbound_domains: int = 0
    unique_inbound_hosts: int = 0
    unique_outbound_hosts: int = 0

    inbound_link_diversity: float = 1.0
    outbound_link_diversity: float = 1.0

    graph_degree: float = 0.0
    graph_centrality: float = 0.0
    clustering_coefficient: float = 0.0
    graph_density: float = 0.0

    authority_concentration: float = 0.0
    artificial_authority_score: float = 0.0

    low_quality_source_ratio: float = 0.0
    source_reputation: float = 0.5

    anchor: LinkGraphAnchorEvidence = field(
        default_factory=LinkGraphAnchorEvidence
    )
    reciprocity: LinkGraphReciprocityEvidence = field(
        default_factory=LinkGraphReciprocityEvidence
    )
    network: LinkGraphNetworkEvidence = field(
        default_factory=LinkGraphNetworkEvidence
    )
    temporal: LinkGraphTemporalEvidence = field(
        default_factory=LinkGraphTemporalEvidence
    )
    cross_resource: LinkGraphCrossResourceEvidence = field(
        default_factory=LinkGraphCrossResourceEvidence
    )

    nodes: Tuple[LinkGraphNodeInput, ...] = ()
    edges: Tuple[LinkGraphEdgeInput, ...] = ()

    identity_anomaly_score: float = 0.0
    technical_anomaly_score: float = 0.0

    confidence: float = 1.0
    partial: bool = False


@dataclass(frozen=True)
class LinkGraphSignal:
    signal_id: str
    family: LinkGraphSignalFamily
    signal_type: LinkGraphSignalType

    value: float
    confidence: float

    evidence_count: int = 1
    explanation: str = ""

    source: str = "phase_14_3"
    partial: bool = False
    version: str = ARCHITECTURE_VERSION


@dataclass(frozen=True)
class LinkGraphEvidence:
    evidence_id: str

    kind: LinkGraphEvidenceKind
    family: LinkGraphSignalFamily
    signal_type: LinkGraphSignalType

    score: float
    strength: LinkGraphEvidenceStrength
    confidence: float

    explanation: str
    source: str

    observed_at: str
    lineage: LinkGraphLineage

    partial: bool = False
    version: str = ARCHITECTURE_VERSION


@dataclass(frozen=True)
class LinkGraphPolicy:
    max_resources_per_batch: int = 100_000
    max_signals_per_resource: int = 512
    max_evidence_per_resource: int = 512

    minimum_confidence: float = 0.10

    review_threshold: float = 0.60
    high_risk_threshold: float = 0.80
    critical_threshold: float = 0.95

    partial_allowed: bool = True
    deterministic: bool = True
    checkpoint_enabled: bool = True

    min_link_volume_for_graph_analysis: int = 10
    min_cluster_size_for_network_analysis: int = 5

    anchor_manipulation_threshold: float = 0.70
    reciprocity_threshold: float = 0.70
    network_coordination_threshold: float = 0.70
    velocity_anomaly_threshold: float = 0.70
    authority_manipulation_threshold: float = 0.70

    version: str = ARCHITECTURE_VERSION


@dataclass(frozen=True)
class LinkGraphResult:
    result_id: str
    identity: LinkGraphIdentity

    state: LinkGraphState

    risk_score: float
    risk_band: LinkGraphRiskBand

    evidence: Tuple[LinkGraphEvidence, ...]
    confidence: float

    partial: bool
    decision: LinkGraphDecision

    reasons: Tuple[str, ...]

    created_at: str
    lineage: LinkGraphLineage

    version: str = ARCHITECTURE_VERSION


@dataclass(frozen=True)
class LinkGraphCheckpoint:
    checkpoint_id: str
    resource_id: str

    checkpoint_type: LinkGraphCheckpointType
    state: LinkGraphState

    completed_units: int
    observed_units: int

    partial: bool
    created_at: str

    metadata: Mapping[str, Any] = field(default_factory=dict)
    version: str = ARCHITECTURE_VERSION


@dataclass(frozen=True)
class LinkGraphEvent:
    event_id: str
    event_type: LinkGraphEventType

    resource_id: str
    state: LinkGraphState

    timestamp: str

    metadata: Mapping[str, Any] = field(default_factory=dict)
    version: str = ARCHITECTURE_VERSION


# ============================================================================
# BACKEND PROTOCOL
# ============================================================================


class LinkGraphMetadataBackend(Protocol):
    def persist_event(self, event: LinkGraphEvent) -> None:
        ...

    def persist_checkpoint(self, checkpoint: LinkGraphCheckpoint) -> None:
        ...

    def persist_result(self, result: LinkGraphResult) -> None:
        ...

    def get_result(self, result_id: str) -> Optional[LinkGraphResult]:
        ...


class InMemoryLinkGraphMetadataBackend:
    """
    Reference metadata backend.

    Production deployments can replace this implementation with distributed
    durable storage without changing the analysis architecture.
    """

    def __init__(self) -> None:
        self._events: List[LinkGraphEvent] = []
        self._checkpoints: Dict[str, LinkGraphCheckpoint] = {}
        self._results: Dict[str, LinkGraphResult] = {}

    def persist_event(self, event: LinkGraphEvent) -> None:
        self._events.append(event)

    def persist_checkpoint(self, checkpoint: LinkGraphCheckpoint) -> None:
        self._checkpoints[checkpoint.checkpoint_id] = checkpoint

    def persist_result(self, result: LinkGraphResult) -> None:
        self._results[result.result_id] = result

    def get_result(self, result_id: str) -> Optional[LinkGraphResult]:
        return self._results.get(result_id)

    @property
    def events(self) -> Tuple[LinkGraphEvent, ...]:
        return tuple(self._events)

    @property
    def checkpoints(self) -> Tuple[LinkGraphCheckpoint, ...]:
        return tuple(self._checkpoints.values())

    @property
    def results(self) -> Tuple[LinkGraphResult, ...]:
        return tuple(self._results.values())


# ============================================================================
# MAIN ARCHITECTURE
# ============================================================================


class LinkSpamGraphAbuseDetectionArchitecture:
    """
    Phase 14.3 main architecture.

    This class extracts structured link-spam and graph-abuse evidence.

    It does not perform final enforcement.
    """

    def __init__(
        self,
        backend: Optional[LinkGraphMetadataBackend] = None,
        policy: Optional[LinkGraphPolicy] = None,
    ) -> None:
        self.backend = backend or InMemoryLinkGraphMetadataBackend()
        self.policy = policy or LinkGraphPolicy()

    # ------------------------------------------------------------------------
    # BASIC HELPERS
    # ------------------------------------------------------------------------

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _clamp(value: float, minimum: float = 0.0, maximum: float = 1.0) -> float:
        if not math.isfinite(value):
            return minimum
        return max(minimum, min(maximum, value))

    @classmethod
    def _safe_float(cls, value: Any, default: float = 0.0) -> float:
        try:
            number = float(value)
        except (TypeError, ValueError):
            return default

        if not math.isfinite(number):
            return default

        return number

    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        try:
            return max(0, int(value))
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _deterministic_unit(*parts: Any) -> float:
        payload = "|".join(str(part) for part in parts)
        digest = hashlib.sha256(payload.encode("utf-8")).digest()
        integer = int.from_bytes(digest[:8], "big")
        return integer / float(2**64 - 1)

    @staticmethod
    def _digest_payload(*parts: Any) -> str:
        payload = "|".join(str(part) for part in parts)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    @staticmethod
    def _parse_timestamp(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None

        try:
            timestamp = value.replace("Z", "+00:00")
            parsed = datetime.fromisoformat(timestamp)

            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)

            return parsed.astimezone(timezone.utc)
        except (TypeError, ValueError):
            return None

    # ------------------------------------------------------------------------
    # EVENT / CHECKPOINT
    # ------------------------------------------------------------------------

    def _event(
        self,
        resource_id: str,
        event_type: LinkGraphEventType,
        state: LinkGraphState,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> LinkGraphEvent:
        event = LinkGraphEvent(
            event_id=self._digest_payload(
                resource_id,
                event_type.value,
                state.value,
                self._now(),
            ),
            event_type=event_type,
            resource_id=resource_id,
            state=state,
            timestamp=self._now(),
            metadata=dict(metadata or {}),
        )

        self.backend.persist_event(event)
        return event

    def _checkpoint(
        self,
        resource_id: str,
        checkpoint_type: LinkGraphCheckpointType,
        state: LinkGraphState,
        completed_units: int,
        observed_units: int,
        partial: bool,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> LinkGraphCheckpoint:
        checkpoint = LinkGraphCheckpoint(
            checkpoint_id=self._digest_payload(
                resource_id,
                checkpoint_type.value,
                completed_units,
                observed_units,
            ),
            resource_id=resource_id,
            checkpoint_type=checkpoint_type,
            state=state,
            completed_units=max(0, completed_units),
            observed_units=max(0, observed_units),
            partial=partial,
            created_at=self._now(),
            metadata=dict(metadata or {}),
        )

        self.backend.persist_checkpoint(checkpoint)

        self._event(
            resource_id,
            LinkGraphEventType.CHECKPOINT_CREATED,
            state,
            {
                "checkpoint_id": checkpoint.checkpoint_id,
                "checkpoint_type": checkpoint_type.value,
            },
        )

        return checkpoint

    # ------------------------------------------------------------------------
    # NORMALIZATION
    # ------------------------------------------------------------------------

    def _normalize_edge(
        self,
        edge: LinkGraphEdgeInput,
    ) -> LinkGraphEdgeInput:
        return LinkGraphEdgeInput(
            source_node_id=str(edge.source_node_id or ""),
            target_node_id=str(edge.target_node_id or ""),
            source_host_id=edge.source_host_id,
            target_host_id=edge.target_host_id,
            source_domain_id=edge.source_domain_id,
            target_domain_id=edge.target_domain_id,
            anchor_text=edge.anchor_text,
            anchor_keyword_density=self._clamp(
                self._safe_float(edge.anchor_keyword_density)
            ),
            anchor_repetition=self._clamp(
                self._safe_float(edge.anchor_repetition)
            ),
            contextual_relevance=self._clamp(
                self._safe_float(edge.contextual_relevance, 0.5)
            ),
            placement_quality=self._clamp(
                self._safe_float(edge.placement_quality, 0.5)
            ),
            sponsored=bool(edge.sponsored),
            nofollow=bool(edge.nofollow),
            ugc=bool(edge.ugc),
            link_age_days=(
                None
                if edge.link_age_days is None
                else max(0.0, self._safe_float(edge.link_age_days))
            ),
            created_at=edge.created_at,
            confidence=self._clamp(
                self._safe_float(edge.confidence, 1.0)
            ),
            partial=bool(edge.partial),
        )

    def _normalize_input(
        self,
        data: LinkGraphDocumentInput,
    ) -> LinkGraphDocumentInput:
        return LinkGraphDocumentInput(
            identity=data.identity,

            total_inbound_links=self._safe_int(data.total_inbound_links),
            total_outbound_links=self._safe_int(data.total_outbound_links),

            unique_inbound_domains=self._safe_int(
                data.unique_inbound_domains
            ),
            unique_outbound_domains=self._safe_int(
                data.unique_outbound_domains
            ),
            unique_inbound_hosts=self._safe_int(
                data.unique_inbound_hosts
            ),
            unique_outbound_hosts=self._safe_int(
                data.unique_outbound_hosts
            ),

            inbound_link_diversity=self._clamp(
                self._safe_float(data.inbound_link_diversity, 1.0)
            ),
            outbound_link_diversity=self._clamp(
                self._safe_float(data.outbound_link_diversity, 1.0)
            ),

            graph_degree=max(
                0.0,
                self._safe_float(data.graph_degree),
            ),
            graph_centrality=self._clamp(
                self._safe_float(data.graph_centrality)
            ),
            clustering_coefficient=self._clamp(
                self._safe_float(data.clustering_coefficient)
            ),
            graph_density=self._clamp(
                self._safe_float(data.graph_density)
            ),

            authority_concentration=self._clamp(
                self._safe_float(data.authority_concentration)
            ),
            artificial_authority_score=self._clamp(
                self._safe_float(data.artificial_authority_score)
            ),

            low_quality_source_ratio=self._clamp(
                self._safe_float(data.low_quality_source_ratio)
            ),
            source_reputation=self._clamp(
                self._safe_float(data.source_reputation, 0.5)
            ),

            anchor=data.anchor,
            reciprocity=data.reciprocity,
            network=data.network,
            temporal=data.temporal,
            cross_resource=data.cross_resource,

            nodes=tuple(data.nodes),
            edges=tuple(
                self._normalize_edge(edge)
                for edge in data.edges
            ),

            identity_anomaly_score=self._clamp(
                self._safe_float(data.identity_anomaly_score)
            ),
            technical_anomaly_score=self._clamp(
                self._safe_float(data.technical_anomaly_score)
            ),

            confidence=self._clamp(
                self._safe_float(data.confidence, 1.0)
            ),
            partial=bool(data.partial),
        )

    # ------------------------------------------------------------------------
    # VALIDATION
    # ------------------------------------------------------------------------

    def _validate(
        self,
        data: LinkGraphDocumentInput,
    ) -> Tuple[bool, Tuple[str, ...]]:
        errors: List[str] = []

        if not data.identity.resource_id:
            errors.append("resource_id is required")

        if data.total_inbound_links < 0:
            errors.append("total_inbound_links cannot be negative")

        if data.total_outbound_links < 0:
            errors.append("total_outbound_links cannot be negative")

        if data.unique_inbound_domains > data.total_inbound_links:
            errors.append(
                "unique_inbound_domains exceeds total_inbound_links"
            )

        if data.unique_outbound_domains > data.total_outbound_links:
            errors.append(
                "unique_outbound_domains exceeds total_outbound_links"
            )

        if not self.policy.partial_allowed and data.partial:
            errors.append("partial observations are disabled by policy")

        return not errors, tuple(errors)

    # ------------------------------------------------------------------------
    # SIGNAL BUILDING
    # ------------------------------------------------------------------------

    def _signal(
        self,
        data: LinkGraphDocumentInput,
        family: LinkGraphSignalFamily,
        signal_type: LinkGraphSignalType,
        value: float,
        confidence: float,
        evidence_count: int = 1,
        explanation: str = "",
        partial: Optional[bool] = None,
    ) -> LinkGraphSignal:
        return LinkGraphSignal(
            signal_id=self._digest_payload(
                data.identity.resource_id,
                family.value,
                signal_type.value,
            ),
            family=family,
            signal_type=signal_type,
            value=self._clamp(value),
            confidence=self._clamp(confidence),
            evidence_count=max(1, evidence_count),
            explanation=explanation,
            partial=data.partial if partial is None else partial,
        )

    def _link_signals(
        self,
        data: LinkGraphDocumentInput,
    ) -> List[LinkGraphSignal]:
        signals: List[LinkGraphSignal] = []

        total_inbound = max(1, data.total_inbound_links)
        total_outbound = max(1, data.total_outbound_links)

        inbound_source_diversity = (
            data.unique_inbound_domains / total_inbound
        )

        outbound_source_diversity = (
            data.unique_outbound_domains / total_outbound
        )

        low_inbound_diversity = 1.0 - self._clamp(
            inbound_source_diversity
        )

        low_outbound_diversity = 1.0 - self._clamp(
            outbound_source_diversity
        )

        if data.total_inbound_links >= self.policy.min_link_volume_for_graph_analysis:
            if low_inbound_diversity >= 0.70:
                signals.append(
                    self._signal(
                        data,
                        LinkGraphSignalFamily.LINK_SOURCE_DIVERSITY,
                        LinkGraphSignalType.LOW_SOURCE_DIVERSITY,
                        low_inbound_diversity,
                        data.confidence,
                        explanation=(
                            "Inbound links are concentrated among relatively "
                            "few source domains."
                        ),
                    )
                )

            if low_outbound_diversity >= 0.70:
                signals.append(
                    self._signal(
                        data,
                        LinkGraphSignalFamily.LINK_TARGET_DIVERSITY,
                        LinkGraphSignalType.LOW_TARGET_DIVERSITY,
                        low_outbound_diversity,
                        data.confidence,
                        explanation=(
                            "Outbound links are concentrated among relatively "
                            "few target domains."
                        ),
                    )
                )

        volume_signal = self._clamp(
            math.log1p(data.total_inbound_links)
            / math.log1p(1_000_000)
        )

        if data.total_inbound_links > 0 and volume_signal >= 0.80:
            signals.append(
                self._signal(
                    data,
                    LinkGraphSignalFamily.LINK_VOLUME,
                    LinkGraphSignalType.ABNORMAL_LINK_VOLUME,
                    volume_signal,
                    data.confidence,
                    explanation=(
                        "Observed inbound link volume is unusually large "
                        "relative to the reference scale."
                    ),
                )
            )

        density_signal = self._clamp(data.graph_density)

        if density_signal >= 0.80:
            signals.append(
                self._signal(
                    data,
                    LinkGraphSignalFamily.LINK_DENSITY,
                    LinkGraphSignalType.ABNORMAL_LINK_DENSITY,
                    density_signal,
                    data.confidence,
                    explanation=(
                        "Graph connectivity density is unusually concentrated."
                    ),
                )
            )

        return signals

    # ------------------------------------------------------------------------
    # ANCHOR SIGNALS
    # ------------------------------------------------------------------------

    def _anchor_signals(
        self,
        data: LinkGraphDocumentInput,
    ) -> List[LinkGraphSignal]:
        signals: List[LinkGraphSignal] = []

        anchor = data.anchor

        concentration = self._clamp(anchor.anchor_concentration)

        if concentration >= self.policy.anchor_manipulation_threshold:
            signals.append(
                self._signal(
                    data,
                    LinkGraphSignalFamily.ANCHOR_CONCENTRATION,
                    LinkGraphSignalType.ANCHOR_TEXT_CONCENTRATION,
                    concentration,
                    data.confidence,
                    evidence_count=max(1, anchor.total_anchor_links),
                    explanation=(
                        "Anchor text is unusually concentrated around a "
                        "small set of patterns."
                    ),
                    partial=anchor.partial,
                )
            )

        repetition = self._clamp(anchor.repeated_anchor_ratio)

        if repetition >= 0.70:
            signals.append(
                self._signal(
                    data,
                    LinkGraphSignalFamily.ANCHOR_TEXT,
                    LinkGraphSignalType.ANCHOR_TEXT_REPETITION,
                    repetition,
                    data.confidence,
                    evidence_count=max(1, anchor.total_anchor_links),
                    explanation=(
                        "Repeated anchor text appears at unusually high "
                        "frequency."
                    ),
                    partial=anchor.partial,
                )
            )

        manipulation = self._clamp(
            max(
                anchor.exact_match_ratio,
                anchor.keyword_anchor_ratio,
                anchor.commercial_anchor_ratio,
            )
        )

        if manipulation >= self.policy.anchor_manipulation_threshold:
            signals.append(
                self._signal(
                    data,
                    LinkGraphSignalFamily.ANCHOR_MANIPULATION,
                    LinkGraphSignalType.ANCHOR_KEYWORD_MANIPULATION,
                    manipulation,
                    data.confidence,
                    evidence_count=max(1, anchor.total_anchor_links),
                    explanation=(
                        "Anchor-text composition contains a strong "
                        "concentration of exact, keyword-oriented, or "
                        "commercial patterns."
                    ),
                    partial=anchor.partial,
                )
            )

        return signals

    # ------------------------------------------------------------------------
    # RECIPROCITY SIGNALS
    # ------------------------------------------------------------------------

    def _reciprocity_signals(
        self,
        data: LinkGraphDocumentInput,
    ) -> List[LinkGraphSignal]:
        signals: List[LinkGraphSignal] = []

        reciprocity = data.reciprocity

        ratio = self._clamp(reciprocity.reciprocal_ratio)

        if ratio >= self.policy.reciprocity_threshold:
            signals.append(
                self._signal(
                    data,
                    LinkGraphSignalFamily.RECIPROCAL_LINKING,
                    LinkGraphSignalType.HIGH_RECIPROCITY,
                    ratio,
                    data.confidence,
                    evidence_count=max(1, reciprocity.outbound_pairs),
                    explanation=(
                        "A high proportion of observed link relationships "
                        "are reciprocal."
                    ),
                    partial=reciprocity.partial,
                )
            )

        cluster = self._clamp(reciprocity.mutual_cluster_ratio)

        if cluster >= self.policy.reciprocity_threshold:
            signals.append(
                self._signal(
                    data,
                    LinkGraphSignalFamily.MUTUAL_LINKING,
                    LinkGraphSignalType.RECIPROCAL_CLUSTER,
                    cluster,
                    data.confidence,
                    evidence_count=max(1, reciprocity.outbound_pairs),
                    explanation=(
                        "Mutual-link relationships are concentrated into "
                        "a graph cluster."
                    ),
                    partial=reciprocity.partial,
                )
            )

        exchange = self._clamp(reciprocity.exchange_pattern_score)

        if exchange >= self.policy.reciprocity_threshold:
            signals.append(
                self._signal(
                    data,
                    LinkGraphSignalFamily.LINK_EXCHANGE,
                    LinkGraphSignalType.LINK_EXCHANGE_PATTERN,
                    exchange,
                    data.confidence,
                    evidence_count=max(1, reciprocity.outbound_pairs),
                    explanation=(
                        "The observed graph contains a structured link "
                        "exchange pattern."
                    ),
                    partial=reciprocity.partial,
                )
            )

        return signals

    # ------------------------------------------------------------------------
    # NETWORK SIGNALS
    # ------------------------------------------------------------------------

    def _network_signals(
        self,
        data: LinkGraphDocumentInput,
    ) -> List[LinkGraphSignal]:
        signals: List[LinkGraphSignal] = []

        network = data.network

        if network.cluster_size >= self.policy.min_cluster_size_for_network_analysis:
            internal_density = self._clamp(network.internal_density)

            if internal_density >= 0.80:
                signals.append(
                    self._signal(
                        data,
                        LinkGraphSignalFamily.LINK_FARM,
                        LinkGraphSignalType.LINK_FARM_PATTERN,
                        internal_density,
                        data.confidence,
                        evidence_count=network.cluster_size,
                        explanation=(
                            "A graph cluster exhibits unusually dense "
                            "internal connectivity."
                        ),
                        partial=network.partial,
                    )
                )

            coordination = self._clamp(
                network.network_coordination_score
            )

            if coordination >= self.policy.network_coordination_threshold:
                signals.append(
                    self._signal(
                        data,
                        LinkGraphSignalFamily.LINK_NETWORK,
                        LinkGraphSignalType.LINK_NETWORK_PATTERN,
                        coordination,
                        data.confidence,
                        evidence_count=network.cluster_size,
                        explanation=(
                            "Multiple graph resources exhibit coordinated "
                            "linking behavior."
                        ),
                        partial=network.partial,
                    )
                )

            satellite = self._clamp(network.satellite_ratio)

            if satellite >= 0.70:
                signals.append(
                    self._signal(
                        data,
                        LinkGraphSignalFamily.SATELLITE_NETWORK,
                        LinkGraphSignalType.SATELLITE_NETWORK_PATTERN,
                        satellite,
                        data.confidence,
                        evidence_count=network.cluster_size,
                        explanation=(
                            "The graph contains a high concentration of "
                            "satellite-like nodes around a smaller target set."
                        ),
                        partial=network.partial,
                    )
                )

            identity = self._clamp(network.shared_identity_ratio)

            if identity >= 0.80:
                signals.append(
                    self._signal(
                        data,
                        LinkGraphSignalFamily.PRIVATE_LINK_NETWORK,
                        LinkGraphSignalType.PRIVATE_LINK_NETWORK_PATTERN,
                        identity,
                        data.confidence,
                        evidence_count=network.cluster_size,
                        explanation=(
                            "Graph resources share unusually strong identity "
                            "or ownership-like relationships."
                        ),
                        partial=network.partial,
                    )
                )

        return signals

    # ------------------------------------------------------------------------
    # GRAPH SIGNALS
    # ------------------------------------------------------------------------

    def _graph_signals(
        self,
        data: LinkGraphDocumentInput,
    ) -> List[LinkGraphSignal]:
        signals: List[LinkGraphSignal] = []

        clustering = self._clamp(data.clustering_coefficient)

        if clustering >= 0.85:
            signals.append(
                self._signal(
                    data,
                    LinkGraphSignalFamily.GRAPH_CLUSTERING,
                    LinkGraphSignalType.GRAPH_CLUSTER_ANOMALY,
                    clustering,
                    data.confidence,
                    explanation=(
                        "Graph clustering is unusually high for the observed "
                        "resource neighborhood."
                    ),
                )
            )

        centrality = self._clamp(data.graph_centrality)

        if centrality >= 0.90:
            signals.append(
                self._signal(
                    data,
                    LinkGraphSignalFamily.GRAPH_CENTRALITY,
                    LinkGraphSignalType.GRAPH_CENTRALITY_ANOMALY,
                    centrality,
                    data.confidence,
                    explanation=(
                        "Graph centrality is unusually concentrated around "
                        "the observed resource."
                    ),
                )
            )

        topology = self._clamp(
            max(
                data.graph_density,
                data.clustering_coefficient,
                data.authority_concentration,
            )
        )

        if topology >= 0.85:
            signals.append(
                self._signal(
                    data,
                    LinkGraphSignalFamily.GRAPH_TOPOLOGY,
                    LinkGraphSignalType.GRAPH_TOPOLOGY_ANOMALY,
                    topology,
                    data.confidence,
                    explanation=(
                        "Multiple graph-topology properties indicate an "
                        "unusually concentrated structure."
                    ),
                )
            )

        authority = self._clamp(
            max(
                data.artificial_authority_score,
                data.authority_concentration,
            )
        )

        if authority >= self.policy.authority_manipulation_threshold:
            signals.append(
                self._signal(
                    data,
                    LinkGraphSignalFamily.ARTIFICIAL_AUTHORITY,
                    LinkGraphSignalType.ARTIFICIAL_AUTHORITY_PATTERN,
                    authority,
                    data.confidence,
                    explanation=(
                        "Observed graph evidence indicates possible "
                        "artificial concentration of authority."
                    ),
                )
            )

        return signals

    # ------------------------------------------------------------------------
    # CROSS-RESOURCE SIGNALS
    # ------------------------------------------------------------------------

    def _cross_resource_signals(
        self,
        data: LinkGraphDocumentInput,
    ) -> List[LinkGraphSignal]:
        signals: List[LinkGraphSignal] = []

        evidence = data.cross_resource

        duplicated = self._clamp(
            evidence.duplicated_link_pattern_score
        )

        if duplicated >= 0.70:
            signals.append(
                self._signal(
                    data,
                    LinkGraphSignalFamily.CROSS_RESOURCE,
                    LinkGraphSignalType.CROSS_RESOURCE_DUPLICATED_LINK_PATTERN,
                    duplicated,
                    data.confidence,
                    evidence_count=max(
                        1,
                        evidence.related_resource_count,
                    ),
                    explanation=(
                        "Similar link structures recur across multiple "
                        "resources."
                    ),
                    partial=evidence.partial,
                )
            )

        cross_domain = self._clamp(
            evidence.cross_domain_coordination_score
        )

        if cross_domain >= 0.70:
            signals.append(
                self._signal(
                    data,
                    LinkGraphSignalFamily.CROSS_DOMAIN,
                    LinkGraphSignalType.CROSS_DOMAIN_LINK_PATTERN,
                    cross_domain,
                    data.confidence,
                    evidence_count=max(
                        1,
                        evidence.coordinated_resource_count,
                    ),
                    explanation=(
                        "Link behavior appears coordinated across domains."
                    ),
                    partial=evidence.partial,
                )
            )

        cross_host = self._clamp(
            evidence.cross_host_coordination_score
        )

        if cross_host >= 0.70:
            signals.append(
                self._signal(
                    data,
                    LinkGraphSignalFamily.CROSS_HOST,
                    LinkGraphSignalType.CROSS_HOST_LINK_PATTERN,
                    cross_host,
                    data.confidence,
                    evidence_count=max(
                        1,
                        evidence.coordinated_resource_count,
                    ),
                    explanation=(
                        "Link behavior appears coordinated across hosts."
                    ),
                    partial=evidence.partial,
                )
            )

        return signals

    # ------------------------------------------------------------------------
    # TEMPORAL SIGNALS
    # ------------------------------------------------------------------------

    def _temporal_signals(
        self,
        data: LinkGraphDocumentInput,
    ) -> List[LinkGraphSignal]:
        signals: List[LinkGraphSignal] = []

        temporal = data.temporal

        velocity = self._clamp(temporal.velocity_anomaly)

        if velocity >= self.policy.velocity_anomaly_threshold:
            signals.append(
                self._signal(
                    data,
                    LinkGraphSignalFamily.VELOCITY,
                    LinkGraphSignalType.LINK_VELOCITY_ANOMALY,
                    velocity,
                    data.confidence,
                    evidence_count=max(1, temporal.new_links),
                    explanation=(
                        "The observed rate of link acquisition differs "
                        "substantially from the expected temporal pattern."
                    ),
                    partial=temporal.partial,
                )
            )

        cluster = self._clamp(
            temporal.temporal_cluster_score
        )

        if cluster >= 0.70:
            signals.append(
                self._signal(
                    data,
                    LinkGraphSignalFamily.TEMPORAL,
                    LinkGraphSignalType.TEMPORAL_CLUSTER_ANOMALY,
                    cluster,
                    data.confidence,
                    evidence_count=max(1, temporal.links_observed),
                    explanation=(
                        "Link creation appears unusually concentrated into "
                        "temporal clusters."
                    ),
                    partial=temporal.partial,
                )
            )

        historical = self._clamp(
            temporal.historical_repetition_score
        )

        if historical >= 0.70:
            signals.append(
                self._signal(
                    data,
                    LinkGraphSignalFamily.HISTORICAL,
                    LinkGraphSignalType.HISTORICAL_REPETITION,
                    historical,
                    data.confidence,
                    evidence_count=max(1, temporal.links_observed),
                    explanation=(
                        "Historical observations contain repeated graph "
                        "manipulation-like structures."
                    ),
                    partial=temporal.partial,
                )
            )

        return signals

    # ------------------------------------------------------------------------
    # CONTEXT / SOURCE / IDENTITY SIGNALS
    # ------------------------------------------------------------------------

    def _context_signals(
        self,
        data: LinkGraphDocumentInput,
    ) -> List[LinkGraphSignal]:
        signals: List[LinkGraphSignal] = []

        low_quality = self._clamp(
            data.low_quality_source_ratio
        )

        if low_quality >= 0.70:
            signals.append(
                self._signal(
                    data,
                    LinkGraphSignalFamily.LOW_QUALITY_SOURCE,
                    LinkGraphSignalType.LOW_QUALITY_SOURCE_PATTERN,
                    low_quality,
                    data.confidence,
                    explanation=(
                        "A high proportion of observed links originate from "
                        "low-quality sources."
                    ),
                )
            )

        reputation = 1.0 - self._clamp(
            data.source_reputation,
            0.0,
            1.0,
        )

        if reputation >= 0.70:
            signals.append(
                self._signal(
                    data,
                    LinkGraphSignalFamily.SOURCE_REPUTATION,
                    LinkGraphSignalType.SOURCE_REPUTATION_ANOMALY,
                    reputation,
                    data.confidence,
                    explanation=(
                        "Source reputation evidence is unusually weak."
                    ),
                )
            )

        identity = self._clamp(
            data.identity_anomaly_score
        )

        if identity >= 0.70:
            signals.append(
                self._signal(
                    data,
                    LinkGraphSignalFamily.IDENTITY,
                    LinkGraphSignalType.IDENTITY_GRAPH_ANOMALY,
                    identity,
                    data.confidence,
                    explanation=(
                        "Graph relationships contain identity-related "
                        "anomalies."
                    ),
                )
            )

        technical = self._clamp(
            data.technical_anomaly_score
        )

        if technical >= 0.70:
            signals.append(
                self._signal(
                    data,
                    LinkGraphSignalFamily.TECHNICAL,
                    LinkGraphSignalType.TECHNICAL_GRAPH_ANOMALY,
                    technical,
                    data.confidence,
                    explanation=(
                        "Technical graph evidence contains anomalous "
                        "link-structure characteristics."
                    ),
                )
            )

        return signals

    # ------------------------------------------------------------------------
    # ALL SIGNALS
    # ------------------------------------------------------------------------

    def _extract_signals(
        self,
        data: LinkGraphDocumentInput,
    ) -> List[LinkGraphSignal]:
        signals: List[LinkGraphSignal] = []

        signals.extend(self._link_signals(data))
        signals.extend(self._anchor_signals(data))
        signals.extend(self._reciprocity_signals(data))
        signals.extend(self._network_signals(data))
        signals.extend(self._graph_signals(data))
        signals.extend(self._cross_resource_signals(data))
        signals.extend(self._temporal_signals(data))
        signals.extend(self._context_signals(data))

        return signals[: self.policy.max_signals_per_resource]

    # ------------------------------------------------------------------------
    # SIGNAL → EVIDENCE
    # ------------------------------------------------------------------------

    @staticmethod
    def _strength(
        score: float,
    ) -> LinkGraphEvidenceStrength:
        if score <= 0.0:
            return LinkGraphEvidenceStrength.NONE
        if score < 0.25:
            return LinkGraphEvidenceStrength.WEAK
        if score < 0.50:
            return LinkGraphEvidenceStrength.MODERATE
        if score < 0.80:
            return LinkGraphEvidenceStrength.STRONG
        return LinkGraphEvidenceStrength.VERY_STRONG

    @staticmethod
    def _evidence_kind(
        family: LinkGraphSignalFamily,
    ) -> LinkGraphEvidenceKind:
        mapping = {
            LinkGraphSignalFamily.ANCHOR_TEXT: LinkGraphEvidenceKind.ANCHOR,
            LinkGraphSignalFamily.ANCHOR_CONCENTRATION: LinkGraphEvidenceKind.ANCHOR,
            LinkGraphSignalFamily.ANCHOR_MANIPULATION: LinkGraphEvidenceKind.ANCHOR,
            LinkGraphSignalFamily.RECIPROCAL_LINKING: LinkGraphEvidenceKind.RECIPROCITY,
            LinkGraphSignalFamily.MUTUAL_LINKING: LinkGraphEvidenceKind.RECIPROCITY,
            LinkGraphSignalFamily.LINK_EXCHANGE: LinkGraphEvidenceKind.RECIPROCITY,
            LinkGraphSignalFamily.LINK_FARM: LinkGraphEvidenceKind.NETWORK,
            LinkGraphSignalFamily.LINK_NETWORK: LinkGraphEvidenceKind.NETWORK,
            LinkGraphSignalFamily.SATELLITE_NETWORK: LinkGraphEvidenceKind.NETWORK,
            LinkGraphSignalFamily.PRIVATE_LINK_NETWORK: LinkGraphEvidenceKind.NETWORK,
            LinkGraphSignalFamily.TEMPORAL: LinkGraphEvidenceKind.TEMPORAL,
            LinkGraphSignalFamily.HISTORICAL: LinkGraphEvidenceKind.HISTORICAL,
            LinkGraphSignalFamily.VELOCITY: LinkGraphEvidenceKind.TEMPORAL,
            LinkGraphSignalFamily.CROSS_RESOURCE: LinkGraphEvidenceKind.CROSS_RESOURCE,
            LinkGraphSignalFamily.CROSS_DOMAIN: LinkGraphEvidenceKind.CROSS_RESOURCE,
            LinkGraphSignalFamily.CROSS_HOST: LinkGraphEvidenceKind.CROSS_RESOURCE,
            LinkGraphSignalFamily.IDENTITY: LinkGraphEvidenceKind.IDENTITY,
            LinkGraphSignalFamily.TECHNICAL: LinkGraphEvidenceKind.TECHNICAL,
        }

        return mapping.get(
            family,
            LinkGraphEvidenceKind.GRAPH,
        )

    def _signals_to_evidence(
        self,
        data: LinkGraphDocumentInput,
        signals: Sequence[LinkGraphSignal],
        lineage: LinkGraphLineage,
    ) -> List[LinkGraphEvidence]:
        timestamp = self._now()

        evidence: List[LinkGraphEvidence] = []

        for signal in signals:
            confidence = self._clamp(
                signal.confidence * data.confidence
            )

            if confidence < self.policy.minimum_confidence:
                continue

            evidence.append(
                LinkGraphEvidence(
                    evidence_id=self._digest_payload(
                        data.identity.resource_id,
                        signal.signal_id,
                    ),
                    kind=self._evidence_kind(signal.family),
                    family=signal.family,
                    signal_type=signal.signal_type,
                    score=self._clamp(signal.value),
                    strength=self._strength(signal.value),
                    confidence=confidence,
                    explanation=signal.explanation,
                    source=signal.source,
                    observed_at=timestamp,
                    lineage=lineage,
                    partial=signal.partial,
                )
            )

        return evidence[: self.policy.max_evidence_per_resource]

    # ------------------------------------------------------------------------
    # RISK AGGREGATION
    # ------------------------------------------------------------------------

    def _aggregate_risk(
        self,
        evidence: Sequence[LinkGraphEvidence],
    ) -> float:
        if not evidence:
            return 0.0

        weighted_sum = 0.0
        weight_sum = 0.0

        family_weights = {
            LinkGraphSignalFamily.LINK_FARM: 1.30,
            LinkGraphSignalFamily.LINK_NETWORK: 1.25,
            LinkGraphSignalFamily.PRIVATE_LINK_NETWORK: 1.25,
            LinkGraphSignalFamily.SATELLITE_NETWORK: 1.20,
            LinkGraphSignalFamily.ANCHOR_MANIPULATION: 1.20,
            LinkGraphSignalFamily.ANCHOR_CONCENTRATION: 1.10,
            LinkGraphSignalFamily.RECIPROCAL_LINKING: 1.00,
            LinkGraphSignalFamily.LINK_EXCHANGE: 1.10,
            LinkGraphSignalFamily.ARTIFICIAL_AUTHORITY: 1.30,
            LinkGraphSignalFamily.CROSS_DOMAIN: 1.20,
            LinkGraphSignalFamily.CROSS_RESOURCE: 1.10,
            LinkGraphSignalFamily.VELOCITY: 1.00,
            LinkGraphSignalFamily.HISTORICAL: 1.00,
            LinkGraphSignalFamily.LOW_QUALITY_SOURCE: 0.80,
            LinkGraphSignalFamily.SOURCE_REPUTATION: 0.70,
            LinkGraphSignalFamily.GRAPH_TOPOLOGY: 1.00,
            LinkGraphSignalFamily.GRAPH_CLUSTERING: 0.95,
            LinkGraphSignalFamily.GRAPH_CENTRALITY: 0.90,
            LinkGraphSignalFamily.IDENTITY: 0.80,
            LinkGraphSignalFamily.TECHNICAL: 0.60,
        }

        for item in evidence:
            weight = family_weights.get(item.family, 0.75)

            if item.partial:
                weight *= 0.75

            confidence_weight = self._clamp(
                item.confidence
            )

            effective_weight = weight * confidence_weight

            weighted_sum += (
                item.score * effective_weight
            )
            weight_sum += effective_weight

        if weight_sum <= 0.0:
            return 0.0

        return self._clamp(
            weighted_sum / weight_sum
        )

    # ------------------------------------------------------------------------
    # CONFIDENCE
    # ------------------------------------------------------------------------

    def _aggregate_confidence(
        self,
        data: LinkGraphDocumentInput,
        evidence: Sequence[LinkGraphEvidence],
    ) -> float:
        if not evidence:
            return self._clamp(data.confidence)

        total = sum(
            self._clamp(item.confidence)
            for item in evidence
        )

        average = total / len(evidence)

        if data.partial:
            average *= 0.80

        if len(evidence) >= 10:
            average = min(
                1.0,
                average + 0.05,
            )

        return self._clamp(average)

    # ------------------------------------------------------------------------
    # RISK BAND
    # ------------------------------------------------------------------------

    def _risk_band(
        self,
        risk_score: float,
    ) -> LinkGraphRiskBand:
        score = self._clamp(risk_score)

        if score < 0.10:
            return LinkGraphRiskBand.NONE

        if score < 0.30:
            return LinkGraphRiskBand.LOW

        if score < self.policy.review_threshold:
            return LinkGraphRiskBand.MODERATE

        if score < self.policy.high_risk_threshold:
            return LinkGraphRiskBand.HIGH

        if score < self.policy.critical_threshold:
            return LinkGraphRiskBand.VERY_HIGH

        return LinkGraphRiskBand.CRITICAL

    # ------------------------------------------------------------------------
    # DECISION
    # ------------------------------------------------------------------------

    def _decision(
        self,
        risk_score: float,
        confidence: float,
        partial: bool,
        rejected: bool = False,
    ) -> LinkGraphDecision:
        if rejected:
            return LinkGraphDecision.REJECTED_INPUT

        if partial:
            return LinkGraphDecision.PARTIAL_EVIDENCE

        if confidence < self.policy.minimum_confidence:
            return LinkGraphDecision.DEFERRED

        if risk_score >= self.policy.review_threshold:
            return LinkGraphDecision.REVIEW_REQUIRED

        return LinkGraphDecision.EVIDENCE_READY

    # ------------------------------------------------------------------------
    # REASONS
    # ------------------------------------------------------------------------

    def _reasons(
        self,
        evidence: Sequence[LinkGraphEvidence],
    ) -> Tuple[str, ...]:
        reasons: List[str] = []

        ordered = sorted(
            evidence,
            key=lambda item: (
                item.score * item.confidence
            ),
            reverse=True,
        )

        seen = set()

        for item in ordered:
            reason = item.explanation.strip()

            if not reason or reason in seen:
                continue

            seen.add(reason)
            reasons.append(reason)

            if len(reasons) >= 12:
                break

        return tuple(reasons)

    # ------------------------------------------------------------------------
    # ANALYZE ONE RESOURCE
    # ------------------------------------------------------------------------

    def analyze(
        self,
        data: LinkGraphDocumentInput,
    ) -> LinkGraphResult:
        resource_id = data.identity.resource_id
        started_at = self._now()

        self._event(
            resource_id,
            LinkGraphEventType.ANALYSIS_STARTED,
            LinkGraphState.RECEIVED,
            {
                "architecture_version": ARCHITECTURE_VERSION,
                "phase": PHASE,
            },
        )

        normalized = self._normalize_input(data)

        self._event(
            resource_id,
            LinkGraphEventType.INPUT_NORMALIZED,
            LinkGraphState.NORMALIZING,
        )

        valid, errors = self._validate(normalized)

        self._event(
            resource_id,
            LinkGraphEventType.VALIDATION_COMPLETED,
            LinkGraphState.VALIDATING,
            {
                "valid": valid,
                "error_count": len(errors),
            },
        )

        lineage = LinkGraphLineage(
            resource_id=resource_id,
            source_evidence_ids=(),
            parent_result_ids=(),
            lineage_metadata={
                "phase": PHASE,
                "started_at": started_at,
            },
        )

        if not valid:
            result = LinkGraphResult(
                result_id=self._digest_payload(
                    resource_id,
                    "rejected",
                    errors,
                ),
                identity=normalized.identity,
                state=LinkGraphState.REJECTED,
                risk_score=0.0,
                risk_band=LinkGraphRiskBand.UNKNOWN,
                evidence=(),
                confidence=0.0,
                partial=False,
                decision=LinkGraphDecision.REJECTED_INPUT,
                reasons=errors,
                created_at=self._now(),
                lineage=lineage,
            )

            self.backend.persist_result(result)

            self._event(
                resource_id,
                LinkGraphEventType.ANALYSIS_REJECTED,
                LinkGraphState.REJECTED,
                {"errors": errors},
            )

            return result

        if self.policy.checkpoint_enabled:
            self._checkpoint(
                resource_id,
                LinkGraphCheckpointType.INPUT_NORMALIZED,
                LinkGraphState.NORMALIZING,
                1,
                1,
                normalized.partial,
            )

        signals = self._extract_signals(normalized)

        for signal in signals:
            event_type = (
                LinkGraphEventType.GRAPH_SIGNAL_DETECTED
                if signal.family
                in {
                    LinkGraphSignalFamily.GRAPH_DENSITY,
                    LinkGraphSignalFamily.GRAPH_CLUSTERING,
                    LinkGraphSignalFamily.GRAPH_COMPONENT,
                    LinkGraphSignalFamily.GRAPH_CENTRALITY,
                    LinkGraphSignalFamily.GRAPH_TOPOLOGY,
                    LinkGraphSignalFamily.LINK_FARM,
                    LinkGraphSignalFamily.LINK_NETWORK,
                }
                else LinkGraphEventType.LINK_SIGNAL_DETECTED
            )

            self._event(
                resource_id,
                event_type,
                LinkGraphState.PATTERN_ANALYSIS,
                {
                    "signal_type": signal.signal_type.value,
                    "family": signal.family.value,
                    "value": signal.value,
                    "confidence": signal.confidence,
                },
            )

        if self.policy.checkpoint_enabled:
            self._checkpoint(
                resource_id,
                LinkGraphCheckpointType.SIGNALS_EXTRACTED,
                LinkGraphState.PATTERN_ANALYSIS,
                len(signals),
                len(signals),
                normalized.partial,
            )

        evidence = self._signals_to_evidence(
            normalized,
            signals,
            lineage,
        )

        self._event(
            resource_id,
            LinkGraphEventType.EVIDENCE_AGGREGATED,
            LinkGraphState.EVIDENCE_AGGREGATION,
            {
                "evidence_count": len(evidence),
            },
        )

        if self.policy.checkpoint_enabled:
            self._checkpoint(
                resource_id,
                LinkGraphCheckpointType.EVIDENCE_AGGREGATED,
                LinkGraphState.EVIDENCE_AGGREGATION,
                len(evidence),
                len(evidence),
                normalized.partial,
            )

        risk_score = self._aggregate_risk(
            evidence
        )

        confidence = self._aggregate_confidence(
            normalized,
            evidence,
        )

        self._event(
            resource_id,
            LinkGraphEventType.CONFIDENCE_CALCULATED,
            LinkGraphState.CONFIDENCE_CALCULATION,
            {
                "risk_score": risk_score,
                "confidence": confidence,
            },
        )

        risk_band = self._risk_band(
            risk_score
        )

        decision = self._decision(
            risk_score,
            confidence,
            normalized.partial,
        )

        reasons = self._reasons(
            evidence
        )

        state = (
            LinkGraphState.PARTIAL
            if normalized.partial
            else LinkGraphState.COMPLETED
        )

        if decision == LinkGraphDecision.DEFERRED:
            state = LinkGraphState.DEFERRED

        result = LinkGraphResult(
            result_id=self._digest_payload(
                resource_id,
                risk_score,
                confidence,
                risk_band.value,
                decision.value,
            ),
            identity=normalized.identity,
            state=state,
            risk_score=risk_score,
            risk_band=risk_band,
            evidence=tuple(evidence),
            confidence=confidence,
            partial=normalized.partial,
            decision=decision,
            reasons=reasons,
            created_at=self._now(),
            lineage=lineage,
        )

        self.backend.persist_result(
            result
        )

        if self.policy.checkpoint_enabled:
            self._checkpoint(
                resource_id,
                LinkGraphCheckpointType.RESULT_PERSISTED,
                state,
                1,
                1,
                normalized.partial,
                {
                    "result_id": result.result_id,
                },
            )

        self._event(
            resource_id,
            (
                LinkGraphEventType.ANALYSIS_PARTIAL
                if normalized.partial
                else LinkGraphEventType.ANALYSIS_COMPLETED
            ),
            state,
            {
                "result_id": result.result_id,
                "risk_score": risk_score,
                "risk_band": risk_band.value,
                "decision": decision.value,
            },
        )

        return result

    # ------------------------------------------------------------------------
    # ANALYZE MANY
    # ------------------------------------------------------------------------

    def analyze_many(
        self,
        resources: Iterable[LinkGraphDocumentInput],
    ) -> List[LinkGraphResult]:
        results: List[LinkGraphResult] = []

        for index, resource in enumerate(resources):
            if index >= self.policy.max_resources_per_batch:
                break

            results.append(
                self.analyze(resource)
            )

        return results

    # ------------------------------------------------------------------------
    # ACCESSORS
    # ------------------------------------------------------------------------

    def result(
        self,
        result_id: str,
    ) -> Optional[LinkGraphResult]:
        return self.backend.get_result(
            result_id
        )

    def events(self) -> Tuple[LinkGraphEvent, ...]:
        events = getattr(
            self.backend,
            "events",
            (),
        )
        return tuple(events)

    def checkpoints(self) -> Tuple[LinkGraphCheckpoint, ...]:
        checkpoints = getattr(
            self.backend,
            "checkpoints",
            (),
        )
        return tuple(checkpoints)

    # ------------------------------------------------------------------------
    # ARCHITECTURE DESCRIPTION
    # ------------------------------------------------------------------------

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
                "Structured link-spam and graph-abuse evidence extraction"
            ),
            "pipeline": [
                "link evidence",
                "edge evidence",
                "node evidence",
                "anchor analysis",
                "reciprocity analysis",
                "network-pattern analysis",
                "graph-topology analysis",
                "cross-resource analysis",
                "temporal analysis",
                "source-quality analysis",
                "identity analysis",
                "evidence aggregation",
                "confidence calculation",
                "evidence-ready output",
            ],
            "does_not": [
                "final spam classification",
                "index deletion",
                "URL deletion",
                "crawler execution",
                "HTTP requests",
                "worker assignment",
                "ranking",
                "final enforcement",
                "direct quarantine",
                "Google Search dependency",
                "Google index dependency",
                "Google crawler dependency",
                "Google infrastructure dependency",
                "Google ranking technology dependency",
            ],
            "scale_properties": [
                "partition-aware",
                "shard-aware",
                "checkpointable",
                "restartable",
                "partial-observation capable",
                "deterministic reference implementation",
                "distributed-backend replaceable",
                "no fixed global resource ceiling",
            ],
            "signal_families": [
                family.value
                for family in LinkGraphSignalFamily
            ],
        }


# ============================================================================
# COMPATIBILITY ALIASES
# ============================================================================


LinkSpamGraphAbuseDetection = (
    LinkSpamGraphAbuseDetectionArchitecture
)

GlobalLinkSpamGraphAbuseDetection = (
    LinkSpamGraphAbuseDetectionArchitecture
)

Phase14_3LinkSpamGraphAbuseDetection = (
    LinkSpamGraphAbuseDetectionArchitecture
)

LinkGraphAbuseArchitecture = (
    LinkSpamGraphAbuseDetectionArchitecture
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

    "LinkGraphState",
    "LinkGraphSignalFamily",
    "LinkGraphSignalType",
    "LinkGraphEvidenceKind",
    "LinkGraphEvidenceStrength",
    "LinkGraphRiskBand",
    "LinkGraphDecision",
    "LinkGraphEventType",
    "LinkGraphCheckpointType",

    "LinkGraphIdentity",
    "LinkGraphLineage",
    "LinkGraphNodeInput",
    "LinkGraphEdgeInput",
    "LinkGraphAnchorEvidence",
    "LinkGraphReciprocityEvidence",
    "LinkGraphNetworkEvidence",
    "LinkGraphTemporalEvidence",
    "LinkGraphCrossResourceEvidence",
    "LinkGraphDocumentInput",
    "LinkGraphSignal",
    "LinkGraphEvidence",
    "LinkGraphPolicy",
    "LinkGraphResult",
    "LinkGraphCheckpoint",
    "LinkGraphEvent",

    "LinkGraphMetadataBackend",
    "InMemoryLinkGraphMetadataBackend",

    "LinkSpamGraphAbuseDetectionArchitecture",
    "LinkSpamGraphAbuseDetection",
    "GlobalLinkSpamGraphAbuseDetection",
    "Phase14_3LinkSpamGraphAbuseDetection",
    "LinkGraphAbuseArchitecture",
]
