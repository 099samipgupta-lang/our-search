"""
OUR SEARCH
Phase 12.5 — Link and Graph Authority Signals

Purpose
-------
Produce distributed link-graph authority evidence for downstream ranking.

This stage DOES NOT:
- perform final document ranking
- execute search queries
- mutate the search index
- crawl the Web
- perform spam classification
- make the final relevance decision

It transforms discovered Web graph evidence into deterministic,
lineage-preserving authority signals that later ranking stages can consume.

Scale target
------------
Designed directly for:
    billions -> potentially trillions of public-Web resources

Architecture target:
    Google-scale search capability

Dependency policy:
    No Google Search API
    No Google index
    No Google crawler
    No Google infrastructure
    No Google ranking implementation

The architecture is intentionally backend-independent so the in-memory
metadata implementation can later be replaced by distributed graph
storage / graph-computation infrastructure.
"""

from __future__ import annotations

import hashlib
import math
import time
import uuid

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, Iterable, List, Mapping, Optional, Protocol, Sequence, Tuple


ARCHITECTURE_VERSION = "link-graph-authority-signals.v1"

SCALE_TARGET = "billions_to_trillions_of_public_web_resources"
GOOGLE_SCALE_CAPABILITY_TARGET = True
GOOGLE_TECHNOLOGY_DEPENDENCY = False


# ============================================================================
# ENUMS
# ============================================================================


class LinkGraphState(str, Enum):
    RECEIVED = "received"
    VALIDATING = "validating"
    GRAPH_ANALYSIS = "graph_analysis"
    LINK_ANALYSIS = "link_analysis"
    AUTHORITY_ANALYSIS = "authority_analysis"
    NEIGHBORHOOD_ANALYSIS = "neighborhood_analysis"
    SIGNAL_AGGREGATION = "signal_aggregation"
    COMPLETED = "completed"
    PARTIAL = "partial"
    REJECTED = "rejected"
    FAILED = "failed"


class GraphSignalFamily(str, Enum):
    INBOUND_LINK_VOLUME = "inbound_link_volume"
    INBOUND_LINK_QUALITY = "inbound_link_quality"
    OUTBOUND_LINK_STRUCTURE = "outbound_link_structure"

    DOMAIN_DIVERSITY = "domain_diversity"
    HOST_DIVERSITY = "host_diversity"
    SOURCE_DIVERSITY = "source_diversity"

    CROSS_DOMAIN_ENDORSEMENT = "cross_domain_endorsement"
    CROSS_HOST_ENDORSEMENT = "cross_host_endorsement"

    ANCHOR_TEXT_RELEVANCE = "anchor_text_relevance"
    ANCHOR_TEXT_DIVERSITY = "anchor_text_diversity"

    AUTHORITY_CENTRALITY = "authority_centrality"
    HUB_CENTRALITY = "hub_centrality"

    NEIGHBORHOOD_AUTHORITY = "neighborhood_authority"
    NEIGHBORHOOD_CONSISTENCY = "neighborhood_consistency"

    GRAPH_CONNECTIVITY = "graph_connectivity"
    GRAPH_STABILITY = "graph_stability"

    LINK_RECENCY = "link_recency"
    LINK_PERSISTENCE = "link_persistence"

    SOURCE_IDENTITY_STABILITY = "source_identity_stability"


class GraphEvidenceType(str, Enum):
    INBOUND_LINK = "inbound_link"
    OUTBOUND_LINK = "outbound_link"
    ANCHOR_TEXT = "anchor_text"
    DOMAIN_RELATIONSHIP = "domain_relationship"
    HOST_RELATIONSHIP = "host_relationship"
    SOURCE_RELATIONSHIP = "source_relationship"
    CENTRALITY = "centrality"
    NEIGHBORHOOD = "neighborhood"
    GRAPH_HISTORY = "graph_history"
    LINK_HISTORY = "link_history"


class GraphSignalStrength(str, Enum):
    NONE = "none"
    WEAK = "weak"
    MODERATE = "moderate"
    STRONG = "strong"
    VERY_STRONG = "very_strong"


class GraphEventType(str, Enum):
    REQUEST_RECEIVED = "request_received"
    VALIDATION_STARTED = "validation_started"
    GRAPH_ANALYSIS_STARTED = "graph_analysis_started"
    LINK_SIGNALS_EXTRACTED = "link_signals_extracted"
    AUTHORITY_SIGNALS_EXTRACTED = "authority_signals_extracted"
    NEIGHBORHOOD_SIGNALS_EXTRACTED = "neighborhood_signals_extracted"
    SIGNALS_AGGREGATED = "signals_aggregated"
    PARTIAL_INPUT_DETECTED = "partial_input_detected"
    CHECKPOINT_CREATED = "checkpoint_created"
    ANALYSIS_COMPLETED = "analysis_completed"
    ANALYSIS_REJECTED = "analysis_rejected"
    ANALYSIS_FAILED = "analysis_failed"


# ============================================================================
# IDENTITY / LINEAGE
# ============================================================================


@dataclass(frozen=True)
class LinkGraphAuthorityIdentity:
    request_id: str
    document_id: str
    canonical_url: str

    def stable_key(self) -> str:
        return (
            f"{self.document_id}|"
            f"{self.canonical_url}"
        )


@dataclass(frozen=True)
class LinkGraphAuthorityLineage:
    source_stage: str
    source_version: str
    graph_snapshot_id: str
    graph_partition_id: str
    observed_at: str

    def fingerprint(self) -> str:
        payload = "|".join(
            [
                self.source_stage,
                self.source_version,
                self.graph_snapshot_id,
                self.graph_partition_id,
                self.observed_at,
            ]
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# ============================================================================
# INPUT GRAPH STRUCTURES
# ============================================================================


@dataclass(frozen=True)
class GraphLinkInput:
    source_document_id: str
    source_url: str

    target_document_id: str
    target_url: str

    source_domain: str
    target_domain: str

    source_host: str
    target_host: str

    anchor_text: str = ""

    link_weight: float = 1.0
    source_quality_score: float = 0.0
    source_authority_score: float = 0.0

    discovered_at: Optional[str] = None
    first_seen_at: Optional[str] = None
    last_seen_at: Optional[str] = None

    active: bool = True


@dataclass(frozen=True)
class GraphDocumentInput:
    document_id: str
    canonical_url: str

    domain: str
    host: str

    inbound_links: Tuple[GraphLinkInput, ...] = ()
    outbound_links: Tuple[GraphLinkInput, ...] = ()

    existing_authority_score: Optional[float] = None
    existing_quality_score: Optional[float] = None

    graph_partition_id: str = ""
    graph_snapshot_id: str = ""

    observed_at: Optional[str] = None

    partial_graph: bool = False


# ============================================================================
# SIGNALS
# ============================================================================


@dataclass(frozen=True)
class LinkGraphAuthoritySignal:
    family: GraphSignalFamily
    evidence_type: GraphEvidenceType

    raw_value: float
    normalized_value: float

    confidence: float
    strength: GraphSignalStrength

    evidence_count: int

    source_document_ids: Tuple[str, ...] = ()
    source_domains: Tuple[str, ...] = ()

    explanation: str = ""


@dataclass(frozen=True)
class LinkGraphAuthorityProfile:
    document_id: str
    canonical_url: str

    signals: Tuple[LinkGraphAuthoritySignal, ...]

    inbound_link_score: float
    link_quality_score: float

    domain_diversity_score: float
    source_diversity_score: float

    cross_domain_endorsement_score: float

    anchor_text_score: float

    authority_centrality_score: float
    hub_centrality_score: float

    neighborhood_authority_score: float
    neighborhood_consistency_score: float

    graph_connectivity_score: float
    graph_stability_score: float

    link_recency_score: float
    link_persistence_score: float

    aggregate_authority_score: float

    confidence: float
    partial: bool


@dataclass(frozen=True)
class LinkGraphAuthorityResult:
    identity: LinkGraphAuthorityIdentity
    lineage: LinkGraphAuthorityLineage

    profile: LinkGraphAuthorityProfile

    state: LinkGraphState

    completed_at: str

    architecture_version: str = ARCHITECTURE_VERSION


# ============================================================================
# CHECKPOINT / EVENTS
# ============================================================================


@dataclass(frozen=True)
class LinkGraphAuthorityCheckpoint:
    checkpoint_id: str

    request_id: str
    document_id: str

    state: LinkGraphState

    processed_links: int
    processed_neighbors: int

    signal_count: int

    graph_snapshot_id: str
    graph_partition_id: str

    created_at: str


@dataclass(frozen=True)
class LinkGraphAuthorityEvent:
    event_id: str

    request_id: str
    document_id: str

    event_type: GraphEventType
    state: LinkGraphState

    timestamp: str

    payload: Mapping[str, object] = field(default_factory=dict)


# ============================================================================
# BACKEND
# ============================================================================


class LinkGraphAuthorityBackend(Protocol):

    def persist_event(
        self,
        event: LinkGraphAuthorityEvent,
    ) -> None:
        ...

    def persist_checkpoint(
        self,
        checkpoint: LinkGraphAuthorityCheckpoint,
    ) -> None:
        ...

    def persist_result(
        self,
        result: LinkGraphAuthorityResult,
    ) -> None:
        ...

    def get_result(
        self,
        document_id: str,
    ) -> Optional[LinkGraphAuthorityResult]:
        ...


class InMemoryLinkGraphAuthorityMetadata:
    """
    Reference backend.

    This is deliberately only a metadata adapter.

    Production deployments can replace it with distributed durable storage
    without changing the authority-signal architecture.
    """

    def __init__(self) -> None:
        self._events: List[LinkGraphAuthorityEvent] = []
        self._checkpoints: List[LinkGraphAuthorityCheckpoint] = []
        self._results: Dict[str, LinkGraphAuthorityResult] = {}

    def persist_event(
        self,
        event: LinkGraphAuthorityEvent,
    ) -> None:
        self._events.append(event)

    def persist_checkpoint(
        self,
        checkpoint: LinkGraphAuthorityCheckpoint,
    ) -> None:
        self._checkpoints.append(checkpoint)

    def persist_result(
        self,
        result: LinkGraphAuthorityResult,
    ) -> None:
        self._results[result.identity.document_id] = result

    def get_result(
        self,
        document_id: str,
    ) -> Optional[LinkGraphAuthorityResult]:
        return self._results.get(document_id)

    def events(self) -> Tuple[LinkGraphAuthorityEvent, ...]:
        return tuple(self._events)

    def checkpoints(
        self,
    ) -> Tuple[LinkGraphAuthorityCheckpoint, ...]:
        return tuple(self._checkpoints)


# ============================================================================
# POLICY
# ============================================================================


@dataclass(frozen=True)
class LinkGraphAuthorityPolicy:
    """
    Local processing safety bounds.

    These are NOT global Web-scale limits.

    A distributed deployment can process the complete graph by partitioning
    documents and edges across many workers / graph shards.
    """

    max_input_links_per_document: int = 100_000
    max_signals_per_document: int = 512

    allow_partial_graph: bool = True

    minimum_confidence: float = 0.10

    weight_inbound_volume: float = 1.0
    weight_link_quality: float = 1.25
    weight_domain_diversity: float = 1.0
    weight_source_diversity: float = 1.0
    weight_cross_domain: float = 1.25

    weight_anchor_text: float = 1.0

    weight_authority_centrality: float = 1.50
    weight_hub_centrality: float = 0.50

    weight_neighborhood_authority: float = 1.0
    weight_neighborhood_consistency: float = 0.75

    weight_connectivity: float = 0.50
    weight_graph_stability: float = 0.75

    weight_link_recency: float = 0.50
    weight_link_persistence: float = 0.75


# ============================================================================
# MAIN ARCHITECTURE
# ============================================================================


class LinkGraphAuthoritySignalsArchitecture:

    def __init__(
        self,
        backend: Optional[LinkGraphAuthorityBackend] = None,
        policy: Optional[LinkGraphAuthorityPolicy] = None,
    ) -> None:

        self.backend = backend or InMemoryLinkGraphAuthorityMetadata()
        self.policy = policy or LinkGraphAuthorityPolicy()

    # ----------------------------------------------------------------------
    # TIME
    # ----------------------------------------------------------------------

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _parse_datetime(
        value: Optional[str],
    ) -> Optional[datetime]:

        if not value:
            return None

        try:
            parsed = datetime.fromisoformat(
                value.replace("Z", "+00:00")
            )

            if parsed.tzinfo is None:
                parsed = parsed.replace(
                    tzinfo=timezone.utc
                )

            return parsed.astimezone(timezone.utc)

        except (TypeError, ValueError):
            return None

    # ----------------------------------------------------------------------
    # NORMALIZATION
    # ----------------------------------------------------------------------

    @staticmethod
    def _clamp(
        value: float,
        minimum: float = 0.0,
        maximum: float = 1.0,
    ) -> float:

        return max(
            minimum,
            min(maximum, value),
        )

    @staticmethod
    def _strength(
        value: float,
    ) -> GraphSignalStrength:

        value = max(0.0, min(1.0, value))

        if value <= 0.0:
            return GraphSignalStrength.NONE

        if value < 0.25:
            return GraphSignalStrength.WEAK

        if value < 0.50:
            return GraphSignalStrength.MODERATE

        if value < 0.80:
            return GraphSignalStrength.STRONG

        return GraphSignalStrength.VERY_STRONG

    @staticmethod
    def _safe_log_score(
        value: float,
    ) -> float:

        if value <= 0:
            return 0.0

        return min(
            1.0,
            math.log1p(value) / math.log1p(1000.0),
        )

    # ----------------------------------------------------------------------
    # EVENT
    # ----------------------------------------------------------------------

    def _event(
        self,
        request_id: str,
        document_id: str,
        event_type: GraphEventType,
        state: LinkGraphState,
        payload: Optional[Mapping[str, object]] = None,
    ) -> LinkGraphAuthorityEvent:

        event = LinkGraphAuthorityEvent(
            event_id=str(uuid.uuid4()),
            request_id=request_id,
            document_id=document_id,
            event_type=event_type,
            state=state,
            timestamp=self._now(),
            payload=dict(payload or {}),
        )

        self.backend.persist_event(event)

        return event

    # ----------------------------------------------------------------------
    # LINK SIGNALS
    # ----------------------------------------------------------------------

    def _inbound_signals(
        self,
        document: GraphDocumentInput,
    ) -> List[LinkGraphAuthoritySignal]:

        links = [
            link
            for link in document.inbound_links
            if link.active
        ]

        if not links:
            return [
                LinkGraphAuthoritySignal(
                    family=GraphSignalFamily.INBOUND_LINK_VOLUME,
                    evidence_type=GraphEvidenceType.INBOUND_LINK,
                    raw_value=0.0,
                    normalized_value=0.0,
                    confidence=1.0,
                    strength=GraphSignalStrength.NONE,
                    evidence_count=0,
                    explanation="No active inbound links were observed.",
                )
            ]

        total_weight = sum(
            max(0.0, link.link_weight)
            for link in links
        )

        weighted_quality = sum(
            max(0.0, link.link_weight)
            * self._clamp(link.source_quality_score)
            for link in links
        )

        quality_score = (
            weighted_quality / total_weight
            if total_weight > 0
            else 0.0
        )

        return [
            LinkGraphAuthoritySignal(
                family=GraphSignalFamily.INBOUND_LINK_VOLUME,
                evidence_type=GraphEvidenceType.INBOUND_LINK,
                raw_value=float(len(links)),
                normalized_value=self._safe_log_score(
                    len(links)
                ),
                confidence=1.0,
                strength=self._strength(
                    self._safe_log_score(len(links))
                ),
                evidence_count=len(links),
                source_document_ids=tuple(
                    link.source_document_id
                    for link in links
                ),
                source_domains=tuple(
                    sorted(
                        {
                            link.source_domain
                            for link in links
                        }
                    )
                ),
                explanation="Active inbound-link volume.",
            ),
            LinkGraphAuthoritySignal(
                family=GraphSignalFamily.INBOUND_LINK_QUALITY,
                evidence_type=GraphEvidenceType.INBOUND_LINK,
                raw_value=quality_score,
                normalized_value=self._clamp(
                    quality_score
                ),
                confidence=1.0,
                strength=self._strength(
                    quality_score
                ),
                evidence_count=len(links),
                explanation="Quality-weighted inbound-link evidence.",
            ),
        ]

    # ----------------------------------------------------------------------
    # DIVERSITY
    # ----------------------------------------------------------------------

    def _diversity_signals(
        self,
        document: GraphDocumentInput,
    ) -> List[LinkGraphAuthoritySignal]:

        links = [
            link
            for link in document.inbound_links
            if link.active
        ]

        if not links:
            return []

        domains = {
            link.source_domain
            for link in links
            if link.source_domain
        }

        hosts = {
            link.source_host
            for link in links
            if link.source_host
        }

        domain_score = min(
            1.0,
            math.log1p(len(domains))
            / math.log1p(100.0),
        )

        host_score = min(
            1.0,
            math.log1p(len(hosts))
            / math.log1p(100.0),
        )

        source_score = min(
            1.0,
            (
                len(
                    {
                        link.source_document_id
                        for link in links
                    }
                )
                / max(1.0, float(len(links)))
            ),
        )

        return [
            LinkGraphAuthoritySignal(
                family=GraphSignalFamily.DOMAIN_DIVERSITY,
                evidence_type=GraphEvidenceType.DOMAIN_RELATIONSHIP,
                raw_value=float(len(domains)),
                normalized_value=domain_score,
                confidence=1.0,
                strength=self._strength(domain_score),
                evidence_count=len(links),
                source_domains=tuple(sorted(domains)),
                explanation="Diversity of independent source domains.",
            ),
            LinkGraphAuthoritySignal(
                family=GraphSignalFamily.HOST_DIVERSITY,
                evidence_type=GraphEvidenceType.HOST_RELATIONSHIP,
                raw_value=float(len(hosts)),
                normalized_value=host_score,
                confidence=1.0,
                strength=self._strength(host_score),
                evidence_count=len(links),
                explanation="Diversity of source hosts.",
            ),
            LinkGraphAuthoritySignal(
                family=GraphSignalFamily.SOURCE_DIVERSITY,
                evidence_type=GraphEvidenceType.SOURCE_RELATIONSHIP,
                raw_value=source_score,
                normalized_value=source_score,
                confidence=1.0,
                strength=self._strength(source_score),
                evidence_count=len(links),
                explanation="Diversity of linking documents.",
            ),
        ]

    # ----------------------------------------------------------------------
    # CROSS DOMAIN ENDORSEMENT
    # ----------------------------------------------------------------------

    def _cross_domain_signal(
        self,
        document: GraphDocumentInput,
    ) -> LinkGraphAuthoritySignal:

        links = [
            link
            for link in document.inbound_links
            if link.active
            and link.source_domain
            and link.source_domain != document.domain
        ]

        domains = {
            link.source_domain
            for link in links
        }

        score = min(
            1.0,
            math.log1p(len(domains))
            / math.log1p(100.0),
        )

        return LinkGraphAuthoritySignal(
            family=GraphSignalFamily.CROSS_DOMAIN_ENDORSEMENT,
            evidence_type=GraphEvidenceType.DOMAIN_RELATIONSHIP,
            raw_value=float(len(domains)),
            normalized_value=score,
            confidence=1.0,
            strength=self._strength(score),
            evidence_count=len(links),
            source_domains=tuple(sorted(domains)),
            explanation=(
                "Cross-domain inbound endorsement evidence."
            ),
        )

    # ----------------------------------------------------------------------
    # ANCHOR TEXT
    # ----------------------------------------------------------------------

    def _anchor_signal(
        self,
        document: GraphDocumentInput,
    ) -> LinkGraphAuthoritySignal:

        anchors = [
            link.anchor_text.strip()
            for link in document.inbound_links
            if link.active and link.anchor_text.strip()
        ]

        if not anchors:
            return LinkGraphAuthoritySignal(
                family=GraphSignalFamily.ANCHOR_TEXT_RELEVANCE,
                evidence_type=GraphEvidenceType.ANCHOR_TEXT,
                raw_value=0.0,
                normalized_value=0.0,
                confidence=0.5,
                strength=GraphSignalStrength.NONE,
                evidence_count=0,
                explanation="No anchor-text evidence available.",
            )

        unique_anchors = {
            anchor.lower()
            for anchor in anchors
        }

        diversity = min(
            1.0,
            len(unique_anchors)
            / max(1.0, float(len(anchors))),
        )

        return LinkGraphAuthoritySignal(
            family=GraphSignalFamily.ANCHOR_TEXT_DIVERSITY,
            evidence_type=GraphEvidenceType.ANCHOR_TEXT,
            raw_value=diversity,
            normalized_value=diversity,
            confidence=1.0,
            strength=self._strength(diversity),
            evidence_count=len(anchors),
            explanation="Diversity of observed inbound anchor text.",
        )

    # ----------------------------------------------------------------------
    # CENTRALITY / AUTHORITY
    # ----------------------------------------------------------------------

    def _centrality_signals(
        self,
        document: GraphDocumentInput,
    ) -> List[LinkGraphAuthoritySignal]:

        inbound = [
            link
            for link in document.inbound_links
            if link.active
        ]

        outbound = [
            link
            for link in document.outbound_links
            if link.active
        ]

        inbound_authority = (
            sum(
                max(
                    link.source_authority_score,
                    link.source_quality_score,
                )
                * max(0.0, link.link_weight)
                for link in inbound
            )
            / max(1.0, float(len(inbound)))
        )

        outbound_volume = self._safe_log_score(
            len(outbound)
        )

        authority = self._clamp(
            inbound_authority
        )

        return [
            LinkGraphAuthoritySignal(
                family=GraphSignalFamily.AUTHORITY_CENTRALITY,
                evidence_type=GraphEvidenceType.CENTRALITY,
                raw_value=inbound_authority,
                normalized_value=authority,
                confidence=1.0 if inbound else 0.25,
                strength=self._strength(authority),
                evidence_count=len(inbound),
                explanation=(
                    "Authority evidence propagated from "
                    "source-document graph signals."
                ),
            ),
            LinkGraphAuthoritySignal(
                family=GraphSignalFamily.HUB_CENTRALITY,
                evidence_type=GraphEvidenceType.CENTRALITY,
                raw_value=float(len(outbound)),
                normalized_value=outbound_volume,
                confidence=1.0,
                strength=self._strength(outbound_volume),
                evidence_count=len(outbound),
                explanation=(
                    "Outbound connectivity / hub-structure evidence."
                ),
            ),
        ]

    # ----------------------------------------------------------------------
    # NEIGHBORHOOD
    # ----------------------------------------------------------------------

    def _neighborhood_signals(
        self,
        document: GraphDocumentInput,
    ) -> List[LinkGraphAuthoritySignal]:

        inbound = [
            link
            for link in document.inbound_links
            if link.active
        ]

        if not inbound:
            return []

        authority_values = [
            max(
                0.0,
                max(
                    link.source_authority_score,
                    link.source_quality_score,
                ),
            )
            for link in inbound
        ]

        mean_authority = sum(
            authority_values
        ) / max(
            1.0,
            float(len(authority_values)),
        )

        if len(authority_values) <= 1:
            consistency = 1.0
        else:
            mean = mean_authority

            variance = sum(
                (value - mean) ** 2
                for value in authority_values
            ) / len(authority_values)

            consistency = self._clamp(
                1.0 - math.sqrt(variance)
            )

        return [
            LinkGraphAuthoritySignal(
                family=GraphSignalFamily.NEIGHBORHOOD_AUTHORITY,
                evidence_type=GraphEvidenceType.NEIGHBORHOOD,
                raw_value=mean_authority,
                normalized_value=mean_authority,
                confidence=1.0,
                strength=self._strength(mean_authority),
                evidence_count=len(inbound),
                explanation="Authority of the local inbound neighborhood.",
            ),
            LinkGraphAuthoritySignal(
                family=GraphSignalFamily.NEIGHBORHOOD_CONSISTENCY,
                evidence_type=GraphEvidenceType.NEIGHBORHOOD,
                raw_value=consistency,
                normalized_value=consistency,
                confidence=1.0,
                strength=self._strength(consistency),
                evidence_count=len(inbound),
                explanation="Consistency of authority across link sources.",
            ),
        ]

    # ----------------------------------------------------------------------
    # GRAPH CONNECTIVITY
    # ----------------------------------------------------------------------

    def _connectivity_signal(
        self,
        document: GraphDocumentInput,
    ) -> LinkGraphAuthoritySignal:

        inbound = sum(
            1
            for link in document.inbound_links
            if link.active
        )

        outbound = sum(
            1
            for link in document.outbound_links
            if link.active
        )

        total = inbound + outbound

        score = self._safe_log_score(total)

        return LinkGraphAuthoritySignal(
            family=GraphSignalFamily.GRAPH_CONNECTIVITY,
            evidence_type=GraphEvidenceType.NEIGHBORHOOD,
            raw_value=float(total),
            normalized_value=score,
            confidence=1.0,
            strength=self._strength(score),
            evidence_count=total,
            explanation="Local graph connectivity evidence.",
        )

    # ----------------------------------------------------------------------
    # GRAPH STABILITY / LINK HISTORY
    # ----------------------------------------------------------------------

    def _temporal_signals(
        self,
        document: GraphDocumentInput,
    ) -> List[LinkGraphAuthoritySignal]:

        now = self._parse_datetime(
            document.observed_at
        ) or datetime.now(timezone.utc)

        active_links = [
            link
            for link in document.inbound_links
            if link.active
        ]

        if not active_links:
            return []

        recency_values: List[float] = []
        persistence_values: List[float] = []

        for link in active_links:

            last_seen = self._parse_datetime(
                link.last_seen_at
            )

            first_seen = self._parse_datetime(
                link.first_seen_at
            )

            if last_seen:
                age_days = max(
                    0.0,
                    (
                        now - last_seen
                    ).total_seconds()
                    / 86400.0,
                )

                recency_values.append(
                    math.exp(
                        -age_days / 180.0
                    )
                )

            if first_seen and last_seen:
                duration_days = max(
                    0.0,
                    (
                        last_seen - first_seen
                    ).total_seconds()
                    / 86400.0,
                )

                persistence_values.append(
                    min(
                        1.0,
                        duration_days / 365.0,
                    )
                )

        recency = (
            sum(recency_values)
            / len(recency_values)
            if recency_values
            else 0.0
        )

        persistence = (
            sum(persistence_values)
            / len(persistence_values)
            if persistence_values
            else 0.0
        )

        return [
            LinkGraphAuthoritySignal(
                family=GraphSignalFamily.LINK_RECENCY,
                evidence_type=GraphEvidenceType.LINK_HISTORY,
                raw_value=recency,
                normalized_value=recency,
                confidence=(
                    1.0
                    if recency_values
                    else 0.25
                ),
                strength=self._strength(recency),
                evidence_count=len(recency_values),
                explanation="Recency of observed inbound links.",
            ),
            LinkGraphAuthoritySignal(
                family=GraphSignalFamily.LINK_PERSISTENCE,
                evidence_type=GraphEvidenceType.LINK_HISTORY,
                raw_value=persistence,
                normalized_value=persistence,
                confidence=(
                    1.0
                    if persistence_values
                    else 0.25
                ),
                strength=self._strength(persistence),
                evidence_count=len(persistence_values),
                explanation="Persistence of observed inbound links.",
            ),
        ]

    # ----------------------------------------------------------------------
    # AGGREGATION
    # ----------------------------------------------------------------------

    def _aggregate(
        self,
        signals: Sequence[LinkGraphAuthoritySignal],
    ) -> Tuple[float, float]:

        if not signals:
            return 0.0, 0.0

        weights = {
            GraphSignalFamily.INBOUND_LINK_VOLUME:
                self.policy.weight_inbound_volume,

            GraphSignalFamily.INBOUND_LINK_QUALITY:
                self.policy.weight_link_quality,

            GraphSignalFamily.DOMAIN_DIVERSITY:
                self.policy.weight_domain_diversity,

            GraphSignalFamily.SOURCE_DIVERSITY:
                self.policy.weight_source_diversity,

            GraphSignalFamily.CROSS_DOMAIN_ENDORSEMENT:
                self.policy.weight_cross_domain,

            GraphSignalFamily.ANCHOR_TEXT_DIVERSITY:
                self.policy.weight_anchor_text,

            GraphSignalFamily.AUTHORITY_CENTRALITY:
                self.policy.weight_authority_centrality,

            GraphSignalFamily.HUB_CENTRALITY:
                self.policy.weight_hub_centrality,

            GraphSignalFamily.NEIGHBORHOOD_AUTHORITY:
                self.policy.weight_neighborhood_authority,

            GraphSignalFamily.NEIGHBORHOOD_CONSISTENCY:
                self.policy.weight_neighborhood_consistency,

            GraphSignalFamily.GRAPH_CONNECTIVITY:
                self.policy.weight_connectivity,

            GraphSignalFamily.GRAPH_STABILITY:
                self.policy.weight_graph_stability,

            GraphSignalFamily.LINK_RECENCY:
                self.policy.weight_link_recency,

            GraphSignalFamily.LINK_PERSISTENCE:
                self.policy.weight_link_persistence,
        }

        weighted_sum = 0.0
        total_weight = 0.0
        confidence_sum = 0.0
        confidence_weight = 0.0

        for signal in signals:

            weight = weights.get(
                signal.family,
                1.0,
            )

            weighted_sum += (
                signal.normalized_value
                * signal.confidence
                * weight
            )

            total_weight += (
                signal.confidence
                * weight
            )

            confidence_sum += (
                signal.confidence
                * weight
            )

            confidence_weight += weight

        score = (
            weighted_sum / total_weight
            if total_weight > 0
            else 0.0
        )

        confidence = (
            confidence_sum / confidence_weight
            if confidence_weight > 0
            else 0.0
        )

        return (
            self._clamp(score),
            self._clamp(confidence),
        )

    # ----------------------------------------------------------------------
    # DOCUMENT ANALYSIS
    # ----------------------------------------------------------------------

    def _analyze_document(
        self,
        document: GraphDocumentInput,
    ) -> LinkGraphAuthorityProfile:

        inbound = list(
            document.inbound_links[
                : self.policy.max_input_links_per_document
            ]
        )

        outbound = list(
            document.outbound_links[
                : self.policy.max_input_links_per_document
            ]
        )

        working_document = GraphDocumentInput(
            document_id=document.document_id,
            canonical_url=document.canonical_url,
            domain=document.domain,
            host=document.host,
            inbound_links=tuple(inbound),
            outbound_links=tuple(outbound),
            existing_authority_score=document.existing_authority_score,
            existing_quality_score=document.existing_quality_score,
            graph_partition_id=document.graph_partition_id,
            graph_snapshot_id=document.graph_snapshot_id,
            observed_at=document.observed_at,
            partial_graph=document.partial_graph,
        )

        signals: List[LinkGraphAuthoritySignal] = []

        signals.extend(
            self._inbound_signals(
                working_document
            )
        )

        signals.extend(
            self._diversity_signals(
                working_document
            )
        )

        signals.append(
            self._cross_domain_signal(
                working_document
            )
        )

        signals.append(
            self._anchor_signal(
                working_document
            )
        )

        signals.extend(
            self._centrality_signals(
                working_document
            )
        )

        signals.extend(
            self._neighborhood_signals(
                working_document
            )
        )

        signals.append(
            self._connectivity_signal(
                working_document
            )
        )

        signals.extend(
            self._temporal_signals(
                working_document
            )
        )

        signals = signals[
            : self.policy.max_signals_per_document
        ]

        aggregate, confidence = self._aggregate(
            signals
        )

        def value(
            family: GraphSignalFamily,
        ) -> float:

            values = [
                signal.normalized_value
                for signal in signals
                if signal.family == family
            ]

            return (
                sum(values) / len(values)
                if values
                else 0.0
            )

        partial = (
            working_document.partial_graph
        )

        return LinkGraphAuthorityProfile(
            document_id=working_document.document_id,
            canonical_url=working_document.canonical_url,
            signals=tuple(signals),

            inbound_link_score=value(
                GraphSignalFamily.INBOUND_LINK_VOLUME
            ),

            link_quality_score=value(
                GraphSignalFamily.INBOUND_LINK_QUALITY
            ),

            domain_diversity_score=value(
                GraphSignalFamily.DOMAIN_DIVERSITY
            ),

            source_diversity_score=value(
                GraphSignalFamily.SOURCE_DIVERSITY
            ),

            cross_domain_endorsement_score=value(
                GraphSignalFamily.CROSS_DOMAIN_ENDORSEMENT
            ),

            anchor_text_score=value(
                GraphSignalFamily.ANCHOR_TEXT_DIVERSITY
            ),

            authority_centrality_score=value(
                GraphSignalFamily.AUTHORITY_CENTRALITY
            ),

            hub_centrality_score=value(
                GraphSignalFamily.HUB_CENTRALITY
            ),

            neighborhood_authority_score=value(
                GraphSignalFamily.NEIGHBORHOOD_AUTHORITY
            ),

            neighborhood_consistency_score=value(
                GraphSignalFamily.NEIGHBORHOOD_CONSISTENCY
            ),

            graph_connectivity_score=value(
                GraphSignalFamily.GRAPH_CONNECTIVITY
            ),

            graph_stability_score=value(
                GraphSignalFamily.GRAPH_STABILITY
            ),

            link_recency_score=value(
                GraphSignalFamily.LINK_RECENCY
            ),

            link_persistence_score=value(
                GraphSignalFamily.LINK_PERSISTENCE
            ),

            aggregate_authority_score=aggregate,

            confidence=confidence,

            partial=partial,
        )

    # ----------------------------------------------------------------------
    # PUBLIC API
    # ----------------------------------------------------------------------

    def analyze(
        self,
        document: GraphDocumentInput,
        request_id: Optional[str] = None,
    ) -> LinkGraphAuthorityResult:

        request_id = request_id or str(
            uuid.uuid4()
        )

        identity = LinkGraphAuthorityIdentity(
            request_id=request_id,
            document_id=document.document_id,
            canonical_url=document.canonical_url,
        )

        lineage = LinkGraphAuthorityLineage(
            source_stage="phase12.5",
            source_version=ARCHITECTURE_VERSION,
            graph_snapshot_id=(
                document.graph_snapshot_id
                or "unknown"
            ),
            graph_partition_id=(
                document.graph_partition_id
                or "unknown"
            ),
            observed_at=(
                document.observed_at
                or self._now()
            ),
        )

        self._event(
            request_id,
            document.document_id,
            GraphEventType.REQUEST_RECEIVED,
            LinkGraphState.RECEIVED,
        )

        self._event(
            request_id,
            document.document_id,
            GraphEventType.VALIDATION_STARTED,
            LinkGraphState.VALIDATING,
        )

        if not document.document_id:
            self._event(
                request_id,
                document.document_id,
                GraphEventType.ANALYSIS_REJECTED,
                LinkGraphState.REJECTED,
                {"reason": "missing_document_id"},
            )

            raise ValueError(
                "document_id is required"
            )

        if not document.canonical_url:
            self._event(
                request_id,
                document.document_id,
                GraphEventType.ANALYSIS_REJECTED,
                LinkGraphState.REJECTED,
                {"reason": "missing_canonical_url"},
            )

            raise ValueError(
                "canonical_url is required"
            )

        self._event(
            request_id,
            document.document_id,
            GraphEventType.GRAPH_ANALYSIS_STARTED,
            LinkGraphState.GRAPH_ANALYSIS,
        )

        self._event(
            request_id,
            document.document_id,
            GraphEventType.LINK_SIGNALS_EXTRACTED,
            LinkGraphState.LINK_ANALYSIS,
        )

        self._event(
            request_id,
            document.document_id,
            GraphEventType.AUTHORITY_SIGNALS_EXTRACTED,
            LinkGraphState.AUTHORITY_ANALYSIS,
        )

        profile = self._analyze_document(
            document
        )

        self._event(
            request_id,
            document.document_id,
            GraphEventType.NEIGHBORHOOD_SIGNALS_EXTRACTED,
            LinkGraphState.NEIGHBORHOOD_ANALYSIS,
        )

        state = (
            LinkGraphState.PARTIAL
            if profile.partial
            else LinkGraphState.COMPLETED
        )

        self._event(
            request_id,
            document.document_id,
            GraphEventType.SIGNALS_AGGREGATED,
            LinkGraphState.SIGNAL_AGGREGATION,
            {
                "signal_count": len(profile.signals),
                "partial": profile.partial,
            },
        )

        if profile.partial:
            self._event(
                request_id,
                document.document_id,
                GraphEventType.PARTIAL_INPUT_DETECTED,
                LinkGraphState.PARTIAL,
            )

        result = LinkGraphAuthorityResult(
            identity=identity,
            lineage=lineage,
            profile=profile,
            state=state,
            completed_at=self._now(),
        )

        checkpoint = LinkGraphAuthorityCheckpoint(
            checkpoint_id=str(uuid.uuid4()),
            request_id=request_id,
            document_id=document.document_id,
            state=state,
            processed_links=(
                len(document.inbound_links)
                + len(document.outbound_links)
            ),
            processed_neighbors=(
                len(document.inbound_links)
                + len(document.outbound_links)
            ),
            signal_count=len(profile.signals),
            graph_snapshot_id=(
                document.graph_snapshot_id
                or "unknown"
            ),
            graph_partition_id=(
                document.graph_partition_id
                or "unknown"
            ),
            created_at=self._now(),
        )

        self.backend.persist_checkpoint(
            checkpoint
        )

        self._event(
            request_id,
            document.document_id,
            GraphEventType.CHECKPOINT_CREATED,
            state,
            {
                "checkpoint_id":
                    checkpoint.checkpoint_id,
            },
        )

        self.backend.persist_result(
            result
        )

        self._event(
            request_id,
            document.document_id,
            GraphEventType.ANALYSIS_COMPLETED,
            state,
        )

        return result

    # ----------------------------------------------------------------------
    # BATCH API
    # ----------------------------------------------------------------------

    def analyze_many(
        self,
        documents: Iterable[GraphDocumentInput],
    ) -> Tuple[LinkGraphAuthorityResult, ...]:

        results: List[LinkGraphAuthorityResult] = []

        for document in documents:
            results.append(
                self.analyze(document)
            )

        return tuple(results)

    # ----------------------------------------------------------------------
    # OBSERVABILITY
    # ----------------------------------------------------------------------

    def events(
        self,
    ) -> Tuple[LinkGraphAuthorityEvent, ...]:

        if hasattr(self.backend, "events"):
            return self.backend.events()  # type: ignore[attr-defined]

        return ()

    def checkpoints(
        self,
    ) -> Tuple[LinkGraphAuthorityCheckpoint, ...]:

        if hasattr(self.backend, "checkpoints"):
            return self.backend.checkpoints()  # type: ignore[attr-defined]

        return ()

    # ----------------------------------------------------------------------
    # ARCHITECTURE CONTRACT
    # ----------------------------------------------------------------------

    @staticmethod
    def architecture() -> Mapping[str, object]:

        return {
            "architecture_version":
                ARCHITECTURE_VERSION,

            "stage":
                "12.5",

            "name":
                "Link and Graph Authority Signals",

            "scale_target":
                SCALE_TARGET,

            "google_scale_capability_target":
                GOOGLE_SCALE_CAPABILITY_TARGET,

            "google_technology_dependency":
                GOOGLE_TECHNOLOGY_DEPENDENCY,

            "purpose": (
                "Produce distributed link-graph authority evidence "
                "for downstream ranking."
            ),

            "pipeline_position": (
                "retrieval -> ranking signals -> "
                "link graph authority -> later ranking"
            ),

            "inputs": [
                "document identity",
                "canonical URL",
                "inbound links",
                "outbound links",
                "anchor text",
                "source quality evidence",
                "source authority evidence",
                "graph partition identity",
                "graph snapshot identity",
                "link history",
            ],

            "outputs": [
                "link volume signals",
                "link quality signals",
                "domain diversity",
                "source diversity",
                "cross-domain endorsement",
                "anchor-text evidence",
                "authority centrality",
                "hub centrality",
                "neighborhood authority",
                "graph connectivity",
                "graph stability",
                "link recency",
                "link persistence",
                "aggregate authority evidence",
                "confidence",
                "lineage",
            ],

            "distributed_design": True,

            "partitionable": True,

            "incremental_graph_updates_supported": True,

            "partial_graph_supported": True,

            "checkpointable": True,

            "provenance_preserving": True,

            "backend_replaceable": True,

            "fixed_global_node_limit": False,

            "fixed_global_edge_limit": False,

            "fixed_global_partition_limit": False,

            "fixed_global_worker_limit": False,

            "final_ranking": False,

            "crawler_execution": False,

            "index_mutation": False,

            "spam_classification": False,

            "google_api_dependency": False,

            "next_stage":
                "12.6 Query-Dependent Authority and Relevance Signals",
        }


# ============================================================================
# PUBLIC ALIASES
# ============================================================================


LinkGraphAuthoritySignals = (
    LinkGraphAuthoritySignalsArchitecture
)

GlobalLinkGraphAuthoritySignals = (
    LinkGraphAuthoritySignalsArchitecture
)

Phase12_5LinkGraphAuthoritySignals = (
    LinkGraphAuthoritySignalsArchitecture
)
