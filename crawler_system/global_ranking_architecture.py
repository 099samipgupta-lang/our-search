from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from hashlib import sha256
from typing import Any, Dict, List, Mapping, Optional, Protocol, Sequence, Tuple
from uuid import uuid4


# ============================================================
# OUR SEARCH
# Phase 12.1 — Global Ranking Architecture
# ============================================================

ARCHITECTURE_VERSION = "global-ranking-architecture.v1"
SCALE_TARGET = "billions_to_trillions_of_public_web_resources"
GOOGLE_SCALE_CAPABILITY_TARGET = True
GOOGLE_TECHNOLOGY_DEPENDENCY = False


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex}"


def _hash(value: Any) -> str:
    return sha256(
        repr(value).encode("utf-8", errors="replace")
    ).hexdigest()


# ============================================================
# STATES
# ============================================================

class RankingState(str, Enum):
    RECEIVED = "received"
    VALIDATING = "validating"
    SIGNAL_PREPARATION = "signal_preparation"
    FEATURE_PREPARATION = "feature_preparation"
    RANKING_PLANNING = "ranking_planning"
    RANKING_EXECUTION = "ranking_execution"
    ORDERING = "ordering"
    COMPLETED = "completed"
    PARTIAL = "partial"
    REJECTED = "rejected"
    FAILED = "failed"


class RankingScope(str, Enum):
    DOCUMENT = "document"
    CANDIDATE_SET = "candidate_set"
    PARTITION = "partition"
    REGION = "region"
    GLOBAL = "global"


class RankingExecutionMode(str, Enum):
    LOCAL = "local"
    DISTRIBUTED = "distributed"
    HYBRID = "hybrid"


class RankingSignalFamily(str, Enum):
    RELEVANCE = "relevance"
    QUALITY = "quality"
    AUTHORITY = "authority"
    FRESHNESS = "freshness"
    TRUST = "trust"
    DUPLICATION = "duplication"
    QUERY_MATCH = "query_match"
    DOCUMENT_MATCH = "document_match"
    CONTEXT = "context"


class RankingArtifactState(str, Enum):
    RECEIVED = "received"
    VALID = "valid"
    PARTIAL = "partial"
    COMPLETE = "complete"
    INVALID = "invalid"


class RankingEventType(str, Enum):
    REQUEST_RECEIVED = "request_received"
    VALIDATION_STARTED = "validation_started"
    RANKING_STARTED = "ranking_started"
    SIGNALS_PREPARED = "signals_prepared"
    FEATURES_PREPARED = "features_prepared"
    PLAN_CREATED = "plan_created"
    EXECUTION_STARTED = "execution_started"
    EXECUTION_COMPLETED = "execution_completed"
    PARTIAL_INPUT_DETECTED = "partial_input_detected"
    ORDERING_STARTED = "ordering_started"
    ORDERING_COMPLETED = "ordering_completed"
    CHECKPOINT_CREATED = "checkpoint_created"
    RANKING_COMPLETED = "ranking_completed"
    RANKING_REJECTED = "ranking_rejected"
    RANKING_FAILED = "ranking_failed"


# ============================================================
# IDENTITIES
# ============================================================

@dataclass(frozen=True)
class RankingIdentity:
    ranking_id: str
    retrieval_id: str
    query_id: str
    candidate_set_id: str
    architecture_version: str = ARCHITECTURE_VERSION
    created_at: datetime = field(default_factory=_now)


@dataclass(frozen=True)
class RankingLineage:
    ranking_id: str
    retrieval_id: str
    query_id: str
    canonical_query_hash: str
    retrieval_candidate_hash: str
    ranking_input_hash: str


# ============================================================
# SIGNALS
# ============================================================

@dataclass(frozen=True)
class RankingSignal:
    signal_id: str
    family: RankingSignalFamily
    name: str
    value: float
    confidence: float = 1.0
    source: Optional[str] = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass
class RankingSignalProfile:
    resource_id: str
    signals: Tuple[RankingSignal, ...]
    signal_hash: str = ""

    def __post_init__(self) -> None:
        if not self.signal_hash:
            self.signal_hash = _hash(
                tuple(
                    (
                        signal.family.value,
                        signal.name,
                        signal.value,
                        signal.confidence,
                    )
                    for signal in self.signals
                )
            )


# ============================================================
# RANKING FEATURES
# ============================================================

@dataclass(frozen=True)
class RankingFeature:
    feature_id: str
    name: str
    value: float
    family: RankingSignalFamily
    source_signal_ids: Tuple[str, ...] = ()
    confidence: float = 1.0


@dataclass
class RankingFeatureVector:
    resource_id: str
    features: Tuple[RankingFeature, ...]
    feature_hash: str = ""

    def __post_init__(self) -> None:
        if not self.feature_hash:
            self.feature_hash = _hash(
                tuple(
                    (
                        feature.name,
                        feature.value,
                        feature.family.value,
                    )
                    for feature in self.features
                )
            )


# ============================================================
# RANKING CANDIDATE
# ============================================================

@dataclass(frozen=True)
class RankingCandidate:
    resource_id: str

    document_id: Optional[str] = None
    canonical_url: Optional[str] = None
    content_identity: Optional[str] = None

    retrieval_score: float = 0.0

    signal_profile: Optional[RankingSignalProfile] = None
    feature_vector: Optional[RankingFeatureVector] = None

    partition_id: Optional[str] = None
    region_id: Optional[str] = None

    provenance: Mapping[str, Any] = field(
        default_factory=dict
    )


# ============================================================
# RANKING PLAN
# ============================================================

@dataclass(frozen=True)
class RankingShard:
    shard_id: str
    scope: RankingScope

    partition_id: Optional[str] = None
    region_id: Optional[str] = None

    candidate_ids: Tuple[str, ...] = ()

    execution_mode: RankingExecutionMode = (
        RankingExecutionMode.DISTRIBUTED
    )


@dataclass(frozen=True)
class RankingPlan:
    plan_id: str
    ranking_id: str

    shards: Tuple[RankingShard, ...]

    execution_mode: RankingExecutionMode

    parallelism_hint: int

    partial_execution_allowed: bool

    plan_hash: str

    created_at: datetime = field(default_factory=_now)


# ============================================================
# RANKED OUTPUT
# ============================================================

@dataclass(frozen=True)
class RankedCandidate:
    candidate: RankingCandidate

    ranking_score: float

    rank_features: Mapping[str, float] = field(
        default_factory=dict
    )

    ranking_stage: str = "global-ranking"

    score_version: str = ARCHITECTURE_VERSION


@dataclass
class RankedCandidateSet:
    ranked_set_id: str
    ranking_id: str

    candidates: Tuple[RankedCandidate, ...]

    partial: bool = False

    ranked_count: int = 0

    ranking_hash: str = ""

    created_at: datetime = field(default_factory=_now)

    def __post_init__(self) -> None:

        if self.ranked_count == 0:
            self.ranked_count = len(self.candidates)

        if not self.ranking_hash:
            self.ranking_hash = _hash(
                tuple(
                    (
                        item.candidate.resource_id,
                        item.ranking_score,
                    )
                    for item in self.candidates
                )
            )


# ============================================================
# CHECKPOINT
# ============================================================

@dataclass(frozen=True)
class RankingCheckpoint:
    checkpoint_id: str
    ranking_id: str

    state: RankingState

    completed_shards: Tuple[str, ...]
    partial_shards: Tuple[str, ...]

    ranked_set_id: Optional[str]

    checkpoint_hash: str

    created_at: datetime = field(default_factory=_now)


# ============================================================
# EVENTS
# ============================================================

@dataclass(frozen=True)
class RankingEvent:
    event_id: str
    ranking_id: str

    event_type: RankingEventType

    message: str

    metadata: Mapping[str, Any] = field(
        default_factory=dict
    )

    created_at: datetime = field(default_factory=_now)


# ============================================================
# BACKEND
# ============================================================

class GlobalRankingBackend(Protocol):

    def persist_event(
        self,
        event: RankingEvent,
    ) -> None:
        ...

    def persist_checkpoint(
        self,
        checkpoint: RankingCheckpoint,
    ) -> None:
        ...

    def persist_plan(
        self,
        plan: RankingPlan,
    ) -> None:
        ...

    def persist_ranked_set(
        self,
        ranked_set: RankedCandidateSet,
    ) -> None:
        ...


class InMemoryGlobalRankingMetadata:

    def __init__(self) -> None:
        self._events: List[RankingEvent] = []
        self._checkpoints: List[RankingCheckpoint] = []
        self._plans: List[RankingPlan] = []
        self._ranked_sets: List[RankedCandidateSet] = []

    def persist_event(
        self,
        event: RankingEvent,
    ) -> None:
        self._events.append(event)

    def persist_checkpoint(
        self,
        checkpoint: RankingCheckpoint,
    ) -> None:
        self._checkpoints.append(checkpoint)

    def persist_plan(
        self,
        plan: RankingPlan,
    ) -> None:
        self._plans.append(plan)

    def persist_ranked_set(
        self,
        ranked_set: RankedCandidateSet,
    ) -> None:
        self._ranked_sets.append(ranked_set)

    def events(self) -> Tuple[RankingEvent, ...]:
        return tuple(self._events)

    def checkpoints(
        self,
    ) -> Tuple[RankingCheckpoint, ...]:
        return tuple(self._checkpoints)

    def plans(self) -> Tuple[RankingPlan, ...]:
        return tuple(self._plans)

    def ranked_sets(
        self,
    ) -> Tuple[RankedCandidateSet, ...]:
        return tuple(self._ranked_sets)


# ============================================================
# POLICY
# ============================================================

@dataclass(frozen=True)
class GlobalRankingPolicy:

    # Per-ranking execution controls.
    # These are not global Web limits.
    max_candidates_per_shard: int = 100000
    max_shards_per_ranking: int = 1024
    max_parallel_shards: int = 64

    allow_partial_execution: bool = True

    require_retrieval_score: bool = True
    require_candidate_identity: bool = True

    create_checkpoint_on_completion: bool = True

    execution_mode: RankingExecutionMode = (
        RankingExecutionMode.DISTRIBUTED
    )

    # Phase 12 later stages will provide richer signals.
    initial_retrieval_score_weight: float = 1.0


# ============================================================
# GLOBAL RANKING ARCHITECTURE
# ============================================================

class GlobalRankingArchitecture:

    """
    OUR SEARCH Phase 12.1.

    Establishes the global ranking control architecture.

    This layer defines how retrieval candidates become ranking
    inputs and how ranking work is distributed.

    It intentionally does NOT attempt to implement every ranking
    signal yet. Later Phase 12 stages add relevance, authority,
    quality, freshness, trust, feature construction, multi-stage
    ranking and evaluation.

    It also does NOT:
        - crawl the Web
        - mutate the index
        - perform search retrieval
        - replace Phase 11
        - implement final production ranking models
        - depend on Google technology
    """

    def __init__(
        self,
        backend: Optional[GlobalRankingBackend] = None,
        policy: Optional[GlobalRankingPolicy] = None,
    ) -> None:

        self.backend = (
            backend
            if backend is not None
            else InMemoryGlobalRankingMetadata()
        )

        self.policy = (
            policy
            if policy is not None
            else GlobalRankingPolicy()
        )

        self._events: List[RankingEvent] = []
        self._checkpoints: List[RankingCheckpoint] = []

    # --------------------------------------------------------
    # Events
    # --------------------------------------------------------

    def _emit(
        self,
        ranking_id: str,
        event_type: RankingEventType,
        message: str,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> RankingEvent:

        event = RankingEvent(
            event_id=_new_id("ranking_event"),
            ranking_id=ranking_id,
            event_type=event_type,
            message=message,
            metadata=dict(metadata or {}),
        )

        self._events.append(event)
        self.backend.persist_event(event)

        return event

    # --------------------------------------------------------
    # Candidate normalization
    # --------------------------------------------------------

    @staticmethod
    def _candidate_from_mapping(
        value: Any,
    ) -> RankingCandidate:

        if isinstance(value, RankingCandidate):
            return value

        if not isinstance(value, Mapping):
            raise TypeError(
                "ranking candidate must be a mapping"
            )

        resource_id = value.get(
            "resource_id",
            value.get(
                "document_id",
                value.get(
                    "canonical_url",
                    "",
                ),
            ),
        )

        if not resource_id:
            raise ValueError(
                "ranking candidate missing resource identity"
            )

        return RankingCandidate(
            resource_id=str(resource_id),

            document_id=(
                str(value["document_id"])
                if value.get("document_id")
                else None
            ),

            canonical_url=(
                str(value["canonical_url"])
                if value.get("canonical_url")
                else None
            ),

            content_identity=(
                str(value["content_identity"])
                if value.get("content_identity")
                else None
            ),

            retrieval_score=float(
                value.get(
                    "retrieval_score",
                    value.get(
                        "score",
                        0.0,
                    ),
                )
            ),

            partition_id=(
                str(value["partition_id"])
                if value.get("partition_id")
                else None
            ),

            region_id=(
                str(value["region_id"])
                if value.get("region_id")
                else None
            ),

            provenance=dict(
                value.get(
                    "provenance",
                    {},
                )
            ),
        )

    # --------------------------------------------------------
    # Plan construction
    # --------------------------------------------------------

    def _build_plan(
        self,
        ranking_id: str,
        candidates: Sequence[RankingCandidate],
    ) -> RankingPlan:

        shards: List[RankingShard] = []

        grouped: Dict[
            Tuple[Optional[str], Optional[str]],
            List[RankingCandidate],
        ] = {}

        for candidate in candidates:

            key = (
                candidate.region_id,
                candidate.partition_id,
            )

            grouped.setdefault(
                key,
                [],
            ).append(candidate)

        if not grouped:
            grouped[(
                None,
                None,
            )] = []

        for (
            region_id,
            partition_id,
        ), group in grouped.items():

            for offset in range(
                0,
                len(group),
                self.policy.max_candidates_per_shard,
            ):

                chunk = group[
                    offset:
                    offset
                    + self.policy.max_candidates_per_shard
                ]

                shard = RankingShard(
                    shard_id=_new_id(
                        "ranking_shard"
                    ),

                    scope=(
                        RankingScope.PARTITION
                        if partition_id
                        else RankingScope.REGION
                        if region_id
                        else RankingScope.CANDIDATE_SET
                    ),

                    partition_id=partition_id,
                    region_id=region_id,

                    candidate_ids=tuple(
                        candidate.resource_id
                        for candidate in chunk
                    ),

                    execution_mode=(
                        self.policy.execution_mode
                    ),
                )

                shards.append(shard)

        partial = False

        if len(shards) > self.policy.max_shards_per_ranking:

            shards = shards[
                :self.policy.max_shards_per_ranking
            ]

            partial = True

        plan_hash = _hash(
            tuple(
                (
                    shard.shard_id,
                    shard.scope.value,
                    shard.partition_id,
                    shard.region_id,
                    shard.candidate_ids,
                )
                for shard in shards
            )
        )

        plan = RankingPlan(
            plan_id=_new_id("ranking_plan"),
            ranking_id=ranking_id,

            shards=tuple(shards),

            execution_mode=(
                self.policy.execution_mode
            ),

            parallelism_hint=min(
                self.policy.max_parallel_shards,
                max(1, len(shards)),
            ),

            partial_execution_allowed=(
                self.policy.allow_partial_execution
                or partial
            ),

            plan_hash=plan_hash,
        )

        self.backend.persist_plan(plan)

        return plan

    # --------------------------------------------------------
    # Initial feature construction
    # --------------------------------------------------------

    def _build_initial_features(
        self,
        candidate: RankingCandidate,
    ) -> RankingFeatureVector:

        features = (
            RankingFeature(
                feature_id=_new_id("ranking_feature"),

                name="retrieval_score",

                value=max(
                    0.0,
                    float(
                        candidate.retrieval_score
                    ),
                ),

                family=(
                    RankingSignalFamily.RELEVANCE
                ),

                confidence=1.0,
            ),
        )

        return RankingFeatureVector(
            resource_id=candidate.resource_id,
            features=features,
        )

    # --------------------------------------------------------
    # Initial ranking
    # --------------------------------------------------------

    def _rank_candidate(
        self,
        candidate: RankingCandidate,
    ) -> RankedCandidate:

        feature_vector = (
            candidate.feature_vector
            if candidate.feature_vector
            else self._build_initial_features(
                candidate
            )
        )

        score = 0.0
        feature_map: Dict[str, float] = {}

        for feature in feature_vector.features:

            weighted = (
                feature.value
                * feature.confidence
            )

            if feature.name == "retrieval_score":

                weighted *= (
                    self.policy
                    .initial_retrieval_score_weight
                )

            score += weighted

            feature_map[
                feature.name
            ] = weighted

        return RankedCandidate(
            candidate=RankingCandidate(
                resource_id=candidate.resource_id,
                document_id=candidate.document_id,
                canonical_url=candidate.canonical_url,
                content_identity=candidate.content_identity,
                retrieval_score=candidate.retrieval_score,
                signal_profile=candidate.signal_profile,
                feature_vector=feature_vector,
                partition_id=candidate.partition_id,
                region_id=candidate.region_id,
                provenance=candidate.provenance,
            ),

            ranking_score=score,

            rank_features=feature_map,
        )

    # --------------------------------------------------------
    # Main architecture entry point
    # --------------------------------------------------------

    def rank(
        self,
        *,
        retrieval_id: str,
        query_id: str,
        candidate_set_id: str,
        canonical_query_hash: str,
        retrieval_candidate_hash: str,
        candidates: Sequence[Any],
    ) -> Dict[str, Any]:

        ranking_id = _new_id("ranking")

        identity = RankingIdentity(
            ranking_id=ranking_id,
            retrieval_id=retrieval_id,
            query_id=query_id,
            candidate_set_id=candidate_set_id,
        )

        self._emit(
            ranking_id,
            RankingEventType.REQUEST_RECEIVED,
            "Global ranking request received",
        )

        self._emit(
            ranking_id,
            RankingEventType.VALIDATION_STARTED,
            "Global ranking validation started",
        )

        normalized: List[
            RankingCandidate
        ] = []

        for raw_candidate in candidates:

            try:

                candidate = (
                    self._candidate_from_mapping(
                        raw_candidate
                    )
                )

            except (
                TypeError,
                ValueError,
            ):

                continue

            if (
                self.policy.require_candidate_identity
                and not candidate.resource_id
            ):
                continue

            if (
                self.policy.require_retrieval_score
                and candidate.retrieval_score < 0
            ):
                continue

            normalized.append(candidate)

        if not normalized:

            self._emit(
                ranking_id,
                RankingEventType.RANKING_REJECTED,
                "No valid ranking candidates available",
            )

            return {
                "ranking_id": ranking_id,
                "state": RankingState.REJECTED.value,
                "identity": identity,
                "lineage": None,
                "plan": None,
                "ranked_set": None,
                "checkpoint": None,
            }

        lineage = RankingLineage(
            ranking_id=ranking_id,
            retrieval_id=retrieval_id,
            query_id=query_id,
            canonical_query_hash=(
                canonical_query_hash
            ),
            retrieval_candidate_hash=(
                retrieval_candidate_hash
            ),
            ranking_input_hash=_hash(
                tuple(
                    candidate.resource_id
                    for candidate in normalized
                )
            ),
        )

        self._emit(
            ranking_id,
            RankingEventType.RANKING_STARTED,
            "Global ranking architecture started",
        )

        plan = self._build_plan(
            ranking_id,
            normalized,
        )

        self._emit(
            ranking_id,
            RankingEventType.PLAN_CREATED,
            "Distributed ranking plan created",
            metadata={
                "plan_id": plan.plan_id,
                "shard_count": len(plan.shards),
                "parallelism": (
                    plan.parallelism_hint
                ),
            },
        )

        self._emit(
            ranking_id,
            RankingEventType.EXECUTION_STARTED,
            "Initial distributed ranking execution started",
        )

        ranked: List[
            RankedCandidate
        ] = []

        for candidate in normalized:

            ranked.append(
                self._rank_candidate(
                    candidate
                )
            )

        partial = (
            len(plan.shards)
            > self.policy.max_shards_per_ranking
        )

        self._emit(
            ranking_id,
            RankingEventType.EXECUTION_COMPLETED,
            "Initial ranking execution completed",
            metadata={
                "candidate_count": len(ranked),
                "partial": partial,
            },
        )

        self._emit(
            ranking_id,
            RankingEventType.ORDERING_STARTED,
            "Global ordering started",
        )

        ranked.sort(
            key=lambda item: (
                -item.ranking_score,
                item.candidate.resource_id,
            )
        )

        ranked_set = RankedCandidateSet(
            ranked_set_id=_new_id(
                "ranked_candidate_set"
            ),

            ranking_id=ranking_id,

            candidates=tuple(ranked),

            partial=partial,
        )

        self.backend.persist_ranked_set(
            ranked_set
        )

        self._emit(
            ranking_id,
            RankingEventType.ORDERING_COMPLETED,
            "Global ordering completed",
            metadata={
                "ranked_set_id": (
                    ranked_set.ranked_set_id
                ),
                "ranked_count": (
                    ranked_set.ranked_count
                ),
            },
        )

        checkpoint = None

        if self.policy.create_checkpoint_on_completion:

            checkpoint = RankingCheckpoint(
                checkpoint_id=_new_id(
                    "ranking_checkpoint"
                ),

                ranking_id=ranking_id,

                state=(
                    RankingState.PARTIAL
                    if partial
                    else RankingState.COMPLETED
                ),

                completed_shards=tuple(
                    shard.shard_id
                    for shard in plan.shards
                ),

                partial_shards=(),

                ranked_set_id=(
                    ranked_set.ranked_set_id
                ),

                checkpoint_hash=_hash(
                    (
                        ranking_id,
                        plan.plan_hash,
                        ranked_set.ranking_hash,
                    )
                ),
            )

            self._checkpoints.append(
                checkpoint
            )

            self.backend.persist_checkpoint(
                checkpoint
            )

            self._emit(
                ranking_id,
                RankingEventType.CHECKPOINT_CREATED,
                "Ranking checkpoint created",
                metadata={
                    "checkpoint_id": (
                        checkpoint.checkpoint_id
                    ),
                },
            )

        final_state = (
            RankingState.PARTIAL
            if partial
            else RankingState.COMPLETED
        )

        self._emit(
            ranking_id,
            RankingEventType.RANKING_COMPLETED,
            "Global ranking architecture completed",
            metadata={
                "state": final_state.value,
                "ranked_count": (
                    ranked_set.ranked_count
                ),
            },
        )

        return {
            "ranking_id": ranking_id,
            "state": final_state.value,
            "identity": identity,
            "lineage": lineage,
            "plan": plan,
            "ranked_set": ranked_set,
            "checkpoint": checkpoint,
        }

    # --------------------------------------------------------
    # Introspection
    # --------------------------------------------------------

    def events(
        self,
    ) -> Tuple[RankingEvent, ...]:
        return tuple(self._events)

    def checkpoints(
        self,
    ) -> Tuple[RankingCheckpoint, ...]:
        return tuple(self._checkpoints)

    # --------------------------------------------------------
    # Architecture declaration
    # --------------------------------------------------------

    @staticmethod
    def architecture() -> Dict[str, Any]:

        return {
            "name": "OUR SEARCH Global Ranking Architecture",
            "version": ARCHITECTURE_VERSION,

            "phase": 12,
            "stage": "12.1",

            "scale_target": SCALE_TARGET,

            "google_scale_capability_target":
                GOOGLE_SCALE_CAPABILITY_TARGET,

            "google_technology_dependency":
                GOOGLE_TECHNOLOGY_DEPENDENCY,

            "input": (
                "Phase 11 final retrieval candidate set"
            ),

            "pipeline": [
                "retrieval candidates",
                "ranking input validation",
                "distributed ranking planning",
                "ranking signal preparation",
                "ranking feature preparation",
                "distributed ranking execution",
                "global candidate ordering",
                "Phase 12 later ranking stages",
            ],

            "ranking_signal_families": [
                family.value
                for family
                in RankingSignalFamily
            ],

            "execution_modes": [
                mode.value
                for mode
                in RankingExecutionMode
            ],

            "distributed_properties": [
                "partition aware",
                "region aware",
                "distributed execution",
                "horizontal scalability",
                "partial execution support",
                "checkpointable",
                "backend replaceable",
                "lineage preserving",
                "provenance preserving",
            ],

            "global_limits": {
                "fixed_global_document_limit": False,
                "fixed_global_candidate_limit": False,
                "fixed_global_partition_limit": False,
                "fixed_global_worker_limit": False,
                "fixed_global_web_resource_limit": False,
            },

            "operational_limits_are": [
                "per-ranking execution controls",
                "per-shard safety controls",
                "resource-management controls",
                "not global Web-scale limits",
            ],

            "later_phase_12_responsibilities": [
                "relevance signal extraction",
                "document quality signals",
                "authority signals",
                "freshness signals",
                "trust signals",
                "ranking feature construction",
                "multi-stage ranking",
                "global result quality evaluation",
                "final production ranking architecture",
            ],

            "not_responsible_for": [
                "Web crawling",
                "index construction",
                "index mutation",
                "retrieval",
                "Phase 11 query understanding",
                "Google Search API",
                "Google index",
                "Google crawler",
                "Google infrastructure",
            ],
        }


# ============================================================
# PUBLIC ALIASES
# ============================================================

GlobalRanking = GlobalRankingArchitecture
GlobalRankingArchitectureV1 = GlobalRankingArchitecture
Phase12_1GlobalRankingArchitecture = GlobalRankingArchitecture


__all__ = [
    "ARCHITECTURE_VERSION",
    "SCALE_TARGET",
    "GOOGLE_SCALE_CAPABILITY_TARGET",
    "GOOGLE_TECHNOLOGY_DEPENDENCY",

    "RankingState",
    "RankingScope",
    "RankingExecutionMode",
    "RankingSignalFamily",
    "RankingArtifactState",
    "RankingEventType",

    "RankingIdentity",
    "RankingLineage",

    "RankingSignal",
    "RankingSignalProfile",

    "RankingFeature",
    "RankingFeatureVector",

    "RankingCandidate",

    "RankingShard",
    "RankingPlan",

    "RankedCandidate",
    "RankedCandidateSet",

    "RankingCheckpoint",
    "RankingEvent",

    "GlobalRankingBackend",
    "InMemoryGlobalRankingMetadata",

    "GlobalRankingPolicy",
    "GlobalRankingArchitecture",

    "GlobalRanking",
    "GlobalRankingArchitectureV1",
    "Phase12_1GlobalRankingArchitecture",
]
