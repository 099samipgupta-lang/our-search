from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from hashlib import sha256
from typing import Any, Dict, List, Mapping, Optional, Protocol, Sequence, Tuple
from uuid import uuid4


# ============================================================
# OUR SEARCH
# Phase 11.9 — Final Retrieval Architecture
# ============================================================

ARCHITECTURE_VERSION = "final-retrieval-architecture.v1"
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

class FinalRetrievalState(str, Enum):
    RECEIVED = "received"
    VALIDATING = "validating"
    QUERY_UNDERSTANDING = "query_understanding"
    NORMALIZING = "normalizing"
    INTENT_ANALYSIS = "intent_analysis"
    EXPANSION = "expansion"
    RETRIEVAL_PLANNING = "retrieval_planning"
    PARTITION_RETRIEVAL = "partition_retrieval"
    FUSION = "fusion"
    RESCORING = "rescoring"
    EARLY_FILTERING = "early_filtering"
    FINALIZING = "finalizing"
    COMPLETED = "completed"
    PARTIAL = "partial"
    REJECTED = "rejected"
    FAILED = "failed"


class RetrievalStage(str, Enum):
    QUERY_UNDERSTANDING = "11.1_query_understanding"
    NORMALIZATION = "11.2_normalization"
    INTENT_ANALYSIS = "11.3_intent_analysis"
    EXPANSION = "11.4_expansion"
    DISTRIBUTED_PLANNING = "11.5_distributed_planning"
    MULTI_PARTITION_RETRIEVAL = "11.6_multi_partition_retrieval"
    FUSION_DEDUPLICATION = "11.7_fusion_deduplication"
    RESCORING_FILTERING = "11.8_rescoring_filtering"
    FINAL_RETRIEVAL = "11.9_final_retrieval"


class ArtifactState(str, Enum):
    RECEIVED = "received"
    VALID = "valid"
    INVALID = "invalid"
    PARTIAL = "partial"
    COMPLETE = "complete"


class RetrievalReadiness(str, Enum):
    READY_FOR_RANKING = "ready_for_ranking"
    PARTIAL_READY_FOR_RANKING = "partial_ready_for_ranking"
    NOT_READY = "not_ready"


class FailureDomain(str, Enum):
    QUERY = "query"
    PLANNING = "planning"
    PARTITION = "partition"
    REPLICA = "replica"
    REGION = "region"
    ZONE = "zone"
    NODE = "node"
    FUSION = "fusion"
    RESCORING = "rescoring"
    UNKNOWN = "unknown"


class FinalRetrievalEventType(str, Enum):
    REQUEST_RECEIVED = "request_received"
    VALIDATION_STARTED = "validation_started"
    STAGE_STARTED = "stage_started"
    STAGE_COMPLETED = "stage_completed"
    PARTIAL_INPUT_DETECTED = "partial_input_detected"
    FAILURE_DETECTED = "failure_detected"
    RECOVERY_PLANNED = "recovery_planned"
    FINAL_SET_ASSEMBLED = "final_set_assembled"
    RANKING_HANDOFF_CREATED = "ranking_handoff_created"
    CHECKPOINT_CREATED = "checkpoint_created"
    RETRIEVAL_COMPLETED = "retrieval_completed"
    RETRIEVAL_REJECTED = "retrieval_rejected"
    RETRIEVAL_FAILED = "retrieval_failed"


# ============================================================
# IDENTITIES
# ============================================================

@dataclass(frozen=True)
class FinalRetrievalIdentity:
    retrieval_id: str
    query_id: str
    request_id: str
    architecture_version: str = ARCHITECTURE_VERSION
    created_at: datetime = field(default_factory=_now)


@dataclass(frozen=True)
class QueryLineage:
    query_id: str
    original_query: str
    canonical_query_hash: str
    normalized_query_hash: str
    intent_analysis_hash: Optional[str] = None
    expansion_hash: Optional[str] = None


@dataclass(frozen=True)
class RetrievalArtifactReference:
    stage: RetrievalStage
    artifact_id: str
    artifact_hash: str
    state: ArtifactState
    source: str
    created_at: datetime = field(default_factory=_now)


# ============================================================
# STAGE RESULTS
# ============================================================

@dataclass
class RetrievalStageResult:
    stage: RetrievalStage
    artifact_id: str
    artifact_hash: str
    state: ArtifactState
    payload: Mapping[str, Any]
    started_at: datetime = field(default_factory=_now)
    completed_at: Optional[datetime] = None
    partial: bool = False
    errors: Tuple[str, ...] = ()
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def completed(self) -> bool:
        return self.completed_at is not None


@dataclass(frozen=True)
class RetrievalFailure:
    failure_id: str
    stage: RetrievalStage
    domain: FailureDomain
    message: str
    recoverable: bool
    partial_result_available: bool
    created_at: datetime = field(default_factory=_now)


@dataclass(frozen=True)
class RetrievalRecoveryAction:
    action_id: str
    failure_id: str
    stage: RetrievalStage
    action: str
    target: Optional[str]
    retryable: bool
    created_at: datetime = field(default_factory=_now)


# ============================================================
# FINAL CANDIDATE CONTRACT
# ============================================================

@dataclass(frozen=True)
class FinalRetrievalCandidate:
    resource_id: str

    document_id: Optional[str] = None
    canonical_url: Optional[str] = None
    content_identity: Optional[str] = None

    retrieval_score: float = 0.0

    matched_terms: Tuple[str, ...] = ()
    matched_fields: Tuple[str, ...] = ()
    matched_phrases: Tuple[str, ...] = ()

    evidence_count: int = 0
    partition_count: int = 0
    replica_count: int = 0

    source_partitions: Tuple[str, ...] = ()
    source_replicas: Tuple[str, ...] = ()

    retrieval_features: Mapping[str, Any] = field(
        default_factory=dict
    )

    provenance: Mapping[str, Any] = field(
        default_factory=dict
    )


@dataclass
class FinalCandidateSet:
    candidate_set_id: str
    candidates: Tuple[FinalRetrievalCandidate, ...]

    source_fusion_id: Optional[str] = None
    source_rescoring_id: Optional[str] = None

    partial: bool = False
    candidate_count: int = 0

    candidate_set_hash: str = ""

    created_at: datetime = field(default_factory=_now)

    def __post_init__(self) -> None:
        if self.candidate_count == 0:
            self.candidate_count = len(self.candidates)

        if not self.candidate_set_hash:
            object.__setattr__(
                self,
                "candidate_set_hash",
                _hash(
                    tuple(
                        (
                            candidate.resource_id,
                            candidate.document_id,
                            candidate.canonical_url,
                            candidate.retrieval_score,
                        )
                        for candidate in self.candidates
                    )
                ),
            )


# ============================================================
# PHASE 12 HANDOFF
# ============================================================

@dataclass(frozen=True)
class RankingHandoffContract:
    handoff_id: str
    retrieval_id: str
    query_id: str

    canonical_query_hash: str

    candidate_set_id: str
    candidate_set_hash: str

    readiness: RetrievalReadiness

    candidate_count: int
    partial: bool

    retrieval_features_available: Tuple[str, ...]
    provenance_available: bool

    ranking_boundary: str = "phase_12_ranking"

    created_at: datetime = field(default_factory=_now)


# ============================================================
# CHECKPOINT
# ============================================================

@dataclass(frozen=True)
class FinalRetrievalCheckpoint:
    checkpoint_id: str
    retrieval_id: str

    state: FinalRetrievalState

    completed_stages: Tuple[RetrievalStage, ...]
    partial_stages: Tuple[RetrievalStage, ...]

    stage_artifacts: Tuple[RetrievalArtifactReference, ...]

    candidate_set_id: Optional[str]

    checkpoint_hash: str

    created_at: datetime = field(default_factory=_now)


# ============================================================
# EVENTS
# ============================================================

@dataclass(frozen=True)
class FinalRetrievalEvent:
    event_id: str
    retrieval_id: str
    event_type: FinalRetrievalEventType

    stage: Optional[RetrievalStage]

    message: str

    metadata: Mapping[str, Any] = field(
        default_factory=dict
    )

    created_at: datetime = field(default_factory=_now)


# ============================================================
# BACKEND
# ============================================================

class FinalRetrievalBackend(Protocol):

    def persist_checkpoint(
        self,
        checkpoint: FinalRetrievalCheckpoint,
    ) -> None:
        ...

    def persist_event(
        self,
        event: FinalRetrievalEvent,
    ) -> None:
        ...

    def persist_candidate_set(
        self,
        candidate_set: FinalCandidateSet,
    ) -> None:
        ...

    def persist_handoff(
        self,
        handoff: RankingHandoffContract,
    ) -> None:
        ...


class InMemoryFinalRetrievalBackend:
    """
    Reference metadata backend only.

    This is not intended to be the global production storage system.
    Production deployments can replace this backend with distributed
    durable infrastructure.
    """

    def __init__(self) -> None:
        self._checkpoints: List[FinalRetrievalCheckpoint] = []
        self._events: List[FinalRetrievalEvent] = []
        self._candidate_sets: List[FinalCandidateSet] = []
        self._handoffs: List[RankingHandoffContract] = []

    def persist_checkpoint(
        self,
        checkpoint: FinalRetrievalCheckpoint,
    ) -> None:
        self._checkpoints.append(checkpoint)

    def persist_event(
        self,
        event: FinalRetrievalEvent,
    ) -> None:
        self._events.append(event)

    def persist_candidate_set(
        self,
        candidate_set: FinalCandidateSet,
    ) -> None:
        self._candidate_sets.append(candidate_set)

    def persist_handoff(
        self,
        handoff: RankingHandoffContract,
    ) -> None:
        self._handoffs.append(handoff)

    def checkpoints(self) -> Tuple[FinalRetrievalCheckpoint, ...]:
        return tuple(self._checkpoints)

    def events(self) -> Tuple[FinalRetrievalEvent, ...]:
        return tuple(self._events)

    def candidate_sets(self) -> Tuple[FinalCandidateSet, ...]:
        return tuple(self._candidate_sets)

    def handoffs(self) -> Tuple[RankingHandoffContract, ...]:
        return tuple(self._handoffs)


# ============================================================
# POLICY
# ============================================================

@dataclass(frozen=True)
class FinalRetrievalPolicy:

    # These are operational safety controls per retrieval execution.
    # They are NOT global Web-scale limits.
    max_stage_errors: int = 32
    max_recovery_actions: int = 128

    require_query_lineage: bool = True
    require_canonical_query: bool = True
    require_candidate_identity: bool = True

    allow_partial_retrieval: bool = True

    require_rescoring_stage: bool = True
    require_fusion_stage: bool = True
    require_partition_retrieval_stage: bool = True

    create_checkpoint_on_completion: bool = True

    # Phase 12 boundary.
    handoff_to_ranking: bool = True


# ============================================================
# ARCHITECTURE
# ============================================================

class FinalRetrievalArchitecture:

    """
    OUR SEARCH Phase 11.9.

    This class is the final retrieval control architecture.

    It does NOT:
        - crawl the Web
        - mutate the index
        - perform final ranking
        - replace Phase 12 ranking
        - depend on Google Search
        - depend on Google's index
        - depend on Google's crawler
        - depend on Google's infrastructure

    It coordinates the outputs of Phase 11.1 through 11.8
    and creates the final retrieval contract consumed by Phase 12.
    """

    REQUIRED_STAGES: Tuple[RetrievalStage, ...] = (
        RetrievalStage.QUERY_UNDERSTANDING,
        RetrievalStage.NORMALIZATION,
        RetrievalStage.INTENT_ANALYSIS,
        RetrievalStage.EXPANSION,
        RetrievalStage.DISTRIBUTED_PLANNING,
        RetrievalStage.MULTI_PARTITION_RETRIEVAL,
        RetrievalStage.FUSION_DEDUPLICATION,
        RetrievalStage.RESCORING_FILTERING,
    )

    def __init__(
        self,
        backend: Optional[FinalRetrievalBackend] = None,
        policy: Optional[FinalRetrievalPolicy] = None,
    ) -> None:
        self.backend = (
            backend
            if backend is not None
            else InMemoryFinalRetrievalBackend()
        )

        self.policy = (
            policy
            if policy is not None
            else FinalRetrievalPolicy()
        )

        self._events: List[FinalRetrievalEvent] = []
        self._checkpoints: List[FinalRetrievalCheckpoint] = []
        self._failures: List[RetrievalFailure] = []
        self._recoveries: List[RetrievalRecoveryAction] = []

    # --------------------------------------------------------
    # Event helpers
    # --------------------------------------------------------

    def _emit(
        self,
        retrieval_id: str,
        event_type: FinalRetrievalEventType,
        message: str,
        stage: Optional[RetrievalStage] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> FinalRetrievalEvent:

        event = FinalRetrievalEvent(
            event_id=_new_id("retrieval_event"),
            retrieval_id=retrieval_id,
            event_type=event_type,
            stage=stage,
            message=message,
            metadata=dict(metadata or {}),
        )

        self._events.append(event)
        self.backend.persist_event(event)

        return event

    # --------------------------------------------------------
    # Stage conversion
    # --------------------------------------------------------

    @staticmethod
    def _stage_from_mapping(
        stage: RetrievalStage,
        value: Any,
    ) -> RetrievalStageResult:

        if isinstance(value, RetrievalStageResult):
            return value

        if isinstance(value, Mapping):

            artifact_id = str(
                value.get(
                    "artifact_id",
                    value.get(
                        "result_id",
                        _new_id("artifact"),
                    ),
                )
            )

            payload = value.get(
                "payload",
                value,
            )

            if not isinstance(payload, Mapping):
                payload = {
                    "value": payload,
                }

            partial = bool(
                value.get(
                    "partial",
                    value.get(
                        "is_partial",
                        False,
                    ),
                )
            )

            state_value = value.get(
                "state",
                ArtifactState.COMPLETE.value,
            )

            try:
                state = ArtifactState(state_value)
            except ValueError:
                state = (
                    ArtifactState.PARTIAL
                    if partial
                    else ArtifactState.COMPLETE
                )

            artifact_hash = str(
                value.get(
                    "artifact_hash",
                    _hash(payload),
                )
            )

            return RetrievalStageResult(
                stage=stage,
                artifact_id=artifact_id,
                artifact_hash=artifact_hash,
                state=state,
                payload=payload,
                completed_at=_now(),
                partial=partial,
                errors=tuple(
                    str(item)
                    for item in value.get(
                        "errors",
                        (),
                    )
                ),
                metadata=dict(
                    value.get(
                        "metadata",
                        {},
                    )
                ),
            )

        raise TypeError(
            f"Unsupported result for {stage.value}: "
            f"{type(value).__name__}"
        )

    # --------------------------------------------------------
    # Validation
    # --------------------------------------------------------

    def _validate(
        self,
        query_lineage: QueryLineage,
        stages: Mapping[RetrievalStage, RetrievalStageResult],
    ) -> Tuple[str, ...]:

        errors: List[str] = []

        if self.policy.require_query_lineage:

            if not query_lineage.query_id:
                errors.append("missing query_id")

            if not query_lineage.original_query:
                errors.append("missing original_query")

        if (
            self.policy.require_canonical_query
            and not query_lineage.canonical_query_hash
        ):
            errors.append(
                "missing canonical_query_hash"
            )

        for required_stage in self.REQUIRED_STAGES:

            if required_stage not in stages:
                errors.append(
                    f"missing stage: {required_stage.value}"
                )

        if (
            self.policy.require_partition_retrieval_stage
            and RetrievalStage.MULTI_PARTITION_RETRIEVAL
            not in stages
        ):
            errors.append(
                "missing multi-partition retrieval"
            )

        if (
            self.policy.require_fusion_stage
            and RetrievalStage.FUSION_DEDUPLICATION
            not in stages
        ):
            errors.append(
                "missing candidate fusion"
            )

        if (
            self.policy.require_rescoring_stage
            and RetrievalStage.RESCORING_FILTERING
            not in stages
        ):
            errors.append(
                "missing retrieval rescoring"
            )

        return tuple(errors)

    # --------------------------------------------------------
    # Candidate extraction
    # --------------------------------------------------------

    @staticmethod
    def _candidate_from_mapping(
        value: Any,
    ) -> FinalRetrievalCandidate:

        if isinstance(value, FinalRetrievalCandidate):
            return value

        if not isinstance(value, Mapping):
            raise TypeError(
                "Candidate must be a mapping or "
                "FinalRetrievalCandidate"
            )

        identity = value.get(
            "resource_id",
            value.get(
                "document_id",
                value.get(
                    "canonical_url",
                    value.get(
                        "resource",
                        "",
                    ),
                ),
            ),
        )

        if not identity:
            raise ValueError(
                "candidate has no resource identity"
            )

        return FinalRetrievalCandidate(
            resource_id=str(identity),

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
                        value.get(
                            "normalized_score",
                            0.0,
                        ),
                    ),
                )
            ),

            matched_terms=tuple(
                str(item)
                for item in value.get(
                    "matched_terms",
                    (),
                )
            ),

            matched_fields=tuple(
                str(item)
                for item in value.get(
                    "matched_fields",
                    (),
                )
            ),

            matched_phrases=tuple(
                str(item)
                for item in value.get(
                    "matched_phrases",
                    (),
                )
            ),

            evidence_count=int(
                value.get(
                    "evidence_count",
                    0,
                )
            ),

            partition_count=int(
                value.get(
                    "partition_count",
                    len(
                        value.get(
                            "source_partitions",
                            (),
                        )
                    ),
                )
            ),

            replica_count=int(
                value.get(
                    "replica_count",
                    len(
                        value.get(
                            "source_replicas",
                            (),
                        )
                    ),
                )
            ),

            source_partitions=tuple(
                str(item)
                for item in value.get(
                    "source_partitions",
                    (),
                )
            ),

            source_replicas=tuple(
                str(item)
                for item in value.get(
                    "source_replicas",
                    (),
                )
            ),

            retrieval_features=dict(
                value.get(
                    "retrieval_features",
                    value.get(
                        "features",
                        {},
                    ),
                )
            ),

            provenance=dict(
                value.get(
                    "provenance",
                    {},
                )
            ),
        )

    # --------------------------------------------------------
    # Final candidate-set assembly
    # --------------------------------------------------------

    def _assemble_candidate_set(
        self,
        retrieval_id: str,
        stages: Mapping[
            RetrievalStage,
            RetrievalStageResult,
        ],
    ) -> FinalCandidateSet:

        rescoring = stages[
            RetrievalStage.RESCORING_FILTERING
        ]

        payload = rescoring.payload

        raw_candidates = payload.get(
            "candidates",
            payload.get(
                "rescored_candidates",
                payload.get(
                    "surviving_candidates",
                    payload.get(
                        "results",
                        (),
                    ),
                ),
            ),
        )

        if isinstance(raw_candidates, Mapping):
            raw_candidates = tuple(
                raw_candidates.values()
            )

        if raw_candidates is None:
            raw_candidates = ()

        candidates: List[
            FinalRetrievalCandidate
        ] = []

        for raw_candidate in raw_candidates:

            try:
                candidate = (
                    self._candidate_from_mapping(
                        raw_candidate
                    )
                )

            except (TypeError, ValueError):
                continue

            if (
                self.policy.require_candidate_identity
                and not candidate.resource_id
            ):
                continue

            candidates.append(candidate)

        partial = any(
            result.partial
            for result in stages.values()
        )

        candidate_set = FinalCandidateSet(
            candidate_set_id=_new_id(
                "final_candidate_set"
            ),
            candidates=tuple(candidates),
            source_fusion_id=(
                stages[
                    RetrievalStage.FUSION_DEDUPLICATION
                ].artifact_id
            ),
            source_rescoring_id=(
                rescoring.artifact_id
            ),
            partial=partial,
        )

        self.backend.persist_candidate_set(
            candidate_set
        )

        return candidate_set

    # --------------------------------------------------------
    # Ranking handoff
    # --------------------------------------------------------

    def _create_ranking_handoff(
        self,
        identity: FinalRetrievalIdentity,
        query_lineage: QueryLineage,
        candidate_set: FinalCandidateSet,
    ) -> RankingHandoffContract:

        readiness = (
            RetrievalReadiness.PARTIAL_READY_FOR_RANKING
            if candidate_set.partial
            else RetrievalReadiness.READY_FOR_RANKING
        )

        features = set()

        for candidate in candidate_set.candidates:
            features.update(
                str(key)
                for key in candidate.retrieval_features
            )

        handoff = RankingHandoffContract(
            handoff_id=_new_id(
                "ranking_handoff"
            ),

            retrieval_id=identity.retrieval_id,
            query_id=identity.query_id,

            canonical_query_hash=(
                query_lineage.canonical_query_hash
            ),

            candidate_set_id=(
                candidate_set.candidate_set_id
            ),

            candidate_set_hash=(
                candidate_set.candidate_set_hash
            ),

            readiness=readiness,

            candidate_count=(
                candidate_set.candidate_count
            ),

            partial=candidate_set.partial,

            retrieval_features_available=tuple(
                sorted(features)
            ),

            provenance_available=any(
                bool(candidate.provenance)
                for candidate in candidate_set.candidates
            ),
        )

        self.backend.persist_handoff(handoff)

        return handoff

    # --------------------------------------------------------
    # Checkpoint
    # --------------------------------------------------------

    def _checkpoint(
        self,
        identity: FinalRetrievalIdentity,
        state: FinalRetrievalState,
        stages: Mapping[
            RetrievalStage,
            RetrievalStageResult,
        ],
        candidate_set: Optional[FinalCandidateSet],
    ) -> FinalRetrievalCheckpoint:

        completed = tuple(
            stage
            for stage, result in stages.items()
            if result.state == ArtifactState.COMPLETE
            and not result.partial
        )

        partial = tuple(
            stage
            for stage, result in stages.items()
            if result.partial
            or result.state == ArtifactState.PARTIAL
        )

        artifacts = tuple(
            RetrievalArtifactReference(
                stage=stage,
                artifact_id=result.artifact_id,
                artifact_hash=result.artifact_hash,
                state=result.state,
            )
            for stage, result in stages.items()
        )

        checkpoint_material = (
            identity.retrieval_id,
            state.value,
            completed,
            partial,
            tuple(
                (
                    artifact.stage.value,
                    artifact.artifact_id,
                    artifact.artifact_hash,
                    artifact.state.value,
                )
                for artifact in artifacts
            ),
            (
                candidate_set.candidate_set_id
                if candidate_set
                else None
            ),
        )

        checkpoint = FinalRetrievalCheckpoint(
            checkpoint_id=_new_id(
                "retrieval_checkpoint"
            ),

            retrieval_id=identity.retrieval_id,

            state=state,

            completed_stages=completed,
            partial_stages=partial,

            stage_artifacts=artifacts,

            candidate_set_id=(
                candidate_set.candidate_set_id
                if candidate_set
                else None
            ),

            checkpoint_hash=_hash(
                checkpoint_material
            ),
        )

        self._checkpoints.append(checkpoint)
        self.backend.persist_checkpoint(checkpoint)

        self._emit(
            identity.retrieval_id,
            FinalRetrievalEventType.CHECKPOINT_CREATED,
            "Final retrieval checkpoint created",
            metadata={
                "checkpoint_id": checkpoint.checkpoint_id,
                "state": state.value,
            },
        )

        return checkpoint

    # --------------------------------------------------------
    # Main orchestration
    # --------------------------------------------------------

    def retrieve(
        self,
        *,
        query_id: str,
        request_id: Optional[str],
        original_query: str,
        canonical_query_hash: str,
        normalized_query_hash: str,
        stage_results: Mapping[Any, Any],
        intent_analysis_hash: Optional[str] = None,
        expansion_hash: Optional[str] = None,
    ) -> Dict[str, Any]:

        retrieval_id = _new_id(
            "final_retrieval"
        )

        identity = FinalRetrievalIdentity(
            retrieval_id=retrieval_id,
            query_id=query_id,
            request_id=(
                request_id
                if request_id
                else _new_id("retrieval_request")
            ),
        )

        query_lineage = QueryLineage(
            query_id=query_id,
            original_query=original_query,
            canonical_query_hash=canonical_query_hash,
            normalized_query_hash=normalized_query_hash,
            intent_analysis_hash=intent_analysis_hash,
            expansion_hash=expansion_hash,
        )

        self._emit(
            retrieval_id,
            FinalRetrievalEventType.REQUEST_RECEIVED,
            "Final retrieval request received",
        )

        self._emit(
            retrieval_id,
            FinalRetrievalEventType.VALIDATION_STARTED,
            "Final retrieval validation started",
        )

        normalized_stages: Dict[
            RetrievalStage,
            RetrievalStageResult,
        ] = {}

        for raw_stage, value in stage_results.items():

            if isinstance(raw_stage, RetrievalStage):
                stage = raw_stage
            else:
                try:
                    stage = RetrievalStage(str(raw_stage))
                except ValueError:
                    continue

            normalized_stages[stage] = (
                self._stage_from_mapping(
                    stage,
                    value,
                )
            )

        validation_errors = self._validate(
            query_lineage,
            normalized_stages,
        )

        if validation_errors:

            self._emit(
                retrieval_id,
                FinalRetrievalEventType.RETRIEVAL_REJECTED,
                "Final retrieval validation rejected request",
                metadata={
                    "errors": validation_errors,
                },
            )

            return {
                "retrieval_id": retrieval_id,
                "state": FinalRetrievalState.REJECTED.value,
                "identity": identity,
                "query_lineage": query_lineage,
                "errors": validation_errors,
                "candidate_set": None,
                "ranking_handoff": None,
                "checkpoint": None,
            }

        for stage in self.REQUIRED_STAGES:

            self._emit(
                retrieval_id,
                FinalRetrievalEventType.STAGE_STARTED,
                f"Final retrieval accepted {stage.value}",
                stage=stage,
            )

            result = normalized_stages[stage]

            if result.partial:

                self._emit(
                    retrieval_id,
                    FinalRetrievalEventType.PARTIAL_INPUT_DETECTED,
                    f"Partial result received from {stage.value}",
                    stage=stage,
                    metadata={
                        "artifact_id": result.artifact_id,
                    },
                )

            self._emit(
                retrieval_id,
                FinalRetrievalEventType.STAGE_COMPLETED,
                f"Stage result accepted from {stage.value}",
                stage=stage,
                metadata={
                    "artifact_id": result.artifact_id,
                    "partial": result.partial,
                },
            )

        state = (
            FinalRetrievalState.PARTIAL
            if any(
                result.partial
                for result in normalized_stages.values()
            )
            else FinalRetrievalState.FINALIZING
        )

        candidate_set = self._assemble_candidate_set(
            retrieval_id,
            normalized_stages,
        )

        self._emit(
            retrieval_id,
            FinalRetrievalEventType.FINAL_SET_ASSEMBLED,
            "Final retrieval candidate set assembled",
            stage=RetrievalStage.FINAL_RETRIEVAL,
            metadata={
                "candidate_set_id": (
                    candidate_set.candidate_set_id
                ),
                "candidate_count": (
                    candidate_set.candidate_count
                ),
                "partial": candidate_set.partial,
            },
        )

        ranking_handoff = None

        if self.policy.handoff_to_ranking:

            ranking_handoff = (
                self._create_ranking_handoff(
                    identity,
                    query_lineage,
                    candidate_set,
                )
            )

            self._emit(
                retrieval_id,
                FinalRetrievalEventType.RANKING_HANDOFF_CREATED,
                "Final retrieval contract handed to Phase 12 ranking",
                stage=RetrievalStage.FINAL_RETRIEVAL,
                metadata={
                    "handoff_id": (
                        ranking_handoff.handoff_id
                    ),
                    "readiness": (
                        ranking_handoff.readiness.value
                    ),
                },
            )

        final_state = (
            FinalRetrievalState.PARTIAL
            if candidate_set.partial
            else FinalRetrievalState.COMPLETED
        )

        checkpoint = None

        if self.policy.create_checkpoint_on_completion:

            checkpoint = self._checkpoint(
                identity,
                final_state,
                normalized_stages,
                candidate_set,
            )

        self._emit(
            retrieval_id,
            FinalRetrievalEventType.RETRIEVAL_COMPLETED,
            "Final retrieval architecture completed",
            stage=RetrievalStage.FINAL_RETRIEVAL,
            metadata={
                "candidate_count": (
                    candidate_set.candidate_count
                ),
                "partial": candidate_set.partial,
                "ranking_ready": (
                    ranking_handoff is not None
                ),
            },
        )

        return {
            "retrieval_id": retrieval_id,
            "state": final_state.value,
            "identity": identity,
            "query_lineage": query_lineage,
            "candidate_set": candidate_set,
            "ranking_handoff": ranking_handoff,
            "checkpoint": checkpoint,
            "failures": tuple(self._failures),
            "recovery_actions": tuple(
                self._recoveries
            ),
        }

    # --------------------------------------------------------
    # Introspection
    # --------------------------------------------------------

    def events(
        self,
    ) -> Tuple[FinalRetrievalEvent, ...]:
        return tuple(self._events)

    def checkpoints(
        self,
    ) -> Tuple[FinalRetrievalCheckpoint, ...]:
        return tuple(self._checkpoints)

    def failures(
        self,
    ) -> Tuple[RetrievalFailure, ...]:
        return tuple(self._failures)

    def recovery_actions(
        self,
    ) -> Tuple[RetrievalRecoveryAction, ...]:
        return tuple(self._recoveries)

    # --------------------------------------------------------
    # Architecture declaration
    # --------------------------------------------------------

    @staticmethod
    def architecture() -> Dict[str, Any]:

        return {
            "name": "OUR SEARCH Final Retrieval Architecture",
            "version": ARCHITECTURE_VERSION,

            "phase": 11,
            "stage": "11.9",

            "scale_target": SCALE_TARGET,

            "google_scale_capability_target":
                GOOGLE_SCALE_CAPABILITY_TARGET,

            "google_technology_dependency":
                GOOGLE_TECHNOLOGY_DEPENDENCY,

            "pipeline": [
                RetrievalStage.QUERY_UNDERSTANDING.value,
                RetrievalStage.NORMALIZATION.value,
                RetrievalStage.INTENT_ANALYSIS.value,
                RetrievalStage.EXPANSION.value,
                RetrievalStage.DISTRIBUTED_PLANNING.value,
                RetrievalStage.MULTI_PARTITION_RETRIEVAL.value,
                RetrievalStage.FUSION_DEDUPLICATION.value,
                RetrievalStage.RESCORING_FILTERING.value,
                RetrievalStage.FINAL_RETRIEVAL.value,
                "phase_12_ranking",
            ],

            "responsibilities": [
                "unify phase 11 retrieval stages",
                "preserve query lineage",
                "preserve retrieval provenance",
                "preserve distributed retrieval evidence",
                "preserve partition and replica information",
                "assemble final retrieval candidate set",
                "preserve partial-result state",
                "create recovery/checkpoint metadata",
                "create Phase 12 ranking handoff",
            ],

            "not_responsible_for": [
                "final ranking",
                "authority ranking",
                "global quality ranking",
                "spam ranking",
                "crawl execution",
                "index mutation",
                "document storage",
                "Web crawling",
                "Google Search API",
                "Google index",
                "Google crawler",
                "Google infrastructure",
            ],

            "distributed_properties": [
                "horizontally scalable",
                "partition aware",
                "replica aware",
                "region aware",
                "failure aware",
                "partial-result aware",
                "checkpointable",
                "backend replaceable",
                "lineage preserving",
                "provenance preserving",
            ],

            "global_limit_policy": {
                "fixed_global_document_limit": False,
                "fixed_global_candidate_limit": False,
                "fixed_global_partition_limit": False,
                "fixed_global_worker_limit": False,
                "fixed_global_replica_limit": False,
                "fixed_global_web_resource_limit": False,
            },

            "operational_limits_are": [
                "per-request safety controls",
                "per-execution resource controls",
                "not global Web-scale architecture limits",
            ],

            "phase_12_boundary": {
                "input": "final retrieval candidate set",
                "handoff": "ranking_handoff_contract",
                "ranking_owns": [
                    "relevance ranking",
                    "authority signals",
                    "quality signals",
                    "global ranking features",
                    "final ordering",
                ],
            },

            "dependencies": [
                "Phase 10 massive index/storage architecture",
                "Phase 11.1 global query understanding",
                "Phase 11.2 query normalization/canonicalization",
                "Phase 11.3 intent analysis",
                "Phase 11.4 term/field expansion",
                "Phase 11.5 distributed retrieval planning",
                "Phase 11.6 multi-partition retrieval",
                "Phase 11.7 candidate fusion/deduplication",
                "Phase 11.8 retrieval rescoring/early filtering",
            ],

            "protected_components": [
                "website_server.py",
                "search_service/server.py",
            ],
        }


# ============================================================
# PUBLIC ALIASES
# ============================================================

FinalRetrievalArchitectureV1 = (
    FinalRetrievalArchitecture
)

GlobalFinalRetrievalArchitecture = (
    FinalRetrievalArchitecture
)

FinalRetrieval = (
    FinalRetrievalArchitecture
)

Phase11_9FinalRetrievalArchitecture = (
    FinalRetrievalArchitecture
)


__all__ = [
    "ARCHITECTURE_VERSION",
    "SCALE_TARGET",
    "GOOGLE_SCALE_CAPABILITY_TARGET",
    "GOOGLE_TECHNOLOGY_DEPENDENCY",

    "FinalRetrievalState",
    "RetrievalStage",
    "ArtifactState",
    "RetrievalReadiness",
    "FailureDomain",
    "FinalRetrievalEventType",

    "FinalRetrievalIdentity",
    "QueryLineage",
    "RetrievalArtifactReference",

    "RetrievalStageResult",
    "RetrievalFailure",
    "RetrievalRecoveryAction",

    "FinalRetrievalCandidate",
    "FinalCandidateSet",

    "RankingHandoffContract",

    "FinalRetrievalCheckpoint",
    "FinalRetrievalEvent",

    "FinalRetrievalBackend",
    "InMemoryFinalRetrievalBackend",

    "FinalRetrievalPolicy",
    "FinalRetrievalArchitecture",

    "FinalRetrievalArchitectureV1",
    "GlobalFinalRetrievalArchitecture",
    "FinalRetrieval",
    "Phase11_9FinalRetrievalArchitecture",
]
