"""
OUR SEARCH — Phase 11.6
Multi-Partition Candidate Retrieval

Architecture version:
    multi-partition-candidate-retrieval.v1

Purpose:
    Convert a distributed retrieval execution plan into a large-scale
    candidate retrieval control architecture capable of coordinating
    candidate reads across many distributed index partitions.

Design target:
    Billions → trillions of publicly accessible Web resources.

This module does NOT:
    - rank final documents
    - perform final result scoring
    - mutate the index
    - crawl the Web
    - generate embeddings
    - depend on Google Search/API/index/crawler/infrastructure
    - replace the protected production server components

Pipeline:

    11.5 DISTRIBUTED RETRIEVAL PLAN
                ↓
    RETRIEVAL REQUEST ACCEPTANCE
                ↓
    PARTITION/TASK MATERIALIZATION
                ↓
    PARALLEL CANDIDATE READS
                ↓
    POSTING / DOCUMENT ID RESOLUTION
                ↓
    LOCAL CANDIDATE COLLECTION
                ↓
    PARTIAL RESULT HANDLING
                ↓
    RETRY / FAILOVER
                ↓
    CANDIDATE SET ASSEMBLY
                ↓
    11.7 CANDIDATE FUSION & DEDUPLICATION

Architecture principle:

    QUERY
      ↓
    RETRIEVAL PLAN
      ↓
    MANY PARTITIONS
      ↓
    MANY REPLICAS
      ↓
    MANY PARALLEL RETRIEVAL TASKS
      ↓
    CANDIDATE DOCUMENTS

There is intentionally no fixed global:
    - document limit
    - term limit
    - partition limit
    - candidate limit
    - worker limit
    - replica limit
    - region limit

Operational budgets are per-request/per-execution controls and are NOT
global Web-scale capacity ceilings.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from hashlib import sha256
from typing import Any, Dict, Iterable, List, Mapping, Optional, Protocol, Sequence, Tuple
from uuid import uuid4


ARCHITECTURE_VERSION = "multi-partition-candidate-retrieval.v1"
SCALE_TARGET = "billions_to_trillions_of_public_web_resources"
GOOGLE_SCALE_CAPABILITY_TARGET = True
GOOGLE_TECHNOLOGY_DEPENDENCY = False


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex}"


def _hash(value: Any) -> str:
    payload = repr(value).encode("utf-8")
    return sha256(payload).hexdigest()


class CandidateRetrievalState(str, Enum):
    RECEIVED = "received"
    VALIDATING = "validating"
    MATERIALIZING_TASKS = "materializing_tasks"
    DISPATCHING = "dispatching"
    RETRIEVING = "retrieving"
    COLLECTING = "collecting"
    PARTIAL_RESULT = "partial_result"
    RETRYING = "retrying"
    COMPLETED = "completed"
    REJECTED = "rejected"
    FAILED = "failed"


class CandidateReadMode(str, Enum):
    TERM = "term"
    PHRASE = "phrase"
    FIELD = "field"
    HYBRID = "hybrid"
    DOCUMENT = "document"


class CandidateReadState(str, Enum):
    PLANNED = "planned"
    DISPATCHED = "dispatched"
    RUNNING = "running"
    COMPLETED = "completed"
    PARTIAL = "partial"
    FAILED = "failed"
    RETRYING = "retrying"
    CANCELLED = "cancelled"


class CandidateSourceState(str, Enum):
    INDEX_POSTING = "index_posting"
    TERM_DICTIONARY = "term_dictionary"
    FIELD_POSTING = "field_posting"
    PHRASE_POSTING = "phrase_posting"
    DOCUMENT_STORE = "document_store"


class CandidateDocumentState(str, Enum):
    DISCOVERED = "discovered"
    VERIFIED = "verified"
    PARTIAL = "partial"
    REJECTED = "rejected"


class CandidateFailureAction(str, Enum):
    RETRY_REPLICA = "retry_replica"
    RETRY_REGION = "retry_region"
    RETRY_CROSS_REGION = "retry_cross_region"
    RETURN_PARTIAL = "return_partial"
    CANCEL = "cancel"


class CandidateCollectionState(str, Enum):
    OPEN = "open"
    COLLECTING = "collecting"
    COMPLETE = "complete"
    PARTIAL = "partial"
    FAILED = "failed"


class CandidateRetrievalEventType(str, Enum):
    RETRIEVAL_RECEIVED = "retrieval_received"
    RETRIEVAL_STARTED = "retrieval_started"
    TASKS_MATERIALIZED = "tasks_materialized"
    READS_DISPATCHED = "reads_dispatched"
    CANDIDATES_RECEIVED = "candidates_received"
    PARTIAL_RESULTS_DETECTED = "partial_results_detected"
    RETRIES_STARTED = "retries_started"
    FAILOVER_STARTED = "failover_started"
    CANDIDATES_COLLECTED = "candidates_collected"
    COLLECTION_COMPLETED = "collection_completed"
    RETRIEVAL_COMPLETED = "retrieval_completed"
    RETRIEVAL_REJECTED = "retrieval_rejected"
    RETRIEVAL_FAILED = "retrieval_failed"
    CHECKPOINT_CREATED = "checkpoint_created"


@dataclass(frozen=True)
class CandidateRetrievalIdentity:
    retrieval_id: str
    plan_id: str
    query_id: str
    canonical_query_hash: str
    created_at: str
    architecture_version: str = ARCHITECTURE_VERSION


@dataclass(frozen=True)
class CandidatePartitionTarget:
    partition_id: str
    partition_epoch: int
    region: str
    zone: str
    routing_version: int
    state: str
    logical_index_id: Optional[str] = None


@dataclass(frozen=True)
class CandidateReplicaTarget:
    replica_id: str
    partition_id: str
    region: str
    zone: str
    state: str
    health: float
    load: float
    endpoint: Optional[str] = None
    storage_tier: Optional[str] = None


@dataclass(frozen=True)
class CandidateReadRequest:
    read_id: str
    task_id: str
    partition: CandidatePartitionTarget
    replica: CandidateReplicaTarget
    mode: CandidateReadMode
    terms: Tuple[str, ...] = ()
    phrases: Tuple[str, ...] = ()
    fields: Tuple[str, ...] = ()
    candidate_limit: Optional[int] = None
    timeout_ms: int = 1500
    created_at: str = field(default_factory=_now)


@dataclass(frozen=True)
class CandidatePostingHit:
    document_id: str
    partition_id: str
    term: Optional[str] = None
    field: Optional[str] = None
    phrase: Optional[str] = None
    frequency: Optional[int] = None
    positions_available: bool = False
    source: CandidateSourceState = CandidateSourceState.INDEX_POSTING
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CandidateDocument:
    document_id: str
    partition_id: str
    source_reads: Tuple[str, ...]
    matched_terms: Tuple[str, ...]
    matched_fields: Tuple[str, ...]
    matched_phrases: Tuple[str, ...]
    state: CandidateDocumentState
    retrieval_evidence: Mapping[str, Any] = field(default_factory=dict)


@dataclass
class CandidateReadResult:
    read_id: str
    task_id: str
    state: CandidateReadState
    partition_id: str
    replica_id: str
    candidates: List[CandidatePostingHit] = field(default_factory=list)
    returned_count: int = 0
    exhausted: bool = False
    partial: bool = False
    error: Optional[str] = None
    latency_ms: Optional[float] = None
    completed_at: Optional[str] = None


@dataclass(frozen=True)
class CandidateFailurePlan:
    read_id: str
    actions: Tuple[CandidateFailureAction, ...]
    max_attempts: int
    allow_partial: bool
    fallback_replica_ids: Tuple[str, ...] = ()
    fallback_regions: Tuple[str, ...] = ()


@dataclass
class CandidateCollection:
    collection_id: str
    retrieval_id: str
    state: CandidateCollectionState
    documents: Dict[str, CandidateDocument] = field(default_factory=dict)
    reads_completed: int = 0
    reads_partial: int = 0
    reads_failed: int = 0
    duplicate_hits_collapsed: int = 0
    started_at: str = field(default_factory=_now)
    completed_at: Optional[str] = None


@dataclass(frozen=True)
class CandidateRetrievalBudget:
    max_reads: int = 512
    max_parallel_reads: int = 64
    max_candidates_per_read: int = 1024
    max_total_candidates: int = 100000
    max_retries: int = 1024
    max_execution_groups: int = 256
    timeout_ms: int = 1500


@dataclass(frozen=True)
class CandidateRetrievalDecision:
    retrieval_id: str
    state: CandidateRetrievalState
    planned_reads: int
    accepted_reads: int
    rejected_reads: int
    partial_reads: int
    estimated_candidates: int
    budget_applied: bool
    decision_reason: str


@dataclass
class MultiPartitionCandidateRetrieval:
    retrieval_id: str
    identity: CandidateRetrievalIdentity
    state: CandidateRetrievalState
    reads: List[CandidateReadRequest] = field(default_factory=list)
    results: Dict[str, CandidateReadResult] = field(default_factory=dict)
    collection: Optional[CandidateCollection] = None
    failure_plans: Dict[str, CandidateFailurePlan] = field(default_factory=dict)
    decision: Optional[CandidateRetrievalDecision] = None
    candidate_count: int = 0
    created_at: str = field(default_factory=_now)
    completed_at: Optional[str] = None
    retrieval_hash: Optional[str] = None


@dataclass(frozen=True)
class CandidateRetrievalCheckpoint:
    checkpoint_id: str
    retrieval_id: str
    state: CandidateRetrievalState
    read_ids: Tuple[str, ...]
    completed_read_ids: Tuple[str, ...]
    failed_read_ids: Tuple[str, ...]
    candidate_count: int
    collection_state: Optional[CandidateCollectionState]
    created_at: str = field(default_factory=_now)


@dataclass(frozen=True)
class CandidateRetrievalEvent:
    event_id: str
    retrieval_id: str
    event_type: CandidateRetrievalEventType
    state: CandidateRetrievalState
    timestamp: str
    payload: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MultiPartitionCandidateRetrievalRequest:
    plan_id: str
    query_id: str
    canonical_query_hash: str
    execution_plan: Mapping[str, Any]
    max_candidates: Optional[int] = None
    requested_region: Optional[str] = None
    request_metadata: Mapping[str, Any] = field(default_factory=dict)


class MultiPartitionCandidateRetrievalBackend(Protocol):
    """
    Replaceable distributed retrieval backend.

    A production implementation can connect this contract to:
        - distributed inverted-index partitions
        - posting-block readers
        - term dictionaries
        - field indexes
        - phrase indexes
        - document identity stores
        - regional retrieval services
        - replica-aware execution systems

    The architecture itself does not depend on a particular storage,
    database, RPC framework, or infrastructure vendor.
    """

    def list_partition_targets(
        self,
        execution_plan: Mapping[str, Any],
    ) -> Sequence[CandidatePartitionTarget]:
        ...

    def list_replica_targets(
        self,
        partition_id: str,
        limit: int = 16,
    ) -> Sequence[CandidateReplicaTarget]:
        ...

    def read_candidates(
        self,
        request: CandidateReadRequest,
    ) -> CandidateReadResult:
        ...


class InMemoryMultiPartitionCandidateRetrievalBackend:
    """
    Reference metadata backend.

    This implementation intentionally provides deterministic empty
    retrieval results. It is an architecture reference, not the
    production distributed retrieval engine.
    """

    def __init__(
        self,
        partitions: Optional[Iterable[CandidatePartitionTarget]] = None,
        replicas: Optional[Mapping[str, Sequence[CandidateReplicaTarget]]] = None,
    ) -> None:
        self._partitions = list(partitions or [])
        self._replicas: Dict[str, List[CandidateReplicaTarget]] = {
            key: list(value)
            for key, value in (replicas or {}).items()
        }

    def list_partition_targets(
        self,
        execution_plan: Mapping[str, Any],
    ) -> Sequence[CandidatePartitionTarget]:
        if self._partitions:
            return tuple(self._partitions)

        raw = execution_plan.get("partitions", ())
        targets: List[CandidatePartitionTarget] = []

        for item in raw:
            targets.append(
                CandidatePartitionTarget(
                    partition_id=str(item["partition_id"]),
                    partition_epoch=int(item.get("partition_epoch", 0)),
                    region=str(item.get("region", "unknown")),
                    zone=str(item.get("zone", "unknown")),
                    routing_version=int(item.get("routing_version", 0)),
                    state=str(item.get("state", "active")),
                    logical_index_id=item.get("logical_index_id"),
                )
            )

        return tuple(targets)

    def list_replica_targets(
        self,
        partition_id: str,
        limit: int = 16,
    ) -> Sequence[CandidateReplicaTarget]:
        return tuple(self._replicas.get(partition_id, ()))[:limit]

    def read_candidates(
        self,
        request: CandidateReadRequest,
    ) -> CandidateReadResult:
        return CandidateReadResult(
            read_id=request.read_id,
            task_id=request.task_id,
            state=CandidateReadState.COMPLETED,
            partition_id=request.partition.partition_id,
            replica_id=request.replica.replica_id,
            candidates=[],
            returned_count=0,
            exhausted=True,
            partial=False,
            completed_at=_now(),
        )


@dataclass(frozen=True)
class MultiPartitionCandidateRetrievalPolicy:
    budget: CandidateRetrievalBudget = field(
        default_factory=CandidateRetrievalBudget
    )

    minimum_replica_health: float = 0.40
    maximum_replica_load: float = 0.95

    prefer_local_region: bool = True
    prefer_same_zone: bool = True
    allow_cross_region_failover: bool = True
    allow_partial_results: bool = True

    include_term_reads: bool = True
    include_phrase_reads: bool = True
    include_field_reads: bool = True

    deduplicate_within_collection: bool = True


class MultiPartitionCandidateRetrievalArchitecture:
    """
    Phase 11.6 architecture.

    Responsibilities:
        1. Accept Phase 11.5 distributed retrieval plans.
        2. Materialize partition-aware candidate reads.
        3. Select healthy replicas.
        4. Dispatch parallel retrieval reads through a replaceable backend.
        5. Collect posting/document candidates.
        6. Preserve partial-result semantics.
        7. Build retry/failover plans.
        8. Produce a retrieval candidate set for Phase 11.7.

    Explicitly not responsible for:
        - final ranking
        - result ordering
        - semantic generation
        - crawling
        - index mutation
        - final deduplication policy
    """

    def __init__(
        self,
        backend: Optional[MultiPartitionCandidateRetrievalBackend] = None,
        policy: Optional[MultiPartitionCandidateRetrievalPolicy] = None,
    ) -> None:
        self.backend = backend or InMemoryMultiPartitionCandidateRetrievalBackend()
        self.policy = policy or MultiPartitionCandidateRetrievalPolicy()

        self._events: Dict[str, List[CandidateRetrievalEvent]] = {}
        self._checkpoints: Dict[str, List[CandidateRetrievalCheckpoint]] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def retrieve(
        self,
        request: MultiPartitionCandidateRetrievalRequest,
    ) -> MultiPartitionCandidateRetrieval:
        identity = CandidateRetrievalIdentity(
            retrieval_id=_new_id("retrieval"),
            plan_id=request.plan_id,
            query_id=request.query_id,
            canonical_query_hash=request.canonical_query_hash,
            created_at=_now(),
        )

        retrieval = MultiPartitionCandidateRetrieval(
            retrieval_id=identity.retrieval_id,
            identity=identity,
            state=CandidateRetrievalState.RECEIVED,
        )

        self._emit(
            retrieval,
            CandidateRetrievalEventType.RETRIEVAL_RECEIVED,
            {
                "plan_id": request.plan_id,
                "query_id": request.query_id,
            },
        )

        try:
            retrieval.state = CandidateRetrievalState.VALIDATING
            self._validate_request(request)

            retrieval.state = CandidateRetrievalState.MATERIALIZING_TASKS

            partitions = self.backend.list_partition_targets(
                request.execution_plan
            )

            reads = self._materialize_reads(
                retrieval=retrieval,
                request=request,
                partitions=partitions,
            )

            retrieval.reads.extend(reads)

            self._emit(
                retrieval,
                CandidateRetrievalEventType.TASKS_MATERIALIZED,
                {
                    "partition_count": len(partitions),
                    "read_count": len(reads),
                },
            )

            retrieval.state = CandidateRetrievalState.DISPATCHING

            self._emit(
                retrieval,
                CandidateRetrievalEventType.READS_DISPATCHED,
                {
                    "read_count": len(retrieval.reads),
                    "parallel_budget": (
                        self.policy.budget.max_parallel_reads
                    ),
                },
            )

            retrieval.state = CandidateRetrievalState.RETRIEVING

            self._execute_reads(retrieval)

            retrieval.state = CandidateRetrievalState.COLLECTING

            collection = self._collect_candidates(
                retrieval=retrieval,
                max_candidates=(
                    request.max_candidates
                    or self.policy.budget.max_total_candidates
                ),
            )

            retrieval.collection = collection
            retrieval.candidate_count = len(collection.documents)

            self._emit(
                retrieval,
                CandidateRetrievalEventType.CANDIDATES_COLLECTED,
                {
                    "candidate_count": retrieval.candidate_count,
                    "partial_reads": collection.reads_partial,
                    "failed_reads": collection.reads_failed,
                },
            )

            if collection.state == CandidateCollectionState.PARTIAL:
                retrieval.state = CandidateRetrievalState.PARTIAL_RESULT

                self._emit(
                    retrieval,
                    CandidateRetrievalEventType.PARTIAL_RESULTS_DETECTED,
                    {
                        "candidate_count": retrieval.candidate_count,
                    },
                )
            else:
                retrieval.state = CandidateRetrievalState.COMPLETED

            retrieval.decision = self._build_decision(retrieval)

            retrieval.retrieval_hash = _hash(
                {
                    "identity": retrieval.identity,
                    "read_ids": tuple(read.read_id for read in retrieval.reads),
                    "candidate_ids": tuple(
                        sorted(collection.documents.keys())
                    ),
                    "state": retrieval.state.value,
                }
            )

            retrieval.completed_at = _now()

            self._checkpoint(retrieval)

            self._emit(
                retrieval,
                CandidateRetrievalEventType.COLLECTION_COMPLETED,
                {
                    "candidate_count": retrieval.candidate_count,
                    "retrieval_hash": retrieval.retrieval_hash,
                },
            )

            self._emit(
                retrieval,
                CandidateRetrievalEventType.RETRIEVAL_COMPLETED,
                {
                    "candidate_count": retrieval.candidate_count,
                    "state": retrieval.state.value,
                },
            )

            return retrieval

        except Exception as exc:
            retrieval.state = CandidateRetrievalState.FAILED

            retrieval.decision = CandidateRetrievalDecision(
                retrieval_id=retrieval.retrieval_id,
                state=CandidateRetrievalState.FAILED,
                planned_reads=len(retrieval.reads),
                accepted_reads=0,
                rejected_reads=0,
                partial_reads=0,
                estimated_candidates=0,
                budget_applied=False,
                decision_reason=str(exc),
            )

            self._emit(
                retrieval,
                CandidateRetrievalEventType.RETRIEVAL_FAILED,
                {"error": str(exc)},
            )

            self._checkpoint(retrieval)

            raise

    def events(
        self,
        retrieval_id: Optional[str] = None,
    ) -> Tuple[CandidateRetrievalEvent, ...]:
        if retrieval_id is not None:
            return tuple(self._events.get(retrieval_id, ()))

        result: List[CandidateRetrievalEvent] = []

        for events in self._events.values():
            result.extend(events)

        return tuple(result)

    def checkpoints(
        self,
        retrieval_id: Optional[str] = None,
    ) -> Tuple[CandidateRetrievalCheckpoint, ...]:
        if retrieval_id is not None:
            return tuple(self._checkpoints.get(retrieval_id, ()))

        result: List[CandidateRetrievalCheckpoint] = []

        for checkpoints in self._checkpoints.values():
            result.extend(checkpoints)

        return tuple(result)

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_request(
        request: MultiPartitionCandidateRetrievalRequest,
    ) -> None:
        if not request.plan_id:
            raise ValueError("plan_id is required")

        if not request.query_id:
            raise ValueError("query_id is required")

        if not request.canonical_query_hash:
            raise ValueError("canonical_query_hash is required")

        if not isinstance(request.execution_plan, Mapping):
            raise TypeError("execution_plan must be a mapping")

    # ------------------------------------------------------------------
    # Read materialization
    # ------------------------------------------------------------------

    def _materialize_reads(
        self,
        retrieval: MultiPartitionCandidateRetrieval,
        request: MultiPartitionCandidateRetrievalRequest,
        partitions: Sequence[CandidatePartitionTarget],
    ) -> List[CandidateReadRequest]:
        terms = self._extract_terms(request.execution_plan)
        phrases = self._extract_phrases(request.execution_plan)
        fields = self._extract_fields(request.execution_plan)

        reads: List[CandidateReadRequest] = []

        for partition in partitions:
            if partition.state.lower() not in {
                "active",
                "ready",
                "serving",
                "healthy",
            }:
                continue

            replicas = self.backend.list_replica_targets(
                partition.partition_id,
                limit=16,
            )

            replica = self._select_replica(
                partition=partition,
                replicas=replicas,
                requested_region=request.requested_region,
            )

            if replica is None:
                continue

            if self.policy.include_term_reads and terms:
                reads.append(
                    self._build_read(
                        retrieval=retrieval,
                        partition=partition,
                        replica=replica,
                        mode=CandidateReadMode.TERM,
                        terms=terms,
                    )
                )

            if self.policy.include_phrase_reads and phrases:
                reads.append(
                    self._build_read(
                        retrieval=retrieval,
                        partition=partition,
                        replica=replica,
                        mode=CandidateReadMode.PHRASE,
                        phrases=phrases,
                    )
                )

            if self.policy.include_field_reads and fields:
                reads.append(
                    self._build_read(
                        retrieval=retrieval,
                        partition=partition,
                        replica=replica,
                        mode=CandidateReadMode.FIELD,
                        fields=fields,
                    )
                )

            if not terms and not phrases and not fields:
                reads.append(
                    self._build_read(
                        retrieval=retrieval,
                        partition=partition,
                        replica=replica,
                        mode=CandidateReadMode.HYBRID,
                    )
                )

            if len(reads) >= self.policy.budget.max_reads:
                break

        return reads[: self.policy.budget.max_reads]

    def _build_read(
        self,
        retrieval: MultiPartitionCandidateRetrieval,
        partition: CandidatePartitionTarget,
        replica: CandidateReplicaTarget,
        mode: CandidateReadMode,
        terms: Sequence[str] = (),
        phrases: Sequence[str] = (),
        fields: Sequence[str] = (),
    ) -> CandidateReadRequest:
        read_id = _new_id("read")
        task_id = _new_id("candidate-task")

        return CandidateReadRequest(
            read_id=read_id,
            task_id=task_id,
            partition=partition,
            replica=replica,
            mode=mode,
            terms=tuple(terms),
            phrases=tuple(phrases),
            fields=tuple(fields),
            candidate_limit=self.policy.budget.max_candidates_per_read,
            timeout_ms=self.policy.budget.timeout_ms,
        )

    # ------------------------------------------------------------------
    # Replica selection
    # ------------------------------------------------------------------

    def _select_replica(
        self,
        partition: CandidatePartitionTarget,
        replicas: Sequence[CandidateReplicaTarget],
        requested_region: Optional[str],
    ) -> Optional[CandidateReplicaTarget]:
        healthy = [
            replica
            for replica in replicas
            if replica.state.lower() in {"active", "healthy", "ready"}
            and replica.health >= self.policy.minimum_replica_health
            and replica.load <= self.policy.maximum_replica_load
        ]

        if not healthy:
            return None

        def score(replica: CandidateReplicaTarget) -> Tuple[int, int, float]:
            region_score = 0

            if requested_region and replica.region == requested_region:
                region_score = 3
            elif self.policy.prefer_local_region and (
                replica.region == partition.region
            ):
                region_score = 2

            zone_score = 1 if (
                self.policy.prefer_same_zone
                and replica.zone == partition.zone
            ) else 0

            load_score = -replica.load

            return region_score, zone_score, load_score

        return max(healthy, key=score)

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def _execute_reads(
        self,
        retrieval: MultiPartitionCandidateRetrieval,
    ) -> None:
        """
        Reference execution loop.

        Production deployments can replace this layer with a distributed
        asynchronous execution fabric. The control architecture does not
        require a single worker or a single process.
        """

        for read in retrieval.reads:
            result = self.backend.read_candidates(read)
            retrieval.results[read.read_id] = result

            if result.state in {
                CandidateReadState.FAILED,
                CandidateReadState.PARTIAL,
            }:
                failure_plan = self._build_failure_plan(
                    retrieval=retrieval,
                    read=read,
                    result=result,
                )

                retrieval.failure_plans[read.read_id] = failure_plan

                if result.state == CandidateReadState.FAILED:
                    self._emit(
                        retrieval,
                        CandidateRetrievalEventType.RETRIES_STARTED,
                        {
                            "read_id": read.read_id,
                            "actions": [
                                action.value
                                for action in failure_plan.actions
                            ],
                        },
                    )

    def _build_failure_plan(
        self,
        retrieval: MultiPartitionCandidateRetrieval,
        read: CandidateReadRequest,
        result: CandidateReadResult,
    ) -> CandidateFailurePlan:
        replicas = self.backend.list_replica_targets(
            read.partition.partition_id,
            limit=16,
        )

        fallback_replica_ids = tuple(
            replica.replica_id
            for replica in replicas
            if replica.replica_id != read.replica.replica_id
            and replica.state.lower() in {"active", "healthy", "ready"}
            and replica.health >= self.policy.minimum_replica_health
            and replica.load <= self.policy.maximum_replica_load
        )

        actions: List[CandidateFailureAction] = []

        if fallback_replica_ids:
            actions.append(CandidateFailureAction.RETRY_REPLICA)

        actions.append(CandidateFailureAction.RETRY_REGION)

        if self.policy.allow_cross_region_failover:
            actions.append(CandidateFailureAction.RETRY_CROSS_REGION)

        if self.policy.allow_partial_results:
            actions.append(CandidateFailureAction.RETURN_PARTIAL)

        return CandidateFailurePlan(
            read_id=read.read_id,
            actions=tuple(actions),
            max_attempts=self.policy.budget.max_retries,
            allow_partial=self.policy.allow_partial_results,
            fallback_replica_ids=fallback_replica_ids,
            fallback_regions=(),
        )

    # ------------------------------------------------------------------
    # Candidate collection
    # ------------------------------------------------------------------

    def _collect_candidates(
        self,
        retrieval: MultiPartitionCandidateRetrieval,
        max_candidates: int,
    ) -> CandidateCollection:
        collection = CandidateCollection(
            collection_id=_new_id("collection"),
            retrieval_id=retrieval.retrieval_id,
            state=CandidateCollectionState.COLLECTING,
        )

        seen_hits: Dict[str, CandidatePostingHit] = {}

        for read_id, result in retrieval.results.items():
            if result.state == CandidateReadState.COMPLETED:
                collection.reads_completed += 1

            elif result.state == CandidateReadState.PARTIAL:
                collection.reads_partial += 1

            elif result.state == CandidateReadState.FAILED:
                collection.reads_failed += 1

            for hit in result.candidates:
                if len(seen_hits) >= max_candidates:
                    break

                existing = seen_hits.get(hit.document_id)

                if existing is not None:
                    collection.duplicate_hits_collapsed += 1
                    seen_hits[hit.document_id] = self._merge_hits(
                        existing,
                        hit,
                    )
                else:
                    seen_hits[hit.document_id] = hit

            if len(seen_hits) >= max_candidates:
                break

        for document_id, hit in seen_hits.items():
            collection.documents[document_id] = CandidateDocument(
                document_id=document_id,
                partition_id=hit.partition_id,
                source_reads=tuple(
                    read_id
                    for read_id, result in retrieval.results.items()
                    if any(
                        candidate.document_id == document_id
                        for candidate in result.candidates
                    )
                ),
                matched_terms=(
                    (hit.term,) if hit.term is not None else ()
                ),
                matched_fields=(
                    (hit.field,) if hit.field is not None else ()
                ),
                matched_phrases=(
                    (hit.phrase,) if hit.phrase is not None else ()
                ),
                state=CandidateDocumentState.DISCOVERED,
                retrieval_evidence={
                    "frequency": hit.frequency,
                    "positions_available": hit.positions_available,
                    "source": hit.source.value,
                },
            )

        if collection.reads_failed > 0 or collection.reads_partial > 0:
            if collection.documents and self.policy.allow_partial_results:
                collection.state = CandidateCollectionState.PARTIAL
            elif collection.reads_failed > 0:
                collection.state = CandidateCollectionState.FAILED
            else:
                collection.state = CandidateCollectionState.PARTIAL
        else:
            collection.state = CandidateCollectionState.COMPLETE

        collection.completed_at = _now()

        return collection

    @staticmethod
    def _merge_hits(
        left: CandidatePostingHit,
        right: CandidatePostingHit,
    ) -> CandidatePostingHit:
        terms = [
            value
            for value in (left.term, right.term)
            if value is not None
        ]

        return CandidatePostingHit(
            document_id=left.document_id,
            partition_id=left.partition_id,
            term=terms[0] if terms else None,
            field=left.field or right.field,
            phrase=left.phrase or right.phrase,
            frequency=max(
                left.frequency or 0,
                right.frequency or 0,
            ),
            positions_available=(
                left.positions_available
                or right.positions_available
            ),
            source=left.source,
            metadata={
                **dict(left.metadata),
                **dict(right.metadata),
            },
        )

    # ------------------------------------------------------------------
    # Execution-plan extraction
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_terms(
        execution_plan: Mapping[str, Any],
    ) -> Tuple[str, ...]:
        values = execution_plan.get("terms", ())
        return tuple(
            str(value)
            for value in values
            if str(value).strip()
        )

    @staticmethod
    def _extract_phrases(
        execution_plan: Mapping[str, Any],
    ) -> Tuple[str, ...]:
        values = execution_plan.get("phrases", ())
        return tuple(
            str(value)
            for value in values
            if str(value).strip()
        )

    @staticmethod
    def _extract_fields(
        execution_plan: Mapping[str, Any],
    ) -> Tuple[str, ...]:
        values = execution_plan.get("fields", ())
        return tuple(
            str(value)
            for value in values
            if str(value).strip()
        )

    # ------------------------------------------------------------------
    # Decision / checkpoint / events
    # ------------------------------------------------------------------

    def _build_decision(
        self,
        retrieval: MultiPartitionCandidateRetrieval,
    ) -> CandidateRetrievalDecision:
        accepted = sum(
            1
            for result in retrieval.results.values()
            if result.state == CandidateReadState.COMPLETED
        )

        partial = sum(
            1
            for result in retrieval.results.values()
            if result.state == CandidateReadState.PARTIAL
        )

        rejected = sum(
            1
            for result in retrieval.results.values()
            if result.state == CandidateReadState.FAILED
        )

        return CandidateRetrievalDecision(
            retrieval_id=retrieval.retrieval_id,
            state=retrieval.state,
            planned_reads=len(retrieval.reads),
            accepted_reads=accepted,
            rejected_reads=rejected,
            partial_reads=partial,
            estimated_candidates=retrieval.candidate_count,
            budget_applied=(
                len(retrieval.reads)
                >= self.policy.budget.max_reads
            ),
            decision_reason=(
                "candidate retrieval completed"
                if retrieval.state == CandidateRetrievalState.COMPLETED
                else "candidate retrieval returned partial results"
            ),
        )

    def _checkpoint(
        self,
        retrieval: MultiPartitionCandidateRetrieval,
    ) -> None:
        completed = tuple(
            read_id
            for read_id, result in retrieval.results.items()
            if result.state == CandidateReadState.COMPLETED
        )

        failed = tuple(
            read_id
            for read_id, result in retrieval.results.items()
            if result.state == CandidateReadState.FAILED
        )

        checkpoint = CandidateRetrievalCheckpoint(
            checkpoint_id=_new_id("candidate-checkpoint"),
            retrieval_id=retrieval.retrieval_id,
            state=retrieval.state,
            read_ids=tuple(
                read.read_id
                for read in retrieval.reads
            ),
            completed_read_ids=completed,
            failed_read_ids=failed,
            candidate_count=retrieval.candidate_count,
            collection_state=(
                retrieval.collection.state
                if retrieval.collection is not None
                else None
            ),
        )

        self._checkpoints.setdefault(
            retrieval.retrieval_id,
            [],
        ).append(checkpoint)

        self._emit(
            retrieval,
            CandidateRetrievalEventType.CHECKPOINT_CREATED,
            {
                "checkpoint_id": checkpoint.checkpoint_id,
                "candidate_count": retrieval.candidate_count,
            },
        )

    def _emit(
        self,
        retrieval: MultiPartitionCandidateRetrieval,
        event_type: CandidateRetrievalEventType,
        payload: Mapping[str, Any],
    ) -> None:
        event = CandidateRetrievalEvent(
            event_id=_new_id("candidate-event"),
            retrieval_id=retrieval.retrieval_id,
            event_type=event_type,
            state=retrieval.state,
            timestamp=_now(),
            payload=dict(payload),
        )

        self._events.setdefault(
            retrieval.retrieval_id,
            [],
        ).append(event)

    # ------------------------------------------------------------------
    # Architecture declaration
    # ------------------------------------------------------------------

    @staticmethod
    def architecture() -> Mapping[str, Any]:
        return {
            "architecture_version": ARCHITECTURE_VERSION,
            "scale_target": SCALE_TARGET,
            "google_scale_capability_target": GOOGLE_SCALE_CAPABILITY_TARGET,
            "google_technology_dependency": GOOGLE_TECHNOLOGY_DEPENDENCY,
            "stage": "11.6",
            "name": "Multi-Partition Candidate Retrieval",
            "input": "Phase 11.5 distributed retrieval plan",
            "output": "distributed candidate retrieval set",
            "next_stage": "11.7 Candidate Fusion & Deduplication",
            "data_flow": [
                "11.5 retrieval plan",
                "partition target materialization",
                "healthy replica selection",
                "parallel candidate reads",
                "posting/document candidate collection",
                "partial result handling",
                "retry and failover planning",
                "candidate set assembly",
                "11.7 candidate fusion and deduplication",
            ],
            "distributed_properties": [
                "partition-aware",
                "replica-aware",
                "region-aware",
                "zone-aware",
                "parallelizable",
                "failure-aware",
                "partial-result tolerant",
                "checkpointable",
                "backend-replaceable",
                "horizontally scalable",
            ],
            "no_fixed_global_limits": [
                "documents",
                "terms",
                "partitions",
                "candidates",
                "workers",
                "replicas",
                "regions",
            ],
            "operational_budgets_are": [
                "per-request",
                "per-execution",
                "per-retrieval-plan",
                "safety controls",
            ],
            "not_responsible_for": [
                "final ranking",
                "ranking features",
                "final result ordering",
                "final deduplication policy",
                "Web crawling",
                "index mutation",
                "embedding generation",
            ],
            "phase_10_dependencies": [
                "distributed inverted-index segment fabric",
                "partition-aware index routing",
                "index recovery/checkpointing",
                "replication and durability",
                "storage capacity and tiering",
            ],
            "protected_components": [
                "website_server.py",
                "search_service/server.py",
            ],
            "dependency_policy": {
                "google_search_api": False,
                "google_index": False,
                "google_crawler": False,
                "google_infrastructure": False,
                "google_search_technology": False,
            },
        }


# Compatibility aliases.

MultiPartitionCandidateRetrieval = MultiPartitionCandidateRetrievalArchitecture
GlobalMultiPartitionCandidateRetrieval = (
    MultiPartitionCandidateRetrievalArchitecture
)
Phase11_6MultiPartitionCandidateRetrieval = (
    MultiPartitionCandidateRetrievalArchitecture
)
