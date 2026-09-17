"""
OUR SEARCH
Phase 11.5 — Distributed Retrieval Planning

Architecture version:
    distributed-retrieval-planning.v1

Purpose:
    Convert the retrieval expansion contract produced by Phase 11.4
    into a distributed, partition-aware retrieval execution plan.

Scale target:
    Billions -> potentially trillions of publicly accessible Web resources.

This module does NOT:
    - execute retrieval
    - rank documents
    - mutate the index
    - crawl the Web
    - generate embeddings
    - select final search results

It creates the distributed execution plan that later retrieval workers execute.

Architecture:

    11.4 Retrieval Expansion Contract
            ↓
    Query Planning
            ↓
    Partition Routing
            ↓
    Term / Field / Phrase Routing
            ↓
    Replica Selection
            ↓
    Region / Zone Locality
            ↓
    Parallel Retrieval Tasks
            ↓
    Failure / Retry / Partial Result Plan
            ↓
    Retrieval Execution
            ↓
    Phase 12 Ranking
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from hashlib import sha256
from typing import Any, Dict, List, Mapping, Optional, Protocol, Sequence, Tuple
import json
import uuid


ARCHITECTURE_VERSION = "distributed-retrieval-planning.v1"

SCALE_TARGET = "billions_to_trillions_of_public_web_resources"
GOOGLE_SCALE_CAPABILITY_TARGET = True
GOOGLE_TECHNOLOGY_DEPENDENCY = False


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def _hash(value: Any) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )
    return sha256(payload.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class PlanningState(str, Enum):
    RECEIVED = "received"
    VALIDATING = "validating"
    PARTITION_DISCOVERY = "partition_discovery"
    ROUTE_CONSTRUCTION = "route_construction"
    REPLICA_SELECTION = "replica_selection"
    TASK_PLANNING = "task_planning"
    FANOUT_CONTROL = "fanout_control"
    FAILURE_PLANNING = "failure_planning"
    COMPLETED = "completed"
    REJECTED = "rejected"


class RoutingMode(str, Enum):
    TERM = "term"
    FIELD = "field"
    PHRASE = "phrase"
    DOCUMENT = "document"
    BROADCAST = "broadcast"
    HYBRID = "hybrid"


class RetrievalTaskState(str, Enum):
    PLANNED = "planned"
    READY = "ready"
    DISPATCHED = "dispatched"
    RUNNING = "running"
    COMPLETED = "completed"
    PARTIAL = "partial"
    FAILED = "failed"
    RETRYING = "retrying"
    CANCELLED = "cancelled"


class ReplicaState(str, Enum):
    ACTIVE = "active"
    DEGRADED = "degraded"
    DRAINING = "draining"
    FAILED = "failed"
    UNKNOWN = "unknown"


class ReplicaPreference(str, Enum):
    LOCAL = "local"
    SAME_ZONE = "same_zone"
    SAME_REGION = "same_region"
    CROSS_REGION = "cross_region"
    ANY_HEALTHY = "any_healthy"


class FailureAction(str, Enum):
    RETRY_SAME_REPLICA = "retry_same_replica"
    RETRY_ALTERNATE_REPLICA = "retry_alternate_replica"
    RETRY_SAME_REGION = "retry_same_region"
    RETRY_CROSS_REGION = "retry_cross_region"
    RETURN_PARTIAL = "return_partial"
    ABORT = "abort"


class FanoutState(str, Enum):
    WITHIN_BUDGET = "within_budget"
    REDUCED = "reduced"
    CAPPED = "capped"
    REJECTED = "rejected"


class PlanningEventType(str, Enum):
    PLAN_RECEIVED = "plan_received"
    PLAN_STARTED = "plan_started"
    PARTITIONS_DISCOVERED = "partitions_discovered"
    ROUTES_CONSTRUCTED = "routes_constructed"
    REPLICAS_SELECTED = "replicas_selected"
    TASKS_CREATED = "tasks_created"
    FANOUT_CONTROLLED = "fanout_controlled"
    FAILURE_PLAN_CREATED = "failure_plan_created"
    PLAN_COMPLETED = "plan_completed"
    PLAN_REJECTED = "plan_rejected"
    CHECKPOINT_CREATED = "checkpoint_created"


# ---------------------------------------------------------------------------
# Core identities
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RetrievalPlanningIdentity:
    plan_id: str
    query_id: str
    expansion_contract_hash: str
    architecture_version: str = ARCHITECTURE_VERSION


@dataclass(frozen=True)
class IndexPartitionTarget:
    partition_id: str

    region: str
    zone: str

    routing_key: str

    health_score: float = 1.0

    active: bool = True

    metadata: Mapping[str, Any] = field(
        default_factory=dict
    )


@dataclass(frozen=True)
class IndexReplicaTarget:
    replica_id: str
    partition_id: str

    region: str
    zone: str

    state: ReplicaState = ReplicaState.ACTIVE

    health_score: float = 1.0

    load_score: float = 0.0

    preference: ReplicaPreference = ReplicaPreference.ANY_HEALTHY

    metadata: Mapping[str, Any] = field(
        default_factory=dict
    )


@dataclass(frozen=True)
class RetrievalRoute:
    route_id: str

    source_type: str
    source_id: str

    partition_id: str

    routing_mode: RoutingMode

    field: Optional[str] = None

    term: Optional[str] = None

    phrase: Optional[str] = None

    priority: int = 0

    route_weight: float = 1.0

    metadata: Mapping[str, Any] = field(
        default_factory=dict
    )


@dataclass(frozen=True)
class ReplicaSelection:
    selection_id: str

    route_id: str

    replica_id: str
    partition_id: str

    preference: ReplicaPreference

    score: float

    selected_region: str
    selected_zone: str

    fallback_replica_ids: Tuple[str, ...] = ()

    metadata: Mapping[str, Any] = field(
        default_factory=dict
    )


@dataclass(frozen=True)
class RetrievalTask:
    task_id: str

    route_id: str
    selection_id: str

    partition_id: str
    replica_id: str

    routing_mode: RoutingMode

    state: RetrievalTaskState = RetrievalTaskState.PLANNED

    term: Optional[str] = None
    field: Optional[str] = None
    phrase: Optional[str] = None

    priority: int = 0

    timeout_ms: int = 1500

    max_retries: int = 2

    metadata: Mapping[str, Any] = field(
        default_factory=dict
    )


@dataclass(frozen=True)
class RetrievalFailurePlan:
    task_id: str

    actions: Tuple[FailureAction, ...]

    max_attempts: int

    allow_partial_result: bool

    fallback_replica_ids: Tuple[str, ...]

    fallback_regions: Tuple[str, ...]

    metadata: Mapping[str, Any] = field(
        default_factory=dict
    )


# ---------------------------------------------------------------------------
# Fanout
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RetrievalFanoutBudget:
    max_partitions: int
    max_routes: int
    max_tasks: int

    max_tasks_per_term: int
    max_tasks_per_field: int

    max_total_retries: int

    max_parallelism: int


@dataclass(frozen=True)
class RetrievalFanoutDecision:
    state: FanoutState

    candidate_partition_count: int
    accepted_partition_count: int

    candidate_route_count: int
    accepted_route_count: int

    candidate_task_count: int
    accepted_task_count: int

    rejected_count: int

    budget: RetrievalFanoutBudget

    reasons: Tuple[str, ...]

    fingerprint: str


# ---------------------------------------------------------------------------
# Parallel execution groups
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RetrievalExecutionGroup:
    group_id: str

    region: str
    zone: str

    task_ids: Tuple[str, ...]

    max_parallelism: int

    locality_score: float

    metadata: Mapping[str, Any] = field(
        default_factory=dict
    )


# ---------------------------------------------------------------------------
# Final retrieval plan
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DistributedRetrievalPlan:
    identity: RetrievalPlanningIdentity

    query_id: str

    routing_mode: RoutingMode

    state: PlanningState

    partitions: Tuple[IndexPartitionTarget, ...]
    routes: Tuple[RetrievalRoute, ...]
    selections: Tuple[ReplicaSelection, ...]
    tasks: Tuple[RetrievalTask, ...]

    failure_plans: Tuple[RetrievalFailurePlan, ...]

    execution_groups: Tuple[RetrievalExecutionGroup, ...]

    fanout: RetrievalFanoutDecision

    execution_contract: Mapping[str, Any]

    created_at: str
    completed_at: str

    plan_hash: str


@dataclass(frozen=True)
class RetrievalPlanningCheckpoint:
    checkpoint_id: str

    plan_id: str

    state: PlanningState

    epoch: int

    plan_hash: str

    task_count: int
    partition_count: int

    created_at: str


@dataclass(frozen=True)
class RetrievalPlanningEvent:
    event_id: str

    plan_id: str

    event_type: PlanningEventType

    state: PlanningState

    timestamp: str

    payload: Mapping[str, Any]


@dataclass(frozen=True)
class RetrievalPlanningRequest:
    expansion_contract: Mapping[str, Any]

    routing_mode: RoutingMode = RoutingMode.HYBRID

    preferred_region: Optional[str] = None
    preferred_zone: Optional[str] = None

    budget: Optional[RetrievalFanoutBudget] = None

    metadata: Mapping[str, Any] = field(
        default_factory=dict
    )


# ---------------------------------------------------------------------------
# Backend
# ---------------------------------------------------------------------------

class DistributedRetrievalPlanningBackend(Protocol):
    def locate_partitions(
        self,
        *,
        term: Optional[str],
        field: Optional[str],
        phrase: Optional[str],
        routing_mode: RoutingMode,
        limit: int,
    ) -> Sequence[IndexPartitionTarget]:
        ...

    def list_replicas(
        self,
        partition_id: str,
        *,
        limit: int,
    ) -> Sequence[IndexReplicaTarget]:
        ...


class InMemoryDistributedRetrievalPlanningBackend:
    """
    Reference metadata backend.

    Production deployments should use a distributed partition-routing
    fabric and live replica-health/load metadata.
    """

    def __init__(
        self,
        partitions: Optional[
            Sequence[IndexPartitionTarget]
        ] = None,
        replicas: Optional[
            Sequence[IndexReplicaTarget]
        ] = None,
    ) -> None:
        self._partitions = tuple(
            partitions or ()
        )

        self._replicas = tuple(
            replicas or ()
        )

    def locate_partitions(
        self,
        *,
        term: Optional[str],
        field: Optional[str],
        phrase: Optional[str],
        routing_mode: RoutingMode,
        limit: int,
    ) -> Sequence[IndexPartitionTarget]:
        active = [
            partition
            for partition in self._partitions
            if partition.active
        ]

        return sorted(
            active,
            key=lambda partition: (
                -partition.health_score,
                partition.partition_id,
            ),
        )[:limit]

    def list_replicas(
        self,
        partition_id: str,
        *,
        limit: int,
    ) -> Sequence[IndexReplicaTarget]:
        values = [
            replica
            for replica in self._replicas
            if replica.partition_id == partition_id
        ]

        return sorted(
            values,
            key=lambda replica: (
                replica.state != ReplicaState.ACTIVE,
                -replica.health_score,
                replica.load_score,
                replica.replica_id,
            ),
        )[:limit]


# ---------------------------------------------------------------------------
# Policy
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DistributedRetrievalPlanningPolicy:
    default_budget: RetrievalFanoutBudget = field(
        default_factory=lambda: RetrievalFanoutBudget(
            max_partitions=256,
            max_routes=512,
            max_tasks=512,
            max_tasks_per_term=32,
            max_tasks_per_field=64,
            max_total_retries=1024,
            max_parallelism=64,
        )
    )

    minimum_replica_health: float = 0.40

    prefer_local_replica: bool = True
    prefer_same_zone: bool = True
    prefer_same_region: bool = True

    allow_cross_region_failover: bool = True

    allow_partial_results: bool = True

    default_timeout_ms: int = 1500
    default_retries: int = 2


# ---------------------------------------------------------------------------
# Main architecture
# ---------------------------------------------------------------------------

class DistributedRetrievalPlanning:
    """
    Phase 11.5 distributed retrieval planner.

    Converts a Phase 11.4 retrieval expansion contract into a distributed
    execution plan.

    The planner is intentionally separated from actual retrieval execution.
    """

    def __init__(
        self,
        backend: Optional[
            DistributedRetrievalPlanningBackend
        ] = None,
        policy: Optional[
            DistributedRetrievalPlanningPolicy
        ] = None,
    ) -> None:
        self.backend = (
            backend
            or InMemoryDistributedRetrievalPlanningBackend()
        )

        self.policy = (
            policy
            or DistributedRetrievalPlanningPolicy()
        )

        self._events: List[
            RetrievalPlanningEvent
        ] = []

        self._checkpoints: Dict[
            str,
            RetrievalPlanningCheckpoint,
        ] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def plan(
        self,
        request: RetrievalPlanningRequest,
    ) -> DistributedRetrievalPlan:
        created_at = _now()

        contract = dict(
            request.expansion_contract
        )

        query_id = self._query_id(
            contract
        )

        contract_hash = self._contract_hash(
            contract
        )

        identity = RetrievalPlanningIdentity(
            plan_id=_new_id("retrieval_plan"),
            query_id=query_id,
            expansion_contract_hash=contract_hash,
        )

        self._emit(
            identity,
            PlanningEventType.PLAN_RECEIVED,
            PlanningState.RECEIVED,
            {
                "query_id": query_id,
                "expansion_contract_hash": contract_hash,
            },
        )

        if not self._validate(contract):
            self._emit(
                identity,
                PlanningEventType.PLAN_REJECTED,
                PlanningState.REJECTED,
                {
                    "reason": (
                        "invalid retrieval expansion contract"
                    )
                },
            )

            raise ValueError(
                "Invalid retrieval expansion contract"
            )

        self._emit(
            identity,
            PlanningEventType.PLAN_STARTED,
            PlanningState.VALIDATING,
            {},
        )

        budget = (
            request.budget
            or self.policy.default_budget
        )

        terms = self._extract_terms(
            contract
        )

        phrases = self._extract_phrases(
            contract
        )

        fields = self._extract_fields(
            contract
        )

        partitions = self._discover_partitions(
            terms=terms,
            phrases=phrases,
            fields=fields,
            routing_mode=request.routing_mode,
            budget=budget,
        )

        self._emit(
            identity,
            PlanningEventType.PARTITIONS_DISCOVERED,
            PlanningState.PARTITION_DISCOVERY,
            {
                "partition_count": len(partitions),
            },
        )

        routes = self._construct_routes(
            partitions=partitions,
            terms=terms,
            phrases=phrases,
            fields=fields,
            routing_mode=request.routing_mode,
            budget=budget,
        )

        self._emit(
            identity,
            PlanningEventType.ROUTES_CONSTRUCTED,
            PlanningState.ROUTE_CONSTRUCTION,
            {
                "route_count": len(routes),
            },
        )

        selections = self._select_replicas(
            routes=routes,
            preferred_region=request.preferred_region,
            preferred_zone=request.preferred_zone,
            budget=budget,
        )

        self._emit(
            identity,
            PlanningEventType.REPLICAS_SELECTED,
            PlanningState.REPLICA_SELECTION,
            {
                "selection_count": len(selections),
            },
        )

        tasks = self._build_tasks(
            routes=routes,
            selections=selections,
            budget=budget,
        )

        self._emit(
            identity,
            PlanningEventType.TASKS_CREATED,
            PlanningState.TASK_PLANNING,
            {
                "task_count": len(tasks),
            },
        )

        fanout = self._control_fanout(
            partitions=partitions,
            routes=routes,
            tasks=tasks,
            budget=budget,
        )

        self._emit(
            identity,
            PlanningEventType.FANOUT_CONTROLLED,
            PlanningState.FANOUT_CONTROL,
            {
                "state": fanout.state.value,
                "accepted_tasks": (
                    fanout.accepted_task_count
                ),
                "rejected": fanout.rejected_count,
            },
        )

        if fanout.state == FanoutState.REJECTED:
            self._emit(
                identity,
                PlanningEventType.PLAN_REJECTED,
                PlanningState.REJECTED,
                {
                    "reason": "retrieval fanout rejected"
                },
            )

            raise ValueError(
                "Retrieval fanout rejected"
            )

        tasks = self._apply_task_cap(
            tasks,
            budget,
        )

        failure_plans = self._build_failure_plans(
            tasks=tasks,
            selections=selections,
            budget=budget,
        )

        self._emit(
            identity,
            PlanningEventType.FAILURE_PLAN_CREATED,
            PlanningState.FAILURE_PLANNING,
            {
                "failure_plan_count": len(
                    failure_plans
                ),
            },
        )

        execution_groups = (
            self._build_execution_groups(
                tasks,
                budget,
            )
        )

        execution_contract = (
            self._build_execution_contract(
                request=request,
                terms=terms,
                phrases=phrases,
                fields=fields,
                partitions=partitions,
                routes=routes,
                selections=selections,
                tasks=tasks,
                execution_groups=execution_groups,
                fanout=fanout,
            )
        )

        completed_at = _now()

        plan_hash = _hash(
            {
                "query_id": query_id,
                "expansion_contract_hash": contract_hash,
                "routing_mode": (
                    request.routing_mode.value
                ),
                "partitions": [
                    partition.partition_id
                    for partition in partitions
                ],
                "routes": [
                    {
                        "source_id": route.source_id,
                        "partition_id": route.partition_id,
                        "routing_mode": (
                            route.routing_mode.value
                        ),
                        "field": route.field,
                        "term": route.term,
                        "phrase": route.phrase,
                    }
                    for route in routes
                ],
                "selections": [
                    {
                        "route_id": selection.route_id,
                        "replica_id": selection.replica_id,
                    }
                    for selection in selections
                ],
                "tasks": [
                    {
                        "partition_id": task.partition_id,
                        "replica_id": task.replica_id,
                        "routing_mode": (
                            task.routing_mode.value
                        ),
                    }
                    for task in tasks
                ],
                "fanout": fanout.fingerprint,
                "execution_groups": [
                    group.group_id
                    for group in execution_groups
                ],
            }
        )

        result = DistributedRetrievalPlan(
            identity=identity,
            query_id=query_id,
            routing_mode=request.routing_mode,
            state=PlanningState.COMPLETED,
            partitions=tuple(partitions),
            routes=tuple(routes),
            selections=tuple(selections),
            tasks=tuple(tasks),
            failure_plans=tuple(failure_plans),
            execution_groups=tuple(
                execution_groups
            ),
            fanout=fanout,
            execution_contract=execution_contract,
            created_at=created_at,
            completed_at=completed_at,
            plan_hash=plan_hash,
        )

        self._create_checkpoint(
            result,
            epoch=0,
        )

        self._emit(
            identity,
            PlanningEventType.CHECKPOINT_CREATED,
            PlanningState.COMPLETED,
            {
                "plan_hash": plan_hash,
            },
        )

        self._emit(
            identity,
            PlanningEventType.PLAN_COMPLETED,
            PlanningState.COMPLETED,
            {
                "plan_hash": plan_hash,
                "task_count": len(tasks),
                "partition_count": len(partitions),
            },
        )

        return result

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate(
        self,
        contract: Mapping[str, Any],
    ) -> bool:
        if not contract:
            return False

        query_id = self._query_id(
            contract
        )

        if not query_id:
            return False

        return bool(
            contract.get("term_groups")
            or contract.get("exact_terms")
            or contract.get("phrases")
            or contract.get("field_targets")
        )

    # ------------------------------------------------------------------
    # Contract helpers
    # ------------------------------------------------------------------

    def _query_id(
        self,
        contract: Mapping[str, Any],
    ) -> str:
        value = contract.get(
            "query_id"
        )

        if value:
            return str(value)

        identity = contract.get(
            "identity"
        )

        if isinstance(identity, Mapping):
            value = identity.get(
                "query_id"
            )

            if value:
                return str(value)

        return _hash(
            {
                "exact_terms": contract.get(
                    "exact_terms",
                    [],
                ),
                "term_groups": contract.get(
                    "term_groups",
                    [],
                ),
                "phrases": contract.get(
                    "phrases",
                    [],
                ),
            }
        )

    def _contract_hash(
        self,
        contract: Mapping[str, Any],
    ) -> str:
        value = contract.get(
            "contract_hash"
        )

        if value:
            return str(value)

        return _hash(
            {
                "query_id": self._query_id(
                    contract
                ),
                "exact_terms": contract.get(
                    "exact_terms",
                    [],
                ),
                "term_groups": contract.get(
                    "term_groups",
                    [],
                ),
                "phrases": contract.get(
                    "phrases",
                    [],
                ),
                "field_targets": contract.get(
                    "field_targets",
                    [],
                ),
            }
        )

    # ------------------------------------------------------------------
    # Extraction
    # ------------------------------------------------------------------

    def _extract_terms(
        self,
        contract: Mapping[str, Any],
    ) -> Tuple[Mapping[str, Any], ...]:
        result: List[Mapping[str, Any]] = []

        exact_terms = contract.get(
            "exact_terms",
            [],
        ) or []

        for term in exact_terms:
            if isinstance(term, Mapping):
                result.append(term)

        groups = contract.get(
            "term_groups",
            [],
        ) or []

        for group in groups:
            if not isinstance(group, Mapping):
                continue

            terms = group.get(
                "terms",
                [],
            ) or []

            for term in terms:
                if isinstance(term, Mapping):
                    result.append(term)

        return tuple(
            result
        )

    def _extract_phrases(
        self,
        contract: Mapping[str, Any],
    ) -> Tuple[Mapping[str, Any], ...]:
        values = contract.get(
            "phrases",
            [],
        ) or []

        return tuple(
            value
            for value in values
            if isinstance(value, Mapping)
        )

    def _extract_fields(
        self,
        contract: Mapping[str, Any],
    ) -> Tuple[Mapping[str, Any], ...]:
        values = contract.get(
            "field_targets",
            [],
        ) or []

        return tuple(
            value
            for value in values
            if isinstance(value, Mapping)
        )

    # ------------------------------------------------------------------
    # Partition discovery
    # ------------------------------------------------------------------

    def _discover_partitions(
        self,
        *,
        terms: Sequence[Mapping[str, Any]],
        phrases: Sequence[Mapping[str, Any]],
        fields: Sequence[Mapping[str, Any]],
        routing_mode: RoutingMode,
        budget: RetrievalFanoutBudget,
    ) -> List[IndexPartitionTarget]:
        discovered: Dict[
            str,
            IndexPartitionTarget,
        ] = {}

        sources: List[
            Tuple[
                Optional[str],
                Optional[str],
                Optional[str],
            ]
        ] = []

        for term in terms:
            sources.append(
                (
                    str(
                        term.get(
                            "normalized_text",
                            term.get(
                                "text",
                                "",
                            ),
                        )
                    ),
                    None,
                    None,
                )
            )

        for phrase in phrases:
            sources.append(
                (
                    None,
                    None,
                    str(
                        phrase.get(
                            "normalized_text",
                            phrase.get(
                                "text",
                                "",
                            ),
                        )
                    ),
                )
            )

        for target in fields:
            field_value = target.get(
                "field"
            )

            if field_value:
                sources.append(
                    (
                        None,
                        str(field_value),
                        None,
                    )
                )

        if not sources:
            sources.append(
                (None, None, None)
            )

        for term, field_value, phrase in sources:
            values = self.backend.locate_partitions(
                term=term,
                field=field_value,
                phrase=phrase,
                routing_mode=routing_mode,
                limit=budget.max_partitions,
            )

            for partition in values:
                if not partition.active:
                    continue

                existing = discovered.get(
                    partition.partition_id
                )

                if existing is None:
                    discovered[
                        partition.partition_id
                    ] = partition

        ordered = sorted(
            discovered.values(),
            key=lambda partition: (
                -partition.health_score,
                partition.region,
                partition.zone,
                partition.partition_id,
            ),
        )

        return ordered[
            : budget.max_partitions
        ]

    # ------------------------------------------------------------------
    # Route construction
    # ------------------------------------------------------------------

    def _construct_routes(
        self,
        *,
        partitions: Sequence[IndexPartitionTarget],
        terms: Sequence[Mapping[str, Any]],
        phrases: Sequence[Mapping[str, Any]],
        fields: Sequence[Mapping[str, Any]],
        routing_mode: RoutingMode,
        budget: RetrievalFanoutBudget,
    ) -> List[RetrievalRoute]:
        routes: List[RetrievalRoute] = []

        partition_ids = {
            partition.partition_id
            for partition in partitions
        }

        # Term routes
        for term in terms:
            normalized = str(
                term.get(
                    "normalized_text",
                    term.get(
                        "text",
                        "",
                    ),
                )
            ).strip()

            if not normalized:
                continue

            field_value = term.get(
                "field"
            )

            for partition in partitions:
                if partition.partition_id not in partition_ids:
                    continue

                route_mode = (
                    RoutingMode.FIELD
                    if field_value
                    else RoutingMode.TERM
                )

                routes.append(
                    RetrievalRoute(
                        route_id=_new_id(
                            "route"
                        ),
                        source_type="term",
                        source_id=str(
                            term.get(
                                "term_id",
                                normalized,
                            )
                        ),
                        partition_id=partition.partition_id,
                        routing_mode=route_mode,
                        field=(
                            str(field_value)
                            if field_value
                            else None
                        ),
                        term=normalized,
                        priority=(
                            100
                            if term.get(
                                "exact_preserved",
                                False,
                            )
                            else 50
                        ),
                        route_weight=float(
                            term.get(
                                "confidence",
                                1.0,
                            )
                        ),
                        metadata={
                            "architecture_stage": "11.5",
                        },
                    )
                )

                if len(routes) >= budget.max_routes:
                    return routes

        # Phrase routes
        for phrase in phrases:
            text = str(
                phrase.get(
                    "normalized_text",
                    phrase.get(
                        "text",
                        "",
                    ),
                )
            ).strip()

            if not text:
                continue

            for partition in partitions:
                routes.append(
                    RetrievalRoute(
                        route_id=_new_id(
                            "route"
                        ),
                        source_type="phrase",
                        source_id=str(
                            phrase.get(
                                "phrase_id",
                                text,
                            )
                        ),
                        partition_id=partition.partition_id,
                        routing_mode=RoutingMode.PHRASE,
                        phrase=text,
                        priority=90,
                        route_weight=float(
                            phrase.get(
                                "confidence",
                                1.0,
                            )
                        ),
                        metadata={
                            "architecture_stage": "11.5",
                        },
                    )
                )

                if len(routes) >= budget.max_routes:
                    return routes

        # Field-only routes
        for target in fields:
            field_value = target.get(
                "field"
            )

            if not field_value:
                continue

            source_id = str(
                target.get(
                    "source_term_id",
                    target.get(
                        "source_phrase_id",
                        field_value,
                    ),
                )
            )

            for partition in partitions:
                routes.append(
                    RetrievalRoute(
                        route_id=_new_id(
                            "route"
                        ),
                        source_type="field",
                        source_id=source_id,
                        partition_id=partition.partition_id,
                        routing_mode=RoutingMode.FIELD,
                        field=str(
                            field_value
                        ),
                        priority=70,
                        route_weight=float(
                            target.get(
                                "weight",
                                0.60,
                            )
                        ),
                        metadata={
                            "architecture_stage": "11.5",
                        },
                    )
                )

                if len(routes) >= budget.max_routes:
                    return routes

        return routes[
            : budget.max_routes
        ]

    # ------------------------------------------------------------------
    # Replica selection
    # ------------------------------------------------------------------

    def _select_replicas(
        self,
        *,
        routes: Sequence[RetrievalRoute],
        preferred_region: Optional[str],
        preferred_zone: Optional[str],
        budget: RetrievalFanoutBudget,
    ) -> List[ReplicaSelection]:
        result: List[
            ReplicaSelection
        ] = []

        for route in routes:
            replicas = self.backend.list_replicas(
                route.partition_id,
                limit=16,
            )

            healthy = [
                replica
                for replica in replicas
                if replica.state
                in (
                    ReplicaState.ACTIVE,
                    ReplicaState.DEGRADED,
                )
                and replica.health_score
                >= self.policy.minimum_replica_health
            ]

            if not healthy:
                continue

            scored = sorted(
                healthy,
                key=lambda replica: (
                    self._replica_score(
                        replica,
                        preferred_region,
                        preferred_zone,
                    ),
                    replica.replica_id,
                ),
                reverse=True,
            )

            primary = scored[0]

            fallbacks = tuple(
                replica.replica_id
                for replica in scored[1:5]
            )

            score = self._replica_score(
                primary,
                preferred_region,
                preferred_zone,
            )

            result.append(
                ReplicaSelection(
                    selection_id=_new_id(
                        "selection"
                    ),
                    route_id=route.route_id,
                    replica_id=primary.replica_id,
                    partition_id=route.partition_id,
                    preference=(
                        primary.preference
                    ),
                    score=score,
                    selected_region=primary.region,
                    selected_zone=primary.zone,
                    fallback_replica_ids=fallbacks,
                    metadata={
                        "architecture_stage": "11.5",
                    },
                )
            )

            if len(result) >= budget.max_tasks:
                break

        return result

    def _replica_score(
        self,
        replica: IndexReplicaTarget,
        preferred_region: Optional[str],
        preferred_zone: Optional[str],
    ) -> float:
        score = (
            replica.health_score * 0.60
            + (1.0 - replica.load_score) * 0.25
        )

        if (
            preferred_region
            and replica.region == preferred_region
        ):
            score += 0.10

        if (
            preferred_zone
            and replica.zone == preferred_zone
        ):
            score += 0.05

        if (
            self.policy.prefer_local_replica
            and replica.preference
            == ReplicaPreference.LOCAL
        ):
            score += 0.05

        return score

    # ------------------------------------------------------------------
    # Task construction
    # ------------------------------------------------------------------

    def _build_tasks(
        self,
        *,
        routes: Sequence[RetrievalRoute],
        selections: Sequence[ReplicaSelection],
        budget: RetrievalFanoutBudget,
    ) -> List[RetrievalTask]:
        selection_by_route = {
            selection.route_id: selection
            for selection in selections
        }

        tasks: List[
            RetrievalTask
        ] = []

        for route in routes:
            selection = selection_by_route.get(
                route.route_id
            )

            if selection is None:
                continue

            tasks.append(
                RetrievalTask(
                    task_id=_new_id(
                        "retrieval_task"
                    ),
                    route_id=route.route_id,
                    selection_id=selection.selection_id,
                    partition_id=route.partition_id,
                    replica_id=selection.replica_id,
                    routing_mode=route.routing_mode,
                    state=RetrievalTaskState.PLANNED,
                    term=route.term,
                    field=route.field,
                    phrase=route.phrase,
                    priority=route.priority,
                    timeout_ms=(
                        self.policy.default_timeout_ms
                    ),
                    max_retries=(
                        self.policy.default_retries
                    ),
                    metadata={
                        "architecture_stage": "11.5",
                        "route_weight": route.route_weight,
                    },
                )
            )

            if len(tasks) >= budget.max_tasks:
                break

        return tasks

    # ------------------------------------------------------------------
    # Fanout control
    # ------------------------------------------------------------------

    def _control_fanout(
        self,
        *,
        partitions: Sequence[IndexPartitionTarget],
        routes: Sequence[RetrievalRoute],
        tasks: Sequence[RetrievalTask],
        budget: RetrievalFanoutBudget,
    ) -> RetrievalFanoutDecision:
        candidate_partitions = len(
            partitions
        )

        candidate_routes = len(
            routes
        )

        candidate_tasks = len(
            tasks
        )

        reasons: List[str] = []

        accepted_partitions = min(
            candidate_partitions,
            budget.max_partitions,
        )

        accepted_routes = min(
            candidate_routes,
            budget.max_routes,
        )

        accepted_tasks = min(
            candidate_tasks,
            budget.max_tasks,
        )

        rejected = (
            max(
                0,
                candidate_partitions
                - accepted_partitions,
            )
            + max(
                0,
                candidate_routes
                - accepted_routes,
            )
            + max(
                0,
                candidate_tasks
                - accepted_tasks,
            )
        )

        if candidate_partitions > budget.max_partitions:
            reasons.append(
                "partition fanout exceeded budget"
            )

        if candidate_routes > budget.max_routes:
            reasons.append(
                "route fanout exceeded budget"
            )

        if candidate_tasks > budget.max_tasks:
            reasons.append(
                "task fanout exceeded budget"
            )

        if rejected == 0:
            state = FanoutState.WITHIN_BUDGET
        elif accepted_tasks > 0:
            state = FanoutState.REDUCED
        else:
            state = FanoutState.REJECTED

        fingerprint = _hash(
            {
                "state": state.value,
                "candidate_partitions": candidate_partitions,
                "accepted_partitions": accepted_partitions,
                "candidate_routes": candidate_routes,
                "accepted_routes": accepted_routes,
                "candidate_tasks": candidate_tasks,
                "accepted_tasks": accepted_tasks,
                "rejected": rejected,
                "reasons": reasons,
            }
        )

        return RetrievalFanoutDecision(
            state=state,
            candidate_partition_count=(
                candidate_partitions
            ),
            accepted_partition_count=(
                accepted_partitions
            ),
            candidate_route_count=(
                candidate_routes
            ),
            accepted_route_count=(
                accepted_routes
            ),
            candidate_task_count=(
                candidate_tasks
            ),
            accepted_task_count=(
                accepted_tasks
            ),
            rejected_count=rejected,
            budget=budget,
            reasons=tuple(reasons),
            fingerprint=fingerprint,
        )

    def _apply_task_cap(
        self,
        tasks: Sequence[RetrievalTask],
        budget: RetrievalFanoutBudget,
    ) -> List[RetrievalTask]:
        ordered = sorted(
            tasks,
            key=lambda task: (
                -task.priority,
                task.task_id,
            ),
        )

        return list(
            ordered[
                : budget.max_tasks
            ]
        )

    # ------------------------------------------------------------------
    # Failure planning
    # ------------------------------------------------------------------

    def _build_failure_plans(
        self,
        *,
        tasks: Sequence[RetrievalTask],
        selections: Sequence[ReplicaSelection],
        budget: RetrievalFanoutBudget,
    ) -> List[RetrievalFailurePlan]:
        selection_by_id = {
            selection.selection_id: selection
            for selection in selections
        }

        plans: List[
            RetrievalFailurePlan
        ] = []

        for task in tasks:
            selection = selection_by_id.get(
                task.selection_id
            )

            if selection is None:
                continue

            actions = [
                FailureAction.RETRY_SAME_REPLICA,
            ]

            if selection.fallback_replica_ids:
                actions.append(
                    FailureAction.RETRY_ALTERNATE_REPLICA
                )

            if self.policy.allow_cross_region_failover:
                actions.append(
                    FailureAction.RETRY_CROSS_REGION
                )

            if self.policy.allow_partial_results:
                actions.append(
                    FailureAction.RETURN_PARTIAL
                )

            fallback_regions: Tuple[
                str, ...
            ] = ()

            plans.append(
                RetrievalFailurePlan(
                    task_id=task.task_id,
                    actions=tuple(actions),
                    max_attempts=(
                        task.max_retries + 1
                    ),
                    allow_partial_result=(
                        self.policy.allow_partial_results
                    ),
                    fallback_replica_ids=(
                        selection.fallback_replica_ids
                    ),
                    fallback_regions=fallback_regions,
                    metadata={
                        "architecture_stage": "11.5",
                    },
                )
            )

        return plans

    # ------------------------------------------------------------------
    # Execution groups
    # ------------------------------------------------------------------

    def _build_execution_groups(
        self,
        tasks: Sequence[RetrievalTask],
        budget: RetrievalFanoutBudget,
    ) -> List[RetrievalExecutionGroup]:
        grouped: Dict[
            Tuple[str, str],
            List[str],
        ] = {}

        # Replica region/zone is encoded indirectly in the task metadata
        # by the planner contract. When live execution is introduced,
        # workers can regroup dynamically according to actual placement.
        for task in tasks:
            region = str(
                task.metadata.get(
                    "region",
                    "unknown",
                )
            )

            zone = str(
                task.metadata.get(
                    "zone",
                    "unknown",
                )
            )

            grouped.setdefault(
                (region, zone),
                [],
            ).append(
                task.task_id
            )

        result: List[
            RetrievalExecutionGroup
        ] = []

        for (
            region,
            zone,
        ), task_ids in sorted(
            grouped.items()
        ):
            result.append(
                RetrievalExecutionGroup(
                    group_id=_new_id(
                        "execution_group"
                    ),
                    region=region,
                    zone=zone,
                    task_ids=tuple(
                        task_ids
                    ),
                    max_parallelism=min(
                        budget.max_parallelism,
                        len(task_ids),
                    ),
                    locality_score=1.0,
                    metadata={
                        "architecture_stage": "11.5",
                    },
                )
            )

        return result

    # ------------------------------------------------------------------
    # Execution contract
    # ------------------------------------------------------------------

    def _build_execution_contract(
        self,
        *,
        request: RetrievalPlanningRequest,
        terms: Sequence[Mapping[str, Any]],
        phrases: Sequence[Mapping[str, Any]],
        fields: Sequence[Mapping[str, Any]],
        partitions: Sequence[IndexPartitionTarget],
        routes: Sequence[RetrievalRoute],
        selections: Sequence[ReplicaSelection],
        tasks: Sequence[RetrievalTask],
        execution_groups: Sequence[
            RetrievalExecutionGroup
        ],
        fanout: RetrievalFanoutDecision,
    ) -> Mapping[str, Any]:
        return {
            "architecture_version": ARCHITECTURE_VERSION,

            "routing_mode": (
                request.routing_mode.value
            ),

            "query_id": self._query_id(
                request.expansion_contract
            ),

            "term_count": len(terms),
            "phrase_count": len(phrases),
            "field_count": len(fields),

            "partition_count": len(partitions),
            "route_count": len(routes),
            "replica_selection_count": len(
                selections
            ),
            "task_count": len(tasks),

            "execution_group_count": len(
                execution_groups
            ),

            "fanout_state": fanout.state.value,

            "parallel_execution": True,

            "partial_results_supported": (
                self.policy.allow_partial_results
            ),

            "cross_region_failover": (
                self.policy.allow_cross_region_failover
            ),

            "retry_strategy": [
                FailureAction.RETRY_SAME_REPLICA.value,
                FailureAction.RETRY_ALTERNATE_REPLICA.value,
                FailureAction.RETRY_CROSS_REGION.value,
            ],

            "result_contract": {
                "returns_partition_results": True,
                "returns_partial_results": True,
                "ranking_stage": "phase_12",
            },

            "next_stage": (
                "phase_12_ranking_and_result_quality"
            ),
        }

    # ------------------------------------------------------------------
    # Events and checkpoints
    # ------------------------------------------------------------------

    def _emit(
        self,
        identity: RetrievalPlanningIdentity,
        event_type: PlanningEventType,
        state: PlanningState,
        payload: Mapping[str, Any],
    ) -> None:
        self._events.append(
            RetrievalPlanningEvent(
                event_id=_new_id(
                    "planning_event"
                ),
                plan_id=identity.plan_id,
                event_type=event_type,
                state=state,
                timestamp=_now(),
                payload=dict(payload),
            )
        )

    def _create_checkpoint(
        self,
        plan: DistributedRetrievalPlan,
        *,
        epoch: int,
    ) -> RetrievalPlanningCheckpoint:
        checkpoint = RetrievalPlanningCheckpoint(
            checkpoint_id=_new_id(
                "retrieval_checkpoint"
            ),
            plan_id=plan.identity.plan_id,
            state=plan.state,
            epoch=epoch,
            plan_hash=plan.plan_hash,
            task_count=len(plan.tasks),
            partition_count=len(
                plan.partitions
            ),
            created_at=_now(),
        )

        self._checkpoints[
            checkpoint.checkpoint_id
        ] = checkpoint

        return checkpoint

    # ------------------------------------------------------------------
    # Inspection
    # ------------------------------------------------------------------

    def events(
        self,
        plan_id: Optional[str] = None,
    ) -> Tuple[
        RetrievalPlanningEvent,
        ...
    ]:
        if plan_id is None:
            return tuple(
                self._events
            )

        return tuple(
            event
            for event in self._events
            if event.plan_id == plan_id
        )

    def checkpoints(
        self,
        plan_id: Optional[str] = None,
    ) -> Tuple[
        RetrievalPlanningCheckpoint,
        ...
    ]:
        values = tuple(
            self._checkpoints.values()
        )

        if plan_id is None:
            return values

        return tuple(
            checkpoint
            for checkpoint in values
            if checkpoint.plan_id == plan_id
        )

    # ------------------------------------------------------------------
    # Architecture contract
    # ------------------------------------------------------------------

    @staticmethod
    def architecture() -> Mapping[str, Any]:
        return {
            "name": (
                "OUR SEARCH Phase 11.5 "
                "Distributed Retrieval Planning"
            ),

            "version": ARCHITECTURE_VERSION,

            "scale_target": SCALE_TARGET,

            "google_scale_capability_target": (
                GOOGLE_SCALE_CAPABILITY_TARGET
            ),

            "google_technology_dependency": (
                GOOGLE_TECHNOLOGY_DEPENDENCY
            ),

            "input": (
                "Phase 11.4 Retrieval Expansion Contract"
            ),

            "pipeline": [
                "query retrieval plan creation",
                "partition discovery",
                "term routing",
                "phrase routing",
                "field routing",
                "replica selection",
                "locality-aware placement",
                "parallel task creation",
                "fanout control",
                "failure planning",
                "partial-result planning",
                "execution grouping",
                "retrieval execution contract",
            ],

            "distributed_design": {
                "partition_aware": True,
                "replica_aware": True,
                "region_aware": True,
                "zone_aware": True,
                "parallel_execution": True,
                "horizontal_scaling": True,
                "stateless_planning_preferred": True,
                "backend_replaceable": True,
                "checkpointable": True,
                "epoch_aware": True,
                "global_single_planner": False,
                "global_single_queue": False,
                "global_single_worker": False,
                "global_single_database": False,
            },

            "routing_capabilities": [
                "term routing",
                "field routing",
                "phrase routing",
                "hybrid routing",
                "broadcast routing",
                "partition fanout",
                "replica selection",
                "locality preference",
                "cross-region failover",
            ],

            "failure_capabilities": [
                "same-replica retry",
                "alternate-replica retry",
                "cross-region retry",
                "partial-result return",
                "bounded retry attempts",
            ],

            "anti_explosion_controls": [
                "maximum partitions",
                "maximum routes",
                "maximum tasks",
                "maximum tasks per term",
                "maximum tasks per field",
                "maximum retry budget",
                "maximum parallelism",
            ],

            "explicit_non_responsibilities": [
                "retrieval execution",
                "document ranking",
                "final result selection",
                "crawler execution",
                "index mutation",
                "embedding generation",
                "semantic model inference",
            ],

            "next_stage": (
                "Phase 12 Ranking + Result Quality "
                "at Enormous Scale"
            ),

            "protected_components": [
                "website_server.py",
                "search_service/server.py",
            ],
        }


# ---------------------------------------------------------------------------
# Compatibility aliases
# ---------------------------------------------------------------------------

DistributedRetrievalPlanningArchitecture = (
    DistributedRetrievalPlanning
)

GlobalDistributedRetrievalPlanning = (
    DistributedRetrievalPlanning
)

Phase11_5DistributedRetrievalPlanning = (
    DistributedRetrievalPlanning
)


__all__ = [
    "ARCHITECTURE_VERSION",
    "SCALE_TARGET",
    "GOOGLE_SCALE_CAPABILITY_TARGET",
    "GOOGLE_TECHNOLOGY_DEPENDENCY",

    "PlanningState",
    "RoutingMode",
    "RetrievalTaskState",
    "ReplicaState",
    "ReplicaPreference",
    "FailureAction",
    "FanoutState",
    "PlanningEventType",

    "RetrievalPlanningIdentity",
    "IndexPartitionTarget",
    "IndexReplicaTarget",
    "RetrievalRoute",
    "ReplicaSelection",
    "RetrievalTask",
    "RetrievalFailurePlan",

    "RetrievalFanoutBudget",
    "RetrievalFanoutDecision",

    "RetrievalExecutionGroup",
    "DistributedRetrievalPlan",
    "RetrievalPlanningCheckpoint",
    "RetrievalPlanningEvent",
    "RetrievalPlanningRequest",

    "DistributedRetrievalPlanningBackend",
    "InMemoryDistributedRetrievalPlanningBackend",
    "DistributedRetrievalPlanningPolicy",
    "DistributedRetrievalPlanning",

    "DistributedRetrievalPlanningArchitecture",
    "GlobalDistributedRetrievalPlanning",
    "Phase11_5DistributedRetrievalPlanning",
]
