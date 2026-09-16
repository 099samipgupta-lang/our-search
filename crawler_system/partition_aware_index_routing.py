"""
OUR SEARCH
Phase 10.6 — Partition-Aware Index Routing

Version:
    partition-aware-index-routing.v1

Purpose:
    Global partition-aware routing architecture for the OUR SEARCH index fabric.

Architecture:

    GLOBAL INDEX
        ↓
    PARTITION ROUTER
        ↓
    TERM / FIELD / DOCUMENT ROUTING
        ↓
    ROUTE FANOUT
        ↓
    REGION / ZONE / REPLICA SELECTION
        ↓
    INDEX READ TARGET

Scale target:

    Billions → potentially trillions of publicly accessible Web resources.

This module is an architecture/control-plane layer.

It deliberately separates:

    logical partition identity
        from
    physical storage placement.

The router does not depend on Google Search, Google's index,
Google infrastructure, or Google APIs.

The design supports:

    - deterministic partition routing
    - hash routing
    - range routing
    - prefix routing
    - field-aware routing
    - document routing
    - term routing
    - multi-partition fanout
    - region locality
    - zone locality
    - replica-aware reads
    - routing epochs
    - stale-route rejection
    - partition movement
    - partition splitting
    - partition merging
    - draining
    - degraded routing
    - failover
    - route-table snapshots
    - atomic route-table publication
    - routing checkpoints
    - rebalancing
    - migration-aware routing
    - horizontal expansion
    - replaceable routing backend

Important architectural principle:

    The global index must never require a single global routing table
    that becomes a physical bottleneck.

The routing namespace is therefore logically global but physically
partitionable and independently publishable.

No tests or benchmarks are performed by this module.
"""

from __future__ import annotations

import hashlib
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Iterable, List, Optional, Protocol, Sequence, Tuple


ARCHITECTURE_VERSION = "partition-aware-index-routing.v1"

GLOBAL_SCALE_TARGET = "billions_to_trillions_of_public_web_resources"
GOOGLE_SCALE_CAPABILITY_TARGET = True
GOOGLE_TECHNOLOGY_DEPENDENCY = False


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _now() -> float:
    return time.time()


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _partition_hash(value: str, partition_count: int) -> int:
    if partition_count <= 0:
        raise ValueError("partition_count must be greater than zero")

    digest = hashlib.sha256(value.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % partition_count


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex}"


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class PartitionRoutingState(str, Enum):
    ACTIVE = "active"
    DRAINING = "draining"
    MOVING = "moving"
    SPLITTING = "splitting"
    MERGING = "merging"
    DEGRADED = "degraded"
    RECOVERING = "recovering"
    RETIRED = "retired"


class PartitionRoutingMode(str, Enum):
    HASH = "hash"
    RANGE = "range"
    PREFIX = "prefix"
    FIELD_AWARE = "field_aware"
    DOCUMENT = "document"
    TERM = "term"
    BROADCAST = "broadcast"


class RouteTargetState(str, Enum):
    AVAILABLE = "available"
    PREFERRED = "preferred"
    DEGRADED = "degraded"
    DRAINING = "draining"
    FAILED = "failed"
    RECOVERING = "recovering"


class ReplicaPreference(str, Enum):
    LOCAL_REGION = "local_region"
    LOCAL_ZONE = "local_zone"
    ANY_HEALTHY = "any_healthy"
    PREFERRED_REPLICA = "preferred_replica"


class RoutingDecisionState(str, Enum):
    ROUTED = "routed"
    PARTIAL = "partial"
    DEGRADED = "degraded"
    STALE = "stale"
    FENCED = "fenced"
    UNROUTABLE = "unroutable"


class PartitionMigrationState(str, Enum):
    PLANNED = "planned"
    PREPARING = "preparing"
    COPYING = "copying"
    CATCHING_UP = "catching_up"
    CUTOVER = "cutover"
    COMPLETED = "completed"
    ABORTED = "aborted"


class RoutingEventType(str, Enum):
    PARTITION_REGISTERED = "partition_registered"
    PARTITION_STATE_CHANGED = "partition_state_changed"
    ROUTE_TABLE_CREATED = "route_table_created"
    ROUTE_TABLE_PUBLISHED = "route_table_published"
    ROUTE_TABLE_SWAPPED = "route_table_swapped"
    ROUTE_DECISION_CREATED = "route_decision_created"
    ROUTE_FANOUT_CREATED = "route_fanout_created"
    REPLICA_SELECTED = "replica_selected"
    FAILOVER_TRIGGERED = "failover_triggered"
    PARTITION_MOVING = "partition_moving"
    PARTITION_SPLIT = "partition_split"
    PARTITION_MERGED = "partition_merged"
    ROUTE_FENCED = "route_fenced"
    CHECKPOINT_CREATED = "checkpoint_created"
    REBALANCE_PLANNED = "rebalance_planned"


# ---------------------------------------------------------------------------
# Core identities
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class IndexPartitionKey:
    """
    Stable logical identity of an index partition.

    The partition key is independent of the physical storage location.
    """

    namespace: str
    partition_id: str
    generation: int = 0

    def stable_key(self) -> str:
        return (
            f"{self.namespace}:"
            f"{self.partition_id}:"
            f"{self.generation}"
        )


@dataclass(frozen=True)
class RoutingToken:
    """
    Logical token used by the router to determine candidate partitions.
    """

    value: str
    token_type: PartitionRoutingMode
    field: Optional[str] = None

    def stable_key(self) -> str:
        field_part = self.field or ""
        return f"{self.token_type.value}:{field_part}:{self.value}"


# ---------------------------------------------------------------------------
# Physical routing targets
# ---------------------------------------------------------------------------

@dataclass
class PartitionReplicaRoute:
    replica_id: str
    partition_id: str

    region: str
    zone: str
    node_id: str

    state: RouteTargetState = RouteTargetState.AVAILABLE
    preference: ReplicaPreference = ReplicaPreference.ANY_HEALTHY

    routing_epoch: int = 0
    last_healthy_at: float = field(default_factory=_now)

    read_capable: bool = True
    write_capable: bool = False

    failure_count: int = 0

    metadata: Dict[str, str] = field(default_factory=dict)


@dataclass
class PartitionRoute:
    """
    Logical partition plus its current physical replica routes.
    """

    key: IndexPartitionKey

    state: PartitionRoutingState = PartitionRoutingState.ACTIVE

    routing_mode: PartitionRoutingMode = PartitionRoutingMode.HASH

    hash_start: Optional[int] = None
    hash_end: Optional[int] = None

    range_start: Optional[str] = None
    range_end: Optional[str] = None

    prefixes: List[str] = field(default_factory=list)

    replicas: Dict[str, PartitionReplicaRoute] = field(default_factory=dict)

    routing_epoch: int = 0

    parent_partition_ids: List[str] = field(default_factory=list)
    child_partition_ids: List[str] = field(default_factory=list)

    migration_state: Optional[PartitionMigrationState] = None

    created_at: float = field(default_factory=_now)
    updated_at: float = field(default_factory=_now)

    metadata: Dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Route table
# ---------------------------------------------------------------------------

@dataclass
class PartitionRoutingTable:
    """
    Versioned snapshot of logical-to-physical partition routing.

    A table is immutable after publication from the router's perspective.
    New routing state is published as a new table version.
    """

    table_id: str
    namespace: str

    version: int
    routing_epoch: int

    partitions: Dict[str, PartitionRoute] = field(default_factory=dict)

    created_at: float = field(default_factory=_now)

    published: bool = False
    checksum: str = ""

    parent_table_id: Optional[str] = None

    metadata: Dict[str, str] = field(default_factory=dict)

    def calculate_checksum(self) -> str:
        entries = []

        for partition_id in sorted(self.partitions):
            partition = self.partitions[partition_id]

            replica_entries = []

            for replica_id in sorted(partition.replicas):
                replica = partition.replicas[replica_id]

                replica_entries.append(
                    "|".join(
                        [
                            replica.replica_id,
                            replica.region,
                            replica.zone,
                            replica.node_id,
                            replica.state.value,
                            str(replica.routing_epoch),
                        ]
                    )
                )

            entries.append(
                "|".join(
                    [
                        partition.key.stable_key(),
                        partition.state.value,
                        partition.routing_mode.value,
                        str(partition.routing_epoch),
                        str(partition.hash_start),
                        str(partition.hash_end),
                        str(partition.range_start),
                        str(partition.range_end),
                        ",".join(sorted(partition.prefixes)),
                        ",".join(replica_entries),
                    ]
                )
            )

        return _hash(
            f"{self.table_id}|"
            f"{self.namespace}|"
            f"{self.version}|"
            f"{self.routing_epoch}|"
            f"{'~'.join(entries)}"
        )


# ---------------------------------------------------------------------------
# Requests and decisions
# ---------------------------------------------------------------------------

@dataclass
class PartitionRoutingRequest:
    request_id: str

    namespace: str

    routing_mode: PartitionRoutingMode

    token: RoutingToken

    source_region: Optional[str] = None
    source_zone: Optional[str] = None

    required_epoch: Optional[int] = None

    replica_preference: ReplicaPreference = ReplicaPreference.LOCAL_REGION

    allow_degraded: bool = True
    allow_failover: bool = True

    max_route_fanout: Optional[int] = None

    metadata: Dict[str, str] = field(default_factory=dict)


@dataclass
class PartitionRoutingDecision:
    request_id: str

    state: RoutingDecisionState

    routing_table_id: Optional[str]
    routing_table_version: Optional[int]

    routing_epoch: Optional[int]

    selected_partitions: List[str] = field(default_factory=list)

    selected_replicas: Dict[str, str] = field(default_factory=dict)

    failed_partitions: List[str] = field(default_factory=list)

    reason: Optional[str] = None

    created_at: float = field(default_factory=_now)

    metadata: Dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Fanout
# ---------------------------------------------------------------------------

@dataclass
class RouteFanoutPlan:
    fanout_id: str

    request_id: str

    partition_ids: List[str]

    replica_targets: Dict[str, str]

    source_region: Optional[str]
    source_zone: Optional[str]

    routing_epoch: int

    bounded: bool

    created_at: float = field(default_factory=_now)

    metadata: Dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Migration / split / merge
# ---------------------------------------------------------------------------

@dataclass
class PartitionMigration:
    migration_id: str

    source_partition_id: str
    destination_partition_ids: List[str]

    state: PartitionMigrationState

    source_epoch: int
    destination_epoch: int

    started_at: float = field(default_factory=_now)
    completed_at: Optional[float] = None

    metadata: Dict[str, str] = field(default_factory=dict)


@dataclass
class PartitionSplitPlan:
    split_id: str

    source_partition_id: str

    child_partition_ids: List[str]

    source_epoch: int
    child_epoch: int

    created_at: float = field(default_factory=_now)

    metadata: Dict[str, str] = field(default_factory=dict)


@dataclass
class PartitionMergePlan:
    merge_id: str

    source_partition_ids: List[str]

    destination_partition_id: str

    source_epoch: int
    destination_epoch: int

    created_at: float = field(default_factory=_now)

    metadata: Dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Events and checkpoints
# ---------------------------------------------------------------------------

@dataclass
class PartitionRoutingEvent:
    event_id: str
    event_type: RoutingEventType

    namespace: str

    routing_epoch: int

    partition_ids: List[str] = field(default_factory=list)

    request_id: Optional[str] = None

    timestamp: float = field(default_factory=_now)

    metadata: Dict[str, str] = field(default_factory=dict)


@dataclass
class PartitionRoutingCheckpoint:
    checkpoint_id: str

    namespace: str

    routing_table_id: str
    routing_table_version: int
    routing_epoch: int

    active_partition_count: int

    timestamp: float = field(default_factory=_now)

    checksum: str = ""

    metadata: Dict[str, str] = field(default_factory=dict)


@dataclass
class PartitionRoutingCapacity:
    namespace: str

    logical_partition_count: int
    active_partition_count: int
    degraded_partition_count: int
    moving_partition_count: int

    replica_route_count: int

    routing_table_version: int
    routing_epoch: int

    supports_horizontal_partition_growth: bool
    supports_physical_node_growth: bool


# ---------------------------------------------------------------------------
# Backend contract
# ---------------------------------------------------------------------------

class PartitionAwareIndexRoutingBackend(Protocol):
    """
    Replaceable metadata/control-plane backend.

    Physical routing metadata must not be tied to one database,
    one file, one machine, or one region.
    """

    def save_partition(self, partition: PartitionRoute) -> None:
        ...

    def get_partition(self, partition_id: str) -> Optional[PartitionRoute]:
        ...

    def save_table(self, table: PartitionRoutingTable) -> None:
        ...

    def get_table(
        self,
        namespace: str,
        version: Optional[int] = None,
    ) -> Optional[PartitionRoutingTable]:
        ...

    def save_checkpoint(
        self,
        checkpoint: PartitionRoutingCheckpoint,
    ) -> None:
        ...

    def save_event(
        self,
        event: PartitionRoutingEvent,
    ) -> None:
        ...


# ---------------------------------------------------------------------------
# Reference backend
# ---------------------------------------------------------------------------

class InMemoryPartitionAwareRoutingMetadata:
    """
    Reference backend for architecture development.

    This is intentionally metadata-only.

    Production deployments can replace this backend with distributed
    metadata storage without changing routing semantics.
    """

    def __init__(self) -> None:
        self.partitions: Dict[str, PartitionRoute] = {}
        self.tables: Dict[Tuple[str, int], PartitionRoutingTable] = {}
        self.checkpoints: Dict[str, PartitionRoutingCheckpoint] = {}
        self.events: List[PartitionRoutingEvent] = []

    def save_partition(self, partition: PartitionRoute) -> None:
        self.partitions[partition.key.partition_id] = partition

    def get_partition(
        self,
        partition_id: str,
    ) -> Optional[PartitionRoute]:
        return self.partitions.get(partition_id)

    def save_table(
        self,
        table: PartitionRoutingTable,
    ) -> None:
        self.tables[(table.namespace, table.version)] = table

    def get_table(
        self,
        namespace: str,
        version: Optional[int] = None,
    ) -> Optional[PartitionRoutingTable]:

        candidates = [
            table
            for (table_namespace, _), table in self.tables.items()
            if table_namespace == namespace
        ]

        if not candidates:
            return None

        if version is None:
            return max(candidates, key=lambda item: item.version)

        return self.tables.get((namespace, version))

    def save_checkpoint(
        self,
        checkpoint: PartitionRoutingCheckpoint,
    ) -> None:
        self.checkpoints[checkpoint.checkpoint_id] = checkpoint

    def save_event(
        self,
        event: PartitionRoutingEvent,
    ) -> None:
        self.events.append(event)


# ---------------------------------------------------------------------------
# Policy
# ---------------------------------------------------------------------------

@dataclass
class PartitionAwareRoutingPolicy:
    """
    Routing policy.

    These values are routing-policy defaults, not global capacity limits.
    """

    default_routing_mode: PartitionRoutingMode = PartitionRoutingMode.HASH

    preferred_replica_order: Tuple[
        ReplicaPreference,
        ...
    ] = (
        ReplicaPreference.LOCAL_ZONE,
        ReplicaPreference.LOCAL_REGION,
        ReplicaPreference.PREFERRED_REPLICA,
        ReplicaPreference.ANY_HEALTHY,
    )

    allow_degraded_reads: bool = True
    allow_failover: bool = True

    route_table_epoch_grace: int = 0

    max_candidate_fanout: Optional[int] = None

    hash_ring_virtualization: bool = True

    prefer_same_zone: bool = True
    prefer_same_region: bool = True

    reject_retired_partitions: bool = True
    reject_stale_epochs: bool = True

    checkpoint_every_epoch: bool = True


# ---------------------------------------------------------------------------
# Main router
# ---------------------------------------------------------------------------

class PartitionAwareIndexRouter:
    """
    Global partition-aware index routing control architecture.

    Responsibilities:

        1. Maintain logical partition routing metadata.
        2. Resolve terms, fields, and documents to partitions.
        3. Select healthy physical replicas.
        4. Prefer locality when possible.
        5. Fail over when replicas become unavailable.
        6. Fence stale routing epochs.
        7. Publish routing tables atomically.
        8. Support partition movement.
        9. Support partition splitting.
        10. Support partition merging.
        11. Produce recovery-safe checkpoints.
        12. Enable global rebalancing without changing logical IDs.
    """

    def __init__(
        self,
        namespace: str = "global-index",
        backend: Optional[PartitionAwareIndexRoutingBackend] = None,
        policy: Optional[PartitionAwareRoutingPolicy] = None,
    ) -> None:

        self.namespace = namespace

        self.backend = (
            backend
            if backend is not None
            else InMemoryPartitionAwareRoutingMetadata()
        )

        self.policy = (
            policy
            if policy is not None
            else PartitionAwareRoutingPolicy()
        )

        self._routing_epoch = 0
        self._table_version = 0

        self._active_table: Optional[PartitionRoutingTable] = None

        self._events: List[PartitionRoutingEvent] = []

        self._migrations: Dict[str, PartitionMigration] = {}
        self._splits: Dict[str, PartitionSplitPlan] = {}
        self._merges: Dict[str, PartitionMergePlan] = {}

    # ------------------------------------------------------------------
    # Epoch management
    # ------------------------------------------------------------------

    @property
    def routing_epoch(self) -> int:
        return self._routing_epoch

    @property
    def table_version(self) -> int:
        return self._table_version

    def advance_epoch(self) -> int:
        self._routing_epoch += 1
        return self._routing_epoch

    # ------------------------------------------------------------------
    # Partition registration
    # ------------------------------------------------------------------

    def register_partition(
        self,
        partition_id: str,
        routing_mode: Optional[PartitionRoutingMode] = None,
        *,
        hash_start: Optional[int] = None,
        hash_end: Optional[int] = None,
        range_start: Optional[str] = None,
        range_end: Optional[str] = None,
        prefixes: Optional[Sequence[str]] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> PartitionRoute:

        mode = (
            routing_mode
            if routing_mode is not None
            else self.policy.default_routing_mode
        )

        key = IndexPartitionKey(
            namespace=self.namespace,
            partition_id=partition_id,
            generation=0,
        )

        partition = PartitionRoute(
            key=key,
            state=PartitionRoutingState.ACTIVE,
            routing_mode=mode,
            hash_start=hash_start,
            hash_end=hash_end,
            range_start=range_start,
            range_end=range_end,
            prefixes=list(prefixes or []),
            routing_epoch=self._routing_epoch,
            metadata=dict(metadata or {}),
        )

        self.backend.save_partition(partition)

        self._emit_event(
            RoutingEventType.PARTITION_REGISTERED,
            partition_ids=[partition_id],
        )

        return partition

    # ------------------------------------------------------------------
    # Replica registration
    # ------------------------------------------------------------------

    def register_replica(
        self,
        partition_id: str,
        replica_id: str,
        region: str,
        zone: str,
        node_id: str,
        *,
        state: RouteTargetState = RouteTargetState.AVAILABLE,
        preference: ReplicaPreference = ReplicaPreference.ANY_HEALTHY,
        read_capable: bool = True,
        write_capable: bool = False,
        metadata: Optional[Dict[str, str]] = None,
    ) -> PartitionReplicaRoute:

        partition = self.backend.get_partition(partition_id)

        if partition is None:
            raise KeyError(
                f"Unknown partition: {partition_id}"
            )

        replica = PartitionReplicaRoute(
            replica_id=replica_id,
            partition_id=partition_id,
            region=region,
            zone=zone,
            node_id=node_id,
            state=state,
            preference=preference,
            routing_epoch=partition.routing_epoch,
            read_capable=read_capable,
            write_capable=write_capable,
            metadata=dict(metadata or {}),
        )

        partition.replicas[replica_id] = replica
        partition.updated_at = _now()

        self.backend.save_partition(partition)

        self._emit_event(
            RoutingEventType.REPLICA_SELECTED,
            partition_ids=[partition_id],
        )

        return replica

    # ------------------------------------------------------------------
    # State management
    # ------------------------------------------------------------------

    def set_partition_state(
        self,
        partition_id: str,
        state: PartitionRoutingState,
    ) -> PartitionRoute:

        partition = self.backend.get_partition(partition_id)

        if partition is None:
            raise KeyError(
                f"Unknown partition: {partition_id}"
            )

        partition.state = state
        partition.updated_at = _now()

        self.backend.save_partition(partition)

        self._emit_event(
            RoutingEventType.PARTITION_STATE_CHANGED,
            partition_ids=[partition_id],
            metadata={"state": state.value},
        )

        return partition

    def set_replica_state(
        self,
        partition_id: str,
        replica_id: str,
        state: RouteTargetState,
    ) -> PartitionReplicaRoute:

        partition = self.backend.get_partition(partition_id)

        if partition is None:
            raise KeyError(
                f"Unknown partition: {partition_id}"
            )

        replica = partition.replicas.get(replica_id)

        if replica is None:
            raise KeyError(
                f"Unknown replica: {replica_id}"
            )

        replica.state = state

        if state == RouteTargetState.AVAILABLE:
            replica.last_healthy_at = _now()

        if state == RouteTargetState.FAILED:
            replica.failure_count += 1

        partition.updated_at = _now()

        self.backend.save_partition(partition)

        return replica

    # ------------------------------------------------------------------
    # Routing table publication
    # ------------------------------------------------------------------

    def create_routing_table(
        self,
        *,
        advance_epoch: bool = True,
    ) -> PartitionRoutingTable:

        if advance_epoch:
            self.advance_epoch()

        self._table_version += 1

        partitions: Dict[str, PartitionRoute] = {}

        if self._active_table is not None:
            partitions.update(self._active_table.partitions)

        table = PartitionRoutingTable(
            table_id=_new_id("route-table"),
            namespace=self.namespace,
            version=self._table_version,
            routing_epoch=self._routing_epoch,
            partitions=partitions,
            parent_table_id=(
                self._active_table.table_id
                if self._active_table is not None
                else None
            ),
        )

        table.checksum = table.calculate_checksum()

        self.backend.save_table(table)

        self._emit_event(
            RoutingEventType.ROUTE_TABLE_CREATED,
        )

        return table

    def publish_routing_table(
        self,
        table: PartitionRoutingTable,
    ) -> PartitionRoutingTable:

        calculated_checksum = table.calculate_checksum()

        if table.checksum != calculated_checksum:
            table.checksum = calculated_checksum

        table.published = True

        self.backend.save_table(table)

        # Atomic control-plane pointer swap.
        self._active_table = table

        self._emit_event(
            RoutingEventType.ROUTE_TABLE_PUBLISHED,
        )

        self._emit_event(
            RoutingEventType.ROUTE_TABLE_SWAPPED,
        )

        return table

    def publish_current_partition_state(self) -> PartitionRoutingTable:
        table = self.create_routing_table()
        return self.publish_routing_table(table)

    # ------------------------------------------------------------------
    # Candidate partition routing
    # ------------------------------------------------------------------

    def _hash_candidates(
        self,
        token: RoutingToken,
        partitions: Iterable[PartitionRoute],
    ) -> List[PartitionRoute]:

        candidates = []

        token_hash = int(
            _hash(token.stable_key()),
            16,
        )

        # When explicit hash ranges exist, use them.
        ranged = [
            partition
            for partition in partitions
            if partition.hash_start is not None
            and partition.hash_end is not None
        ]

        if ranged:
            for partition in ranged:
                assert partition.hash_start is not None
                assert partition.hash_end is not None

                if (
                    partition.hash_start
                    <= token_hash
                    < partition.hash_end
                ):
                    candidates.append(partition)

            if candidates:
                return candidates

        # Fallback deterministic partition selection.
        partitions_list = list(partitions)

        if not partitions_list:
            return []

        index = _partition_hash(
            token.stable_key(),
            len(partitions_list),
        )

        return [partitions_list[index]]

    def _range_candidates(
        self,
        token: RoutingToken,
        partitions: Iterable[PartitionRoute],
    ) -> List[PartitionRoute]:

        candidates = []

        for partition in partitions:

            start = partition.range_start
            end = partition.range_end

            if start is not None and token.value < start:
                continue

            if end is not None and token.value >= end:
                continue

            candidates.append(partition)

        return candidates

    def _prefix_candidates(
        self,
        token: RoutingToken,
        partitions: Iterable[PartitionRoute],
    ) -> List[PartitionRoute]:

        candidates = []

        for partition in partitions:
            if not partition.prefixes:
                continue

            if any(
                token.value.startswith(prefix)
                for prefix in partition.prefixes
            ):
                candidates.append(partition)

        return candidates

    def _candidate_partitions(
        self,
        request: PartitionRoutingRequest,
        table: PartitionRoutingTable,
    ) -> List[PartitionRoute]:

        active_partitions = [
            partition
            for partition in table.partitions.values()
            if partition.state != PartitionRoutingState.RETIRED
        ]

        if request.routing_mode in (
            PartitionRoutingMode.HASH,
            PartitionRoutingMode.TERM,
            PartitionRoutingMode.DOCUMENT,
        ):
            return self._hash_candidates(
                request.token,
                active_partitions,
            )

        if request.routing_mode == PartitionRoutingMode.RANGE:
            return self._range_candidates(
                request.token,
                active_partitions,
            )

        if request.routing_mode == PartitionRoutingMode.PREFIX:
            return self._prefix_candidates(
                request.token,
                active_partitions,
            )

        if request.routing_mode == PartitionRoutingMode.FIELD_AWARE:
            field_partitions = [
                partition
                for partition in active_partitions
                if (
                    not partition.metadata.get("field")
                    or partition.metadata.get("field")
                    == request.token.field
                )
            ]

            return self._hash_candidates(
                request.token,
                field_partitions,
            )

        if request.routing_mode == PartitionRoutingMode.BROADCAST:
            return active_partitions

        return self._hash_candidates(
            request.token,
            active_partitions,
        )

    # ------------------------------------------------------------------
    # Replica selection
    # ------------------------------------------------------------------

    @staticmethod
    def _healthy_read_replicas(
        partition: PartitionRoute,
        allow_degraded: bool,
    ) -> List[PartitionReplicaRoute]:

        healthy_states = {
            RouteTargetState.AVAILABLE,
            RouteTargetState.PREFERRED,
        }

        if allow_degraded:
            healthy_states.add(
                RouteTargetState.DEGRADED
            )

        return [
            replica
            for replica in partition.replicas.values()
            if (
                replica.read_capable
                and replica.state in healthy_states
            )
        ]

    def _select_replica(
        self,
        partition: PartitionRoute,
        request: PartitionRoutingRequest,
    ) -> Optional[PartitionReplicaRoute]:

        replicas = self._healthy_read_replicas(
            partition,
            request.allow_degraded,
        )

        if not replicas:
            return None

        # Strongest locality: same zone.
        if (
            request.source_zone is not None
            and self.policy.prefer_same_zone
        ):
            same_zone = [
                replica
                for replica in replicas
                if replica.zone == request.source_zone
            ]

            if same_zone:
                return self._rank_replicas(
                    same_zone
                )[0]

        # Next locality: same region.
        if (
            request.source_region is not None
            and self.policy.prefer_same_region
        ):
            same_region = [
                replica
                for replica in replicas
                if replica.region == request.source_region
            ]

            if same_region:
                return self._rank_replicas(
                    same_region
                )[0]

        return self._rank_replicas(replicas)[0]

    @staticmethod
    def _rank_replicas(
        replicas: Sequence[PartitionReplicaRoute],
    ) -> List[PartitionReplicaRoute]:

        preference_order = {
            RouteTargetState.PREFERRED: 0,
            RouteTargetState.AVAILABLE: 1,
            RouteTargetState.DEGRADED: 2,
            RouteTargetState.RECOVERING: 3,
            RouteTargetState.DRAINING: 4,
            RouteTargetState.FAILED: 5,
        }

        return sorted(
            replicas,
            key=lambda replica: (
                preference_order.get(
                    replica.state,
                    99,
                ),
                -replica.last_healthy_at,
                replica.replica_id,
            ),
        )

    # ------------------------------------------------------------------
    # Main route operation
    # ------------------------------------------------------------------

    def route(
        self,
        request: PartitionRoutingRequest,
    ) -> PartitionRoutingDecision:

        table = self._active_table

        if table is None:
            return PartitionRoutingDecision(
                request_id=request.request_id,
                state=RoutingDecisionState.UNROUTABLE,
                routing_table_id=None,
                routing_table_version=None,
                routing_epoch=None,
                reason="No published routing table",
            )

        if request.required_epoch is not None:

            if table.routing_epoch < request.required_epoch:

                return PartitionRoutingDecision(
                    request_id=request.request_id,
                    state=RoutingDecisionState.STALE,
                    routing_table_id=table.table_id,
                    routing_table_version=table.version,
                    routing_epoch=table.routing_epoch,
                    reason="Published routing epoch is older than required epoch",
                )

            if (
                self.policy.reject_stale_epochs
                and table.routing_epoch < request.required_epoch
            ):
                return PartitionRoutingDecision(
                    request_id=request.request_id,
                    state=RoutingDecisionState.FENCED,
                    routing_table_id=table.table_id,
                    routing_table_version=table.version,
                    routing_epoch=table.routing_epoch,
                    reason="Routing epoch fenced",
                )

        candidates = self._candidate_partitions(
            request,
            table,
        )

        if request.max_route_fanout is not None:
            candidates = candidates[
                :request.max_route_fanout
            ]

        if (
            self.policy.max_candidate_fanout is not None
            and len(candidates)
            > self.policy.max_candidate_fanout
        ):
            candidates = candidates[
                :self.policy.max_candidate_fanout
            ]

        selected_partitions = []
        selected_replicas: Dict[str, str] = {}
        failed_partitions = []

        for partition in candidates:

            if partition.state in (
                PartitionRoutingState.RETIRED,
                PartitionRoutingState.RECOVERING,
            ):
                failed_partitions.append(
                    partition.key.partition_id
                )
                continue

            replica = self._select_replica(
                partition,
                request,
            )

            if replica is None:

                if (
                    request.allow_failover
                    and self.policy.allow_failover
                ):
                    failed_partitions.append(
                        partition.key.partition_id
                    )

                    self._emit_event(
                        RoutingEventType.FAILOVER_TRIGGERED,
                        partition_ids=[
                            partition.key.partition_id
                        ],
                        request_id=request.request_id,
                    )

                    continue

                failed_partitions.append(
                    partition.key.partition_id
                )
                continue

            selected_partitions.append(
                partition.key.partition_id
            )

            selected_replicas[
                partition.key.partition_id
            ] = replica.replica_id

        if not selected_partitions:

            state = RoutingDecisionState.UNROUTABLE

            if failed_partitions:
                state = RoutingDecisionState.DEGRADED

            decision = PartitionRoutingDecision(
                request_id=request.request_id,
                state=state,
                routing_table_id=table.table_id,
                routing_table_version=table.version,
                routing_epoch=table.routing_epoch,
                selected_partitions=[],
                selected_replicas={},
                failed_partitions=failed_partitions,
                reason="No healthy routing target available",
            )

        elif failed_partitions:

            decision = PartitionRoutingDecision(
                request_id=request.request_id,
                state=RoutingDecisionState.PARTIAL,
                routing_table_id=table.table_id,
                routing_table_version=table.version,
                routing_epoch=table.routing_epoch,
                selected_partitions=selected_partitions,
                selected_replicas=selected_replicas,
                failed_partitions=failed_partitions,
                reason="Some candidate partitions could not be routed",
            )

        else:

            decision = PartitionRoutingDecision(
                request_id=request.request_id,
                state=RoutingDecisionState.ROUTED,
                routing_table_id=table.table_id,
                routing_table_version=table.version,
                routing_epoch=table.routing_epoch,
                selected_partitions=selected_partitions,
                selected_replicas=selected_replicas,
            )

        self._emit_event(
            RoutingEventType.ROUTE_DECISION_CREATED,
            partition_ids=decision.selected_partitions,
            request_id=request.request_id,
        )

        if len(decision.selected_partitions) > 1:

            self._emit_event(
                RoutingEventType.ROUTE_FANOUT_CREATED,
                partition_ids=decision.selected_partitions,
                request_id=request.request_id,
            )

        return decision

    # ------------------------------------------------------------------
    # Convenience routing methods
    # ------------------------------------------------------------------

    def route_term(
        self,
        term: str,
        *,
        field: Optional[str] = None,
        source_region: Optional[str] = None,
        source_zone: Optional[str] = None,
        required_epoch: Optional[int] = None,
    ) -> PartitionRoutingDecision:

        request = PartitionRoutingRequest(
            request_id=_new_id("route-request"),
            namespace=self.namespace,
            routing_mode=PartitionRoutingMode.TERM,
            token=RoutingToken(
                value=term,
                token_type=PartitionRoutingMode.TERM,
                field=field,
            ),
            source_region=source_region,
            source_zone=source_zone,
            required_epoch=required_epoch,
            replica_preference=(
                ReplicaPreference.LOCAL_REGION
            ),
            allow_degraded=(
                self.policy.allow_degraded_reads
            ),
            allow_failover=(
                self.policy.allow_failover
            ),
        )

        return self.route(request)

    def route_document(
        self,
        document_id: str,
        *,
        source_region: Optional[str] = None,
        source_zone: Optional[str] = None,
        required_epoch: Optional[int] = None,
    ) -> PartitionRoutingDecision:

        request = PartitionRoutingRequest(
            request_id=_new_id("route-request"),
            namespace=self.namespace,
            routing_mode=PartitionRoutingMode.DOCUMENT,
            token=RoutingToken(
                value=document_id,
                token_type=PartitionRoutingMode.DOCUMENT,
            ),
            source_region=source_region,
            source_zone=source_zone,
            required_epoch=required_epoch,
            allow_degraded=(
                self.policy.allow_degraded_reads
            ),
            allow_failover=(
                self.policy.allow_failover
            ),
        )

        return self.route(request)

    def route_field_value(
        self,
        field: str,
        value: str,
        *,
        source_region: Optional[str] = None,
        source_zone: Optional[str] = None,
        required_epoch: Optional[int] = None,
    ) -> PartitionRoutingDecision:

        request = PartitionRoutingRequest(
            request_id=_new_id("route-request"),
            namespace=self.namespace,
            routing_mode=PartitionRoutingMode.FIELD_AWARE,
            token=RoutingToken(
                value=value,
                token_type=PartitionRoutingMode.FIELD_AWARE,
                field=field,
            ),
            source_region=source_region,
            source_zone=source_zone,
            required_epoch=required_epoch,
            allow_degraded=(
                self.policy.allow_degraded_reads
            ),
            allow_failover=(
                self.policy.allow_failover
            ),
        )

        return self.route(request)

    # ------------------------------------------------------------------
    # Fanout plan
    # ------------------------------------------------------------------

    def create_fanout_plan(
        self,
        decision: PartitionRoutingDecision,
        *,
        source_region: Optional[str] = None,
        source_zone: Optional[str] = None,
    ) -> RouteFanoutPlan:

        routing_epoch = (
            decision.routing_epoch
            if decision.routing_epoch is not None
            else self._routing_epoch
        )

        plan = RouteFanoutPlan(
            fanout_id=_new_id("fanout"),
            request_id=decision.request_id,
            partition_ids=list(
                decision.selected_partitions
            ),
            replica_targets=dict(
                decision.selected_replicas
            ),
            source_region=source_region,
            source_zone=source_zone,
            routing_epoch=routing_epoch,
            bounded=(
                decision.state
                != RoutingDecisionState.UNROUTABLE
            ),
        )

        return plan

    # ------------------------------------------------------------------
    # Partition movement
    # ------------------------------------------------------------------

    def plan_partition_movement(
        self,
        source_partition_id: str,
        destination_partition_ids: Sequence[str],
    ) -> PartitionMigration:

        source = self.backend.get_partition(
            source_partition_id
        )

        if source is None:
            raise KeyError(
                f"Unknown partition: {source_partition_id}"
            )

        source.state = PartitionRoutingState.MOVING
        source.migration_state = (
            PartitionMigrationState.PLANNED
        )

        self.backend.save_partition(source)

        migration = PartitionMigration(
            migration_id=_new_id("migration"),
            source_partition_id=source_partition_id,
            destination_partition_ids=list(
                destination_partition_ids
            ),
            state=PartitionMigrationState.PLANNED,
            source_epoch=source.routing_epoch,
            destination_epoch=self._routing_epoch + 1,
        )

        self._migrations[
            migration.migration_id
        ] = migration

        self._emit_event(
            RoutingEventType.PARTITION_MOVING,
            partition_ids=[
                source_partition_id,
                *destination_partition_ids,
            ],
        )

        return migration

    # ------------------------------------------------------------------
    # Partition splitting
    # ------------------------------------------------------------------

    def plan_partition_split(
        self,
        source_partition_id: str,
        child_partition_ids: Sequence[str],
    ) -> PartitionSplitPlan:

        source = self.backend.get_partition(
            source_partition_id
        )

        if source is None:
            raise KeyError(
                f"Unknown partition: {source_partition_id}"
            )

        if not child_partition_ids:
            raise ValueError(
                "At least one child partition is required"
            )

        source.state = PartitionRoutingState.SPLITTING
        source.child_partition_ids = list(
            child_partition_ids
        )

        self.backend.save_partition(source)

        plan = PartitionSplitPlan(
            split_id=_new_id("split"),
            source_partition_id=source_partition_id,
            child_partition_ids=list(
                child_partition_ids
            ),
            source_epoch=source.routing_epoch,
            child_epoch=self._routing_epoch + 1,
        )

        self._splits[plan.split_id] = plan

        self._emit_event(
            RoutingEventType.PARTITION_SPLIT,
            partition_ids=[
                source_partition_id,
                *child_partition_ids,
            ],
        )

        return plan

    # ------------------------------------------------------------------
    # Partition merging
    # ------------------------------------------------------------------

    def plan_partition_merge(
        self,
        source_partition_ids: Sequence[str],
        destination_partition_id: str,
    ) -> PartitionMergePlan:

        if not source_partition_ids:
            raise ValueError(
                "At least one source partition is required"
            )

        for partition_id in source_partition_ids:

            partition = self.backend.get_partition(
                partition_id
            )

            if partition is None:
                raise KeyError(
                    f"Unknown partition: {partition_id}"
                )

            partition.state = PartitionRoutingState.MERGING

            self.backend.save_partition(
                partition
            )

        plan = PartitionMergePlan(
            merge_id=_new_id("merge"),
            source_partition_ids=list(
                source_partition_ids
            ),
            destination_partition_id=(
                destination_partition_id
            ),
            source_epoch=self._routing_epoch,
            destination_epoch=self._routing_epoch + 1,
        )

        self._merges[plan.merge_id] = plan

        self._emit_event(
            RoutingEventType.PARTITION_MERGED,
            partition_ids=[
                *source_partition_ids,
                destination_partition_id,
            ],
        )

        return plan

    # ------------------------------------------------------------------
    # Rebalancing
    # ------------------------------------------------------------------

    def plan_rebalance(
        self,
        *,
        partition_ids: Optional[Sequence[str]] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> PartitionRoutingEvent:

        selected = list(
            partition_ids
            if partition_ids is not None
            else (
                list(self._active_table.partitions.keys())
                if self._active_table is not None
                else []
            )
        )

        return self._emit_event(
            RoutingEventType.REBALANCE_PLANNED,
            partition_ids=selected,
            metadata=dict(metadata or {}),
        )

    # ------------------------------------------------------------------
    # Recovery / checkpoint
    # ------------------------------------------------------------------

    def checkpoint(
        self,
    ) -> PartitionRoutingCheckpoint:

        table = self._active_table

        if table is None:
            raise RuntimeError(
                "Cannot checkpoint without a published routing table"
            )

        active_count = sum(
            1
            for partition in table.partitions.values()
            if partition.state
            not in (
                PartitionRoutingState.RETIRED,
            )
        )

        checkpoint = PartitionRoutingCheckpoint(
            checkpoint_id=_new_id("routing-checkpoint"),
            namespace=self.namespace,
            routing_table_id=table.table_id,
            routing_table_version=table.version,
            routing_epoch=table.routing_epoch,
            active_partition_count=active_count,
            checksum=table.checksum,
        )

        self.backend.save_checkpoint(
            checkpoint
        )

        self._emit_event(
            RoutingEventType.CHECKPOINT_CREATED,
        )

        return checkpoint

    # ------------------------------------------------------------------
    # Capacity / state
    # ------------------------------------------------------------------

    def capacity(
        self,
    ) -> PartitionRoutingCapacity:

        table = self._active_table

        if table is None:
            return PartitionRoutingCapacity(
                namespace=self.namespace,
                logical_partition_count=0,
                active_partition_count=0,
                degraded_partition_count=0,
                moving_partition_count=0,
                replica_route_count=0,
                routing_table_version=0,
                routing_epoch=self._routing_epoch,
                supports_horizontal_partition_growth=True,
                supports_physical_node_growth=True,
            )

        partitions = list(
            table.partitions.values()
        )

        replica_count = sum(
            len(partition.replicas)
            for partition in partitions
        )

        return PartitionRoutingCapacity(
            namespace=self.namespace,
            logical_partition_count=len(
                partitions
            ),
            active_partition_count=sum(
                1
                for partition in partitions
                if partition.state
                == PartitionRoutingState.ACTIVE
            ),
            degraded_partition_count=sum(
                1
                for partition in partitions
                if partition.state
                == PartitionRoutingState.DEGRADED
            ),
            moving_partition_count=sum(
                1
                for partition in partitions
                if partition.state
                == PartitionRoutingState.MOVING
            ),
            replica_route_count=replica_count,
            routing_table_version=table.version,
            routing_epoch=table.routing_epoch,
            supports_horizontal_partition_growth=True,
            supports_physical_node_growth=True,
        )

    # ------------------------------------------------------------------
    # Architecture description
    # ------------------------------------------------------------------

    def architecture(self) -> Dict[str, object]:

        return {
            "version": ARCHITECTURE_VERSION,
            "scale_target": GLOBAL_SCALE_TARGET,
            "google_scale_capability_target": (
                GOOGLE_SCALE_CAPABILITY_TARGET
            ),
            "google_technology_dependency": (
                GOOGLE_TECHNOLOGY_DEPENDENCY
            ),

            "pipeline": [
                "global_index",
                "partition_router",
                "term_field_document_routing",
                "route_fanout",
                "region_zone_replica_selection",
                "index_read_target",
            ],

            "logical_layers": [
                "global_logical_partition_namespace",
                "versioned_route_table",
                "routing_epoch_fencing",
                "candidate_partition_resolution",
                "replica_selection",
                "locality_aware_routing",
                "failover_routing",
                "migration_aware_routing",
                "split_merge_routing",
                "atomic_route_table_publication",
                "checkpoint_recovery",
            ],

            "routing_modes": [
                mode.value
                for mode in PartitionRoutingMode
            ],

            "partition_states": [
                state.value
                for state in PartitionRoutingState
            ],

            "replica_states": [
                state.value
                for state in RouteTargetState
            ],

            "supports": {
                "term_routing": True,
                "field_routing": True,
                "document_routing": True,
                "hash_routing": True,
                "range_routing": True,
                "prefix_routing": True,
                "broadcast_routing": True,
                "multi_partition_fanout": True,
                "region_locality": True,
                "zone_locality": True,
                "replica_selection": True,
                "replica_failover": True,
                "routing_epoch_fencing": True,
                "partition_movement": True,
                "partition_split": True,
                "partition_merge": True,
                "rebalancing": True,
                "atomic_route_table_swap": True,
                "checkpointing": True,
                "horizontal_partition_growth": True,
                "horizontal_storage_growth": True,
                "replaceable_backend": True,
            },

            "global_constraints": {
                "single_global_router_required": False,
                "single_global_route_table_required": False,
                "single_global_partition_required": False,
                "single_global_replica_required": False,
                "single_global_database_required": False,
                "fixed_global_partition_limit": False,
                "fixed_global_document_limit": False,
                "fixed_global_term_limit": False,
                "fixed_global_replica_limit": False,
            },

            "separation_of_concerns": {
                "logical_partition_identity": True,
                "physical_replica_location": True,
                "routing_epoch": True,
                "storage_placement": True,
                "routing_policy": True,
            },

            "consistency": {
                "immutable_published_route_snapshots": True,
                "atomic_route_table_publication": True,
                "epoch_fencing": True,
                "stale_route_rejection": True,
                "recovery_checkpoints": True,
            },
        }

    # ------------------------------------------------------------------
    # Event helper
    # ------------------------------------------------------------------

    def _emit_event(
        self,
        event_type: RoutingEventType,
        *,
        partition_ids: Optional[Sequence[str]] = None,
        request_id: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> PartitionRoutingEvent:

        event = PartitionRoutingEvent(
            event_id=_new_id("routing-event"),
            event_type=event_type,
            namespace=self.namespace,
            routing_epoch=self._routing_epoch,
            partition_ids=list(
                partition_ids or []
            ),
            request_id=request_id,
            metadata=dict(metadata or {}),
        )

        self._events.append(event)
        self.backend.save_event(event)

        return event

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def active_table(
        self,
    ) -> Optional[PartitionRoutingTable]:
        return self._active_table

    @property
    def events(
        self,
    ) -> List[PartitionRoutingEvent]:
        return list(self._events)

    @property
    def migrations(
        self,
    ) -> Dict[str, PartitionMigration]:
        return dict(self._migrations)

    @property
    def split_plans(
        self,
    ) -> Dict[str, PartitionSplitPlan]:
        return dict(self._splits)

    @property
    def merge_plans(
        self,
    ) -> Dict[str, PartitionMergePlan]:
        return dict(self._merges)


# ---------------------------------------------------------------------------
# Stable aliases
# ---------------------------------------------------------------------------

GlobalIndexRouter = PartitionAwareIndexRouter
PartitionAwareIndexRouting = PartitionAwareIndexRouter
Phase10_6PartitionAwareIndexRouter = PartitionAwareIndexRouter


__all__ = [
    "ARCHITECTURE_VERSION",
    "GLOBAL_SCALE_TARGET",
    "GOOGLE_SCALE_CAPABILITY_TARGET",
    "GOOGLE_TECHNOLOGY_DEPENDENCY",

    "PartitionRoutingState",
    "PartitionRoutingMode",
    "RouteTargetState",
    "ReplicaPreference",
    "RoutingDecisionState",
    "PartitionMigrationState",
    "RoutingEventType",

    "IndexPartitionKey",
    "RoutingToken",

    "PartitionReplicaRoute",
    "PartitionRoute",
    "PartitionRoutingTable",

    "PartitionRoutingRequest",
    "PartitionRoutingDecision",

    "RouteFanoutPlan",

    "PartitionMigration",
    "PartitionSplitPlan",
    "PartitionMergePlan",

    "PartitionRoutingEvent",
    "PartitionRoutingCheckpoint",
    "PartitionRoutingCapacity",

    "PartitionAwareIndexRoutingBackend",
    "InMemoryPartitionAwareRoutingMetadata",

    "PartitionAwareRoutingPolicy",
    "PartitionAwareIndexRouter",

    "GlobalIndexRouter",
    "PartitionAwareIndexRouting",
    "Phase10_6PartitionAwareIndexRouter",
]
