"""
OUR SEARCH
Phase 10.4 — Replication + Durability

Version:
    replication-durability.v1

Purpose:
    Global replication and durability control architecture for the
    enormous OUR SEARCH index and document-storage platform.

Scale target:
    - Billions of public-Web resources.
    - Potentially trillions of public-Web resources.
    - Google-scale capability target from the beginning.
    - No prototype-first scaling assumptions.

Architecture:

    INDEX / DOCUMENT OBJECT
            |
            v
    REPLICA IDENTITY
            |
            v
    FAILURE-DOMAIN PLACEMENT
            |
      +-----+-----+
      |     |     |
    REGION REGION REGION
      |     |     |
     ZONE  ZONE  ZONE
      |     |     |
     NODE  NODE  NODE
      +-----+-----+
            |
            v
    REPLICA HEALTH
            |
            v
    QUORUM / ACKNOWLEDGEMENT
            |
            v
    DURABILITY STATE
            |
       +----+----+
       |         |
    REPAIR    RECOVERY
       |         |
       +----+----+
            |
            v
    DURABLE OBJECT STATE

Important principles:

    1. Replication is independent from physical storage implementation.
    2. Replica placement is failure-domain aware.
    3. Regions, zones and nodes are explicit domains.
    4. Replicas should avoid correlated failure domains.
    5. Replica health is continuously represented by metadata.
    6. Write durability can require configurable acknowledgements.
    7. Read durability can require configurable healthy replicas.
    8. Failed replicas can enter repair/recovery states.
    9. Repair is represented as a controlled state transition.
    10. Recovery is epoch-aware and fenceable.
    11. Replica generations prevent stale ownership.
    12. Quorum policies are configurable rather than hard-coded.
    13. There is no single global durable copy.
    14. There is no single global storage node.
    15. There is no single global durability database.
    16. There is no fixed global document limit.
    17. There is no fixed global replica limit.
    18. Logical durability is separated from physical storage.
    19. The architecture supports horizontal expansion.
    20. Google technologies are not used as dependencies.

This module is an architecture/control-plane layer.
It does not replace the Phase 10.1, 10.2 or 10.3 storage abstractions.
"""


from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from hashlib import sha256
from typing import Dict, List, Mapping, Optional, Protocol, Sequence, Tuple
from uuid import uuid4
from datetime import datetime, timezone


# ============================================================================
# Utilities
# ============================================================================

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _hash(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex}"


# ============================================================================
# Enumerations
# ============================================================================

class ReplicaLifecycleState(str, Enum):
    BUILDING = "building"
    ACTIVE = "active"
    DEGRADED = "degraded"
    REPAIRING = "repairing"
    RECOVERING = "recovering"
    STALE = "stale"
    RETIRED = "retired"
    DELETED = "deleted"


class ReplicaHealthState(str, Enum):
    UNKNOWN = "unknown"
    HEALTHY = "healthy"
    SUSPECT = "suspect"
    UNHEALTHY = "unhealthy"
    RECOVERING = "recovering"


class DurabilityState(str, Enum):
    BUILDING = "building"
    DURABLE = "durable"
    DEGRADED = "degraded"
    AT_RISK = "at_risk"
    RECOVERING = "recovering"
    LOST = "lost"
    RETIRED = "retired"


class FailureDomainType(str, Enum):
    GLOBAL = "global"
    REGION = "region"
    ZONE = "zone"
    NODE = "node"


class AcknowledgementType(str, Enum):
    MEMORY = "memory"
    LOCAL_DURABLE = "local_durable"
    ZONE_DURABLE = "zone_durable"
    REGION_DURABLE = "region_durable"


class RepairState(str, Enum):
    NONE = "none"
    QUEUED = "queued"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"


class RecoveryState(str, Enum):
    NONE = "none"
    QUEUED = "queued"
    RUNNING = "running"
    FENCED = "fenced"
    COMPLETED = "completed"
    FAILED = "failed"


class DurabilityEventType(str, Enum):
    REPLICA_CREATED = "replica_created"
    REPLICA_ACTIVATED = "replica_activated"
    REPLICA_DEGRADED = "replica_degraded"
    REPLICA_FAILED = "replica_failed"
    REPLICA_REPAIR_STARTED = "replica_repair_started"
    REPLICA_REPAIR_COMPLETED = "replica_repair_completed"
    REPLICA_RECOVERY_STARTED = "replica_recovery_started"
    REPLICA_RECOVERY_COMPLETED = "replica_recovery_completed"
    QUORUM_CHANGED = "quorum_changed"
    DURABILITY_CHANGED = "durability_changed"
    FAILURE_DOMAIN_CHANGED = "failure_domain_changed"
    CHECKPOINT_CREATED = "checkpoint_created"


# ============================================================================
# Core identities
# ============================================================================

@dataclass(frozen=True)
class ReplicatedObjectIdentity:
    """
    Logical identity of an object that requires durability.

    object_id:
        Stable logical object identifier.

    object_type:
        Document, content version, index segment, posting block,
        metadata object, etc.

    generation:
        Logical ownership/content generation.

    fingerprint:
        Content/object fingerprint.
    """

    object_id: str
    object_type: str
    generation: int
    fingerprint: str
    created_at: str = field(default_factory=_now)


@dataclass(frozen=True)
class FailureDomain:
    """
    Explicit physical failure-domain identity.

    Failure domains allow the placement system to avoid putting every
    replica inside the same correlated failure boundary.
    """

    domain_id: str
    domain_type: FailureDomainType
    parent_domain_id: Optional[str]
    region_id: Optional[str]
    zone_id: Optional[str]
    node_id: Optional[str]
    active: bool = True
    created_at: str = field(default_factory=_now)


@dataclass(frozen=True)
class ReplicaPlacement:
    """
    Placement identity for one replica.
    """

    region_id: str
    zone_id: str
    node_id: str
    failure_domain_chain: Tuple[str, ...]


@dataclass(frozen=True)
class DurableReplica:
    """
    Logical metadata describing one durable replica.

    The actual bytes remain owned by the storage backend.
    """

    replica_id: str
    object_id: str
    generation: int

    region_id: str
    zone_id: str
    node_id: str

    failure_domain_chain: Tuple[str, ...]

    lifecycle_state: ReplicaLifecycleState
    health_state: ReplicaHealthState

    acknowledgement_type: AcknowledgementType

    byte_size: int
    checksum: str

    replica_epoch: int
    source_replica_id: Optional[str]

    repair_state: RepairState = RepairState.NONE
    recovery_state: RecoveryState = RecoveryState.NONE

    created_at: str = field(default_factory=_now)
    updated_at: str = field(default_factory=_now)


@dataclass(frozen=True)
class ReplicaHealth:
    """
    Health observation for a replica.
    """

    replica_id: str
    health_state: ReplicaHealthState

    last_heartbeat_at: str
    last_verified_at: Optional[str]

    checksum_verified: bool
    readable: bool
    writable: bool

    consecutive_failures: int
    observation_epoch: int

    observed_at: str = field(default_factory=_now)


@dataclass(frozen=True)
class QuorumPolicy:
    """
    Write/read durability quorum policy.

    The policy is intentionally expressed in logical terms rather than
    assuming a particular storage technology.
    """

    replication_factor: int

    write_acknowledgements: int
    read_minimum_healthy_replicas: int

    required_distinct_regions: int
    required_distinct_zones: int
    required_distinct_nodes: int

    acknowledgement_type: AcknowledgementType

    allow_degraded_reads: bool = False
    allow_stale_reads: bool = False

    def __post_init__(self) -> None:
        if self.replication_factor <= 0:
            raise ValueError(
                "replication_factor must be positive"
            )

        if not 0 < self.write_acknowledgements <= self.replication_factor:
            raise ValueError(
                "write_acknowledgements must be within replication factor"
            )

        if not 0 < self.read_minimum_healthy_replicas <= self.replication_factor:
            raise ValueError(
                "read_minimum_healthy_replicas must be within replication factor"
            )

        if self.required_distinct_regions <= 0:
            raise ValueError(
                "required_distinct_regions must be positive"
            )

        if self.required_distinct_zones <= 0:
            raise ValueError(
                "required_distinct_zones must be positive"
            )

        if self.required_distinct_nodes <= 0:
            raise ValueError(
                "required_distinct_nodes must be positive"
            )


@dataclass(frozen=True)
class DurabilityAssessment:
    """
    Current durability assessment for one logical object.
    """

    object_id: str
    generation: int

    durability_state: DurabilityState

    total_replicas: int
    healthy_replicas: int
    active_replicas: int

    distinct_regions: int
    distinct_zones: int
    distinct_nodes: int

    required_replication_factor: int
    required_write_acknowledgements: int
    required_healthy_replicas: int

    quorum_satisfied: bool
    placement_policy_satisfied: bool

    assessed_at: str = field(default_factory=_now)


@dataclass(frozen=True)
class RepairPlan:
    """
    Controlled repair plan for an under-replicated object.
    """

    repair_id: str
    object_id: str
    generation: int

    source_replica_id: str
    target_placement: ReplicaPlacement

    missing_bytes: int
    expected_checksum: str

    state: RepairState

    created_at: str = field(default_factory=_now)
    updated_at: str = field(default_factory=_now)


@dataclass(frozen=True)
class RecoveryPlan:
    """
    Recovery plan for a failed or degraded replica/object.
    """

    recovery_id: str
    object_id: str
    generation: int

    source_replica_ids: Tuple[str, ...]
    target_placement: ReplicaPlacement

    recovery_epoch: int
    state: RecoveryState

    created_at: str = field(default_factory=_now)
    updated_at: str = field(default_factory=_now)


@dataclass(frozen=True)
class DurabilityCheckpoint:
    """
    Control-plane durability checkpoint.
    """

    checkpoint_id: str
    epoch: int

    object_count: int
    replica_count: int

    healthy_replica_count: int
    degraded_replica_count: int

    logical_bytes: int
    replicated_bytes: int

    durable_object_count: int
    degraded_object_count: int
    at_risk_object_count: int

    created_at: str = field(default_factory=_now)


@dataclass(frozen=True)
class DurabilityEvent:
    """
    Append-oriented durability event.
    """

    event_id: str
    event_type: DurabilityEventType

    object_id: Optional[str]
    replica_id: Optional[str]

    epoch: int

    payload: Mapping[str, object]

    created_at: str = field(default_factory=_now)


@dataclass(frozen=True)
class DurabilityCapacity:
    """
    Capacity and durability accounting view.

    These are observed values, not global hard limits.
    """

    object_count: int
    replica_count: int

    healthy_replica_count: int
    degraded_replica_count: int

    logical_bytes: int
    replicated_bytes: int

    durable_object_count: int
    degraded_object_count: int
    at_risk_object_count: int


# ============================================================================
# Backend contract
# ============================================================================

class ReplicationDurabilityBackend(Protocol):
    """
    Replaceable durability metadata backend.

    A future distributed implementation can store these records across
    many machines/regions without changing the logical architecture.
    """

    def put_object(
        self,
        identity: ReplicatedObjectIdentity,
    ) -> None:
        ...

    def get_object(
        self,
        object_id: str,
    ) -> Optional[ReplicatedObjectIdentity]:
        ...

    def put_failure_domain(
        self,
        domain: FailureDomain,
    ) -> None:
        ...

    def get_failure_domain(
        self,
        domain_id: str,
    ) -> Optional[FailureDomain]:
        ...

    def put_replica(
        self,
        replica: DurableReplica,
    ) -> None:
        ...

    def get_replica(
        self,
        replica_id: str,
    ) -> Optional[DurableReplica]:
        ...

    def put_health(
        self,
        health: ReplicaHealth,
    ) -> None:
        ...

    def get_health(
        self,
        replica_id: str,
    ) -> Optional[ReplicaHealth]:
        ...

    def put_repair_plan(
        self,
        plan: RepairPlan,
    ) -> None:
        ...

    def put_recovery_plan(
        self,
        plan: RecoveryPlan,
    ) -> None:
        ...

    def append_event(
        self,
        event: DurabilityEvent,
    ) -> None:
        ...

    def save_checkpoint(
        self,
        checkpoint: DurabilityCheckpoint,
    ) -> None:
        ...


# ============================================================================
# Reference backend
# ============================================================================

class InMemoryReplicationDurabilityMetadata:
    """
    Reference metadata backend.

    This is intentionally a control-plane reference implementation.

    It is NOT the final physical storage layer and does not impose a
    single-file global storage architecture on OUR SEARCH.
    """

    def __init__(self) -> None:
        self.objects: Dict[
            str,
            ReplicatedObjectIdentity,
        ] = {}

        self.failure_domains: Dict[
            str,
            FailureDomain,
        ] = {}

        self.replicas: Dict[
            str,
            DurableReplica,
        ] = {}

        self.health: Dict[
            str,
            ReplicaHealth,
        ] = {}

        self.repairs: Dict[
            str,
            RepairPlan,
        ] = {}

        self.recoveries: Dict[
            str,
            RecoveryPlan,
        ] = {}

        self.events: List[
            DurabilityEvent
        ] = []

        self.checkpoints: Dict[
            int,
            DurabilityCheckpoint,
        ] = {}

    def put_object(
        self,
        identity: ReplicatedObjectIdentity,
    ) -> None:
        self.objects[identity.object_id] = identity

    def get_object(
        self,
        object_id: str,
    ) -> Optional[ReplicatedObjectIdentity]:
        return self.objects.get(object_id)

    def put_failure_domain(
        self,
        domain: FailureDomain,
    ) -> None:
        self.failure_domains[domain.domain_id] = domain

    def get_failure_domain(
        self,
        domain_id: str,
    ) -> Optional[FailureDomain]:
        return self.failure_domains.get(domain_id)

    def put_replica(
        self,
        replica: DurableReplica,
    ) -> None:
        self.replicas[replica.replica_id] = replica

    def get_replica(
        self,
        replica_id: str,
    ) -> Optional[DurableReplica]:
        return self.replicas.get(replica_id)

    def put_health(
        self,
        health: ReplicaHealth,
    ) -> None:
        self.health[health.replica_id] = health

    def get_health(
        self,
        replica_id: str,
    ) -> Optional[ReplicaHealth]:
        return self.health.get(replica_id)

    def put_repair_plan(
        self,
        plan: RepairPlan,
    ) -> None:
        self.repairs[plan.repair_id] = plan

    def put_recovery_plan(
        self,
        plan: RecoveryPlan,
    ) -> None:
        self.recoveries[plan.recovery_id] = plan

    def append_event(
        self,
        event: DurabilityEvent,
    ) -> None:
        self.events.append(event)

    def save_checkpoint(
        self,
        checkpoint: DurabilityCheckpoint,
    ) -> None:
        self.checkpoints[checkpoint.epoch] = checkpoint


# ============================================================================
# Global policy
# ============================================================================

@dataclass(frozen=True)
class ReplicationDurabilityPolicy:
    """
    Global replication/durability policy.

    These are architecture policy parameters and can later be tuned
    independently of the logical storage contracts.
    """

    replication_factor: int = 3

    write_acknowledgements: int = 2
    read_minimum_healthy_replicas: int = 1

    required_distinct_regions: int = 2
    required_distinct_zones: int = 2
    required_distinct_nodes: int = 3

    acknowledgement_type: AcknowledgementType = (
        AcknowledgementType.REGION_DURABLE
    )

    heartbeat_timeout_seconds: int = 30
    repair_failure_threshold: int = 2

    checkpoint_epoch_interval: int = 1

    allow_degraded_reads: bool = False
    allow_stale_reads: bool = False

    def __post_init__(self) -> None:
        if self.replication_factor <= 0:
            raise ValueError(
                "replication_factor must be positive"
            )

        if not (
            0 < self.write_acknowledgements
            <= self.replication_factor
        ):
            raise ValueError(
                "write_acknowledgements must be within replication factor"
            )

        if not (
            0 < self.read_minimum_healthy_replicas
            <= self.replication_factor
        ):
            raise ValueError(
                "read_minimum_healthy_replicas must be within replication factor"
            )

        if self.required_distinct_regions <= 0:
            raise ValueError(
                "required_distinct_regions must be positive"
            )

        if self.required_distinct_zones <= 0:
            raise ValueError(
                "required_distinct_zones must be positive"
            )

        if self.required_distinct_nodes <= 0:
            raise ValueError(
                "required_distinct_nodes must be positive"
            )

        if self.heartbeat_timeout_seconds <= 0:
            raise ValueError(
                "heartbeat_timeout_seconds must be positive"
            )

        if self.repair_failure_threshold <= 0:
            raise ValueError(
                "repair_failure_threshold must be positive"
            )

        if self.checkpoint_epoch_interval <= 0:
            raise ValueError(
                "checkpoint_epoch_interval must be positive"
            )

    def quorum_policy(self) -> QuorumPolicy:
        return QuorumPolicy(
            replication_factor=self.replication_factor,
            write_acknowledgements=self.write_acknowledgements,
            read_minimum_healthy_replicas=(
                self.read_minimum_healthy_replicas
            ),
            required_distinct_regions=(
                self.required_distinct_regions
            ),
            required_distinct_zones=(
                self.required_distinct_zones
            ),
            required_distinct_nodes=(
                self.required_distinct_nodes
            ),
            acknowledgement_type=self.acknowledgement_type,
            allow_degraded_reads=(
                self.allow_degraded_reads
            ),
            allow_stale_reads=(
                self.allow_stale_reads
            ),
        )


# ============================================================================
# Main architecture
# ============================================================================

class ReplicationDurabilityArchitecture:
    """
    Global replication + durability control architecture.

    Responsibilities:

        - logical object identity
        - failure-domain modeling
        - anti-correlated replica placement
        - replica lifecycle
        - replica health
        - acknowledgement policies
        - quorum assessment
        - durability assessment
        - repair planning
        - recovery planning
        - epoch fencing
        - durability checkpointing
        - capacity accounting
        - backend abstraction

    This is designed as a control-plane layer for an enormous
    distributed search platform.
    """

    ARCHITECTURE_VERSION = (
        "replication-durability.v1"
    )

    SCALE_TARGET = (
        "billions_to_trillions_of_public_web_resources"
    )

    GOOGLE_SCALE_CAPABILITY_TARGET = True
    GOOGLE_TECHNOLOGY_DEPENDENCY = False

    FORBIDDEN_SINGLE_GLOBAL_ASSUMPTIONS = (
        "single_global_replica",
        "single_global_storage_node",
        "single_global_durability_database",
        "single_global_replica_log",
        "fixed_global_object_limit",
        "fixed_global_replica_limit",
    )

    def __init__(
        self,
        backend: Optional[
            ReplicationDurabilityBackend
        ] = None,
        policy: Optional[
            ReplicationDurabilityPolicy
        ] = None,
    ) -> None:
        self.backend = (
            backend
            if backend is not None
            else InMemoryReplicationDurabilityMetadata()
        )

        self.policy = (
            policy
            if policy is not None
            else ReplicationDurabilityPolicy()
        )

        self._epoch = 0

    # ------------------------------------------------------------------
    # Epoch management
    # ------------------------------------------------------------------

    def current_epoch(self) -> int:
        return self._epoch

    def advance_epoch(self) -> int:
        self._epoch += 1
        return self._epoch

    # ------------------------------------------------------------------
    # Object identity
    # ------------------------------------------------------------------

    def register_object(
        self,
        object_id: str,
        object_type: str,
        generation: int,
        fingerprint: str,
    ) -> ReplicatedObjectIdentity:
        if not object_id:
            raise ValueError(
                "object_id must not be empty"
            )

        if not object_type:
            raise ValueError(
                "object_type must not be empty"
            )

        if generation < 0:
            raise ValueError(
                "generation must be non-negative"
            )

        if not fingerprint:
            raise ValueError(
                "fingerprint must not be empty"
            )

        identity = ReplicatedObjectIdentity(
            object_id=object_id,
            object_type=object_type,
            generation=generation,
            fingerprint=fingerprint,
        )

        self.backend.put_object(identity)

        self._emit_event(
            event_type=DurabilityEventType.REPLICA_CREATED,
            object_id=object_id,
            epoch=self._epoch,
            payload={
                "object_type": object_type,
                "generation": generation,
            },
        )

        return identity

    # ------------------------------------------------------------------
    # Failure-domain registration
    # ------------------------------------------------------------------

    def register_failure_domain(
        self,
        domain_id: str,
        domain_type: FailureDomainType,
        parent_domain_id: Optional[str] = None,
        region_id: Optional[str] = None,
        zone_id: Optional[str] = None,
        node_id: Optional[str] = None,
    ) -> FailureDomain:
        if not domain_id:
            raise ValueError(
                "domain_id must not be empty"
            )

        domain = FailureDomain(
            domain_id=domain_id,
            domain_type=domain_type,
            parent_domain_id=parent_domain_id,
            region_id=region_id,
            zone_id=zone_id,
            node_id=node_id,
        )

        self.backend.put_failure_domain(domain)

        self._emit_event(
            event_type=DurabilityEventType.FAILURE_DOMAIN_CHANGED,
            epoch=self._epoch,
            payload={
                "domain_id": domain_id,
                "domain_type": domain_type.value,
                "parent_domain_id": parent_domain_id,
                "region_id": region_id,
                "zone_id": zone_id,
                "node_id": node_id,
            },
        )

        return domain

    # ------------------------------------------------------------------
    # Placement validation
    # ------------------------------------------------------------------

    def validate_placement(
        self,
        object_id: str,
        candidate: ReplicaPlacement,
    ) -> bool:
        replicas = self._object_replicas(
            object_id
        )

        existing_regions = {
            replica.region_id
            for replica in replicas
            if replica.lifecycle_state
            not in {
                ReplicaLifecycleState.DELETED,
                ReplicaLifecycleState.RETIRED,
            }
        }

        existing_zones = {
            replica.zone_id
            for replica in replicas
            if replica.lifecycle_state
            not in {
                ReplicaLifecycleState.DELETED,
                ReplicaLifecycleState.RETIRED,
            }
        }

        existing_nodes = {
            replica.node_id
            for replica in replicas
            if replica.lifecycle_state
            not in {
                ReplicaLifecycleState.DELETED,
                ReplicaLifecycleState.RETIRED,
            }
        }

        if (
            candidate.node_id
            in existing_nodes
        ):
            return False

        projected_regions = set(
            existing_regions
        )
        projected_regions.add(
            candidate.region_id
        )

        projected_zones = set(
            existing_zones
        )
        projected_zones.add(
            candidate.zone_id
        )

        projected_nodes = set(
            existing_nodes
        )
        projected_nodes.add(
            candidate.node_id
        )

        if (
            len(projected_regions)
            < self.policy.required_distinct_regions
            and len(replicas) + 1
            >= self.policy.replication_factor
        ):
            return False

        if (
            len(projected_zones)
            < self.policy.required_distinct_zones
            and len(replicas) + 1
            >= self.policy.replication_factor
        ):
            return False

        if (
            len(projected_nodes)
            < self.policy.required_distinct_nodes
            and len(replicas) + 1
            >= self.policy.replication_factor
        ):
            return False

        return True

    # ------------------------------------------------------------------
    # Replica creation
    # ------------------------------------------------------------------

    def create_replica(
        self,
        object_id: str,
        generation: int,
        placement: ReplicaPlacement,
        byte_size: int,
        checksum: str,
        source_replica_id: Optional[str] = None,
        acknowledgement_type: Optional[
            AcknowledgementType
        ] = None,
    ) -> DurableReplica:
        if byte_size < 0:
            raise ValueError(
                "byte_size must be non-negative"
            )

        if not checksum:
            raise ValueError(
                "checksum must not be empty"
            )

        object_identity = self.backend.get_object(
            object_id
        )

        if object_identity is None:
            raise KeyError(
                f"object not registered: {object_id}"
            )

        if object_identity.generation != generation:
            raise ValueError(
                "generation does not match object identity"
            )

        if not self.validate_placement(
            object_id,
            placement,
        ):
            raise ValueError(
                "replica placement violates failure-domain policy"
            )

        self.advance_epoch()

        replica = DurableReplica(
            replica_id=_new_id(
                "durable-replica"
            ),
            object_id=object_id,
            generation=generation,
            region_id=placement.region_id,
            zone_id=placement.zone_id,
            node_id=placement.node_id,
            failure_domain_chain=(
                placement.failure_domain_chain
            ),
            lifecycle_state=(
                ReplicaLifecycleState.BUILDING
            ),
            health_state=(
                ReplicaHealthState.UNKNOWN
            ),
            acknowledgement_type=(
                acknowledgement_type
                if acknowledgement_type is not None
                else self.policy.acknowledgement_type
            ),
            byte_size=byte_size,
            checksum=checksum,
            replica_epoch=self._epoch,
            source_replica_id=source_replica_id,
        )

        self.backend.put_replica(replica)

        self._emit_event(
            event_type=DurabilityEventType.REPLICA_CREATED,
            object_id=object_id,
            replica_id=replica.replica_id,
            epoch=self._epoch,
            payload={
                "region_id": placement.region_id,
                "zone_id": placement.zone_id,
                "node_id": placement.node_id,
                "generation": generation,
            },
        )

        return replica

    # ------------------------------------------------------------------
    # Replica activation
    # ------------------------------------------------------------------

    def activate_replica(
        self,
        replica_id: str,
    ) -> DurableReplica:
        replica = self.backend.get_replica(
            replica_id
        )

        if replica is None:
            raise KeyError(
                f"replica not found: {replica_id}"
            )

        self.advance_epoch()

        active = DurableReplica(
            replica_id=replica.replica_id,
            object_id=replica.object_id,
            generation=replica.generation,
            region_id=replica.region_id,
            zone_id=replica.zone_id,
            node_id=replica.node_id,
            failure_domain_chain=(
                replica.failure_domain_chain
            ),
            lifecycle_state=(
                ReplicaLifecycleState.ACTIVE
            ),
            health_state=(
                ReplicaHealthState.HEALTHY
            ),
            acknowledgement_type=(
                replica.acknowledgement_type
            ),
            byte_size=replica.byte_size,
            checksum=replica.checksum,
            replica_epoch=self._epoch,
            source_replica_id=(
                replica.source_replica_id
            ),
            repair_state=(
                replica.repair_state
            ),
            recovery_state=(
                replica.recovery_state
            ),
            created_at=replica.created_at,
            updated_at=_now(),
        )

        self.backend.put_replica(active)

        health = ReplicaHealth(
            replica_id=replica_id,
            health_state=ReplicaHealthState.HEALTHY,
            last_heartbeat_at=_now(),
            last_verified_at=_now(),
            checksum_verified=True,
            readable=True,
            writable=True,
            consecutive_failures=0,
            observation_epoch=self._epoch,
        )

        self.backend.put_health(health)

        self._emit_event(
            event_type=DurabilityEventType.REPLICA_ACTIVATED,
            object_id=replica.object_id,
            replica_id=replica_id,
            epoch=self._epoch,
            payload={
                "generation": replica.generation,
            },
        )

        return active

    # ------------------------------------------------------------------
    # Health updates
    # ------------------------------------------------------------------

    def update_replica_health(
        self,
        replica_id: str,
        health_state: ReplicaHealthState,
        checksum_verified: bool,
        readable: bool,
        writable: bool,
        consecutive_failures: int = 0,
    ) -> ReplicaHealth:
        replica = self.backend.get_replica(
            replica_id
        )

        if replica is None:
            raise KeyError(
                f"replica not found: {replica_id}"
            )

        if consecutive_failures < 0:
            raise ValueError(
                "consecutive_failures must be non-negative"
            )

        self.advance_epoch()

        health = ReplicaHealth(
            replica_id=replica_id,
            health_state=health_state,
            last_heartbeat_at=_now(),
            last_verified_at=(
                _now()
                if checksum_verified
                else None
            ),
            checksum_verified=checksum_verified,
            readable=readable,
            writable=writable,
            consecutive_failures=consecutive_failures,
            observation_epoch=self._epoch,
        )

        self.backend.put_health(health)

        lifecycle_state = (
            replica.lifecycle_state
        )

        if (
            health_state
            == ReplicaHealthState.HEALTHY
        ):
            lifecycle_state = (
                ReplicaLifecycleState.ACTIVE
            )

        elif (
            health_state
            in {
                ReplicaHealthState.SUSPECT,
                ReplicaHealthState.UNHEALTHY,
            }
        ):
            lifecycle_state = (
                ReplicaLifecycleState.DEGRADED
            )

        elif (
            health_state
            == ReplicaHealthState.RECOVERING
        ):
            lifecycle_state = (
                ReplicaLifecycleState.RECOVERING
            )

        updated_replica = DurableReplica(
            replica_id=replica.replica_id,
            object_id=replica.object_id,
            generation=replica.generation,
            region_id=replica.region_id,
            zone_id=replica.zone_id,
            node_id=replica.node_id,
            failure_domain_chain=(
                replica.failure_domain_chain
            ),
            lifecycle_state=lifecycle_state,
            health_state=health_state,
            acknowledgement_type=(
                replica.acknowledgement_type
            ),
            byte_size=replica.byte_size,
            checksum=replica.checksum,
            replica_epoch=self._epoch,
            source_replica_id=(
                replica.source_replica_id
            ),
            repair_state=(
                replica.repair_state
            ),
            recovery_state=(
                replica.recovery_state
            ),
            created_at=replica.created_at,
            updated_at=_now(),
        )

        self.backend.put_replica(
            updated_replica
        )

        if (
            health_state
            == ReplicaHealthState.HEALTHY
        ):
            event_type = (
                DurabilityEventType.REPLICA_ACTIVATED
            )
        elif (
            health_state
            == ReplicaHealthState.UNHEALTHY
        ):
            event_type = (
                DurabilityEventType.REPLICA_FAILED
            )
        else:
            event_type = (
                DurabilityEventType.REPLICA_DEGRADED
            )

        self._emit_event(
            event_type=event_type,
            object_id=replica.object_id,
            replica_id=replica_id,
            epoch=self._epoch,
            payload={
                "health_state": health_state.value,
                "checksum_verified": (
                    checksum_verified
                ),
                "readable": readable,
                "writable": writable,
                "consecutive_failures": (
                    consecutive_failures
                ),
            },
        )

        return updated_replica

    # ------------------------------------------------------------------
    # Object replica retrieval
    # ------------------------------------------------------------------

    def _object_replicas(
        self,
        object_id: str,
    ) -> List[DurableReplica]:
        if not isinstance(
            self.backend,
            InMemoryReplicationDurabilityMetadata,
        ):
            return []

        return [
            replica
            for replica
            in self.backend.replicas.values()
            if replica.object_id == object_id
        ]

    # ------------------------------------------------------------------
    # Durability assessment
    # ------------------------------------------------------------------

    def assess_durability(
        self,
        object_id: str,
    ) -> DurabilityAssessment:
        identity = self.backend.get_object(
            object_id
        )

        if identity is None:
            raise KeyError(
                f"object not found: {object_id}"
            )

        replicas = self._object_replicas(
            object_id
        )

        active_replicas = [
            replica
            for replica in replicas
            if replica.lifecycle_state
            not in {
                ReplicaLifecycleState.DELETED,
                ReplicaLifecycleState.RETIRED,
                ReplicaLifecycleState.STALE,
            }
        ]

        healthy_replicas = [
            replica
            for replica in active_replicas
            if replica.health_state
            == ReplicaHealthState.HEALTHY
        ]

        regions = {
            replica.region_id
            for replica in healthy_replicas
        }

        zones = {
            replica.zone_id
            for replica in healthy_replicas
        }

        nodes = {
            replica.node_id
            for replica in healthy_replicas
        }

        quorum = self.policy.quorum_policy()

        quorum_satisfied = (
            len(healthy_replicas)
            >= quorum.read_minimum_healthy_replicas
            and len(healthy_replicas)
            >= quorum.write_acknowledgements
        )

        placement_policy_satisfied = (
            len(regions)
            >= quorum.required_distinct_regions
            and len(zones)
            >= quorum.required_distinct_zones
            and len(nodes)
            >= quorum.required_distinct_nodes
        )

        if (
            len(healthy_replicas)
            >= quorum.replication_factor
            and quorum_satisfied
            and placement_policy_satisfied
        ):
            state = DurabilityState.DURABLE

        elif (
            len(healthy_replicas) > 0
            and quorum_satisfied
        ):
            state = DurabilityState.DEGRADED

        elif len(healthy_replicas) > 0:
            state = DurabilityState.AT_RISK

        else:
            state = DurabilityState.LOST

        assessment = DurabilityAssessment(
            object_id=object_id,
            generation=identity.generation,
            durability_state=state,
            total_replicas=len(replicas),
            healthy_replicas=len(
                healthy_replicas
            ),
            active_replicas=len(
                active_replicas
            ),
            distinct_regions=len(regions),
            distinct_zones=len(zones),
            distinct_nodes=len(nodes),
            required_replication_factor=(
                quorum.replication_factor
            ),
            required_write_acknowledgements=(
                quorum.write_acknowledgements
            ),
            required_healthy_replicas=(
                quorum.read_minimum_healthy_replicas
            ),
            quorum_satisfied=quorum_satisfied,
            placement_policy_satisfied=(
                placement_policy_satisfied
            ),
        )

        self._emit_event(
            event_type=DurabilityEventType.DURABILITY_CHANGED,
            object_id=object_id,
            epoch=self._epoch,
            payload={
                "durability_state": state.value,
                "healthy_replicas": len(
                    healthy_replicas
                ),
                "total_replicas": len(
                    replicas
                ),
                "distinct_regions": len(
                    regions
                ),
                "distinct_zones": len(
                    zones
                ),
                "distinct_nodes": len(
                    nodes
                ),
            },
        )

        return assessment

    # ------------------------------------------------------------------
    # Repair planning
    # ------------------------------------------------------------------

    def create_repair_plan(
        self,
        object_id: str,
        target_placement: ReplicaPlacement,
    ) -> RepairPlan:
        assessment = self.assess_durability(
            object_id
        )

        if assessment.durability_state == (
            DurabilityState.DURABLE
        ):
            raise ValueError(
                "object does not currently require repair"
            )

        replicas = [
            replica
            for replica
            in self._object_replicas(
                object_id
            )
            if (
                replica.health_state
                == ReplicaHealthState.HEALTHY
                and replica.lifecycle_state
                not in {
                    ReplicaLifecycleState.DELETED,
                    ReplicaLifecycleState.RETIRED,
                    ReplicaLifecycleState.STALE,
                }
            )
        ]

        if not replicas:
            raise ValueError(
                "no healthy source replica available for repair"
            )

        source = replicas[0]

        if not self.validate_placement(
            object_id,
            target_placement,
        ):
            raise ValueError(
                "repair target violates failure-domain policy"
            )

        identity = self.backend.get_object(
            object_id
        )

        if identity is None:
            raise KeyError(
                f"object not found: {object_id}"
            )

        self.advance_epoch()

        plan = RepairPlan(
            repair_id=_new_id(
                "repair-plan"
            ),
            object_id=object_id,
            generation=identity.generation,
            source_replica_id=source.replica_id,
            target_placement=target_placement,
            missing_bytes=source.byte_size,
            expected_checksum=source.checksum,
            state=RepairState.QUEUED,
        )

        self.backend.put_repair_plan(
            plan
        )

        self._emit_event(
            event_type=(
                DurabilityEventType.REPLICA_REPAIR_STARTED
            ),
            object_id=object_id,
            replica_id=source.replica_id,
            epoch=self._epoch,
            payload={
                "repair_id": plan.repair_id,
                "target_region": (
                    target_placement.region_id
                ),
                "target_zone": (
                    target_placement.zone_id
                ),
                "target_node": (
                    target_placement.node_id
                ),
            },
        )

        return plan

    # ------------------------------------------------------------------
    # Recovery planning
    # ------------------------------------------------------------------

    def create_recovery_plan(
        self,
        object_id: str,
        target_placement: ReplicaPlacement,
    ) -> RecoveryPlan:
        identity = self.backend.get_object(
            object_id
        )

        if identity is None:
            raise KeyError(
                f"object not found: {object_id}"
            )

        replicas = self._object_replicas(
            object_id
        )

        source_replica_ids = tuple(
            replica.replica_id
            for replica in replicas
            if (
                replica.health_state
                == ReplicaHealthState.HEALTHY
                and replica.lifecycle_state
                not in {
                    ReplicaLifecycleState.DELETED,
                    ReplicaLifecycleState.RETIRED,
                    ReplicaLifecycleState.STALE,
                }
            )
        )

        if not source_replica_ids:
            raise ValueError(
                "recovery requires at least one healthy source replica"
            )

        if not self.validate_placement(
            object_id,
            target_placement,
        ):
            raise ValueError(
                "recovery target violates failure-domain policy"
            )

        self.advance_epoch()

        plan = RecoveryPlan(
            recovery_id=_new_id(
                "recovery-plan"
            ),
            object_id=object_id,
            generation=identity.generation,
            source_replica_ids=(
                source_replica_ids
            ),
            target_placement=target_placement,
            recovery_epoch=self._epoch,
            state=RecoveryState.QUEUED,
        )

        self.backend.put_recovery_plan(
            plan
        )

        self._emit_event(
            event_type=(
                DurabilityEventType.REPLICA_RECOVERY_STARTED
            ),
            object_id=object_id,
            epoch=self._epoch,
            payload={
                "recovery_id": plan.recovery_id,
                "recovery_epoch": (
                    plan.recovery_epoch
                ),
                "source_replica_count": (
                    len(source_replica_ids)
                ),
                "target_region": (
                    target_placement.region_id
                ),
                "target_zone": (
                    target_placement.zone_id
                ),
                "target_node": (
                    target_placement.node_id
                ),
            },
        )

        return plan

    # ------------------------------------------------------------------
    # Epoch fencing
    # ------------------------------------------------------------------

    def fence_replica(
        self,
        replica_id: str,
    ) -> DurableReplica:
        replica = self.backend.get_replica(
            replica_id
        )

        if replica is None:
            raise KeyError(
                f"replica not found: {replica_id}"
            )

        self.advance_epoch()

        fenced = DurableReplica(
            replica_id=replica.replica_id,
            object_id=replica.object_id,
            generation=replica.generation,
            region_id=replica.region_id,
            zone_id=replica.zone_id,
            node_id=replica.node_id,
            failure_domain_chain=(
                replica.failure_domain_chain
            ),
            lifecycle_state=(
                ReplicaLifecycleState.STALE
            ),
            health_state=(
                ReplicaHealthState.UNHEALTHY
            ),
            acknowledgement_type=(
                replica.acknowledgement_type
            ),
            byte_size=replica.byte_size,
            checksum=replica.checksum,
            replica_epoch=self._epoch,
            source_replica_id=(
                replica.source_replica_id
            ),
            repair_state=(
                replica.repair_state
            ),
            recovery_state=(
                RecoveryState.FENCED
            ),
            created_at=replica.created_at,
            updated_at=_now(),
        )

        self.backend.put_replica(
            fenced
        )

        self._emit_event(
            event_type=(
                DurabilityEventType.REPLICA_FAILED
            ),
            object_id=replica.object_id,
            replica_id=replica_id,
            epoch=self._epoch,
            payload={
                "fenced": True,
                "new_epoch": self._epoch,
            },
        )

        return fenced

    # ------------------------------------------------------------------
    # Checkpoint
    # ------------------------------------------------------------------

    def checkpoint(
        self,
    ) -> DurabilityCheckpoint:
        self.advance_epoch()

        if isinstance(
            self.backend,
            InMemoryReplicationDurabilityMetadata,
        ):
            replicas = list(
                self.backend.replicas.values()
            )

            healthy_replicas = [
                replica
                for replica in replicas
                if replica.health_state
                == ReplicaHealthState.HEALTHY
                and replica.lifecycle_state
                not in {
                    ReplicaLifecycleState.DELETED,
                    ReplicaLifecycleState.RETIRED,
                    ReplicaLifecycleState.STALE,
                }
            ]

            degraded_replicas = [
                replica
                for replica in replicas
                if replica.lifecycle_state
                in {
                    ReplicaLifecycleState.DEGRADED,
                    ReplicaLifecycleState.REPAIRING,
                    ReplicaLifecycleState.RECOVERING,
                }
            ]

            logical_bytes = 0

            for object_id in (
                self.backend.objects.keys()
            ):
                object_replicas = (
                    self._object_replicas(
                        object_id
                    )
                )

                healthy = [
                    replica
                    for replica
                    in object_replicas
                    if replica.health_state
                    == ReplicaHealthState.HEALTHY
                ]

                if healthy:
                    logical_bytes += max(
                        replica.byte_size
                        for replica in healthy
                    )

            replicated_bytes = sum(
                replica.byte_size
                for replica in replicas
                if replica.lifecycle_state
                not in {
                    ReplicaLifecycleState.DELETED,
                    ReplicaLifecycleState.RETIRED,
                }
            )

            durable_count = 0
            degraded_count = 0
            at_risk_count = 0

            for object_id in (
                self.backend.objects.keys()
            ):
                assessment = (
                    self.assess_durability(
                        object_id
                    )
                )

                if (
                    assessment.durability_state
                    == DurabilityState.DURABLE
                ):
                    durable_count += 1

                elif (
                    assessment.durability_state
                    == DurabilityState.DEGRADED
                ):
                    degraded_count += 1

                elif (
                    assessment.durability_state
                    == DurabilityState.AT_RISK
                ):
                    at_risk_count += 1

            checkpoint = DurabilityCheckpoint(
                checkpoint_id=_new_id(
                    "durability-checkpoint"
                ),
                epoch=self._epoch,
                object_count=len(
                    self.backend.objects
                ),
                replica_count=len(
                    replicas
                ),
                healthy_replica_count=len(
                    healthy_replicas
                ),
                degraded_replica_count=len(
                    degraded_replicas
                ),
                logical_bytes=logical_bytes,
                replicated_bytes=(
                    replicated_bytes
                ),
                durable_object_count=(
                    durable_count
                ),
                degraded_object_count=(
                    degraded_count
                ),
                at_risk_object_count=(
                    at_risk_count
                ),
            )

        else:
            checkpoint = DurabilityCheckpoint(
                checkpoint_id=_new_id(
                    "durability-checkpoint"
                ),
                epoch=self._epoch,
                object_count=0,
                replica_count=0,
                healthy_replica_count=0,
                degraded_replica_count=0,
                logical_bytes=0,
                replicated_bytes=0,
                durable_object_count=0,
                degraded_object_count=0,
                at_risk_object_count=0,
            )

        self.backend.save_checkpoint(
            checkpoint
        )

        self._emit_event(
            event_type=(
                DurabilityEventType.CHECKPOINT_CREATED
            ),
            epoch=self._epoch,
            payload={
                "object_count": (
                    checkpoint.object_count
                ),
                "replica_count": (
                    checkpoint.replica_count
                ),
                "healthy_replica_count": (
                    checkpoint.healthy_replica_count
                ),
                "durable_object_count": (
                    checkpoint.durable_object_count
                ),
            },
        )

        return checkpoint

    # ------------------------------------------------------------------
    # Capacity
    # ------------------------------------------------------------------

    def capacity(
        self,
    ) -> DurabilityCapacity:
        if isinstance(
            self.backend,
            InMemoryReplicationDurabilityMetadata,
        ):
            replicas = list(
                self.backend.replicas.values()
            )

            healthy = [
                replica
                for replica in replicas
                if replica.health_state
                == ReplicaHealthState.HEALTHY
                and replica.lifecycle_state
                not in {
                    ReplicaLifecycleState.DELETED,
                    ReplicaLifecycleState.RETIRED,
                    ReplicaLifecycleState.STALE,
                }
            ]

            degraded = [
                replica
                for replica in replicas
                if replica.lifecycle_state
                in {
                    ReplicaLifecycleState.DEGRADED,
                    ReplicaLifecycleState.REPAIRING,
                    ReplicaLifecycleState.RECOVERING,
                }
            ]

            logical_bytes = 0

            for object_id in (
                self.backend.objects.keys()
            ):
                object_replicas = (
                    self._object_replicas(
                        object_id
                    )
                )

                healthy_object_replicas = [
                    replica
                    for replica in object_replicas
                    if replica.health_state
                    == ReplicaHealthState.HEALTHY
                ]

                if healthy_object_replicas:
                    logical_bytes += max(
                        replica.byte_size
                        for replica
                        in healthy_object_replicas
                    )

            replicated_bytes = sum(
                replica.byte_size
                for replica in replicas
                if replica.lifecycle_state
                not in {
                    ReplicaLifecycleState.DELETED,
                    ReplicaLifecycleState.RETIRED,
                }
            )

            durable_count = 0
            degraded_count = 0
            at_risk_count = 0

            for object_id in (
                self.backend.objects.keys()
            ):
                assessment = (
                    self.assess_durability(
                        object_id
                    )
                )

                if (
                    assessment.durability_state
                    == DurabilityState.DURABLE
                ):
                    durable_count += 1

                elif (
                    assessment.durability_state
                    == DurabilityState.DEGRADED
                ):
                    degraded_count += 1

                elif (
                    assessment.durability_state
                    == DurabilityState.AT_RISK
                ):
                    at_risk_count += 1

            return DurabilityCapacity(
                object_count=len(
                    self.backend.objects
                ),
                replica_count=len(
                    replicas
                ),
                healthy_replica_count=len(
                    healthy
                ),
                degraded_replica_count=len(
                    degraded
                ),
                logical_bytes=logical_bytes,
                replicated_bytes=(
                    replicated_bytes
                ),
                durable_object_count=(
                    durable_count
                ),
                degraded_object_count=(
                    degraded_count
                ),
                at_risk_object_count=(
                    at_risk_count
                ),
            )

        return DurabilityCapacity(
            object_count=0,
            replica_count=0,
            healthy_replica_count=0,
            degraded_replica_count=0,
            logical_bytes=0,
            replicated_bytes=0,
            durable_object_count=0,
            degraded_object_count=0,
            at_risk_object_count=0,
        )

    # ------------------------------------------------------------------
    # Event helper
    # ------------------------------------------------------------------

    def _emit_event(
        self,
        event_type: DurabilityEventType,
        object_id: Optional[str] = None,
        replica_id: Optional[str] = None,
        epoch: Optional[int] = None,
        payload: Optional[
            Mapping[str, object]
        ] = None,
    ) -> DurabilityEvent:
        event = DurabilityEvent(
            event_id=_new_id(
                "durability-event"
            ),
            event_type=event_type,
            object_id=object_id,
            replica_id=replica_id,
            epoch=(
                self._epoch
                if epoch is None
                else epoch
            ),
            payload=(
                payload
                if payload is not None
                else {}
            ),
        )

        self.backend.append_event(
            event
        )

        return event

    # ------------------------------------------------------------------
    # Architecture description
    # ------------------------------------------------------------------

    def architecture(
        self,
    ) -> Mapping[str, object]:
        return {
            "version": (
                self.ARCHITECTURE_VERSION
            ),
            "scale_target": (
                self.SCALE_TARGET
            ),
            "google_scale_capability_target": (
                self.GOOGLE_SCALE_CAPABILITY_TARGET
            ),
            "google_technology_dependency": (
                self.GOOGLE_TECHNOLOGY_DEPENDENCY
            ),
            "pipeline": [
                "logical_object",
                "replica_identity",
                "failure_domain_model",
                "anti_correlated_placement",
                "replica_creation",
                "replica_activation",
                "health_monitoring",
                "quorum_policy",
                "durability_assessment",
                "repair_planning",
                "recovery_planning",
                "epoch_fencing",
                "durability_checkpoint",
                "capacity_accounting",
            ],
            "failure_domains": [
                domain.value
                for domain
                in FailureDomainType
            ],
            "replica_states": [
                state.value
                for state
                in ReplicaLifecycleState
            ],
            "health_states": [
                state.value
                for state
                in ReplicaHealthState
            ],
            "durability_states": [
                state.value
                for state
                in DurabilityState
            ],
            "quorum": {
                "replication_factor": (
                    self.policy.replication_factor
                ),
                "write_acknowledgements": (
                    self.policy.write_acknowledgements
                ),
                "read_minimum_healthy_replicas": (
                    self.policy
                    .read_minimum_healthy_replicas
                ),
                "required_distinct_regions": (
                    self.policy
                    .required_distinct_regions
                ),
                "required_distinct_zones": (
                    self.policy
                    .required_distinct_zones
                ),
                "required_distinct_nodes": (
                    self.policy
                    .required_distinct_nodes
                ),
                "acknowledgement_type": (
                    self.policy
                    .acknowledgement_type.value
                ),
            },
            "anti_correlated_replication": True,
            "repair": True,
            "recovery": True,
            "epoch_fencing": True,
            "checkpointing": True,
            "replaceable_backend": True,
            "horizontal_scaling": True,
            "forbidden_single_global_assumptions": list(
                self.FORBIDDEN_SINGLE_GLOBAL_ASSUMPTIONS
            ),
        }


# ============================================================================
# Stable aliases
# ============================================================================

ReplicationDurability = (
    ReplicationDurabilityArchitecture
)

GlobalReplicationDurability = (
    ReplicationDurabilityArchitecture
)

Phase10_4ReplicationDurability = (
    ReplicationDurabilityArchitecture
)


# ============================================================================
# Public contract
# ============================================================================

__all__ = [
    "ReplicaLifecycleState",
    "ReplicaHealthState",
    "DurabilityState",
    "FailureDomainType",
    "AcknowledgementType",
    "RepairState",
    "RecoveryState",
    "DurabilityEventType",
    "ReplicatedObjectIdentity",
    "FailureDomain",
    "ReplicaPlacement",
    "DurableReplica",
    "ReplicaHealth",
    "QuorumPolicy",
    "DurabilityAssessment",
    "RepairPlan",
    "RecoveryPlan",
    "DurabilityCheckpoint",
    "DurabilityEvent",
    "DurabilityCapacity",
    "ReplicationDurabilityBackend",
    "InMemoryReplicationDurabilityMetadata",
    "ReplicationDurabilityPolicy",
    "ReplicationDurabilityArchitecture",
    "ReplicationDurability",
    "GlobalReplicationDurability",
    "Phase10_4ReplicationDurability",
]
