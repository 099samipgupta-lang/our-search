"""
OUR SEARCH
Phase 10.9 — Final Index / Storage Architecture

Version:
    final-index-storage-architecture.v1

Purpose:
    Final control-plane architecture that unifies the complete
    Phase 10 index and storage system.

Phase 10 architecture:

    10.1 Global Index Storage Architecture
        ↓
    10.2 Distributed Inverted Index / Segment Fabric
        ↓
    10.3 Massive Document & Content Storage
        ↓
    10.4 Replication + Durability
        ↓
    10.5 Massive Segment Compaction
        ↓
    10.6 Partition-Aware Index Routing
        ↓
    10.7 Index Recovery + Checkpointing
        ↓
    10.8 Storage Capacity + Tiering
        ↓
    10.9 FINAL INDEX / STORAGE ARCHITECTURE

Scale target:

    billions → potentially trillions of publicly accessible
    Web resources.

Design target:

    Google-scale search capability.

Dependency rule:

    No Google technology.
    No Google Search API.
    No Google index.
    No Google crawler.
    No Google infrastructure.

This module is the final architectural control plane.
It does not replace the specialized Phase 10 subsystems.

Instead, it defines their contracts, lifecycle, dependencies,
ownership boundaries, health state, recovery state, and
global coordination model.

The architecture deliberately avoids:

    - one global index file
    - one global document file
    - one global segment directory
    - one global database
    - one global storage node
    - one global router
    - one global compaction worker
    - one global recovery worker
    - one global migration queue
    - fixed global storage ceilings
    - fixed global document ceilings
    - fixed global partition ceilings
    - fixed global segment ceilings

The system is intended to grow horizontally by adding
partitions, replicas, storage domains, workers, regions,
zones, and capacity.

No tests or benchmarks are performed by this module.
"""

from __future__ import annotations

import hashlib
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Protocol, Sequence


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ARCHITECTURE_VERSION = (
    "final-index-storage-architecture.v1"
)

GLOBAL_SCALE_TARGET = (
    "billions_to_trillions_of_public_web_resources"
)

GOOGLE_SCALE_CAPABILITY_TARGET = True
GOOGLE_TECHNOLOGY_DEPENDENCY = False


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _now() -> float:
    return time.time()


def _hash(value: str) -> str:
    return hashlib.sha256(
        value.encode("utf-8")
    ).hexdigest()


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex}"


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class StorageSubsystem(str, Enum):
    GLOBAL_STORAGE = "global_storage"
    SEGMENT_FABRIC = "segment_fabric"
    DOCUMENT_STORAGE = "document_storage"
    DURABILITY = "durability"
    COMPACTION = "compaction"
    ROUTING = "routing"
    RECOVERY = "recovery"
    CAPACITY_TIERING = "capacity_tiering"


class SubsystemState(str, Enum):
    PLANNED = "planned"
    INITIALIZING = "initializing"
    ACTIVE = "active"
    DEGRADED = "degraded"
    RECOVERING = "recovering"
    DRAINING = "draining"
    RETIRED = "retired"
    FAILED = "failed"


class DataLifecycleState(str, Enum):
    INGESTING = "ingesting"
    INDEXED = "indexed"
    REPLICATED = "replicated"
    COMPACTING = "compacting"
    ROUTABLE = "routable"
    RECOVERING = "recovering"
    TIERING = "tiering"
    RETIRED = "retired"
    DELETED = "deleted"


class ArchitectureEpochState(str, Enum):
    OPEN = "open"
    PREPARING = "preparing"
    PUBLISHING = "publishing"
    PUBLISHED = "published"
    FENCED = "fenced"
    RETIRED = "retired"


class FailureDomain(str, Enum):
    NODE = "node"
    ZONE = "zone"
    REGION = "region"
    STORAGE = "storage"
    SEGMENT = "segment"
    ROUTING = "routing"
    RECOVERY = "recovery"
    CAPACITY = "capacity"
    UNKNOWN = "unknown"


class FinalArchitectureEventType(str, Enum):
    SUBSYSTEM_REGISTERED = "subsystem_registered"
    SUBSYSTEM_STATE_CHANGED = "subsystem_state_changed"

    EPOCH_CREATED = "epoch_created"
    EPOCH_PUBLISHED = "epoch_published"
    EPOCH_FENCED = "epoch_fenced"

    DOCUMENT_FLOW_REGISTERED = "document_flow_registered"

    INDEX_FLOW_REGISTERED = "index_flow_registered"

    CROSS_SUBSYSTEM_DEPENDENCY_REGISTERED = (
        "cross_subsystem_dependency_registered"
    )

    FAILURE_REGISTERED = "failure_registered"

    RECOVERY_REQUIRED = "recovery_required"

    CHECKPOINT_BOUND = "checkpoint_bound"

    CAPACITY_BOUND = "capacity_bound"

    FINAL_ARCHITECTURE_CHECKPOINT = (
        "final_architecture_checkpoint"
    )


# ---------------------------------------------------------------------------
# Subsystem identity
# ---------------------------------------------------------------------------

@dataclass
class StorageSubsystemRecord:
    subsystem_id: str

    subsystem: StorageSubsystem

    state: SubsystemState

    owner_domain: str

    version: str

    epoch: int

    dependencies: List[
        StorageSubsystem
    ] = field(default_factory=list)

    created_at: float = field(
        default_factory=_now
    )

    updated_at: float = field(
        default_factory=_now
    )

    metadata: Dict[str, str] = field(
        default_factory=dict
    )


# ---------------------------------------------------------------------------
# Logical data identity
# ---------------------------------------------------------------------------

@dataclass
class FinalLogicalDataIdentity:
    logical_object_id: str

    document_id: str

    canonical_url: str

    content_version: str

    partition_id: str

    index_generation: str

    document_generation: str

    lifecycle_state: DataLifecycleState

    created_at: float = field(
        default_factory=_now
    )

    metadata: Dict[str, str] = field(
        default_factory=dict
    )


# ---------------------------------------------------------------------------
# Architecture epoch
# ---------------------------------------------------------------------------

@dataclass
class FinalArchitectureEpoch:
    epoch_id: str

    epoch: int

    state: ArchitectureEpochState

    parent_epoch: Optional[int]

    active_subsystems: List[
        StorageSubsystem
    ]

    routing_generation: str

    index_generation: str

    document_generation: str

    checkpoint_generation: str

    capacity_generation: str

    created_at: float = field(
        default_factory=_now
    )

    published_at: Optional[float] = None

    metadata: Dict[str, str] = field(
        default_factory=dict
    )


# ---------------------------------------------------------------------------
# Cross-subsystem dependency
# ---------------------------------------------------------------------------

@dataclass
class StorageSubsystemDependency:
    dependency_id: str

    source: StorageSubsystem

    target: StorageSubsystem

    required: bool

    ordering: str

    failure_propagation: str

    created_at: float = field(
        default_factory=_now
    )

    metadata: Dict[str, str] = field(
        default_factory=dict
    )


# ---------------------------------------------------------------------------
# Document/index flow
# ---------------------------------------------------------------------------

@dataclass
class FinalDataFlowContract:
    flow_id: str

    name: str

    source: StorageSubsystem

    destination: StorageSubsystem

    ordered_stages: List[str]

    durable_boundary: str

    recovery_boundary: str

    routing_boundary: str

    tiering_boundary: str

    created_at: float = field(
        default_factory=_now
    )

    metadata: Dict[str, str] = field(
        default_factory=dict
    )


# ---------------------------------------------------------------------------
# Failure
# ---------------------------------------------------------------------------

@dataclass
class FinalStorageFailure:
    failure_id: str

    subsystem: StorageSubsystem

    failure_domain: FailureDomain

    affected_scope: str

    epoch: int

    detected_at: float = field(
        default_factory=_now
    )

    recovery_required: bool = True

    resolved: bool = False

    metadata: Dict[str, str] = field(
        default_factory=dict
    )


# ---------------------------------------------------------------------------
# Final checkpoint
# ---------------------------------------------------------------------------

@dataclass
class FinalIndexStorageCheckpoint:
    checkpoint_id: str

    epoch: int

    subsystem_versions: Dict[
        str,
        str,
    ]

    active_subsystems: List[
        str
    ]

    logical_data_count: int

    dependency_count: int

    failure_count: int

    checksum: str

    created_at: float = field(
        default_factory=_now
    )

    metadata: Dict[str, str] = field(
        default_factory=dict
    )


# ---------------------------------------------------------------------------
# Architecture capacity
# ---------------------------------------------------------------------------

@dataclass
class FinalIndexStorageCapacity:
    namespace: str

    subsystem_count: int

    dependency_count: int

    flow_count: int

    logical_object_count: int

    failure_count: int

    checkpoint_count: int

    current_epoch: int

    horizontal_scaling: bool

    partition_scaling: bool

    region_scaling: bool

    zone_scaling: bool

    node_scaling: bool

    tier_scaling: bool

    no_global_fixed_ceiling: bool


# ---------------------------------------------------------------------------
# Backend contract
# ---------------------------------------------------------------------------

class FinalIndexStorageBackend(Protocol):

    def save_subsystem(
        self,
        record: StorageSubsystemRecord,
    ) -> None:
        ...

    def save_epoch(
        self,
        epoch: FinalArchitectureEpoch,
    ) -> None:
        ...

    def save_dependency(
        self,
        dependency: StorageSubsystemDependency,
    ) -> None:
        ...

    def save_flow(
        self,
        flow: FinalDataFlowContract,
    ) -> None:
        ...

    def save_failure(
        self,
        failure: FinalStorageFailure,
    ) -> None:
        ...

    def save_checkpoint(
        self,
        checkpoint: FinalIndexStorageCheckpoint,
    ) -> None:
        ...

    def save_event(
        self,
        event: "FinalArchitectureEvent",
    ) -> None:
        ...


# ---------------------------------------------------------------------------
# Reference backend
# ---------------------------------------------------------------------------

class InMemoryFinalIndexStorageMetadata:

    def __init__(self) -> None:

        self.subsystems: Dict[
            str,
            StorageSubsystemRecord,
        ] = {}

        self.epochs: Dict[
            int,
            FinalArchitectureEpoch,
        ] = {}

        self.dependencies: Dict[
            str,
            StorageSubsystemDependency,
        ] = {}

        self.flows: Dict[
            str,
            FinalDataFlowContract,
        ] = {}

        self.failures: Dict[
            str,
            FinalStorageFailure,
        ] = {}

        self.checkpoints: Dict[
            str,
            FinalIndexStorageCheckpoint,
        ] = {}

        self.events: List[
            FinalArchitectureEvent
        ] = []

    def save_subsystem(
        self,
        record: StorageSubsystemRecord,
    ) -> None:

        self.subsystems[
            record.subsystem_id
        ] = record

    def save_epoch(
        self,
        epoch: FinalArchitectureEpoch,
    ) -> None:

        self.epochs[
            epoch.epoch
        ] = epoch

    def save_dependency(
        self,
        dependency: StorageSubsystemDependency,
    ) -> None:

        self.dependencies[
            dependency.dependency_id
        ] = dependency

    def save_flow(
        self,
        flow: FinalDataFlowContract,
    ) -> None:

        self.flows[
            flow.flow_id
        ] = flow

    def save_failure(
        self,
        failure: FinalStorageFailure,
    ) -> None:

        self.failures[
            failure.failure_id
        ] = failure

    def save_checkpoint(
        self,
        checkpoint: FinalIndexStorageCheckpoint,
    ) -> None:

        self.checkpoints[
            checkpoint.checkpoint_id
        ] = checkpoint

    def save_event(
        self,
        event: "FinalArchitectureEvent",
    ) -> None:

        self.events.append(event)


# ---------------------------------------------------------------------------
# Event
# ---------------------------------------------------------------------------

@dataclass
class FinalArchitectureEvent:

    event_id: str

    event_type: FinalArchitectureEventType

    namespace: str

    epoch: int

    subsystem: Optional[
        StorageSubsystem
    ] = None

    subsystem_id: Optional[str] = None

    dependency_id: Optional[str] = None

    flow_id: Optional[str] = None

    failure_id: Optional[str] = None

    checkpoint_id: Optional[str] = None

    timestamp: float = field(
        default_factory=_now
    )

    metadata: Dict[str, str] = field(
        default_factory=dict
    )


# ---------------------------------------------------------------------------
# Main architecture
# ---------------------------------------------------------------------------

class FinalIndexStorageArchitecture:

    def __init__(
        self,
        namespace: str = "our-search-final-index-storage",
        backend: Optional[
            FinalIndexStorageBackend
        ] = None,
    ) -> None:

        self.namespace = namespace

        self.backend = (
            backend
            if backend is not None
            else InMemoryFinalIndexStorageMetadata()
        )

        self._epoch = 0

        self._subsystems: Dict[
            str,
            StorageSubsystemRecord,
        ] = {}

        self._epochs: Dict[
            int,
            FinalArchitectureEpoch,
        ] = {}

        self._dependencies: Dict[
            str,
            StorageSubsystemDependency,
        ] = {}

        self._flows: Dict[
            str,
            FinalDataFlowContract,
        ] = {}

        self._logical_objects: Dict[
            str,
            FinalLogicalDataIdentity,
        ] = {}

        self._failures: Dict[
            str,
            FinalStorageFailure,
        ] = {}

        self._checkpoints: Dict[
            str,
            FinalIndexStorageCheckpoint,
        ] = {}

        self._events: List[
            FinalArchitectureEvent
        ] = []

    # ------------------------------------------------------------------
    # Epoch management
    # ------------------------------------------------------------------

    @property
    def current_epoch(self) -> int:
        return self._epoch

    def create_epoch(
        self,
    ) -> FinalArchitectureEpoch:

        parent = (
            self._epoch
            if self._epoch > 0
            else None
        )

        self._epoch += 1

        active = [
            record.subsystem
            for record
            in self._subsystems.values()
            if record.state
            in (
                SubsystemState.ACTIVE,
                SubsystemState.DEGRADED,
                SubsystemState.RECOVERING,
            )
        ]

        epoch = FinalArchitectureEpoch(
            epoch_id=_new_id(
                "architecture-epoch"
            ),
            epoch=self._epoch,
            state=ArchitectureEpochState.OPEN,
            parent_epoch=parent,
            active_subsystems=active,
            routing_generation=(
                f"routing-{self._epoch}"
            ),
            index_generation=(
                f"index-{self._epoch}"
            ),
            document_generation=(
                f"document-{self._epoch}"
            ),
            checkpoint_generation=(
                f"checkpoint-{self._epoch}"
            ),
            capacity_generation=(
                f"capacity-{self._epoch}"
            ),
        )

        self._epochs[
            self._epoch
        ] = epoch

        self.backend.save_epoch(
            epoch
        )

        self._emit_event(
            FinalArchitectureEventType.EPOCH_CREATED
        )

        return epoch

    def publish_epoch(
        self,
        epoch_number: Optional[int] = None,
    ) -> FinalArchitectureEpoch:

        number = (
            epoch_number
            if epoch_number is not None
            else self._epoch
        )

        epoch = self._epochs.get(
            number
        )

        if epoch is None:
            raise KeyError(
                f"Unknown architecture epoch: {number}"
            )

        epoch.state = (
            ArchitectureEpochState.PUBLISHED
        )

        epoch.published_at = _now()

        self.backend.save_epoch(
            epoch
        )

        self._emit_event(
            FinalArchitectureEventType.EPOCH_PUBLISHED
        )

        return epoch

    def fence_epoch(
        self,
        epoch_number: int,
    ) -> FinalArchitectureEpoch:

        epoch = self._epochs.get(
            epoch_number
        )

        if epoch is None:
            raise KeyError(
                f"Unknown architecture epoch: {epoch_number}"
            )

        epoch.state = (
            ArchitectureEpochState.FENCED
        )

        self.backend.save_epoch(
            epoch
        )

        self._emit_event(
            FinalArchitectureEventType.EPOCH_FENCED
        )

        return epoch

    # ------------------------------------------------------------------
    # Subsystem registration
    # ------------------------------------------------------------------

    def register_subsystem(
        self,
        *,
        subsystem: StorageSubsystem,
        owner_domain: str,
        version: str,
        dependencies: Optional[
            Sequence[StorageSubsystem]
        ] = None,
        state: SubsystemState = (
            SubsystemState.ACTIVE
        ),
    ) -> StorageSubsystemRecord:

        record = StorageSubsystemRecord(
            subsystem_id=_new_id(
                "storage-subsystem"
            ),
            subsystem=subsystem,
            state=state,
            owner_domain=owner_domain,
            version=version,
            epoch=self._epoch,
            dependencies=list(
                dependencies or []
            ),
        )

        self._subsystems[
            record.subsystem_id
        ] = record

        self.backend.save_subsystem(
            record
        )

        self._emit_event(
            FinalArchitectureEventType.SUBSYSTEM_REGISTERED,
            subsystem=subsystem,
            subsystem_id=record.subsystem_id,
        )

        return record

    def update_subsystem_state(
        self,
        subsystem_id: str,
        state: SubsystemState,
    ) -> StorageSubsystemRecord:

        record = self._subsystems.get(
            subsystem_id
        )

        if record is None:
            raise KeyError(
                f"Unknown subsystem: {subsystem_id}"
            )

        record.state = state
        record.epoch = self._epoch
        record.updated_at = _now()

        self.backend.save_subsystem(
            record
        )

        self._emit_event(
            FinalArchitectureEventType.SUBSYSTEM_STATE_CHANGED,
            subsystem=record.subsystem,
            subsystem_id=subsystem_id,
            metadata={
                "state": state.value
            },
        )

        return record

    # ------------------------------------------------------------------
    # Dependencies
    # ------------------------------------------------------------------

    def register_dependency(
        self,
        *,
        source: StorageSubsystem,
        target: StorageSubsystem,
        required: bool = True,
        ordering: str = "source_before_target",
        failure_propagation: str = "target_requires_source",
    ) -> StorageSubsystemDependency:

        dependency = (
            StorageSubsystemDependency(
                dependency_id=_new_id(
                    "storage-dependency"
                ),
                source=source,
                target=target,
                required=required,
                ordering=ordering,
                failure_propagation=(
                    failure_propagation
                ),
            )
        )

        self._dependencies[
            dependency.dependency_id
        ] = dependency

        self.backend.save_dependency(
            dependency
        )

        self._emit_event(
            FinalArchitectureEventType
            .CROSS_SUBSYSTEM_DEPENDENCY_REGISTERED,
            dependency_id=(
                dependency.dependency_id
            ),
            metadata={
                "source": source.value,
                "target": target.value,
            },
        )

        return dependency

    # ------------------------------------------------------------------
    # Data flows
    # ------------------------------------------------------------------

    def register_flow(
        self,
        *,
        name: str,
        source: StorageSubsystem,
        destination: StorageSubsystem,
        ordered_stages: Sequence[str],
        durable_boundary: str,
        recovery_boundary: str,
        routing_boundary: str,
        tiering_boundary: str,
    ) -> FinalDataFlowContract:

        flow = FinalDataFlowContract(
            flow_id=_new_id(
                "data-flow"
            ),
            name=name,
            source=source,
            destination=destination,
            ordered_stages=list(
                ordered_stages
            ),
            durable_boundary=(
                durable_boundary
            ),
            recovery_boundary=(
                recovery_boundary
            ),
            routing_boundary=(
                routing_boundary
            ),
            tiering_boundary=(
                tiering_boundary
            ),
        )

        self._flows[
            flow.flow_id
        ] = flow

        self.backend.save_flow(
            flow
        )

        if "document" in name.lower():

            self._emit_event(
                FinalArchitectureEventType
                .DOCUMENT_FLOW_REGISTERED,
                flow_id=flow.flow_id,
            )

        else:

            self._emit_event(
                FinalArchitectureEventType
                .INDEX_FLOW_REGISTERED,
                flow_id=flow.flow_id,
            )

        return flow

    # ------------------------------------------------------------------
    # Logical object registration
    # ------------------------------------------------------------------

    def register_logical_object(
        self,
        *,
        document_id: str,
        canonical_url: str,
        content_version: str,
        partition_id: str,
        index_generation: str,
        document_generation: str,
        lifecycle_state: DataLifecycleState = (
            DataLifecycleState.INDEXED
        ),
    ) -> FinalLogicalDataIdentity:

        logical_object_id = _hash(
            "|".join(
                [
                    document_id,
                    canonical_url,
                    content_version,
                    partition_id,
                ]
            )
        )

        identity = FinalLogicalDataIdentity(
            logical_object_id=logical_object_id,
            document_id=document_id,
            canonical_url=canonical_url,
            content_version=content_version,
            partition_id=partition_id,
            index_generation=index_generation,
            document_generation=document_generation,
            lifecycle_state=lifecycle_state,
        )

        self._logical_objects[
            logical_object_id
        ] = identity

        return identity

    def update_logical_lifecycle(
        self,
        logical_object_id: str,
        state: DataLifecycleState,
    ) -> FinalLogicalDataIdentity:

        identity = (
            self._logical_objects.get(
                logical_object_id
            )
        )

        if identity is None:
            raise KeyError(
                f"Unknown logical object: "
                f"{logical_object_id}"
            )

        identity.lifecycle_state = state

        return identity

    # ------------------------------------------------------------------
    # Failure management
    # ------------------------------------------------------------------

    def register_failure(
        self,
        *,
        subsystem: StorageSubsystem,
        failure_domain: FailureDomain,
        affected_scope: str,
        recovery_required: bool = True,
    ) -> FinalStorageFailure:

        failure = FinalStorageFailure(
            failure_id=_new_id(
                "storage-failure"
            ),
            subsystem=subsystem,
            failure_domain=failure_domain,
            affected_scope=affected_scope,
            epoch=self._epoch,
            recovery_required=recovery_required,
        )

        self._failures[
            failure.failure_id
        ] = failure

        self.backend.save_failure(
            failure
        )

        self._emit_event(
            FinalArchitectureEventType.FAILURE_REGISTERED,
            subsystem=subsystem,
            failure_id=failure.failure_id,
            metadata={
                "failure_domain":
                    failure_domain.value,
                "affected_scope":
                    affected_scope,
            },
        )

        if recovery_required:

            self._emit_event(
                FinalArchitectureEventType
                .RECOVERY_REQUIRED,
                subsystem=subsystem,
                failure_id=failure.failure_id,
            )

        return failure

    def resolve_failure(
        self,
        failure_id: str,
    ) -> FinalStorageFailure:

        failure = self._failures.get(
            failure_id
        )

        if failure is None:
            raise KeyError(
                f"Unknown failure: {failure_id}"
            )

        failure.resolved = True

        self.backend.save_failure(
            failure
        )

        return failure

    # ------------------------------------------------------------------
    # Final checkpoint
    # ------------------------------------------------------------------

    def checkpoint(
        self,
    ) -> FinalIndexStorageCheckpoint:

        subsystem_versions = {
            record.subsystem.value:
                record.version
            for record
            in self._subsystems.values()
        }

        active_subsystems = [
            record.subsystem.value
            for record
            in self._subsystems.values()
            if record.state
            not in (
                SubsystemState.RETIRED,
                SubsystemState.FAILED,
            )
        ]

        raw = "|".join(
            [
                self.namespace,
                str(self._epoch),
                str(
                    sorted(
                        subsystem_versions.items()
                    )
                ),
                str(
                    sorted(
                        active_subsystems
                    )
                ),
                str(
                    len(
                        self._logical_objects
                    )
                ),
                str(
                    len(
                        self._dependencies
                    )
                ),
                str(
                    len(
                        self._failures
                    )
                ),
            ]
        )

        checkpoint = FinalIndexStorageCheckpoint(
            checkpoint_id=_new_id(
                "final-index-storage-checkpoint"
            ),
            epoch=self._epoch,
            subsystem_versions=(
                subsystem_versions
            ),
            active_subsystems=(
                active_subsystems
            ),
            logical_data_count=len(
                self._logical_objects
            ),
            dependency_count=len(
                self._dependencies
            ),
            failure_count=len(
                self._failures
            ),
            checksum=_hash(raw),
        )

        self._checkpoints[
            checkpoint.checkpoint_id
        ] = checkpoint

        self.backend.save_checkpoint(
            checkpoint
        )

        self._emit_event(
            FinalArchitectureEventType
            .FINAL_ARCHITECTURE_CHECKPOINT,
            checkpoint_id=(
                checkpoint.checkpoint_id
            ),
        )

        return checkpoint

    # ------------------------------------------------------------------
    # Capacity
    # ------------------------------------------------------------------

    def capacity(
        self,
    ) -> FinalIndexStorageCapacity:

        return FinalIndexStorageCapacity(
            namespace=self.namespace,
            subsystem_count=len(
                self._subsystems
            ),
            dependency_count=len(
                self._dependencies
            ),
            flow_count=len(
                self._flows
            ),
            logical_object_count=len(
                self._logical_objects
            ),
            failure_count=len(
                self._failures
            ),
            checkpoint_count=len(
                self._checkpoints
            ),
            current_epoch=self._epoch,
            horizontal_scaling=True,
            partition_scaling=True,
            region_scaling=True,
            zone_scaling=True,
            node_scaling=True,
            tier_scaling=True,
            no_global_fixed_ceiling=True,
        )

    # ------------------------------------------------------------------
    # Complete architecture description
    # ------------------------------------------------------------------

    def architecture(
        self,
    ) -> Dict[str, object]:

        return {
            "version": ARCHITECTURE_VERSION,

            "scale_target": (
                GLOBAL_SCALE_TARGET
            ),

            "google_scale_capability_target": (
                GOOGLE_SCALE_CAPABILITY_TARGET
            ),

            "google_technology_dependency": (
                GOOGLE_TECHNOLOGY_DEPENDENCY
            ),

            "phase": "10.9",

            "phase_10_status": (
                "architecturally_complete"
            ),

            "storage_pipeline": [
                "global_index_storage",
                "distributed_inverted_index_segment_fabric",
                "massive_document_content_storage",
                "replication_and_durability",
                "massive_segment_compaction",
                "partition_aware_index_routing",
                "index_recovery_and_checkpointing",
                "storage_capacity_and_tiering",
                "final_index_storage_control_plane",
            ],

            "logical_architecture": {
                "logical_document_identity": True,
                "logical_index_identity": True,
                "physical_storage_separation": True,
                "immutable_index_generations": True,
                "immutable_content_versions": True,
                "partition_aware_storage": True,
                "partition_aware_routing": True,
                "replicated_storage": True,
                "tiered_storage": True,
                "checkpointed_state": True,
                "recovery_generations": True,
            },

            "index_architecture": {
                "distributed_inverted_index": True,
                "immutable_segments": True,
                "term_dictionaries": True,
                "posting_blocks": True,
                "segment_generations": True,
                "segment_compaction": True,
                "partition_routing": True,
                "replica_routing": True,
                "epoch_fencing": True,
            },

            "document_architecture": {
                "distributed_document_storage": True,
                "content_chunking": True,
                "document_manifests": True,
                "content_versions": True,
                "document_replicas": True,
                "document_tiering": True,
            },

            "durability_architecture": {
                "multi_region_replication": True,
                "multi_zone_replication": True,
                "anti_correlated_placement": True,
                "replica_health": True,
                "repair": True,
                "recovery": True,
                "checkpointing": True,
                "journal_replay": True,
                "atomic_restore": True,
            },

            "compaction_architecture": {
                "multi_level_compaction": True,
                "resource_aware_scheduling": True,
                "merge_lineage": True,
                "immutable_output_generations": True,
                "atomic_manifest_swap": True,
                "source_retirement": True,
                "epoch_fencing": True,
                "retryable_jobs": True,
            },

            "routing_architecture": {
                "hash_routing": True,
                "range_routing": True,
                "prefix_routing": True,
                "field_aware_routing": True,
                "document_routing": True,
                "term_routing": True,
                "broadcast_routing": True,
                "locality_aware_replica_selection": True,
                "failover": True,
                "route_epoch_fencing": True,
                "partition_split": True,
                "partition_merge": True,
                "partition_migration": True,
            },

            "capacity_architecture": {
                "hot_tier": True,
                "warm_tier": True,
                "cold_tier": True,
                "archive_tier": True,
                "capacity_accounting": True,
                "capacity_pressure_detection": True,
                "tier_promotion": True,
                "tier_demotion": True,
                "storage_migration": True,
                "rebalancing": True,
                "capacity_expansion": True,
                "storage_evacuation": True,
            },

            "global_control_plane": {
                "subsystem_registry": True,
                "cross_subsystem_contracts": True,
                "architecture_epochs": True,
                "published_generations": True,
                "epoch_fencing": True,
                "failure_registry": True,
                "checkpoint_binding": True,
                "logical_identity_registry": True,
            },

            "complete_data_path": [
                "public_web_discovery",
                "crawl_result",
                "document_identity",
                "document_content_storage",
                "content_version",
                "indexing_pipeline",
                "term_and_field_partitioning",
                "immutable_index_segment",
                "segment_replication",
                "segment_compaction",
                "partition_routing",
                "capacity_and_tiering",
                "checkpoint_and_recovery",
                "search_read_path",
            ],

            "failure_path": [
                "failure_detection",
                "failure_domain_identification",
                "affected_partition_detection",
                "replica_selection",
                "checkpoint_selection",
                "journal_replay",
                "segment_or_document_restore",
                "routing_revalidation",
                "capacity_revalidation",
                "epoch_fencing",
                "atomic_publication",
            ],

            "growth_path": [
                "new_storage_node",
                "new_storage_zone",
                "new_storage_region",
                "new_index_partition",
                "new_document_partition",
                "new_segment_replica",
                "new_compaction_capacity",
                "new_recovery_capacity",
                "new_tier_capacity",
                "routing_rebalance",
                "capacity_rebalance",
            ],

            "explicit_non_goals": {
                "google_dependency": False,
                "single_global_index_file": False,
                "single_global_document_file": False,
                "single_global_segment_directory": False,
                "single_global_database": False,
                "single_global_storage_pool": False,
                "single_global_router": False,
                "single_global_compaction_worker": False,
                "single_global_recovery_worker": False,
                "single_global_migration_queue": False,
                "fixed_global_document_limit": False,
                "fixed_global_index_limit": False,
                "fixed_global_segment_limit": False,
                "fixed_global_partition_limit": False,
                "fixed_global_storage_limit": False,
                "fixed_global_replica_limit": False,
            },

            "scale_properties": {
                "horizontal_partition_growth": True,
                "horizontal_storage_growth": True,
                "horizontal_worker_growth": True,
                "horizontal_replica_growth": True,
                "horizontal_region_growth": True,
                "horizontal_zone_growth": True,
                "horizontal_tier_growth": True,
                "logical_identity_stability": True,
                "physical_location_mutability": True,
                "replaceable_backend": True,
                "no_global_fixed_ceiling": True,
            },

            "architecture_principles": [
                "logical_identity_is_not_physical_location",
                "immutable_generations_are_the_durable_unit",
                "manifests_are_control_plane_state",
                "partitions_are_independent_scaling_domains",
                "replicas_are_failure_isolation_domains",
                "epochs_prevent_stale_writes",
                "checkpoints_bound_recovery",
                "routing_is_partition_aware",
                "capacity_is_domain_aware",
                "tiering_is_policy_driven",
                "migration_is_verification_driven",
                "horizontal_growth_is_preferred",
                "failure_recovery_is_generation_safe",
                "backend_implementations_are_replaceable",
            ],

            "phase_10_completion": {
                "10_1_global_storage": True,
                "10_2_segment_fabric": True,
                "10_3_document_storage": True,
                "10_4_replication_durability": True,
                "10_5_compaction": True,
                "10_6_partition_routing": True,
                "10_7_recovery_checkpointing": True,
                "10_8_capacity_tiering": True,
                "10_9_final_architecture": True,
            },
        }

    # ------------------------------------------------------------------
    # Internal event helper
    # ------------------------------------------------------------------

    def _emit_event(
        self,
        event_type: FinalArchitectureEventType,
        *,
        subsystem: Optional[
            StorageSubsystem
        ] = None,
        subsystem_id: Optional[str] = None,
        dependency_id: Optional[str] = None,
        flow_id: Optional[str] = None,
        failure_id: Optional[str] = None,
        checkpoint_id: Optional[str] = None,
        metadata: Optional[
            Dict[str, str]
        ] = None,
    ) -> FinalArchitectureEvent:

        event = FinalArchitectureEvent(
            event_id=_new_id(
                "final-storage-event"
            ),
            event_type=event_type,
            namespace=self.namespace,
            epoch=self._epoch,
            subsystem=subsystem,
            subsystem_id=subsystem_id,
            dependency_id=dependency_id,
            flow_id=flow_id,
            failure_id=failure_id,
            checkpoint_id=checkpoint_id,
            metadata=dict(
                metadata or {}
            ),
        )

        self._events.append(
            event
        )

        self.backend.save_event(
            event
        )

        return event

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def subsystems(
        self,
    ) -> Dict[
        str,
        StorageSubsystemRecord,
    ]:

        return dict(
            self._subsystems
        )

    @property
    def epochs(
        self,
    ) -> Dict[
        int,
        FinalArchitectureEpoch,
    ]:

        return dict(
            self._epochs
        )

    @property
    def dependencies(
        self,
    ) -> Dict[
        str,
        StorageSubsystemDependency,
    ]:

        return dict(
            self._dependencies
        )

    @property
    def flows(
        self,
    ) -> Dict[
        str,
        FinalDataFlowContract,
    ]:

        return dict(
            self._flows
        )

    @property
    def logical_objects(
        self,
    ) -> Dict[
        str,
        FinalLogicalDataIdentity,
    ]:

        return dict(
            self._logical_objects
        )

    @property
    def failures(
        self,
    ) -> Dict[
        str,
        FinalStorageFailure,
    ]:

        return dict(
            self._failures
        )

    @property
    def checkpoints(
        self,
    ) -> Dict[
        str,
        FinalIndexStorageCheckpoint,
    ]:

        return dict(
            self._checkpoints
        )

    @property
    def events(
        self,
    ) -> List[
        FinalArchitectureEvent
    ]:

        return list(
            self._events
        )


# ---------------------------------------------------------------------------
# Stable aliases
# ---------------------------------------------------------------------------

FinalIndexStorage = (
    FinalIndexStorageArchitecture
)

GlobalFinalIndexStorageArchitecture = (
    FinalIndexStorageArchitecture
)

Phase10_9FinalIndexStorageArchitecture = (
    FinalIndexStorageArchitecture
)


# ---------------------------------------------------------------------------
# Public exports
# ---------------------------------------------------------------------------

__all__ = [
    "ARCHITECTURE_VERSION",
    "GLOBAL_SCALE_TARGET",
    "GOOGLE_SCALE_CAPABILITY_TARGET",
    "GOOGLE_TECHNOLOGY_DEPENDENCY",

    "StorageSubsystem",
    "SubsystemState",
    "DataLifecycleState",
    "ArchitectureEpochState",
    "FailureDomain",
    "FinalArchitectureEventType",

    "StorageSubsystemRecord",
    "FinalLogicalDataIdentity",
    "FinalArchitectureEpoch",
    "StorageSubsystemDependency",
    "FinalDataFlowContract",
    "FinalStorageFailure",
    "FinalIndexStorageCheckpoint",
    "FinalIndexStorageCapacity",
    "FinalArchitectureEvent",

    "FinalIndexStorageBackend",
    "InMemoryFinalIndexStorageMetadata",

    "FinalIndexStorageArchitecture",

    "FinalIndexStorage",
    "GlobalFinalIndexStorageArchitecture",
    "Phase10_9FinalIndexStorageArchitecture",
]
