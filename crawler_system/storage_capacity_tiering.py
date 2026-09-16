"""
OUR SEARCH
Phase 10.8 — Storage Capacity + Tiering

Version:
    storage-capacity-tiering.v1

Architecture:

    INDEX / DOCUMENT DATA
        ↓
    CAPACITY MODEL
        ↓
    HOT / WARM / COLD / ARCHIVE TIERS
        ↓
    TIER PLACEMENT
        ↓
    CAPACITY MONITORING
        ↓
    REBALANCING
        ↓
    DATA MIGRATION
        ↓
    CAPACITY EXPANSION
        ↓
    STORAGE PRESSURE CONTROL

Scale target:

    Billions → potentially trillions of publicly accessible Web resources.

This module is a distributed storage capacity and tiering
control-plane architecture.

It does not require one global disk, one global filesystem,
one global database, or one storage machine.

Logical data identity is separated from physical storage placement.

The architecture supports:

    - multiple storage tiers
    - capacity accounting
    - partition-level capacity
    - region-level capacity
    - zone-level capacity
    - node-level capacity
    - storage pressure
    - placement policy
    - migration planning
    - migration generations
    - rebalancing
    - capacity expansion
    - tier promotion
    - tier demotion
    - storage evacuation
    - failure-aware placement
    - durable capacity metadata
    - epoch fencing
    - atomic placement publication
    - recovery-safe migration lineage
    - horizontal storage growth
    - replaceable storage backend

No Google technology or Google infrastructure is used.

No tests or benchmarks are performed by this module.
"""

from __future__ import annotations

import hashlib
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Protocol, Sequence, Tuple


ARCHITECTURE_VERSION = "storage-capacity-tiering.v1"

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


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex}"


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class StorageTier(str, Enum):
    HOT = "hot"
    WARM = "warm"
    COLD = "cold"
    ARCHIVE = "archive"


class StorageState(str, Enum):
    ACTIVE = "active"
    DEGRADED = "degraded"
    DRAINING = "draining"
    EVACUATING = "evacuating"
    RECOVERING = "recovering"
    RETIRED = "retired"


class CapacityPressure(str, Enum):
    NORMAL = "normal"
    ELEVATED = "elevated"
    HIGH = "high"
    CRITICAL = "critical"


class PlacementState(str, Enum):
    PLANNED = "planned"
    ACTIVE = "active"
    MOVING = "moving"
    DRAINING = "draining"
    FAILED = "failed"
    RETIRED = "retired"


class MigrationState(str, Enum):
    PLANNED = "planned"
    PREPARING = "preparing"
    COPYING = "copying"
    VERIFYING = "verifying"
    CUTOVER = "cutover"
    COMPLETED = "completed"
    ABORTED = "aborted"
    FENCED = "fenced"


class MigrationReason(str, Enum):
    CAPACITY = "capacity"
    TIER_POLICY = "tier_policy"
    HOTSPOT = "hotspot"
    FAILURE = "failure"
    REBALANCE = "rebalance"
    EXPANSION = "expansion"
    EVACUATION = "evacuation"
    RETENTION = "retention"


class CapacityScope(str, Enum):
    NODE = "node"
    ZONE = "zone"
    REGION = "region"
    TIER = "tier"
    PARTITION = "partition"
    GLOBAL = "global"


class CapacityEventType(str, Enum):
    CAPACITY_REGISTERED = "capacity_registered"
    CAPACITY_UPDATED = "capacity_updated"

    PRESSURE_CHANGED = "pressure_changed"

    PLACEMENT_CREATED = "placement_created"
    PLACEMENT_PUBLISHED = "placement_published"

    MIGRATION_PLANNED = "migration_planned"
    MIGRATION_STARTED = "migration_started"
    MIGRATION_VERIFIED = "migration_verified"
    MIGRATION_CUTOVER = "migration_cutover"
    MIGRATION_COMPLETED = "migration_completed"
    MIGRATION_ABORTED = "migration_aborted"
    MIGRATION_FENCED = "migration_fenced"

    TIER_PROMOTION = "tier_promotion"
    TIER_DEMOTION = "tier_demotion"

    REBALANCE_PLANNED = "rebalance_planned"
    CAPACITY_EXPANSION = "capacity_expansion"

    STORAGE_EVACUATION = "storage_evacuation"

    CHECKPOINT_CREATED = "checkpoint_created"


# ---------------------------------------------------------------------------
# Storage capacity
# ---------------------------------------------------------------------------

@dataclass
class StorageCapacityRecord:
    capacity_id: str

    scope: CapacityScope

    scope_id: str

    tier: StorageTier

    region: Optional[str]
    zone: Optional[str]
    node_id: Optional[str]

    total_bytes: int
    used_bytes: int
    reserved_bytes: int

    state: StorageState = StorageState.ACTIVE

    epoch: int = 0

    created_at: float = field(default_factory=_now)
    updated_at: float = field(default_factory=_now)

    metadata: Dict[str, str] = field(default_factory=dict)

    @property
    def available_bytes(self) -> int:
        return max(
            0,
            self.total_bytes
            - self.used_bytes
            - self.reserved_bytes,
        )

    @property
    def utilization(self) -> float:
        if self.total_bytes <= 0:
            return 1.0

        return (
            self.used_bytes
            + self.reserved_bytes
        ) / self.total_bytes


# ---------------------------------------------------------------------------
# Capacity policy
# ---------------------------------------------------------------------------

@dataclass
class StorageCapacityPolicy:
    """
    Policy thresholds.

    These are operational policy values, not global capacity limits.
    """

    hot_high_watermark: float = 0.80
    hot_critical_watermark: float = 0.92

    warm_high_watermark: float = 0.85
    warm_critical_watermark: float = 0.95

    cold_high_watermark: float = 0.90
    cold_critical_watermark: float = 0.97

    archive_high_watermark: float = 0.95
    archive_critical_watermark: float = 0.99

    default_tier: StorageTier = StorageTier.WARM

    promote_on_high_access: bool = True
    demote_on_low_access: bool = True

    prefer_local_region: bool = True
    prefer_local_zone: bool = True

    avoid_failed_nodes: bool = True
    avoid_draining_nodes: bool = True

    migration_verification_required: bool = True
    atomic_cutover_required: bool = True
    epoch_fencing_enabled: bool = True

    checkpoint_every_epoch: bool = True


# ---------------------------------------------------------------------------
# Data placement
# ---------------------------------------------------------------------------

@dataclass
class StoragePlacement:
    placement_id: str

    logical_object_id: str

    partition_id: str

    tier: StorageTier

    region: str
    zone: str
    node_id: str

    byte_size: int

    state: PlacementState = PlacementState.ACTIVE

    placement_epoch: int = 0

    created_at: float = field(default_factory=_now)
    updated_at: float = field(default_factory=_now)

    source_placement_id: Optional[str] = None

    metadata: Dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Tier policy decision
# ---------------------------------------------------------------------------

@dataclass
class TierPlacementDecision:
    logical_object_id: str

    current_tier: Optional[StorageTier]

    target_tier: StorageTier

    reason: MigrationReason

    confidence: float

    policy_epoch: int

    created_at: float = field(default_factory=_now)

    metadata: Dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Migration
# ---------------------------------------------------------------------------

@dataclass
class StorageMigration:
    migration_id: str

    logical_object_id: str

    partition_id: str

    source_placement_id: str
    destination_tier: StorageTier

    destination_region: str
    destination_zone: str
    destination_node_id: str

    source_epoch: int
    destination_epoch: int

    reason: MigrationReason

    state: MigrationState = MigrationState.PLANNED

    bytes_to_move: int = 0

    created_at: float = field(default_factory=_now)
    started_at: Optional[float] = None
    verified_at: Optional[float] = None
    cutover_at: Optional[float] = None
    completed_at: Optional[float] = None

    checksum: str = ""

    metadata: Dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Rebalance plan
# ---------------------------------------------------------------------------

@dataclass
class StorageRebalancePlan:
    rebalance_id: str

    scope: CapacityScope

    scope_id: str

    source_placement_ids: List[str]

    destination_tiers: Dict[
        str,
        StorageTier,
    ]

    destination_nodes: Dict[
        str,
        str,
    ]

    created_at: float = field(default_factory=_now)

    state: MigrationState = MigrationState.PLANNED

    metadata: Dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Capacity expansion
# ---------------------------------------------------------------------------

@dataclass
class StorageCapacityExpansion:

    expansion_id: str

    scope: CapacityScope

    scope_id: str

    tier: StorageTier

    region: Optional[str]
    zone: Optional[str]
    node_id: Optional[str]

    additional_bytes: int

    previous_total_bytes: int
    resulting_total_bytes: int

    epoch: int

    created_at: float = field(default_factory=_now)

    metadata: Dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Storage checkpoint
# ---------------------------------------------------------------------------

@dataclass
class StorageTieringCheckpoint:

    checkpoint_id: str

    namespace: str

    epoch: int

    capacity_record_ids: List[str]

    placement_ids: List[str]

    migration_ids: List[str]

    checksum: str

    created_at: float = field(default_factory=_now)

    metadata: Dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------

@dataclass
class StorageCapacityEvent:

    event_id: str

    event_type: CapacityEventType

    namespace: str

    epoch: int

    capacity_ids: List[str] = field(default_factory=list)

    placement_ids: List[str] = field(default_factory=list)

    migration_ids: List[str] = field(default_factory=list)

    timestamp: float = field(default_factory=_now)

    metadata: Dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Capacity snapshot
# ---------------------------------------------------------------------------

@dataclass
class StorageCapacitySnapshot:

    namespace: str

    epoch: int

    total_bytes: int
    used_bytes: int
    reserved_bytes: int
    available_bytes: int

    capacity_records: int

    placement_count: int

    active_migration_count: int

    pressure_by_tier: Dict[
        str,
        CapacityPressure,
    ]

    created_at: float = field(default_factory=_now)


# ---------------------------------------------------------------------------
# Architecture capacity
# ---------------------------------------------------------------------------

@dataclass
class StorageTieringCapacity:

    namespace: str

    capacity_record_count: int

    placement_count: int

    active_migration_count: int

    expansion_count: int

    checkpoint_count: int

    current_epoch: int

    total_bytes: int
    used_bytes: int
    reserved_bytes: int
    available_bytes: int

    supports_hot_tier: bool
    supports_warm_tier: bool
    supports_cold_tier: bool
    supports_archive_tier: bool

    supports_horizontal_capacity_growth: bool
    supports_partition_rebalancing: bool
    supports_tier_migration: bool


# ---------------------------------------------------------------------------
# Backend contract
# ---------------------------------------------------------------------------

class StorageCapacityTieringBackend(Protocol):

    def save_capacity(
        self,
        record: StorageCapacityRecord,
    ) -> None:
        ...

    def get_capacity(
        self,
        capacity_id: str,
    ) -> Optional[StorageCapacityRecord]:
        ...

    def save_placement(
        self,
        placement: StoragePlacement,
    ) -> None:
        ...

    def get_placement(
        self,
        placement_id: str,
    ) -> Optional[StoragePlacement]:
        ...

    def save_migration(
        self,
        migration: StorageMigration,
    ) -> None:
        ...

    def save_checkpoint(
        self,
        checkpoint: StorageTieringCheckpoint,
    ) -> None:
        ...

    def save_event(
        self,
        event: StorageCapacityEvent,
    ) -> None:
        ...


# ---------------------------------------------------------------------------
# Reference backend
# ---------------------------------------------------------------------------

class InMemoryStorageCapacityTieringMetadata:

    def __init__(self) -> None:

        self.capacities: Dict[
            str,
            StorageCapacityRecord,
        ] = {}

        self.placements: Dict[
            str,
            StoragePlacement,
        ] = {}

        self.migrations: Dict[
            str,
            StorageMigration,
        ] = {}

        self.checkpoints: Dict[
            str,
            StorageTieringCheckpoint,
        ] = {}

        self.events: List[
            StorageCapacityEvent
        ] = []

    def save_capacity(
        self,
        record: StorageCapacityRecord,
    ) -> None:

        self.capacities[
            record.capacity_id
        ] = record

    def get_capacity(
        self,
        capacity_id: str,
    ) -> Optional[StorageCapacityRecord]:

        return self.capacities.get(
            capacity_id
        )

    def save_placement(
        self,
        placement: StoragePlacement,
    ) -> None:

        self.placements[
            placement.placement_id
        ] = placement

    def get_placement(
        self,
        placement_id: str,
    ) -> Optional[StoragePlacement]:

        return self.placements.get(
            placement_id
        )

    def save_migration(
        self,
        migration: StorageMigration,
    ) -> None:

        self.migrations[
            migration.migration_id
        ] = migration

    def save_checkpoint(
        self,
        checkpoint: StorageTieringCheckpoint,
    ) -> None:

        self.checkpoints[
            checkpoint.checkpoint_id
        ] = checkpoint

    def save_event(
        self,
        event: StorageCapacityEvent,
    ) -> None:

        self.events.append(event)


# ---------------------------------------------------------------------------
# Main architecture
# ---------------------------------------------------------------------------

class StorageCapacityTieringArchitecture:

    def __init__(
        self,
        namespace: str = "global-index-storage",
        backend: Optional[
            StorageCapacityTieringBackend
        ] = None,
        policy: Optional[
            StorageCapacityPolicy
        ] = None,
    ) -> None:

        self.namespace = namespace

        self.backend = (
            backend
            if backend is not None
            else InMemoryStorageCapacityTieringMetadata()
        )

        self.policy = (
            policy
            if policy is not None
            else StorageCapacityPolicy()
        )

        self._epoch = 0

        self._capacities: Dict[
            str,
            StorageCapacityRecord,
        ] = {}

        self._placements: Dict[
            str,
            StoragePlacement,
        ] = {}

        self._migrations: Dict[
            str,
            StorageMigration,
        ] = {}

        self._expansions: Dict[
            str,
            StorageCapacityExpansion,
        ] = {}

        self._checkpoints: Dict[
            str,
            StorageTieringCheckpoint,
        ] = {}

        self._events: List[
            StorageCapacityEvent
        ] = []

    # ------------------------------------------------------------------
    # Epoch
    # ------------------------------------------------------------------

    @property
    def epoch(self) -> int:
        return self._epoch

    def advance_epoch(self) -> int:

        self._epoch += 1

        return self._epoch

    # ------------------------------------------------------------------
    # Capacity registration
    # ------------------------------------------------------------------

    def register_capacity(
        self,
        *,
        scope: CapacityScope,
        scope_id: str,
        tier: StorageTier,
        total_bytes: int,
        used_bytes: int = 0,
        reserved_bytes: int = 0,
        region: Optional[str] = None,
        zone: Optional[str] = None,
        node_id: Optional[str] = None,
        state: StorageState = StorageState.ACTIVE,
        metadata: Optional[
            Dict[str, str]
        ] = None,
    ) -> StorageCapacityRecord:

        if total_bytes < 0:
            raise ValueError(
                "total_bytes cannot be negative"
            )

        if used_bytes < 0:
            raise ValueError(
                "used_bytes cannot be negative"
            )

        if reserved_bytes < 0:
            raise ValueError(
                "reserved_bytes cannot be negative"
            )

        record = StorageCapacityRecord(
            capacity_id=_new_id("capacity"),
            scope=scope,
            scope_id=scope_id,
            tier=tier,
            region=region,
            zone=zone,
            node_id=node_id,
            total_bytes=total_bytes,
            used_bytes=used_bytes,
            reserved_bytes=reserved_bytes,
            state=state,
            epoch=self._epoch,
            metadata=dict(metadata or {}),
        )

        self._capacities[
            record.capacity_id
        ] = record

        self.backend.save_capacity(
            record
        )

        self._emit_event(
            CapacityEventType.CAPACITY_REGISTERED,
            capacity_ids=[
                record.capacity_id
            ],
        )

        return record

    # ------------------------------------------------------------------
    # Capacity updates
    # ------------------------------------------------------------------

    def update_capacity(
        self,
        capacity_id: str,
        *,
        total_bytes: Optional[int] = None,
        used_bytes: Optional[int] = None,
        reserved_bytes: Optional[int] = None,
        state: Optional[StorageState] = None,
    ) -> StorageCapacityRecord:

        record = self._get_capacity(
            capacity_id
        )

        if total_bytes is not None:
            record.total_bytes = total_bytes

        if used_bytes is not None:
            record.used_bytes = used_bytes

        if reserved_bytes is not None:
            record.reserved_bytes = reserved_bytes

        if state is not None:
            record.state = state

        record.updated_at = _now()
        record.epoch = self._epoch

        self.backend.save_capacity(
            record
        )

        self._emit_event(
            CapacityEventType.CAPACITY_UPDATED,
            capacity_ids=[
                capacity_id
            ],
        )

        return record

    # ------------------------------------------------------------------
    # Pressure
    # ------------------------------------------------------------------

    def pressure(
        self,
        tier: StorageTier,
    ) -> CapacityPressure:

        records = [
            record
            for record in self._capacities.values()
            if record.tier == tier
            and record.state
            != StorageState.RETIRED
        ]

        if not records:
            return CapacityPressure.NORMAL

        utilization = max(
            record.utilization
            for record in records
        )

        if tier == StorageTier.HOT:

            critical = (
                self.policy.hot_critical_watermark
            )

            high = (
                self.policy.hot_high_watermark
            )

        elif tier == StorageTier.WARM:

            critical = (
                self.policy.warm_critical_watermark
            )

            high = (
                self.policy.warm_high_watermark
            )

        elif tier == StorageTier.COLD:

            critical = (
                self.policy.cold_critical_watermark
            )

            high = (
                self.policy.cold_high_watermark
            )

        else:

            critical = (
                self.policy.archive_critical_watermark
            )

            high = (
                self.policy.archive_high_watermark
            )

        if utilization >= critical:
            result = CapacityPressure.CRITICAL

        elif utilization >= high:
            result = CapacityPressure.HIGH

        elif utilization >= high * 0.85:
            result = CapacityPressure.ELEVATED

        else:
            result = CapacityPressure.NORMAL

        self._emit_event(
            CapacityEventType.PRESSURE_CHANGED,
            metadata={
                "tier": tier.value,
                "pressure": result.value,
            },
        )

        return result

    # ------------------------------------------------------------------
    # Placement
    # ------------------------------------------------------------------

    def create_placement(
        self,
        *,
        logical_object_id: str,
        partition_id: str,
        tier: StorageTier,
        region: str,
        zone: str,
        node_id: str,
        byte_size: int,
        source_placement_id: Optional[str] = None,
    ) -> StoragePlacement:

        if byte_size < 0:
            raise ValueError(
                "byte_size cannot be negative"
            )

        placement = StoragePlacement(
            placement_id=_new_id("placement"),
            logical_object_id=logical_object_id,
            partition_id=partition_id,
            tier=tier,
            region=region,
            zone=zone,
            node_id=node_id,
            byte_size=byte_size,
            placement_epoch=self._epoch,
            source_placement_id=(
                source_placement_id
            ),
        )

        self._placements[
            placement.placement_id
        ] = placement

        self.backend.save_placement(
            placement
        )

        self._emit_event(
            CapacityEventType.PLACEMENT_CREATED,
            placement_ids=[
                placement.placement_id
            ],
        )

        return placement

    def publish_placement(
        self,
        placement_id: str,
    ) -> StoragePlacement:

        placement = self._get_placement(
            placement_id
        )

        placement.state = PlacementState.ACTIVE
        placement.placement_epoch = self._epoch
        placement.updated_at = _now()

        self.backend.save_placement(
            placement
        )

        self._emit_event(
            CapacityEventType.PLACEMENT_PUBLISHED,
            placement_ids=[
                placement_id
            ],
        )

        return placement

    # ------------------------------------------------------------------
    # Tier selection
    # ------------------------------------------------------------------

    def choose_tier(
        self,
        *,
        logical_object_id: str,
        access_frequency: float,
        recency_score: float,
        durability_requirement: float,
        current_tier: Optional[
            StorageTier
        ] = None,
    ) -> TierPlacementDecision:

        """
        Determine the logical storage tier.

        The scores are abstract policy inputs. The module deliberately
        does not prescribe a particular production storage technology.
        """

        access = max(
            0.0,
            min(1.0, access_frequency),
        )

        recency = max(
            0.0,
            min(1.0, recency_score),
        )

        durability = max(
            0.0,
            min(1.0, durability_requirement),
        )

        if (
            self.policy.promote_on_high_access
            and access >= 0.80
        ):
            target = StorageTier.HOT
            reason = MigrationReason.TIER_POLICY

        elif (
            access >= 0.45
            or recency >= 0.55
        ):
            target = StorageTier.WARM
            reason = MigrationReason.TIER_POLICY

        elif durability >= 0.85:
            target = StorageTier.COLD
            reason = MigrationReason.RETENTION

        else:
            target = StorageTier.ARCHIVE
            reason = MigrationReason.RETENTION

        confidence = max(
            access,
            recency,
            durability,
        )

        return TierPlacementDecision(
            logical_object_id=logical_object_id,
            current_tier=current_tier,
            target_tier=target,
            reason=reason,
            confidence=confidence,
            policy_epoch=self._epoch,
        )

    # ------------------------------------------------------------------
    # Migration
    # ------------------------------------------------------------------

    def plan_migration(
        self,
        *,
        logical_object_id: str,
        partition_id: str,
        source_placement_id: str,
        destination_tier: StorageTier,
        destination_region: str,
        destination_zone: str,
        destination_node_id: str,
        reason: MigrationReason,
    ) -> StorageMigration:

        source = self._get_placement(
            source_placement_id
        )

        migration = StorageMigration(
            migration_id=_new_id("migration"),
            logical_object_id=logical_object_id,
            partition_id=partition_id,
            source_placement_id=source_placement_id,
            destination_tier=destination_tier,
            destination_region=destination_region,
            destination_zone=destination_zone,
            destination_node_id=destination_node_id,
            source_epoch=source.placement_epoch,
            destination_epoch=self._epoch + 1,
            reason=reason,
            bytes_to_move=source.byte_size,
        )

        migration.checksum = _hash(
            "|".join(
                [
                    migration.migration_id,
                    logical_object_id,
                    source_placement_id,
                    destination_tier.value,
                    destination_region,
                    destination_zone,
                    destination_node_id,
                    str(source.placement_epoch),
                    str(migration.destination_epoch),
                    str(source.byte_size),
                ]
            )
        )

        self._migrations[
            migration.migration_id
        ] = migration

        source.state = PlacementState.MOVING
        source.updated_at = _now()

        self.backend.save_placement(
            source
        )

        self.backend.save_migration(
            migration
        )

        self._emit_event(
            CapacityEventType.MIGRATION_PLANNED,
            placement_ids=[
                source_placement_id
            ],
            migration_ids=[
                migration.migration_id
            ],
        )

        return migration

    def start_migration(
        self,
        migration_id: str,
    ) -> StorageMigration:

        migration = self._get_migration(
            migration_id
        )

        if (
            self.policy.epoch_fencing_enabled
            and migration.source_epoch
            > self._epoch
        ):
            migration.state = MigrationState.FENCED

            self.backend.save_migration(
                migration
            )

            self._emit_event(
                CapacityEventType.MIGRATION_FENCED,
                migration_ids=[
                    migration_id
                ],
            )

            return migration

        migration.state = (
            MigrationState.COPYING
        )

        migration.started_at = _now()

        self.backend.save_migration(
            migration
        )

        self._emit_event(
            CapacityEventType.MIGRATION_STARTED,
            migration_ids=[
                migration_id
            ],
        )

        return migration

    def verify_migration(
        self,
        migration_id: str,
    ) -> StorageMigration:

        migration = self._get_migration(
            migration_id
        )

        if (
            self.policy.migration_verification_required
            and not migration.checksum
        ):
            migration.state = (
                MigrationState.ABORTED
            )

            self.backend.save_migration(
                migration
            )

            return migration

        migration.state = (
            MigrationState.VERIFYING
        )

        migration.verified_at = _now()

        self.backend.save_migration(
            migration
        )

        self._emit_event(
            CapacityEventType.MIGRATION_VERIFIED,
            migration_ids=[
                migration_id
            ],
        )

        return migration

    def cutover_migration(
        self,
        migration_id: str,
    ) -> StorageMigration:

        migration = self._get_migration(
            migration_id
        )

        if (
            self.policy.atomic_cutover_required
            and migration.verified_at is None
        ):
            raise RuntimeError(
                "Migration must be verified before cutover"
            )

        if (
            self.policy.epoch_fencing_enabled
            and migration.destination_epoch
            < self._epoch
        ):
            migration.state = MigrationState.FENCED

            self.backend.save_migration(
                migration
            )

            self._emit_event(
                CapacityEventType.MIGRATION_FENCED,
                migration_ids=[
                    migration_id
                ],
            )

            return migration

        migration.state = (
            MigrationState.CUTOVER
        )

        migration.cutover_at = _now()

        self.backend.save_migration(
            migration
        )

        self._emit_event(
            CapacityEventType.MIGRATION_CUTOVER,
            migration_ids=[
                migration_id
            ],
        )

        return migration

    def complete_migration(
        self,
        migration_id: str,
    ) -> StorageMigration:

        migration = self._get_migration(
            migration_id
        )

        if (
            self.policy.atomic_cutover_required
            and migration.cutover_at is None
        ):
            raise RuntimeError(
                "Migration must reach cutover before completion"
            )

        source = self._get_placement(
            migration.source_placement_id
        )

        new_placement = self.create_placement(
            logical_object_id=(
                migration.logical_object_id
            ),
            partition_id=(
                migration.partition_id
            ),
            tier=(
                migration.destination_tier
            ),
            region=(
                migration.destination_region
            ),
            zone=(
                migration.destination_zone
            ),
            node_id=(
                migration.destination_node_id
            ),
            byte_size=(
                migration.bytes_to_move
            ),
            source_placement_id=(
                source.placement_id
            ),
        )

        new_placement.state = (
            PlacementState.ACTIVE
        )

        self.backend.save_placement(
            new_placement
        )

        source.state = (
            PlacementState.RETIRED
        )

        source.updated_at = _now()

        self.backend.save_placement(
            source
        )

        migration.state = (
            MigrationState.COMPLETED
        )

        migration.completed_at = _now()

        migration.destination_epoch = (
            self._epoch
        )

        self.backend.save_migration(
            migration
        )

        self._emit_event(
            CapacityEventType.MIGRATION_COMPLETED,
            placement_ids=[
                source.placement_id,
                new_placement.placement_id,
            ],
            migration_ids=[
                migration_id
            ],
        )

        return migration

    def abort_migration(
        self,
        migration_id: str,
        reason: str = "",
    ) -> StorageMigration:

        migration = self._get_migration(
            migration_id
        )

        migration.state = (
            MigrationState.ABORTED
        )

        migration.metadata[
            "abort_reason"
        ] = reason

        source = self._get_placement(
            migration.source_placement_id
        )

        source.state = (
            PlacementState.ACTIVE
        )

        source.updated_at = _now()

        self.backend.save_placement(
            source
        )

        self.backend.save_migration(
            migration
        )

        self._emit_event(
            CapacityEventType.MIGRATION_ABORTED,
            migration_ids=[
                migration_id
            ],
            metadata={
                "reason": reason,
            },
        )

        return migration

    # ------------------------------------------------------------------
    # Tier promotion / demotion
    # ------------------------------------------------------------------

    def promote_tier(
        self,
        logical_object_id: str,
        source_placement_id: str,
        destination_region: str,
        destination_zone: str,
        destination_node_id: str,
    ) -> StorageMigration:

        source = self._get_placement(
            source_placement_id
        )

        destination_tier = (
            StorageTier.HOT
        )

        self._emit_event(
            CapacityEventType.TIER_PROMOTION,
            placement_ids=[
                source_placement_id
            ],
        )

        return self.plan_migration(
            logical_object_id=logical_object_id,
            partition_id=source.partition_id,
            source_placement_id=source_placement_id,
            destination_tier=destination_tier,
            destination_region=destination_region,
            destination_zone=destination_zone,
            destination_node_id=destination_node_id,
            reason=MigrationReason.TIER_POLICY,
        )

    def demote_tier(
        self,
        logical_object_id: str,
        source_placement_id: str,
        destination_tier: StorageTier,
        destination_region: str,
        destination_zone: str,
        destination_node_id: str,
    ) -> StorageMigration:

        source = self._get_placement(
            source_placement_id
        )

        if destination_tier == StorageTier.HOT:
            raise ValueError(
                "Demotion target cannot be HOT"
            )

        self._emit_event(
            CapacityEventType.TIER_DEMOTION,
            placement_ids=[
                source_placement_id
            ],
        )

        return self.plan_migration(
            logical_object_id=logical_object_id,
            partition_id=source.partition_id,
            source_placement_id=source_placement_id,
            destination_tier=destination_tier,
            destination_region=destination_region,
            destination_zone=destination_zone,
            destination_node_id=destination_node_id,
            reason=MigrationReason.TIER_POLICY,
        )

    # ------------------------------------------------------------------
    # Rebalancing
    # ------------------------------------------------------------------

    def plan_rebalance(
        self,
        *,
        scope: CapacityScope,
        scope_id: str,
        source_placement_ids: Sequence[str],
        destination_tiers: Optional[
            Dict[str, StorageTier]
        ] = None,
        destination_nodes: Optional[
            Dict[str, str]
        ] = None,
    ) -> StorageRebalancePlan:

        plan = StorageRebalancePlan(
            rebalance_id=_new_id("rebalance"),
            scope=scope,
            scope_id=scope_id,
            source_placement_ids=list(
                source_placement_ids
            ),
            destination_tiers=dict(
                destination_tiers or {}
            ),
            destination_nodes=dict(
                destination_nodes or {}
            ),
        )

        self._emit_event(
            CapacityEventType.REBALANCE_PLANNED,
            placement_ids=list(
                source_placement_ids
            ),
        )

        return plan

    # ------------------------------------------------------------------
    # Capacity expansion
    # ------------------------------------------------------------------

    def expand_capacity(
        self,
        capacity_id: str,
        additional_bytes: int,
    ) -> StorageCapacityExpansion:

        if additional_bytes <= 0:
            raise ValueError(
                "additional_bytes must be greater than zero"
            )

        record = self._get_capacity(
            capacity_id
        )

        previous = record.total_bytes

        record.total_bytes += (
            additional_bytes
        )

        record.epoch = self._epoch
        record.updated_at = _now()

        self.backend.save_capacity(
            record
        )

        expansion = StorageCapacityExpansion(
            expansion_id=_new_id(
                "capacity-expansion"
            ),
            scope=record.scope,
            scope_id=record.scope_id,
            tier=record.tier,
            region=record.region,
            zone=record.zone,
            node_id=record.node_id,
            additional_bytes=additional_bytes,
            previous_total_bytes=previous,
            resulting_total_bytes=record.total_bytes,
            epoch=self._epoch,
        )

        self._expansions[
            expansion.expansion_id
        ] = expansion

        self._emit_event(
            CapacityEventType.CAPACITY_EXPANSION,
            capacity_ids=[
                capacity_id
            ],
            metadata={
                "additional_bytes":
                    str(additional_bytes),
            },
        )

        return expansion

    # ------------------------------------------------------------------
    # Evacuation
    # ------------------------------------------------------------------

    def begin_storage_evacuation(
        self,
        node_id: str,
    ) -> List[StoragePlacement]:

        affected = [
            placement
            for placement
            in self._placements.values()
            if placement.node_id == node_id
            and placement.state
            != PlacementState.RETIRED
        ]

        for placement in affected:

            placement.state = (
                PlacementState.DRAINING
            )

            placement.updated_at = _now()

            self.backend.save_placement(
                placement
            )

        self._emit_event(
            CapacityEventType.STORAGE_EVACUATION,
            placement_ids=[
                placement.placement_id
                for placement in affected
            ],
            metadata={
                "node_id": node_id,
            },
        )

        return affected

    # ------------------------------------------------------------------
    # Snapshot
    # ------------------------------------------------------------------

    def snapshot(
        self,
    ) -> StorageCapacitySnapshot:

        records = list(
            self._capacities.values()
        )

        total = sum(
            record.total_bytes
            for record in records
        )

        used = sum(
            record.used_bytes
            for record in records
        )

        reserved = sum(
            record.reserved_bytes
            for record in records
        )

        pressure_by_tier = {
            tier.value: self.pressure(tier)
            for tier in StorageTier
        }

        active_migrations = sum(
            1
            for migration
            in self._migrations.values()
            if migration.state
            not in (
                MigrationState.COMPLETED,
                MigrationState.ABORTED,
                MigrationState.FENCED,
            )
        )

        return StorageCapacitySnapshot(
            namespace=self.namespace,
            epoch=self._epoch,
            total_bytes=total,
            used_bytes=used,
            reserved_bytes=reserved,
            available_bytes=max(
                0,
                total - used - reserved,
            ),
            capacity_records=len(
                records
            ),
            placement_count=len(
                self._placements
            ),
            active_migration_count=(
                active_migrations
            ),
            pressure_by_tier=pressure_by_tier,
        )

    # ------------------------------------------------------------------
    # Checkpoint
    # ------------------------------------------------------------------

    def checkpoint(
        self,
    ) -> StorageTieringCheckpoint:

        capacity_ids = sorted(
            self._capacities.keys()
        )

        placement_ids = sorted(
            self._placements.keys()
        )

        migration_ids = sorted(
            self._migrations.keys()
        )

        checksum = _hash(
            "|".join(
                [
                    self.namespace,
                    str(self._epoch),
                    ",".join(capacity_ids),
                    ",".join(placement_ids),
                    ",".join(migration_ids),
                ]
            )
        )

        checkpoint = StorageTieringCheckpoint(
            checkpoint_id=_new_id(
                "tiering-checkpoint"
            ),
            namespace=self.namespace,
            epoch=self._epoch,
            capacity_record_ids=capacity_ids,
            placement_ids=placement_ids,
            migration_ids=migration_ids,
            checksum=checksum,
        )

        self._checkpoints[
            checkpoint.checkpoint_id
        ] = checkpoint

        self.backend.save_checkpoint(
            checkpoint
        )

        self._emit_event(
            CapacityEventType.CHECKPOINT_CREATED,
        )

        return checkpoint

    # ------------------------------------------------------------------
    # Capacity
    # ------------------------------------------------------------------

    def capacity(
        self,
    ) -> StorageTieringCapacity:

        total = sum(
            record.total_bytes
            for record in self._capacities.values()
        )

        used = sum(
            record.used_bytes
            for record in self._capacities.values()
        )

        reserved = sum(
            record.reserved_bytes
            for record in self._capacities.values()
        )

        active_migrations = sum(
            1
            for migration
            in self._migrations.values()
            if migration.state
            not in (
                MigrationState.COMPLETED,
                MigrationState.ABORTED,
                MigrationState.FENCED,
            )
        )

        return StorageTieringCapacity(
            namespace=self.namespace,
            capacity_record_count=len(
                self._capacities
            ),
            placement_count=len(
                self._placements
            ),
            active_migration_count=(
                active_migrations
            ),
            expansion_count=len(
                self._expansions
            ),
            checkpoint_count=len(
                self._checkpoints
            ),
            current_epoch=self._epoch,
            total_bytes=total,
            used_bytes=used,
            reserved_bytes=reserved,
            available_bytes=max(
                0,
                total - used - reserved,
            ),
            supports_hot_tier=True,
            supports_warm_tier=True,
            supports_cold_tier=True,
            supports_archive_tier=True,
            supports_horizontal_capacity_growth=True,
            supports_partition_rebalancing=True,
            supports_tier_migration=True,
        )

    # ------------------------------------------------------------------
    # Architecture
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

            "pipeline": [
                "index_and_document_data",
                "capacity_model",
                "storage_tiers",
                "tier_placement",
                "capacity_monitoring",
                "rebalancing",
                "data_migration",
                "capacity_expansion",
                "storage_pressure_control",
            ],

            "storage_tiers": [
                tier.value
                for tier in StorageTier
            ],

            "capacity_scopes": [
                scope.value
                for scope in CapacityScope
            ],

            "capacity_pressure_states": [
                pressure.value
                for pressure in CapacityPressure
            ],

            "migration_states": [
                state.value
                for state in MigrationState
            ],

            "supports": {
                "hot_storage": True,
                "warm_storage": True,
                "cold_storage": True,
                "archive_storage": True,

                "capacity_accounting": True,
                "tier_policy": True,
                "tier_promotion": True,
                "tier_demotion": True,

                "capacity_pressure_detection": True,

                "partition_rebalancing": True,
                "node_rebalancing": True,
                "zone_rebalancing": True,
                "region_rebalancing": True,

                "storage_migration": True,
                "migration_verification": True,
                "atomic_migration_cutover": True,

                "capacity_expansion": True,
                "storage_evacuation": True,

                "epoch_fencing": True,
                "checkpointing": True,

                "horizontal_storage_growth": True,
                "replaceable_backend": True,
            },

            "global_constraints": {
                "single_global_disk": False,
                "single_global_filesystem": False,
                "single_global_storage_node": False,
                "single_global_storage_database": False,
                "single_global_capacity_record": False,
                "fixed_global_storage_capacity": False,
                "fixed_global_document_storage": False,
                "fixed_global_index_storage": False,
                "fixed_global_partition_count": False,
                "fixed_global_node_count": False,
            },

            "placement_model": {
                "logical_identity_separate_from_physical_location": True,
                "partition_aware": True,
                "region_aware": True,
                "zone_aware": True,
                "node_aware": True,
                "tier_aware": True,
            },

            "growth_model": {
                "add_storage_nodes": True,
                "add_storage_zones": True,
                "add_storage_regions": True,
                "expand_existing_capacity": True,
                "rebalance_existing_partitions": True,
                "migrate_between_tiers": True,
                "no_global_fixed_ceiling": True,
            },

            "safety": {
                "migration_checksum": True,
                "migration_verification": True,
                "epoch_fencing": True,
                "atomic_cutover": True,
                "checkpoint_metadata": True,
                "source_retirement_after_cutover": True,
            },
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_capacity(
        self,
        capacity_id: str,
    ) -> StorageCapacityRecord:

        record = self._capacities.get(
            capacity_id
        )

        if record is None:

            record = self.backend.get_capacity(
                capacity_id
            )

        if record is None:
            raise KeyError(
                f"Unknown capacity record: {capacity_id}"
            )

        return record

    def _get_placement(
        self,
        placement_id: str,
    ) -> StoragePlacement:

        placement = self._placements.get(
            placement_id
        )

        if placement is None:

            placement = self.backend.get_placement(
                placement_id
            )

        if placement is None:
            raise KeyError(
                f"Unknown placement: {placement_id}"
            )

        return placement

    def _get_migration(
        self,
        migration_id: str,
    ) -> StorageMigration:

        migration = self._migrations.get(
            migration_id
        )

        if migration is None:
            raise KeyError(
                f"Unknown migration: {migration_id}"
            )

        return migration

    def _emit_event(
        self,
        event_type: CapacityEventType,
        *,
        capacity_ids: Optional[
            Sequence[str]
        ] = None,
        placement_ids: Optional[
            Sequence[str]
        ] = None,
        migration_ids: Optional[
            Sequence[str]
        ] = None,
        metadata: Optional[
            Dict[str, str]
        ] = None,
    ) -> StorageCapacityEvent:

        event = StorageCapacityEvent(
            event_id=_new_id(
                "capacity-event"
            ),
            event_type=event_type,
            namespace=self.namespace,
            epoch=self._epoch,
            capacity_ids=list(
                capacity_ids or []
            ),
            placement_ids=list(
                placement_ids or []
            ),
            migration_ids=list(
                migration_ids or []
            ),
            metadata=dict(metadata or {}),
        )

        self._events.append(event)

        self.backend.save_event(
            event
        )

        return event

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def capacities(
        self,
    ) -> Dict[
        str,
        StorageCapacityRecord,
    ]:

        return dict(self._capacities)

    @property
    def placements(
        self,
    ) -> Dict[
        str,
        StoragePlacement,
    ]:

        return dict(self._placements)

    @property
    def migrations(
        self,
    ) -> Dict[
        str,
        StorageMigration,
    ]:

        return dict(self._migrations)

    @property
    def expansions(
        self,
    ) -> Dict[
        str,
        StorageCapacityExpansion,
    ]:

        return dict(self._expansions)

    @property
    def checkpoints(
        self,
    ) -> Dict[
        str,
        StorageTieringCheckpoint,
    ]:

        return dict(self._checkpoints)

    @property
    def events(
        self,
    ) -> List[
        StorageCapacityEvent
    ]:

        return list(self._events)


# ---------------------------------------------------------------------------
# Stable aliases
# ---------------------------------------------------------------------------

StorageCapacityTiering = (
    StorageCapacityTieringArchitecture
)

GlobalStorageCapacity = (
    StorageCapacityTieringArchitecture
)

Phase10_8StorageCapacityTiering = (
    StorageCapacityTieringArchitecture
)


__all__ = [
    "ARCHITECTURE_VERSION",
    "GLOBAL_SCALE_TARGET",
    "GOOGLE_SCALE_CAPABILITY_TARGET",
    "GOOGLE_TECHNOLOGY_DEPENDENCY",

    "StorageTier",
    "StorageState",
    "CapacityPressure",
    "PlacementState",
    "MigrationState",
    "MigrationReason",
    "CapacityScope",
    "CapacityEventType",

    "StorageCapacityRecord",
    "StorageCapacityPolicy",
    "StoragePlacement",
    "TierPlacementDecision",
    "StorageMigration",
    "StorageRebalancePlan",
    "StorageCapacityExpansion",
    "StorageTieringCheckpoint",
    "StorageCapacityEvent",
    "StorageCapacitySnapshot",
    "StorageTieringCapacity",

    "StorageCapacityTieringBackend",
    "InMemoryStorageCapacityTieringMetadata",

    "StorageCapacityTieringArchitecture",

    "StorageCapacityTiering",
    "GlobalStorageCapacity",
    "Phase10_8StorageCapacityTiering",
]
