"""
OUR SEARCH — Global Index Storage Architecture

Phase 10.1
Version: global-index-storage-architecture.v1

Purpose
-------
Define the global storage architecture for an enormous independent search
engine targeting billions to potentially trillions of public-Web resources.

This module is an architecture/control-plane layer. It does NOT attempt to
pretend that a local filesystem, JSON file, SQLite database, or one machine
is a global storage system.

The architecture separates:

    global identity
        ↓
    logical index partitions
        ↓
    physical placement
        ↓
    immutable index segments
        ↓
    replicas
        ↓
    storage tiers
        ↓
    checkpoints / recovery
        ↓
    future distributed storage backends

Design principles
-----------------
- No Google technology or Google dependency.
- No single global documents.json.
- No single global segment directory.
- No single global index database.
- No fixed global document ceiling.
- No fixed global segment ceiling.
- No fixed global partition ceiling.
- Logical partitions are independent from physical machines.
- Segments are immutable after publication.
- Placement is epoch-aware.
- Replicas are explicitly represented.
- Storage backends are replaceable.
- Current local storage implementations remain usable underneath this layer.
- The architecture is designed for global horizontal expansion.

Important:
This module defines contracts and architecture. It does not prove that OUR
SEARCH currently operates at Google-scale throughput or corpus size.
"""

from __future__ import annotations

import hashlib
import threading
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Iterable, List, Mapping, Optional, Protocol, Sequence, Tuple


ARCHITECTURE_VERSION = "global-index-storage-architecture.v1"


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class PartitionState(str, Enum):
    ACTIVE = "active"
    CREATING = "creating"
    MOVING = "moving"
    SPLITTING = "splitting"
    MERGING = "merging"
    DRAINING = "draining"
    RECOVERING = "recovering"
    DEGRADED = "degraded"
    FENCED = "fenced"
    REMOVED = "removed"


class ReplicaState(str, Enum):
    BUILDING = "building"
    ACTIVE = "active"
    VERIFYING = "verifying"
    DEGRADED = "degraded"
    STALE = "stale"
    DRAINING = "draining"
    FAILED = "failed"
    FENCED = "fenced"


class StorageTier(str, Enum):
    HOT = "hot"
    WARM = "warm"
    COLD = "cold"
    ARCHIVE = "archive"


class SegmentState(str, Enum):
    BUILDING = "building"
    PUBLISHED = "published"
    MOVING = "moving"
    VERIFYING = "verifying"
    DEGRADED = "degraded"
    RETIRED = "retired"
    DELETED = "deleted"


class DocumentState(str, Enum):
    ACTIVE = "active"
    DELETED = "deleted"
    SUPERSEDED = "superseded"


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------


def _now() -> float:
    return time.time()


def _stable_hash(value: str) -> str:
    if not isinstance(value, str):
        raise TypeError("value must be a string")

    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _stable_partition(hash_value: str, partition_count: int) -> int:
    if partition_count <= 0:
        raise ValueError("partition_count must be greater than zero")

    numeric = int(hash_value, 16)
    return numeric % partition_count


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex}"


# ---------------------------------------------------------------------------
# Global document identity
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class IndexDocumentIdentity:
    """
    Globally stable identity for one indexed Web resource.

    document_id
        OUR SEARCH's logical document identifier.

    canonical_url
        Canonical public-Web URL represented by the document.

    content_version
        Version/fingerprint representing the indexed content state.

    partition_id
        Logical storage partition responsible for the document.

    shard_key
        Stable routing key.

    fingerprint
        Optional content identity/fingerprint.

    created_at
        Creation timestamp for this identity record.
    """

    document_id: str
    canonical_url: str
    content_version: str
    partition_id: str
    shard_key: str
    fingerprint: str = ""
    created_at: float = field(default_factory=_now)

    def __post_init__(self) -> None:
        if not self.document_id:
            raise ValueError("document_id is required")

        if not self.canonical_url:
            raise ValueError("canonical_url is required")

        if not self.content_version:
            raise ValueError("content_version is required")

        if not self.partition_id:
            raise ValueError("partition_id is required")

        if not self.shard_key:
            raise ValueError("shard_key is required")


# ---------------------------------------------------------------------------
# Storage replicas
# ---------------------------------------------------------------------------


@dataclass
class StorageReplica:
    """
    Physical replica of an immutable index object.

    region_id / zone_id / node_id
        Physical fault-domain placement.

    epoch
        Placement epoch protecting against stale owners.

    checksum
        Integrity verification value.

    size_bytes
        Physical stored size.

    last_verified_at
        Last successful integrity verification timestamp.
    """

    replica_id: str
    object_id: str
    region_id: str
    zone_id: str
    node_id: str
    state: ReplicaState = ReplicaState.BUILDING
    epoch: int = 0
    checksum: str = ""
    size_bytes: int = 0
    last_verified_at: float = 0.0
    created_at: float = field(default_factory=_now)

    def activate(
        self,
        *,
        epoch: int,
        checksum: str = "",
        size_bytes: int = 0,
    ) -> None:
        if epoch < self.epoch:
            raise ValueError(
                "replica activation epoch cannot move backwards"
            )

        self.epoch = epoch
        self.checksum = checksum
        self.size_bytes = max(0, int(size_bytes))
        self.last_verified_at = _now()
        self.state = ReplicaState.ACTIVE

    def mark_degraded(self) -> None:
        self.state = ReplicaState.DEGRADED

    def mark_failed(self) -> None:
        self.state = ReplicaState.FAILED

    def fence(self, epoch: int) -> None:
        if epoch < self.epoch:
            raise ValueError("fencing epoch cannot move backwards")

        self.epoch = epoch
        self.state = ReplicaState.FENCED


# ---------------------------------------------------------------------------
# Index segment placement
# ---------------------------------------------------------------------------


@dataclass
class IndexSegmentPlacement:
    """
    Placement metadata for an immutable index segment.

    A segment is logically attached to a partition but can have multiple
    physical replicas across independent fault domains.

    Once published, segment contents are immutable. Movement creates new
    physical placement rather than mutating the logical segment contents.
    """

    segment_id: str
    partition_id: str
    generation: int
    epoch: int
    storage_tier: StorageTier
    state: SegmentState = SegmentState.BUILDING
    size_bytes: int = 0
    document_count: int = 0
    vocabulary_size: int = 0
    checksum: str = ""
    replicas: Dict[str, StorageReplica] = field(default_factory=dict)
    created_at: float = field(default_factory=_now)
    published_at: float = 0.0

    def publish(self, *, epoch: int, checksum: str = "") -> None:
        if epoch < self.epoch:
            raise ValueError("publication epoch cannot move backwards")

        if self.state not in (
            SegmentState.BUILDING,
            SegmentState.VERIFYING,
        ):
            raise ValueError(
                f"segment cannot be published from state {self.state.value}"
            )

        self.epoch = epoch
        self.checksum = checksum or self.checksum
        self.state = SegmentState.PUBLISHED
        self.published_at = _now()

    def add_replica(self, replica: StorageReplica) -> None:
        if replica.object_id != self.segment_id:
            raise ValueError("replica object_id does not match segment_id")

        self.replicas[replica.replica_id] = replica

    def active_replica_count(self) -> int:
        return sum(
            1
            for replica in self.replicas.values()
            if replica.state == ReplicaState.ACTIVE
        )

    def healthy_replica_count(self) -> int:
        return sum(
            1
            for replica in self.replicas.values()
            if replica.state == ReplicaState.ACTIVE
            and replica.epoch == self.epoch
        )


# ---------------------------------------------------------------------------
# Logical partitions
# ---------------------------------------------------------------------------


@dataclass
class IndexStoragePartition:
    """
    Logical index-storage partition.

    A logical partition is not a machine.

    It can move between machines, zones, regions, or storage backends without
    changing the logical identity of the partition.
    """

    partition_id: str
    partition_number: int
    namespace: str
    epoch: int = 0
    state: PartitionState = PartitionState.CREATING

    primary_region: str = ""
    replica_regions: Tuple[str, ...] = ()

    document_count: int = 0
    segment_count: int = 0
    vocabulary_entries: int = 0
    byte_size: int = 0

    hot_score: float = 0.0
    storage_tier: StorageTier = StorageTier.HOT

    updated_at: float = field(default_factory=_now)

    def activate(self, *, epoch: int) -> None:
        if epoch < self.epoch:
            raise ValueError("partition activation epoch cannot move backwards")

        self.epoch = epoch
        self.state = PartitionState.ACTIVE
        self.updated_at = _now()

    def fence(self, *, epoch: int) -> None:
        if epoch < self.epoch:
            raise ValueError("partition fencing epoch cannot move backwards")

        self.epoch = epoch
        self.state = PartitionState.FENCED
        self.updated_at = _now()

    def update_capacity(
        self,
        *,
        document_delta: int = 0,
        segment_delta: int = 0,
        vocabulary_delta: int = 0,
        byte_delta: int = 0,
    ) -> None:
        self.document_count = max(0, self.document_count + document_delta)
        self.segment_count = max(0, self.segment_count + segment_delta)
        self.vocabulary_entries = max(
            0,
            self.vocabulary_entries + vocabulary_delta,
        )
        self.byte_size = max(0, self.byte_size + byte_delta)
        self.updated_at = _now()

    def set_heat(self, hot_score: float) -> None:
        self.hot_score = max(0.0, float(hot_score))
        self.updated_at = _now()


# ---------------------------------------------------------------------------
# Storage capacity
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class IndexStorageCapacity:
    """
    Snapshot of logical storage capacity.

    None of these values represent a hard global ceiling.

    They are observations/capacity advertisements from the current topology.
    """

    logical_partition_count: int
    active_partition_count: int
    document_count: int
    segment_count: int
    vocabulary_entries: int
    stored_bytes: int
    replicated_bytes: int
    active_replica_count: int

    available_bytes: Optional[int] = None
    observed_at: float = field(default_factory=_now)

    @property
    def total_bytes(self) -> int:
        return self.stored_bytes + self.replicated_bytes


# ---------------------------------------------------------------------------
# Routing result
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class IndexStorageRoute:
    """
    Result of global index-storage routing.
    """

    object_type: str
    object_id: str
    partition_id: str
    partition_number: int
    epoch: int
    primary_region: str
    replica_regions: Tuple[str, ...]
    storage_tier: StorageTier


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class IndexStorageEvent:
    """
    Architecture-level storage event.

    Events allow future distributed systems to connect this architecture to
    durable event streams without making this module dependent on one queue,
    broker, database, or cloud vendor.
    """

    event_id: str
    event_type: str
    object_type: str
    object_id: str

    partition_id: str = ""
    epoch: int = 0

    region_id: str = ""
    node_id: str = ""

    metadata: Mapping[str, object] = field(default_factory=dict)
    created_at: float = field(default_factory=_now)


# ---------------------------------------------------------------------------
# Checkpoints
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class IndexStorageCheckpoint:
    """
    Consistent logical checkpoint of storage metadata.

    A production implementation can persist this through a distributed
    metadata/checkpoint service.
    """

    checkpoint_id: str
    architecture_version: str
    epoch: int

    partition_count: int
    partition_ids: Tuple[str, ...]

    segment_ids: Tuple[str, ...]

    created_at: float = field(default_factory=_now)


# ---------------------------------------------------------------------------
# Storage backend protocol
# ---------------------------------------------------------------------------


class GlobalIndexStorageBackend(Protocol):
    """
    Replaceable metadata/storage backend contract.

    The reference implementation below is intentionally in-memory.

    Production implementations can connect this contract to:
        - distributed metadata services
        - partitioned document stores
        - object storage
        - distributed filesystems
        - replicated segment stores
        - immutable object stores
        - consensus-backed control planes
    """

    def put_partition(self, partition: IndexStoragePartition) -> None:
        ...

    def get_partition(
        self,
        partition_id: str,
    ) -> Optional[IndexStoragePartition]:
        ...

    def list_partitions(self) -> Sequence[IndexStoragePartition]:
        ...

    def put_segment(self, segment: IndexSegmentPlacement) -> None:
        ...

    def get_segment(
        self,
        segment_id: str,
    ) -> Optional[IndexSegmentPlacement]:
        ...

    def list_segments(self) -> Sequence[IndexSegmentPlacement]:
        ...

    def append_event(self, event: IndexStorageEvent) -> None:
        ...

    def list_events(self) -> Sequence[IndexStorageEvent]:
        ...


# ---------------------------------------------------------------------------
# Reference metadata backend
# ---------------------------------------------------------------------------


class InMemoryGlobalIndexStorageMetadata:
    """
    Reference metadata adapter.

    This exists to make the architecture executable and understandable
    without forcing a particular database.

    It is NOT intended to represent the production global storage backend.
    """

    def __init__(self) -> None:
        self._partitions: Dict[str, IndexStoragePartition] = {}
        self._segments: Dict[str, IndexSegmentPlacement] = {}
        self._events: List[IndexStorageEvent] = []
        self._lock = threading.RLock()

    def put_partition(self, partition: IndexStoragePartition) -> None:
        with self._lock:
            self._partitions[partition.partition_id] = partition

    def get_partition(
        self,
        partition_id: str,
    ) -> Optional[IndexStoragePartition]:
        with self._lock:
            return self._partitions.get(partition_id)

    def list_partitions(self) -> Sequence[IndexStoragePartition]:
        with self._lock:
            return tuple(self._partitions.values())

    def put_segment(self, segment: IndexSegmentPlacement) -> None:
        with self._lock:
            self._segments[segment.segment_id] = segment

    def get_segment(
        self,
        segment_id: str,
    ) -> Optional[IndexSegmentPlacement]:
        with self._lock:
            return self._segments.get(segment_id)

    def list_segments(self) -> Sequence[IndexSegmentPlacement]:
        with self._lock:
            return tuple(self._segments.values())

    def append_event(self, event: IndexStorageEvent) -> None:
        with self._lock:
            self._events.append(event)

    def list_events(self) -> Sequence[IndexStorageEvent]:
        with self._lock:
            return tuple(self._events)


# ---------------------------------------------------------------------------
# Storage policy
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class IndexStoragePolicy:
    """
    Placement policy for deciding storage tiers and replica targets.

    Thresholds are policy values, not global scale limits.
    """

    hot_score_threshold: float = 0.75
    warm_score_threshold: float = 0.35
    cold_score_threshold: float = 0.10

    default_replication_factor: int = 3

    require_distinct_regions: bool = True
    require_distinct_zones: bool = True

    def choose_tier(self, hot_score: float) -> StorageTier:
        score = max(0.0, float(hot_score))

        if score >= self.hot_score_threshold:
            return StorageTier.HOT

        if score >= self.warm_score_threshold:
            return StorageTier.WARM

        if score >= self.cold_score_threshold:
            return StorageTier.COLD

        return StorageTier.ARCHIVE

    def validate_replication_factor(self, factor: int) -> None:
        if factor <= 0:
            raise ValueError("replication factor must be greater than zero")


# ---------------------------------------------------------------------------
# Global Index Storage Architecture
# ---------------------------------------------------------------------------


class GlobalIndexStorageArchitecture:
    """
    Global index-storage architecture for OUR SEARCH.

    Responsibilities
    ----------------
    1. Stable document routing.
    2. Stable segment routing.
    3. Logical partition registry.
    4. Partition epochs/fencing.
    5. Segment placement metadata.
    6. Replica metadata.
    7. Storage-tier policy.
    8. Capacity accounting.
    9. Checkpoint creation.
    10. Architecture-level events.
    11. Backend abstraction.

    This class deliberately does NOT own the entire Web corpus.

    It defines how an enormous corpus can be divided and physically stored
    without requiring one machine or one database to contain everything.
    """

    def __init__(
        self,
        *,
        partition_count: int = 1_048_576,
        replication_factor: int = 3,
        namespace: str = "web-index",
        metadata_backend: Optional[GlobalIndexStorageBackend] = None,
        storage_policy: Optional[IndexStoragePolicy] = None,
        regions: Optional[Iterable[str]] = None,
    ) -> None:
        if partition_count <= 0:
            raise ValueError("partition_count must be greater than zero")

        if replication_factor <= 0:
            raise ValueError(
                "replication_factor must be greater than zero"
            )

        if not namespace:
            raise ValueError("namespace is required")

        self.partition_count = int(partition_count)
        self.replication_factor = int(replication_factor)
        self.namespace = namespace

        self.metadata = (
            metadata_backend
            if metadata_backend is not None
            else InMemoryGlobalIndexStorageMetadata()
        )

        self.storage_policy = (
            storage_policy
            if storage_policy is not None
            else IndexStoragePolicy()
        )

        self.storage_policy.validate_replication_factor(
            self.replication_factor
        )

        self.regions = tuple(
            sorted(set(regions or ()))
        )

        self._epoch = 1
        self._running = False

        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @property
    def epoch(self) -> int:
        with self._lock:
            return self._epoch

    @property
    def running(self) -> bool:
        with self._lock:
            return self._running

    def start(self) -> None:
        with self._lock:
            self._running = True

    def stop(self) -> None:
        with self._lock:
            self._running = False

    def advance_epoch(self) -> int:
        with self._lock:
            self._epoch += 1
            return self._epoch

    # ------------------------------------------------------------------
    # Partition identity / routing
    # ------------------------------------------------------------------

    def partition_number_for(self, routing_key: str) -> int:
        """
        Return the stable logical partition number for a routing key.

        SHA-256 provides deterministic distribution without depending on
        Google-specific technology.
        """
        if not routing_key:
            raise ValueError("routing_key is required")

        return _stable_partition(
            _stable_hash(routing_key),
            self.partition_count,
        )

    def partition_id_for(self, partition_number: int) -> str:
        if partition_number < 0:
            raise ValueError("partition_number cannot be negative")

        if partition_number >= self.partition_count:
            raise ValueError(
                "partition_number is outside the configured logical range"
            )

        return (
            f"{self.namespace}/"
            f"partition-{partition_number:012d}"
        )

    def partition_for(self, routing_key: str) -> IndexStoragePartition:
        number = self.partition_number_for(routing_key)
        partition_id = self.partition_id_for(number)

        partition = self.metadata.get_partition(partition_id)

        if partition is None:
            partition = IndexStoragePartition(
                partition_id=partition_id,
                partition_number=number,
                namespace=self.namespace,
                epoch=self.epoch,
                state=PartitionState.CREATING,
            )

            self.metadata.put_partition(partition)

        return partition

    def ensure_all_partition_metadata(self) -> None:
        """
        Materialize logical partition metadata.

        This method is intentionally optional.

        Production systems should normally create partitions lazily or use a
        distributed metadata service rather than allocating a local object
        for every logical partition.
        """
        for number in range(self.partition_count):
            partition_id = self.partition_id_for(number)

            if self.metadata.get_partition(partition_id) is None:
                self.metadata.put_partition(
                    IndexStoragePartition(
                        partition_id=partition_id,
                        partition_number=number,
                        namespace=self.namespace,
                        epoch=self.epoch,
                        state=PartitionState.CREATING,
                    )
                )

    # ------------------------------------------------------------------
    # Partition lifecycle
    # ------------------------------------------------------------------

    def register_partition(
        self,
        partition: IndexStoragePartition,
    ) -> IndexStoragePartition:
        if partition.namespace != self.namespace:
            raise ValueError(
                "partition namespace does not match architecture namespace"
            )

        with self._lock:
            existing = self.metadata.get_partition(
                partition.partition_id
            )

            if existing is not None and partition.epoch < existing.epoch:
                raise ValueError(
                    "cannot register partition with older epoch"
                )

            self.metadata.put_partition(partition)

            self._emit(
                event_type="partition_registered",
                object_type="partition",
                object_id=partition.partition_id,
                partition_id=partition.partition_id,
                epoch=partition.epoch,
            )

            return partition

    def activate_partition(
        self,
        partition_id: str,
        *,
        primary_region: str = "",
        replica_regions: Sequence[str] = (),
        epoch: Optional[int] = None,
    ) -> IndexStoragePartition:
        partition = self.metadata.get_partition(partition_id)

        if partition is None:
            raise KeyError(f"unknown partition: {partition_id}")

        activation_epoch = (
            self.epoch
            if epoch is None
            else int(epoch)
        )

        partition.primary_region = primary_region
        partition.replica_regions = tuple(
            sorted(set(replica_regions))
        )
        partition.activate(epoch=activation_epoch)

        self.metadata.put_partition(partition)

        self._emit(
            event_type="partition_activated",
            object_type="partition",
            object_id=partition_id,
            partition_id=partition_id,
            epoch=activation_epoch,
            region_id=primary_region,
        )

        return partition

    def fence_partition(
        self,
        partition_id: str,
        *,
        epoch: Optional[int] = None,
    ) -> IndexStoragePartition:
        partition = self.metadata.get_partition(partition_id)

        if partition is None:
            raise KeyError(f"unknown partition: {partition_id}")

        fencing_epoch = (
            self.advance_epoch()
            if epoch is None
            else int(epoch)
        )

        partition.fence(epoch=fencing_epoch)
        self.metadata.put_partition(partition)

        self._emit(
            event_type="partition_fenced",
            object_type="partition",
            object_id=partition_id,
            partition_id=partition_id,
            epoch=fencing_epoch,
        )

        return partition

    # ------------------------------------------------------------------
    # Document identity
    # ------------------------------------------------------------------

    def create_document_identity(
        self,
        *,
        document_id: str,
        canonical_url: str,
        content_version: str,
        fingerprint: str = "",
    ) -> IndexDocumentIdentity:
        if not document_id:
            raise ValueError("document_id is required")

        if not canonical_url:
            raise ValueError("canonical_url is required")

        routing_key = canonical_url

        partition = self.partition_for(routing_key)

        shard_key = _stable_hash(
            f"{self.namespace}:{canonical_url}"
        )

        identity = IndexDocumentIdentity(
            document_id=document_id,
            canonical_url=canonical_url,
            content_version=content_version,
            partition_id=partition.partition_id,
            shard_key=shard_key,
            fingerprint=fingerprint,
        )

        self._emit(
            event_type="document_identity_created",
            object_type="document",
            object_id=document_id,
            partition_id=partition.partition_id,
            epoch=self.epoch,
            metadata={
                "canonical_url": canonical_url,
                "content_version": content_version,
            },
        )

        return identity

    # ------------------------------------------------------------------
    # Segment identity / routing
    # ------------------------------------------------------------------

    def segment_partition_for(
        self,
        *,
        partition_id: str,
        segment_id: str,
    ) -> IndexStoragePartition:
        if not partition_id:
            raise ValueError("partition_id is required")

        if not segment_id:
            raise ValueError("segment_id is required")

        partition = self.metadata.get_partition(partition_id)

        if partition is None:
            raise KeyError(
                f"unknown partition: {partition_id}"
            )

        return partition

    def create_segment_placement(
        self,
        *,
        segment_id: str,
        partition_id: str,
        generation: int,
        size_bytes: int = 0,
        document_count: int = 0,
        vocabulary_size: int = 0,
        hot_score: float = 0.0,
        checksum: str = "",
        epoch: Optional[int] = None,
    ) -> IndexSegmentPlacement:
        if not segment_id:
            raise ValueError("segment_id is required")

        partition = self.segment_partition_for(
            partition_id=partition_id,
            segment_id=segment_id,
        )

        segment_epoch = (
            self.epoch
            if epoch is None
            else int(epoch)
        )

        tier = self.storage_policy.choose_tier(
            hot_score
        )

        return IndexSegmentPlacement(
            segment_id=segment_id,
            partition_id=partition.partition_id,
            generation=int(generation),
            epoch=segment_epoch,
            storage_tier=tier,
            size_bytes=max(0, int(size_bytes)),
            document_count=max(0, int(document_count)),
            vocabulary_size=max(0, int(vocabulary_size)),
            checksum=checksum,
        )

    def register_segment(
        self,
        segment: IndexSegmentPlacement,
    ) -> IndexSegmentPlacement:
        partition = self.metadata.get_partition(
            segment.partition_id
        )

        if partition is None:
            raise KeyError(
                f"unknown partition: {segment.partition_id}"
            )

        existing = self.metadata.get_segment(
            segment.segment_id
        )

        if existing is not None:
            if segment.epoch < existing.epoch:
                raise ValueError(
                    "cannot register segment with older epoch"
                )

            if existing.state == SegmentState.PUBLISHED:
                raise ValueError(
                    "published segment metadata is immutable"
                )

        self.metadata.put_segment(segment)

        self._emit(
            event_type="segment_registered",
            object_type="segment",
            object_id=segment.segment_id,
            partition_id=segment.partition_id,
            epoch=segment.epoch,
        )

        return segment

    def publish_segment(
        self,
        segment_id: str,
        *,
        checksum: str = "",
        epoch: Optional[int] = None,
    ) -> IndexSegmentPlacement:
        segment = self.metadata.get_segment(segment_id)

        if segment is None:
            raise KeyError(
                f"unknown segment: {segment_id}"
            )

        publication_epoch = (
            self.epoch
            if epoch is None
            else int(epoch)
        )

        segment.publish(
            epoch=publication_epoch,
            checksum=checksum,
        )

        self.metadata.put_segment(segment)

        partition = self.metadata.get_partition(
            segment.partition_id
        )

        if partition is not None:
            partition.update_capacity(
                segment_delta=1,
                document_delta=segment.document_count,
                vocabulary_delta=segment.vocabulary_size,
                byte_delta=segment.size_bytes,
            )

            self.metadata.put_partition(partition)

        self._emit(
            event_type="segment_published",
            object_type="segment",
            object_id=segment.segment_id,
            partition_id=segment.partition_id,
            epoch=publication_epoch,
        )

        return segment

    # ------------------------------------------------------------------
    # Replica management
    # ------------------------------------------------------------------

    def add_segment_replica(
        self,
        *,
        segment_id: str,
        region_id: str,
        zone_id: str,
        node_id: str,
        replica_id: Optional[str] = None,
        epoch: Optional[int] = None,
    ) -> StorageReplica:
        segment = self.metadata.get_segment(segment_id)

        if segment is None:
            raise KeyError(
                f"unknown segment: {segment_id}"
            )

        replica = StorageReplica(
            replica_id=replica_id or _new_id("replica"),
            object_id=segment_id,
            region_id=region_id,
            zone_id=zone_id,
            node_id=node_id,
            epoch=(
                segment.epoch
                if epoch is None
                else int(epoch)
            ),
        )

        segment.add_replica(replica)

        self.metadata.put_segment(segment)

        self._emit(
            event_type="segment_replica_added",
            object_type="segment_replica",
            object_id=replica.replica_id,
            partition_id=segment.partition_id,
            epoch=replica.epoch,
            region_id=region_id,
            node_id=node_id,
            metadata={
                "segment_id": segment_id,
                "zone_id": zone_id,
            },
        )

        return replica

    def activate_segment_replica(
        self,
        *,
        segment_id: str,
        replica_id: str,
        checksum: str = "",
        size_bytes: int = 0,
        epoch: Optional[int] = None,
    ) -> StorageReplica:
        segment = self.metadata.get_segment(segment_id)

        if segment is None:
            raise KeyError(
                f"unknown segment: {segment_id}"
            )

        replica = segment.replicas.get(replica_id)

        if replica is None:
            raise KeyError(
                f"unknown replica: {replica_id}"
            )

        activation_epoch = (
            segment.epoch
            if epoch is None
            else int(epoch)
        )

        replica.activate(
            epoch=activation_epoch,
            checksum=checksum or segment.checksum,
            size_bytes=size_bytes or segment.size_bytes,
        )

        self.metadata.put_segment(segment)

        self._emit(
            event_type="segment_replica_activated",
            object_type="segment_replica",
            object_id=replica_id,
            partition_id=segment.partition_id,
            epoch=activation_epoch,
            region_id=replica.region_id,
            node_id=replica.node_id,
        )

        return replica

    # ------------------------------------------------------------------
    # Storage tier movement
    # ------------------------------------------------------------------

    def retier_segment(
        self,
        segment_id: str,
        *,
        hot_score: float,
    ) -> IndexSegmentPlacement:
        segment = self.metadata.get_segment(segment_id)

        if segment is None:
            raise KeyError(
                f"unknown segment: {segment_id}"
            )

        if segment.state == SegmentState.DELETED:
            raise ValueError(
                "cannot retier deleted segment"
            )

        segment.storage_tier = (
            self.storage_policy.choose_tier(hot_score)
        )

        self.metadata.put_segment(segment)

        self._emit(
            event_type="segment_retiered",
            object_type="segment",
            object_id=segment_id,
            partition_id=segment.partition_id,
            epoch=segment.epoch,
            metadata={
                "storage_tier": segment.storage_tier.value,
                "hot_score": float(hot_score),
            },
        )

        return segment

    # ------------------------------------------------------------------
    # Routing
    # ------------------------------------------------------------------

    def route_document(
        self,
        *,
        document_id: str,
        canonical_url: str,
    ) -> IndexStorageRoute:
        partition = self.partition_for(canonical_url)

        return IndexStorageRoute(
            object_type="document",
            object_id=document_id,
            partition_id=partition.partition_id,
            partition_number=partition.partition_number,
            epoch=partition.epoch,
            primary_region=partition.primary_region,
            replica_regions=partition.replica_regions,
            storage_tier=partition.storage_tier,
        )

    def route_segment(
        self,
        segment_id: str,
    ) -> IndexStorageRoute:
        segment = self.metadata.get_segment(segment_id)

        if segment is None:
            raise KeyError(
                f"unknown segment: {segment_id}"
            )

        partition = self.metadata.get_partition(
            segment.partition_id
        )

        if partition is None:
            raise KeyError(
                f"unknown partition: {segment.partition_id}"
            )

        return IndexStorageRoute(
            object_type="segment",
            object_id=segment.segment_id,
            partition_id=segment.partition_id,
            partition_number=partition.partition_number,
            epoch=segment.epoch,
            primary_region=partition.primary_region,
            replica_regions=partition.replica_regions,
            storage_tier=segment.storage_tier,
        )

    # ------------------------------------------------------------------
    # Placement validation
    # ------------------------------------------------------------------

    def validate_replica_placement(
        self,
        segment_id: str,
    ) -> Mapping[str, object]:
        segment = self.metadata.get_segment(segment_id)

        if segment is None:
            raise KeyError(
                f"unknown segment: {segment_id}"
            )

        replicas = list(segment.replicas.values())

        regions = {
            replica.region_id
            for replica in replicas
            if replica.region_id
        }

        zones = {
            (replica.region_id, replica.zone_id)
            for replica in replicas
            if replica.region_id and replica.zone_id
        }

        healthy = [
            replica
            for replica in replicas
            if replica.state == ReplicaState.ACTIVE
            and replica.epoch == segment.epoch
        ]

        region_diversity_ok = (
            not self.storage_policy.require_distinct_regions
            or len(regions) >= min(
                self.replication_factor,
                max(1, len(regions)),
            )
        )

        zone_diversity_ok = (
            not self.storage_policy.require_distinct_zones
            or len(zones) >= min(
                self.replication_factor,
                max(1, len(zones)),
            )
        )

        return {
            "segment_id": segment_id,
            "required_replication_factor": self.replication_factor,
            "total_replicas": len(replicas),
            "healthy_replicas": len(healthy),
            "region_count": len(regions),
            "zone_count": len(zones),
            "region_diversity_ok": region_diversity_ok,
            "zone_diversity_ok": zone_diversity_ok,
            "replication_satisfied": (
                len(healthy) >= self.replication_factor
            ),
        }

    # ------------------------------------------------------------------
    # Capacity
    # ------------------------------------------------------------------

    def capacity(self) -> IndexStorageCapacity:
        partitions = list(
            self.metadata.list_partitions()
        )

        segments = list(
            self.metadata.list_segments()
        )

        active_partitions = sum(
            1
            for partition in partitions
            if partition.state == PartitionState.ACTIVE
        )

        document_count = sum(
            partition.document_count
            for partition in partitions
        )

        vocabulary_entries = sum(
            partition.vocabulary_entries
            for partition in partitions
        )

        stored_bytes = sum(
            segment.size_bytes
            for segment in segments
            if segment.state != SegmentState.DELETED
        )

        replicated_bytes = sum(
            replica.size_bytes
            for segment in segments
            for replica in segment.replicas.values()
            if replica.state == ReplicaState.ACTIVE
        )

        active_replica_count = sum(
            1
            for segment in segments
            for replica in segment.replicas.values()
            if replica.state == ReplicaState.ACTIVE
        )

        return IndexStorageCapacity(
            logical_partition_count=self.partition_count,
            active_partition_count=active_partitions,
            document_count=document_count,
            segment_count=len(segments),
            vocabulary_entries=vocabulary_entries,
            stored_bytes=stored_bytes,
            replicated_bytes=replicated_bytes,
            active_replica_count=active_replica_count,
        )

    # ------------------------------------------------------------------
    # Checkpointing
    # ------------------------------------------------------------------

    def create_checkpoint(self) -> IndexStorageCheckpoint:
        partitions = tuple(
            sorted(
                (
                    partition.partition_id
                    for partition in self.metadata.list_partitions()
                )
            )
        )

        segments = tuple(
            sorted(
                (
                    segment.segment_id
                    for segment in self.metadata.list_segments()
                )
            )
        )

        checkpoint = IndexStorageCheckpoint(
            checkpoint_id=_new_id("checkpoint"),
            architecture_version=ARCHITECTURE_VERSION,
            epoch=self.epoch,
            partition_count=self.partition_count,
            partition_ids=partitions,
            segment_ids=segments,
        )

        self._emit(
            event_type="storage_checkpoint_created",
            object_type="checkpoint",
            object_id=checkpoint.checkpoint_id,
            epoch=checkpoint.epoch,
            metadata={
                "partition_count": checkpoint.partition_count,
                "partition_metadata_count": len(
                    checkpoint.partition_ids
                ),
                "segment_count": len(
                    checkpoint.segment_ids
                ),
            },
        )

        return checkpoint

    # ------------------------------------------------------------------
    # Events
    # ------------------------------------------------------------------

    def _emit(
        self,
        *,
        event_type: str,
        object_type: str,
        object_id: str,
        partition_id: str = "",
        epoch: int = 0,
        region_id: str = "",
        node_id: str = "",
        metadata: Optional[Mapping[str, object]] = None,
    ) -> IndexStorageEvent:
        event = IndexStorageEvent(
            event_id=_new_id("storage-event"),
            event_type=event_type,
            object_type=object_type,
            object_id=object_id,
            partition_id=partition_id,
            epoch=epoch,
            region_id=region_id,
            node_id=node_id,
            metadata=dict(metadata or {}),
        )

        self.metadata.append_event(event)

        return event

    def events(self) -> Sequence[IndexStorageEvent]:
        return self.metadata.list_events()

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    def stats(self) -> Mapping[str, object]:
        capacity = self.capacity()

        partitions = list(
            self.metadata.list_partitions()
        )

        segments = list(
            self.metadata.list_segments()
        )

        return {
            "architecture_version": ARCHITECTURE_VERSION,
            "namespace": self.namespace,

            "logical_partition_count": self.partition_count,

            "observed_partition_metadata_count": len(
                partitions
            ),

            "active_partition_count": (
                capacity.active_partition_count
            ),

            "document_count": capacity.document_count,
            "segment_count": capacity.segment_count,
            "vocabulary_entries": (
                capacity.vocabulary_entries
            ),

            "stored_bytes": capacity.stored_bytes,
            "replicated_bytes": capacity.replicated_bytes,

            "active_replica_count": (
                capacity.active_replica_count
            ),

            "replication_factor": (
                self.replication_factor
            ),

            "epoch": self.epoch,
            "running": self.running,

            "storage_tiers": {
                tier.value: sum(
                    1
                    for segment in segments
                    if segment.storage_tier == tier
                )
                for tier in StorageTier
            },

            "partition_states": {
                state.value: sum(
                    1
                    for partition in partitions
                    if partition.state == state
                )
                for state in PartitionState
            },

            "segment_states": {
                state.value: sum(
                    1
                    for segment in segments
                    if segment.state == state
                )
                for state in SegmentState
            },

            "target_scale": (
                "billions_to_trillions_of_public_web_resources"
            ),

            "google_scale_capability_target": True,

            "google_technology_dependency": False,

            "fixed_global_document_limit": False,
            "fixed_global_segment_limit": False,
            "fixed_global_partition_limit": False,
            "fixed_global_replica_limit": False,

            "single_global_documents_file": False,
            "single_global_segment_directory": False,
            "single_global_index_database": False,

            "logical_physical_separation": True,
            "immutable_segment_model": True,
            "epoch_aware_placement": True,
            "replication_model": True,
            "storage_tiering_model": True,
            "checkpoint_model": True,
            "replaceable_backend": True,
        }


# ---------------------------------------------------------------------------
# Compatibility aliases
# ---------------------------------------------------------------------------


GlobalIndexStorage = GlobalIndexStorageArchitecture
Phase10_1GlobalIndexStorageArchitecture = (
    GlobalIndexStorageArchitecture
)


__all__ = [
    "ARCHITECTURE_VERSION",

    "PartitionState",
    "ReplicaState",
    "StorageTier",
    "SegmentState",
    "DocumentState",

    "IndexDocumentIdentity",
    "StorageReplica",
    "IndexSegmentPlacement",
    "IndexStoragePartition",
    "IndexStorageCapacity",
    "IndexStorageRoute",
    "IndexStorageEvent",
    "IndexStorageCheckpoint",

    "GlobalIndexStorageBackend",
    "InMemoryGlobalIndexStorageMetadata",
    "IndexStoragePolicy",

    "GlobalIndexStorageArchitecture",
    "GlobalIndexStorage",
    "Phase10_1GlobalIndexStorageArchitecture",
]
