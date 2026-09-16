"""
OUR SEARCH — Distributed Inverted Index / Segment Fabric

Phase 10.2
Version: distributed-inverted-index-segment-fabric.v1

Purpose
-------
Define the distributed inverted-index and immutable-segment fabric for an
enormous independent Web search engine.

Scale target
------------
This architecture is designed from the beginning for:

    billions → potentially trillions of public-Web resources

with Google-scale capability as the architectural target.

This is NOT a claim that the current implementation already operates at that
scale. Actual scale, latency, durability, availability, and corpus parity
must be demonstrated experimentally.

Core architecture
-----------------

    INDEXING PIPELINE
           |
           v
    DOCUMENT IDENTITY
           |
           v
    TERM / FIELD ROUTING
           |
           v
    LOGICAL INDEX PARTITION
           |
           v
    SEGMENT BUILD
           |
           v
    IMMUTABLE SEGMENT
           |
           +----------------------+
           |                      |
           v                      v
      POSTING BLOCKS          TERM DICTIONARY
           |                      |
           +----------+-----------+
                      |
                      v
              SEGMENT MANIFEST
                      |
                      v
              SEGMENT FABRIC
                      |
          +-----------+-----------+
          |           |           |
          v           v           v
       REGION       REGION      REGION
          |           |           |
          v           v           v
       REPLICA      REPLICA     REPLICA

Design principles
-----------------
- No Google technology or dependency.
- No single global inverted-index file.
- No single global posting-list file.
- No single global segment directory.
- No single machine owns the entire index.
- Logical partitions are independent from physical placement.
- Segments are immutable after publication.
- Posting data is independently addressable.
- Term dictionaries are partition-aware.
- Segment generations are explicit.
- Segment manifests are authoritative metadata.
- Replicas are independently tracked.
- Epochs fence stale publishers.
- Compaction can create new generations without mutating old segments.
- Reads can fan out across relevant partitions.
- Writes can be distributed across independent partitions.
- Future storage backends can replace the reference metadata backend.
"""

from __future__ import annotations

import hashlib
import threading
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Iterable, List, Mapping, Optional, Protocol, Sequence, Tuple


ARCHITECTURE_VERSION = (
    "distributed-inverted-index-segment-fabric.v1"
)


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def _now() -> float:
    return time.time()


def _hash(value: str) -> str:
    if not isinstance(value, str):
        raise TypeError("value must be a string")

    return hashlib.sha256(
        value.encode("utf-8")
    ).hexdigest()


def _partition_number(
    routing_key: str,
    partition_count: int,
) -> int:
    if not routing_key:
        raise ValueError("routing_key is required")

    if partition_count <= 0:
        raise ValueError(
            "partition_count must be greater than zero"
        )

    return int(_hash(routing_key), 16) % partition_count


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex}"


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class FabricPartitionState(str, Enum):
    CREATING = "creating"
    ACTIVE = "active"
    MOVING = "moving"
    SPLITTING = "splitting"
    MERGING = "merging"
    DEGRADED = "degraded"
    RECOVERING = "recovering"
    DRAINING = "draining"
    FENCED = "fenced"
    REMOVED = "removed"


class SegmentState(str, Enum):
    BUILDING = "building"
    SEALED = "sealed"
    PUBLISHED = "published"
    COMPACTING = "compacting"
    MOVING = "moving"
    VERIFYING = "verifying"
    RETIRED = "retired"
    DELETED = "deleted"


class PostingBlockState(str, Enum):
    BUILDING = "building"
    SEALED = "sealed"
    PUBLISHED = "published"
    VERIFYING = "verifying"
    RETIRED = "retired"


class ReplicaState(str, Enum):
    BUILDING = "building"
    ACTIVE = "active"
    VERIFYING = "verifying"
    STALE = "stale"
    DEGRADED = "degraded"
    FAILED = "failed"
    FENCED = "fenced"
    DRAINING = "draining"


class FieldType(str, Enum):
    ALL = "all"
    TITLE = "title"
    BODY = "body"
    ANCHOR = "anchor"
    URL = "url"
    DESCRIPTION = "description"
    METADATA = "metadata"


class QueryReadMode(str, Enum):
    PRIMARY = "primary"
    REPLICA = "replica"
    ANY_HEALTHY = "any_healthy"
    QUORUM = "quorum"


# ---------------------------------------------------------------------------
# Term identity
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TermIdentity:
    """
    Globally routable term identity.

    field allows the inverted index to distinguish title/body/anchor/etc.
    """

    term: str
    field: FieldType = FieldType.BODY
    language: str = ""
    analyzer_version: str = "default"

    @property
    def routing_key(self) -> str:
        return (
            f"{self.field.value}|"
            f"{self.language}|"
            f"{self.analyzer_version}|"
            f"{self.term}"
        )

    @property
    def fingerprint(self) -> str:
        return _hash(self.routing_key)


# ---------------------------------------------------------------------------
# Posting entry / block
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PostingEntry:
    """
    One logical inverted-index posting.

    positions are optional token positions.

    The actual posting payload can be encoded by a future high-performance
    storage implementation. This object defines the logical contract.
    """

    document_id: str
    term_frequency: int = 1
    positions: Tuple[int, ...] = ()
    field_mask: int = 0
    document_version: str = ""

    def __post_init__(self) -> None:
        if not self.document_id:
            raise ValueError("document_id is required")

        if self.term_frequency < 0:
            raise ValueError(
                "term_frequency cannot be negative"
            )


@dataclass
class PostingBlock:
    """
    Immutable-addressable group of postings.

    A posting block belongs to exactly one segment and one term identity.
    """

    block_id: str
    segment_id: str
    term_identity: TermIdentity

    ordinal: int

    document_count: int = 0
    encoded_size_bytes: int = 0

    min_document_id: str = ""
    max_document_id: str = ""

    checksum: str = ""

    state: PostingBlockState = PostingBlockState.BUILDING

    created_at: float = field(default_factory=_now)
    published_at: float = 0.0

    def seal(
        self,
        *,
        checksum: str = "",
    ) -> None:
        if self.state != PostingBlockState.BUILDING:
            raise ValueError(
                "posting block is not in building state"
            )

        self.checksum = checksum
        self.state = PostingBlockState.SEALED

    def publish(
        self,
        *,
        checksum: str = "",
    ) -> None:
        if self.state not in (
            PostingBlockState.SEALED,
            PostingBlockState.VERIFYING,
        ):
            raise ValueError(
                "posting block must be sealed before publication"
            )

        if checksum:
            self.checksum = checksum

        self.state = PostingBlockState.PUBLISHED
        self.published_at = _now()


# ---------------------------------------------------------------------------
# Term dictionary entry
# ---------------------------------------------------------------------------


@dataclass
class TermDictionaryEntry:
    """
    Term-to-posting-block metadata.

    A term can span multiple immutable blocks and multiple generations.
    """

    term_identity: TermIdentity

    document_frequency: int = 0
    total_term_frequency: int = 0

    posting_block_ids: List[str] = field(
        default_factory=list
    )

    latest_generation: int = 0

    updated_at: float = field(default_factory=_now)

    def add_posting_block(
        self,
        block_id: str,
        *,
        generation: int,
        document_frequency_delta: int = 0,
        term_frequency_delta: int = 0,
    ) -> None:
        if block_id not in self.posting_block_ids:
            self.posting_block_ids.append(block_id)

        self.latest_generation = max(
            self.latest_generation,
            int(generation),
        )

        self.document_frequency = max(
            0,
            self.document_frequency
            + int(document_frequency_delta),
        )

        self.total_term_frequency = max(
            0,
            self.total_term_frequency
            + int(term_frequency_delta),
        )

        self.updated_at = _now()


# ---------------------------------------------------------------------------
# Segment manifest
# ---------------------------------------------------------------------------


@dataclass
class SegmentManifest:
    """
    Authoritative logical metadata for an immutable index segment.

    The manifest is small metadata.

    The actual segment payload is stored through the future storage backend.
    """

    segment_id: str
    partition_id: str
    generation: int
    epoch: int

    state: SegmentState = SegmentState.BUILDING

    document_count: int = 0
    term_count: int = 0
    posting_block_count: int = 0

    encoded_size_bytes: int = 0

    min_document_id: str = ""
    max_document_id: str = ""

    checksum: str = ""

    created_at: float = field(default_factory=_now)
    published_at: float = 0.0

    parent_segment_ids: Tuple[str, ...] = ()

    def seal(
        self,
        *,
        checksum: str = "",
    ) -> None:
        if self.state != SegmentState.BUILDING:
            raise ValueError(
                "segment must be building before sealing"
            )

        if checksum:
            self.checksum = checksum

        self.state = SegmentState.SEALED

    def publish(
        self,
        *,
        epoch: int,
        checksum: str = "",
    ) -> None:
        if epoch < self.epoch:
            raise ValueError(
                "publication epoch cannot move backwards"
            )

        if self.state not in (
            SegmentState.SEALED,
            SegmentState.VERIFYING,
        ):
            raise ValueError(
                "segment must be sealed before publication"
            )

        self.epoch = epoch

        if checksum:
            self.checksum = checksum

        self.state = SegmentState.PUBLISHED
        self.published_at = _now()


# ---------------------------------------------------------------------------
# Segment replica
# ---------------------------------------------------------------------------


@dataclass
class SegmentReplica:
    """
    Physical replica of an immutable segment.

    Replicas can exist across independent regions, zones and nodes.
    """

    replica_id: str
    segment_id: str

    region_id: str
    zone_id: str
    node_id: str

    epoch: int

    state: ReplicaState = ReplicaState.BUILDING

    encoded_size_bytes: int = 0
    checksum: str = ""

    last_verified_at: float = 0.0
    created_at: float = field(default_factory=_now)

    def activate(
        self,
        *,
        epoch: int,
        checksum: str = "",
        encoded_size_bytes: int = 0,
    ) -> None:
        if epoch < self.epoch:
            raise ValueError(
                "replica epoch cannot move backwards"
            )

        self.epoch = epoch

        if checksum:
            self.checksum = checksum

        if encoded_size_bytes:
            self.encoded_size_bytes = (
                int(encoded_size_bytes)
            )

        self.last_verified_at = _now()
        self.state = ReplicaState.ACTIVE

    def verify(self) -> None:
        self.last_verified_at = _now()

        if self.state == ReplicaState.VERIFYING:
            self.state = ReplicaState.ACTIVE

    def fence(self, *, epoch: int) -> None:
        if epoch < self.epoch:
            raise ValueError(
                "fencing epoch cannot move backwards"
            )

        self.epoch = epoch
        self.state = ReplicaState.FENCED


# ---------------------------------------------------------------------------
# Logical fabric partition
# ---------------------------------------------------------------------------


@dataclass
class FabricPartition:
    """
    Logical inverted-index partition.

    Physical placement is intentionally separate from this object.
    """

    partition_id: str
    partition_number: int
    namespace: str

    epoch: int = 0
    state: FabricPartitionState = (
        FabricPartitionState.CREATING
    )

    primary_region: str = ""
    replica_regions: Tuple[str, ...] = ()

    segment_ids: List[str] = field(
        default_factory=list
    )

    term_count: int = 0
    document_count: int = 0
    posting_block_count: int = 0

    encoded_size_bytes: int = 0

    created_at: float = field(default_factory=_now)
    updated_at: float = field(default_factory=_now)

    def activate(
        self,
        *,
        epoch: int,
        primary_region: str = "",
        replica_regions: Sequence[str] = (),
    ) -> None:
        if epoch < self.epoch:
            raise ValueError(
                "partition epoch cannot move backwards"
            )

        self.epoch = epoch
        self.primary_region = primary_region
        self.replica_regions = tuple(
            sorted(set(replica_regions))
        )
        self.state = FabricPartitionState.ACTIVE
        self.updated_at = _now()

    def add_segment(
        self,
        segment: SegmentManifest,
    ) -> None:
        if segment.segment_id not in self.segment_ids:
            self.segment_ids.append(
                segment.segment_id
            )

        self.document_count += max(
            0,
            segment.document_count,
        )

        self.term_count += max(
            0,
            segment.term_count,
        )

        self.posting_block_count += max(
            0,
            segment.posting_block_count,
        )

        self.encoded_size_bytes += max(
            0,
            segment.encoded_size_bytes,
        )

        self.updated_at = _now()

    def fence(
        self,
        *,
        epoch: int,
    ) -> None:
        if epoch < self.epoch:
            raise ValueError(
                "fencing epoch cannot move backwards"
            )

        self.epoch = epoch
        self.state = FabricPartitionState.FENCED
        self.updated_at = _now()


# ---------------------------------------------------------------------------
# Segment build contract
# ---------------------------------------------------------------------------


@dataclass
class SegmentBuildContext:
    """
    Mutable build state before a segment becomes immutable.
    """

    build_id: str
    segment_id: str
    partition_id: str

    generation: int
    epoch: int

    posting_blocks: Dict[str, PostingBlock] = field(
        default_factory=dict
    )

    term_dictionary: Dict[str, TermDictionaryEntry] = field(
        default_factory=dict
    )

    document_ids: set[str] = field(
        default_factory=set
    )

    created_at: float = field(default_factory=_now)

    def add_posting(
        self,
        term_identity: TermIdentity,
        posting: PostingEntry,
    ) -> PostingBlock:
        if not posting.document_id:
            raise ValueError(
                "posting document_id is required"
            )

        self.document_ids.add(
            posting.document_id
        )

        term_key = term_identity.fingerprint

        dictionary_entry = self.term_dictionary.get(
            term_key
        )

        if dictionary_entry is None:
            dictionary_entry = TermDictionaryEntry(
                term_identity=term_identity
            )

            self.term_dictionary[term_key] = (
                dictionary_entry
            )

        block_ids = dictionary_entry.posting_block_ids

        if block_ids:
            block_id = block_ids[-1]
            block = self.posting_blocks[block_id]
        else:
            block = PostingBlock(
                block_id=_new_id("posting-block"),
                segment_id=self.segment_id,
                term_identity=term_identity,
                ordinal=len(self.posting_blocks),
            )

            self.posting_blocks[block.block_id] = block

        block.document_count += 1
        block.encoded_size_bytes += max(
            1,
            len(posting.document_id.encode("utf-8")),
        )

        dictionary_entry.add_posting_block(
            block.block_id,
            generation=self.generation,
            document_frequency_delta=1,
            term_frequency_delta=posting.term_frequency,
        )

        return block

    def seal(
        self,
        *,
        checksum: str = "",
    ) -> SegmentManifest:
        for block in self.posting_blocks.values():
            if block.state == PostingBlockState.BUILDING:
                block.seal()

            if block.state == PostingBlockState.SEALED:
                block.publish()

        manifest = SegmentManifest(
            segment_id=self.segment_id,
            partition_id=self.partition_id,
            generation=self.generation,
            epoch=self.epoch,
            document_count=len(self.document_ids),
            term_count=len(self.term_dictionary),
            posting_block_count=len(
                self.posting_blocks
            ),
            encoded_size_bytes=sum(
                block.encoded_size_bytes
                for block in self.posting_blocks.values()
            ),
            checksum=checksum,
        )

        manifest.seal(checksum=checksum)

        return manifest


# ---------------------------------------------------------------------------
# Read routing
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class InvertedIndexReadRoute:
    """
    Routing result for a term lookup.
    """

    term_identity: TermIdentity

    partition_id: str
    partition_number: int

    segment_ids: Tuple[str, ...]

    replica_ids: Tuple[str, ...]

    epoch: int

    read_mode: QueryReadMode


# ---------------------------------------------------------------------------
# Fabric events
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SegmentFabricEvent:
    event_id: str
    event_type: str

    object_type: str
    object_id: str

    partition_id: str = ""
    segment_id: str = ""
    epoch: int = 0

    metadata: Mapping[str, object] = field(
        default_factory=dict
    )

    created_at: float = field(default_factory=_now)


# ---------------------------------------------------------------------------
# Backend contract
# ---------------------------------------------------------------------------


class DistributedIndexSegmentBackend(Protocol):
    """
    Replaceable distributed backend contract.

    The production implementation can connect to distributed storage,
    metadata services, object stores, replicated filesystems, or other
    independently built infrastructure.

    This module does not require any particular vendor.
    """

    def put_partition(
        self,
        partition: FabricPartition,
    ) -> None:
        ...

    def get_partition(
        self,
        partition_id: str,
    ) -> Optional[FabricPartition]:
        ...

    def list_partitions(
        self,
    ) -> Sequence[FabricPartition]:
        ...

    def put_segment(
        self,
        segment: SegmentManifest,
    ) -> None:
        ...

    def get_segment(
        self,
        segment_id: str,
    ) -> Optional[SegmentManifest]:
        ...

    def list_segments(
        self,
    ) -> Sequence[SegmentManifest]:
        ...

    def put_term_entry(
        self,
        entry: TermDictionaryEntry,
    ) -> None:
        ...

    def get_term_entry(
        self,
        fingerprint: str,
    ) -> Optional[TermDictionaryEntry]:
        ...

    def list_term_entries(
        self,
    ) -> Sequence[TermDictionaryEntry]:
        ...

    def put_replica(
        self,
        replica: SegmentReplica,
    ) -> None:
        ...

    def get_replica(
        self,
        replica_id: str,
    ) -> Optional[SegmentReplica]:
        ...

    def list_replicas(
        self,
    ) -> Sequence[SegmentReplica]:
        ...

    def append_event(
        self,
        event: SegmentFabricEvent,
    ) -> None:
        ...

    def list_events(
        self,
    ) -> Sequence[SegmentFabricEvent]:
        ...


# ---------------------------------------------------------------------------
# Reference backend
# ---------------------------------------------------------------------------


class InMemoryDistributedIndexSegmentMetadata:
    """
    Reference metadata implementation.

    This is intentionally small and dependency-free.

    It is NOT the production global index storage backend.

    Its purpose is to define the contract that a truly distributed backend
    can implement later.
    """

    def __init__(self) -> None:
        self._partitions: Dict[
            str,
            FabricPartition,
        ] = {}

        self._segments: Dict[
            str,
            SegmentManifest,
        ] = {}

        self._terms: Dict[
            str,
            TermDictionaryEntry,
        ] = {}

        self._replicas: Dict[
            str,
            SegmentReplica,
        ] = {}

        self._events: List[
            SegmentFabricEvent
        ] = []

        self._lock = threading.RLock()

    def put_partition(
        self,
        partition: FabricPartition,
    ) -> None:
        with self._lock:
            self._partitions[
                partition.partition_id
            ] = partition

    def get_partition(
        self,
        partition_id: str,
    ) -> Optional[FabricPartition]:
        with self._lock:
            return self._partitions.get(
                partition_id
            )

    def list_partitions(
        self,
    ) -> Sequence[FabricPartition]:
        with self._lock:
            return tuple(
                self._partitions.values()
            )

    def put_segment(
        self,
        segment: SegmentManifest,
    ) -> None:
        with self._lock:
            self._segments[
                segment.segment_id
            ] = segment

    def get_segment(
        self,
        segment_id: str,
    ) -> Optional[SegmentManifest]:
        with self._lock:
            return self._segments.get(
                segment_id
            )

    def list_segments(
        self,
    ) -> Sequence[SegmentManifest]:
        with self._lock:
            return tuple(
                self._segments.values()
            )

    def put_term_entry(
        self,
        entry: TermDictionaryEntry,
    ) -> None:
        with self._lock:
            self._terms[
                entry.term_identity.fingerprint
            ] = entry

    def get_term_entry(
        self,
        fingerprint: str,
    ) -> Optional[TermDictionaryEntry]:
        with self._lock:
            return self._terms.get(
                fingerprint
            )

    def list_term_entries(
        self,
    ) -> Sequence[TermDictionaryEntry]:
        with self._lock:
            return tuple(
                self._terms.values()
            )

    def put_replica(
        self,
        replica: SegmentReplica,
    ) -> None:
        with self._lock:
            self._replicas[
                replica.replica_id
            ] = replica

    def get_replica(
        self,
        replica_id: str,
    ) -> Optional[SegmentReplica]:
        with self._lock:
            return self._replicas.get(
                replica_id
            )

    def list_replicas(
        self,
    ) -> Sequence[SegmentReplica]:
        with self._lock:
            return tuple(
                self._replicas.values()
            )

    def append_event(
        self,
        event: SegmentFabricEvent,
    ) -> None:
        with self._lock:
            self._events.append(event)

    def list_events(
        self,
    ) -> Sequence[SegmentFabricEvent]:
        with self._lock:
            return tuple(self._events)


# ---------------------------------------------------------------------------
# Fabric policy
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SegmentFabricPolicy:
    """
    Global segment-fabric policy.

    These are operational policies, not global scale ceilings.
    """

    replication_factor: int = 3

    require_distinct_regions: bool = True
    require_distinct_zones: bool = True
    require_distinct_nodes: bool = True

    max_posting_blocks_per_segment: int = 0

    def __post_init__(self) -> None:
        if self.replication_factor <= 0:
            raise ValueError(
                "replication_factor must be greater than zero"
            )

        if self.max_posting_blocks_per_segment < 0:
            raise ValueError(
                "max_posting_blocks_per_segment cannot be negative"
            )


# ---------------------------------------------------------------------------
# Distributed inverted-index segment fabric
# ---------------------------------------------------------------------------


class DistributedInvertedIndexSegmentFabric:
    """
    Global distributed inverted-index / segment fabric.

    This class sits above physical storage.

    It defines:

        term routing
        partition identity
        segment generations
        immutable segment publication
        posting blocks
        term dictionaries
        segment replicas
        epoch fencing
        read routing
        compaction lineage
        events
        metadata contracts

    It is intentionally independent from the existing local SegmentManager.

    The current local SegmentManager can remain a local segment implementation
    while this fabric becomes the global architecture around it.
    """

    def __init__(
        self,
        *,
        partition_count: int = 1_048_576,
        namespace: str = "web-inverted-index",
        policy: Optional[SegmentFabricPolicy] = None,
        backend: Optional[
            DistributedIndexSegmentBackend
        ] = None,
        regions: Optional[Iterable[str]] = None,
    ) -> None:
        if partition_count <= 0:
            raise ValueError(
                "partition_count must be greater than zero"
            )

        if not namespace:
            raise ValueError(
                "namespace is required"
            )

        self.partition_count = int(
            partition_count
        )

        self.namespace = namespace

        self.policy = (
            policy
            if policy is not None
            else SegmentFabricPolicy()
        )

        self.backend = (
            backend
            if backend is not None
            else InMemoryDistributedIndexSegmentMetadata()
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
    # Partition routing
    # ------------------------------------------------------------------

    def partition_number_for_term(
        self,
        term_identity: TermIdentity,
    ) -> int:
        return _partition_number(
            term_identity.routing_key,
            self.partition_count,
        )

    def partition_id_for_number(
        self,
        partition_number: int,
    ) -> str:
        if partition_number < 0:
            raise ValueError(
                "partition_number cannot be negative"
            )

        if partition_number >= self.partition_count:
            raise ValueError(
                "partition_number is outside logical range"
            )

        return (
            f"{self.namespace}/"
            f"partition-{partition_number:012d}"
        )

    def partition_for_term(
        self,
        term_identity: TermIdentity,
    ) -> FabricPartition:
        number = self.partition_number_for_term(
            term_identity
        )

        partition_id = (
            self.partition_id_for_number(number)
        )

        partition = self.backend.get_partition(
            partition_id
        )

        if partition is None:
            partition = FabricPartition(
                partition_id=partition_id,
                partition_number=number,
                namespace=self.namespace,
                epoch=self.epoch,
            )

            self.backend.put_partition(
                partition
            )

        return partition

    # ------------------------------------------------------------------
    # Partition lifecycle
    # ------------------------------------------------------------------

    def activate_partition(
        self,
        partition_id: str,
        *,
        primary_region: str = "",
        replica_regions: Sequence[str] = (),
        epoch: Optional[int] = None,
    ) -> FabricPartition:
        partition = self.backend.get_partition(
            partition_id
        )

        if partition is None:
            raise KeyError(
                f"unknown partition: {partition_id}"
            )

        activation_epoch = (
            self.epoch
            if epoch is None
            else int(epoch)
        )

        partition.activate(
            epoch=activation_epoch,
            primary_region=primary_region,
            replica_regions=replica_regions,
        )

        self.backend.put_partition(
            partition
        )

        self._emit(
            event_type="partition_activated",
            object_type="partition",
            object_id=partition_id,
            partition_id=partition_id,
            epoch=activation_epoch,
            metadata={
                "primary_region": primary_region,
                "replica_regions": tuple(
                    replica_regions
                ),
            },
        )

        return partition

    def fence_partition(
        self,
        partition_id: str,
        *,
        epoch: Optional[int] = None,
    ) -> FabricPartition:
        partition = self.backend.get_partition(
            partition_id
        )

        if partition is None:
            raise KeyError(
                f"unknown partition: {partition_id}"
            )

        fencing_epoch = (
            self.advance_epoch()
            if epoch is None
            else int(epoch)
        )

        partition.fence(
            epoch=fencing_epoch
        )

        self.backend.put_partition(
            partition
        )

        self._emit(
            event_type="partition_fenced",
            object_type="partition",
            object_id=partition_id,
            partition_id=partition_id,
            epoch=fencing_epoch,
        )

        return partition

    # ------------------------------------------------------------------
    # Segment creation
    # ------------------------------------------------------------------

    def create_segment_build(
        self,
        *,
        partition_id: str,
        generation: int,
        epoch: Optional[int] = None,
        build_id: Optional[str] = None,
        segment_id: Optional[str] = None,
    ) -> SegmentBuildContext:
        partition = self.backend.get_partition(
            partition_id
        )

        if partition is None:
            raise KeyError(
                f"unknown partition: {partition_id}"
            )

        build_epoch = (
            self.epoch
            if epoch is None
            else int(epoch)
        )

        return SegmentBuildContext(
            build_id=(
                build_id
                or _new_id("segment-build")
            ),
            segment_id=(
                segment_id
                or _new_id("segment")
            ),
            partition_id=partition_id,
            generation=int(generation),
            epoch=build_epoch,
        )

    def publish_build(
        self,
        build: SegmentBuildContext,
        *,
        checksum: str = "",
        epoch: Optional[int] = None,
    ) -> SegmentManifest:
        if build.partition_id == "":
            raise ValueError(
                "build partition_id is required"
            )

        publication_epoch = (
            self.epoch
            if epoch is None
            else int(epoch)
        )

        if publication_epoch < build.epoch:
            raise ValueError(
                "publication epoch cannot be older than build epoch"
            )

        manifest = build.seal(
            checksum=checksum
        )

        manifest.publish(
            epoch=publication_epoch,
            checksum=checksum,
        )

        self.register_segment(
            manifest
        )

        for entry in build.term_dictionary.values():
            self.backend.put_term_entry(
                entry
            )

        partition = self.backend.get_partition(
            manifest.partition_id
        )

        if partition is None:
            raise KeyError(
                f"unknown partition: {manifest.partition_id}"
            )

        partition.add_segment(
            manifest
        )

        self.backend.put_partition(
            partition
        )

        self._emit(
            event_type="segment_published",
            object_type="segment",
            object_id=manifest.segment_id,
            partition_id=manifest.partition_id,
            segment_id=manifest.segment_id,
            epoch=publication_epoch,
            metadata={
                "generation": manifest.generation,
                "document_count": manifest.document_count,
                "term_count": manifest.term_count,
                "posting_block_count": (
                    manifest.posting_block_count
                ),
            },
        )

        return manifest

    # ------------------------------------------------------------------
    # Segment registry
    # ------------------------------------------------------------------

    def register_segment(
        self,
        segment: SegmentManifest,
    ) -> SegmentManifest:
        existing = self.backend.get_segment(
            segment.segment_id
        )

        if existing is not None:
            if segment.epoch < existing.epoch:
                raise ValueError(
                    "segment epoch cannot move backwards"
                )

            if (
                existing.state
                == SegmentState.PUBLISHED
            ):
                raise ValueError(
                    "published segment metadata is immutable"
                )

        partition = self.backend.get_partition(
            segment.partition_id
        )

        if partition is None:
            raise KeyError(
                f"unknown partition: {segment.partition_id}"
            )

        self.backend.put_segment(
            segment
        )

        self._emit(
            event_type="segment_registered",
            object_type="segment",
            object_id=segment.segment_id,
            partition_id=segment.partition_id,
            segment_id=segment.segment_id,
            epoch=segment.epoch,
        )

        return segment

    # ------------------------------------------------------------------
    # Term dictionary
    # ------------------------------------------------------------------

    def register_term_entry(
        self,
        entry: TermDictionaryEntry,
    ) -> None:
        self.backend.put_term_entry(
            entry
        )

        partition = self.partition_for_term(
            entry.term_identity
        )

        partition.term_count = max(
            partition.term_count,
            1,
        )

        self.backend.put_partition(
            partition
        )

    def get_term_entry(
        self,
        term_identity: TermIdentity,
    ) -> Optional[TermDictionaryEntry]:
        return self.backend.get_term_entry(
            term_identity.fingerprint
        )

    # ------------------------------------------------------------------
    # Replica placement
    # ------------------------------------------------------------------

    def add_replica(
        self,
        *,
        segment_id: str,
        region_id: str,
        zone_id: str,
        node_id: str,
        replica_id: Optional[str] = None,
        epoch: Optional[int] = None,
    ) -> SegmentReplica:
        segment = self.backend.get_segment(
            segment_id
        )

        if segment is None:
            raise KeyError(
                f"unknown segment: {segment_id}"
            )

        replica = SegmentReplica(
            replica_id=(
                replica_id
                or _new_id("segment-replica")
            ),
            segment_id=segment_id,
            region_id=region_id,
            zone_id=zone_id,
            node_id=node_id,
            epoch=(
                segment.epoch
                if epoch is None
                else int(epoch)
            ),
        )

        self.backend.put_replica(
            replica
        )

        self._emit(
            event_type="replica_added",
            object_type="replica",
            object_id=replica.replica_id,
            partition_id=segment.partition_id,
            segment_id=segment_id,
            epoch=replica.epoch,
            metadata={
                "region_id": region_id,
                "zone_id": zone_id,
                "node_id": node_id,
            },
        )

        return replica

    def activate_replica(
        self,
        *,
        replica_id: str,
        checksum: str = "",
        encoded_size_bytes: int = 0,
        epoch: Optional[int] = None,
    ) -> SegmentReplica:
        replica = self.backend.get_replica(
            replica_id
        )

        if replica is None:
            raise KeyError(
                f"unknown replica: {replica_id}"
            )

        segment = self.backend.get_segment(
            replica.segment_id
        )

        if segment is None:
            raise KeyError(
                f"unknown segment: {replica.segment_id}"
            )

        activation_epoch = (
            segment.epoch
            if epoch is None
            else int(epoch)
        )

        replica.activate(
            epoch=activation_epoch,
            checksum=checksum or segment.checksum,
            encoded_size_bytes=(
                encoded_size_bytes
                or segment.encoded_size_bytes
            ),
        )

        self.backend.put_replica(
            replica
        )

        self._emit(
            event_type="replica_activated",
            object_type="replica",
            object_id=replica.replica_id,
            partition_id=segment.partition_id,
            segment_id=segment.segment_id,
            epoch=activation_epoch,
            metadata={
                "region_id": replica.region_id,
                "zone_id": replica.zone_id,
                "node_id": replica.node_id,
            },
        )

        return replica

    # ------------------------------------------------------------------
    # Replica health
    # ------------------------------------------------------------------

    def segment_replicas(
        self,
        segment_id: str,
    ) -> Sequence[SegmentReplica]:
        return tuple(
            replica
            for replica in self.backend.list_replicas()
            if replica.segment_id == segment_id
        )

    def healthy_replicas(
        self,
        segment_id: str,
    ) -> Sequence[SegmentReplica]:
        segment = self.backend.get_segment(
            segment_id
        )

        if segment is None:
            raise KeyError(
                f"unknown segment: {segment_id}"
            )

        return tuple(
            replica
            for replica in self.segment_replicas(
                segment_id
            )
            if (
                replica.state
                == ReplicaState.ACTIVE
                and replica.epoch == segment.epoch
            )
        )

    def validate_replication(
        self,
        segment_id: str,
    ) -> Mapping[str, object]:
        replicas = list(
            self.segment_replicas(
                segment_id
            )
        )

        healthy = [
            replica
            for replica in replicas
            if replica.state
            == ReplicaState.ACTIVE
        ]

        regions = {
            replica.region_id
            for replica in healthy
            if replica.region_id
        }

        zones = {
            (
                replica.region_id,
                replica.zone_id,
            )
            for replica in healthy
            if replica.region_id
            and replica.zone_id
        }

        nodes = {
            (
                replica.region_id,
                replica.zone_id,
                replica.node_id,
            )
            for replica in healthy
            if (
                replica.region_id
                and replica.zone_id
                and replica.node_id
            )
        }

        return {
            "segment_id": segment_id,
            "required_replication_factor": (
                self.policy.replication_factor
            ),
            "total_replicas": len(replicas),
            "healthy_replicas": len(healthy),
            "region_count": len(regions),
            "zone_count": len(zones),
            "node_count": len(nodes),
            "replication_satisfied": (
                len(healthy)
                >= self.policy.replication_factor
            ),
            "region_diversity_ok": (
                not self.policy.require_distinct_regions
                or len(regions)
                >= min(
                    self.policy.replication_factor,
                    max(1, len(regions)),
                )
            ),
            "zone_diversity_ok": (
                not self.policy.require_distinct_zones
                or len(zones)
                >= min(
                    self.policy.replication_factor,
                    max(1, len(zones)),
                )
            ),
            "node_diversity_ok": (
                not self.policy.require_distinct_nodes
                or len(nodes)
                >= min(
                    self.policy.replication_factor,
                    max(1, len(nodes)),
                )
            ),
        }

    # ------------------------------------------------------------------
    # Read routing
    # ------------------------------------------------------------------

    def route_term(
        self,
        term_identity: TermIdentity,
        *,
        read_mode: QueryReadMode = (
            QueryReadMode.ANY_HEALTHY
        ),
    ) -> InvertedIndexReadRoute:
        partition = self.partition_for_term(
            term_identity
        )

        entry = self.get_term_entry(
            term_identity
        )

        segment_ids: Tuple[str, ...]

        if entry is None:
            segment_ids = ()
        else:
            segment_ids = tuple(
                entry.posting_block_ids
            )

        replicas: List[str] = []

        for segment in self.backend.list_segments():
            if segment.partition_id != (
                partition.partition_id
            ):
                continue

            for replica in self.segment_replicas(
                segment.segment_id
            ):
                if replica.state == ReplicaState.ACTIVE:
                    replicas.append(
                        replica.replica_id
                    )

        return InvertedIndexReadRoute(
            term_identity=term_identity,
            partition_id=partition.partition_id,
            partition_number=partition.partition_number,
            segment_ids=segment_ids,
            replica_ids=tuple(
                sorted(set(replicas))
            ),
            epoch=partition.epoch,
            read_mode=read_mode,
        )

    # ------------------------------------------------------------------
    # Compaction lineage
    # ------------------------------------------------------------------

    def create_compaction_manifest(
        self,
        *,
        partition_id: str,
        parent_segment_ids: Sequence[str],
        generation: int,
        epoch: Optional[int] = None,
        segment_id: Optional[str] = None,
    ) -> SegmentManifest:
        if not parent_segment_ids:
            raise ValueError(
                "parent_segment_ids cannot be empty"
            )

        partition = self.backend.get_partition(
            partition_id
        )

        if partition is None:
            raise KeyError(
                f"unknown partition: {partition_id}"
            )

        for parent_id in parent_segment_ids:
            parent = self.backend.get_segment(
                parent_id
            )

            if parent is None:
                raise KeyError(
                    f"unknown parent segment: {parent_id}"
                )

            if parent.partition_id != partition_id:
                raise ValueError(
                    "all parent segments must belong to the same partition"
                )

        compaction_epoch = (
            self.epoch
            if epoch is None
            else int(epoch)
        )

        manifest = SegmentManifest(
            segment_id=(
                segment_id
                or _new_id("compacted-segment")
            ),
            partition_id=partition_id,
            generation=int(generation),
            epoch=compaction_epoch,
            state=SegmentState.COMPACTING,
            parent_segment_ids=tuple(
                parent_segment_ids
            ),
        )

        self.backend.put_segment(
            manifest
        )

        self._emit(
            event_type="compaction_manifest_created",
            object_type="segment",
            object_id=manifest.segment_id,
            partition_id=partition_id,
            segment_id=manifest.segment_id,
            epoch=compaction_epoch,
            metadata={
                "parent_segment_ids": tuple(
                    parent_segment_ids
                ),
                "generation": generation,
            },
        )

        return manifest

    # ------------------------------------------------------------------
    # Segment generation / retirement
    # ------------------------------------------------------------------

    def retire_segment(
        self,
        segment_id: str,
    ) -> SegmentManifest:
        segment = self.backend.get_segment(
            segment_id
        )

        if segment is None:
            raise KeyError(
                f"unknown segment: {segment_id}"
            )

        if segment.state == SegmentState.DELETED:
            return segment

        segment.state = SegmentState.RETIRED

        self.backend.put_segment(
            segment
        )

        self._emit(
            event_type="segment_retired",
            object_type="segment",
            object_id=segment_id,
            partition_id=segment.partition_id,
            segment_id=segment_id,
            epoch=segment.epoch,
        )

        return segment

    # ------------------------------------------------------------------
    # Epoch fencing
    # ------------------------------------------------------------------

    def fence_segment(
        self,
        segment_id: str,
        *,
        epoch: Optional[int] = None,
    ) -> SegmentManifest:
        segment = self.backend.get_segment(
            segment_id
        )

        if segment is None:
            raise KeyError(
                f"unknown segment: {segment_id}"
            )

        fencing_epoch = (
            self.advance_epoch()
            if epoch is None
            else int(epoch)
        )

        if fencing_epoch < segment.epoch:
            raise ValueError(
                "segment fencing epoch cannot move backwards"
            )

        segment.epoch = fencing_epoch
        segment.state = SegmentState.MOVING

        self.backend.put_segment(
            segment
        )

        self._emit(
            event_type="segment_fenced",
            object_type="segment",
            object_id=segment_id,
            partition_id=segment.partition_id,
            segment_id=segment_id,
            epoch=fencing_epoch,
        )

        return segment

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
        segment_id: str = "",
        epoch: int = 0,
        metadata: Optional[
            Mapping[str, object]
        ] = None,
    ) -> SegmentFabricEvent:
        event = SegmentFabricEvent(
            event_id=_new_id("fabric-event"),
            event_type=event_type,
            object_type=object_type,
            object_id=object_id,
            partition_id=partition_id,
            segment_id=segment_id,
            epoch=epoch,
            metadata=dict(metadata or {}),
        )

        self.backend.append_event(
            event
        )

        return event

    def events(
        self,
    ) -> Sequence[SegmentFabricEvent]:
        return self.backend.list_events()

    # ------------------------------------------------------------------
    # Capacity / topology
    # ------------------------------------------------------------------

    def capacity(self) -> Mapping[str, int]:
        partitions = list(
            self.backend.list_partitions()
        )

        segments = list(
            self.backend.list_segments()
        )

        replicas = list(
            self.backend.list_replicas()
        )

        published_segments = [
            segment
            for segment in segments
            if segment.state
            in (
                SegmentState.PUBLISHED,
                SegmentState.COMPACTING,
            )
        ]

        active_replicas = [
            replica
            for replica in replicas
            if replica.state == ReplicaState.ACTIVE
        ]

        return {
            "logical_partition_count": (
                self.partition_count
            ),
            "observed_partition_metadata_count": (
                len(partitions)
            ),
            "segment_count": len(segments),
            "published_segment_count": (
                len(published_segments)
            ),
            "term_dictionary_entry_count": (
                len(
                    self.backend.list_term_entries()
                )
            ),
            "replica_count": len(replicas),
            "active_replica_count": (
                len(active_replicas)
            ),
            "document_count": sum(
                partition.document_count
                for partition in partitions
            ),
            "posting_block_count": sum(
                partition.posting_block_count
                for partition in partitions
            ),
            "encoded_segment_bytes": sum(
                segment.encoded_size_bytes
                for segment in published_segments
            ),
            "replicated_bytes": sum(
                replica.encoded_size_bytes
                for replica in active_replicas
            ),
        }

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    def stats(self) -> Mapping[str, object]:
        capacity = self.capacity()

        return {
            "architecture_version": (
                ARCHITECTURE_VERSION
            ),
            "namespace": self.namespace,

            "logical_partition_count": (
                self.partition_count
            ),

            "observed_partition_metadata_count": (
                capacity[
                    "observed_partition_metadata_count"
                ]
            ),

            "segment_count": (
                capacity["segment_count"]
            ),

            "published_segment_count": (
                capacity["published_segment_count"]
            ),

            "term_dictionary_entry_count": (
                capacity[
                    "term_dictionary_entry_count"
                ]
            ),

            "replica_count": (
                capacity["replica_count"]
            ),

            "active_replica_count": (
                capacity["active_replica_count"]
            ),

            "document_count": (
                capacity["document_count"]
            ),

            "posting_block_count": (
                capacity["posting_block_count"]
            ),

            "encoded_segment_bytes": (
                capacity["encoded_segment_bytes"]
            ),

            "replicated_bytes": (
                capacity["replicated_bytes"]
            ),

            "epoch": self.epoch,
            "running": self.running,

            "replication_factor": (
                self.policy.replication_factor
            ),

            "target_scale": (
                "billions_to_trillions_of_public_web_resources"
            ),

            "google_scale_capability_target": True,

            "google_technology_dependency": False,

            "fixed_global_document_limit": False,
            "fixed_global_term_limit": False,
            "fixed_global_segment_limit": False,
            "fixed_global_posting_block_limit": False,
            "fixed_global_partition_limit": False,
            "fixed_global_replica_limit": False,

            "single_global_inverted_index_file": False,
            "single_global_posting_file": False,
            "single_global_segment_directory": False,
            "single_global_index_database": False,

            "logical_physical_separation": True,
            "partition_aware_term_routing": True,
            "immutable_segments": True,
            "immutable_published_posting_blocks": True,
            "generation_aware_segments": True,
            "replicated_segment_model": True,
            "epoch_fencing": True,
            "compaction_lineage": True,
            "replaceable_backend": True,
            "distributed_read_routing": True,
        }


# ---------------------------------------------------------------------------
# Compatibility aliases
# ---------------------------------------------------------------------------


DistributedInvertedIndexFabric = (
    DistributedInvertedIndexSegmentFabric
)

DistributedSegmentFabric = (
    DistributedInvertedIndexSegmentFabric
)

Phase10_2DistributedInvertedIndexSegmentFabric = (
    DistributedInvertedIndexSegmentFabric
)


__all__ = [
    "ARCHITECTURE_VERSION",

    "FabricPartitionState",
    "SegmentState",
    "PostingBlockState",
    "ReplicaState",
    "FieldType",
    "QueryReadMode",

    "TermIdentity",
    "PostingEntry",
    "PostingBlock",
    "TermDictionaryEntry",
    "SegmentManifest",
    "SegmentReplica",
    "FabricPartition",
    "SegmentBuildContext",
    "InvertedIndexReadRoute",
    "SegmentFabricEvent",

    "DistributedIndexSegmentBackend",
    "InMemoryDistributedIndexSegmentMetadata",
    "SegmentFabricPolicy",

    "DistributedInvertedIndexSegmentFabric",
    "DistributedInvertedIndexFabric",
    "DistributedSegmentFabric",
    "Phase10_2DistributedInvertedIndexSegmentFabric",
]
