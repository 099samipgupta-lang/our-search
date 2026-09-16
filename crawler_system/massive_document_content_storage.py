"""
OUR SEARCH
Phase 10.3 — Massive Document & Content Storage

Version:
    massive-document-content-storage.v1

Purpose:
    Global document/content storage control architecture for an
    enormous public-Web search platform.

Scale target:
    - Billions of public-Web resources.
    - Potentially trillions of public-Web resources.
    - Google-scale capability target from the beginning.
    - No small-prototype-first storage assumptions.

Important:
    This module is an architecture/control-plane layer.
    It does not replace the existing local DocumentStore.
    It defines the distributed abstraction that a future durable
    storage implementation can realize.

Architecture:

    CRAWL RESULT
        |
        v
    DOCUMENT IDENTITY
        |
        v
    CONTENT INGESTION
        |
        v
    CONTENT VERSION
        |
        v
    CONTENT CHUNKING
        |
        v
    DOCUMENT MANIFEST
        |
        v
    DISTRIBUTED DOCUMENT STORAGE
        |
        +--> REPLICAS
        |
        +--> HOT / WARM / COLD / ARCHIVE TIERS
        |
        +--> VERSION HISTORY
        |
        +--> CHECKPOINTS / RECOVERY
        |
        v
    DOCUMENT READ ROUTING

Design principles:

    1. No single global document file.
    2. No single global content file.
    3. No single storage node.
    4. No single storage database.
    5. No fixed global document limit.
    6. No fixed global content-byte limit.
    7. No fixed global chunk limit.
    8. Document versions are immutable content objects.
    9. Current document state is represented by metadata/manifests.
    10. Content is partitioned deterministically.
    11. Replicas are independently tracked.
    12. Storage tiers are policy-driven.
    13. Regions/zones/nodes are explicit placement dimensions.
    14. Routing is separated from physical storage.
    15. Recovery is checkpoint-aware.
    16. Backend implementation is replaceable.
    17. The architecture is designed for horizontal expansion.
    18. Google technologies are not used as dependencies.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from hashlib import sha256
from typing import Dict, Iterable, List, Mapping, Optional, Protocol, Sequence, Tuple
from uuid import uuid4
from datetime import datetime, timezone


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _hash(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex}"


def _partition_number(value: str, partition_count: int) -> int:
    if partition_count <= 0:
        raise ValueError("partition_count must be positive")

    digest = int(_hash(value), 16)
    return digest % partition_count


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------

class DocumentStorageState(str, Enum):
    BUILDING = "building"
    ACTIVE = "active"
    MOVING = "moving"
    DEGRADED = "degraded"
    RECOVERING = "recovering"
    RETIRED = "retired"
    DELETED = "deleted"


class ContentObjectType(str, Enum):
    DOCUMENT = "document"
    TEXT = "text"
    RAW = "raw"
    METADATA = "metadata"
    CHUNK = "chunk"
    SNAPSHOT = "snapshot"


class DocumentStorageTier(str, Enum):
    HOT = "hot"
    WARM = "warm"
    COLD = "cold"
    ARCHIVE = "archive"


class ContentCompression(str, Enum):
    NONE = "none"
    BLOCK = "block"
    STREAM = "stream"
    ZSTD = "zstd"


class DocumentReplicaState(str, Enum):
    BUILDING = "building"
    ACTIVE = "active"
    DEGRADED = "degraded"
    RECOVERING = "recovering"
    RETIRED = "retired"
    DELETED = "deleted"


class ContentVersionState(str, Enum):
    BUILDING = "building"
    ACTIVE = "active"
    SUPERSEDED = "superseded"
    DELETED = "deleted"


class DocumentPartitionState(str, Enum):
    BUILDING = "building"
    ACTIVE = "MOVING".lower()
    DEGRADED = "degraded"
    RECOVERING = "recovering"
    RETIRED = "retired"


# ---------------------------------------------------------------------------
# Core identities
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DocumentContentIdentity:
    """
    Stable logical identity of a crawled public-Web resource.

    document_id:
        Stable logical resource identity.

    canonical_url:
        Canonical URL associated with the logical resource.

    content_version:
        Immutable version identifier.

    fingerprint:
        Content fingerprint used for version/change identity.

    partition_id:
        Logical storage partition responsible for the resource.
    """

    document_id: str
    canonical_url: str
    content_version: str
    fingerprint: str
    partition_id: str
    created_at: str = field(default_factory=_now)


@dataclass(frozen=True)
class ContentChunk:
    """
    Immutable content chunk.

    A large document is represented as many independently addressable
    chunks rather than one enormous object that must live on one node.
    """

    chunk_id: str
    document_id: str
    content_version: str
    ordinal: int
    logical_offset: int
    length: int
    byte_size: int
    content_hash: str
    object_type: ContentObjectType = ContentObjectType.CHUNK
    compression: ContentCompression = ContentCompression.NONE
    created_at: str = field(default_factory=_now)


@dataclass(frozen=True)
class ContentVersion:
    """
    Immutable document content version.

    The manifest/current-pointer layer can change, while the actual
    content version remains immutable.
    """

    version_id: str
    document_id: str
    fingerprint: str
    total_bytes: int
    chunk_count: int
    state: ContentVersionState
    created_at: str = field(default_factory=_now)
    superseded_at: Optional[str] = None


@dataclass(frozen=True)
class DocumentManifest:
    """
    Distributed metadata describing one document version.

    The manifest is not the content itself. It maps the logical document
    to its immutable content chunks and storage placement.
    """

    document_id: str
    canonical_url: str
    current_version: str
    partition_id: str
    total_bytes: int
    chunk_count: int
    content_fingerprint: str
    state: DocumentStorageState
    storage_tier: DocumentStorageTier
    replica_ids: Tuple[str, ...] = ()
    chunk_ids: Tuple[str, ...] = ()
    created_at: str = field(default_factory=_now)
    updated_at: str = field(default_factory=_now)


@dataclass(frozen=True)
class DocumentReplica:
    """
    Physical/logical replica placement metadata.

    Actual bytes are owned by the backend implementation.
    """

    replica_id: str
    document_id: str
    content_version: str
    region_id: str
    zone_id: str
    node_id: str
    storage_tier: DocumentStorageTier
    state: DocumentReplicaState
    byte_size: int
    checksum: str
    created_at: str = field(default_factory=_now)


@dataclass(frozen=True)
class DocumentStoragePartition:
    """
    Logical partition of the enormous document universe.

    Partitions can be distributed across regions and storage workers.
    """

    partition_id: str
    partition_number: int
    state: DocumentPartitionState
    region_ids: Tuple[str, ...] = ()
    document_count: int = 0
    logical_bytes: int = 0
    replicated_bytes: int = 0
    chunk_count: int = 0
    created_at: str = field(default_factory=_now)
    updated_at: str = field(default_factory=_now)


@dataclass(frozen=True)
class DocumentStorageRoute:
    """
    Logical read route for a document or content chunk.
    """

    route_id: str
    document_id: str
    content_version: str
    partition_id: str
    preferred_replica_ids: Tuple[str, ...]
    fallback_replica_ids: Tuple[str, ...]
    storage_tier: DocumentStorageTier
    generated_at: str = field(default_factory=_now)


@dataclass(frozen=True)
class DocumentStorageCheckpoint:
    """
    Durable control-plane checkpoint.

    A checkpoint records the known storage metadata frontier without
    requiring the whole document universe to be represented in one file.
    """

    checkpoint_id: str
    checkpoint_epoch: int
    partition_ids: Tuple[str, ...]
    manifest_count: int
    content_version_count: int
    replica_count: int
    logical_bytes: int
    replicated_bytes: int
    created_at: str = field(default_factory=_now)


@dataclass(frozen=True)
class DocumentStorageEvent:
    """
    Append-oriented storage-control event.
    """

    event_id: str
    event_type: str
    document_id: Optional[str]
    partition_id: Optional[str]
    content_version: Optional[str]
    payload: Mapping[str, object]
    created_at: str = field(default_factory=_now)


@dataclass(frozen=True)
class DocumentStorageCapacity:
    """
    Capacity view.

    Values are observations from a backend/control plane, not global
    hard-coded limits.
    """

    logical_bytes: int
    replicated_bytes: int
    document_count: int
    content_version_count: int
    chunk_count: int
    replica_count: int
    available_bytes: Optional[int] = None
    reserved_bytes: Optional[int] = None


# ---------------------------------------------------------------------------
# Backend contract
# ---------------------------------------------------------------------------

class MassiveDocumentStorageBackend(Protocol):
    """
    Replaceable distributed document-storage backend contract.

    Implementations may use different distributed storage technologies.
    The architecture itself does not depend on Google technologies.
    """

    def put_content_version(
        self,
        content_version: ContentVersion,
    ) -> None:
        ...

    def get_content_version(
        self,
        document_id: str,
        version_id: str,
    ) -> Optional[ContentVersion]:
        ...

    def put_chunk(
        self,
        chunk: ContentChunk,
    ) -> None:
        ...

    def get_chunk(
        self,
        chunk_id: str,
    ) -> Optional[ContentChunk]:
        ...

    def put_manifest(
        self,
        manifest: DocumentManifest,
    ) -> None:
        ...

    def get_manifest(
        self,
        document_id: str,
    ) -> Optional[DocumentManifest]:
        ...

    def put_replica(
        self,
        replica: DocumentReplica,
    ) -> None:
        ...

    def get_replica(
        self,
        replica_id: str,
    ) -> Optional[DocumentReplica]:
        ...

    def put_partition(
        self,
        partition: DocumentStoragePartition,
    ) -> None:
        ...

    def get_partition(
        self,
        partition_id: str,
    ) -> Optional[DocumentStoragePartition]:
        ...

    def append_event(
        self,
        event: DocumentStorageEvent,
    ) -> None:
        ...

    def save_checkpoint(
        self,
        checkpoint: DocumentStorageCheckpoint,
    ) -> None:
        ...


# ---------------------------------------------------------------------------
# Reference metadata backend
# ---------------------------------------------------------------------------

class InMemoryMassiveDocumentStorageMetadata:
    """
    Reference metadata implementation.

    This is intentionally metadata-only and is not intended to represent
    the physical storage architecture of the final platform.

    It exists so the control-plane contract has a concrete reference
    implementation without introducing a single-file global store.
    """

    def __init__(self) -> None:
        self.content_versions: Dict[Tuple[str, str], ContentVersion] = {}
        self.chunks: Dict[str, ContentChunk] = {}
        self.manifests: Dict[str, DocumentManifest] = {}
        self.replicas: Dict[str, DocumentReplica] = {}
        self.partitions: Dict[str, DocumentStoragePartition] = {}
        self.events: List[DocumentStorageEvent] = []
        self.checkpoints: Dict[int, DocumentStorageCheckpoint] = {}

    def put_content_version(
        self,
        content_version: ContentVersion,
    ) -> None:
        key = (
            content_version.document_id,
            content_version.version_id,
        )
        self.content_versions[key] = content_version

    def get_content_version(
        self,
        document_id: str,
        version_id: str,
    ) -> Optional[ContentVersion]:
        return self.content_versions.get((document_id, version_id))

    def put_chunk(
        self,
        chunk: ContentChunk,
    ) -> None:
        self.chunks[chunk.chunk_id] = chunk

    def get_chunk(
        self,
        chunk_id: str,
    ) -> Optional[ContentChunk]:
        return self.chunks.get(chunk_id)

    def put_manifest(
        self,
        manifest: DocumentManifest,
    ) -> None:
        self.manifests[manifest.document_id] = manifest

    def get_manifest(
        self,
        document_id: str,
    ) -> Optional[DocumentManifest]:
        return self.manifests.get(document_id)

    def put_replica(
        self,
        replica: DocumentReplica,
    ) -> None:
        self.replicas[replica.replica_id] = replica

    def get_replica(
        self,
        replica_id: str,
    ) -> Optional[DocumentReplica]:
        return self.replicas.get(replica_id)

    def put_partition(
        self,
        partition: DocumentStoragePartition,
    ) -> None:
        self.partitions[partition.partition_id] = partition

    def get_partition(
        self,
        partition_id: str,
    ) -> Optional[DocumentStoragePartition]:
        return self.partitions.get(partition_id)

    def append_event(
        self,
        event: DocumentStorageEvent,
    ) -> None:
        self.events.append(event)

    def save_checkpoint(
        self,
        checkpoint: DocumentStorageCheckpoint,
    ) -> None:
        self.checkpoints[checkpoint.checkpoint_epoch] = checkpoint


# ---------------------------------------------------------------------------
# Policy
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class MassiveDocumentStoragePolicy:
    """
    Global policy for massive document/content storage.

    The values are policy parameters, not claims about final physical
    infrastructure limits.
    """

    partition_count: int = 1_048_576
    chunk_size_bytes: int = 8 * 1024 * 1024
    replication_factor: int = 3
    checkpoint_epoch_interval: int = 1
    default_storage_tier: DocumentStorageTier = DocumentStorageTier.WARM
    default_compression: ContentCompression = ContentCompression.BLOCK

    require_distinct_regions: bool = True
    require_distinct_zones: bool = True
    require_distinct_nodes: bool = True

    enable_version_history: bool = True
    enable_content_deduplication: bool = True
    enable_tiering: bool = True
    enable_checkpointing: bool = True

    def __post_init__(self) -> None:
        if self.partition_count <= 0:
            raise ValueError("partition_count must be positive")

        if self.chunk_size_bytes <= 0:
            raise ValueError("chunk_size_bytes must be positive")

        if self.replication_factor <= 0:
            raise ValueError("replication_factor must be positive")

        if self.checkpoint_epoch_interval <= 0:
            raise ValueError(
                "checkpoint_epoch_interval must be positive"
            )


# ---------------------------------------------------------------------------
# Main architecture
# ---------------------------------------------------------------------------

class MassiveDocumentContentStorage:
    """
    Global document/content storage architecture.

    Responsibilities:

        - stable document identity
        - deterministic partition routing
        - immutable content versions
        - content chunk metadata
        - document manifests
        - replica registration
        - tier metadata
        - document routing
        - version supersession
        - checkpoint generation
        - storage statistics
        - backend abstraction

    This class is a control-plane architecture, not a local global
    filesystem implementation.
    """

    ARCHITECTURE_VERSION = (
        "massive-document-content-storage.v1"
    )

    SCALE_TARGET = (
        "billions_to_trillions_of_public_web_resources"
    )

    GOOGLE_SCALE_CAPABILITY_TARGET = True
    GOOGLE_TECHNOLOGY_DEPENDENCY = False

    FORBIDDEN_SINGLE_GLOBAL_STORAGE_ASSUMPTIONS = (
        "single_global_document_file",
        "single_global_content_file",
        "single_global_storage_node",
        "single_global_storage_database",
        "fixed_global_document_limit",
        "fixed_global_content_byte_limit",
        "fixed_global_chunk_limit",
    )

    def __init__(
        self,
        backend: Optional[MassiveDocumentStorageBackend] = None,
        policy: Optional[MassiveDocumentStoragePolicy] = None,
    ) -> None:
        self.backend = (
            backend
            if backend is not None
            else InMemoryMassiveDocumentStorageMetadata()
        )

        self.policy = (
            policy
            if policy is not None
            else MassiveDocumentStoragePolicy()
        )

        self._epoch = 0

    # ------------------------------------------------------------------
    # Identity and routing
    # ------------------------------------------------------------------

    def create_document_identity(
        self,
        canonical_url: str,
        content_fingerprint: str,
    ) -> DocumentContentIdentity:
        """
        Create a stable logical identity and initial content version.
        """

        if not canonical_url:
            raise ValueError("canonical_url must not be empty")

        if not content_fingerprint:
            raise ValueError(
                "content_fingerprint must not be empty"
            )

        document_id = _hash(
            f"document:{canonical_url}"
        )

        partition_number = _partition_number(
            document_id,
            self.policy.partition_count,
        )

        partition_id = (
            f"document-partition-{partition_number:08d}"
        )

        version_id = _hash(
            f"version:{document_id}:{content_fingerprint}"
        )

        return DocumentContentIdentity(
            document_id=document_id,
            canonical_url=canonical_url,
            content_version=version_id,
            fingerprint=content_fingerprint,
            partition_id=partition_id,
        )

    def route_partition(
        self,
        document_id: str,
    ) -> str:
        number = _partition_number(
            document_id,
            self.policy.partition_count,
        )

        return (
            f"document-partition-{number:08d}"
        )

    # ------------------------------------------------------------------
    # Partition lifecycle
    # ------------------------------------------------------------------

    def ensure_partition(
        self,
        partition_id: str,
        region_ids: Sequence[str] = (),
    ) -> DocumentStoragePartition:
        existing = self.backend.get_partition(
            partition_id
        )

        if existing is not None:
            return existing

        try:
            partition_number = int(
                partition_id.rsplit("-", 1)[1]
            )
        except (ValueError, IndexError):
            partition_number = _partition_number(
                partition_id,
                self.policy.partition_count,
            )

        partition = DocumentStoragePartition(
            partition_id=partition_id,
            partition_number=partition_number,
            state=DocumentPartitionState.ACTIVE,
            region_ids=tuple(region_ids),
        )

        self.backend.put_partition(partition)

        self._emit_event(
            event_type="partition_created",
            partition_id=partition_id,
            payload={
                "partition_number": partition_number,
                "regions": list(region_ids),
            },
        )

        return partition

    # ------------------------------------------------------------------
    # Content version creation
    # ------------------------------------------------------------------

    def create_content_version(
        self,
        identity: DocumentContentIdentity,
        total_bytes: int,
        chunk_count: int,
    ) -> ContentVersion:
        if total_bytes < 0:
            raise ValueError("total_bytes must be non-negative")

        if chunk_count < 0:
            raise ValueError(
                "chunk_count must be non-negative"
            )

        version = ContentVersion(
            version_id=identity.content_version,
            document_id=identity.document_id,
            fingerprint=identity.fingerprint,
            total_bytes=total_bytes,
            chunk_count=chunk_count,
            state=ContentVersionState.BUILDING,
        )

        self.backend.put_content_version(version)

        self._emit_event(
            event_type="content_version_created",
            document_id=identity.document_id,
            partition_id=identity.partition_id,
            content_version=version.version_id,
            payload={
                "total_bytes": total_bytes,
                "chunk_count": chunk_count,
            },
        )

        return version

    # ------------------------------------------------------------------
    # Content chunk metadata
    # ------------------------------------------------------------------

    def create_chunk_metadata(
        self,
        document_id: str,
        content_version: str,
        ordinal: int,
        logical_offset: int,
        content_bytes: int,
        content_hash: str,
        compression: Optional[ContentCompression] = None,
    ) -> ContentChunk:
        if ordinal < 0:
            raise ValueError("ordinal must be non-negative")

        if logical_offset < 0:
            raise ValueError(
                "logical_offset must be non-negative"
            )

        if content_bytes < 0:
            raise ValueError(
                "content_bytes must be non-negative"
            )

        if not content_hash:
            raise ValueError(
                "content_hash must not be empty"
            )

        compression_mode = (
            compression
            if compression is not None
            else self.policy.default_compression
        )

        chunk_id = _hash(
            (
                f"chunk:{document_id}:"
                f"{content_version}:{ordinal}:"
                f"{content_hash}"
            )
        )

        chunk = ContentChunk(
            chunk_id=chunk_id,
            document_id=document_id,
            content_version=content_version,
            ordinal=ordinal,
            logical_offset=logical_offset,
            length=content_bytes,
            byte_size=content_bytes,
            content_hash=content_hash,
            compression=compression_mode,
        )

        self.backend.put_chunk(chunk)

        return chunk

    # ------------------------------------------------------------------
    # Manifest creation/publication
    # ------------------------------------------------------------------

    def create_manifest(
        self,
        identity: DocumentContentIdentity,
        total_bytes: int,
        chunk_ids: Sequence[str],
        storage_tier: Optional[DocumentStorageTier] = None,
    ) -> DocumentManifest:
        tier = (
            storage_tier
            if storage_tier is not None
            else self.policy.default_storage_tier
        )

        manifest = DocumentManifest(
            document_id=identity.document_id,
            canonical_url=identity.canonical_url,
            current_version=identity.content_version,
            partition_id=identity.partition_id,
            total_bytes=total_bytes,
            chunk_count=len(chunk_ids),
            content_fingerprint=identity.fingerprint,
            state=DocumentStorageState.BUILDING,
            storage_tier=tier,
            chunk_ids=tuple(chunk_ids),
        )

        self.backend.put_manifest(manifest)

        self._emit_event(
            event_type="manifest_created",
            document_id=identity.document_id,
            partition_id=identity.partition_id,
            content_version=identity.content_version,
            payload={
                "chunk_count": len(chunk_ids),
                "total_bytes": total_bytes,
                "storage_tier": tier.value,
            },
        )

        return manifest

    def publish_document(
        self,
        document_id: str,
    ) -> DocumentManifest:
        manifest = self.backend.get_manifest(
            document_id
        )

        if manifest is None:
            raise KeyError(
                f"document manifest not found: {document_id}"
            )

        current = ContentVersion(
            version_id=manifest.current_version,
            document_id=manifest.document_id,
            fingerprint=manifest.content_fingerprint,
            total_bytes=manifest.total_bytes,
            chunk_count=manifest.chunk_count,
            state=ContentVersionState.ACTIVE,
        )

        self.backend.put_content_version(current)

        published = DocumentManifest(
            document_id=manifest.document_id,
            canonical_url=manifest.canonical_url,
            current_version=manifest.current_version,
            partition_id=manifest.partition_id,
            total_bytes=manifest.total_bytes,
            chunk_count=manifest.chunk_count,
            content_fingerprint=manifest.content_fingerprint,
            state=DocumentStorageState.ACTIVE,
            storage_tier=manifest.storage_tier,
            replica_ids=manifest.replica_ids,
            chunk_ids=manifest.chunk_ids,
            created_at=manifest.created_at,
            updated_at=_now(),
        )

        self.backend.put_manifest(published)

        self._emit_event(
            event_type="document_published",
            document_id=document_id,
            partition_id=manifest.partition_id,
            content_version=manifest.current_version,
            payload={
                "replica_count": len(
                    manifest.replica_ids
                ),
            },
        )

        return published

    # ------------------------------------------------------------------
    # Replica management
    # ------------------------------------------------------------------

    def add_replica(
        self,
        document_id: str,
        content_version: str,
        region_id: str,
        zone_id: str,
        node_id: str,
        byte_size: int,
        checksum: str,
        storage_tier: Optional[DocumentStorageTier] = None,
    ) -> DocumentReplica:
        if byte_size < 0:
            raise ValueError(
                "byte_size must be non-negative"
            )

        if not region_id:
            raise ValueError(
                "region_id must not be empty"
            )

        if not zone_id:
            raise ValueError(
                "zone_id must not be empty"
            )

        if not node_id:
            raise ValueError(
                "node_id must not be empty"
            )

        if not checksum:
            raise ValueError(
                "checksum must not be empty"
            )

        tier = (
            storage_tier
            if storage_tier is not None
            else self.policy.default_storage_tier
        )

        replica = DocumentReplica(
            replica_id=_new_id("document-replica"),
            document_id=document_id,
            content_version=content_version,
            region_id=region_id,
            zone_id=zone_id,
            node_id=node_id,
            storage_tier=tier,
            state=DocumentReplicaState.ACTIVE,
            byte_size=byte_size,
            checksum=checksum,
        )

        self.backend.put_replica(replica)

        manifest = self.backend.get_manifest(
            document_id
        )

        if manifest is not None:
            replica_ids = list(
                manifest.replica_ids
            )

            if replica.replica_id not in replica_ids:
                replica_ids.append(
                    replica.replica_id
                )

            updated = DocumentManifest(
                document_id=manifest.document_id,
                canonical_url=manifest.canonical_url,
                current_version=manifest.current_version,
                partition_id=manifest.partition_id,
                total_bytes=manifest.total_bytes,
                chunk_count=manifest.chunk_count,
                content_fingerprint=manifest.content_fingerprint,
                state=manifest.state,
                storage_tier=manifest.storage_tier,
                replica_ids=tuple(replica_ids),
                chunk_ids=manifest.chunk_ids,
                created_at=manifest.created_at,
                updated_at=_now(),
            )

            self.backend.put_manifest(updated)

        self._emit_event(
            event_type="replica_added",
            document_id=document_id,
            content_version=content_version,
            partition_id=(
                manifest.partition_id
                if manifest is not None
                else None
            ),
            payload={
                "replica_id": replica.replica_id,
                "region_id": region_id,
                "zone_id": zone_id,
                "node_id": node_id,
                "tier": tier.value,
            },
        )

        return replica

    # ------------------------------------------------------------------
    # Replica-aware document routing
    # ------------------------------------------------------------------

    def route_document(
        self,
        document_id: str,
    ) -> DocumentStorageRoute:
        manifest = self.backend.get_manifest(
            document_id
        )

        if manifest is None:
            raise KeyError(
                f"document manifest not found: {document_id}"
            )

        active_replicas: List[
            DocumentReplica
        ] = []

        for replica_id in manifest.replica_ids:
            replica = self.backend.get_replica(
                replica_id
            )

            if replica is None:
                continue

            if (
                replica.state
                == DocumentReplicaState.ACTIVE
            ):
                active_replicas.append(replica)

        preferred = tuple(
            replica.replica_id
            for replica in active_replicas
            if replica.storage_tier
            == manifest.storage_tier
        )

        fallback = tuple(
            replica.replica_id
            for replica in active_replicas
            if replica.replica_id not in preferred
        )

        route = DocumentStorageRoute(
            route_id=_new_id("document-route"),
            document_id=document_id,
            content_version=manifest.current_version,
            partition_id=manifest.partition_id,
            preferred_replica_ids=preferred,
            fallback_replica_ids=fallback,
            storage_tier=manifest.storage_tier,
        )

        self._emit_event(
            event_type="document_route_created",
            document_id=document_id,
            partition_id=manifest.partition_id,
            content_version=manifest.current_version,
            payload={
                "preferred_replica_count": len(
                    preferred
                ),
                "fallback_replica_count": len(
                    fallback
                ),
            },
        )

        return route

    # ------------------------------------------------------------------
    # Version supersession
    # ------------------------------------------------------------------

    def supersede_version(
        self,
        document_id: str,
        new_identity: DocumentContentIdentity,
        total_bytes: int,
        chunk_ids: Sequence[str],
        storage_tier: Optional[DocumentStorageTier] = None,
    ) -> DocumentManifest:
        old_manifest = self.backend.get_manifest(
            document_id
        )

        if old_manifest is not None:
            old_version = self.backend.get_content_version(
                document_id,
                old_manifest.current_version,
            )

            if old_version is not None:
                superseded = ContentVersion(
                    version_id=old_version.version_id,
                    document_id=old_version.document_id,
                    fingerprint=old_version.fingerprint,
                    total_bytes=old_version.total_bytes,
                    chunk_count=old_version.chunk_count,
                    state=ContentVersionState.SUPERSEDED,
                    created_at=old_version.created_at,
                    superseded_at=_now(),
                )

                self.backend.put_content_version(
                    superseded
                )

        new_manifest = self.create_manifest(
            identity=new_identity,
            total_bytes=total_bytes,
            chunk_ids=chunk_ids,
            storage_tier=storage_tier,
        )

        self._emit_event(
            event_type="document_version_superseded",
            document_id=document_id,
            partition_id=new_identity.partition_id,
            content_version=new_identity.content_version,
            payload={
                "previous_version": (
                    old_manifest.current_version
                    if old_manifest is not None
                    else None
                ),
                "new_version": (
                    new_identity.content_version
                ),
            },
        )

        return new_manifest

    # ------------------------------------------------------------------
    # Tier movement
    # ------------------------------------------------------------------

    def change_storage_tier(
        self,
        document_id: str,
        storage_tier: DocumentStorageTier,
    ) -> DocumentManifest:
        manifest = self.backend.get_manifest(
            document_id
        )

        if manifest is None:
            raise KeyError(
                f"document manifest not found: {document_id}"
            )

        moved = DocumentManifest(
            document_id=manifest.document_id,
            canonical_url=manifest.canonical_url,
            current_version=manifest.current_version,
            partition_id=manifest.partition_id,
            total_bytes=manifest.total_bytes,
            chunk_count=manifest.chunk_count,
            content_fingerprint=manifest.content_fingerprint,
            state=DocumentStorageState.MOVING,
            storage_tier=storage_tier,
            replica_ids=manifest.replica_ids,
            chunk_ids=manifest.chunk_ids,
            created_at=manifest.created_at,
            updated_at=_now(),
        )

        self.backend.put_manifest(moved)

        self._emit_event(
            event_type="storage_tier_change_started",
            document_id=document_id,
            partition_id=manifest.partition_id,
            content_version=manifest.current_version,
            payload={
                "from": manifest.storage_tier.value,
                "to": storage_tier.value,
            },
        )

        active = DocumentManifest(
            document_id=moved.document_id,
            canonical_url=moved.canonical_url,
            current_version=moved.current_version,
            partition_id=moved.partition_id,
            total_bytes=moved.total_bytes,
            chunk_count=moved.chunk_count,
            content_fingerprint=moved.content_fingerprint,
            state=DocumentStorageState.ACTIVE,
            storage_tier=moved.storage_tier,
            replica_ids=moved.replica_ids,
            chunk_ids=moved.chunk_ids,
            created_at=moved.created_at,
            updated_at=_now(),
        )

        self.backend.put_manifest(active)

        self._emit_event(
            event_type="storage_tier_change_completed",
            document_id=document_id,
            partition_id=manifest.partition_id,
            content_version=manifest.current_version,
            payload={
                "storage_tier": storage_tier.value,
            },
        )

        return active

    # ------------------------------------------------------------------
    # Checkpointing
    # ------------------------------------------------------------------

    def checkpoint(self) -> DocumentStorageCheckpoint:
        self._epoch += 1

        if isinstance(
            self.backend,
            InMemoryMassiveDocumentStorageMetadata,
        ):
            manifests = list(
                self.backend.manifests.values()
            )

            logical_bytes = sum(
                manifest.total_bytes
                for manifest in manifests
                if manifest.state
                != DocumentStorageState.DELETED
            )

            replicated_bytes = sum(
                replica.byte_size
                for replica
                in self.backend.replicas.values()
                if replica.state
                != DocumentReplicaState.DELETED
            )

            checkpoint = DocumentStorageCheckpoint(
                checkpoint_id=_new_id(
                    "document-storage-checkpoint"
                ),
                checkpoint_epoch=self._epoch,
                partition_ids=tuple(
                    self.backend.partitions.keys()
                ),
                manifest_count=len(
                    self.backend.manifests
                ),
                content_version_count=len(
                    self.backend.content_versions
                ),
                replica_count=len(
                    self.backend.replicas
                ),
                logical_bytes=logical_bytes,
                replicated_bytes=replicated_bytes,
            )
        else:
            checkpoint = DocumentStorageCheckpoint(
                checkpoint_id=_new_id(
                    "document-storage-checkpoint"
                ),
                checkpoint_epoch=self._epoch,
                partition_ids=(),
                manifest_count=0,
                content_version_count=0,
                replica_count=0,
                logical_bytes=0,
                replicated_bytes=0,
            )

        self.backend.save_checkpoint(
            checkpoint
        )

        self._emit_event(
            event_type="checkpoint_created",
            payload={
                "checkpoint_epoch": (
                    checkpoint.checkpoint_epoch
                ),
                "manifest_count": (
                    checkpoint.manifest_count
                ),
                "content_version_count": (
                    checkpoint.content_version_count
                ),
                "replica_count": (
                    checkpoint.replica_count
                ),
            },
        )

        return checkpoint

    # ------------------------------------------------------------------
    # Capacity/statistics
    # ------------------------------------------------------------------

    def capacity(self) -> DocumentStorageCapacity:
        if isinstance(
            self.backend,
            InMemoryMassiveDocumentStorageMetadata,
        ):
            manifests = list(
                self.backend.manifests.values()
            )

            logical_bytes = sum(
                manifest.total_bytes
                for manifest in manifests
                if manifest.state
                != DocumentStorageState.DELETED
            )

            replicated_bytes = sum(
                replica.byte_size
                for replica
                in self.backend.replicas.values()
                if replica.state
                != DocumentReplicaState.DELETED
            )

            chunk_count = len(
                self.backend.chunks
            )

            return DocumentStorageCapacity(
                logical_bytes=logical_bytes,
                replicated_bytes=replicated_bytes,
                document_count=len(
                    [
                        manifest
                        for manifest in manifests
                        if manifest.state
                        != DocumentStorageState.DELETED
                    ]
                ),
                content_version_count=len(
                    self.backend.content_versions
                ),
                chunk_count=chunk_count,
                replica_count=len(
                    [
                        replica
                        for replica
                        in self.backend.replicas.values()
                        if replica.state
                        != DocumentReplicaState.DELETED
                    ]
                ),
            )

        return DocumentStorageCapacity(
            logical_bytes=0,
            replicated_bytes=0,
            document_count=0,
            content_version_count=0,
            chunk_count=0,
            replica_count=0,
        )

    # ------------------------------------------------------------------
    # Event handling
    # ------------------------------------------------------------------

    def _emit_event(
        self,
        event_type: str,
        document_id: Optional[str] = None,
        partition_id: Optional[str] = None,
        content_version: Optional[str] = None,
        payload: Optional[Mapping[str, object]] = None,
    ) -> DocumentStorageEvent:
        event = DocumentStorageEvent(
            event_id=_new_id(
                "document-storage-event"
            ),
            event_type=event_type,
            document_id=document_id,
            partition_id=partition_id,
            content_version=content_version,
            payload=(
                payload
                if payload is not None
                else {}
            ),
        )

        self.backend.append_event(event)

        return event

    # ------------------------------------------------------------------
    # Architecture description
    # ------------------------------------------------------------------

    def architecture(self) -> Mapping[str, object]:
        return {
            "version": self.ARCHITECTURE_VERSION,
            "scale_target": self.SCALE_TARGET,
            "google_scale_capability_target": (
                self.GOOGLE_SCALE_CAPABILITY_TARGET
            ),
            "google_technology_dependency": (
                self.GOOGLE_TECHNOLOGY_DEPENDENCY
            ),
            "pipeline": [
                "crawl_result",
                "document_identity",
                "content_ingestion",
                "content_version",
                "content_chunking",
                "document_manifest",
                "distributed_document_storage",
                "replication",
                "storage_tiering",
                "version_history",
                "checkpoint_recovery",
                "document_read_routing",
            ],
            "storage_model": {
                "logical_identity": True,
                "immutable_content_versions": True,
                "distributed_partitions": True,
                "distributed_replicas": True,
                "storage_tiers": [
                    tier.value
                    for tier
                    in DocumentStorageTier
                ],
                "content_chunking": True,
                "manifest_based_access": True,
            },
            "forbidden_single_global_assumptions": list(
                self.FORBIDDEN_SINGLE_GLOBAL_STORAGE_ASSUMPTIONS
            ),
            "horizontal_scaling": True,
            "replaceable_backend": True,
            "checkpointing": (
                self.policy.enable_checkpointing
            ),
            "content_deduplication": (
                self.policy.enable_content_deduplication
            ),
            "version_history": (
                self.policy.enable_version_history
            ),
            "tiering": (
                self.policy.enable_tiering
            ),
        }


# ---------------------------------------------------------------------------
# Stable aliases
# ---------------------------------------------------------------------------

MassiveDocumentContentStore = (
    MassiveDocumentContentStorage
)

Phase10_3MassiveDocumentContentStorage = (
    MassiveDocumentContentStorage
)


# ---------------------------------------------------------------------------
# Architectural contract
# ---------------------------------------------------------------------------

__all__ = [
    "DocumentStorageState",
    "ContentObjectType",
    "DocumentStorageTier",
    "ContentCompression",
    "DocumentReplicaState",
    "ContentVersionState",
    "DocumentPartitionState",
    "DocumentContentIdentity",
    "ContentChunk",
    "ContentVersion",
    "DocumentManifest",
    "DocumentReplica",
    "DocumentStoragePartition",
    "DocumentStorageRoute",
    "DocumentStorageCheckpoint",
    "DocumentStorageEvent",
    "DocumentStorageCapacity",
    "MassiveDocumentStorageBackend",
    "InMemoryMassiveDocumentStorageMetadata",
    "MassiveDocumentStoragePolicy",
    "MassiveDocumentContentStorage",
    "MassiveDocumentContentStore",
    "Phase10_3MassiveDocumentContentStorage",
]
