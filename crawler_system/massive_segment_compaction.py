"""
OUR SEARCH
Phase 10.5 — Compaction / Segment Merge at Scale

Version:
    massive-segment-compaction.v1

Purpose:
    Global compaction and segment-merge control architecture for the
    enormous OUR SEARCH distributed inverted-index platform.

Scale target:
    - Billions of public-Web resources.
    - Potentially trillions of public-Web resources.
    - Google-scale capability target from the beginning.
    - No prototype-first scaling assumptions.

Architecture:

    IMMUTABLE SEGMENTS
            |
            v
    SEGMENT DISCOVERY
            |
            v
    SEGMENT FAMILIES
            |
            v
    MERGE CANDIDATE PLANNING
            |
            v
    MULTI-LEVEL COMPACTION
            |
            v
    RESOURCE-AWARE SCHEDULING
            |
            v
    MERGE EXECUTION
            |
            v
    NEW IMMUTABLE GENERATION
            |
            v
    ATOMIC MANIFEST REPLACEMENT
            |
            v
    OLD GENERATION RETIREMENT
            |
            v
    RECOVERY-SAFE LINEAGE

Important principles:

    1. Existing immutable segments remain immutable.
    2. Compaction creates new immutable segment generations.
    3. Existing readers can continue using old generations.
    4. Manifest replacement changes the active generation atomically.
    5. Old generations are retired only after safe-reader conditions.
    6. Segment lineage is retained for recovery and auditing.
    7. Compaction is partition-aware.
    8. Compaction is family-aware.
    9. Compaction levels prevent uncontrolled fragmentation.
    10. Scheduling is resource-aware.
    11. Scheduling can account for CPU, memory, IO and storage pressure.
    12. Merge fan-in is policy-controlled.
    13. Merge jobs can be paused, resumed, retried and fenced.
    14. Compaction ownership is epoch-aware.
    15. Duplicate concurrent compactions are prevented logically.
    16. Failed merges do not destroy source generations.
    17. Source generations are retired only after successful publication.
    18. No single global segment directory is assumed.
    19. No single global merge queue is assumed.
    20. No single global index database is assumed.
    21. No fixed global segment count is assumed.
    22. No fixed global document count is assumed.
    23. No fixed global posting count is assumed.
    24. Google technologies are not dependencies.

This module is an architecture/control-plane layer.
It complements Phase 10.1 and Phase 10.2 rather than replacing them.
"""


from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from hashlib import sha256
from typing import Dict, Iterable, List, Mapping, Optional, Protocol, Sequence, Tuple
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

class SegmentLifecycleState(str, Enum):
    BUILDING = "building"
    ACTIVE = "active"
    MERGING = "merging"
    RETIRING = "retiring"
    RETIRED = "retired"
    FAILED = "failed"
    DELETED = "deleted"


class SegmentFamilyState(str, Enum):
    ACTIVE = "active"
    COMPACTING = "compacting"
    DEGRADED = "degraded"
    RETIRING = "retiring"


class CompactionLevel(str, Enum):
    """
    Logical compaction levels.

    Level numbering is intentionally abstract. Physical storage
    implementations may map these levels to different policies.
    """

    L0 = "L0"
    L1 = "L1"
    L2 = "L2"
    L3 = "L3"
    L4 = "L4"
    L5 = "L5"
    L6 = "L6"
    L7 = "L7"


class CompactionJobState(str, Enum):
    PLANNED = "planned"
    QUEUED = "queued"
    RUNNING = "running"
    PAUSED = "paused"
    PUBLISHING = "publishing"
    COMPLETED = "completed"
    FAILED = "failed"
    FENCED = "fenced"
    CANCELLED = "cancelled"


class MergeGenerationState(str, Enum):
    PLANNED = "planned"
    BUILDING = "building"
    PUBLISHED = "published"
    RETIRED = "retired"
    ABORTED = "aborted"


class ResourcePressureState(str, Enum):
    NORMAL = "normal"
    ELEVATED = "elevated"
    HIGH = "high"
    CRITICAL = "critical"


class CompactionTrigger(str, Enum):
    SEGMENT_COUNT = "segment_count"
    SIZE_RATIO = "size_ratio"
    LEVEL_PRESSURE = "level_pressure"
    WRITE_AMPLIFICATION = "write_amplification"
    STORAGE_PRESSURE = "storage_pressure"
    FRAGMENTATION = "fragmentation"
    MANUAL_POLICY = "manual_policy"


class LineageEventType(str, Enum):
    SEGMENT_REGISTERED = "segment_registered"
    FAMILY_CREATED = "family_created"
    COMPACTION_PLANNED = "compaction_planned"
    COMPACTION_STARTED = "compaction_started"
    COMPACTION_PAUSED = "compaction_paused"
    COMPACTION_FAILED = "compaction_failed"
    GENERATION_PUBLISHED = "generation_published"
    MANIFEST_SWAPPED = "manifest_swapped"
    SOURCE_RETIREMENT_STARTED = "source_retirement_started"
    SOURCE_RETIRED = "source_retired"
    COMPACTION_FENCED = "compaction_fenced"
    CHECKPOINT_CREATED = "checkpoint_created"


# ============================================================================
# Core segment identity
# ============================================================================

@dataclass(frozen=True)
class SegmentIdentity:
    """
    Stable identity of an immutable index segment.
    """

    segment_id: str
    partition_id: str

    generation: int
    level: CompactionLevel

    document_count: int
    posting_count: int
    vocabulary_count: int

    logical_bytes: int
    fingerprint: str

    created_at: str = field(default_factory=_now)


@dataclass(frozen=True)
class SegmentRecord:
    """
    Immutable segment metadata.

    The actual segment contents remain owned by the storage backend.
    """

    identity: SegmentIdentity
    lifecycle_state: SegmentLifecycleState

    source_segment_ids: Tuple[str, ...] = ()
    family_id: Optional[str] = None

    created_epoch: int = 0
    published_epoch: Optional[int] = None
    retired_epoch: Optional[int] = None

    storage_region_ids: Tuple[str, ...] = ()

    created_at: str = field(default_factory=_now)
    updated_at: str = field(default_factory=_now)


# ============================================================================
# Segment family
# ============================================================================

@dataclass(frozen=True)
class SegmentFamily:
    """
    Logical family of segments belonging to the same index partition
    and compaction lineage.

    Families prevent the merge planner from treating the entire global
    index as one giant merge domain.
    """

    family_id: str
    partition_id: str

    level: CompactionLevel

    segment_ids: Tuple[str, ...]

    state: SegmentFamilyState

    total_logical_bytes: int
    total_documents: int
    total_postings: int

    generation_epoch: int

    created_at: str = field(default_factory=_now)
    updated_at: str = field(default_factory=_now)


# ============================================================================
# Resource model
# ============================================================================

@dataclass(frozen=True)
class CompactionResourceProfile:
    """
    Resource availability for one compaction execution domain.
    """

    resource_domain_id: str

    cpu_capacity: float
    cpu_used: float

    memory_capacity_bytes: int
    memory_used_bytes: int

    io_capacity_bytes_per_second: int
    io_used_bytes_per_second: int

    storage_capacity_bytes: int
    storage_used_bytes: int

    pressure_state: ResourcePressureState

    observed_at: str = field(default_factory=_now)


# ============================================================================
# Merge plan
# ============================================================================

@dataclass(frozen=True)
class MergePlan:
    """
    Immutable description of one planned merge.
    """

    merge_plan_id: str

    partition_id: str
    family_id: str

    source_segment_ids: Tuple[str, ...]

    target_level: CompactionLevel

    estimated_input_bytes: int
    estimated_output_bytes: int

    estimated_documents: int
    estimated_postings: int

    trigger: CompactionTrigger

    priority: int

    created_epoch: int

    created_at: str = field(default_factory=_now)


@dataclass(frozen=True)
class CompactionJob:
    """
    Distributed compaction job state.
    """

    job_id: str
    merge_plan_id: str

    partition_id: str
    family_id: str

    source_segment_ids: Tuple[str, ...]
    target_level: CompactionLevel

    resource_domain_id: str

    state: CompactionJobState

    owner_epoch: int

    retry_count: int

    max_retries: int

    started_at: Optional[str] = None
    completed_at: Optional[str] = None

    updated_at: str = field(default_factory=_now)


# ============================================================================
# Merge generation
# ============================================================================

@dataclass(frozen=True)
class MergeGeneration:
    """
    New immutable segment generation produced by a compaction.

    A generation is published before its source segments are retired.
    """

    generation_id: str

    partition_id: str
    family_id: str

    target_level: CompactionLevel

    source_segment_ids: Tuple[str, ...]

    output_segment_ids: Tuple[str, ...]

    state: MergeGenerationState

    generation_epoch: int

    output_logical_bytes: int
    output_documents: int
    output_postings: int

    output_fingerprint: str

    created_at: str = field(default_factory=_now)
    updated_at: str = field(default_factory=_now)


# ============================================================================
# Active manifest
# ============================================================================

@dataclass(frozen=True)
class PartitionSegmentManifest:
    """
    Active segment view for one logical index partition.

    This is the logical pointer that readers use.

    It is not the physical segment contents.
    """

    manifest_id: str

    partition_id: str

    manifest_epoch: int

    active_segment_ids: Tuple[str, ...]

    generation_ids: Tuple[str, ...]

    created_at: str = field(default_factory=_now)


# ============================================================================
# Retirement tracking
# ============================================================================

@dataclass(frozen=True)
class SegmentRetirementRecord:
    """
    Tracks safe retirement of old immutable generations.
    """

    retirement_id: str

    segment_id: str
    partition_id: str

    retirement_epoch: int

    minimum_reader_epoch: int

    safe_to_delete: bool

    state: SegmentLifecycleState

    created_at: str = field(default_factory=_now)


# ============================================================================
# Lineage / checkpoint
# ============================================================================

@dataclass(frozen=True)
class CompactionLineageEvent:
    """
    Append-oriented lineage event.
    """

    event_id: str
    event_type: LineageEventType

    partition_id: Optional[str]
    segment_id: Optional[str]
    job_id: Optional[str]
    generation_id: Optional[str]

    epoch: int

    payload: Mapping[str, object]

    created_at: str = field(default_factory=_now)


@dataclass(frozen=True)
class CompactionCheckpoint:
    """
    Recovery-safe compaction checkpoint.
    """

    checkpoint_id: str
    epoch: int

    partition_count: int
    segment_count: int
    family_count: int

    active_job_count: int
    completed_job_count: int
    failed_job_count: int

    logical_bytes: int
    output_bytes: int

    active_generation_count: int
    retiring_segment_count: int

    created_at: str = field(default_factory=_now)


@dataclass(frozen=True)
class CompactionCapacity:
    """
    Global compaction accounting view.

    These are observed values, not fixed global limits.
    """

    partition_count: int
    segment_count: int
    family_count: int

    active_jobs: int
    running_jobs: int
    failed_jobs: int

    logical_bytes: int
    output_bytes: int

    active_generations: int
    retiring_segments: int


# ============================================================================
# Backend contract
# ============================================================================

class MassiveSegmentCompactionBackend(Protocol):
    """
    Replaceable distributed compaction metadata backend.

    The final physical implementation can distribute these records
    across many machines, regions and storage systems.
    """

    def put_segment(
        self,
        segment: SegmentRecord,
    ) -> None:
        ...

    def get_segment(
        self,
        segment_id: str,
    ) -> Optional[SegmentRecord]:
        ...

    def put_family(
        self,
        family: SegmentFamily,
    ) -> None:
        ...

    def get_family(
        self,
        family_id: str,
    ) -> Optional[SegmentFamily]:
        ...

    def put_resource_profile(
        self,
        profile: CompactionResourceProfile,
    ) -> None:
        ...

    def get_resource_profile(
        self,
        resource_domain_id: str,
    ) -> Optional[CompactionResourceProfile]:
        ...

    def put_merge_plan(
        self,
        plan: MergePlan,
    ) -> None:
        ...

    def put_job(
        self,
        job: CompactionJob,
    ) -> None:
        ...

    def get_job(
        self,
        job_id: str,
    ) -> Optional[CompactionJob]:
        ...

    def put_generation(
        self,
        generation: MergeGeneration,
    ) -> None:
        ...

    def get_generation(
        self,
        generation_id: str,
    ) -> Optional[MergeGeneration]:
        ...

    def put_manifest(
        self,
        manifest: PartitionSegmentManifest,
    ) -> None:
        ...

    def get_manifest(
        self,
        partition_id: str,
    ) -> Optional[PartitionSegmentManifest]:
        ...

    def put_retirement(
        self,
        retirement: SegmentRetirementRecord,
    ) -> None:
        ...

    def append_event(
        self,
        event: CompactionLineageEvent,
    ) -> None:
        ...

    def save_checkpoint(
        self,
        checkpoint: CompactionCheckpoint,
    ) -> None:
        ...


# ============================================================================
# Reference backend
# ============================================================================

class InMemoryMassiveSegmentCompactionMetadata:
    """
    Reference metadata implementation.

    This is intentionally a control-plane reference backend.

    It does NOT represent the final physical global storage system.
    """

    def __init__(self) -> None:
        self.segments: Dict[
            str,
            SegmentRecord,
        ] = {}

        self.families: Dict[
            str,
            SegmentFamily,
        ] = {}

        self.resources: Dict[
            str,
            CompactionResourceProfile,
        ] = {}

        self.merge_plans: Dict[
            str,
            MergePlan,
        ] = {}

        self.jobs: Dict[
            str,
            CompactionJob,
        ] = {}

        self.generations: Dict[
            str,
            MergeGeneration,
        ] = {}

        self.manifests: Dict[
            str,
            PartitionSegmentManifest,
        ] = {}

        self.retirements: Dict[
            str,
            SegmentRetirementRecord,
        ] = {}

        self.events: List[
            CompactionLineageEvent
        ] = []

        self.checkpoints: Dict[
            int,
            CompactionCheckpoint,
        ] = {}

    def put_segment(
        self,
        segment: SegmentRecord,
    ) -> None:
        self.segments[
            segment.identity.segment_id
        ] = segment

    def get_segment(
        self,
        segment_id: str,
    ) -> Optional[SegmentRecord]:
        return self.segments.get(
            segment_id
        )

    def put_family(
        self,
        family: SegmentFamily,
    ) -> None:
        self.families[
            family.family_id
        ] = family

    def get_family(
        self,
        family_id: str,
    ) -> Optional[SegmentFamily]:
        return self.families.get(
            family_id
        )

    def put_resource_profile(
        self,
        profile: CompactionResourceProfile,
    ) -> None:
        self.resources[
            profile.resource_domain_id
        ] = profile

    def get_resource_profile(
        self,
        resource_domain_id: str,
    ) -> Optional[CompactionResourceProfile]:
        return self.resources.get(
            resource_domain_id
        )

    def put_merge_plan(
        self,
        plan: MergePlan,
    ) -> None:
        self.merge_plans[
            plan.merge_plan_id
        ] = plan

    def put_job(
        self,
        job: CompactionJob,
    ) -> None:
        self.jobs[
            job.job_id
        ] = job

    def get_job(
        self,
        job_id: str,
    ) -> Optional[CompactionJob]:
        return self.jobs.get(
            job_id
        )

    def put_generation(
        self,
        generation: MergeGeneration,
    ) -> None:
        self.generations[
            generation.generation_id
        ] = generation

    def get_generation(
        self,
        generation_id: str,
    ) -> Optional[MergeGeneration]:
        return self.generations.get(
            generation_id
        )

    def put_manifest(
        self,
        manifest: PartitionSegmentManifest,
    ) -> None:
        self.manifests[
            manifest.partition_id
        ] = manifest

    def get_manifest(
        self,
        partition_id: str,
    ) -> Optional[PartitionSegmentManifest]:
        return self.manifests.get(
            partition_id
        )

    def put_retirement(
        self,
        retirement: SegmentRetirementRecord,
    ) -> None:
        self.retirements[
            retirement.retirement_id
        ] = retirement

    def append_event(
        self,
        event: CompactionLineageEvent,
    ) -> None:
        self.events.append(
            event
        )

    def save_checkpoint(
        self,
        checkpoint: CompactionCheckpoint,
    ) -> None:
        self.checkpoints[
            checkpoint.epoch
        ] = checkpoint


# ============================================================================
# Policy
# ============================================================================

@dataclass(frozen=True)
class MassiveCompactionPolicy:
    """
    Global compaction policy.

    Values describe logical policy rather than final physical
    infrastructure limits.
    """

    max_source_segments_per_merge: int = 32

    target_segment_size_bytes: int = (
        512 * 1024 * 1024
    )

    l0_segment_threshold: int = 8
    level_segment_threshold: int = 16

    size_ratio_threshold: float = 10.0

    max_concurrent_jobs_per_resource_domain: int = 8

    cpu_pressure_pause_threshold: float = 0.90
    memory_pressure_pause_threshold: float = 0.90
    io_pressure_pause_threshold: float = 0.90
    storage_pressure_pause_threshold: float = 0.95

    max_job_retries: int = 5

    checkpoint_epoch_interval: int = 1

    reader_grace_epochs: int = 2

    enable_size_ratio_trigger: bool = True
    enable_fragmentation_trigger: bool = True
    enable_storage_pressure_trigger: bool = True

    def __post_init__(self) -> None:
        if (
            self.max_source_segments_per_merge
            <= 1
        ):
            raise ValueError(
                "max_source_segments_per_merge must be greater than 1"
            )

        if (
            self.target_segment_size_bytes
            <= 0
        ):
            raise ValueError(
                "target_segment_size_bytes must be positive"
            )

        if self.l0_segment_threshold <= 0:
            raise ValueError(
                "l0_segment_threshold must be positive"
            )

        if self.level_segment_threshold <= 0:
            raise ValueError(
                "level_segment_threshold must be positive"
            )

        if self.size_ratio_threshold <= 1.0:
            raise ValueError(
                "size_ratio_threshold must be greater than 1"
            )

        if (
            self.max_concurrent_jobs_per_resource_domain
            <= 0
        ):
            raise ValueError(
                "max_concurrent_jobs_per_resource_domain must be positive"
            )

        for value, name in (
            (
                self.cpu_pressure_pause_threshold,
                "cpu_pressure_pause_threshold",
            ),
            (
                self.memory_pressure_pause_threshold,
                "memory_pressure_pause_threshold",
            ),
            (
                self.io_pressure_pause_threshold,
                "io_pressure_pause_threshold",
            ),
            (
                self.storage_pressure_pause_threshold,
                "storage_pressure_pause_threshold",
            ),
        ):
            if not 0.0 < value <= 1.0:
                raise ValueError(
                    f"{name} must be within (0, 1]"
                )

        if self.max_job_retries < 0:
            raise ValueError(
                "max_job_retries must be non-negative"
            )

        if self.checkpoint_epoch_interval <= 0:
            raise ValueError(
                "checkpoint_epoch_interval must be positive"
            )

        if self.reader_grace_epochs < 0:
            raise ValueError(
                "reader_grace_epochs must be non-negative"
            )


# ============================================================================
# Main architecture
# ============================================================================

class MassiveSegmentCompactionArchitecture:
    """
    Global compaction / segment merge architecture.

    Responsibilities:

        - immutable segment registration
        - segment-family construction
        - multi-level compaction policy
        - merge candidate planning
        - resource-aware scheduling
        - compaction job lifecycle
        - immutable output generations
        - atomic manifest publication
        - source-generation retirement
        - lineage tracking
        - epoch fencing
        - recovery-safe checkpoints
        - capacity accounting

    This is a control-plane architecture for enormous distributed
    index infrastructure.
    """

    ARCHITECTURE_VERSION = (
        "massive-segment-compaction.v1"
    )

    SCALE_TARGET = (
        "billions_to_trillions_of_public_web_resources"
    )

    GOOGLE_SCALE_CAPABILITY_TARGET = True
    GOOGLE_TECHNOLOGY_DEPENDENCY = False

    FORBIDDEN_SINGLE_GLOBAL_ASSUMPTIONS = (
        "single_global_segment_directory",
        "single_global_merge_queue",
        "single_global_compaction_worker",
        "single_global_index_database",
        "fixed_global_segment_count",
        "fixed_global_document_count",
        "fixed_global_posting_count",
    )

    def __init__(
        self,
        backend: Optional[
            MassiveSegmentCompactionBackend
        ] = None,
        policy: Optional[
            MassiveCompactionPolicy
        ] = None,
    ) -> None:
        self.backend = (
            backend
            if backend is not None
            else InMemoryMassiveSegmentCompactionMetadata()
        )

        self.policy = (
            policy
            if policy is not None
            else MassiveCompactionPolicy()
        )

        self._epoch = 0

    # ------------------------------------------------------------------
    # Epoch
    # ------------------------------------------------------------------

    def current_epoch(self) -> int:
        return self._epoch

    def advance_epoch(self) -> int:
        self._epoch += 1
        return self._epoch

    # ------------------------------------------------------------------
    # Segment registration
    # ------------------------------------------------------------------

    def register_segment(
        self,
        segment_id: str,
        partition_id: str,
        generation: int,
        level: CompactionLevel,
        document_count: int,
        posting_count: int,
        vocabulary_count: int,
        logical_bytes: int,
        fingerprint: str,
        family_id: Optional[str] = None,
        storage_region_ids: Sequence[str] = (),
    ) -> SegmentRecord:
        if not segment_id:
            raise ValueError(
                "segment_id must not be empty"
            )

        if not partition_id:
            raise ValueError(
                "partition_id must not be empty"
            )

        if generation < 0:
            raise ValueError(
                "generation must be non-negative"
            )

        if document_count < 0:
            raise ValueError(
                "document_count must be non-negative"
            )

        if posting_count < 0:
            raise ValueError(
                "posting_count must be non-negative"
            )

        if vocabulary_count < 0:
            raise ValueError(
                "vocabulary_count must be non-negative"
            )

        if logical_bytes < 0:
            raise ValueError(
                "logical_bytes must be non-negative"
            )

        if not fingerprint:
            raise ValueError(
                "fingerprint must not be empty"
            )

        self.advance_epoch()

        identity = SegmentIdentity(
            segment_id=segment_id,
            partition_id=partition_id,
            generation=generation,
            level=level,
            document_count=document_count,
            posting_count=posting_count,
            vocabulary_count=vocabulary_count,
            logical_bytes=logical_bytes,
            fingerprint=fingerprint,
        )

        segment = SegmentRecord(
            identity=identity,
            lifecycle_state=SegmentLifecycleState.ACTIVE,
            family_id=family_id,
            created_epoch=self._epoch,
            storage_region_ids=tuple(
                storage_region_ids
            ),
        )

        self.backend.put_segment(
            segment
        )

        self._emit_event(
            event_type=(
                LineageEventType.SEGMENT_REGISTERED
            ),
            partition_id=partition_id,
            segment_id=segment_id,
            epoch=self._epoch,
            payload={
                "generation": generation,
                "level": level.value,
                "logical_bytes": logical_bytes,
            },
        )

        return segment

    # ------------------------------------------------------------------
    # Family management
    # ------------------------------------------------------------------

    def create_family(
        self,
        partition_id: str,
        level: CompactionLevel,
        segment_ids: Sequence[str],
        generation_epoch: Optional[int] = None,
    ) -> SegmentFamily:
        if not partition_id:
            raise ValueError(
                "partition_id must not be empty"
            )

        if not segment_ids:
            raise ValueError(
                "segment_ids must not be empty"
            )

        segments: List[
            SegmentRecord
        ] = []

        for segment_id in segment_ids:
            segment = self.backend.get_segment(
                segment_id
            )

            if segment is None:
                raise KeyError(
                    f"segment not found: {segment_id}"
                )

            if (
                segment.identity.partition_id
                != partition_id
            ):
                raise ValueError(
                    "all family segments must belong to the same partition"
                )

            segments.append(
                segment
            )

        self.advance_epoch()

        family_id = _new_id(
            "segment-family"
        )

        total_bytes = sum(
            segment.identity.logical_bytes
            for segment in segments
        )

        total_documents = sum(
            segment.identity.document_count
            for segment in segments
        )

        total_postings = sum(
            segment.identity.posting_count
            for segment in segments
        )

        family = SegmentFamily(
            family_id=family_id,
            partition_id=partition_id,
            level=level,
            segment_ids=tuple(
                segment_ids
            ),
            state=SegmentFamilyState.ACTIVE,
            total_logical_bytes=total_bytes,
            total_documents=total_documents,
            total_postings=total_postings,
            generation_epoch=(
                self._epoch
                if generation_epoch is None
                else generation_epoch
            ),
        )

        self.backend.put_family(
            family
        )

        for segment in segments:
            updated_segment = SegmentRecord(
                identity=segment.identity,
                lifecycle_state=(
                    segment.lifecycle_state
                ),
                source_segment_ids=(
                    segment.source_segment_ids
                ),
                family_id=family_id,
                created_epoch=(
                    segment.created_epoch
                ),
                published_epoch=(
                    segment.published_epoch
                ),
                retired_epoch=(
                    segment.retired_epoch
                ),
                storage_region_ids=(
                    segment.storage_region_ids
                ),
                created_at=segment.created_at,
                updated_at=_now(),
            )

            self.backend.put_segment(
                updated_segment
            )

        self._emit_event(
            event_type=(
                LineageEventType.FAMILY_CREATED
            ),
            partition_id=partition_id,
            epoch=self._epoch,
            payload={
                "family_id": family_id,
                "level": level.value,
                "segment_count": len(
                    segment_ids
                ),
            },
        )

        return family

    # ------------------------------------------------------------------
    # Resource profiles
    # ------------------------------------------------------------------

    def register_resource_profile(
        self,
        resource_domain_id: str,
        cpu_capacity: float,
        cpu_used: float,
        memory_capacity_bytes: int,
        memory_used_bytes: int,
        io_capacity_bytes_per_second: int,
        io_used_bytes_per_second: int,
        storage_capacity_bytes: int,
        storage_used_bytes: int,
    ) -> CompactionResourceProfile:
        if not resource_domain_id:
            raise ValueError(
                "resource_domain_id must not be empty"
            )

        if cpu_capacity <= 0:
            raise ValueError(
                "cpu_capacity must be positive"
            )

        if cpu_used < 0:
            raise ValueError(
                "cpu_used must be non-negative"
            )

        if memory_capacity_bytes <= 0:
            raise ValueError(
                "memory_capacity_bytes must be positive"
            )

        if memory_used_bytes < 0:
            raise ValueError(
                "memory_used_bytes must be non-negative"
            )

        if io_capacity_bytes_per_second <= 0:
            raise ValueError(
                "io_capacity_bytes_per_second must be positive"
            )

        if io_used_bytes_per_second < 0:
            raise ValueError(
                "io_used_bytes_per_second must be non-negative"
            )

        if storage_capacity_bytes <= 0:
            raise ValueError(
                "storage_capacity_bytes must be positive"
            )

        if storage_used_bytes < 0:
            raise ValueError(
                "storage_used_bytes must be non-negative"
            )

        cpu_ratio = (
            cpu_used / cpu_capacity
        )

        memory_ratio = (
            memory_used_bytes
            / memory_capacity_bytes
        )

        io_ratio = (
            io_used_bytes_per_second
            / io_capacity_bytes_per_second
        )

        storage_ratio = (
            storage_used_bytes
            / storage_capacity_bytes
        )

        maximum_pressure = max(
            cpu_ratio,
            memory_ratio,
            io_ratio,
            storage_ratio,
        )

        if maximum_pressure >= 0.95:
            pressure = (
                ResourcePressureState.CRITICAL
            )
        elif maximum_pressure >= 0.85:
            pressure = (
                ResourcePressureState.HIGH
            )
        elif maximum_pressure >= 0.70:
            pressure = (
                ResourcePressureState.ELEVATED
            )
        else:
            pressure = (
                ResourcePressureState.NORMAL
            )

        profile = CompactionResourceProfile(
            resource_domain_id=resource_domain_id,
            cpu_capacity=cpu_capacity,
            cpu_used=cpu_used,
            memory_capacity_bytes=(
                memory_capacity_bytes
            ),
            memory_used_bytes=(
                memory_used_bytes
            ),
            io_capacity_bytes_per_second=(
                io_capacity_bytes_per_second
            ),
            io_used_bytes_per_second=(
                io_used_bytes_per_second
            ),
            storage_capacity_bytes=(
                storage_capacity_bytes
            ),
            storage_used_bytes=(
                storage_used_bytes
            ),
            pressure_state=pressure,
        )

        self.backend.put_resource_profile(
            profile
        )

        return profile

    # ------------------------------------------------------------------
    # Compaction candidate discovery
    # ------------------------------------------------------------------

    def plan_compaction(
        self,
        family_id: str,
        trigger: CompactionTrigger = (
            CompactionTrigger.SEGMENT_COUNT
        ),
        priority: int = 0,
    ) -> MergePlan:
        family = self.backend.get_family(
            family_id
        )

        if family is None:
            raise KeyError(
                f"segment family not found: {family_id}"
            )

        if family.state in {
            SegmentFamilyState.COMPACTING,
            SegmentFamilyState.RETIRING,
        }:
            raise ValueError(
                "segment family is not available for a new compaction plan"
            )

        source_ids = list(
            family.segment_ids
        )

        if (
            len(source_ids)
            > self.policy.max_source_segments_per_merge
        ):
            source_ids = source_ids[
                : self.policy.max_source_segments_per_merge
            ]

        if len(source_ids) <= 1:
            raise ValueError(
                "compaction requires multiple source segments"
            )

        source_segments = []

        for segment_id in source_ids:
            segment = self.backend.get_segment(
                segment_id
            )

            if segment is None:
                raise KeyError(
                    f"segment not found: {segment_id}"
                )

            if (
                segment.lifecycle_state
                != SegmentLifecycleState.ACTIVE
            ):
                raise ValueError(
                    "all compaction sources must be active"
                )

            source_segments.append(
                segment
            )

        target_level = self._next_level(
            family.level
        )

        input_bytes = sum(
            segment.identity.logical_bytes
            for segment in source_segments
        )

        estimated_output = min(
            input_bytes,
            self.policy.target_segment_size_bytes
            * max(1, len(source_segments)),
        )

        documents = sum(
            segment.identity.document_count
            for segment in source_segments
        )

        postings = sum(
            segment.identity.posting_count
            for segment in source_segments
        )

        self.advance_epoch()

        plan = MergePlan(
            merge_plan_id=_new_id(
                "merge-plan"
            ),
            partition_id=family.partition_id,
            family_id=family.family_id,
            source_segment_ids=tuple(
                source_ids
            ),
            target_level=target_level,
            estimated_input_bytes=input_bytes,
            estimated_output_bytes=estimated_output,
            estimated_documents=documents,
            estimated_postings=postings,
            trigger=trigger,
            priority=priority,
            created_epoch=self._epoch,
        )

        self.backend.put_merge_plan(
            plan
        )

        compacting_family = SegmentFamily(
            family_id=family.family_id,
            partition_id=family.partition_id,
            level=family.level,
            segment_ids=family.segment_ids,
            state=SegmentFamilyState.COMPACTING,
            total_logical_bytes=(
                family.total_logical_bytes
            ),
            total_documents=(
                family.total_documents
            ),
            total_postings=(
                family.total_postings
            ),
            generation_epoch=family.generation_epoch,
            created_at=family.created_at,
            updated_at=_now(),
        )

        self.backend.put_family(
            compacting_family
        )

        self._emit_event(
            event_type=(
                LineageEventType.COMPACTION_PLANNED
            ),
            partition_id=family.partition_id,
            epoch=self._epoch,
            payload={
                "merge_plan_id": (
                    plan.merge_plan_id
                ),
                "family_id": family.family_id,
                "source_segment_count": (
                    len(source_ids)
                ),
                "target_level": (
                    target_level.value
                ),
                "trigger": trigger.value,
                "priority": priority,
            },
        )

        return plan

    # ------------------------------------------------------------------
    # Scheduling
    # ------------------------------------------------------------------

    def schedule_compaction(
        self,
        merge_plan_id: str,
        resource_domain_id: str,
    ) -> CompactionJob:
        profile = self.backend.get_resource_profile(
            resource_domain_id
        )

        if profile is None:
            raise KeyError(
                f"resource profile not found: {resource_domain_id}"
            )

        if profile.pressure_state in {
            ResourcePressureState.CRITICAL,
        }:
            raise ValueError(
                "resource domain is under critical pressure"
            )

        plan = self._find_merge_plan(
            merge_plan_id
        )

        if plan is None:
            raise KeyError(
                f"merge plan not found: {merge_plan_id}"
            )

        running_jobs = self._running_jobs_for_resource(
            resource_domain_id
        )

        if (
            len(running_jobs)
            >= self.policy
            .max_concurrent_jobs_per_resource_domain
        ):
            raise ValueError(
                "resource domain has reached compaction concurrency policy"
            )

        self.advance_epoch()

        job = CompactionJob(
            job_id=_new_id(
                "compaction-job"
            ),
            merge_plan_id=merge_plan_id,
            partition_id=plan.partition_id,
            family_id=plan.family_id,
            source_segment_ids=(
                plan.source_segment_ids
            ),
            target_level=plan.target_level,
            resource_domain_id=resource_domain_id,
            state=CompactionJobState.QUEUED,
            owner_epoch=self._epoch,
            retry_count=0,
            max_retries=(
                self.policy.max_job_retries
            ),
        )

        self.backend.put_job(
            job
        )

        return job

    # ------------------------------------------------------------------
    # Start job
    # ------------------------------------------------------------------

    def start_job(
        self,
        job_id: str,
    ) -> CompactionJob:
        job = self.backend.get_job(
            job_id
        )

        if job is None:
            raise KeyError(
                f"compaction job not found: {job_id}"
            )

        profile = self.backend.get_resource_profile(
            job.resource_domain_id
        )

        if profile is None:
            raise KeyError(
                "resource profile not found for job"
            )

        if self._resource_should_pause(
            profile
        ):
            paused = CompactionJob(
                job_id=job.job_id,
                merge_plan_id=job.merge_plan_id,
                partition_id=job.partition_id,
                family_id=job.family_id,
                source_segment_ids=(
                    job.source_segment_ids
                ),
                target_level=job.target_level,
                resource_domain_id=(
                    job.resource_domain_id
                ),
                state=CompactionJobState.PAUSED,
                owner_epoch=job.owner_epoch,
                retry_count=job.retry_count,
                max_retries=job.max_retries,
                started_at=job.started_at,
                completed_at=job.completed_at,
                updated_at=_now(),
            )

            self.backend.put_job(
                paused
            )

            self._emit_event(
                event_type=(
                    LineageEventType.COMPACTION_PAUSED
                ),
                partition_id=job.partition_id,
                job_id=job.job_id,
                epoch=self._epoch,
                payload={
                    "resource_domain_id": (
                        job.resource_domain_id
                    ),
                },
            )

            return paused

        self.advance_epoch()

        running = CompactionJob(
            job_id=job.job_id,
            merge_plan_id=job.merge_plan_id,
            partition_id=job.partition_id,
            family_id=job.family_id,
            source_segment_ids=(
                job.source_segment_ids
            ),
            target_level=job.target_level,
            resource_domain_id=(
                job.resource_domain_id
            ),
            state=CompactionJobState.RUNNING,
            owner_epoch=self._epoch,
            retry_count=job.retry_count,
            max_retries=job.max_retries,
            started_at=(
                job.started_at
                if job.started_at is not None
                else _now()
            ),
            completed_at=None,
            updated_at=_now(),
        )

        self.backend.put_job(
            running
        )

        self._emit_event(
            event_type=(
                LineageEventType.COMPACTION_STARTED
            ),
            partition_id=job.partition_id,
            job_id=job.job_id,
            epoch=self._epoch,
            payload={
                "merge_plan_id": (
                    job.merge_plan_id
                ),
                "target_level": (
                    job.target_level.value
                ),
            },
        )

        return running

    # ------------------------------------------------------------------
    # Pause/resume
    # ------------------------------------------------------------------

    def pause_job(
        self,
        job_id: str,
    ) -> CompactionJob:
        job = self.backend.get_job(
            job_id
        )

        if job is None:
            raise KeyError(
                f"compaction job not found: {job_id}"
            )

        self.advance_epoch()

        paused = CompactionJob(
            job_id=job.job_id,
            merge_plan_id=job.merge_plan_id,
            partition_id=job.partition_id,
            family_id=job.family_id,
            source_segment_ids=(
                job.source_segment_ids
            ),
            target_level=job.target_level,
            resource_domain_id=(
                job.resource_domain_id
            ),
            state=CompactionJobState.PAUSED,
            owner_epoch=job.owner_epoch,
            retry_count=job.retry_count,
            max_retries=job.max_retries,
            started_at=job.started_at,
            completed_at=job.completed_at,
            updated_at=_now(),
        )

        self.backend.put_job(
            paused
        )

        self._emit_event(
            event_type=(
                LineageEventType.COMPACTION_PAUSED
            ),
            partition_id=job.partition_id,
            job_id=job.job_id,
            epoch=self._epoch,
            payload={},
        )

        return paused

    def resume_job(
        self,
        job_id: str,
    ) -> CompactionJob:
        job = self.backend.get_job(
            job_id
        )

        if job is None:
            raise KeyError(
                f"compaction job not found: {job_id}"
            )

        if job.state not in {
            CompactionJobState.PAUSED,
            CompactionJobState.QUEUED,
        }:
            raise ValueError(
                "only paused or queued jobs can be resumed"
            )

        self.advance_epoch()

        resumed = CompactionJob(
            job_id=job.job_id,
            merge_plan_id=job.merge_plan_id,
            partition_id=job.partition_id,
            family_id=job.family_id,
            source_segment_ids=(
                job.source_segment_ids
            ),
            target_level=job.target_level,
            resource_domain_id=(
                job.resource_domain_id
            ),
            state=CompactionJobState.RUNNING,
            owner_epoch=self._epoch,
            retry_count=job.retry_count,
            max_retries=job.max_retries,
            started_at=(
                job.started_at
                if job.started_at is not None
                else _now()
            ),
            completed_at=None,
            updated_at=_now(),
        )

        self.backend.put_job(
            resumed
        )

        return resumed

    # ------------------------------------------------------------------
    # Generation building
    # ------------------------------------------------------------------

    def build_output_generation(
        self,
        job_id: str,
        output_segment_id: str,
        output_logical_bytes: int,
        output_documents: int,
        output_postings: int,
        output_fingerprint: str,
    ) -> MergeGeneration:
        job = self.backend.get_job(
            job_id
        )

        if job is None:
            raise KeyError(
                f"compaction job not found: {job_id}"
            )

        if job.state != CompactionJobState.RUNNING:
            raise ValueError(
                "job must be running before output generation is built"
            )

        if output_logical_bytes < 0:
            raise ValueError(
                "output_logical_bytes must be non-negative"
            )

        if output_documents < 0:
            raise ValueError(
                "output_documents must be non-negative"
            )

        if output_postings < 0:
            raise ValueError(
                "output_postings must be non-negative"
            )

        if not output_fingerprint:
            raise ValueError(
                "output_fingerprint must not be empty"
            )

        self.advance_epoch()

        generation = MergeGeneration(
            generation_id=_new_id(
                "merge-generation"
            ),
            partition_id=job.partition_id,
            family_id=job.family_id,
            target_level=job.target_level,
            source_segment_ids=(
                job.source_segment_ids
            ),
            output_segment_ids=(
                output_segment_id,
            ),
            state=MergeGenerationState.BUILDING,
            generation_epoch=self._epoch,
            output_logical_bytes=(
                output_logical_bytes
            ),
            output_documents=(
                output_documents
            ),
            output_postings=(
                output_postings
            ),
            output_fingerprint=(
                output_fingerprint
            ),
        )

        self.backend.put_generation(
            generation
        )

        return generation

    # ------------------------------------------------------------------
    # Atomic publication
    # ------------------------------------------------------------------

    def publish_generation(
        self,
        generation_id: str,
    ) -> PartitionSegmentManifest:
        generation = self.backend.get_generation(
            generation_id
        )

        if generation is None:
            raise KeyError(
                f"generation not found: {generation_id}"
            )

        if generation.state not in {
            MergeGenerationState.BUILDING,
            MergeGenerationState.PLANNED,
        }:
            raise ValueError(
                "generation cannot be published from current state"
            )

        for segment_id in (
            generation.source_segment_ids
        ):
            source = self.backend.get_segment(
                segment_id
            )

            if source is None:
                raise KeyError(
                    f"source segment not found: {segment_id}"
                )

            if source.lifecycle_state not in {
                SegmentLifecycleState.ACTIVE,
                SegmentLifecycleState.MERGING,
            }:
                raise ValueError(
                    "source segment is not publish-compatible"
                )

        self.advance_epoch()

        output_records: List[
            SegmentRecord
        ] = []

        for output_id in (
            generation.output_segment_ids
        ):
            identity = SegmentIdentity(
                segment_id=output_id,
                partition_id=(
                    generation.partition_id
                ),
                generation=(
                    generation.generation_epoch
                ),
                level=(
                    generation.target_level
                ),
                document_count=(
                    generation.output_documents
                ),
                posting_count=(
                    generation.output_postings
                ),
                vocabulary_count=0,
                logical_bytes=(
                    generation.output_logical_bytes
                ),
                fingerprint=(
                    generation.output_fingerprint
                ),
            )

            output = SegmentRecord(
                identity=identity,
                lifecycle_state=(
                    SegmentLifecycleState.ACTIVE
                ),
                source_segment_ids=(
                    generation.source_segment_ids
                ),
                family_id=generation.family_id,
                created_epoch=(
                    generation.generation_epoch
                ),
                published_epoch=self._epoch,
                storage_region_ids=(),
            )

            self.backend.put_segment(
                output
            )

            output_records.append(
                output
            )

        old_manifest = self.backend.get_manifest(
            generation.partition_id
        )

        old_active_ids = (
            list(
                old_manifest.active_segment_ids
            )
            if old_manifest is not None
            else []
        )

        remaining_ids = [
            segment_id
            for segment_id
            in old_active_ids
            if segment_id
            not in generation.source_segment_ids
        ]

        new_active_ids = (
            remaining_ids
            + list(
                generation.output_segment_ids
            )
        )

        old_generation_ids = (
            list(
                old_manifest.generation_ids
            )
            if old_manifest is not None
            else []
        )

        new_generation_ids = (
            old_generation_ids
            + [generation.generation_id]
        )

        manifest = PartitionSegmentManifest(
            manifest_id=_new_id(
                "partition-segment-manifest"
            ),
            partition_id=(
                generation.partition_id
            ),
            manifest_epoch=self._epoch,
            active_segment_ids=tuple(
                new_active_ids
            ),
            generation_ids=tuple(
                new_generation_ids
            ),
        )

        self.backend.put_manifest(
            manifest
        )

        published_generation = MergeGeneration(
            generation_id=generation.generation_id,
            partition_id=generation.partition_id,
            family_id=generation.family_id,
            target_level=generation.target_level,
            source_segment_ids=(
                generation.source_segment_ids
            ),
            output_segment_ids=(
                generation.output_segment_ids
            ),
            state=MergeGenerationState.PUBLISHED,
            generation_epoch=(
                generation.generation_epoch
            ),
            output_logical_bytes=(
                generation.output_logical_bytes
            ),
            output_documents=(
                generation.output_documents
            ),
            output_postings=(
                generation.output_postings
            ),
            output_fingerprint=(
                generation.output_fingerprint
            ),
            created_at=generation.created_at,
            updated_at=_now(),
        )

        self.backend.put_generation(
            published_generation
        )

        self._emit_event(
            event_type=(
                LineageEventType.GENERATION_PUBLISHED
            ),
            partition_id=(
                generation.partition_id
            ),
            generation_id=(
                generation.generation_id
            ),
            epoch=self._epoch,
            payload={
                "output_segment_ids": list(
                    generation.output_segment_ids
                ),
            },
        )

        self._emit_event(
            event_type=(
                LineageEventType.MANIFEST_SWAPPED
            ),
            partition_id=(
                generation.partition_id
            ),
            generation_id=(
                generation.generation_id
            ),
            epoch=self._epoch,
            payload={
                "manifest_id": manifest.manifest_id,
                "active_segment_count": len(
                    manifest.active_segment_ids
                ),
            },
        )

        return manifest

    # ------------------------------------------------------------------
    # Source retirement
    # ------------------------------------------------------------------

    def begin_source_retirement(
        self,
        generation_id: str,
    ) -> Tuple[
        SegmentRetirementRecord,
        ...,
    ]:
        generation = self.backend.get_generation(
            generation_id
        )

        if generation is None:
            raise KeyError(
                f"generation not found: {generation_id}"
            )

        if generation.state != (
            MergeGenerationState.PUBLISHED
        ):
            raise ValueError(
                "only published generations can retire sources"
            )

        self.advance_epoch()

        records: List[
            SegmentRetirementRecord
        ] = []

        minimum_reader_epoch = (
            self._epoch
            - self.policy.reader_grace_epochs
        )

        for segment_id in (
            generation.source_segment_ids
        ):
            source = self.backend.get_segment(
                segment_id
            )

            if source is None:
                continue

            retiring = SegmentRecord(
                identity=source.identity,
                lifecycle_state=(
                    SegmentLifecycleState.RETIRING
                ),
                source_segment_ids=(
                    source.source_segment_ids
                ),
                family_id=source.family_id,
                created_epoch=source.created_epoch,
                published_epoch=source.published_epoch,
                retired_epoch=None,
                storage_region_ids=(
                    source.storage_region_ids
                ),
                created_at=source.created_at,
                updated_at=_now(),
            )

            self.backend.put_segment(
                retiring
            )

            retirement = SegmentRetirementRecord(
                retirement_id=_new_id(
                    "segment-retirement"
                ),
                segment_id=segment_id,
                partition_id=(
                    generation.partition_id
                ),
                retirement_epoch=self._epoch,
                minimum_reader_epoch=(
                    minimum_reader_epoch
                ),
                safe_to_delete=False,
                state=(
                    SegmentLifecycleState.RETIRING
                ),
            )

            self.backend.put_retirement(
                retirement
            )

            records.append(
                retirement
            )

        self._emit_event(
            event_type=(
                LineageEventType.SOURCE_RETIREMENT_STARTED
            ),
            partition_id=(
                generation.partition_id
            ),
            generation_id=generation_id,
            epoch=self._epoch,
            payload={
                "source_segment_count": len(
                    records
                ),
                "minimum_reader_epoch": (
                    minimum_reader_epoch
                ),
            },
        )

        return tuple(records)

    # ------------------------------------------------------------------
    # Safe retirement
    # ------------------------------------------------------------------

    def finalize_retirement(
        self,
        retirement_id: str,
        current_reader_epoch: int,
    ) -> SegmentRetirementRecord:
        if current_reader_epoch < 0:
            raise ValueError(
                "current_reader_epoch must be non-negative"
            )

        if not isinstance(
            self.backend,
            InMemoryMassiveSegmentCompactionMetadata,
        ):
            raise ValueError(
                "reference backend required for retirement lookup"
            )

        retirement = (
            self.backend.retirements.get(
                retirement_id
            )
        )

        if retirement is None:
            raise KeyError(
                f"retirement record not found: {retirement_id}"
            )

        if (
            current_reader_epoch
            < retirement.minimum_reader_epoch
        ):
            return retirement

        segment = self.backend.get_segment(
            retirement.segment_id
        )

        if segment is None:
            return retirement

        self.advance_epoch()

        retired = SegmentRecord(
            identity=segment.identity,
            lifecycle_state=(
                SegmentLifecycleState.RETIRED
            ),
            source_segment_ids=(
                segment.source_segment_ids
            ),
            family_id=segment.family_id,
            created_epoch=segment.created_epoch,
            published_epoch=segment.published_epoch,
            retired_epoch=self._epoch,
            storage_region_ids=(
                segment.storage_region_ids
            ),
            created_at=segment.created_at,
            updated_at=_now(),
        )

        self.backend.put_segment(
            retired
        )

        completed = SegmentRetirementRecord(
            retirement_id=retirement.retirement_id,
            segment_id=retirement.segment_id,
            partition_id=retirement.partition_id,
            retirement_epoch=(
                retirement.retirement_epoch
            ),
            minimum_reader_epoch=(
                retirement.minimum_reader_epoch
            ),
            safe_to_delete=True,
            state=SegmentLifecycleState.RETIRED,
            created_at=retirement.created_at,
        )

        self.backend.put_retirement(
            completed
        )

        self._emit_event(
            event_type=(
                LineageEventType.SOURCE_RETIRED
            ),
            partition_id=(
                retirement.partition_id
            ),
            segment_id=(
                retirement.segment_id
            ),
            epoch=self._epoch,
            payload={
                "retirement_id": (
                    retirement.retirement_id
                ),
                "safe_to_delete": True,
            },
        )

        return completed

    # ------------------------------------------------------------------
    # Job completion
    # ------------------------------------------------------------------

    def complete_job(
        self,
        job_id: str,
    ) -> CompactionJob:
        job = self.backend.get_job(
            job_id
        )

        if job is None:
            raise KeyError(
                f"compaction job not found: {job_id}"
            )

        self.advance_epoch()

        completed = CompactionJob(
            job_id=job.job_id,
            merge_plan_id=job.merge_plan_id,
            partition_id=job.partition_id,
            family_id=job.family_id,
            source_segment_ids=(
                job.source_segment_ids
            ),
            target_level=job.target_level,
            resource_domain_id=(
                job.resource_domain_id
            ),
            state=CompactionJobState.COMPLETED,
            owner_epoch=job.owner_epoch,
            retry_count=job.retry_count,
            max_retries=job.max_retries,
            started_at=job.started_at,
            completed_at=_now(),
            updated_at=_now(),
        )

        self.backend.put_job(
            completed
        )

        return completed

    # ------------------------------------------------------------------
    # Job failure / retry
    # ------------------------------------------------------------------

    def fail_job(
        self,
        job_id: str,
        retry: bool = True,
    ) -> CompactionJob:
        job = self.backend.get_job(
            job_id
        )

        if job is None:
            raise KeyError(
                f"compaction job not found: {job_id}"
            )

        self.advance_epoch()

        next_retry = (
            job.retry_count + 1
        )

        if (
            retry
            and next_retry
            <= job.max_retries
        ):
            state = (
                CompactionJobState.QUEUED
            )
        else:
            state = (
                CompactionJobState.FAILED
            )

        failed = CompactionJob(
            job_id=job.job_id,
            merge_plan_id=job.merge_plan_id,
            partition_id=job.partition_id,
            family_id=job.family_id,
            source_segment_ids=(
                job.source_segment_ids
            ),
            target_level=job.target_level,
            resource_domain_id=(
                job.resource_domain_id
            ),
            state=state,
            owner_epoch=job.owner_epoch,
            retry_count=next_retry,
            max_retries=job.max_retries,
            started_at=job.started_at,
            completed_at=(
                _now()
                if state
                == CompactionJobState.FAILED
                else None
            ),
            updated_at=_now(),
        )

        self.backend.put_job(
            failed
        )

        self._emit_event(
            event_type=(
                LineageEventType.COMPACTION_FAILED
            ),
            partition_id=job.partition_id,
            job_id=job.job_id,
            epoch=self._epoch,
            payload={
                "retry_count": next_retry,
                "retry_scheduled": (
                    state
                    == CompactionJobState.QUEUED
                ),
            },
        )

        return failed

    # ------------------------------------------------------------------
    # Epoch fencing
    # ------------------------------------------------------------------

    def fence_job(
        self,
        job_id: str,
    ) -> CompactionJob:
        job = self.backend.get_job(
            job_id
        )

        if job is None:
            raise KeyError(
                f"compaction job not found: {job_id}"
            )

        self.advance_epoch()

        fenced = CompactionJob(
            job_id=job.job_id,
            merge_plan_id=job.merge_plan_id,
            partition_id=job.partition_id,
            family_id=job.family_id,
            source_segment_ids=(
                job.source_segment_ids
            ),
            target_level=job.target_level,
            resource_domain_id=(
                job.resource_domain_id
            ),
            state=CompactionJobState.FENCED,
            owner_epoch=job.owner_epoch,
            retry_count=job.retry_count,
            max_retries=job.max_retries,
            started_at=job.started_at,
            completed_at=None,
            updated_at=_now(),
        )

        self.backend.put_job(
            fenced
        )

        self._emit_event(
            event_type=(
                LineageEventType.COMPACTION_FENCED
            ),
            partition_id=job.partition_id,
            job_id=job.job_id,
            epoch=self._epoch,
            payload={
                "owner_epoch": (
                    job.owner_epoch
                ),
                "fencing_epoch": (
                    self._epoch
                ),
            },
        )

        return fenced

    # ------------------------------------------------------------------
    # Checkpoint
    # ------------------------------------------------------------------

    def checkpoint(
        self,
    ) -> CompactionCheckpoint:
        self.advance_epoch()

        if isinstance(
            self.backend,
            InMemoryMassiveSegmentCompactionMetadata,
        ):
            segments = list(
                self.backend.segments.values()
            )

            families = list(
                self.backend.families.values()
            )

            jobs = list(
                self.backend.jobs.values()
            )

            generations = list(
                self.backend.generations.values()
            )

            retirements = list(
                self.backend.retirements.values()
            )

            logical_bytes = sum(
                segment.identity.logical_bytes
                for segment in segments
                if segment.lifecycle_state
                != SegmentLifecycleState.DELETED
            )

            output_bytes = sum(
                generation.output_logical_bytes
                for generation
                in generations
                if generation.state
                == MergeGenerationState.PUBLISHED
            )

            checkpoint = CompactionCheckpoint(
                checkpoint_id=_new_id(
                    "compaction-checkpoint"
                ),
                epoch=self._epoch,
                partition_count=len(
                    {
                        segment.identity.partition_id
                        for segment
                        in segments
                    }
                ),
                segment_count=len(
                    segments
                ),
                family_count=len(
                    families
                ),
                active_job_count=len(
                    [
                        job
                        for job
                        in jobs
                        if job.state
                        in {
                            CompactionJobState.QUEUED,
                            CompactionJobState.RUNNING,
                            CompactionJobState.PAUSED,
                            CompactionJobState.PUBLISHING,
                        }
                    ]
                ),
                completed_job_count=len(
                    [
                        job
                        for job
                        in jobs
                        if job.state
                        == CompactionJobState.COMPLETED
                    ]
                ),
                failed_job_count=len(
                    [
                        job
                        for job
                        in jobs
                        if job.state
                        == CompactionJobState.FAILED
                    ]
                ),
                logical_bytes=logical_bytes,
                output_bytes=output_bytes,
                active_generation_count=len(
                    [
                        generation
                        for generation
                        in generations
                        if generation.state
                        == MergeGenerationState.PUBLISHED
                    ]
                ),
                retiring_segment_count=len(
                    [
                        retirement
                        for retirement
                        in retirements
                        if not retirement.safe_to_delete
                    ]
                ),
            )

        else:
            checkpoint = CompactionCheckpoint(
                checkpoint_id=_new_id(
                    "compaction-checkpoint"
                ),
                epoch=self._epoch,
                partition_count=0,
                segment_count=0,
                family_count=0,
                active_job_count=0,
                completed_job_count=0,
                failed_job_count=0,
                logical_bytes=0,
                output_bytes=0,
                active_generation_count=0,
                retiring_segment_count=0,
            )

        self.backend.save_checkpoint(
            checkpoint
        )

        self._emit_event(
            event_type=(
                LineageEventType.CHECKPOINT_CREATED
            ),
            epoch=self._epoch,
            payload={
                "segment_count": (
                    checkpoint.segment_count
                ),
                "family_count": (
                    checkpoint.family_count
                ),
                "active_job_count": (
                    checkpoint.active_job_count
                ),
                "active_generation_count": (
                    checkpoint.active_generation_count
                ),
            },
        )

        return checkpoint

    # ------------------------------------------------------------------
    # Capacity
    # ------------------------------------------------------------------

    def capacity(
        self,
    ) -> CompactionCapacity:
        if isinstance(
            self.backend,
            InMemoryMassiveSegmentCompactionMetadata,
        ):
            segments = list(
                self.backend.segments.values()
            )

            families = list(
                self.backend.families.values()
            )

            jobs = list(
                self.backend.jobs.values()
            )

            generations = list(
                self.backend.generations.values()
            )

            retirements = list(
                self.backend.retirements.values()
            )

            return CompactionCapacity(
                partition_count=len(
                    {
                        segment.identity.partition_id
                        for segment
                        in segments
                    }
                ),
                segment_count=len(
                    segments
                ),
                family_count=len(
                    families
                ),
                active_jobs=len(
                    [
                        job
                        for job
                        in jobs
                        if job.state
                        in {
                            CompactionJobState.QUEUED,
                            CompactionJobState.RUNNING,
                            CompactionJobState.PAUSED,
                            CompactionJobState.PUBLISHING,
                        }
                    ]
                ),
                running_jobs=len(
                    [
                        job
                        for job
                        in jobs
                        if job.state
                        == CompactionJobState.RUNNING
                    ]
                ),
                failed_jobs=len(
                    [
                        job
                        for job
                        in jobs
                        if job.state
                        == CompactionJobState.FAILED
                    ]
                ),
                logical_bytes=sum(
                    segment.identity.logical_bytes
                    for segment
                    in segments
                    if segment.lifecycle_state
                    != SegmentLifecycleState.DELETED
                ),
                output_bytes=sum(
                    generation.output_logical_bytes
                    for generation
                    in generations
                    if generation.state
                    == MergeGenerationState.PUBLISHED
                ),
                active_generations=len(
                    [
                        generation
                        for generation
                        in generations
                        if generation.state
                        == MergeGenerationState.PUBLISHED
                    ]
                ),
                retiring_segments=len(
                    [
                        retirement
                        for retirement
                        in retirements
                        if not retirement.safe_to_delete
                    ]
                ),
            )

        return CompactionCapacity(
            partition_count=0,
            segment_count=0,
            family_count=0,
            active_jobs=0,
            running_jobs=0,
            failed_jobs=0,
            logical_bytes=0,
            output_bytes=0,
            active_generations=0,
            retiring_segments=0,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _next_level(
        self,
        current: CompactionLevel,
    ) -> CompactionLevel:
        levels = list(
            CompactionLevel
        )

        index = levels.index(
            current
        )

        if index >= len(levels) - 1:
            return current

        return levels[index + 1]

    def _find_merge_plan(
        self,
        merge_plan_id: str,
    ) -> Optional[MergePlan]:
        if not isinstance(
            self.backend,
            InMemoryMassiveSegmentCompactionMetadata,
        ):
            return None

        return self.backend.merge_plans.get(
            merge_plan_id
        )

    def _running_jobs_for_resource(
        self,
        resource_domain_id: str,
    ) -> List[CompactionJob]:
        if not isinstance(
            self.backend,
            InMemoryMassiveSegmentCompactionMetadata,
        ):
            return []

        return [
            job
            for job
            in self.backend.jobs.values()
            if (
                job.resource_domain_id
                == resource_domain_id
                and job.state
                == CompactionJobState.RUNNING
            )
        ]

    def _resource_should_pause(
        self,
        profile: CompactionResourceProfile,
    ) -> bool:
        cpu_ratio = (
            profile.cpu_used
            / profile.cpu_capacity
        )

        memory_ratio = (
            profile.memory_used_bytes
            / profile.memory_capacity_bytes
        )

        io_ratio = (
            profile.io_used_bytes_per_second
            / profile.io_capacity_bytes_per_second
        )

        storage_ratio = (
            profile.storage_used_bytes
            / profile.storage_capacity_bytes
        )

        return (
            cpu_ratio
            >= self.policy.cpu_pressure_pause_threshold
            or memory_ratio
            >= self.policy.memory_pressure_pause_threshold
            or io_ratio
            >= self.policy.io_pressure_pause_threshold
            or storage_ratio
            >= self.policy.storage_pressure_pause_threshold
        )

    def _emit_event(
        self,
        event_type: LineageEventType,
        partition_id: Optional[str] = None,
        segment_id: Optional[str] = None,
        job_id: Optional[str] = None,
        generation_id: Optional[str] = None,
        epoch: Optional[int] = None,
        payload: Optional[
            Mapping[str, object]
        ] = None,
    ) -> CompactionLineageEvent:
        event = CompactionLineageEvent(
            event_id=_new_id(
                "compaction-event"
            ),
            event_type=event_type,
            partition_id=partition_id,
            segment_id=segment_id,
            job_id=job_id,
            generation_id=generation_id,
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
                "immutable_segments",
                "segment_discovery",
                "segment_families",
                "merge_candidate_planning",
                "multi_level_compaction",
                "resource_aware_scheduling",
                "merge_execution",
                "immutable_output_generation",
                "atomic_manifest_replacement",
                "old_generation_retirement",
                "recovery_safe_lineage",
            ],
            "compaction_levels": [
                level.value
                for level
                in CompactionLevel
            ],
            "segment_states": [
                state.value
                for state
                in SegmentLifecycleState
            ],
            "job_states": [
                state.value
                for state
                in CompactionJobState
            ],
            "resource_pressure_states": [
                state.value
                for state
                in ResourcePressureState
            ],
            "triggers": [
                trigger.value
                for trigger
                in CompactionTrigger
            ],
            "immutable_sources": True,
            "immutable_outputs": True,
            "atomic_manifest_swap": True,
            "lineage_tracking": True,
            "resource_aware_scheduling": True,
            "epoch_fencing": True,
            "retryable_jobs": True,
            "safe_source_retirement": True,
            "checkpointing": True,
            "horizontal_scaling": True,
            "replaceable_backend": True,
            "forbidden_single_global_assumptions": list(
                self.FORBIDDEN_SINGLE_GLOBAL_ASSUMPTIONS
            ),
        }


# ============================================================================
# Stable aliases
# ============================================================================

MassiveSegmentCompaction = (
    MassiveSegmentCompactionArchitecture
)

GlobalSegmentCompaction = (
    MassiveSegmentCompactionArchitecture
)

Phase10_5MassiveSegmentCompaction = (
    MassiveSegmentCompactionArchitecture
)


# ============================================================================
# Public contract
# ============================================================================

__all__ = [
    "SegmentLifecycleState",
    "SegmentFamilyState",
    "CompactionLevel",
    "CompactionJobState",
    "MergeGenerationState",
    "ResourcePressureState",
    "CompactionTrigger",
    "LineageEventType",
    "SegmentIdentity",
    "SegmentRecord",
    "SegmentFamily",
    "CompactionResourceProfile",
    "MergePlan",
    "CompactionJob",
    "MergeGeneration",
    "PartitionSegmentManifest",
    "SegmentRetirementRecord",
    "CompactionLineageEvent",
    "CompactionCheckpoint",
    "CompactionCapacity",
    "MassiveSegmentCompactionBackend",
    "InMemoryMassiveSegmentCompactionMetadata",
    "MassiveCompactionPolicy",
    "MassiveSegmentCompactionArchitecture",
    "MassiveSegmentCompaction",
    "GlobalSegmentCompaction",
    "Phase10_5MassiveSegmentCompaction",
]
