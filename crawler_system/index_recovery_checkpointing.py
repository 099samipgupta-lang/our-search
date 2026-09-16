"""
OUR SEARCH
Phase 10.7 — Index Recovery + Checkpointing

Version:
    index-recovery-checkpointing.v1

Purpose:
    Global recovery and checkpoint architecture for the OUR SEARCH
    distributed index.

Architecture:

    GLOBAL INDEX
        ↓
    CHECKPOINT FABRIC
        ↓
    INDEX STATE SNAPSHOTS
        ↓
    RECOVERY METADATA / JOURNAL
        ↓
    FAILURE DETECTION
        ↓
    PARTITION RECOVERY
        ↓
    SEGMENT RECOVERY
        ↓
    REPLICA RECOVERY
        ↓
    ROUTING REVALIDATION
        ↓
    ATOMIC RESTORE

Scale target:

    Billions → potentially trillions of publicly accessible Web resources.

This module is an architecture/control-plane layer.

It does not replace the underlying distributed storage, segment,
replication, or routing systems. It coordinates recovery across them.

Design principles:

    - immutable recovery artifacts
    - versioned checkpoints
    - partition-scoped recovery
    - segment lineage awareness
    - replica-aware recovery
    - epoch fencing
    - recovery generations
    - atomic restore publication
    - checkpoint validation
    - corruption detection
    - partial recovery
    - incremental recovery
    - replay-safe recovery
    - failure-domain awareness
    - stale-state rejection
    - recovery checkpoints
    - replaceable backend
    - horizontal scaling

No Google technology or Google dependency is used.

No tests or benchmarks are performed by this module.
"""

from __future__ import annotations

import hashlib
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Protocol, Sequence


ARCHITECTURE_VERSION = "index-recovery-checkpointing.v1"

GLOBAL_SCALE_TARGET = "billions_to_trillions_of_public_web_resources"
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

class CheckpointState(str, Enum):
    BUILDING = "building"
    SEALED = "sealed"
    VALIDATING = "validating"
    VALID = "valid"
    INVALID = "invalid"
    RETIRED = "retired"


class RecoveryState(str, Enum):
    PLANNED = "planned"
    PREPARING = "preparing"
    VALIDATING = "validating"
    REPLAYING = "replaying"
    RESTORING = "restoring"
    REVALIDATING = "revalidating"
    PUBLISHING = "publishing"
    COMPLETED = "completed"
    FAILED = "failed"
    FENCED = "fenced"
    ABORTED = "aborted"


class RecoveryScope(str, Enum):
    PARTITION = "partition"
    SEGMENT = "segment"
    REPLICA = "replica"
    REGION = "region"
    ZONE = "zone"
    GLOBAL = "global"


class RecoveryMode(str, Enum):
    FULL = "full"
    INCREMENTAL = "incremental"
    REPLAY = "replay"
    REPLICA_RESTORE = "replica_restore"
    CHECKPOINT_RESTORE = "checkpoint_restore"


class FailureDomain(str, Enum):
    NODE = "node"
    ZONE = "zone"
    REGION = "region"
    STORAGE = "storage"
    METADATA = "metadata"
    ROUTING = "routing"
    SEGMENT = "segment"
    UNKNOWN = "unknown"


class ArtifactState(str, Enum):
    AVAILABLE = "available"
    COPYING = "copying"
    CORRUPTED = "corrupted"
    MISSING = "missing"
    RETIRED = "retired"


class RecoveryArtifactType(str, Enum):
    INDEX_CHECKPOINT = "index_checkpoint"
    PARTITION_CHECKPOINT = "partition_checkpoint"
    SEGMENT_CHECKPOINT = "segment_checkpoint"
    ROUTING_CHECKPOINT = "routing_checkpoint"
    REPLICA_CHECKPOINT = "replica_checkpoint"
    JOURNAL_POSITION = "journal_position"


class JournalRecordType(str, Enum):
    INDEX_MUTATION = "index_mutation"
    SEGMENT_PUBLISHED = "segment_published"
    SEGMENT_RETIRED = "segment_retired"
    MANIFEST_SWAPPED = "manifest_swapped"
    PARTITION_MOVED = "partition_moved"
    REPLICA_CHANGED = "replica_changed"
    ROUTING_EPOCH_ADVANCED = "routing_epoch_advanced"
    CHECKPOINT_CREATED = "checkpoint_created"
    CHECKPOINT_SEALED = "checkpoint_sealed"


class RecoveryEventType(str, Enum):
    CHECKPOINT_CREATED = "checkpoint_created"
    CHECKPOINT_SEALED = "checkpoint_sealed"
    CHECKPOINT_VALIDATED = "checkpoint_validated"
    CHECKPOINT_INVALIDATED = "checkpoint_invalidated"

    FAILURE_DETECTED = "failure_detected"

    RECOVERY_PLANNED = "recovery_planned"
    RECOVERY_STARTED = "recovery_started"
    RECOVERY_VALIDATION_STARTED = "recovery_validation_started"
    RECOVERY_REPLAY_STARTED = "recovery_replay_started"
    RECOVERY_RESTORE_STARTED = "recovery_restore_started"

    ROUTING_REVALIDATION_STARTED = (
        "routing_revalidation_started"
    )

    RECOVERY_PUBLISHING = "recovery_publishing"
    RECOVERY_COMPLETED = "recovery_completed"
    RECOVERY_FAILED = "recovery_failed"
    RECOVERY_FENCED = "recovery_fenced"

    CHECKPOINT_RETIREMENT = "checkpoint_retirement"


# ---------------------------------------------------------------------------
# Checkpoint identity
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RecoveryCheckpointIdentity:
    checkpoint_id: str
    namespace: str

    checkpoint_generation: int
    routing_epoch: int

    index_epoch: int

    partition_id: Optional[str] = None

    def stable_key(self) -> str:
        return "|".join(
            [
                self.namespace,
                self.checkpoint_id,
                str(self.checkpoint_generation),
                str(self.routing_epoch),
                str(self.index_epoch),
                self.partition_id or "",
            ]
        )


# ---------------------------------------------------------------------------
# Journal position
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class JournalPosition:
    journal_id: str

    sequence: int

    partition_id: Optional[str]

    index_epoch: int

    checksum: str

    timestamp: float = field(default_factory=_now)


# ---------------------------------------------------------------------------
# Checkpoint artifact
# ---------------------------------------------------------------------------

@dataclass
class RecoveryArtifact:
    artifact_id: str

    artifact_type: RecoveryArtifactType

    checkpoint_id: str

    partition_id: Optional[str]

    segment_ids: List[str] = field(default_factory=list)

    replica_ids: List[str] = field(default_factory=list)

    storage_location: Optional[str] = None

    content_hash: str = ""

    byte_size: int = 0

    state: ArtifactState = ArtifactState.AVAILABLE

    created_at: float = field(default_factory=_now)

    metadata: Dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Checkpoint
# ---------------------------------------------------------------------------

@dataclass
class IndexCheckpoint:
    """
    Immutable logical checkpoint describing a recoverable index state.

    The checkpoint references immutable artifacts rather than requiring
    the entire index to exist inside one physical file/database.
    """

    identity: RecoveryCheckpointIdentity

    state: CheckpointState

    recovery_mode: RecoveryMode

    journal_position: Optional[JournalPosition]

    artifacts: Dict[str, RecoveryArtifact] = field(
        default_factory=dict
    )

    partition_ids: List[str] = field(
        default_factory=list
    )

    segment_ids: List[str] = field(
        default_factory=list
    )

    manifest_versions: Dict[str, int] = field(
        default_factory=dict
    )

    created_at: float = field(default_factory=_now)
    sealed_at: Optional[float] = None
    validated_at: Optional[float] = None

    checksum: str = ""

    parent_checkpoint_id: Optional[str] = None

    metadata: Dict[str, str] = field(
        default_factory=dict
    )

    def calculate_checksum(self) -> str:

        artifact_entries = []

        for artifact_id in sorted(self.artifacts):

            artifact = self.artifacts[artifact_id]

            artifact_entries.append(
                "|".join(
                    [
                        artifact.artifact_id,
                        artifact.artifact_type.value,
                        artifact.partition_id or "",
                        ",".join(
                            sorted(artifact.segment_ids)
                        ),
                        ",".join(
                            sorted(artifact.replica_ids)
                        ),
                        artifact.content_hash,
                        str(artifact.byte_size),
                        artifact.state.value,
                    ]
                )
            )

        journal = ""

        if self.journal_position is not None:
            journal = "|".join(
                [
                    self.journal_position.journal_id,
                    str(self.journal_position.sequence),
                    self.journal_position.partition_id or "",
                    str(self.journal_position.index_epoch),
                    self.journal_position.checksum,
                ]
            )

        return _hash(
            "||".join(
                [
                    self.identity.stable_key(),
                    self.state.value,
                    self.recovery_mode.value,
                    journal,
                    ",".join(
                        sorted(self.partition_ids)
                    ),
                    ",".join(
                        sorted(self.segment_ids)
                    ),
                    ",".join(
                        f"{key}:{value}"
                        for key, value
                        in sorted(
                            self.manifest_versions.items()
                        )
                    ),
                    "~~".join(artifact_entries),
                ]
            )
        )


# ---------------------------------------------------------------------------
# Journal record
# ---------------------------------------------------------------------------

@dataclass
class RecoveryJournalRecord:
    record_id: str

    record_type: JournalRecordType

    namespace: str

    sequence: int

    index_epoch: int

    partition_id: Optional[str]

    segment_id: Optional[str]

    timestamp: float = field(default_factory=_now)

    payload_hash: str = ""

    previous_checksum: Optional[str] = None

    checksum: str = ""

    metadata: Dict[str, str] = field(
        default_factory=dict
    )

    def calculate_checksum(self) -> str:

        return _hash(
            "|".join(
                [
                    self.record_id,
                    self.record_type.value,
                    self.namespace,
                    str(self.sequence),
                    str(self.index_epoch),
                    self.partition_id or "",
                    self.segment_id or "",
                    self.payload_hash,
                    self.previous_checksum or "",
                ]
            )
        )


# ---------------------------------------------------------------------------
# Failure
# ---------------------------------------------------------------------------

@dataclass
class IndexFailure:
    failure_id: str

    scope: RecoveryScope

    failure_domain: FailureDomain

    namespace: str

    partition_id: Optional[str] = None

    segment_ids: List[str] = field(
        default_factory=list
    )

    replica_ids: List[str] = field(
        default_factory=list
    )

    detected_at: float = field(default_factory=_now)

    affected_epoch: Optional[int] = None

    reason: Optional[str] = None

    metadata: Dict[str, str] = field(
        default_factory=dict
    )


# ---------------------------------------------------------------------------
# Recovery target
# ---------------------------------------------------------------------------

@dataclass
class RecoveryTarget:
    target_id: str

    partition_id: Optional[str]

    region: Optional[str]
    zone: Optional[str]
    node_id: Optional[str]

    target_epoch: int

    available: bool = True

    metadata: Dict[str, str] = field(
        default_factory=dict
    )


# ---------------------------------------------------------------------------
# Recovery plan
# ---------------------------------------------------------------------------

@dataclass
class IndexRecoveryPlan:
    recovery_id: str

    namespace: str

    scope: RecoveryScope

    mode: RecoveryMode

    checkpoint_id: Optional[str]

    target: Optional[RecoveryTarget]

    failure_id: Optional[str]

    source_partition_ids: List[str] = field(
        default_factory=list
    )

    source_segment_ids: List[str] = field(
        default_factory=list
    )

    replay_from_sequence: Optional[int] = None

    replay_to_sequence: Optional[int] = None

    required_routing_epoch: Optional[int] = None

    state: RecoveryState = RecoveryState.PLANNED

    created_at: float = field(default_factory=_now)

    metadata: Dict[str, str] = field(
        default_factory=dict
    )


# ---------------------------------------------------------------------------
# Recovery generation
# ---------------------------------------------------------------------------

@dataclass
class RecoveryGeneration:
    generation_id: str

    namespace: str

    recovery_id: str

    source_checkpoint_id: Optional[str]

    source_index_epoch: int

    target_index_epoch: int

    state: RecoveryState

    restored_partition_ids: List[str] = field(
        default_factory=list
    )

    restored_segment_ids: List[str] = field(
        default_factory=list
    )

    routing_epoch_validated: Optional[int] = None

    created_at: float = field(default_factory=_now)

    published_at: Optional[float] = None

    checksum: str = ""

    metadata: Dict[str, str] = field(
        default_factory=dict
    )


# ---------------------------------------------------------------------------
# Recovery event
# ---------------------------------------------------------------------------

@dataclass
class IndexRecoveryEvent:
    event_id: str

    event_type: RecoveryEventType

    namespace: str

    recovery_id: Optional[str]

    checkpoint_id: Optional[str]

    index_epoch: int

    routing_epoch: int

    partition_ids: List[str] = field(
        default_factory=list
    )

    segment_ids: List[str] = field(
        default_factory=list
    )

    timestamp: float = field(default_factory=_now)

    metadata: Dict[str, str] = field(
        default_factory=dict
    )


# ---------------------------------------------------------------------------
# Recovery checkpoint catalog
# ---------------------------------------------------------------------------

@dataclass
class RecoveryCheckpointCatalog:
    namespace: str

    checkpoints: Dict[str, IndexCheckpoint] = field(
        default_factory=dict
    )

    latest_checkpoint_id: Optional[str] = None

    latest_valid_checkpoint_id: Optional[str] = None


# ---------------------------------------------------------------------------
# Recovery capacity
# ---------------------------------------------------------------------------

@dataclass
class IndexRecoveryCapacity:
    namespace: str

    checkpoint_count: int

    valid_checkpoint_count: int

    active_recovery_count: int

    completed_recovery_count: int

    failed_recovery_count: int

    journal_record_count: int

    artifact_count: int

    current_index_epoch: int

    current_routing_epoch: int

    supports_partition_scoped_recovery: bool
    supports_incremental_recovery: bool
    supports_parallel_recovery: bool
    supports_horizontal_growth: bool


# ---------------------------------------------------------------------------
# Backend contract
# ---------------------------------------------------------------------------

class IndexRecoveryCheckpointBackend(Protocol):
    """
    Replaceable distributed recovery metadata backend.
    """

    def save_checkpoint(
        self,
        checkpoint: IndexCheckpoint,
    ) -> None:
        ...

    def get_checkpoint(
        self,
        checkpoint_id: str,
    ) -> Optional[IndexCheckpoint]:
        ...

    def save_journal_record(
        self,
        record: RecoveryJournalRecord,
    ) -> None:
        ...

    def save_recovery_plan(
        self,
        plan: IndexRecoveryPlan,
    ) -> None:
        ...

    def save_generation(
        self,
        generation: RecoveryGeneration,
    ) -> None:
        ...

    def save_failure(
        self,
        failure: IndexFailure,
    ) -> None:
        ...

    def save_event(
        self,
        event: IndexRecoveryEvent,
    ) -> None:
        ...


# ---------------------------------------------------------------------------
# Reference backend
# ---------------------------------------------------------------------------

class InMemoryIndexRecoveryMetadata:
    """
    Reference metadata backend.

    Production storage can replace this implementation.
    """

    def __init__(self) -> None:

        self.checkpoints: Dict[
            str,
            IndexCheckpoint,
        ] = {}

        self.journal_records: Dict[
            str,
            RecoveryJournalRecord,
        ] = {}

        self.recovery_plans: Dict[
            str,
            IndexRecoveryPlan,
        ] = {}

        self.generations: Dict[
            str,
            RecoveryGeneration,
        ] = {}

        self.failures: Dict[
            str,
            IndexFailure,
        ] = {}

        self.events: List[
            IndexRecoveryEvent
        ] = []

    def save_checkpoint(
        self,
        checkpoint: IndexCheckpoint,
    ) -> None:

        self.checkpoints[
            checkpoint.identity.checkpoint_id
        ] = checkpoint

    def get_checkpoint(
        self,
        checkpoint_id: str,
    ) -> Optional[IndexCheckpoint]:

        return self.checkpoints.get(
            checkpoint_id
        )

    def save_journal_record(
        self,
        record: RecoveryJournalRecord,
    ) -> None:

        self.journal_records[
            record.record_id
        ] = record

    def save_recovery_plan(
        self,
        plan: IndexRecoveryPlan,
    ) -> None:

        self.recovery_plans[
            plan.recovery_id
        ] = plan

    def save_generation(
        self,
        generation: RecoveryGeneration,
    ) -> None:

        self.generations[
            generation.generation_id
        ] = generation

    def save_failure(
        self,
        failure: IndexFailure,
    ) -> None:

        self.failures[
            failure.failure_id
        ] = failure

    def save_event(
        self,
        event: IndexRecoveryEvent,
    ) -> None:

        self.events.append(event)


# ---------------------------------------------------------------------------
# Policy
# ---------------------------------------------------------------------------

@dataclass
class IndexRecoveryPolicy:

    checkpoint_mode: RecoveryMode = (
        RecoveryMode.CHECKPOINT_RESTORE
    )

    default_recovery_mode: RecoveryMode = (
        RecoveryMode.INCREMENTAL
    )

    checkpoint_epoch_interval: int = 1

    journal_replay_enabled: bool = True

    checksum_validation_enabled: bool = True

    epoch_fencing_enabled: bool = True

    atomic_publish_enabled: bool = True

    allow_partition_scoped_recovery: bool = True

    allow_incremental_recovery: bool = True

    allow_replica_restore: bool = True

    allow_parallel_recovery: bool = True

    checkpoint_retention_epochs: int = 16

    max_replay_gap_before_full_restore: Optional[int] = None


# ---------------------------------------------------------------------------
# Main architecture
# ---------------------------------------------------------------------------

class IndexRecoveryCheckpointingArchitecture:
    """
    Global index recovery and checkpointing architecture.

    The architecture coordinates:

        checkpoint creation
        journal positions
        failure detection
        recovery planning
        checkpoint validation
        artifact validation
        journal replay
        partition restoration
        segment restoration
        replica restoration
        routing revalidation
        atomic generation publication

    The actual physical storage implementation remains replaceable.
    """

    def __init__(
        self,
        namespace: str = "global-index",
        backend: Optional[
            IndexRecoveryCheckpointBackend
        ] = None,
        policy: Optional[
            IndexRecoveryPolicy
        ] = None,
    ) -> None:

        self.namespace = namespace

        self.backend = (
            backend
            if backend is not None
            else InMemoryIndexRecoveryMetadata()
        )

        self.policy = (
            policy
            if policy is not None
            else IndexRecoveryPolicy()
        )

        self._index_epoch = 0
        self._routing_epoch = 0

        self._checkpoint_generation = 0
        self._journal_sequence = 0

        self._checkpoints: Dict[
            str,
            IndexCheckpoint,
        ] = {}

        self._failures: Dict[
            str,
            IndexFailure,
        ] = {}

        self._plans: Dict[
            str,
            IndexRecoveryPlan,
        ] = {}

        self._generations: Dict[
            str,
            RecoveryGeneration,
        ] = {}

        self._journal: List[
            RecoveryJournalRecord
        ] = []

        self._events: List[
            IndexRecoveryEvent
        ] = []

        self._published_generation_id: Optional[
            str
        ] = None

    # ------------------------------------------------------------------
    # Epochs
    # ------------------------------------------------------------------

    @property
    def index_epoch(self) -> int:
        return self._index_epoch

    @property
    def routing_epoch(self) -> int:
        return self._routing_epoch

    def advance_index_epoch(self) -> int:

        self._index_epoch += 1

        return self._index_epoch

    def advance_routing_epoch(self) -> int:

        self._routing_epoch += 1

        return self._routing_epoch

    # ------------------------------------------------------------------
    # Journal
    # ------------------------------------------------------------------

    def append_journal_record(
        self,
        record_type: JournalRecordType,
        *,
        partition_id: Optional[str] = None,
        segment_id: Optional[str] = None,
        payload: str = "",
        metadata: Optional[
            Dict[str, str]
        ] = None,
    ) -> RecoveryJournalRecord:

        self._journal_sequence += 1

        previous_checksum = (
            self._journal[-1].checksum
            if self._journal
            else None
        )

        record = RecoveryJournalRecord(
            record_id=_new_id("journal"),
            record_type=record_type,
            namespace=self.namespace,
            sequence=self._journal_sequence,
            index_epoch=self._index_epoch,
            partition_id=partition_id,
            segment_id=segment_id,
            payload_hash=_hash(payload),
            previous_checksum=previous_checksum,
            metadata=dict(metadata or {}),
        )

        record.checksum = (
            record.calculate_checksum()
        )

        self._journal.append(record)

        self.backend.save_journal_record(
            record
        )

        return record

    # ------------------------------------------------------------------
    # Checkpoint creation
    # ------------------------------------------------------------------

    def create_checkpoint(
        self,
        *,
        partition_ids: Optional[
            Sequence[str]
        ] = None,
        segment_ids: Optional[
            Sequence[str]
        ] = None,
        manifest_versions: Optional[
            Dict[str, int]
        ] = None,
        artifacts: Optional[
            Sequence[RecoveryArtifact]
        ] = None,
        recovery_mode: Optional[
            RecoveryMode
        ] = None,
        parent_checkpoint_id: Optional[
            str
        ] = None,
    ) -> IndexCheckpoint:

        self._checkpoint_generation += 1

        journal_position = None

        if self._journal:

            last = self._journal[-1]

            journal_position = JournalPosition(
                journal_id=last.journal_id
                if hasattr(last, "journal_id")
                else "global-journal",
                sequence=last.sequence,
                partition_id=last.partition_id,
                index_epoch=last.index_epoch,
                checksum=last.checksum,
            )

        checkpoint_id = _new_id(
            "checkpoint"
        )

        identity = RecoveryCheckpointIdentity(
            checkpoint_id=checkpoint_id,
            namespace=self.namespace,
            checkpoint_generation=(
                self._checkpoint_generation
            ),
            routing_epoch=self._routing_epoch,
            index_epoch=self._index_epoch,
        )

        checkpoint = IndexCheckpoint(
            identity=identity,
            state=CheckpointState.BUILDING,
            recovery_mode=(
                recovery_mode
                if recovery_mode is not None
                else self.policy.checkpoint_mode
            ),
            journal_position=journal_position,
            partition_ids=list(
                partition_ids or []
            ),
            segment_ids=list(
                segment_ids or []
            ),
            manifest_versions=dict(
                manifest_versions or {}
            ),
            parent_checkpoint_id=(
                parent_checkpoint_id
            ),
        )

        for artifact in artifacts or []:

            checkpoint.artifacts[
                artifact.artifact_id
            ] = artifact

        checkpoint.checksum = (
            checkpoint.calculate_checksum()
        )

        self._checkpoints[
            checkpoint_id
        ] = checkpoint

        self.backend.save_checkpoint(
            checkpoint
        )

        self._emit_event(
            RecoveryEventType.CHECKPOINT_CREATED,
            checkpoint_id=checkpoint_id,
            partition_ids=checkpoint.partition_ids,
            segment_ids=checkpoint.segment_ids,
        )

        return checkpoint

    # ------------------------------------------------------------------
    # Seal checkpoint
    # ------------------------------------------------------------------

    def seal_checkpoint(
        self,
        checkpoint_id: str,
    ) -> IndexCheckpoint:

        checkpoint = self._get_checkpoint(
            checkpoint_id
        )

        if checkpoint.state not in (
            CheckpointState.BUILDING,
            CheckpointState.VALIDATING,
        ):
            raise ValueError(
                "Checkpoint cannot be sealed from current state"
            )

        checkpoint.checksum = (
            checkpoint.calculate_checksum()
        )

        checkpoint.state = CheckpointState.SEALED
        checkpoint.sealed_at = _now()

        self.backend.save_checkpoint(
            checkpoint
        )

        self._emit_event(
            RecoveryEventType.CHECKPOINT_SEALED,
            checkpoint_id=checkpoint_id,
            partition_ids=checkpoint.partition_ids,
            segment_ids=checkpoint.segment_ids,
        )

        return checkpoint

    # ------------------------------------------------------------------
    # Validate checkpoint
    # ------------------------------------------------------------------

    def validate_checkpoint(
        self,
        checkpoint_id: str,
    ) -> bool:

        checkpoint = self._get_checkpoint(
            checkpoint_id
        )

        checkpoint.state = (
            CheckpointState.VALIDATING
        )

        self.backend.save_checkpoint(
            checkpoint
        )

        calculated = (
            checkpoint.calculate_checksum()
        )

        valid = (
            calculated == checkpoint.checksum
        )

        artifact_valid = all(
            artifact.state
            == ArtifactState.AVAILABLE
            and (
                not artifact.content_hash
                or bool(artifact.content_hash)
            )
            for artifact
            in checkpoint.artifacts.values()
        )

        valid = valid and artifact_valid

        checkpoint.validated_at = _now()

        checkpoint.state = (
            CheckpointState.VALID
            if valid
            else CheckpointState.INVALID
        )

        self.backend.save_checkpoint(
            checkpoint
        )

        self._emit_event(
            (
                RecoveryEventType.CHECKPOINT_VALIDATED
                if valid
                else RecoveryEventType.CHECKPOINT_INVALIDATED
            ),
            checkpoint_id=checkpoint_id,
            partition_ids=checkpoint.partition_ids,
            segment_ids=checkpoint.segment_ids,
        )

        return valid

    # ------------------------------------------------------------------
    # Failure detection
    # ------------------------------------------------------------------

    def register_failure(
        self,
        scope: RecoveryScope,
        failure_domain: FailureDomain,
        *,
        partition_id: Optional[str] = None,
        segment_ids: Optional[
            Sequence[str]
        ] = None,
        replica_ids: Optional[
            Sequence[str]
        ] = None,
        affected_epoch: Optional[int] = None,
        reason: Optional[str] = None,
        metadata: Optional[
            Dict[str, str]
        ] = None,
    ) -> IndexFailure:

        failure = IndexFailure(
            failure_id=_new_id("failure"),
            scope=scope,
            failure_domain=failure_domain,
            namespace=self.namespace,
            partition_id=partition_id,
            segment_ids=list(
                segment_ids or []
            ),
            replica_ids=list(
                replica_ids or []
            ),
            affected_epoch=affected_epoch,
            reason=reason,
            metadata=dict(metadata or {}),
        )

        self._failures[
            failure.failure_id
        ] = failure

        self.backend.save_failure(
            failure
        )

        self._emit_event(
            RecoveryEventType.FAILURE_DETECTED,
            partition_ids=(
                [partition_id]
                if partition_id
                else []
            ),
            segment_ids=list(
                segment_ids or []
            ),
            metadata={
                "failure_domain":
                    failure_domain.value,
                "scope":
                    scope.value,
                "reason":
                    reason or "",
            },
        )

        return failure

    # ------------------------------------------------------------------
    # Recovery planning
    # ------------------------------------------------------------------

    def plan_recovery(
        self,
        *,
        scope: RecoveryScope,
        mode: Optional[RecoveryMode] = None,
        checkpoint_id: Optional[str] = None,
        failure_id: Optional[str] = None,
        partition_ids: Optional[
            Sequence[str]
        ] = None,
        segment_ids: Optional[
            Sequence[str]
        ] = None,
        target: Optional[RecoveryTarget] = None,
        required_routing_epoch: Optional[
            int
        ] = None,
    ) -> IndexRecoveryPlan:

        selected_mode = (
            mode
            if mode is not None
            else self.policy.default_recovery_mode
        )

        if (
            scope == RecoveryScope.PARTITION
            and not self.policy.allow_partition_scoped_recovery
        ):
            raise RuntimeError(
                "Partition-scoped recovery disabled by policy"
            )

        if (
            selected_mode
            == RecoveryMode.INCREMENTAL
            and not self.policy.allow_incremental_recovery
        ):
            selected_mode = RecoveryMode.FULL

        checkpoint = None

        if checkpoint_id is not None:

            checkpoint = self._get_checkpoint(
                checkpoint_id
            )

            if checkpoint.state != CheckpointState.VALID:
                raise ValueError(
                    "Recovery checkpoint must be valid"
                )

        plan = IndexRecoveryPlan(
            recovery_id=_new_id("recovery"),
            namespace=self.namespace,
            scope=scope,
            mode=selected_mode,
            checkpoint_id=checkpoint_id,
            target=target,
            failure_id=failure_id,
            source_partition_ids=list(
                partition_ids or []
            ),
            source_segment_ids=list(
                segment_ids or []
            ),
            required_routing_epoch=(
                required_routing_epoch
                if required_routing_epoch is not None
                else self._routing_epoch
            ),
        )

        if (
            selected_mode
            in (
                RecoveryMode.INCREMENTAL,
                RecoveryMode.REPLAY,
            )
            and checkpoint is not None
            and checkpoint.journal_position is not None
        ):
            plan.replay_from_sequence = (
                checkpoint.journal_position.sequence
                + 1
            )

            plan.replay_to_sequence = (
                self._journal_sequence
            )

        self._plans[
            plan.recovery_id
        ] = plan

        self.backend.save_recovery_plan(
            plan
        )

        self._emit_event(
            RecoveryEventType.RECOVERY_PLANNED,
            recovery_id=plan.recovery_id,
            checkpoint_id=checkpoint_id,
            partition_ids=plan.source_partition_ids,
            segment_ids=plan.source_segment_ids,
        )

        return plan

    # ------------------------------------------------------------------
    # Start recovery
    # ------------------------------------------------------------------

    def start_recovery(
        self,
        recovery_id: str,
    ) -> IndexRecoveryPlan:

        plan = self._get_plan(
            recovery_id
        )

        if (
            plan.required_routing_epoch
            is not None
            and self.policy.epoch_fencing_enabled
            and self._routing_epoch
            < plan.required_routing_epoch
        ):
            plan.state = RecoveryState.FENCED

            self.backend.save_recovery_plan(
                plan
            )

            self._emit_event(
                RecoveryEventType.RECOVERY_FENCED,
                recovery_id=recovery_id,
                checkpoint_id=plan.checkpoint_id,
                partition_ids=(
                    plan.source_partition_ids
                ),
                segment_ids=(
                    plan.source_segment_ids
                ),
            )

            return plan

        plan.state = RecoveryState.PREPARING

        self.backend.save_recovery_plan(
            plan
        )

        self._emit_event(
            RecoveryEventType.RECOVERY_STARTED,
            recovery_id=recovery_id,
            checkpoint_id=plan.checkpoint_id,
            partition_ids=(
                plan.source_partition_ids
            ),
            segment_ids=(
                plan.source_segment_ids
            ),
        )

        return plan

    # ------------------------------------------------------------------
    # Recovery validation
    # ------------------------------------------------------------------

    def begin_validation(
        self,
        recovery_id: str,
    ) -> IndexRecoveryPlan:

        plan = self._get_plan(
            recovery_id
        )

        plan.state = RecoveryState.VALIDATING

        self.backend.save_recovery_plan(
            plan
        )

        self._emit_event(
            RecoveryEventType.RECOVERY_VALIDATION_STARTED,
            recovery_id=recovery_id,
            checkpoint_id=plan.checkpoint_id,
            partition_ids=(
                plan.source_partition_ids
            ),
            segment_ids=(
                plan.source_segment_ids
            ),
        )

        return plan

    # ------------------------------------------------------------------
    # Journal replay
    # ------------------------------------------------------------------

    def begin_replay(
        self,
        recovery_id: str,
    ) -> IndexRecoveryPlan:

        plan = self._get_plan(
            recovery_id
        )

        if not self.policy.journal_replay_enabled:
            raise RuntimeError(
                "Journal replay disabled by policy"
            )

        plan.state = RecoveryState.REPLAYING

        self.backend.save_recovery_plan(
            plan
        )

        self._emit_event(
            RecoveryEventType.RECOVERY_REPLAY_STARTED,
            recovery_id=recovery_id,
            checkpoint_id=plan.checkpoint_id,
            partition_ids=(
                plan.source_partition_ids
            ),
            segment_ids=(
                plan.source_segment_ids
            ),
        )

        return plan

    def journal_records_for_recovery(
        self,
        recovery_id: str,
    ) -> List[RecoveryJournalRecord]:

        plan = self._get_plan(
            recovery_id
        )

        start = (
            plan.replay_from_sequence
            if plan.replay_from_sequence
            is not None
            else 1
        )

        end = (
            plan.replay_to_sequence
            if plan.replay_to_sequence
            is not None
            else self._journal_sequence
        )

        return [
            record
            for record in self._journal
            if start
            <= record.sequence
            <= end
            and (
                not plan.source_partition_ids
                or record.partition_id
                in plan.source_partition_ids
                or record.partition_id is None
            )
        ]

    # ------------------------------------------------------------------
    # Restore generation
    # ------------------------------------------------------------------

    def create_recovery_generation(
        self,
        recovery_id: str,
    ) -> RecoveryGeneration:

        plan = self._get_plan(
            recovery_id
        )

        source_epoch = self._index_epoch

        if plan.checkpoint_id:

            checkpoint = self._get_checkpoint(
                plan.checkpoint_id
            )

            source_epoch = (
                checkpoint.identity.index_epoch
            )

        target_epoch = max(
            self._index_epoch,
            source_epoch,
        ) + 1

        generation = RecoveryGeneration(
            generation_id=_new_id(
                "recovery-generation"
            ),
            namespace=self.namespace,
            recovery_id=recovery_id,
            source_checkpoint_id=(
                plan.checkpoint_id
            ),
            source_index_epoch=source_epoch,
            target_index_epoch=target_epoch,
            state=RecoveryState.RESTORING,
            restored_partition_ids=list(
                plan.source_partition_ids
            ),
            restored_segment_ids=list(
                plan.source_segment_ids
            ),
        )

        generation.checksum = _hash(
            "|".join(
                [
                    generation.generation_id,
                    recovery_id,
                    str(source_epoch),
                    str(target_epoch),
                    ",".join(
                        sorted(
                            generation.restored_partition_ids
                        )
                    ),
                    ",".join(
                        sorted(
                            generation.restored_segment_ids
                        )
                    ),
                ]
            )
        )

        self._generations[
            generation.generation_id
        ] = generation

        self.backend.save_generation(
            generation
        )

        plan.state = RecoveryState.RESTORING

        self.backend.save_recovery_plan(
            plan
        )

        self._emit_event(
            RecoveryEventType.RECOVERY_RESTORE_STARTED,
            recovery_id=recovery_id,
            checkpoint_id=plan.checkpoint_id,
            partition_ids=(
                plan.source_partition_ids
            ),
            segment_ids=(
                plan.source_segment_ids
            ),
        )

        return generation

    # ------------------------------------------------------------------
    # Routing revalidation
    # ------------------------------------------------------------------

    def revalidate_routing(
        self,
        generation_id: str,
        routing_epoch: Optional[int] = None,
    ) -> RecoveryGeneration:

        generation = self._get_generation(
            generation_id
        )

        epoch = (
            routing_epoch
            if routing_epoch is not None
            else self._routing_epoch
        )

        if self.policy.epoch_fencing_enabled:

            if epoch < self._routing_epoch:
                raise RuntimeError(
                    "Recovery generation uses stale routing epoch"
                )

        generation.routing_epoch_validated = epoch
        generation.state = RecoveryState.REVALIDATING

        self.backend.save_generation(
            generation
        )

        self._emit_event(
            RecoveryEventType.ROUTING_REVALIDATION_STARTED,
            recovery_id=generation.recovery_id,
            checkpoint_id=(
                generation.source_checkpoint_id
            ),
            partition_ids=(
                generation.restored_partition_ids
            ),
            segment_ids=(
                generation.restored_segment_ids
            ),
        )

        return generation

    # ------------------------------------------------------------------
    # Atomic recovery publication
    # ------------------------------------------------------------------

    def publish_recovery_generation(
        self,
        generation_id: str,
    ) -> RecoveryGeneration:

        generation = self._get_generation(
            generation_id
        )

        if (
            generation.routing_epoch_validated
            is None
        ):
            raise RuntimeError(
                "Routing must be revalidated before publication"
            )

        if self.policy.epoch_fencing_enabled:

            if (
                generation.routing_epoch_validated
                < self._routing_epoch
            ):
                generation.state = RecoveryState.FENCED

                self.backend.save_generation(
                    generation
                )

                self._emit_event(
                    RecoveryEventType.RECOVERY_FENCED,
                    recovery_id=(
                        generation.recovery_id
                    ),
                    checkpoint_id=(
                        generation.source_checkpoint_id
                    ),
                    partition_ids=(
                        generation.restored_partition_ids
                    ),
                    segment_ids=(
                        generation.restored_segment_ids
                    ),
                )

                return generation

        generation.state = RecoveryState.PUBLISHING

        self.backend.save_generation(
            generation
        )

        self._emit_event(
            RecoveryEventType.RECOVERY_PUBLISHING,
            recovery_id=generation.recovery_id,
            checkpoint_id=(
                generation.source_checkpoint_id
            ),
            partition_ids=(
                generation.restored_partition_ids
            ),
            segment_ids=(
                generation.restored_segment_ids
            ),
        )

        # Atomic control-plane publication.
        self._published_generation_id = (
            generation.generation_id
        )

        generation.state = RecoveryState.COMPLETED
        generation.published_at = _now()

        self._index_epoch = max(
            self._index_epoch,
            generation.target_index_epoch,
        )

        self.backend.save_generation(
            generation
        )

        plan = self._get_plan(
            generation.recovery_id
        )

        plan.state = RecoveryState.COMPLETED

        self.backend.save_recovery_plan(
            plan
        )

        self._emit_event(
            RecoveryEventType.RECOVERY_COMPLETED,
            recovery_id=generation.recovery_id,
            checkpoint_id=(
                generation.source_checkpoint_id
            ),
            partition_ids=(
                generation.restored_partition_ids
            ),
            segment_ids=(
                generation.restored_segment_ids
            ),
        )

        return generation

    # ------------------------------------------------------------------
    # Recovery failure / fencing
    # ------------------------------------------------------------------

    def fail_recovery(
        self,
        recovery_id: str,
        reason: str,
    ) -> IndexRecoveryPlan:

        plan = self._get_plan(
            recovery_id
        )

        plan.state = RecoveryState.FAILED
        plan.metadata["failure_reason"] = reason

        self.backend.save_recovery_plan(
            plan
        )

        self._emit_event(
            RecoveryEventType.RECOVERY_FAILED,
            recovery_id=recovery_id,
            checkpoint_id=plan.checkpoint_id,
            partition_ids=(
                plan.source_partition_ids
            ),
            segment_ids=(
                plan.source_segment_ids
            ),
            metadata={
                "reason": reason,
            },
        )

        return plan

    def fence_recovery(
        self,
        recovery_id: str,
        reason: str = "epoch_fenced",
    ) -> IndexRecoveryPlan:

        plan = self._get_plan(
            recovery_id
        )

        plan.state = RecoveryState.FENCED
        plan.metadata["fence_reason"] = reason

        self.backend.save_recovery_plan(
            plan
        )

        self._emit_event(
            RecoveryEventType.RECOVERY_FENCED,
            recovery_id=recovery_id,
            checkpoint_id=plan.checkpoint_id,
            partition_ids=(
                plan.source_partition_ids
            ),
            segment_ids=(
                plan.source_segment_ids
            ),
            metadata={
                "reason": reason,
            },
        )

        return plan

    # ------------------------------------------------------------------
    # Checkpoint retirement
    # ------------------------------------------------------------------

    def retire_checkpoint(
        self,
        checkpoint_id: str,
    ) -> IndexCheckpoint:

        checkpoint = self._get_checkpoint(
            checkpoint_id
        )

        checkpoint.state = (
            CheckpointState.RETIRED
        )

        self.backend.save_checkpoint(
            checkpoint
        )

        self._emit_event(
            RecoveryEventType.CHECKPOINT_RETIREMENT,
            checkpoint_id=checkpoint_id,
            partition_ids=checkpoint.partition_ids,
            segment_ids=checkpoint.segment_ids,
        )

        return checkpoint

    # ------------------------------------------------------------------
    # Capacity
    # ------------------------------------------------------------------

    def capacity(
        self,
    ) -> IndexRecoveryCapacity:

        active_recoveries = sum(
            1
            for plan in self._plans.values()
            if plan.state
            not in (
                RecoveryState.COMPLETED,
                RecoveryState.FAILED,
                RecoveryState.ABORTED,
            )
        )

        completed = sum(
            1
            for plan in self._plans.values()
            if plan.state
            == RecoveryState.COMPLETED
        )

        failed = sum(
            1
            for plan in self._plans.values()
            if plan.state
            == RecoveryState.FAILED
        )

        valid_checkpoints = sum(
            1
            for checkpoint
            in self._checkpoints.values()
            if checkpoint.state
            == CheckpointState.VALID
        )

        artifact_count = sum(
            len(checkpoint.artifacts)
            for checkpoint
            in self._checkpoints.values()
        )

        return IndexRecoveryCapacity(
            namespace=self.namespace,
            checkpoint_count=len(
                self._checkpoints
            ),
            valid_checkpoint_count=(
                valid_checkpoints
            ),
            active_recovery_count=(
                active_recoveries
            ),
            completed_recovery_count=(
                completed
            ),
            failed_recovery_count=(
                failed
            ),
            journal_record_count=len(
                self._journal
            ),
            artifact_count=artifact_count,
            current_index_epoch=self._index_epoch,
            current_routing_epoch=self._routing_epoch,
            supports_partition_scoped_recovery=(
                self.policy.allow_partition_scoped_recovery
            ),
            supports_incremental_recovery=(
                self.policy.allow_incremental_recovery
            ),
            supports_parallel_recovery=(
                self.policy.allow_parallel_recovery
            ),
            supports_horizontal_growth=True,
        )

    # ------------------------------------------------------------------
    # Architecture description
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
                "global_index",
                "checkpoint_fabric",
                "index_state_snapshots",
                "recovery_metadata_and_journal",
                "failure_detection",
                "partition_recovery",
                "segment_recovery",
                "replica_recovery",
                "routing_revalidation",
                "atomic_restore",
            ],

            "recovery_layers": [
                "immutable_checkpoint_identity",
                "checkpoint_generation",
                "checkpoint_checksum",
                "checkpoint_artifacts",
                "journal_position",
                "journal_replay",
                "failure_domain_detection",
                "recovery_planning",
                "recovery_generation",
                "index_epoch_fencing",
                "routing_epoch_revalidation",
                "atomic_generation_publication",
                "checkpoint_retirement",
            ],

            "checkpoint_types": [
                artifact_type.value
                for artifact_type
                in RecoveryArtifactType
            ],

            "recovery_modes": [
                mode.value
                for mode
                in RecoveryMode
            ],

            "recovery_scopes": [
                scope.value
                for scope
                in RecoveryScope
            ],

            "failure_domains": [
                domain.value
                for domain
                in FailureDomain
            ],

            "supports": {
                "global_checkpointing": True,
                "partition_checkpointing": True,
                "segment_checkpointing": True,
                "routing_checkpointing": True,
                "replica_checkpointing": True,
                "journal_positions": True,
                "incremental_recovery": True,
                "full_recovery": True,
                "journal_replay": True,
                "partition_scoped_recovery": True,
                "segment_scoped_recovery": True,
                "replica_restore": True,
                "failure_domain_recovery": True,
                "parallel_recovery": True,
                "epoch_fencing": True,
                "routing_revalidation": True,
                "atomic_restore_publication": True,
                "checksum_validation": True,
                "checkpoint_retirement": True,
                "recovery_generations": True,
                "replaceable_backend": True,
                "horizontal_growth": True,
            },

            "global_constraints": {
                "single_global_checkpoint_file": False,
                "single_global_recovery_worker": False,
                "single_global_journal_file": False,
                "single_global_recovery_database": False,
                "single_global_storage_node": False,
                "fixed_global_document_limit": False,
                "fixed_global_segment_limit": False,
                "fixed_global_partition_limit": False,
                "fixed_global_checkpoint_limit": False,
                "fixed_global_recovery_limit": False,
            },

            "safety_properties": {
                "immutable_checkpoint_identity": True,
                "checkpoint_checksum_validation": True,
                "journal_chain_integrity": True,
                "stale_epoch_rejection": True,
                "routing_epoch_revalidation": True,
                "atomic_recovery_generation_publish": True,
                "partial_partition_recovery": True,
                "incremental_recovery": True,
                "failure_domain_awareness": True,
            },

            "scale_model": {
                "documents": (
                    "billions_to_trillions"
                ),
                "partitions": (
                    "horizontally expandable"
                ),
                "segments": (
                    "horizontally expandable"
                ),
                "recovery_workers": (
                    "horizontally expandable"
                ),
                "checkpoint_artifacts": (
                    "distributed and partitionable"
                ),
            },
        }

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def checkpoints(
        self,
    ) -> Dict[str, IndexCheckpoint]:

        return dict(self._checkpoints)

    @property
    def failures(
        self,
    ) -> Dict[str, IndexFailure]:

        return dict(self._failures)

    @property
    def recovery_plans(
        self,
    ) -> Dict[str, IndexRecoveryPlan]:

        return dict(self._plans)

    @property
    def generations(
        self,
    ) -> Dict[str, RecoveryGeneration]:

        return dict(self._generations)

    @property
    def journal(
        self,
    ) -> List[RecoveryJournalRecord]:

        return list(self._journal)

    @property
    def events(
        self,
    ) -> List[IndexRecoveryEvent]:

        return list(self._events)

    @property
    def published_generation_id(
        self,
    ) -> Optional[str]:

        return self._published_generation_id

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_checkpoint(
        self,
        checkpoint_id: str,
    ) -> IndexCheckpoint:

        checkpoint = self._checkpoints.get(
            checkpoint_id
        )

        if checkpoint is None:

            checkpoint = (
                self.backend.get_checkpoint(
                    checkpoint_id
                )
            )

        if checkpoint is None:
            raise KeyError(
                f"Unknown checkpoint: {checkpoint_id}"
            )

        return checkpoint

    def _get_plan(
        self,
        recovery_id: str,
    ) -> IndexRecoveryPlan:

        plan = self._plans.get(
            recovery_id
        )

        if plan is None:
            raise KeyError(
                f"Unknown recovery: {recovery_id}"
            )

        return plan

    def _get_generation(
        self,
        generation_id: str,
    ) -> RecoveryGeneration:

        generation = self._generations.get(
            generation_id
        )

        if generation is None:
            raise KeyError(
                f"Unknown recovery generation: {generation_id}"
            )

        return generation

    def _emit_event(
        self,
        event_type: RecoveryEventType,
        *,
        recovery_id: Optional[str] = None,
        checkpoint_id: Optional[str] = None,
        partition_ids: Optional[
            Sequence[str]
        ] = None,
        segment_ids: Optional[
            Sequence[str]
        ] = None,
        metadata: Optional[
            Dict[str, str]
        ] = None,
    ) -> IndexRecoveryEvent:

        event = IndexRecoveryEvent(
            event_id=_new_id(
                "recovery-event"
            ),
            event_type=event_type,
            namespace=self.namespace,
            recovery_id=recovery_id,
            checkpoint_id=checkpoint_id,
            index_epoch=self._index_epoch,
            routing_epoch=self._routing_epoch,
            partition_ids=list(
                partition_ids or []
            ),
            segment_ids=list(
                segment_ids or []
            ),
            metadata=dict(metadata or {}),
        )

        self._events.append(event)

        self.backend.save_event(
            event
        )

        return event


# ---------------------------------------------------------------------------
# Stable aliases
# ---------------------------------------------------------------------------

IndexRecoveryCheckpointing = (
    IndexRecoveryCheckpointingArchitecture
)

GlobalIndexRecovery = (
    IndexRecoveryCheckpointingArchitecture
)

Phase10_7IndexRecoveryCheckpointing = (
    IndexRecoveryCheckpointingArchitecture
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

__all__ = [
    "ARCHITECTURE_VERSION",
    "GLOBAL_SCALE_TARGET",
    "GOOGLE_SCALE_CAPABILITY_TARGET",
    "GOOGLE_TECHNOLOGY_DEPENDENCY",

    "CheckpointState",
    "RecoveryState",
    "RecoveryScope",
    "RecoveryMode",
    "FailureDomain",
    "ArtifactState",
    "RecoveryArtifactType",
    "JournalRecordType",
    "RecoveryEventType",

    "RecoveryCheckpointIdentity",
    "JournalPosition",
    "RecoveryArtifact",
    "IndexCheckpoint",
    "RecoveryJournalRecord",
    "IndexFailure",
    "RecoveryTarget",
    "IndexRecoveryPlan",
    "RecoveryGeneration",
    "IndexRecoveryEvent",
    "RecoveryCheckpointCatalog",
    "IndexRecoveryCapacity",

    "IndexRecoveryCheckpointBackend",
    "InMemoryIndexRecoveryMetadata",

    "IndexRecoveryPolicy",
    "IndexRecoveryCheckpointingArchitecture",

    "IndexRecoveryCheckpointing",
    "GlobalIndexRecovery",
    "Phase10_7IndexRecoveryCheckpointing",
]
