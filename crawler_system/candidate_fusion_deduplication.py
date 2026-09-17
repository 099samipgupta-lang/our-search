"""
OUR SEARCH — Phase 11.7
Candidate Fusion & Deduplication

Architecture version:
    candidate-fusion-deduplication.v1

Purpose:
    Fuse candidate documents returned from many distributed retrieval
    partitions and replicas into one canonical retrieval candidate set.

Scale target:
    Billions → trillions of publicly accessible Web resources.

This is a retrieval-stage control architecture.

It does NOT:
    - perform final ranking
    - replace Phase 12 ranking
    - crawl the Web
    - mutate the index
    - depend on Google technology
    - perform final search-result presentation

Pipeline:

    11.6 MULTI-PARTITION CANDIDATE RETRIEVAL
                    ↓
             CANDIDATE STREAMS
                    ↓
             IDENTITY RESOLUTION
                    ↓
          CROSS-PARTITION FUSION
                    ↓
           DUPLICATE DETECTION
                    ↓
       EVIDENCE / MATCH AGGREGATION
                    ↓
          CANDIDATE CONSOLIDATION
                    ↓
       UNIFIED RETRIEVAL CANDIDATE SET
                    ↓
       11.8 RESCORING / EARLY FILTERING

Important architecture principle:

    The same logical document may appear through:
        - multiple partitions
        - multiple replicas
        - multiple terms
        - multiple fields
        - multiple phrases
        - multiple retrieval tasks

    11.7 must therefore preserve one canonical candidate identity while
    aggregating all useful retrieval evidence.

There is intentionally no fixed global:
    - document count
    - partition count
    - replica count
    - candidate count
    - worker count
    - Web-resource count

Operational candidate/fanout limits are execution controls, not global
Web-scale capacity ceilings.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from hashlib import sha256
from typing import Any, Dict, Iterable, List, Mapping, Optional, Protocol, Sequence, Tuple
from uuid import uuid4


ARCHITECTURE_VERSION = "candidate-fusion-deduplication.v1"
SCALE_TARGET = "billions_to_trillions_of_public_web_resources"
GOOGLE_SCALE_CAPABILITY_TARGET = True
GOOGLE_TECHNOLOGY_DEPENDENCY = False


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex}"


def _hash(value: Any) -> str:
    return sha256(repr(value).encode("utf-8")).hexdigest()


class FusionState(str, Enum):
    RECEIVED = "received"
    VALIDATING = "validating"
    IDENTITY_RESOLUTION = "identity_resolution"
    FUSION = "fusion"
    DEDUPLICATION = "deduplication"
    EVIDENCE_AGGREGATION = "evidence_aggregation"
    CONSOLIDATION = "consolidation"
    COMPLETED = "completed"
    PARTIAL = "partial"
    REJECTED = "rejected"
    FAILED = "failed"


class CandidateIdentityType(str, Enum):
    DOCUMENT_ID = "document_id"
    CANONICAL_URL = "canonical_url"
    CONTENT_IDENTITY = "content_identity"
    RESOURCE_ID = "resource_id"


class CandidateMatchType(str, Enum):
    EXACT_DOCUMENT = "exact_document"
    SAME_CANONICAL_URL = "same_canonical_url"
    SAME_CONTENT = "same_content"
    SAME_RESOURCE = "same_resource"
    POSSIBLE_DUPLICATE = "possible_duplicate"


class EvidenceType(str, Enum):
    TERM_MATCH = "term_match"
    FIELD_MATCH = "field_match"
    PHRASE_MATCH = "phrase_match"
    POSTING_MATCH = "posting_match"
    REPLICA_MATCH = "replica_match"
    PARTITION_MATCH = "partition_match"
    DOCUMENT_MATCH = "document_match"


class EvidenceStrength(str, Enum):
    WEAK = "weak"
    MODERATE = "moderate"
    STRONG = "strong"
    EXACT = "exact"


class CandidateState(str, Enum):
    RECEIVED = "received"
    VERIFIED = "verified"
    FUSED = "fused"
    DUPLICATE = "duplicate"
    CONSOLIDATED = "consolidated"
    PARTIAL = "partial"
    REJECTED = "rejected"


class DeduplicationState(str, Enum):
    NOT_CHECKED = "not_checked"
    UNIQUE = "unique"
    DUPLICATE = "duplicate"
    POSSIBLE_DUPLICATE = "possible_duplicate"


class FusionEventType(str, Enum):
    FUSION_RECEIVED = "fusion_received"
    FUSION_STARTED = "fusion_started"
    IDENTITIES_RESOLVED = "identities_resolved"
    CANDIDATES_MERGED = "candidates_merged"
    DUPLICATES_DETECTED = "duplicates_detected"
    EVIDENCE_AGGREGATED = "evidence_aggregated"
    CANDIDATES_CONSOLIDATED = "candidates_consolidated"
    PARTIAL_INPUT_DETECTED = "partial_input_detected"
    FUSION_COMPLETED = "fusion_completed"
    FUSION_REJECTED = "fusion_rejected"
    FUSION_FAILED = "fusion_failed"
    CHECKPOINT_CREATED = "checkpoint_created"


@dataclass(frozen=True)
class FusionIdentity:
    fusion_id: str
    retrieval_id: str
    query_id: str
    canonical_query_hash: str
    created_at: str
    architecture_version: str = ARCHITECTURE_VERSION


@dataclass(frozen=True)
class CandidateIdentity:
    identity_type: CandidateIdentityType
    identity_value: str
    normalized_value: str
    identity_hash: str


@dataclass(frozen=True)
class CandidateEvidence:
    evidence_id: str
    candidate_identity: CandidateIdentity
    evidence_type: EvidenceType
    strength: EvidenceStrength
    partition_id: Optional[str] = None
    replica_id: Optional[str] = None
    read_id: Optional[str] = None
    term: Optional[str] = None
    field: Optional[str] = None
    phrase: Optional[str] = None
    frequency: Optional[int] = None
    positions_available: bool = False
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CandidateObservation:
    observation_id: str
    identity: CandidateIdentity
    partition_id: str
    read_id: str
    replica_id: Optional[str]
    canonical_url: Optional[str]
    content_identity: Optional[str]
    terms: Tuple[str, ...] = ()
    fields: Tuple[str, ...] = ()
    phrases: Tuple[str, ...] = ()
    evidence: Tuple[CandidateEvidence, ...] = ()
    source_metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass
class CandidateFusionRecord:
    identity: CandidateIdentity
    state: CandidateState
    deduplication_state: DeduplicationState
    observations: List[CandidateObservation] = field(default_factory=list)
    evidence: List[CandidateEvidence] = field(default_factory=list)
    partitions: set[str] = field(default_factory=set)
    replicas: set[str] = field(default_factory=set)
    reads: set[str] = field(default_factory=set)
    matched_terms: set[str] = field(default_factory=set)
    matched_fields: set[str] = field(default_factory=set)
    matched_phrases: set[str] = field(default_factory=set)
    canonical_urls: set[str] = field(default_factory=set)
    content_identities: set[str] = field(default_factory=set)
    duplicate_observation_count: int = 0
    fusion_evidence_count: int = 0


@dataclass
class UnifiedCandidate:
    identity: CandidateIdentity
    state: CandidateState
    deduplication_state: DeduplicationState
    document_id: Optional[str]
    canonical_url: Optional[str]
    content_identity: Optional[str]
    partitions: Tuple[str, ...]
    replicas: Tuple[str, ...]
    source_reads: Tuple[str, ...]
    matched_terms: Tuple[str, ...]
    matched_fields: Tuple[str, ...]
    matched_phrases: Tuple[str, ...]
    evidence_count: int
    duplicate_observation_count: int
    retrieval_features: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CandidateFusionBudget:
    max_input_observations: int = 100000
    max_unified_candidates: int = 100000
    max_evidence_per_candidate: int = 256
    max_observations_per_candidate: int = 256
    max_identity_keys: int = 200000
    max_parallel_fusion_groups: int = 64


@dataclass(frozen=True)
class CandidateFusionDecision:
    fusion_id: str
    state: FusionState
    input_observations: int
    unified_candidates: int
    duplicates_collapsed: int
    evidence_records: int
    partial_input: bool
    budget_applied: bool
    decision_reason: str


@dataclass
class CandidateFusionResult:
    fusion_id: str
    identity: FusionIdentity
    state: FusionState
    candidates: Dict[str, UnifiedCandidate] = field(default_factory=dict)
    records: Dict[str, CandidateFusionRecord] = field(default_factory=dict)
    decision: Optional[CandidateFusionDecision] = None
    duplicates_collapsed: int = 0
    evidence_records: int = 0
    input_observations: int = 0
    partial_input: bool = False
    fusion_hash: Optional[str] = None
    created_at: str = field(default_factory=_now)
    completed_at: Optional[str] = None


@dataclass(frozen=True)
class CandidateFusionCheckpoint:
    checkpoint_id: str
    fusion_id: str
    state: FusionState
    candidate_count: int
    observation_count: int
    duplicate_count: int
    created_at: str = field(default_factory=_now)


@dataclass(frozen=True)
class CandidateFusionEvent:
    event_id: str
    fusion_id: str
    event_type: FusionEventType
    state: FusionState
    timestamp: str
    payload: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CandidateFusionRequest:
    retrieval_id: str
    query_id: str
    canonical_query_hash: str
    candidate_streams: Sequence[Mapping[str, Any]]
    max_candidates: Optional[int] = None
    request_metadata: Mapping[str, Any] = field(default_factory=dict)


class CandidateFusionBackend(Protocol):
    """
    Replaceable identity/evidence backend.

    Production implementations can connect this interface to:
        - distributed document identity services
        - canonical URL stores
        - content identity systems
        - document metadata stores
        - distributed candidate exchange fabrics
        - persistent fusion state
        - externalized deduplication indexes

    The fusion architecture does not require a particular database,
    filesystem, vendor, or distributed-computing framework.
    """

    def resolve_identity(
        self,
        observation: CandidateObservation,
    ) -> CandidateIdentity:
        ...

    def normalize_url(
        self,
        url: str,
    ) -> str:
        ...

    def normalize_content_identity(
        self,
        content_identity: str,
    ) -> str:
        ...


class InMemoryCandidateFusionBackend:
    """
    Reference identity backend.

    Production identity resolution can be replaced without changing
    the Phase 11.7 control architecture.
    """

    def resolve_identity(
        self,
        observation: CandidateObservation,
    ) -> CandidateIdentity:
        if observation.identity.identity_value:
            return observation.identity

        if observation.canonical_url:
            normalized = self.normalize_url(
                observation.canonical_url
            )

            return CandidateIdentity(
                identity_type=CandidateIdentityType.CANONICAL_URL,
                identity_value=observation.canonical_url,
                normalized_value=normalized,
                identity_hash=_hash(normalized),
            )

        if observation.content_identity:
            normalized = self.normalize_content_identity(
                observation.content_identity
            )

            return CandidateIdentity(
                identity_type=CandidateIdentityType.CONTENT_IDENTITY,
                identity_value=observation.content_identity,
                normalized_value=normalized,
                identity_hash=_hash(normalized),
            )

        return observation.identity

    def normalize_url(self, url: str) -> str:
        value = str(url).strip().lower()

        if value.endswith("/"):
            value = value[:-1]

        return value

    def normalize_content_identity(
        self,
        content_identity: str,
    ) -> str:
        return str(content_identity).strip().lower()


@dataclass(frozen=True)
class CandidateFusionPolicy:
    budget: CandidateFusionBudget = field(
        default_factory=CandidateFusionBudget
    )

    merge_same_document_id: bool = True
    merge_same_canonical_url: bool = True
    merge_same_content_identity: bool = True

    preserve_all_evidence: bool = True
    preserve_partition_provenance: bool = True
    preserve_replica_provenance: bool = True
    preserve_read_provenance: bool = True

    allow_partial_input: bool = True

    require_document_identity_for_exact_merge: bool = False


class CandidateFusionDeduplicationArchitecture:
    """
    Phase 11.7.

    Converts many distributed candidate observations into one unified
    retrieval candidate representation.

    Main responsibilities:
        - identity normalization
        - candidate-key generation
        - cross-partition merging
        - replica duplicate collapsing
        - evidence aggregation
        - provenance preservation
        - canonical candidate construction
        - partial-input awareness

    Final ranking remains Phase 12.
    """

    def __init__(
        self,
        backend: Optional[CandidateFusionBackend] = None,
        policy: Optional[CandidateFusionPolicy] = None,
    ) -> None:
        self.backend = backend or InMemoryCandidateFusionBackend()
        self.policy = policy or CandidateFusionPolicy()

        self._events: Dict[str, List[CandidateFusionEvent]] = {}
        self._checkpoints: Dict[
            str,
            List[CandidateFusionCheckpoint],
        ] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fuse(
        self,
        request: CandidateFusionRequest,
    ) -> CandidateFusionResult:
        identity = FusionIdentity(
            fusion_id=_new_id("fusion"),
            retrieval_id=request.retrieval_id,
            query_id=request.query_id,
            canonical_query_hash=request.canonical_query_hash,
            created_at=_now(),
        )

        result = CandidateFusionResult(
            fusion_id=identity.fusion_id,
            identity=identity,
            state=FusionState.RECEIVED,
        )

        self._emit(
            result,
            FusionEventType.FUSION_RECEIVED,
            {
                "retrieval_id": request.retrieval_id,
                "query_id": request.query_id,
                "stream_count": len(request.candidate_streams),
            },
        )

        try:
            result.state = FusionState.VALIDATING
            self._validate_request(request)

            result.state = FusionState.IDENTITY_RESOLUTION

            observations = self._materialize_observations(
                request.candidate_streams
            )

            if len(observations) > self.policy.budget.max_input_observations:
                observations = observations[
                    : self.policy.budget.max_input_observations
                ]

                result.partial_input = True

            result.input_observations = len(observations)

            resolved_observations = [
                self._resolve_observation(observation)
                for observation in observations
            ]

            self._emit(
                result,
                FusionEventType.IDENTITIES_RESOLVED,
                {
                    "observation_count": len(resolved_observations),
                },
            )

            result.state = FusionState.FUSION

            records = self._fuse_observations(
                resolved_observations
            )

            result.records = records

            self._emit(
                result,
                FusionEventType.CANDIDATES_MERGED,
                {
                    "candidate_identity_count": len(records),
                },
            )

            result.state = FusionState.DEDUPLICATION

            duplicates = self._calculate_duplicates(records)

            result.duplicates_collapsed = duplicates

            self._emit(
                result,
                FusionEventType.DUPLICATES_DETECTED,
                {
                    "duplicates_collapsed": duplicates,
                },
            )

            result.state = FusionState.EVIDENCE_AGGREGATION

            self._aggregate_evidence(records)

            result.evidence_records = sum(
                len(record.evidence)
                for record in records.values()
            )

            self._emit(
                result,
                FusionEventType.EVIDENCE_AGGREGATED,
                {
                    "evidence_records": result.evidence_records,
                },
            )

            result.state = FusionState.CONSOLIDATION

            result.candidates = self._consolidate(
                records,
                max_candidates=(
                    request.max_candidates
                    or self.policy.budget.max_unified_candidates
                ),
            )

            if result.partial_input:
                result.state = FusionState.PARTIAL

                self._emit(
                    result,
                    FusionEventType.PARTIAL_INPUT_DETECTED,
                    {
                        "input_observations": result.input_observations,
                        "candidate_count": len(result.candidates),
                    },
                )
            else:
                result.state = FusionState.COMPLETED

            result.fusion_hash = _hash(
                {
                    "identity": result.identity,
                    "candidate_ids": tuple(
                        sorted(result.candidates.keys())
                    ),
                    "duplicates": result.duplicates_collapsed,
                    "evidence": result.evidence_records,
                    "state": result.state.value,
                }
            )

            result.decision = self._build_decision(result)

            result.completed_at = _now()

            self._checkpoint(result)

            self._emit(
                result,
                FusionEventType.CANDIDATES_CONSOLIDATED,
                {
                    "candidate_count": len(result.candidates),
                    "fusion_hash": result.fusion_hash,
                },
            )

            self._emit(
                result,
                FusionEventType.FUSION_COMPLETED,
                {
                    "candidate_count": len(result.candidates),
                    "state": result.state.value,
                },
            )

            return result

        except Exception as exc:
            result.state = FusionState.FAILED

            self._emit(
                result,
                FusionEventType.FUSION_FAILED,
                {"error": str(exc)},
            )

            self._checkpoint(result)

            raise

    def events(
        self,
        fusion_id: Optional[str] = None,
    ) -> Tuple[CandidateFusionEvent, ...]:
        if fusion_id is not None:
            return tuple(self._events.get(fusion_id, ()))

        values: List[CandidateFusionEvent] = []

        for events in self._events.values():
            values.extend(events)

        return tuple(values)

    def checkpoints(
        self,
        fusion_id: Optional[str] = None,
    ) -> Tuple[CandidateFusionCheckpoint, ...]:
        if fusion_id is not None:
            return tuple(
                self._checkpoints.get(fusion_id, ())
            )

        values: List[CandidateFusionCheckpoint] = []

        for checkpoints in self._checkpoints.values():
            values.extend(checkpoints)

        return tuple(values)

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_request(
        request: CandidateFusionRequest,
    ) -> None:
        if not request.retrieval_id:
            raise ValueError("retrieval_id is required")

        if not request.query_id:
            raise ValueError("query_id is required")

        if not request.canonical_query_hash:
            raise ValueError(
                "canonical_query_hash is required"
            )

        if not isinstance(
            request.candidate_streams,
            Sequence,
        ):
            raise TypeError(
                "candidate_streams must be a sequence"
            )

    # ------------------------------------------------------------------
    # Observation materialization
    # ------------------------------------------------------------------

    def _materialize_observations(
        self,
        streams: Sequence[Mapping[str, Any]],
    ) -> List[CandidateObservation]:
        observations: List[CandidateObservation] = []

        for stream in streams:
            raw_candidates = stream.get(
                "candidates",
                (),
            )

            stream_partition = str(
                stream.get(
                    "partition_id",
                    "unknown",
                )
            )

            stream_read = str(
                stream.get(
                    "read_id",
                    "unknown",
                )
            )

            stream_replica = stream.get(
                "replica_id"
            )

            for raw in raw_candidates:
                document_id = raw.get(
                    "document_id"
                )

                canonical_url = raw.get(
                    "canonical_url"
                )

                content_identity = raw.get(
                    "content_identity"
                )

                identity_value = (
                    document_id
                    or canonical_url
                    or content_identity
                )

                if not identity_value:
                    continue

                if document_id:
                    identity_type = (
                        CandidateIdentityType.DOCUMENT_ID
                    )
                elif canonical_url:
                    identity_type = (
                        CandidateIdentityType.CANONICAL_URL
                    )
                else:
                    identity_type = (
                        CandidateIdentityType.CONTENT_IDENTITY
                    )

                identity = CandidateIdentity(
                    identity_type=identity_type,
                    identity_value=str(identity_value),
                    normalized_value=str(identity_value),
                    identity_hash=_hash(
                        str(identity_value)
                    ),
                )

                evidence = self._materialize_evidence(
                    raw=raw,
                    identity=identity,
                    partition_id=stream_partition,
                    read_id=stream_read,
                    replica_id=stream_replica,
                )

                observations.append(
                    CandidateObservation(
                        observation_id=_new_id(
                            "candidate-observation"
                        ),
                        identity=identity,
                        partition_id=stream_partition,
                        read_id=stream_read,
                        replica_id=(
                            str(stream_replica)
                            if stream_replica is not None
                            else None
                        ),
                        canonical_url=(
                            str(canonical_url)
                            if canonical_url
                            else None
                        ),
                        content_identity=(
                            str(content_identity)
                            if content_identity
                            else None
                        ),
                        terms=tuple(
                            str(value)
                            for value in raw.get(
                                "matched_terms",
                                (),
                            )
                        ),
                        fields=tuple(
                            str(value)
                            for value in raw.get(
                                "matched_fields",
                                (),
                            )
                        ),
                        phrases=tuple(
                            str(value)
                            for value in raw.get(
                                "matched_phrases",
                                (),
                            )
                        ),
                        evidence=tuple(evidence),
                        source_metadata=dict(
                            raw.get(
                                "metadata",
                                {},
                            )
                        ),
                    )
                )

                if len(observations) >= (
                    self.policy.budget.max_input_observations
                ):
                    return observations

        return observations

    def _materialize_evidence(
        self,
        raw: Mapping[str, Any],
        identity: CandidateIdentity,
        partition_id: str,
        read_id: str,
        replica_id: Optional[str],
    ) -> List[CandidateEvidence]:
        evidence: List[CandidateEvidence] = []

        for term in raw.get(
            "matched_terms",
            (),
        ):
            evidence.append(
                CandidateEvidence(
                    evidence_id=_new_id("evidence"),
                    candidate_identity=identity,
                    evidence_type=EvidenceType.TERM_MATCH,
                    strength=EvidenceStrength.MODERATE,
                    partition_id=partition_id,
                    replica_id=(
                        str(replica_id)
                        if replica_id is not None
                        else None
                    ),
                    read_id=read_id,
                    term=str(term),
                )
            )

        for field in raw.get(
            "matched_fields",
            (),
        ):
            evidence.append(
                CandidateEvidence(
                    evidence_id=_new_id("evidence"),
                    candidate_identity=identity,
                    evidence_type=EvidenceType.FIELD_MATCH,
                    strength=EvidenceStrength.STRONG,
                    partition_id=partition_id,
                    replica_id=(
                        str(replica_id)
                        if replica_id is not None
                        else None
                    ),
                    read_id=read_id,
                    field=str(field),
                )
            )

        for phrase in raw.get(
            "matched_phrases",
            (),
        ):
            evidence.append(
                CandidateEvidence(
                    evidence_id=_new_id("evidence"),
                    candidate_identity=identity,
                    evidence_type=EvidenceType.PHRASE_MATCH,
                    strength=EvidenceStrength.EXACT,
                    partition_id=partition_id,
                    replica_id=(
                        str(replica_id)
                        if replica_id is not None
                        else None
                    ),
                    read_id=read_id,
                    phrase=str(phrase),
                )
            )

        return evidence

    # ------------------------------------------------------------------
    # Identity resolution
    # ------------------------------------------------------------------

    def _resolve_observation(
        self,
        observation: CandidateObservation,
    ) -> CandidateObservation:
        identity = self.backend.resolve_identity(
            observation
        )

        canonical_url = observation.canonical_url

        if canonical_url:
            canonical_url = self.backend.normalize_url(
                canonical_url
            )

        content_identity = observation.content_identity

        if content_identity:
            content_identity = (
                self.backend.normalize_content_identity(
                    content_identity
                )
            )

        return CandidateObservation(
            observation_id=observation.observation_id,
            identity=identity,
            partition_id=observation.partition_id,
            read_id=observation.read_id,
            replica_id=observation.replica_id,
            canonical_url=canonical_url,
            content_identity=content_identity,
            terms=observation.terms,
            fields=observation.fields,
            phrases=observation.phrases,
            evidence=observation.evidence,
            source_metadata=observation.source_metadata,
        )

    # ------------------------------------------------------------------
    # Fusion
    # ------------------------------------------------------------------

    def _fuse_observations(
        self,
        observations: Sequence[CandidateObservation],
    ) -> Dict[str, CandidateFusionRecord]:
        records: Dict[str, CandidateFusionRecord] = {}

        identity_aliases: Dict[str, str] = {}

        for observation in observations:
            primary_key = self._candidate_key(
                observation
            )

            canonical_key = identity_aliases.get(
                primary_key,
                primary_key,
            )

            record = records.get(canonical_key)

            if record is None:
                record = CandidateFusionRecord(
                    identity=observation.identity,
                    state=CandidateState.RECEIVED,
                    deduplication_state=(
                        DeduplicationState.NOT_CHECKED
                    ),
                )

                records[canonical_key] = record

            record.observations.append(
                observation
            )

            record.partitions.add(
                observation.partition_id
            )

            if observation.replica_id:
                record.replicas.add(
                    observation.replica_id
                )

            record.reads.add(
                observation.read_id
            )

            record.matched_terms.update(
                observation.terms
            )

            record.matched_fields.update(
                observation.fields
            )

            record.matched_phrases.update(
                observation.phrases
            )

            if observation.canonical_url:
                record.canonical_urls.add(
                    observation.canonical_url
                )

            if observation.content_identity:
                record.content_identities.add(
                    observation.content_identity
                )

            record.evidence.extend(
                observation.evidence
            )

            record.fusion_evidence_count += len(
                observation.evidence
            )

            if len(record.observations) > 1:
                record.duplicate_observation_count += 1

            record.state = CandidateState.FUSED

            identity_aliases[
                primary_key
            ] = canonical_key

            # Secondary identity aliases allow observations from
            # different partitions to converge on one candidate.

            for alias in self._identity_aliases(
                observation
            ):
                identity_aliases.setdefault(
                    alias,
                    canonical_key,
                )

        return records

    def _candidate_key(
        self,
        observation: CandidateObservation,
    ) -> str:
        identity = observation.identity

        if (
            self.policy.merge_same_document_id
            and identity.identity_type
            == CandidateIdentityType.DOCUMENT_ID
        ):
            return (
                "document:"
                + identity.normalized_value
            )

        if (
            self.policy.merge_same_canonical_url
            and observation.canonical_url
        ):
            return (
                "url:"
                + observation.canonical_url
            )

        if (
            self.policy.merge_same_content_identity
            and observation.content_identity
        ):
            return (
                "content:"
                + observation.content_identity
            )

        return (
            identity.identity_type.value
            + ":"
            + identity.normalized_value
        )

    def _identity_aliases(
        self,
        observation: CandidateObservation,
    ) -> Tuple[str, ...]:
        aliases: List[str] = []

        if observation.canonical_url:
            aliases.append(
                "url:" + observation.canonical_url
            )

        if observation.content_identity:
            aliases.append(
                "content:"
                + observation.content_identity
            )

        if (
            observation.identity.identity_type
            == CandidateIdentityType.DOCUMENT_ID
        ):
            aliases.append(
                "document:"
                + observation.identity.normalized_value
            )

        return tuple(aliases)

    # ------------------------------------------------------------------
    # Deduplication
    # ------------------------------------------------------------------

    @staticmethod
    def _calculate_duplicates(
        records: Mapping[str, CandidateFusionRecord],
    ) -> int:
        duplicates = 0

        for record in records.values():
            if record.duplicate_observation_count > 0:
                duplicates += (
                    record.duplicate_observation_count
                )

                record.deduplication_state = (
                    DeduplicationState.DUPLICATE
                )
            else:
                record.deduplication_state = (
                    DeduplicationState.UNIQUE
                )

        return duplicates

    # ------------------------------------------------------------------
    # Evidence aggregation
    # ------------------------------------------------------------------

    def _aggregate_evidence(
        self,
        records: Mapping[str, CandidateFusionRecord],
    ) -> None:
        for record in records.values():
            if len(record.evidence) > (
                self.policy.budget.max_evidence_per_candidate
            ):
                record.evidence = record.evidence[
                    : self.policy.budget.max_evidence_per_candidate
                ]

            record.fusion_evidence_count = len(
                record.evidence
            )

            record.state = CandidateState.CONSOLIDATED

    # ------------------------------------------------------------------
    # Consolidation
    # ------------------------------------------------------------------

    def _consolidate(
        self,
        records: Mapping[str, CandidateFusionRecord],
        max_candidates: int,
    ) -> Dict[str, UnifiedCandidate]:
        candidates: Dict[str, UnifiedCandidate] = {}

        for key, record in records.items():
            if len(candidates) >= max_candidates:
                break

            document_id = None

            if (
                record.identity.identity_type
                == CandidateIdentityType.DOCUMENT_ID
            ):
                document_id = (
                    record.identity.normalized_value
                )

            canonical_url = (
                sorted(record.canonical_urls)[0]
                if record.canonical_urls
                else None
            )

            content_identity = (
                sorted(record.content_identities)[0]
                if record.content_identities
                else None
            )

            retrieval_features = {
                "partition_count": len(
                    record.partitions
                ),
                "replica_count": len(
                    record.replicas
                ),
                "read_count": len(
                    record.reads
                ),
                "matched_term_count": len(
                    record.matched_terms
                ),
                "matched_field_count": len(
                    record.matched_fields
                ),
                "matched_phrase_count": len(
                    record.matched_phrases
                ),
                "evidence_count": len(
                    record.evidence
                ),
                "duplicate_observation_count": (
                    record.duplicate_observation_count
                ),
                "cross_partition_match": (
                    len(record.partitions) > 1
                ),
                "cross_replica_match": (
                    len(record.replicas) > 1
                ),
            }

            candidates[key] = UnifiedCandidate(
                identity=record.identity,
                state=CandidateState.CONSOLIDATED,
                deduplication_state=(
                    record.deduplication_state
                ),
                document_id=document_id,
                canonical_url=canonical_url,
                content_identity=content_identity,
                partitions=tuple(
                    sorted(record.partitions)
                ),
                replicas=tuple(
                    sorted(record.replicas)
                ),
                source_reads=tuple(
                    sorted(record.reads)
                ),
                matched_terms=tuple(
                    sorted(record.matched_terms)
                ),
                matched_fields=tuple(
                    sorted(record.matched_fields)
                ),
                matched_phrases=tuple(
                    sorted(record.matched_phrases)
                ),
                evidence_count=len(
                    record.evidence
                ),
                duplicate_observation_count=(
                    record.duplicate_observation_count
                ),
                retrieval_features=retrieval_features,
            )

        return candidates

    # ------------------------------------------------------------------
    # Decision
    # ------------------------------------------------------------------

    def _build_decision(
        self,
        result: CandidateFusionResult,
    ) -> CandidateFusionDecision:
        return CandidateFusionDecision(
            fusion_id=result.fusion_id,
            state=result.state,
            input_observations=result.input_observations,
            unified_candidates=len(result.candidates),
            duplicates_collapsed=result.duplicates_collapsed,
            evidence_records=result.evidence_records,
            partial_input=result.partial_input,
            budget_applied=(
                result.input_observations
                >= self.policy.budget.max_input_observations
                or len(result.candidates)
                >= self.policy.budget.max_unified_candidates
            ),
            decision_reason=(
                "candidate fusion completed"
                if result.state == FusionState.COMPLETED
                else "candidate fusion completed with partial input"
            ),
        )

    # ------------------------------------------------------------------
    # Checkpointing / events
    # ------------------------------------------------------------------

    def _checkpoint(
        self,
        result: CandidateFusionResult,
    ) -> None:
        checkpoint = CandidateFusionCheckpoint(
            checkpoint_id=_new_id(
                "fusion-checkpoint"
            ),
            fusion_id=result.fusion_id,
            state=result.state,
            candidate_count=len(
                result.candidates
            ),
            observation_count=result.input_observations,
            duplicate_count=result.duplicates_collapsed,
        )

        self._checkpoints.setdefault(
            result.fusion_id,
            [],
        ).append(checkpoint)

        self._emit(
            result,
            FusionEventType.CHECKPOINT_CREATED,
            {
                "checkpoint_id": checkpoint.checkpoint_id,
                "candidate_count": len(
                    result.candidates
                ),
            },
        )

    def _emit(
        self,
        result: CandidateFusionResult,
        event_type: FusionEventType,
        payload: Mapping[str, Any],
    ) -> None:
        event = CandidateFusionEvent(
            event_id=_new_id("fusion-event"),
            fusion_id=result.fusion_id,
            event_type=event_type,
            state=result.state,
            timestamp=_now(),
            payload=dict(payload),
        )

        self._events.setdefault(
            result.fusion_id,
            [],
        ).append(event)

    # ------------------------------------------------------------------
    # Architecture declaration
    # ------------------------------------------------------------------

    @staticmethod
    def architecture() -> Mapping[str, Any]:
        return {
            "architecture_version": ARCHITECTURE_VERSION,
            "scale_target": SCALE_TARGET,
            "google_scale_capability_target": (
                GOOGLE_SCALE_CAPABILITY_TARGET
            ),
            "google_technology_dependency": (
                GOOGLE_TECHNOLOGY_DEPENDENCY
            ),
            "stage": "11.7",
            "name": (
                "Candidate Fusion & Deduplication"
            ),
            "input": (
                "Phase 11.6 multi-partition "
                "candidate retrieval streams"
            ),
            "output": (
                "unified distributed retrieval "
                "candidate set"
            ),
            "next_stage": (
                "11.8 Retrieval Rescoring / "
                "Early Filtering"
            ),
            "data_flow": [
                "11.6 candidate streams",
                "candidate identity resolution",
                "document identity normalization",
                "canonical URL normalization",
                "content identity normalization",
                "cross-partition fusion",
                "cross-replica duplicate collapsing",
                "evidence aggregation",
                "provenance preservation",
                "unified candidate construction",
                "11.8 retrieval rescoring and early filtering",
            ],
            "distributed_properties": [
                "partition-aware",
                "replica-aware",
                "identity-aware",
                "cross-partition fusion",
                "cross-replica deduplication",
                "evidence preserving",
                "provenance preserving",
                "partial-input aware",
                "checkpointable",
                "backend-replaceable",
                "horizontally scalable",
            ],
            "identity_layers": [
                "document identity",
                "canonical URL identity",
                "content identity",
                "resource identity",
            ],
            "deduplication_scope": [
                "same partition",
                "cross partition",
                "same replica",
                "cross replica",
                "same canonical URL",
                "same content identity",
            ],
            "preserved_provenance": [
                "partitions",
                "replicas",
                "retrieval reads",
                "matched terms",
                "matched fields",
                "matched phrases",
                "retrieval evidence",
            ],
            "no_fixed_global_limits": [
                "documents",
                "partitions",
                "replicas",
                "candidates",
                "workers",
                "public Web resources",
            ],
            "operational_budgets_are": [
                "per-request",
                "per-fusion execution",
                "safety controls",
            ],
            "not_responsible_for": [
                "final ranking",
                "final result ordering",
                "Phase 12 relevance scoring",
                "Web crawling",
                "index mutation",
                "embedding generation",
            ],
            "phase_10_dependencies": [
                "distributed inverted-index segment fabric",
                "partition-aware index routing",
                "replication and durability",
                "index recovery/checkpointing",
            ],
            "phase_11_dependencies": [
                "11.2 query normalization",
                "11.3 intent analysis",
                "11.4 term and field expansion",
                "11.5 distributed retrieval planning",
                "11.6 multi-partition candidate retrieval",
            ],
            "protected_components": [
                "website_server.py",
                "search_service/server.py",
            ],
            "dependency_policy": {
                "google_search_api": False,
                "google_index": False,
                "google_crawler": False,
                "google_infrastructure": False,
                "google_search_technology": False,
            },
        }


# Compatibility aliases.

CandidateFusionDeduplication = (
    CandidateFusionDeduplicationArchitecture
)

GlobalCandidateFusionDeduplication = (
    CandidateFusionDeduplicationArchitecture
)

Phase11_7CandidateFusionDeduplication = (
    CandidateFusionDeduplicationArchitecture
)
