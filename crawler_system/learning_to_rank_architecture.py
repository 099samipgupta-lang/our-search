"""
OUR SEARCH
Phase 12.8 — Learning-to-Rank / Ranking Model Architecture

Purpose
-------
Defines the production architecture for learning-to-rank (LTR) model
training, validation, versioning, deployment, inference, checkpointing,
lineage, and rollback at enormous public-Web scale.

Scale target
------------
Billions to trillions of public-Web resources.

Design goals
------------
- Distributed training and inference
- Feature-schema versioning
- Model versioning
- Offline/online separation
- Partition-local inference
- Batch and streaming inference
- Deterministic execution
- Checkpointable workflows
- Model rollout and rollback
- Partial feature support
- Backend-replaceable persistence
- Strong provenance and lineage
- No fixed global corpus ceiling
- No Google technology dependency

This stage does NOT:
- perform final global ranking
- select final search results
- crawl the Web
- mutate the search index
- implement spam classification
- replace Phase 11 retrieval
- replace Phase 12.7 feature fusion
- require a specific ML framework

Phase relationship
------------------
Phase 11
    Retrieval architecture
        ↓
Phase 12.1–12.6
    Ranking signals
        ↓
Phase 12.7
    Multi-signal ranking feature fusion
        ↓
Phase 12.8
    Learning-to-rank / model architecture
        ↓
Phase 12.9
    Final ranking architecture and global ranking pipeline
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from math import isfinite
from typing import Any, Dict, Iterable, List, Mapping, Optional, Protocol, Sequence, Tuple
from uuid import uuid4


# ============================================================================
# GLOBAL ARCHITECTURE CONSTANTS
# ============================================================================

SCALE_TARGET = "billions_to_trillions_of_public_web_resources"
GOOGLE_SCALE_CAPABILITY_TARGET = True
GOOGLE_TECHNOLOGY_DEPENDENCY = False

ARCHITECTURE_VERSION = "learning-to-rank-architecture.v1"
PHASE = "12.8"
NEXT_STAGE = "12.9"


# ============================================================================
# ENUMS
# ============================================================================


class LTRState(str, Enum):
    RECEIVED = "received"
    VALIDATING = "validating"
    FEATURE_VALIDATION = "feature_validation"
    TRAINING_PREPARATION = "training_preparation"
    TRAINING = "training"
    VALIDATION = "validation"
    MODEL_REGISTRATION = "model_registration"
    MODEL_STAGING = "model_staging"
    MODEL_ROLLOUT = "model_rollout"
    INFERENCE_READY = "inference_ready"
    INFERENCE = "inference"
    CHECKPOINTING = "checkpointing"
    PARTIAL = "partial"
    COMPLETED = "completed"
    REJECTED = "rejected"
    FAILED = "failed"


class LTRLearningMode(str, Enum):
    POINTWISE = "pointwise"
    PAIRWISE = "pairwise"
    LISTWISE = "listwise"


class LTRExecutionMode(str, Enum):
    OFFLINE_TRAINING = "offline_training"
    OFFLINE_EVALUATION = "offline_evaluation"
    ONLINE_INFERENCE = "online_inference"
    BATCH_INFERENCE = "batch_inference"
    STREAMING_INFERENCE = "streaming_inference"


class ModelLifecycleState(str, Enum):
    CREATED = "created"
    REGISTERED = "registered"
    VALIDATING = "validating"
    STAGED = "staged"
    CANARY = "canary"
    ACTIVE = "active"
    PAUSED = "paused"
    ROLLED_BACK = "rolled_back"
    RETIRED = "retired"
    FAILED = "failed"


class ModelType(str, Enum):
    LINEAR = "linear"
    TREE_BASED = "tree_based"
    NEURAL = "neural"
    HYBRID = "hybrid"
    ENSEMBLE = "ensemble"
    CUSTOM = "custom"


class FeatureInputState(str, Enum):
    COMPLETE = "complete"
    PARTIAL = "partial"
    INVALID = "invalid"
    MISSING = "missing"


class TrainingExampleType(str, Enum):
    POINTWISE = "pointwise"
    PAIRWISE = "pairwise"
    LISTWISE = "listwise"


class CheckpointType(str, Enum):
    TRAINING = "training"
    VALIDATION = "validation"
    MODEL_REGISTRATION = "model_registration"
    ROLLOUT = "rollout"
    INFERENCE = "inference"


class LTREventType(str, Enum):
    REQUEST_RECEIVED = "request_received"
    VALIDATION_STARTED = "validation_started"
    FEATURES_VALIDATED = "features_validated"
    TRAINING_STARTED = "training_started"
    TRAINING_CHECKPOINT_CREATED = "training_checkpoint_created"
    TRAINING_COMPLETED = "training_completed"
    VALIDATION_STARTED_EVENT = "validation_started_event"
    VALIDATION_COMPLETED = "validation_completed"
    MODEL_REGISTERED = "model_registered"
    MODEL_STAGED = "model_staged"
    MODEL_ROLLOUT_STARTED = "model_rollout_started"
    MODEL_ROLLOUT_COMPLETED = "model_rollout_completed"
    MODEL_ROLLBACK = "model_rollback"
    INFERENCE_STARTED = "inference_started"
    INFERENCE_COMPLETED = "inference_completed"
    PARTIAL_INPUT_DETECTED = "partial_input_detected"
    CHECKPOINT_CREATED = "checkpoint_created"
    REQUEST_COMPLETED = "request_completed"
    REQUEST_REJECTED = "request_rejected"
    REQUEST_FAILED = "request_failed"


class MetricType(str, Enum):
    NDCG = "ndcg"
    MRR = "mrr"
    MAP = "map"
    PRECISION = "precision"
    RECALL = "recall"
    PAIRWISE_ACCURACY = "pairwise_accuracy"
    LOG_LOSS = "log_loss"
    CALIBRATION = "calibration"
    CUSTOM = "custom"


# ============================================================================
# CORE IDENTITIES
# ============================================================================


@dataclass(frozen=True)
class LTRIdentity:
    request_id: str
    query_id: str
    model_request_id: str = ""
    document_id: str = ""
    canonical_url: str = ""

    def stable_key(self) -> str:
        return "|".join(
            (
                self.query_id,
                self.document_id,
                self.canonical_url,
                self.model_request_id,
            )
        )


@dataclass(frozen=True)
class FeatureSchemaIdentity:
    schema_id: str
    schema_version: str
    feature_count: int
    feature_names: Tuple[str, ...]

    def fingerprint(self) -> str:
        payload = (
            self.schema_id,
            self.schema_version,
            str(self.feature_count),
            ",".join(self.feature_names),
        )
        return "|".join(payload)


@dataclass(frozen=True)
class ModelIdentity:
    model_id: str
    model_version: str
    model_type: ModelType
    feature_schema_id: str
    feature_schema_version: str

    def stable_key(self) -> str:
        return "|".join(
            (
                self.model_id,
                self.model_version,
                self.model_type.value,
                self.feature_schema_id,
                self.feature_schema_version,
            )
        )


@dataclass(frozen=True)
class LTRLineage:
    retrieval_stage: str = "phase11-retrieval-architecture"
    retrieval_version: str = ""
    feature_fusion_stage: str = (
        "phase12-multi-signal-ranking-feature-fusion"
    )
    feature_fusion_version: str = ""
    signal_stage: str = "phase12-ranking-signals"
    signal_version: str = ""
    feature_schema_id: str = ""
    feature_schema_version: str = ""
    model_id: str = ""
    model_version: str = ""
    training_dataset_id: str = ""
    training_dataset_version: str = ""
    observed_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def fingerprint(self) -> str:
        return "|".join(
            (
                self.retrieval_stage,
                self.retrieval_version,
                self.feature_fusion_stage,
                self.feature_fusion_version,
                self.signal_stage,
                self.signal_version,
                self.feature_schema_id,
                self.feature_schema_version,
                self.model_id,
                self.model_version,
                self.training_dataset_id,
                self.training_dataset_version,
                self.observed_at.isoformat(),
            )
        )


# ============================================================================
# FEATURE REPRESENTATION
# ============================================================================


@dataclass(frozen=True)
class LTRFeatureValue:
    feature_id: str
    name: str
    value: float
    confidence: float = 1.0
    state: FeatureInputState = FeatureInputState.COMPLETE
    source: str = ""
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def normalized_value(self) -> float:
        if not isfinite(self.value):
            return 0.0

        if self.value < 0.0:
            return 0.0

        if self.value > 1.0:
            return 1.0

        return self.value


@dataclass(frozen=True)
class LTRFeatureVector:
    query_id: str
    document_id: str
    canonical_url: str
    schema: FeatureSchemaIdentity
    features: Tuple[LTRFeatureValue, ...]
    partial: bool = False
    partition_id: str = ""
    shard_id: str = ""
    observed_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def feature_map(self) -> Dict[str, float]:
        return {
            feature.name: feature.normalized_value()
            for feature in self.features
        }

    def feature_count(self) -> int:
        return len(self.features)


# ============================================================================
# TRAINING DATA REPRESENTATION
# ============================================================================


@dataclass(frozen=True)
class RelevanceJudgment:
    query_id: str
    document_id: str
    relevance: float
    assessor: str = ""
    source: str = ""
    confidence: float = 1.0
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class LTRTrainingExample:
    example_id: str
    example_type: TrainingExampleType
    query_id: str
    feature_vectors: Tuple[LTRFeatureVector, ...]
    judgments: Tuple[RelevanceJudgment, ...]
    weight: float = 1.0
    partial: bool = False
    dataset_id: str = ""
    dataset_version: str = ""
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def stable_key(self) -> str:
        return "|".join(
            (
                self.dataset_id,
                self.dataset_version,
                self.example_id,
                self.query_id,
            )
        )


@dataclass(frozen=True)
class LTRTrainingDataset:
    dataset_id: str
    dataset_version: str
    examples: Tuple[LTRTrainingExample, ...]
    feature_schema: FeatureSchemaIdentity
    learning_mode: LTRLearningMode
    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def example_count(self) -> int:
        return len(self.examples)


# ============================================================================
# MODEL REPRESENTATION
# ============================================================================


@dataclass(frozen=True)
class ModelHyperparameters:
    values: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ModelArtifact:
    model_identity: ModelIdentity
    artifact_uri: str
    artifact_digest: str
    artifact_size_bytes: int = 0
    framework: str = ""
    runtime: str = ""
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ModelEvaluation:
    model_id: str
    model_version: str
    metrics: Mapping[str, float]
    primary_metric: str = ""
    evaluated_examples: int = 0
    evaluation_dataset_id: str = ""
    evaluation_dataset_version: str = ""
    completed_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass
class RankingModel:
    identity: ModelIdentity
    lifecycle_state: ModelLifecycleState
    hyperparameters: ModelHyperparameters
    artifact: Optional[ModelArtifact] = None
    evaluation: Optional[ModelEvaluation] = None
    lineage: Optional[LTRLineage] = None
    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    updated_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    metadata: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# INFERENCE REPRESENTATION
# ============================================================================


@dataclass(frozen=True)
class LTRInferenceRequest:
    identity: LTRIdentity
    feature_vector: LTRFeatureVector
    model_identity: ModelIdentity
    execution_mode: LTRExecutionMode = LTRExecutionMode.ONLINE_INFERENCE
    partial: bool = False
    timeout_ms: int = 1500
    partition_id: str = ""
    shard_id: str = ""
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class LTRInferenceScore:
    query_id: str
    document_id: str
    canonical_url: str
    model_id: str
    model_version: str
    score: float
    confidence: float
    partial: bool
    feature_count: int
    observed_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class LTRInferenceResult:
    request_id: str
    scores: Tuple[LTRInferenceScore, ...]
    model_identity: ModelIdentity
    partial: bool = False
    completed_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    metadata: Mapping[str, Any] = field(default_factory=dict)


# ============================================================================
# CHECKPOINTS
# ============================================================================


@dataclass(frozen=True)
class LTRCheckpoint:
    checkpoint_id: str
    checkpoint_type: CheckpointType
    request_id: str
    state: LTRState
    model_id: str = ""
    model_version: str = ""
    dataset_id: str = ""
    dataset_version: str = ""
    partition_id: str = ""
    shard_id: str = ""
    progress: float = 0.0
    artifact_uri: str = ""
    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    metadata: Mapping[str, Any] = field(default_factory=dict)


# ============================================================================
# EVENTS
# ============================================================================


@dataclass(frozen=True)
class LTREvent:
    event_id: str
    event_type: LTREventType
    request_id: str
    state: LTRState
    message: str = ""
    model_id: str = ""
    model_version: str = ""
    query_id: str = ""
    document_id: str = ""
    partition_id: str = ""
    shard_id: str = ""
    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    metadata: Mapping[str, Any] = field(default_factory=dict)


# ============================================================================
# POLICY
# ============================================================================


@dataclass(frozen=True)
class LearningToRankPolicy:
    # Logical limits are deliberately per-request / per-worker / per-batch,
    # not global Web-scale corpus ceilings.
    max_features_per_vector: int = 2048
    max_training_examples_per_batch: int = 100000
    max_models_per_request: int = 32
    max_inference_vectors_per_batch: int = 100000
    max_hyperparameters: int = 1024
    max_metrics: int = 256

    minimum_feature_confidence: float = 0.10
    minimum_training_weight: float = 0.01
    minimum_inference_confidence: float = 0.10

    allow_partial_features: bool = True
    allow_partial_training_examples: bool = True
    allow_partial_inference: bool = True

    deterministic_inference: bool = True
    checkpoint_training: bool = True
    checkpoint_inference: bool = True

    enable_model_rollout: bool = True
    enable_model_rollback: bool = True

    default_timeout_ms: int = 1500

    max_parallel_training_partitions: int = 256
    max_parallel_inference_partitions: int = 256


# ============================================================================
# BACKEND CONTRACT
# ============================================================================


class LearningToRankBackend(Protocol):
    def persist_event(self, event: LTREvent) -> None:
        ...

    def persist_checkpoint(self, checkpoint: LTRCheckpoint) -> None:
        ...

    def persist_model(self, model: RankingModel) -> None:
        ...

    def persist_evaluation(self, evaluation: ModelEvaluation) -> None:
        ...

    def persist_inference_result(
        self,
        result: LTRInferenceResult,
    ) -> None:
        ...

    def get_model(
        self,
        model_id: str,
        model_version: str,
    ) -> Optional[RankingModel]:
        ...

    def get_inference_result(
        self,
        request_id: str,
    ) -> Optional[LTRInferenceResult]:
        ...


# ============================================================================
# IN-MEMORY METADATA BACKEND
# ============================================================================


class InMemoryLearningToRankMetadata:
    """
    Development/reference backend.

    This backend is intentionally replaceable. Production deployment should
    use distributed metadata, model registry, checkpoint storage, artifact
    storage, and telemetry infrastructure appropriate to the deployment.
    """

    def __init__(self) -> None:
        self._events: List[LTREvent] = []
        self._checkpoints: List[LTRCheckpoint] = []
        self._models: Dict[Tuple[str, str], RankingModel] = {}
        self._evaluations: Dict[Tuple[str, str], ModelEvaluation] = {}
        self._inference_results: Dict[str, LTRInferenceResult] = {}

    def persist_event(self, event: LTREvent) -> None:
        self._events.append(event)

    def persist_checkpoint(self, checkpoint: LTRCheckpoint) -> None:
        self._checkpoints.append(checkpoint)

    def persist_model(self, model: RankingModel) -> None:
        key = (
            model.identity.model_id,
            model.identity.model_version,
        )
        self._models[key] = model

    def persist_evaluation(
        self,
        evaluation: ModelEvaluation,
    ) -> None:
        key = (
            evaluation.model_id,
            evaluation.model_version,
        )
        self._evaluations[key] = evaluation

    def persist_inference_result(
        self,
        result: LTRInferenceResult,
    ) -> None:
        self._inference_results[result.request_id] = result

    def get_model(
        self,
        model_id: str,
        model_version: str,
    ) -> Optional[RankingModel]:
        return self._models.get((model_id, model_version))

    def get_inference_result(
        self,
        request_id: str,
    ) -> Optional[LTRInferenceResult]:
        return self._inference_results.get(request_id)

    def events(self) -> Tuple[LTREvent, ...]:
        return tuple(self._events)

    def checkpoints(self) -> Tuple[LTRCheckpoint, ...]:
        return tuple(self._checkpoints)

    def models(self) -> Tuple[RankingModel, ...]:
        return tuple(self._models.values())


# ============================================================================
# ARCHITECTURE
# ============================================================================


class LearningToRankArchitecture:
    """
    Phase 12.8 learning-to-rank architecture.

    The class defines the contracts and orchestration boundaries around LTR.
    It deliberately does not force a particular machine-learning framework.

    The production architecture can map these interfaces onto distributed
    training clusters, model registries, object storage, feature stores,
    inference workers, telemetry systems, and global deployment control
    planes.
    """

    def __init__(
        self,
        backend: Optional[LearningToRankBackend] = None,
        policy: Optional[LearningToRankPolicy] = None,
    ) -> None:
        self.backend = backend or InMemoryLearningToRankMetadata()
        self.policy = policy or LearningToRankPolicy()

    # ------------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------------

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
        if not isfinite(value):
            return lower

        if value < lower:
            return lower

        if value > upper:
            return upper

        return value

    @staticmethod
    def _safe_float(value: Any) -> float:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return 0.0

        if not isfinite(numeric):
            return 0.0

        return numeric

    # ------------------------------------------------------------------------
    # Event / checkpoint creation
    # ------------------------------------------------------------------------

    def _event(
        self,
        event_type: LTREventType,
        request_id: str,
        state: LTRState,
        message: str = "",
        *,
        model_id: str = "",
        model_version: str = "",
        query_id: str = "",
        document_id: str = "",
        partition_id: str = "",
        shard_id: str = "",
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> LTREvent:
        event = LTREvent(
            event_id=str(uuid4()),
            event_type=event_type,
            request_id=request_id,
            state=state,
            message=message,
            model_id=model_id,
            model_version=model_version,
            query_id=query_id,
            document_id=document_id,
            partition_id=partition_id,
            shard_id=shard_id,
            metadata=dict(metadata or {}),
        )

        self.backend.persist_event(event)
        return event

    def _checkpoint(
        self,
        checkpoint_type: CheckpointType,
        request_id: str,
        state: LTRState,
        *,
        model_id: str = "",
        model_version: str = "",
        dataset_id: str = "",
        dataset_version: str = "",
        partition_id: str = "",
        shard_id: str = "",
        progress: float = 0.0,
        artifact_uri: str = "",
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> LTRCheckpoint:
        checkpoint = LTRCheckpoint(
            checkpoint_id=str(uuid4()),
            checkpoint_type=checkpoint_type,
            request_id=request_id,
            state=state,
            model_id=model_id,
            model_version=model_version,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            partition_id=partition_id,
            shard_id=shard_id,
            progress=self._clamp(progress),
            artifact_uri=artifact_uri,
            metadata=dict(metadata or {}),
        )

        self.backend.persist_checkpoint(checkpoint)
        return checkpoint

    # ------------------------------------------------------------------------
    # Feature validation
    # ------------------------------------------------------------------------

    def validate_feature_schema(
        self,
        schema: FeatureSchemaIdentity,
    ) -> None:
        if not schema.schema_id:
            raise ValueError("feature schema id is required")

        if not schema.schema_version:
            raise ValueError("feature schema version is required")

        if schema.feature_count < 0:
            raise ValueError("feature count cannot be negative")

        if schema.feature_count != len(schema.feature_names):
            raise ValueError(
                "feature_count must match the number of feature_names"
            )

        if schema.feature_count > self.policy.max_features_per_vector:
            raise ValueError(
                "feature schema exceeds the configured per-vector feature limit"
            )

        if len(set(schema.feature_names)) != len(schema.feature_names):
            raise ValueError("feature names must be unique")

    def validate_feature_vector(
        self,
        vector: LTRFeatureVector,
    ) -> FeatureInputState:
        self.validate_feature_schema(vector.schema)

        if vector.feature_count() > self.policy.max_features_per_vector:
            return FeatureInputState.INVALID

        schema_names = set(vector.schema.feature_names)

        seen_names: set[str] = set()
        partial = vector.partial

        for feature in vector.features:
            if not feature.feature_id:
                return FeatureInputState.INVALID

            if not feature.name:
                return FeatureInputState.INVALID

            if feature.name not in schema_names:
                return FeatureInputState.INVALID

            if feature.name in seen_names:
                return FeatureInputState.INVALID

            seen_names.add(feature.name)

            if not isfinite(feature.confidence):
                return FeatureInputState.INVALID

            if feature.confidence < 0.0:
                return FeatureInputState.INVALID

            if feature.state == FeatureInputState.INVALID:
                return FeatureInputState.INVALID

            if feature.state in (
                FeatureInputState.PARTIAL,
                FeatureInputState.MISSING,
            ):
                partial = True

        if partial:
            return FeatureInputState.PARTIAL

        return FeatureInputState.COMPLETE

    # ------------------------------------------------------------------------
    # Training dataset validation
    # ------------------------------------------------------------------------

    def validate_training_dataset(
        self,
        dataset: LTRTrainingDataset,
    ) -> None:
        self.validate_feature_schema(dataset.feature_schema)

        if not dataset.dataset_id:
            raise ValueError("training dataset id is required")

        if not dataset.dataset_version:
            raise ValueError("training dataset version is required")

        if len(dataset.examples) > self.policy.max_training_examples_per_batch:
            raise ValueError(
                "training batch exceeds configured batch limit"
            )

        for example in dataset.examples:
            if not example.example_id:
                raise ValueError("training example id is required")

            if example.weight < self.policy.minimum_training_weight:
                raise ValueError(
                    "training example weight is below the configured minimum"
                )

            if not example.feature_vectors:
                raise ValueError(
                    "training example must contain feature vectors"
                )

            for vector in example.feature_vectors:
                state = self.validate_feature_vector(vector)

                if state == FeatureInputState.INVALID:
                    raise ValueError(
                        "training example contains invalid feature vector"
                    )

                if (
                    state == FeatureInputState.PARTIAL
                    and not self.policy.allow_partial_training_examples
                ):
                    raise ValueError(
                        "partial training examples are disabled by policy"
                    )

    # ------------------------------------------------------------------------
    # Model validation
    # ------------------------------------------------------------------------

    def validate_model(
        self,
        model: RankingModel,
    ) -> None:
        if not model.identity.model_id:
            raise ValueError("model id is required")

        if not model.identity.model_version:
            raise ValueError("model version is required")

        if not model.identity.feature_schema_id:
            raise ValueError("model feature schema id is required")

        if not model.identity.feature_schema_version:
            raise ValueError(
                "model feature schema version is required"
            )

        if len(model.hyperparameters.values) > self.policy.max_hyperparameters:
            raise ValueError(
                "model has too many hyperparameters"
            )

    # ------------------------------------------------------------------------
    # Training plan
    # ------------------------------------------------------------------------

    def build_training_plan(
        self,
        dataset: LTRTrainingDataset,
        *,
        model_type: ModelType = ModelType.TREE_BASED,
        hyperparameters: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        self.validate_training_dataset(dataset)

        hyperparameters = dict(hyperparameters or {})

        if len(hyperparameters) > self.policy.max_hyperparameters:
            raise ValueError(
                "training plan contains too many hyperparameters"
            )

        partitions = max(1, min(len(dataset.examples), 256))

        return {
            "architecture_version": ARCHITECTURE_VERSION,
            "phase": PHASE,
            "learning_mode": dataset.learning_mode.value,
            "execution_mode": LTRExecutionMode.OFFLINE_TRAINING.value,
            "model_type": model_type.value,
            "dataset_id": dataset.dataset_id,
            "dataset_version": dataset.dataset_version,
            "feature_schema_id": dataset.feature_schema.schema_id,
            "feature_schema_version": dataset.feature_schema.schema_version,
            "example_count": dataset.example_count(),
            "distributed": True,
            "partition_local_training": True,
            "partition_count_hint": partitions,
            "data_parallel_training": True,
            "checkpointable": self.policy.checkpoint_training,
            "incremental_training_supported": True,
            "streaming_dataset_ingestion_supported": True,
            "global_parameter_synchronization": True,
            "deterministic_feature_schema": True,
            "model_registry_required": True,
            "artifact_storage_required": True,
            "offline_online_separation": True,
            "no_global_corpus_ceiling": True,
            "google_technology_dependency": False,
            "hyperparameters": hyperparameters,
        }

    # ------------------------------------------------------------------------
    # Model creation
    # ------------------------------------------------------------------------

    def create_model(
        self,
        *,
        model_id: str,
        model_version: str,
        model_type: ModelType,
        feature_schema: FeatureSchemaIdentity,
        hyperparameters: Optional[Mapping[str, Any]] = None,
        lineage: Optional[LTRLineage] = None,
    ) -> RankingModel:
        self.validate_feature_schema(feature_schema)

        identity = ModelIdentity(
            model_id=model_id,
            model_version=model_version,
            model_type=model_type,
            feature_schema_id=feature_schema.schema_id,
            feature_schema_version=feature_schema.schema_version,
        )

        model = RankingModel(
            identity=identity,
            lifecycle_state=ModelLifecycleState.CREATED,
            hyperparameters=ModelHyperparameters(
                values=dict(hyperparameters or {})
            ),
            lineage=lineage,
        )

        self.validate_model(model)

        self.backend.persist_model(model)

        return model

    # ------------------------------------------------------------------------
    # Model lifecycle
    # ------------------------------------------------------------------------

    def register_model(
        self,
        model: RankingModel,
        artifact: ModelArtifact,
    ) -> RankingModel:
        self.validate_model(model)

        if artifact.model_identity.stable_key() != model.identity.stable_key():
            raise ValueError(
                "model artifact identity does not match model identity"
            )

        model.artifact = artifact
        model.lifecycle_state = ModelLifecycleState.REGISTERED
        model.updated_at = self._now()

        self.backend.persist_model(model)

        return model

    def stage_model(
        self,
        model: RankingModel,
    ) -> RankingModel:
        if model.lifecycle_state not in (
            ModelLifecycleState.REGISTERED,
            ModelLifecycleState.VALIDATING,
        ):
            raise ValueError(
                "only registered or validating models can be staged"
            )

        model.lifecycle_state = ModelLifecycleState.STAGED
        model.updated_at = self._now()

        self.backend.persist_model(model)

        return model

    def activate_model(
        self,
        model: RankingModel,
    ) -> RankingModel:
        if model.lifecycle_state not in (
            ModelLifecycleState.STAGED,
            ModelLifecycleState.CANARY,
        ):
            raise ValueError(
                "only staged or canary models can become active"
            )

        model.lifecycle_state = ModelLifecycleState.ACTIVE
        model.updated_at = self._now()

        self.backend.persist_model(model)

        return model

    def rollback_model(
        self,
        model: RankingModel,
    ) -> RankingModel:
        if not self.policy.enable_model_rollback:
            raise ValueError("model rollback is disabled")

        model.lifecycle_state = ModelLifecycleState.ROLLED_BACK
        model.updated_at = self._now()

        self.backend.persist_model(model)

        return model

    def retire_model(
        self,
        model: RankingModel,
    ) -> RankingModel:
        model.lifecycle_state = ModelLifecycleState.RETIRED
        model.updated_at = self._now()

        self.backend.persist_model(model)

        return model

    # ------------------------------------------------------------------------
    # Model evaluation
    # ------------------------------------------------------------------------

    def evaluate_model(
        self,
        model: RankingModel,
        evaluation_dataset: LTRTrainingDataset,
        metrics: Mapping[str, float],
        *,
        primary_metric: str = "",
    ) -> ModelEvaluation:
        self.validate_model(model)
        self.validate_training_dataset(evaluation_dataset)

        normalized_metrics: Dict[str, float] = {}

        if len(metrics) > self.policy.max_metrics:
            raise ValueError("too many evaluation metrics")

        for name, value in metrics.items():
            numeric = self._safe_float(value)
            normalized_metrics[name] = numeric

        evaluation = ModelEvaluation(
            model_id=model.identity.model_id,
            model_version=model.identity.model_version,
            metrics=normalized_metrics,
            primary_metric=primary_metric,
            evaluated_examples=evaluation_dataset.example_count(),
            evaluation_dataset_id=evaluation_dataset.dataset_id,
            evaluation_dataset_version=evaluation_dataset.dataset_version,
        )

        model.evaluation = evaluation
        model.updated_at = self._now()

        self.backend.persist_evaluation(evaluation)
        self.backend.persist_model(model)

        return evaluation

    # ------------------------------------------------------------------------
    # Model rollout plan
    # ------------------------------------------------------------------------

    def build_rollout_plan(
        self,
        model: RankingModel,
        *,
        canary_percentage: float = 1.0,
        rollout_percentage: float = 100.0,
        rollback_on_failure: bool = True,
    ) -> Dict[str, Any]:
        self.validate_model(model)

        canary_percentage = self._clamp(
            canary_percentage / 100.0
        ) * 100.0

        rollout_percentage = self._clamp(
            rollout_percentage / 100.0
        ) * 100.0

        return {
            "model_id": model.identity.model_id,
            "model_version": model.identity.model_version,
            "feature_schema_id": model.identity.feature_schema_id,
            "feature_schema_version": model.identity.feature_schema_version,
            "canary_percentage": canary_percentage,
            "rollout_percentage": rollout_percentage,
            "rollback_on_failure": rollback_on_failure,
            "supports_global_rollout": True,
            "supports_region_rollout": True,
            "supports_zone_rollout": True,
            "supports_partition_rollout": True,
            "supports_shard_rollout": True,
            "supports_online_rollback": True,
            "supports_previous_model_recovery": True,
            "deterministic_assignment_supported": True,
        }

    # ------------------------------------------------------------------------
    # Feature vector preparation for inference
    # ------------------------------------------------------------------------

    def prepare_inference_vector(
        self,
        vector: LTRFeatureVector,
    ) -> Tuple[Dict[str, float], float, bool]:
        state = self.validate_feature_vector(vector)

        if state == FeatureInputState.INVALID:
            raise ValueError("invalid inference feature vector")

        if (
            state == FeatureInputState.PARTIAL
            and not self.policy.allow_partial_inference
        ):
            raise ValueError("partial inference is disabled")

        values: Dict[str, float] = {}
        confidence_values: List[float] = []
        partial = vector.partial or state == FeatureInputState.PARTIAL

        for feature in vector.features:
            normalized = feature.normalized_value()

            confidence = self._clamp(
                self._safe_float(feature.confidence)
            )

            if confidence < self.policy.minimum_feature_confidence:
                partial = True

            values[feature.name] = normalized
            confidence_values.append(confidence)

        if confidence_values:
            confidence = sum(confidence_values) / len(confidence_values)
        else:
            confidence = 0.0
            partial = True

        return values, confidence, partial

    # ------------------------------------------------------------------------
    # Deterministic reference scorer
    # ------------------------------------------------------------------------

    def _reference_score(
        self,
        feature_values: Mapping[str, float],
    ) -> float:
        """
        Deterministic framework-neutral reference scorer.

        This is NOT the production learned model.

        It exists only to make the architecture executable without coupling
        Phase 12.8 to a specific ML framework.

        Production implementations should replace this method with a model
        runtime adapter loaded from a registered ModelArtifact.
        """

        if not feature_values:
            return 0.0

        values = [
            self._clamp(self._safe_float(value))
            for value in feature_values.values()
        ]

        return sum(values) / len(values)

    # ------------------------------------------------------------------------
    # Single inference
    # ------------------------------------------------------------------------

    def infer(
        self,
        request: LTRInferenceRequest,
    ) -> LTRInferenceScore:
        request_id = request.identity.request_id

        self._event(
            LTREventType.INFERENCE_STARTED,
            request_id,
            LTRState.INFERENCE,
            model_id=request.model_identity.model_id,
            model_version=request.model_identity.model_version,
            query_id=request.identity.query_id,
            document_id=request.identity.document_id,
            partition_id=request.partition_id,
            shard_id=request.shard_id,
        )

        model = self.backend.get_model(
            request.model_identity.model_id,
            request.model_identity.model_version,
        )

        if model is None:
            self._event(
                LTREventType.REQUEST_FAILED,
                request_id,
                LTRState.FAILED,
                message="model not found",
                model_id=request.model_identity.model_id,
                model_version=request.model_identity.model_version,
            )
            raise ValueError("model not found")

        if model.lifecycle_state != ModelLifecycleState.ACTIVE:
            raise ValueError("model is not active")

        if (
            model.identity.feature_schema_id
            != request.feature_vector.schema.schema_id
        ):
            raise ValueError("feature schema id mismatch")

        if (
            model.identity.feature_schema_version
            != request.feature_vector.schema.schema_version
        ):
            raise ValueError("feature schema version mismatch")

        feature_values, confidence, partial = (
            self.prepare_inference_vector(
                request.feature_vector
            )
        )

        if confidence < self.policy.minimum_inference_confidence:
            partial = True

        score = self._reference_score(feature_values)

        result = LTRInferenceScore(
            query_id=request.identity.query_id,
            document_id=request.identity.document_id,
            canonical_url=request.identity.canonical_url,
            model_id=model.identity.model_id,
            model_version=model.identity.model_version,
            score=self._clamp(score),
            confidence=self._clamp(confidence),
            partial=partial,
            feature_count=len(feature_values),
        )

        if partial:
            self._event(
                LTREventType.PARTIAL_INPUT_DETECTED,
                request_id,
                LTRState.PARTIAL,
                message="partial feature input used during inference",
                model_id=model.identity.model_id,
                model_version=model.identity.model_version,
                query_id=request.identity.query_id,
                document_id=request.identity.document_id,
            )

        self._event(
            LTREventType.INFERENCE_COMPLETED,
            request_id,
            LTRState.INFERENCE,
            model_id=model.identity.model_id,
            model_version=model.identity.model_version,
            query_id=request.identity.query_id,
            document_id=request.identity.document_id,
        )

        return result

    # ------------------------------------------------------------------------
    # Batch inference
    # ------------------------------------------------------------------------

    def infer_many(
        self,
        requests: Sequence[LTRInferenceRequest],
    ) -> LTRInferenceResult:
        if not requests:
            raise ValueError("at least one inference request is required")

        if len(requests) > self.policy.max_inference_vectors_per_batch:
            raise ValueError(
                "inference batch exceeds configured limit"
            )

        request_id = requests[0].identity.model_request_id or str(uuid4())

        scores: List[LTRInferenceScore] = []
        partial = False

        for request in requests:
            score = self.infer(request)
            scores.append(score)

            if score.partial:
                partial = True

        model_identity = requests[0].model_identity

        result = LTRInferenceResult(
            request_id=request_id,
            scores=tuple(scores),
            model_identity=model_identity,
            partial=partial,
        )

        self.backend.persist_inference_result(result)

        if self.policy.checkpoint_inference:
            self._checkpoint(
                CheckpointType.INFERENCE,
                request_id,
                LTRState.COMPLETED,
                model_id=model_identity.model_id,
                model_version=model_identity.model_version,
                progress=1.0,
                metadata={
                    "score_count": len(scores),
                    "partial": partial,
                },
            )

        return result

    # ------------------------------------------------------------------------
    # Distributed inference plan
    # ------------------------------------------------------------------------

    def build_inference_plan(
        self,
        requests: Sequence[LTRInferenceRequest],
    ) -> Dict[str, Any]:
        if not requests:
            raise ValueError("inference requests are required")

        if len(requests) > self.policy.max_inference_vectors_per_batch:
            raise ValueError(
                "inference batch exceeds configured limit"
            )

        partitions = {
            request.partition_id
            for request in requests
            if request.partition_id
        }

        shards = {
            request.shard_id
            for request in requests
            if request.shard_id
        }

        return {
            "execution_mode": LTRExecutionMode.ONLINE_INFERENCE.value,
            "request_count": len(requests),
            "partition_count": len(partitions) or 1,
            "shard_count": len(shards) or 1,
            "partition_local_inference": True,
            "distributed_inference": True,
            "horizontal_scaling": True,
            "batch_inference_supported": True,
            "streaming_inference_supported": True,
            "deterministic_inference": self.policy.deterministic_inference,
            "model_artifact_locality_supported": True,
            "replica_aware_model_loading": True,
            "model_cache_supported": True,
            "partial_results_supported": self.policy.allow_partial_inference,
            "checkpointable": self.policy.checkpoint_inference,
            "no_global_document_ceiling": True,
            "no_global_query_ceiling": True,
        }

    # ------------------------------------------------------------------------
    # Training orchestration
    # ------------------------------------------------------------------------

    def start_training(
        self,
        dataset: LTRTrainingDataset,
        *,
        model_id: str,
        model_version: str,
        model_type: ModelType = ModelType.TREE_BASED,
        hyperparameters: Optional[Mapping[str, Any]] = None,
        lineage: Optional[LTRLineage] = None,
    ) -> RankingModel:
        request_id = str(uuid4())

        self._event(
            LTREventType.REQUEST_RECEIVED,
            request_id,
            LTRState.RECEIVED,
            message="learning-to-rank training request received",
        )

        self._event(
            LTREventType.VALIDATION_STARTED,
            request_id,
            LTRState.VALIDATING,
        )

        self.validate_training_dataset(dataset)

        self._event(
            LTREventType.FEATURES_VALIDATED,
            request_id,
            LTRState.FEATURE_VALIDATION,
            message="training feature schema validated",
        )

        self._event(
            LTREventType.TRAINING_STARTED,
            request_id,
            LTRState.TRAINING,
            model_id=model_id,
            model_version=model_version,
        )

        model = self.create_model(
            model_id=model_id,
            model_version=model_version,
            model_type=model_type,
            feature_schema=dataset.feature_schema,
            hyperparameters=hyperparameters,
            lineage=lineage,
        )

        if self.policy.checkpoint_training:
            self._checkpoint(
                CheckpointType.TRAINING,
                request_id,
                LTRState.TRAINING,
                model_id=model_id,
                model_version=model_version,
                dataset_id=dataset.dataset_id,
                dataset_version=dataset.dataset_version,
                progress=0.0,
                metadata={
                    "example_count": dataset.example_count(),
                },
            )

        # --------------------------------------------------------------------
        # IMPORTANT:
        # This architecture does not pretend that a framework-neutral Python
        # average is a trained production LTR model.
        #
        # A production implementation connects here to distributed model
        # training infrastructure.
        # --------------------------------------------------------------------

        artifact = ModelArtifact(
            model_identity=model.identity,
            artifact_uri=(
                f"model://{model_id}/{model_version}"
            ),
            artifact_digest=str(uuid4()),
            artifact_size_bytes=0,
            framework="framework-neutral",
            runtime="architecture-reference-runtime",
            metadata={
                "training_state": "architecture_defined",
                "production_training_adapter_required": True,
            },
        )

        model = self.register_model(model, artifact)

        if self.policy.checkpoint_training:
            self._checkpoint(
                CheckpointType.TRAINING,
                request_id,
                LTRState.TRAINING,
                model_id=model_id,
                model_version=model_version,
                dataset_id=dataset.dataset_id,
                dataset_version=dataset.dataset_version,
                progress=1.0,
                artifact_uri=artifact.artifact_uri,
            )

        self._event(
            LTREventType.TRAINING_COMPLETED,
            request_id,
            LTRState.MODEL_REGISTRATION,
            model_id=model_id,
            model_version=model_version,
        )

        return model

    # ------------------------------------------------------------------------
    # Training lifecycle orchestration
    # ------------------------------------------------------------------------

    def complete_training_lifecycle(
        self,
        model: RankingModel,
        *,
        evaluation_dataset: Optional[LTRTrainingDataset] = None,
        metrics: Optional[Mapping[str, float]] = None,
        primary_metric: str = "",
        canary_percentage: float = 1.0,
        rollout_percentage: float = 100.0,
    ) -> RankingModel:
        request_id = str(uuid4())

        self._event(
            LTREventType.MODEL_STAGED,
            request_id,
            LTRState.MODEL_STAGING,
            model_id=model.identity.model_id,
            model_version=model.identity.model_version,
        )

        model = self.stage_model(model)

        if evaluation_dataset is not None:
            self._event(
                LTREventType.VALIDATION_STARTED_EVENT,
                request_id,
                LTRState.VALIDATION,
                model_id=model.identity.model_id,
                model_version=model.identity.model_version,
            )

            evaluation = self.evaluate_model(
                model,
                evaluation_dataset,
                metrics or {},
                primary_metric=primary_metric,
            )

            self._event(
                LTREventType.VALIDATION_COMPLETED,
                request_id,
                LTRState.MODEL_REGISTRATION,
                model_id=model.identity.model_id,
                model_version=model.identity.model_version,
                metadata={
                    "metrics": dict(evaluation.metrics),
                },
            )

        rollout = self.build_rollout_plan(
            model,
            canary_percentage=canary_percentage,
            rollout_percentage=rollout_percentage,
        )

        self._event(
            LTREventType.MODEL_ROLLOUT_STARTED,
            request_id,
            LTRState.MODEL_ROLLOUT,
            model_id=model.identity.model_id,
            model_version=model.identity.model_version,
            metadata=rollout,
        )

        model.lifecycle_state = ModelLifecycleState.CANARY
        model.updated_at = self._now()
        self.backend.persist_model(model)

        model = self.activate_model(model)

        self._event(
            LTREventType.MODEL_ROLLOUT_COMPLETED,
            request_id,
            LTRState.INFERENCE_READY,
            model_id=model.identity.model_id,
            model_version=model.identity.model_version,
            metadata=rollout,
        )

        return model

    # ------------------------------------------------------------------------
    # Architecture description
    # ------------------------------------------------------------------------

    def architecture(self) -> Dict[str, Any]:
        return {
            "phase": PHASE,
            "version": ARCHITECTURE_VERSION,
            "name": "Learning-to-Rank / Ranking Model Architecture",
            "scale_target": SCALE_TARGET,
            "google_scale_capability_target": GOOGLE_SCALE_CAPABILITY_TARGET,
            "google_technology_dependency": GOOGLE_TECHNOLOGY_DEPENDENCY,

            "input": {
                "stage": "12.7",
                "artifact": "RankingFeatureBundle",
                "feature_schema_versioned": True,
                "partial_features_supported": True,
            },

            "learning": {
                "pointwise": True,
                "pairwise": True,
                "listwise": True,
                "framework_neutral": True,
                "distributed_training": True,
                "data_parallelism": True,
                "model_parallelism_supported": True,
                "incremental_training": True,
                "continuous_training_supported": True,
            },

            "training": {
                "offline_training": True,
                "distributed_dataset_partitioning": True,
                "partition_local_processing": True,
                "global_parameter_synchronization": True,
                "checkpointing": True,
                "resume_from_checkpoint": True,
                "dataset_versioning": True,
                "feature_schema_validation": True,
                "model_artifact_generation": True,
                "evaluation_pipeline": True,
            },

            "model_registry": {
                "model_versioning": True,
                "feature_schema_compatibility": True,
                "artifact_digest": True,
                "model_lineage": True,
                "evaluation_metadata": True,
                "staging": True,
                "canary": True,
                "active": True,
                "rollback": True,
                "retirement": True,
            },

            "inference": {
                "online": True,
                "batch": True,
                "streaming": True,
                "partition_local": True,
                "shard_local": True,
                "horizontal_scaling": True,
                "replica_aware_loading": True,
                "model_cache": True,
                "deterministic": True,
                "partial_results": True,
            },

            "storage": {
                "feature_metadata": "backend_replaceable",
                "model_registry": "backend_replaceable",
                "model_artifacts": "backend_replaceable",
                "checkpoints": "backend_replaceable",
                "evaluation_metadata": "backend_replaceable",
                "telemetry": "backend_replaceable",
            },

            "reliability": {
                "checkpointable": True,
                "restartable": True,
                "partial_input_tolerant": True,
                "provenance_preserving": True,
                "lineage_preserving": True,
                "deterministic_feature_contract": True,
                "model_rollback": True,
                "failure_recovery": True,
            },

            "scale": {
                "billions_to_trillions_of_public_web_resources": True,
                "distributed_workers": True,
                "distributed_partitions": True,
                "distributed_model_replicas": True,
                "no_fixed_global_document_limit": True,
                "no_fixed_global_query_limit": True,
                "no_fixed_global_model_training_limit": True,
                "no_fixed_global_partition_limit": True,
            },

            "boundaries": {
                "does_not_crawl": True,
                "does_not_mutate_index": True,
                "does_not_replace_retrieval": True,
                "does_not_perform_spam_classification": True,
                "does_not_select_final_results": True,
                "does_not_define_final_global_ranking_pipeline": True,
                "does_not_depend_on_google": True,
            },

            "pipeline_position": [
                "human_query",
                "phase11_global_query_understanding",
                "phase11_retrieval",
                "phase12_ranking_signals",
                "phase12_7_multi_signal_feature_fusion",
                "phase12_8_learning_to_rank_model_architecture",
                "phase12_9_final_ranking_architecture",
            ],

            "next_stage": NEXT_STAGE,
        }

    # ------------------------------------------------------------------------
    # Runtime metadata access
    # ------------------------------------------------------------------------

    def events(self) -> Tuple[LTREvent, ...]:
        if hasattr(self.backend, "events"):
            return getattr(self.backend, "events")()

        return tuple()

    def checkpoints(self) -> Tuple[LTRCheckpoint, ...]:
        if hasattr(self.backend, "checkpoints"):
            return getattr(self.backend, "checkpoints")()

        return tuple()

    def models(self) -> Tuple[RankingModel, ...]:
        if hasattr(self.backend, "models"):
            return getattr(self.backend, "models")()

        return tuple()


# ============================================================================
# ALIASES
# ============================================================================

LearningToRank = LearningToRankArchitecture
GlobalLearningToRank = LearningToRankArchitecture
Phase12_8LearningToRank = LearningToRankArchitecture
LearningToRankModelArchitecture = LearningToRankArchitecture
GlobalLearningToRankModelArchitecture = LearningToRankArchitecture


# ============================================================================
# MODULE EXPORTS
# ============================================================================

__all__ = [
    "SCALE_TARGET",
    "GOOGLE_SCALE_CAPABILITY_TARGET",
    "GOOGLE_TECHNOLOGY_DEPENDENCY",
    "ARCHITECTURE_VERSION",
    "PHASE",
    "NEXT_STAGE",

    "LTRState",
    "LTRLearningMode",
    "LTRExecutionMode",
    "ModelLifecycleState",
    "ModelType",
    "FeatureInputState",
    "TrainingExampleType",
    "CheckpointType",
    "LTREventType",
    "MetricType",

    "LTRIdentity",
    "FeatureSchemaIdentity",
    "ModelIdentity",
    "LTRLineage",

    "LTRFeatureValue",
    "LTRFeatureVector",

    "RelevanceJudgment",
    "LTRTrainingExample",
    "LTRTrainingDataset",

    "ModelHyperparameters",
    "ModelArtifact",
    "ModelEvaluation",
    "RankingModel",

    "LTRInferenceRequest",
    "LTRInferenceScore",
    "LTRInferenceResult",

    "LTRCheckpoint",
    "LTREvent",

    "LearningToRankPolicy",
    "LearningToRankBackend",
    "InMemoryLearningToRankMetadata",

    "LearningToRankArchitecture",
    "LearningToRank",
    "GlobalLearningToRank",
    "Phase12_8LearningToRank",
    "LearningToRankModelArchitecture",
    "GlobalLearningToRankModelArchitecture",
]
