"""Artifact Registry, projection, DAG and invalidation primitives."""

from video_truthfulness.core.artifacts.hashing import input_fingerprint, sha256_file
from video_truthfulness.core.artifacts.models import (
    ArtifactRecord,
    ArtifactRecordV1_1,
    ArtifactRecordV1_2,
    ArtifactRecordView,
    DAGDefinition,
    EntityIndexDocument,
    parse_artifact_record,
    to_artifact_record_view,
)
from video_truthfulness.core.artifacts.registry import AppendOnlyRegistry, RegistryEntry

__all__ = [
    "AppendOnlyRegistry",
    "ArtifactRecord",
    "ArtifactRecordV1_1",
    "ArtifactRecordV1_2",
    "ArtifactRecordView",
    "DAGDefinition",
    "EntityIndexDocument",
    "RegistryEntry",
    "input_fingerprint",
    "parse_artifact_record",
    "sha256_file",
    "to_artifact_record_view",
]
