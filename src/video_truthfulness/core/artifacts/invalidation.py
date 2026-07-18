"""Forward-only container and entity dependency invalidation."""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Iterable

from video_truthfulness.core.artifacts.models import ArtifactRecord, UpstreamEntityRef


@dataclass(frozen=True)
class InvalidationResult:
    artifact_id: str
    reason: str
    via_artifact_id: str | None = None
    via_entity_ref: str | None = None


def entity_ref_key(ref: UpstreamEntityRef) -> str:
    return f"{ref.container_artifact_id}:{ref.entity_type}:{ref.entity_id}"


def propagate_stale(
    records: Iterable[ArtifactRecord],
    *,
    changed_artifact_ids: Iterable[str] = (),
    changed_entity_refs: Iterable[str] = (),
) -> list[InvalidationResult]:
    """Return downstream stale candidates without mutating Registry history."""

    latest: dict[str, ArtifactRecord] = {}
    for record in records:
        latest[record.artifact_id] = record
    downstream: dict[str, set[str]] = defaultdict(set)
    for record in latest.values():
        for upstream in record.upstream_artifact_ids:
            downstream[upstream].add(record.artifact_id)

    results: dict[str, InvalidationResult] = {}
    queue: deque[str] = deque()
    for artifact_id in sorted(set(changed_artifact_ids)):
        queue.append(artifact_id)
    for record in latest.values():
        matched = sorted(set(changed_entity_refs).intersection(entity_ref_key(ref) for ref in record.upstream_entity_refs))
        if matched and record.artifact_id not in results:
            results[record.artifact_id] = InvalidationResult(
                artifact_id=record.artifact_id,
                reason="upstream_entity_changed",
                via_entity_ref=matched[0],
            )
            queue.append(record.artifact_id)

    visited = set(queue)
    while queue:
        changed = queue.popleft()
        for child in sorted(downstream.get(changed, set())):
            if child not in results:
                results[child] = InvalidationResult(
                    artifact_id=child,
                    reason="upstream_artifact_changed",
                    via_artifact_id=changed,
                )
            if child not in visited:
                visited.add(child)
                queue.append(child)
    return [results[key] for key in sorted(results)]


def fingerprint_is_stale(record: ArtifactRecord, current_input_fingerprint: str | None) -> bool:
    if record.input_fingerprint is None:
        return False
    return record.input_fingerprint != current_input_fingerprint
