"""Crash-safe single-writer loader for the rebuildable Claim warehouse."""

from __future__ import annotations

import os
import stat
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from pathlib import PurePosixPath
from typing import Callable, Iterable, Literal, Mapping, Sequence

from video_truthfulness.core.artifacts.registry import (
    AppendOnlyRegistry,
    RegistryEntry,
    create_artifact_record,
)

from .warehouse_export import (
    read_export_manifest,
    read_export_package,
    resolve_external_storage_ref,
)
from .warehouse_models import (
    LOGICAL_LAYER_ORDER,
    PROJECTION_STAGE_ORDER,
    ExternalStorageRef,
    WarehouseConflictError,
    WarehouseContractError,
    WarehouseDependencyUnavailable,
    WarehouseExportBinding,
    WarehouseLoadBatchV1,
    WarehouseLoadPlanV1,
    WarehouseLoadReceiptV1,
    WarehouseProjectionAttemptV1,
    WAREHOUSE_PROJECTION_VERSION,
    canonical_json_bytes,
    deterministic_typed_id,
    sha256_bytes,
    split_export_bindings,
    validate_plan_relations,
)
from .warehouse_projection import (
    apply_receipt_transaction,
    build_parquet_artifacts,
    dependency_status,
    publish_staged_parquet_artifacts,
    read_committed_receipt_payload,
    read_existing_warehouse_rows,
    stage_parquet_artifacts,
    validate_staged_parquet_artifacts,
    verify_receipt_parquet,
)


LoaderStage = str
FaultHook = Callable[[LoaderStage], None]
LOADER_ADMISSION_POLICY_V1 = "claim_warehouse_loader_admission_v1.0.0"


@dataclass(frozen=True, slots=True)
class LoaderAdmissionProof:
    """Zero-side-effect proof attached to every payload-gate rejection."""

    payload_read_count: Literal[0] = 0
    database_write_count: Literal[0] = 0
    registry_append_count: Literal[0] = 0


class LoaderAdmissionRejected(WarehouseContractError):
    """Stable fail-closed error raised before the projection journal starts."""

    def __init__(self, error_code: str, message: str) -> None:
        self.error_code = error_code
        self.proof = LoaderAdmissionProof()
        super().__init__(f"{error_code}: {message}")


@dataclass(frozen=True, slots=True)
class _LoaderAdmissionPolicy:
    policy_version: str
    project_version: str
    storage_version: str
    registry_path_segment: str
    export_registry_schema_version: str
    export_artifact_type: str


_LOADER_ADMISSION_POLICY_V1 = _LoaderAdmissionPolicy(
    policy_version=LOADER_ADMISSION_POLICY_V1,
    project_version="v0.2",
    storage_version="V02",
    registry_path_segment="V02",
    export_registry_schema_version="artifact_record_v1.2.0",
    export_artifact_type="warehouse.export_batch",
)


@dataclass(frozen=True, slots=True)
class LoaderExportArtifactIdentity:
    """Outer control identity for one immutable warehouse.export_batch record."""

    artifact_id: str
    record_id: str
    record_hash: str
    registry_schema_version: str
    project_version: str
    storage_version: str
    run_id: str | None
    artifact_type: str
    storage_root_ref: str
    relative_path: str
    content_hash: str
    semantic_hash: str | None
    size_bytes: int

    @classmethod
    def from_registry_entry(
        cls, entry: RegistryEntry
    ) -> "LoaderExportArtifactIdentity":
        record = entry.canonical_view
        return cls(
            artifact_id=record.artifact_id,
            record_id=record.record_id,
            record_hash=record.record_hash,
            registry_schema_version=entry.wire_record.registry_schema_version,
            project_version=record.project_version,
            storage_version=record.storage_version,
            run_id=record.run_id,
            artifact_type=record.artifact_type,
            storage_root_ref=record.storage_root_ref,
            relative_path=record.relative_path,
            content_hash=record.content_hash,
            semantic_hash=record.semantic_hash,
            size_bytes=record.size_bytes,
        )


@dataclass(frozen=True, slots=True)
class LoaderExportAdmission:
    """Payload-free source Registry binding for one planned export."""

    export_id: str
    source_run_id: str
    source_registry_storage_root_ref: str
    source_registry_relative_path: str
    export_record: LoaderExportArtifactIdentity


@dataclass(frozen=True, slots=True)
class LoaderAdmissionContext:
    """Required V02-only control envelope checked before all Loader effects."""

    policy_version: str
    project_version: str
    storage_version: str
    exports: tuple[LoaderExportAdmission, ...]


def _loader_admission_policy(policy_version: str) -> _LoaderAdmissionPolicy:
    """Single fail-closed dispatch point for Loader admission generations."""

    if policy_version == LOADER_ADMISSION_POLICY_V1:
        return _LOADER_ADMISSION_POLICY_V1
    raise LoaderAdmissionRejected(
        "E_LOADER_ADMISSION_POLICY_FORBIDDEN",
        f"unsupported Loader admission policy: {policy_version}",
    )


@dataclass(frozen=True, slots=True)
class WarehouseFactPublication:
    artifact_type: str
    storage_ref: ExternalStorageRef
    content_hash: str
    semantic_hash: str
    size_bytes: int


@dataclass(frozen=True, slots=True)
class WarehouseLoadOutcome:
    plan: WarehouseLoadPlanV1
    receipt: WarehouseLoadReceiptV1
    attempt: WarehouseProjectionAttemptV1
    parquet_paths: tuple[Path, ...]
    fact_publications: tuple[WarehouseFactPublication, ...]
    registry_append_required: bool = False


class WarehouseSingleWriterLock:
    """Small fail-closed process lock; stale lock recovery is always explicit."""

    def __init__(self, path: Path, *, load_plan_id: str) -> None:
        self.path = path
        self.load_plan_id = load_plan_id
        self._acquired = False

    def __enter__(self) -> "WarehouseSingleWriterLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = canonical_json_bytes(
            {"load_plan_id": self.load_plan_id, "pid": os.getpid()}
        )
        try:
            descriptor = os.open(
                self.path,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                0o600,
            )
        except FileExistsError as exc:
            raise WarehouseConflictError("WAREHOUSE_SECOND_WRITER_LOCKED") from exc
        try:
            os.write(descriptor, payload)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        self._acquired = True
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if self._acquired and self.path.exists() and not self.path.is_symlink():
            self.path.unlink()
        self._acquired = False


def build_load_plan(
    exports: Sequence[WarehouseExportBinding],
    *,
    created_at: str,
    storage_root_ref: str,
    load_plan_id: str | None = None,
) -> WarehouseLoadPlanV1:
    ordered = sorted(
        exports,
        key=lambda item: (LOGICAL_LAYER_ORDER[item.logical_layer], item.export_id),
    )
    seed = canonical_json_bytes(
        [
            {
                "export_id": item.export_id,
                "logical_hash": item.logical_hash,
                "manifest_hash": item.manifest_hash,
            }
            for item in ordered
        ]
    ).decode("utf-8")
    plan_id = load_plan_id or deterministic_typed_id("load_plan", seed)
    return WarehouseLoadPlanV1.build(
        load_plan_id=plan_id,
        created_at=created_at,
        storage_root_ref=storage_root_ref,
        plan_relative_path=f"receipts/{plan_id}.json",
        exports=ordered,
    )


def build_load_plans(
    exports: Sequence[WarehouseExportBinding],
    *,
    created_at: str,
    storage_root_ref: str,
    max_exports: int = 100,
) -> tuple[WarehouseLoadPlanV1, ...]:
    batches = split_export_bindings(exports, max_exports=max_exports)
    return tuple(
        build_load_plan(
            batch,
            created_at=created_at,
            storage_root_ref=storage_root_ref,
        )
        for batch in batches
    )


def build_loader_admission_context(
    plan: WarehouseLoadPlanV1,
    *,
    source_registry_roots: Mapping[str, Path | str],
) -> LoaderAdmissionContext:
    """Build a V02 admission envelope from already-published Registry metadata.

    This helper is intended for coordinators that do not already retain the export
    Artifact record returned by publication.  :meth:`WarehouseLoader.load` still
    re-reads and verifies the exact record before opening manifest or row payloads.
    """

    exports: list[LoaderExportAdmission] = []
    cached_entries: dict[tuple[str, str, str], tuple[RegistryEntry, ...]] = {}
    for binding in plan.exports:
        key = (
            binding.source_registry_ref.storage_root_ref,
            binding.source_registry_ref.relative_path,
            binding.source_run_id,
        )
        entries = cached_entries.get(key)
        if entries is None:
            registry_path = _resolve_registered_storage_ref(
                source_registry_roots, binding.source_registry_ref
            )
            entries = tuple(
                AppendOnlyRegistry(
                    registry_path,
                    scope="run",
                    expected_run_id=binding.source_run_id,
                ).read_entries()
            )
            cached_entries[key] = entries
        matches = [
            entry
            for entry in entries
            if entry.canonical_view.artifact_type
            == _LOADER_ADMISSION_POLICY_V1.export_artifact_type
            and entry.canonical_view.run_id == binding.source_run_id
            and entry.canonical_view.storage_root_ref == binding.storage_root_ref
            and entry.canonical_view.relative_path == binding.manifest_relative_path
            and entry.canonical_view.content_hash == binding.manifest_hash
            and entry.canonical_view.semantic_hash == binding.logical_hash
        ]
        if not matches:
            raise WarehouseContractError(
                "source Registry has no exact warehouse.export_batch control record"
            )
        entry = matches[-1]
        exports.append(
            LoaderExportAdmission(
                export_id=binding.export_id,
                source_run_id=binding.source_run_id,
                source_registry_storage_root_ref=(
                    binding.source_registry_ref.storage_root_ref
                ),
                source_registry_relative_path=binding.source_registry_ref.relative_path,
                export_record=LoaderExportArtifactIdentity.from_registry_entry(entry),
            )
        )
    return LoaderAdmissionContext(
        policy_version=_LOADER_ADMISSION_POLICY_V1.policy_version,
        project_version=_LOADER_ADMISSION_POLICY_V1.project_version,
        storage_version=_LOADER_ADMISSION_POLICY_V1.storage_version,
        exports=tuple(exports),
    )


def write_immutable_fact(
    value: WarehouseLoadPlanV1
    | WarehouseProjectionAttemptV1
    | WarehouseLoadReceiptV1,
    *,
    storage_ref: ExternalStorageRef,
    storage_roots: Mapping[str, Path | str],
) -> WarehouseFactPublication:
    """Create one immutable fact; byte-identical replay is idempotent."""

    data = value.canonical_bytes()
    path = resolve_external_storage_ref(storage_roots, storage_ref)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.is_symlink() or path.read_bytes() != data:
            raise WarehouseConflictError(
                f"immutable warehouse fact conflict: {storage_ref.relative_path}"
            )
    else:
        with path.open("xb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
    return describe_immutable_fact(value, storage_ref=storage_ref)


def describe_immutable_fact(
    value: WarehouseLoadPlanV1
    | WarehouseProjectionAttemptV1
    | WarehouseLoadReceiptV1,
    *,
    storage_ref: ExternalStorageRef,
) -> WarehouseFactPublication:
    """Describe canonical immutable bytes without publishing the final path."""

    data = value.canonical_bytes()
    artifact_type, semantic_hash = _fact_identity(value)
    return WarehouseFactPublication(
        artifact_type=artifact_type,
        storage_ref=storage_ref,
        content_hash=sha256_bytes(data),
        semantic_hash=semantic_hash,
        size_bytes=len(data),
    )


class WarehouseLoader:
    """Execute the eight frozen projection steps without mutating source S01 files."""

    def __init__(
        self,
        *,
        storage_roots: Mapping[str, Path | str],
        storage_root_ref: str,
        source_registry_roots: Mapping[str, Path | str],
        cross_run_registry: AppendOnlyRegistry,
        database_relative_path: str = "duckdb/truthfulness_v02.duckdb",
        lock_relative_path: str = "locks/warehouse-loader.lock",
    ) -> None:
        self.storage_roots = dict(storage_roots)
        self.storage_root_ref = storage_root_ref
        self.source_registry_roots = dict(source_registry_roots)
        self.cross_run_registry = cross_run_registry
        if cross_run_registry.scope != "cross_run":
            raise WarehouseContractError("Loader Registry must use cross_run scope")
        self.database_ref = ExternalStorageRef(
            storage_root_ref=storage_root_ref,
            relative_path=database_relative_path,
        )
        self.lock_ref = ExternalStorageRef(
            storage_root_ref=storage_root_ref,
            relative_path=lock_relative_path,
        )

    def load(
        self,
        plan: WarehouseLoadPlanV1,
        *,
        admission: LoaderAdmissionContext,
        attempt_no: int,
        started_at: str,
        completed_at: str,
        fault_hook: FaultHook | None = None,
    ) -> WarehouseLoadOutcome:
        """Load one immutable plan; callers append returned facts to Registry."""

        admitted_registries = self._admit(plan, admission)
        if plan.storage_root_ref != self.storage_root_ref:
            raise WarehouseContractError("load plan targets a different storage root")
        plan_ref = ExternalStorageRef(
            storage_root_ref=self.storage_root_ref,
            relative_path=plan.plan_relative_path,
        )
        plan_publication = write_immutable_fact(
            plan,
            storage_ref=plan_ref,
            storage_roots=self.storage_roots,
        )
        self._append_fact_publications([plan_publication], recorded_at=started_at)
        publications: list[WarehouseFactPublication] = [plan_publication]
        stage: LoaderStage = "load_plan"
        completed_stages: list[str] = ["load_plan"]
        success_attempt_published = False
        database_path = resolve_external_storage_ref(
            self.storage_roots, self.database_ref
        )
        lock_path = resolve_external_storage_ref(self.storage_roots, self.lock_ref)
        attempt_id = deterministic_typed_id(
            "attempt", f"{plan.plan_hash}:{attempt_no}"
        )
        try:
            self._fault(fault_hook, stage)
            with WarehouseSingleWriterLock(lock_path, load_plan_id=plan.load_plan_id):
                recovered = self._recover_committed_receipt(
                    plan,
                    database_path=database_path,
                )
                if recovered is not None:
                    parquet_paths = verify_receipt_parquet(
                        recovered,
                        storage_root_ref=self.storage_root_ref,
                        storage_roots=self.storage_roots,
                    )
                    receipt_publication = write_immutable_fact(
                        recovered,
                        storage_ref=ExternalStorageRef(
                            storage_root_ref=self.storage_root_ref,
                            relative_path=recovered.receipt_relative_path,
                        ),
                        storage_roots=self.storage_roots,
                    )
                    publications.append(receipt_publication)
                    completed_stages = list(PROJECTION_STAGE_ORDER[:7])
                    stage = "registry_append"
                    self._fault(fault_hook, stage)
                    attempt = self._success_attempt(
                        attempt_id=attempt_id,
                        plan=plan,
                        attempt_no=attempt_no,
                        started_at=started_at,
                        completed_at=completed_at,
                        receipt=recovered,
                    )
                    attempt_publication = self._publish_attempt(attempt)
                    success_attempt_published = True
                    self._append_fact_publications(
                        [receipt_publication, attempt_publication],
                        recorded_at=completed_at,
                    )
                    publications.append(attempt_publication)
                    completed_stages.append(stage)
                    return WarehouseLoadOutcome(
                        plan=plan,
                        receipt=recovered,
                        attempt=attempt,
                        parquet_paths=parquet_paths,
                        fact_publications=tuple(publications),
                    )

                stage = "export_validate"
                exports = self._read_plan_exports(
                    plan, admitted_registries=admitted_registries
                )
                validate_plan_relations(
                    (item.rows for item in exports),
                    existing_rows=read_existing_warehouse_rows(database_path),
                )
                completed_stages.append(stage)
                self._fault(fault_hook, stage)

                stage = "parquet_staging"
                parquet_artifacts = build_parquet_artifacts(exports)
                staged_parquet = stage_parquet_artifacts(
                    parquet_artifacts,
                    load_plan_id=plan.load_plan_id,
                    storage_root_ref=self.storage_root_ref,
                    storage_roots=self.storage_roots,
                )
                completed_stages.append(stage)
                self._fault(fault_hook, stage)

                stage = "parquet_validate"
                if sum(item.descriptor.row_count for item in parquet_artifacts) != sum(
                    item.manifest.row_count for item in exports
                ):
                    raise WarehouseContractError("Parquet staging row count mismatch")
                validate_staged_parquet_artifacts(staged_parquet)
                completed_stages.append(stage)
                self._fault(fault_hook, stage)

                stage = "parquet_publish"
                parquet_paths = publish_staged_parquet_artifacts(
                    staged_parquet,
                    load_plan_id=plan.load_plan_id,
                    storage_root_ref=self.storage_root_ref,
                    storage_roots=self.storage_roots,
                )
                completed_stages.append(stage)
                self._fault(fault_hook, stage)

                stage = "duckdb_transaction"
                receipt = self._build_receipt(
                    plan,
                    exports=exports,
                    parquet_artifacts=parquet_artifacts,
                    started_at=started_at,
                    completed_at=completed_at,
                )
                apply_receipt_transaction(
                    database_path,
                    receipt,
                    parquet_paths=parquet_paths,
                )
                completed_stages.append(stage)
                self._fault(fault_hook, stage)

                stage = "receipt_publish"
                receipt_publication = write_immutable_fact(
                    receipt,
                    storage_ref=ExternalStorageRef(
                        storage_root_ref=self.storage_root_ref,
                        relative_path=receipt.receipt_relative_path,
                    ),
                    storage_roots=self.storage_roots,
                )
                publications.append(receipt_publication)
                completed_stages.append(stage)
                self._fault(fault_hook, stage)

                stage = "registry_append"
                self._fault(fault_hook, stage)
                attempt = self._success_attempt(
                    attempt_id=attempt_id,
                    plan=plan,
                    attempt_no=attempt_no,
                    started_at=started_at,
                    completed_at=completed_at,
                    receipt=receipt,
                )
                attempt_publication = self._publish_attempt(attempt)
                success_attempt_published = True
                self._append_fact_publications(
                    [receipt_publication, attempt_publication],
                    recorded_at=completed_at,
                )
                publications.append(attempt_publication)
                completed_stages.append(stage)
                return WarehouseLoadOutcome(
                    plan=plan,
                    receipt=receipt,
                    attempt=attempt,
                    parquet_paths=parquet_paths,
                    fact_publications=tuple(publications),
                )
        except Exception as exc:
            if success_attempt_published:
                raise WarehouseConflictError(
                    "immutable success attempt was published before Loader Registry append "
                    "completed; replay the same attempt bytes to finish registration"
                ) from exc
            attempt = WarehouseProjectionAttemptV1.build(
                attempt_id=attempt_id,
                load_plan_id=plan.load_plan_id,
                load_plan_hash=plan.plan_hash,
                attempt_no=attempt_no,
                status="failed",
                last_completed_stage=completed_stages[-1],
                started_at=started_at,
                completed_at=completed_at,
                error_code=_error_code(exc),
                error_message=f"{type(exc).__name__}: failed at {stage}",
                receipt_id=None,
                receipt_hash=None,
            )
            failed_publication = self._publish_attempt(attempt)
            try:
                self._append_fact_publications(
                    [failed_publication],
                    recorded_at=completed_at,
                )
            except Exception as registry_exc:
                raise WarehouseConflictError(
                    "failed projection attempt could not be appended to Loader Registry"
                ) from registry_exc
            raise

    def _admit(
        self,
        plan: WarehouseLoadPlanV1,
        admission: LoaderAdmissionContext,
    ) -> dict[str, tuple[RegistryEntry, ...]]:
        """Validate V02-only control metadata before any projection side effect."""

        policy = _loader_admission_policy(admission.policy_version)
        if admission.project_version != policy.project_version or any(
            item.export_record.project_version != policy.project_version
            for item in admission.exports
        ):
            raise LoaderAdmissionRejected(
                "E_PROJECT_VERSION_FORBIDDEN",
                "Loader project_version must be exactly v0.2",
            )
        if admission.storage_version != policy.storage_version or any(
            item.export_record.storage_version != policy.storage_version
            for item in admission.exports
        ):
            raise LoaderAdmissionRejected(
                "E_STORAGE_VERSION_FORBIDDEN",
                "Loader storage_version must be exactly V02",
            )

        by_export: dict[str, LoaderExportAdmission] = {}
        for item in admission.exports:
            if item.export_id in by_export:
                raise LoaderAdmissionRejected(
                    "E_REGISTRY_PATH_VERSION_FORBIDDEN",
                    "Loader admission contains a duplicate export identity",
                )
            by_export[item.export_id] = item
        if set(by_export) != {binding.export_id for binding in plan.exports}:
            raise LoaderAdmissionRejected(
                "E_REGISTRY_PATH_VERSION_FORBIDDEN",
                "Loader admission export set differs from the immutable plan",
            )

        # Phase one is pure control validation.  It must finish for every export
        # before even source Registry metadata is opened.
        for binding in plan.exports:
            item = by_export[binding.export_id]
            expected_registry_path = (
                PurePosixPath("runs")
                / policy.registry_path_segment
                / binding.source_run_id
                / "artifact_registry.jsonl"
            )
            if (
                plan.storage_root_ref != self.storage_root_ref
                or binding.storage_root_ref != self.storage_root_ref
                or item.source_run_id != binding.source_run_id
                or item.source_registry_storage_root_ref
                != binding.source_registry_ref.storage_root_ref
                or item.source_registry_relative_path
                != binding.source_registry_ref.relative_path
                or binding.source_registry_ref.relative_path
                != expected_registry_path.as_posix()
                or item.source_registry_relative_path
                != expected_registry_path.as_posix()
                or PurePosixPath(item.source_registry_relative_path)
                != expected_registry_path
                or item.source_registry_storage_root_ref
                not in self.source_registry_roots
            ):
                raise LoaderAdmissionRejected(
                    "E_REGISTRY_PATH_VERSION_FORBIDDEN",
                    "source Registry must use an authorized runs/V02/<run_id> path",
                )
            identity = item.export_record
            if (
                identity.registry_schema_version
                != policy.export_registry_schema_version
                or identity.artifact_type != policy.export_artifact_type
                or identity.run_id != binding.source_run_id
                or identity.storage_root_ref != binding.storage_root_ref
                or identity.relative_path != binding.manifest_relative_path
                or identity.content_hash != binding.manifest_hash
                or identity.semantic_hash != binding.logical_hash
                or identity.size_bytes < 1
            ):
                raise LoaderAdmissionRejected(
                    "E_REGISTRY_PATH_VERSION_FORBIDDEN",
                    "outer export Artifact record identity differs from the load plan",
                )

        # Source Registry JSONL is control metadata, not business payload.  The
        # exact validated record is retained so manifest/row reads cannot race a
        # second Registry read.
        admitted: dict[str, tuple[RegistryEntry, ...]] = {}
        registry_cache: dict[tuple[str, str, str], tuple[RegistryEntry, ...]] = {}
        for binding in plan.exports:
            item = by_export[binding.export_id]
            cache_key = (
                item.source_registry_storage_root_ref,
                item.source_registry_relative_path,
                item.source_run_id,
            )
            entries = registry_cache.get(cache_key)
            if entries is None:
                try:
                    registry_path = _resolve_registered_storage_ref(
                        self.source_registry_roots,
                        binding.source_registry_ref,
                    )
                    entries = tuple(
                        AppendOnlyRegistry(
                            registry_path,
                            scope="run",
                            expected_run_id=binding.source_run_id,
                        ).read_entries()
                    )
                except (OSError, ValueError) as exc:
                    raise LoaderAdmissionRejected(
                        "E_REGISTRY_PATH_VERSION_FORBIDDEN",
                        "source Registry control metadata is unavailable or invalid",
                    ) from exc
                registry_cache[cache_key] = entries

            admitted[binding.export_id] = entries

        if any(
            entry.canonical_view.project_version != policy.project_version
            for entries in admitted.values()
            for entry in entries
        ):
            raise LoaderAdmissionRejected(
                "E_PROJECT_VERSION_FORBIDDEN",
                "source Registry contains a non-v0.2 record",
            )
        if any(
            entry.canonical_view.storage_version != policy.storage_version
            for entries in admitted.values()
            for entry in entries
        ):
            raise LoaderAdmissionRejected(
                "E_STORAGE_VERSION_FORBIDDEN",
                "source Registry contains a non-V02 record",
            )

        for binding in plan.exports:
            entries = admitted[binding.export_id]
            item = by_export[binding.export_id]
            identity = item.export_record
            exact = [
                entry
                for entry in entries
                if entry.canonical_view.record_id == identity.record_id
                and entry.canonical_view.record_hash == identity.record_hash
            ]
            if len(exact) != 1 or not _registry_entry_matches_admission(
                exact[0], binding=binding, identity=identity, policy=policy
            ):
                raise LoaderAdmissionRejected(
                    "E_REGISTRY_PATH_VERSION_FORBIDDEN",
                    "source Registry export record identity differs from admission",
                )
        return admitted

    def _read_plan_exports(
        self,
        plan: WarehouseLoadPlanV1,
        *,
        admitted_registries: Mapping[str, tuple[RegistryEntry, ...]],
    ) -> tuple:
        exports = []
        for binding in plan.exports:
            if binding.storage_root_ref != self.storage_root_ref:
                raise WarehouseContractError("mixed external roots are not authorized")
            manifest_ref = ExternalStorageRef(
                storage_root_ref=binding.storage_root_ref,
                relative_path=binding.manifest_relative_path,
            )
            _, manifest, manifest_bytes = read_export_manifest(
                storage_roots=self.storage_roots,
                manifest_ref=manifest_ref,
                expected_manifest_hash=binding.manifest_hash,
            )
            if (
                manifest.run_id != binding.source_run_id
                or manifest.export_id != binding.export_id
                or manifest.export_idempotency_key != binding.export_idempotency_key
            ):
                raise WarehouseConflictError("export manifest provenance differs from plan")
            self._validate_source_registry(
                binding=binding,
                manifest=manifest,
                manifest_size=len(manifest_bytes),
                entries=admitted_registries[binding.export_id],
            )
            result = read_export_package(
                storage_roots=self.storage_roots,
                manifest_ref=manifest_ref,
                expected_manifest_hash=binding.manifest_hash,
            )
            if (
                result.manifest.export_id != binding.export_id
                or result.manifest.export_idempotency_key
                != binding.export_idempotency_key
                or result.rows_hash != binding.rows_hash
                or result.logical_hash != binding.logical_hash
                or result.manifest.row_count != binding.row_count
            ):
                raise WarehouseConflictError("export binding differs from package")
            exports.append(result)
        return tuple(exports)

    def _validate_source_registry(
        self,
        *,
        binding,
        manifest,
        manifest_size: int,
        entries: Sequence[RegistryEntry],
    ) -> None:
        prefix = manifest.source_registry_prefix
        if len(entries) < prefix.record_count:
            raise WarehouseContractError("source Registry is shorter than frozen prefix")
        prefix_entries = entries[: prefix.record_count]
        prefix_bytes = b"".join(
            canonical_json_bytes(entry.wire_record.model_dump(mode="json")) + b"\n"
            for entry in prefix_entries
        )
        if sha256_bytes(prefix_bytes) != prefix.prefix_hash:
            raise WarehouseConflictError("source Registry prefix bytes drift")
        if prefix.record_count:
            head = prefix_entries[-1].wire_record
            if (
                head.record_id != prefix.head_record_id
                or head.record_hash != prefix.head_record_hash
            ):
                raise WarehouseConflictError("source Registry prefix head/count drift")
        elif prefix.head_record_id is not None or prefix.head_record_hash is not None:
            raise WarehouseContractError("empty source Registry prefix cannot have a head")

        prefix_by_record = {
            entry.wire_record.record_id: entry.canonical_view
            for entry in prefix_entries
        }
        for expected in manifest.input_artifacts:
            observed = prefix_by_record.get(expected.record_id)
            if observed is None:
                raise WarehouseContractError(
                    "input Artifact record is outside the frozen Registry prefix"
                )
            if (
                observed.artifact_id != expected.artifact_id
                or observed.artifact_type != expected.artifact_type
                or observed.content_hash != expected.content_hash
            ):
                raise WarehouseConflictError("input Artifact Registry record drift")

        export_records = [
            entry
            for entry in entries[prefix.record_count :]
            if entry.canonical_view.artifact_type == "warehouse.export_batch"
            and entry.canonical_view.run_id == manifest.run_id
            and entry.canonical_view.relative_path == manifest.manifest_relative_path
        ]
        if not export_records:
            raise WarehouseContractError(
                "source Registry has no v1.2 warehouse.export_batch suffix record"
            )
        for entry in export_records:
            record = entry.canonical_view
            if entry.wire_record.registry_schema_version != "artifact_record_v1.2.0":
                raise WarehouseContractError("warehouse export Registry record must be v1.2")
            if (
                record.storage_root_ref == manifest.storage_root_ref
                and record.content_hash == binding.manifest_hash
                and record.semantic_hash == manifest.logical_hash
                and record.size_bytes == manifest_size
            ):
                break
        else:
            raise WarehouseConflictError("warehouse export Registry record binding drift")

    def _build_receipt(
        self,
        plan: WarehouseLoadPlanV1,
        *,
        exports: Sequence,
        parquet_artifacts: Sequence,
        started_at: str,
        completed_at: str,
    ) -> WarehouseLoadReceiptV1:
        load_batch_id = deterministic_typed_id("load_batch", plan.plan_hash)
        receipt_id = deterministic_typed_id("load_receipt", plan.plan_hash)
        row_counts: Counter[str] = Counter()
        for export in exports:
            row_counts.update(export.manifest.row_counts)
        logical_hash = sha256_bytes(
            canonical_json_bytes(
                [
                    {"export_id": item.export_id, "logical_hash": item.logical_hash}
                    for item in plan.exports
                ]
            )
        )
        batch = WarehouseLoadBatchV1.build(
            load_batch_id=load_batch_id,
            load_plan_id=plan.load_plan_id,
            load_plan_hash=plan.plan_hash,
            started_at=started_at,
            completed_at=completed_at,
            export_count=len(plan.exports),
            row_count=sum(row_counts.values()),
            logical_hash=logical_hash,
        )
        watermark: dict[str, str] = {}
        for artifact in parquet_artifacts:
            descriptor = artifact.descriptor
            current = watermark.get(descriptor.logical_layer)
            if current is None or descriptor.export_id > current:
                watermark[descriptor.logical_layer] = descriptor.export_id
        observed_dependencies = dependency_status()
        versions = {
            name: str(details["observed_version"])
            for name, details in observed_dependencies.items()
            if details["observed_version"] is not None
        }
        transaction_marker = sha256_bytes(
            canonical_json_bytes(
                {
                    "batch_hash": batch.batch_hash,
                    "parquet_manifest": [
                        item.descriptor.model_dump(mode="json")
                        for item in parquet_artifacts
                    ],
                }
            )
        )
        return WarehouseLoadReceiptV1.build(
            receipt_id=receipt_id,
            receipt_relative_path=f"receipts/{receipt_id}.json",
            load_batch=batch,
            exports=list(plan.exports),
            parquet_manifest=[item.descriptor for item in parquet_artifacts],
            row_counts=dict(sorted(row_counts.items())),
            watermark=dict(sorted(watermark.items())),
            dependency_versions=versions,
            duckdb_transaction_marker=transaction_marker,
        )

    def _recover_committed_receipt(
        self, plan: WarehouseLoadPlanV1, *, database_path: Path
    ) -> WarehouseLoadReceiptV1 | None:
        load_batch_id = deterministic_typed_id("load_batch", plan.plan_hash)
        payload = read_committed_receipt_payload(database_path, load_batch_id)
        if payload is None:
            return None
        receipt = WarehouseLoadReceiptV1.model_validate_json(payload)
        if receipt.canonical_bytes() != payload:
            raise WarehouseConflictError("committed receipt payload is not canonical")
        if (
            receipt.load_batch.load_plan_id != plan.load_plan_id
            or receipt.load_batch.load_plan_hash != plan.plan_hash
        ):
            raise WarehouseConflictError("committed receipt binds another load plan")
        return receipt

    def _publish_attempt(
        self, attempt: WarehouseProjectionAttemptV1
    ) -> WarehouseFactPublication:
        return write_immutable_fact(
            attempt,
            storage_ref=ExternalStorageRef(
                storage_root_ref=self.storage_root_ref,
                relative_path=f"receipts/{attempt.attempt_id}.json",
            ),
            storage_roots=self.storage_roots,
        )

    def _append_fact_publications(
        self,
        publications: Sequence[WarehouseFactPublication],
        *,
        recorded_at: str,
    ) -> None:
        entries = self.cross_run_registry.read_entries()
        by_artifact = {entry.canonical_view.artifact_id: entry.canonical_view for entry in entries}
        by_location = {
            (entry.canonical_view.storage_root_ref, entry.canonical_view.relative_path):
            entry.canonical_view
            for entry in entries
        }
        candidates = []
        for publication in publications:
            seed = (
                f"claim-warehouse-fact:{publication.artifact_type}:"
                f"{publication.storage_ref.storage_root_ref}:"
                f"{publication.storage_ref.relative_path}"
            )
            artifact_id = deterministic_typed_id("artifact", seed)
            prior = by_artifact.get(artifact_id) or by_location.get(
                (
                    publication.storage_ref.storage_root_ref,
                    publication.storage_ref.relative_path,
                )
            )
            if prior is not None:
                if (
                    prior.artifact_id != artifact_id
                    or prior.artifact_type != publication.artifact_type
                    or prior.content_hash != publication.content_hash
                    or prior.semantic_hash != publication.semantic_hash
                    or prior.size_bytes != publication.size_bytes
                ):
                    raise WarehouseConflictError(
                        "Loader Registry fact identity was reused with different bytes"
                    )
                continue
            candidate = create_artifact_record(
                registry_schema_version="artifact_record_v1.2.0",
                record_id=deterministic_typed_id(
                    "record", f"{artifact_id}:{publication.content_hash}"
                ),
                recorded_at=recorded_at,
                artifact_id=artifact_id,
                artifact_type=publication.artifact_type,
                logical_name=publication.storage_ref.relative_path.rsplit("/", 1)[-1],
                container_kind="file",
                project_version="v0.2",
                storage_version="V02",
                dataset_version=WAREHOUSE_PROJECTION_VERSION,
                relative_path=publication.storage_ref.relative_path,
                storage_root_ref=publication.storage_ref.storage_root_ref,
                storage_scope="cross_run",
                media_type="application/json",
                size_bytes=publication.size_bytes,
                content_hash=publication.content_hash,
                semantic_hash_algorithm="sha256",
                semantic_hash=publication.semantic_hash,
                producer_type="projection_builder",
                writer_agent_id="claim_warehouse_loader",
                schema_versions=[publication.artifact_type],
                tool_versions={"warehouse_projection": WAREHOUSE_PROJECTION_VERSION},
                authority_level="projection",
                lifecycle_state="validated",
                validation_status="passed",
                privacy_class="private_derived",
                access_scope="local_private",
                retention_policy="rebuildable warehouse projection fact",
                created_at=recorded_at,
                validated_at=recorded_at,
            )
            candidates.append(candidate)
            by_artifact[artifact_id] = candidate
            by_location[
                (
                    publication.storage_ref.storage_root_ref,
                    publication.storage_ref.relative_path,
                )
            ] = candidate
        if candidates:
            self.cross_run_registry.append_many(candidates)

    @staticmethod
    def _success_attempt(
        *,
        attempt_id: str,
        plan: WarehouseLoadPlanV1,
        attempt_no: int,
        started_at: str,
        completed_at: str,
        receipt: WarehouseLoadReceiptV1,
    ) -> WarehouseProjectionAttemptV1:
        return WarehouseProjectionAttemptV1.build(
            attempt_id=attempt_id,
            load_plan_id=plan.load_plan_id,
            load_plan_hash=plan.plan_hash,
            attempt_no=attempt_no,
            status="succeeded",
            last_completed_stage="registry_append",
            started_at=started_at,
            completed_at=completed_at,
            error_code=None,
            error_message=None,
            receipt_id=receipt.receipt_id,
            receipt_hash=receipt.receipt_hash,
        )

    @staticmethod
    def _fault(fault_hook: FaultHook | None, stage: LoaderStage) -> None:
        if fault_hook is not None:
            fault_hook(stage)


def _registry_entry_matches_admission(
    entry: RegistryEntry,
    *,
    binding: WarehouseExportBinding,
    identity: LoaderExportArtifactIdentity,
    policy: _LoaderAdmissionPolicy,
) -> bool:
    record = entry.canonical_view
    return (
        entry.wire_record.registry_schema_version
        == identity.registry_schema_version
        == policy.export_registry_schema_version
        and record.artifact_id == identity.artifact_id
        and record.record_id == identity.record_id
        and record.record_hash == identity.record_hash
        and record.project_version
        == identity.project_version
        == policy.project_version
        and record.storage_version
        == identity.storage_version
        == policy.storage_version
        and record.run_id == identity.run_id == binding.source_run_id
        and record.artifact_type
        == identity.artifact_type
        == policy.export_artifact_type
        and record.storage_root_ref
        == identity.storage_root_ref
        == binding.storage_root_ref
        and record.relative_path
        == identity.relative_path
        == binding.manifest_relative_path
        and record.content_hash == identity.content_hash == binding.manifest_hash
        and record.semantic_hash == identity.semantic_hash == binding.logical_hash
        and record.size_bytes == identity.size_bytes
    )


def _fact_identity(
    value: WarehouseLoadPlanV1
    | WarehouseProjectionAttemptV1
    | WarehouseLoadReceiptV1,
) -> tuple[str, str]:
    if isinstance(value, WarehouseLoadPlanV1):
        return "warehouse.load_plan", value.plan_hash
    if isinstance(value, WarehouseProjectionAttemptV1):
        return "warehouse.projection_attempt", value.attempt_hash
    return "warehouse.load_receipt", value.receipt_hash


def _error_code(exc: Exception) -> str:
    if isinstance(exc, WarehouseDependencyUnavailable):
        return "WAREHOUSE_DEPENDENCY_UNAVAILABLE"
    if isinstance(exc, WarehouseConflictError):
        return "WAREHOUSE_EXPORT_CONFLICT"
    if isinstance(exc, WarehouseContractError):
        return "WAREHOUSE_CONTRACT_INVALID"
    return "WAREHOUSE_LOAD_FAILED"


def _resolve_registered_storage_ref(
    storage_roots: Mapping[str, Path | str], ref: ExternalStorageRef
) -> Path:
    """Resolve a registered non-warehouse root without weakening warehouse allowlists."""

    if ref.storage_root_ref not in storage_roots:
        raise WarehouseContractError(
            f"unknown source Registry storage_root_ref: {ref.storage_root_ref}"
        )
    root = Path(storage_roots[ref.storage_root_ref])
    if not root.is_absolute() or not root.is_dir():
        raise WarehouseContractError("source Registry root must be an existing absolute dir")
    current_root = Path(root.anchor)
    for part in root.parts[1:]:
        current_root = current_root / part
        if os.path.lexists(current_root) and stat.S_ISLNK(os.lstat(current_root).st_mode):
            raise WarehouseContractError("source Registry root cannot traverse symlinks")
    resolved_root = root.resolve(strict=True)
    candidate = root.joinpath(*PurePosixPath(ref.relative_path).parts)
    current = root
    root_device = os.stat(root).st_dev
    for part in PurePosixPath(ref.relative_path).parts:
        current = current / part
        if os.path.lexists(current):
            if stat.S_ISLNK(os.lstat(current).st_mode):
                raise WarehouseContractError("source Registry path cannot traverse symlinks")
            if os.stat(current).st_dev != root_device:
                raise WarehouseContractError("source Registry path cannot cross a mount")
    try:
        candidate.resolve(strict=False).relative_to(resolved_root)
    except ValueError as exc:
        raise WarehouseContractError("source Registry path escapes registered root") from exc
    return candidate


__all__ = [
    "FaultHook",
    "LOADER_ADMISSION_POLICY_V1",
    "LoaderAdmissionContext",
    "LoaderAdmissionProof",
    "LoaderAdmissionRejected",
    "LoaderExportAdmission",
    "LoaderExportArtifactIdentity",
    "WarehouseFactPublication",
    "WarehouseLoadOutcome",
    "WarehouseLoader",
    "WarehouseSingleWriterLock",
    "build_loader_admission_context",
    "build_load_plan",
    "build_load_plans",
    "describe_immutable_fact",
    "write_immutable_fact",
]
