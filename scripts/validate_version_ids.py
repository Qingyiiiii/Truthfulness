"""Read-only validation for the v0.2 version and canonical ID policy."""

from __future__ import annotations

import argparse
import concurrent.futures
import copy
import datetime as dt
import json
import re
import secrets
import sys
import time
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


EXPECTED_POLICY = {
    "id_policy_version": "id_policy_v1.0.0",
    "project_version": "v0.2",
    "storage_version": "V02",
    "release_id": "truthfulness_v0.2_youtube_video",
    "primary_source_type": "video",
    "primary_source_platform": "youtube",
    "release_status": "development",
}
EXPECTED_V01_DIRECTORY_COUNT = 27
ULID_ALPHABET = "0123456789abcdefghjkmnpqrstvwxyz"

RUN_REQUIRED = {
    "id_policy_version",
    "project_version",
    "storage_version",
    "release_id",
    "run_id",
    "source_id",
    "source_external_id",
    "source_type",
    "source_platform",
    "source_title",
    "storage_path",
    "directory_mode",
    "created_at",
    "created_at_provenance",
}
RUN_ALLOWED = RUN_REQUIRED | {"legacy_timestamp_hint"}

MAP_REQUIRED = {
    "id_policy_version",
    "project_version",
    "storage_version",
    "canonical_run_id",
    "legacy_directory_name",
    "legacy_relative_path",
    "source_id",
    "mapping_status",
    "mapping_basis",
    "mapped_at",
}


@dataclass
class Audit:
    checks: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def passed(self, label: str) -> None:
        self.checks.append(label)

    def failed(self, label: str, detail: str) -> None:
        self.errors.append(f"{label}: {detail}")

    def warn(self, label: str, detail: str) -> None:
        self.warnings.append(f"{label}: {detail}")


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError("top-level JSON value must be an object")
    return value


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_no, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                raise ValueError(f"blank line at {line_no}")
            value = json.loads(line)
            if not isinstance(value, dict):
                raise ValueError(f"line {line_no} is not an object")
            records.append(value)
    return records


def _is_utc_timestamp(value: Any, pattern: re.Pattern[str]) -> bool:
    if not isinstance(value, str) or not pattern.fullmatch(value):
        return False
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.utcoffset() == dt.timedelta(0)


def _is_safe_relative_path(value: Any) -> bool:
    if not isinstance(value, str) or not value:
        return False
    if "\\" in value or ".." in value or value.startswith("/"):
        return False
    if re.match(r"^[A-Za-z]:", value):
        return False
    return True


def _field_errors(record: dict[str, Any], required: set[str], allowed: set[str]) -> list[str]:
    errors: list[str] = []
    missing = sorted(required - record.keys())
    extra = sorted(record.keys() - allowed)
    if missing:
        errors.append(f"missing fields: {', '.join(missing)}")
    if extra:
        errors.append(f"unexpected fields: {', '.join(extra)}")
    return errors


def _compile_patterns(config: dict[str, Any]) -> dict[str, re.Pattern[str]]:
    patterns = config.get("patterns")
    if not isinstance(patterns, dict):
        raise ValueError("missing [patterns] table")
    return {name: re.compile(value) for name, value in patterns.items()}


def validate_run_record(
    record: dict[str, Any], patterns: dict[str, re.Pattern[str]]
) -> list[str]:
    errors = _field_errors(record, RUN_REQUIRED, RUN_ALLOWED)
    for field_name, expected in EXPECTED_POLICY.items():
        if field_name in RUN_ALLOWED and record.get(field_name) != expected:
            errors.append(f"{field_name} must be {expected!r}")

    run_id = record.get("run_id")
    if not isinstance(run_id, str) or not patterns["run_id"].fullmatch(run_id):
        errors.append("run_id does not match canonical run_<ulid> format")

    source_external_id = record.get("source_external_id")
    source_id = record.get("source_id")
    if not isinstance(source_id, str) or not patterns["source_youtube"].fullmatch(source_id):
        errors.append("source_id is not a canonical YouTube source ID")
    if (
        not isinstance(source_external_id, str)
        or not re.fullmatch(r"[A-Za-z0-9_-]{11}", source_external_id)
    ):
        errors.append("source_external_id must preserve the 11-character YouTube ID")
    elif source_id != f"youtube_{source_external_id}":
        errors.append("source_id does not match source_external_id")

    if record.get("source_type") != "video":
        errors.append("source_type must be 'video'")
    if record.get("source_platform") != "youtube":
        errors.append("source_platform must be 'youtube'")
    if not isinstance(record.get("source_title"), str) or not record["source_title"].strip():
        errors.append("source_title must be a non-empty string")

    storage_path = record.get("storage_path")
    if not _is_safe_relative_path(storage_path):
        errors.append("storage_path must be a safe repository-relative POSIX path")
    elif not re.fullmatch(r"runs/V02/[^/]+/", storage_path):
        errors.append("storage_path must be below runs/V02/")

    directory_mode = record.get("directory_mode")
    if directory_mode not in {"canonical", "legacy_alias"}:
        errors.append("directory_mode must be canonical or legacy_alias")
    elif directory_mode == "canonical" and storage_path != f"runs/V02/{run_id}/":
        errors.append("canonical storage_path must end with the canonical run_id")
    elif directory_mode == "legacy_alias" and storage_path == f"runs/V02/{run_id}/":
        errors.append("legacy_alias must point to a non-canonical physical directory")

    if not _is_utc_timestamp(record.get("created_at"), patterns["utc_timestamp"]):
        errors.append("created_at must be a valid UTC ISO-8601 timestamp ending in Z")
    if record.get("created_at_provenance") not in {"observed_time", "registration_time"}:
        errors.append("created_at_provenance is invalid")

    hint = record.get("legacy_timestamp_hint")
    if hint is not None and not re.fullmatch(r"[0-9]{8}_[0-9]{6}", str(hint)):
        errors.append("legacy_timestamp_hint must use YYYYMMDD_HHMMSS")
    return errors


def validate_map_record(
    record: dict[str, Any], patterns: dict[str, re.Pattern[str]]
) -> list[str]:
    errors = _field_errors(record, MAP_REQUIRED, MAP_REQUIRED)
    if record.get("id_policy_version") != EXPECTED_POLICY["id_policy_version"]:
        errors.append("id_policy_version is not canonical")
    run_id = record.get("canonical_run_id")
    if not isinstance(run_id, str) or not patterns["run_id"].fullmatch(run_id):
        errors.append("canonical_run_id does not match run_<ulid>")

    directory_name = record.get("legacy_directory_name")
    if (
        not isinstance(directory_name, str)
        or not directory_name
        or directory_name in {".", ".."}
        or "/" in directory_name
        or "\\" in directory_name
    ):
        errors.append("legacy_directory_name is invalid")

    relative_path = record.get("legacy_relative_path")
    if not _is_safe_relative_path(relative_path):
        errors.append("legacy_relative_path must be a safe relative POSIX path")

    project_version = record.get("project_version")
    source_id = record.get("source_id")
    if project_version == "v0.1":
        if record.get("storage_version") != "V01":
            errors.append("v0.1 mapping storage_version must be V01")
        if record.get("mapping_status") != "indexed_without_rewrite":
            errors.append("v0.1 mapping_status must be indexed_without_rewrite")
        if record.get("mapping_basis") != "directory_and_existing_metadata":
            errors.append("v0.1 mapping_basis is invalid")
        expected_path = f"runs/V01/{directory_name}/"
        if relative_path != expected_path:
            errors.append("v0.1 legacy_relative_path does not match directory name")
        if source_id is not None and (
            not isinstance(source_id, str)
            or not patterns["source_bilibili_legacy"].fullmatch(source_id)
        ):
            errors.append("v0.1 source_id must be null or canonical Bilibili ID")
    elif project_version == "v0.2":
        if record.get("storage_version") != "V02":
            errors.append("v0.2 mapping storage_version must be V02")
        if record.get("mapping_status") != "legacy_alias":
            errors.append("v0.2 mapping_status must be legacy_alias")
        if record.get("mapping_basis") != "existing_trial_directory_and_source_external_id":
            errors.append("v0.2 mapping_basis is invalid")
        expected_path = f"runs/V02/{directory_name}/"
        if relative_path != expected_path:
            errors.append("v0.2 legacy_relative_path does not match directory name")
        if not isinstance(source_id, str) or not patterns["source_youtube"].fullmatch(source_id):
            errors.append("v0.2 source_id must be a canonical YouTube source ID")
    else:
        errors.append("project_version must be v0.1 or v0.2")

    if not _is_utc_timestamp(record.get("mapped_at"), patterns["utc_timestamp"]):
        errors.append("mapped_at must be a valid UTC ISO-8601 timestamp ending in Z")
    return errors


def _duplicate_values(records: list[dict[str, Any]], field_name: str) -> set[str]:
    values = [record.get(field_name) for record in records]
    return {value for value in values if isinstance(value, str) and values.count(value) > 1}


def _validate_dataset_state(record: dict[str, Any], patterns: dict[str, re.Pattern[str]]) -> list[str]:
    errors: list[str] = []
    status = record.get("dataset_status")
    version = record.get("dataset_version")
    build_id = record.get("dataset_build_id")
    if not isinstance(build_id, str) or not patterns["dataset_build_id"].fullmatch(build_id):
        errors.append("dataset_build_id is invalid")
    if status == "draft" and version is not None:
        errors.append("draft dataset_version must be null")
    elif status == "frozen" and (
        not isinstance(version, str) or not patterns["dataset_version"].fullmatch(version)
    ):
        errors.append("frozen dataset_version is missing or invalid")
    elif status not in {"draft", "frozen"}:
        errors.append("dataset_status must be draft or frozen")
    return errors


def _encode_ulid(timestamp_ms: int, random_bits: int) -> str:
    value = (timestamp_ms << 80) | random_bits
    chars: list[str] = []
    for _ in range(26):
        value, index = divmod(value, 32)
        chars.append(ULID_ALPHABET[index])
    return "".join(reversed(chars))


def _reference_ulid() -> str:
    timestamp_ms = time.time_ns() // 1_000_000
    return _encode_ulid(timestamp_ms, int.from_bytes(secrets.token_bytes(10), "big"))


def run_self_tests(patterns: dict[str, re.Pattern[str]], audit: Audit) -> None:
    valid_run = {
        "id_policy_version": "id_policy_v1.0.0",
        "project_version": "v0.2",
        "storage_version": "V02",
        "release_id": "truthfulness_v0.2_youtube_video",
        "run_id": "run_01arz3ndektsv4rrffq69g5fav",
        "source_id": "youtube_A1b2C3d4E5F",
        "source_external_id": "A1b2C3d4E5F",
        "source_type": "video",
        "source_platform": "youtube",
        "source_title": "Synthetic title",
        "storage_path": "runs/V02/run_01arz3ndektsv4rrffq69g5fav/",
        "directory_mode": "canonical",
        "created_at": "2026-07-17T08:00:00Z",
        "created_at_provenance": "registration_time",
    }
    if validate_run_record(valid_run, patterns):
        audit.failed("self-test valid run", "valid synthetic run was rejected")
    else:
        audit.passed("self-test accepts a valid canonical run")

    invalid_mutations = {
        "title-shaped run ID": {"run_id": "youtube_title_20260717"},
        "lowercase storage version": {"storage_version": "v02"},
        "short source ID": {"source_external_id": "too_short", "source_id": "youtube_too_short"},
        "canonical path mismatch": {"storage_path": "runs/V02/other/"},
    }
    for label, mutation in invalid_mutations.items():
        candidate = copy.deepcopy(valid_run)
        candidate.update(mutation)
        if validate_run_record(candidate, patterns):
            audit.passed(f"self-test rejects {label}")
        else:
            audit.failed(f"self-test {label}", "invalid synthetic record was accepted")

    valid_draft = {
        "dataset_build_id": "dataset_build_01arz3ndektsv4rrffq69g5fav",
        "dataset_status": "draft",
        "dataset_version": None,
    }
    invalid_draft = dict(valid_draft, dataset_version="truthfulness_youtube_video_ds_v0.1.0")
    if not _validate_dataset_state(valid_draft, patterns):
        audit.passed("self-test accepts draft dataset without release version")
    else:
        audit.failed("self-test draft dataset", "valid draft state was rejected")
    if _validate_dataset_state(invalid_draft, patterns):
        audit.passed("self-test rejects formal dataset_version on draft data")
    else:
        audit.failed("self-test dataset freeze", "draft data received a formal version")

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        generated = list(executor.map(lambda _: _reference_ulid(), range(2048)))
    unique = len(set(generated)) == len(generated)
    valid_case = all(patterns["ulid"].fullmatch(value) for value in generated)
    ordered = _encode_ulid(1_000, 0) < _encode_ulid(1_001, 0)
    if unique and valid_case and ordered:
        audit.passed("self-test ULID uniqueness, concurrency, lowercase charset and time ordering")
    else:
        audit.failed(
            "self-test ULID generator",
            f"unique={unique}, valid_case={valid_case}, ordered={ordered}",
        )

    canonical_additions = {
        "event_id": "event_01arz3ndektsv4rrffq69g5fav",
        "exp_id": "exp_01arz3ndektsv4rrffq69g5fav",
        "dag_version": "youtube_truthfulness_dag_v1.1.0",
        "export_id": "export_01arz3ndektsv4rrffq69g5fav",
        "load_plan_id": "load_plan_01arz3ndektsv4rrffq69g5fav",
        "load_batch_id": "load_batch_01arz3ndektsv4rrffq69g5fav",
        "projection_attempt_id": "attempt_01arz3ndektsv4rrffq69g5fav",
        "load_receipt_id": "load_receipt_01arz3ndektsv4rrffq69g5fav",
    }
    invalid_additions = {
        "event_id": "event_title_20260718",
        "exp_id": "experiment_01arz3ndektsv4rrffq69g5fav",
        "dag_version": "youtube_truthfulness_dag_v1_1_0",
        "export_id": "warehouse_export_title",
        "load_plan_id": "loadplan_01arz3ndektsv4rrffq69g5fav",
        "load_batch_id": "loadbatch_01arz3ndektsv4rrffq69g5fav",
        "projection_attempt_id": "projection_attempt_title",
        "load_receipt_id": "receipt_01arz3ndektsv4rrffq69g5fav",
    }
    accepted = all(patterns[name].fullmatch(value) for name, value in canonical_additions.items())
    rejected = all(not patterns[name].fullmatch(value) for name, value in invalid_additions.items())
    if accepted and rejected:
        audit.passed(
            "self-test execution, warehouse and DAG canonical ID/version formats"
        )
    else:
        audit.failed(
            "self-test WP1 canonical formats",
            f"accepted={accepted}, rejected={rejected}",
        )


def validate_policy(root: Path, audit: Audit) -> tuple[dict[str, Any], dict[str, re.Pattern[str]]] | None:
    path = root / "configs" / "version_id_policy.toml"
    try:
        with path.open("rb") as handle:
            config = tomllib.load(handle)
        patterns = _compile_patterns(config)
    except (OSError, ValueError, tomllib.TOMLDecodeError, re.error) as exc:
        audit.failed("policy", str(exc))
        return None

    policy = config.get("policy", {})
    mismatches = [
        f"{field_name}={policy.get(field_name)!r}"
        for field_name, expected in EXPECTED_POLICY.items()
        if policy.get(field_name) != expected
    ]
    if mismatches:
        audit.failed("policy fixed values", ", ".join(mismatches))
    else:
        audit.passed("canonical policy fixed values and regular expressions")
    storage_roots = config.get("storage_roots", {})
    expected_storage_roots = {
        "repository": "repository",
        "claim_warehouse": "ubuntu_v02_claim_warehouse",
        "claim_warehouse_environment_variable": (
            "VIDEO_TRUTHFULNESS_WAREHOUSE_V02_ROOT"
        ),
        "allow_private_absolute_mapping_in_public_records": False,
    }
    storage_mismatches = [
        f"{name}={storage_roots.get(name)!r}"
        for name, expected in expected_storage_roots.items()
        if storage_roots.get(name) != expected
    ]
    if storage_mismatches:
        audit.failed("storage-root policy", ", ".join(storage_mismatches))
    else:
        audit.passed("repository and private Claim warehouse storage roots are frozen")
    return config, patterns


def validate_schemas(root: Path, audit: Audit) -> dict[str, dict[str, Any]]:
    schema_paths = {
        "run": root / "schemas" / "run_identity_v1.schema.json",
        "map": root / "schemas" / "legacy_run_id_map_v1.schema.json",
    }
    schemas: dict[str, dict[str, Any]] = {}
    try:
        for name, path in schema_paths.items():
            schemas[name] = _load_json(path)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        audit.failed("schemas", str(exc))
        return schemas

    try:
        from jsonschema import Draft202012Validator

        for schema in schemas.values():
            Draft202012Validator.check_schema(schema)
        audit.passed("JSON Schema draft 2020-12 structural validation")
    except ImportError:
        audit.warn("schemas", "jsonschema is unavailable; JSON syntax was checked")
    except Exception as exc:  # jsonschema exposes several schema-error subclasses
        audit.failed("schemas", str(exc))
    return schemas


def _validate_with_jsonschema(
    instance: dict[str, Any], schema: dict[str, Any]
) -> list[str]:
    try:
        from jsonschema import Draft202012Validator, FormatChecker
    except ImportError:
        return []
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    return [error.message for error in sorted(validator.iter_errors(instance), key=str)]


def validate_v01(
    root: Path,
    patterns: dict[str, re.Pattern[str]],
    map_schema: dict[str, Any] | None,
    require_private: bool,
    audit: Audit,
) -> None:
    run_root = root / "runs" / "V01"
    map_path = run_root / "legacy_run_id_map.jsonl"
    if not run_root.is_dir() or not map_path.is_file():
        message = "runs/V01 and its mapping index are required"
        if require_private:
            audit.failed("V01 mapping", message)
        else:
            audit.warn("V01 mapping", f"{message}; private validation skipped")
        return

    try:
        records = _load_jsonl(map_path)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        audit.failed("V01 mapping", str(exc))
        return

    record_errors: list[str] = []
    for index, record in enumerate(records, start=1):
        for error in validate_map_record(record, patterns):
            record_errors.append(f"line {index}: {error}")
        if map_schema:
            for error in _validate_with_jsonschema(record, map_schema):
                record_errors.append(f"line {index} schema: {error}")
    for field_name in ("canonical_run_id", "legacy_directory_name", "legacy_relative_path"):
        duplicates = sorted(_duplicate_values(records, field_name))
        if duplicates:
            record_errors.append(f"duplicate {field_name}: {', '.join(duplicates)}")

    directories = sorted(path.name for path in run_root.iterdir() if path.is_dir())
    mapped = sorted(str(record.get("legacy_directory_name")) for record in records)
    if len(directories) != EXPECTED_V01_DIRECTORY_COUNT:
        record_errors.append(
            f"expected {EXPECTED_V01_DIRECTORY_COUNT} V01 directories, found {len(directories)}"
        )
    if directories != mapped:
        record_errors.append("mapping coverage does not exactly match V01 directory names")

    if record_errors:
        for error in record_errors:
            audit.failed("V01 mapping", error)
    else:
        audit.passed(f"V01 mapping covers exactly {len(records)} frozen directories")


def validate_v02(
    root: Path,
    patterns: dict[str, re.Pattern[str]],
    schemas: dict[str, dict[str, Any]],
    require_private: bool,
    audit: Audit,
) -> None:
    run_root = root / "runs" / "V02"
    map_path = run_root / "run_path_map.jsonl"
    if not run_root.is_dir() or not map_path.is_file():
        message = "runs/V02 and its path map are required"
        if require_private:
            audit.failed("V02 mapping", message)
        else:
            audit.warn("V02 mapping", f"{message}; private validation skipped")
        return

    try:
        records = _load_jsonl(map_path)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        audit.failed("V02 mapping", str(exc))
        return

    errors: list[str] = []
    for index, record in enumerate(records, start=1):
        for error in validate_map_record(record, patterns):
            errors.append(f"map line {index}: {error}")
        map_schema = schemas.get("map")
        if map_schema:
            for error in _validate_with_jsonschema(record, map_schema):
                errors.append(f"map line {index} schema: {error}")

        relative_path = record.get("legacy_relative_path")
        physical_dir = root / str(relative_path).rstrip("/")
        if not physical_dir.is_dir():
            errors.append(f"mapped directory does not exist: {relative_path}")
            continue
        identity_path = physical_dir / "run.json"
        try:
            identity = _load_json(identity_path)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            errors.append(f"{identity_path.relative_to(root).as_posix()}: {exc}")
            continue
        for error in validate_run_record(identity, patterns):
            errors.append(f"{identity_path.relative_to(root).as_posix()}: {error}")
        run_schema = schemas.get("run")
        if run_schema:
            for error in _validate_with_jsonschema(identity, run_schema):
                errors.append(f"{identity_path.relative_to(root).as_posix()} schema: {error}")
        if identity.get("run_id") != record.get("canonical_run_id"):
            errors.append("V02 run.json and path map have different run IDs")
        if identity.get("source_id") != record.get("source_id"):
            errors.append("V02 run.json and path map have different source IDs")
        if identity.get("storage_path") != relative_path:
            errors.append("V02 run.json and path map have different storage paths")

    for field_name in ("canonical_run_id", "legacy_directory_name", "legacy_relative_path"):
        duplicates = sorted(_duplicate_values(records, field_name))
        if duplicates:
            errors.append(f"duplicate {field_name}: {', '.join(duplicates)}")

    mapped_directories = {str(record.get("legacy_directory_name")) for record in records}
    for directory in (path for path in run_root.iterdir() if path.is_dir()):
        if patterns["run_id"].fullmatch(directory.name):
            identity_path = directory / "run.json"
            if not identity_path.is_file():
                errors.append(f"canonical directory lacks run.json: {directory.name}")
        elif directory.name not in mapped_directories:
            errors.append(f"non-canonical V02 directory lacks mapping: {directory.name}")

    incomplete = [
        path.relative_to(root).as_posix()
        for path in run_root.rglob("*")
        if path.is_file() and (path.suffix in {".part", ".tmp"} or path.name.endswith(".ytdl"))
    ]
    if incomplete:
        errors.append(f"incomplete media files found: {', '.join(incomplete)}")

    if errors:
        for error in errors:
            audit.failed("V02 mapping", error)
    else:
        audit.passed(f"V02 canonical identity resolves {len(records)} legacy alias record(s)")


def _result_payload(audit: Audit) -> dict[str, Any]:
    return {
        "status": "PASS" if not audit.errors else "FAIL",
        "checks": audit.checks,
        "warnings": audit.warnings,
        "errors": audit.errors,
        "summary": {
            "passed": len(audit.checks),
            "warnings": len(audit.warnings),
            "errors": len(audit.errors),
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="repository root (default: inferred from this script)",
    )
    parser.add_argument(
        "--require-private",
        action="store_true",
        help="fail if ignored V01/V02 identity files are unavailable",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="run in-memory positive, negative and ULID tests",
    )
    parser.add_argument("--json", action="store_true", help="emit one JSON result")
    args = parser.parse_args(argv)

    root = args.root.resolve()
    audit = Audit()
    policy_result = validate_policy(root, audit)
    schemas = validate_schemas(root, audit)
    if policy_result is not None:
        _, patterns = policy_result
        if args.self_test:
            run_self_tests(patterns, audit)
        validate_v01(root, patterns, schemas.get("map"), args.require_private, audit)
        validate_v02(root, patterns, schemas, args.require_private, audit)

    payload = _result_payload(audit)
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        for check in audit.checks:
            print(f"PASS: {check}")
        for warning in audit.warnings:
            print(f"WARN: {warning}")
        for error in audit.errors:
            print(f"ERROR: {error}")
        summary = payload["summary"]
        print(
            f"RESULT: {payload['status']} "
            f"(passed={summary['passed']}, warnings={summary['warnings']}, errors={summary['errors']})"
        )
    return 0 if not audit.errors else 1


if __name__ == "__main__":
    sys.exit(main())
