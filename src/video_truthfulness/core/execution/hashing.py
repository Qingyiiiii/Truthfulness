"""Canonical serialization and SHA-256 helpers for execution contracts."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping


def canonical_json_bytes(value: Any) -> bytes:
    """Serialize a JSON-compatible value deterministically without a trailing LF."""

    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def embedded_hash(value: Mapping[str, Any], hash_field: str) -> str:
    """Hash a contract object after removing its own hash field."""

    payload = dict(value)
    if hash_field not in payload:
        raise ValueError(f"Missing embedded hash field: {hash_field}")
    payload.pop(hash_field)
    return sha256_bytes(canonical_json_bytes(payload))


def verify_embedded_hash(value: Mapping[str, Any], hash_field: str) -> None:
    actual = value.get(hash_field)
    expected = embedded_hash(value, hash_field)
    if actual != expected:
        raise ValueError(f"{hash_field} mismatch: expected {expected}, observed {actual}")
