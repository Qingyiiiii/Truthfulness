"""Bounded JSON I/O primitives used by the execution contract layer."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Mapping

from video_truthfulness.core.execution.hashing import canonical_json_bytes, sha256_file


class ContractIOError(ValueError):
    """Raised when contract bytes cannot be read or published safely."""


def read_json(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ContractIOError(f"Invalid JSON contract at {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ContractIOError(f"JSON contract root must be an object: {path}")
    return raw


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise ContractIOError(f"Cannot read JSONL contract at {path}: {exc}") from exc
    if not text:
        return []
    if not text.endswith("\n"):
        raise ContractIOError(f"JSONL contract must end with LF: {path}")
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            raise ContractIOError(f"Blank JSONL line at {path}:{line_number}")
        try:
            raw = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ContractIOError(f"Invalid JSONL line at {path}:{line_number}: {exc}") from exc
        if not isinstance(raw, dict):
            raise ContractIOError(f"JSONL line must be an object at {path}:{line_number}")
        rows.append(raw)
    return rows


def _publish_temp(path: Path, data: bytes, *, immutable: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        if immutable:
            try:
                os.link(temporary, path)
            except FileExistsError as exc:
                raise ContractIOError(f"Immutable contract already exists: {path}") from exc
        else:
            os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if temporary.exists():
            temporary.unlink()


def write_json(path: Path, value: Mapping[str, Any], *, immutable: bool) -> str:
    """Publish canonical JSON plus one LF and return the final file hash."""

    data = canonical_json_bytes(dict(value)) + b"\n"
    _publish_temp(path, data, immutable=immutable)
    return sha256_file(path)
