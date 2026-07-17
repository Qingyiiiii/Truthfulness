"""Small JSON helpers for the offline MVP."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel


def read_json(path: Path) -> Any:
    """Read UTF-8 JSON from disk."""

    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def to_jsonable(value: Any) -> Any:
    """Convert Pydantic models and containers into JSON-safe data."""

    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, list):
        return [to_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {key: to_jsonable(item) for key, item in value.items()}
    return value


def write_json(path: Path, value: Any) -> None:
    """Write pretty UTF-8 JSON to disk."""

    path.parent.mkdir(parents=True, exist_ok=True)
    payload = to_jsonable(value)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")


def append_jsonl(path: Path, value: Any) -> None:
    """Append one JSON object to a JSONL file."""

    path.parent.mkdir(parents=True, exist_ok=True)
    payload = to_jsonable(value)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False))
        handle.write("\n")
