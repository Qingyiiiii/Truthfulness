"""Read-only CLI for validating execution contracts and their bound inputs."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence

from video_truthfulness.core.execution.hashing import canonical_json_bytes
from video_truthfulness.core.execution.materialization import (
    validate_input_materialization,
)
from video_truthfulness.core.execution.models import ExecutionContractError
from video_truthfulness.core.execution.recovery import (
    RecoveryValidationError,
    validate_handoff_recovery,
    validate_recovery_bundle,
)


class _UsageError(Exception):
    """Raised instead of terminating inside argparse."""


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise _UsageError(message)


def _parser() -> argparse.ArgumentParser:
    parser = _ArgumentParser(prog="python -m video_truthfulness.core.execution")
    groups = parser.add_subparsers(dest="group", required=True)
    recovery = groups.add_parser("recovery")
    commands = recovery.add_subparsers(dest="command", required=True)
    validate = commands.add_parser("validate")
    validate.add_argument("--bundle", required=True, type=Path)
    validate_handoff = commands.add_parser("validate-handoff")
    validate_handoff.add_argument("--handoff", required=True, type=Path)
    validate_handoff.add_argument("--repository-root", required=True, type=Path)
    materialization = groups.add_parser("materialization")
    materialization_commands = materialization.add_subparsers(
        dest="command",
        required=True,
    )
    materialization_validate = materialization_commands.add_parser("validate")
    materialization_validate.add_argument("--receipt", required=True, type=Path)
    materialization_validate.add_argument(
        "--repository-root",
        required=True,
        type=Path,
    )
    materialization_validate.add_argument(
        "--storage-root",
        required=True,
        type=Path,
    )
    return parser


def _single_line_error(error: BaseException) -> str:
    message = " ".join(str(error).split())
    return message or error.__class__.__name__


def _write_bytes(stream: object, data: bytes) -> None:
    """Write exact LF bytes even when Windows text streams translate newlines."""

    buffer = getattr(stream, "buffer", None)
    if buffer is not None:
        buffer.write(data)
        return
    stream.write(data.decode("utf-8"))


def main(argv: Sequence[str] | None = None) -> int:
    """Validate one declared contract without executing or publishing anything."""

    parser = _parser()
    try:
        args = parser.parse_args(argv)
    except _UsageError as error:
        _write_bytes(sys.stderr, f"error: {_single_line_error(error)}\n".encode())
        return 2

    try:
        if args.group == "recovery" and args.command == "validate":
            result = validate_recovery_bundle(args.bundle)
        elif args.group == "recovery":
            result = validate_handoff_recovery(
                args.handoff,
                repository_root=args.repository_root,
            )
        else:
            result = validate_input_materialization(
                args.receipt,
                repository_root=args.repository_root,
                storage_root=args.storage_root,
            )
        _write_bytes(sys.stdout, canonical_json_bytes(result.summary()) + b"\n")
    except (
        ExecutionContractError,
        RecoveryValidationError,
        ValueError,
        OSError,
    ) as error:
        _write_bytes(sys.stderr, f"error: {_single_line_error(error)}\n".encode())
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
