"""Explicit read/write command surface for Artifact Registry and DAG inspection."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from video_truthfulness.core.artifacts.dag import derive_dag_state, explain_node, load_dag, validate_dag, write_dag_state
from video_truthfulness.core.artifacts.models import ArtifactRecord
from video_truthfulness.core.artifacts.projection import query_artifact, rebuild_sqlite_projection
from video_truthfulness.core.artifacts.registry import AppendOnlyRegistry


def _registry(path: str, scope: str, run_id: str | None) -> AppendOnlyRegistry:
    return AppendOnlyRegistry(Path(path), scope=scope, expected_run_id=run_id)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m video_truthfulness.core.artifacts")
    groups = parser.add_subparsers(dest="group", required=True)

    registry = groups.add_parser("registry")
    registry_commands = registry.add_subparsers(dest="command", required=True)
    for name in ("validate", "list", "stale"):
        command = registry_commands.add_parser(name)
        command.add_argument("--registry", required=True)
        command.add_argument("--scope", choices=("run", "cross_run"), required=True)
        command.add_argument("--run-id")
    show = registry_commands.add_parser("show")
    show.add_argument("artifact_id")
    show.add_argument("--registry", required=True)
    show.add_argument("--scope", choices=("run", "cross_run"), required=True)
    show.add_argument("--run-id")
    for name in ("register", "append-revision"):
        write = registry_commands.add_parser(name)
        write.add_argument("--manifest", required=True)
        write.add_argument("--registry", required=True)
        write.add_argument("--scope", choices=("run", "cross_run"), required=True)
        write.add_argument("--run-id")
    rebuild = registry_commands.add_parser("rebuild-index")
    rebuild.add_argument("--registry", action="append", required=True)
    rebuild.add_argument("--scope", action="append", choices=("run", "cross_run"), required=True)
    rebuild.add_argument("--run-id", action="append")
    rebuild.add_argument("--output", required=True)
    query = registry_commands.add_parser("query-index")
    query.add_argument("artifact_id")
    query.add_argument("--index", required=True)

    dag = groups.add_parser("dag")
    dag_commands = dag.add_subparsers(dest="command", required=True)
    dag_validate = dag_commands.add_parser("validate")
    dag_validate.add_argument("--dag", required=True)
    dag_status = dag_commands.add_parser("status")
    dag_status.add_argument("--dag", required=True)
    dag_status.add_argument("--registry", required=True)
    dag_status.add_argument("--run-id", required=True)
    dag_status.add_argument("--output")
    dag_explain = dag_commands.add_parser("explain")
    dag_explain.add_argument("--state", required=True)
    dag_explain.add_argument("--node", required=True)
    return parser


def main() -> None:
    args = _parser().parse_args()
    if args.group == "registry":
        if args.command == "rebuild-index":
            run_ids = args.run_id or []
            registries = [
                _registry(path, scope, run_ids[index] if index < len(run_ids) else None)
                for index, (path, scope) in enumerate(zip(args.registry, args.scope, strict=True))
            ]
            print(json.dumps(rebuild_sqlite_projection(Path(args.output), registries), sort_keys=True))
            return
        if args.command == "query-index":
            print(json.dumps(query_artifact(Path(args.index), args.artifact_id), sort_keys=True))
            return
        registry = _registry(args.registry, args.scope, args.run_id)
        if args.command == "validate":
            print(json.dumps(registry.validate(), sort_keys=True))
        elif args.command == "list":
            rows = [record.model_dump(mode="json") for record in registry.latest_records().values()]
            print(json.dumps(rows, ensure_ascii=False, sort_keys=True))
        elif args.command == "stale":
            rows = [record.artifact_id for record in registry.latest_records().values() if record.lifecycle_state == "stale"]
            print(json.dumps(sorted(rows)))
        elif args.command == "show":
            record = registry.latest_records().get(args.artifact_id)
            print(json.dumps(record.model_dump(mode="json") if record else None, ensure_ascii=False, sort_keys=True))
        elif args.command in {"register", "append-revision"}:
            raw = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
            records = [ArtifactRecord.model_validate(item) for item in raw["records"]]
            if args.command == "register" and any(record.record_revision != 1 for record in records):
                raise SystemExit("registry register only accepts revision 1 records; use append-revision.")
            if args.command == "append-revision" and any(record.record_revision <= 1 for record in records):
                raise SystemExit("registry append-revision only accepts metadata revisions greater than 1.")
            registry.append_many(records)
            print(json.dumps(registry.validate(), sort_keys=True))
        return

    dag = load_dag(Path(args.dag)) if hasattr(args, "dag") else None
    if args.command == "validate":
        print(json.dumps(validate_dag(dag), sort_keys=True))
    elif args.command == "status":
        registry = AppendOnlyRegistry(Path(args.registry), scope="run", expected_run_id=args.run_id)
        state = derive_dag_state(dag, registry.read_records(), run_id=args.run_id)
        if args.output:
            write_dag_state(Path(args.output), state)
        print(json.dumps(state, ensure_ascii=False, sort_keys=True))
    elif args.command == "explain":
        state = json.loads(Path(args.state).read_text(encoding="utf-8"))
        print(json.dumps(explain_node(state, args.node), ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
