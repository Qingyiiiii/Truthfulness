# runs

Runtime outputs are written here during local execution. Only this README should be committed; real run directories are ignored by Git.

## Version index

| Version | Local path | Primary source | State |
|---|---|---|---|
| v0.1 Seed | `runs/V01/` | Bilibili | Archived on 2026-07-16; 27 run directories |
| v0.2 | `runs/V02/` | YouTube video truthfulness | Active development; no frozen dataset is implied |

Canonical structure for every new v0.2 run:

```text
runs/V02/run_<ulid>/
  run.json
  report.md
  report.json
  evidence_manifest.json
  screenshots/
  media/
  frames/
```

`V01` and `V02` are uppercase storage partitions, not dataset versions. Historical v0.1 paths remain unchanged under `runs/V01/`; the pre-policy v0.2 trial directory is resolved through `run_path_map.jsonl` and is not a template for new runs.

The canonical definitions and validation command are in [Version and canonical ID policy](../docs/version_and_id_system.md).
