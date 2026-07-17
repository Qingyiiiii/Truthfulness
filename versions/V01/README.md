# V01 frozen boundary

V01 is the frozen Bilibili seed and v0.1.1 quality-gate boundary.

- Historical run directories remain unchanged under `runs/V01/`.
- Version-specific data, experiments, reports, evaluation fixtures, tools, code, configs, schemas, tests, examples, and scripts are indexed from this directory.
- V01 compatibility code may import `video_truthfulness.core`; shared core and V02 code must not import V01.
- V01 commands are explicit compatibility commands and must not be selected as V02 defaults.
- V01 write-capable CLI commands are read-only by default and require `--allow-frozen-v01-write`; their default outputs are under `runtime/V01/`, never the 27 frozen runs or V02.
- Private manifests use repository-relative paths and remain ignored by Git.
- Cookie values, browser profiles, tokens, and private request headers are never frozen here.

Public source checkpoint: [`SOURCE_CHECKPOINT.md`](SOURCE_CHECKPOINT.md).

The private Bilibili seed implementation is preserved byte-for-byte as `v01_bilibili_seed_frozen.py`. Reproduction must use the guarded `v01_bilibili_seed.py` wrapper; the frozen implementation is hash evidence, not a direct active entry.

The current version and ID authority remains [`docs/version_and_id_system.md`](../../docs/version_and_id_system.md). V01 legacy IDs are not retroactively rewritten to V02 contracts.
