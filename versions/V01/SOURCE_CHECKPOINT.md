# V01 source checkpoint

> Checkpoint time: `2026-07-17T10:16:41Z`
> Canonical switch time: `2026-07-17T10:36:42Z`
> Freeze status: `FROZEN_MIGRATED / TAG_NOT_CREATED`
> Git branch at checkpoint: `main`
> Public freeze commit: `b4b8ce5b07fb38c6ce97afdf0b2ecb793b606d26`
> Remote state at checkpoint: `main...origin/main`, no divergence

## Public freeze anchor

The pushed commit above contains the accepted v0.1.1 quality-gate and phase-one version/ID work before the stage-two directory split. At this checkpoint, no stage-two commit or tag had been created. Any later stage-two publication is a separate history entry and does not replace this pre-split freeze anchor.

The atomic v0.1.1 group is now discoverable at:

```text
src/video_truthfulness/versions/v01/training_data_quality.py
src/video_truthfulness/versions/v01/training_data_schemas.py
src/video_truthfulness/versions/v01/training.py
tests/versions/v01/test_training_data_quality.py
tests/versions/v01/test_training.py
configs/versions/v01/training_data_quality.example.toml
configs/versions/v01/train_baseline.smoke.example.toml
docs/versions/v01/training_data_quality.md
schemas/versions/v01/training_data/
examples/versions/v01/training_data/
scripts/versions/v01/export_training_data_schemas.py
README and explicit v01-* CLI references
```

Git history at the freeze commit restores the pre-split public paths. The active tree uses the paths above.

## Private source

| Role | Path | Bytes | SHA-256 | Policy |
|---|---|---:|---|---|
| exact frozen implementation | `src/video_truthfulness/versions/v01/v01_bilibili_seed_frozen.py` | 68158 | `c758441109d222cad9e21230b33039e517591c5b79a970a62efb38210b6047ec` | byte-identical to the pre-split private source; do not execute directly |
| compatibility wrapper | `src/video_truthfulness/versions/v01/v01_bilibili_seed.py` | 1808 | `a0019b884455e546a19a63206fb2df41e6e716032f08775acb6319a3e1437840` | read-only by default; explicit opt-in; outputs default below `runtime/V01/`; rejects every V02 partition |

Both files remain ignored/private. The old root source was removed only after the frozen copy hash matched.

## Validation evidence

- Version/ID validator at the freeze checkpoint: `12 passed, 0 warnings, 0 errors`.
- V01/v0.1.1 and shared non-Agent tests before split: `22 passed, 1 existing pytest configuration warning`.
- Post-split shared/core/V01/V02 boundary and V01 compatibility tests: `28 passed, 1 existing pytest configuration warning`.
- Pydantic training-data Schema export at checkpoint: byte-for-byte match with four public JSON Schemas.
- Cookie safety: two files under `cookie/`, both zero bytes; contents were not read.
- Credential scan: no real credential pattern found; only explicit fake test fixtures matched.
- Agent integration tests are environment-blocked because optional `langgraph` is not installed; no dependency was installed.

Post-migration safety history: a recheck detected three files totaling 51 bytes and triggered the mandatory stop without reading content. After the user removed all files, the `2026-07-17T10:49:49Z` recheck found `0` files and `0` bytes. Credential/public-boundary checks were then rerun and passed.

## Tag boundary

Stage two creates no Git tag, commit, or push during migration execution itself. Publication requires separate authorization. The immutable pre-split public anchor remains the pushed commit above; `tag = null` is recorded in the local manifest.
