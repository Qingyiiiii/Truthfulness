# V02 active boundary

V02 is the active YouTube video-truthfulness project boundary.

- `project_version = v0.2`
- `storage_version = V02`
- `release_id = truthfulness_v0.2_youtube_video`
- New run directories use `runs/V02/run_<ulid>/`.
- V02 code may import `video_truthfulness.core` but must not import `video_truthfulness.versions.v01`.
- Bilibili defaults, title-shaped run IDs, V01 dataset paths, and the V01 Cookie extension are forbidden in V02 code.

This directory does not imply that a V02 dataset, experiment, report, or downstream Artifact exists. Empty business artifacts are not created for layout symmetry.

Canonical identity rules: [`docs/version_and_id_system.md`](../../docs/version_and_id_system.md).
