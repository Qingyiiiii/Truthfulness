#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-$(pwd)}"
VENV="${VIDEO_TRUTHFULNESS_VENV:-$HOME/.venvs/video-truthfulness}"
WORKSPACE="${VIDEO_TRUTHFULNESS_WORKSPACE:-$HOME/video-truthfulness-workspace}"

mkdir -p "$WORKSPACE/v01-reproduction-runs" "$WORKSPACE/logs"

cd "$PROJECT_DIR"
export PYTHONPATH=src

"$VENV/bin/python" -m pytest -q -p no:cacheprovider

"$VENV/bin/python" -m video_truthfulness.cli v01-offline \
  --transcript examples/offline_demo/transcript.json \
  --evidence examples/offline_demo/evidence.json \
  --runs-dir "$WORKSPACE/v01-reproduction-runs" \
  --title offline_demo_wsl_standardized \
  --allow-frozen-v01-write
