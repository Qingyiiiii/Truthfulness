#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-$(pwd)}"
VENV="${VIDEO_TRUTHFULNESS_VENV:-$HOME/.venvs/video-truthfulness}"
PORT="${1:-8501}"

cd "$PROJECT_DIR"
export PYTHONPATH=src

exec "$VENV/bin/streamlit" run app/streamlit_app.py \
  --server.address 0.0.0.0 \
  --server.port "$PORT" \
  --server.headless true \
  --browser.gatherUsageStats false
