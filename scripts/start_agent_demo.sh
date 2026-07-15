#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
cd "$PROJECT_DIR"

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: docker is not installed." >&2
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  if [ "${EUID:-$(id -u)}" -ne 0 ] && command -v sudo >/dev/null 2>&1; then
    echo "Docker socket requires elevated access; retrying through sudo."
    exec sudo bash "$0" "$@"
  fi
  echo "ERROR: Docker daemon is unavailable." >&2
  exit 1
fi

docker compose up --build --detach

echo "Truthfulness API: http://localhost:8000/docs"
echo "Truthfulness UI:  http://localhost:8501"
docker compose ps
