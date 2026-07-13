#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-$(pwd)}"
VENV="${VIDEO_TRUTHFULNESS_VENV:-$HOME/.venvs/video-truthfulness}"
WORKSPACE="${VIDEO_TRUTHFULNESS_WORKSPACE:-$HOME/video-truthfulness-workspace}"

export PROJECT_DIR
export WORKSPACE
export HF_HOME="${HF_HOME:-$HOME/.cache/huggingface}"
export LD_LIBRARY_PATH="$VENV/lib/python3.12/site-packages/nvidia/cublas/lib:$VENV/lib/python3.12/site-packages/nvidia/cudnn/lib:${LD_LIBRARY_PATH:-}"

cd "$PROJECT_DIR"

"$VENV/bin/python" - <<'PY'
from pathlib import Path
import json
import os
import subprocess
import time

import imageio_ffmpeg
from faster_whisper import WhisperModel

project = Path(os.environ["PROJECT_DIR"])
workspace = Path(os.environ["WORKSPACE"])
logs = workspace / "logs"
asr_dir = workspace / "asr-test"
logs.mkdir(parents=True, exist_ok=True)
asr_dir.mkdir(parents=True, exist_ok=True)

wavs = sorted((project / "runs").rglob("*.wav"), key=lambda p: p.stat().st_size)
if not wavs:
    raise SystemExit("no wav files found under runs")

source = wavs[0]
sample = asr_dir / "asr_gpu_tiny_sample_20s.wav"
ffmpeg = imageio_ffmpeg.get_ffmpeg_exe()
subprocess.run(
    [ffmpeg, "-y", "-ss", "10", "-t", "20", "-i", str(source), "-ar", "16000", "-ac", "1", str(sample)],
    check=True,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,
)

start = time.perf_counter()
model = WhisperModel("tiny", device="cuda", compute_type="float16")
segments_iter, info = model.transcribe(str(sample), language="zh", beam_size=1)
segments = list(segments_iter)
elapsed = round(time.perf_counter() - start, 3)

record = {
    "validation": "asr_gpu_tiny_short_audio",
    "source_audio": str(source),
    "sample_audio": str(sample),
    "model": "tiny",
    "device": "cuda",
    "compute_type": "float16",
    "language": getattr(info, "language", None),
    "duration": getattr(info, "duration", None),
    "segment_count": len(segments),
    "elapsed_seconds": elapsed,
    "first_segments": [
        {"start": round(segment.start, 2), "end": round(segment.end, 2), "text": segment.text.strip()}
        for segment in segments[:3]
    ],
}
log_path = logs / "asr_gpu_tiny_validation_20260709.json"
log_path.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")

print("source_audio=" + str(source))
print("sample_audio=" + str(sample))
print("segment_count=" + str(len(segments)))
print("elapsed_seconds=" + str(elapsed))
print("log_path=" + str(log_path))

if not segments:
    raise SystemExit("ASR completed but produced zero segments")
PY
