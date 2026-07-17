"""Export public JSON Schema files from the strict Pydantic models."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from video_truthfulness.versions.v01.training_data_schemas import SCHEMA_MODELS


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("schemas") / "versions" / "v01" / "training_data",
    )
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    for schema_name, model in sorted(SCHEMA_MODELS.items()):
        path = args.output_dir / f"{schema_name}.schema.json"
        path.write_text(
            json.dumps(
                model.model_json_schema(),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
            newline="\n",
        )
        print(f"schema={path}")


if __name__ == "__main__":
    main()
