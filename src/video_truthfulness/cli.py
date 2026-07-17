"""Command line entry point with explicit version routing."""

from __future__ import annotations

import argparse
from pathlib import Path

from video_truthfulness.core.schemas import Platform
from video_truthfulness.versions.v01.media import MultiStrategyDownloadRunner, YtDlpDownloader
from video_truthfulness.versions.v01.offline_pipeline import run_offline_demo
from video_truthfulness.versions.v01.training import run_gold_baseline_smoke
from video_truthfulness.versions.v01.training_data_quality import (
    build_training_data_pack_from_toml,
    validate_preference_review_file,
)


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser."""

    parser = argparse.ArgumentParser(prog="video-truthfulness")
    subparsers = parser.add_subparsers(dest="command", required=True)

    offline = subparsers.add_parser("v01-offline", help="Run the frozen V01 transcript/evidence MVP.")
    offline.add_argument("--transcript", required=True, type=Path, help="Path to transcript JSON.")
    offline.add_argument("--evidence", required=True, type=Path, help="Path to evidence JSON.")
    offline.add_argument(
        "--runs-dir",
        default=Path("runtime/V01/reproduction-runs"),
        type=Path,
        help="V01 compatibility output directory; frozen runs/V01 is never the default.",
    )
    offline.add_argument("--title", default="offline_demo", help="Human-readable video title for this run.")
    offline.add_argument("--source-url", default=None, help="Optional source URL for report metadata.")
    _add_v01_write_gate(offline)
    download = subparsers.add_parser("v01-download", help="Try one frozen V01 platform download.")
    download.add_argument("--url", required=True, help="Video URL to download.")
    download.add_argument("--platform", required=True, choices=[Platform.BILIBILI.value], help="Frozen V01 platform.")
    download.add_argument("--title", required=True, help="Video title used for safe filename creation.")
    download.add_argument("--runs-dir", default=Path("runtime/V01/reproduction-runs"), type=Path)
    download.add_argument("--extension", default="mp4", help="Requested merged output extension.")
    download.add_argument("--cookies", default=None, type=Path, help="Optional local cookie file; values are not logged.")
    _add_v01_write_gate(download)
    download_multi = subparsers.add_parser("v01-download-multi", help="Run frozen V01 download strategies.")
    download_multi.add_argument("--url", required=True, help="Video URL to download.")
    download_multi.add_argument("--platform", required=True, choices=[Platform.BILIBILI.value], help="Frozen V01 platform.")
    download_multi.add_argument("--title", required=True, help="Video title used for safe filename creation.")
    download_multi.add_argument("--runs-dir", default=Path("runtime/V01/reproduction-runs"), type=Path)
    download_multi.add_argument("--extension", default="mp4", help="Requested merged output extension.")
    download_multi.add_argument("--cookies", default=None, type=Path, help="Optional local cookie file; values are not logged.")
    _add_v01_write_gate(download_multi)
    train_baseline = subparsers.add_parser(
        "v01-train-baseline",
        help="Validate gold JSONL and run a tiny majority-label training smoke test.",
    )
    train_baseline.add_argument("--gold-jsonl", required=True, type=Path, help="Gold-only claim JSONL batch.")
    train_baseline.add_argument("--batch-id", required=True, help="Expected gold batch id.")
    train_baseline.add_argument("--exp-id", required=True, help="Experiment id under --experiments-dir.")
    train_baseline.add_argument(
        "--experiments-dir",
        default=Path("runtime/V01/reproduction-experiments"),
        type=Path,
        help="Directory for training smoke outputs.",
    )
    train_baseline.add_argument("--seed", default=20260701, type=int, help="Deterministic split seed.")
    train_baseline.add_argument("--train-ratio", default=0.5, type=float, help="Train split ratio.")
    train_baseline.add_argument("--dev-ratio", default=0.25, type=float, help="Dev split ratio.")
    train_baseline.add_argument("--test-ratio", default=0.25, type=float, help="Test split ratio.")
    train_baseline.add_argument(
        "--smoke-test",
        action="store_true",
        help="Required acknowledgement that this is not a formal long training run.",
    )
    train_baseline.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace existing generated files in the experiment directory.",
    )
    _add_v01_write_gate(train_baseline)
    training_data_pack = subparsers.add_parser(
        "v01-training-data-pack",
        help="Build quality, SFT, synthetic, and preference artifacts from reviewed JSONL.",
    )
    training_data_pack.add_argument(
        "--config",
        required=True,
        type=Path,
        help="TOML configuration for the versioned training-data pack.",
    )
    _add_v01_write_gate(training_data_pack)
    validate_preference = subparsers.add_parser(
        "v01-validate-preference-reviews",
        help="Validate pending or completed single-human preference review JSONL.",
    )
    validate_preference.add_argument(
        "--preference-jsonl",
        required=True,
        type=Path,
        help="PreferencePair JSONL to validate.",
    )
    validate_preference.add_argument(
        "--require-all-reviewed",
        action="store_true",
        help="Fail if any pair still has review_status=pending.",
    )
    return parser


def _add_v01_write_gate(command_parser: argparse.ArgumentParser) -> None:
    """Require an explicit opt-in before a frozen V01 command may write outputs."""

    command_parser.add_argument(
        "--allow-frozen-v01-write",
        action="store_true",
        help="Acknowledge that this compatibility command writes only to the supplied non-V02 output path.",
    )


def _require_v01_write_opt_in(args: argparse.Namespace) -> None:
    """Keep frozen V01 commands read-only unless the caller opts in explicitly."""

    write_commands = {
        "v01-offline",
        "v01-download",
        "v01-download-multi",
        "v01-train-baseline",
        "v01-training-data-pack",
    }
    if args.command in write_commands and not args.allow_frozen_v01_write:
        raise SystemExit(
            "Frozen V01 is read-only by default. Re-run with --allow-frozen-v01-write "
            "and an output path outside runs/V01 and all V02 directories."
        )


def main() -> None:
    """Run the selected CLI command."""

    parser = build_parser()
    args = parser.parse_args()
    _require_v01_write_opt_in(args)
    if args.command == "v01-offline":
        result = run_offline_demo(
            transcript_path=args.transcript,
            evidence_path=args.evidence,
            runs_dir=args.runs_dir,
            video_title=args.title,
            source_url=args.source_url,
        )
        print(f"run_dir={result.run_dir}")
        print(f"report_md={result.markdown_report_path}")
        print(f"report_json={result.json_report_path}")
    elif args.command == "v01-download":
        result = YtDlpDownloader().download_single(
            source_url=args.url,
            platform=Platform(args.platform),
            video_title=args.title,
            runs_dir=args.runs_dir,
            extension=args.extension,
            cookies_path=args.cookies,
        )
        print(result.model_dump_json(indent=2))
        if result.status.value != "success":
            raise SystemExit(2)
    elif args.command == "v01-download-multi":
        result = MultiStrategyDownloadRunner().run(
            source_url=args.url,
            platform=Platform(args.platform),
            video_title=args.title,
            runs_dir=args.runs_dir,
            extension=args.extension,
            cookies_path=args.cookies,
        )
        print(result.model_dump_json(indent=2))
        if result.final_status.value != "success":
            raise SystemExit(2)
    elif args.command == "v01-train-baseline":
        if not args.smoke_test:
            raise SystemExit("v01-train-baseline currently supports only explicit --smoke-test runs.")
        result = run_gold_baseline_smoke(
            gold_jsonl=args.gold_jsonl,
            batch_id=args.batch_id,
            exp_id=args.exp_id,
            experiments_dir=args.experiments_dir,
            seed=args.seed,
            train_ratio=args.train_ratio,
            dev_ratio=args.dev_ratio,
            test_ratio=args.test_ratio,
            overwrite=args.overwrite,
        )
        print(f"exp_dir={result.exp_dir}")
        print(f"config={result.config_path}")
        print(f"log={result.log_path}")
        print(f"metrics={result.metrics_path}")
        print(f"summary={result.summary_path}")
        print(f"handoff={result.handoff_path}")
    elif args.command == "v01-training-data-pack":
        result = build_training_data_pack_from_toml(args.config)
        print(f"output_dir={result.output_dir}")
        print(f"quality_records={result.quality_records_path}")
        print(f"sft_examples={result.sft_examples_path}")
        print(f"synthetic_examples={result.synthetic_examples_path}")
        print(f"preference_pairs={result.preference_pairs_path}")
        print(f"quality_report={result.report_md_path}")
        print(f"review_packet={result.review_packet_path}")
        print(f"handoff={result.handoff_path}")
        print(
            "counts="
            + str(
                {
                    "quality_records": result.summary["records"]["quality_records"],
                    "sft_examples": result.summary["records"]["sft_examples"],
                    "synthetic_examples": result.summary["records"]["synthetic_examples"],
                    "preference_pairs": result.summary["records"]["preference_pairs"],
                }
            )
        )
    elif args.command == "v01-validate-preference-reviews":
        summary = validate_preference_review_file(
            args.preference_jsonl,
            require_all_reviewed=args.require_all_reviewed,
        )
        print("validation=" + str(summary))
if __name__ == "__main__":
    main()
