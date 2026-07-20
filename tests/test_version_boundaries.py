"""Dependency-direction and explicit-routing checks for the V01/V02 split."""

from __future__ import annotations

import argparse
import ast
import tomllib
from pathlib import Path

import pytest

from video_truthfulness.cli import _require_v01_write_opt_in, build_parser
from video_truthfulness.core.schemas import Platform
from video_truthfulness.versions.v01.media import YtDlpDownloader
from video_truthfulness.versions.v02.youtube import no_cookie_strategies


ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = ROOT / "src" / "video_truthfulness"


def _absolute_imports(package_dir: Path) -> set[str]:
    imports: set[str] = set()
    for path in package_dir.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                imports.add(node.module)
            elif isinstance(node, ast.Import):
                imports.update(alias.name for alias in node.names)
    return imports


def test_core_does_not_import_version_packages() -> None:
    assert not {
        name for name in _absolute_imports(PACKAGE_ROOT / "core") if name.startswith("video_truthfulness.versions")
    }


def test_v02_does_not_import_v01() -> None:
    assert not {
        name for name in _absolute_imports(PACKAGE_ROOT / "versions" / "v02") if ".versions.v01" in name
    }


def test_cli_routes_every_current_command_explicitly_to_v01() -> None:
    subparsers = next(
        action for action in build_parser()._actions if isinstance(action, argparse._SubParsersAction)
    )
    assert subparsers.choices
    assert all(name.startswith("v01-") for name in subparsers.choices)


def test_v01_write_commands_are_read_only_by_default() -> None:
    args = build_parser().parse_args(
        ["v01-offline", "--transcript", "input.json", "--evidence", "evidence.json"]
    )
    with pytest.raises(SystemExit, match="read-only by default"):
        _require_v01_write_opt_in(args)


def test_v01_downloader_rejects_v02_platform_and_path(tmp_path: Path) -> None:
    downloader = YtDlpDownloader()
    with pytest.raises(ValueError, match="only platform=bilibili"):
        downloader.download_single("https://example.invalid", Platform.YOUTUBE, "title", tmp_path)
    with pytest.raises(ValueError, match="cannot write into runs/V02"):
        downloader.download_single(
            "https://example.invalid",
            Platform.BILIBILI,
            "title",
            ROOT / "runs" / "V02" / "compatibility-output",
        )


def test_v02_youtube_strategy_has_no_v01_cookie_or_bilibili_policy() -> None:
    strategies = no_cookie_strategies()
    serialized = repr(strategies).lower()
    assert "bilibili" not in serialized
    assert "--cookies" not in serialized
    assert "referer" not in serialized
    assert "412" not in serialized


def test_v02_youtube_prefers_exact_480p_then_lowest_quality() -> None:
    strategies = no_cookie_strategies()
    selectors = [
        strategy.format_selector
        for strategy in strategies
        if not strategy.metadata_only
    ]

    assert selectors == [
        (
            "bestvideo[height=480]+bestaudio/"
            "best[height=480]/worstvideo+worstaudio/worst"
        ),
        "best[height=480]/worst",
    ]
    assert all("height=480" in selector for selector in selectors)
    assert all("height<=" not in selector for selector in selectors)
    assert all(selector.index("height=480") < selector.index("worst") for selector in selectors)


def test_gdb1_warehouse_uses_logical_root_without_public_absolute_mapping() -> None:
    with (ROOT / "configs" / "version_id_policy.toml").open("rb") as handle:
        policy = tomllib.load(handle)
    with (ROOT / "configs" / "storage" / "claim_warehouse_v1.toml").open(
        "rb"
    ) as handle:
        storage = tomllib.load(handle)

    assert policy["storage_roots"] == {
        "repository": "repository",
        "claim_warehouse": "ubuntu_v02_claim_warehouse",
        "claim_warehouse_environment_variable": (
            "VIDEO_TRUTHFULNESS_WAREHOUSE_V02_ROOT"
        ),
        "allow_private_absolute_mapping_in_public_records": False,
    }
    assert storage["storage_root_ref"] == "ubuntu_v02_claim_warehouse"
    assert (
        storage["storage_root_environment_variable"]
        == "VIDEO_TRUTHFULNESS_WAREHOUSE_V02_ROOT"
    )
    assert not any(
        token in repr(storage) for token in ("/home/", "D:\\\\", "C:\\\\")
    )


def test_gdb1_runtime_database_and_projection_files_remain_git_ignored() -> None:
    ignore = (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
    assert "*.duckdb" in ignore
    assert "*.duckdb.wal" in ignore
    assert "*.parquet" in ignore
