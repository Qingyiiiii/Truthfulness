from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from video_truthfulness.config import load_config
from video_truthfulness.evidence_store import evidence_screenshot_name
from video_truthfulness.fallback import save_page_fallback, strip_tracking_query
from video_truthfulness import media as media_module
from video_truthfulness.media import DownloadStrategy, MultiStrategyDownloadRunner, YtDlpDownloader
from video_truthfulness.naming import build_media_filename
from video_truthfulness.schemas import DownloadStatus, Evidence, EvidenceRelation, Platform, SourceType


def test_media_filename_contains_platform_title_and_timestamp() -> None:
    moment = datetime(2026, 6, 28, 17, 30, 15, tzinfo=timezone.utc)

    filename = build_media_filename(Platform.BILIBILI, "测试 视频 标题", "mp4", moment)

    assert filename.startswith("bilibili_测试_视频_标题_20260628_173015")
    assert filename.endswith(".mp4")


def test_config_loads_example_file() -> None:
    config = load_config(Path("configs/demo1.local.example.toml"))

    assert config.runs_dir == "runs"
    assert config.default_llm_provider == "ollama"
    assert config.single_download_only is True


def test_downloader_reports_missing_component_when_yt_dlp_absent(monkeypatch) -> None:
    downloader = YtDlpDownloader()
    runs_dir = Path("tmp/test_downloads")
    runs_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(downloader, "is_available", lambda: False)

    result = downloader.download_single(
        source_url="https://www.bilibili.com/video/BV_TEST",
        platform=Platform.BILIBILI,
        video_title="测试视频",
        runs_dir=runs_dir,
    )

    assert result.status == DownloadStatus.MISSING_COMPONENT
    assert "yt-dlp" in result.error_summary
    assert result.filename.startswith("bilibili_测试视频_")


def test_evidence_screenshot_name_contains_claim_evidence_and_source_type() -> None:
    evidence = Evidence(
        evidence_id="ev_001",
        claim_id="claim_001",
        search_query="query",
        source_url="https://example.com",
        page_title="Example",
        publisher="Example Publisher",
        retrieved_at=datetime(2026, 6, 28, 17, 30, 15, tzinfo=timezone.utc),
        selected_text="Evidence text long enough for testing.",
        source_type=SourceType.OFFICIAL,
        relation_to_claim=EvidenceRelation.SUPPORTS,
    )
    moment = datetime(2026, 6, 28, 17, 30, 15, tzinfo=timezone.utc)

    name = evidence_screenshot_name(evidence, moment)

    assert name == "claim_001_ev_001_20260628_173015_official.png"


def test_multi_strategy_runner_records_412_as_blocked(monkeypatch) -> None:
    runs_dir = Path("tmp/test_multi_strategy")
    runs_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(media_module, "_yt_dlp_available", lambda: True)

    def fake_run(command, capture_output, text, timeout, check):
        return SimpleNamespace(
            returncode=1,
            stdout="",
            stderr="ERROR: Unable to download JSON metadata: HTTP Error 412: Precondition Failed",
        )

    monkeypatch.setattr(media_module.subprocess, "run", fake_run)
    result = MultiStrategyDownloadRunner().run(
        source_url="https://www.bilibili.com/video/BV_TEST",
        platform=Platform.BILIBILI,
        video_title="测试视频",
        runs_dir=runs_dir,
        strategies=[DownloadStrategy(name="metadata_412_probe", extra_args=["--dump-json"], metadata_only=True)],
    )

    assert result.final_status == DownloadStatus.BLOCKED
    assert result.fallback_required is True
    assert result.attempts[0].blocked_or_denied is True
    assert (Path(result.run_dir) / "download_attempts.json").exists()


def test_save_page_fallback_writes_text_and_screenshot() -> None:
    run_dir = Path("tmp/test_page_fallback")
    artifact = save_page_fallback(
        run_dir=run_dir,
        page_url="https://www.bilibili.com/video/BV_TEST",
        page_title="测试页面",
        visible_text="页面可见文本",
        screenshot_bytes=b"fake-png-bytes",
        notes="test fallback",
    )

    assert Path(artifact.page_text_path).exists()
    assert artifact.screenshot_path is not None
    assert Path(artifact.screenshot_path).exists()
    assert (run_dir / "page_fallback.json").exists()


def test_strip_tracking_query_removes_browser_tracking_parameters() -> None:
    cleaned = strip_tracking_query("https://www.bilibili.com/video/BV_TEST/?vd_source=abc&spm_id_from=333")

    assert cleaned == "https://www.bilibili.com/video/BV_TEST/"


def test_multi_strategy_runner_uses_cookie_file_without_logging_values(monkeypatch) -> None:
    test_id = uuid4().hex
    runs_dir = Path("tmp") / f"test_cookie_download_{test_id}"
    cookie_dir = Path("tmp") / f"test_cookie_input_{test_id}"
    runs_dir.mkdir(parents=True, exist_ok=True)
    cookie_dir.mkdir(parents=True, exist_ok=True)
    cookie_path = cookie_dir / "bilibili-cookie.txt"
    cookie_path.write_text("Cookie: SESSDATA=fake-secret; bili_jct=fake-token\n", encoding="utf-8")
    monkeypatch.setattr(media_module, "_yt_dlp_available", lambda: True)
    monkeypatch.setattr(media_module, "_ffmpeg_location_args", lambda: [])

    def fake_run(command, capture_output, text, timeout, check):
        output_index = command.index("-o") + 1
        Path(command[output_index]).parent.mkdir(parents=True, exist_ok=True)
        Path(command[output_index]).write_bytes(b"fake media")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(media_module.subprocess, "run", fake_run)
    result = MultiStrategyDownloadRunner().run(
        source_url="https://www.bilibili.com/video/BV_TEST/",
        platform=Platform.BILIBILI,
        video_title="cookie测试视频",
        runs_dir=runs_dir,
        cookies_path=cookie_path,
        strategies=[DownloadStrategy(name="cookie_strategy", format_selector="worst")],
    )
    manifest = (Path(result.run_dir) / "download_attempts.json").read_text(encoding="utf-8")

    assert result.final_status == DownloadStatus.SUCCESS
    assert "--cookies" in result.attempts[0].command
    assert "[redacted]" in result.attempts[0].command
    assert "fake-secret" not in manifest
    assert "fake-token" not in manifest
    converted_cookie = Path(result.run_dir) / "private" / "cookies.netscape.txt"
    assert not converted_cookie.exists() or converted_cookie.stat().st_size == 0


def test_successful_download_clears_cookie_directory_source_file(monkeypatch) -> None:
    test_id = uuid4().hex
    runs_dir = Path("tmp") / f"test_cookie_source_cleanup_{test_id}"
    cookie_path = Path("cookie") / f"cleanup_{test_id}.txt"
    cookie_path.parent.mkdir(parents=True, exist_ok=True)
    cookie_path.write_text("Cookie: SESSDATA=fake-secret; bili_jct=fake-token\n", encoding="utf-8")
    monkeypatch.setattr(media_module, "_yt_dlp_available", lambda: True)
    monkeypatch.setattr(media_module, "_ffmpeg_location_args", lambda: [])

    def fake_run(command, capture_output, text, timeout, check):
        output_index = command.index("-o") + 1
        Path(command[output_index]).parent.mkdir(parents=True, exist_ok=True)
        Path(command[output_index]).write_bytes(b"fake media")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(media_module.subprocess, "run", fake_run)
    result = MultiStrategyDownloadRunner().run(
        source_url="https://www.bilibili.com/video/BV_TEST/",
        platform=Platform.BILIBILI,
        video_title="cookie清理测试视频",
        runs_dir=runs_dir,
        cookies_path=cookie_path,
        strategies=[DownloadStrategy(name="cookie_strategy", format_selector="worst")],
    )

    assert result.final_status == DownloadStatus.SUCCESS
    assert not cookie_path.exists() or cookie_path.stat().st_size == 0
