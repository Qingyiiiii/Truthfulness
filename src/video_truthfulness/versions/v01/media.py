"""Media download and intake helpers.

Real platform download is intentionally isolated here. The module enforces
single-video execution, safe filenames, and clear failure statuses so platform
access problems do not pollute claim extraction or verdict logic.
"""

from __future__ import annotations

import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

from video_truthfulness.core.json_io import write_json
from video_truthfulness.core.media import (
    DownloadStrategy,
    build_yt_dlp_command as _build_yt_dlp_command,
    ffmpeg_location_args as _ffmpeg_location_args,
    redact_command as _redact_command,
    redact_sensitive_output as _redact_sensitive_output,
    yt_dlp_available as _yt_dlp_available,
)
from video_truthfulness.core.schemas import DownloadAttempt, DownloadRun, DownloadStatus, MediaAsset, Platform
from video_truthfulness.versions.v01.naming import build_media_filename, build_run_id


class DownloadBlockedError(RuntimeError):
    """Raised when a platform download is blocked or denied."""


class YtDlpDownloader:
    """Single-video downloader backed by the optional `yt-dlp` package."""

    downloader_name = "yt-dlp"

    def is_available(self) -> bool:
        """Return whether the Python `yt_dlp` module is installed."""

        return _yt_dlp_available()

    def download_single(
        self,
        source_url: str,
        platform: Platform,
        video_title: str,
        runs_dir: Path = Path("runtime/V01/reproduction-runs"),
        extension: str = "mp4",
        cookies_path: Path | None = None,
    ) -> MediaAsset:
        """Download one video into an explicit V01 reproduction directory."""

        _validate_v01_download_target(platform, runs_dir)
        created_at = datetime.now(timezone.utc)
        # Build a media filename that includes platform, title, and timestamp.
        filename = build_media_filename(platform, video_title, extension, created_at)
        # Keep the run directory readable and aligned with the saved media name.
        run_id = build_run_id(platform, video_title, created_at)
        # Store downloaded media under the run-local media directory.
        media_dir = runs_dir / run_id / "media"
        # Create the output directory before invoking the downloader.
        media_dir.mkdir(parents=True, exist_ok=True)
        # Resolve the expected final media path before building the command.
        media_path = media_dir / filename

        # Fail before platform access if the local downloader component is missing.
        if not self.is_available():
            return MediaAsset(
                asset_id=f"media_{run_id}",
                platform=platform,
                title=video_title,
                source_url=source_url,
                media_path=None,
                filename=filename,
                status=DownloadStatus.MISSING_COMPONENT,
                created_at=created_at,
                downloader=self.downloader_name,
                error_summary="yt-dlp is not installed. Install it before attempting platform download.",
            )

        # Convert user-provided cookie text to a run-local Netscape file when needed.
        cookie_args = _cookie_args(cookies_path, media_dir.parent, source_url)
        # Build one conservative yt-dlp command: no playlist, restricted filenames,
        # and 480p or lower to avoid oversized validation downloads.
        command = [
            sys.executable,
            "-m",
            "yt_dlp",
            "--no-playlist",
            "--restrict-filenames",
            "-f",
            "bv*[height<=480]+ba/b[height<=480]/b",
            "--merge-output-format",
            extension,
            *_ffmpeg_location_args(),
            *cookie_args,
            "-o",
            str(media_path),
            source_url,
        ]
        try:
            # Run one child process and capture output for structured error reporting.
            completed = subprocess.run(command, capture_output=True, text=True, timeout=1800, check=False)
        finally:
            # Remove only the converter-created cookie file; user-provided files stay untouched.
            _cleanup_generated_cookie_args(cookie_args, media_dir.parent)
        # Merge stdout and stderr because yt-dlp may emit useful diagnostics to either stream.
        combined_output = f"{completed.stdout}\n{completed.stderr}".strip()
        # Treat success as valid only when the expected media file exists.
        if completed.returncode == 0 and media_path.exists():
            # For user-authorized local cookie files under cookie/, remove source material after success.
            _cleanup_successful_source_cookie(cookies_path)
            return MediaAsset(
                asset_id=f"media_{run_id}",
                platform=platform,
                title=video_title,
                source_url=source_url,
                media_path=str(media_path),
                filename=filename,
                status=DownloadStatus.SUCCESS,
                created_at=created_at,
                downloader=self.downloader_name,
            )
        # Convert downloader text into public status labels such as blocked/failed.
        status = _classify_download_failure(combined_output)
        return MediaAsset(
            asset_id=f"media_{run_id}",
            platform=platform,
            title=video_title,
            source_url=source_url,
            media_path=None,
            filename=filename,
            status=status,
            created_at=created_at,
            downloader=self.downloader_name,
            error_summary=_redact_sensitive_output(combined_output),
        )


class MultiStrategyDownloadRunner:
    """Run a small, sequential set of compliant yt-dlp strategies."""

    def __init__(self, downloader_name: str = "yt-dlp") -> None:
        """Store the public downloader name used in attempt records."""

        # Keep the downloader name in one place so attempt records are stable.
        self.downloader_name = downloader_name

    def default_strategies(self) -> list[DownloadStrategy]:
        """Return the bounded Demo1 strategy list."""

        # The list is intentionally short: it tries meaningful variants without
        # repeatedly hitting the platform or creating batch-like behavior.
        return [
            DownloadStrategy(
                name="yt_dlp_default_480p",
                # Standard B站 attempt with a modest quality cap.
                format_selector="bv*[height<=480]+ba/b[height<=480]/b",
            ),
            DownloadStrategy(
                name="yt_dlp_browser_headers_480p",
                # Same quality cap, but with ordinary browser-like public headers.
                format_selector="bv*[height<=480]+ba/b[height<=480]/b",
                extra_args=[
                    "--user-agent",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/125 Safari/537.36",
                    "--referer",
                    "https://www.bilibili.com/",
                    "--add-header",
                    "Accept-Language: zh-CN,zh;q=0.9,en;q=0.8",
                ],
            ),
            DownloadStrategy(
                name="yt_dlp_metadata_probe",
                # Probe metadata separately to distinguish webpage/metadata failure
                # from media-stream or ffmpeg merge failure.
                extra_args=["--dump-json", "--skip-download"],
                metadata_only=True,
            ),
            DownloadStrategy(
                name="yt_dlp_low_quality",
                # Last media attempt asks yt-dlp for the smallest MP4-like format.
                format_selector="worst[ext=mp4]/worst",
            ),
        ]

    def run(
        self,
        source_url: str,
        platform: Platform,
        video_title: str,
        runs_dir: Path = Path("runtime/V01/reproduction-runs"),
        extension: str = "mp4",
        strategies: list[DownloadStrategy] | None = None,
        cookies_path: Path | None = None,
    ) -> DownloadRun:
        """Run strategies sequentially and stop after the first success."""

        _validate_v01_download_target(platform, runs_dir)
        # One timestamp anchors the run directory and the overall run summary.
        created_at = datetime.now(timezone.utc)
        # The run ID includes platform, title, and time for easy manual lookup.
        run_id = build_run_id(platform, video_title, created_at)
        # All attempts and fallback artifacts for this video stay in one run directory.
        run_dir = runs_dir / run_id
        # Media attempts write only under the run-local media directory.
        media_dir = run_dir / "media"
        # Create the media directory once before any strategy runs.
        media_dir.mkdir(parents=True, exist_ok=True)
        # Attempt records are appended in the exact execution order.
        attempts: list[DownloadAttempt] = []
        # Tests can inject a short strategy list; production uses the default list.
        selected_strategies = strategies or self.default_strategies()

        # If yt-dlp is missing, record a single local failure without touching B站.
        if not _yt_dlp_available():
            attempt = _missing_component_attempt(selected_strategies[0], platform, video_title, extension, created_at)
            attempts.append(attempt)
            result = self._build_result(run_id, platform, video_title, source_url, created_at, run_dir, attempts)
            self._write_result(run_dir, result)
            return result

        # Run strategies sequentially; this is the anti-fengkong boundary.
        for strategy in selected_strategies:
            attempt = self._run_strategy(
                strategy=strategy,
                source_url=source_url,
                platform=platform,
                video_title=video_title,
                media_dir=media_dir,
                extension=extension,
                cookies_path=cookies_path,
            )
            # Keep both success and failure attempts for later diagnosis.
            attempts.append(attempt)
            # Stop immediately after the first successful media file.
            if attempt.status == DownloadStatus.SUCCESS:
                break

        # Build and persist one run-level summary after all attempts finish.
        result = self._build_result(run_id, platform, video_title, source_url, created_at, run_dir, attempts)
        self._write_result(run_dir, result)
        return result

    def _run_strategy(
        self,
        strategy: DownloadStrategy,
        source_url: str,
        platform: Platform,
        video_title: str,
        media_dir: Path,
        extension: str,
        cookies_path: Path | None = None,
    ) -> DownloadAttempt:
        """Execute one yt-dlp strategy and normalize its result."""

        # Each strategy gets its own timing so slow/failing phases are visible.
        started_at = datetime.now(timezone.utc)
        # Include the strategy name in the filename to avoid overwriting attempts.
        filename = build_media_filename(platform, f"{video_title}_{strategy.name}", extension, started_at)
        # Expected path for this specific strategy's media output.
        media_path = media_dir / filename
        # Convert cookies inside the run directory so generated files stay ignored.
        cookie_args = _cookie_args(cookies_path, media_dir.parent, source_url)
        # Build the exact command that will be recorded after redaction.
        command = _build_yt_dlp_command(
            strategy,
            source_url,
            media_path,
            extension,
            cookie_args,
            ffmpeg_args=_ffmpeg_location_args(),
        )
        try:
            # Execute one strategy. check=False lets us normalize all failures ourselves.
            completed = subprocess.run(command, capture_output=True, text=True, timeout=1800, check=False)
        finally:
            # Do not keep copied browser cookies in run artifacts after the process exits.
            _cleanup_generated_cookie_args(cookie_args, media_dir.parent)
        # Capture the end time after the child process exits.
        ended_at = datetime.now(timezone.utc)
        # Preserve useful diagnostics without assuming which stream yt-dlp used.
        combined_output = f"{completed.stdout}\n{completed.stderr}".strip()
        # Return code 0 means the command completed, but media-only strategies still
        # need an actual file existence check.
        if completed.returncode == 0:
            if strategy.metadata_only:
                # Metadata probes validate access but intentionally do not produce media.
                status = DownloadStatus.FAILED
                error_summary = "Metadata probe succeeded, but this strategy does not download media."
            elif media_path.exists():
                # Remove the local source cookie after a successful authorized download.
                _cleanup_successful_source_cookie(cookies_path)
                # Success requires both a zero return code and an output file.
                return DownloadAttempt(
                    strategy_name=strategy.name,
                    status=DownloadStatus.SUCCESS,
                    started_at=started_at,
                    ended_at=ended_at,
                    command=_redact_command(command),
                    media_path=str(media_path),
                    filename=filename,
                    error_summary="",
                    blocked_or_denied=False,
                )
            else:
                # Guard against a downloader reporting success while producing no file.
                status = DownloadStatus.FAILED
                error_summary = "yt-dlp exited successfully but expected media file was not found."
        else:
            # Non-zero return codes are classified for public reporting.
            status = _classify_download_failure(combined_output)
            # Redact and trim downloader output before writing it to disk.
            error_summary = _redact_sensitive_output(combined_output)
        # Normalize every failure into the same schema.
        return DownloadAttempt(
            strategy_name=strategy.name,
            status=status,
            started_at=started_at,
            ended_at=ended_at,
            command=_redact_command(command),
            media_path=None,
            filename=filename,
            error_summary=error_summary,
            blocked_or_denied=status == DownloadStatus.BLOCKED,
        )

    def _build_result(
        self,
        run_id: str,
        platform: Platform,
        video_title: str,
        source_url: str,
        created_at: datetime,
        run_dir: Path,
        attempts: list[DownloadAttempt],
    ) -> DownloadRun:
        """Build the final run summary from attempts."""

        # Find the first successful attempt, if any.
        success = next((attempt for attempt in attempts if attempt.status == DownloadStatus.SUCCESS), None)
        # If nothing succeeded, use the last attempt status as the run status.
        final_status = DownloadStatus.SUCCESS if success else (attempts[-1].status if attempts else DownloadStatus.FAILED)
        return DownloadRun(
            run_id=run_id,
            platform=platform,
            title=video_title,
            source_url=source_url,
            created_at=created_at,
            run_dir=str(run_dir),
            attempts=attempts,
            final_status=final_status,
            final_media_path=success.media_path if success else None,
            # Browser/manual fallback is needed whenever no media file exists.
            fallback_required=success is None,
        )

    def _write_result(self, run_dir: Path, result: DownloadRun) -> None:
        """Persist the download attempt summary for review."""

        # Persist attempt data next to fallback artifacts for manual review.
        write_json(run_dir / "download_attempts.json", result)


def _classify_download_failure(output: str) -> DownloadStatus:
    """Map downloader text to a conservative public failure status."""

    # Match in lower case so English downloader errors are handled consistently.
    lowered = output.lower()
    # 412 is included because B站 frequently uses it for request/risk rejection.
    blocked_markers = (
        "login",
        "sign in",
        "forbidden",
        "403",
        "412",
        "precondition failed",
        "private",
        "copyright",
        "drm",
        "blocked",
        "risk",
        "风控",
    )
    # Any blocker marker means we should stop treating this as an ordinary failure.
    if any(marker in lowered for marker in blocked_markers):
        return DownloadStatus.BLOCKED
    # Unknown non-zero downloader failures stay generic until manually inspected.
    return DownloadStatus.FAILED


def _validate_v01_download_target(platform: Platform, runs_dir: Path) -> None:
    """Prevent the frozen Bilibili compatibility layer from writing into V02."""

    if platform != Platform.BILIBILI:
        raise ValueError("The frozen V01 download layer accepts only platform=bilibili.")
    resolved_target = Path(runs_dir).resolve(strict=False)
    v02_root = (Path.cwd() / "runs" / "V02").resolve(strict=False)
    try:
        resolved_target.relative_to(v02_root)
    except ValueError:
        return
    raise ValueError("The frozen V01 download layer cannot write into runs/V02.")


def _missing_component_attempt(
    strategy: DownloadStrategy,
    platform: Platform,
    video_title: str,
    extension: str,
    moment: datetime,
) -> DownloadAttempt:
    """Create a synthetic attempt for a missing local component."""

    # Build the filename that would have been used, so the run remains inspectable.
    filename = build_media_filename(platform, f"{video_title}_{strategy.name}", extension, moment)
    return DownloadAttempt(
        strategy_name=strategy.name,
        status=DownloadStatus.MISSING_COMPONENT,
        started_at=moment,
        ended_at=moment,
        command=[],
        media_path=None,
        filename=filename,
        error_summary="yt-dlp is not installed. Install it before attempting platform download.",
        blocked_or_denied=False,
    )


def _cookie_args(cookies_path: Path | None, run_dir: Path, source_url: str) -> list[str]:
    """Return yt-dlp cookie args without exposing cookie values."""

    # No cookie file means the strategy remains a public unauthenticated attempt.
    if cookies_path is None:
        return []
    # Resolve the user-provided cookie path before reading it.
    source_cookie_path = Path(cookies_path)
    # Prepare a Netscape cookie file under the ignored run directory.
    prepared_cookie_path = _prepare_netscape_cookie_file(source_cookie_path, run_dir, source_url)
    # yt-dlp consumes Netscape cookies through --cookies <path>.
    return ["--cookies", str(prepared_cookie_path)]


def _prepare_netscape_cookie_file(cookies_path: Path, run_dir: Path, source_url: str) -> Path:
    """Convert a raw Cookie header file to Netscape format when necessary."""

    # Read the cookie file without printing or logging its content.
    raw = cookies_path.read_text(encoding="utf-8-sig", errors="replace").strip()
    # Reuse existing Netscape files directly because yt-dlp already accepts them.
    if raw.startswith("# Netscape") or _looks_like_netscape_cookie(raw):
        return cookies_path
    # Store converted cookies in a private run-local directory ignored by Git.
    private_dir = run_dir / "private"
    private_dir.mkdir(parents=True, exist_ok=True)
    converted_path = private_dir / "cookies.netscape.txt"
    # Convert the single-line Cookie header into Netscape rows for the video domain.
    converted_path.write_text(_cookie_header_to_netscape(raw, source_url), encoding="utf-8")
    return converted_path


def _cleanup_generated_cookie_args(cookie_args: list[str], run_dir: Path) -> None:
    """Delete the run-local converted cookie file, leaving user files untouched."""

    # Nothing to clean when no cookie file was used.
    if "--cookies" not in cookie_args:
        return
    # Locate the path value that follows yt-dlp's --cookies flag.
    cookies_index = cookie_args.index("--cookies") + 1
    if cookies_index >= len(cookie_args):
        return
    # Only the converter-created file in run_dir/private is eligible for cleanup.
    cookie_path = Path(cookie_args[cookies_index]).resolve(strict=False)
    private_dir = (run_dir / "private").resolve(strict=False)
    try:
        cookie_path.relative_to(private_dir)
    except ValueError:
        return
    if cookie_path.name != "cookies.netscape.txt":
        return
    _clear_and_delete_file(cookie_path)
    # Remove the private directory when it became empty after deleting the cookie file.
    if private_dir.exists() and not any(private_dir.iterdir()):
        private_dir.rmdir()


def _cleanup_successful_source_cookie(cookies_path: Path | None) -> None:
    """Clear and delete successful source cookie files stored in local cookie folders."""

    # Nothing was supplied, so no user-authorized source file exists.
    if cookies_path is None:
        return
    # Only clear files placed in the repository's ignored cookie folders.
    cookie_path = Path(cookies_path).resolve(strict=False)
    allowed_dirs = [Path("cookie").resolve(strict=False), Path("cookies").resolve(strict=False)]
    if not any(_is_path_relative_to(cookie_path, allowed_dir) for allowed_dir in allowed_dirs):
        return
    # Never try to remove directories through the cookie cleanup path.
    if cookie_path.exists() and not cookie_path.is_file():
        return
    _clear_and_delete_file(cookie_path)


def _clear_and_delete_file(path: Path) -> None:
    """Blank a sensitive file before trying to delete it on Windows."""

    # Missing files are already clean.
    if not path.exists():
        return
    content_cleared = False
    try:
        # Blank first; deletion can be temporarily denied on Windows.
        path.write_text("", encoding="utf-8")
        content_cleared = True
    except OSError:
        content_cleared = False
    for attempt_index in range(5):
        try:
            path.unlink(missing_ok=True)
            return
        except PermissionError:
            if attempt_index == 4 and not content_cleared:
                raise
            if attempt_index == 4:
                return
            time.sleep(0.1)


def _is_path_relative_to(path: Path, parent: Path) -> bool:
    """Return whether `path` is inside `parent` on Python versions with Path.relative_to."""

    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _looks_like_netscape_cookie(raw: str) -> bool:
    """Detect Netscape cookie rows without exposing values."""

    # Netscape cookie rows have seven tab-separated fields.
    return any(line.count("\t") >= 6 for line in raw.splitlines() if line and not line.startswith("#"))


def _cookie_header_to_netscape(raw: str, source_url: str) -> str:
    """Convert `Cookie: a=b; c=d` text into a Netscape cookie file."""

    # Remove the optional Cookie: prefix used by copied browser headers.
    header = raw.strip()
    if header.lower().startswith("cookie:"):
        header = header.split(":", 1)[1].strip()
    # Use the video URL host as the cookie domain.
    host = urlsplit(source_url).hostname or "bilibili.com"
    # B站 cookies generally need to be valid for subdomains as well.
    domain = ".bilibili.com" if host.endswith("bilibili.com") else (host if host.startswith(".") else f".{host}")
    # Netscape header is required by yt-dlp's cookie parser.
    lines = ["# Netscape HTTP Cookie File"]
    for pair in header.split(";"):
        # Drop empty fragments caused by trailing semicolons.
        if not pair.strip() or "=" not in pair:
            continue
        # Split only once so cookie values containing "=" survive.
        name, value = pair.strip().split("=", 1)
        # Avoid writing malformed rows with empty cookie names.
        if not name:
            continue
        # Fields: domain, include_subdomains, path, secure, expiry, name, value.
        lines.append(f"{domain}\tTRUE\t/\tFALSE\t0\t{name}\t{value}")
    return "\n".join(lines) + "\n"
