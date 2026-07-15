"""Streamlit shell for Demo1.

The full UI is scheduled for a later stage. This shell documents the intended
entry point and can run the offline MVP when Streamlit is installed.
"""

from __future__ import annotations

import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import streamlit as st
import requests

from video_truthfulness.media import YtDlpDownloader
from video_truthfulness.offline_pipeline import run_offline_demo
from video_truthfulness.schemas import Platform


def main() -> None:
    """Render a minimal local Demo1 page."""

    st.set_page_config(page_title="Video Truthfulness Demo1", layout="wide")
    st.title("Video Truthfulness Demo1")
    st.caption("Evidence-first single-video verification demo.")

    agent_tab, offline_tab, download_tab, artifacts_tab = st.tabs(
        ["Evidence Agent", "Offline MVP", "Single Download", "Artifacts"]
    )

    with agent_tab:
        st.subheader("LangGraph + Chroma evidence agent")
        st.caption("Classification → retrieval → evidence check → generation → citation validation → refusal/review")
        api_url = st.text_input(
            "FastAPI base URL",
            os.getenv("TRUTHFULNESS_API_URL", "http://localhost:8000"),
        ).rstrip("/")
        query = st.text_area(
            "Question",
            "Aurora 地铁三号线何时正式开通？",
            height=100,
        )
        authorized = st.checkbox("I am authorized to submit this input", value=True)
        if st.button("Run evidence agent", type="primary"):
            try:
                response = requests.post(
                    f"{api_url}/v1/query",
                    json={"query": query, "authorized": authorized, "top_k": 4},
                    timeout=40,
                )
                response.raise_for_status()
                payload = response.json()
            except requests.RequestException as exc:
                st.error(f"FastAPI request failed: {exc}")
            else:
                status = payload["status"]
                if status == "answered":
                    st.success(status)
                elif status in {"insufficient_evidence", "human_review_required"}:
                    st.warning(status)
                else:
                    st.error(status)
                st.markdown("#### Answer")
                st.write(payload["answer"])
                citations = payload.get("citations", [])
                st.markdown("#### Citations")
                if citations:
                    for citation in citations:
                        st.markdown(
                            f"- **{citation['page_title']}** — {citation['publisher']} "
                            f"([source]({citation['source_url']}), score={citation['score']:.3f})"
                        )
                        st.code(citation["quote"], language=None)
                else:
                    st.caption("No citations returned for this terminal status.")
                telemetry = payload["telemetry"]
                token_usage = telemetry["tokens"]
                cost_usage = telemetry["cost"]
                metric_columns = st.columns(5)
                metric_columns[0].metric("Trace", payload["trace_id"][:12])
                metric_columns[1].metric("Latency", f"{telemetry['total_elapsed_ms']:.1f} ms")
                metric_columns[2].metric("Tokens", token_usage.get("total_tokens") or 0)
                amount = cost_usage.get("amount_usd")
                metric_columns[3].metric("Cost", "N/A" if amount is None else f"${amount:.6f}")
                metric_columns[4].metric("Retries", telemetry["retries"])
                if payload.get("review_task_id"):
                    st.info(f"Human review task: {payload['review_task_id']}")
                with st.expander("Trace and structured response"):
                    st.dataframe(telemetry["nodes"], use_container_width=True)
                    st.json(payload)

    with offline_tab:
        st.subheader("Offline transcript/evidence MVP")
        transcript_path = st.text_input("Transcript JSON", "examples/offline_demo/transcript.json")
        evidence_path = st.text_input("Evidence JSON", "examples/offline_demo/evidence.json")
        title = st.text_input("Video title", "offline_demo")
        if st.button("Run offline MVP"):
            result = run_offline_demo(Path(transcript_path), Path(evidence_path), video_title=title)
            st.success(f"Run written to {result.run_dir}")
            st.markdown(result.markdown_report_path.read_text(encoding="utf-8"))
            st.json(result.report.model_dump(mode="json"))

    with download_tab:
        st.subheader("Single compliant platform download")
        st.warning("This action attempts exactly one download. Stop if the platform blocks access.")
        platform = st.selectbox("Platform", [Platform.BILIBILI.value, Platform.DOUYIN.value, Platform.YOUTUBE.value])
        source_url = st.text_input("Video URL")
        video_title = st.text_input("Video title for filename")
        if st.button("Try one download"):
            if not source_url or not video_title:
                st.error("Video URL and title are required.")
            else:
                result = YtDlpDownloader().download_single(
                    source_url=source_url,
                    platform=Platform(platform),
                    video_title=video_title,
                )
                st.json(result.model_dump(mode="json"))
                if result.status.value == "success":
                    st.success(f"Saved media to {result.media_path}")
                else:
                    st.error(result.error_summary or result.status.value)

    with artifacts_tab:
        st.subheader("Run artifacts")
        st.write("Runtime outputs are written under `runs/<run_id>/` and ignored by Git except `runs/README.md`.")
        st.write("Evidence screenshots belong under `runs/<run_id>/screenshots/`.")
        st.write("Downloaded media belongs under `runs/<run_id>/media/`.")


if __name__ == "__main__":
    main()
