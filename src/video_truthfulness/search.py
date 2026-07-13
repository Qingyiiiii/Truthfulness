"""Offline query planning and manual evidence loading."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from video_truthfulness.json_io import read_json
from video_truthfulness.schemas import Claim, Evidence, SearchQueryPlan


def build_query_plan(claims: list[Claim]) -> list[SearchQueryPlan]:
    """Build deterministic search queries from extracted claims."""

    plans: list[SearchQueryPlan] = []
    for index, claim in enumerate(claims, start=1):
        plans.append(
            SearchQueryPlan(
                query_id=f"query_{index:03d}",
                claim_id=claim.claim_id,
                query=claim.normalized_text,
                priority=1,
                notes="Offline MVP uses the claim text as the manual search query.",
            )
        )
    return plans


def load_manual_evidence(path: Path) -> list[Evidence]:
    """Load manual evidence JSON for the offline MVP."""

    raw = read_json(path)
    evidence_items = raw["evidence"] if isinstance(raw, dict) and "evidence" in raw else raw
    if not isinstance(evidence_items, list):
        raise ValueError("Evidence JSON must be a list or an object with an 'evidence' list.")
    return [Evidence.model_validate(item) for item in evidence_items]


def now_utc() -> datetime:
    """Return a timezone-aware timestamp for generated artifacts."""

    return datetime.now(timezone.utc)
