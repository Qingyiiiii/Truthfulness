"""Evidence quality scoring for the offline MVP."""

from __future__ import annotations

import re

from video_truthfulness.schemas import Claim, Evidence, EvidenceScore, SourceType

AUTHORITY_BY_SOURCE = {
    SourceType.OFFICIAL: 0.95,
    SourceType.ACADEMIC: 0.9,
    SourceType.DATABASE: 0.82,
    SourceType.NEWS: 0.7,
    SourceType.SOCIAL: 0.35,
    SourceType.UNKNOWN: 0.25,
}


def score_evidence(claims: list[Claim], evidence: list[Evidence]) -> list[EvidenceScore]:
    """Score evidence relevance and quality using deterministic heuristics."""

    claim_by_id = {claim.claim_id: claim for claim in claims}
    scores: list[EvidenceScore] = []
    for item in evidence:
        claim = claim_by_id.get(item.claim_id)
        relevance = _keyword_overlap(claim.normalized_text if claim else "", item.selected_text)
        authority = AUTHORITY_BY_SOURCE.get(item.source_type, 0.25)
        completeness = _completeness(item)
        screenshot = 1.0 if item.screenshot_path else 0.0
        final_score = round(
            (relevance * 0.35) + (authority * 0.35) + (completeness * 0.2) + (screenshot * 0.1),
            4,
        )
        scores.append(
            EvidenceScore(
                evidence_id=item.evidence_id,
                claim_id=item.claim_id,
                relevance=relevance,
                authority=authority,
                completeness=completeness,
                screenshot=screenshot,
                final_score=final_score,
                reason=(
                    f"source_type={item.source_type.value}; "
                    f"keyword_overlap={relevance:.2f}; "
                    f"screenshot={'yes' if item.screenshot_path else 'no'}"
                ),
            )
        )
    return scores


def _keyword_overlap(claim_text: str, evidence_text: str) -> float:
    """Estimate text relevance by overlap between claim and evidence tokens."""

    claim_tokens = _tokens(claim_text)
    evidence_tokens = _tokens(evidence_text)
    if not claim_tokens:
        return 0.0
    overlap = claim_tokens.intersection(evidence_tokens)
    return round(min(1.0, len(overlap) / max(1, len(claim_tokens))), 4)


def _tokens(text: str) -> set[str]:
    """Create coarse tokens that work for mixed Chinese and numeric text."""

    ascii_tokens = set(re.findall(r"[A-Za-z0-9.]+", text.lower()))
    chinese_chunks = set(re.findall(r"[\u4e00-\u9fff]{2,}", text))
    return ascii_tokens.union(chinese_chunks)


def _completeness(evidence: Evidence) -> float:
    """Score whether the evidence record has enough review context."""

    score = 0.0
    score += 0.25 if evidence.source_url else 0.0
    score += 0.2 if evidence.page_title else 0.0
    score += 0.2 if evidence.publisher else 0.0
    score += 0.25 if len(evidence.selected_text.strip()) >= 30 else 0.1
    score += 0.1 if evidence.published_at else 0.0
    return round(min(1.0, score), 4)
