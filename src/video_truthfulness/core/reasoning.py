"""Conservative claim-evidence reasoning for Demo1."""

from __future__ import annotations

from collections import defaultdict

from video_truthfulness.core.schemas import (
    Checkability,
    Claim,
    Evidence,
    EvidenceRelation,
    EvidenceScore,
    Verdict,
    VerdictLabel,
)


def generate_verdicts(
    claims: list[Claim],
    evidence: list[Evidence],
    evidence_scores: list[EvidenceScore],
    strong_threshold: float = 0.6,
) -> list[Verdict]:
    """Generate one verdict per claim using only existing evidence records."""

    evidence_by_claim: dict[str, list[Evidence]] = defaultdict(list)
    score_by_evidence = {score.evidence_id: score for score in evidence_scores}
    for item in evidence:
        evidence_by_claim[item.claim_id].append(item)

    verdicts: list[Verdict] = []
    for claim in claims:
        if claim.checkability == Checkability.NOT_CHECKABLE:
            verdicts.append(_uncheckable(claim.claim_id))
            continue

        claim_evidence = evidence_by_claim.get(claim.claim_id, [])
        if not claim_evidence:
            verdicts.append(_insufficient(claim.claim_id, "No evidence was provided for this claim."))
            continue

        best = max(claim_evidence, key=lambda item: score_by_evidence[item.evidence_id].final_score)
        best_score = score_by_evidence[best.evidence_id]
        if best_score.final_score < strong_threshold:
            verdicts.append(
                _insufficient(
                    claim.claim_id,
                    f"Best evidence score is {best_score.final_score:.2f}, below threshold {strong_threshold:.2f}.",
                )
            )
            continue

        verdicts.append(_from_best_evidence(claim.claim_id, best, best_score))
    return verdicts


def _from_best_evidence(claim_id: str, evidence: Evidence, score: EvidenceScore) -> Verdict:
    """Map the best evidence relation into a conservative verdict."""

    if evidence.relation_to_claim == EvidenceRelation.SUPPORTS:
        return Verdict(
            claim_id=claim_id,
            verdict=VerdictLabel.SUPPORTS,
            confidence=score.final_score,
            reason=f"Highest-quality evidence supports the claim: {evidence.evidence_id}.",
            supporting_evidence_ids=[evidence.evidence_id],
            review_required=score.screenshot < 1.0,
        )
    if evidence.relation_to_claim == EvidenceRelation.CONTRADICTS:
        return Verdict(
            claim_id=claim_id,
            verdict=VerdictLabel.CONTRADICTS,
            confidence=score.final_score,
            reason=f"Highest-quality evidence contradicts the claim: {evidence.evidence_id}.",
            contradicting_evidence_ids=[evidence.evidence_id],
            review_required=score.screenshot < 1.0,
        )
    if evidence.relation_to_claim == EvidenceRelation.MIXED:
        return Verdict(
            claim_id=claim_id,
            verdict=VerdictLabel.PARTIALLY_SUPPORTS,
            confidence=score.final_score,
            reason=f"Evidence is mixed or only partially covers the claim: {evidence.evidence_id}.",
            supporting_evidence_ids=[evidence.evidence_id],
            missing_context=["Evidence relation is mixed."],
            review_required=True,
        )
    return _insufficient(claim_id, "Evidence relation to the claim is unknown.")


def _insufficient(claim_id: str, reason: str) -> Verdict:
    """Create an insufficient-evidence verdict."""

    return Verdict(
        claim_id=claim_id,
        verdict=VerdictLabel.INSUFFICIENT_EVIDENCE,
        confidence=0.0,
        reason=reason,
        review_required=True,
    )


def _uncheckable(claim_id: str) -> Verdict:
    """Create an uncheckable verdict."""

    return Verdict(
        claim_id=claim_id,
        verdict=VerdictLabel.UNCHECKABLE,
        confidence=0.0,
        reason="The extracted item is not externally checkable.",
        review_required=True,
    )
