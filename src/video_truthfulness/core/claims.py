"""Rule-based claim extraction for the offline MVP."""

from __future__ import annotations

import re

from video_truthfulness.core.schemas import Checkability, Claim, ClaimType, Transcript

SENTENCE_SPLIT_PATTERN = re.compile(r"(?<=[。！？!?；;])\s*|\n+")
FACTUAL_CUES = ("发布", "宣布", "显示", "达到", "超过", "低于", "增加", "下降", "发生", "成立", "为")
OPINION_CUES = ("观点：", "观点:", "我觉得", "我认为", "作者认为", "认为", "可能", "应该", "喜欢", "讨厌", "希望")
VAGUE_CUES = ("很多", "大量", "最近", "有人说", "据说")


class RuleBasedClaimExtractor:
    """Extract simple factual claims without LLM calls."""

    def extract(self, transcript: Transcript) -> list[Claim]:
        """Return atomic claims from transcript segments."""

        claims: list[Claim] = []
        for segment in transcript.segments:
            sentences = self._split_sentences(segment.text)
            for sentence in sentences:
                cleaned = self._clean_claim_prefix(sentence)
                if not cleaned:
                    continue
                checkability = self._classify_checkability(cleaned)
                if checkability == Checkability.NOT_CHECKABLE:
                    continue
                if not self._looks_factual(cleaned):
                    continue
                claim_id = f"claim_{len(claims) + 1:03d}"
                claims.append(
                    Claim(
                        claim_id=claim_id,
                        text=cleaned,
                        normalized_text=self._normalize(cleaned),
                        type=self._classify_type(cleaned),
                        source_segment_ids=[segment.segment_id],
                        checkability=checkability,
                        entities=[],
                    )
                )
        return claims

    def _split_sentences(self, text: str) -> list[str]:
        """Split Chinese or English transcript text into candidate sentences."""

        return [part.strip() for part in SENTENCE_SPLIT_PATTERN.split(text) if part.strip()]

    def _clean_claim_prefix(self, sentence: str) -> str:
        """Remove manual labels while preserving the actual claim text."""

        for prefix in ("主张：", "主张:", "[CLAIM]", "Claim:"):
            if sentence.startswith(prefix):
                return sentence[len(prefix) :].strip()
        if sentence.startswith(("观点：", "观点:")):
            return ""
        return sentence.strip()

    def _classify_checkability(self, sentence: str) -> Checkability:
        """Classify whether the candidate should become a checkable claim."""

        if any(cue in sentence for cue in OPINION_CUES):
            return Checkability.NOT_CHECKABLE
        if any(cue in sentence for cue in VAGUE_CUES):
            return Checkability.NEEDS_CONTEXT
        return Checkability.CHECKABLE

    def _looks_factual(self, sentence: str) -> bool:
        """Keep claims with numbers, dates, or explicit factual cues."""

        has_number = bool(re.search(r"\d|[一二三四五六七八九十百千万亿]+", sentence))
        has_cue = any(cue in sentence for cue in FACTUAL_CUES)
        return has_number or has_cue

    def _classify_type(self, sentence: str) -> ClaimType:
        """Assign a broad claim type for downstream reporting."""

        if re.search(r"\d|%|百分比|亿|万|吨|元|人", sentence):
            return ClaimType.NUMERIC
        if any(word in sentence for word in ("政策", "法规", "规定", "条例")):
            return ClaimType.POLICY
        if any(word in sentence for word in ("表示", "称", "引用", "说")):
            return ClaimType.QUOTE
        if any(word in sentence for word in ("导致", "造成", "因为", "由于")):
            return ClaimType.CAUSAL
        if any(word in sentence for word in ("高于", "低于", "相比", "超过")):
            return ClaimType.COMPARISON
        if any(word in sentence for word in ("发布", "宣布", "发生", "成立")):
            return ClaimType.EVENT
        return ClaimType.OTHER

    def _normalize(self, sentence: str) -> str:
        """Normalize spacing for stable downstream matching."""

        return re.sub(r"\s+", " ", sentence).strip()
