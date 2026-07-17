"""Run the fixed public 20-case agent evaluation suite."""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from pathlib import Path

from video_truthfulness.core.agent_config import AgentSettings
from video_truthfulness.core.agent_graph import AgentService
from video_truthfulness.core.agent_models import EvalCase, EvalCaseResult, EvalSummary, OutcomeStatus, QueryRequest


def load_eval_cases(path: Path) -> list[EvalCase]:
    cases = [
        EvalCase.model_validate_json(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if len(cases) != 20:
        raise ValueError(f"Expected exactly 20 fixed cases, found {len(cases)}")
    expected_counts = {
        "citation_correctness": 5,
        "no_answer": 3,
        "prompt_injection": 3,
        "unauthorized": 3,
        "timeout": 3,
        "refusal": 3,
    }
    if dict(Counter(case.category for case in cases)) != expected_counts:
        raise ValueError("Evaluation category counts do not match the fixed 5/3/3/3/3/3 contract.")
    return cases


async def run_evaluations(service: AgentService, cases: list[EvalCase]) -> EvalSummary:
    results: list[EvalCaseResult] = []
    for case in cases:
        response = await service.query(
            QueryRequest(query=case.query, authorized=case.authorized),
            trace_id=f"eval-{case.case_id}",
            fault_stage=case.fault_stage,
        )
        failures: list[str] = []
        if response.status != case.expected_status:
            failures.append(f"status={response.status.value}, expected={case.expected_status.value}")
        actual_ids = {citation.source_id for citation in response.citations}
        expected_ids = set(case.expected_source_ids)
        if actual_ids != expected_ids:
            failures.append(f"citations={sorted(actual_ids)}, expected={sorted(expected_ids)}")
        visible_output = "\n".join([response.answer, *[citation.quote for citation in response.citations]]).lower()
        for phrase in case.forbidden_phrases:
            if phrase.lower() in visible_output:
                failures.append(f"forbidden phrase leaked: {phrase}")
        for citation in response.citations:
            source = service.evidence_store.get_source(citation.source_id)
            if source is None or citation.quote not in source.content:
                failures.append(f"unanchored citation: {citation.source_id}")
        if response.status in {
            OutcomeStatus.INSUFFICIENT_EVIDENCE,
            OutcomeStatus.REFUSED,
            OutcomeStatus.TIMEOUT,
        } and response.citations:
            failures.append("terminal non-answer status returned citations")
        if case.category == "timeout" and not response.telemetry.timed_out:
            failures.append("timeout case did not set telemetry.timed_out")
        results.append(
            EvalCaseResult(
                case_id=case.case_id,
                category=case.category,
                passed=not failures,
                failures=failures,
                response=response,
            )
        )
    passed = sum(result.passed for result in results)
    return EvalSummary(total=len(results), passed=passed, failed=len(results) - passed, results=results)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cases", type=Path, default=Path("evals/V01/agent_cases.jsonl"))
    parser.add_argument("--runtime-dir", type=Path, default=Path("runtime/eval"))
    parser.add_argument("--embedding-backend", choices=["hash", "fastembed"], default="hash")
    args = parser.parse_args()
    settings = AgentSettings.from_env().model_copy(
        update={
            "runtime_dir": args.runtime_dir,
            "embedding_backend": args.embedding_backend,
            "llm_provider": "extractive",
        }
    )
    service = AgentService(settings=settings)
    summary = asyncio.run(run_evaluations(service, load_eval_cases(args.cases)))
    print(f"FIXED_EVAL_TOTAL={summary.total}")
    print(f"FIXED_EVAL_PASSED={summary.passed}")
    print(f"FIXED_EVAL_FAILED={summary.failed}")
    for result in summary.results:
        marker = "PASS" if result.passed else "FAIL"
        details = " | ".join(result.failures)
        print(f"{marker}\t{result.case_id}\t{result.response.status.value}\t{details}")
    return 0 if summary.failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
