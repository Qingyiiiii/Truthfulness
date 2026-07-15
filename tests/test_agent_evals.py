from pathlib import Path

from video_truthfulness.agent_config import AgentSettings
from video_truthfulness.agent_graph import AgentService
from video_truthfulness.evals import load_eval_cases, run_evaluations


async def test_all_twenty_fixed_agent_evaluations_pass(tmp_path: Path) -> None:
    settings = AgentSettings(
        runtime_dir=tmp_path / "runtime",
        source_path=Path("examples/agent_demo/sources.jsonl"),
        collection_name="fixed_eval_sources",
        embedding_backend="hash",
        embedding_cache_dir=tmp_path / "model_cache",
        llm_provider="extractive",
        min_relevance_score=0.2,
        stage_timeout_seconds=2,
        request_timeout_seconds=10,
    )
    service = AgentService(settings=settings)
    summary = await run_evaluations(service, load_eval_cases(Path("evals/agent_cases.jsonl")))

    assert summary.total == 20
    assert summary.failed == 0, [
        (result.case_id, result.failures) for result in summary.results if not result.passed
    ]
