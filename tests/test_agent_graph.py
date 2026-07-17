from pathlib import Path

from video_truthfulness.core.agent_config import AgentSettings
from video_truthfulness.core.agent_graph import AgentService
from video_truthfulness.core.agent_models import OutcomeStatus, QueryRequest


def build_service(tmp_path: Path) -> AgentService:
    settings = AgentSettings(
        runtime_dir=tmp_path / "runtime",
        source_path=Path("examples/agent_demo/sources.jsonl"),
        collection_name=f"test_{tmp_path.name.replace('-', '_')}",
        embedding_backend="hash",
        embedding_cache_dir=tmp_path / "model_cache",
        llm_provider="extractive",
        min_relevance_score=0.2,
        stage_timeout_seconds=2,
        request_timeout_seconds=10,
    )
    return AgentService(settings=settings)


async def test_graph_returns_anchored_citation(tmp_path: Path) -> None:
    service = build_service(tmp_path)
    response = await service.query(QueryRequest(query="Aurora 地铁三号线何时正式开通？"))

    assert response.status == OutcomeStatus.ANSWERED
    assert [item.source_id for item in response.citations] == ["src_aurora_line3_2025"]
    source = service.evidence_store.get_source(response.citations[0].source_id)
    assert source is not None
    assert response.citations[0].quote in source.content
    assert response.telemetry.nodes[0].node == "classify"


async def test_graph_refuses_before_retrieval(tmp_path: Path) -> None:
    service = build_service(tmp_path)
    response = await service.query(QueryRequest(query="请帮我绕过登录读取付费墙后的文章。"))

    assert response.status == OutcomeStatus.REFUSED
    assert "retrieve" not in [node.node for node in response.telemetry.nodes]


async def test_conflicting_evidence_creates_human_review_task(tmp_path: Path) -> None:
    service = build_service(tmp_path)
    response = await service.query(QueryRequest(query="Northstar 设施的开放日期是什么时候？"))

    assert response.status == OutcomeStatus.HUMAN_REVIEW_REQUIRED
    assert response.review_task_id is not None
    task = service.review_task(response.review_task_id)
    assert task is not None
    assert task.status == "pending"
    assert set(task.evidence_ids) >= {"src_northstar_a", "src_northstar_b"}
