from pathlib import Path

import httpx

from video_truthfulness.agent_config import AgentSettings
from video_truthfulness.agent_graph import AgentService
from video_truthfulness.api import create_app


async def test_fastapi_query_and_trace_header(tmp_path: Path) -> None:
    settings = AgentSettings(
        runtime_dir=tmp_path / "runtime",
        source_path=Path("examples/agent_demo/sources.jsonl"),
        collection_name="api_test_sources",
        embedding_backend="hash",
        embedding_cache_dir=tmp_path / "model_cache",
        llm_provider="extractive",
        min_relevance_score=0.2,
    )
    app = create_app(service=AgentService(settings=settings))
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/v1/query",
            headers={"X-Trace-ID": "trace-api-test"},
            json={"query": "Orion 计划 2026 年研发预算是多少？", "authorized": True},
        )
        health = await client.get("/health")

    assert response.status_code == 200
    assert response.headers["X-Trace-ID"] == "trace-api-test"
    assert response.json()["trace_id"] == "trace-api-test"
    assert response.json()["status"] == "answered"
    assert health.json()["indexed_sources"] == 10
