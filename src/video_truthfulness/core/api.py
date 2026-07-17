"""FastAPI surface for the evidence-aware LangGraph service."""

from __future__ import annotations

from contextlib import asynccontextmanager
import json
import logging
from time import perf_counter
import uuid

from fastapi import FastAPI, HTTPException, Request, Response

from video_truthfulness.core.agent_config import AgentSettings
from video_truthfulness.core.agent_graph import AgentService
from video_truthfulness.core.agent_models import (
    HealthResponse,
    QueryRequest,
    QueryResponse,
    ReviewTask,
    SourceInfo,
)


logger = logging.getLogger("video_truthfulness.core.api")


def create_app(
    settings: AgentSettings | None = None,
    service: AgentService | None = None,
) -> FastAPI:
    """Build an app with dependency injection for deterministic tests."""

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.service = service or AgentService(settings=settings)
        yield

    api = FastAPI(
        title="Truthfulness Evidence Agent API",
        version="0.2.0",
        description="Evidence-bound LangGraph RAG demo with citation validation and human escalation.",
        lifespan=lifespan,
    )
    if service is not None:
        api.state.service = service

    @api.middleware("http")
    async def trace_middleware(request: Request, call_next):
        trace_id = request.headers.get("X-Trace-ID", "").strip()[:128] or uuid.uuid4().hex
        request.state.trace_id = trace_id
        started = perf_counter()
        response: Response = await call_next(request)
        elapsed_ms = round((perf_counter() - started) * 1000, 3)
        response.headers["X-Trace-ID"] = trace_id
        response.headers["X-Process-Time-Ms"] = str(elapsed_ms)
        logger.info(
            json.dumps(
                {
                    "trace_id": trace_id,
                    "method": request.method,
                    "path": request.url.path,
                    "status_code": response.status_code,
                    "elapsed_ms": elapsed_ms,
                }
            )
        )
        return response

    @api.get("/health", response_model=HealthResponse, tags=["operations"])
    async def health(request: Request) -> HealthResponse:
        return HealthResponse.model_validate(request.app.state.service.health())

    @api.post("/v1/query", response_model=QueryResponse, tags=["agent"])
    async def query(payload: QueryRequest, request: Request) -> QueryResponse:
        return await request.app.state.service.query(payload, trace_id=request.state.trace_id)

    @api.get("/v1/sources/{source_id}", response_model=SourceInfo, tags=["tools"])
    async def source_info(source_id: str, request: Request) -> SourceInfo:
        try:
            return request.app.state.service.source_info(source_id)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @api.get("/v1/review-tasks/{task_id}", response_model=ReviewTask, tags=["tools"])
    async def review_task(task_id: str, request: Request) -> ReviewTask:
        task = request.app.state.service.review_task(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail=f"Unknown task_id: {task_id}")
        return task

    return api


app = create_app()
