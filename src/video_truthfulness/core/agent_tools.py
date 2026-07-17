"""The two bounded tools exposed to the LangGraph workflow."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import uuid

from langchain_core.tools import StructuredTool

from video_truthfulness.core.agent_models import (
    CreateReviewTaskInput,
    ReviewTask,
    SourceInfo,
    SourceLookupInput,
)
from video_truthfulness.core.agent_retrieval import ChromaEvidenceStore


class ReviewTaskStore:
    """Small durable SQLite queue; review decisions remain human-owned."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS review_tasks (
                    task_id TEXT PRIMARY KEY,
                    trace_id TEXT NOT NULL,
                    query TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    evidence_ids_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )

    def create(self, request: CreateReviewTaskInput) -> ReviewTask:
        task = ReviewTask(
            task_id=f"review_{uuid.uuid4().hex[:16]}",
            trace_id=request.trace_id,
            query=request.query,
            reason=request.reason,
            evidence_ids=request.evidence_ids,
            status="pending",
            created_at=datetime.now(timezone.utc),
        )
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO review_tasks
                    (task_id, trace_id, query, reason, evidence_ids_json, status, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task.task_id,
                    task.trace_id,
                    task.query,
                    task.reason,
                    json.dumps(task.evidence_ids, ensure_ascii=False),
                    task.status,
                    task.created_at.isoformat(),
                ),
            )
        return task

    def get(self, task_id: str) -> ReviewTask | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM review_tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
        if row is None:
            return None
        return ReviewTask(
            task_id=row["task_id"],
            trace_id=row["trace_id"],
            query=row["query"],
            reason=row["reason"],
            evidence_ids=json.loads(row["evidence_ids_json"]),
            status=row["status"],
            created_at=datetime.fromisoformat(row["created_at"]),
        )


class AgentTools:
    """Build strict, inspectable tools with Pydantic argument schemas."""

    def __init__(self, evidence_store: ChromaEvidenceStore, review_store: ReviewTaskStore) -> None:
        self.evidence_store = evidence_store
        self.review_store = review_store
        self.lookup_source_info = StructuredTool.from_function(
            name="lookup_source_info",
            description="Look up metadata for an already indexed and authorized source ID.",
            func=self._lookup_source_info,
            args_schema=SourceLookupInput,
        )
        self.create_human_review_task = StructuredTool.from_function(
            name="create_human_review_task",
            description="Create a pending review task without changing gold labels.",
            func=self._create_human_review_task,
            args_schema=CreateReviewTaskInput,
        )

    def _lookup_source_info(self, source_id: str) -> str:
        info = self.evidence_store.get_source_info(source_id)
        if info is None:
            raise ValueError(f"Unknown source_id: {source_id}")
        return info.model_dump_json()

    def _create_human_review_task(
        self,
        trace_id: str,
        query: str,
        reason: str,
        evidence_ids: list[str],
    ) -> str:
        task = self.review_store.create(
            CreateReviewTaskInput(
                trace_id=trace_id,
                query=query,
                reason=reason,
                evidence_ids=evidence_ids,
            )
        )
        return task.model_dump_json()

    def source_info(self, source_id: str) -> SourceInfo:
        raw = self.lookup_source_info.invoke({"source_id": source_id})
        return SourceInfo.model_validate_json(raw)

    def create_review(self, request: CreateReviewTaskInput) -> ReviewTask:
        raw = self.create_human_review_task.invoke(request.model_dump())
        return ReviewTask.model_validate_json(raw)
