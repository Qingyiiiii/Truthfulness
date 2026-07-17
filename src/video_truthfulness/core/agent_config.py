"""Environment-backed configuration for the agent demo."""

from __future__ import annotations

import os
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field


class AgentSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    runtime_dir: Path = Path("runtime")
    source_path: Path = Path("examples/agent_demo/sources.jsonl")
    collection_name: str = "truthfulness_demo_sources"
    embedding_backend: str = "fastembed"
    embedding_model: str = "BAAI/bge-small-zh-v1.5"
    embedding_cache_dir: Path = Path("runtime/model_cache")
    llm_provider: str = "extractive"
    llm_base_url: str = "http://localhost:11434"
    llm_model: str = "deepseek-r1:7b"
    max_attempts: int = Field(default=2, ge=1, le=4)
    stage_timeout_seconds: float = Field(default=8.0, gt=0, le=120)
    request_timeout_seconds: float = Field(default=30.0, gt=0, le=300)
    min_relevance_score: float = Field(default=0.38, ge=0, le=1)
    input_cost_per_million: float | None = Field(default=None, ge=0)
    output_cost_per_million: float | None = Field(default=None, ge=0)

    @property
    def chroma_dir(self) -> Path:
        return self.runtime_dir / "chroma"

    @property
    def review_db_path(self) -> Path:
        return self.runtime_dir / "review_tasks.sqlite3"

    @classmethod
    def from_env(cls) -> "AgentSettings":
        def optional_float(name: str) -> float | None:
            value = os.getenv(name)
            return float(value) if value not in (None, "") else None

        return cls(
            runtime_dir=Path(os.getenv("TRUTHFULNESS_RUNTIME_DIR", "runtime")),
            source_path=Path(os.getenv("TRUTHFULNESS_SOURCE_PATH", "examples/agent_demo/sources.jsonl")),
            collection_name=os.getenv("TRUTHFULNESS_COLLECTION", "truthfulness_demo_sources"),
            embedding_backend=os.getenv("TRUTHFULNESS_EMBEDDING_BACKEND", "fastembed"),
            embedding_model=os.getenv("TRUTHFULNESS_EMBEDDING_MODEL", "BAAI/bge-small-zh-v1.5"),
            embedding_cache_dir=Path(os.getenv("TRUTHFULNESS_EMBEDDING_CACHE", "runtime/model_cache")),
            llm_provider=os.getenv("TRUTHFULNESS_LLM_PROVIDER", "extractive"),
            llm_base_url=os.getenv("TRUTHFULNESS_LLM_BASE_URL", "http://localhost:11434"),
            llm_model=os.getenv("TRUTHFULNESS_LLM_MODEL", "deepseek-r1:7b"),
            max_attempts=int(os.getenv("TRUTHFULNESS_MAX_ATTEMPTS", "2")),
            stage_timeout_seconds=float(os.getenv("TRUTHFULNESS_STAGE_TIMEOUT_SECONDS", "8")),
            request_timeout_seconds=float(os.getenv("TRUTHFULNESS_REQUEST_TIMEOUT_SECONDS", "30")),
            min_relevance_score=float(os.getenv("TRUTHFULNESS_MIN_RELEVANCE_SCORE", "0.38")),
            input_cost_per_million=optional_float("TRUTHFULNESS_INPUT_COST_PER_MILLION"),
            output_cost_per_million=optional_float("TRUTHFULNESS_OUTPUT_COST_PER_MILLION"),
        )
