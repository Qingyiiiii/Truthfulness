"""Configuration loading for local Demo1 execution."""

from __future__ import annotations

from pathlib import Path
import tomllib

from pydantic import BaseModel, ConfigDict


class DemoConfig(BaseModel):
    """Small typed view over the local TOML configuration file."""

    model_config = ConfigDict(extra="ignore")

    runs_dir: str = "runs"
    default_platform: str = "manual"
    default_llm_provider: str = "ollama"
    ollama_base_url: str = "http://localhost:11434"
    ollama_model: str = "qwen2.5:7b"
    lmstudio_base_url: str = "http://localhost:1234/v1"
    default_search_provider: str = "manual"
    single_download_only: bool = True


def load_config(path: Path) -> DemoConfig:
    """Load `configs/*.toml` and flatten the sections used by Demo1."""

    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    demo = raw.get("demo1", {})
    llm = raw.get("llm", {})
    search = raw.get("search", {})
    media = raw.get("media", {})
    return DemoConfig(
        runs_dir=demo.get("runs_dir", "runs"),
        default_platform=demo.get("default_platform", "manual"),
        default_llm_provider=llm.get("default_provider", "ollama"),
        ollama_base_url=llm.get("ollama_base_url", "http://localhost:11434"),
        ollama_model=llm.get("ollama_model", "qwen2.5:7b"),
        lmstudio_base_url=llm.get("lmstudio_base_url", "http://localhost:1234/v1"),
        default_search_provider=search.get("default_provider", "manual"),
        single_download_only=bool(media.get("single_download_only", True)),
    )
