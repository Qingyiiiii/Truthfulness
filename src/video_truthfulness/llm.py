"""Local and OpenAI-compatible LLM provider abstractions.

The offline MVP does not require an LLM. These providers are intentionally thin
wrappers so later extraction and reasoning steps can depend on a stable local
interface while still validating output with Pydantic schemas.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests


@dataclass(frozen=True)
class LLMResponse:
    """Text returned by a provider plus the raw provider payload."""

    text: str
    raw: dict[str, Any]


class OpenAICompatibleProvider:
    """Provider for `/v1/chat/completions` compatible local or remote APIs."""

    def __init__(self, base_url: str, model: str, timeout_seconds: int = 60) -> None:
        """Store endpoint settings without making a network call."""

        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout_seconds = timeout_seconds

    def complete(self, system_prompt: str, user_prompt: str) -> LLMResponse:
        """Call a chat-completions compatible provider."""

        endpoint = f"{self.base_url}/chat/completions"
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0,
        }
        response = requests.post(endpoint, json=payload, timeout=self.timeout_seconds)
        response.raise_for_status()
        data = response.json()
        text = data["choices"][0]["message"]["content"]
        return LLMResponse(text=text, raw=data)


class OllamaProvider:
    """Provider for Ollama's local `/api/chat` endpoint."""

    def __init__(self, base_url: str = "http://localhost:11434", model: str = "qwen2.5:7b", timeout_seconds: int = 60) -> None:
        """Store Ollama endpoint settings."""

        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout_seconds = timeout_seconds

    def complete(self, system_prompt: str, user_prompt: str) -> LLMResponse:
        """Call local Ollama and return the assistant message."""

        endpoint = f"{self.base_url}/api/chat"
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "stream": False,
            "options": {"temperature": 0},
        }
        response = requests.post(endpoint, json=payload, timeout=self.timeout_seconds)
        response.raise_for_status()
        data = response.json()
        text = data["message"]["content"]
        return LLMResponse(text=text, raw=data)


class LMStudioProvider(OpenAICompatibleProvider):
    """LM Studio provider using its OpenAI-compatible local server."""

    def __init__(self, base_url: str = "http://localhost:1234/v1", model: str = "local-model", timeout_seconds: int = 60) -> None:
        """Initialize the provider with LM Studio's default local URL."""

        super().__init__(base_url=base_url, model=model, timeout_seconds=timeout_seconds)
