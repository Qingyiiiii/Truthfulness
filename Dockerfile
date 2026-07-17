FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TRUTHFULNESS_RUNTIME_DIR=/app/runtime \
    TRUTHFULNESS_SOURCE_PATH=/app/examples/agent_demo/sources.jsonl \
    TRUTHFULNESS_EMBEDDING_BACKEND=fastembed \
    TRUTHFULNESS_EMBEDDING_MODEL=BAAI/bge-small-zh-v1.5 \
    TRUTHFULNESS_EMBEDDING_CACHE=/opt/truthfulness-models \
    TRUTHFULNESS_LLM_PROVIDER=extractive

WORKDIR /app

COPY configs/agent-requirements.txt /tmp/agent-requirements.txt

RUN --mount=type=cache,target=/root/.cache/pip \
    python -m pip install -r /tmp/agent-requirements.txt \
    && python -c "from fastembed import TextEmbedding; TextEmbedding(model_name='BAAI/bge-small-zh-v1.5', cache_dir='/opt/truthfulness-models')" \
    && chmod -R a+rX /opt/truthfulness-models

COPY pyproject.toml README.md LICENSE ./
COPY src ./src

RUN python -m pip install --no-build-isolation --no-deps . \
    && rm -rf build

COPY app ./app
COPY configs ./configs
COPY docs ./docs
COPY evals ./evals
COPY examples ./examples
COPY schemas ./schemas

RUN useradd --create-home --uid 10001 appuser \
    && mkdir -p /app/runtime \
    && chown -R appuser:appuser /app/runtime

USER appuser

EXPOSE 8000 8501

HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=5 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=3)" || exit 1

CMD ["uvicorn", "video_truthfulness.core.api:app", "--host", "0.0.0.0", "--port", "8000"]
