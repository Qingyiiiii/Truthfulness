"""Chroma-backed retrieval with replaceable local embedding backends."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Protocol

import numpy as np
from sklearn.feature_extraction.text import HashingVectorizer

from video_truthfulness.core.agent_models import RetrievedEvidence, SourceDocument, SourceInfo


class EmbeddingBackend(Protocol):
    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        """Embed passages for indexing."""

    def embed_query(self, text: str) -> list[float]:
        """Embed one retrieval query."""


class HashEmbeddingBackend:
    """Small deterministic backend for tests and offline CI."""

    def __init__(self, dimensions: int = 512) -> None:
        self.vectorizer = HashingVectorizer(
            analyzer="char",
            ngram_range=(2, 5),
            n_features=dimensions,
            alternate_sign=False,
            norm="l2",
        )

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        matrix = self.vectorizer.transform(texts).astype(np.float32)
        return matrix.toarray().tolist()

    def embed_query(self, text: str) -> list[float]:
        return self.embed_documents([text])[0]


class FastEmbedBackend:
    """ONNX embedding backend used by the public Docker demo."""

    def __init__(self, model_name: str, cache_dir: Path) -> None:
        from fastembed import TextEmbedding

        cache_dir.mkdir(parents=True, exist_ok=True)
        self.model = TextEmbedding(model_name=model_name, cache_dir=str(cache_dir))

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return [vector.astype(np.float32).tolist() for vector in self.model.passage_embed(texts)]

    def embed_query(self, text: str) -> list[float]:
        vector = next(iter(self.model.query_embed(text)))
        return vector.astype(np.float32).tolist()


def build_embedding_backend(backend: str, model_name: str, cache_dir: Path) -> EmbeddingBackend:
    if backend == "hash":
        return HashEmbeddingBackend()
    if backend == "fastembed":
        return FastEmbedBackend(model_name=model_name, cache_dir=cache_dir)
    raise ValueError(f"Unsupported embedding backend: {backend}")


def load_sources(path: Path) -> list[SourceDocument]:
    """Read a synthetic/public JSONL source catalog."""

    sources: list[SourceDocument] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            sources.append(SourceDocument.model_validate_json(line))
        except Exception as exc:  # pragma: no cover - message is the useful boundary
            raise ValueError(f"Invalid source catalog line {line_number}: {exc}") from exc
    if not sources:
        raise ValueError(f"Source catalog is empty: {path}")
    return sources


class ChromaEvidenceStore:
    """Persistent Chroma collection plus source metadata lookup."""

    def __init__(
        self,
        persist_dir: Path,
        collection_name: str,
        embedding_backend: EmbeddingBackend,
    ) -> None:
        import chromadb
        from chromadb.config import Settings

        persist_dir.mkdir(parents=True, exist_ok=True)
        self.embedding_backend = embedding_backend
        self.client = chromadb.PersistentClient(
            path=str(persist_dir),
            settings=Settings(anonymized_telemetry=False),
        )
        self.collection = self.client.get_or_create_collection(
            name=collection_name,
            metadata={"hnsw:space": "cosine", "schema_version": "1"},
        )

    def index_sources(self, sources: list[SourceDocument]) -> None:
        """Idempotently upsert source text, metadata, and explicit vectors."""

        sources = [source for source in sources if source.authorized]
        if not sources:
            raise ValueError("No authorized sources are available for indexing.")
        ids = [source.source_id for source in sources]
        documents = [source.content for source in sources]
        embeddings = self.embedding_backend.embed_documents(documents)
        metadatas = [
            {
                "source_json": source.model_dump_json(),
                "authorized": source.authorized,
                "source_type": source.source_type,
                "publisher": source.publisher,
            }
            for source in sources
        ]
        self.collection.upsert(ids=ids, documents=documents, metadatas=metadatas, embeddings=embeddings)

    def count(self) -> int:
        return self.collection.count()

    def retrieve(self, query: str, top_k: int) -> list[RetrievedEvidence]:
        if self.count() == 0:
            return []
        result = self.collection.query(
            query_embeddings=[self.embedding_backend.embed_query(query)],
            n_results=min(top_k, self.count()),
            where={"authorized": True},
            include=["metadatas", "distances"],
        )
        ids = result.get("ids", [[]])[0]
        metadatas = result.get("metadatas", [[]])[0]
        distances = result.get("distances", [[]])[0]
        retrieved: list[RetrievedEvidence] = []
        for source_id, metadata, distance in zip(ids, metadatas, distances, strict=True):
            if not metadata or "source_json" not in metadata:
                continue
            source = SourceDocument.model_validate_json(str(metadata["source_json"]))
            if source.source_id != source_id:
                raise ValueError(f"Source ID mismatch in vector store: {source_id}")
            score = max(0.0, min(1.0, 1.0 - float(distance)))
            retrieved.append(RetrievedEvidence(source=source, score=round(score, 6)))
        return retrieved

    def get_source(self, source_id: str) -> SourceDocument | None:
        result = self.collection.get(ids=[source_id], include=["metadatas"])
        metadatas = result.get("metadatas") or []
        if not metadatas:
            return None
        metadata = metadatas[0]
        if not metadata or "source_json" not in metadata:
            return None
        return SourceDocument.model_validate_json(str(metadata["source_json"]))

    def get_source_info(self, source_id: str) -> SourceInfo | None:
        source = self.get_source(source_id)
        if source is None or not source.authorized:
            return None
        return SourceInfo(
            source_id=source.source_id,
            title=source.title,
            publisher=source.publisher,
            source_url=source.source_url,
            source_type=source.source_type,
            published_at=source.published_at,
            retrieved_at=source.retrieved_at,
            authorized=source.authorized,
        )

    def export_source_json(self, source_id: str) -> str | None:
        source = self.get_source(source_id)
        return json.dumps(source.model_dump(mode="json"), ensure_ascii=False) if source else None
