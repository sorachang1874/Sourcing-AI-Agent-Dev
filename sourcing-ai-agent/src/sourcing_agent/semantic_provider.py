from __future__ import annotations

import json
import math
from typing import Any, Protocol
from urllib import error, request

from .runtime_environment import external_provider_mode
from .settings import SemanticProviderSettings


def _external_provider_mode() -> str:
    return external_provider_mode()


class SemanticProvider(Protocol):
    def provider_name(self) -> str: ...

    def healthcheck(self) -> dict[str, Any]: ...

    def embed_texts(self, texts: list[str]) -> list[list[float]]: ...

    def rerank(self, query: str, documents: list[str], *, top_n: int) -> list[dict[str, Any]]: ...

    def score_media_records(self, query: str, records: list[dict[str, Any]], *, top_n: int) -> list[dict[str, Any]]: ...


class LocalSemanticProvider:
    def provider_name(self) -> str:
        return "local_sparse"

    def healthcheck(self) -> dict[str, Any]:
        return {"provider": "local_sparse", "status": "ready"}

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        return [_hashed_vector(text) for text in texts]

    def rerank(self, query: str, documents: list[str], *, top_n: int) -> list[dict[str, Any]]:
        query_vector = _hashed_vector(query)
        scored: list[dict[str, Any]] = []
        for index, document in enumerate(documents):
            score = _cosine(query_vector, _hashed_vector(document))
            scored.append(
                {
                    "index": index,
                    "document": document,
                    "relevance_score": round(score, 4),
                }
            )
        scored.sort(key=lambda item: (-float(item.get("relevance_score") or 0.0), int(item.get("index") or 0)))
        return scored[:top_n]

    def score_media_records(self, query: str, records: list[dict[str, Any]], *, top_n: int) -> list[dict[str, Any]]:
        documents = [_media_text(record) for record in records]
        reranked = self.rerank(query, documents, top_n=top_n)
        results: list[dict[str, Any]] = []
        for item in reranked:
            index = int(item.get("index") or 0)
            if index >= len(records):
                continue
            results.append(
                {
                    "index": index,
                    "record": records[index],
                    "relevance_score": float(item.get("relevance_score") or 0.0),
                }
            )
        return results


class OfflineSemanticProvider(LocalSemanticProvider):
    def __init__(self, *, mode: str) -> None:
        normalized_mode = str(mode or "simulate").strip().lower() or "simulate"
        self.mode = normalized_mode if normalized_mode in {"simulate", "replay", "scripted"} else "simulate"

    def provider_name(self) -> str:
        return "offline_semantic"

    def healthcheck(self) -> dict[str, Any]:
        if self.mode == "simulate":
            note = "Simulated semantic provider; no external embedding or rerank request will be sent."
        elif self.mode == "replay":
            note = "Replay semantic provider; external embedding and rerank requests are disabled."
        else:
            note = "Scripted semantic provider; external embedding and rerank requests are disabled."
        return {
            "provider": self.provider_name(),
            "status": "ready",
            "provider_mode": self.mode,
            "note": note,
        }


class DashScopeSemanticProvider(LocalSemanticProvider):
    def __init__(self, settings: SemanticProviderSettings) -> None:
        self.settings = settings

    def provider_name(self) -> str:
        return "dashscope_semantic"

    def healthcheck(self) -> dict[str, Any]:
        if not self.settings.enabled:
            return {"provider": self.provider_name(), "status": "disabled"}
        try:
            vectors = self.embed_texts(["semantic provider healthcheck"])
            if not vectors or not vectors[0]:
                return {"provider": self.provider_name(), "status": "unexpected_response"}
            return {
                "provider": self.provider_name(),
                "status": "ready",
                "embedding_model": self.settings.embedding_model,
                "rerank_model": self.settings.rerank_model,
            }
        except Exception as exc:
            return {
                "provider": self.provider_name(),
                "status": "degraded",
                "embedding_model": self.settings.embedding_model,
                "rerank_model": self.settings.rerank_model,
                "error": str(exc),
            }

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        endpoint = f"{self.settings.embedding_base_url}/embeddings"
        payload = {
            "model": self.settings.embedding_model,
            "input": texts,
            "dimensions": self.settings.embedding_dimensions,
            "encoding_format": "float",
        }
        body = self._post_json(endpoint, payload, timeout=self.settings.embedding_timeout_seconds)
        data = list(body.get("data") or [])
        vectors: list[list[float]] = []
        for item in data:
            embedding = item.get("embedding") if isinstance(item, dict) else None
            if isinstance(embedding, list):
                vectors.append([float(value) for value in embedding])
        return vectors

    def rerank(self, query: str, documents: list[str], *, top_n: int) -> list[dict[str, Any]]:
        if not documents:
            return []
        payload = {
            "model": self.settings.rerank_model,
            "input": {
                "query": query,
                "documents": documents[: self.settings.max_documents_per_call],
            },
            "parameters": {
                "top_n": max(1, min(top_n, self.settings.max_documents_per_call)),
                "return_documents": True,
            },
        }
        body = self._post_json(self.settings.rerank_base_url, payload, timeout=self.settings.rerank_timeout_seconds)
        results = list(body.get("output", {}).get("results") or body.get("results") or [])
        normalized: list[dict[str, Any]] = []
        for item in results:
            if not isinstance(item, dict):
                continue
            document = item.get("document") or item.get("text") or ""
            if isinstance(document, dict):
                document = str(document.get("text") or "")
            normalized.append(
                {
                    "index": int(item.get("index") or 0),
                    "document": document,
                    "relevance_score": float(item.get("relevance_score") or item.get("score") or 0.0),
                }
            )
        return normalized

    def score_media_records(self, query: str, records: list[dict[str, Any]], *, top_n: int) -> list[dict[str, Any]]:
        documents = [_media_text(record) for record in records][: self.settings.max_documents_per_call]
        reranked = self.rerank(query, documents, top_n=top_n)
        results: list[dict[str, Any]] = []
        for item in reranked:
            index = int(item.get("index") or 0)
            if index >= len(records):
                continue
            results.append(
                {
                    "index": index,
                    "record": records[index],
                    "relevance_score": float(item.get("relevance_score") or 0.0),
                }
            )
        return results

    def _post_json(self, endpoint: str, payload: dict[str, Any], *, timeout: int) -> dict[str, Any]:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        http_request = request.Request(
            endpoint,
            data=data,
            headers={
                "Authorization": f"Bearer {self.settings.api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with request.urlopen(http_request, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore")
            raise RuntimeError(f"DashScope semantic HTTP {exc.code}: {detail[:200]}") from exc
        except error.URLError as exc:
            raise RuntimeError(f"DashScope semantic request failed: {exc.reason}") from exc


def build_semantic_provider(settings: SemanticProviderSettings) -> SemanticProvider:
    external_mode = _external_provider_mode()
    if external_mode in {"simulate", "replay", "scripted"}:
        return OfflineSemanticProvider(mode=external_mode)
    if settings.enabled:
        return DashScopeSemanticProvider(settings)
    return LocalSemanticProvider()


def _hashed_vector(text: str, dimensions: int = 64) -> list[float]:
    vector = [0.0] * dimensions
    for token in str(text or "").lower().split():
        slot = hash(token) % dimensions
        vector[slot] += 1.0
    norm = math.sqrt(sum(value * value for value in vector)) or 1.0
    return [round(value / norm, 6) for value in vector]


def _cosine(left: list[float], right: list[float]) -> float:
    size = min(len(left), len(right))
    if size == 0:
        return 0.0
    return sum(left[index] * right[index] for index in range(size))


def _media_text(record: dict[str, Any]) -> str:
    return " ".join(
        [
            str(record.get("title") or "").strip(),
            str(record.get("snippet") or "").strip(),
            str(record.get("url") or "").strip(),
        ]
    ).strip()
