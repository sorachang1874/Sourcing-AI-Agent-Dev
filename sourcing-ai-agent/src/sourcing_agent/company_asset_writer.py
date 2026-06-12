from __future__ import annotations

from typing import Any

from .storage import ControlPlaneStore


class CompanyAssetWriter:
    """Owner-facing facade for company assets, evidence, and assertions."""

    def __init__(self, store: ControlPlaneStore, *, writer_id: str = "company_asset_writer_v1") -> None:
        self.store = store
        self.writer_id = str(writer_id or "company_asset_writer_v1").strip() or "company_asset_writer_v1"

    def record_asset(self, payload: dict[str, Any]) -> dict[str, Any]:
        metadata = {**dict(payload.get("metadata") or {}), "writer_id": self.writer_id}
        return self.store.upsert_company_asset({**dict(payload or {}), "metadata": metadata})

    def record_evidence(self, payload: dict[str, Any]) -> dict[str, Any]:
        metadata = {**dict(payload.get("metadata") or {}), "writer_id": self.writer_id}
        return self.store.upsert_company_evidence({**dict(payload or {}), "metadata": metadata})

    def record_assertion(self, payload: dict[str, Any]) -> dict[str, Any]:
        metadata = {**dict(payload.get("metadata") or {}), "writer_id": self.writer_id}
        return self.store.upsert_company_assertion({**dict(payload or {}), "metadata": metadata})
