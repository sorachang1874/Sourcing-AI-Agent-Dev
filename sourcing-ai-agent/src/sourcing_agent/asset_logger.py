from __future__ import annotations

import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from hashlib import sha1
from pathlib import Path
from typing import Any

_REGISTRY_LOCK = threading.Lock()
_DISABLE_BATCH_JSON_WRITES_ENV = "SOURCING_CANDIDATE_ARTIFACT_DISABLE_BATCH_JSON_WRITES"


def _batch_json_writes_disabled() -> bool:
    raw_value = str(os.getenv(_DISABLE_BATCH_JSON_WRITES_ENV) or "").strip().lower()
    if not raw_value:
        return False
    return raw_value not in {"0", "false", "no", "off"}


@dataclass(slots=True)
class AssetLogEntry:
    asset_id: str
    relative_path: str
    absolute_path: str
    asset_type: str
    source_kind: str
    content_type: str
    size_bytes: int
    is_raw_asset: bool
    model_safe: bool
    metadata: dict[str, Any]
    updated_at: str


class AssetLogger:
    def __init__(self, root_dir: str | Path) -> None:
        self.root_dir = Path(root_dir)
        self.root_dir.mkdir(parents=True, exist_ok=True)
        self.registry_path = self.root_dir / "asset_registry.json"

    def write_json(
        self,
        path: str | Path,
        payload: Any,
        *,
        asset_type: str,
        source_kind: str,
        is_raw_asset: bool = True,
        model_safe: bool = False,
        metadata: dict[str, Any] | None = None,
    ) -> Path:
        target_path = self._resolve_path(path)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        text = json.dumps(payload, ensure_ascii=False, indent=2)
        target_path.write_text(text)
        self._record(
            target_path,
            asset_type=asset_type,
            source_kind=source_kind,
            content_type="application/json",
            is_raw_asset=is_raw_asset,
            model_safe=model_safe,
            metadata=metadata or {},
        )
        return target_path

    def write_json_batch(
        self,
        entries: list[dict[str, Any]] | tuple[dict[str, Any], ...],
        *,
        parallel_workers: int = 1,
    ) -> list[Path]:
        if _batch_json_writes_disabled():
            written_paths: list[Path] = []
            for item in list(entries or []):
                if not isinstance(item, dict):
                    continue
                target_path = item.get("path") or ""
                if not str(target_path).strip():
                    continue
                written_paths.append(
                    self.write_json(
                        target_path,
                        item.get("payload"),
                        asset_type=str(item.get("asset_type") or "").strip(),
                        source_kind=str(item.get("source_kind") or "").strip(),
                        is_raw_asset=bool(item.get("is_raw_asset", True)),
                        model_safe=bool(item.get("model_safe", False)),
                        metadata=dict(item.get("metadata") or {}),
                    )
                )
            return written_paths
        prepared_entries: list[dict[str, Any]] = []
        for item in list(entries or []):
            if not isinstance(item, dict):
                continue
            target_path = self._resolve_path(item.get("path") or "")
            if not str(target_path).strip():
                continue
            target_path.parent.mkdir(parents=True, exist_ok=True)
            prepared_entries.append(
                {
                    "target_path": target_path,
                    "text": json.dumps(item.get("payload"), ensure_ascii=False, indent=2),
                    "asset_type": str(item.get("asset_type") or "").strip(),
                    "source_kind": str(item.get("source_kind") or "").strip(),
                    "is_raw_asset": bool(item.get("is_raw_asset", True)),
                    "model_safe": bool(item.get("model_safe", False)),
                    "metadata": dict(item.get("metadata") or {}),
                }
            )
        if not prepared_entries:
            return []

        def _write_prepared_entry(entry: dict[str, Any]) -> Path:
            target_path = Path(entry["target_path"])
            target_path.write_text(str(entry["text"]))
            return target_path

        normalized_parallel_workers = max(1, int(parallel_workers or 1))
        if normalized_parallel_workers > 1 and len(prepared_entries) > 1:
            with ThreadPoolExecutor(max_workers=normalized_parallel_workers, thread_name_prefix="asset-json-batch") as executor:
                written_paths = list(executor.map(_write_prepared_entry, prepared_entries))
        else:
            written_paths = [_write_prepared_entry(entry) for entry in prepared_entries]

        self._record_many(
            [
                self._build_entry(
                    Path(entry["target_path"]),
                    asset_type=str(entry["asset_type"]),
                    source_kind=str(entry["source_kind"]),
                    content_type="application/json",
                    is_raw_asset=bool(entry["is_raw_asset"]),
                    model_safe=bool(entry["model_safe"]),
                    metadata=dict(entry["metadata"]),
                )
                for entry in prepared_entries
            ]
        )
        return written_paths

    def write_text(
        self,
        path: str | Path,
        text: str,
        *,
        asset_type: str,
        source_kind: str,
        content_type: str = "text/plain",
        is_raw_asset: bool = True,
        model_safe: bool = False,
        metadata: dict[str, Any] | None = None,
    ) -> Path:
        target_path = self._resolve_path(path)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text(text)
        self._record(
            target_path,
            asset_type=asset_type,
            source_kind=source_kind,
            content_type=content_type,
            is_raw_asset=is_raw_asset,
            model_safe=model_safe,
            metadata=metadata or {},
        )
        return target_path

    def write_bytes(
        self,
        path: str | Path,
        content: bytes,
        *,
        asset_type: str,
        source_kind: str,
        content_type: str = "application/octet-stream",
        is_raw_asset: bool = True,
        model_safe: bool = False,
        metadata: dict[str, Any] | None = None,
    ) -> Path:
        target_path = self._resolve_path(path)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_bytes(content)
        self._record(
            target_path,
            asset_type=asset_type,
            source_kind=source_kind,
            content_type=content_type,
            is_raw_asset=is_raw_asset,
            model_safe=model_safe,
            metadata=metadata or {},
        )
        return target_path

    def record_existing(
        self,
        path: str | Path,
        *,
        asset_type: str,
        source_kind: str,
        content_type: str = "application/octet-stream",
        is_raw_asset: bool = True,
        model_safe: bool = False,
        metadata: dict[str, Any] | None = None,
    ) -> Path:
        target_path = self._resolve_path(path)
        if not target_path.exists():
            raise FileNotFoundError(target_path)
        self._record(
            target_path,
            asset_type=asset_type,
            source_kind=source_kind,
            content_type=content_type,
            is_raw_asset=is_raw_asset,
            model_safe=model_safe,
            metadata=metadata or {},
        )
        return target_path

    def list_entries(self) -> list[dict[str, Any]]:
        if not self.registry_path.exists():
            return []
        try:
            payload = json.loads(self.registry_path.read_text())
        except (OSError, json.JSONDecodeError):
            return []
        assets = payload.get("assets") or []
        return [item for item in assets if isinstance(item, dict)]

    def _record(
        self,
        target_path: Path,
        *,
        asset_type: str,
        source_kind: str,
        content_type: str,
        is_raw_asset: bool,
        model_safe: bool,
        metadata: dict[str, Any],
    ) -> None:
        entry = self._build_entry(
            target_path,
            asset_type=asset_type,
            source_kind=source_kind,
            content_type=content_type,
            is_raw_asset=is_raw_asset,
            model_safe=model_safe,
            metadata=metadata,
        )
        self._record_many([entry])

    def _build_entry(
        self,
        target_path: Path,
        *,
        asset_type: str,
        source_kind: str,
        content_type: str,
        is_raw_asset: bool,
        model_safe: bool,
        metadata: dict[str, Any],
    ) -> AssetLogEntry:
        target_path = target_path.resolve()
        relative_path = self._relative_path(target_path)
        return AssetLogEntry(
            asset_id=_asset_id(relative_path, asset_type, source_kind),
            relative_path=relative_path,
            absolute_path=str(target_path),
            asset_type=asset_type,
            source_kind=source_kind,
            content_type=content_type,
            size_bytes=target_path.stat().st_size if target_path.exists() else 0,
            is_raw_asset=is_raw_asset,
            model_safe=model_safe,
            metadata=metadata,
            updated_at=_timestamp(),
        )

    def _record_many(self, entries: list[AssetLogEntry] | tuple[AssetLogEntry, ...]) -> None:
        normalized_entries = [entry for entry in list(entries or []) if isinstance(entry, AssetLogEntry)]
        if not normalized_entries:
            return
        with _REGISTRY_LOCK:
            payload = self._load_registry_payload()
            assets = payload.setdefault("assets", [])
            index_by_relative_path = {
                str(item.get("relative_path") or ""): index
                for index, item in enumerate(assets)
                if isinstance(item, dict) and str(item.get("relative_path") or "").strip()
            }
            for entry in normalized_entries:
                serialized_entry = asdict(entry)
                existing_index = index_by_relative_path.get(entry.relative_path)
                if existing_index is not None:
                    assets[existing_index] = serialized_entry
                    continue
                index_by_relative_path[entry.relative_path] = len(assets)
                assets.append(serialized_entry)
            payload["root_dir"] = str(self.root_dir.resolve())
            payload["updated_at"] = normalized_entries[-1].updated_at
            self.registry_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))

    def _load_registry_payload(self) -> dict[str, Any]:
        if not self.registry_path.exists():
            return {"root_dir": str(self.root_dir.resolve()), "assets": [], "updated_at": _timestamp()}
        try:
            payload = json.loads(self.registry_path.read_text())
        except (OSError, json.JSONDecodeError):
            return {"root_dir": str(self.root_dir.resolve()), "assets": [], "updated_at": _timestamp()}
        if not isinstance(payload, dict):
            return {"root_dir": str(self.root_dir.resolve()), "assets": [], "updated_at": _timestamp()}
        if not isinstance(payload.get("assets"), list):
            payload["assets"] = []
        return payload

    def _resolve_path(self, path: str | Path) -> Path:
        target_path = Path(path)
        if target_path.is_absolute():
            return target_path
        return self.root_dir / target_path

    def _relative_path(self, target_path: Path) -> str:
        try:
            return str(target_path.relative_to(self.root_dir.resolve()))
        except ValueError:
            return target_path.name


def _asset_id(relative_path: str, asset_type: str, source_kind: str) -> str:
    raw = f"{relative_path}|{asset_type}|{source_kind}"
    return sha1(raw.encode("utf-8")).hexdigest()[:16]


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()
