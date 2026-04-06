from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from hashlib import sha1
import json
from pathlib import Path
import threading
from typing import Any


_REGISTRY_LOCK = threading.Lock()


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
        target_path = target_path.resolve()
        relative_path = self._relative_path(target_path)
        entry = AssetLogEntry(
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
        with _REGISTRY_LOCK:
            payload = self._load_registry_payload()
            assets = payload.setdefault("assets", [])
            replaced = False
            for index, item in enumerate(assets):
                if not isinstance(item, dict):
                    continue
                if item.get("relative_path") == relative_path:
                    assets[index] = asdict(entry)
                    replaced = True
                    break
            if not replaced:
                assets.append(asdict(entry))
            payload["root_dir"] = str(self.root_dir.resolve())
            payload["updated_at"] = entry.updated_at
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
