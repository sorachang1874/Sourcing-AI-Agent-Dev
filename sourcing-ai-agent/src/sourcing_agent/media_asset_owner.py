from __future__ import annotations

import base64
from dataclasses import asdict, is_dataclass
from hashlib import sha1, sha256
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote, urlparse

import requests

from .company_asset_writer import CompanyAssetWriter
from .company_registry import resolve_company_alias_key
from .object_storage import ObjectStorageConfig, build_object_storage_client
from .person_asset_writer import PersonAssetWriter
from .storage import ControlPlaneStore


MEDIA_ASSET_CACHE_CONTRACT = "media_asset_cache_owner_v1"
MEDIA_ASSET_READ_CONTRACT = "media_asset_read_contract_v1"
_ALLOWED_ENTITY_TYPES = {"person", "company"}
_DEFAULT_ASSET_TYPE_BY_ENTITY = {
    "person": "avatar_media",
    "company": "logo_media",
}
_ALLOWED_ASSET_TYPES_BY_ENTITY = {
    "person": {"avatar_media"},
    "company": {"logo_media"},
}
_MAX_MEDIA_BYTES = 5 * 1024 * 1024
_DEFAULT_TIMEOUT_SECONDS = 10
_MEDIA_ASSET_TYPES = {"avatar_media", "logo_media"}


def cache_media_asset(
    *,
    store: ControlPlaneStore,
    runtime_dir: Path,
    object_storage_settings: Any,
    payload: dict[str, Any],
) -> dict[str, Any]:
    request = dict(payload or {})
    entity_type = str(request.get("entity_type") or "").strip().lower()
    if entity_type not in _ALLOWED_ENTITY_TYPES:
        return {"status": "invalid", "reason": "entity_type must be person or company"}
    asset_type = str(request.get("asset_type") or _DEFAULT_ASSET_TYPE_BY_ENTITY[entity_type]).strip()
    if asset_type not in _ALLOWED_ASSET_TYPES_BY_ENTITY[entity_type]:
        return {"status": "invalid", "reason": f"asset_type {asset_type!r} is not allowed for {entity_type}"}

    entity_key = _entity_key(entity_type=entity_type, payload=request)
    if not entity_key:
        return {"status": "invalid", "reason": f"{entity_type}_entity_key_required"}

    max_bytes = _coerce_positive_int(request.get("max_bytes"), _MAX_MEDIA_BYTES)
    if max_bytes > _MAX_MEDIA_BYTES:
        max_bytes = _MAX_MEDIA_BYTES
    timeout_seconds = _coerce_positive_int(request.get("timeout_seconds"), _DEFAULT_TIMEOUT_SECONDS)

    media = _resolve_media_payload(
        request,
        max_bytes=max_bytes,
        timeout_seconds=timeout_seconds,
    )
    if str(media.get("status") or "") != "ok":
        return media

    media_bytes = bytes(media["payload_bytes"])
    content_type = str(media.get("content_type") or _infer_content_type(media_bytes, request.get("source_url"))).strip()
    if not _content_type_allowed(content_type):
        return {
            "status": "invalid",
            "reason": "unsupported_media_content_type",
            "content_type": content_type,
        }
    content_hash = sha256(media_bytes).hexdigest()
    extension = _extension_for_media(content_type, request.get("source_url"))
    object_key = _media_object_key(
        entity_type=entity_type,
        asset_type=asset_type,
        entity_key=entity_key,
        content_hash=content_hash,
        extension=extension,
    )
    object_storage = build_object_storage_client(
        _object_storage_config(object_storage_settings, runtime_dir=runtime_dir)
    )
    upload = object_storage.upload_bytes(media_bytes, object_key, content_type=content_type)
    now = str(request.get("fetched_at") or request.get("observed_at") or "").strip()
    source_url = str(request.get("source_url") or media.get("source_url") or "").strip()
    common_metadata = {
        "contract": MEDIA_ASSET_CACHE_CONTRACT,
        "object_key": str(upload.get("object_key") or object_key),
        "object_storage_provider": str(upload.get("provider") or ""),
        "content_type": content_type,
        "size_bytes": int(upload.get("size_bytes") or len(media_bytes)),
        "source_command_id": str(request.get("source_command_id") or "").strip(),
        "activity_run_id": str(request.get("activity_run_id") or "").strip(),
        "attempt_id": str(request.get("attempt_id") or "").strip(),
        "source": str(media.get("source") or "payload").strip(),
    }
    if entity_type == "person":
        asset = PersonAssetWriter(store, writer_id="media_asset_cache_owner").record_asset(
            {
                "asset_id": str(request.get("asset_id") or _asset_id("person", asset_type, entity_key, content_hash)),
                "person_identity_key": entity_key,
                "linkedin_url": str(request.get("linkedin_url") or "").strip(),
                "candidate_id": str(request.get("candidate_id") or "").strip(),
                "asset_type": asset_type,
                "source_kind": "media_asset_cache",
                "source_run_id": str(request.get("workflow_run_id") or "").strip(),
                "source_projection_id": str(request.get("source_projection_id") or "").strip(),
                "content_ref": str(upload.get("object_url") or ""),
                "content_hash": content_hash,
                "source_url": source_url,
                "fetched_at": now,
                "visibility_scope": str(request.get("visibility_scope") or "public_summary").strip()
                or "public_summary",
                "status": "available",
                "metadata": common_metadata,
            }
        )
    else:
        company_key = str(request.get("company_key") or "").strip() or entity_key
        target_company = str(request.get("target_company") or request.get("company") or company_key).strip()
        asset = CompanyAssetWriter(store, writer_id="media_asset_cache_owner").record_asset(
            {
                "asset_id": str(request.get("asset_id") or _asset_id("company", asset_type, company_key, content_hash)),
                "workspace_id": str(request.get("workspace_id") or "default").strip() or "default",
                "company_key": company_key,
                "target_company": target_company,
                "asset_type": asset_type,
                "source_kind": "media_asset_cache",
                "source_run_id": str(request.get("workflow_run_id") or "").strip(),
                "source_command_id": str(request.get("source_command_id") or "").strip(),
                "activity_run_id": str(request.get("activity_run_id") or "").strip(),
                "content_ref": str(upload.get("object_url") or ""),
                "content_hash": content_hash,
                "source_url": source_url,
                "fetched_at": now,
                "visibility_scope": str(request.get("visibility_scope") or "public_summary").strip()
                or "public_summary",
                "status": "available",
                "metadata": common_metadata,
            }
        )
    return {
        "status": "completed",
        "contract": MEDIA_ASSET_CACHE_CONTRACT,
        "entity_type": entity_type,
        "entity_key": entity_key,
        "asset_type": asset_type,
        "asset": asset,
        "asset_id": str(asset.get("asset_id") or ""),
        "content_hash": content_hash,
        "content_type": content_type,
        "size_bytes": len(media_bytes),
        "object_key": object_key,
        "object_url": str(upload.get("object_url") or ""),
        "upload": upload,
    }


def media_asset_frontend_url(asset: dict[str, Any], *, route_prefix: str = "/api/media/assets") -> str:
    payload = dict(asset or {})
    asset_id = str(payload.get("asset_id") or "").strip()
    content_ref = str(payload.get("content_ref") or "").strip()
    if not asset_id:
        return content_ref if content_ref.startswith(("http://", "https://")) else ""
    metadata = dict(payload.get("metadata") or {})
    if str(metadata.get("object_key") or "").strip() or content_ref.startswith("file://"):
        return f"{str(route_prefix or '/api/media/assets').rstrip('/')}/{quote(asset_id, safe='')}"
    if content_ref.startswith(("http://", "https://")):
        return content_ref
    return ""


def read_media_asset_content(
    *,
    store: ControlPlaneStore,
    runtime_dir: Path,
    object_storage_settings: Any,
    asset_id: str,
) -> dict[str, Any]:
    normalized_asset_id = str(asset_id or "").strip()
    if not normalized_asset_id:
        return {"status": "not_found", "reason": "asset_id_required", "contract": MEDIA_ASSET_READ_CONTRACT}
    asset = store.get_person_asset(normalized_asset_id)
    entity_type = "person" if asset else ""
    if not asset:
        asset = store.get_company_asset(normalized_asset_id)
        entity_type = "company" if asset else ""
    if not asset:
        return {"status": "not_found", "reason": "media_asset_not_found", "contract": MEDIA_ASSET_READ_CONTRACT}
    asset_type = str(asset.get("asset_type") or "").strip()
    if asset_type not in _MEDIA_ASSET_TYPES:
        return {
            "status": "invalid",
            "reason": "asset_is_not_public_media",
            "asset_id": normalized_asset_id,
            "asset_type": asset_type,
            "contract": MEDIA_ASSET_READ_CONTRACT,
        }
    if str(asset.get("status") or "").strip() != "available":
        return {
            "status": "not_ready",
            "reason": "media_asset_not_available",
            "asset_id": normalized_asset_id,
            "contract": MEDIA_ASSET_READ_CONTRACT,
        }
    if str(asset.get("visibility_scope") or "").strip() != "public_summary":
        return {
            "status": "forbidden",
            "reason": "media_asset_not_public_summary",
            "asset_id": normalized_asset_id,
            "contract": MEDIA_ASSET_READ_CONTRACT,
        }
    metadata = dict(asset.get("metadata") or {})
    object_key = str(metadata.get("object_key") or "").strip()
    content_ref = str(asset.get("content_ref") or "").strip()
    content_type = str(metadata.get("content_type") or _infer_content_type(b"", content_ref)).strip()
    if object_key:
        client = build_object_storage_client(_object_storage_config(object_storage_settings, runtime_dir=runtime_dir))
        payload = client.download_bytes(object_key)
        return {
            "status": "ready",
            "contract": MEDIA_ASSET_READ_CONTRACT,
            "asset": asset,
            "asset_id": normalized_asset_id,
            "entity_type": entity_type,
            "asset_type": asset_type,
            "content_type": content_type if _content_type_allowed(content_type) else "application/octet-stream",
            "content": payload,
            "size_bytes": len(payload),
            "read_contract": {
                "source": "PersonAsset/CompanyAsset.media_object_key",
                "fallback_used": False,
                "fail_closed": True,
            },
        }
    if content_ref.startswith("file://"):
        local_path = Path(unquote(urlparse(content_ref).path))
        if not local_path.exists() or not local_path.is_file():
            return {
                "status": "not_found",
                "reason": "media_file_not_found",
                "asset_id": normalized_asset_id,
                "contract": MEDIA_ASSET_READ_CONTRACT,
            }
        payload = local_path.read_bytes()
        return {
            "status": "ready",
            "contract": MEDIA_ASSET_READ_CONTRACT,
            "asset": asset,
            "asset_id": normalized_asset_id,
            "entity_type": entity_type,
            "asset_type": asset_type,
            "content_type": _infer_content_type(payload, content_ref),
            "content": payload,
            "size_bytes": len(payload),
            "read_contract": {
                "source": "PersonAsset/CompanyAsset.file_content_ref",
                "fallback_used": False,
                "fail_closed": True,
            },
        }
    return {
        "status": "external",
        "reason": "media_asset_uses_external_url",
        "asset_id": normalized_asset_id,
        "external_url": content_ref if content_ref.startswith(("http://", "https://")) else "",
        "contract": MEDIA_ASSET_READ_CONTRACT,
        "read_contract": {
            "source": "PersonAsset/CompanyAsset.external_content_ref",
            "fallback_used": False,
            "fail_closed": True,
        },
    }


def _object_storage_config(settings: Any, *, runtime_dir: Path) -> ObjectStorageConfig:
    if isinstance(settings, ObjectStorageConfig):
        config_payload = asdict(settings)
    elif is_dataclass(settings):
        config_payload = asdict(settings)
    elif isinstance(settings, dict):
        config_payload = dict(settings)
    else:
        config_payload = {}
    provider = str(config_payload.get("provider") or "filesystem").strip() or "filesystem"
    local_dir = str(config_payload.get("local_dir") or "").strip()
    if provider.lower() in {"", "filesystem"} and not local_dir:
        local_dir = str(Path(runtime_dir) / "object_store")
    return ObjectStorageConfig(
        enabled=bool(config_payload.get("enabled", True)),
        provider=provider,
        bucket=str(config_payload.get("bucket") or ""),
        prefix=str(config_payload.get("prefix") or "sourcing-ai-agent-dev"),
        endpoint_url=str(config_payload.get("endpoint_url") or ""),
        region=str(config_payload.get("region") or "us-east-1"),
        access_key_id=str(config_payload.get("access_key_id") or ""),
        secret_access_key=str(config_payload.get("secret_access_key") or ""),
        timeout_seconds=_coerce_positive_int(config_payload.get("timeout_seconds"), 60),
        force_path_style=bool(config_payload.get("force_path_style", True)),
        local_dir=local_dir,
        max_workers=_coerce_positive_int(config_payload.get("max_workers"), 8),
        multipart_threshold_bytes=_coerce_positive_int(
            config_payload.get("multipart_threshold_bytes"),
            64 * 1024 * 1024,
        ),
        multipart_chunk_size_bytes=_coerce_positive_int(
            config_payload.get("multipart_chunk_size_bytes"),
            64 * 1024 * 1024,
        ),
        multipart_max_workers=_coerce_positive_int(config_payload.get("multipart_max_workers"), 4),
    )


def _resolve_media_payload(
    payload: dict[str, Any],
    *,
    max_bytes: int,
    timeout_seconds: int,
) -> dict[str, Any]:
    encoded = str(payload.get("payload_bytes_base64") or payload.get("content_base64") or "").strip()
    if encoded:
        try:
            data = base64.b64decode(encoded, validate=True)
        except Exception as exc:
            return {"status": "invalid", "reason": "invalid_base64_media_payload", "error": str(exc)}
        if len(data) > max_bytes:
            return {"status": "invalid", "reason": "media_payload_exceeds_max_bytes", "size_bytes": len(data)}
        return {
            "status": "ok",
            "payload_bytes": data,
            "content_type": str(payload.get("content_type") or _infer_content_type(data, payload.get("source_url"))),
            "source": "payload_bytes_base64",
            "source_url": str(payload.get("source_url") or ""),
        }

    source_url = str(payload.get("source_url") or "").strip()
    if not source_url:
        return {"status": "invalid", "reason": "source_url_or_payload_bytes_base64_required"}
    parsed = urlparse(source_url)
    if parsed.scheme == "file":
        local_path = Path(unquote(parsed.path))
        if not local_path.exists() or not local_path.is_file():
            return {"status": "not_found", "reason": "source_file_not_found", "source_url": source_url}
        size_bytes = local_path.stat().st_size
        if size_bytes > max_bytes:
            return {"status": "invalid", "reason": "media_payload_exceeds_max_bytes", "size_bytes": size_bytes}
        data = local_path.read_bytes()
        return {
            "status": "ok",
            "payload_bytes": data,
            "content_type": str(payload.get("content_type") or _infer_content_type(data, source_url)),
            "source": "file_url",
            "source_url": source_url,
        }
    if parsed.scheme not in {"http", "https"}:
        return {"status": "invalid", "reason": "unsupported_media_source_url_scheme", "source_url": source_url}
    response = requests.get(source_url, stream=True, timeout=max(1, int(timeout_seconds or _DEFAULT_TIMEOUT_SECONDS)))
    if response.status_code >= 400:
        return {
            "status": "failed",
            "reason": "media_source_fetch_failed",
            "source_url": source_url,
            "http_status": response.status_code,
        }
    chunks: list[bytes] = []
    total = 0
    for chunk in response.iter_content(chunk_size=64 * 1024):
        if not chunk:
            continue
        total += len(chunk)
        if total > max_bytes:
            return {"status": "invalid", "reason": "media_payload_exceeds_max_bytes", "size_bytes": total}
        chunks.append(chunk)
    data = b"".join(chunks)
    return {
        "status": "ok",
        "payload_bytes": data,
        "content_type": str(payload.get("content_type") or response.headers.get("content-type") or ""),
        "source": "remote_url",
        "source_url": source_url,
    }


def _entity_key(*, entity_type: str, payload: dict[str, Any]) -> str:
    if entity_type == "person":
        return str(payload.get("person_identity_key") or "").strip()
    company_key = str(payload.get("company_key") or "").strip()
    if company_key:
        return company_key
    return resolve_company_alias_key(str(payload.get("target_company") or payload.get("company") or "").strip())


def _asset_id(entity_type: str, asset_type: str, entity_key: str, content_hash: str) -> str:
    prefix = "pma" if entity_type == "person" else "cma"
    digest = sha1(f"{entity_type}:{asset_type}:{entity_key}:{content_hash}".encode("utf-8")).hexdigest()[:24]
    return f"{prefix}_{digest}"


def _media_object_key(
    *,
    entity_type: str,
    asset_type: str,
    entity_key: str,
    content_hash: str,
    extension: str,
) -> str:
    safe_entity = _safe_path_component(entity_key)
    return f"media/{entity_type}/{asset_type}/{safe_entity}/{content_hash[:2]}/{content_hash}.{extension}"


def _safe_path_component(value: str) -> str:
    safe = []
    for char in str(value or "").strip().lower():
        if char.isalnum() or char in {"-", "_"}:
            safe.append(char)
        else:
            safe.append("-")
    normalized = "".join(safe).strip("-")
    return normalized[:96] or "unknown"


def _infer_content_type(payload: bytes, source_url: Any = "") -> str:
    if payload.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if payload.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if payload[:6] in {b"GIF87a", b"GIF89a"}:
        return "image/gif"
    stripped = payload.lstrip()[:200].lower()
    if stripped.startswith(b"<svg") or b"<svg" in stripped:
        return "image/svg+xml"
    suffix = Path(urlparse(str(source_url or "")).path).suffix.lower()
    if suffix in {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg"}:
        return {
            ".png": "image/png",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".gif": "image/gif",
            ".webp": "image/webp",
            ".svg": "image/svg+xml",
        }[suffix]
    return "application/octet-stream"


def _content_type_allowed(content_type: str) -> bool:
    normalized = str(content_type or "").split(";")[0].strip().lower()
    return normalized in {"image/png", "image/jpeg", "image/gif", "image/webp", "image/svg+xml"}


def _extension_for_media(content_type: str, source_url: Any = "") -> str:
    normalized = str(content_type or "").split(";")[0].strip().lower()
    if normalized == "image/png":
        return "png"
    if normalized == "image/jpeg":
        return "jpg"
    if normalized == "image/gif":
        return "gif"
    if normalized == "image/webp":
        return "webp"
    if normalized == "image/svg+xml":
        return "svg"
    suffix = Path(urlparse(str(source_url or "")).path).suffix.lower().strip(".")
    return suffix if suffix in {"png", "jpg", "jpeg", "gif", "webp", "svg"} else "bin"


def _coerce_positive_int(value: Any, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return int(default)
    return parsed if parsed > 0 else int(default)
