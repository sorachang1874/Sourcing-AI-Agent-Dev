from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any


def normalize_company_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def canonicalize_company_key(value: str) -> str:
    normalized = normalize_company_key(value)
    if not normalized:
        return ""
    try:
        from .company_registry import resolve_company_alias_key
    except Exception:
        return normalized
    resolved = normalize_company_key(resolve_company_alias_key(value))
    return resolved or normalized


def company_key_resolution_candidates(value: str) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()
    for candidate in (normalize_company_key(value), canonicalize_company_key(value)):
        normalized = normalize_company_key(candidate)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        candidates.append(normalized)
    return candidates


def _resolve_root_env(value: str | None, *, runtime_dir: Path) -> Path | None:
    if not value:
        return None
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = (runtime_dir / path).resolve()
    return path


def configured_company_assets_dir(runtime_dir: str | Path) -> Path:
    runtime_root = Path(runtime_dir).expanduser()
    override = _resolve_root_env(os.getenv("SOURCING_COMPANY_ASSETS_DIR"), runtime_dir=runtime_root)
    return (override or (runtime_root / "company_assets")).resolve()


def canonical_company_assets_dir(runtime_dir: str | Path) -> Path:
    runtime_root = Path(runtime_dir).expanduser()
    override = _resolve_root_env(os.getenv("SOURCING_CANONICAL_ASSETS_DIR"), runtime_dir=runtime_root)
    return (override or configured_company_assets_dir(runtime_root)).resolve()


def hot_cache_company_assets_dir(runtime_dir: str | Path) -> Path | None:
    runtime_root = Path(runtime_dir).expanduser()
    raw_override = str(os.getenv("SOURCING_HOT_CACHE_ASSETS_DIR") or "").strip()
    if raw_override.lower() in {"0", "false", "no", "off", "disabled", "none"}:
        return None
    override = _resolve_root_env(raw_override or None, runtime_dir=runtime_root)
    return (override or (runtime_root / "hot_cache_company_assets")).resolve()


def company_assets_roots(
    runtime_dir: str | Path,
    *,
    prefer_hot_cache: bool = True,
    existing_only: bool = False,
) -> list[Path]:
    canonical_root = canonical_company_assets_dir(runtime_dir)
    hot_cache_root = hot_cache_company_assets_dir(runtime_dir)
    ordered_candidates = [hot_cache_root, canonical_root] if prefer_hot_cache else [canonical_root, hot_cache_root]
    roots: list[Path] = []
    seen: set[str] = set()
    for candidate in ordered_candidates:
        if candidate is None:
            continue
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        if existing_only and not candidate.exists():
            continue
        roots.append(candidate)
    return roots


def iter_company_asset_company_dirs(
    runtime_dir: str | Path,
    *,
    prefer_hot_cache: bool = True,
    existing_only: bool = True,
) -> list[Path]:
    company_dirs: list[Path] = []
    seen_company_keys: set[str] = set()
    for root in company_assets_roots(
        runtime_dir,
        prefer_hot_cache=prefer_hot_cache,
        existing_only=existing_only,
    ):
        if not root.exists():
            continue
        for company_dir in sorted(path for path in root.iterdir() if path.is_dir()):
            company_key = normalize_company_key(company_dir.name) or company_dir.name
            if company_key in seen_company_keys:
                continue
            seen_company_keys.add(company_key)
            company_dirs.append(company_dir)
    return company_dirs


def iter_company_asset_snapshot_dirs(
    runtime_dir: str | Path,
    *,
    prefer_hot_cache: bool = True,
    existing_only: bool = True,
) -> list[Path]:
    snapshot_dirs: list[Path] = []
    seen_snapshot_keys: set[tuple[str, str]] = set()
    for root in company_assets_roots(
        runtime_dir,
        prefer_hot_cache=prefer_hot_cache,
        existing_only=existing_only,
    ):
        if not root.exists():
            continue
        for company_dir in sorted(path for path in root.iterdir() if path.is_dir()):
            company_key = normalize_company_key(company_dir.name) or company_dir.name
            for snapshot_dir in sorted(path for path in company_dir.iterdir() if path.is_dir()):
                snapshot_key = (company_key, str(snapshot_dir.name or "").strip())
                if not snapshot_key[1] or snapshot_key in seen_snapshot_keys:
                    continue
                seen_snapshot_keys.add(snapshot_key)
                snapshot_dirs.append(snapshot_dir)
    return snapshot_dirs


def iter_company_asset_files(
    runtime_dir: str | Path,
    *,
    pattern: str,
    prefer_hot_cache: bool = True,
    existing_only: bool = True,
) -> list[Path]:
    resolved_paths: list[Path] = []
    seen_relative_paths: set[str] = set()
    for root in company_assets_roots(
        runtime_dir,
        prefer_hot_cache=prefer_hot_cache,
        existing_only=existing_only,
    ):
        if not root.exists():
            continue
        for path in sorted(root.rglob(pattern)):
            if not path.is_file():
                continue
            try:
                relative_key = path.relative_to(root).as_posix()
            except ValueError:
                relative_key = str(path)
            if relative_key in seen_relative_paths:
                continue
            seen_relative_paths.add(relative_key)
            resolved_paths.append(path)
    return resolved_paths


def load_company_snapshot_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def load_latest_snapshot_pointer(company_dir: Path) -> dict:
    return load_company_snapshot_json(company_dir / "latest_snapshot.json")


def canonical_company_dir(runtime_dir: str | Path, company_key: str) -> Path:
    return canonical_company_assets_dir(runtime_dir) / normalize_company_key(company_key)


def hot_cache_company_dir(runtime_dir: str | Path, company_key: str) -> Path | None:
    hot_cache_root = hot_cache_company_assets_dir(runtime_dir)
    if hot_cache_root is None:
        return None
    return hot_cache_root / normalize_company_key(company_key)


def canonical_snapshot_dir(runtime_dir: str | Path, company_key: str, snapshot_id: str) -> Path:
    return canonical_company_dir(runtime_dir, company_key) / str(snapshot_id or "").strip()


def hot_cache_snapshot_dir(runtime_dir: str | Path, company_key: str, snapshot_id: str) -> Path | None:
    company_dir = hot_cache_company_dir(runtime_dir, company_key)
    if company_dir is None:
        return None
    return company_dir / str(snapshot_id or "").strip()


def latest_snapshot_id_for_company_dir(company_dir: Path) -> str:
    latest_payload = load_latest_snapshot_pointer(company_dir)
    snapshot_id = str(latest_payload.get("snapshot_id") or "").strip()
    if snapshot_id:
        return snapshot_id
    snapshot_dirs = sorted(path.name for path in company_dir.iterdir() if path.is_dir())
    if not snapshot_dirs:
        return ""
    return snapshot_dirs[-1]


def load_company_snapshot_identity(
    snapshot_dir: Path,
    *,
    fallback_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    for path, key_path in [
        (snapshot_dir / "identity.json", ("",)),
        (snapshot_dir / "manifest.json", ("company_identity",)),
        (snapshot_dir / "candidate_documents.json", ("snapshot", "company_identity")),
    ]:
        payload = load_company_snapshot_json(path)
        if not payload:
            continue
        current: Any = payload
        for key in key_path:
            if not key:
                break
            current = dict(current).get(key) if isinstance(current, dict) else {}
        if isinstance(current, dict) and current:
            return dict(current)
    return dict((fallback_payload or {}).get("company_identity") or {})


def resolve_snapshot_serving_artifact_path(
    snapshot_dir: Path,
    *,
    asset_view: str = "canonical_merged",
    allow_candidate_documents_fallback: bool = False,
) -> Path | None:
    normalized_snapshot_dir = Path(snapshot_dir).expanduser()
    normalized_view = str(asset_view or "canonical_merged").strip() or "canonical_merged"
    artifact_dir = normalized_snapshot_dir / "normalized_artifacts"
    if normalized_view != "canonical_merged":
        artifact_dir = artifact_dir / normalized_view
    for candidate in (
        artifact_dir / "manifest.json",
        artifact_dir / "artifact_summary.json",
        artifact_dir / "snapshot_manifest.json",
        artifact_dir / "materialized_candidate_documents.json",
    ):
        if candidate.exists():
            return candidate
    if allow_candidate_documents_fallback and normalized_view == "canonical_merged":
        candidate_documents_path = normalized_snapshot_dir / "candidate_documents.json"
        if candidate_documents_path.exists():
            return candidate_documents_path
    return None


def _snapshot_dir_from_company_assets_parts(path: Path) -> Path | None:
    parts = path.parts
    for index, segment in enumerate(parts):
        if segment != "company_assets" or index + 2 >= len(parts):
            continue
        snapshot_dir = Path(*parts[: index + 3])
        if not snapshot_dir.is_dir():
            continue
        if path.is_dir() or len(parts) > index + 3:
            return snapshot_dir
    return None


def _resolve_snapshot_dir_from_existing_path(path: Path) -> Path | None:
    snapshot_dir = _snapshot_dir_from_company_assets_parts(path)
    if snapshot_dir is not None:
        return snapshot_dir
    if path.is_dir():
        if (path / "candidate_documents.json").exists() or (path / "normalized_artifacts").exists():
            return path
        if path.name == "normalized_artifacts":
            return path.parent
        if path.parent.name == "normalized_artifacts":
            return path.parent.parent
        return None
    if path.name.startswith("candidate_documents."):
        return path.parent
    if path.name in {"materialized_candidate_documents.json", "artifact_summary.json", "manifest.json", "snapshot_manifest.json"}:
        if path.parent.name == "normalized_artifacts":
            return path.parent.parent
        if path.parent.parent.name == "normalized_artifacts":
            return path.parent.parent.parent
    if path.parent.name in {"candidates", "pages", "backlogs"}:
        if path.parent.parent.name == "normalized_artifacts":
            return path.parent.parent.parent
        if path.parent.parent.parent.name == "normalized_artifacts":
            return path.parent.parent.parent.parent
    return None


def extract_company_snapshot_ref(path: str | Path | None) -> tuple[str, str] | None:
    if path in (None, ""):
        return None
    candidate = Path(str(path)).expanduser()
    parts = candidate.parts
    for index, segment in enumerate(parts):
        if segment != "company_assets":
            continue
        if index + 2 >= len(parts):
            return None
        company_key = str(parts[index + 1] or "").strip()
        snapshot_id = str(parts[index + 2] or "").strip()
        if company_key and snapshot_id:
            return company_key, snapshot_id
        return None
    return None


def _snapshot_relative_path_from_source_path(path: str | Path | None) -> Path | None:
    if path in (None, ""):
        return None
    candidate = Path(str(path)).expanduser()
    parts = candidate.parts
    for index, segment in enumerate(parts):
        if segment != "company_assets":
            continue
        if index + 2 >= len(parts):
            return None
        relative_parts = parts[index + 3 :]
        return Path(*relative_parts) if relative_parts else Path()
    return None


def _default_runtime_dir(runtime_dir: str | Path | None = None) -> Path | None:
    if runtime_dir not in (None, ""):
        return Path(str(runtime_dir)).expanduser().resolve()
    project_root = Path(__file__).resolve().parents[2]
    raw_runtime_dir = str(os.getenv("SOURCING_RUNTIME_DIR") or "").strip()
    if raw_runtime_dir:
        candidate = Path(raw_runtime_dir).expanduser()
        if not candidate.is_absolute():
            candidate = (project_root / candidate).resolve()
        return candidate
    return (project_root / "runtime").resolve()


def resolve_company_snapshot_dir_by_key(
    runtime_dir: str | Path,
    *,
    company_key: str,
    snapshot_id: str,
    prefer_hot_cache: bool = True,
) -> Path | None:
    normalized_company_key = normalize_company_key(company_key)
    normalized_snapshot_id = str(snapshot_id or "").strip()
    if not normalized_company_key or not normalized_snapshot_id:
        return None
    canonical_root = canonical_company_assets_dir(runtime_dir)
    search_roots = company_assets_roots(
        runtime_dir,
        prefer_hot_cache=prefer_hot_cache,
        existing_only=False,
    )
    candidates: list[dict[str, Any]] = []
    for root_index, root in enumerate(search_roots):
        candidate = root / normalized_company_key / normalized_snapshot_id
        if candidate.exists():
            candidates.append(
                {
                    "snapshot_dir": candidate,
                    "root_priority": len(search_roots) - root_index,
                    "canonical_root": root == canonical_root,
                }
            )
    if not candidates:
        return None
    selected = max(
        candidates,
        key=lambda entry: _snapshot_dir_serving_preference_sort_key(Path(entry["snapshot_dir"]))
        + (
            int(entry.get("root_priority") or 0),
            1 if bool(entry.get("canonical_root")) else 0,
            str(Path(entry["snapshot_dir"]).parent.name),
            str(Path(entry["snapshot_dir"]).name),
        ),
    )
    return Path(selected["snapshot_dir"])


def resolve_source_path_in_runtime(
    source_path: str | Path | None,
    *,
    runtime_dir: str | Path | None = None,
    prefer_hot_cache: bool = True,
) -> Path | None:
    if source_path in (None, ""):
        return None
    candidate = Path(str(source_path)).expanduser()
    if candidate.exists():
        return candidate.resolve()

    resolved_runtime_dir = _default_runtime_dir(runtime_dir)
    snapshot_ref = extract_company_snapshot_ref(candidate)
    if snapshot_ref is None or resolved_runtime_dir is None:
        return None
    company_key, snapshot_id = snapshot_ref
    snapshot_dir = resolve_company_snapshot_dir_by_key(
        resolved_runtime_dir,
        company_key=company_key,
        snapshot_id=snapshot_id,
        prefer_hot_cache=prefer_hot_cache,
    )
    if snapshot_dir is None:
        return None
    relative_path = _snapshot_relative_path_from_source_path(candidate)
    if relative_path is None or relative_path == Path():
        return snapshot_dir
    remapped = snapshot_dir / relative_path
    if remapped.exists():
        return remapped
    return None


def build_company_snapshot_resolution_entry(
    company_dir: Path,
    *,
    snapshot_id: str = "",
    canonical_root: bool = False,
    root_priority: int = 0,
) -> dict[str, Any] | None:
    if not company_dir.exists() or not company_dir.is_dir():
        return None
    normalized_snapshot_id = str(snapshot_id or "").strip()
    if normalized_snapshot_id and not (company_dir / normalized_snapshot_id).exists():
        return None
    latest_payload = load_latest_snapshot_pointer(company_dir)
    preview_snapshot_id = str(
        normalized_snapshot_id or latest_payload.get("snapshot_id") or latest_snapshot_id_for_company_dir(company_dir)
    ).strip()
    if not preview_snapshot_id:
        return None
    return {
        "company_dir": company_dir,
        "latest_payload": latest_payload,
        "preview_snapshot_id": preview_snapshot_id,
        "canonical_root": canonical_root,
        "root_priority": root_priority,
    }


def build_company_snapshot_match_entry(
    company_dir: Path,
    *,
    target_company: str,
    snapshot_id: str = "",
    canonical_root: bool = False,
    root_priority: int = 0,
) -> dict[str, Any] | None:
    resolution_entry = build_company_snapshot_resolution_entry(
        company_dir,
        snapshot_id=snapshot_id,
        canonical_root=canonical_root,
        root_priority=root_priority,
    )
    if resolution_entry is None:
        return None
    latest_payload = dict(resolution_entry.get("latest_payload") or {})
    preview_snapshot_id = str(resolution_entry.get("preview_snapshot_id") or "").strip()
    preview_identity = dict(latest_payload.get("company_identity") or {})
    if preview_snapshot_id:
        preview_identity = load_company_snapshot_identity(
            company_dir / preview_snapshot_id,
            fallback_payload=latest_payload,
        )
    match_score = score_company_snapshot_dir_match(
        company_dir,
        preview_identity,
        target_company,
    )
    if match_score <= 0:
        return None
    return dict(
        resolution_entry,
        match_score=match_score,
        preview_identity=preview_identity,
        group_key=company_snapshot_group_key(
            company_dir=company_dir,
            latest_payload=latest_payload,
            identity_payload=preview_identity,
        ),
    )


def _snapshot_dir_serving_preference_sort_key(snapshot_dir: Path) -> tuple[int, int, int, int, int]:
    normalized_dir = Path(snapshot_dir)
    normalized_artifact_dir = normalized_dir / "normalized_artifacts"
    has_materialized_candidate_documents = int((normalized_artifact_dir / "materialized_candidate_documents.json").exists())
    has_manifest = int(
        (normalized_artifact_dir / "manifest.json").exists()
        or (normalized_artifact_dir / "snapshot_manifest.json").exists()
    )
    has_artifact_summary = int((normalized_artifact_dir / "artifact_summary.json").exists())
    has_candidate_documents = int((normalized_dir / "candidate_documents.json").exists())
    has_identity = int((normalized_dir / "identity.json").exists() or (normalized_dir / "manifest.json").exists())
    has_serving_payload = int(
        any(
            (
                has_materialized_candidate_documents,
                has_manifest,
                has_artifact_summary,
                has_candidate_documents,
            )
        )
    )
    return (
        has_serving_payload,
        has_materialized_candidate_documents,
        has_manifest,
        has_artifact_summary,
        has_candidate_documents + has_identity,
    )


def select_company_snapshot_resolution(
    entries: list[dict[str, Any]],
    *,
    snapshot_id: str = "",
) -> dict[str, Any] | None:
    if not entries:
        return None

    normalized_snapshot_id = str(snapshot_id or "").strip()
    pointer_entry: dict[str, Any]
    if normalized_snapshot_id:
        pointer_entry = max(entries, key=_company_snapshot_entry_sort_key)
        resolved_snapshot_id = normalized_snapshot_id
    else:
        pointer_entry = next((entry for entry in entries if bool(entry.get("canonical_root"))), {})
        if not pointer_entry:
            pointer_entry = max(entries, key=_company_snapshot_entry_sort_key)
        pointer_company_dir = Path(pointer_entry["company_dir"])
        latest_payload = dict(pointer_entry.get("latest_payload") or {})
        resolved_snapshot_id = str(
            pointer_entry.get("preview_snapshot_id")
            or latest_payload.get("snapshot_id")
            or latest_snapshot_id_for_company_dir(pointer_company_dir)
        ).strip()
    if not resolved_snapshot_id:
        return None

    snapshot_dir = _preferred_snapshot_dir_for_snapshot_id(entries, snapshot_id=resolved_snapshot_id)
    if snapshot_dir is None:
        return None

    pointer_company_dir = Path(pointer_entry["company_dir"])
    pointer_latest_payload = dict(pointer_entry.get("latest_payload") or {})
    return {
        "company_key": normalize_company_key(pointer_company_dir.name) or pointer_company_dir.name,
        "company_dir": pointer_company_dir,
        "latest_payload": pointer_latest_payload,
        "snapshot_id": resolved_snapshot_id,
        "snapshot_dir": snapshot_dir,
        "canonical_root": bool(pointer_entry.get("canonical_root")),
        "root_priority": int(pointer_entry.get("root_priority") or 0),
    }


def resolve_company_snapshot_selection(
    runtime_dir: str | Path,
    *,
    company_keys: str | list[str],
    snapshot_id: str = "",
    prefer_hot_cache: bool = True,
) -> dict[str, Any] | None:
    candidate_values = [company_keys] if isinstance(company_keys, str) else list(company_keys)
    normalized_company_keys: list[str] = []
    seen_company_keys: set[str] = set()
    for value in candidate_values:
        normalized = normalize_company_key(str(value or ""))
        if not normalized or normalized in seen_company_keys:
            continue
        seen_company_keys.add(normalized)
        normalized_company_keys.append(normalized)
    if not normalized_company_keys:
        return None

    canonical_root = canonical_company_assets_dir(runtime_dir)
    search_roots = company_assets_roots(
        runtime_dir,
        prefer_hot_cache=prefer_hot_cache,
        existing_only=False,
    )
    entries: list[dict[str, Any]] = []
    for root_index, assets_root in enumerate(search_roots):
        root_priority = len(search_roots) - root_index
        for company_key in normalized_company_keys:
            entry = build_company_snapshot_resolution_entry(
                assets_root / company_key,
                snapshot_id=snapshot_id,
                canonical_root=assets_root == canonical_root,
                root_priority=root_priority,
            )
            if entry is not None:
                entries.append({**entry, "requested_company_key": company_key})
    if not entries:
        return None
    for company_key in normalized_company_keys:
        company_key_entries = [entry for entry in entries if str(entry.get("requested_company_key") or "") == company_key]
        if company_key_entries:
            return select_company_snapshot_resolution(company_key_entries, snapshot_id=snapshot_id)
    return None


def resolve_company_snapshot_match_selection(
    runtime_dir: str | Path,
    *,
    target_company: str,
    snapshot_id: str = "",
    prefer_hot_cache: bool = True,
) -> dict[str, Any] | None:
    normalized_target = canonicalize_company_key(target_company)
    if not normalized_target:
        return None

    canonical_root = canonical_company_assets_dir(runtime_dir)
    search_roots = company_assets_roots(
        runtime_dir,
        prefer_hot_cache=prefer_hot_cache,
        existing_only=True,
    )
    matched_entries: list[dict[str, Any]] = []
    for root_index, assets_root in enumerate(search_roots):
        root_priority = len(search_roots) - root_index
        for company_dir in sorted(path for path in assets_root.iterdir() if path.is_dir()):
            entry = build_company_snapshot_match_entry(
                company_dir,
                target_company=target_company,
                snapshot_id=snapshot_id,
                canonical_root=assets_root == canonical_root,
                root_priority=root_priority,
            )
            if entry is not None:
                matched_entries.append(entry)
    if not matched_entries:
        return None

    selected_group = select_company_snapshot_match_group(matched_entries, snapshot_id=snapshot_id)
    selection = select_company_snapshot_resolution(selected_group, snapshot_id=snapshot_id)
    if selection is None:
        return None
    snapshot_dir = Path(selection["snapshot_dir"])
    latest_payload = dict(selection.get("latest_payload") or {})
    identity_payload = load_company_snapshot_identity(snapshot_dir, fallback_payload=latest_payload)
    return dict(
        selection,
        identity_payload=identity_payload,
        group_key=str(selected_group[0].get("group_key") or ""),
        match_score=max(int(entry.get("match_score") or 0) for entry in selected_group),
    )


def resolve_company_snapshot_dir(
    runtime_dir: str | Path,
    *,
    target_company: str,
    snapshot_id: str = "",
) -> Path | None:
    company_keys = company_key_resolution_candidates(target_company)
    if not company_keys:
        return None
    selection = resolve_company_snapshot_selection(
        runtime_dir,
        company_keys=company_keys,
        snapshot_id=snapshot_id,
        prefer_hot_cache=True,
    )
    if selection is None:
        return None
    snapshot_dir = selection.get("snapshot_dir")
    return Path(snapshot_dir) if snapshot_dir is not None else None


def company_snapshot_group_key(
    *,
    company_dir: Path,
    latest_payload: dict[str, Any],
    identity_payload: dict[str, Any],
) -> str:
    for value in [
        identity_payload.get("company_key"),
        latest_payload.get("company_key"),
        identity_payload.get("canonical_name"),
        latest_payload.get("canonical_name"),
        identity_payload.get("requested_name"),
        latest_payload.get("requested_name"),
        company_dir.name,
    ]:
        normalized = normalize_company_key(str(value or ""))
        if normalized:
            return normalized
    return company_dir.name


def score_company_snapshot_dir_match(
    company_dir: Path,
    identity_payload: dict[str, Any],
    target_company: str,
) -> int:
    normalized_target = canonicalize_company_key(target_company)
    if not normalized_target:
        return 0
    company_dir_key = normalize_company_key(company_dir.name)
    company_key = normalize_company_key(str(identity_payload.get("company_key") or ""))
    if company_key and company_key == normalized_target:
        return 130

    linkedin_slug = normalize_company_key(str(identity_payload.get("linkedin_slug") or ""))
    if linkedin_slug and linkedin_slug == normalized_target:
        return 120

    aliases = [
        normalize_company_key(str(item)) for item in list(identity_payload.get("aliases") or []) if str(item).strip()
    ]
    if normalized_target in aliases:
        return 110

    if company_dir_key == normalized_target:
        return 100

    for field in ("requested_name", "canonical_name"):
        value = normalize_company_key(str(identity_payload.get(field) or ""))
        if value and value == normalized_target:
            return 80
    return 0


def select_company_snapshot_match_group(
    entries: list[dict[str, Any]],
    *,
    snapshot_id: str = "",
) -> list[dict[str, Any]]:
    grouped_entries: dict[str, list[dict[str, Any]]] = {}
    for entry in entries:
        group_key = str(entry.get("group_key") or "")
        grouped_entries.setdefault(group_key, []).append(entry)

    if snapshot_id:
        best_entry = max(
            entries,
            key=lambda item: (
                int(item.get("match_score") or 0),
                int(item.get("root_priority") or 0),
                str(item.get("preview_snapshot_id") or ""),
                Path(item.get("company_dir") or ".").name,
            ),
        )
        return grouped_entries[str(best_entry.get("group_key") or "")]
    return max(grouped_entries.values(), key=_company_snapshot_match_group_sort_key)


def _company_snapshot_entry_sort_key(entry: dict[str, Any]) -> tuple[int, int, str, str]:
    return (
        int(entry.get("root_priority") or 0),
        1 if bool(entry.get("canonical_root")) else 0,
        str(entry.get("preview_snapshot_id") or ""),
        Path(entry.get("company_dir") or ".").name,
    )


def _company_snapshot_match_group_sort_key(group_entries: list[dict[str, Any]]) -> tuple[int, int, int, str]:
    best_match_score = max(int(entry.get("match_score") or 0) for entry in group_entries)
    canonical_present = 1 if any(bool(entry.get("canonical_root")) for entry in group_entries) else 0
    best_root_priority = max(int(entry.get("root_priority") or 0) for entry in group_entries)
    display_name = min(Path(entry.get("company_dir") or ".").name for entry in group_entries)
    return (best_match_score, canonical_present, best_root_priority, display_name)


def _preferred_snapshot_dir_for_snapshot_id(
    entries: list[dict[str, Any]],
    *,
    snapshot_id: str,
) -> Path | None:
    normalized_snapshot_id = str(snapshot_id or "").strip()
    if not normalized_snapshot_id:
        return None
    matching_entries = [entry for entry in entries if (Path(entry["company_dir"]) / normalized_snapshot_id).exists()]
    if not matching_entries:
        return None
    selected_entry = max(
        matching_entries,
        key=lambda entry: _snapshot_dir_serving_preference_sort_key(Path(entry["company_dir"]) / normalized_snapshot_id)
        + _company_snapshot_entry_sort_key(entry),
    )
    return Path(selected_entry["company_dir"]) / normalized_snapshot_id


def resolve_snapshot_dir_from_source_path(
    source_path: str | Path | None,
    *,
    runtime_dir: str | Path | None = None,
    prefer_hot_cache: bool = True,
) -> Path | None:
    if source_path in (None, ""):
        return None
    resolved_source_path = resolve_source_path_in_runtime(
        source_path,
        runtime_dir=runtime_dir,
        prefer_hot_cache=prefer_hot_cache,
    )
    if resolved_source_path is not None:
        snapshot_dir = _resolve_snapshot_dir_from_existing_path(resolved_source_path)
        if snapshot_dir is not None:
            return snapshot_dir

    candidate = Path(str(source_path)).expanduser()
    resolved_runtime_dir = _default_runtime_dir(runtime_dir)
    snapshot_ref = extract_company_snapshot_ref(candidate)
    if snapshot_ref is None or resolved_runtime_dir is None:
        return None
    company_key, snapshot_id = snapshot_ref
    return resolve_company_snapshot_dir_by_key(
        resolved_runtime_dir,
        company_key=company_key,
        snapshot_id=snapshot_id,
        prefer_hot_cache=prefer_hot_cache,
    )
