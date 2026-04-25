from __future__ import annotations

import json
import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .asset_paths import hot_cache_company_assets_dir, load_company_snapshot_identity, load_company_snapshot_json

_DEFAULT_HOT_CACHE_MIN_FREE_RATIO = 0.15
_DEFAULT_HOT_CACHE_MIN_FREE_FLOOR_BYTES = 8 * 1024 * 1024 * 1024
_HOT_CACHE_ACCESS_MARKER = ".hot_cache_accessed_at"
_DEFAULT_HOT_CACHE_HEALTHY_TTL_SECONDS = 14 * 24 * 60 * 60
_DEFAULT_HOT_CACHE_PRESSURED_TTL_SECONDS = 3 * 24 * 60 * 60
_DEFAULT_HOT_CACHE_SEVERE_TTL_SECONDS = 24 * 60 * 60
_DEFAULT_HOT_CACHE_GOVERNANCE_INTERVAL_SECONDS = 15 * 60
_DEFAULT_HOT_CACHE_PRESSURED_INTERVAL_SECONDS = 5 * 60
_DEFAULT_HOT_CACHE_SEVERE_INTERVAL_SECONDS = 60
_DEFAULT_HOT_CACHE_TARGET_BUDGET_RATIO = 0.05
_DEFAULT_HOT_CACHE_TARGET_BUDGET_FLOOR_BYTES = 2 * 1024 * 1024 * 1024
_DEFAULT_HOT_CACHE_TARGET_BUDGET_CAP_BYTES = 32 * 1024 * 1024 * 1024
_DEFAULT_HOT_CACHE_MAX_COMPANY_SHARE_RATIO = 0.35
_DEFAULT_HOT_CACHE_MAX_COMPANY_BYTES_FLOOR = 512 * 1024 * 1024


def configured_hot_cache_retention_policy(
    runtime_dir: str | Path | None = None,
    *,
    inventory: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ttl_seconds = _coerce_positive_int(os.getenv("SOURCING_HOT_CACHE_TTL_SECONDS"))
    explicit_size_budget_bytes = _coerce_positive_int(os.getenv("SOURCING_HOT_CACHE_SIZE_BUDGET_BYTES"))
    keep_latest_snapshots_per_company = max(
        1,
        _coerce_positive_int(os.getenv("SOURCING_HOT_CACHE_KEEP_LATEST_SNAPSHOTS_PER_COMPANY"), default=1),
    )
    max_generations_per_scope = max(
        1,
        _coerce_positive_int(os.getenv("SOURCING_HOT_CACHE_MAX_GENERATIONS_PER_SCOPE"), default=1),
    )
    explicit_max_bytes_per_company = _coerce_positive_int(os.getenv("SOURCING_HOT_CACHE_MAX_BYTES_PER_COMPANY"))
    auto_retention_enabled = _coerce_env_bool(os.getenv("SOURCING_HOT_CACHE_AUTO_RETENTION"), default=True)
    policy: dict[str, Any] = {
        "ttl_seconds": ttl_seconds,
        "size_budget_bytes": explicit_size_budget_bytes,
        "effective_size_budget_bytes": explicit_size_budget_bytes,
        "max_bytes_per_company": explicit_max_bytes_per_company,
        "keep_latest_snapshots_per_company": keep_latest_snapshots_per_company,
        "max_generations_per_scope": max_generations_per_scope,
        "auto_retention_enabled": auto_retention_enabled,
        "size_budget_source": "configured" if explicit_size_budget_bytes > 0 else "disabled",
        "company_budget_source": "configured" if explicit_max_bytes_per_company > 0 else "disabled",
        "skip_reason": "runtime_dir_unavailable",
    }
    if runtime_dir is None:
        return policy
    if not auto_retention_enabled and explicit_size_budget_bytes <= 0 and explicit_max_bytes_per_company <= 0:
        policy["effective_size_budget_bytes"] = 0
        policy["max_bytes_per_company"] = 0
        policy["company_budget_source"] = "disabled"
        policy["size_budget_source"] = "disabled"
        policy["skip_reason"] = "auto_retention_disabled"
        return policy

    runtime_root = Path(runtime_dir).expanduser()
    hot_cache_root = hot_cache_company_assets_dir(runtime_root)
    policy["runtime_dir"] = str(runtime_root)
    policy["hot_cache_root"] = str(hot_cache_root or "")
    if hot_cache_root is None or not hot_cache_root.exists():
        policy["size_budget_source"] = "configured" if explicit_size_budget_bytes > 0 else "disabled"
        policy["company_budget_source"] = "configured" if explicit_max_bytes_per_company > 0 else "disabled"
        policy["skip_reason"] = "hot_cache_root_unavailable"
        return policy

    disk_usage = shutil.disk_usage(hot_cache_root)
    reserve_free_ratio = _coerce_nonnegative_float(
        os.getenv("SOURCING_HOT_CACHE_MIN_FREE_RATIO"),
        default=_DEFAULT_HOT_CACHE_MIN_FREE_RATIO,
    )
    reserve_free_floor_bytes = _coerce_positive_int(
        os.getenv("SOURCING_HOT_CACHE_MIN_FREE_BYTES"),
        default=_DEFAULT_HOT_CACHE_MIN_FREE_FLOOR_BYTES,
    )
    reserve_free_bytes = _resolve_hot_cache_free_space_reserve(
        total_bytes=int(disk_usage.total),
        reserve_ratio=reserve_free_ratio,
        reserve_floor_bytes=reserve_free_floor_bytes,
    )
    effective_inventory = dict(inventory or collect_hot_cache_inventory(runtime_root))
    hot_cache_total_bytes = int(effective_inventory.get("total_bytes") or 0)
    company_records = [
        dict(item)
        for item in list(effective_inventory.get("company_records") or [])
        if isinstance(item, dict)
    ]
    if not company_records:
        company_records = _summarize_hot_cache_company_records(
            snapshot_records=[
                dict(item)
                for item in list(effective_inventory.get("snapshot_records") or [])
                if isinstance(item, dict)
            ],
            generation_records=[
                dict(item)
                for item in list(effective_inventory.get("generation_records") or [])
                if isinstance(item, dict)
            ],
        )
    free_space_deficit_bytes = max(0, reserve_free_bytes - int(disk_usage.free))
    target_budget_ratio = _coerce_nonnegative_float(
        os.getenv("SOURCING_HOT_CACHE_TARGET_BUDGET_RATIO"),
        default=_DEFAULT_HOT_CACHE_TARGET_BUDGET_RATIO,
    )
    target_budget_floor_bytes = _coerce_positive_int(
        os.getenv("SOURCING_HOT_CACHE_TARGET_BUDGET_FLOOR_BYTES"),
        default=_DEFAULT_HOT_CACHE_TARGET_BUDGET_FLOOR_BYTES,
    )
    target_budget_cap_bytes = _coerce_positive_int(
        os.getenv("SOURCING_HOT_CACHE_TARGET_BUDGET_CAP_BYTES"),
        default=_DEFAULT_HOT_CACHE_TARGET_BUDGET_CAP_BYTES,
    )
    auto_target_budget_bytes = _resolve_hot_cache_target_budget(
        disk_total_bytes=int(disk_usage.total),
        reserve_free_bytes=reserve_free_bytes,
        target_ratio=target_budget_ratio,
        floor_bytes=target_budget_floor_bytes,
        cap_bytes=target_budget_cap_bytes,
    )
    effective_size_budget_bytes = explicit_size_budget_bytes or auto_target_budget_bytes
    size_budget_source = "configured" if explicit_size_budget_bytes > 0 else "auto_target_budget"
    if free_space_deficit_bytes > 0 and hot_cache_total_bytes > 0:
        pressure_budget_bytes = max(0, hot_cache_total_bytes - free_space_deficit_bytes)
        if effective_size_budget_bytes <= 0:
            effective_size_budget_bytes = pressure_budget_bytes
            size_budget_source = "auto_disk_reserve"
        else:
            constrained_budget_bytes = min(effective_size_budget_bytes, pressure_budget_bytes)
            if constrained_budget_bytes < effective_size_budget_bytes:
                effective_size_budget_bytes = constrained_budget_bytes
                size_budget_source = f"{size_budget_source}_under_pressure"

    company_count = len(company_records)
    effective_max_bytes_per_company = explicit_max_bytes_per_company
    company_budget_source = "configured" if explicit_max_bytes_per_company > 0 else "disabled"
    if effective_max_bytes_per_company <= 0 and effective_size_budget_bytes > 0 and company_count > 1:
        company_share_ratio = _coerce_nonnegative_float(
            os.getenv("SOURCING_HOT_CACHE_MAX_COMPANY_SHARE_RATIO"),
            default=_DEFAULT_HOT_CACHE_MAX_COMPANY_SHARE_RATIO,
        )
        company_floor_bytes = _coerce_positive_int(
            os.getenv("SOURCING_HOT_CACHE_MAX_COMPANY_BYTES_FLOOR"),
            default=_DEFAULT_HOT_CACHE_MAX_COMPANY_BYTES_FLOOR,
        )
        fair_share_bytes = max(1, effective_size_budget_bytes // company_count)
        share_cap_bytes = max(0, int(float(effective_size_budget_bytes) * company_share_ratio))
        derived_company_budget_bytes = min(
            effective_size_budget_bytes,
            max(
                min(company_floor_bytes, effective_size_budget_bytes),
                min(
                    share_cap_bytes if share_cap_bytes > 0 else effective_size_budget_bytes,
                    fair_share_bytes * 2,
                ),
            ),
        )
        effective_max_bytes_per_company = max(0, int(derived_company_budget_bytes))
        company_budget_source = "auto_company_share"

    policy.update(
        {
            "effective_size_budget_bytes": max(0, int(effective_size_budget_bytes or 0)),
            "max_bytes_per_company": max(0, int(effective_max_bytes_per_company or 0)),
            "size_budget_source": size_budget_source,
            "company_budget_source": company_budget_source,
            "skip_reason": "",
            "runtime_dir": str(runtime_root),
            "hot_cache_root": str(hot_cache_root),
            "disk_total_bytes": int(disk_usage.total),
            "disk_used_bytes": int(disk_usage.used),
            "disk_free_bytes": int(disk_usage.free),
            "free_space_reserve_bytes": reserve_free_bytes,
            "hot_cache_total_bytes": hot_cache_total_bytes,
            "free_space_deficit_bytes": free_space_deficit_bytes,
            "auto_target_budget_bytes": auto_target_budget_bytes,
            "inventory_company_count": company_count,
        }
    )
    if policy["effective_size_budget_bytes"] <= 0 and ttl_seconds <= 0 and max_generations_per_scope <= 0:
        policy["skip_reason"] = "retention_policy_not_configured"
    return policy


def hot_cache_governance_state_path(runtime_dir: str | Path) -> Path:
    runtime_root = Path(runtime_dir).expanduser()
    return runtime_root / "object_sync" / "hot_cache" / "governance_state.json"


def load_hot_cache_governance_state(runtime_dir: str | Path) -> dict[str, Any]:
    state_path = hot_cache_governance_state_path(runtime_dir)
    if not state_path.exists():
        return {}
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def build_hot_cache_governance_policy(
    runtime_dir: str | Path,
    *,
    inventory: dict[str, Any] | None = None,
) -> dict[str, Any]:
    effective_inventory = dict(inventory or collect_hot_cache_inventory(runtime_dir))
    configured_policy = configured_hot_cache_retention_policy(runtime_dir=runtime_dir, inventory=effective_inventory)
    self_tuning_enabled = _coerce_env_bool(os.getenv("SOURCING_HOT_CACHE_SELF_TUNING"), default=True)
    reserve_bytes = int(configured_policy.get("free_space_reserve_bytes") or 0)
    free_space_deficit_bytes = int(configured_policy.get("free_space_deficit_bytes") or 0)
    pressure_ratio = (
        (float(free_space_deficit_bytes) / float(reserve_bytes))
        if reserve_bytes > 0 and free_space_deficit_bytes > 0
        else 0.0
    )
    pressure_level = "healthy"
    if free_space_deficit_bytes > 0:
        pressure_level = "severe" if pressure_ratio >= 0.5 else "pressured"

    configured_ttl_seconds = int(configured_policy.get("ttl_seconds") or 0)
    effective_ttl_seconds = configured_ttl_seconds
    if self_tuning_enabled and configured_ttl_seconds <= 0:
        if pressure_level == "severe":
            effective_ttl_seconds = _coerce_positive_int(
                os.getenv("SOURCING_HOT_CACHE_SEVERE_TTL_SECONDS"),
                default=_DEFAULT_HOT_CACHE_SEVERE_TTL_SECONDS,
            )
        elif pressure_level == "pressured":
            effective_ttl_seconds = _coerce_positive_int(
                os.getenv("SOURCING_HOT_CACHE_PRESSURED_TTL_SECONDS"),
                default=_DEFAULT_HOT_CACHE_PRESSURED_TTL_SECONDS,
            )
        else:
            effective_ttl_seconds = _coerce_positive_int(
                os.getenv("SOURCING_HOT_CACHE_HEALTHY_TTL_SECONDS"),
                default=_DEFAULT_HOT_CACHE_HEALTHY_TTL_SECONDS,
            )

    governance_interval_seconds = _coerce_positive_int(os.getenv("SOURCING_HOT_CACHE_GOVERNANCE_INTERVAL_SECONDS"))
    if governance_interval_seconds <= 0:
        if pressure_level == "severe":
            governance_interval_seconds = _coerce_positive_int(
                os.getenv("SOURCING_HOT_CACHE_SEVERE_GOVERNANCE_INTERVAL_SECONDS"),
                default=_DEFAULT_HOT_CACHE_SEVERE_INTERVAL_SECONDS,
            )
        elif pressure_level == "pressured":
            governance_interval_seconds = _coerce_positive_int(
                os.getenv("SOURCING_HOT_CACHE_PRESSURED_GOVERNANCE_INTERVAL_SECONDS"),
                default=_DEFAULT_HOT_CACHE_PRESSURED_INTERVAL_SECONDS,
            )
        else:
            governance_interval_seconds = _coerce_positive_int(
                os.getenv("SOURCING_HOT_CACHE_HEALTHY_GOVERNANCE_INTERVAL_SECONDS"),
                default=_DEFAULT_HOT_CACHE_GOVERNANCE_INTERVAL_SECONDS,
            )

    return {
        **configured_policy,
        "self_tuning_enabled": self_tuning_enabled,
        "pressure_level": pressure_level,
        "pressure_ratio": round(pressure_ratio, 4),
        "effective_ttl_seconds": max(0, int(effective_ttl_seconds or 0)),
        "governance_interval_seconds": max(1, int(governance_interval_seconds or 1)),
        "inventory_total_bytes": int(effective_inventory.get("total_bytes") or 0),
        "inventory_snapshot_count": len(list(effective_inventory.get("snapshot_records") or [])),
        "inventory_generation_count": len(list(effective_inventory.get("generation_records") or [])),
        "inventory_company_count": len(list(effective_inventory.get("company_records") or [])),
    }


def run_hot_cache_governance_cycle(
    *,
    runtime_dir: str | Path,
    store: Any | None = None,
    min_interval_seconds: float = 0.0,
    force: bool = False,
) -> dict[str, Any]:
    runtime_root = Path(runtime_dir).expanduser()
    state_path = hot_cache_governance_state_path(runtime_root)
    previous_state = load_hot_cache_governance_state(runtime_root)
    inventory_before = collect_hot_cache_inventory(runtime_root)
    policy = build_hot_cache_governance_policy(runtime_root, inventory=inventory_before)
    effective_min_interval_seconds = max(
        float(min_interval_seconds or 0.0),
        float(policy.get("governance_interval_seconds") or 0.0),
    )
    summary_base = {
        "runtime_dir": str(runtime_root),
        "state_path": str(state_path),
        "force": bool(force),
        "min_interval_seconds": effective_min_interval_seconds,
        "policy": policy,
    }
    now_epoch = time.time()
    last_started_epoch = _timestamp_to_epoch(str(previous_state.get("last_started_at") or ""))
    if (
        not force
        and effective_min_interval_seconds > 0
        and last_started_epoch > 0
        and (now_epoch - last_started_epoch) < effective_min_interval_seconds
    ):
        summary = {
            **summary_base,
            "status": "skipped",
            "reason": "throttled",
            "seconds_since_last_start": round(now_epoch - last_started_epoch, 3),
        }
        _write_hot_cache_governance_state(state_path, previous_state=previous_state, summary=summary)
        return summary

    started_at = _utc_now_iso()
    _write_hot_cache_governance_state(
        state_path,
        previous_state=previous_state,
        summary={**summary_base, "status": "running", "started_at": started_at},
    )
    try:
        from .candidate_artifacts import cleanup_candidate_artifact_hot_cache

        cleanup = cleanup_candidate_artifact_hot_cache(
            runtime_dir=runtime_root,
            store=store,
            dry_run=False,
            drop_compatibility_exports=True,
            ttl_seconds=int(policy.get("effective_ttl_seconds") or 0),
            size_budget_bytes=int(policy.get("effective_size_budget_bytes") or policy.get("size_budget_bytes") or 0),
            max_bytes_per_company=int(policy.get("max_bytes_per_company") or 0),
            keep_latest_snapshots_per_company=int(policy.get("keep_latest_snapshots_per_company") or 1),
            max_generations_per_scope=int(policy.get("max_generations_per_scope") or 0),
        )
    except Exception as exc:
        summary = {
            **summary_base,
            "status": "failed",
            "started_at": started_at,
            "error": f"{type(exc).__name__}: {exc}",
        }
        _write_hot_cache_governance_state(state_path, previous_state=previous_state, summary=summary)
        return summary

    inventory_after = collect_hot_cache_inventory(runtime_root)
    summary = {
        **summary_base,
        "status": "completed",
        "started_at": started_at,
        "cleanup": cleanup,
        "inventory_before": {
            "status": str(inventory_before.get("status") or ""),
            "total_bytes": int(inventory_before.get("total_bytes") or 0),
            "snapshot_count": len(list(inventory_before.get("snapshot_records") or [])),
            "generation_count": len(list(inventory_before.get("generation_records") or [])),
            "company_count": len(list(inventory_before.get("company_records") or [])),
        },
        "inventory_after": {
            "status": str(inventory_after.get("status") or ""),
            "total_bytes": int(inventory_after.get("total_bytes") or 0),
            "snapshot_count": len(list(inventory_after.get("snapshot_records") or [])),
            "generation_count": len(list(inventory_after.get("generation_records") or [])),
            "company_count": len(list(inventory_after.get("company_records") or [])),
        },
        "freed_total_bytes": max(
            0,
            int(inventory_before.get("total_bytes") or 0) - int(inventory_after.get("total_bytes") or 0),
        ),
    }
    _write_hot_cache_governance_state(state_path, previous_state=previous_state, summary=summary)
    return summary


def mark_hot_cache_snapshot_access(
    snapshot_dir: str | Path,
    *,
    runtime_dir: str | Path | None = None,
    at_epoch: float | None = None,
) -> dict[str, Any]:
    resolved_snapshot_dir = Path(snapshot_dir).expanduser()
    if not resolved_snapshot_dir.exists() or not resolved_snapshot_dir.is_dir():
        return {
            "status": "missing_snapshot_dir",
            "snapshot_dir": str(resolved_snapshot_dir),
            "access_marker_path": "",
        }
    if runtime_dir is not None:
        hot_cache_root = hot_cache_company_assets_dir(runtime_dir)
        if hot_cache_root is None:
            return {
                "status": "disabled",
                "snapshot_dir": str(resolved_snapshot_dir),
                "access_marker_path": "",
            }
        try:
            resolved_snapshot_dir.resolve().relative_to(Path(hot_cache_root).expanduser().resolve())
        except ValueError:
            return {
                "status": "not_hot_cache_snapshot",
                "snapshot_dir": str(resolved_snapshot_dir),
                "access_marker_path": "",
            }
    access_marker = resolved_snapshot_dir / _HOT_CACHE_ACCESS_MARKER
    access_marker.parent.mkdir(parents=True, exist_ok=True)
    access_epoch = float(at_epoch if at_epoch is not None else time.time())
    access_marker.touch(exist_ok=True)
    os.utime(access_marker, (access_epoch, access_epoch))
    return {
        "status": "updated",
        "snapshot_dir": str(resolved_snapshot_dir),
        "access_marker_path": str(access_marker),
        "access_epoch": access_epoch,
    }


def materialize_link_first_file(source: Path, destination: Path) -> str:
    resolved_source = Path(source).expanduser().resolve()
    resolved_destination = Path(destination).expanduser()
    resolved_destination.parent.mkdir(parents=True, exist_ok=True)
    _remove_existing_path(resolved_destination)
    if resolved_source.is_symlink():
        target = os.readlink(resolved_source)
        resolved_destination.symlink_to(target)
        return "symlink_passthrough"
    try:
        os.link(resolved_source, resolved_destination)
        return "hardlink"
    except OSError:
        pass
    try:
        relative_target = os.path.relpath(resolved_source, resolved_destination.parent.resolve())
        resolved_destination.symlink_to(relative_target)
        return "symlink"
    except OSError:
        shutil.copy2(resolved_source, resolved_destination)
        return "copy"


def mirror_tree_link_first(source_dir: str | Path, destination_dir: str | Path) -> dict[str, Any]:
    resolved_source = Path(source_dir).expanduser().resolve()
    resolved_destination = Path(destination_dir).expanduser()
    if not resolved_source.exists() or not resolved_source.is_dir():
        return {
            "status": "missing_source",
            "source_dir": str(resolved_source),
            "destination_dir": str(resolved_destination),
            "file_count": 0,
            "directory_count": 0,
            "mode_counts": {},
        }
    if resolved_source == resolved_destination.resolve():
        return {
            "status": "same_path",
            "source_dir": str(resolved_source),
            "destination_dir": str(resolved_destination),
            "file_count": 0,
            "directory_count": 0,
            "mode_counts": {},
        }
    if resolved_destination.exists():
        shutil.rmtree(resolved_destination)
    resolved_destination.mkdir(parents=True, exist_ok=True)
    file_count = 0
    directory_count = 0
    mode_counts: dict[str, int] = {}
    for directory in sorted(path for path in resolved_source.rglob("*") if path.is_dir()):
        relative_dir = directory.relative_to(resolved_source)
        (resolved_destination / relative_dir).mkdir(parents=True, exist_ok=True)
        directory_count += 1
    for file_path in sorted(path for path in resolved_source.rglob("*") if path.is_file() or path.is_symlink()):
        relative_path = file_path.relative_to(resolved_source)
        sync_mode = materialize_link_first_file(file_path, resolved_destination / relative_path)
        mode_counts[sync_mode] = int(mode_counts.get(sync_mode) or 0) + 1
        file_count += 1
    return {
        "status": "completed",
        "source_dir": str(resolved_source),
        "destination_dir": str(resolved_destination),
        "file_count": file_count,
        "directory_count": directory_count,
        "mode_counts": mode_counts,
    }


def collect_hot_cache_inventory(runtime_dir: str | Path) -> dict[str, Any]:
    runtime_root = Path(runtime_dir).expanduser()
    hot_cache_root = hot_cache_company_assets_dir(runtime_root)
    generation_root = runtime_root / "object_sync" / "generations"
    if hot_cache_root is None or not hot_cache_root.exists():
        return {
            "status": "disabled" if hot_cache_root is None else "missing_hot_cache_root",
            "runtime_dir": str(runtime_root),
            "hot_cache_root": str(hot_cache_root or ""),
            "generation_root": str(generation_root),
            "snapshot_records": [],
            "generation_records": [],
            "company_records": [],
            "total_snapshot_bytes": 0,
            "total_generation_bytes": 0,
            "total_bytes": 0,
        }

    snapshot_records: list[dict[str, Any]] = []
    for company_dir in sorted(path for path in hot_cache_root.iterdir() if path.is_dir()):
        latest_payload = load_company_snapshot_json(company_dir / "latest_snapshot.json")
        latest_snapshot_id = str(latest_payload.get("snapshot_id") or "").strip()
        for snapshot_dir in sorted(path for path in company_dir.iterdir() if path.is_dir()):
            snapshot_stats = _path_tree_stats(snapshot_dir)
            access_mtime_epoch = _hot_cache_access_epoch(
                snapshot_dir, fallback_epoch=float(snapshot_stats.get("latest_mtime_epoch") or 0.0)
            )
            identity_payload = load_company_snapshot_identity(snapshot_dir, fallback_payload=latest_payload)
            target_company = (
                str(identity_payload.get("canonical_name") or "").strip()
                or str(identity_payload.get("requested_name") or "").strip()
                or company_dir.name
            )
            view_records: list[dict[str, Any]] = []
            generation_keys: set[str] = set()
            max_generation_sequence = 0
            latest_generation_watermark = ""
            for asset_view, artifact_dir in _iter_hot_cache_view_dirs(snapshot_dir):
                manifest_payload = load_company_snapshot_json(artifact_dir / "manifest.json")
                artifact_summary = load_company_snapshot_json(artifact_dir / "artifact_summary.json")
                snapshot_manifest = load_company_snapshot_json(artifact_dir / "snapshot_manifest.json")
                view_stats = _path_tree_stats(artifact_dir)
                generation_key = str(
                    snapshot_manifest.get("materialization_generation_key")
                    or artifact_summary.get("materialization_generation_key")
                    or manifest_payload.get("materialization_generation_key")
                    or ""
                ).strip()
                generation_sequence = int(
                    snapshot_manifest.get("materialization_generation_sequence")
                    or artifact_summary.get("materialization_generation_sequence")
                    or manifest_payload.get("materialization_generation_sequence")
                    or 0
                )
                generation_watermark = str(
                    snapshot_manifest.get("materialization_watermark")
                    or artifact_summary.get("materialization_watermark")
                    or manifest_payload.get("materialization_watermark")
                    or ""
                ).strip()
                if generation_key:
                    generation_keys.add(generation_key)
                if generation_sequence >= max_generation_sequence:
                    max_generation_sequence = generation_sequence
                    latest_generation_watermark = generation_watermark
                view_records.append(
                    {
                        "asset_view": asset_view,
                        "artifact_dir": str(artifact_dir),
                        "file_count": int(view_stats.get("file_count") or 0),
                        "total_bytes": int(view_stats.get("total_bytes") or 0),
                        "latest_mtime_epoch": float(view_stats.get("latest_mtime_epoch") or 0.0),
                        "generation_key": generation_key,
                        "generation_sequence": generation_sequence,
                        "generation_watermark": generation_watermark,
                    }
                )
            snapshot_records.append(
                {
                    "kind": "snapshot",
                    "company_key": company_dir.name,
                    "target_company": target_company,
                    "snapshot_id": snapshot_dir.name,
                    "snapshot_dir": str(snapshot_dir),
                    "file_count": int(snapshot_stats.get("file_count") or 0),
                    "total_bytes": int(snapshot_stats.get("total_bytes") or 0),
                    "latest_mtime_epoch": float(snapshot_stats.get("latest_mtime_epoch") or 0.0),
                    "access_mtime_epoch": access_mtime_epoch,
                    "access_age_seconds": _age_seconds(access_mtime_epoch),
                    "age_seconds": _age_seconds(float(snapshot_stats.get("latest_mtime_epoch") or 0.0)),
                    "is_latest_snapshot": bool(latest_snapshot_id and latest_snapshot_id == snapshot_dir.name),
                    "asset_views": [
                        str(item.get("asset_view") or "")
                        for item in view_records
                        if str(item.get("asset_view") or "").strip()
                    ],
                    "view_records": view_records,
                    "generation_keys": sorted(generation_keys),
                    "generation_sequence_max": max_generation_sequence,
                    "generation_watermark": latest_generation_watermark,
                }
            )

    generation_records: list[dict[str, Any]] = []
    for generation_dir in (
        sorted(path for path in generation_root.iterdir() if path.is_dir()) if generation_root.exists() else []
    ):
        generation_manifest = load_company_snapshot_json(generation_dir / "generation_manifest.json")
        generation_stats = _path_tree_stats(generation_dir)
        generation_key = (
            str(generation_manifest.get("generation_key") or generation_dir.name).strip() or generation_dir.name
        )
        generation_records.append(
            {
                "kind": "generation_dir",
                "generation_key": generation_key,
                "company_key": str(generation_manifest.get("company_key") or "").strip(),
                "target_company": str(generation_manifest.get("target_company") or "").strip(),
                "snapshot_id": str(generation_manifest.get("snapshot_id") or "").strip(),
                "asset_view": str(generation_manifest.get("asset_view") or "").strip() or "canonical_merged",
                "generation_sequence": int(generation_manifest.get("generation_sequence") or 0),
                "generation_watermark": str(generation_manifest.get("generation_watermark") or "").strip(),
                "generation_dir": str(generation_dir),
                "file_count": int(generation_stats.get("file_count") or 0),
                "total_bytes": int(generation_stats.get("total_bytes") or 0),
                "latest_mtime_epoch": float(generation_stats.get("latest_mtime_epoch") or 0.0),
                "access_mtime_epoch": float(generation_stats.get("latest_mtime_epoch") or 0.0),
                "access_age_seconds": _age_seconds(float(generation_stats.get("latest_mtime_epoch") or 0.0)),
                "age_seconds": _age_seconds(float(generation_stats.get("latest_mtime_epoch") or 0.0)),
            }
        )

    total_snapshot_bytes = sum(int(item.get("total_bytes") or 0) for item in snapshot_records)
    total_generation_bytes = sum(int(item.get("total_bytes") or 0) for item in generation_records)
    company_records = _summarize_hot_cache_company_records(
        snapshot_records=snapshot_records,
        generation_records=generation_records,
    )
    return {
        "status": "completed",
        "runtime_dir": str(runtime_root),
        "hot_cache_root": str(hot_cache_root),
        "generation_root": str(generation_root),
        "snapshot_records": snapshot_records,
        "generation_records": generation_records,
        "company_records": company_records,
        "total_snapshot_bytes": total_snapshot_bytes,
        "total_generation_bytes": total_generation_bytes,
        "total_bytes": total_snapshot_bytes + total_generation_bytes,
    }


def plan_hot_cache_retention(
    runtime_dir: str | Path,
    *,
    ttl_seconds: int = 0,
    size_budget_bytes: int = 0,
    max_bytes_per_company: int = 0,
    keep_latest_snapshots_per_company: int = 1,
    max_generations_per_scope: int = 0,
    inventory: dict[str, Any] | None = None,
) -> dict[str, Any]:
    effective_inventory = dict(inventory or collect_hot_cache_inventory(runtime_dir))
    snapshot_records = [
        dict(item) for item in list(effective_inventory.get("snapshot_records") or []) if isinstance(item, dict)
    ]
    generation_records = [
        dict(item) for item in list(effective_inventory.get("generation_records") or []) if isinstance(item, dict)
    ]
    keep_count = max(1, int(keep_latest_snapshots_per_company or 1))
    protected_snapshot_paths = _protected_snapshot_paths(snapshot_records, keep_count=keep_count)
    protected_generation_keys = _protected_generation_keys(
        snapshot_records,
        generation_records,
        protected_snapshot_paths=protected_snapshot_paths,
    )
    eviction_reasons: dict[tuple[str, str], set[str]] = {}

    def _mark_eviction(kind: str, identifier: str, reason: str) -> None:
        eviction_reasons.setdefault((kind, identifier), set()).add(reason)

    if max_generations_per_scope > 0:
        generation_compaction = _generation_compaction_candidates(
            generation_records,
            protected_generation_keys=protected_generation_keys,
            max_generations_per_scope=max_generations_per_scope,
        )
        for record in generation_compaction:
            _mark_eviction("generation_dir", str(record.get("generation_dir") or ""), "compaction")

    if ttl_seconds > 0:
        for record in snapshot_records:
            snapshot_dir = str(record.get("snapshot_dir") or "")
            if snapshot_dir in protected_snapshot_paths:
                continue
            if float(record.get("age_seconds") or 0.0) > float(ttl_seconds):
                _mark_eviction("snapshot", snapshot_dir, "ttl")
        for record in generation_records:
            generation_key = str(record.get("generation_key") or "")
            if generation_key and generation_key in protected_generation_keys:
                continue
            if float(record.get("age_seconds") or 0.0) > float(ttl_seconds):
                _mark_eviction("generation_dir", str(record.get("generation_dir") or ""), "ttl")

    if max_bytes_per_company > 0:
        retained_company_bytes: dict[str, int] = {}
        company_budget_candidates: dict[str, list[dict[str, Any]]] = {}
        for record in snapshot_records:
            snapshot_dir = str(record.get("snapshot_dir") or "")
            company_bucket = _retention_company_bucket(record)
            if not snapshot_dir or not company_bucket:
                continue
            if ("snapshot", snapshot_dir) not in eviction_reasons:
                retained_company_bytes[company_bucket] = retained_company_bytes.get(company_bucket, 0) + int(
                    record.get("total_bytes") or 0
                )
            if snapshot_dir in protected_snapshot_paths:
                continue
            company_budget_candidates.setdefault(company_bucket, []).append(
                {
                    "kind": "snapshot",
                    "identifier": snapshot_dir,
                    "total_bytes": int(record.get("total_bytes") or 0),
                    "access_mtime_epoch": float(
                        record.get("access_mtime_epoch") or record.get("latest_mtime_epoch") or 0.0
                    ),
                    "latest_mtime_epoch": float(record.get("latest_mtime_epoch") or 0.0),
                }
            )
        for record in generation_records:
            generation_dir = str(record.get("generation_dir") or "")
            generation_key = str(record.get("generation_key") or "")
            company_bucket = _retention_company_bucket(record)
            if not generation_dir or not company_bucket:
                continue
            if ("generation_dir", generation_dir) not in eviction_reasons:
                retained_company_bytes[company_bucket] = retained_company_bytes.get(company_bucket, 0) + int(
                    record.get("total_bytes") or 0
                )
            if generation_key and generation_key in protected_generation_keys:
                continue
            company_budget_candidates.setdefault(company_bucket, []).append(
                {
                    "kind": "generation_dir",
                    "identifier": generation_dir,
                    "total_bytes": int(record.get("total_bytes") or 0),
                    "access_mtime_epoch": float(
                        record.get("access_mtime_epoch") or record.get("latest_mtime_epoch") or 0.0
                    ),
                    "latest_mtime_epoch": float(record.get("latest_mtime_epoch") or 0.0),
                }
            )
        for company_bucket, retained_total_bytes in retained_company_bytes.items():
            if retained_total_bytes <= max_bytes_per_company:
                continue
            candidates = sorted(
                list(company_budget_candidates.get(company_bucket) or []),
                key=lambda item: (
                    0 if str(item.get("kind") or "") == "generation_dir" else 1,
                    float(item.get("access_mtime_epoch") or 0.0),
                    float(item.get("latest_mtime_epoch") or 0.0),
                    -int(item.get("total_bytes") or 0),
                    str(item.get("identifier") or ""),
                ),
            )
            for candidate in candidates:
                if retained_total_bytes <= max_bytes_per_company:
                    break
                identifier = str(candidate.get("identifier") or "")
                if not identifier:
                    continue
                retained_total_bytes -= int(candidate.get("total_bytes") or 0)
                _mark_eviction(str(candidate.get("kind") or ""), identifier, "company_budget")

    retained_total_bytes = int(effective_inventory.get("total_bytes") or 0)
    retained_total_bytes -= sum(
        int(record.get("total_bytes") or 0)
        for record in snapshot_records
        if ("snapshot", str(record.get("snapshot_dir") or "")) in eviction_reasons
    )
    retained_total_bytes -= sum(
        int(record.get("total_bytes") or 0)
        for record in generation_records
        if ("generation_dir", str(record.get("generation_dir") or "")) in eviction_reasons
    )

    if size_budget_bytes > 0 and retained_total_bytes > size_budget_bytes:
        budget_candidates: list[dict[str, Any]] = []
        for record in generation_records:
            generation_dir = str(record.get("generation_dir") or "")
            generation_key = str(record.get("generation_key") or "")
            if generation_key and generation_key in protected_generation_keys:
                continue
            budget_candidates.append(
                {
                    "kind": "generation_dir",
                    "identifier": generation_dir,
                    "total_bytes": int(record.get("total_bytes") or 0),
                    "access_mtime_epoch": float(
                        record.get("access_mtime_epoch") or record.get("latest_mtime_epoch") or 0.0
                    ),
                    "latest_mtime_epoch": float(record.get("latest_mtime_epoch") or 0.0),
                }
            )
        for record in snapshot_records:
            snapshot_dir = str(record.get("snapshot_dir") or "")
            if snapshot_dir in protected_snapshot_paths:
                continue
            budget_candidates.append(
                {
                    "kind": "snapshot",
                    "identifier": snapshot_dir,
                    "total_bytes": int(record.get("total_bytes") or 0),
                    "access_mtime_epoch": float(
                        record.get("access_mtime_epoch") or record.get("latest_mtime_epoch") or 0.0
                    ),
                    "latest_mtime_epoch": float(record.get("latest_mtime_epoch") or 0.0),
                }
            )
        budget_candidates.sort(
            key=lambda item: (
                0 if str(item.get("kind") or "") == "generation_dir" else 1,
                float(item.get("access_mtime_epoch") or 0.0),
                float(item.get("latest_mtime_epoch") or 0.0),
                -int(item.get("total_bytes") or 0),
                str(item.get("identifier") or ""),
            )
        )
        for candidate in budget_candidates:
            if retained_total_bytes <= size_budget_bytes:
                break
            identifier = str(candidate.get("identifier") or "")
            if not identifier:
                continue
            _mark_eviction(str(candidate.get("kind") or ""), identifier, "size_budget")
            retained_total_bytes -= int(candidate.get("total_bytes") or 0)

    evictions: list[dict[str, Any]] = []
    for record in snapshot_records:
        snapshot_dir = str(record.get("snapshot_dir") or "")
        reasons = sorted(eviction_reasons.get(("snapshot", snapshot_dir), set()))
        if not reasons:
            continue
        evictions.append(
            {
                **record,
                "kind": "snapshot",
                "path": snapshot_dir,
                "eviction_reasons": reasons,
            }
        )
    for record in generation_records:
        generation_dir = str(record.get("generation_dir") or "")
        reasons = sorted(eviction_reasons.get(("generation_dir", generation_dir), set()))
        if not reasons:
            continue
        evictions.append(
            {
                **record,
                "kind": "generation_dir",
                "path": generation_dir,
                "eviction_reasons": reasons,
            }
        )
    evictions.sort(
        key=lambda item: (
            0 if str(item.get("kind") or "") == "generation_dir" else 1,
            float(item.get("latest_mtime_epoch") or 0.0),
            -int(item.get("total_bytes") or 0),
            str(item.get("path") or ""),
        )
    )
    return {
        "status": "planned",
        "runtime_dir": str(Path(runtime_dir).expanduser()),
        "inventory": effective_inventory,
        "ttl_seconds": max(0, int(ttl_seconds or 0)),
        "size_budget_bytes": max(0, int(size_budget_bytes or 0)),
        "max_bytes_per_company": max(0, int(max_bytes_per_company or 0)),
        "keep_latest_snapshots_per_company": keep_count,
        "max_generations_per_scope": max(0, int(max_generations_per_scope or 0)),
        "protected_snapshot_count": len(protected_snapshot_paths),
        "protected_generation_count": len(protected_generation_keys),
        "eviction_candidates": evictions,
        "planned_eviction_count": len(evictions),
        "planned_snapshot_eviction_count": sum(1 for item in evictions if str(item.get("kind") or "") == "snapshot"),
        "planned_generation_eviction_count": sum(
            1 for item in evictions if str(item.get("kind") or "") == "generation_dir"
        ),
        "planned_bytes_to_free": sum(int(item.get("total_bytes") or 0) for item in evictions),
        "estimated_retained_total_bytes": max(retained_total_bytes, 0),
    }


def apply_hot_cache_retention_plan(plan: dict[str, Any], *, dry_run: bool = True) -> dict[str, Any]:
    evictions = [dict(item) for item in list(plan.get("eviction_candidates") or []) if isinstance(item, dict)]
    deleted_paths: list[str] = []
    company_pointer_updates: list[dict[str, Any]] = []
    total_bytes_freed = 0
    for candidate in evictions:
        path = Path(str(candidate.get("path") or "")).expanduser()
        if not path.exists():
            continue
        if dry_run:
            total_bytes_freed += int(candidate.get("total_bytes") or 0)
            continue
        shutil.rmtree(path)
        deleted_paths.append(str(path))
        total_bytes_freed += int(candidate.get("total_bytes") or 0)
        if str(candidate.get("kind") or "") == "snapshot":
            company_pointer_updates.append(_repair_hot_cache_company_dir(path.parent))
    return {
        "status": "dry_run" if dry_run else "completed",
        "planned_eviction_count": len(evictions),
        "deleted_count": len(deleted_paths),
        "deleted_paths": deleted_paths,
        "planned_bytes_to_free": sum(int(item.get("total_bytes") or 0) for item in evictions),
        "freed_total_bytes": total_bytes_freed,
        "company_pointer_updates": company_pointer_updates,
    }


def _remove_existing_path(path: Path) -> None:
    if not path.exists() and not path.is_symlink():
        return
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
        return
    path.unlink(missing_ok=True)


def _coerce_positive_int(value: Any, *, default: int = 0) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return max(0, int(default))
    return max(0, parsed)


def _coerce_nonnegative_float(value: Any, *, default: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return max(0.0, float(default))
    return max(0.0, parsed)


def _coerce_env_bool(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    normalized = str(value).strip().lower()
    if not normalized:
        return default
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def _resolve_hot_cache_free_space_reserve(
    *,
    total_bytes: int,
    reserve_ratio: float,
    reserve_floor_bytes: int,
) -> int:
    if total_bytes <= 0:
        return max(0, int(reserve_floor_bytes))
    ratio_reserve_bytes = int(max(0.0, reserve_ratio) * total_bytes)
    capped_floor_bytes = min(max(0, int(reserve_floor_bytes)), max(total_bytes // 2, ratio_reserve_bytes))
    return max(ratio_reserve_bytes, capped_floor_bytes)


def _resolve_hot_cache_target_budget(
    *,
    disk_total_bytes: int,
    reserve_free_bytes: int,
    target_ratio: float,
    floor_bytes: int,
    cap_bytes: int,
) -> int:
    usable_ceiling_bytes = max(0, int(disk_total_bytes) - max(0, int(reserve_free_bytes)))
    if usable_ceiling_bytes <= 0:
        return 0
    ratio_budget_bytes = int(max(0.0, float(target_ratio)) * max(0, int(disk_total_bytes)))
    target_budget_bytes = max(ratio_budget_bytes, max(0, int(floor_bytes)))
    if cap_bytes > 0:
        target_budget_bytes = min(target_budget_bytes, int(cap_bytes))
    return min(target_budget_bytes, usable_ceiling_bytes)


def _hot_cache_access_epoch(snapshot_dir: Path, *, fallback_epoch: float) -> float:
    access_marker = snapshot_dir / _HOT_CACHE_ACCESS_MARKER
    if not access_marker.exists():
        return fallback_epoch
    try:
        stat_result = access_marker.stat()
    except OSError:
        return fallback_epoch
    return max(float(stat_result.st_mtime), fallback_epoch)


def _age_seconds(latest_mtime_epoch: float) -> float:
    if latest_mtime_epoch <= 0:
        return 0.0
    return max(0.0, round(time.time() - latest_mtime_epoch, 2))


def _iter_hot_cache_view_dirs(snapshot_dir: Path) -> list[tuple[str, Path]]:
    normalized_root = snapshot_dir / "normalized_artifacts"
    if not normalized_root.exists() or not normalized_root.is_dir():
        return []
    view_dirs: list[tuple[str, Path]] = []
    if (normalized_root / "manifest.json").exists():
        view_dirs.append(("canonical_merged", normalized_root))
    for child in sorted(normalized_root.iterdir()):
        if not child.is_dir():
            continue
        if child.name in {"candidate_shards", "pages", "backlogs"}:
            continue
        if (child / "manifest.json").exists():
            view_dirs.append((child.name, child))
    return view_dirs


def _path_tree_stats(root: Path) -> dict[str, Any]:
    if not root.exists():
        return {
            "file_count": 0,
            "total_bytes": 0,
            "latest_mtime_epoch": 0.0,
        }
    file_count = 0
    total_bytes = 0
    latest_mtime_epoch = 0.0
    for path in [root, *root.rglob("*")]:
        if not path.exists() and not path.is_symlink():
            continue
        if path.is_dir() and not path.is_symlink():
            continue
        try:
            stat_result = path.stat()
        except OSError:
            continue
        file_count += 1
        total_bytes += int(stat_result.st_size)
        latest_mtime_epoch = max(latest_mtime_epoch, float(stat_result.st_mtime))
    return {
        "file_count": file_count,
        "total_bytes": total_bytes,
        "latest_mtime_epoch": latest_mtime_epoch,
    }


def _retention_company_bucket(record: dict[str, Any]) -> str:
    normalized_company_key = str(record.get("company_key") or "").strip().lower()
    if normalized_company_key:
        return normalized_company_key
    return str(record.get("target_company") or "").strip().lower()


def _summarize_hot_cache_company_records(
    *,
    snapshot_records: list[dict[str, Any]],
    generation_records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    companies: dict[str, dict[str, Any]] = {}
    for record in snapshot_records:
        company_bucket = _retention_company_bucket(record)
        if not company_bucket:
            continue
        summary = companies.setdefault(
            company_bucket,
            {
                "company_key": str(record.get("company_key") or "").strip() or company_bucket,
                "target_company": str(record.get("target_company") or "").strip(),
                "snapshot_count": 0,
                "generation_count": 0,
                "total_bytes": 0,
                "snapshot_bytes": 0,
                "generation_bytes": 0,
                "latest_access_mtime_epoch": 0.0,
                "latest_snapshot_mtime_epoch": 0.0,
            },
        )
        summary["snapshot_count"] = int(summary.get("snapshot_count") or 0) + 1
        summary["total_bytes"] = int(summary.get("total_bytes") or 0) + int(record.get("total_bytes") or 0)
        summary["snapshot_bytes"] = int(summary.get("snapshot_bytes") or 0) + int(record.get("total_bytes") or 0)
        summary["latest_access_mtime_epoch"] = max(
            float(summary.get("latest_access_mtime_epoch") or 0.0),
            float(record.get("access_mtime_epoch") or 0.0),
        )
        summary["latest_snapshot_mtime_epoch"] = max(
            float(summary.get("latest_snapshot_mtime_epoch") or 0.0),
            float(record.get("latest_mtime_epoch") or 0.0),
        )
    for record in generation_records:
        company_bucket = _retention_company_bucket(record)
        if not company_bucket:
            continue
        summary = companies.setdefault(
            company_bucket,
            {
                "company_key": str(record.get("company_key") or "").strip() or company_bucket,
                "target_company": str(record.get("target_company") or "").strip(),
                "snapshot_count": 0,
                "generation_count": 0,
                "total_bytes": 0,
                "snapshot_bytes": 0,
                "generation_bytes": 0,
                "latest_access_mtime_epoch": 0.0,
                "latest_snapshot_mtime_epoch": 0.0,
            },
        )
        summary["generation_count"] = int(summary.get("generation_count") or 0) + 1
        summary["total_bytes"] = int(summary.get("total_bytes") or 0) + int(record.get("total_bytes") or 0)
        summary["generation_bytes"] = int(summary.get("generation_bytes") or 0) + int(record.get("total_bytes") or 0)
        summary["latest_access_mtime_epoch"] = max(
            float(summary.get("latest_access_mtime_epoch") or 0.0),
            float(record.get("access_mtime_epoch") or record.get("latest_mtime_epoch") or 0.0),
        )
        summary["latest_snapshot_mtime_epoch"] = max(
            float(summary.get("latest_snapshot_mtime_epoch") or 0.0),
            float(record.get("latest_mtime_epoch") or 0.0),
        )
    company_records = sorted(
        companies.values(),
        key=lambda item: (-int(item.get("total_bytes") or 0), str(item.get("company_key") or "")),
    )
    for record in company_records:
        record["access_age_seconds"] = _age_seconds(float(record.get("latest_access_mtime_epoch") or 0.0))
        record["age_seconds"] = _age_seconds(float(record.get("latest_snapshot_mtime_epoch") or 0.0))
    return company_records


def _protected_snapshot_paths(snapshot_records: list[dict[str, Any]], *, keep_count: int) -> set[str]:
    protected: set[str] = set()
    by_company: dict[str, list[dict[str, Any]]] = {}
    for record in snapshot_records:
        company_key = str(record.get("company_key") or "").strip()
        if not company_key:
            continue
        by_company.setdefault(company_key, []).append(record)
    for records in by_company.values():
        sorted_records = sorted(
            records,
            key=lambda item: (
                1 if bool(item.get("is_latest_snapshot")) else 0,
                float(item.get("latest_mtime_epoch") or 0.0),
                int(item.get("generation_sequence_max") or 0),
                str(item.get("snapshot_id") or ""),
            ),
            reverse=True,
        )
        for record in sorted_records[:keep_count]:
            snapshot_dir = str(record.get("snapshot_dir") or "")
            if snapshot_dir:
                protected.add(snapshot_dir)
        for record in sorted_records:
            if bool(record.get("is_latest_snapshot")):
                snapshot_dir = str(record.get("snapshot_dir") or "")
                if snapshot_dir:
                    protected.add(snapshot_dir)
    return protected


def _protected_generation_keys(
    snapshot_records: list[dict[str, Any]],
    generation_records: list[dict[str, Any]],
    *,
    protected_snapshot_paths: set[str],
) -> set[str]:
    protected = {
        str(generation_key).strip()
        for record in snapshot_records
        if str(record.get("snapshot_dir") or "") in protected_snapshot_paths
        for generation_key in list(record.get("generation_keys") or [])
        if str(generation_key).strip()
    }
    snapshot_scopes = {
        (
            str(record.get("company_key") or "").strip(),
            str(record.get("snapshot_id") or "").strip(),
            str(view_record.get("asset_view") or "").strip() or "canonical_merged",
        )
        for record in snapshot_records
        for view_record in list(record.get("view_records") or [])
        if isinstance(view_record, dict)
    }
    latest_by_scope: dict[tuple[str, str, str], tuple[int, float, str]] = {}
    for record in generation_records:
        generation_key = str(record.get("generation_key") or "").strip()
        if not generation_key:
            continue
        scope = (
            str(record.get("company_key") or "").strip(),
            str(record.get("snapshot_id") or "").strip(),
            str(record.get("asset_view") or "").strip(),
        )
        if scope in snapshot_scopes:
            continue
        rank = (
            int(record.get("generation_sequence") or 0),
            float(record.get("latest_mtime_epoch") or 0.0),
            generation_key,
        )
        current = latest_by_scope.get(scope)
        if current is None or rank > current:
            latest_by_scope[scope] = rank
    protected.update(item[2] for item in latest_by_scope.values() if item[2])
    return protected


def _generation_compaction_candidates(
    generation_records: list[dict[str, Any]],
    *,
    protected_generation_keys: set[str],
    max_generations_per_scope: int,
) -> list[dict[str, Any]]:
    keep_count = max(0, int(max_generations_per_scope or 0))
    if keep_count <= 0:
        return []
    records_by_scope: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for record in generation_records:
        company_key = str(record.get("company_key") or "").strip()
        snapshot_id = str(record.get("snapshot_id") or "").strip()
        asset_view = str(record.get("asset_view") or "").strip()
        generation_key = str(record.get("generation_key") or "").strip()
        if not company_key or not snapshot_id or not asset_view or not generation_key:
            continue
        records_by_scope.setdefault((company_key, snapshot_id, asset_view), []).append(record)
    evictions: list[dict[str, Any]] = []
    for records in records_by_scope.values():
        ranked_records = sorted(
            records,
            key=lambda item: (
                1 if str(item.get("generation_key") or "").strip() in protected_generation_keys else 0,
                int(item.get("generation_sequence") or 0),
                float(item.get("access_mtime_epoch") or item.get("latest_mtime_epoch") or 0.0),
                float(item.get("latest_mtime_epoch") or 0.0),
                str(item.get("generation_key") or ""),
            ),
            reverse=True,
        )
        retained_generation_keys = {
            str(item.get("generation_key") or "").strip() for item in ranked_records[:keep_count] if item
        }
        retained_generation_keys.update(protected_generation_keys)
        for record in ranked_records:
            generation_key = str(record.get("generation_key") or "").strip()
            if generation_key in retained_generation_keys:
                continue
            evictions.append(record)
    return evictions


def _repair_hot_cache_company_dir(company_dir: Path) -> dict[str, Any]:
    latest_snapshot_path = company_dir / "latest_snapshot.json"
    remaining_snapshots = (
        sorted(path for path in company_dir.iterdir() if path.is_dir()) if company_dir.exists() else []
    )
    if not remaining_snapshots:
        latest_snapshot_path.unlink(missing_ok=True)
        try:
            company_dir.rmdir()
        except OSError:
            pass
        return {
            "company_dir": str(company_dir),
            "status": "removed_empty_company_dir",
            "snapshot_id": "",
        }
    selected_snapshot = remaining_snapshots[-1]
    identity_payload = load_company_snapshot_identity(selected_snapshot, fallback_payload={})
    latest_payload = {
        "snapshot_id": selected_snapshot.name,
        "snapshot_dir": str(selected_snapshot),
        "company_identity": identity_payload,
    }
    latest_snapshot_path.write_text(json.dumps(latest_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "company_dir": str(company_dir),
        "status": "updated_latest_snapshot",
        "snapshot_id": selected_snapshot.name,
    }


def _write_hot_cache_governance_state(
    path: Path,
    *,
    previous_state: dict[str, Any],
    summary: dict[str, Any],
) -> None:
    resolved_path = Path(path).expanduser()
    resolved_path.parent.mkdir(parents=True, exist_ok=True)
    completed_at = _utc_now_iso()
    status = str(summary.get("status") or "")
    payload = {
        "status": status,
        "runtime_dir": str(summary.get("runtime_dir") or previous_state.get("runtime_dir") or ""),
        "state_path": str(summary.get("state_path") or resolved_path),
        "last_checked_at": completed_at,
        "last_started_at": str(summary.get("started_at") or previous_state.get("last_started_at") or ""),
        "last_completed_at": completed_at,
        "last_error": str(summary.get("error") or ""),
        "last_summary": summary,
        "last_success_summary": (
            dict(summary) if status == "completed" else dict(previous_state.get("last_success_summary") or {})
        ),
    }
    resolved_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _timestamp_to_epoch(value: str) -> float:
    normalized = str(value or "").strip()
    if not normalized:
        return 0.0
    try:
        return datetime.fromisoformat(normalized.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0.0


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
