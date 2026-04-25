from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .asset_reuse_planning import build_acquisition_shard_registry_record
from .company_registry import resolve_company_alias_key
from .local_postgres import resolve_default_control_plane_db_path
from .organization_execution_profile import ensure_organization_execution_profile
from .storage import SQLiteStore


def _write_candidate_documents(
    *,
    runtime_dir: str | Path,
    target_company: str,
    snapshot_id: str,
    claimed_candidate_count: int,
    current_count: int,
    former_count: int,
    candidate_specs: list[dict[str, str]],
) -> Path:
    runtime_root = Path(runtime_dir)
    company_key = resolve_company_alias_key(target_company)
    snapshot_dir = runtime_root / "company_assets" / company_key / snapshot_id
    normalized_dir = snapshot_dir / "normalized_artifacts"
    normalized_dir.mkdir(parents=True, exist_ok=True)
    identity = {
        "requested_name": target_company,
        "canonical_name": target_company,
        "company_key": company_key,
        "linkedin_slug": company_key,
        "aliases": [target_company],
    }
    candidates: list[dict[str, object]] = []
    for index, spec in enumerate(candidate_specs, start=1):
        employment_status = str(spec.get("employment_status") or "current")
        slug = str(spec.get("slug") or f"{company_key}-{index}")
        focus_text = str(spec.get("focus_text") or "")
        role = str(spec.get("role") or "Research Engineer")
        candidates.append(
            {
                "candidate_id": f"{company_key}-{index}",
                "name_en": str(spec.get("name") or f"{target_company} Candidate {index}"),
                "display_name": str(spec.get("name") or f"{target_company} Candidate {index}"),
                "category": "employee",
                "target_company": target_company,
                "organization": target_company,
                "employment_status": employment_status,
                "role": role,
                "focus_areas": focus_text,
                "work_history": f"{target_company} | {focus_text}",
                "notes": focus_text,
                "linkedin_url": f"https://www.linkedin.com/in/{slug}/",
                "source_dataset": f"{company_key}_snapshot",
                "metadata": {
                    "profile_url": f"https://www.linkedin.com/in/{slug}/",
                    "public_identifier": slug,
                },
            }
        )
    candidate_payload = {
        "snapshot": {
            "snapshot_id": snapshot_id,
            "company_identity": identity,
        },
        "target_company": target_company,
        "snapshot_id": snapshot_id,
        "candidates": candidates,
        "evidence": [],
        "candidate_count": claimed_candidate_count,
        "evidence_count": 0,
    }
    artifact_summary = {
        "snapshot_id": snapshot_id,
        "target_company": target_company,
        "candidate_count": claimed_candidate_count,
        "evidence_count": 0,
        "profile_detail_count": claimed_candidate_count,
        "missing_linkedin_count": 0,
        "current_lane_coverage": {
            "effective_candidate_count": current_count,
            "effective_ready": current_count > 0,
        },
        "former_lane_coverage": {
            "effective_candidate_count": former_count,
            "effective_ready": former_count > 0,
        },
    }
    (snapshot_dir / "identity.json").write_text(json.dumps(identity, ensure_ascii=False, indent=2), encoding="utf-8")
    (snapshot_dir.parent / "latest_snapshot.json").write_text(
        json.dumps({"snapshot_id": snapshot_id, "company_identity": identity}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (snapshot_dir / "candidate_documents.json").write_text(
        json.dumps(candidate_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (normalized_dir / "materialized_candidate_documents.json").write_text(
        json.dumps(candidate_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (normalized_dir / "artifact_summary.json").write_text(
        json.dumps(artifact_summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return snapshot_dir


def _seed_authoritative_company_asset(
    *,
    runtime_dir: str | Path,
    store: SQLiteStore,
    target_company: str,
    snapshot_id: str,
    current_count: int,
    former_count: int,
    candidate_specs: list[dict[str, str]],
) -> dict[str, Any]:
    claimed_candidate_count = max(current_count + former_count, len(candidate_specs))
    snapshot_dir = _write_candidate_documents(
        runtime_dir=runtime_dir,
        target_company=target_company,
        snapshot_id=snapshot_id,
        claimed_candidate_count=claimed_candidate_count,
        current_count=current_count,
        former_count=former_count,
        candidate_specs=candidate_specs,
    )
    company_key = resolve_company_alias_key(target_company)
    summary_payload = {
        "target_company": target_company,
        "snapshot_id": snapshot_id,
        "candidate_count": claimed_candidate_count,
        "profile_detail_count": claimed_candidate_count,
        "current_lane_coverage": {
            "effective_candidate_count": current_count,
            "effective_ready": current_count > 0,
        },
        "former_lane_coverage": {
            "effective_candidate_count": former_count,
            "effective_ready": former_count > 0,
        },
    }
    persisted = store.upsert_organization_asset_registry(
        {
            "target_company": target_company,
            "company_key": company_key,
            "snapshot_id": snapshot_id,
            "asset_view": "canonical_merged",
            "status": "ready",
            "authoritative": True,
            "candidate_count": claimed_candidate_count,
            "evidence_count": 0,
            "profile_detail_count": claimed_candidate_count,
            "explicit_profile_capture_count": claimed_candidate_count,
            "missing_linkedin_count": 0,
            "manual_review_backlog_count": 0,
            "profile_completion_backlog_count": 0,
            "source_snapshot_count": 1,
            "completeness_score": 90.0,
            "completeness_band": "high",
            "current_lane_coverage": summary_payload["current_lane_coverage"],
            "former_lane_coverage": summary_payload["former_lane_coverage"],
            "current_lane_effective_candidate_count": current_count,
            "former_lane_effective_candidate_count": former_count,
            "current_lane_effective_ready": current_count > 0,
            "former_lane_effective_ready": former_count > 0,
            "selected_snapshot_ids": [snapshot_id],
            "source_snapshot_selection": {"selected_snapshot_ids": [snapshot_id]},
            "source_path": str(snapshot_dir / "normalized_artifacts" / "artifact_summary.json"),
            "source_job_id": "",
            "summary": summary_payload,
        },
        authoritative=True,
    )
    ensure_organization_execution_profile(
        runtime_dir=runtime_dir,
        store=store,
        target_company=target_company,
        asset_view="canonical_merged",
    )
    return {
        "target_company": target_company,
        "company_key": company_key,
        "snapshot_id": snapshot_id,
        "snapshot_dir": str(snapshot_dir),
        "candidate_count": claimed_candidate_count,
        "registry_id": int(persisted.get("registry_id") or 0),
    }


def _register_materialization_generation(
    *,
    runtime_dir: str | Path,
    store: SQLiteStore,
    target_company: str,
    snapshot_id: str,
    artifact_kind: str,
    artifact_key: str,
    candidate_specs: list[dict[str, str]],
    lane: str = "",
    employment_scope: str = "",
) -> dict[str, Any]:
    company_key = resolve_company_alias_key(target_company)
    members = []
    for index, spec in enumerate(candidate_specs, start=1):
        slug = str(spec.get("slug") or f"{company_key}-{artifact_key}-{index}")
        linkedin_url = f"https://www.linkedin.com/in/{slug}/"
        members.append(
            {
                "target_company": target_company,
                "snapshot_id": snapshot_id,
                "asset_view": "canonical_merged",
                "artifact_kind": artifact_kind,
                "artifact_key": artifact_key,
                "lane": lane,
                "employment_scope": str(spec.get("employment_status") or employment_scope or "current"),
                "member_key": store.normalize_linkedin_profile_url(linkedin_url),
                "member_key_kind": "profile_url_key",
                "candidate_id": f"{company_key}-{artifact_key}-{index}",
                "profile_url_key": store.normalize_linkedin_profile_url(linkedin_url),
                "metadata": {
                    "display_name": str(spec.get("name") or f"{target_company} Candidate {index}"),
                    "role": str(spec.get("role") or ""),
                },
            }
        )
    return store.register_asset_materialization(
        target_company=target_company,
        snapshot_id=snapshot_id,
        asset_view="canonical_merged",
        artifact_kind=artifact_kind,
        artifact_key=artifact_key,
        source_path=str(
            Path(runtime_dir)
            / "company_assets"
            / company_key
            / snapshot_id
            / "normalized_artifacts"
            / "artifact_summary.json"
        ),
        summary={
            "target_company": target_company,
            "snapshot_id": snapshot_id,
            "candidate_count": len(candidate_specs),
        },
        metadata={"seed_source": "smoke_runtime_seed"},
        members=members,
    )


def _append_selected_snapshots(
    *,
    runtime_dir: str | Path,
    store: SQLiteStore,
    target_company: str,
    snapshot_ids: list[str],
) -> dict[str, Any]:
    row = store.get_authoritative_organization_asset_registry(target_company=target_company)
    selected_snapshot_ids = list(row.get("selected_snapshot_ids") or [])
    for snapshot_id in snapshot_ids:
        if snapshot_id and snapshot_id not in selected_snapshot_ids:
            selected_snapshot_ids.append(snapshot_id)
    row["selected_snapshot_ids"] = selected_snapshot_ids
    row["source_snapshot_selection"] = {"selected_snapshot_ids": selected_snapshot_ids}
    row["source_snapshot_count"] = len(selected_snapshot_ids)
    summary = dict(row.get("summary") or {})
    summary["selected_snapshot_ids"] = selected_snapshot_ids
    row["summary"] = summary
    persisted = store.upsert_organization_asset_registry(row, authoritative=True)
    ensure_organization_execution_profile(
        runtime_dir=runtime_dir,
        store=store,
        target_company=target_company,
        asset_view="canonical_merged",
    )
    return persisted


def seed_reference_smoke_runtime(
    *,
    runtime_dir: str | Path,
    store: SQLiteStore | None = None,
) -> dict[str, Any]:
    runtime_root = Path(runtime_dir)
    runtime_root.mkdir(parents=True, exist_ok=True)
    effective_store = store or SQLiteStore(
        resolve_default_control_plane_db_path(runtime_root, base_dir=runtime_root)
    )

    seeded_assets = [
        _seed_authoritative_company_asset(
            runtime_dir=runtime_root,
            store=effective_store,
            target_company="Skild AI",
            snapshot_id="20260414T120000",
            current_count=153,
            former_count=22,
            candidate_specs=[
                {
                    "name": "Skild Pretrain",
                    "slug": "skild-pretrain",
                    "role": "Pre-train Engineer",
                    "focus_text": "Pre-train systems, foundation models, model training infrastructure.",
                },
                {
                    "name": "Skild Systems",
                    "slug": "skild-systems",
                    "role": "Training Systems Engineer",
                    "focus_text": "Large-scale model training, distributed systems, pretraining pipelines.",
                },
            ],
        ),
        _seed_authoritative_company_asset(
            runtime_dir=runtime_root,
            store=effective_store,
            target_company="Humans&",
            snapshot_id="20260414T120100",
            current_count=520,
            former_count=80,
            candidate_specs=[
                {
                    "name": "Humans Coding",
                    "slug": "humans-coding",
                    "role": "Research Scientist",
                    "focus_text": "Coding agents, tool use, agent reliability, evaluation.",
                },
                {
                    "name": "Humans Infra",
                    "slug": "humans-infra",
                    "role": "Member of Technical Staff",
                    "focus_text": "Coding systems, agent platform, infrastructure for coding models.",
                },
            ],
        ),
        _seed_authoritative_company_asset(
            runtime_dir=runtime_root,
            store=effective_store,
            target_company="Anthropic",
            snapshot_id="20260414T120200",
            current_count=880,
            former_count=140,
            candidate_specs=[
                {
                    "name": "Anthropic Pretraining",
                    "slug": "anthropic-pretraining",
                    "role": "Research Scientist",
                    "focus_text": "Pre-training, scaling laws, language model training.",
                },
                {
                    "name": "Anthropic Safety Pretrain",
                    "slug": "anthropic-safety-pretrain",
                    "role": "Research Engineer",
                    "focus_text": "Pre-training systems, safety-aware model training, large-scale experimentation.",
                },
            ],
        ),
        _seed_authoritative_company_asset(
            runtime_dir=runtime_root,
            store=effective_store,
            target_company="OpenAI",
            snapshot_id="20260414T120300",
            current_count=1400,
            former_count=220,
            candidate_specs=[
                {
                    "name": "OpenAI Reasoning",
                    "slug": "openai-reasoning",
                    "role": "Research Scientist",
                    "focus_text": "Reasoning models, chain-of-thought, o-series model development.",
                },
                {
                    "name": "OpenAI Reasoning Engineer",
                    "slug": "openai-reasoning-eng",
                    "role": "Software Engineer",
                    "focus_text": "Reasoning systems, inference optimization, model evaluation.",
                },
            ],
        ),
        _seed_authoritative_company_asset(
            runtime_dir=runtime_root,
            store=effective_store,
            target_company="Google",
            snapshot_id="20260414T120400",
            current_count=5200,
            former_count=800,
            candidate_specs=[
                {
                    "name": "Google Veo",
                    "slug": "google-veo",
                    "role": "Research Engineer",
                    "focus_text": "Veo, multimodal generation, video generation, pre-train systems at Google DeepMind.",
                },
                {
                    "name": "Google Nano Banana",
                    "slug": "google-nano-banana",
                    "role": "Research Scientist",
                    "focus_text": "Nano Banana, multimodal generation, vision-language, pre-train workflows at Google DeepMind.",
                },
                {
                    "name": "Google Former Multimodal",
                    "slug": "google-former-multimodal",
                    "role": "Former Staff Researcher",
                    "employment_status": "former",
                    "focus_text": "Former Google multimodal and pre-train researcher from Veo-related work.",
                },
            ],
        ),
    ]

    openai_reasoning_snapshot_id = "20260414T120301"
    openai_current_specs = [
        {
            "name": "OpenAI Reasoning Current",
            "slug": "openai-reasoning-current",
            "role": "Research Scientist",
            "focus_text": "Reasoning models, chain-of-thought, inference research.",
        }
    ]
    openai_former_specs = [
        {
            "name": "OpenAI Reasoning Former",
            "slug": "openai-reasoning-former",
            "role": "Former Research Scientist",
            "employment_status": "former",
            "focus_text": "Former OpenAI reasoning and evaluation researcher.",
        }
    ]
    _write_candidate_documents(
        runtime_dir=runtime_root,
        target_company="OpenAI",
        snapshot_id=openai_reasoning_snapshot_id,
        claimed_candidate_count=2,
        current_count=1,
        former_count=1,
        candidate_specs=[*openai_current_specs, *openai_former_specs],
    )
    openai_current_row = build_acquisition_shard_registry_record(
        target_company="OpenAI",
        company_key="openai",
        snapshot_id=openai_reasoning_snapshot_id,
        lane="profile_search",
        employment_scope="current",
        strategy_type="scoped_search_roster",
        shard_id="Reasoning-current",
        shard_title="Reasoning",
        search_query="Reasoning",
        company_filters={
            "companies": ["https://www.linkedin.com/company/openai/"],
            "function_ids": ["24", "8"],
            "search_query": "Reasoning",
        },
        result_count=1,
        estimated_total_count=1,
        status="completed",
    )
    openai_current_generation = _register_materialization_generation(
        runtime_dir=runtime_root,
        store=effective_store,
        target_company="OpenAI",
        snapshot_id=openai_reasoning_snapshot_id,
        artifact_kind="acquisition_shard_bundle",
        artifact_key=str(openai_current_row.get("shard_key") or "reasoning-current"),
        candidate_specs=openai_current_specs,
        lane="profile_search",
        employment_scope="current",
    )
    openai_current_row["materialization_generation_key"] = str(openai_current_generation.get("generation_key") or "")
    openai_current_row["materialization_generation_sequence"] = int(
        openai_current_generation.get("generation_sequence") or 0
    )
    openai_current_row["materialization_watermark"] = str(openai_current_generation.get("generation_watermark") or "")
    effective_store.upsert_acquisition_shard_registry(openai_current_row)

    openai_former_row = build_acquisition_shard_registry_record(
        target_company="OpenAI",
        company_key="openai",
        snapshot_id=openai_reasoning_snapshot_id,
        lane="profile_search",
        employment_scope="former",
        strategy_type="former_employee_search",
        shard_id="Reasoning-former",
        shard_title="Reasoning",
        search_query="Reasoning",
        company_filters={
            "companies": ["https://www.linkedin.com/company/openai/"],
            "function_ids": ["24", "8"],
            "search_query": "Reasoning",
        },
        result_count=1,
        estimated_total_count=1,
        status="completed",
    )
    openai_former_generation = _register_materialization_generation(
        runtime_dir=runtime_root,
        store=effective_store,
        target_company="OpenAI",
        snapshot_id=openai_reasoning_snapshot_id,
        artifact_kind="acquisition_shard_bundle",
        artifact_key=str(openai_former_row.get("shard_key") or "reasoning-former"),
        candidate_specs=openai_former_specs,
        lane="profile_search",
        employment_scope="former",
    )
    openai_former_row["materialization_generation_key"] = str(openai_former_generation.get("generation_key") or "")
    openai_former_row["materialization_generation_sequence"] = int(
        openai_former_generation.get("generation_sequence") or 0
    )
    openai_former_row["materialization_watermark"] = str(openai_former_generation.get("generation_watermark") or "")
    effective_store.upsert_acquisition_shard_registry(openai_former_row)
    updated_openai_row = _append_selected_snapshots(
        runtime_dir=runtime_root,
        store=effective_store,
        target_company="OpenAI",
        snapshot_ids=[openai_reasoning_snapshot_id],
    )

    return {
        "status": "seeded",
        "runtime_dir": str(runtime_root),
        "db_path": str(
            getattr(
                effective_store,
                "sqlite_shadow_connect_target",
                lambda: resolve_default_control_plane_db_path(runtime_root, base_dir=runtime_root),
            )()
        ),
        "authoritative_assets": seeded_assets,
        "openai_reasoning": {
            "snapshot_id": openai_reasoning_snapshot_id,
            "selected_snapshot_ids": list(updated_openai_row.get("selected_snapshot_ids") or []),
            "current_generation_key": str(openai_current_generation.get("generation_key") or ""),
            "former_generation_key": str(openai_former_generation.get("generation_key") or ""),
        },
    }
