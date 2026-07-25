from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from typing import Any

from .asset_reuse_planning import build_acquisition_shard_registry_record
from .company_registry import resolve_company_alias_key
from .linkedin_url_normalization import normalize_linkedin_profile_url_key
from .local_postgres import resolve_default_control_plane_db_path
from .organization_execution_profile import ensure_organization_execution_profile
from .storage import ControlPlaneStore

_GOOGLE_LARGE_BASELINE_SOURCE_SNAPSHOT_ID = "20260428T011339"
_GOOGLE_LARGE_BASELINE_SEEDED_SNAPSHOT_ID = "20260511T000000"
_GOOGLE_LARGE_BASELINE_MAX_CANDIDATES = 5000
_GOOGLE_LARGE_BASELINE_DEFAULT_EXCLUDE_TERMS = ("vision-language",)


def _build_openai_baseline_candidate_specs() -> list[dict[str, str]]:
    specs: list[dict[str, str]] = [
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
    ]
    focus_cycle = [
        "Reasoning models, evaluation harnesses, and model behavior research.",
        "Multimodal model systems, data pipelines, and evaluation workflows.",
        "Coding model reliability, tool-use evaluation, and developer workflows.",
        "Training infrastructure, inference systems, and frontier-model operations.",
        "Post-training, alignment evaluation, and applied model quality.",
    ]
    for index in range(3, 301):
        employment_status = "former" if index > 260 else "current"
        specs.append(
            {
                "name": f"OpenAI Baseline {index:03d}",
                "slug": f"openai-baseline-{index:03d}",
                "role": "Former Research Engineer" if employment_status == "former" else "Research Engineer",
                "employment_status": employment_status,
                "focus_text": focus_cycle[index % len(focus_cycle)],
            }
        )
    return specs


def _build_google_baseline_candidate_specs() -> list[dict[str, str]]:
    specs: list[dict[str, str]] = [
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
    ]
    focus_cycle = [
        "Gemini model quality, evaluation systems, and product launch workflows.",
        "Veo and video generation model systems at Google DeepMind.",
        "Nano Banana multimodal generation, vision-language tooling, and AI product research.",
        "Google AI infrastructure, data pipelines, and production model serving.",
        "Search, ranking, model safety, and AI platform reliability.",
    ]
    for index in range(4, 301):
        employment_status = "former" if index > 261 else "current"
        specs.append(
            {
                "name": f"Google Baseline {index:03d}",
                "slug": f"google-baseline-{index:03d}",
                "role": "Former Research Engineer" if employment_status == "former" else "Research Engineer",
                "employment_status": employment_status,
                "focus_text": focus_cycle[index % len(focus_cycle)],
            }
        )
    return specs


def _build_meta_baseline_candidate_specs() -> list[dict[str, str]]:
    specs: list[dict[str, str]] = [
        {
            "name": "Meta Agent Systems",
            "slug": "meta-agent-systems",
            "role": "Research Engineer",
            "focus_text": "Agent systems, tool use, Meta Superintelligence, and AI product workflows.",
        },
        {
            "name": "Meta Agent Research",
            "slug": "meta-agent-research",
            "role": "Research Scientist",
            "focus_text": "Agents, planning, reasoning, and Llama post-training at Meta.",
        },
        {
            "name": "Meta Multimodal Agent",
            "slug": "meta-multimodal-agent",
            "role": "Staff Research Engineer",
            "focus_text": "Multimodal agents, vision-language systems, and generative AI products at Meta.",
        },
    ]
    focus_cycle = [
        "Agentic AI systems, tool-use workflows, and assistant product infrastructure.",
        "Llama model post-training, reasoning, and evaluation workflows.",
        "Multimodal model systems across image, video, and assistant experiences.",
        "AI infrastructure, ranking, retrieval, and large-scale serving.",
        "Audio, speech, translation, and on-device AI research.",
    ]
    for index in range(4, 1728):
        employment_status = "former" if index > 1500 else "current"
        specs.append(
            {
                "name": f"Meta Candidate {index:04d}",
                "slug": f"meta-candidate-{index:04d}",
                "role": "Former Research Engineer" if employment_status == "former" else "Research Engineer",
                "employment_status": employment_status,
                "focus_text": focus_cycle[index % len(focus_cycle)],
                "previous_company": "Facebook AI Research" if employment_status == "former" else "Google DeepMind",
            }
        )
    return specs


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
        school = str(spec.get("school") or "Stanford University")
        degree = str(spec.get("degree") or "MS")
        field_of_study = str(spec.get("field_of_study") or "Computer Science")
        previous_company = str(spec.get("previous_company") or "Google DeepMind")
        previous_role = str(spec.get("previous_role") or "Research Engineer")
        education_line = f"{degree}, {school}, {field_of_study}"
        experience_lines = [
            f"{target_company}, {role}",
            f"{previous_company}, {previous_role}",
        ]
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
                "headline": f"{role} at {target_company}",
                "focus_areas": focus_text,
                "education": education_line,
                "work_history": " / ".join(experience_lines),
                "experience_lines": experience_lines,
                "education_lines": [education_line],
                "notes": focus_text,
                "linkedin_url": f"https://www.linkedin.com/in/{slug}/",
                "media_url": f"https://cdn.example.com/{slug}.jpg",
                "has_profile_detail": True,
                "needs_profile_completion": False,
                "profile_capture_kind": "seeded_reference_profile",
                "source_dataset": f"{company_key}_snapshot",
                "metadata": {
                    "profile_url": f"https://www.linkedin.com/in/{slug}/",
                    "public_identifier": slug,
                    "headline": f"{role} at {target_company}",
                    "summary": focus_text,
                    "experience_lines": experience_lines,
                    "education_lines": [education_line],
                    "profile_capture_kind": "seeded_reference_profile",
                    "profile_timeline_source": "seeded_reference_profile",
                    "media_url": f"https://cdn.example.com/{slug}.jpg",
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
        "explicit_profile_capture_count": claimed_candidate_count,
        "structured_timeline_count": claimed_candidate_count,
        "structured_experience_count": claimed_candidate_count,
        "structured_education_count": claimed_candidate_count,
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
    store: ControlPlaneStore,
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
    population_coverage = {
        "contract_version": 1,
        "coverage_kind": "full_company_roster",
        "coverage_status": "complete",
        "full_company_coverage_proven": True,
        "proof_source": "reference_smoke_runtime_seed",
        "selected_snapshot_ids": [snapshot_id],
        "candidate_count": claimed_candidate_count,
        "current_lane_effective_candidate_count": current_count,
        "former_lane_effective_candidate_count": former_count,
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
            "source_snapshot_selection": {
                "selected_snapshot_ids": [snapshot_id],
                "population_coverage": population_coverage,
            },
            "source_path": str(snapshot_dir / "normalized_artifacts" / "artifact_summary.json"),
            "source_job_id": "",
            "summary": summary_payload,
            "population_coverage": population_coverage,
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
    store: ControlPlaneStore,
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
                "member_key": normalize_linkedin_profile_url_key(linkedin_url),
                "member_key_kind": "profile_url_key",
                "candidate_id": f"{company_key}-{artifact_key}-{index}",
                "profile_url_key": normalize_linkedin_profile_url_key(linkedin_url),
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


def _candidate_profile_url(candidate: dict[str, Any]) -> str:
    metadata = dict(candidate.get("metadata") or {})
    return str(
        candidate.get("linkedin_url")
        or candidate.get("profile_url")
        or metadata.get("profile_url")
        or metadata.get("linkedin_url")
        or ""
    ).strip()


def _candidate_employment_status(candidate: dict[str, Any]) -> str:
    metadata = dict(candidate.get("metadata") or {})
    return (
        str(candidate.get("employment_status") or metadata.get("membership_claim_employment_status") or "current")
        .strip()
        .lower()
        or "current"
    )


def _candidate_matches_text(candidate: dict[str, Any], term: str) -> bool:
    normalized_term = str(term or "").strip().lower()
    if not normalized_term:
        return False
    return normalized_term in json.dumps(candidate, ensure_ascii=False, sort_keys=True).lower()


def _load_google_large_baseline_source_candidates() -> tuple[Path, list[dict[str, Any]]]:
    source_path = (
        Path(__file__).resolve().parents[2]
        / "runtime"
        / "company_assets"
        / "google"
        / _GOOGLE_LARGE_BASELINE_SOURCE_SNAPSHOT_ID
        / "candidate_documents.json"
    )
    if not source_path.exists():
        return source_path, []
    try:
        payload = json.loads(source_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return source_path, []
    rows = payload.get("candidates") if isinstance(payload, dict) else []
    return source_path, [dict(item) for item in list(rows or []) if isinstance(item, dict)]


def _google_large_baseline_candidate_limit() -> int:
    raw_value = str(os.getenv("SOURCING_SEED_GOOGLE_LARGE_BASELINE_MAX_CANDIDATES") or "").strip()
    if raw_value:
        try:
            limit = int(raw_value)
        except ValueError:
            limit = _GOOGLE_LARGE_BASELINE_MAX_CANDIDATES
        if limit > 0:
            return limit
    return _GOOGLE_LARGE_BASELINE_MAX_CANDIDATES


def _google_large_baseline_exclude_terms() -> list[str]:
    raw_value = str(os.getenv("SOURCING_SEED_GOOGLE_LARGE_BASELINE_EXCLUDE_TERMS") or "").strip()
    if not raw_value:
        return list(_GOOGLE_LARGE_BASELINE_DEFAULT_EXCLUDE_TERMS)
    return [item.strip().lower() for item in raw_value.replace(";", ",").split(",") if item.strip()]


def _google_large_baseline_candidates(source_candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    limit = _google_large_baseline_candidate_limit()
    exclude_terms = _google_large_baseline_exclude_terms()
    selected: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for candidate in source_candidates:
        if any(_candidate_matches_text(candidate, term) for term in exclude_terms):
            continue
        profile_url = _candidate_profile_url(candidate)
        profile_key = normalize_linkedin_profile_url_key(profile_url)
        if not profile_key or profile_key in seen_keys:
            continue
        seen_keys.add(profile_key)
        selected.append(dict(candidate))
        if len(selected) >= limit:
            break
    return selected


def _candidate_members(
    candidates: list[dict[str, Any]],
    *,
    target_company: str,
    snapshot_id: str,
    artifact_kind: str,
    artifact_key: str,
) -> list[dict[str, Any]]:
    members: list[dict[str, Any]] = []
    for index, candidate in enumerate(candidates, start=1):
        profile_url = _candidate_profile_url(candidate)
        profile_url_key = normalize_linkedin_profile_url_key(profile_url)
        if not profile_url_key:
            continue
        members.append(
            {
                "target_company": target_company,
                "snapshot_id": snapshot_id,
                "asset_view": "canonical_merged",
                "artifact_kind": artifact_kind,
                "artifact_key": artifact_key,
                "lane": "baseline",
                "employment_scope": _candidate_employment_status(candidate),
                "member_key": profile_url_key,
                "member_key_kind": "profile_url_key",
                "candidate_id": str(candidate.get("candidate_id") or f"google-large-baseline-{index}"),
                "profile_url_key": profile_url_key,
                "metadata": {
                    "display_name": str(
                        candidate.get("display_name") or candidate.get("name_en") or f"Google Candidate {index}"
                    ),
                    "profile_url": profile_url,
                },
            }
        )
    return members


def _seed_google_large_baseline_real_asset(
    *,
    runtime_dir: str | Path,
    store: ControlPlaneStore,
) -> dict[str, Any]:
    source_candidate_path, source_candidates = _load_google_large_baseline_source_candidates()
    if not source_candidates:
        return {
            "status": "skipped",
            "reason": "google_large_baseline_source_missing",
            "source_candidate_documents_path": str(source_candidate_path),
        }
    baseline_candidates = _google_large_baseline_candidates(source_candidates)
    seed_exclude_terms = _google_large_baseline_exclude_terms()
    seed_candidate_limit = _google_large_baseline_candidate_limit()
    if not baseline_candidates:
        return {
            "status": "skipped",
            "reason": "google_large_baseline_candidates_empty",
            "source_candidate_documents_path": str(source_candidate_path),
        }
    runtime_root = Path(runtime_dir)
    company_key = resolve_company_alias_key("Google")
    snapshot_id = _GOOGLE_LARGE_BASELINE_SEEDED_SNAPSHOT_ID
    snapshot_dir = runtime_root / "company_assets" / company_key / snapshot_id
    normalized_dir = snapshot_dir / "normalized_artifacts"
    normalized_dir.mkdir(parents=True, exist_ok=True)
    identity = {
        "requested_name": "Google",
        "canonical_name": "Google",
        "company_key": company_key,
        "linkedin_slug": "google",
        "aliases": ["Google", "Google DeepMind"],
    }
    current_count = sum(1 for item in baseline_candidates if _candidate_employment_status(item) != "former")
    former_count = sum(1 for item in baseline_candidates if _candidate_employment_status(item) == "former")
    candidate_payload = {
        "snapshot": {
            "snapshot_id": snapshot_id,
            "company_identity": identity,
        },
        "target_company": "Google",
        "snapshot_id": snapshot_id,
        "candidates": baseline_candidates,
        "evidence": [],
        "candidate_count": len(baseline_candidates),
        "evidence_count": 0,
    }
    artifact_summary = {
        "snapshot_id": snapshot_id,
        "target_company": "Google",
        "candidate_count": len(baseline_candidates),
        "evidence_count": 0,
        "profile_detail_count": len(baseline_candidates),
        "explicit_profile_capture_count": len(baseline_candidates),
        "structured_timeline_count": len(baseline_candidates),
        "structured_experience_count": len(baseline_candidates),
        "structured_education_count": len(baseline_candidates),
        "missing_linkedin_count": 0,
        "current_lane_coverage": {
            "effective_candidate_count": current_count,
            "effective_ready": current_count > 0,
        },
        "former_lane_coverage": {
            "effective_candidate_count": former_count,
            "effective_ready": former_count > 0,
        },
        "seed_source": "google_large_baseline_real_asset",
        "source_candidate_documents_path": str(source_candidate_path),
        "seed_candidate_limit": seed_candidate_limit,
        "seed_exclude_terms": seed_exclude_terms,
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
    source_harvest_dir = source_candidate_path.parent / "harvest_profiles"
    if source_harvest_dir.exists() and not (snapshot_dir / "harvest_profiles").exists():
        try:
            shutil.copytree(source_harvest_dir, snapshot_dir / "harvest_profiles", dirs_exist_ok=True)
        except OSError:
            pass
    generation = store.register_asset_materialization(
        target_company="Google",
        snapshot_id=snapshot_id,
        asset_view="canonical_merged",
        artifact_kind="baseline_population",
        artifact_key="google-large-real-baseline",
        source_path=str(normalized_dir / "artifact_summary.json"),
        summary={
            "target_company": "Google",
            "snapshot_id": snapshot_id,
            "candidate_count": len(baseline_candidates),
            "seed_source": "google_large_baseline_real_asset",
        },
        metadata={"seed_source": "google_large_baseline_real_asset"},
        members=_candidate_members(
            baseline_candidates,
            target_company="Google",
            snapshot_id=snapshot_id,
            artifact_kind="baseline_population",
            artifact_key="google-large-real-baseline",
        ),
    )
    population_coverage = {
        "contract_version": 1,
        "coverage_kind": "scoped_asset",
        "coverage_status": "ready",
        "coverage_scope": (
            "google_large_baseline"
            + (
                "_excluding_" + "_".join(term.replace(" ", "_").replace("-", "_") for term in seed_exclude_terms)
                if seed_exclude_terms
                else ""
            )
        ),
        "full_company_coverage_proven": False,
        "directional_scope_reuse_allowed": False,
        "proof_source": "google_large_baseline_real_asset_seed",
        "selected_snapshot_ids": [snapshot_id],
        "candidate_count": len(baseline_candidates),
        "current_lane_effective_candidate_count": current_count,
        "former_lane_effective_candidate_count": former_count,
        "seed_candidate_limit": seed_candidate_limit,
        "seed_exclude_terms": seed_exclude_terms,
    }
    registry_payload = {
        "target_company": "Google",
        "company_key": company_key,
        "snapshot_id": snapshot_id,
        "asset_view": "canonical_merged",
        "status": "ready",
        "authoritative": True,
        "candidate_count": len(baseline_candidates),
        "evidence_count": 0,
        "profile_detail_count": len(baseline_candidates),
        "explicit_profile_capture_count": len(baseline_candidates),
        "missing_linkedin_count": 0,
        "manual_review_backlog_count": 0,
        "profile_completion_backlog_count": 0,
        "source_snapshot_count": 1,
        "completeness_score": 88.0,
        "completeness_band": "high",
        "current_lane_coverage": artifact_summary["current_lane_coverage"],
        "former_lane_coverage": artifact_summary["former_lane_coverage"],
        "current_lane_effective_candidate_count": current_count,
        "former_lane_effective_candidate_count": former_count,
        "current_lane_effective_ready": current_count > 0,
        "former_lane_effective_ready": former_count > 0,
        "selected_snapshot_ids": [snapshot_id],
        "source_snapshot_selection": {
            "selected_snapshot_ids": [snapshot_id],
            "population_coverage": population_coverage,
        },
        "source_path": str(normalized_dir / "artifact_summary.json"),
        "source_job_id": "",
        "summary": {
            **artifact_summary,
            "population_coverage": population_coverage,
        },
        "population_coverage": population_coverage,
        "materialization_generation_key": str(generation.get("generation_key") or ""),
        "materialization_generation_sequence": int(generation.get("generation_sequence") or 0),
        "materialization_watermark": str(generation.get("generation_watermark") or ""),
    }
    persisted = store.upsert_organization_asset_registry(registry_payload, authoritative=True)
    ensure_organization_execution_profile(
        runtime_dir=runtime_root,
        store=store,
        target_company="Google",
        asset_view="canonical_merged",
    )
    return {
        "status": "seeded",
        "target_company": "Google",
        "snapshot_id": snapshot_id,
        "source_snapshot_id": _GOOGLE_LARGE_BASELINE_SOURCE_SNAPSHOT_ID,
        "snapshot_dir": str(snapshot_dir),
        "candidate_count": len(baseline_candidates),
        "current_count": current_count,
        "former_count": former_count,
        "seed_candidate_limit": seed_candidate_limit,
        "seed_exclude_terms": seed_exclude_terms,
        "registry_id": int(persisted.get("registry_id") or 0),
        "generation_key": str(generation.get("generation_key") or ""),
    }


def _append_selected_snapshots(
    *,
    runtime_dir: str | Path,
    store: ControlPlaneStore,
    target_company: str,
    snapshot_ids: list[str],
) -> dict[str, Any]:
    row = store.get_authoritative_organization_asset_registry(target_company=target_company)
    selected_snapshot_ids = list(row.get("selected_snapshot_ids") or [])
    for snapshot_id in snapshot_ids:
        if snapshot_id and snapshot_id not in selected_snapshot_ids:
            selected_snapshot_ids.append(snapshot_id)
    source_snapshot_selection = dict(row.get("source_snapshot_selection") or {})
    source_snapshot_selection["selected_snapshot_ids"] = selected_snapshot_ids
    row["selected_snapshot_ids"] = selected_snapshot_ids
    row["source_snapshot_selection"] = source_snapshot_selection
    row["source_snapshot_count"] = len(selected_snapshot_ids)
    summary = dict(row.get("summary") or {})
    summary["selected_snapshot_ids"] = selected_snapshot_ids
    if source_snapshot_selection:
        summary["source_snapshot_selection"] = source_snapshot_selection
    row["summary"] = summary
    persisted = store.upsert_organization_asset_registry(row, authoritative=True)
    ensure_organization_execution_profile(
        runtime_dir=runtime_dir,
        store=store,
        target_company=target_company,
        asset_view="canonical_merged",
    )
    return persisted


def _seed_profile_search_shard_pair(
    *,
    runtime_dir: str | Path,
    store: ControlPlaneStore,
    target_company: str,
    snapshot_id: str,
    shard_title: str,
    current_specs: list[dict[str, str]],
    former_specs: list[dict[str, str]],
    function_ids: list[str],
    company_filter_urls: list[str],
) -> dict[str, Any]:
    company_key = resolve_company_alias_key(target_company)
    normalized_company_filter_urls = [
        str(item or "").strip() for item in list(company_filter_urls or []) if str(item or "").strip()
    ]
    _write_candidate_documents(
        runtime_dir=runtime_dir,
        target_company=target_company,
        snapshot_id=snapshot_id,
        claimed_candidate_count=len(current_specs) + len(former_specs),
        current_count=len(current_specs),
        former_count=len(former_specs),
        candidate_specs=[*current_specs, *former_specs],
    )
    current_row = build_acquisition_shard_registry_record(
        target_company=target_company,
        company_key=company_key,
        snapshot_id=snapshot_id,
        lane="profile_search",
        employment_scope="current",
        strategy_type="scoped_search_roster",
        shard_id=f"{shard_title}-current",
        shard_title=shard_title,
        search_query=shard_title,
        company_filters={
            "companies": normalized_company_filter_urls,
            "function_ids": function_ids,
            "search_query": shard_title,
        },
        result_count=len(current_specs),
        estimated_total_count=len(current_specs),
        status="completed",
    )
    current_generation = _register_materialization_generation(
        runtime_dir=runtime_dir,
        store=store,
        target_company=target_company,
        snapshot_id=snapshot_id,
        artifact_kind="acquisition_shard_bundle",
        artifact_key=str(current_row.get("shard_key") or f"{shard_title.lower()}-current"),
        candidate_specs=current_specs,
        lane="profile_search",
        employment_scope="current",
    )
    current_row["materialization_generation_key"] = str(current_generation.get("generation_key") or "")
    current_row["materialization_generation_sequence"] = int(current_generation.get("generation_sequence") or 0)
    current_row["materialization_watermark"] = str(current_generation.get("generation_watermark") or "")
    store.upsert_acquisition_shard_registry(current_row)

    former_row = build_acquisition_shard_registry_record(
        target_company=target_company,
        company_key=company_key,
        snapshot_id=snapshot_id,
        lane="profile_search",
        employment_scope="former",
        strategy_type="former_employee_search",
        shard_id=f"{shard_title}-former",
        shard_title=shard_title,
        search_query=shard_title,
        company_filters={
            "companies": normalized_company_filter_urls,
            "function_ids": function_ids,
            "search_query": shard_title,
        },
        result_count=len(former_specs),
        estimated_total_count=len(former_specs),
        status="completed",
    )
    former_generation = _register_materialization_generation(
        runtime_dir=runtime_dir,
        store=store,
        target_company=target_company,
        snapshot_id=snapshot_id,
        artifact_kind="acquisition_shard_bundle",
        artifact_key=str(former_row.get("shard_key") or f"{shard_title.lower()}-former"),
        candidate_specs=former_specs,
        lane="profile_search",
        employment_scope="former",
    )
    former_row["materialization_generation_key"] = str(former_generation.get("generation_key") or "")
    former_row["materialization_generation_sequence"] = int(former_generation.get("generation_sequence") or 0)
    former_row["materialization_watermark"] = str(former_generation.get("generation_watermark") or "")
    store.upsert_acquisition_shard_registry(former_row)
    return {
        "snapshot_id": snapshot_id,
        "shard_title": shard_title,
        "current_generation_key": str(current_generation.get("generation_key") or ""),
        "former_generation_key": str(former_generation.get("generation_key") or ""),
    }


def seed_reference_smoke_runtime(
    *,
    runtime_dir: str | Path,
    store: ControlPlaneStore | None = None,
) -> dict[str, Any]:
    runtime_root = Path(runtime_dir)
    runtime_root.mkdir(parents=True, exist_ok=True)
    effective_store = store or ControlPlaneStore(
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
            current_count=260,
            former_count=40,
            candidate_specs=_build_openai_baseline_candidate_specs(),
        ),
        _seed_authoritative_company_asset(
            runtime_dir=runtime_root,
            store=effective_store,
            target_company="Google",
            snapshot_id="20260414T120400",
            current_count=260,
            former_count=40,
            candidate_specs=_build_google_baseline_candidate_specs(),
        ),
        _seed_authoritative_company_asset(
            runtime_dir=runtime_root,
            store=effective_store,
            target_company="Meta",
            snapshot_id="20260414T120500",
            current_count=1500,
            former_count=227,
            candidate_specs=_build_meta_baseline_candidate_specs(),
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

    google_veo_snapshot_id = "20260414T120401"
    google_veo_shards = _seed_profile_search_shard_pair(
        runtime_dir=runtime_root,
        store=effective_store,
        target_company="Google",
        snapshot_id=google_veo_snapshot_id,
        shard_title="Veo",
        current_specs=[
            {
                "name": "Google Veo Current",
                "slug": "google-veo-current",
                "role": "Research Engineer",
                "focus_text": "Veo, multimodal generation, video generation, and pre-train systems.",
            }
        ],
        former_specs=[
            {
                "name": "Google Veo Former",
                "slug": "google-veo-former",
                "role": "Former Research Scientist",
                "employment_status": "former",
                "focus_text": "Former Google DeepMind researcher on Veo and multimodal generation.",
            }
        ],
        function_ids=["8", "9", "19", "24"],
        company_filter_urls=[
            "https://www.linkedin.com/company/google/",
            "https://www.linkedin.com/company/deepmind/",
        ],
    )
    google_nano_snapshot_id = "20260414T120402"
    google_nano_shards = _seed_profile_search_shard_pair(
        runtime_dir=runtime_root,
        store=effective_store,
        target_company="Google",
        snapshot_id=google_nano_snapshot_id,
        shard_title="Nano Banana",
        current_specs=[
            {
                "name": "Google Nano Banana Current",
                "slug": "google-nano-banana-current",
                "role": "Research Scientist",
                "focus_text": "Nano Banana, multimodal generation, vision-language, and AI product research.",
            }
        ],
        former_specs=[
            {
                "name": "Google Nano Banana Former",
                "slug": "google-nano-banana-former",
                "role": "Former Research Engineer",
                "employment_status": "former",
                "focus_text": "Former Google DeepMind contributor on Nano Banana multimodal workflows.",
            }
        ],
        function_ids=["8", "9", "19", "24"],
        company_filter_urls=[
            "https://www.linkedin.com/company/google/",
            "https://www.linkedin.com/company/deepmind/",
        ],
    )
    google_multimodal_snapshot_id = "20260414T120403"
    google_multimodal_shards = _seed_profile_search_shard_pair(
        runtime_dir=runtime_root,
        store=effective_store,
        target_company="Google",
        snapshot_id=google_multimodal_snapshot_id,
        shard_title="Multimodal",
        current_specs=[
            {
                "name": "Google Multimodal Current",
                "slug": "google-multimodal-current",
                "role": "Research Engineer",
                "focus_text": "Multimodal systems spanning Veo, Nano Banana, vision-language, and video generation.",
            }
        ],
        former_specs=[
            {
                "name": "Google Multimodal Former",
                "slug": "google-multimodal-former",
                "role": "Former Staff Researcher",
                "employment_status": "former",
                "focus_text": "Former Google DeepMind contributor across Veo, Nano Banana, and multimodal generation.",
            }
        ],
        function_ids=["8", "9", "19", "24"],
        company_filter_urls=[
            "https://www.linkedin.com/company/google/",
            "https://www.linkedin.com/company/deepmind/",
        ],
    )
    updated_google_row = _append_selected_snapshots(
        runtime_dir=runtime_root,
        store=effective_store,
        target_company="Google",
        snapshot_ids=[google_veo_snapshot_id, google_nano_snapshot_id, google_multimodal_snapshot_id],
    )

    meta_agent_snapshot_id = "20260414T120501"
    meta_agent_shards = _seed_profile_search_shard_pair(
        runtime_dir=runtime_root,
        store=effective_store,
        target_company="Meta",
        snapshot_id=meta_agent_snapshot_id,
        shard_title="Agent",
        current_specs=[
            {
                "name": "Meta Agent Current",
                "slug": "meta-agent-current",
                "role": "Research Engineer",
                "focus_text": "Agent systems, tool use, planning, and assistant product workflows at Meta.",
            }
        ],
        former_specs=[
            {
                "name": "Meta Agent Former",
                "slug": "meta-agent-former",
                "role": "Former Research Scientist",
                "employment_status": "former",
                "focus_text": "Former Meta agent systems researcher across planning and assistant workflows.",
            }
        ],
        function_ids=["8", "9", "24"],
        company_filter_urls=[
            "https://www.linkedin.com/company/meta/",
            "https://www.linkedin.com/company/facebook/",
        ],
    )
    updated_meta_row = _append_selected_snapshots(
        runtime_dir=runtime_root,
        store=effective_store,
        target_company="Meta",
        snapshot_ids=[meta_agent_snapshot_id],
    )
    google_large_baseline: dict[str, Any] = {}
    if str(os.getenv("SOURCING_SEED_GOOGLE_LARGE_BASELINE_REAL_ASSET") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        google_large_baseline = _seed_google_large_baseline_real_asset(
            runtime_dir=runtime_root,
            store=effective_store,
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
        "google_veo_nano": {
            "selected_snapshot_ids": list(updated_google_row.get("selected_snapshot_ids") or []),
            "veo": google_veo_shards,
            "nano_banana": google_nano_shards,
            "multimodal": google_multimodal_shards,
        },
        "meta_agent": {
            "selected_snapshot_ids": list(updated_meta_row.get("selected_snapshot_ids") or []),
            "agent": meta_agent_shards,
        },
        "google_large_baseline_real_asset": google_large_baseline,
    }
