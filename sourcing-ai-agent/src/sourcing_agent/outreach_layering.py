from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sqlite3
import time
from typing import Any
from urllib import parse

from .candidate_artifacts import load_company_snapshot_candidate_documents
from .domain import Candidate
from .harvest_connectors import parse_harvest_profile_payload
from .model_provider import ModelClient, get_outreach_layer_prompt_template


_PINYIN_SURNAMES = {
    "bai",
    "bao",
    "cao",
    "cai",
    "chang",
    "chen",
    "cheng",
    "chou",
    "chu",
    "cui",
    "dai",
    "deng",
    "ding",
    "dong",
    "du",
    "duan",
    "fan",
    "fang",
    "feng",
    "fu",
    "gao",
    "ge",
    "gong",
    "gu",
    "guan",
    "guo",
    "han",
    "hao",
    "he",
    "hong",
    "hou",
    "hu",
    "hua",
    "huang",
    "hsu",
    "jiang",
    "jin",
    "kang",
    "ke",
    "kong",
    "lan",
    "lei",
    "li",
    "lian",
    "liang",
    "liao",
    "lin",
    "liu",
    "long",
    "lou",
    "lu",
    "luo",
    "lv",
    "ma",
    "mao",
    "meng",
    "mo",
    "ou",
    "pan",
    "peng",
    "qian",
    "qin",
    "qiu",
    "ren",
    "ruan",
    "sha",
    "shan",
    "shao",
    "shen",
    "shi",
    "song",
    "su",
    "sun",
    "tan",
    "tang",
    "tao",
    "teng",
    "tian",
    "tong",
    "wan",
    "wang",
    "wei",
    "wen",
    "wu",
    "xia",
    "xiao",
    "xie",
    "xin",
    "xiong",
    "xu",
    "xue",
    "yan",
    "yang",
    "yao",
    "ye",
    "yi",
    "yin",
    "yu",
    "yuan",
    "yue",
    "yun",
    "zeng",
    "zhai",
    "zhang",
    "zhao",
    "zheng",
    "zhou",
    "zhu",
    "zhuang",
    "zou",
}

_GREATER_CHINA_REGION_TOKENS = (
    "greater china",
    "china",
    "mainland china",
    "中国",
    "中国大陆",
    "大陆",
    "beijing",
    "北京",
    "shanghai",
    "上海",
    "shenzhen",
    "深圳",
    "guangzhou",
    "广州",
    "hangzhou",
    "杭州",
    "hong kong",
    "hong kong sar",
    "香港",
    "taiwan",
    "台湾",
    "台灣",
    "taipei",
    "台北",
    "macau",
    "macao",
    "澳门",
    "澳門",
    "singapore",
    "新加坡",
)

_MAINLAND_TOKENS = (
    "mainland china",
    "china",
    "中国大陆",
    "中国",
    "beijing",
    "北京",
    "shanghai",
    "上海",
    "shenzhen",
    "深圳",
    "guangzhou",
    "广州",
    "hangzhou",
    "杭州",
    "nanjing",
    "南京",
    "wuhan",
    "武汉",
    "chengdu",
    "成都",
    "peking university",
    "清华大学",
    "tsinghua",
    "fudan",
    "复旦",
)

_CHINESE_LANGUAGE_TOKENS = (
    "mandarin",
    "mandarin chinese",
    "chinese mandarin",
    "cantonese",
    "cantonese chinese",
    "putonghua",
    "putong hua",
    "guoyu",
    "国语",
    "國語",
    "shanghainese",
    "hokkien",
    "taiwanese hokkien",
    "teochew",
    "chinese language",
    "chinese (",
    "chinese proficiency",
    "chinese speaker",
    "simplified chinese",
    "traditional chinese",
    "中文",
    "汉语",
    "漢語",
    "华语",
    "華語",
    "普通话",
    "普通話",
    "粤语",
    "粵語",
)

_NAME_TOKEN_PATTERN = re.compile(r"[A-Za-z]+")
_CJK_PATTERN = re.compile(r"[\u4e00-\u9fff]")

_LAYER_0_KEY = "layer_0_roster"
_LAYER_1_KEY = "layer_1_name_signal"
_LAYER_2_KEY = "layer_2_greater_china_region_experience"
_LAYER_3_KEY = "layer_3_mainland_china_experience_or_chinese_language"
_PRIMARY_LAYER_KEYS = (_LAYER_0_KEY, _LAYER_1_KEY, _LAYER_2_KEY, _LAYER_3_KEY)
_LEGACY_LAYER_KEY_ALIASES = {
    "layer_2_greater_china_experience": _LAYER_2_KEY,
    "layer_3_mainland_or_chinese_language": _LAYER_3_KEY,
}


def _with_legacy_layer_aliases(payload: dict[str, Any]) -> dict[str, Any]:
    merged = dict(payload)
    for legacy_key, primary_key in _LEGACY_LAYER_KEY_ALIASES.items():
        if legacy_key not in merged and primary_key in merged:
            merged[legacy_key] = merged[primary_key]
    return merged


def analyze_company_outreach_layers(
    *,
    runtime_dir: str | Path,
    target_company: str,
    snapshot_id: str = "",
    view: str = "canonical_merged",
    query: str = "",
    model_client: ModelClient | None = None,
    max_ai_verifications: int = 80,
    ai_workers: int = 8,
    ai_max_retries: int = 2,
    ai_retry_backoff_seconds: float = 0.8,
    output_dir: str | Path | None = None,
) -> dict[str, Any]:
    loaded = load_company_snapshot_candidate_documents(
        runtime_dir=runtime_dir,
        target_company=target_company,
        snapshot_id=snapshot_id,
        view=view,
    )
    candidates = list(loaded.get("candidates") or [])
    registry_raw_paths = _load_registry_raw_paths_for_candidates(
        runtime_dir=runtime_dir,
        candidates=candidates,
    )
    analysis = build_outreach_layer_analysis(
        candidates=candidates,
        query=query,
        model_client=model_client,
        max_ai_verifications=max_ai_verifications,
        ai_workers=ai_workers,
        ai_max_retries=ai_max_retries,
        ai_retry_backoff_seconds=ai_retry_backoff_seconds,
        registry_raw_paths=registry_raw_paths,
    )
    analysis.update(
        {
            "status": "completed",
            "target_company": str(loaded.get("target_company") or target_company).strip() or target_company,
            "company_key": str(loaded.get("company_key") or "").strip(),
            "snapshot_id": str(loaded.get("snapshot_id") or "").strip(),
            "asset_view": str(loaded.get("asset_view") or view).strip() or "canonical_merged",
            "source_path": str(loaded.get("source_path") or "").strip(),
        }
    )

    snapshot_dir = Path(str(loaded.get("snapshot_dir") or "")).resolve()
    run_token = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    default_output_dir = snapshot_dir / "layered_segmentation" / f"greater_china_outreach_{run_token}"
    analysis_dir = Path(output_dir).resolve() if output_dir else default_output_dir
    analysis_dir.mkdir(parents=True, exist_ok=True)

    summary_payload = {
        "status": "completed",
        "target_company": analysis["target_company"],
        "snapshot_id": analysis["snapshot_id"],
        "asset_view": analysis["asset_view"],
        "query": analysis.get("query", ""),
        "ai_prompt_template_version": str((analysis.get("ai_prompt_template") or {}).get("version") or ""),
        "generated_at": analysis.get("generated_at", ""),
        "candidate_count": analysis.get("candidate_count", 0),
        "layer_counts": {
            key: int((analysis.get("layers") or {}).get(key, {}).get("count") or 0)
            for key in _PRIMARY_LAYER_KEYS
        },
        "legacy_layer_count_aliases": {
            legacy_key: int((analysis.get("layers") or {}).get(primary_key, {}).get("count") or 0)
            for legacy_key, primary_key in _LEGACY_LAYER_KEY_ALIASES.items()
        },
        "cumulative_layer_counts": dict(analysis.get("cumulative_layer_counts") or {}),
        "final_layer_distribution": dict(analysis.get("final_layer_distribution") or {}),
        "ai_verification": dict(analysis.get("ai_verification") or {}),
    }

    full_path = analysis_dir / "layered_analysis.json"
    summary_path = analysis_dir / "analysis_summary.json"
    full_path.write_text(json.dumps(analysis, ensure_ascii=False, indent=2))
    summary_path.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2))

    analysis["analysis_dir"] = str(analysis_dir)
    analysis["analysis_paths"] = {
        "full": str(full_path),
        "summary": str(summary_path),
    }
    return analysis


def build_outreach_layer_analysis(
    *,
    candidates: list[Candidate],
    query: str = "",
    model_client: ModelClient | None = None,
    max_ai_verifications: int = 80,
    ai_workers: int = 8,
    ai_max_retries: int = 2,
    ai_retry_backoff_seconds: float = 0.8,
    registry_raw_paths: dict[str, str] | None = None,
) -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    source_profile_cache: dict[str, dict[str, Any]] = {}
    registry_paths = dict(registry_raw_paths or {})
    for candidate in candidates:
        candidate_url = str(candidate.linkedin_url or "").strip()
        candidate_url_key = _normalize_linkedin_profile_url_key(candidate_url)
        fallback_raw_path = str(registry_paths.get(candidate_url_key) or "").strip()
        source_profile = _load_source_profile_signals(
            candidate.source_path,
            candidate_url=candidate_url,
            fallback_raw_path=fallback_raw_path,
            cache=source_profile_cache,
        )
        profile_text = _build_candidate_profile_text(candidate, source_profile=source_profile)
        education_value = str(candidate.education or source_profile.get("education") or "").strip()
        work_history_value = str(candidate.work_history or source_profile.get("work_history") or "").strip()
        note_parts = [str(candidate.notes or "").strip(), str(source_profile.get("summary") or "").strip()]
        notes_value = " | ".join(_dedupe_text_fragments(note_parts))
        name_signal, name_signal_hits = _detect_name_signal(candidate)
        greater_hits = _collect_keyword_hits(profile_text, _GREATER_CHINA_REGION_TOKENS)
        mainland_hits = _collect_keyword_hits(profile_text, _MAINLAND_TOKENS)
        language_hits = _collect_keyword_hits(profile_text, _CHINESE_LANGUAGE_TOKENS)
        deterministic_proposed_layer = _infer_deterministic_layer(
            name_signal=bool(name_signal),
            greater_hits=greater_hits,
            mainland_hits=mainland_hits,
            language_hits=language_hits,
        )
        records.append(
            {
                "candidate_id": candidate.candidate_id,
                "display_name": candidate.display_name,
                "linkedin_url": candidate.linkedin_url,
                "employment_status": candidate.employment_status,
                "organization": candidate.organization,
                "role": candidate.role,
                "education": education_value,
                "work_history": work_history_value,
                "notes": notes_value,
                "deterministic_proposed_layer": deterministic_proposed_layer,
                "final_layer": deterministic_proposed_layer,
                "final_layer_source": "deterministic",
                "deterministic_signals": {
                    "name_signal_hits": name_signal_hits[:8],
                    "greater_china_region_hits": greater_hits[:12],
                    "mainland_hits": mainland_hits[:12],
                    "chinese_language_hits": language_hits[:12],
                },
                "ai_verification": {},
                "profile_text_excerpt": profile_text[:1200],
            }
        )

    eligible_indexes = [
        idx
        for idx, item in enumerate(records)
        if int(item.get("deterministic_proposed_layer") or 0) > 0
    ]
    ai_eligible_indexes = [
        idx
        for idx in eligible_indexes
        if _record_supports_ai_verification(records[idx])
    ]
    ai_enabled = model_client is not None
    requested = 0
    attempted = 0
    successful = 0
    max_ai_verifications_requested = None if max_ai_verifications is None else max(0, int(max_ai_verifications or 0))
    max_ai_verifications_effective = 0
    if ai_enabled:
        if max_ai_verifications_requested is None:
            selected_indexes = list(ai_eligible_indexes)
        elif max_ai_verifications_requested <= 0:
            selected_indexes = []
        else:
            selected_indexes = list(ai_eligible_indexes[:max_ai_verifications_requested])
        max_ai_verifications_effective = len(selected_indexes)
        requested = len(selected_indexes)
        attempted = requested
        worker_count = max(1, min(int(ai_workers or 1), requested or 1))

        if worker_count <= 1:
            for idx in selected_indexes:
                ai_result, error = _evaluate_candidate_with_model(
                    model_client,
                    records[idx],
                    query=query,
                    max_retries=max(0, int(ai_max_retries or 0)),
                    retry_backoff_seconds=max(0.0, float(ai_retry_backoff_seconds or 0.0)),
                )
                if error:
                    records[idx]["ai_verification"] = {"error": error}
                    continue
                if not ai_result:
                    continue
                ai_layer = _coerce_layer_index(ai_result.get("final_layer"), default=-1)
                if ai_layer < 0:
                    records[idx]["ai_verification"] = {
                        **ai_result,
                        "error": "missing_final_layer",
                    }
                    continue
                successful += 1
                records[idx]["ai_verification"] = ai_result
                records[idx]["final_layer"] = ai_layer
                records[idx]["final_layer_source"] = "ai"
        else:
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                future_to_index = {
                    executor.submit(
                        _evaluate_candidate_with_model,
                        model_client,
                        records[idx],
                        query=query,
                        max_retries=max(0, int(ai_max_retries or 0)),
                        retry_backoff_seconds=max(0.0, float(ai_retry_backoff_seconds or 0.0)),
                    ): idx
                    for idx in selected_indexes
                }
                for future in as_completed(future_to_index):
                    idx = future_to_index[future]
                    try:
                        ai_result, error = future.result()
                    except Exception as exc:  # pragma: no cover - defensive executor safety
                        records[idx]["ai_verification"] = {"error": str(exc)}
                        continue
                    if error:
                        records[idx]["ai_verification"] = {"error": error}
                        continue
                    if not ai_result:
                        continue
                    ai_layer = _coerce_layer_index(ai_result.get("final_layer"), default=-1)
                    if ai_layer < 0:
                        records[idx]["ai_verification"] = {
                            **ai_result,
                            "error": "missing_final_layer",
                        }
                        continue
                    successful += 1
                    records[idx]["ai_verification"] = ai_result
                    records[idx]["final_layer"] = ai_layer
                    records[idx]["final_layer_source"] = "ai"

    for item in records:
        final_layer = _coerce_layer_index(item.get("final_layer"), default=0)
        item["final_layer"] = final_layer
        item["layer_membership"] = {
            _LAYER_0_KEY: True,
            _LAYER_1_KEY: final_layer == 1,
            _LAYER_2_KEY: final_layer == 2,
            _LAYER_3_KEY: final_layer == 3,
        }

    layers = _with_legacy_layer_aliases(
        {
            _LAYER_0_KEY: _layer_from_records(records, _LAYER_0_KEY),
            _LAYER_1_KEY: _layer_from_records(records, _LAYER_1_KEY),
            _LAYER_2_KEY: _layer_from_records(records, _LAYER_2_KEY),
            _LAYER_3_KEY: _layer_from_records(records, _LAYER_3_KEY),
        }
    )
    layer_definitions = _with_legacy_layer_aliases(
        {
            _LAYER_0_KEY: "组织 roster 全量成员。",
            _LAYER_1_KEY: "姓名弱信号层：AI 认为主要依据姓名线索，且缺少更强地区/语言信号。",
            _LAYER_2_KEY: "泛华人地区经历层：AI 认为存在中国大陆、港澳台、新加坡等 Greater China 公开经历信号。",
            _LAYER_3_KEY: "强信号层：AI 认为存在中国大陆经历，或中文/普通话/粤语等语言公开信号。",
        }
    )
    distribution = _final_layer_distribution(records)
    prompt_template = get_outreach_layer_prompt_template()
    return {
        "query": str(query or "").strip(),
        "ai_prompt_template": {
            "version": str(prompt_template.get("version") or ""),
            "required_output_keys": list(prompt_template.get("required_output_keys") or []),
        },
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "candidate_count": len(records),
        "layer_schema": {
            "primary_layer_keys": list(_PRIMARY_LAYER_KEYS),
            "legacy_layer_key_aliases": dict(_LEGACY_LAYER_KEY_ALIASES),
        },
        "layer_definitions": layer_definitions,
        "staging_policy": {
            "stage_1_rules": "规则引擎先基于姓名、教育、工作、地点、语言关键词给出 deterministic_proposed_layer。",
            "stage_2_ai": "AI 再基于完整 LinkedIn profile 文本摘要综合判断 final_layer。",
        },
        "layers": layers,
        "cumulative_layer_counts": {
            "layer_0_roster": len(records),
            "layer_1_or_higher": distribution["layer_1"] + distribution["layer_2"] + distribution["layer_3"],
            "layer_2_or_higher": distribution["layer_2"] + distribution["layer_3"],
            "layer_3_only": distribution["layer_3"],
        },
        "final_layer_distribution": distribution,
        "ai_verification": {
            "enabled": ai_enabled,
            "eligible_for_ai_verification": len(ai_eligible_indexes),
            "max_ai_verifications_requested": (
                "auto" if max_ai_verifications_requested is None else max_ai_verifications_requested
            ),
            "max_ai_verifications": max_ai_verifications_effective,
            "ai_workers": max(1, int(ai_workers or 1)),
            "ai_max_retries": max(0, int(ai_max_retries or 0)),
            "ai_retry_backoff_seconds": max(0.0, float(ai_retry_backoff_seconds or 0.0)),
            "requested": requested,
            "attempted": attempted,
            "successful": successful,
        },
        "candidates": records,
    }


def _layer_from_records(records: list[dict[str, Any]], layer_key: str) -> dict[str, Any]:
    members = [item for item in records if bool((item.get("layer_membership") or {}).get(layer_key))]
    return {
        "count": len(members),
        "candidate_ids": [str(item.get("candidate_id") or "").strip() for item in members],
        "sample_display_names": [str(item.get("display_name") or "").strip() for item in members[:15]],
    }


def _build_candidate_profile_text(candidate: Candidate, *, source_profile: dict[str, Any] | None = None) -> str:
    metadata = dict(candidate.metadata or {})
    fragments: list[str] = [
        candidate.display_name,
        candidate.name_en,
        candidate.name_zh,
        candidate.role,
        candidate.team,
        candidate.focus_areas,
        candidate.education,
        candidate.work_history,
        candidate.notes,
        candidate.organization,
        candidate.current_destination,
    ]
    for key in [
        "profile_location",
        "location",
        "headline",
        "summary",
        "about",
        "language",
        "languages",
        "skills",
        "profile_location",
        "profile_languages",
        "profile_skills",
        "profile_headline",
        "profile_summary",
        "positions",
        "education",
        "work_history",
    ]:
        if key in metadata:
            _append_text_fragments(fragments, metadata.get(key))
    if source_profile:
        for key in ["headline", "summary", "location", "languages", "skills", "education", "work_history"]:
            if key in source_profile:
                _append_text_fragments(fragments, source_profile.get(key))
    compact = " | ".join(_dedupe_text_fragments(fragments))
    return re.sub(r"\s+", " ", compact).strip()


def _dedupe_text_fragments(parts: list[Any]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for part in parts:
        text = _sanitize_outreach_text_fragment(part)
        if not text:
            continue
        normalized = re.sub(r"\s+", " ", text).strip().lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(text)
    return deduped


def _sanitize_outreach_text_fragment(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = re.sub(r"LinkedIn company roster baseline\.?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"Location:\s*[^|.]+\.?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"Source account:\s*[^|.]+\.?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip().strip("|")
    return text


def _load_source_profile_signals(
    source_path: str,
    *,
    candidate_url: str,
    fallback_raw_path: str = "",
    cache: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    path_value = _select_profile_detail_path(source_path=source_path, fallback_raw_path=fallback_raw_path)
    if not path_value:
        return {}
    cache_key = f"{path_value}|{str(candidate_url or '').strip()}"
    if cache_key in cache:
        return dict(cache[cache_key])
    path = Path(path_value)
    if not path.exists():
        cache[cache_key] = {}
        return {}
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        cache[cache_key] = {}
        return {}
    try:
        parsed = _select_matching_profile_payload(payload, candidate_url=candidate_url)
    except Exception:  # pragma: no cover - defensive parser fallback
        cache[cache_key] = {}
        return {}
    signals = {
        "headline": str(parsed.get("headline") or "").strip(),
        "summary": str(parsed.get("summary") or "").strip(),
        "location": str(parsed.get("location") or "").strip(),
        "education": _format_profile_education(parsed.get("education") or []),
        "work_history": _format_profile_experience(parsed.get("experience") or []),
        "languages": _format_profile_signal_list(parsed.get("languages") or []),
        "skills": _format_profile_signal_list(parsed.get("skills") or []),
    }
    cache[cache_key] = signals
    return dict(signals)


def _select_profile_detail_path(*, source_path: str, fallback_raw_path: str) -> str:
    source_path_value = str(source_path or "").strip()
    if source_path_value:
        source = Path(source_path_value)
        if source.exists() and _is_harvest_profile_path(source):
            return str(source)
    fallback_value = str(fallback_raw_path or "").strip()
    if fallback_value:
        fallback = Path(fallback_value)
        if fallback.exists() and _is_harvest_profile_path(fallback):
            return str(fallback)
    return ""


def _is_harvest_profile_path(path: Path) -> bool:
    return "harvest_profiles" in {part.lower() for part in path.parts}


def _normalize_linkedin_profile_url_key(profile_url: str) -> str:
    raw_value = str(profile_url or "").strip()
    if not raw_value:
        return ""
    if "://" not in raw_value:
        raw_value = f"https://{raw_value}"
    parsed = parse.urlsplit(raw_value)
    netloc = str(parsed.netloc or "").strip().lower()
    path = str(parsed.path or "").strip()
    if not netloc and path:
        reparsed = parse.urlsplit(f"https://{path}")
        netloc = str(reparsed.netloc or "").strip().lower()
        path = str(reparsed.path or "").strip()
    if not netloc:
        return ""
    normalized_path = re.sub(r"/{2,}", "/", path).rstrip("/")
    if not normalized_path:
        normalized_path = "/"
    return f"https://{netloc}{normalized_path}".lower()


def _dedupe_nonempty_strings(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in list(values or []):
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        deduped.append(text)
    return deduped


def _load_registry_raw_paths_for_candidates(*, runtime_dir: str | Path, candidates: list[Candidate]) -> dict[str, str]:
    keys = _dedupe_nonempty_strings(
        [
            _normalize_linkedin_profile_url_key(str(candidate.linkedin_url or "").strip())
            for candidate in list(candidates or [])
        ]
    )
    if not keys:
        return {}
    db_path = Path(runtime_dir) / "sourcing_agent.db"
    if not db_path.exists():
        return {}
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(db_path)
        connection.row_factory = sqlite3.Row
        placeholders = ",".join("?" for _ in keys)
        alias_rows = connection.execute(
            f"""
            SELECT alias_url_key, profile_url_key
            FROM linkedin_profile_registry_aliases
            WHERE alias_url_key IN ({placeholders})
            """,
            tuple(keys),
        ).fetchall()
        alias_map = {
            str(row["alias_url_key"] or "").strip(): str(row["profile_url_key"] or "").strip()
            for row in alias_rows
            if str(row["alias_url_key"] or "").strip() and str(row["profile_url_key"] or "").strip()
        }
        canonical_keys = _dedupe_nonempty_strings([str(alias_map.get(key) or key).strip() for key in keys])
        if not canonical_keys:
            return {}
        canonical_placeholders = ",".join("?" for _ in canonical_keys)
        registry_rows = connection.execute(
            f"""
            SELECT profile_url_key, last_raw_path
            FROM linkedin_profile_registry
            WHERE profile_url_key IN ({canonical_placeholders})
            """,
            tuple(canonical_keys),
        ).fetchall()
    except sqlite3.Error:
        return {}
    finally:
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass
    raw_by_canonical = {
        str(row["profile_url_key"] or "").strip(): str(row["last_raw_path"] or "").strip()
        for row in registry_rows
        if str(row["profile_url_key"] or "").strip()
    }
    resolved: dict[str, str] = {}
    for key in keys:
        canonical_key = str(alias_map.get(key) or key).strip()
        raw_path = str(raw_by_canonical.get(canonical_key) or "").strip()
        if raw_path and Path(raw_path).exists():
            resolved[key] = raw_path
    return resolved


def _select_matching_profile_payload(payload: Any, *, candidate_url: str) -> dict[str, Any]:
    candidate_url_norm = str(candidate_url or "").strip().rstrip("/")
    if isinstance(payload, dict):
        return parse_harvest_profile_payload(payload)
    if isinstance(payload, list):
        parsed_rows: list[dict[str, Any]] = []
        for item in payload:
            if isinstance(item, dict):
                parsed_rows.append(parse_harvest_profile_payload(item))
        if not parsed_rows:
            return {}
        if candidate_url_norm:
            for row in parsed_rows:
                profile_url = str(row.get("profile_url") or "").strip().rstrip("/")
                requested_profile_url = str(row.get("requested_profile_url") or "").strip().rstrip("/")
                if candidate_url_norm in {profile_url, requested_profile_url}:
                    return row
        if len(parsed_rows) == 1:
            return parsed_rows[0]
    return {}


def _format_profile_education(items: list[dict[str, Any]]) -> str:
    segments: list[str] = []
    for item in list(items or [])[:4]:
        school = _profile_signal_label(
            item.get("school")
            or item.get("school_name")
            or item.get("schoolName")
        )
        degree = _profile_signal_label(item.get("degree") or item.get("degreeName"))
        field = _profile_signal_label(item.get("field_of_study") or item.get("fieldOfStudy") or item.get("field"))
        if not school:
            continue
        segment = ", ".join(part for part in [degree, school, field] if part)
        if segment:
            segments.append(segment)
    return " / ".join(_dedupe_text_fragments(segments))


def _format_profile_experience(items: list[dict[str, Any]]) -> str:
    segments: list[str] = []
    for item in list(items or [])[:6]:
        title = _profile_signal_label(item.get("title") or item.get("position"))
        company = _profile_signal_label(item.get("companyName") or item.get("company_name") or item.get("company"))
        if not title and not company:
            continue
        segments.append(", ".join(part for part in [company, title] if part))
    return " / ".join(_dedupe_text_fragments(segments))


def _format_profile_signal_list(items: list[Any]) -> list[str]:
    labels: list[str] = []
    for item in list(items or [])[:24]:
        label = _profile_signal_label(item)
        if label:
            labels.append(label)
    return _dedupe_text_fragments(labels)[:12]


def _profile_signal_label(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in ["name", "title", "value", "text", "schoolName", "school", "companyName", "company", "label"]:
            text = str(value.get(key) or "").strip()
            if text:
                return text
        return ""
    if isinstance(value, (int, float, bool)):
        return str(value)
    return ""


def _append_text_fragments(buffer: list[str], value: Any, *, depth: int = 0) -> None:
    if depth > 2:
        return
    if isinstance(value, str):
        text = value.strip()
        if text:
            buffer.append(text)
        return
    if isinstance(value, (int, float, bool)):
        buffer.append(str(value))
        return
    if isinstance(value, list):
        for item in value[:12]:
            _append_text_fragments(buffer, item, depth=depth + 1)
        return
    if isinstance(value, dict):
        for key in list(value.keys())[:12]:
            _append_text_fragments(buffer, value.get(key), depth=depth + 1)


def _detect_name_signal(candidate: Candidate) -> tuple[bool, list[str]]:
    hits: list[str] = []
    name_fields = [candidate.name_zh, candidate.display_name, candidate.name_en]
    if any(_CJK_PATTERN.search(str(item or "")) for item in name_fields):
        hits.append("cjk_name_script")
    tokens = _NAME_TOKEN_PATTERN.findall(" ".join(str(item or "") for item in [candidate.name_en, candidate.display_name]).lower())
    if tokens:
        if tokens[0] in _PINYIN_SURNAMES:
            hits.append(f"pinyin_surname:{tokens[0]}")
        if len(tokens) > 1 and tokens[-1] in _PINYIN_SURNAMES:
            hits.append(f"pinyin_surname:{tokens[-1]}")
    deduped = []
    seen = set()
    for item in hits:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return bool(deduped), deduped


def _collect_keyword_hits(text: str, tokens: tuple[str, ...]) -> list[str]:
    hits: list[str] = []
    lowered = text.lower()
    for token in tokens:
        probe = str(token or "").strip()
        if not probe:
            continue
        if _contains_ascii_alpha(probe):
            matched = probe.lower() in lowered
        else:
            matched = probe in text
        if matched:
            hits.append(probe)
    deduped = []
    seen = set()
    for item in hits:
        key = item.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _contains_ascii_alpha(text: str) -> bool:
    return any("a" <= ch.lower() <= "z" for ch in text)


def _infer_deterministic_layer(
    *,
    name_signal: bool,
    greater_hits: list[str],
    mainland_hits: list[str],
    language_hits: list[str],
) -> int:
    if mainland_hits or language_hits:
        return 3
    if greater_hits:
        return 2
    if name_signal:
        return 1
    return 0


def _coerce_layer_index(value: Any, *, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(0, min(parsed, 3))


def _final_layer_distribution(records: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"layer_0": 0, "layer_1": 0, "layer_2": 0, "layer_3": 0}
    for item in records:
        layer = _coerce_layer_index(item.get("final_layer"), default=0)
        counts[f"layer_{layer}"] += 1
    return counts


def _record_supports_ai_verification(record: dict[str, Any]) -> bool:
    education = str(record.get("education") or "").strip()
    work_history = str(record.get("work_history") or "").strip()
    notes = str(record.get("notes") or "").strip()
    profile_excerpt = str(record.get("profile_text_excerpt") or "").strip()

    if education or work_history:
        return True
    if len(notes) >= 40:
        return True
    if len(profile_excerpt) >= 160:
        return True
    return False


def _evaluate_candidate_with_model(
    model_client: ModelClient,
    record: dict[str, Any],
    *,
    query: str,
    max_retries: int = 2,
    retry_backoff_seconds: float = 0.8,
) -> tuple[dict[str, Any], str]:
    payload = {
        "query": query,
        "layer_hypothesis": {
            "deterministic_proposed_layer": int(record.get("deterministic_proposed_layer") or 0),
            "name_signal_hit": bool((record.get("deterministic_signals") or {}).get("name_signal_hits")),
            "greater_china_region_experience_hit": bool((record.get("deterministic_signals") or {}).get("greater_china_region_hits")),
            "greater_china_experience_hit": bool((record.get("deterministic_signals") or {}).get("greater_china_region_hits")),
            "mainland_china_experience_or_chinese_language_hit": bool(
                (record.get("deterministic_signals") or {}).get("mainland_hits")
                or (record.get("deterministic_signals") or {}).get("chinese_language_hits")
            ),
            "mainland_or_chinese_language_hit": bool(
                (record.get("deterministic_signals") or {}).get("mainland_hits")
                or (record.get("deterministic_signals") or {}).get("chinese_language_hits")
            ),
        },
        "candidate": {
            "candidate_id": record.get("candidate_id", ""),
            "display_name": record.get("display_name", ""),
            "employment_status": record.get("employment_status", ""),
            "organization": record.get("organization", ""),
            "role": record.get("role", ""),
            "linkedin_url": record.get("linkedin_url", ""),
        },
        "candidate_profile": {
            "education": record.get("education", ""),
            "work_history": record.get("work_history", ""),
            "notes": record.get("notes", ""),
        },
        "deterministic_signals": record.get("deterministic_signals", {}),
        "profile_text_excerpt": record.get("profile_text_excerpt", ""),
    }
    attempts = 1 + max(0, int(max_retries or 0))
    last_error = ""
    for idx in range(attempts):
        try:
            result = dict(model_client.evaluate_outreach_profile(payload) or {})
        except Exception as exc:  # pragma: no cover - defensive network/runtime protection
            last_error = str(exc)
            result = {}
        else:
            model_error = str(result.get("error") or "").strip()
            if not model_error:
                return result, ""
            last_error = model_error
        if idx >= attempts - 1:
            break
        sleep_seconds = max(0.0, float(retry_backoff_seconds or 0.0)) * (2 ** idx)
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
    return {}, last_error
