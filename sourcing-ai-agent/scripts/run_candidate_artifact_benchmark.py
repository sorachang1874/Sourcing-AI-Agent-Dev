#!/usr/bin/env python3
# ruff: noqa: E402
from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import tempfile
import time
from copy import deepcopy
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if sys.version_info < (3, 10):
    raise SystemExit(
        "run_candidate_artifact_benchmark.py requires Python 3.10+. "
        "Use .venv-tests/bin/python or the repository virtualenv."
    )
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from sourcing_agent.candidate_artifacts import build_company_candidate_artifacts
from sourcing_agent.domain import Candidate, EvidenceRecord, make_candidate_id, make_evidence_id
from sourcing_agent.storage import SQLiteStore

_BENCHMARK_MODES: dict[str, dict[str, str]] = {
    "optimized": {},
    "legacy_like": {
        "SOURCING_CANDIDATE_ARTIFACT_FORCE_SERIAL": "1",
        "SOURCING_CANDIDATE_ARTIFACT_DISABLE_BATCH_JSON_WRITES": "1",
        "SOURCING_CANDIDATE_ARTIFACT_DISABLE_BULK_STATE_UPSERT": "1",
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark candidate artifact materialization and compare the current optimized path "
            "against a legacy-like serial/batch-disabled mode."
        )
    )
    parser.add_argument("--target-company", default="Benchmark AI", help="Synthetic benchmark company name.")
    parser.add_argument("--snapshot-id", default="20260422T120000", help="Synthetic snapshot id.")
    parser.add_argument("--candidate-count", type=int, default=320, help="Synthetic candidate count.")
    parser.add_argument(
        "--dirty-candidates",
        type=int,
        default=24,
        help="Incremental patch run dirty candidate count.",
    )
    parser.add_argument(
        "--evidence-per-candidate",
        type=int,
        default=2,
        help="Synthetic evidence rows generated per candidate.",
    )
    parser.add_argument("--repeat", type=int, default=3, help="Repeat count per mode and scenario.")
    parser.add_argument(
        "--build-profile",
        default="foreground_fast",
        help="Artifact build profile passed to build_company_candidate_artifacts.",
    )
    parser.add_argument(
        "--report-json",
        default="",
        help="Optional JSON report path. stdout always prints the report.",
    )
    parser.add_argument(
        "--env-file",
        default=".local-postgres.env",
        help="Optional dotenv-style file loaded before benchmark runs. Pass an empty string to disable.",
    )
    return parser.parse_args()


def _company_key(target_company: str) -> str:
    normalized = "".join(ch.lower() for ch in str(target_company or "") if ch.isalnum())
    return normalized or "benchmarkai"


def _write_runtime_snapshot(
    *,
    runtime_dir: Path,
    target_company: str,
    snapshot_id: str,
    candidates: list[Candidate],
    evidence: list[EvidenceRecord],
) -> None:
    company_key = _company_key(target_company)
    company_root = runtime_dir / "company_assets" / company_key
    snapshot_dir = company_root / snapshot_id
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    company_identity = {
        "requested_name": target_company,
        "canonical_name": target_company,
        "company_key": company_key,
        "aliases": [target_company],
    }
    (company_root / "latest_snapshot.json").write_text(
        json.dumps(
            {
                "snapshot_id": snapshot_id,
                "company_identity": company_identity,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    (snapshot_dir / "candidate_documents.json").write_text(
        json.dumps(
            {
                "snapshot": {
                    "snapshot_id": snapshot_id,
                    "target_company": target_company,
                    "company_identity": company_identity,
                },
                "candidates": [candidate.to_record() for candidate in candidates],
                "evidence": [item.to_record() for item in evidence],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _build_synthetic_dataset(
    *,
    target_company: str,
    candidate_count: int,
    evidence_per_candidate: int,
) -> tuple[list[Candidate], list[EvidenceRecord]]:
    candidates: list[Candidate] = []
    evidence: list[EvidenceRecord] = []
    company_key = _company_key(target_company)
    for index in range(max(0, int(candidate_count or 0))):
        person_number = index + 1
        name = f"Benchmark Person {person_number:04d}"
        organization = target_company if index % 11 else f"{target_company} Labs"
        employment_status = "former" if index % 7 == 0 else "current"
        role = (
            "Research Engineer"
            if index % 5 in {0, 1}
            else "Research Scientist"
            if index % 5 == 2
            else "Member of Technical Staff"
        )
        focus_areas = "Reasoning, Post-training" if index % 3 == 0 else "Coding, Agents"
        candidate_id = make_candidate_id(name, organization, target_company)
        candidate = Candidate(
            candidate_id=candidate_id,
            name_en=name,
            display_name=name,
            category="employee",
            target_company=target_company,
            organization=organization,
            employment_status=employment_status,
            role=role,
            team=f"Team {index % 12:02d}",
            focus_areas=focus_areas,
            education="PhD in Computer Science",
            work_history=f"{organization}; Prior Lab {index % 9:02d}",
            linkedin_url=f"https://www.linkedin.com/in/{company_key}-{person_number:04d}/",
            source_dataset=f"{company_key}_roster_snapshot",
            metadata={
                "function_ids": ["research", "engineering"] if index % 4 != 0 else ["research"],
                "benchmark_rank": person_number,
            },
        )
        candidates.append(candidate)
        for evidence_index in range(max(0, int(evidence_per_candidate or 0))):
            source_type = "company_roster" if evidence_index == 0 else "public_web"
            title = (
                f"{target_company} roster row {person_number:04d}"
                if evidence_index == 0
                else f"{name} publication signal {evidence_index}"
            )
            url = (
                candidate.linkedin_url
                if evidence_index == 0
                else f"https://example.com/{company_key}/{person_number:04d}/{evidence_index}"
            )
            evidence.append(
                EvidenceRecord(
                    evidence_id=make_evidence_id(
                        candidate_id,
                        candidate.source_dataset or f"{company_key}_roster_snapshot",
                        title,
                        url,
                    ),
                    candidate_id=candidate_id,
                    source_type=source_type,
                    title=title,
                    url=url,
                    summary=f"Synthetic benchmark evidence {evidence_index} for {name}",
                    source_dataset=candidate.source_dataset or f"{company_key}_roster_snapshot",
                    source_path=f"synthetic/{person_number:04d}/{evidence_index}",
                    metadata={"benchmark_evidence_index": evidence_index},
                )
            )
    return candidates, evidence


def _mutate_candidate_slice(candidates: list[Candidate], dirty_candidates: int) -> list[Candidate]:
    mutated = deepcopy(candidates)
    for index, candidate in enumerate(mutated[: max(0, int(dirty_candidates or 0))]):
        metadata = dict(candidate.metadata or {})
        metadata["benchmark_patch_version"] = 2
        metadata["benchmark_patch_index"] = index + 1
        mutated[index] = Candidate(
            candidate_id=candidate.candidate_id,
            name_en=candidate.name_en,
            name_zh=candidate.name_zh,
            display_name=candidate.display_name,
            category=candidate.category,
            target_company=candidate.target_company,
            organization=candidate.organization,
            employment_status=candidate.employment_status,
            role=f"{candidate.role} v2",
            team=f"{candidate.team} patched",
            joined_at=candidate.joined_at,
            left_at=candidate.left_at,
            current_destination=candidate.current_destination,
            ethnicity_background=candidate.ethnicity_background,
            investment_involvement=candidate.investment_involvement,
            focus_areas=f"{candidate.focus_areas}, Benchmark Patch",
            education=candidate.education,
            work_history=candidate.work_history,
            notes=candidate.notes,
            linkedin_url=candidate.linkedin_url,
            media_url=candidate.media_url,
            source_dataset=candidate.source_dataset,
            source_path=candidate.source_path,
            metadata=metadata,
        )
    return mutated


def _load_env_file(env_file: str) -> dict[str, str]:
    normalized = str(env_file or "").strip()
    if not normalized:
        return {}
    path = Path(normalized).expanduser()
    if not path.is_absolute():
        path = REPO_ROOT / path
    if not path.is_file():
        return {}
    loaded: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = str(raw_line or "").strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        normalized_key = str(key or "").strip()
        if not normalized_key:
            continue
        loaded[normalized_key] = str(value or "").strip()
    return loaded


class _EnvOverride:
    def __init__(self, overrides: dict[str, str]) -> None:
        self._overrides = dict(overrides)
        self._previous: dict[str, str | None] = {}

    def __enter__(self) -> None:
        for key, value in self._overrides.items():
            self._previous[key] = os.environ.get(key)
            os.environ[key] = value

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        for key, previous in self._previous.items():
            if previous is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = previous


def _extract_run_summary(result: dict[str, Any], *, wall_ms: float) -> dict[str, Any]:
    canonical_view = dict(dict(result.get("views") or {}).get("canonical_merged") or {})
    summary = dict(canonical_view.get("summary") or result.get("summary") or {})
    return {
        "wall_ms": round(float(wall_ms), 2),
        "build_profile": str(result.get("build_profile") or ""),
        "candidate_count": int(summary.get("candidate_count") or 0),
        "dirty_candidate_count": int(summary.get("dirty_candidate_count") or 0),
        "reused_candidate_count": int(summary.get("reused_candidate_count") or 0),
        "state_upsert_candidate_count": int(summary.get("state_upsert_candidate_count") or 0),
        "removed_candidate_count": int(summary.get("removed_candidate_count") or 0),
        "manual_review_backlog_count": int(summary.get("manual_review_backlog_count") or 0),
        "profile_completion_backlog_count": int(summary.get("profile_completion_backlog_count") or 0),
        "build_execution": dict(summary.get("build_execution") or {}),
        "timings_ms": dict(summary.get("timings_ms") or {}),
    }


def _run_single_build(
    *,
    runtime_dir: Path,
    target_company: str,
    snapshot_id: str,
    build_profile: str,
    base_env_overrides: dict[str, str],
    env_overrides: dict[str, str],
) -> dict[str, Any]:
    store = SQLiteStore(runtime_dir / "sourcing_agent.db")
    with _EnvOverride(
        {
            "SOURCING_WRITE_COMPATIBILITY_ARTIFACTS": "0",
            **base_env_overrides,
            **env_overrides,
        }
    ):
        started_at = time.perf_counter()
        result = build_company_candidate_artifacts(
            runtime_dir=runtime_dir,
            store=store,
            target_company=target_company,
            snapshot_id=snapshot_id,
            build_profile=build_profile,
        )
        wall_ms = (time.perf_counter() - started_at) * 1000
    return _extract_run_summary(result, wall_ms=wall_ms)


def _numeric_summary(values: list[float]) -> dict[str, float]:
    if not values:
        return {}
    normalized = [round(float(item), 2) for item in values]
    return {
        "min": round(min(normalized), 2),
        "median": round(statistics.median(normalized), 2),
        "mean": round(statistics.mean(normalized), 2),
        "max": round(max(normalized), 2),
    }


def _aggregate_runs(runs: list[dict[str, Any]]) -> dict[str, Any]:
    if not runs:
        return {}
    timing_keys = sorted(
        {
            str(key)
            for run in runs
            for key in dict(run.get("timings_ms") or {}).keys()
            if str(key).strip()
        }
    )
    return {
        "wall_ms": _numeric_summary([float(run.get("wall_ms") or 0.0) for run in runs]),
        "candidate_count": _numeric_summary([float(run.get("candidate_count") or 0.0) for run in runs]),
        "dirty_candidate_count": _numeric_summary([float(run.get("dirty_candidate_count") or 0.0) for run in runs]),
        "reused_candidate_count": _numeric_summary([float(run.get("reused_candidate_count") or 0.0) for run in runs]),
        "state_upsert_candidate_count": _numeric_summary(
            [float(run.get("state_upsert_candidate_count") or 0.0) for run in runs]
        ),
        "timings_ms": {
            key: _numeric_summary([float(dict(run.get("timings_ms") or {}).get(key) or 0.0) for run in runs])
            for key in timing_keys
        },
        "build_execution": dict(runs[-1].get("build_execution") or {}),
    }


def _speedup_ratio(faster_ms: float, slower_ms: float) -> float | None:
    if faster_ms <= 0 or slower_ms <= 0:
        return None
    return round(slower_ms / faster_ms, 2)


def _build_comparison(optimized: dict[str, Any], legacy_like: dict[str, Any]) -> dict[str, Any]:
    optimized_wall = float(dict(optimized.get("wall_ms") or {}).get("median") or 0.0)
    legacy_wall = float(dict(legacy_like.get("wall_ms") or {}).get("median") or 0.0)
    timing_keys = sorted(
        {
            str(key)
            for payload in (optimized, legacy_like)
            for key in dict(payload.get("timings_ms") or {}).keys()
            if str(key).strip()
        }
    )
    timing_speedups: dict[str, Any] = {}
    for key in timing_keys:
        optimized_ms = float(dict(dict(optimized.get("timings_ms") or {}).get(key) or {}).get("median") or 0.0)
        legacy_ms = float(dict(dict(legacy_like.get("timings_ms") or {}).get(key) or {}).get("median") or 0.0)
        timing_speedups[key] = {
            "optimized_median_ms": optimized_ms,
            "legacy_like_median_ms": legacy_ms,
            "speedup_x": _speedup_ratio(optimized_ms, legacy_ms),
        }
    return {
        "wall_ms": {
            "optimized_median_ms": optimized_wall,
            "legacy_like_median_ms": legacy_wall,
            "speedup_x": _speedup_ratio(optimized_wall, legacy_wall),
            "saved_ms": round(max(0.0, legacy_wall - optimized_wall), 2),
        },
        "timings_ms": timing_speedups,
    }


def _run_cold_full_case(
    *,
    target_company: str,
    snapshot_id: str,
    candidate_count: int,
    evidence_per_candidate: int,
    repeat: int,
    build_profile: str,
    base_env_overrides: dict[str, str],
) -> dict[str, Any]:
    candidates, evidence = _build_synthetic_dataset(
        target_company=target_company,
        candidate_count=candidate_count,
        evidence_per_candidate=evidence_per_candidate,
    )
    mode_runs: dict[str, list[dict[str, Any]]] = {}
    for mode_name, env_overrides in _BENCHMARK_MODES.items():
        mode_runs[mode_name] = []
        for _ in range(max(1, int(repeat or 1))):
            with tempfile.TemporaryDirectory(prefix=f"candidate-artifact-bench-{mode_name}-") as tempdir:
                runtime_dir = Path(tempdir) / "runtime"
                runtime_dir.mkdir(parents=True, exist_ok=True)
                _write_runtime_snapshot(
                    runtime_dir=runtime_dir,
                    target_company=target_company,
                    snapshot_id=snapshot_id,
                    candidates=candidates,
                    evidence=evidence,
                )
                mode_runs[mode_name].append(
                    _run_single_build(
                        runtime_dir=runtime_dir,
                        target_company=target_company,
                        snapshot_id=snapshot_id,
                        build_profile=build_profile,
                        base_env_overrides=base_env_overrides,
                        env_overrides=env_overrides,
                    )
                )
    aggregates = {mode_name: _aggregate_runs(runs) for mode_name, runs in mode_runs.items()}
    return {
        "scenario": "cold_full",
        "mode_runs": mode_runs,
        "aggregates": aggregates,
        "comparison": _build_comparison(aggregates["optimized"], aggregates["legacy_like"]),
    }


def _run_incremental_patch_case(
    *,
    target_company: str,
    snapshot_id: str,
    candidate_count: int,
    dirty_candidates: int,
    evidence_per_candidate: int,
    repeat: int,
    build_profile: str,
    base_env_overrides: dict[str, str],
) -> dict[str, Any]:
    baseline_candidates, evidence = _build_synthetic_dataset(
        target_company=target_company,
        candidate_count=candidate_count,
        evidence_per_candidate=evidence_per_candidate,
    )
    patched_candidates = _mutate_candidate_slice(baseline_candidates, dirty_candidates)
    mode_runs: dict[str, list[dict[str, Any]]] = {}
    warmup_runs: dict[str, list[dict[str, Any]]] = {}
    for mode_name, env_overrides in _BENCHMARK_MODES.items():
        mode_runs[mode_name] = []
        warmup_runs[mode_name] = []
        for _ in range(max(1, int(repeat or 1))):
            with tempfile.TemporaryDirectory(prefix=f"candidate-artifact-patch-{mode_name}-") as tempdir:
                runtime_dir = Path(tempdir) / "runtime"
                runtime_dir.mkdir(parents=True, exist_ok=True)
                _write_runtime_snapshot(
                    runtime_dir=runtime_dir,
                    target_company=target_company,
                    snapshot_id=snapshot_id,
                    candidates=baseline_candidates,
                    evidence=evidence,
                )
                warmup_runs[mode_name].append(
                    _run_single_build(
                        runtime_dir=runtime_dir,
                        target_company=target_company,
                        snapshot_id=snapshot_id,
                        build_profile=build_profile,
                        base_env_overrides=base_env_overrides,
                        env_overrides=env_overrides,
                    )
                )
                _write_runtime_snapshot(
                    runtime_dir=runtime_dir,
                    target_company=target_company,
                    snapshot_id=snapshot_id,
                    candidates=patched_candidates,
                    evidence=evidence,
                )
                mode_runs[mode_name].append(
                    _run_single_build(
                        runtime_dir=runtime_dir,
                        target_company=target_company,
                        snapshot_id=snapshot_id,
                        build_profile=build_profile,
                        base_env_overrides=base_env_overrides,
                        env_overrides=env_overrides,
                    )
                )
    aggregates = {mode_name: _aggregate_runs(runs) for mode_name, runs in mode_runs.items()}
    return {
        "scenario": "incremental_patch",
        "dirty_candidates": max(0, int(dirty_candidates or 0)),
        "warmup_runs": warmup_runs,
        "warmup_aggregates": {mode_name: _aggregate_runs(runs) for mode_name, runs in warmup_runs.items()},
        "mode_runs": mode_runs,
        "aggregates": aggregates,
        "comparison": _build_comparison(aggregates["optimized"], aggregates["legacy_like"]),
    }


def main() -> int:
    args = parse_args()
    base_env_overrides = _load_env_file(str(args.env_file or ""))
    report = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "parameters": {
            "target_company": str(args.target_company),
            "snapshot_id": str(args.snapshot_id),
            "candidate_count": max(0, int(args.candidate_count or 0)),
            "dirty_candidates": max(0, int(args.dirty_candidates or 0)),
            "evidence_per_candidate": max(0, int(args.evidence_per_candidate or 0)),
            "repeat": max(1, int(args.repeat or 1)),
            "build_profile": str(args.build_profile or ""),
            "env_file": str(args.env_file or ""),
        },
        "modes": deepcopy(_BENCHMARK_MODES),
        "base_env_overrides": dict(base_env_overrides),
        "scenarios": [
            _run_cold_full_case(
                target_company=str(args.target_company),
                snapshot_id=str(args.snapshot_id),
                candidate_count=max(0, int(args.candidate_count or 0)),
                evidence_per_candidate=max(0, int(args.evidence_per_candidate or 0)),
                repeat=max(1, int(args.repeat or 1)),
                build_profile=str(args.build_profile or ""),
                base_env_overrides=base_env_overrides,
            ),
            _run_incremental_patch_case(
                target_company=str(args.target_company),
                snapshot_id=str(args.snapshot_id),
                candidate_count=max(0, int(args.candidate_count or 0)),
                dirty_candidates=max(0, int(args.dirty_candidates or 0)),
                evidence_per_candidate=max(0, int(args.evidence_per_candidate or 0)),
                repeat=max(1, int(args.repeat or 1)),
                build_profile=str(args.build_profile or ""),
                base_env_overrides=base_env_overrides,
            ),
        ],
    }
    payload = json.dumps(report, ensure_ascii=False, indent=2)
    if args.report_json:
        destination = Path(str(args.report_json)).expanduser()
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(payload, encoding="utf-8")
    sys.stdout.write(payload)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
