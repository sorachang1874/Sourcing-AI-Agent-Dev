from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from x_first.capability_probe import REQUEST_SCHEMA_VERSION, RESULT_SCHEMA_VERSION, canonical_sha256

ROOT = Path(__file__).resolve().parents[1]
TIMESTAMP = "2026-07-14T00:00:00Z"


def build_request_fixture() -> dict[str, Any]:
    return {
        "schema_version": REQUEST_SCHEMA_VERSION,
        "probe_id": "xprobe_fixture_openai_official_v1",
        "execution_mode": "fixture_only",
        "owner_decisions": {
            "live_execution": "deferred",
            "legal_privacy": "deferred",
            "model_access": "deferred",
            "retention_policy": "deferred",
        },
        "target": {
            "lab_id": "openai",
            "account_kind": "official_lab",
            "platform_user_id": "xuid_fixture_official_openai",
            "current_handle": "fixture_openai_official",
        },
        "query": {
            "query_kind": "recent_public_technical_posts",
            "query_template": "fixture://openai/official/recent-public-technical-posts",
        },
        "hard_budgets": {
            "max_executions": 1,
            "max_external_calls": 1,
            "max_pages": 1,
            "max_observations": 5,
            "max_cost_usd": 0,
            "deadline_ms": 1000,
        },
        "kill_switch": {
            "armed": True,
            "trip_conditions": [
                "external_execution_attempted",
                "live_url_observed",
                "budget_exceeded",
                "credential_material_observed",
                "canonical_write_attempted",
            ],
        },
        "retention": {
            "class": "synthetic_fixture_only",
            "delete_after": None,
            "bounded_excerpt_max_chars": 280,
            "full_body_allowed": False,
        },
        "safety_policy_version": "x-first-public-professional-v1",
        "claims": {
            "external_execution_authorized": False,
            "researcher_mapping_authorized": False,
            "graph_expansion_authorized": False,
            "provider_fallback_authorized": False,
            "canonical_writes_authorized": False,
            "outreach_authorized": False,
        },
    }


def build_result_fixture(request: dict[str, Any] | None = None) -> dict[str, Any]:
    bound_request = request or build_request_fixture()
    request_hash = canonical_sha256(bound_request)
    target = bound_request["target"]
    observations = []
    for number in range(1, 4):
        object_id = f"xpost_fixture_{number:03d}"
        observations.append(
            {
                "observation_id": f"xprobe_obs_fixture_{number:03d}",
                "platform_object_id": object_id,
                "platform_user_id": target["platform_user_id"],
                "author_handle": target["current_handle"],
                "canonical_url": f"https://posts.invalid/{target['current_handle']}/status/{object_id}",
                "authored_at": f"2026-07-{10 + number:02d}T12:00:00Z",
                "observed_at": TIMESTAMP,
                "excerpt": (
                    f"Synthetic capability evidence {number:03d} about public technical model-training work."
                ),
                "full_body_stored": False,
            }
        )
    return {
        "schema_version": RESULT_SCHEMA_VERSION,
        "probe_id": bound_request["probe_id"],
        "request_sha256": request_hash,
        "execution_mode": "fixture_only",
        "run": {
            "run_id": "xprobe_run_fixture_openai_official_v1",
            "status": "completed",
            "started_at": TIMESTAMP,
            "completed_at": TIMESTAMP,
        },
        "task": {
            "task_id": "xprobe_task_fixture_openai_official_v1",
            "status": "succeeded",
            "stop_reason": "fixture_complete",
        },
        "capability": {
            "verdict": "fixture_contract_validated",
            "proof_scope": "offline_synthetic_only",
            "x_native_access_proven": False,
        },
        "provenance": {
            "provider_id": "offline_fixture",
            "access_mode": "fixture",
            "model_id": None,
            "tool_id": None,
            "provider_request_id": None,
            "prompt_version": "fixture-capability-v1",
            "request_sha256": request_hash,
            "raw_response_sha256": hashlib.sha256(b"synthetic fixture response v1").hexdigest(),
        },
        "usage": {
            "executions": 0,
            "external_calls": 0,
            "pages": 0,
            "observations": len(observations),
            "cost_usd": 0,
            "elapsed_ms": 1,
        },
        "observations": observations,
        "errors": [],
        "retention": {
            "class": "synthetic_fixture_only",
            "delete_after": None,
            "full_body_stored": False,
            "deletion_status": "not_applicable_synthetic",
        },
        "candidate_packets": [],
        "identity_link_proposals": [],
        "assertions": [],
        "canonical_writes": [],
        "claims": {
            "exhaustive": False,
            "current_employment_guaranteed": False,
            "outreach_permission": False,
            "researcher_mapping_authorized": False,
        },
    }


def _serialized(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def _check(path: Path, expected: str) -> bool:
    return path.exists() and path.read_text(encoding="utf-8") == expected


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate or check deterministic capability-probe fixtures")
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--write", action="store_true")
    action.add_argument("--check", action="store_true")
    args = parser.parse_args()

    request_path = ROOT / "fixtures/capability_probe_request_fixture_v1.json"
    result_path = ROOT / "fixtures/capability_probe_result_fixture_v1.json"
    request = build_request_fixture()
    values = (
        (request_path, _serialized(request)),
        (result_path, _serialized(build_result_fixture(request))),
    )
    if args.write:
        for path, content in values:
            path.write_text(content, encoding="utf-8")
        print(json.dumps({"status": "written", "paths": [str(path) for path, _ in values]}, indent=2))
        return 0

    stale = [str(path) for path, expected in values if not _check(path, expected)]
    if stale:
        print(json.dumps({"status": "stale", "paths": stale}, indent=2))
        return 1
    print(json.dumps({"status": "current", "paths": [str(path) for path, _ in values]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
