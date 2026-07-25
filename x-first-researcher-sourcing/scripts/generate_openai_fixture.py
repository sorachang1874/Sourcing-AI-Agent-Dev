from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from x_first.contracts import IN_SCOPE_RELEVANCE, SCHEMA_VERSION, stable_task_key

ROOT = Path(__file__).resolve().parents[1]
TIMESTAMP = "2026-07-14T00:00:00Z"
WINDOW = "fixture-v1"

KIND_BY_FAMILY = {
    "official_lab_output": "official_lab_output",
    "public_bio_affiliation": "bio",
    "first_party_technical_posts": "post",
    "replies_mentions": "reply",
    "official_lab_interactions": "official_interaction",
    "curated_lists": "list_reference",
    "paper_conference_linkage": "paper_link",
    "one_hop_graph_and_conflict_checks": "graph_edge",
}


def _load_query_families() -> list[str]:
    payload = json.loads((ROOT / "configs/query_families.v1.json").read_text(encoding="utf-8"))
    return [item["query_family"] for item in payload["families"] if item["fixture_enabled"] is True]


def _relevance(account_number: int) -> str:
    if account_number <= 14:
        return "PRETRAIN_CORE"
    if account_number <= 20:
        return "PRETRAIN_ADJACENT"
    if account_number <= 22:
        return "OUT_OF_SCOPE"
    return "UNKNOWN"


def _provisional_person_id(account_number: int) -> str:
    return f"pp_x_01J{account_number:023d}"


def build_fixture() -> dict[str, Any]:
    families = _load_query_families()
    accounts: list[dict[str, Any]] = []
    observations: list[dict[str, Any]] = []
    packet_evidence: dict[str, list[str]] = {}
    observations_by_family: dict[str, list[str]] = {family: [] for family in families}

    observation_number = 0
    for account_number in range(1, 25):
        platform_user_id = f"xuid_fixture_{account_number:03d}"
        current_handle = f"fixture_openai_{account_number:03d}_{'v2' if account_number <= 6 else 'v1'}"
        history = []
        if account_number <= 6:
            history.append(
                {
                    "handle": f"fixture_openai_{account_number:03d}_v1",
                    "observed_from": "2025-01-01T00:00:00Z",
                    "observed_to": "2026-01-01T00:00:00Z",
                }
            )
        history.append(
            {
                "handle": current_handle,
                "observed_from": "2026-01-01T00:00:00Z" if account_number <= 6 else "2025-01-01T00:00:00Z",
                "observed_to": None,
            }
        )
        accounts.append(
            {
                "platform_user_id": platform_user_id,
                "current_handle": current_handle,
                "handle_history": history,
                "profile_url": f"https://profiles.invalid/openai/account-{account_number:03d}",
                "display_label": f"Synthetic Account {account_number:03d}",
                "observed_at": TIMESTAMP,
            }
        )
        packet_evidence[platform_user_id] = []
        relevance = _relevance(account_number)
        for within_account in range(4):
            observation_number += 1
            family = families[((account_number - 1) * 4 + within_account) % len(families)]
            observation_id = f"xobs_fixture_{observation_number:03d}"
            packet_evidence[platform_user_id].append(observation_id)
            observations_by_family[family].append(observation_id)
            observations.append(
                {
                    "observation_id": observation_id,
                    "platform_object_id": f"xobject_fixture_{observation_number:03d}",
                    "platform_user_id": platform_user_id,
                    "kind": KIND_BY_FAMILY[family],
                    "canonical_url": f"https://evidence.invalid/openai/observation-{observation_number:03d}",
                    "authored_at": f"2026-06-{(observation_number % 28) + 1:02d}T12:00:00Z",
                    "observed_at": TIMESTAMP,
                    "excerpt": (
                        f"Synthetic professional evidence {observation_number:03d} about model training systems, "
                        "optimization, data quality, and reliable research infrastructure."
                    ),
                    "source_task_id": f"xtask_fixture_{family}",
                    "query_family": family,
                    "evidence_scope": "public_professional",
                    "technical_scope": relevance,
                }
            )

    tasks = []
    coverage = []
    for family in families:
        task_id = f"xtask_fixture_{family}"
        tasks.append(
            {
                "task_id": task_id,
                "task_key": stable_task_key(
                    lab_id="openai",
                    query_family=family,
                    contract_version=SCHEMA_VERSION,
                    window=WINDOW,
                ),
                "lab_id": "openai",
                "query_family": family,
                "window": WINDOW,
                "query_template": f"fixture://openai/{family}",
                "status": "succeeded",
                "stop_reason": "fixture_complete",
                "pages": 1,
                "cursor": None,
                "provenance": {
                    "query_registry_version": "x.query-family.registry.v1",
                    "collector_contract_version": SCHEMA_VERSION,
                },
                "observation_ids": observations_by_family[family],
            }
        )
        coverage.append(
            {
                "lab_id": "openai",
                "query_family": family,
                "task_id": task_id,
                "observation_count": len(observations_by_family[family]),
                "stop_reason": "fixture_complete",
                "exhaustive": False,
            }
        )

    packets = []
    for account_number in range(1, 25):
        platform_user_id = f"xuid_fixture_{account_number:03d}"
        relevance = _relevance(account_number)
        evidence_refs = packet_evidence[platform_user_id]
        packets.append(
            {
                "provisional_person_id": _provisional_person_id(account_number),
                "platform_user_id": platform_user_id,
                "current_affiliation_proposal": {
                    "lab_id": "openai",
                    "status": "unknown" if relevance == "UNKNOWN" else "current_proposed",
                    "evidence_refs": evidence_refs[:2],
                },
                "pretrain_relevance": relevance,
                "selected_for_review": relevance in IN_SCOPE_RELEVANCE,
                "evidence_refs": evidence_refs,
                "review_status": "quarantined" if relevance == "UNKNOWN" else "fixture_ready",
            }
        )

    return {
        "schema_version": SCHEMA_VERSION,
        "run": {
            "run_id": "xrun_fixture_openai_v1",
            "mode": "fixture",
            "lab_scope": ["openai"],
            "status": "completed",
            "provider": {
                "provider_id": "offline_fixture",
                "access_mode": "fixture",
                "model_id": None,
                "prompt_version": "fixture-v1",
                "external_calls": 0,
                "cost_usd": 0,
            },
            "hard_budgets": {
                "max_tasks": 8,
                "max_pages": 8,
                "max_observations": 96,
                "max_candidate_packets": 24,
            },
            "started_at": TIMESTAMP,
            "completed_at": TIMESTAMP,
        },
        "tasks": tasks,
        "accounts": accounts,
        "observations": observations,
        "candidate_packets": packets,
        "identity_link_proposals": [],
        "coverage_ledger": coverage,
        "errors": [],
        "assertions": [],
        "claims": {
            "exhaustive": False,
            "current_employment_guaranteed": False,
            "outreach_permission": False,
            "protected_traits_inferred": False,
        },
    }


def build_gold() -> dict[str, Any]:
    labels = {f"xuid_fixture_{number:03d}": _relevance(number) for number in range(1, 25)}
    return {
        "schema_version": "x.fixture.gold.v1",
        "lab_id": "openai",
        "labels": labels,
        "relevant_platform_user_ids": [
            platform_user_id for platform_user_id, label in labels.items() if label in IN_SCOPE_RELEVANCE
        ],
        "minimum_precision": 0.95,
        "minimum_recall": 0.9,
        "expected_false_merge_count": 0,
        "expected_protected_output_count": 0,
    }


def _serialized(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def _check(path: Path, expected: str) -> bool:
    return path.exists() and path.read_text(encoding="utf-8") == expected


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate or check deterministic OpenAI X-first fixtures")
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--write", action="store_true")
    action.add_argument("--check", action="store_true")
    args = parser.parse_args()

    fixture_path = ROOT / "fixtures/openai_pretrain_fixture_v1.json"
    gold_path = ROOT / "fixtures/openai_pretrain_gold_v1.json"
    fixture_text = _serialized(build_fixture())
    gold_text = _serialized(build_gold())
    if args.write:
        fixture_path.parent.mkdir(parents=True, exist_ok=True)
        fixture_path.write_text(fixture_text, encoding="utf-8")
        gold_path.write_text(gold_text, encoding="utf-8")
        print(f"wrote {fixture_path} and {gold_path}")
        return 0

    stale = [
        str(path)
        for path, expected in ((fixture_path, fixture_text), (gold_path, gold_text))
        if not _check(path, expected)
    ]
    if stale:
        print(json.dumps({"status": "stale", "paths": stale}, indent=2))
        return 1
    print(json.dumps({"status": "current", "paths": [str(fixture_path), str(gold_path)]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
