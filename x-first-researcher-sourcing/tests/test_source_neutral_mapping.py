from __future__ import annotations

import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from tests.grok_raw_session_fixture import (
    fixture_grok_session_precommit,
    raw_grok_session,
)
from x_first.grok_operator_session_replay import replay_grok_operator_session
from x_first.recall_pool_schema import assert_schema_valid
from x_first.source_neutral_mapping import (
    AGGREGATE_SCHEMA_FILE,
    CALIBRATION_SCHEMA_FILE,
    MANIFEST_SCHEMA_FILE,
    PLAN_SCHEMA_FILE,
    POLICY_SCHEMA_FILE,
    RECEIPT_SCHEMA_FILE,
    SourceNeutralMappingError,
    build_candidate_free_aggregate,
    build_frontier_queues,
    build_luna_input_queue,
    build_session_precommit,
    canonical_sha256,
    expand_saturation_item,
    freeze_candidate_manifest,
    load_policy,
    plan_wave_p,
    project_root,
    render_flat_terminal,
    replay_flat_session,
    split_invalid_batch,
    strict_load_json,
    structural_stop,
    validate_candidate_manifest,
    validate_checked_in_assets,
    validate_policy,
    validate_private_calibration_binding,
    validate_wave_plan,
)

ROOT = project_root()
MANIFEST_PATH = ROOT / "fixtures" / "source_neutral_mapping_manifest_fixture_v1.json"
CALIBRATION_PATH = (
    ROOT / "docs" / "live-evidence" / "2026-07-17-gdm-query-family-calibration.aggregate.v1.json"
)


def _manifest() -> dict:
    return strict_load_json(MANIFEST_PATH)


def _candidate(index: int) -> dict:
    handle = f"fxmap{index:06d}"
    return {
        "candidate_ref": f"fixture_candidate_{index:06d}",
        "platform_user_id": str(7_000_000 + index),
        "current_handle": handle,
        "profile_url": f"https://x.invalid/{handle}",
        "lab_affiliation_prior": {
            "state": ("current", "historical", "ambiguous", "unsupported")[index % 4],
            "evidence_status": "fixture_asserted" if index % 4 != 3 else "unsupported",
        },
        "pretraining_experience_prior": {
            "state": ("historical", "current", "unsupported", "ambiguous")[index % 4],
            "evidence_status": "fixture_asserted" if index % 4 != 2 else "unsupported",
        },
    }


def _large_manifest(count: int = 137) -> dict:
    return freeze_candidate_manifest(
        manifest_id="fixture_large_mapping_manifest_v1",
        lab_descriptor=_manifest()["lab_descriptor"],
        candidates=[_candidate(index) for index in range(1, count + 1)],
    )


def _accepted_receipt(references: list[dict], *, receipt_salt: str = "a") -> dict:
    payload = {
        "schema_version": "x.source_neutral.mapping.session_receipt.v1",
        "status": "accepted",
        "commit_allowed": True,
        "proof_authority": "existing_raw_grok_six_file_replay",
        "errors": [],
        "precommit_sha256": receipt_salt * 64,
        "source_artifact_manifest_sha256": "b" * 64,
        "source_artifact_count": 6,
        "source_artifact_hashes_sha256": "c" * 64,
        "terminal_sha256": "d" * 64,
        "planned_native_x_call_count": max(1, len(references)),
        "attested_completed_native_x_call_count": max(1, len(references)),
        "x_user_search_call_count": 0,
        "pretool_terminal_count": 0,
        "unique_stable_post_id_count": len(references),
        "saturation_lower_bound_count": 0,
        "references": references,
        "saturation_queue_seeds": [],
        "receipt_sha256": "",
    }
    payload["receipt_sha256"] = canonical_sha256(
        {key: value for key, value in payload.items() if key != "receipt_sha256"}
    )
    return payload


def _fixture_batch(
    *,
    terminal_blocks: list[list[str]] | None = None,
    terminal_before_tools: bool = False,
    raw_call_override: list[tuple[str, dict]] | None = None,
) -> tuple[dict, dict, str, dict, object]:
    manifest = _manifest()
    policy = load_policy()
    plan = plan_wave_p(manifest, policy, plan_id="fixture_wave_p_v1")
    batch = plan["batches"][0]
    marker = f"MAPPING_BATCH_SHA256={batch['batch_sha256']}"
    prompt = f"Synthetic flat mapping session.\n{marker}"
    grok_precommit = fixture_grok_session_precommit(user_prompt_text=prompt)
    mapping_precommit = build_session_precommit(
        plan=plan,
        batch_index=0,
        grok_precommit=grok_precommit,
    )
    if terminal_blocks is None:
        terminal_blocks = [
            [
                f"https://x.invalid/{call['expected_author_handle']}/status/{10000 + ordinal}"
            ]
            for ordinal, call in enumerate(batch["calls"], 1)
        ]
    terminal = render_flat_terminal(terminal_blocks)
    calls = raw_call_override or [
        (call["tool_name"], call["arguments"]) for call in mapping_precommit["calls"]
    ]
    raw = raw_grok_session(
        {"ignored_by_literal_flat_replay": True},
        calls=calls,
        user_prompt_text=prompt,
        terminal_text_override=terminal,
        terminal_before_tools=terminal_before_tools,
    )
    return manifest, plan, terminal, raw, grok_precommit


class SourceNeutralMappingTests(unittest.TestCase):
    def test_checked_policy_and_manifest_are_schema_valid_lab_neutral_and_hash_bound(self) -> None:
        policy = load_policy()
        manifest = _manifest()
        validate_policy(policy)
        validate_candidate_manifest(manifest)
        assert_schema_valid(policy, POLICY_SCHEMA_FILE)
        assert_schema_valid(manifest, MANIFEST_SCHEMA_FILE)
        serialized = json.dumps(policy).casefold()
        self.assertNotIn("googledeepmind", serialized)
        self.assertNotIn("gdm", serialized)
        self.assertIsNone(policy["authority"]["business_candidate_cap"])
        self.assertIsNone(policy["authority"]["business_reference_cap"])
        axes = manifest["candidates"][:4]
        self.assertEqual(
            {
                (row["lab_affiliation_prior"]["state"], row["pretraining_experience_prior"]["state"])
                for row in axes
            },
            {
                ("current", "current"),
                ("current", "historical"),
                ("historical", "current"),
                ("historical", "historical"),
            },
        )
        tampered = copy.deepcopy(manifest)
        tampered["candidates"][0]["pretraining_experience_prior"]["state"] = "historical"
        with self.assertRaisesRegex(SourceNeutralMappingError, "hash_mismatch"):
            validate_candidate_manifest(tampered)

    def test_planner_covers_more_than_one_hundred_candidates_without_business_cap(self) -> None:
        manifest = _large_manifest(137)
        policy = load_policy()
        plan = plan_wave_p(manifest, policy, plan_id="fixture_large_wave_p_v1")
        validate_wave_plan(plan, manifest=manifest, policy=policy)
        assert_schema_valid(plan, PLAN_SCHEMA_FILE)
        self.assertEqual(plan["candidate_count"], 137)
        self.assertEqual(plan["planned_batch_count"], 46)
        self.assertEqual(plan["planned_native_x_call_count"], 274)
        self.assertEqual([len(batch["candidate_refs"]) for batch in plan["batches"]][-2:], [3, 2])
        refs = [ref for batch in plan["batches"] for ref in batch["candidate_refs"]]
        self.assertEqual(refs, [row["candidate_ref"] for row in manifest["candidates"]])
        self.assertTrue(all(len(batch["calls"]) == 2 * len(batch["candidate_refs"]) for batch in plan["batches"]))

    def test_grain_and_call_count_come_from_policy_not_code(self) -> None:
        manifest = _manifest()
        policy = load_policy()
        policy["wave_p"]["candidate_grain"] = 4
        policy["wave_p"]["champion_cells"].append(
            {
                "cell_id": "architecture_latest",
                "concept_alias_group_id": "architecture_latest",
                "mode": "Latest",
            }
        )
        policy["wave_p"]["calls_per_candidate"] = 3
        validate_policy(policy)
        plan = plan_wave_p(manifest, policy, plan_id="fixture_configurable_wave_v1")
        self.assertEqual([len(batch["candidate_refs"]) for batch in plan["batches"]], [4, 2])
        self.assertEqual(plan["planned_native_x_call_count"], 18)

    def test_default_champion_arguments_are_byte_equal_to_calibrated_keyword_cells(self) -> None:
        manifest = _manifest()
        plan = plan_wave_p(manifest, load_policy(), plan_id="fixture_exact_champion_wave_v1")
        calls = plan["batches"][0]["calls"][:2]
        self.assertEqual(
            calls[0]["arguments"],
            {
                "limit": "10",
                "mode": "Top",
                "query": (
                    'from:fixturemap001 (pretrain OR pretraining OR "pre-training" OR tokenizer OR '
                    'tokenization OR "training data" OR "data mixture" OR scaling OR optimization OR '
                    '"training stability" OR "distributed training" OR accelerator OR TPU OR multimodal OR '
                    '"base model" OR "foundation model" OR "model training")'
                ),
            },
        )
        self.assertEqual(
            calls[1]["arguments"],
            {
                "limit": "10",
                "mode": "Top",
                "query": (
                    'from:fixturemap001 (dataset OR corpus OR objective OR loss OR "training recipe" OR '
                    "curriculum OR deduplication OR filtering)"
                ),
            },
        )

    def test_invalid_batch_split_is_three_to_two_plus_one_then_binary(self) -> None:
        self.assertEqual(split_invalid_batch(["a", "b", "c"]), [["a", "b"], ["c"]])
        self.assertEqual(split_invalid_batch(list("abcdefgh")), [list("abcd"), list("efgh")])
        self.assertEqual(split_invalid_batch(["a", "b"]), [["a"], ["b"]])
        self.assertEqual(split_invalid_batch(["a"]), [])

    def test_literal_flat_terminal_is_accepted_only_through_exact_six_file_replay(self) -> None:
        manifest, plan, terminal, raw, grok_precommit = _fixture_batch()
        mapping_precommit = build_session_precommit(
            plan=plan,
            batch_index=0,
            grok_precommit=grok_precommit,
        )
        receipt = replay_flat_session(
            mapping_precommit=mapping_precommit,
            terminal_text=terminal,
            lab_descriptor=manifest["lab_descriptor"],
            raw_session_files=raw,
            grok_precommit=grok_precommit,
        )
        assert_schema_valid(receipt, RECEIPT_SCHEMA_FILE)
        self.assertEqual(receipt["status"], "accepted")
        self.assertTrue(receipt["commit_allowed"])
        self.assertEqual(receipt["proof_authority"], "existing_raw_grok_six_file_replay")
        self.assertEqual(receipt["source_artifact_count"], 6)
        self.assertEqual(receipt["planned_native_x_call_count"], 6)
        self.assertEqual(receipt["attested_completed_native_x_call_count"], 6)
        self.assertEqual(receipt["unique_stable_post_id_count"], 6)
        self.assertEqual(receipt["x_user_search_call_count"], 0)
        self.assertEqual(receipt["pretool_terminal_count"], 0)

    def test_caller_projection_never_self_proves_execution_or_commits_references(self) -> None:
        manifest, plan, terminal, _, grok_precommit = _fixture_batch()
        mapping_precommit = build_session_precommit(plan=plan, batch_index=0, grok_precommit=grok_precommit)
        receipt = replay_flat_session(
            mapping_precommit=mapping_precommit,
            terminal_text=terminal,
            lab_descriptor=manifest["lab_descriptor"],
            raw_session_files=None,
            grok_precommit=None,
        )
        self.assertEqual(receipt["status"], "rejected")
        self.assertEqual(receipt["proof_authority"], "operator_projected_unverified")
        self.assertFalse(receipt["commit_allowed"])
        self.assertEqual(receipt["references"], [])
        self.assertEqual(receipt["attested_completed_native_x_call_count"], 0)

    def test_raw_replay_rejects_pretool_terminal_wrong_tool_and_argument_drift(self) -> None:
        manifest, plan, terminal, raw, grok_precommit = _fixture_batch(terminal_before_tools=True)
        mapping_precommit = build_session_precommit(plan=plan, batch_index=0, grok_precommit=grok_precommit)
        before = replay_flat_session(
            mapping_precommit=mapping_precommit,
            terminal_text=terminal,
            lab_descriptor=manifest["lab_descriptor"],
            raw_session_files=raw,
            grok_precommit=grok_precommit,
        )
        self.assertEqual(before["errors"], ["raw_six_file_replay_invalid"])

        wrong_calls = [("x_user_search", {"query": "fixture", "count": "1"})]
        wrong_calls.extend(
            (call["tool_name"], call["arguments"]) for call in mapping_precommit["calls"][1:]
        )
        _, _, terminal, raw, grok_precommit = _fixture_batch(raw_call_override=wrong_calls)
        wrong_tool = replay_flat_session(
            mapping_precommit=build_session_precommit(plan=plan, batch_index=0, grok_precommit=grok_precommit),
            terminal_text=terminal,
            lab_descriptor=manifest["lab_descriptor"],
            raw_session_files=raw,
            grok_precommit=grok_precommit,
        )
        self.assertFalse(wrong_tool["commit_allowed"])
        self.assertEqual(wrong_tool["references"], [])

        drift_calls = [(call["tool_name"], copy.deepcopy(call["arguments"])) for call in mapping_precommit["calls"]]
        drift_calls[0][1]["query"] += " drift"
        _, _, terminal, raw, grok_precommit = _fixture_batch(raw_call_override=drift_calls)
        drift = replay_flat_session(
            mapping_precommit=build_session_precommit(plan=plan, batch_index=0, grok_precommit=grok_precommit),
            terminal_text=terminal,
            lab_descriptor=manifest["lab_descriptor"],
            raw_session_files=raw,
            grok_precommit=grok_precommit,
        )
        self.assertFalse(drift["commit_allowed"])

    def test_literal_flat_replay_extension_does_not_break_json_terminal_replay(self) -> None:
        terminal = {"schema_version": "fixture.json.v1", "status": "ok"}
        grok_precommit = fixture_grok_session_precommit()
        raw = raw_grok_session(
            terminal,
            calls=[("x_keyword_search", {"query": "fixture", "limit": "1", "mode": "Top"})],
        )
        replay = replay_grok_operator_session(
            raw,
            session_precommit=grok_precommit,
            allowed_tool_names=frozenset({"x_keyword_search"}),
            expected_terminal=terminal,
        )
        self.assertEqual(replay.terminal, terminal)

    def test_url_author_binding_collision_and_block_order_fail_closed(self) -> None:
        manifest, plan, _, _, grok_precommit = _fixture_batch()
        calls = plan["batches"][0]["calls"]
        wrong_author_blocks = [
            [f"https://x.invalid/{calls[1]['expected_author_handle']}/status/{30000 + index}"]
            for index in range(1, 7)
        ]
        terminal = render_flat_terminal(wrong_author_blocks)
        mapping_precommit = build_session_precommit(plan=plan, batch_index=0, grok_precommit=grok_precommit)
        rejected = replay_flat_session(
            mapping_precommit=mapping_precommit,
            terminal_text=terminal,
            lab_descriptor=manifest["lab_descriptor"],
            raw_session_files=None,
            grok_precommit=None,
        )
        self.assertEqual(rejected["errors"], ["flat_projection_invalid"])
        reordered = terminal.replace("BEGIN_CALL_0001_URLS", "BEGIN_CALL_9999_URLS", 1)
        rejected = replay_flat_session(
            mapping_precommit=mapping_precommit,
            terminal_text=reordered,
            lab_descriptor=manifest["lab_descriptor"],
            raw_session_files=None,
            grok_precommit=None,
        )
        self.assertEqual(rejected["references"], [])

    def test_request_limit_equality_is_only_saturation_lower_bound(self) -> None:
        manifest, plan, _, _, grok_precommit = _fixture_batch()
        calls = plan["batches"][0]["calls"]
        blocks = [
            [
                f"https://x.invalid/{call['expected_author_handle']}/status/{ordinal * 1000 + index}"
                for index in range(10)
            ]
            if ordinal == 1
            else []
            for ordinal, call in enumerate(calls, 1)
        ]
        _, _, terminal, raw, grok_precommit = _fixture_batch(terminal_blocks=blocks)
        receipt = replay_flat_session(
            mapping_precommit=build_session_precommit(plan=plan, batch_index=0, grok_precommit=grok_precommit),
            terminal_text=terminal,
            lab_descriptor=manifest["lab_descriptor"],
            raw_session_files=raw,
            grok_precommit=grok_precommit,
        )
        self.assertEqual(receipt["saturation_lower_bound_count"], 1)
        seed = receipt["saturation_queue_seeds"][0]
        self.assertEqual(seed["reason"], "request_limit_equal_is_lower_bound_only")
        topic_children = expand_saturation_item(seed, load_policy())
        self.assertEqual(len(topic_children), 2)
        self.assertTrue(all(row["next_child_rule"] == "mode" for row in topic_children))
        mode_child = expand_saturation_item(topic_children[0], load_policy())[0]
        self.assertEqual(mode_child["mode"], "Latest")
        self.assertEqual(mode_child["next_child_rule"], "time")
        time_children = expand_saturation_item(mode_child, load_policy())
        self.assertEqual(len(time_children), 2)
        self.assertTrue(all(row["next_child_rule"] == "closed" for row in time_children))

    def test_frontier_queues_scale_past_one_hundred_refs_and_challengers_are_never_self_evidence(self) -> None:
        manifest = _large_manifest(121)
        references = [
            {
                "candidate_ref": row["candidate_ref"],
                "stable_post_id": str(8_000_000 + index),
                "url": f"https://x.invalid/{row['current_handle']}/status/{8_000_000 + index}",
                "author_handle": row["current_handle"],
                "source_status": "model_mediated_url_from_verified_native_x_session",
                "observed_call_ordinals": [index],
            }
            for index, row in enumerate(manifest["candidates"], 1)
        ]
        receipt = _accepted_receipt(references)
        queues = build_frontier_queues(manifest=manifest, accepted_receipts=[receipt], policy=load_policy())
        self.assertEqual(len(queues["thread_hydration_queue"]), 121)
        self.assertEqual(queues["luna_input_queue"], [])
        self.assertGreater(len(queues["challenger_queue"]), 0)
        self.assertTrue(
            all(row["self_evidence_allowed"] is False for row in queues["challenger_queue"])
        )
        self.assertTrue(
            all(row["relationship"] == "official_or_third_party_non_self" for row in queues["challenger_queue"])
        )

    def test_frontier_rejects_receipt_hash_unknown_candidates_and_cross_batch_post_collisions(self) -> None:
        manifest = _manifest()
        row = manifest["candidates"][0]
        reference = {
            "candidate_ref": row["candidate_ref"],
            "stable_post_id": "880001",
            "url": f"https://x.invalid/{row['current_handle']}/status/880001",
            "author_handle": row["current_handle"],
            "source_status": "model_mediated_url_from_verified_native_x_session",
            "observed_call_ordinals": [1],
        }
        tampered = _accepted_receipt([reference])
        tampered["terminal_sha256"] = "e" * 64
        with self.assertRaisesRegex(SourceNeutralMappingError, "frontier_receipt_not_accepted"):
            build_frontier_queues(manifest=manifest, accepted_receipts=[tampered], policy=load_policy())

        unknown = copy.deepcopy(reference)
        unknown["candidate_ref"] = "fixture_candidate_unknown"
        with self.assertRaisesRegex(SourceNeutralMappingError, "candidate_unknown"):
            build_frontier_queues(
                manifest=manifest,
                accepted_receipts=[_accepted_receipt([unknown])],
                policy=load_policy(),
            )

        other = manifest["candidates"][1]
        collision = copy.deepcopy(reference)
        collision.update(
            {
                "candidate_ref": other["candidate_ref"],
                "url": f"https://x.invalid/{other['current_handle']}/status/880001",
                "author_handle": other["current_handle"],
            }
        )
        with self.assertRaisesRegex(SourceNeutralMappingError, "cross_candidate"):
            build_frontier_queues(
                manifest=manifest,
                accepted_receipts=[
                    _accepted_receipt([reference], receipt_salt="1"),
                    _accepted_receipt([collision], receipt_salt="2"),
                ],
                policy=load_policy(),
            )

    def test_exact_hydration_is_required_before_luna_queue(self) -> None:
        hydration_queue = [
            {
                "candidate_ref": "fixture_candidate_001",
                "stable_post_id": "12345",
                "url": "https://x.invalid/fixturemap001/status/12345",
                "queue_reason": "exact_thread_hydration_required",
            }
        ]
        text = "Synthetic source-bound technical Post."
        exact = [
            {
                "candidate_ref": "fixture_candidate_001",
                "stable_post_id": "12345",
                "source_url": "https://x.invalid/fixturemap001/status/12345",
                "author_handle": "fixturemap001",
                "full_text": text,
                "full_text_sha256": canonical_sha256(text),
                "hydration_status": "exact_source_bound",
            }
        ]
        queue = build_luna_input_queue(thread_hydration_queue=hydration_queue, exact_hydrations=exact)
        self.assertEqual(queue[0]["source_binding_status"], "exact_source_bound")
        tampered = copy.deepcopy(exact)
        tampered[0]["full_text"] += " tampered"
        with self.assertRaisesRegex(SourceNeutralMappingError, "text_hash_mismatch"):
            build_luna_input_queue(thread_hydration_queue=hydration_queue, exact_hydrations=tampered)

        wrong_source = copy.deepcopy(exact)
        wrong_source[0]["source_url"] = "https://x.invalid/someone_else/status/12345"
        with self.assertRaisesRegex(SourceNeutralMappingError, "source_url_binding_invalid"):
            build_luna_input_queue(
                thread_hydration_queue=hydration_queue,
                exact_hydrations=wrong_source,
            )

        wrong_author = copy.deepcopy(exact)
        wrong_author[0]["author_handle"] = "someone_else"
        with self.assertRaisesRegex(SourceNeutralMappingError, "source_url_binding_invalid"):
            build_luna_input_queue(
                thread_hydration_queue=hydration_queue,
                exact_hydrations=wrong_author,
            )

    def test_structural_stop_requires_empty_queues_and_two_distinct_double_zero_waves(self) -> None:
        empty = {
            key: []
            for key in (
                "saturation_queue",
                "thread_hydration_queue",
                "luna_input_queue",
                "challenger_queue",
            )
        }
        waves = [
            {
                "wave_id": "wave_a",
                "strategy_signature_sha256": "a" * 64,
                "new_stable_post_id_count": 0,
                "luna_qualified_state_upgrade_count": 0,
            },
            {
                "wave_id": "wave_b",
                "strategy_signature_sha256": "b" * 64,
                "new_stable_post_id_count": 0,
                "luna_qualified_state_upgrade_count": 0,
            },
        ]
        self.assertTrue(structural_stop(queues=empty, recent_waves=waves, policy=load_policy())["stop"])
        nonempty = copy.deepcopy(empty)
        nonempty["luna_input_queue"] = [{}]
        self.assertFalse(structural_stop(queues=nonempty, recent_waves=waves, policy=load_policy())["stop"])
        waves[1]["luna_qualified_state_upgrade_count"] = 1
        self.assertFalse(structural_stop(queues=empty, recent_waves=waves, policy=load_policy())["stop"])

        forged_boolean = copy.deepcopy(waves)
        forged_boolean[1]["luna_qualified_state_upgrade_count"] = False
        self.assertFalse(
            structural_stop(queues=empty, recent_waves=forged_boolean, policy=load_policy())["stop"]
        )

        malformed_empty = copy.deepcopy(empty)
        malformed_empty["challenger_queue"] = ""
        self.assertFalse(
            structural_stop(queues=malformed_empty, recent_waves=waves, policy=load_policy())["stop"]
        )

    def test_structural_stop_derives_distinctness_and_rejects_flags_or_malformed_hashes(self) -> None:
        empty = {
            key: []
            for key in (
                "saturation_queue",
                "thread_hydration_queue",
                "luna_input_queue",
                "challenger_queue",
            )
        }
        waves = [
            {
                "wave_id": "wave_a",
                "strategy_signature_sha256": "a" * 64,
                "new_stable_post_id_count": 0,
                "luna_qualified_state_upgrade_count": 0,
            },
            {
                "wave_id": "wave_b",
                "strategy_signature_sha256": "a" * 64,
                "new_stable_post_id_count": 0,
                "luna_qualified_state_upgrade_count": 0,
            },
        ]
        self.assertFalse(structural_stop(queues=empty, recent_waves=waves, policy=load_policy())["stop"])
        waves[1]["strategy_signature_sha256"] = "not-a-sha"
        self.assertFalse(structural_stop(queues=empty, recent_waves=waves, policy=load_policy())["stop"])
        waves[1]["strategy_signature_sha256"] = "b" * 64
        waves[1]["materially_distinct_from_previous"] = True
        self.assertFalse(structural_stop(queues=empty, recent_waves=waves, policy=load_policy())["stop"])
        waves[1]["materially_distinct_from_previous"] = False
        self.assertFalse(structural_stop(queues=empty, recent_waves=waves, policy=load_policy())["stop"])

    def test_candidate_free_aggregate_has_exact_four_denominators_and_hash_bindings(self) -> None:
        bindings = {
            "policy_sha256": "1" * 64,
            "candidate_manifest_sha256": "2" * 64,
            "wave_plan_sha256": "3" * 64,
            "session_receipt_manifest_sha256": "4" * 64,
            "hydration_receipt_manifest_sha256": "5" * 64,
            "luna_result_manifest_sha256": "6" * 64,
        }
        aggregate = build_candidate_free_aggregate(
            bindings=bindings,
            planned_native_x_calls=10,
            attested_completed_native_x_calls=8,
            unique_stable_post_ids=6,
            exact_source_bound_hydrations=5,
            luna_terminal_reviews=5,
            luna_qualified_state_upgrades=2,
        )
        assert_schema_valid(aggregate, AGGREGATE_SCHEMA_FILE)
        self.assertEqual(
            list(aggregate["metric_denominators"]),
            [
                "execution_compliance",
                "stable_post_id_retrieval",
                "exact_hydration",
                "luna_qualified_state_upgrades",
            ],
        )
        self.assertFalse(aggregate["model_call_counts_included"])
        serialized = json.dumps(aggregate).casefold()
        for token in ("candidate_ref", "handle", "profile_url", "source_text", "excerpt"):
            self.assertNotIn(token, serialized)

    def test_calibration_binding_is_candidate_free_hash_bound_and_not_plateau_claim(self) -> None:
        aggregate = strict_load_json(CALIBRATION_PATH)
        assert_schema_valid(aggregate, CALIBRATION_SCHEMA_FILE)
        self.assertEqual(
            aggregate["aggregate_sha256"],
            canonical_sha256({key: value for key, value in aggregate.items() if key != "aggregate_sha256"}),
        )
        self.assertEqual(aggregate["calibration_counts"]["actual_native_x_calls"], 46)
        self.assertEqual(aggregate["batch_canary"]["candidate_grain"], 3)
        self.assertEqual(aggregate["conclusion"], "method_shape_calibrated_not_global_recall_plateau")
        self.assertFalse(aggregate["candidate_fields_included"])
        self.assertFalse(aggregate["candidate_text_included"])

    def test_private_calibration_binding_digest_is_mechanically_derived(self) -> None:
        aggregate = strict_load_json(CALIBRATION_PATH)
        source_bindings = [{"artifact": "synthetic.invalid/a", "sha256": "a" * 64}]
        summary_bytes = b'{"candidate_fields_included":false}'
        receipt = {
            "builder_source_sha256": "b" * 64,
            "candidate_fields_in_summary": False,
            "raw_files_modified": False,
            "schema_version": "x_first.gdm_query_family_calibration_receipt.v1",
            "source_binding_count": 1,
            "source_bindings": source_bindings,
            "summary_sha256": hashlib.sha256(summary_bytes).hexdigest(),
        }
        receipt_bytes = json.dumps(receipt, indent=2, sort_keys=True).encode() + b"\n"
        synthetic = copy.deepcopy(aggregate)
        synthetic["source_bindings"] = {
            "private_receipt_sha256": hashlib.sha256(receipt_bytes).hexdigest(),
            "private_source_binding_count": 1,
            "private_source_binding_manifest_sha256": canonical_sha256(source_bindings),
            "private_summary_sha256": hashlib.sha256(summary_bytes).hexdigest(),
        }
        with tempfile.TemporaryDirectory() as directory:
            receipt_path = Path(directory) / "receipt.json"
            summary_path = Path(directory) / "summary.json"
            receipt_path.write_bytes(receipt_bytes)
            summary_path.write_bytes(summary_bytes)
            validate_private_calibration_binding(
                calibration=synthetic,
                private_receipt_path=receipt_path,
                private_summary_path=summary_path,
            )
            synthetic["source_bindings"]["private_source_binding_manifest_sha256"] = "f" * 64
            with self.assertRaisesRegex(SourceNeutralMappingError, "source_binding_invalid"):
                validate_private_calibration_binding(
                    calibration=synthetic,
                    private_receipt_path=receipt_path,
                    private_summary_path=summary_path,
                )

    def test_duplicate_json_keys_fail_closed_and_checked_assets_validate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "duplicate.json"
            path.write_text('{"schema_version":"a","schema_version":"b"}', encoding="utf-8")
            with self.assertRaisesRegex(SourceNeutralMappingError, "duplicate_json_key"):
                strict_load_json(path)
        self.assertEqual(validate_checked_in_assets(), [])


if __name__ == "__main__":
    unittest.main()
