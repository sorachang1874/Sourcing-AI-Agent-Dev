from __future__ import annotations

import copy
import hashlib
import json
import tempfile
import unittest
from dataclasses import replace
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
    ExactPostHydrationProjection,
    ExecutedWaveFacts,
    SourceNeutralMappingError,
    build_candidate_free_aggregate,
    build_exact_post_hydration_projection,
    build_executed_wave_facts,
    build_frontier_queues,
    build_luna_input_queue,
    build_luna_state_review_projection,
    build_mapping_session_projection,
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
CALIBRATION_PATH = ROOT / "docs" / "live-evidence" / "2026-07-17-gdm-query-family-calibration.aggregate.v1.json"


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
        manifest=manifest,
        policy=policy,
        batch_index=0,
        grok_precommit=grok_precommit,
    )
    if terminal_blocks is None:
        terminal_blocks = [
            [f"https://x.invalid/{call['expected_author_handle']}/status/{10000 + ordinal}"]
            for ordinal, call in enumerate(batch["calls"], 1)
        ]
    terminal = render_flat_terminal(terminal_blocks)
    calls = raw_call_override or [(call["tool_name"], call["arguments"]) for call in mapping_precommit["calls"]]
    raw = raw_grok_session(
        {"ignored_by_literal_flat_replay": True},
        calls=calls,
        user_prompt_text=prompt,
        terminal_text_override=terminal,
        terminal_before_tools=terminal_before_tools,
    )
    return manifest, plan, terminal, raw, grok_precommit


def _projection_for_batch(
    manifest: dict,
    plan: dict,
    batch_index: int,
    *,
    terminal_blocks: list[list[str]] | None = None,
    terminal_before_tools: bool = False,
    include_references: bool = True,
    references_per_candidate: int = 1,
    session_namespace: str = "fixture-map",
    policy: dict | None = None,
) -> object:
    policy = policy or load_policy()
    batch = plan["batches"][batch_index]
    marker = f"MAPPING_BATCH_SHA256={batch['batch_sha256']}"
    prompt = f"Synthetic flat mapping session.\n{marker}"
    session_id = f"{session_namespace}-session-{batch_index + 1:04d}"
    request_id = f"{session_namespace}-request-{batch_index + 1:04d}"
    grok_precommit = fixture_grok_session_precommit(
        session_id=session_id,
        request_id=request_id,
        user_prompt_text=prompt,
    )
    mapping_precommit = build_session_precommit(
        plan=plan,
        manifest=manifest,
        policy=policy,
        batch_index=batch_index,
        grok_precommit=grok_precommit,
    )
    if terminal_blocks is None:
        terminal_blocks = []
        seen_candidates: set[str] = set()
        for call in batch["calls"]:
            if not include_references or call["candidate_ref"] in seen_candidates:
                terminal_blocks.append([])
                continue
            seen_candidates.add(call["candidate_ref"])
            stable_base = 8_000_000 + call["global_call_ordinal"] * 10
            terminal_blocks.append(
                [
                    f"https://x.invalid/{call['expected_author_handle']}/status/{stable_base + offset}"
                    for offset in range(references_per_candidate)
                ]
            )
    terminal = render_flat_terminal(terminal_blocks)
    raw = raw_grok_session(
        {"ignored_by_literal_flat_replay": True},
        calls=[(call["tool_name"], call["arguments"]) for call in batch["calls"]],
        session_id=session_id,
        request_id=request_id,
        user_prompt_text=prompt,
        terminal_text_override=terminal,
        terminal_before_tools=terminal_before_tools,
    )
    return build_mapping_session_projection(
        mapping_precommit=mapping_precommit,
        terminal_text=terminal,
        lab_descriptor=manifest["lab_descriptor"],
        raw_session_files=raw,
        grok_precommit=grok_precommit,
    )


def _all_session_projections(
    manifest: dict,
    plan: dict,
    *,
    include_references: bool = True,
    references_per_candidate: int = 1,
    session_namespace: str = "fixture-map",
    policy: dict | None = None,
) -> list[object]:
    return [
        _projection_for_batch(
            manifest,
            plan,
            index,
            include_references=include_references,
            references_per_candidate=references_per_candidate,
            session_namespace=session_namespace,
            policy=policy,
        )
        for index in range(len(plan["batches"]))
    ]


def _hydration_projection(
    task: dict,
    index: int = 1,
    *,
    session_namespace: str = "fixture-hydration",
) -> ExactPostHydrationProjection:
    text = f"Synthetic source-bound technical Post {index}."
    terminal = {
        "schema_version": "x.source_neutral.mapping.exact_post_hydration.v1",
        "candidate_ref": task["candidate_ref"],
        "requested_stable_post_id": task["stable_post_id"],
        "returned_stable_post_id": task["stable_post_id"],
        "source_url": task["url"],
        "author_handle": task["expected_author_handle"],
        "full_text": text,
        "full_text_sha256": hashlib.sha256(text.encode()).hexdigest(),
        "lookup_status": "matched",
    }
    session_id = f"{session_namespace}-session-{index:04d}"
    request_id = f"{session_namespace}-request-{index:04d}"
    precommit = fixture_grok_session_precommit(
        session_id=session_id,
        request_id=request_id,
        user_prompt_text=f"Hydrate {task['stable_post_id']}",
    )
    raw = raw_grok_session(
        terminal,
        calls=[("x_thread_fetch", {"post_id": task["stable_post_id"]})],
        session_id=session_id,
        request_id=request_id,
        user_prompt_text=f"Hydrate {task['stable_post_id']}",
    )
    return build_exact_post_hydration_projection(
        task=task,
        terminal=terminal,
        raw_session_files=raw,
        grok_precommit=precommit,
    )


def _luna_projection(
    hydration: ExactPostHydrationProjection,
    *,
    index: int,
    upgrade: bool = False,
    review_namespace: str = "fixture_luna_review",
):
    terminal = json.loads(hydration.terminal_json)
    result = {
        "schema_version": "x.source_neutral.mapping.luna_state_review.v1",
        "review_id": f"{review_namespace}_{index:04d}",
        "candidate_ref": terminal["candidate_ref"],
        "stable_post_id": terminal["returned_stable_post_id"],
        "hydration_projection_sha256": hydration.projection_sha256,
        "source_text_sha256": hashlib.sha256(hydration.source_text).hexdigest(),
        "terminal_status": "reviewed",
        "lab_affiliation_state": "current",
        "pretraining_experience_state": "current",
        "qualified_state_upgrade": upgrade,
        "upgrade_id": f"fixture_upgrade_{index:04d}" if upgrade else None,
    }
    return build_luna_state_review_projection(hydration_projection=hydration, result=result)


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
            {(row["lab_affiliation_prior"]["state"], row["pretraining_experience_prior"]["state"]) for row in axes},
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

    def test_plan_validation_reconstructs_every_policy_owned_call_and_batch_field(self) -> None:
        manifest = _manifest()
        policy = load_policy()
        plan = plan_wave_p(manifest, policy, plan_id="fixture_exact_reconstruction_v1")
        mutations = {
            "candidate_ref": lambda call: call.__setitem__("candidate_ref", manifest["candidates"][3]["candidate_ref"]),
            "expected_author_handle": lambda call: call.__setitem__("expected_author_handle", "unrelated"),
            "batch_candidate_ordinal": lambda call: call.__setitem__("batch_candidate_ordinal", 2),
            "champion_cell_id": lambda call: call.__setitem__("champion_cell_id", "architecture_latest"),
            "concept_alias_group_id": lambda call: call.__setitem__("concept_alias_group_id", "architecture_latest"),
            "topic_aliases": lambda call: call.__setitem__("topic_aliases", ["attention"]),
            "tool_name": lambda call: call.__setitem__("tool_name", "x_user_search"),
            "query": lambda call: call["arguments"].__setitem__("query", "from:unrelated (attention)"),
            "limit": lambda call: call["arguments"].__setitem__("limit", "9"),
            "mode": lambda call: call["arguments"].__setitem__("mode", "Latest"),
        }
        for label, mutate in mutations.items():
            with self.subTest(label=label):
                changed = copy.deepcopy(plan)
                mutate(changed["batches"][0]["calls"][0])
                batch = changed["batches"][0]
                batch["batch_sha256"] = canonical_sha256(
                    {key: value for key, value in batch.items() if key != "batch_sha256"}
                )
                changed["plan_sha256"] = canonical_sha256(
                    {key: value for key, value in changed.items() if key != "plan_sha256"}
                )
                with self.assertRaises(SourceNeutralMappingError):
                    validate_wave_plan(changed, manifest=manifest, policy=policy)

        changed = copy.deepcopy(plan)
        changed["batches"][0]["calls"][0]["arguments"]["query"] = "from:unrelated (attention)"
        changed["batches"][0]["batch_sha256"] = canonical_sha256(
            {key: value for key, value in changed["batches"][0].items() if key != "batch_sha256"}
        )
        changed["plan_sha256"] = canonical_sha256(
            {key: value for key, value in changed.items() if key != "plan_sha256"}
        )
        marker = f"MAPPING_BATCH_SHA256={changed['batches'][0]['batch_sha256']}"
        grok_precommit = fixture_grok_session_precommit(user_prompt_text=marker)
        with self.assertRaisesRegex(SourceNeutralMappingError, "exact_policy_reconstruction"):
            build_session_precommit(
                plan=changed,
                manifest=manifest,
                policy=policy,
                batch_index=0,
                grok_precommit=grok_precommit,
            )

        regrouped = copy.deepcopy(plan)
        regrouped["batches"][0]["candidate_refs"], regrouped["batches"][1]["candidate_refs"] = (
            regrouped["batches"][1]["candidate_refs"],
            regrouped["batches"][0]["candidate_refs"],
        )
        for batch in regrouped["batches"][:2]:
            batch["candidate_refs_sha256"] = canonical_sha256(batch["candidate_refs"])
            batch["batch_sha256"] = canonical_sha256(
                {key: value for key, value in batch.items() if key != "batch_sha256"}
            )
        regrouped["plan_sha256"] = canonical_sha256(
            {key: value for key, value in regrouped.items() if key != "plan_sha256"}
        )
        with self.assertRaises(SourceNeutralMappingError):
            validate_wave_plan(regrouped, manifest=manifest, policy=policy)

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
            manifest=manifest,
            policy=load_policy(),
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
        mapping_precommit = build_session_precommit(
            plan=plan,
            manifest=manifest,
            policy=load_policy(),
            batch_index=0,
            grok_precommit=grok_precommit,
        )
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
        self.assertEqual(receipt["planned_native_x_call_count"], 6)

        invalid_precommit = copy.deepcopy(mapping_precommit)
        invalid_precommit["precommit_sha256"] = "invalid"
        rejected = replay_flat_session(
            mapping_precommit=invalid_precommit,
            terminal_text=terminal,
            lab_descriptor=manifest["lab_descriptor"],
            raw_session_files=None,
            grok_precommit=None,
        )
        self.assertIsNone(rejected["planned_native_x_call_count"])

    def test_raw_replay_rejects_pretool_terminal_wrong_tool_and_argument_drift(self) -> None:
        manifest, plan, terminal, raw, grok_precommit = _fixture_batch(terminal_before_tools=True)
        mapping_precommit = build_session_precommit(
            plan=plan,
            manifest=manifest,
            policy=load_policy(),
            batch_index=0,
            grok_precommit=grok_precommit,
        )
        before = replay_flat_session(
            mapping_precommit=mapping_precommit,
            terminal_text=terminal,
            lab_descriptor=manifest["lab_descriptor"],
            raw_session_files=raw,
            grok_precommit=grok_precommit,
        )
        self.assertEqual(before["errors"], ["raw_six_file_replay_invalid"])

        wrong_calls = [("x_user_search", {"query": "fixture", "count": "1"})]
        wrong_calls.extend((call["tool_name"], call["arguments"]) for call in mapping_precommit["calls"][1:])
        _, _, terminal, raw, grok_precommit = _fixture_batch(raw_call_override=wrong_calls)
        wrong_tool = replay_flat_session(
            mapping_precommit=build_session_precommit(
                plan=plan,
                manifest=manifest,
                policy=load_policy(),
                batch_index=0,
                grok_precommit=grok_precommit,
            ),
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
            mapping_precommit=build_session_precommit(
                plan=plan,
                manifest=manifest,
                policy=load_policy(),
                batch_index=0,
                grok_precommit=grok_precommit,
            ),
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
            [f"https://x.invalid/{calls[1]['expected_author_handle']}/status/{30000 + index}"] for index in range(1, 7)
        ]
        terminal = render_flat_terminal(wrong_author_blocks)
        mapping_precommit = build_session_precommit(
            plan=plan,
            manifest=manifest,
            policy=load_policy(),
            batch_index=0,
            grok_precommit=grok_precommit,
        )
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
            mapping_precommit=build_session_precommit(
                plan=plan,
                manifest=manifest,
                policy=load_policy(),
                batch_index=0,
                grok_precommit=grok_precommit,
            ),
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
        policy = load_policy()
        plan = plan_wave_p(manifest, policy, plan_id="fixture_large_frontier_v1")
        projections = _all_session_projections(manifest, plan)
        queues = build_frontier_queues(
            manifest=manifest,
            plan=plan,
            session_projections=projections,
            policy=policy,
        )
        self.assertEqual(len(queues["thread_hydration_queue"]), 121)
        self.assertEqual(queues["pending_wave_p_queue"], [])
        self.assertEqual(queues["retry_split_queue"], [])
        self.assertEqual(queues["luna_input_queue"], [])
        self.assertGreater(len(queues["challenger_queue"]), 0)
        self.assertTrue(all(row["self_evidence_allowed"] is False for row in queues["challenger_queue"]))
        self.assertTrue(
            all(row["relationship"] == "official_or_third_party_non_self" for row in queues["challenger_queue"])
        )

    def test_frontier_replays_typed_sources_and_separates_unexecuted_rejected_from_sparse(self) -> None:
        manifest = _manifest()
        policy = load_policy()
        plan = plan_wave_p(manifest, policy, plan_id="fixture_coverage_states_v1")
        accepted = _projection_for_batch(manifest, plan, 0)
        rejected = _projection_for_batch(manifest, plan, 1, terminal_before_tools=True)
        queues = build_frontier_queues(
            manifest=manifest,
            plan=plan,
            session_projections=[accepted, rejected],
            policy=policy,
        )
        self.assertEqual(len(queues["retry_split_queue"]), 1)
        self.assertEqual(len(queues["pending_wave_p_queue"]), len(plan["batches"]) - 2)
        covered = set(plan["batches"][0]["candidate_refs"])
        self.assertTrue(all(row["candidate_ref"] in covered for row in queues["challenger_queue"]))
        self.assertTrue(
            all(row["challenger_type"] != "thread" or row["seed_stable_post_ids"] for row in queues["challenger_queue"])
        )
        zero_reference = _projection_for_batch(
            manifest,
            plan,
            0,
            include_references=False,
        )
        zero_queues = build_frontier_queues(
            manifest=manifest,
            plan=plan,
            session_projections=[zero_reference],
            policy=policy,
        )
        self.assertGreater(len(zero_queues["challenger_queue"]), 0)
        self.assertNotIn(
            "thread",
            {row["challenger_type"] for row in zero_queues["challenger_queue"]},
        )
        forged = replace(accepted, receipt_sha256="e" * 64)
        with self.assertRaisesRegex(SourceNeutralMappingError, "receipt_mismatch"):
            build_frontier_queues(
                manifest=manifest,
                plan=plan,
                session_projections=[forged],
                policy=policy,
            )

    def test_exact_hydration_is_required_before_luna_queue(self) -> None:
        manifest = _manifest()
        policy = load_policy()
        plan = plan_wave_p(manifest, policy, plan_id="fixture_hydration_lane_v1")
        sessions = _all_session_projections(manifest, plan)
        frontier = build_frontier_queues(
            manifest=manifest,
            plan=plan,
            session_projections=sessions,
            policy=policy,
        )
        hydration = _hydration_projection(frontier["thread_hydration_queue"][0])
        queue = build_luna_input_queue(
            manifest=manifest,
            plan=plan,
            session_projections=sessions,
            hydration_projections=[hydration],
            policy=policy,
        )
        self.assertEqual(queue[0]["source_binding_status"], "exact_source_bound")
        self.assertEqual(queue[0]["source_text"].encode(), hydration.source_text)
        with self.assertRaisesRegex(SourceNeutralMappingError, "projection_required"):
            build_luna_input_queue(
                manifest=manifest,
                plan=plan,
                session_projections=sessions,
                hydration_projections=[{"hydration_status": "exact_source_bound"}],
                policy=policy,
            )
        tampered = replace(hydration, source_text=hydration.source_text + b" tampered")
        with self.assertRaisesRegex(SourceNeutralMappingError, "content_mismatch"):
            build_luna_input_queue(
                manifest=manifest,
                plan=plan,
                session_projections=sessions,
                hydration_projections=[tampered],
                policy=policy,
            )

    def test_structural_stop_requires_empty_queues_and_two_distinct_double_zero_waves(self) -> None:
        source_manifest = _manifest()
        candidates = copy.deepcopy(source_manifest["candidates"])
        for candidate in candidates:
            candidate["lab_affiliation_prior"] = {
                "state": "current",
                "evidence_status": "fixture_asserted",
            }
            candidate["pretraining_experience_prior"] = {
                "state": "current",
                "evidence_status": "fixture_asserted",
            }
        manifest = freeze_candidate_manifest(
            manifest_id="fixture_structural_stop_manifest_v1",
            lab_descriptor=source_manifest["lab_descriptor"],
            candidates=candidates,
        )
        top_policy = load_policy()
        latest_policy = copy.deepcopy(top_policy)
        for cell in latest_policy["wave_p"]["champion_cells"]:
            cell["mode"] = "Latest"
        validate_policy(latest_policy)

        def facts(
            *,
            wave_id: str,
            plan_id: str,
            policy: dict,
            namespace: str,
        ) -> ExecutedWaveFacts:
            plan = plan_wave_p(manifest, policy, plan_id=plan_id)
            sessions = _all_session_projections(
                manifest,
                plan,
                references_per_candidate=2,
                session_namespace=namespace,
                policy=policy,
            )
            frontier = build_frontier_queues(
                manifest=manifest,
                policy=policy,
                plan=plan,
                session_projections=sessions,
            )
            hydrations = [
                _hydration_projection(
                    task,
                    index,
                    session_namespace=f"{namespace}-hydration",
                )
                for index, task in enumerate(frontier["thread_hydration_queue"], 1)
            ]
            luna = [
                _luna_projection(
                    hydration,
                    index=index,
                    review_namespace=f"{namespace}_luna",
                )
                for index, hydration in enumerate(hydrations, 1)
            ]
            return build_executed_wave_facts(
                wave_id=wave_id,
                manifest=manifest,
                policy=policy,
                plan=plan,
                strategy_payload={
                    "strategy_id": plan["plan_id"],
                    "strategy_family": "wave_p",
                    "plan_sha256": plan["plan_sha256"],
                    "query_surface": "candidate_authored",
                    "mode": policy["wave_p"]["champion_cells"][0]["mode"],
                    "time_window": None,
                },
                session_projections=sessions,
                hydration_projections=hydrations,
                luna_review_projections=luna,
                prior_stable_post_ids=[task["stable_post_id"] for task in frontier["thread_hydration_queue"]],
            )

        waves = [
            facts(
                wave_id="wave_a",
                plan_id="fixture_zero_top_plan_v1",
                policy=top_policy,
                namespace="fixture-zero-top",
            ),
            facts(
                wave_id="wave_b",
                plan_id="fixture_zero_latest_plan_v1",
                policy=latest_policy,
                namespace="fixture-zero-latest",
            ),
        ]
        empty = json.loads(waves[-1].remaining_queues_json)
        self.assertTrue(all(not empty[key] for key in latest_policy["queues"]["queue_order"]))
        self.assertTrue(structural_stop(queues=empty, recent_waves=waves, policy=latest_policy)["stop"])
        nonempty = copy.deepcopy(empty)
        nonempty["luna_input_queue"] = [{}]
        self.assertFalse(structural_stop(queues=nonempty, recent_waves=waves, policy=latest_policy)["stop"])
        forged = replace(waves[1], strategy_signature_sha256="c" * 64)
        self.assertFalse(structural_stop(queues=empty, recent_waves=[waves[0], forged], policy=latest_policy)["stop"])

        malformed_empty = copy.deepcopy(empty)
        malformed_empty["challenger_queue"] = ""
        self.assertFalse(structural_stop(queues=malformed_empty, recent_waves=waves, policy=latest_policy)["stop"])

        sparse_waves: list[ExecutedWaveFacts] = []
        for wave_id, plan_id, policy, namespace in (
            ("sparse_wave_a", "fixture_sparse_top_plan_v1", top_policy, "fixture-sparse-top"),
            (
                "sparse_wave_b",
                "fixture_sparse_latest_plan_v1",
                latest_policy,
                "fixture-sparse-latest",
            ),
        ):
            plan = plan_wave_p(manifest, policy, plan_id=plan_id)
            sessions = _all_session_projections(
                manifest,
                plan,
                include_references=False,
                session_namespace=namespace,
                policy=policy,
            )
            sparse_waves.append(
                build_executed_wave_facts(
                    wave_id=wave_id,
                    manifest=manifest,
                    policy=policy,
                    plan=plan,
                    strategy_payload={
                        "strategy_id": plan["plan_id"],
                        "strategy_family": "wave_p",
                        "plan_sha256": plan["plan_sha256"],
                        "query_surface": "candidate_authored",
                        "mode": policy["wave_p"]["champion_cells"][0]["mode"],
                        "time_window": None,
                    },
                    session_projections=sessions,
                    hydration_projections=[],
                    luna_review_projections=[],
                    prior_stable_post_ids=[],
                )
            )
        sparse_remaining = json.loads(sparse_waves[-1].remaining_queues_json)
        self.assertGreater(len(sparse_remaining["challenger_queue"]), 0)
        self.assertFalse(
            structural_stop(
                queues=empty,
                recent_waves=sparse_waves,
                policy=latest_policy,
            )["stop"]
        )

    def test_structural_stop_derives_distinctness_and_rejects_flags_or_malformed_hashes(self) -> None:
        empty = {key: [] for key in load_policy()["queues"]["queue_order"]}
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
        manifest = _manifest()
        policy = load_policy()
        plan = plan_wave_p(manifest, policy, plan_id="fixture_aggregate_v1")
        sessions = _all_session_projections(manifest, plan)
        frontier = build_frontier_queues(
            manifest=manifest,
            plan=plan,
            session_projections=sessions,
            policy=policy,
        )
        hydrations = [
            _hydration_projection(task, index) for index, task in enumerate(frontier["thread_hydration_queue"][:2], 1)
        ]
        luna = [_luna_projection(hydrations[0], index=1, upgrade=True)]
        aggregate = build_candidate_free_aggregate(
            manifest=manifest,
            policy=policy,
            plan=plan,
            session_projections=sessions,
            hydration_projections=hydrations,
            luna_review_projections=luna,
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
        self.assertEqual(aggregate["metric_numerators"]["exact_hydration"], 2)
        self.assertEqual(aggregate["metric_denominators"]["luna_qualified_state_upgrades"], 1)
        serialized = json.dumps(aggregate).casefold()
        for token in ("candidate_ref", "handle", "profile_url", "source_text", "excerpt"):
            self.assertNotIn(token, serialized)

    def test_aggregate_zero_propagation_is_derived_from_replayed_stage_coverage(self) -> None:
        manifest = _manifest()
        policy = load_policy()
        plan = plan_wave_p(manifest, policy, plan_id="fixture_zero_aggregate_v1")
        empty = build_candidate_free_aggregate(
            manifest=manifest,
            policy=policy,
            plan=plan,
            session_projections=[],
        )
        self.assertEqual(empty["metric_numerators"]["execution_compliance"], 0)
        self.assertEqual(empty["metric_numerators"]["stable_post_id_retrieval"], 0)
        self.assertEqual(empty["metric_numerators"]["exact_hydration"], 0)
        self.assertEqual(empty["metric_numerators"]["luna_qualified_state_upgrades"], 0)
        self.assertIsNone(empty["metric_rates"]["stable_post_id_retrieval"])

        sessions = _all_session_projections(manifest, plan)
        frontier = build_frontier_queues(
            manifest=manifest,
            plan=plan,
            session_projections=sessions,
            policy=policy,
        )
        hydration = _hydration_projection(frontier["thread_hydration_queue"][0])
        luna = _luna_projection(hydration, index=1, upgrade=True)
        with self.assertRaisesRegex(SourceNeutralMappingError, "hydration_binding"):
            build_candidate_free_aggregate(
                manifest=manifest,
                policy=policy,
                plan=plan,
                session_projections=sessions,
                hydration_projections=[],
                luna_review_projections=[luna],
            )

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
