from __future__ import annotations

import copy
import hashlib
import json
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from typing import Any
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import x_first.grok_cli_exploration as exploration_module  # noqa: E402
from x_first.grok_cli_exploration import (  # noqa: E402
    ExplorationValidationError,
    build_hydration_tasks,
    canonical_json,
    canonical_sha256,
    evaluate_exploration,
    validate_evaluation_output,
    validate_hydration_task,
)

SESSION_ID = "019a0000-0000-7000-8000-000000000001"
REQUEST_ID = "00000000-0000-4000-8000-000000000001"


class MiniSchemaError(AssertionError):
    pass


def _schema_pointer(root: dict[str, Any], pointer: str) -> Any:
    current: Any = root
    for part in pointer.removeprefix("#/").split("/"):
        current = current[part.replace("~1", "/").replace("~0", "~")]
    return current


def _mini_schema_validate(
    instance: Any,
    schema: dict[str, Any],
    *,
    root: dict[str, Any] | None = None,
    externals: dict[str, dict[str, Any]] | None = None,
    path: str = "$",
) -> None:
    root = schema if root is None else root
    externals = {} if externals is None else externals
    reference = schema.get("$ref")
    if isinstance(reference, str):
        if reference.startswith("#/"):
            target = _schema_pointer(root, reference)
            _mini_schema_validate(instance, target, root=root, externals=externals, path=path)
        else:
            target = externals.get(reference)
            if target is None:
                raise MiniSchemaError(f"{path}: unresolved ref {reference}")
            _mini_schema_validate(instance, target, root=target, externals=externals, path=path)

    if "oneOf" in schema:
        matches = 0
        for choice in schema["oneOf"]:
            try:
                _mini_schema_validate(instance, choice, root=root, externals=externals, path=path)
            except MiniSchemaError:
                continue
            matches += 1
        if matches != 1:
            raise MiniSchemaError(f"{path}: oneOf matched {matches}")

    expected_type = schema.get("type")
    expected_types = [expected_type] if isinstance(expected_type, str) else expected_type
    if expected_types is not None:
        matches_type = any(
            (name == "object" and isinstance(instance, dict))
            or (name == "array" and isinstance(instance, list))
            or (name == "string" and isinstance(instance, str))
            or (name == "integer" and type(instance) is int)
            or (name == "number" and type(instance) in {int, float})
            or (name == "boolean" and type(instance) is bool)
            or (name == "null" and instance is None)
            for name in expected_types
        )
        if not matches_type:
            raise MiniSchemaError(f"{path}: wrong type")
    if "const" in schema and instance != schema["const"]:
        raise MiniSchemaError(f"{path}: const mismatch")
    if "enum" in schema and instance not in schema["enum"]:
        raise MiniSchemaError(f"{path}: enum mismatch")
    if isinstance(instance, str):
        if "pattern" in schema and re.fullmatch(schema["pattern"], instance) is None:
            raise MiniSchemaError(f"{path}: pattern mismatch")
        if len(instance) < schema.get("minLength", 0) or len(instance) > schema.get("maxLength", len(instance)):
            raise MiniSchemaError(f"{path}: string length")
    if type(instance) in {int, float}:
        if instance < schema.get("minimum", instance) or instance > schema.get("maximum", instance):
            raise MiniSchemaError(f"{path}: numeric bound")
    if isinstance(instance, dict):
        required = schema.get("required", [])
        if any(key not in instance for key in required):
            raise MiniSchemaError(f"{path}: required key missing")
        properties = schema.get("properties", {})
        if schema.get("additionalProperties") is False and set(instance) - set(properties):
            raise MiniSchemaError(f"{path}: additional property")
        if len(instance) < schema.get("minProperties", 0) or len(instance) > schema.get("maxProperties", len(instance)):
            raise MiniSchemaError(f"{path}: property count")
        for key, value in instance.items():
            if key in properties:
                _mini_schema_validate(
                    value,
                    properties[key],
                    root=root,
                    externals=externals,
                    path=f"{path}.{key}",
                )
    if isinstance(instance, list):
        if len(instance) < schema.get("minItems", 0) or len(instance) > schema.get("maxItems", len(instance)):
            raise MiniSchemaError(f"{path}: item count")
        if schema.get("uniqueItems") and len({canonical_json(item) for item in instance}) != len(instance):
            raise MiniSchemaError(f"{path}: duplicate item")
        prefix = schema.get("prefixItems", [])
        for index, child_schema in enumerate(prefix):
            if index < len(instance):
                _mini_schema_validate(
                    instance[index],
                    child_schema,
                    root=root,
                    externals=externals,
                    path=f"{path}[{index}]",
                )
        items = schema.get("items")
        start = len(prefix) if prefix else 0
        if items is False and len(instance) > start:
            raise MiniSchemaError(f"{path}: extra items")
        if isinstance(items, dict):
            for index in range(start, len(instance)):
                _mini_schema_validate(
                    instance[index],
                    items,
                    root=root,
                    externals=externals,
                    path=f"{path}[{index}]",
                )


def _call(*, query: str = "OpenAI pretraining", tool_name: str = "x_keyword_search") -> dict[str, Any]:
    arguments = {
        "x_keyword_search": {"query": query, "limit": "10", "mode": "Latest"},
        "x_semantic_search": {"query": query, "limit": "10"},
        "x_user_search": {"query": query, "count": "3"},
        "x_thread_fetch": {"post_id": "123456"},
    }[tool_name]
    return {
        "tool_call_id": "ctc_fixture_call_0",
        "provider_call_id": "xs_fixture_call_0",
        "tool_name": tool_name,
        "arguments": arguments,
    }


def _public_policy() -> dict[str, Any]:
    return json.loads(
        (ROOT / "configs/grok_cli_exploration_query_policy_descriptor.v1.json").read_text()
    )


def _candidate_value_policy() -> dict[str, Any]:
    return json.loads((ROOT / "configs/candidate_value_segment_policy.v1.json").read_text())


def _public_query_policy_registry() -> dict[str, Any]:
    return json.loads(
        (
            ROOT
            / "configs/grok_cli_exploration_query_policy_registries/approved-query-policies-v1.json"
        ).read_text()
    )


def _pre_migration_evaluation_fixture() -> dict[str, Any]:
    return json.loads(
        (
            ROOT
            / "fixtures/grok_cli_exploration_pre_migration_evaluation.v1.json"
        ).read_text()
    )


def _canonical_calls() -> list[dict[str, Any]]:
    call_specs = [
        ("x_keyword_search", {"query": "OpenAI pretraining synthetic fixture", "limit": "10", "mode": "Latest"}),
        ("x_semantic_search", {"query": "OpenAI training team synthetic fixture", "limit": "10"}),
        ("x_semantic_search", {"query": "OpenAI training systems synthetic fixture", "limit": "10"}),
        ("x_keyword_search", {"query": "OpenAI tokenizer synthetic fixture", "limit": "10", "mode": "Top"}),
        ("x_user_search", {"query": "OpenAI researcher synthetic fixture", "count": "3"}),
        ("x_user_search", {"query": "pretraining engineer synthetic fixture", "count": "3"}),
        ("x_keyword_search", {"query": "OpenAI model training synthetic fixture", "limit": "10", "mode": "Latest"}),
        ("x_keyword_search", {"query": "OpenAI scaling synthetic fixture", "limit": "10", "mode": "Top"}),
    ]
    return [
        {
            "tool_call_id": f"ctc_fixture_call_{index}",
            "provider_call_id": f"xs_fixture_call_{index}",
            "tool_name": tool_name,
            "arguments": arguments,
        }
        for index, (tool_name, arguments) in enumerate(call_specs)
    ]


def _synthetic_legacy_full_policy() -> dict[str, Any]:
    return {
        "schema_version": "x.grok_cli.exploration.query_policy.legacy_full.v0",
        "policy_version": "synthetic-openai-pretrain-base-discovery-fixture-v1",
        "purpose": "base_researcher_discovery",
        "lab_id": "openai",
        "session_id": SESSION_ID,
        "request_id": REQUEST_ID,
        "query_manifest": [
            {
                "tool_name": call["tool_name"],
                "arguments": call["arguments"],
                "call_sha256": canonical_sha256(
                    {"tool_name": call["tool_name"], "arguments": call["arguments"]}
                ),
            }
            for call in _canonical_calls()
        ],
    }


def _canonical_policy() -> dict[str, Any]:
    return {
        "schema_version": "x.grok_cli.exploration.query_policy_descriptor.v1",
        "policy_version": "synthetic-openai-pretrain-base-discovery-fixture-v1",
        "purpose": "base_researcher_discovery",
        "lab_id": "openai",
        "run_binding_sha256": canonical_sha256(
            {"session_id": SESSION_ID, "request_id": REQUEST_ID}
        ),
        "legacy_full_policy_sha256": canonical_sha256(_synthetic_legacy_full_policy()),
        "allowed_decision_dimensions": [
            "lab_affiliation",
            "role_function",
            "pretraining_relevance",
        ],
        "professional_experience_proxy_query_allowed": False,
        "protected_identity_query_allowed": False,
        "query_manifest": [
            {
                "sequence": index,
                "tool_name": call["tool_name"],
                "call_sha256": canonical_sha256(
                    {"tool_name": call["tool_name"], "arguments": call["arguments"]}
                ),
            }
            for index, call in enumerate(_canonical_calls())
        ],
    }


def _query_policy_registry() -> dict[str, Any]:
    policy = _canonical_policy()
    return {
        "schema_version": "x.grok_cli.exploration.query_policy_registry.v1",
        "registry_version": "approved-query-policies-v1",
        "policies": [
            {
                "policy_version": policy["policy_version"],
                "policy_path": "configs/grok_cli_exploration_query_policy_descriptor.v1.json",
                "policy_sha256": canonical_sha256(policy),
                "legacy_full_policy_sha256": policy["legacy_full_policy_sha256"],
                "policy_schema_version": policy["schema_version"],
                "purpose": policy["purpose"],
                "lab_id": policy["lab_id"],
                "run_binding_sha256": policy["run_binding_sha256"],
                "enabled": True,
            }
        ],
    }


def _receipt(
    call: dict[str, Any] | None = None,
    calls: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    selected = calls if calls is not None else _canonical_calls() if call is None else [call]
    return {
        "schema_version": "x.grok_cli.exploration.tool_receipt.v0",
        "cli_version": "0.2.99",
        "model_id": "grok-4.5",
        "session_id": SESSION_ID,
        "request_id": REQUEST_ID,
        "generic_web_disabled_by_cli": True,
        "local_tools_removed_by_denylist": True,
        "evidence_boundary": (
            "tool names and queries are replayable; returned post bodies remain encrypted in provider context"
        ),
        "tool_counts": dict(Counter(item["tool_name"] for item in selected)),
        "calls": selected,
        "raw_session_sha256": {
            "chat_history.jsonl": "0" * 64,
            "events.jsonl": "1" * 64,
            "summary.json": "2" * 64,
            "updates.jsonl": "3" * 64,
        },
    }


def _evaluate(
    result: Any,
    receipt: Any,
    *,
    raw_session_directory: Path | None = None,
) -> dict[str, Any]:
    return evaluate_exploration(
        result,
        receipt,
        raw_session_directory=raw_session_directory,
    )


def _candidate(
    *,
    platform_user_id: str | None = "12345",
    include_post: bool = True,
    handle: str = "xfsynth_fixture",
    target_lab_affiliation_state: str = "current",
    pretraining_experience_state: str = "current",
    confidence: str = "high",
) -> dict[str, Any]:
    evidence = [
        {
            "kind": "bio",
            "relationship": "self",
            "author_handle": handle,
            "post_id": None,
            "url": f"https://x.com/{handle}",
            "published_at": None,
            "excerpt": "Synthetic OpenAI pretraining researcher Bio.",
            "supports": ["target_lab_affiliation_state", "pretraining_experience_state"],
        }
    ]
    if include_post:
        evidence.append(
            {
                "kind": "post",
                "relationship": "self",
                "author_handle": handle,
                "post_id": "123456",
                "url": f"https://x.com/{handle}/status/123456",
                "published_at": "2026-07-14T00:00:00Z",
                "excerpt": "Synthetic first-person pretraining statement.",
                "supports": ["pretraining_experience_state"],
            }
        )
    return {
        "handle": handle,
        "profile_url": f"https://x.com/{handle}",
        "platform_user_id": platform_user_id,
        "bio_excerpt": "Observed Bio: Synthetic OpenAI pretraining researcher Bio.",
        "target_lab_affiliation_state": target_lab_affiliation_state,
        "pretraining_experience_state": pretraining_experience_state,
        "confidence": confidence,
        "evidence": evidence,
        "caveats": ["Synthetic contract-only account; no network access."],
    }


def _result(
    call: dict[str, Any] | None = None,
    calls: list[dict[str, Any]] | None = None,
    candidate: dict[str, Any] | None = None,
    candidates: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    selected_calls = calls if calls is not None else _canonical_calls() if call is None else [call]
    selected_candidates = candidates if candidates is not None else [_candidate() if candidate is None else candidate]

    def query_label(selected_call: dict[str, Any]) -> str:
        if selected_call["tool_name"] == "x_keyword_search":
            annotation = f" [keyword/{selected_call['arguments']['mode']}]"
        else:
            annotation = {
                "x_semantic_search": " [semantic]",
                "x_user_search": " [user_search]",
                "x_thread_fetch": " [thread_fetch]",
            }[selected_call["tool_name"]]
        subject = selected_call["arguments"].get("query") or selected_call["arguments"].get("post_id")
        return subject + annotation

    return {
        "status": "X_SEARCH_OK",
        "status_reason": "Synthetic structurally valid exploration result.",
        "native_x_tool_provenance": {
            "generic_web_used": False,
            "tool_calls_reported": len(selected_calls),
            "tools_reported": sorted({item["tool_name"] for item in selected_calls}),
            "queries": [query_label(item) for item in selected_calls],
        },
        "counts": {
            "observations_inspected_reported": len(selected_candidates),
            "candidates_retained": len(selected_candidates),
        },
        "candidates": selected_candidates,
        "excluded_examples": [],
        "limitations": ["Synthetic contract test."],
        "local_reconciliation": {
            "candidate_records_validated": len(selected_candidates),
            "evidence_items_validated": sum(len(item["evidence"]) for item in selected_candidates),
            "post_urls_structurally_validated": sum(
                evidence["kind"] != "bio" for item in selected_candidates for evidence in item["evidence"]
            ),
            "provider_post_bodies_replayable": False,
            "tool_calls_completed": len(selected_calls),
            "tool_counts": dict(Counter(item["tool_name"] for item in selected_calls)),
        },
    }


def _canonical_payloads() -> tuple[dict[str, Any], dict[str, Any]]:
    return _result(), _receipt()


def _binding() -> dict[str, str]:
    registry = _query_policy_registry()
    registry_row = registry["policies"][0]
    return {
        "lab_id": "openai",
        "session_id": SESSION_ID,
        "request_id": REQUEST_ID,
        "model_id": "grok-4.5",
        "tool_policy_version": "x.grok_cli.exploration.tool_receipt.v0",
        "query_policy_version": _canonical_policy()["policy_version"],
        "query_policy_sha256": canonical_sha256(_canonical_policy()),
        "legacy_full_query_policy_sha256": _canonical_policy()["legacy_full_policy_sha256"],
        "query_policy_registry_version": registry["registry_version"],
        "query_policy_registry_sha256": canonical_sha256(registry),
        "query_policy_registry_row_sha256": canonical_sha256(registry_row),
        "candidate_value_policy_version": _candidate_value_policy()["policy_version"],
        "candidate_value_policy_sha256": canonical_sha256(_candidate_value_policy()),
    }


class GrokCliExplorationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._policy_workspace = tempfile.TemporaryDirectory()
        project_root = Path(cls._policy_workspace.name)
        configs = project_root / "configs"
        registry_directory = configs / "grok_cli_exploration_query_policy_registries"
        registry_directory.mkdir(parents=True)
        (configs / "grok_cli_exploration_query_policy_descriptor.v1.json").write_text(
            json.dumps(_canonical_policy())
        )
        (registry_directory / "approved-query-policies-v1.json").write_text(
            json.dumps(_query_policy_registry())
        )
        cls._root_patch = mock.patch.object(exploration_module, "PROJECT_ROOT", project_root)
        cls._registry_patch = mock.patch.object(
            exploration_module,
            "QUERY_POLICY_REGISTRY_DIRECTORY",
            registry_directory,
        )
        cls._root_patch.start()
        cls._registry_patch.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls._registry_patch.stop()
        cls._root_patch.stop()
        cls._policy_workspace.cleanup()

    def test_persisted_output_schemas_are_recursive_closed_world_contracts(self) -> None:
        evaluation_schema = json.loads(
            (ROOT / "contracts/x.grok_cli.exploration.evaluation.v0.schema.json").read_text()
        )
        hydration_schema = json.loads(
            (ROOT / "contracts/x.grok_cli.candidate_hydration.task.v0.schema.json").read_text()
        )
        candidate_policy_schema = json.loads(
            (ROOT / "contracts/x.grok_cli.candidate_value_segment_policy.v1.schema.json").read_text()
        )
        query_policy_schema = json.loads(
            (
                ROOT
                / "contracts/x.grok_cli.exploration.query_policy_descriptor.v1.schema.json"
            ).read_text()
        )
        registry_schema = json.loads(
            (ROOT / "contracts/x.grok_cli.exploration.query_policy_registry.v1.schema.json").read_text()
        )

        def assert_closed_objects(value: Any, path: str = "$") -> None:
            if isinstance(value, dict):
                if value.get("type") == "object":
                    self.assertIs(value.get("additionalProperties"), False, path)
                for key, child in value.items():
                    assert_closed_objects(child, f"{path}.{key}")
            elif isinstance(value, list):
                for index, child in enumerate(value):
                    assert_closed_objects(child, f"{path}[{index}]")

        self.assertEqual(
            evaluation_schema["properties"]["schema_version"]["const"],
            "x.grok_cli.exploration.evaluation.v0",
        )
        self.assertEqual(
            hydration_schema["properties"]["task_version"]["const"],
            "x.grok_cli.candidate_hydration.task.v0",
        )
        self.assertEqual(
            evaluation_schema["properties"]["hydration_tasks"]["items"]["$ref"],
            "x.grok_cli.candidate_hydration.task.v0.schema.json",
        )
        assert_closed_objects(evaluation_schema)
        assert_closed_objects(hydration_schema)
        result, receipt = _canonical_payloads()
        evaluation = _evaluate(result, receipt)
        task_candidate = _candidate(platform_user_id=None)
        task = build_hydration_tasks([task_candidate], experiment_binding=_binding())[0]
        external_schemas = {"x.grok_cli.candidate_hydration.task.v0.schema.json": hydration_schema}
        _mini_schema_validate(evaluation, evaluation_schema, externals=external_schemas)
        _mini_schema_validate(task, hydration_schema)
        _mini_schema_validate(_candidate_value_policy(), candidate_policy_schema)
        _mini_schema_validate(_canonical_policy(), query_policy_schema)
        _mini_schema_validate(_query_policy_registry(), registry_schema)
        _mini_schema_validate(_public_policy(), query_policy_schema)
        _mini_schema_validate(_public_query_policy_registry(), registry_schema)

        duplicate_segment_policy = copy.deepcopy(_candidate_value_policy())
        duplicate_segment_policy["segments"][-1] = copy.deepcopy(duplicate_segment_policy["segments"][0])
        with self.assertRaises(MiniSchemaError):
            _mini_schema_validate(duplicate_segment_policy, candidate_policy_schema)

        non_v7_evaluation = copy.deepcopy(evaluation)
        non_v7_evaluation["input_binding"]["session_id"] = "49a6ed4f-5933-47ea-9df5-e2da38e74171"
        with self.assertRaises(MiniSchemaError):
            _mini_schema_validate(non_v7_evaluation, evaluation_schema, externals=external_schemas)

    def test_persisted_output_runtime_validators_are_terminal_total_and_fail_closed(self) -> None:
        result, receipt = _canonical_payloads()
        evaluation = _evaluate(result, receipt)
        validate_evaluation_output(evaluation, result, receipt)
        with self.assertRaises(TypeError):
            validate_evaluation_output(evaluation)  # type: ignore[call-arg]

        evaluation_mutations: list[dict[str, Any]] = []
        extra = copy.deepcopy(evaluation)
        extra["unexpected"] = True
        evaluation_mutations.append(extra)
        forged_binding = copy.deepcopy(evaluation)
        forged_binding["input_binding"]["query_policy_registry_sha256"] = "0" * 64
        evaluation_mutations.append(forged_binding)
        arbitrary_blocker = copy.deepcopy(evaluation)
        arbitrary_blocker["scale_blockers"] = ["arbitrary"]
        evaluation_mutations.append(arbitrary_blocker)
        bool_owner = copy.deepcopy(evaluation)
        bool_owner["input_binding"]["raw_session"]["owner_only_permissions"] = 1
        evaluation_mutations.append(bool_owner)
        unhashable_session_count = copy.deepcopy(evaluation)
        unhashable_session_count["metrics"]["session_verified_completed_tool_calls"] = {}
        evaluation_mutations.append(unhashable_session_count)
        unhashable_reason = copy.deepcopy(evaluation)
        unhashable_reason["candidate_value_assessments"][0]["hydration_reasons"] = [{}]
        evaluation_mutations.append(unhashable_reason)
        wrong_rate = copy.deepcopy(evaluation)
        wrong_rate["metrics"]["model_mediated_precision_tranche_rate"] = 0.5
        evaluation_mutations.append(wrong_rate)
        wrong_gap = copy.deepcopy(evaluation)
        wrong_gap["first_field_gate_gaps"]["bio_coverage_gap_to_100_percent"] = 0.5
        evaluation_mutations.append(wrong_gap)
        wrong_proof = copy.deepcopy(evaluation)
        wrong_proof["native_x_call_proof"] = "session_hash_and_calls_verified"
        evaluation_mutations.append(wrong_proof)
        wrong_feasibility = copy.deepcopy(evaluation)
        wrong_feasibility["candidate_search_feasibility"] = "model_mediated_lead_not_demonstrated"
        evaluation_mutations.append(wrong_feasibility)
        wrong_high_authority = copy.deepcopy(evaluation)
        wrong_high_authority["candidate_value_assessments"][0]["high_authority_evidence_complete"] = False
        evaluation_mutations.append(wrong_high_authority)
        wrong_lab = copy.deepcopy(evaluation)
        wrong_lab["input_binding"]["lab_id"] = "OpenAI."
        evaluation_mutations.append(wrong_lab)
        over_total_tool_calls = copy.deepcopy(evaluation)
        over_total_tool_calls["tool_counts"] = {
            "x_keyword_search": 9,
            "x_semantic_search": 8,
            "x_user_search": 8,
            "x_thread_fetch": 8,
        }
        over_total_tool_calls["metrics"]["tool_call_receipt_rows"] = 33
        evaluation_mutations.append(over_total_tool_calls)
        for mutation in evaluation_mutations:
            with self.subTest(evaluation_mutation=mutation):
                with self.assertRaises(ExplorationValidationError):
                    validate_evaluation_output(mutation, result, receipt)

        hydration_result = _result(candidate=_candidate(platform_user_id=None))
        hydration_evaluation = _evaluate(hydration_result, receipt)
        replay_only_mutations: list[dict[str, Any]] = []
        deleted_task = copy.deepcopy(hydration_evaluation)
        deleted_task["hydration_tasks"] = []
        deleted_task["next_phase"] = "manual_adjudication"
        replay_only_mutations.append(deleted_task)
        forged_count = copy.deepcopy(hydration_evaluation)
        forged_count["metrics"]["model_reported_observations"] += 1
        replay_only_mutations.append(forged_count)
        forged_hash = copy.deepcopy(hydration_evaluation)
        forged_hash["input_binding"]["result_sha256"] = "f" * 64
        replay_only_mutations.append(forged_hash)
        forged_rate = copy.deepcopy(hydration_evaluation)
        forged_rate["metrics"]["model_mediated_bio_presence_rate"] = 0.5
        forged_rate["first_field_gate_gaps"]["bio_coverage_gap_to_100_percent"] = 0.5
        coverage_index = forged_rate["scale_blockers"].index(
            "stable_platform_user_id_coverage_below_100_percent"
        )
        forged_rate["scale_blockers"].insert(coverage_index + 1, "bio_coverage_below_100_percent")
        replay_only_mutations.append(forged_rate)
        for mutation in replay_only_mutations:
            with self.subTest(replay_only_mutation=mutation):
                with self.assertRaisesRegex(ExplorationValidationError, "evaluation_source_replay_mismatch"):
                    validate_evaluation_output(mutation, hydration_result, receipt)

        candidate = _candidate(platform_user_id=None, target_lab_affiliation_state="ambiguous")
        task = build_hydration_tasks([candidate], experiment_binding=_binding())[0]
        validate_hydration_task(task)
        task_mutations: list[dict[str, Any]] = []
        bool_calls = copy.deepcopy(task)
        bool_calls["max_tool_calls"] = True
        task_mutations.append(bool_calls)
        bool_posts = copy.deepcopy(task)
        bool_posts["max_posts"] = False
        task_mutations.append(bool_posts)
        unhashable_task_reason = copy.deepcopy(task)
        unhashable_task_reason["reasons"] = [{}]
        task_mutations.append(unhashable_task_reason)
        rewritten_segment = copy.deepcopy(task)
        rewritten_segment["candidate_value_segment"] = "recall_current_historical"
        task_mutations.append(rewritten_segment)
        reversed_reasons = copy.deepcopy(task)
        reversed_reasons["reasons"] = list(reversed(reversed_reasons["reasons"]))
        identity = {
            "candidate_state_binding": reversed_reasons["candidate_state_binding"],
            "handle": reversed_reasons["handle"].casefold(),
            "experiment_binding": reversed_reasons["experiment_binding"],
            "post_urls": reversed_reasons["seed_post_urls"],
            "reasons": reversed_reasons["reasons"],
            "task_status": reversed_reasons["task_status"],
            "task_version": reversed_reasons["task_version"],
        }
        reversed_reasons["task_key"] = (
            "xhydrate_" + hashlib.sha256(canonical_json(identity).encode()).hexdigest()[:24]
        )
        task_mutations.append(reversed_reasons)
        for mutation in task_mutations:
            with self.subTest(task_mutation=mutation):
                with self.assertRaises(ExplorationValidationError):
                    validate_hydration_task(mutation)

    def test_structural_receipt_evaluation_is_explicitly_unverified(self) -> None:
        observed = _evaluate(_result(), _receipt())
        self.assertEqual(observed["native_x_call_proof"], "receipt_only_unverified")
        self.assertEqual(
            observed["candidate_search_feasibility"],
            "receipt_only_model_mediated_result_unverified",
        )
        self.assertEqual(observed["researcher_role_function_feasibility"], "not_evaluated")
        self.assertEqual(observed["metrics"]["model_mediated_bio_presence_rate"], 1.0)
        self.assertEqual(observed["metrics"]["model_mediated_platform_user_id_presence_rate"], 1.0)
        self.assertEqual(observed["metrics"]["model_mediated_precision_tranche_rate"], 1.0)
        self.assertEqual(observed["metrics"]["model_mediated_recall_pool_rate"], 1.0)
        self.assertEqual(observed["scale_verdict"], "no_go")
        self.assertIn("provider_post_bodies_not_replayable", observed["scale_blockers"])
        self.assertEqual(observed["input_binding"]["query_policy_version"], _canonical_policy()["policy_version"])
        self.assertFalse(any(observed["authority"].values()))

    def test_runtime_binds_shape_schema_to_approved_registry_hash(self) -> None:
        result, receipt = _canonical_payloads()
        observed = evaluate_exploration(result, receipt)
        schema = json.loads(
            (
                ROOT
                / "contracts/x.grok_cli.exploration.query_policy_descriptor.v1.schema.json"
            ).read_text()
        )
        registry = _query_policy_registry()
        public_policy = _public_policy()
        public_registry = _public_query_policy_registry()
        self.assertNotIn("const", schema)
        self.assertIn("Approval is declared", schema["description"])
        self.assertEqual(registry["policies"][0]["policy_sha256"], canonical_sha256(_canonical_policy()))
        self.assertEqual(
            observed["input_binding"]["query_policy_sha256"],
            canonical_sha256(_canonical_policy()),
        )
        self.assertEqual(
            observed["input_binding"]["legacy_full_query_policy_sha256"],
            canonical_sha256(_synthetic_legacy_full_policy()),
        )
        self.assertEqual(public_registry["policies"][0]["policy_sha256"], canonical_sha256(public_policy))
        self.assertEqual(
            public_registry["policies"][0]["legacy_full_policy_sha256"],
            public_policy["legacy_full_policy_sha256"],
        )
        self.assertEqual(observed["input_binding"]["lab_id"], "openai")
        with self.assertRaises(TypeError):
            evaluate_exploration(_result(), _receipt(), query_policy={})
        with self.assertRaisesRegex(ExplorationValidationError, "approved_query_policy_not_found"):
            evaluate_exploration(result, receipt, query_policy_version="unapproved-policy-v1")

    def test_public_policy_files_are_hash_only_and_preserve_legacy_binding(self) -> None:
        policy = _public_policy()
        registry = _public_query_policy_registry()
        serialized = canonical_json({"policy": policy, "registry": registry})

        intended_public_migration_files = [
            ROOT / "configs/grok_cli_exploration_query_policy_descriptor.v1.json",
            ROOT
            / "configs/grok_cli_exploration_query_policy_registries/approved-query-policies-v1.json",
            ROOT / "contracts/x.grok_cli.exploration.query_policy_descriptor.v1.schema.json",
            ROOT / "contracts/x.grok_cli.exploration.query_policy_registry.v1.schema.json",
            ROOT / "contracts/x.grok_cli.exploration.evaluation.v0.schema.json",
            ROOT / "contracts/x.grok_cli.candidate_hydration.task.v0.schema.json",
            ROOT / "fixtures/grok_cli_exploration_pre_migration_evaluation.v1.json",
            ROOT
            / "docs/live-evidence/2026-07-14-openai-pretrain-grok-cli-and-luna-exploration.md",
        ]
        forbidden_patterns = {
            "literal_uuid": re.compile(
                r"(?i)\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b"
            ),
            "query_or_profile_operand": re.compile(
                r"(?i)(?:from:|(?<![A-Za-z0-9_])@[A-Za-z0-9_]{1,15}\b|https?://x\.com/[A-Za-z0-9_])"
            ),
            "private_payload_field": re.compile(
                r'(?i)"(?:arguments|query|bio_excerpt|excerpt)"\s*:'
            ),
            "credential_value": re.compile(
                r'''(?ix)
                (?:sk|xai)-[A-Za-z0-9_-]{8,}
                | bearer\s+[A-Za-z0-9._-]{8,}
                | "(?:api_key|access_token|refresh_token)"\s*:
                '''
            ),
        }
        for public_path in intended_public_migration_files:
            contents = public_path.read_text(encoding="utf-8")
            for pattern_name, pattern in forbidden_patterns.items():
                with self.subTest(path=public_path.name, forbidden=pattern_name):
                    self.assertIsNone(pattern.search(contents))

        self.assertNotRegex(serialized, r"(?i)(?:from:|@|https?://x\.com/)")
        self.assertNotIn('"arguments"', serialized)
        self.assertNotIn('"query"', serialized)
        self.assertNotIn('"bio"', serialized.casefold())
        self.assertEqual(
            policy["legacy_full_policy_sha256"],
            "5a8e61b34a9ab920e82a011e7534b65a264710a72df5352a302562f59a380b1e",
        )
        self.assertEqual(registry["policies"][0]["policy_sha256"], canonical_sha256(policy))
        self.assertEqual(
            registry["policies"][0]["legacy_full_policy_sha256"],
            policy["legacy_full_policy_sha256"],
        )
        self.assertEqual(
            [item["sequence"] for item in policy["query_manifest"]],
            list(range(len(policy["query_manifest"]))),
        )
        self.assertIn(
            policy["run_binding_sha256"],
            intended_public_migration_files[-1].read_text(encoding="utf-8"),
        )

    def test_named_pre_migration_evaluation_requires_source_replay_to_migrate(self) -> None:
        fixture = _pre_migration_evaluation_fixture()
        self.assertEqual(
            set(fixture),
            {
                "schema_version",
                "fixture_id",
                "fixture_mode",
                "pre_migration_transform",
                "expected_current_validator_error",
                "migration_contract",
            },
        )
        self.assertEqual(
            fixture["schema_version"],
            "x.grok_cli.exploration.pre_migration_evaluation_fixture.v1",
        )
        self.assertEqual(
            fixture["fixture_mode"],
            "runtime_generated_from_synthetic_source_pair",
        )
        self.assertEqual(
            fixture["migration_contract"],
            {
                "requires_source_result": True,
                "requires_source_receipt": True,
                "detached_evaluation_may_self_validate": False,
            },
        )

        source_result = _result(candidates=[])
        source_receipt = _receipt()
        current_evaluation = _evaluate(source_result, source_receipt)
        pre_migration = copy.deepcopy(current_evaluation)
        transform = fixture["pre_migration_transform"]
        source_field = transform["query_policy_sha256_source_field"]
        pre_migration["input_binding"]["query_policy_sha256"] = pre_migration["input_binding"][
            source_field
        ]
        pre_migration["input_binding"]["query_policy_schema_version"] = transform[
            "query_policy_schema_version"
        ]
        for field in transform["remove_input_binding_fields"]:
            del pre_migration["input_binding"][field]

        with self.assertRaisesRegex(
            ExplorationValidationError,
            fixture["expected_current_validator_error"],
        ):
            validate_evaluation_output(pre_migration, source_result, source_receipt)

        migrated = _evaluate(source_result, source_receipt)
        validate_evaluation_output(migrated, source_result, source_receipt)
        self.assertEqual(canonical_json(migrated), canonical_json(current_evaluation))
        self.assertEqual(
            migrated["input_binding"]["query_policy_schema_version"],
            "x.grok_cli.exploration.query_policy_descriptor.v1",
        )
        self.assertIn("legacy_full_query_policy_sha256", migrated["input_binding"])
        with self.assertRaises(TypeError):
            validate_evaluation_output(migrated)  # type: ignore[call-arg]

        different_source_result = copy.deepcopy(source_result)
        different_source_result["status_reason"] = "Different valid synthetic source payload."
        with self.assertRaisesRegex(ExplorationValidationError, "evaluation_source_replay_mismatch"):
            validate_evaluation_output(migrated, different_source_result, source_receipt)

    def test_hash_only_descriptor_hash_call_and_order_tampering_fail_closed(self) -> None:
        def evaluate_with(policy: dict[str, Any], registry: dict[str, Any]) -> None:
            with tempfile.TemporaryDirectory() as directory:
                project_root = Path(directory)
                configs = project_root / "configs"
                registry_directory = configs / "grok_cli_exploration_query_policy_registries"
                registry_directory.mkdir(parents=True)
                (configs / "grok_cli_exploration_query_policy_descriptor.v1.json").write_text(
                    json.dumps(policy)
                )
                (registry_directory / "approved-query-policies-v1.json").write_text(
                    json.dumps(registry)
                )
                with (
                    mock.patch.object(exploration_module, "PROJECT_ROOT", project_root),
                    mock.patch.object(
                        exploration_module,
                        "QUERY_POLICY_REGISTRY_DIRECTORY",
                        registry_directory,
                    ),
                ):
                    evaluate_exploration(_result(), _receipt())

        changed_call = copy.deepcopy(_canonical_policy())
        changed_call["query_manifest"][0]["call_sha256"] = "0" * 64
        with self.assertRaisesRegex(ExplorationValidationError, "approved_query_policy_hash_mismatch"):
            evaluate_with(changed_call, _query_policy_registry())

        changed_call_registry = copy.deepcopy(_query_policy_registry())
        changed_call_registry["policies"][0]["policy_sha256"] = canonical_sha256(changed_call)
        with self.assertRaisesRegex(ExplorationValidationError, "query_policy_manifest_mismatch"):
            evaluate_with(changed_call, changed_call_registry)

        reordered = copy.deepcopy(_canonical_policy())
        reordered["query_manifest"][0], reordered["query_manifest"][1] = (
            reordered["query_manifest"][1],
            reordered["query_manifest"][0],
        )
        for sequence, item in enumerate(reordered["query_manifest"]):
            item["sequence"] = sequence
        reordered_registry = copy.deepcopy(_query_policy_registry())
        reordered_registry["policies"][0]["policy_sha256"] = canonical_sha256(reordered)
        with self.assertRaisesRegex(ExplorationValidationError, "query_policy_manifest_mismatch"):
            evaluate_with(reordered, reordered_registry)

        changed_legacy_registry = copy.deepcopy(_query_policy_registry())
        changed_legacy_registry["policies"][0]["legacy_full_policy_sha256"] = "f" * 64
        with self.assertRaisesRegex(ExplorationValidationError, "query_policy_binding_invalid"):
            evaluate_with(_canonical_policy(), changed_legacy_registry)

    def test_candidate_value_policy_and_schema_own_temporal_ordering_and_completeness(self) -> None:
        policy = _candidate_value_policy()
        schema = json.loads((ROOT / "contracts/x.grok_cli.candidate_value_segment_policy.v1.schema.json").read_text())
        priorities = {
            item["segment_id"]: policy["priority_tiers"][item["priority_tier"]]
            for item in policy["segments"]
        }
        self.assertEqual(
            priorities["recall_current_historical"],
            priorities["recall_historical_current"],
        )
        self.assertGreater(
            priorities["recall_historical_current"],
            priorities["recall_historical_historical"],
        )
        self.assertFalse(policy["hydration"]["historical_state_triggers_hydration"])
        self.assertEqual(
            schema["properties"]["schema_version"]["const"],
            "x.grok_cli.candidate_value_segment_policy.v1",
        )
        observed = _evaluate(_result(), _receipt())
        self.assertEqual(
            observed["input_binding"]["candidate_value_policy_sha256"],
            canonical_sha256(policy),
        )

    def test_new_lab_is_enabled_by_reviewed_registry_and_policy_without_algorithm_change(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            project_root = Path(directory)
            configs = project_root / "configs"
            configs.mkdir()
            registry_directory = configs / "grok_cli_exploration_query_policy_registries"
            registry_directory.mkdir()
            canonical_policy_path = configs / "grok_cli_exploration_query_policy_descriptor.v1.json"
            canonical_policy_path.write_text(json.dumps(_canonical_policy()))
            canonical_registry = _query_policy_registry()
            (registry_directory / "approved-query-policies-v1.json").write_text(json.dumps(canonical_registry))
            calls = copy.deepcopy(_canonical_calls())
            for call in calls:
                query = call["arguments"].get("query")
                if query is not None:
                    call["arguments"]["query"] = query.replace("OpenAI", "Anthropic")
            session_id = "019f5f72-007a-7791-ab3a-a30ac8d0d97c"
            request_id = "4f95cda0-837c-4b09-9601-71aece907d69"
            legacy_full_policy = {
                **_synthetic_legacy_full_policy(),
                "policy_version": "anthropic-pretrain-base-discovery-fixture-v1",
                "lab_id": "anthropic",
                "session_id": session_id,
                "request_id": request_id,
                "query_manifest": [
                    {
                        "tool_name": call["tool_name"],
                        "arguments": call["arguments"],
                        "call_sha256": canonical_sha256(
                            {"tool_name": call["tool_name"], "arguments": call["arguments"]}
                        ),
                    }
                    for call in calls
                ],
            }
            policy = copy.deepcopy(_canonical_policy())
            policy.update(
                policy_version="anthropic-pretrain-base-discovery-fixture-v1",
                lab_id="anthropic",
                run_binding_sha256=canonical_sha256(
                    {"session_id": session_id, "request_id": request_id}
                ),
                legacy_full_policy_sha256=canonical_sha256(legacy_full_policy),
                query_manifest=[
                    {
                        "sequence": index,
                        "tool_name": call["tool_name"],
                        "call_sha256": canonical_sha256(
                            {"tool_name": call["tool_name"], "arguments": call["arguments"]}
                        ),
                    }
                    for index, call in enumerate(calls)
                ],
            )
            policy_path = configs / "anthropic_query_policy.v1.json"
            policy_path.write_text(json.dumps(policy))
            registry = {
                "schema_version": "x.grok_cli.exploration.query_policy_registry.v1",
                "registry_version": "approved-query-policies-v2",
                "policies": [
                    *canonical_registry["policies"],
                    {
                        "policy_version": policy["policy_version"],
                        "policy_path": "configs/anthropic_query_policy.v1.json",
                        "policy_sha256": canonical_sha256(policy),
                        "legacy_full_policy_sha256": policy["legacy_full_policy_sha256"],
                        "policy_schema_version": policy["schema_version"],
                        "purpose": policy["purpose"],
                        "lab_id": policy["lab_id"],
                        "run_binding_sha256": policy["run_binding_sha256"],
                        "enabled": True,
                    }
                ],
            }
            registry_path = registry_directory / "approved-query-policies-v2.json"
            registry_path.write_text(json.dumps(registry))
            receipt = _receipt(calls=calls)
            receipt["session_id"] = session_id
            receipt["request_id"] = request_id

            with (
                mock.patch.object(exploration_module, "PROJECT_ROOT", project_root),
                mock.patch.object(exploration_module, "QUERY_POLICY_REGISTRY_DIRECTORY", registry_directory),
            ):
                historical = evaluate_exploration(
                    _result(),
                    _receipt(),
                    query_policy_registry_version="approved-query-policies-v1",
                )
                observed = evaluate_exploration(
                    _result(calls=calls),
                    receipt,
                    query_policy_version=policy["policy_version"],
                    query_policy_registry_version="approved-query-policies-v2",
                )
                validate_evaluation_output(historical, _result(), _receipt())

            self.assertEqual(observed["input_binding"]["lab_id"], "anthropic")
            self.assertEqual(observed["hydration_tasks"], [])

    def test_registry_duplicate_traversal_and_symlink_paths_fail_closed(self) -> None:
        canonical_registry = _query_policy_registry()
        with tempfile.TemporaryDirectory() as directory:
            project_root = Path(directory)
            configs = project_root / "configs"
            configs.mkdir()
            policy_path = configs / "grok_cli_exploration_query_policy_descriptor.v1.json"
            policy_path.write_text(json.dumps(_canonical_policy()))
            registry_directory = configs / "grok_cli_exploration_query_policy_registries"
            registry_directory.mkdir()
            registry_path = registry_directory / "approved-query-policies-v1.json"

            def evaluate_registry(registry: dict[str, Any]) -> None:
                registry_path.write_text(json.dumps(registry))
                with (
                    mock.patch.object(exploration_module, "PROJECT_ROOT", project_root),
                    mock.patch.object(exploration_module, "QUERY_POLICY_REGISTRY_DIRECTORY", registry_directory),
                ):
                    evaluate_exploration(_result(), _receipt())

            duplicate = copy.deepcopy(canonical_registry)
            duplicate["policies"].append(copy.deepcopy(duplicate["policies"][0]))
            with self.assertRaisesRegex(ExplorationValidationError, "query_policy_registry_rows_invalid"):
                evaluate_registry(duplicate)

            traversal = copy.deepcopy(canonical_registry)
            traversal["policies"][0]["policy_path"] = "configs/../outside.json"
            with self.assertRaisesRegex(ExplorationValidationError, "query_policy_registry_rows_invalid"):
                evaluate_registry(traversal)

            symlink = copy.deepcopy(canonical_registry)
            link_path = configs / "linked_policy.json"
            link_path.symlink_to(policy_path.name)
            symlink["policies"][0]["policy_path"] = "configs/linked_policy.json"
            with self.assertRaisesRegex(ExplorationValidationError, "query_policy_path_invalid"):
                evaluate_registry(symlink)

    def test_provenance_metadata_queries_counts_and_tool_arguments_fail_closed(self) -> None:
        mutations: list[tuple[dict[str, Any], dict[str, Any]]] = []
        result = _result()
        receipt = _receipt()
        changed = copy.deepcopy(result)
        changed["native_x_tool_provenance"]["queries"] = ["ethnicity targeted query"]
        mutations.append((changed, receipt))
        changed = copy.deepcopy(result)
        changed["native_x_tool_provenance"]["queries"] = ["OpenAI pretraining [semantic]"]
        mutations.append((changed, receipt))
        changed_receipt = copy.deepcopy(receipt)
        changed_receipt["session_id"] = "bogus"
        mutations.append((result, changed_receipt))
        changed_receipt = copy.deepcopy(receipt)
        changed_receipt["calls"][0]["arguments"]["limit"] = "999999999"
        mutations.append((result, changed_receipt))
        changed = copy.deepcopy(result)
        changed["counts"]["candidates_retained"] = True
        mutations.append((changed, receipt))
        changed = copy.deepcopy(result)
        changed["native_x_tool_provenance"]["tool_calls_reported"] = True
        mutations.append((changed, receipt))
        changed_receipt = copy.deepcopy(receipt)
        changed_receipt["tool_counts"]["x_keyword_search"] = True
        mutations.append((result, changed_receipt))
        changed = copy.deepcopy(result)
        changed["local_reconciliation"]["tool_calls_completed"] = True
        mutations.append((changed, receipt))
        changed = copy.deepcopy(result)
        changed["counts"]["observations_inspected_reported"] = 0
        mutations.append((changed, receipt))
        changed = copy.deepcopy(result)
        changed["excluded_examples"] = [{"handle": "xfsynth_fixture", "reason": "Contradictory duplicate."}]
        mutations.append((changed, receipt))
        changed = copy.deepcopy(result)
        changed["trace_id"] = "trace_fixture"
        mutations.append((changed, receipt))
        for candidate_result, candidate_receipt in mutations:
            with self.subTest(result=candidate_result, receipt=candidate_receipt):
                with self.assertRaises(ExplorationValidationError):
                    _evaluate(candidate_result, candidate_receipt)

    def test_query_drift_and_protected_query_bypasses_fail_closed_against_manifest(self) -> None:
        protected_call = _call(query="OpenAI pretraining ethnicity")
        with self.assertRaises(ExplorationValidationError):
            _evaluate(_result(protected_call), _receipt(protected_call))
        professional_call = _call(query="OpenAI pretraining China professional experience")
        with self.assertRaisesRegex(ExplorationValidationError, "query_policy_manifest_mismatch"):
            _evaluate(_result(professional_call), _receipt(professional_call))
        unlisted_protected_proxy = _call(query="OpenAI pretraining East Asian descent")
        with self.assertRaisesRegex(ExplorationValidationError, "query_policy_manifest_mismatch"):
            _evaluate(_result(unlisted_protected_proxy), _receipt(unlisted_protected_proxy))
        chinese_protected_call = _call(query="OpenAI 预训练 华人 researcher")
        with self.assertRaises(ExplorationValidationError):
            _evaluate(_result(chinese_protected_call), _receipt(chinese_protected_call))

    def test_candidate_evidence_actor_timestamp_and_bio_presence_are_bound(self) -> None:
        mutations = []
        actor = _candidate()
        actor["evidence"][0]["author_handle"] = "other_fixture"
        actor["evidence"][0]["url"] = "https://x.com/other_fixture"
        mutations.append(actor)
        timestamp = _candidate()
        timestamp["evidence"][1]["published_at"] = {"bad": "timestamp"}
        mutations.append(timestamp)
        missing_bio = _candidate()
        missing_bio["evidence"] = missing_bio["evidence"][1:]
        mutations.append(missing_bio)
        unrelated_bio = _candidate()
        unrelated_bio["bio_excerpt"] = "Observed Bio: Entirely unrelated professional statement."
        mutations.append(unrelated_bio)
        for candidate in mutations:
            with self.subTest(candidate=candidate):
                with self.assertRaises(ExplorationValidationError):
                    _evaluate(_result(candidate=candidate), _receipt())

    def test_duplicate_provider_call_id_is_rejected(self) -> None:
        receipt = _receipt()
        duplicate = copy.deepcopy(receipt["calls"][0])
        duplicate["tool_call_id"] = "ctc_fixture_call_1"
        receipt["calls"].append(duplicate)
        receipt["tool_counts"] = {"x_keyword_search": 2}
        with self.assertRaises(ExplorationValidationError):
            _evaluate(_result(), receipt)

    def test_unlisted_thread_fetch_is_rejected_by_closed_world_policy(self) -> None:
        call = _call(tool_name="x_thread_fetch")
        with self.assertRaisesRegex(ExplorationValidationError, "query_policy_manifest_mismatch"):
            _evaluate(_result(call), _receipt(call))

    def test_two_axis_value_policy_retains_all_four_complete_temporal_combinations(self) -> None:
        candidates = [
            _candidate(handle="xfs_cc", target_lab_affiliation_state="current", pretraining_experience_state="current"),
            _candidate(
                handle="xfs_ch",
                target_lab_affiliation_state="current",
                pretraining_experience_state="historical",
            ),
            _candidate(
                handle="xfs_hc",
                target_lab_affiliation_state="historical",
                pretraining_experience_state="current",
            ),
            _candidate(
                handle="xfs_hh",
                target_lab_affiliation_state="historical",
                pretraining_experience_state="historical",
            ),
        ]

        observed = _evaluate(_result(candidates=candidates), _receipt())

        self.assertEqual(
            observed["metrics"]["candidate_value_segment_counts"],
            {
                "precision_current_current": 1,
                "recall_current_historical": 1,
                "recall_historical_current": 1,
                "recall_historical_historical": 1,
                "needs_evidence": 0,
            },
        )
        self.assertEqual(observed["metrics"]["recall_pool_candidates"], 4)
        self.assertEqual(observed["metrics"]["precision_tranche_candidates"], 1)
        self.assertEqual(observed["hydration_tasks"], [])
        self.assertTrue(all(item["recall_pool_eligible"] for item in observed["candidate_value_assessments"]))

    def test_precision_requires_complete_current_current_but_confidence_does_not_trigger_hydration(self) -> None:
        candidate = _candidate(confidence="medium")

        observed = _evaluate(_result(candidate=candidate), _receipt())

        assessment = observed["candidate_value_assessments"][0]
        self.assertEqual(assessment["candidate_value_segment"], "precision_current_current")
        self.assertTrue(assessment["recall_pool_eligible"])
        self.assertFalse(assessment["precision_tranche_eligible"])
        self.assertFalse(assessment["hydration_required"])
        self.assertEqual(observed["hydration_tasks"], [])

    def test_ambiguous_or_unsupported_dimensions_need_evidence_and_old_aggregate_counts_are_rejected(self) -> None:
        candidate = _candidate(target_lab_affiliation_state="ambiguous")
        observed = _evaluate(_result(candidate=candidate), _receipt())
        assessment = observed["candidate_value_assessments"][0]
        self.assertEqual(assessment["candidate_value_segment"], "needs_evidence")
        self.assertFalse(assessment["recall_pool_eligible"])
        self.assertIn("target_lab_affiliation_state_unresolved", assessment["hydration_reasons"])
        self.assertEqual(observed["hydration_tasks"][0]["candidate_value_segment"], "needs_evidence")

        legacy_counts = _result()
        legacy_counts["counts"].update(high_confidence_current=1, ambiguous_current=0, historical_only=0)
        with self.assertRaisesRegex(ExplorationValidationError, "exploration_counts_invalid"):
            _evaluate(legacy_counts, _receipt())

    def test_hydration_priority_uses_segment_then_confidence_and_does_not_penalize_historical_state(self) -> None:
        candidates = [
            _candidate(
                handle="xfs_hh",
                platform_user_id=None,
                target_lab_affiliation_state="historical",
                pretraining_experience_state="historical",
            ),
            _candidate(
                handle="xfs_ch",
                platform_user_id=None,
                target_lab_affiliation_state="current",
                pretraining_experience_state="historical",
                confidence="low",
            ),
            _candidate(
                handle="xfs_hc",
                platform_user_id=None,
                target_lab_affiliation_state="historical",
                pretraining_experience_state="current",
                confidence="high",
            ),
            _candidate(handle="xfs_cc", platform_user_id=None),
        ]

        tasks = build_hydration_tasks(candidates, experiment_binding=_binding(), maximum=4)

        # Tier ordering is strict; equal-tier temporal segments are interleaved
        # before taking another row from either segment.
        self.assertEqual([task["handle"] for task in tasks], ["xfs_cc", "xfs_ch", "xfs_hc", "xfs_hh"])
        for task in tasks:
            self.assertEqual(task["reasons"], ["missing_platform_user_id"])

        mixed_candidates = [
            _candidate(
                handle="xfs_ch_hi",
                platform_user_id=None,
                target_lab_affiliation_state="current",
                pretraining_experience_state="historical",
                confidence="high",
            ),
            _candidate(
                handle="xfs_ch_med",
                platform_user_id=None,
                target_lab_affiliation_state="current",
                pretraining_experience_state="historical",
                confidence="medium",
            ),
            _candidate(
                handle="xfs_hc_low",
                platform_user_id=None,
                target_lab_affiliation_state="historical",
                pretraining_experience_state="current",
                confidence="low",
            ),
        ]
        mixed_tasks = build_hydration_tasks(mixed_candidates, experiment_binding=_binding(), maximum=3)
        self.assertEqual(
            [task["handle"] for task in mixed_tasks],
            ["xfs_ch_hi", "xfs_hc_low", "xfs_ch_med"],
        )

    def test_bio_only_hydration_has_no_impossible_post_fields_and_key_is_run_bound(self) -> None:
        candidate = _candidate(platform_user_id=None, include_post=False)
        task = build_hydration_tasks([candidate], experiment_binding=_binding())[0]
        self.assertEqual(task["experiment_binding"], _binding())
        self.assertEqual(task["task_status"], "planned")
        self.assertEqual(task["candidate_state_binding"]["candidate_sha256"], canonical_sha256(candidate))
        self.assertEqual(task["candidate_state_binding"]["candidate_value_segment"], task["candidate_value_segment"])
        self.assertEqual(task["requested_tools"], ["x_user_search"])
        self.assertEqual(task["max_posts"], 0)
        self.assertNotIn("canonical_post_id", task["required_fields"])
        self.assertIs(task["execution_authorized"], False)
        historical_candidate = copy.deepcopy(candidate)
        historical_candidate["target_lab_affiliation_state"] = "historical"
        historical_task = build_hydration_tasks([historical_candidate], experiment_binding=_binding())[0]
        self.assertNotEqual(task["task_key"], historical_task["task_key"])
        other_binding = _binding()
        other_binding["request_id"] = "49a6ed4f-5933-47ea-9df5-e2da38e74171"
        with self.assertRaisesRegex(ExplorationValidationError, "hydration_experiment_binding_invalid"):
            build_hydration_tasks([candidate], experiment_binding=other_binding)

        invalid_bindings = []
        missing_policy_hash = _binding()
        del missing_policy_hash["query_policy_sha256"]
        invalid_bindings.append(missing_policy_hash)
        malformed_policy_hash = _binding()
        malformed_policy_hash["query_policy_sha256"] = "NOT_A_SHA256"
        invalid_bindings.append(malformed_policy_hash)
        malformed_policy_version = _binding()
        malformed_policy_version["query_policy_version"] = "invalid policy version"
        invalid_bindings.append(malformed_policy_version)
        noncanonical_policy = _binding()
        noncanonical_policy["query_policy_version"] = "other-valid-policy-v1"
        noncanonical_policy["query_policy_sha256"] = "0" * 64
        invalid_bindings.append(noncanonical_policy)
        for invalid_binding in invalid_bindings:
            with self.subTest(invalid_binding=invalid_binding):
                with self.assertRaisesRegex(
                    ExplorationValidationError,
                    "hydration_experiment_binding_invalid",
                ):
                    build_hydration_tasks([candidate], experiment_binding=invalid_binding)

    def test_raw_session_hashes_calls_request_and_owner_only_permissions_are_verified(self) -> None:
        receipt = _receipt()
        call = receipt["calls"][0]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / SESSION_ID
            root.mkdir(mode=0o700)
            event_pairs = []
            for index, receipt_call in enumerate(receipt["calls"]):
                event_pairs.append(
                    (
                        {
                            "timestamp": index * 2,
                            "method": "session/update",
                            "params": {
                                "sessionId": SESSION_ID,
                                "update": {
                                    "sessionUpdate": "tool_call",
                                    "toolCallId": receipt_call["tool_call_id"],
                                    "status": "in_progress",
                                },
                                "_meta": {"promptId": REQUEST_ID},
                            },
                        },
                        {
                            "timestamp": index * 2 + 1,
                            "method": "session/update",
                            "params": {
                                "sessionId": SESSION_ID,
                                "update": {
                                    "sessionUpdate": "tool_call_update",
                                    "toolCallId": receipt_call["tool_call_id"],
                                    "status": "completed",
                                    "rawOutput": {
                                        "call_id": receipt_call["provider_call_id"],
                                        "id": receipt_call["tool_call_id"],
                                        "input": json.dumps(receipt_call["arguments"], separators=(",", ":")),
                                        "name": receipt_call["tool_name"],
                                    },
                                },
                                "_meta": {"promptId": REQUEST_ID},
                            },
                        },
                    )
                )
            started_event, event = event_pairs[0]
            session_events = [item for pair in event_pairs for item in pair]
            canonical_updates = "".join(json.dumps(item, separators=(",", ":")) + "\n" for item in session_events)
            contents = {
                "chat_history.jsonl": "{}\n",
                "events.jsonl": "{}\n",
                "summary.json": json.dumps(
                    {
                        "info": {"id": SESSION_ID},
                        "request_id": REQUEST_ID,
                        "current_model_id": "grok-4.5",
                    },
                    separators=(",", ":"),
                )
                + "\n",
                "updates.jsonl": canonical_updates,
            }
            for name, content in contents.items():
                path = root / name
                path.write_text(content)
                path.chmod(0o600)
            receipt["raw_session_sha256"] = {
                name: hashlib.sha256(content.encode()).hexdigest() for name, content in contents.items()
            }
            observed = _evaluate(_result(), receipt, raw_session_directory=root)
            self.assertEqual(observed["native_x_call_proof"], "session_hash_and_calls_verified")
            self.assertEqual(
                observed["candidate_search_feasibility"],
                "model_mediated_precision_tranche_lead_demonstrated",
            )
            self.assertIs(observed["input_binding"]["raw_session"]["owner_only_permissions"], True)
            validate_evaluation_output(observed, _result(), receipt, raw_session_directory=root)
            with self.assertRaisesRegex(ExplorationValidationError, "evaluation_source_replay_mismatch"):
                validate_evaluation_output(observed, _result(), receipt)

            for field, expected_error in (
                ("sessionId", "raw_session_started_session_identity_invalid"),
                ("promptId", "raw_session_started_request_identity_invalid"),
            ):
                changed_started = copy.deepcopy(started_event)
                if field == "sessionId":
                    changed_started["params"]["sessionId"] = "49a6ed4f-5933-47ea-9df5-e2da38e74171"
                else:
                    changed_started["params"]["_meta"]["promptId"] = "49a6ed4f-5933-47ea-9df5-e2da38e74171"
                changed_events = [changed_started, *session_events[1:]]
                changed_updates = "".join(json.dumps(item, separators=(",", ":")) + "\n" for item in changed_events)
                (root / "updates.jsonl").write_text(changed_updates)
                (root / "updates.jsonl").chmod(0o600)
                receipt["raw_session_sha256"]["updates.jsonl"] = hashlib.sha256(changed_updates.encode()).hexdigest()
                with self.subTest(started_identity=field):
                    with self.assertRaisesRegex(ExplorationValidationError, expected_error):
                        _evaluate(_result(), receipt, raw_session_directory=root)
            (root / "updates.jsonl").write_text(contents["updates.jsonl"])
            (root / "updates.jsonl").chmod(0o600)
            receipt["raw_session_sha256"]["updates.jsonl"] = hashlib.sha256(
                contents["updates.jsonl"].encode()
            ).hexdigest()

            second_id = "ctc_fixture_call_extra"
            malformed_started = {
                "method": "session/update",
                "params": {
                    "sessionId": SESSION_ID,
                    "update": {
                        "sessionUpdate": "tool_call",
                        "toolCallId": second_id,
                        "status": "in_progress",
                    },
                    "_meta": {"promptId": REQUEST_ID},
                },
            }
            malformed_completed = {
                "method": "session/update",
                "params": {
                    "sessionId": SESSION_ID,
                    "update": {
                        "sessionUpdate": "tool_call_update",
                        "toolCallId": second_id,
                        "status": "completed",
                        "rawOutput": {
                            "call_id": "xs_fixture_call_extra",
                            "id": second_id,
                            "input": json.dumps(call["arguments"]),
                            "name": call["tool_name"],
                            "unexpected": True,
                        },
                    },
                    "_meta": {"promptId": REQUEST_ID},
                },
            }
            original_updates = contents["updates.jsonl"]
            malformed_updates = (
                original_updates + json.dumps(malformed_started) + "\n" + json.dumps(malformed_completed) + "\n"
            )
            (root / "updates.jsonl").write_text(malformed_updates)
            (root / "updates.jsonl").chmod(0o600)
            receipt["raw_session_sha256"]["updates.jsonl"] = hashlib.sha256(malformed_updates.encode()).hexdigest()
            with self.assertRaisesRegex(ExplorationValidationError, "raw_session_completed_call_unparseable"):
                _evaluate(_result(), receipt, raw_session_directory=root)
            (root / "updates.jsonl").write_text(original_updates)
            (root / "updates.jsonl").chmod(0o600)
            receipt["raw_session_sha256"]["updates.jsonl"] = hashlib.sha256(original_updates.encode()).hexdigest()

            extra = root / "provider-context.json"
            extra.write_text("{}\n")
            extra.chmod(0o640)
            observed = _evaluate(_result(), receipt, raw_session_directory=root)
            self.assertIs(observed["input_binding"]["raw_session"]["owner_only_permissions"], False)

            changed_receipt = copy.deepcopy(receipt)
            changed_receipt["model_id"] = "grok-4.5-shadow"
            with self.assertRaisesRegex(ExplorationValidationError, "raw_session_summary_identity_mismatch"):
                _evaluate(_result(), changed_receipt, raw_session_directory=root)

            (root / "updates.jsonl").write_text("{}\n")
            with self.assertRaisesRegex(ExplorationValidationError, "raw_session_hash_mismatch"):
                _evaluate(_result(), receipt, raw_session_directory=root)

    def test_cli_writes_only_to_owner_only_directory_and_never_echoes_failure_details(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            root.chmod(0o700)
            project_root = root / "project"
            (project_root / "scripts").mkdir(parents=True)
            (project_root / "src/x_first").mkdir(parents=True)
            (project_root / "configs/grok_cli_exploration_query_policy_registries").mkdir(
                parents=True
            )
            shutil.copy2(
                ROOT / "scripts/evaluate_grok_cli_exploration.py",
                project_root / "scripts/evaluate_grok_cli_exploration.py",
            )
            shutil.copy2(
                ROOT / "src/x_first/grok_cli_exploration.py",
                project_root / "src/x_first/grok_cli_exploration.py",
            )
            shutil.copy2(
                ROOT / "src/x_first/__init__.py",
                project_root / "src/x_first/__init__.py",
            )
            shutil.copy2(
                ROOT / "configs/candidate_value_segment_policy.v1.json",
                project_root / "configs/candidate_value_segment_policy.v1.json",
            )
            (
                project_root / "configs/grok_cli_exploration_query_policy_descriptor.v1.json"
            ).write_text(
                json.dumps(_canonical_policy())
            )
            (
                project_root
                / "configs/grok_cli_exploration_query_policy_registries/approved-query-policies-v1.json"
            ).write_text(json.dumps(_query_policy_registry()))
            result_path = root / "result.json"
            receipt_path = root / "receipt.json"
            output_path = root / "evaluation.json"
            canonical_result, canonical_receipt = _canonical_payloads()
            result_path.write_text(json.dumps(canonical_result))
            receipt_path.write_text(json.dumps(canonical_receipt))
            command = [
                sys.executable,
                str(project_root / "scripts/evaluate_grok_cli_exploration.py"),
                "--result",
                str(result_path),
                "--receipt",
                str(receipt_path),
                "--output",
                str(output_path),
            ]
            completed = subprocess.run(command, check=False, capture_output=True, text=True)
            self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
            self.assertEqual(output_path.stat().st_mode & 0o777, 0o600)
            stdout = json.loads(completed.stdout)
            self.assertEqual(
                set(stdout),
                {
                    "evaluation_sha256",
                    "native_x_call_proof",
                    "redacted_summary",
                    "scale_verdict",
                    "status",
                },
            )
            self.assertEqual(stdout["scale_verdict"], "no_go")
            self.assertNotIn(str(output_path), completed.stdout)
            self.assertNotIn("metrics", completed.stdout)

            failed = subprocess.run(command, check=False, capture_output=True, text=True)
            self.assertEqual(failed.returncode, 1)
            self.assertEqual(
                json.loads(failed.stdout),
                {"error": "GROK_CLI_EXPLORATION_EVALUATION_FAILED", "status": "failed"},
            )

            no_output = subprocess.run(command[:-2], check=False, capture_output=True, text=True)
            self.assertEqual(no_output.returncode, 2)
            self.assertEqual(no_output.stdout, "")

            public_parent = root / "public"
            public_parent.mkdir(mode=0o755)
            public_command = [*command[:-1], str(public_parent / "evaluation.json")]
            rejected = subprocess.run(public_command, check=False, capture_output=True, text=True)
            self.assertEqual(rejected.returncode, 1)
            self.assertEqual(
                json.loads(rejected.stdout),
                {"error": "GROK_CLI_EXPLORATION_EVALUATION_FAILED", "status": "failed"},
            )


if __name__ == "__main__":
    unittest.main()
