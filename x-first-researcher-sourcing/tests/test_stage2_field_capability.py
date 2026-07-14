from __future__ import annotations

import ast
import copy
import inspect
import os
import subprocess
import sys
import unittest
from collections.abc import Callable, Iterable
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from x_first.recall_pool_schema import assert_schema_valid  # noqa: E402
from x_first.stage2_field_capability import (  # noqa: E402
    PROFILE_ALWAYS_EXACT_FIELDS,
    PROFILE_OPTIONAL_BIO_FIELDS,
    PROFILE_REQUIRED_FIELDS,
    TECHNICAL_LIMITS,
    _build_collection,
    _build_expectation,
    _collection_id,
    build_fixture_bundle,
    canonical_sha256,
    evaluate_field_capability,
    load_json,
    text_sha256,
    validate_capability_expectation,
    validate_collection,
    validate_evaluation,
    validate_experiment_request,
    validate_field_registry,
    validate_fixture_bundle,
    validate_profile_source_binding,
    validate_selection_manifest,
)


def walk(value: object) -> Iterable[object]:
    yield value
    if isinstance(value, dict):
        for child in value.values():
            yield from walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk(child)


class Stage2FieldCapabilityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.registry = load_json(ROOT / "configs/stage2_field_registry.v1.json")
        cls.fixture = load_json(ROOT / "fixtures/stage2_field_capability_fixture_v1.json")

    def assert_collection_rejected(
        self,
        mutation: Callable[[dict[str, object]], None],
        *,
        contains: str,
    ) -> None:
        collection = copy.deepcopy(self.fixture["collection"])
        mutation(collection)
        errors = validate_collection(
            collection,
            request=self.fixture["request"],
            registry=self.registry,
            selection_manifest=self.fixture["selection_manifest"],
        )
        self.assertTrue(any(contains in error for error in errors), errors)

    @staticmethod
    def rehash_collection(collection: dict[str, object]) -> None:
        collection["retention"]["raw_evidence_manifest_sha256"] = canonical_sha256(collection["source_records"])
        collection["collection_id"] = _collection_id(collection)

    def completed_profile_only_collection(self) -> dict[str, object]:
        collection = copy.deepcopy(self.fixture["collection"])
        row = next(item for item in collection["task_rows"] if item["status"] == "completed")
        task_id = row["task_id"]
        profile = next(item for item in collection["profiles"] if item["task_id"] == task_id)
        profile_source = next(
            item
            for item in collection["source_records"]
            if item["task_id"] == task_id and item["record_kind"] == "profile"
        )
        post_source = next(
            item
            for item in collection["source_records"]
            if item["task_id"] == task_id and item["record_kind"] == "post"
        )
        receipt = next(item for item in collection["call_receipts"] if item["task_id"] == task_id)
        profile_source["raw_record"]["post_fields_explicitly_absent"] = True
        profile_source["raw_record_sha256"] = canonical_sha256(profile_source["raw_record"])
        profile_source["source_record_id"] = (
            "xstage2src_"
            + canonical_sha256(
                {
                    "task_id": task_id,
                    "record_kind": profile_source["record_kind"],
                    "provider_path": profile_source["provider_path"],
                    "raw_record_sha256": profile_source["raw_record_sha256"],
                }
            )[:24]
        )
        collection["source_records"].remove(post_source)
        receipt["source_record_ids"] = [profile_source["source_record_id"]]
        receipt["receipt_id"] = (
            "xstage2call_"
            + canonical_sha256(
                {
                    "task_id": task_id,
                    "tool_name": receipt["tool_name"],
                    "source_record_ids": receipt["source_record_ids"],
                }
            )[:24]
        )
        profile_source["call_receipt_id"] = receipt["receipt_id"]
        profile["source_record_id"] = profile_source["source_record_id"]
        profile["profile_snapshot_id"] = (
            "xstage2profile_"
            + canonical_sha256(
                {
                    "task_id": task_id,
                    "source_record_id": profile["source_record_id"],
                    "platform_user_id": profile["platform_user_id"],
                    "bio_sha256": profile["bio_sha256"],
                    "bio_observed_at": profile["bio_observed_at"],
                }
            )[:24]
        )
        collection["posts"] = [item for item in collection["posts"] if item["task_id"] != task_id]
        row["call_receipt_ids"] = [receipt["receipt_id"]]
        row["source_record_ids"] = [profile_source["source_record_id"]]
        row["profile_snapshot_ids"] = [profile["profile_snapshot_id"]]
        row["post_ids"] = []
        post_fields = {
            "canonical_post_id",
            "canonical_post_url",
            "post_author_platform_user_id",
            "post_author_handle",
            "post_authored_at",
            "bounded_excerpt",
            "thread_relation",
        }
        for state in row["field_states"]:
            if state["field_id"] in post_fields:
                state["state"] = "absent"
        self.rehash_collection(collection)
        self.assertEqual(
            validate_collection(
                collection,
                request=self.fixture["request"],
                registry=self.registry,
                selection_manifest=self.fixture["selection_manifest"],
            ),
            [],
        )
        return collection

    def test_fixture_is_current_deterministic_and_offline(self) -> None:
        self.assertEqual(self.fixture, build_fixture_bundle(self.registry))
        self.assertEqual(validate_fixture_bundle(self.fixture, registry=self.registry), [])
        result = subprocess.run(
            [sys.executable, "scripts/generate_stage2_field_capability_fixture.py", "--check"],
            cwd=ROOT,
            env={**os.environ, "PYTHONPATH": "src"},
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        request = self.fixture["request"]
        self.assertEqual(request["execution_mode"], "offline_fixture")
        self.assertEqual(
            request["fixture_scenario_manifest"]["expectation_semantics_sha256"],
            self.fixture["capability_expectation"]["expectation_semantics_sha256"],
        )
        self.assertFalse(request["authority"]["provider_or_network_allowed"])
        self.assertTrue(
            all(receipt["external_call_count"] == 0 for receipt in self.fixture["collection"]["call_receipts"])
        )

    def test_four_closed_outer_envelopes_match_fixture_owners(self) -> None:
        artifacts = {
            "x.stage2.experiment.request.v1": self.fixture["request"],
            "x.stage2.collection.v1": self.fixture["collection"],
            "x.stage2.capability_expectation.v1": self.fixture["capability_expectation"],
            "x.stage2.evaluation.v1": self.fixture["evaluation"],
        }
        for version, artifact in artifacts.items():
            with self.subTest(version=version):
                schema = load_json(ROOT / f"contracts/{version}.schema.json")
                self.assertFalse(schema["additionalProperties"])
                self.assertEqual(set(schema["required"]), set(schema["properties"]))
                self.assertEqual(set(schema["required"]), set(artifact))
                self.assertEqual(schema["properties"]["schema_version"]["const"], version)
                assert_schema_valid(artifact, f"{version}.schema.json")
        request_schema = load_json(ROOT / "contracts/x.stage2.experiment.request.v1.schema.json")
        self.assertEqual(request_schema["properties"]["tasks"]["maxItems"], TECHNICAL_LIMITS["max_tasks"])
        self.assertGreater(request_schema["properties"]["tasks"]["maxItems"], 5)

    def test_expectation_is_request_frozen_not_collection_derived_accuracy_gold(self) -> None:
        expectation = self.fixture["capability_expectation"]
        self.assertEqual(list(inspect.signature(_build_expectation).parameters), ["request", "registry"])
        self.assertEqual(_build_expectation(self.fixture["request"], self.registry), expectation)
        before = canonical_sha256(expectation)
        _build_collection(self.fixture["request"], self.registry)["task_rows"][0]["status"] = "failed"
        self.assertEqual(canonical_sha256(expectation), before)
        forbidden = {"gold_row_id", "reviewer_labels", "adjudication_status", "built_before_provider_output"}
        self.assertTrue(all(not (isinstance(item, dict) and forbidden & set(item)) for item in walk(expectation)))
        self.assertNotIn("collection_sha256", expectation)
        self.assertEqual(
            validate_capability_expectation(
                expectation,
                request=self.fixture["request"],
                registry=self.registry,
                selection_manifest=self.fixture["selection_manifest"],
            ),
            [],
        )
        self.assertEqual(
            self.fixture["evaluation"]["expectation_metrics"],
            {"denominator": 4, "matched": 4, "mismatched": 0, "mismatch_task_ids": []},
        )
        self.assertEqual(
            validate_evaluation(
                self.fixture["evaluation"],
                request=self.fixture["request"],
                collection=self.fixture["collection"],
                expectation=expectation,
                registry=self.registry,
                selection_manifest=self.fixture["selection_manifest"],
            ),
            [],
        )

    def test_fixture_contains_only_reserved_urls_and_synthetic_handles(self) -> None:
        handles = {task["lookup_handle"] for task in self.fixture["request"]["tasks"]} | {
            profile["current_handle"] for profile in self.fixture["collection"]["profiles"]
        }
        self.assertEqual(handles, {"fixture_a", "fixture_b", "fixture_c", "fixture_d"})
        for item in walk(self.fixture):
            if isinstance(item, str) and item.startswith("https://"):
                hostname = urlparse(item).hostname
                self.assertIsNotNone(hostname)
                self.assertTrue(hostname == "invalid" or hostname.endswith(".invalid"), item)
            if isinstance(item, str):
                self.assertNotIn("x.com/", item.casefold())
                self.assertNotIn("twitter.com/", item.casefold())

    def test_terminal_total_state_machine_and_field_denominators(self) -> None:
        collection = self.fixture["collection"]
        self.assertEqual(
            collection["terminal_summary"],
            {"denominator": 4, "terminal": 4, "completed": 1, "quarantined": 2, "failed": 1, "status": "terminal"},
        )
        self.assertEqual(
            self.fixture["evaluation"]["terminal_metrics"],
            {
                "denominator": 4,
                "terminal": 4,
                "completed": 1,
                "quarantined": 2,
                "failed": 1,
                "replay_bound_profiles": 1,
            },
        )
        field_ids = [field["field_id"] for field in self.registry["fields"]]
        for row in collection["task_rows"]:
            self.assertEqual([item["field_id"] for item in row["field_states"]], field_ids)

    def test_source_bound_profile_is_one_record_exact_and_reported_id_is_diagnostic(self) -> None:
        collection = self.fixture["collection"]
        profile = collection["profiles"][0]
        source = next(
            record
            for record in collection["source_records"]
            if record["source_record_id"] == profile["source_record_id"]
        )
        task = next(task for task in self.fixture["request"]["tasks"] if task["task_id"] == profile["task_id"])
        self.assertEqual(source["record_kind"], "profile")
        self.assertTrue(source["replayable"])
        self.assertEqual(source["payload_visibility"], "full_fixture_payload")
        self.assertEqual(source["raw_record"]["platform_user_ids"], [profile["platform_user_id"]])
        self.assertEqual(profile["bio_sha256"], text_sha256(source["raw_record"]["bio_text"]))
        self.assertEqual(profile["source_binding_status"], "replay_bound_exact")
        self.assertNotEqual(task["reported_platform_user_id"], profile["platform_user_id"])
        self.assertEqual(profile["reported_platform_user_id_diagnostic"]["comparison"], "different_diagnostic_only")
        self.assertFalse(profile["reported_platform_user_id_diagnostic"]["identity_authority"])
        task_row = next(row for row in collection["task_rows"] if row["task_id"] == profile["task_id"])
        states = {item["field_id"]: item["state"] for item in task_row["field_states"]}
        self.assertTrue(all(states[field] == "present_exact" for field in PROFILE_REQUIRED_FIELDS))

    def test_source_bound_profile_can_prove_bio_explicitly_absent(self) -> None:
        collection = self.fixture["collection"]
        profile = copy.deepcopy(collection["profiles"][0])
        source = copy.deepcopy(
            next(
                record
                for record in collection["source_records"]
                if record["source_record_id"] == profile["source_record_id"]
            )
        )
        task = next(task for task in self.fixture["request"]["tasks"] if task["task_id"] == profile["task_id"])
        row = next(row for row in collection["task_rows"] if row["task_id"] == profile["task_id"])
        field_states = copy.deepcopy(row["field_states"])
        source["raw_record"]["bio_text"] = None
        source["raw_record"]["bio_content_version"] = None
        source["raw_record_sha256"] = canonical_sha256(source["raw_record"])
        profile["bio_text"] = None
        profile["bio_sha256"] = None
        profile["bio_content_version"] = None
        for field_state in field_states:
            if field_state["field_id"] in PROFILE_OPTIONAL_BIO_FIELDS:
                field_state["state"] = "absent"
        profile["profile_snapshot_id"] = (
            "xstage2profile_"
            + canonical_sha256(
                {
                    "task_id": profile["task_id"],
                    "source_record_id": profile["source_record_id"],
                    "platform_user_id": profile["platform_user_id"],
                    "bio_sha256": None,
                    "bio_observed_at": profile["bio_observed_at"],
                }
            )[:24]
        )
        self.assertEqual(
            validate_profile_source_binding(profile, task=task, source=source, field_states=field_states),
            [],
        )
        states = {item["field_id"]: item["state"] for item in field_states}
        self.assertTrue(all(states[field] == "present_exact" for field in PROFILE_ALWAYS_EXACT_FIELDS))
        self.assertTrue(all(states[field] == "absent" for field in PROFILE_OPTIONAL_BIO_FIELDS))

    def test_metadata_only_trace_remains_terminal_unverified(self) -> None:
        collection = self.fixture["collection"]
        metadata = next(
            record for record in collection["source_records"] if record["record_kind"] == "tool_metadata_only"
        )
        self.assertEqual(set(metadata["raw_record"]), {"call_id", "id", "input", "name"})
        self.assertFalse(metadata["replayable"])
        row = next(row for row in collection["task_rows"] if row["task_id"] == metadata["task_id"])
        self.assertEqual(row["status"], "failed")
        self.assertEqual(row["error_codes"], ["source_payload_unavailable"])
        self.assertEqual({item["state"] for item in row["field_states"]}, {"unverified"})

        def promote_field(mutated: dict[str, object]) -> None:
            failed = next(row for row in mutated["task_rows"] if row["status"] == "failed")
            failed["field_states"][0]["state"] = "present_exact"

        self.assert_collection_rejected(promote_field, contains="metadata-only fields must remain unverified")

    def test_identity_conflicts_and_handle_renames_fail_closed(self) -> None:
        collection = self.fixture["collection"]
        reasons = {item["reason_code"] for item in collection["quarantine"]}
        self.assertEqual(reasons, {"conflicting_platform_user_ids", "handle_rename_requires_review"})
        self.assertEqual(len(collection["profiles"]), 1)

        def mint_conflicting_profile(mutated: dict[str, object]) -> None:
            template = copy.deepcopy(mutated["profiles"][0])
            source = next(
                record
                for record in mutated["source_records"]
                if record["record_kind"] == "profile" and len(record["raw_record"]["platform_user_ids"]) == 2
            )
            row = next(row for row in mutated["task_rows"] if row["task_id"] == source["task_id"])
            template["task_id"] = source["task_id"]
            template["source_record_id"] = source["source_record_id"]
            template["profile_snapshot_id"] = "xstage2profile_" + "a" * 24
            row["profile_snapshot_ids"].append(template["profile_snapshot_id"])
            mutated["profiles"].append(template)

        self.assert_collection_rejected(mint_conflicting_profile, contains="exactly one numeric id required")

        def mint_renamed_profile(mutated: dict[str, object]) -> None:
            template = copy.deepcopy(mutated["profiles"][0])
            source = next(
                record
                for record in mutated["source_records"]
                if record["record_kind"] == "profile" and record["raw_record"]["current_handle"] == "renamed_c"
            )
            raw = source["raw_record"]
            template.update(
                {
                    "task_id": source["task_id"],
                    "source_record_id": source["source_record_id"],
                    "profile_snapshot_id": "xstage2profile_" + "b" * 24,
                    "platform_user_id": raw["platform_user_ids"][0],
                    "current_handle": raw["current_handle"],
                    "profile_url": raw["profile_url"],
                    "bio_text": raw["bio_text"],
                    "bio_sha256": text_sha256(raw["bio_text"]),
                    "bio_observed_at": raw["bio_observed_at"],
                    "bio_content_version": raw["bio_content_version"],
                }
            )
            row = next(row for row in mutated["task_rows"] if row["task_id"] == source["task_id"])
            row["profile_snapshot_ids"].append(template["profile_snapshot_id"])
            mutated["profiles"].append(template)

        self.assert_collection_rejected(mint_renamed_profile, contains="handle rename requires quarantine")

    def test_unbound_bio_and_cross_account_post_are_rejected(self) -> None:
        def change_bio(mutated: dict[str, object]) -> None:
            mutated["profiles"][0]["bio_text"] = "Unbound text"

        self.assert_collection_rejected(change_bio, contains="do not exactly replay one source record")

        def cross_account(mutated: dict[str, object]) -> None:
            mutated["posts"][0]["post_author_platform_user_id"] = "999999999999999999"

        self.assert_collection_rejected(cross_account, contains="cross-account Post evidence")

        def use_reported_id(mutated: dict[str, object]) -> None:
            task = self.fixture["request"]["tasks"][0]
            mutated["profiles"][0]["platform_user_id"] = task["reported_platform_user_id"]

        self.assert_collection_rejected(use_reported_id, contains="do not exactly replay one source record")

    def test_terminal_retention_and_zero_authority_guardrails(self) -> None:
        self.assert_collection_rejected(
            lambda collection: collection["task_rows"].pop(),
            contains="terminal denominator mismatch",
        )
        self.assert_collection_rejected(
            lambda collection: collection["retention"].update({"file_mode": "0644"}),
            contains="owner-only modes required",
        )
        self.assert_collection_rejected(
            lambda collection: collection["canonical_writes"].append({"forbidden": True}),
            contains="$.schema",
        )
        self.assert_collection_rejected(
            lambda collection: collection["retention"].update({"live_reuse_allowed": True}),
            contains="cannot be reused or promoted",
        )

    def test_zero_receipt_or_source_is_only_failed_unverified_not_absent(self) -> None:
        def remove_task_sources_but_keep_absent_states(collection: dict[str, object]) -> None:
            row = next(row for row in collection["task_rows"] if row["status"] == "quarantined")
            task_id = row["task_id"]
            collection["call_receipts"] = [item for item in collection["call_receipts"] if item["task_id"] != task_id]
            collection["source_records"] = [item for item in collection["source_records"] if item["task_id"] != task_id]
            row["call_receipt_ids"] = []
            row["source_record_ids"] = []

        self.assert_collection_rejected(
            remove_task_sources_but_keep_absent_states,
            contains="missing source receipt/payload must fail unverified",
        )

        collection = copy.deepcopy(self.fixture["collection"])
        row = next(row for row in collection["task_rows"] if row["error_codes"] == ["conflicting_platform_user_ids"])
        task_id = row["task_id"]
        collection["call_receipts"] = [item for item in collection["call_receipts"] if item["task_id"] != task_id]
        collection["source_records"] = [item for item in collection["source_records"] if item["task_id"] != task_id]
        collection["quarantine"] = [item for item in collection["quarantine"] if item["task_id"] != task_id]
        incident = next(item for item in collection["incidents"] if item["task_id"] == task_id)
        incident.update(
            {
                "code": "source_payload_unavailable",
                "disposition": "terminal_failed",
                "source_record_ids": [],
            }
        )
        incident_body = {key: value for key, value in incident.items() if key != "incident_id"}
        incident["incident_id"] = "xstage2incident_" + canonical_sha256(incident_body)[:24]
        row.update(
            {
                "status": "failed",
                "call_receipt_ids": [],
                "source_record_ids": [],
                "profile_snapshot_ids": [],
                "post_ids": [],
                "quarantine_ids": [],
                "incident_ids": [incident["incident_id"]],
                "error_codes": ["source_payload_unavailable"],
            }
        )
        for field_state in row["field_states"]:
            field_state["state"] = "unverified"
        collection["terminal_summary"].update({"quarantined": 1, "failed": 2})
        self.rehash_collection(collection)
        self.assertEqual(
            validate_collection(
                collection,
                request=self.fixture["request"],
                registry=self.registry,
                selection_manifest=self.fixture["selection_manifest"],
            ),
            [],
        )

    def test_every_full_source_is_consumed_and_extra_source_cannot_be_cherry_picked(self) -> None:
        def add_unconsumed_profile_source(collection: dict[str, object]) -> None:
            source = copy.deepcopy(
                next(item for item in collection["source_records"] if item["record_kind"] == "profile")
            )
            source["provider_path"] = "rawOutput.users[99]"
            source["source_record_id"] = (
                "xstage2src_"
                + canonical_sha256(
                    {
                        "task_id": source["task_id"],
                        "record_kind": source["record_kind"],
                        "provider_path": source["provider_path"],
                        "raw_record_sha256": source["raw_record_sha256"],
                    }
                )[:24]
            )
            collection["source_records"].append(source)
            row = next(item for item in collection["task_rows"] if item["task_id"] == source["task_id"])
            row["source_record_ids"].append(source["source_record_id"])

        self.assert_collection_rejected(
            add_unconsumed_profile_source,
            contains="full replayable source is unconsumed",
        )

    def test_registry_v1_is_exactly_14_descriptors_in_canonical_order(self) -> None:
        for mutation in (
            lambda registry: registry["fields"].reverse(),
            lambda registry: registry["fields"][0].update({"value_kind": "free_text"}),
            lambda registry: registry["fields"].append(copy.deepcopy(registry["fields"][-1])),
        ):
            with self.subTest(mutation=mutation):
                registry = copy.deepcopy(self.registry)
                mutation(registry)
                errors = validate_field_registry(registry)
                self.assertTrue(any("canonical 14 descriptors/order mismatch" in error for error in errors), errors)

    def test_post_fields_have_deep_types_reserved_url_and_closed_relation(self) -> None:
        mutations = (
            ("canonical_post_id", "not-numeric", "canonical_post_id"),
            ("canonical_post_url", "https://x.com/real/status/1", "canonical_post_url"),
            ("post_author_platform_user_id", "author", "post_author_platform_user_id"),
            ("post_author_handle", None, "post_author_handle"),
            ("post_authored_at", "yesterday", "post_authored_at"),
            ("bounded_excerpt", "x" * 281, "bounded_excerpt"),
            ("thread_relation", "unknown_relation", "thread_relation"),
        )
        for field, value, contains in mutations:
            with self.subTest(field=field):
                self.assert_collection_rejected(
                    lambda collection, field=field, value=value: collection["posts"][0].update({field: value}),
                    contains=contains,
                )

    def test_cross_handle_numeric_platform_id_requires_collection_quarantine(self) -> None:
        def reuse_id_across_handles(collection: dict[str, object]) -> None:
            good_id = collection["profiles"][0]["platform_user_id"]
            renamed = next(
                source
                for source in collection["source_records"]
                if source["record_kind"] == "profile" and source["raw_record"]["current_handle"] == "renamed_c"
            )
            renamed["raw_record"]["platform_user_ids"] = [good_id]

        self.assert_collection_rejected(
            reuse_id_across_handles,
            contains="source-derived quarantine reason mismatch",
        )

    def test_multiple_full_profile_sources_have_typed_quarantine_not_unavailable(self) -> None:
        collection = copy.deepcopy(self.fixture["collection"])
        row = next(item for item in collection["task_rows"] if item["status"] == "completed")
        task_id = row["task_id"]
        original_profile_source = next(
            item
            for item in collection["source_records"]
            if item["task_id"] == task_id and item["record_kind"] == "profile"
        )
        duplicate_profile_source = copy.deepcopy(original_profile_source)
        duplicate_profile_source["provider_path"] = "rawOutput.users[1]"
        duplicate_profile_source["source_record_id"] = (
            "xstage2src_"
            + canonical_sha256(
                {
                    "task_id": task_id,
                    "record_kind": duplicate_profile_source["record_kind"],
                    "provider_path": duplicate_profile_source["provider_path"],
                    "raw_record_sha256": duplicate_profile_source["raw_record_sha256"],
                }
            )[:24]
        )
        collection["source_records"].append(duplicate_profile_source)

        task_sources = [item for item in collection["source_records"] if item["task_id"] == task_id]
        source_ids = [item["source_record_id"] for item in task_sources]
        receipt = next(item for item in collection["call_receipts"] if item["task_id"] == task_id)
        receipt["source_record_ids"] = source_ids
        receipt["receipt_id"] = (
            "xstage2call_"
            + canonical_sha256(
                {
                    "task_id": task_id,
                    "tool_name": receipt["tool_name"],
                    "source_record_ids": source_ids,
                }
            )[:24]
        )
        for source in task_sources:
            source["call_receipt_id"] = receipt["receipt_id"]

        collection["profiles"] = [item for item in collection["profiles"] if item["task_id"] != task_id]
        collection["posts"] = [item for item in collection["posts"] if item["task_id"] != task_id]
        for field_state in row["field_states"]:
            if field_state["field_id"] in PROFILE_REQUIRED_FIELDS:
                field_state["state"] = "unverified"
        quarantine = {
            "quarantine_id": "",
            "task_id": task_id,
            "reason_code": "multiple_profile_sources",
            "source_record_ids": source_ids,
            "field_ids": list(PROFILE_REQUIRED_FIELDS),
            "resolution": "human_review_required",
        }
        quarantine["quarantine_id"] = (
            "xstage2quarantine_"
            + canonical_sha256(
                {
                    "task_id": task_id,
                    "reason_code": quarantine["reason_code"],
                    "source_record_ids": source_ids,
                }
            )[:24]
        )
        incident = {
            "incident_id": "",
            "task_id": task_id,
            "code": "multiple_profile_sources",
            "severity": "guardrail",
            "disposition": "quarantined",
            "source_record_ids": source_ids,
        }
        incident["incident_id"] = (
            "xstage2incident_"
            + canonical_sha256({key: value for key, value in incident.items() if key != "incident_id"})[:24]
        )
        collection["quarantine"].append(quarantine)
        collection["incidents"].append(incident)
        row.update(
            {
                "status": "quarantined",
                "call_receipt_ids": [receipt["receipt_id"]],
                "source_record_ids": source_ids,
                "profile_snapshot_ids": [],
                "post_ids": [],
                "quarantine_ids": [quarantine["quarantine_id"]],
                "incident_ids": [incident["incident_id"]],
                "error_codes": ["multiple_profile_sources"],
            }
        )
        collection["terminal_summary"].update({"completed": 0, "quarantined": 3})
        self.rehash_collection(collection)

        self.assertEqual(
            validate_collection(
                collection,
                request=self.fixture["request"],
                registry=self.registry,
                selection_manifest=self.fixture["selection_manifest"],
            ),
            [],
        )

    def test_multi_id_and_cross_handle_conflict_has_one_legal_precedence(self) -> None:
        collection = copy.deepcopy(self.fixture["collection"])
        conflicting_source = next(
            item
            for item in collection["source_records"]
            if item["record_kind"] == "profile" and len(item["raw_record"]["platform_user_ids"]) == 2
        )
        shared_id = conflicting_source["raw_record"]["platform_user_ids"][0]
        renamed_source = next(
            item
            for item in collection["source_records"]
            if item["record_kind"] == "profile" and item["raw_record"]["current_handle"] == "renamed_c"
        )
        task_id = renamed_source["task_id"]
        renamed_source["raw_record"]["platform_user_ids"] = [shared_id]
        renamed_source["raw_record_sha256"] = canonical_sha256(renamed_source["raw_record"])
        renamed_source["source_record_id"] = (
            "xstage2src_"
            + canonical_sha256(
                {
                    "task_id": task_id,
                    "record_kind": renamed_source["record_kind"],
                    "provider_path": renamed_source["provider_path"],
                    "raw_record_sha256": renamed_source["raw_record_sha256"],
                }
            )[:24]
        )
        receipt = next(item for item in collection["call_receipts"] if item["task_id"] == task_id)
        receipt["source_record_ids"] = [renamed_source["source_record_id"]]
        receipt["receipt_id"] = (
            "xstage2call_"
            + canonical_sha256(
                {
                    "task_id": task_id,
                    "tool_name": receipt["tool_name"],
                    "source_record_ids": receipt["source_record_ids"],
                }
            )[:24]
        )
        renamed_source["call_receipt_id"] = receipt["receipt_id"]

        quarantine = next(item for item in collection["quarantine"] if item["task_id"] == task_id)
        quarantine.update(
            {
                "reason_code": "platform_user_id_handle_conflict",
                "source_record_ids": [renamed_source["source_record_id"]],
                "field_ids": ["platform_user_id", "current_handle"],
            }
        )
        quarantine["quarantine_id"] = (
            "xstage2quarantine_"
            + canonical_sha256(
                {
                    "task_id": task_id,
                    "reason_code": quarantine["reason_code"],
                    "source_record_ids": quarantine["source_record_ids"],
                }
            )[:24]
        )
        incident = next(item for item in collection["incidents"] if item["task_id"] == task_id)
        incident.update(
            {
                "code": "platform_user_id_handle_conflict",
                "source_record_ids": [renamed_source["source_record_id"]],
            }
        )
        incident["incident_id"] = (
            "xstage2incident_"
            + canonical_sha256({key: value for key, value in incident.items() if key != "incident_id"})[:24]
        )
        row = next(item for item in collection["task_rows"] if item["task_id"] == task_id)
        row.update(
            {
                "call_receipt_ids": [receipt["receipt_id"]],
                "source_record_ids": [renamed_source["source_record_id"]],
                "quarantine_ids": [quarantine["quarantine_id"]],
                "incident_ids": [incident["incident_id"]],
                "error_codes": ["platform_user_id_handle_conflict"],
            }
        )
        self.rehash_collection(collection)

        self.assertEqual(
            validate_collection(
                collection,
                request=self.fixture["request"],
                registry=self.registry,
                selection_manifest=self.fixture["selection_manifest"],
            ),
            [],
        )
        multi_id_row = next(
            item for item in collection["task_rows"] if item["error_codes"] == ["conflicting_platform_user_ids"]
        )
        self.assertNotEqual(multi_id_row["task_id"], task_id)

    def test_post_quarantine_incident_ids_are_unique_and_closed(self) -> None:
        self.assert_collection_rejected(
            lambda collection: collection["posts"].append(copy.deepcopy(collection["posts"][0])),
            contains="post_id: duplicate",
        )
        self.assert_collection_rejected(
            lambda collection: collection["quarantine"].append(copy.deepcopy(collection["quarantine"][0])),
            contains="quarantine_id: duplicate",
        )
        self.assert_collection_rejected(
            lambda collection: collection["incidents"].append(copy.deepcopy(collection["incidents"][0])),
            contains="incident_id: duplicate",
        )
        self.assert_collection_rejected(
            lambda collection: collection["task_rows"][1].update({"incident_ids": []}),
            contains="incident closure mismatch",
        )

    def test_schema_preflight_enforces_max_items_before_cross_field_semantics(self) -> None:
        request = copy.deepcopy(self.fixture["request"])
        request["tasks"] = [request["tasks"][0]] * (TECHNICAL_LIMITS["max_tasks"] + 1)
        errors = validate_experiment_request(
            request,
            registry=self.registry,
            selection_manifest=self.fixture["selection_manifest"],
        )
        self.assertEqual(errors, ["$.schema: MiniDraft202012Error"])

    def test_request_denominator_binds_external_selection_manifest(self) -> None:
        self.assertEqual(validate_selection_manifest(self.fixture["selection_manifest"]), [])
        manifest = copy.deepcopy(self.fixture["selection_manifest"])
        manifest["selected_leads"][1]["candidate_row_sha256"] = manifest["selected_leads"][0]["candidate_row_sha256"]
        self.assertTrue(validate_selection_manifest(manifest))

        request = copy.deepcopy(self.fixture["request"])
        request["source_manifest"]["selection_manifest_sha256"] = "f" * 64
        errors = validate_experiment_request(
            request,
            registry=self.registry,
            selection_manifest=self.fixture["selection_manifest"],
        )
        self.assertTrue(any("source_manifest" in error for error in errors), errors)

        request = copy.deepcopy(self.fixture["request"])
        request["tasks"][1]["candidate_row_sha256"] = request["tasks"][0]["candidate_row_sha256"]
        errors = validate_experiment_request(
            request,
            registry=self.registry,
            selection_manifest=self.fixture["selection_manifest"],
        )
        self.assertTrue(any("duplicate denominator" in error for error in errors), errors)

    def test_typed_terminal_error_quarantine_and_incident_state_machine(self) -> None:
        self.assert_collection_rejected(
            lambda collection: next(row for row in collection["task_rows"] if row["status"] == "quarantined").update(
                {"error_codes": ["source_payload_unavailable"]}
            ),
            contains="typed quarantine state mismatch",
        )
        self.assert_collection_rejected(
            lambda collection: next(row for row in collection["task_rows"] if row["status"] == "completed").update(
                {"incident_ids": [collection["incidents"][0]["incident_id"]]}
            ),
            contains="incident closure mismatch",
        )

    def test_deep_raw_profile_and_post_type_errors_are_terminal_total(self) -> None:
        def raw_bio_is_integer(collection: dict[str, object]) -> None:
            source = next(record for record in collection["source_records"] if record["record_kind"] == "profile")
            source["raw_record"]["bio_text"] = 42
            source["raw_record_sha256"] = canonical_sha256(source["raw_record"])

        def raw_handle_is_null(collection: dict[str, object]) -> None:
            source = next(record for record in collection["source_records"] if record["record_kind"] == "profile")
            source["raw_record"]["current_handle"] = None
            source["raw_record_sha256"] = canonical_sha256(source["raw_record"])

        def profile_bio_is_integer(collection: dict[str, object]) -> None:
            collection["profiles"][0]["bio_text"] = 42

        def profile_handle_is_null(collection: dict[str, object]) -> None:
            collection["profiles"][0]["current_handle"] = None

        def post_handle_is_null(collection: dict[str, object]) -> None:
            collection["posts"][0]["post_author_handle"] = None

        def post_excerpt_is_object(collection: dict[str, object]) -> None:
            collection["posts"][0]["bounded_excerpt"] = {"not": "text"}

        for mutation in (
            raw_bio_is_integer,
            raw_handle_is_null,
            profile_bio_is_integer,
            profile_handle_is_null,
            post_handle_is_null,
            post_excerpt_is_object,
        ):
            with self.subTest(mutation=mutation.__name__):
                collection = copy.deepcopy(self.fixture["collection"])
                mutation(collection)
                errors = validate_collection(
                    collection,
                    request=self.fixture["request"],
                    registry=self.registry,
                    selection_manifest=self.fixture["selection_manifest"],
                )
                self.assertIsInstance(errors, list)
                self.assertTrue(errors)

    def test_request_scenario_manifest_is_closed_and_digest_bound(self) -> None:
        request = copy.deepcopy(self.fixture["request"])
        request["fixture_scenario_manifest"]["rows"][0]["scenario_id"] = "conflicting_platform_user_ids"
        errors = validate_experiment_request(
            request,
            registry=self.registry,
            selection_manifest=self.fixture["selection_manifest"],
        )
        self.assertTrue(any("scenario semantics mismatch" in error for error in errors), errors)
        self.assertTrue(any("scenario_manifest_sha256" in error for error in errors), errors)

        request = copy.deepcopy(self.fixture["request"])
        request["fixture_scenario_manifest"]["rows"][0]["scenario_id"] = 7
        errors = validate_experiment_request(
            request,
            registry=self.registry,
            selection_manifest=self.fixture["selection_manifest"],
        )
        self.assertTrue(errors)

    def test_expectation_semantics_cannot_be_relabelled_after_request_freeze(self) -> None:
        expectation = copy.deepcopy(self.fixture["capability_expectation"])
        row = expectation["rows"][0]
        row["expected_post_count"] = 0
        row["expectation_row_id"] = (
            "xstage2expectrow_"
            + canonical_sha256({key: value for key, value in row.items() if key != "expectation_row_id"})[:24]
        )
        semantic_rows = [
            {key: value for key, value in item.items() if key != "expectation_row_id"} for item in expectation["rows"]
        ]
        expectation["expectation_semantics_sha256"] = canonical_sha256(semantic_rows)
        expectation["manifest_id"] = (
            "xstage2expect_"
            + canonical_sha256(
                {
                    "experiment_id": expectation["experiment_id"],
                    "request_sha256": expectation["request_sha256"],
                    "expectation_version": expectation["expectation_version"],
                    "scenario_manifest_sha256": expectation["scenario_manifest_sha256"],
                    "expectation_semantics_sha256": expectation["expectation_semantics_sha256"],
                    "rows_sha256": canonical_sha256(expectation["rows"]),
                }
            )[:24]
        )
        errors = validate_capability_expectation(
            expectation,
            request=self.fixture["request"],
            registry=self.registry,
            selection_manifest=self.fixture["selection_manifest"],
        )
        self.assertTrue(any("request/expectation binding invalid" in error for error in errors), errors)
        self.assertTrue(any("request-frozen scenario semantics mismatch" in error for error in errors), errors)

    def test_evaluation_decision_is_mechanical_for_conformant_and_mismatch(self) -> None:
        collection = self.completed_profile_only_collection()
        evaluation = evaluate_field_capability(
            request=self.fixture["request"],
            collection=collection,
            expectation=self.fixture["capability_expectation"],
            registry=self.registry,
            selection_manifest=self.fixture["selection_manifest"],
        )
        self.assertEqual(evaluation["expectation_metrics"]["mismatched"], 1)
        self.assertEqual(evaluation["decision"], "offline_fixture_expectation_mismatch")
        self.assertIn("offline_fixture_expectation_mismatch_detected", evaluation["reason_codes"])
        self.assertEqual(
            validate_evaluation(
                evaluation,
                request=self.fixture["request"],
                collection=collection,
                expectation=self.fixture["capability_expectation"],
                registry=self.registry,
                selection_manifest=self.fixture["selection_manifest"],
            ),
            [],
        )
        evaluation["decision"] = "offline_fixture_expectation_conformant"
        errors = validate_evaluation(
            evaluation,
            request=self.fixture["request"],
            collection=collection,
            expectation=self.fixture["capability_expectation"],
            registry=self.registry,
            selection_manifest=self.fixture["selection_manifest"],
        )
        self.assertTrue(any("deterministic replay mismatch" in error for error in errors), errors)

    def test_task_field_states_are_source_derived_and_post_presence_cannot_be_absent(self) -> None:
        def hide_present_post(collection: dict[str, object]) -> None:
            row = next(item for item in collection["task_rows"] if item["status"] == "completed")
            for state in row["field_states"]:
                if state["field_id"] == "canonical_post_id":
                    state["state"] = "absent"

        self.assert_collection_rejected(hide_present_post, contains="source-derived state mismatch")

    def test_post_absence_requires_explicit_replayable_profile_semantics(self) -> None:
        collection = self.completed_profile_only_collection()
        source = next(
            item
            for item in collection["source_records"]
            if item["record_kind"] == "profile" and item["task_id"] == collection["profiles"][0]["task_id"]
        )
        source["raw_record"]["post_fields_explicitly_absent"] = False
        errors = validate_collection(
            collection,
            request=self.fixture["request"],
            registry=self.registry,
            selection_manifest=self.fixture["selection_manifest"],
        )
        self.assertTrue(any("source-derived state mismatch" in error for error in errors), errors)

    def test_post_source_task_and_receipt_task_must_close(self) -> None:
        collection = copy.deepcopy(self.fixture["collection"])
        post = collection["posts"][0]
        other_task_id = next(
            task["task_id"] for task in self.fixture["request"]["tasks"] if task["task_id"] != post["task_id"]
        )
        post["task_id"] = other_task_id
        errors = validate_collection(
            collection,
            request=self.fixture["request"],
            registry=self.registry,
            selection_manifest=self.fixture["selection_manifest"],
        )
        self.assertTrue(any("post source invalid" in error for error in errors), errors)

        collection = copy.deepcopy(self.fixture["collection"])
        source = next(item for item in collection["source_records"] if item["record_kind"] == "post")
        source["call_receipt_id"] = next(
            receipt["receipt_id"] for receipt in collection["call_receipts"] if receipt["task_id"] != source["task_id"]
        )
        errors = validate_collection(
            collection,
            request=self.fixture["request"],
            registry=self.registry,
            selection_manifest=self.fixture["selection_manifest"],
        )
        self.assertTrue(any("call receipt binding missing" in error for error in errors), errors)

    def test_payload_visibility_controls_replay_and_consumption(self) -> None:
        self.assert_collection_rejected(
            lambda collection: next(
                item for item in collection["source_records"] if item["payload_visibility"] == "full_fixture_payload"
            ).update({"replayable": False}),
            contains="full fixture payload must be a replayable profile/Post",
        )
        self.assert_collection_rejected(
            lambda collection: next(
                item for item in collection["source_records"] if item["payload_visibility"] == "metadata_only"
            ).update({"replayable": True}),
            contains="metadata-only source must be non-replayable tool metadata",
        )

    def test_external_selection_compares_all_identity_and_diagnostic_fields(self) -> None:
        mutations = {
            "opaque_lead_ref": "xlead_" + "a" * 24,
            "candidate_row_sha256": "a" * 64,
            "lookup_handle": "other_fixture",
            "reported_platform_user_id": "999999999999999999",
            "reported_platform_user_id_status": "absent",
        }
        for field, value in mutations.items():
            with self.subTest(field=field):
                request = copy.deepcopy(self.fixture["request"])
                task = request["tasks"][0]
                if field == "reported_platform_user_id_status":
                    task["reported_platform_user_id"] = None
                task[field] = value
                errors = validate_experiment_request(
                    request,
                    registry=self.registry,
                    selection_manifest=self.fixture["selection_manifest"],
                )
                self.assertTrue(any("source_manifest" in error for error in errors), errors)

    def test_canonical_post_id_is_unique_across_collection_sources(self) -> None:
        collection = copy.deepcopy(self.fixture["collection"])
        original_source = next(item for item in collection["source_records"] if item["record_kind"] == "post")
        duplicate_source = copy.deepcopy(original_source)
        duplicate_source["provider_path"] = "rawOutput.posts[1]"
        duplicate_source["source_record_id"] = (
            "xstage2src_"
            + canonical_sha256(
                {
                    "task_id": duplicate_source["task_id"],
                    "record_kind": duplicate_source["record_kind"],
                    "provider_path": duplicate_source["provider_path"],
                    "raw_record_sha256": duplicate_source["raw_record_sha256"],
                }
            )[:24]
        )
        collection["source_records"].append(duplicate_source)
        receipt = next(item for item in collection["call_receipts"] if item["task_id"] == duplicate_source["task_id"])
        receipt["source_record_ids"].append(duplicate_source["source_record_id"])
        receipt["receipt_id"] = (
            "xstage2call_"
            + canonical_sha256(
                {
                    "task_id": receipt["task_id"],
                    "tool_name": receipt["tool_name"],
                    "source_record_ids": receipt["source_record_ids"],
                }
            )[:24]
        )
        for source in collection["source_records"]:
            if source["task_id"] == receipt["task_id"]:
                source["call_receipt_id"] = receipt["receipt_id"]
        duplicate_post = copy.deepcopy(collection["posts"][0])
        duplicate_post["source_record_id"] = duplicate_source["source_record_id"]
        duplicate_post["post_id"] = (
            "xstage2post_"
            + canonical_sha256(
                {
                    "task_id": duplicate_post["task_id"],
                    "source_record_id": duplicate_post["source_record_id"],
                    "canonical_post_id": duplicate_post["canonical_post_id"],
                }
            )[:24]
        )
        collection["posts"].append(duplicate_post)
        row = next(item for item in collection["task_rows"] if item["task_id"] == receipt["task_id"])
        row["call_receipt_ids"] = [receipt["receipt_id"]]
        row["source_record_ids"].append(duplicate_source["source_record_id"])
        row["post_ids"].append(duplicate_post["post_id"])
        self.rehash_collection(collection)
        errors = validate_collection(
            collection,
            request=self.fixture["request"],
            registry=self.registry,
            selection_manifest=self.fixture["selection_manifest"],
        )
        self.assertTrue(any("duplicate collection object" in error for error in errors), errors)

    def test_quarantine_and_incident_codes_are_derived_from_source_conflict(self) -> None:
        collection = copy.deepcopy(self.fixture["collection"])
        quarantine = next(
            item for item in collection["quarantine"] if item["reason_code"] == "conflicting_platform_user_ids"
        )
        task_id = quarantine["task_id"]
        quarantine["reason_code"] = "handle_rename_requires_review"
        quarantine["field_ids"] = ["current_handle"]
        quarantine["quarantine_id"] = (
            "xstage2quarantine_"
            + canonical_sha256(
                {
                    "task_id": task_id,
                    "reason_code": quarantine["reason_code"],
                    "source_record_ids": quarantine["source_record_ids"],
                }
            )[:24]
        )
        incident = next(item for item in collection["incidents"] if item["task_id"] == task_id)
        incident["code"] = quarantine["reason_code"]
        incident["incident_id"] = (
            "xstage2incident_"
            + canonical_sha256({key: value for key, value in incident.items() if key != "incident_id"})[:24]
        )
        row = next(item for item in collection["task_rows"] if item["task_id"] == task_id)
        row["error_codes"] = [quarantine["reason_code"]]
        row["quarantine_ids"] = [quarantine["quarantine_id"]]
        row["incident_ids"] = [incident["incident_id"]]
        self.rehash_collection(collection)
        errors = validate_collection(
            collection,
            request=self.fixture["request"],
            registry=self.registry,
            selection_manifest=self.fixture["selection_manifest"],
        )
        self.assertTrue(any("source-derived quarantine reason mismatch" in error for error in errors), errors)

    def test_post_field_states_are_closed_unique_and_ordered(self) -> None:
        collection = copy.deepcopy(self.fixture["collection"])
        collection["posts"][0]["field_states"].reverse()
        errors = validate_collection(
            collection,
            request=self.fixture["request"],
            registry=self.registry,
            selection_manifest=self.fixture["selection_manifest"],
        )
        self.assertTrue(any("registry ordering mismatch" in error for error in errors), errors)

        collection = copy.deepcopy(self.fixture["collection"])
        states = collection["posts"][0]["field_states"]
        states[-1] = copy.deepcopy(states[0])
        errors = validate_collection(
            collection,
            request=self.fixture["request"],
            registry=self.registry,
            selection_manifest=self.fixture["selection_manifest"],
        )
        self.assertTrue(errors)

    def test_temporal_bounded_preflight_and_collection_identity_closure(self) -> None:
        collection = copy.deepcopy(self.fixture["collection"])
        profile_source = next(item for item in collection["source_records"] if item["record_kind"] == "profile")
        profile_source["observed_at"] = "2026-07-14T00:59:59.000Z"
        errors = validate_collection(
            collection,
            request=self.fixture["request"],
            registry=self.registry,
            selection_manifest=self.fixture["selection_manifest"],
        )
        self.assertTrue(any("outside call receipt window" in error for error in errors), errors)
        self.assertTrue(any("profile Bio observation mismatch" in error for error in errors), errors)

        collection = copy.deepcopy(self.fixture["collection"])
        post_source = next(item for item in collection["source_records"] if item["record_kind"] == "post")
        post_source["raw_record"]["post_authored_at"] = "2026-07-14T01:00:02.000Z"
        post_source["raw_record_sha256"] = canonical_sha256(post_source["raw_record"])
        errors = validate_collection(
            collection,
            request=self.fixture["request"],
            registry=self.registry,
            selection_manifest=self.fixture["selection_manifest"],
        )
        self.assertTrue(any("Post authored time cannot exceed observation" in error for error in errors), errors)

        request = copy.deepcopy(self.fixture["request"])
        nested: object = "leaf"
        for _ in range(TECHNICAL_LIMITS["max_validation_depth"] + 1):
            nested = {"next": nested}
        request["authority"] = nested
        errors = validate_experiment_request(
            request,
            registry=self.registry,
            selection_manifest=self.fixture["selection_manifest"],
        )
        self.assertEqual(errors, ["$.validation: validation_depth_ceiling_exceeded"])

        original = self.fixture["collection"]
        for field in ("call_receipts", "profiles", "posts", "quarantine", "incidents"):
            with self.subTest(collection_material=field):
                mutated = copy.deepcopy(original)
                mutated[field].append(copy.deepcopy(mutated[field][0]))
                self.assertNotEqual(_collection_id(mutated), original["collection_id"])
        mutated = copy.deepcopy(original)
        mutated["retention"]["delete_after"] = "2026-07-15T02:00:00.000Z"
        self.assertNotEqual(_collection_id(mutated), original["collection_id"])

    def test_runtime_module_has_no_provider_or_network_import(self) -> None:
        source = (ROOT / "src/x_first/stage2_field_capability.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        imported_roots = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported_roots.add(node.module.split(".", 1)[0])
        self.assertTrue(imported_roots.isdisjoint({"requests", "httpx", "urllib", "socket", "openai", "xai"}))
        self.assertNotIn("XAI_API_KEY", source)
        self.assertNotIn("CHSH", source)

    def test_digest_binding_detects_tampering(self) -> None:
        collection = copy.deepcopy(self.fixture["collection"])
        before = canonical_sha256(collection)
        collection["source_records"][0]["raw_record"]["bio_text"] = "tampered"
        self.assertNotEqual(canonical_sha256(collection), before)
        errors = validate_collection(
            collection,
            request=self.fixture["request"],
            registry=self.registry,
            selection_manifest=self.fixture["selection_manifest"],
        )
        self.assertTrue(any("raw_record_sha256: mismatch" in error for error in errors), errors)


if __name__ == "__main__":
    unittest.main()
