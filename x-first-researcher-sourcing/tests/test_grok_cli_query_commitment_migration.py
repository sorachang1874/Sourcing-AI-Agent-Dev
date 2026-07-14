from __future__ import annotations

import importlib.util
import json
import os
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import x_first.grok_cli_exploration as exploration  # noqa: E402
from tests.test_grok_cli_exploration import (  # noqa: E402
    COMMITMENT_KEY_HEX,
    COMMITMENT_NONCE_HEX,
    _query_commitment_issuance_history,
    _receipt,
    _registry_admissions,
    _result,
    _synthetic_legacy_full_policy,
)

SPEC = importlib.util.spec_from_file_location(
    "grok_cli_query_commitment_migration",
    ROOT / "scripts/migrate_grok_cli_query_commitments_v2.py",
)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("migration_module_unavailable")
migration = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(migration)


class GrokCliQueryCommitmentMigrationTest(unittest.TestCase):
    def _private_json(self, path: Path, value: Any) -> None:
        path.write_text(json.dumps(value), encoding="utf-8")
        path.chmod(0o600)

    def _source_tree(self) -> tuple[tempfile.TemporaryDirectory[str], Path, Path]:
        temporary = tempfile.TemporaryDirectory()
        root = Path(temporary.name)
        root.chmod(0o700)
        bundle = root / "bundle"
        bundle.mkdir(mode=0o700)

        receipt = _receipt()
        receipt.pop("query_commitment")
        result = _result()
        result.pop("status_reason_code")
        result.pop("limitation_codes")
        for candidate in result["candidates"]:
            candidate["caveats"] = ["legacy model caveat"]
            candidate.pop("caveat_codes")
        policy = _synthetic_legacy_full_policy()
        policy.update(
            allowed_decision_dimensions=list(migration.PUBLIC_ALLOWED_DECISION_DIMENSIONS),
            professional_experience_proxy_query_allowed=False,
            protected_identity_query_allowed=False,
        )
        self._private_json(bundle / "grok-tool-receipt.sanitized.json", receipt)
        self._private_json(bundle / "grok-result.two-axis.sanitized.json", result)
        self._private_json(bundle / "private-query-policy.full.v1.json", policy)
        return temporary, root, bundle

    @staticmethod
    def _fixed_material() -> Any:
        issued = iter([bytes.fromhex(COMMITMENT_KEY_HEX), bytes.fromhex(COMMITMENT_NONCE_HEX)])

        def fixed_then_random(size: int) -> bytes:
            try:
                return next(issued)
            except StopIteration:
                return os.urandom(size)

        return mock.patch.object(
            migration.secrets,
            "token_bytes",
            side_effect=fixed_then_random,
        )

    def _prepare_fixed(self, root: Path) -> dict[str, Any]:
        with self._fixed_material():
            return migration.run_operation(root, "prepare")

    @staticmethod
    def _runtime_registry(artifacts: dict[str, Any]) -> Any:
        temporary = tempfile.TemporaryDirectory()
        project_root = Path(temporary.name)
        configs = project_root / "configs"
        registry_directory = configs / "grok_cli_exploration_query_policy_registries"
        registry_directory.mkdir(parents=True)
        (configs / "grok_cli_exploration_query_policy_descriptor.v2.json").write_text(
            json.dumps(artifacts["policy"]), encoding="utf-8"
        )
        (registry_directory / "approved-query-policies-v2.json").write_text(
            json.dumps(artifacts["registry"]), encoding="utf-8"
        )
        (configs / "grok_cli_exploration_query_commitment_issuance_history.v1.json").write_text(
            json.dumps(_query_commitment_issuance_history(artifacts["registry"]["commitment_issuance_lineage"])),
            encoding="utf-8",
        )
        return temporary, project_root, registry_directory, _registry_admissions(artifacts["registry"])

    def test_prepare_is_atomic_durable_owner_only_and_identical_rerun_is_explicit(self) -> None:
        temporary, root, bundle = self._source_tree()
        self.addCleanup(temporary.cleanup)
        fsync_kinds: list[str] = []
        real_fsync = os.fsync

        def observed_fsync(descriptor: int) -> None:
            fsync_kinds.append("directory" if stat.S_ISDIR(os.fstat(descriptor).st_mode) else "file")
            real_fsync(descriptor)

        with mock.patch.object(migration.os, "fsync", side_effect=observed_fsync):
            created = self._prepare_fixed(root)
        self.assertEqual((created["status"], created["idempotent"]), ("prepared", False))
        descriptor = created["policy"]
        registry = created["registry"]
        issuance = registry["commitment_issuance_lineage"][0]
        self.assertEqual(
            issuance["issuance_id"],
            exploration.query_commitment_issuance_id(
                issuance["policy_version"],
                issuance["run_binding_commitment"],
                issuance["commitment_key_id"],
                issuance["commitment_nonce_id"],
            ),
        )
        self.assertEqual(issuance["policy_sha256"], exploration.canonical_sha256(descriptor))
        self.assertEqual(
            issuance["semantic_manifest_sha256"],
            exploration.canonical_sha256(descriptor["query_manifest"]),
        )
        self.assertEqual(issuance["policy_path"], registry["policies"][0]["policy_path"])
        self.assertEqual(
            issuance["protected_category_boundary_version"],
            exploration.PROTECTED_CATEGORY_BOUNDARY_VERSION,
        )
        self.assertEqual(registry["issuance_history_count"], 1)
        self.assertEqual(registry["issuance_history_head_sha256"], exploration.canonical_sha256(issuance))
        self.assertIn("file", fsync_kinds)
        self.assertIn("directory", fsync_kinds)
        for name in (
            migration.PRIVATE_RECEIPT_NAME,
            migration.PRIVATE_RESULT_NAME,
            migration.MIGRATION_RECEIPT_NAME,
        ):
            self.assertEqual((bundle / name).stat().st_mode & 0o777, 0o600)
        self.assertFalse(any(path.name.endswith(".tmp") for path in root.rglob("*")))

        replayed = migration.run_operation(root, "prepare")
        self.assertEqual((replayed["status"], replayed["idempotent"]), ("prepared", True))
        durable = json.loads((bundle / migration.MIGRATION_RECEIPT_NAME).read_text())
        self.assertEqual(durable["idempotency"]["last_operation"], "prepare_identical_replay")
        self.assertEqual(durable["retention"]["delete_owner"], migration.DELETE_OWNER)
        self.assertEqual(
            [row["role"] for row in durable["source_to_target"]],
            [
                "tool_receipt",
                "result",
                "query_policy",
            ],
        )

    def test_root_owner_mode_supplied_symlink_and_private_ancestor_fail_closed(self) -> None:
        temporary, root, bundle = self._source_tree()
        self.addCleanup(temporary.cleanup)
        root.chmod(0o755)
        with self.assertRaisesRegex(migration.MigrationError, "private_root_invalid"):
            migration.run_operation(root, "prepare")
        root.chmod(0o700)

        symlink = root.parent / f"{root.name}-link"
        symlink.symlink_to(root)
        self.addCleanup(symlink.unlink)
        with self.assertRaisesRegex(migration.MigrationError, "private_root_symlink_forbidden"):
            migration.run_operation(symlink, "prepare")

        bundle.chmod(0o755)
        with self.assertRaisesRegex(migration.MigrationError, "private_directory_invalid"):
            migration.run_operation(root, "prepare")

    def test_source_hardlink_and_any_tree_symlink_fail_closed(self) -> None:
        temporary, root, bundle = self._source_tree()
        self.addCleanup(temporary.cleanup)
        source = bundle / "grok-tool-receipt.sanitized.json"
        hardlink = bundle / "receipt-hardlink.json"
        os.link(source, hardlink)
        with self.assertRaisesRegex(migration.MigrationError, "private_file_invalid"):
            migration.run_operation(root, "prepare")
        hardlink.unlink()

        (bundle / "unrelated-link").symlink_to("grok-result.two-axis.sanitized.json")
        with self.assertRaisesRegex(migration.MigrationError, "private_tree_symlink_forbidden"):
            migration.run_operation(root, "prepare")

    def test_partial_symlink_and_stale_destinations_fail_closed(self) -> None:
        for destination_kind in ("partial", "symlink"):
            with self.subTest(destination_kind=destination_kind):
                temporary, root, bundle = self._source_tree()
                self.addCleanup(temporary.cleanup)
                destination = bundle / migration.PRIVATE_RECEIPT_NAME
                if destination_kind == "partial":
                    self._private_json(destination, {"stale": True})
                    expected = "partial_or_stale_destination"
                else:
                    destination.symlink_to("grok-tool-receipt.sanitized.json")
                    expected = "private_tree_symlink_forbidden"
                with self.assertRaisesRegex(migration.MigrationError, expected):
                    migration.run_operation(root, "prepare")

    def test_mutated_existing_target_is_not_treated_as_idempotent(self) -> None:
        temporary, root, bundle = self._source_tree()
        self.addCleanup(temporary.cleanup)
        self._prepare_fixed(root)
        target = bundle / migration.PRIVATE_RESULT_NAME
        value = json.loads(target.read_text())
        value["counts"]["observations_inspected_reported"] += 1
        self._private_json(target, value)
        with self.assertRaisesRegex(migration.MigrationError, "existing_private_target_stale"):
            migration.run_operation(root, "prepare")

    def test_prepare_and_evaluate_share_one_nonblocking_lock(self) -> None:
        temporary, root, _ = self._source_tree()
        self.addCleanup(temporary.cleanup)
        with migration._validated_private_root(root) as private_root, migration._migration_lock(private_root):
            with self.assertRaisesRegex(migration.MigrationError, "migration_lock_busy"):
                migration.run_operation(root, "prepare")

    def test_prepare_crash_leaves_partial_state_that_cannot_be_silently_reused(self) -> None:
        temporary, root, bundle = self._source_tree()
        self.addCleanup(temporary.cleanup)
        real_atomic = migration._atomic_private_json
        calls = 0

        def crash_second_write(*args: Any, **kwargs: Any) -> None:
            nonlocal calls
            calls += 1
            if calls == 2:
                raise OSError("synthetic_crash")
            real_atomic(*args, **kwargs)

        with (
            self._fixed_material(),
            mock.patch.object(
                migration,
                "_atomic_private_json",
                side_effect=crash_second_write,
            ),
        ):
            with self.assertRaisesRegex(OSError, "synthetic_crash"):
                migration.run_operation(root, "prepare")
        self.assertTrue((bundle / migration.PRIVATE_RECEIPT_NAME).exists())
        with self.assertRaisesRegex(migration.MigrationError, "partial_or_stale_destination"):
            migration.run_operation(root, "prepare")

    def test_closed_legacy_policy_and_cli_failure_never_echo_private_values(self) -> None:
        temporary, root, bundle = self._source_tree()
        self.addCleanup(temporary.cleanup)
        policy_path = bundle / "private-query-policy.full.v1.json"
        policy = json.loads(policy_path.read_text())
        private_query = policy["query_manifest"][0]["arguments"]["query"]
        policy["unexpected_private_note"] = private_query
        self._private_json(policy_path, policy)
        completed = subprocess.run(
            [
                sys.executable,
                str(ROOT / "scripts/migrate_grok_cli_query_commitments_v2.py"),
                "prepare",
                "--private-root",
                str(root),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 1)
        self.assertEqual(completed.stderr, "")
        self.assertEqual(
            json.loads(completed.stdout),
            {"error": "QUERY_COMMITMENT_MIGRATION_FAILED", "status": "failed"},
        )
        self.assertNotIn(private_query, completed.stdout + completed.stderr)

    def test_recursive_public_privacy_boundary_rejects_private_key_or_value(self) -> None:
        sensitive = {"OpenAI pretraining secret query", COMMITMENT_KEY_HEX}
        migration._assert_public_safe({"nested": [{"safe": "public"}]}, sensitive)
        for payload in (
            {"nested": [{"query": "redacted"}]},
            {"nested": [{"safe": "prefix OpenAI pretraining secret query suffix"}]},
            {"nested": [{"safe": COMMITMENT_KEY_HEX}]},
        ):
            with self.subTest(payload=payload), self.assertRaises(migration.MigrationError):
                migration._assert_public_safe(payload, sensitive)

    def test_evaluate_then_purge_writes_tombstone_and_is_idempotent(self) -> None:
        temporary, root, bundle = self._source_tree()
        self.addCleanup(temporary.cleanup)
        artifacts = self._prepare_fixed(root)
        registry_tmp, project_root, registry_directory, admissions = self._runtime_registry(artifacts)
        self.addCleanup(registry_tmp.cleanup)
        with (
            mock.patch.object(exploration, "PROJECT_ROOT", project_root),
            mock.patch.object(exploration, "QUERY_POLICY_REGISTRY_DIRECTORY", registry_directory),
            mock.patch.object(exploration, "QUERY_POLICY_REGISTRY_SNAPSHOT_ADMISSIONS", admissions),
        ):
            evaluated = migration.run_operation(root, "evaluate")
            replayed = migration.run_operation(root, "evaluate")
        self.assertFalse(evaluated["idempotent"])
        self.assertTrue(replayed["idempotent"])

        purged = migration.run_operation(root, "purge", confirm_source_purge=True)
        replayed_purge = migration.run_operation(root, "purge", confirm_source_purge=True)
        self.assertEqual((purged["status"], purged["idempotent"]), ("sources_purged", False))
        self.assertEqual((replayed_purge["status"], replayed_purge["idempotent"]), ("sources_purged", True))
        for source_name in (
            "grok-tool-receipt.sanitized.json",
            "grok-result.two-axis.sanitized.json",
            "private-query-policy.full.v1.json",
        ):
            self.assertFalse((bundle / source_name).exists())
        purge_receipt = json.loads((bundle / migration.PURGE_RECEIPT_NAME).read_text())
        self.assertEqual(purge_receipt["delete_owner"], migration.DELETE_OWNER)
        self.assertEqual(len(purge_receipt["deleted_sources"]), 3)

    def test_purge_crash_resumes_from_durable_intent(self) -> None:
        temporary, root, _ = self._source_tree()
        self.addCleanup(temporary.cleanup)
        artifacts = self._prepare_fixed(root)
        registry_tmp, project_root, registry_directory, admissions = self._runtime_registry(artifacts)
        self.addCleanup(registry_tmp.cleanup)
        with (
            mock.patch.object(exploration, "PROJECT_ROOT", project_root),
            mock.patch.object(exploration, "QUERY_POLICY_REGISTRY_DIRECTORY", registry_directory),
            mock.patch.object(exploration, "QUERY_POLICY_REGISTRY_SNAPSHOT_ADMISSIONS", admissions),
        ):
            migration.run_operation(root, "evaluate")
        real_unlink = migration._unlink_private
        calls = 0

        def crash_second_delete(*args: Any, **kwargs: Any) -> None:
            nonlocal calls
            calls += 1
            if calls == 2:
                raise OSError("synthetic_delete_crash")
            real_unlink(*args, **kwargs)

        with mock.patch.object(migration, "_unlink_private", side_effect=crash_second_delete):
            with self.assertRaisesRegex(OSError, "synthetic_delete_crash"):
                migration.run_operation(root, "purge", confirm_source_purge=True)
        resumed = migration.run_operation(root, "purge", confirm_source_purge=True)
        self.assertEqual((resumed["status"], resumed["idempotent"]), ("sources_purged", False))


if __name__ == "__main__":
    unittest.main()
