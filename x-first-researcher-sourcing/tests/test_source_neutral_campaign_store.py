from __future__ import annotations

import fcntl
import hashlib
import json
import os
import stat
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from x_first.source_neutral_campaign_store import (
    PHASE1_VALIDATION_BOUNDARY,
    CampaignHeadConflict,
    CampaignMutationConflict,
    CampaignStore,
    CampaignStoreCorruption,
    CampaignStoreError,
    CampaignStoreLockBusy,
    DirectProof,
    DirectProofCollision,
    ValidatedWaveBundle,
)


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=True, allow_nan=False, separators=(",", ":"), sort_keys=True)


def _validated_wave(label: str, *proof_labels: str) -> ValidatedWaveBundle:
    return ValidatedWaveBundle(
        payload={
            "schema_version": "fixture.validated_wave.v1",
            "label": label,
            "nested": {"accepted": True, "ordinal": len(label)},
        },
        direct_proofs=tuple(
            DirectProof(proof_kind="fixture_direct_receipt", proof_sha256=_digest(item)) for item in proof_labels
        ),
    )


class InjectedCrash(RuntimeError):
    pass


class SourceNeutralCampaignStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / "campaign-store"
        self.store = CampaignStore.create(self.root, store_id="fixture_store_v1")

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _append(
        self,
        *,
        campaign_id: str = "fixture_campaign_a",
        wave_id: str = "wave_001",
        mutation_id: str = "mutation_001",
        expected_head_token: str | None = None,
        label: str = "one",
        proof_labels: tuple[str, ...] = ("proof-one",),
        store: CampaignStore | None = None,
    ):
        return (store or self.store).append_validated_wave(
            campaign_id=campaign_id,
            wave_id=wave_id,
            mutation_id=mutation_id,
            expected_head_token=expected_head_token,
            validated_wave=_validated_wave(label, *proof_labels),
        )

    def _fork_append_and_exit_after_link(self, target_parent_name: str) -> None:
        expected_point = f"after_append_only_link_before_temp_unlink:{target_parent_name}"
        child_pid = os.fork()
        if child_pid == 0:

            def exit_after_link(point: str) -> None:
                if point == expected_point:
                    os._exit(73)

            try:
                child_store = CampaignStore.open(self.root, _fault_injector=exit_after_link)
                self._append(store=child_store)
            except BaseException:
                os._exit(74)
            os._exit(75)
        waited_pid, status = os.waitpid(child_pid, 0)
        self.assertEqual(waited_pid, child_pid)
        self.assertTrue(os.WIFEXITED(status), status)
        self.assertEqual(os.WEXITSTATUS(status), 73)

    def test_private_canonical_store_and_read_replay_apis(self) -> None:
        committed = self._append()

        snapshot = self.store.replay()
        self.assertEqual(snapshot.store_id, "fixture_store_v1")
        self.assertEqual(snapshot.journal_head_token, committed.head_token)
        self.assertEqual(snapshot.waves, (committed,))
        self.assertEqual(snapshot.campaign_head("fixture_campaign_a").head_token, committed.head_token)
        self.assertEqual(snapshot.wave("fixture_campaign_a", "wave_001"), committed)
        self.assertEqual(
            snapshot.direct_proof_registry[0].proof_sha256,
            _digest("proof-one"),
        )
        self.assertEqual(self.store.get_campaign_head("fixture_campaign_a").head_token, committed.head_token)
        self.assertEqual(self.store.get_wave("fixture_campaign_a", "wave_001"), committed)
        self.assertEqual(self.store.read_wave_bundle("fixture_campaign_a", "wave_001")["label"], "one")

        manifest_raw = self.store.manifest_path.read_bytes()
        manifest = json.loads(manifest_raw)
        self.assertEqual(
            manifest_raw,
            json.dumps(manifest, ensure_ascii=True, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
            + b"\n",
        )
        self.assertEqual(manifest["phase1_validation_boundary"], PHASE1_VALIDATION_BOUNDARY)

        for directory in (
            self.root,
            self.store.journal_dir,
            self.store.objects_dir,
            self.store.heads_dir,
            self.store.temp_dir,
        ):
            self.assertEqual(stat.S_IMODE(directory.stat().st_mode), 0o700)
        files = [
            self.store.manifest_path,
            self.store.lock_path,
            *self.store.journal_dir.iterdir(),
            *self.store.objects_dir.iterdir(),
            *self.store.heads_dir.iterdir(),
        ]
        self.assertTrue(files)
        for path in files:
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600, path)

    def test_exact_mutation_retry_is_idempotent_even_after_head_advances(self) -> None:
        first = self._append()
        second = self._append(
            wave_id="wave_002",
            mutation_id="mutation_002",
            expected_head_token=first.head_token,
            label="two",
            proof_labels=("proof-two",),
        )

        retried = self._append()

        self.assertEqual(retried, first)
        snapshot = self.store.replay()
        self.assertEqual(len(snapshot.waves), 2)
        self.assertEqual(snapshot.campaign_head("fixture_campaign_a").head_token, second.head_token)

    def test_mutation_id_conflict_fails_before_cas(self) -> None:
        first = self._append()

        with self.assertRaisesRegex(CampaignMutationConflict, "campaign_mutation_conflict"):
            self._append(
                expected_head_token=first.head_token,
                label="different",
                proof_labels=("proof-different",),
            )

        self.assertEqual(len(self.store.replay().waves), 1)

    def test_sibling_compare_and_swap_rejects_loser(self) -> None:
        root = self._append()
        winner = self._append(
            wave_id="wave_002a",
            mutation_id="mutation_002a",
            expected_head_token=root.head_token,
            label="winner",
            proof_labels=("proof-winner",),
        )

        with self.assertRaisesRegex(CampaignHeadConflict, "campaign_head_conflict"):
            self._append(
                wave_id="wave_002b",
                mutation_id="mutation_002b",
                expected_head_token=root.head_token,
                label="loser",
                proof_labels=("proof-loser",),
            )

        snapshot = self.store.replay()
        self.assertEqual(len(snapshot.waves), 2)
        self.assertEqual(snapshot.campaign_head("fixture_campaign_a").head_token, winner.head_token)

    def test_direct_proof_reuse_is_rejected_across_campaigns(self) -> None:
        self._append(proof_labels=("globally-owned-proof",))

        with self.assertRaisesRegex(DirectProofCollision, "direct_proof_collision"):
            self._append(
                campaign_id="fixture_campaign_b",
                wave_id="wave_001",
                mutation_id="mutation_001",
                label="cross-campaign",
                proof_labels=("globally-owned-proof",),
            )

        snapshot = self.store.replay()
        self.assertEqual(len(snapshot.waves), 1)
        self.assertEqual(snapshot.direct_proof_registry[0].campaign_id, "fixture_campaign_a")

    def test_replay_rejects_journal_gap(self) -> None:
        first = self._append()
        self._append(
            campaign_id="fixture_campaign_b",
            wave_id="wave_001",
            mutation_id="mutation_001",
            label="two",
            proof_labels=("proof-two",),
        )
        first_path = self.store.journal_dir / f"{first.sequence:020d}.{first.head_token}.json"
        first_path.unlink()

        with self.assertRaisesRegex(CampaignStoreCorruption, "journal_sequence_gap"):
            self.store.replay()

    def test_replay_rejects_corrupt_journal_bytes(self) -> None:
        committed = self._append()
        entry_path = self.store.journal_dir / f"{committed.sequence:020d}.{committed.head_token}.json"
        entry_path.write_bytes(b"{}\n")
        entry_path.chmod(0o600)

        with self.assertRaises(CampaignStoreCorruption):
            self.store.replay()

    def test_replay_rejects_tail_truncation_before_proof_or_cas_decision(self) -> None:
        self._append()
        tail = self._append(
            campaign_id="fixture_campaign_z",
            wave_id="wave_001",
            mutation_id="mutation_001",
            label="tail",
            proof_labels=("tail-proof",),
        )
        tail_path = self.store.journal_dir / f"{tail.sequence:020d}.{tail.head_token}.json"
        tail_path.unlink()

        with self.assertRaisesRegex(CampaignStoreCorruption, "frontier_unknown"):
            self.store.replay()
        with self.assertRaisesRegex(CampaignStoreCorruption, "frontier_unknown"):
            self._append(
                campaign_id="fixture_campaign_c",
                wave_id="wave_001",
                mutation_id="mutation_001",
                label="proof-reuse-after-truncation",
                proof_labels=("tail-proof",),
            )

        self.assertEqual(len(list(self.store.journal_dir.iterdir())), 1)

    def test_same_sequence_materialized_head_fork_fails_closed(self) -> None:
        self._append()
        head_path = self.store.heads_dir / "fixture_campaign_a.json"
        head = json.loads(head_path.read_bytes())
        head["head_token"] = _digest("unknown-same-sequence-fork")
        head["materialized_head_sha256"] = _digest(
            _canonical_json({key: item for key, item in head.items() if key != "materialized_head_sha256"})
        )
        head_path.write_bytes(_canonical_json(head).encode("utf-8") + b"\n")
        head_path.chmod(0o600)

        with self.assertRaisesRegex(CampaignStoreCorruption, "materialized_campaign_frontier_unknown"):
            self.store.replay()

    def test_stale_materialized_head_is_repaired_from_journal(self) -> None:
        first = self._append()
        head_path = self.store.heads_dir / "fixture_campaign_a.json"
        stale = head_path.read_bytes()
        second = self._append(
            wave_id="wave_002",
            mutation_id="mutation_002",
            expected_head_token=first.head_token,
            label="two",
            proof_labels=("proof-two",),
        )
        current = head_path.read_bytes()
        self.assertNotEqual(stale, current)
        head_path.write_bytes(stale)
        head_path.chmod(0o600)

        snapshot = self.store.replay()

        self.assertEqual(snapshot.campaign_head("fixture_campaign_a").head_token, second.head_token)
        self.assertEqual(head_path.read_bytes(), current)

    def test_store_global_nonblocking_lock_honors_monotonic_deadline(self) -> None:
        fd = os.open(self.store.lock_path, os.O_RDWR)
        started = time.monotonic()
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaisesRegex(CampaignStoreLockBusy, "campaign_store_lock_busy"):
                CampaignStore.open(self.root, lock_timeout_seconds=0.01)
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)
        elapsed = time.monotonic() - started
        self.assertGreaterEqual(elapsed, 0.005)
        self.assertLess(elapsed, 0.5)

    def test_invalid_and_nonfinite_lock_timeouts_fail_before_layout_creation(self) -> None:
        invalid_values = (True, False, -0.01, float("nan"), float("inf"), float("-inf"), 10**10000)
        for index, timeout in enumerate(invalid_values):
            with self.subTest(index=index):
                root = Path(self.temporary.name) / f"invalid-timeout-{index}"
                with self.assertRaisesRegex(CampaignStoreError, "lock_timeout_invalid"):
                    CampaignStore.create(
                        root,
                        store_id=f"fixture_invalid_timeout_{index}",
                        lock_timeout_seconds=timeout,
                    )
                self.assertFalse(root.exists())
                with self.assertRaisesRegex(CampaignStoreError, "lock_timeout_invalid"):
                    CampaignStore.open(self.root, lock_timeout_seconds=timeout)

    def test_existing_lock_with_public_mode_is_rejected_without_repair(self) -> None:
        self.store.lock_path.chmod(0o666)

        with self.assertRaisesRegex(CampaignStoreCorruption, "store_lock_permission_invalid"):
            CampaignStore.create(self.root, store_id="fixture_store_v1")

        self.assertEqual(stat.S_IMODE(self.store.lock_path.stat().st_mode), 0o666)

    def test_new_store_normalizes_only_new_paths_under_restrictive_umask(self) -> None:
        root = Path(self.temporary.name) / "restrictive-umask-store"
        previous = os.umask(0o777)
        try:
            store = CampaignStore.create(root, store_id="fixture_restrictive_umask")
        finally:
            os.umask(previous)

        self.assertEqual(stat.S_IMODE(root.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE(store.lock_path.stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(store.manifest_path.stat().st_mode), 0o600)

    def test_hardlinked_lock_is_rejected(self) -> None:
        alias = self.root / ".store.lock.alias"
        os.link(self.store.lock_path, alias)

        with self.assertRaisesRegex(CampaignStoreCorruption, "store_lock_link_count_invalid"):
            CampaignStore.open(self.root)

    def test_lock_path_replacement_cannot_open_a_second_writer_lane(self) -> None:
        displaced = self.root / ".store.lock.displaced"
        contender_outcome: list[str] = []
        writer_b = CampaignStore.open(self.root, lock_timeout_seconds=0)

        def replace_lock_and_probe_contender(point: str) -> None:
            if point != "after_bundle_publish_before_journal":
                return
            os.replace(self.store.lock_path, displaced)
            fd = os.open(self.store.lock_path, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
            try:
                os.fchmod(fd, 0o600)
            finally:
                os.close(fd)
            try:
                self._append(
                    label="writer-b",
                    proof_labels=("proof-writer-b",),
                    store=writer_b,
                )
            except CampaignStoreLockBusy as exc:
                contender_outcome.append(exc.code)
            else:
                contender_outcome.append("unexpectedly_opened")

        writer_a = CampaignStore.open(self.root, _fault_injector=replace_lock_and_probe_contender)
        with self.assertRaisesRegex(CampaignStoreCorruption, "store_lock_path_binding_invalid"):
            self._append(
                label="writer-a",
                proof_labels=("proof-writer-a",),
                store=writer_a,
            )

        self.assertEqual(contender_outcome, ["campaign_store_lock_busy"])
        self.assertEqual(list(self.store.journal_dir.iterdir()), [])
        displaced.unlink()

        committed = self._append(
            label="writer-b",
            proof_labels=("proof-writer-b",),
            store=writer_b,
        )
        snapshot = writer_b.replay()
        self.assertEqual(committed.sequence, 1)
        self.assertEqual(snapshot.waves, (committed,))
        self.assertEqual(len(list(self.store.journal_dir.iterdir())), 1)

    def test_hardlinked_authoritative_files_fail_closed(self) -> None:
        for kind in ("manifest", "journal", "object", "head"):
            with self.subTest(kind=kind):
                root = Path(self.temporary.name) / f"campaign-store-{kind}"
                store = CampaignStore.create(root, store_id=f"fixture_store_{kind}")
                committed = store.append_validated_wave(
                    campaign_id="fixture_campaign",
                    wave_id="wave_001",
                    mutation_id="mutation_001",
                    expected_head_token=None,
                    validated_wave=_validated_wave(kind, f"proof-{kind}"),
                )
                authoritative_path = {
                    "manifest": store.manifest_path,
                    "journal": store.journal_dir / f"{committed.sequence:020d}.{committed.head_token}.json",
                    "object": store.objects_dir / f"{committed.bundle_sha256}.json",
                    "head": store.heads_dir / "fixture_campaign.json",
                }[kind]
                os.link(authoritative_path, Path(self.temporary.name) / f"{kind}.hardlink")

                with self.assertRaisesRegex(CampaignStoreCorruption, "private_file_link_count_invalid"):
                    store.replay()

    def test_authoritative_file_and_directory_owner_are_required(self) -> None:
        actual_euid = os.geteuid()
        with mock.patch(
            "x_first.source_neutral_campaign_store.os.geteuid",
            return_value=actual_euid + 1,
        ):
            with self.assertRaisesRegex(CampaignStoreCorruption, "private_file_owner_invalid"):
                self.store._read_private_file(self.store.manifest_path, "store_manifest_read_failed")
            with self.assertRaisesRegex(CampaignStoreCorruption, "campaign_store_directory_owner_invalid"):
                self.store._ensure_private_directory(self.root, create=False)

    def test_existing_directory_with_public_mode_is_rejected_without_repair(self) -> None:
        self.root.chmod(0o755)

        with self.assertRaisesRegex(CampaignStoreCorruption, "campaign_store_directory_permission_invalid"):
            CampaignStore.create(self.root, store_id="fixture_store_v1")

        self.assertEqual(stat.S_IMODE(self.root.stat().st_mode), 0o755)

    def test_pre_journal_crash_leaves_no_commit_and_exact_retry_succeeds(self) -> None:
        def crash(point: str) -> None:
            if point == "after_bundle_publish_before_journal":
                raise InjectedCrash(point)

        crashing = CampaignStore.open(self.root, _fault_injector=crash)
        with self.assertRaisesRegex(InjectedCrash, "after_bundle_publish_before_journal"):
            self._append(store=crashing)

        recovered = CampaignStore.open(self.root)
        self.assertEqual(recovered.replay().waves, ())
        committed = self._append(store=recovered)
        self.assertEqual(len(recovered.replay().waves), 1)
        self.assertEqual(recovered.get_campaign_head("fixture_campaign_a").head_token, committed.head_token)

    def test_post_journal_crash_is_committed_and_repairs_head_without_new_entry(self) -> None:
        def crash(point: str) -> None:
            if point == "after_journal_publish_before_head":
                raise InjectedCrash(point)

        crashing = CampaignStore.open(self.root, _fault_injector=crash)
        with self.assertRaisesRegex(InjectedCrash, "after_journal_publish_before_head"):
            self._append(store=crashing)

        head_path = self.store.heads_dir / "fixture_campaign_a.json"
        self.assertFalse(head_path.exists())
        recovered = CampaignStore.open(self.root)
        snapshot = recovered.replay()
        self.assertEqual(len(snapshot.waves), 1)
        self.assertTrue(head_path.exists())

        retried = self._append(store=recovered)
        self.assertEqual(retried, snapshot.waves[0])
        self.assertEqual(len(recovered.replay().waves), 1)

    def test_replay_cleans_private_orphan_temporary_file(self) -> None:
        orphan = self.store.temp_dir / f".journal-orphan.{('a' * 24)}.tmp"
        orphan.write_bytes(b"partial")
        orphan.chmod(0o600)

        self.store.replay()

        self.assertFalse(orphan.exists())

    def test_object_publish_link_intermediate_recovers_then_exact_retry_commits(self) -> None:
        self._fork_append_and_exit_after_link("objects")

        temps = list(self.store.temp_dir.iterdir())
        objects = list(self.store.objects_dir.iterdir())
        self.assertEqual(len(temps), 1)
        self.assertEqual(len(objects), 1)
        self.assertTrue(os.path.samefile(temps[0], objects[0]))
        self.assertEqual(temps[0].stat().st_nlink, 2)

        recovered = CampaignStore.open(self.root)
        self.assertEqual(list(recovered.temp_dir.iterdir()), [])
        self.assertEqual(recovered.replay().waves, ())
        committed = self._append(store=recovered)
        retried = self._append(store=recovered)
        self.assertEqual(committed.sequence, 1)
        self.assertEqual(retried, committed)
        self.assertEqual(recovered.replay().waves, (committed,))

    def test_journal_publish_link_intermediate_recovers_as_committed_exact_retry(self) -> None:
        self._fork_append_and_exit_after_link("journal")

        temps = list(self.store.temp_dir.iterdir())
        journals = list(self.store.journal_dir.iterdir())
        self.assertEqual(len(temps), 1)
        self.assertEqual(len(journals), 1)
        self.assertTrue(os.path.samefile(temps[0], journals[0]))
        self.assertEqual(temps[0].stat().st_nlink, 2)
        self.assertEqual(list(self.store.heads_dir.iterdir()), [])

        recovered = CampaignStore.open(self.root)
        snapshot = recovered.replay()
        self.assertEqual(list(recovered.temp_dir.iterdir()), [])
        self.assertEqual(len(snapshot.waves), 1)
        self.assertEqual(self._append(store=recovered), snapshot.waves[0])
        self.assertEqual(len(recovered.replay().waves), 1)

    def test_two_link_temp_with_only_non_target_alias_fails_closed(self) -> None:
        payload = {"fixture": "valid-content-addressed-bytes"}
        raw = _canonical_json(payload).encode("utf-8") + b"\n"
        digest = hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()
        temp = self.store.temp_dir / f".objects-{digest}.json.{('a' * 24)}.tmp"
        alias = self.root / "non-target-alias.json"
        temp.write_bytes(raw)
        temp.chmod(0o600)
        os.link(temp, alias)

        with self.assertRaisesRegex(CampaignStoreCorruption, "temporary_publish_intermediate_invalid"):
            self.store.replay()

        self.assertTrue(os.path.samefile(temp, alias))
        self.assertEqual(temp.stat().st_nlink, 2)
        self.assertFalse((self.store.objects_dir / f"{digest}.json").exists())

    def test_two_link_expected_target_with_wrong_bytes_fails_closed(self) -> None:
        claimed_digest = "f" * 64
        temp = self.store.temp_dir / f".objects-{claimed_digest}.json.{('b' * 24)}.tmp"
        target = self.store.objects_dir / f"{claimed_digest}.json"
        temp.write_bytes(b'{"fixture":"wrong-digest"}\n')
        temp.chmod(0o600)
        os.link(temp, target)

        with self.assertRaisesRegex(CampaignStoreCorruption, "temporary_publish_intermediate_invalid"):
            self.store.replay()

        self.assertTrue(os.path.samefile(temp, target))
        self.assertEqual(temp.stat().st_nlink, 2)


if __name__ == "__main__":
    unittest.main()
