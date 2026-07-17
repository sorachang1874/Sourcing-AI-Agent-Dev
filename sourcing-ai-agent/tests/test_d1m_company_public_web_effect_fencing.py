from __future__ import annotations

import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from unittest import mock

import sourcing_agent.company_public_web_assets as company_public_web_assets_module
from sourcing_agent.company_public_web_assets import (
    build_company_public_web_seed_assets,
    company_public_web_materialization_assets_for_run,
    company_public_web_materialization_snapshot_identity,
    refresh_company_public_web_assets,
    sync_company_public_web_assets_to_company_asset_layer,
)
from sourcing_agent.durable_runtime import (
    COMPANY_PUBLIC_WEB_REFRESH_OWNER,
    COMPANY_PUBLIC_WEB_SOURCE_COLLECT_COMMAND_TYPE,
)
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin


class CompanyPublicWebEffectFencingTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    pg_store_schema_label = "d1m_company_public_web_effect_fencing"

    def setUp(self) -> None:
        super().setUp()
        self._tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tempdir.cleanup)

    def _store(self, name: str):
        return self.make_pg_store(Path(self._tempdir.name) / f"{name}.db")

    @staticmethod
    def _filesystem_file_map(root: Path) -> dict[str, bytes]:
        if not root.exists():
            return {}
        return {str(path.relative_to(root)): path.read_bytes() for path in sorted(root.rglob("*")) if path.is_file()}

    def _claim_source_command(
        self,
        store,
        *,
        suffix: str,
        lease_owner: str,
        target_company: str = "Effect Fence Labs",
        company_key: str = "effectfencelabs",
    ) -> dict[str, Any]:
        command_id = f"cmd-d1m-effect-fence-{suffix}"
        command = store.get_workflow_command(command_id)
        if not command:
            command = store.upsert_workflow_command(
                workflow_run_id=f"wf-d1m-effect-fence-{suffix}",
                operation_id=f"op-d1m-effect-fence-{suffix}",
                command_id=command_id,
                command_type=COMPANY_PUBLIC_WEB_SOURCE_COLLECT_COMMAND_TYPE,
                owner=COMPANY_PUBLIC_WEB_REFRESH_OWNER,
                idempotency_key=f"company.public-web.effect-fence:{suffix}",
                payload={"target_company": target_company, "company_key": company_key},
                max_attempts=4,
            )
        claimed = store.claim_workflow_command(
            command["command_id"],
            lease_owner=lease_owner,
            lease_seconds=60,
        )
        self.assertTrue(claimed)
        running = store.mark_workflow_command_running(command["command_id"], lease_owner=lease_owner)
        self.assertEqual(running["status"], "running")
        return running

    @staticmethod
    def _expire_command_lease(store, command_id: str) -> None:
        store._control_plane_postgres._execute_non_query(  # noqa: SLF001
            "UPDATE workflow_commands "
            "SET lease_expires_at = TO_CHAR(clock_timestamp() AT TIME ZONE 'UTC' - INTERVAL '1 second', "
            "'YYYY-MM-DD HH24:MI:SS') WHERE command_id = %s",
            (command_id,),
        )

    def _retry_source_command(
        self,
        store,
        *,
        command: dict[str, Any],
        suffix: str,
        lease_owner: str,
    ) -> dict[str, Any]:
        retry_wait = store.mark_workflow_command_failed(
            command["command_id"],
            error_text="simulated post-terminal publication crash",
            retryable=True,
            retry_delay_seconds=0,
        )
        self.assertEqual(retry_wait["status"], "retry_wait")
        store._control_plane_postgres._execute_non_query(  # noqa: SLF001
            "UPDATE workflow_commands SET not_before_at = '' WHERE command_id = %s",
            (command["command_id"],),
        )
        retried = self._claim_source_command(
            store,
            suffix=suffix,
            lease_owner=lease_owner,
        )
        self.assertEqual(retried["attempt"], int(command["attempt"]) + 1)
        return retried

    @staticmethod
    def _payload_with_source_owner(payload: dict[str, Any], command: dict[str, Any]) -> dict[str, Any]:
        return {
            **payload,
            "_source_workflow_command_id": str(command.get("command_id") or ""),
            "_source_workflow_command_attempt": int(command.get("attempt") or 0),
            "_source_workflow_command_lease_owner": str(command.get("lease_owner") or ""),
        }

    @staticmethod
    def _refresh_payload(command: dict[str, Any], *, artifact_root: Path) -> dict[str, Any]:
        return {
            "run_id": "company-public-web-run-effect-fence",
            "target_company": "Effect Fence Labs",
            "company_key": "effectfencelabs",
            "source_families": ["company_homepage"],
            "seed_urls": ["https://effect-fence.example/"],
            "collection_mode": "seed_url_only",
            "defer_company_asset_sync": True,
            "artifact_root": str(artifact_root),
            "_source_workflow_command_id": str(command.get("command_id") or ""),
            "_source_workflow_command_attempt": int(command.get("attempt") or 0),
            "_source_workflow_command_lease_owner": str(command.get("lease_owner") or ""),
        }

    @staticmethod
    def _asset_payload(run_id: str, *, source_projection_revision: int) -> dict[str, object]:
        return {
            "asset_id": "company-public-web-asset-d1m-atomic-union",
            "target_company": "Concurrent Lineage Labs",
            "company_key": "concurrentlineagelabs",
            "latest_run_id": run_id,
            "source_run_ids": [run_id],
            "source_family": "company_research",
            "asset_kind": "seed_url",
            "title": f"Concurrent Lineage Labs research ({run_id})",
            "url": "https://concurrent-lineage.example/research",
            "normalized_url_key": "concurrent-lineage-research",
            "summary": f"Observed by {run_id}",
            "model_safe_payload": {"run_id": run_id},
            "artifact_refs": {},
            "status": "active",
            "metadata": {
                "collection_mode": "seed_url_only",
                "source_projection_revision": source_projection_revision,
            },
        }

    def test_pg_asset_upsert_atomically_unions_every_concurrent_source_run_id(self) -> None:
        run_ids = [f"company-public-web-run-union-{12 - index:02d}" for index in range(12)]
        stores = [self._store(f"union-{index:02d}") for index in range(len(run_ids))]
        barrier = threading.Barrier(len(run_ids))

        def publish(store, run_id: str, revision: int) -> dict[str, object]:
            barrier.wait(timeout=10)
            return store.upsert_company_public_web_asset(
                self._asset_payload(run_id, source_projection_revision=revision)
            )

        with ThreadPoolExecutor(max_workers=len(run_ids)) as executor:
            futures = [
                executor.submit(publish, store, run_id, revision)
                for revision, (store, run_id) in enumerate(zip(stores, run_ids, strict=True), start=1)
            ]
            written = [future.result(timeout=20) for future in futures]

        final = stores[0].get_company_public_web_asset(asset_id="company-public-web-asset-d1m-atomic-union")
        self.assertIsNotNone(final)
        self.assertEqual(final["source_run_ids"], sorted(run_ids))
        self.assertEqual(final["latest_run_id"], run_ids[-1])
        self.assertEqual(final["metadata"]["source_projection_revision"], len(run_ids))
        self.assertTrue(all(run_id in row["source_run_ids"] for run_id, row in zip(run_ids, written, strict=True)))
        for run_id in run_ids:
            rows = stores[0].list_company_public_web_assets(
                company_key="concurrentlineagelabs",
                source_run_id=run_id,
                limit=10,
            )
            self.assertEqual([row["asset_id"] for row in rows], [final["asset_id"]])

    def test_retry_attempt_repairs_crash_after_owner_finalize_before_source_publication(self) -> None:
        store = self._store("post-finalize-repair")
        suffix = "repair"
        command = self._claim_source_command(store, suffix=suffix, lease_owner="effect-owner-1")
        payload = self._refresh_payload(
            command,
            artifact_root=Path(self._tempdir.name) / "post-finalize-repair-artifacts",
        )

        with mock.patch.object(
            store,
            "upsert_company_public_web_asset",
            side_effect=RuntimeError("simulated publication crash"),
        ):
            with self.assertRaisesRegex(RuntimeError, "simulated publication crash"):
                refresh_company_public_web_assets(
                    store=store,
                    runtime_dir=self._tempdir.name,
                    payload=payload,
                )

        completed = store.get_company_public_web_asset_run(run_id=payload["run_id"])
        self.assertEqual(completed["status"], "completed")
        self.assertEqual(store.list_company_public_web_assets(company_key="effectfencelabs"), [])
        first_revision = completed["metadata"]["source_projection_revision"]

        retried_command = self._retry_source_command(
            store,
            command=command,
            suffix=suffix,
            lease_owner="effect-owner-2",
        )

        replay = refresh_company_public_web_assets(
            store=store,
            runtime_dir=self._tempdir.name,
            payload=self._payload_with_source_owner(payload, retried_command),
        )

        self.assertEqual(replay["status"], "joined")
        self.assertEqual(replay["source_effect_publication"]["reason"], "completed_run_effects_replayed")
        self.assertEqual(len(replay["assets"]), 1)
        self.assertEqual(replay["run"]["metadata"]["source_projection_revision"], first_revision)
        self.assertEqual(
            store.list_company_public_web_assets(
                company_key="effectfencelabs",
                source_run_id=payload["run_id"],
            )[0]["asset_id"],
            replay["assets"][0]["asset_id"],
        )

    def test_open_mode_completed_replay_is_read_only_and_calls_no_effect_publisher(self) -> None:
        store = self._store("open-mode-read-only")
        payload = {
            "run_id": "company-public-web-run-open-mode-read-only",
            "target_company": "Open Mode Read Only Labs",
            "company_key": "openmodereadonlylabs",
            "source_families": ["company_homepage"],
            "seed_urls": ["https://open-mode-read-only.example/"],
            "collection_mode": "seed_url_only",
            "artifact_root": str(Path(self._tempdir.name) / "open-mode-read-only"),
        }
        completed = refresh_company_public_web_assets(
            store=store,
            runtime_dir=self._tempdir.name,
            payload=payload,
        )
        self.assertEqual(completed["status"], "completed")

        with (
            mock.patch.object(
                company_public_web_assets_module,
                "publish_company_public_web_artifact_publication",
                wraps=company_public_web_assets_module.publish_company_public_web_artifact_publication,
            ) as artifact_publisher,
            mock.patch.object(
                company_public_web_assets_module,
                "publish_company_public_web_completed_run_effects",
                wraps=company_public_web_assets_module.publish_company_public_web_completed_run_effects,
            ) as effect_publisher,
        ):
            replay = refresh_company_public_web_assets(
                store=store,
                runtime_dir=self._tempdir.name,
                payload=payload,
            )

        self.assertEqual(replay["status"], "joined")
        self.assertEqual(replay["source_effect_publication"]["status"], "not_attempted")
        self.assertEqual(replay["source_effect_publication"]["reason"], "completed_run_replay_read_only")
        self.assertEqual(len(replay["assets"]), 1)
        artifact_publisher.assert_not_called()
        effect_publisher.assert_not_called()

    def test_reclaimed_stale_attempt_has_zero_source_writes_and_cannot_overwrite_winner_artifacts(self) -> None:
        first_store = self._store("stale-first")
        second_store = self._store("stale-second")
        first_command = self._claim_source_command(first_store, suffix="stale", lease_owner="effect-owner-1")
        artifact_root = Path(self._tempdir.name) / "shared-effect-artifacts"
        first_payload = self._refresh_payload(first_command, artifact_root=artifact_root)
        finalize_entered = threading.Event()
        release_stale_finalize = threading.Event()
        real_finalize = first_store.finalize_company_public_web_asset_run_if_owned

        def block_stale_finalize(payload: dict[str, Any]) -> dict[str, Any]:
            finalize_entered.set()
            self.assertTrue(release_stale_finalize.wait(timeout=15))
            return real_finalize(payload)

        with (
            mock.patch.object(
                first_store,
                "finalize_company_public_web_asset_run_if_owned",
                side_effect=block_stale_finalize,
            ),
            mock.patch.object(
                first_store,
                "upsert_company_public_web_asset",
                wraps=first_store.upsert_company_public_web_asset,
            ) as stale_source_writer,
            ThreadPoolExecutor(max_workers=1) as executor,
        ):
            stale_future = executor.submit(
                refresh_company_public_web_assets,
                store=first_store,
                runtime_dir=self._tempdir.name,
                payload=first_payload,
            )
            self.assertTrue(finalize_entered.wait(timeout=15))
            self.assertEqual(first_store.list_company_public_web_assets(company_key="effectfencelabs"), [])
            self.assertFalse(artifact_root.exists())

            self._expire_command_lease(first_store, first_command["command_id"])
            second_command = self._claim_source_command(
                second_store,
                suffix="stale",
                lease_owner="effect-owner-2",
            )
            winner = refresh_company_public_web_assets(
                store=second_store,
                runtime_dir=self._tempdir.name,
                payload=self._refresh_payload(second_command, artifact_root=artifact_root),
            )
            self.assertEqual(winner["status"], "completed")
            winner_files = self._filesystem_file_map(artifact_root)
            self.assertEqual(len(winner_files), len(winner["artifact_paths"]))

            release_stale_finalize.set()
            stale = stale_future.result(timeout=15)

        self.assertEqual(stale["status"], "owner_lost")
        self.assertEqual(stale_source_writer.call_count, 0)
        self.assertEqual(self._filesystem_file_map(artifact_root), winner_files)
        self.assertEqual(
            len(
                second_store.list_company_public_web_assets(
                    company_key="effectfencelabs",
                    source_run_id=first_payload["run_id"],
                )
            ),
            1,
        )

    def test_completed_replay_repairs_crash_mid_artifact_publication_before_source_effects(self) -> None:
        for crash_call in (1, 2):
            with self.subTest(crash_call=crash_call):
                store = self._store(f"artifact-publication-repair-{crash_call}")
                suffix = f"artifact-repair-{crash_call}"
                command = self._claim_source_command(
                    store,
                    suffix=suffix,
                    lease_owner=f"effect-owner-{crash_call}",
                )
                artifact_root = Path(self._tempdir.name) / f"artifact-publication-repair-{crash_call}"
                payload = {
                    **self._refresh_payload(command, artifact_root=artifact_root),
                    "run_id": f"company-public-web-run-artifact-repair-{crash_call}",
                    "seed_urls": [f"https://artifact-repair-{crash_call}.example/"],
                }
                real_publish = company_public_web_assets_module._publish_company_public_web_content_addressed_object
                published_count = 0

                def crash_during_publication(
                    artifact_path: Path,
                    *,
                    content: bytes,
                    expected_sha256: str,
                ) -> None:
                    nonlocal published_count
                    published_count += 1
                    if published_count == crash_call:
                        raise RuntimeError("simulated artifact publication crash")
                    real_publish(
                        artifact_path,
                        content=content,
                        expected_sha256=expected_sha256,
                    )

                with mock.patch.object(
                    company_public_web_assets_module,
                    "_publish_company_public_web_content_addressed_object",
                    side_effect=crash_during_publication,
                ):
                    with self.assertRaisesRegex(RuntimeError, "simulated artifact publication crash"):
                        refresh_company_public_web_assets(
                            store=store,
                            runtime_dir=self._tempdir.name,
                            payload=payload,
                        )

                completed = store.get_company_public_web_asset_run(run_id=payload["run_id"])
                self.assertEqual(completed["status"], "completed")
                self.assertEqual(len(self._filesystem_file_map(artifact_root)), crash_call - 1)
                self.assertEqual(company_public_web_materialization_snapshot_identity(completed), {})
                self.assertEqual(
                    store.list_company_public_web_assets(
                        company_key="effectfencelabs",
                        source_run_id=payload["run_id"],
                    ),
                    [],
                )

                retried_command = self._retry_source_command(
                    store,
                    command=command,
                    suffix=suffix,
                    lease_owner=f"effect-owner-retry-{crash_call}",
                )

                replay = refresh_company_public_web_assets(
                    store=store,
                    runtime_dir=self._tempdir.name,
                    payload=self._payload_with_source_owner(payload, retried_command),
                )

                self.assertEqual(replay["status"], "joined")
                self.assertEqual(
                    len(self._filesystem_file_map(artifact_root)),
                    len(replay["artifact_paths"]),
                )
                self.assertTrue(company_public_web_materialization_snapshot_identity(replay["run"]))
                self.assertEqual(len(replay["assets"]), 1)

    def test_completed_replay_of_older_run_preserves_newer_source_and_canonical_projection(self) -> None:
        store = self._store("monotonic-replay")
        company_key = "monotonicreplaylabs"
        command_a = self._claim_source_command(
            store,
            suffix="monotonic-a",
            lease_owner="monotonic-owner-a",
            target_company="Monotonic Replay Labs",
            company_key=company_key,
        )
        command_b = self._claim_source_command(
            store,
            suffix="monotonic-b",
            lease_owner="monotonic-owner-b",
            target_company="Monotonic Replay Labs",
            company_key=company_key,
        )
        shared_payload = {
            "target_company": "Monotonic Replay Labs",
            "company_key": company_key,
            "source_families": ["company_homepage"],
            "seed_urls": ["https://monotonic-replay.example/"],
            "collection_mode": "seed_url_only",
            "force_refresh": True,
        }
        run_a_payload = {
            **shared_payload,
            "run_id": "company-public-web-run-monotonic-z-old",
            "refresh_nonce": "monotonic-a",
            "artifact_root": str(Path(self._tempdir.name) / "monotonic-a"),
            "_source_workflow_command_id": command_a["command_id"],
            "_source_workflow_command_attempt": command_a["attempt"],
            "_source_workflow_command_lease_owner": command_a["lease_owner"],
        }
        run_b_payload = {
            **shared_payload,
            "run_id": "company-public-web-run-monotonic-a-new",
            "refresh_nonce": "monotonic-b",
            "artifact_root": str(Path(self._tempdir.name) / "monotonic-b"),
            "_source_workflow_command_id": command_b["command_id"],
            "_source_workflow_command_attempt": command_b["attempt"],
            "_source_workflow_command_lease_owner": command_b["lease_owner"],
        }

        self.assertGreater(run_a_payload["run_id"], run_b_payload["run_id"])
        with (
            mock.patch.object(
                company_public_web_assets_module,
                "utc_sql_timestamp",
                return_value="2026-07-17 01:00:00",
            ),
            mock.patch.object(
                company_public_web_assets_module,
                "utc_source_projection_timestamp",
                return_value="2026-07-17T01:00:00.000001Z",
            ),
        ):
            run_a = refresh_company_public_web_assets(
                store=store,
                runtime_dir=self._tempdir.name,
                payload=run_a_payload,
            )
        with (
            mock.patch.object(
                company_public_web_assets_module,
                "utc_sql_timestamp",
                return_value="2026-07-17 01:00:00",
            ),
            mock.patch.object(
                company_public_web_assets_module,
                "utc_source_projection_timestamp",
                return_value="2026-07-17T00:00:00.000000Z",
            ),
        ):
            run_b = refresh_company_public_web_assets(
                store=store,
                runtime_dir=self._tempdir.name,
                payload=run_b_payload,
            )

        self.assertEqual(run_a["status"], "completed")
        self.assertEqual(run_b["status"], "completed")
        revision_a = run_a["run"]["metadata"]["source_projection_revision"]
        revision_b = run_b["run"]["metadata"]["source_projection_revision"]
        self.assertGreater(revision_b, revision_a)
        replay_a = refresh_company_public_web_assets(
            store=store,
            runtime_dir=self._tempdir.name,
            payload=run_a_payload,
        )

        self.assertEqual(replay_a["status"], "joined")
        source_assets = store.list_company_public_web_assets(company_key=company_key, limit=10)
        self.assertEqual(len(source_assets), 1)
        self.assertEqual(source_assets[0]["latest_run_id"], run_b_payload["run_id"])
        projection_order_key = source_assets[0]["metadata"]["source_projection_order_key"]
        self.assertEqual(projection_order_key, projection_order_key.strip())
        self.assertEqual(
            projection_order_key,
            f"company_public_web_source_projection_v2:{revision_b:020d}:{run_b_payload['run_id']}",
        )
        self.assertEqual(source_assets[0]["metadata"]["source_projection_revision"], revision_b)
        self.assertEqual(
            source_assets[0]["source_run_ids"],
            sorted([run_a_payload["run_id"], run_b_payload["run_id"]]),
        )
        self.assertEqual(replay_a["assets"][0]["latest_run_id"], run_b_payload["run_id"])

        stale_canonical_sync = sync_company_public_web_assets_to_company_asset_layer(
            store=store,
            run=run_a["run"],
            assets=company_public_web_materialization_assets_for_run(run_a["run"]),
        )
        self.assertEqual(stale_canonical_sync["status"], "synced")
        canonical_assets = store.list_company_assets(company_key=company_key, limit=10)
        canonical_evidence = store.list_company_evidence(company_key=company_key, limit=10)
        self.assertEqual(len(canonical_assets), 1)
        self.assertEqual(len(canonical_evidence), 1)
        self.assertEqual(canonical_assets[0]["source_run_id"], run_b_payload["run_id"])
        self.assertEqual(
            canonical_assets[0]["metadata"]["source_run_ids"],
            sorted([run_a_payload["run_id"], run_b_payload["run_id"]]),
        )
        self.assertEqual(
            canonical_assets[0]["metadata"]["materialized_source_run_id"],
            run_b_payload["run_id"],
        )
        self.assertEqual(
            canonical_evidence[0]["metadata"]["materialized_source_run_id"],
            run_b_payload["run_id"],
        )
        self.assertEqual(
            canonical_evidence[0]["metadata"]["source_run_ids"],
            sorted([run_a_payload["run_id"], run_b_payload["run_id"]]),
        )
        self.assertEqual(canonical_assets[0]["metadata"]["source_projection_revision"], revision_b)
        self.assertEqual(canonical_evidence[0]["metadata"]["source_projection_revision"], revision_b)

    def test_snapshot_rejects_content_mutation_at_a_recorded_artifact_path(self) -> None:
        store = self._store("artifact-content-drift")
        result = refresh_company_public_web_assets(
            store=store,
            runtime_dir=self._tempdir.name,
            payload={
                "run_id": "company-public-web-run-artifact-content-drift",
                "target_company": "Artifact Drift Labs",
                "company_key": "artifactdriftlabs",
                "source_families": ["company_homepage"],
                "seed_urls": ["https://artifact-drift.example/"],
                "collection_mode": "seed_url_only",
                "defer_company_asset_sync": True,
            },
        )
        run = dict(result["run"])
        self.assertTrue(company_public_web_materialization_snapshot_identity(run))

        drifted_run = {**run, "metadata": dict(run["metadata"])}
        drifted_run["metadata"]["source_projection_completed_at"] = "2026-07-17T00:00:00.000000Z"
        self.assertEqual(company_public_web_materialization_snapshot_identity(drifted_run), {})

        drifted_revision_run = {**run, "metadata": dict(run["metadata"])}
        drifted_revision_run["metadata"]["source_projection_revision"] += 1
        self.assertEqual(company_public_web_materialization_snapshot_identity(drifted_revision_run), {})

        summary_path = Path(result["artifact_paths"]["summary"])
        summary_path.write_text('{"tampered":true}', encoding="utf-8")

        self.assertEqual(company_public_web_materialization_snapshot_identity(run), {})


def test_seed_url_family_inference_is_not_positionally_zipped_to_sorted_allow_set() -> None:
    assets = build_company_public_web_seed_assets(
        target_company="OpenAI",
        company_key="openai",
        source_families=["company_engineering", "company_homepage"],
        seed_urls=["https://openai.com/", "https://openai.com/engineering"],
        run_id="company-public-web-run-independent-pairing",
        max_assets=10,
    )

    assert {asset["url"]: asset["source_family"] for asset in assets} == {
        "https://openai.com/": "company_homepage",
        "https://openai.com/engineering": "company_engineering",
    }
