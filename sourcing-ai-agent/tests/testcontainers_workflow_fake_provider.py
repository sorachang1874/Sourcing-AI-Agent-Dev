from __future__ import annotations

import json
import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.api import create_server
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.harvest_connectors import _submit_harvest_actor_run
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.object_storage import ObjectStorageConfig, build_object_storage_client
from sourcing_agent.orchestrator import SourcingOrchestrator
from sourcing_agent.semantic_provider import LocalSemanticProvider
from sourcing_agent.settings import (
    AppSettings,
    HarvestActorSettings,
    HarvestSettings,
    ObjectStorageSettings,
    QwenSettings,
    SemanticProviderSettings,
)
from sourcing_agent.storage import ControlPlaneStore
from tests.fake_apify_provider import FakeApifyProvider
from tests.testcontainers_helpers import (
    libpq_dsn_from_container,
    psycopg,
    require_testcontainers_pg,
    start_postgres_container,
)


class TestcontainersWorkflowFakeProviderTest(unittest.TestCase):
    def test_pg_backed_api_accepts_fake_apify_webhook_and_records_remote_terminal_worker(self) -> None:
        require_testcontainers_pg()
        assert psycopg is not None

        with tempfile.TemporaryDirectory() as tempdir:
            project_root = Path(tempdir)
            runtime_dir = project_root / "runtime"
            object_store_dir = runtime_dir / "object_store"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            postgres = start_postgres_container()
            fake_apify = FakeApifyProvider()
            fake_apify.add_actor_run(
                actor_id="harvestapi/linkedin-profile-scraper",
                run_id="run-container-fake-apify",
                dataset_id="dataset-container-fake-apify",
                dataset_items=[
                    {
                        "name": "Ada Lovelace",
                        "linkedinUrl": "https://www.linkedin.com/in/ada-lovelace-real/",
                    }
                ],
            )
            try:
                dsn = libpq_dsn_from_container(postgres)
                base_env = {
                    "SOURCING_CONTROL_PLANE_POSTGRES_DSN": dsn,
                    "SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA": "workflow_fake_provider",
                    "SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE": "postgres_only",
                    "SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES": "1",
                    "SOURCING_PG_ONLY_SQLITE_BACKEND": "shared_memory",
                    "SOURCING_RUNTIME_DIR": str(runtime_dir),
                    "SOURCING_RUNTIME_ENVIRONMENT": "local_dev",
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "live",
                    "SOURCING_LIVE_PROVIDER_CONFIRM": "1",
                    "SOURCING_ALLOW_ISOLATED_LIVE_PROVIDER_ACCESS": "1",
                    "SOURCING_PROVIDER_WEBHOOK_TOKEN": "container-provider-secret",
                    "OBJECT_STORAGE_PROVIDER": "filesystem",
                    "OBJECT_STORAGE_LOCAL_DIR": str(object_store_dir),
                    "OBJECT_STORAGE_PREFIX": "workflow-fake-provider",
                }
                with fake_apify:
                    env = {**base_env, "SOURCING_APIFY_API_BASE_URL": fake_apify.base_url}
                    with mock.patch.dict(os.environ, env, clear=False):
                        store = ControlPlaneStore(runtime_dir / "sourcing_agent.db")
                        settings = AppSettings(
                            project_root=project_root,
                            runtime_dir=runtime_dir,
                            secrets_file=runtime_dir / "secrets" / "providers.local.json",
                            jobs_dir=runtime_dir / "jobs",
                            company_assets_dir=runtime_dir / "company_assets",
                            db_path=runtime_dir / "sourcing_agent.db",
                            qwen=QwenSettings(enabled=False),
                            semantic=SemanticProviderSettings(enabled=False),
                            harvest=HarvestSettings(
                                profile_scraper=HarvestActorSettings(
                                    enabled=True,
                                    api_token="fake-token",
                                    actor_id="harvestapi/linkedin-profile-scraper",
                                    timeout_seconds=60,
                                    max_total_charge_usd=1.25,
                                )
                            ),
                            object_storage=ObjectStorageSettings(
                                enabled=True,
                                provider="filesystem",
                                local_dir=str(object_store_dir),
                                prefix="workflow-fake-provider",
                            ),
                        )
                        catalog = AssetCatalog.discover()
                        model_client = DeterministicModelClient()
                        acquisition_engine = AcquisitionEngine(catalog, settings, store, model_client)
                        orchestrator = SourcingOrchestrator(
                            catalog=catalog,
                            store=store,
                            jobs_dir=settings.jobs_dir,
                            model_client=model_client,
                            semantic_provider=LocalSemanticProvider(),
                            acquisition_engine=acquisition_engine,
                        )
                        dispatches: list[dict[str, object]] = []

                        def _record_dispatch(job_id: str, payload: dict[str, object]) -> dict[str, object]:
                            dispatch = {
                                "status": "already_running",
                                "scope": "job_scoped",
                                "service_name": f"job-recovery-{job_id}",
                                "job_id": job_id,
                                "payload": dict(payload or {}),
                            }
                            dispatches.append(dispatch)
                            return dispatch

                        with mock.patch.object(orchestrator, "ensure_job_scoped_recovery", side_effect=_record_dispatch):
                            session = store.create_agent_runtime_session(
                                job_id="job-container-fake-provider",
                                target_company="Container Test Co",
                                request_payload={
                                    "raw_user_request": "Find people at Container Test Co",
                                    "target_company": "Container Test Co",
                                },
                                plan_payload={"strategy": "fake-provider-contract"},
                                runtime_mode="agent_runtime",
                                lanes=[],
                            )
                            span = store.create_agent_trace_span(
                                session_id=int(session["session_id"]),
                                job_id="job-container-fake-provider",
                                lane_id="enrichment_specialist",
                                span_name="harvest-profile-batch",
                                stage="waiting_remote_harvest",
                            )
                            worker = store.create_or_resume_agent_worker(
                                session_id=int(session["session_id"]),
                                job_id="job-container-fake-provider",
                                span_id=int(span["span_id"]),
                                lane_id="enrichment_specialist",
                                worker_key="harvest-profile-batch::1",
                                input_payload={"urls": ["https://www.linkedin.com/in/ada-lovelace-real/"]},
                                metadata={"recovery_kind": "harvest_profile_batch"},
                            )
                            running = store.mark_agent_worker_running(int(worker["worker_id"]))
                            assert running is not None
                            store.checkpoint_agent_worker(
                                int(worker["worker_id"]),
                                checkpoint_payload={
                                    "stage": "waiting_remote_harvest",
                                    "run_id": "run-container-fake-apify",
                                    "dataset_id": "dataset-container-fake-apify",
                                },
                                output_payload={},
                                status="running",
                            )

                            server = create_server(orchestrator, host="127.0.0.1", port=0)
                            thread = threading.Thread(target=server.serve_forever, daemon=True)
                            thread.start()
                            try:
                                host, port = server.server_address
                                webhook_url = f"http://{host}:{port}/api/providers/apify/webhook"
                                with mock.patch.dict(os.environ, {"SOURCING_APIFY_WEBHOOK_URL": webhook_url}, clear=False):
                                    submit_payload = _submit_harvest_actor_run(
                                        settings.harvest.profile_scraper,
                                        {"urls": ["https://www.linkedin.com/in/ada-lovelace-real/"]},
                                    )
                                    delivered = fake_apify.deliver_webhooks(run_id="run-container-fake-apify")
                            finally:
                                server.shutdown()
                                server.server_close()
                                thread.join(timeout=5)

                        object_client = build_object_storage_client(
                            ObjectStorageConfig(
                                enabled=True,
                                provider="filesystem",
                                local_dir=str(object_store_dir),
                                prefix="workflow-fake-provider",
                            )
                        )
                        object_client.upload_bytes(
                            json.dumps({"run_id": "run-container-fake-apify"}).encode("utf-8"),
                            "provider-events/run-container-fake-apify.json",
                            content_type="application/json",
                        )
                        provider_event_object_visible = object_client.has_object(
                            "provider-events/run-container-fake-apify.json"
                        )
                        final_worker = store.get_agent_worker(worker_id=int(worker["worker_id"]))
                        events = store.list_job_events("job-container-fake-provider")

                with psycopg.connect(dsn) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute("SET search_path TO workflow_fake_provider")
                        cursor.execute(
                            "SELECT COUNT(*) FROM agent_worker_runs WHERE job_id = %s",
                            ("job-container-fake-provider",),
                        )
                        pg_worker_count = int(cursor.fetchone()[0])
            finally:
                postgres.stop()

        self.assertEqual(submit_payload["data"]["id"], "run-container-fake-apify")
        self.assertEqual(delivered[0]["status"], 202)
        assert final_worker is not None
        checkpoint = dict(final_worker.get("checkpoint") or {})
        self.assertTrue(checkpoint.get("force_scripted_terminal_fetch"))
        self.assertEqual(checkpoint["remote_provider_terminal_event"]["run_id"], "run-container-fake-apify")
        self.assertEqual(checkpoint["remote_provider_terminal_event"]["dataset_id"], "dataset-container-fake-apify")
        self.assertEqual(dispatches[0]["job_id"], "job-container-fake-provider")
        dispatch_payload = dict(dispatches[0]["payload"])
        self.assertEqual(dispatch_payload["remote_provider_event_worker_ids"], [int(final_worker["worker_id"])])
        self.assertTrue(provider_event_object_visible)
        self.assertEqual(pg_worker_count, 1)
        self.assertTrue(any(event.get("stage") == "remote_provider_event" for event in events))


if __name__ == "__main__":
    unittest.main()
