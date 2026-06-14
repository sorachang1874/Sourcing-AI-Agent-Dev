import json
import os
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock
from urllib import request as urllib_request

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.api import create_server
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.domain import Candidate, JobRequest
from sourcing_agent.durable_runtime import (
    EXCEL_INTAKE_RUN_COMMAND_TYPE,
    EXCEL_INTAKE_RUN_OWNER,
    PROJECTION_RUN_SCOPE_FINALIZE_COMMAND_TYPE,
    PROJECTION_RUN_SCOPE_FINALIZE_OWNER,
    SNAPSHOT_COMPACTION_RUN_OWNER,
    legacy_job_operation_id,
    legacy_job_workflow_run_id,
)
from sourcing_agent.excel_intake_owner import ExcelIntakeOwner
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.orchestrator import SourcingOrchestrator, _plan_hydration_request_signature
from sourcing_agent.semantic_provider import LocalSemanticProvider
from sourcing_agent.settings import (
    AppSettings,
    HarvestActorSettings,
    HarvestSettings,
    QwenSettings,
    SemanticProviderSettings,
)
from sourcing_agent.storage import ControlPlaneStore
from tests.pg_durable_runtime import PGDurableRuntimeTestMixin


class FrontendHistoryRecoveryTest(PGDurableRuntimeTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self._start_pg_durable_runtime(runtime_dir=Path(self.tempdir.name))
        self.catalog = AssetCatalog.discover()
        self.store = ControlPlaneStore(f"{self.tempdir.name}/test.db")
        self.settings = AppSettings(
            project_root=Path(self.tempdir.name),
            runtime_dir=Path(self.tempdir.name),
            secrets_file=Path(self.tempdir.name) / "providers.local.json",
            jobs_dir=Path(self.tempdir.name) / "jobs",
            company_assets_dir=Path(self.tempdir.name) / "company_assets",
            db_path=Path(self.tempdir.name) / "test.db",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        self.model_client = DeterministicModelClient()
        self.semantic_provider = LocalSemanticProvider()
        self.acquisition_engine = AcquisitionEngine(self.catalog, self.settings, self.store, self.model_client)
        self.orchestrator = SourcingOrchestrator(
            catalog=self.catalog,
            store=self.store,
            jobs_dir=f"{self.tempdir.name}/jobs",
            model_client=self.model_client,
            semantic_provider=self.semantic_provider,
            acquisition_engine=self.acquisition_engine,
        )
        self._runtime_env_patcher = mock.patch.dict(
            os.environ,
            {"SOURCING_COMPANY_REGISTRY_RUNTIME_DIR": str(self.settings.runtime_dir)},
            clear=False,
        )
        self._runtime_env_patcher.start()

    def tearDown(self) -> None:
        self._stop_pg_durable_runtime()
        self._runtime_env_patcher.stop()
        self.tempdir.cleanup()

    def _write_company_identity_snapshot(self, *, target_company: str, snapshot_id: str) -> None:
        company_key = "".join(ch for ch in target_company.lower() if ch.isalnum())
        snapshot_dir = self.settings.company_assets_dir / company_key / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = {
            "requested_name": target_company,
            "canonical_name": target_company,
            "company_key": company_key,
            "linkedin_slug": company_key,
            "aliases": [target_company],
        }
        (snapshot_dir / "identity.json").write_text(
            json.dumps(identity, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (snapshot_dir.parent / "latest_snapshot.json").write_text(
            json.dumps({"snapshot_id": snapshot_id, "company_identity": identity}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def test_plan_workflow_persists_frontend_history_link(self) -> None:
        history_id = "history-plan-1"
        self._write_company_identity_snapshot(target_company="Skild AI", snapshot_id="20260415T020101")
        planned = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "帮我找Skild AI做Pre-train方向的人",
                "history_id": history_id,
            }
        )

        link = self.store.get_frontend_history_link(history_id)
        self.assertIsNotNone(link)
        assert link is not None
        self.assertEqual(link["history_id"], history_id)
        self.assertEqual(link["phase"], "plan")
        self.assertEqual(link["review_id"], int(planned["plan_review_session"]["review_id"]))
        self.assertEqual(link["query_text"], "帮我找Skild AI做Pre-train方向的人")
        self.assertEqual(link["request"]["target_company"], "Skild AI")
        self.assertTrue(link["plan"])
        self.assertTrue(dict(link["metadata"].get("effective_execution_semantics") or {}))
        self.assertTrue(dict(link["metadata"].get("dispatch_preview") or {}))
        self.assertTrue(dict(link["metadata"].get("provider_execution_manifest") or {}))

    def test_plan_workflow_persists_full_roster_provider_manifest_without_generic_seed_queries(self) -> None:
        history_id = "history-lovable-full-roster-manifest"
        self._write_company_identity_snapshot(target_company="Lovable", snapshot_id="20260505T020101")

        planned = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "帮我找Lovable的全部成员",
                "history_id": history_id,
            }
        )

        acquisition_strategy = dict(dict(planned["plan"]).get("acquisition_strategy") or {})
        self.assertEqual(acquisition_strategy.get("strategy_type"), "full_company_roster")
        self.assertEqual(acquisition_strategy.get("search_seed_queries"), [])
        link = self.store.get_frontend_history_link(history_id)
        self.assertIsNotNone(link)
        assert link is not None
        manifest = dict(link["metadata"].get("provider_execution_manifest") or {})
        lanes = list(manifest.get("lanes") or [])
        current_lane = next(lane for lane in lanes if lane.get("lane_id") == "current_company_employees")
        self.assertEqual(current_lane.get("provider"), "harvest_company_employees")
        self.assertEqual(current_lane.get("operation"), "company_employees")
        self.assertEqual(current_lane.get("query_texts"), [])
        self.assertFalse(current_lane.get("provider_facing_query"))

        recovered = self.orchestrator.get_frontend_history_recovery(history_id)
        recovered_manifest = dict(recovered["recovery"]["metadata"].get("provider_execution_manifest") or {})
        self.assertEqual(recovered_manifest.get("strategy_type"), "full_company_roster")
        recovered_current_lane = next(
            lane for lane in list(recovered_manifest.get("lanes") or []) if lane.get("lane_id") == "current_company_employees"
        )
        self.assertEqual(recovered_current_lane.get("query_texts"), [])

    def test_submit_plan_workflow_persists_pending_frontend_history_link(self) -> None:
        history_id = "history-plan-submit-1"
        with mock.patch.object(self.orchestrator, "_queue_plan_hydration", return_value=None):
            submitted = self.orchestrator.submit_plan_workflow(
                {
                    "raw_user_request": "我想要OpenAI做Reasoning方向的人",
                    "history_id": history_id,
                }
            )

        self.assertEqual(submitted["status"], "pending")
        self.assertEqual(submitted["history_id"], history_id)
        link = self.store.get_frontend_history_link(history_id)
        self.assertIsNotNone(link)
        assert link is not None
        self.assertEqual(link["phase"], "plan")
        self.assertFalse(link["plan"])
        self.assertEqual(str(link["metadata"].get("source") or ""), "plan_workflow_submit")
        self.assertEqual(
            str(dict(link["metadata"].get("plan_generation") or {}).get("status") or ""),
            "queued",
        )

    def test_run_plan_hydration_promotes_pending_history_to_ready_plan(self) -> None:
        history_id = "history-plan-hydration-1"
        plan_request_id = "req-plan-hydration-1"
        queued_at = "2026-04-24T01:23:45+00:00"
        self._write_company_identity_snapshot(target_company="OpenAI", snapshot_id="20260424T012345")
        self.store.upsert_frontend_history_link(
            {
                "history_id": history_id,
                "query_text": "我想要OpenAI做Reasoning方向的人",
                "target_company": "OpenAI",
                "phase": "plan",
                "request": {
                    "raw_user_request": "我想要OpenAI做Reasoning方向的人",
                    "history_id": history_id,
                },
                "metadata": {
                    "source": "plan_workflow_submit",
                    "plan_generation": {
                        "status": "queued",
                        "request_id": plan_request_id,
                        "queued_at": queued_at,
                        "submitted_at": queued_at,
                    },
                },
            }
        )
        with self.orchestrator._plan_hydration_lock:
            self.orchestrator._plan_hydration_inflight[history_id] = {
                "request_id": plan_request_id,
                "queued_at": queued_at,
                "payload": {
                    "raw_user_request": "我想要OpenAI做Reasoning方向的人",
                    "history_id": history_id,
                },
            }

        self.orchestrator._run_plan_hydration(
            history_id=history_id,
            payload={
                "raw_user_request": "我想要OpenAI做Reasoning方向的人",
                "history_id": history_id,
            },
            plan_request_id=plan_request_id,
            queued_at=queued_at,
        )

        link = self.store.get_frontend_history_link(history_id)
        self.assertIsNotNone(link)
        assert link is not None
        self.assertEqual(link["phase"], "plan")
        self.assertTrue(link["plan"])
        self.assertGreater(int(link["review_id"] or 0), 0)
        self.assertEqual(
            str(dict(link["metadata"].get("plan_generation") or {}).get("status") or ""),
            "completed",
        )
        self.assertTrue(dict(link["metadata"].get("effective_execution_semantics") or {}))
        self.assertTrue(dict(link["metadata"].get("dispatch_preview") or {}))

    def test_sync_plan_and_async_hydrated_plan_are_equivalent_for_same_request(self) -> None:
        """C1 consolidation-safety oracle: deleting the synchronous /api/plan route
        loses nothing, because the async submit path runs the SAME plan_workflow and
        persists a field-equal compiled plan to the frontend history link (which the
        live UI reads via /api/plan/submit + poll). Pinned before the route deletion.
        """
        raw_request = "我想要OpenAI做Reasoning方向的人"
        self._write_company_identity_snapshot(target_company="OpenAI", snapshot_id="20260424T012345")

        # Synchronous path (the route C1 deletes).
        sync_result = self.orchestrator.plan_workflow(
            {"raw_user_request": raw_request, "history_id": "history-equiv-sync"}
        )
        link_sync = self.store.get_frontend_history_link("history-equiv-sync")

        # Async path: the hydration worker literally calls plan_workflow (orch:1799).
        history_id = "history-equiv-async"
        plan_request_id = "req-equiv-async"
        queued_at = "2026-04-24T01:23:45+00:00"
        self.store.upsert_frontend_history_link(
            {
                "history_id": history_id,
                "query_text": raw_request,
                "target_company": "OpenAI",
                "phase": "plan",
                "request": {"raw_user_request": raw_request, "history_id": history_id},
                "metadata": {
                    "source": "plan_workflow_submit",
                    "plan_generation": {
                        "status": "queued",
                        "request_id": plan_request_id,
                        "queued_at": queued_at,
                        "submitted_at": queued_at,
                    },
                },
            }
        )
        with self.orchestrator._plan_hydration_lock:
            self.orchestrator._plan_hydration_inflight[history_id] = {
                "request_id": plan_request_id,
                "queued_at": queued_at,
                "payload": {"raw_user_request": raw_request, "history_id": history_id},
            }
        self.orchestrator._run_plan_hydration(
            history_id=history_id,
            payload={"raw_user_request": raw_request, "history_id": history_id},
            plan_request_id=plan_request_id,
            queued_at=queued_at,
        )
        link_async = self.store.get_frontend_history_link(history_id)

        assert link_sync is not None and link_async is not None
        # The compiled plan the frontend consumes is field-equal across transports.
        self.assertTrue(link_sync["plan"])
        self.assertEqual(link_sync["plan"], sync_result["plan"])
        self.assertEqual(link_async["plan"], link_sync["plan"])
        # Both transports produce a real review session and the same execution semantics.
        self.assertGreater(int(link_sync["review_id"] or 0), 0)
        self.assertGreater(int(link_async["review_id"] or 0), 0)
        self.assertEqual(
            dict(link_async["metadata"].get("effective_execution_semantics") or {}),
            dict(link_sync["metadata"].get("effective_execution_semantics") or {}),
        )
        self.assertEqual(
            dict(link_async["metadata"].get("dispatch_preview") or {}),
            dict(link_sync["metadata"].get("dispatch_preview") or {}),
        )

    def test_run_plan_hydration_coalesces_same_request_signature_across_histories(self) -> None:
        queued_at = "2026-04-24T01:23:45+00:00"
        base_payload = {"raw_user_request": "我想要OpenAI做Reasoning方向的人"}
        history_payloads = {
            "history-plan-hydration-dedupe-1": {**base_payload, "history_id": "history-plan-hydration-dedupe-1"},
            "history-plan-hydration-dedupe-2": {**base_payload, "history_id": "history-plan-hydration-dedupe-2"},
        }
        request_ids = {
            "history-plan-hydration-dedupe-1": "req-plan-hydration-dedupe-1",
            "history-plan-hydration-dedupe-2": "req-plan-hydration-dedupe-2",
        }
        request_signature = _plan_hydration_request_signature(next(iter(history_payloads.values())))
        histories: dict[str, dict[str, object]] = {}
        for history_id, payload in history_payloads.items():
            request_id = request_ids[history_id]
            self.store.upsert_frontend_history_link(
                {
                    "history_id": history_id,
                    "query_text": str(payload["raw_user_request"]),
                    "target_company": "",
                    "phase": "plan",
                    "request": payload,
                    "metadata": {
                        "source": "plan_workflow_submit",
                        "plan_generation": {
                            "status": "queued",
                            "request_id": request_id,
                            "queued_at": queued_at,
                            "submitted_at": queued_at,
                        },
                    },
                }
            )
            record = {
                "request_id": request_id,
                "queued_at": queued_at,
                "payload": payload,
                "request_signature": request_signature,
            }
            histories[history_id] = record
            self.orchestrator._plan_hydration_inflight[history_id] = dict(record)
        self.orchestrator._plan_hydration_signature_inflight[request_signature] = {
            "request_signature": request_signature,
            "primary_history_id": "history-plan-hydration-dedupe-1",
            "primary_request_id": "req-plan-hydration-dedupe-1",
            "queued_at": queued_at,
            "payload": history_payloads["history-plan-hydration-dedupe-1"],
            "histories": histories,
        }

        fake_result = {
            "status": "needs_plan_review",
            "request": {
                "raw_user_request": "我想要OpenAI做Reasoning方向的人",
                "target_company": "OpenAI",
            },
            "plan": {"target_company": "OpenAI", "acquisition_tasks": [{"task_id": "reasoning"}]},
            "plan_review_session": {"review_id": 4321},
            "dispatch_preview": {"strategy": "reuse_snapshot"},
            "effective_execution_semantics": {"execution_strategy_label": "全量本地资产复用"},
        }
        with mock.patch.object(self.orchestrator, "plan_workflow", return_value=fake_result) as plan_mock:
            self.orchestrator._run_plan_hydration(
                history_id="history-plan-hydration-dedupe-1",
                payload=history_payloads["history-plan-hydration-dedupe-1"],
                plan_request_id="req-plan-hydration-dedupe-1",
                queued_at=queued_at,
                request_signature=request_signature,
            )

        plan_mock.assert_called_once()
        self.assertFalse(self.orchestrator._plan_hydration_inflight)
        self.assertFalse(self.orchestrator._plan_hydration_signature_inflight)
        for history_id in history_payloads:
            link = self.store.get_frontend_history_link(history_id)
            self.assertIsNotNone(link)
            assert link is not None
            self.assertEqual(link["phase"], "plan")
            self.assertTrue(link["plan"])
            self.assertEqual(int(link["review_id"] or 0), 4321)
            plan_generation = dict(link["metadata"].get("plan_generation") or {})
            self.assertEqual(str(plan_generation.get("status") or ""), "completed")
            self.assertEqual(str(plan_generation.get("request_signature") or ""), request_signature)
            self.assertEqual(int(plan_generation.get("coalesced_count") or 0), 2)
            self.assertEqual(str(link["request"].get("history_id") or ""), history_id)
            self.assertEqual(
                dict(link["metadata"].get("dispatch_preview") or {}).get("strategy"),
                "reuse_snapshot",
            )
            self.assertEqual(
                dict(link["metadata"].get("effective_execution_semantics") or {}).get("execution_strategy_label"),
                "全量本地资产复用",
            )

    def test_start_workflow_persists_job_link_for_history(self) -> None:
        history_id = "history-workflow-1"
        self._write_company_identity_snapshot(target_company="Skild AI", snapshot_id="20260415T020102")
        planned = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "帮我找Skild AI做Pre-train方向的人",
                "history_id": history_id,
            }
        )
        review_id = int(planned["plan_review_session"]["review_id"])
        self.orchestrator.review_plan_session(
            {
                "review_id": review_id,
                "action": "approved",
                "reviewer": "test",
            }
        )

        with (
            mock.patch.object(self.orchestrator, "ensure_shared_recovery", return_value={"status": "stubbed"}),
            mock.patch.object(self.orchestrator, "ensure_job_scoped_recovery", return_value={"status": "stubbed"}),
            mock.patch.object(self.orchestrator, "_start_hosted_workflow_thread", return_value={"status": "stubbed"}),
        ):
            queued = self.orchestrator.start_workflow(
                {
                    "plan_review_id": review_id,
                    "history_id": history_id,
                }
            )

        self.assertEqual(queued["status"], "queued")
        link = self.store.get_frontend_history_link(history_id)
        self.assertIsNotNone(link)
        assert link is not None
        self.assertEqual(link["job_id"], str(queued["job_id"]))
        self.assertEqual(link["phase"], "running")

    def test_start_excel_intake_workflow_persists_job_link_for_history(self) -> None:
        history_id = "history-excel-workflow-1"
        prepared_contacts = {
            "workbook": {
                "source_path": str(Path(self.tempdir.name) / "contacts.xlsx"),
                "sheet_count": 1,
                "sheet_names": ["Contacts"],
                "detected_contact_row_count": 1,
            },
            "schema_inference": {},
            "contacts": [
                {
                    "row_key": "Contacts#1",
                    "sheet_name": "Contacts",
                    "row_index": 1,
                    "name": "Ada Import",
                    "company": "OpenAI",
                    "title": "Researcher",
                    "linkedin_url": "",
                    "source_path": "contacts.xlsx#Contacts:1",
                    "raw_row": {},
                }
            ],
        }
        with (
            mock.patch("sourcing_agent.orchestrator.ExcelIntakeService.prepare_contacts", return_value=prepared_contacts),
            mock.patch.object(ExcelIntakeOwner, "_run_excel_intake_workflow_command_thread", autospec=True, return_value=None),
        ):
            queued = self.orchestrator.start_excel_intake_workflow(
                {
                    "target_company": "OpenAI",
                    "history_id": history_id,
                    "query_text": "Excel 批量导入 OpenAI 候选人",
                    "filename": "contacts.xlsx",
                    "file_content_base64": "ZmFrZQ==",
                }
            )

        self.assertEqual(queued["status"], "queued")
        job = self.store.get_job(str(queued["job_id"]))
        self.assertIsNotNone(job)
        assert job is not None
        self.assertEqual(job["job_type"], "excel_intake")
        self.assertEqual(job["status"], "queued")
        excel_bundle = dict(dict(job.get("execution_bundle") or {}).get("excel_intake") or {})
        prepared_batch_path = Path(str(excel_bundle.get("prepared_contact_batch_path") or ""))
        self.assertTrue(prepared_batch_path.is_file())
        persisted_prepared = json.loads(prepared_batch_path.read_text(encoding="utf-8"))
        self.assertEqual(
            persisted_prepared["prepared_contact_batch"]["contacts"][0]["name"],
            "Ada Import",
        )
        self.assertEqual(int(excel_bundle.get("prepared_contact_count") or 0), 1)
        link = self.store.get_frontend_history_link(history_id)
        self.assertIsNotNone(link)
        assert link is not None
        self.assertEqual(link["job_id"], str(queued["job_id"]))
        self.assertEqual(link["phase"], "running")
        self.assertEqual(str(link["metadata"].get("workflow_kind") or ""), "excel_intake")
        self.assertFalse(link["plan"])
        self.assertEqual(queued["workflow_command"]["command_type"], EXCEL_INTAKE_RUN_COMMAND_TYPE)
        self.assertEqual(queued["workflow_command"]["owner"], EXCEL_INTAKE_RUN_OWNER)
        self.assertEqual(queued["workflow_command"]["status"], "running")
        command = self.store.get_workflow_command(str(queued["workflow_command"]["command_id"] or "")) or {}
        self.assertEqual(command["payload"]["job_id"], str(queued["job_id"]))
        self.assertEqual(command["produced_entity_counts"]["excel_intake_job"], 1)
        self.assertEqual(command["produced_entity_counts"]["excel_contact"], 1)

    def test_start_excel_intake_workflow_splits_company_groups_into_multiple_histories(self) -> None:
        prepared_contacts = {
            "workbook": {
                "source_path": str(Path(self.tempdir.name) / "contacts.xlsx"),
                "sheet_count": 1,
                "sheet_names": ["Contacts"],
                "detected_contact_row_count": 3,
            },
            "schema_inference": {},
            "contacts": [
                {
                    "row_key": "Contacts#1",
                    "sheet_name": "Contacts",
                    "row_index": 1,
                    "name": "Ada Import",
                    "company": "OpenAI",
                    "title": "Researcher",
                    "linkedin_url": "",
                    "source_path": "contacts.xlsx#Contacts:1",
                    "raw_row": {},
                },
                {
                    "row_key": "Contacts#2",
                    "sheet_name": "Contacts",
                    "row_index": 2,
                    "name": "Barry Dong",
                    "company": "OpenAI & Meta",
                    "title": "Researcher",
                    "linkedin_url": "",
                    "source_path": "contacts.xlsx#Contacts:2",
                    "raw_row": {},
                },
                {
                    "row_key": "Contacts#3",
                    "sheet_name": "Contacts",
                    "row_index": 3,
                    "name": "Chris Example",
                    "company": "Anthropic",
                    "title": "Engineer",
                    "linkedin_url": "",
                    "source_path": "contacts.xlsx#Contacts:3",
                    "raw_row": {},
                },
            ],
        }
        with (
            mock.patch("sourcing_agent.orchestrator.ExcelIntakeService.prepare_contacts", return_value=prepared_contacts),
            mock.patch.object(ExcelIntakeOwner, "_run_excel_intake_workflow_command_thread", autospec=True, return_value=None),
        ):
            queued = self.orchestrator.start_excel_intake_workflow(
                {
                    "filename": "contacts.xlsx",
                    "file_content_base64": "ZmFrZQ==",
                }
            )

        self.assertEqual(queued["status"], "queued")
        self.assertEqual(queued["workflow_kind"], "excel_intake_batch")
        self.assertEqual(queued["created_job_count"], 3)
        groups = {item["target_company"]: item for item in queued["groups"]}
        self.assertEqual(set(groups.keys()), {"Anthropic", "Meta", "OpenAI"})
        self.assertEqual(groups["OpenAI"]["row_count"], 2)
        self.assertEqual(groups["Meta"]["row_count"], 1)
        self.assertEqual(groups["Anthropic"]["row_count"], 1)
        for group in queued["groups"]:
            link = self.store.get_frontend_history_link(str(group["history_id"]))
            self.assertIsNotNone(link)
            assert link is not None
            self.assertEqual(str(link["metadata"].get("workflow_kind") or ""), "excel_intake")
            self.assertEqual(str(link["metadata"].get("batch_id") or ""), str(queued["batch_id"]))
            self.assertEqual(link["phase"], "running")
            self.assertEqual(group["workflow_command"]["command_type"], EXCEL_INTAKE_RUN_COMMAND_TYPE)
            self.assertEqual(group["workflow_command"]["owner"], EXCEL_INTAKE_RUN_OWNER)
            self.assertEqual(group["workflow_command"]["status"], "running")
        commands = [
            command
            for command in self.store.list_workflow_commands(
                owner=EXCEL_INTAKE_RUN_OWNER,
                statuses=["running"],
                limit=20,
            )
            if command["command_type"] == EXCEL_INTAKE_RUN_COMMAND_TYPE
        ]
        self.assertEqual(len(commands), 3)

    def test_run_excel_intake_workflow_persists_result_view_and_results_history(self) -> None:
        history_id = "history-excel-workflow-results-1"
        snapshot_id = "20260423T130000"
        target_company = "OpenAI"
        self._write_company_identity_snapshot(target_company=target_company, snapshot_id=snapshot_id)
        company_key = "openai"
        snapshot_dir = self.settings.company_assets_dir / company_key / snapshot_id
        candidate = Candidate(
            candidate_id="openai-import-1",
            name_en="Ada Import",
            display_name="Ada Import",
            category="employee",
            target_company=target_company,
            organization=target_company,
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/ada-import/",
            focus_areas="Reasoning, Coding",
            education="MIT",
            work_history="OpenAI",
        )
        baseline_candidate = Candidate(
            candidate_id="openai-baseline-1",
            name_en="Grace Baseline",
            display_name="Grace Baseline",
            category="employee",
            target_company=target_company,
            organization=target_company,
            employment_status="current",
            role="Research Scientist",
            linkedin_url="https://www.linkedin.com/in/grace-baseline/",
            focus_areas="Agent",
            education="Stanford",
            work_history="OpenAI",
        )
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": snapshot_id,
                        "target_company": target_company,
                        "company_identity": {
                            "requested_name": target_company,
                            "canonical_name": target_company,
                            "company_key": company_key,
                            "linkedin_slug": company_key,
                        },
                    },
                    "candidates": [candidate.to_record(), baseline_candidate.to_record()],
                    "evidence": [],
                    "candidate_count": 2,
                    "evidence_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        request = JobRequest.from_payload(
            {
                "raw_user_request": "Excel 批量导入 OpenAI 候选人",
                "query": "Excel 批量导入 OpenAI 候选人",
                "target_company": target_company,
                "target_scope": "full_company_asset",
                "asset_view": "canonical_merged",
                "retrieval_strategy": "asset_population",
                "planning_mode": "excel_intake",
            }
        )
        self.store.upsert_frontend_history_link(
            {
                "history_id": history_id,
                "query_text": "Excel 批量导入 OpenAI 候选人",
                "target_company": target_company,
                "job_id": "excelworkflowresults1",
                "phase": "running",
                "request": request.to_record(),
                "metadata": {"workflow_kind": "excel_intake"},
            }
        )
        mocked_result = {
            "status": "completed",
            "intake_id": "excel-1",
            "workbook": {
                "source_path": str(snapshot_dir / "contacts.xlsx"),
                "sheet_count": 1,
                "sheet_names": ["Contacts"],
                "detected_contact_row_count": 2,
            },
            "summary": {
                "total_rows": 2,
                "persisted_candidate_count": 1,
                "persisted_evidence_count": 0,
                "local_exact_hit_count": 0,
                "manual_review_local_count": 1,
                "fetched_direct_linkedin_count": 1,
                "fetched_via_search_count": 0,
                "manual_review_search_count": 0,
                "unresolved_count": 0,
            },
            "attachment_summary": {
                "status": "completed",
                "snapshot_id": snapshot_id,
                "candidate_count": 2,
                "evidence_count": 0,
                "candidate_doc_path": str(snapshot_dir / "candidate_documents.json"),
                "stage_candidate_doc_path": str(snapshot_dir / "candidate_documents.excel_intake.json"),
                "artifact_result": {
                    "status": "completed",
                    "generation_key": "gen-1",
                    "generation_sequence": 1,
                },
            },
            "artifact_paths": {},
            "results": [
                {
                    "status": "fetched_direct_linkedin",
                    "matched_candidate": candidate.to_record(),
                    "row_key": "Contacts#1",
                },
                {
                    "status": "manual_review_local",
                    "row_key": "Contacts#2",
                    "name": "Needs Review",
                    "company": target_company,
                    "title": "Research",
                    "linkedin_url": "https://www.linkedin.com/in/needs-review/",
                    "manual_review_candidates": [baseline_candidate.to_record()],
                    "match_reason": "local_near_match_candidates",
                },
            ],
        }

        with mock.patch("sourcing_agent.orchestrator.ExcelIntakeService.ingest_contacts", return_value=mocked_result):
            self.orchestrator._run_excel_intake_workflow(
                job_id="excelworkflowresults1",
                request=request,
                payload={
                    "target_company": target_company,
                    "filename": "contacts.xlsx",
                    "attach_to_snapshot": True,
                    "build_artifacts": True,
                },
            )

        job = self.store.get_job("excelworkflowresults1")
        self.assertIsNotNone(job)
        assert job is not None
        self.assertEqual(job["status"], "completed")
        self.assertEqual(job["job_type"], "excel_intake")
        self.assertEqual(str(job["summary"].get("workflow_kind") or ""), "excel_intake")
        result_view = self.store.get_job_result_view(job_id="excelworkflowresults1")
        self.assertIsNotNone(result_view)
        assert result_view is not None
        self.assertEqual(result_view["source_kind"], "company_snapshot")
        self.assertEqual(result_view["snapshot_id"], snapshot_id)
        markers = list(dict(result_view.get("metadata") or {}).get("job_scoped_candidate_markers") or [])
        self.assertEqual(len(markers), 1)
        self.assertEqual(markers[0]["marker_id"], "excel_intake:current_job")
        self.assertEqual(markers[0]["label"], "本次Excel导入")
        self.assertEqual(markers[0]["candidate_ids"], ["openai-import-1"])
        row_manifest_ref = dict(job["summary"].get("excel_row_manifest") or {})
        self.assertEqual(row_manifest_ref.get("review_row_count"), 1)
        row_manifest_path = Path(str(row_manifest_ref.get("path") or ""))
        self.assertTrue(row_manifest_path.exists())
        row_manifest = json.loads(row_manifest_path.read_text(encoding="utf-8"))
        self.assertEqual(row_manifest["review_rows"][0]["row_key"], "Contacts#2")
        self.assertEqual(
            row_manifest["review_rows"][0]["manual_review_candidates"][0]["candidate_id"],
            "openai-baseline-1",
        )
        progress = self.orchestrator.get_job_progress("excelworkflowresults1")
        self.assertIsNotNone(progress)
        assert progress is not None
        excel_progress = dict(progress.get("excel_intake_progress") or {})
        self.assertEqual(excel_progress.get("total_row_count"), 2)
        self.assertEqual(excel_progress.get("matched_row_count"), 1)
        self.assertEqual(excel_progress.get("manual_review_row_count"), 1)
        self.assertEqual(excel_progress.get("unresolved_row_count"), 0)
        self.assertEqual(excel_progress.get("review_row_count"), 1)
        execution_phase = dict(progress.get("execution_phase_contract") or {})
        self.assertEqual(execution_phase.get("active_phase_label"), "Excel Intake")
        self.assertIn("已匹配 1 行", str(execution_phase.get("active_phase_detail") or ""))
        self.assertIn("需人工审核 1 行", str(execution_phase.get("active_phase_detail") or ""))
        dashboard = self.orchestrator.get_job_dashboard("excelworkflowresults1")
        self.assertIsNotNone(dashboard)
        assert dashboard is not None
        self.assertEqual(int(dashboard["asset_population"].get("candidate_count") or 0), 2)
        candidate_payloads = {
            str(item.get("candidate_id") or ""): dict(item)
            for item in list(dashboard["asset_population"].get("candidates") or [])
            if isinstance(item, dict)
        }
        self.assertIn("本次Excel导入", list(candidate_payloads["openai-import-1"].get("matched_keywords") or []))
        self.assertNotIn("本次Excel导入", list(candidate_payloads["openai-baseline-1"].get("matched_keywords") or []))
        link = self.store.get_frontend_history_link(history_id)
        self.assertIsNotNone(link)
        assert link is not None
        self.assertEqual(link["phase"], "results")

    def test_stale_excel_intake_job_recovers_from_persisted_prepared_batch(self) -> None:
        history_id = "history-excel-recovery-prepared-batch-1"
        snapshot_id = "20260423T132500"
        target_company = "OpenAI"
        self._write_company_identity_snapshot(target_company=target_company, snapshot_id=snapshot_id)
        company_key = "openai"
        snapshot_dir = self.settings.company_assets_dir / company_key / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        candidate = Candidate(
            candidate_id="openai-recovered-excel-1",
            name_en="Ada Recovered",
            display_name="Ada Recovered",
            category="employee",
            target_company=target_company,
            organization=target_company,
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/ada-recovered/",
            focus_areas="Agents",
            education="MIT",
            work_history="OpenAI",
        )
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": snapshot_id,
                        "target_company": target_company,
                        "company_identity": {
                            "requested_name": target_company,
                            "canonical_name": target_company,
                            "company_key": company_key,
                            "linkedin_slug": company_key,
                        },
                    },
                    "candidates": [candidate.to_record()],
                    "evidence": [],
                    "candidate_count": 1,
                    "evidence_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        prepared_contacts = {
            "workbook": {
                "source_path": str(snapshot_dir / "contacts.xlsx"),
                "sheet_count": 1,
                "sheet_names": ["Contacts"],
                "detected_contact_row_count": 1,
            },
            "schema_inference": {},
            "contacts": [
                {
                    "row_key": "Contacts#1",
                    "sheet_name": "Contacts",
                    "row_index": 1,
                    "name": "Ada Recovered",
                    "company": target_company,
                    "title": "Research Engineer",
                    "linkedin_url": "https://www.linkedin.com/in/ada-recovered/",
                    "source_path": "contacts.xlsx#Contacts:1",
                    "raw_row": {},
                }
            ],
        }
        with (
            mock.patch("sourcing_agent.orchestrator.ExcelIntakeService.prepare_contacts", return_value=prepared_contacts),
            mock.patch.object(ExcelIntakeOwner, "_run_excel_intake_workflow_command_thread", autospec=True, return_value=None),
        ):
            queued = self.orchestrator.start_excel_intake_workflow(
                {
                    "target_company": target_company,
                    "history_id": history_id,
                    "query_text": "Excel 批量导入 OpenAI 候选人",
                    "filename": "contacts.xlsx",
                    "file_content_base64": "ZmFrZQ==",
                }
            )

        job_id = str(queued["job_id"])
        initial_command_id = str(dict(queued.get("workflow_command") or {}).get("command_id") or "")
        self.assertTrue(initial_command_id)
        self.store.mark_workflow_command_partial_progress(
            initial_command_id,
            result={
                "status": "test_requeue_after_mocked_thread_start",
                "recovery_source": "test_stale_excel_intake_job_recovers_from_persisted_prepared_batch",
            },
        )
        captured_payload: dict[str, object] = {}
        mocked_result = {
            "status": "completed",
            "intake_id": "excel-recovered-1",
            "workbook": dict(prepared_contacts["workbook"]),
            "summary": {
                "total_rows": 1,
                "persisted_candidate_count": 1,
                "persisted_evidence_count": 0,
                "local_exact_hit_count": 0,
                "manual_review_local_count": 0,
                "fetched_direct_linkedin_count": 1,
                "fetched_via_search_count": 0,
                "manual_review_search_count": 0,
                "unresolved_count": 0,
            },
            "attachment_summary": {
                "status": "completed",
                "snapshot_id": snapshot_id,
                "candidate_count": 1,
                "evidence_count": 0,
                "candidate_doc_path": str(snapshot_dir / "candidate_documents.json"),
                "stage_candidate_doc_path": str(snapshot_dir / "candidate_documents.excel_intake.json"),
            },
            "artifact_paths": {},
            "results": [
                {
                    "status": "fetched_direct_linkedin",
                    "matched_candidate": candidate.to_record(),
                    "row_key": "Contacts#1",
                }
            ],
        }

        def _capture_ingest(payload: dict[str, object]) -> dict[str, object]:
            captured_payload.update(payload)
            return mocked_result

        with (
            mock.patch(
                "sourcing_agent.orchestrator.ExcelIntakeService.prepare_contacts",
                side_effect=AssertionError("recovery must use persisted prepared batch"),
            ),
            mock.patch("sourcing_agent.orchestrator.ExcelIntakeService.ingest_contacts", side_effect=_capture_ingest),
        ):
            recovery = self.orchestrator.run_worker_recovery_once(
                {
                    "job_id": job_id,
                    "excel_intake_recovery_enabled": True,
                    "excel_intake_stale_after_seconds": 0,
                    "post_recovery_housekeeping_enabled": False,
                }
            )

        excel_recovery = dict(recovery.get("excel_intake_recovery") or {})
        self.assertEqual(excel_recovery["recovered_count"], 1)
        self.assertEqual(excel_recovery["results"][0]["reason"], "recovered_via_excel_intake_run_command")
        owner_result = dict(excel_recovery["results"][0]["owner_result"])
        self.assertEqual(owner_result["started_count"], 1)
        self.assertEqual(
            dict(captured_payload.get("prepared_contact_batch") or {})["contacts"][0]["name"],
            "Ada Recovered",
        )
        recovered_job = self.store.get_job(job_id)
        deadline = time.monotonic() + 5.0
        while recovered_job and recovered_job.get("status") != "completed" and time.monotonic() < deadline:
            time.sleep(0.05)
            recovered_job = self.store.get_job(job_id)
        self.assertIsNotNone(recovered_job)
        assert recovered_job is not None
        self.assertEqual(recovered_job["status"], "completed")
        result_view = self.store.get_job_result_view(job_id=job_id)
        self.assertIsNotNone(result_view)
        link = self.store.get_frontend_history_link(history_id)
        while link and link.get("phase") != "results" and time.monotonic() < deadline:
            time.sleep(0.05)
            link = self.store.get_frontend_history_link(history_id)
        self.assertIsNotNone(link)
        assert link is not None
        self.assertEqual(link["phase"], "results")

    def test_run_excel_intake_workflow_defers_full_artifact_build_until_after_result_view(self) -> None:
        history_id = "history-excel-workflow-deferred-artifacts-1"
        snapshot_id = "20260423T131500"
        target_company = "OpenAI"
        company_key = "openai"
        self._write_company_identity_snapshot(target_company=target_company, snapshot_id=snapshot_id)
        snapshot_dir = self.settings.company_assets_dir / company_key / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": snapshot_id,
                        "target_company": target_company,
                        "company_identity": {
                            "requested_name": target_company,
                            "canonical_name": target_company,
                            "company_key": company_key,
                            "linkedin_slug": company_key,
                        },
                    },
                    "candidates": [],
                    "evidence": [],
                    "candidate_count": 0,
                    "evidence_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        candidate = Candidate(
            candidate_id="openai-import-deferred-1",
            name_en="Ada Import",
            display_name="Ada Import",
            category="employee",
            target_company=target_company,
            organization=target_company,
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/ada-import-deferred/",
            focus_areas="Reasoning, Coding",
            education="MIT",
            work_history="OpenAI",
        )
        self.store.upsert_candidate(candidate)
        request = JobRequest.from_payload(
            {
                "raw_user_request": "Excel 批量导入 OpenAI 候选人",
                "query": "Excel 批量导入 OpenAI 候选人",
                "target_company": target_company,
                "target_scope": "full_company_asset",
                "asset_view": "canonical_merged",
                "retrieval_strategy": "asset_population",
                "planning_mode": "excel_intake",
            }
        )
        self.store.upsert_frontend_history_link(
            {
                "history_id": history_id,
                "query_text": "Excel 批量导入 OpenAI 候选人",
                "target_company": target_company,
                "job_id": "excelworkflowdeferred1",
                "phase": "running",
                "request": request.to_record(),
                "metadata": {"workflow_kind": "excel_intake"},
            }
        )

        with (
            mock.patch("sourcing_agent.company_asset_supplement.build_company_candidate_artifacts") as supplement_build,
            mock.patch("sourcing_agent.candidate_artifacts.build_company_candidate_artifacts") as artifact_build,
        ):
            supplement_build.side_effect = AssertionError("Excel workflow must not inline full artifact build")
            artifact_build.side_effect = AssertionError("Excel result view must not materialize on first dashboard read")
            self.orchestrator._run_excel_intake_workflow(
                job_id="excelworkflowdeferred1",
                request=request,
                payload={
                    "target_company": target_company,
                    "filename": "contacts.xlsx",
                    "attach_to_snapshot": True,
                    "build_artifacts": True,
                    "prepared_contact_batch": {
                        "workbook": {
                            "source_path": str(snapshot_dir / "contacts.xlsx"),
                            "sheet_count": 1,
                            "sheet_names": ["Contacts"],
                            "detected_contact_row_count": 1,
                        },
                        "schema_inference": {},
                        "contacts": [
                            {
                                "row_key": "Contacts#1",
                                "name": "Ada Import",
                                "company": "OpenAI",
                                "title": "Research Engineer",
                                "linkedin_url": "https://www.linkedin.com/in/ada-import-deferred/",
                            }
                        ],
                    },
                },
            )
            dashboard = self.orchestrator.get_job_dashboard("excelworkflowdeferred1")
            progress = self.orchestrator.get_job_progress("excelworkflowdeferred1")

        job = self.store.get_job("excelworkflowdeferred1")
        self.assertIsNotNone(job)
        assert job is not None
        self.assertEqual(job["status"], "completed")
        self.assertTrue(bool(dict(job["summary"].get("public_web_stage_2") or {}).get("artifact_build_deferred")))
        result_view = self.store.get_job_result_view(job_id="excelworkflowdeferred1")
        self.assertIsNotNone(result_view)
        assert result_view is not None
        self.assertTrue(str(dict(result_view.get("metadata") or {}).get("asset_population_overlay_path") or ""))
        self.assertIsNotNone(dashboard)
        assert dashboard is not None
        self.assertEqual(int(dashboard["asset_population"].get("candidate_count") or 0), 1)
        self.assertEqual(dashboard["asset_population"]["candidates"][0]["display_name"], "Ada Import")
        dashboard_contract = dict(dashboard.get("execution_phase_contract") or {})
        self.assertFalse(bool(dashboard_contract.get("public_web_stage_applicable")))
        self.assertEqual(
            dict(dashboard_contract.get("stage_title_overrides") or {}).get("public_web_stage_2"),
            "公司资产归档",
        )
        self.assertIsNotNone(progress)
        assert progress is not None
        progress_contract = dict(progress.get("execution_phase_contract") or {})
        self.assertFalse(bool(progress_contract.get("public_web_stage_applicable")))
        self.assertEqual(
            dict(progress_contract.get("stage_title_overrides") or {}).get("public_web_stage_2"),
            "公司资产归档",
        )
        link = self.store.get_frontend_history_link(history_id)
        self.assertIsNotNone(link)
        assert link is not None
        self.assertEqual(link["phase"], "results")

    def test_repair_excel_intake_artifacts_uses_durable_snapshot_materialization_item(self) -> None:
        job_id = "excelworkflowartifactrepair1"
        snapshot_id = "20260423T141500"
        target_company = "OpenAI"
        company_key = "openai"
        self._write_company_identity_snapshot(target_company=target_company, snapshot_id=snapshot_id)
        snapshot_dir = self.settings.company_assets_dir / company_key / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": snapshot_id,
                        "target_company": target_company,
                        "company_identity": {
                            "requested_name": target_company,
                            "canonical_name": target_company,
                            "company_key": company_key,
                            "linkedin_slug": company_key,
                        },
                    },
                    "candidates": [],
                    "evidence": [],
                    "candidate_count": 0,
                    "evidence_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        request = JobRequest.from_payload(
            {
                "raw_user_request": "Excel 批量导入 OpenAI 候选人",
                "query": "Excel 批量导入 OpenAI 候选人",
                "target_company": target_company,
                "planning_mode": "excel_intake",
            }
        )
        self.store.save_job(
            job_id=job_id,
            job_type="excel_intake",
            status="completed",
            stage="completed",
            request_payload=request.to_record(),
            plan_payload={},
            summary_payload={
                "workflow_kind": "excel_intake",
                "public_web_stage_2": {
                    "status": "completed",
                    "title": "公司资产归档",
                    "workflow_kind": "excel_intake",
                    "snapshot_id": snapshot_id,
                    "candidate_doc_path": str(candidate_doc_path),
                    "artifact_build_deferred": True,
                    "artifact_build_deferred_reason": "excel_workflow_serves_result_view_overlay",
                },
            },
        )

        dry_run = self.orchestrator.repair_excel_intake_artifacts({"job_id": job_id})

        self.assertEqual(dry_run["status"], "dry_run")
        self.assertTrue(dry_run["would_enqueue"])
        self.assertEqual(
            self.store.list_job_materialization_items(
                job_id=job_id,
                item_kind="snapshot_full_materialization",
            ),
            [],
        )

        with mock.patch(
            "sourcing_agent.orchestrator.build_company_candidate_artifacts",
            return_value={
                "status": "completed",
                "artifact_dir": str(snapshot_dir / "normalized_artifacts"),
                "artifact_paths": {
                    "materialized_candidate_documents": str(
                        snapshot_dir / "normalized_artifacts" / "materialized_candidate_documents.json"
                    )
                },
            },
        ) as artifact_build:
            repair = self.orchestrator.repair_excel_intake_artifacts(
                {"job_id": job_id, "dry_run": False, "run_now": True}
            )

        self.assertEqual(repair["status"], "completed")
        artifact_build.assert_called_once_with(
            runtime_dir=self.settings.runtime_dir,
            store=self.store,
            target_company=target_company,
            snapshot_id=snapshot_id,
        )
        # W6 cutover: snapshot materialization is command-owned; the durable
        # snapshot.compaction.run command is the completion evidence, and the
        # legacy job_materialization_items adapter must stay disabled (no rows).
        compaction_commands = self.store.list_workflow_commands(
            workflow_run_id=legacy_job_workflow_run_id(job_id),
            owner=SNAPSHOT_COMPACTION_RUN_OWNER,
        )
        succeeded_commands = [
            command for command in compaction_commands if str(command.get("status") or "") == "succeeded"
        ]
        self.assertEqual(len(succeeded_commands), 1)
        self.assertEqual(
            self.store.list_job_materialization_items(
                job_id=job_id,
                item_kind="snapshot_full_materialization",
                statuses=["completed"],
            ),
            [],
        )
        refreshed_job = self.store.get_job(job_id)
        self.assertIsNotNone(refreshed_job)
        assert refreshed_job is not None
        refreshed_stage = dict(dict(refreshed_job["summary"]).get("public_web_stage_2") or {})
        self.assertFalse(bool(refreshed_stage.get("artifact_build_deferred")))
        self.assertEqual(refreshed_stage["artifact_build_status"], "completed")
        self.assertEqual(
            dict(refreshed_job["summary"]).get("excel_artifact_materialization", {}).get("status"),
            "completed",
        )
        event_phases = [
            dict(event.get("payload") or {}).get("phase")
            for event in self.store.list_job_events(job_id)
            if dict(event.get("payload") or {}).get("event_family") == "excel_artifact_materialization"
        ]
        self.assertEqual(event_phases, ["started", "completed"])

    def test_start_workflow_recovers_history_id_from_plan_review_session(self) -> None:
        history_id = "history-workflow-review-lookup-1"
        self._write_company_identity_snapshot(target_company="Skild AI", snapshot_id="20260415T020103")
        planned = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "帮我找Skild AI做Pre-train方向的人",
                "history_id": history_id,
            }
        )
        review_id = int(planned["plan_review_session"]["review_id"])
        self.orchestrator.review_plan_session(
            {
                "review_id": review_id,
                "action": "approved",
                "reviewer": "test",
            }
        )

        with (
            mock.patch.object(self.orchestrator, "ensure_shared_recovery", return_value={"status": "stubbed"}),
            mock.patch.object(self.orchestrator, "ensure_job_scoped_recovery", return_value={"status": "stubbed"}),
            mock.patch.object(self.orchestrator, "_start_hosted_workflow_thread", return_value={"status": "stubbed"}),
        ):
            queued = self.orchestrator.start_workflow(
                {
                    "plan_review_id": review_id,
                }
            )

        self.assertEqual(queued["status"], "queued")
        link = self.store.get_frontend_history_link(history_id)
        self.assertIsNotNone(link)
        assert link is not None
        self.assertEqual(link["review_id"], review_id)
        self.assertEqual(link["job_id"], str(queued["job_id"]))
        self.assertEqual(link["phase"], "running")

    def test_progress_reconciliation_marks_frontend_history_results(self) -> None:
        history_id = "history-workflow-results-1"
        job_id = "workflow_results_history_sync"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="retrieving",
            request_payload={
                "raw_user_request": "我想要OpenAI做Reasoning方向的人",
                "query": "OpenAI Reasoning direction",
                "target_company": "OpenAI",
            },
            plan_payload={},
            summary_payload={"message": "Preparing final results"},
        )
        self.store.upsert_frontend_history_link(
            {
                "history_id": history_id,
                "query_text": "我想要OpenAI做Reasoning方向的人",
                "target_company": "OpenAI",
                "job_id": job_id,
                "phase": "running",
            }
        )
        self.store.replace_job_results(
            job_id,
            [
                {
                    "candidate_id": "cand_openai_reasoning_1",
                    "rank": 1,
                    "score": 0.91,
                    "semantic_score": 0.0,
                    "confidence_label": "high",
                    "confidence_score": 0.91,
                    "confidence_reason": "reconciled",
                    "explanation": "Recovered from completed stage 2 final results.",
                    "matched_fields": ["work_history"],
                }
            ],
        )
        with mock.patch.object(
            self.orchestrator,
            "_load_workflow_stage_summaries",
            return_value={"summaries": {"stage_2_final": {"status": "completed"}}},
        ):
            progress = self.orchestrator.get_job_progress(job_id)

        assert progress is not None
        # Owner-approved exemption (2026-06-12): a legacy/recovered job with NO
        # durable workflow run (and no agent runtime session) must still be
        # promotable — it can never produce a serving_finalized proof.
        self.assertEqual(progress["status"], "completed")
        link = self.store.get_frontend_history_link(history_id)
        self.assertIsNotNone(link)
        assert link is not None
        self.assertEqual(link["phase"], "results")

    def test_progress_reconciliation_requires_serving_finalized_proof_for_durable_runs(self) -> None:
        history_id = "history-workflow-results-proof-gate-1"
        job_id = "workflow_results_proof_gate"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="retrieving",
            request_payload={
                "raw_user_request": "我想要OpenAI做Reasoning方向的人",
                "query": "OpenAI Reasoning direction",
                "target_company": "OpenAI",
            },
            plan_payload={},
            summary_payload={"message": "Preparing final results"},
        )
        self.store.upsert_frontend_history_link(
            {
                "history_id": history_id,
                "query_text": "我想要OpenAI做Reasoning方向的人",
                "target_company": "OpenAI",
                "job_id": job_id,
                "phase": "running",
            }
        )
        self.store.replace_job_results(
            job_id,
            [
                {
                    "candidate_id": "cand_openai_reasoning_proof_1",
                    "rank": 1,
                    "score": 0.91,
                    "semantic_score": 0.0,
                    "confidence_label": "high",
                    "confidence_score": 0.91,
                    "confidence_reason": "reconciled",
                    "explanation": "Recovered from completed stage 2 final results.",
                    "matched_fields": ["work_history"],
                }
            ],
        )
        # A durable workflow run EXISTS (any reduced event creates the current
        # state row) but carries no serving_finalized proof: the exemption must
        # not apply and promotion must stay fail-closed.
        self.orchestrator.durable_runtime_writer.append_event_and_reduce(
            workflow_run_id=legacy_job_workflow_run_id(job_id),
            operation_id=legacy_job_operation_id(job_id),
            command_id=f"{job_id}:proof_gate_seed_other",
            event_family="workflow_event",
            event_type="CompletionProofRecorded",
            idempotency_key=f"{job_id}:other_proof:seed",
            actor=PROJECTION_RUN_SCOPE_FINALIZE_OWNER,
            source=PROJECTION_RUN_SCOPE_FINALIZE_COMMAND_TYPE,
            payload={
                "workflow_type": "linkedin_acquisition",
                "stage_key": "run_scope_projection_finalize",
                "proof_key": "other_seed_proof",
                "status": "proved",
                "job_id": job_id,
            },
        )
        stage_summaries = {"summaries": {"stage_2_final": {"status": "completed"}}}

        with mock.patch.object(
            self.orchestrator,
            "_load_workflow_stage_summaries",
            return_value=stage_summaries,
        ):
            blocked_progress = self.orchestrator.get_job_progress(job_id)

        assert blocked_progress is not None
        self.assertEqual(blocked_progress["status"], "running")
        blocked_link = self.store.get_frontend_history_link(history_id)
        assert blocked_link is not None
        self.assertEqual(blocked_link["phase"], "running")

        # Recording the serving_finalized proof unblocks promotion; the agent
        # runtime session row is intentionally absent to lock the PG-only
        # missing-row no-op parity on the promote path.
        self.orchestrator.durable_runtime_writer.append_event_and_reduce(
            workflow_run_id=legacy_job_workflow_run_id(job_id),
            operation_id=legacy_job_operation_id(job_id),
            command_id=f"{job_id}:run_scope_projection_finalize_seed",
            event_family="workflow_event",
            event_type="CompletionProofRecorded",
            idempotency_key=f"{job_id}:serving_finalized:seed",
            actor=PROJECTION_RUN_SCOPE_FINALIZE_OWNER,
            source=PROJECTION_RUN_SCOPE_FINALIZE_COMMAND_TYPE,
            payload={
                "workflow_type": "linkedin_acquisition",
                "stage_key": "run_scope_projection_finalize",
                "proof_key": "serving_finalized",
                "status": "proved",
                "job_id": job_id,
            },
        )
        with mock.patch.object(
            self.orchestrator,
            "_load_workflow_stage_summaries",
            return_value=stage_summaries,
        ):
            promoted_progress = self.orchestrator.get_job_progress(job_id)

        assert promoted_progress is not None
        self.assertEqual(promoted_progress["status"], "completed")
        promoted_link = self.store.get_frontend_history_link(history_id)
        assert promoted_link is not None
        self.assertEqual(promoted_link["phase"], "results")

    def test_frontend_history_recovery_falls_back_to_review_registry(self) -> None:
        history_id = "29fc2046-17c4-4e2b-a7a9-d02ff84d0040"
        job_id = "5d0c75f58ec7"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "帮我找Reflection AI的Post-train方向的人",
                "target_company": "Reflection AI",
            },
            plan_payload={},
            summary_payload={"message": "done"},
        )
        self.store.upsert_candidate_review_record(
            {
                "history_id": history_id,
                "job_id": job_id,
                "candidate_id": "reflection-1",
                "candidate_name": "Candidate One",
                "status": "needs_review",
            }
        )
        self.store.upsert_target_candidate(
            {
                "history_id": history_id,
                "job_id": job_id,
                "candidate_id": "reflection-1",
                "candidate_name": "Candidate One",
            }
        )

        recovered = self.orchestrator.get_frontend_history_recovery(history_id)
        self.assertEqual(recovered["status"], "found")
        self.assertEqual(recovered["recovery"]["job_id"], job_id)
        self.assertEqual(recovered["recovery"]["phase"], "results")
        self.assertEqual(recovered["recovery"]["query_text"], "帮我找Reflection AI的Post-train方向的人")
        self.assertEqual(recovered["recovery"]["source"], "target_candidates")

    def test_frontend_history_recovery_api_exposes_recovered_payload(self) -> None:
        history_id = "history-api-1"
        self.store.upsert_frontend_history_link(
            {
                "history_id": history_id,
                "query_text": "我想要OpenAI做Reasoning方向的人",
                "target_company": "OpenAI",
                "review_id": 0,
                "phase": "plan",
                "request": {
                    "raw_user_request": "我想要OpenAI做Reasoning方向的人",
                    "target_company": "OpenAI",
                },
                "plan": {
                    "target_company": "OpenAI",
                    "keywords": ["Reasoning"],
                },
            }
        )
        server = create_server(self.orchestrator, host="127.0.0.1", port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))
        try:
            req = urllib_request.Request(
                f"http://{host}:{port}/api/frontend-history/{history_id}",
                method="GET",
            )
            with opener.open(req) as response:
                payload = json.loads(response.read().decode("utf-8"))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        self.assertEqual(payload["status"], "found")
        self.assertEqual(payload["recovery"]["history_id"], history_id)
        self.assertEqual(payload["recovery"]["query_text"], "我想要OpenAI做Reasoning方向的人")
        self.assertEqual(payload["recovery"]["phase"], "plan")

    def test_frontend_history_recovery_plan_link_skips_candidate_and_target_fallback_scans(self) -> None:
        history_id = "history-plan-direct-1"
        self.store.upsert_frontend_history_link(
            {
                "history_id": history_id,
                "query_text": "帮我找Reflection AI的Post-train方向的人",
                "target_company": "Reflection AI",
                "review_id": 95,
                "phase": "plan",
                "request": {
                    "raw_user_request": "帮我找Reflection AI的Post-train方向的人",
                    "target_company": "Reflection AI",
                },
                "plan": {
                    "target_company": "Reflection AI",
                    "keywords": ["Post-train"],
                },
            }
        )

        with (
            mock.patch.object(
                self.store,
                "list_candidate_review_records",
                side_effect=AssertionError("candidate fallback should not run for direct plan history"),
            ),
            mock.patch.object(
                self.store,
                "list_target_candidates",
                side_effect=AssertionError("target fallback should not run for direct plan history"),
            ),
        ):
            recovered = self.orchestrator.get_frontend_history_recovery(history_id)

        self.assertEqual(recovered["status"], "found")
        self.assertEqual(recovered["recovery"]["phase"], "plan")
        self.assertEqual(recovered["recovery"]["query_text"], "帮我找Reflection AI的Post-train方向的人")
        self.assertEqual(recovered["recovery"]["plan"]["target_company"], "Reflection AI")

    def test_frontend_history_recovery_exposes_metadata_for_reused_completed_job(self) -> None:
        history_id = "history-reused-completed-1"
        job_id = "job-reused-completed-1"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "我想要OpenAI做Reasoning方向的人",
                "target_company": "OpenAI",
            },
            plan_payload={"target_company": "OpenAI", "keywords": ["Reasoning"]},
            summary_payload={"message": "done"},
        )
        self.store.upsert_frontend_history_link(
            {
                "history_id": history_id,
                "query_text": "我想要OpenAI做Reasoning方向的人",
                "target_company": "OpenAI",
                "job_id": job_id,
                "phase": "results",
                "metadata": {
                    "source": "start_workflow",
                    "workflow_status": "reused_completed_job",
                    "dispatch": {"strategy": "reuse_completed"},
                },
            }
        )

        recovered = self.orchestrator.get_frontend_history_recovery(history_id)

        self.assertEqual(recovered["status"], "found")
        self.assertEqual(recovered["recovery"]["job_id"], job_id)
        self.assertEqual(recovered["recovery"]["metadata"]["workflow_status"], "reused_completed_job")
        self.assertEqual(recovered["recovery"]["metadata"]["dispatch"]["strategy"], "reuse_completed")

    def test_frontend_history_recovery_keeps_completed_job_running_while_background_workers_active(self) -> None:
        history_id = "history-post-completion-workers-1"
        job_id = "job-post-completion-workers-1"
        request_payload = {
            "raw_user_request": "帮我找Lovable的全部成员",
            "target_company": "Lovable",
        }
        plan_payload = {"target_company": "Lovable"}
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload=request_payload,
            plan_payload=plan_payload,
            summary_payload={"message": "baseline ready"},
        )
        self.store.upsert_frontend_history_link(
            {
                "history_id": history_id,
                "query_text": "帮我找Lovable的全部成员",
                "target_company": "Lovable",
                "job_id": job_id,
                "phase": "results",
                "request": request_payload,
                "plan": plan_payload,
            }
        )
        worker = self.store.create_or_resume_agent_worker(
            session_id=1,
            job_id=job_id,
            span_id=1,
            lane_id="enrichment_specialist",
            worker_key="harvest-profile-batch-1",
            metadata={"recovery_kind": "harvest_profile_batch"},
        )
        self.store.mark_agent_worker_running(int(worker["worker_id"]))
        self.store.checkpoint_agent_worker(
            int(worker["worker_id"]),
            checkpoint_payload={"stage": "waiting_remote_harvest"},
            output_payload={"message": "waiting on provider"},
            status="running",
        )

        self.orchestrator._sync_frontend_history_phase_for_job(
            job={
                "job_id": job_id,
                "status": "completed",
                "stage": "completed",
                "request": request_payload,
                "plan": plan_payload,
            },
            phase="results",
        )
        stored_link = self.store.get_frontend_history_link(history_id)
        assert stored_link is not None
        self.assertEqual(stored_link["phase"], "running")

        recovered = self.orchestrator.get_frontend_history_recovery(history_id)

        self.assertEqual(recovered["status"], "found")
        self.assertEqual(recovered["recovery"]["phase"], "running")
        self.assertEqual(recovered["recovery"]["job_id"], job_id)

    def test_plan_api_round_trips_history_id_without_500(self) -> None:
        history_id = "history-plan-api-roundtrip-1"
        self._write_company_identity_snapshot(target_company="OpenAI", snapshot_id="20260422T115901")

        server = create_server(self.orchestrator, host="127.0.0.1", port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))
        try:
            # C1 deleted the synchronous /api/plan HTTP route; plan_workflow is the
            # internal compile function the async submit worker calls. Invoke it
            # directly (equivalent to the old route body) to drive the history_id
            # round-trip; the recovery surface below is still exercised over HTTP.
            plan_payload = self.orchestrator.plan_workflow(
                {
                    "raw_user_request": "我想要OpenAI做Reasoning方向的人",
                    "history_id": history_id,
                }
            )

            recovery_req = urllib_request.Request(
                f"http://{host}:{port}/api/frontend-history/{history_id}",
                method="GET",
            )
            with opener.open(recovery_req) as response:
                recovery_payload = json.loads(response.read().decode("utf-8"))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        self.assertEqual(plan_payload["status"], "needs_plan_review")
        self.assertEqual(plan_payload["request"]["target_company"], "OpenAI")
        self.assertEqual(recovery_payload["status"], "found")
        self.assertEqual(recovery_payload["recovery"]["history_id"], history_id)
        self.assertEqual(recovery_payload["recovery"]["phase"], "plan")
        self.assertEqual(recovery_payload["recovery"]["query_text"], "我想要OpenAI做Reasoning方向的人")
        self.assertNotIn("execution_bundle", recovery_payload["recovery"]["plan_review_session"])

    def test_plan_submit_api_returns_pending_history_without_500(self) -> None:
        history_id = "history-plan-submit-api-roundtrip-1"
        self._write_company_identity_snapshot(target_company="OpenAI", snapshot_id="20260424T115901")

        server = create_server(self.orchestrator, host="127.0.0.1", port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))
        try:
            with mock.patch.object(self.orchestrator, "_queue_plan_hydration", return_value=None):
                plan_req = urllib_request.Request(
                    f"http://{host}:{port}/api/plan/submit",
                    data=json.dumps(
                        {
                            "raw_user_request": "我想要OpenAI做Reasoning方向的人",
                            "history_id": history_id,
                        },
                        ensure_ascii=False,
                    ).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with opener.open(plan_req) as response:
                    plan_payload = json.loads(response.read().decode("utf-8"))

            recovery_req = urllib_request.Request(
                f"http://{host}:{port}/api/frontend-history/{history_id}",
                method="GET",
            )
            with opener.open(recovery_req) as response:
                recovery_payload = json.loads(response.read().decode("utf-8"))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        self.assertEqual(plan_payload["status"], "pending")
        self.assertEqual(plan_payload["history_id"], history_id)
        self.assertEqual(recovery_payload["status"], "found")
        self.assertEqual(recovery_payload["recovery"]["history_id"], history_id)
        self.assertEqual(
            str(dict(recovery_payload["recovery"]["metadata"].get("plan_generation") or {}).get("status") or ""),
            "queued",
        )

    def test_frontend_history_recovery_serializes_utc_timestamps_for_api(self) -> None:
        history_id = "history-api-timestamp-1"
        self.store.upsert_frontend_history_link(
            {
                "history_id": history_id,
                "query_text": "我想要OpenAI做Reasoning方向的人",
                "target_company": "OpenAI",
                "phase": "results",
                "created_at": "2026-04-21 11:53:32",
            }
        )

        recovered = self.orchestrator.get_frontend_history_recovery(history_id)

        self.assertEqual(recovered["status"], "found")
        self.assertEqual(recovered["recovery"]["created_at"], "2026-04-21T11:53:32Z")
        self.assertRegex(str(recovered["recovery"]["updated_at"]), r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")

    def test_frontend_history_list_returns_latest_recoveries(self) -> None:
        self.store.upsert_frontend_history_link(
            {
                "history_id": "history-list-1",
                "query_text": "帮我找Anthropic做Post-train的人",
                "target_company": "Anthropic",
                "phase": "results",
                "job_id": "job-list-1",
                "request": {"raw_user_request": "帮我找Anthropic做Post-train的人", "target_company": "Anthropic"},
                "plan": {"target_company": "Anthropic", "keywords": ["Post-train"]},
            }
        )
        self.store.upsert_frontend_history_link(
            {
                "history_id": "history-list-2",
                "query_text": "帮我找OpenAI做Reasoning方向的人",
                "target_company": "OpenAI",
                "phase": "plan",
                "request": {"raw_user_request": "帮我找OpenAI做Reasoning方向的人", "target_company": "OpenAI"},
                "plan": {"target_company": "OpenAI", "keywords": ["Reasoning"]},
            }
        )

        payload = self.orchestrator.list_frontend_history(limit=10)
        history = list(payload.get("history") or [])

        self.assertEqual(payload["count"], 2)
        self.assertEqual(history[0]["history_id"], "history-list-2")
        self.assertEqual(history[1]["history_id"], "history-list-1")
        self.assertEqual(history[0]["target_company"], "OpenAI")
        self.assertEqual(history[1]["job_id"], "job-list-1")

    def test_frontend_history_delete_api_removes_shared_entry(self) -> None:
        history_id = "history-delete-1"
        self.store.upsert_frontend_history_link(
            {
                "history_id": history_id,
                "query_text": "帮我找Safe Superintelligence的人",
                "target_company": "Safe Superintelligence",
                "phase": "plan",
                "request": {
                    "raw_user_request": "帮我找Safe Superintelligence的人",
                    "target_company": "Safe Superintelligence",
                },
            }
        )
        server = create_server(self.orchestrator, host="127.0.0.1", port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))
        try:
            req = urllib_request.Request(
                f"http://{host}:{port}/api/frontend-history/{history_id}",
                method="DELETE",
            )
            with opener.open(req) as response:
                payload = json.loads(response.read().decode("utf-8"))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        self.assertEqual(payload["status"], "deleted")
        self.assertIsNone(self.store.get_frontend_history_link(history_id))


if __name__ == "__main__":
    unittest.main()
