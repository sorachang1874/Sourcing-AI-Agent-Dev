import json
import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock
from urllib import request as urllib_request

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.api import create_server
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.domain import Candidate, JobRequest
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
from sourcing_agent.storage import SQLiteStore


class FrontendHistoryRecoveryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.catalog = AssetCatalog.discover()
        self.store = SQLiteStore(f"{self.tempdir.name}/test.db")
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
            mock.patch.object(threading.Thread, "start", autospec=True, return_value=None),
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
        link = self.store.get_frontend_history_link(history_id)
        self.assertIsNotNone(link)
        assert link is not None
        self.assertEqual(link["job_id"], str(queued["job_id"]))
        self.assertEqual(link["phase"], "running")
        self.assertEqual(str(link["metadata"].get("workflow_kind") or ""), "excel_intake")
        self.assertFalse(link["plan"])

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
            mock.patch.object(threading.Thread, "start", autospec=True, return_value=None),
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
                "detected_contact_row_count": 1,
            },
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
                "artifact_result": {
                    "status": "completed",
                    "generation_key": "gen-1",
                    "generation_sequence": 1,
                },
            },
            "artifact_paths": {},
            "results": [],
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
        dashboard = self.orchestrator.get_job_dashboard("excelworkflowresults1")
        self.assertIsNotNone(dashboard)
        assert dashboard is not None
        self.assertEqual(int(dashboard["asset_population"].get("candidate_count") or 0), 1)
        link = self.store.get_frontend_history_link(history_id)
        self.assertIsNotNone(link)
        assert link is not None
        self.assertEqual(link["phase"], "results")

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
        self.assertEqual(progress["status"], "completed")
        link = self.store.get_frontend_history_link(history_id)
        self.assertIsNotNone(link)
        assert link is not None
        self.assertEqual(link["phase"], "results")

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

    def test_plan_api_round_trips_history_id_without_500(self) -> None:
        history_id = "history-plan-api-roundtrip-1"
        self._write_company_identity_snapshot(target_company="OpenAI", snapshot_id="20260422T115901")

        server = create_server(self.orchestrator, host="127.0.0.1", port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))
        try:
            plan_req = urllib_request.Request(
                f"http://{host}:{port}/api/plan",
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
