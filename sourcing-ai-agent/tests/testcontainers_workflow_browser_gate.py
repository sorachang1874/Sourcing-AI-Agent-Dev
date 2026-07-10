from __future__ import annotations

import json
import os
import subprocess
import tempfile
import threading
import time
import unittest
import urllib.request
from pathlib import Path
from unittest import mock

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.api import create_server
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.model_provider import DeterministicModelClient
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
from tests.testcontainers_frontend_browser_gate import (
    FRONTEND_ROOT,
    REPO_ROOT,
    _build_frontend,
    _find_free_port,
    _start_frontend_preview,
    _terminate_process,
    _wait_for_url,
)
from tests.testcontainers_helpers import (
    libpq_dsn_from_container,
    require_testcontainers_pg,
    start_postgres_container,
)

_COMPANY = "Container Workflow Co"
_EXPECTED_COUNT = 30
_FIRST_NAME = "Container Workflow Candidate 01"
_PAGE_TWO_NAME = "Probe Page Two Candidate"
_SEARCH_NAME = "Needle Search Candidate"
_SEARCH_KEYWORD = "needle"


class TestcontainersWorkflowBrowserGateTest(unittest.TestCase):
    def test_workflow_publishes_run_projection_and_browser_reads_without_fallback(self) -> None:
        require_testcontainers_pg()

        with tempfile.TemporaryDirectory() as tempdir:
            temp_path = Path(tempdir)
            project_root = temp_path / "project"
            runtime_dir = project_root / "runtime"
            object_store_dir = runtime_dir / "object_store"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            postgres = start_postgres_container()
            fake_apify = _build_fake_apify_fixture()
            try:
                dsn = libpq_dsn_from_container(postgres)
                api_port = _find_free_port()
                frontend_port = _find_free_port()
                base_env = {
                    "SOURCING_CONTROL_PLANE_POSTGRES_DSN": dsn,
                    "SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA": "workflow_browser_gate",
                    "SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE": "postgres_only",
                    "SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES": "1",
                    "SOURCING_PG_ONLY_SQLITE_BACKEND": "shared_memory",
                    "SOURCING_RUNTIME_DIR": str(runtime_dir),
                    "SOURCING_RUNTIME_ENVIRONMENT": "local_dev",
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "live",
                    "SOURCING_LIVE_PROVIDER_CONFIRM": "1",
                    "SOURCING_ALLOW_ISOLATED_LIVE_PROVIDER_ACCESS": "1",
                    "SOURCING_PROVIDER_WEBHOOK_TOKEN": "container-workflow-browser-secret",
                    "SOURCING_DEFAULT_APIFY_WEBHOOK_URL_ENABLED": "1",
                    "SOURCING_APIFY_API_BASE_URL": "",
                    "HARVEST_RUN_STATUS_WAIT_FOR_FINISH_SECONDS": "0",
                    "HARVEST_POLL_INTERVAL_SECONDS": "0",
                    "HARVEST_DATASET_FETCH_MAX_ATTEMPTS": "1",
                    "HARVEST_PROFILE_PREFETCH_NONBLOCKING_PROVIDER_HANDOFF_DEFAULT": "0",
                    "OBJECT_STORAGE_PROVIDER": "filesystem",
                    "OBJECT_STORAGE_LOCAL_DIR": str(object_store_dir),
                    "OBJECT_STORAGE_PREFIX": "workflow-browser-gate",
                }
                with fake_apify:
                    env = {
                        **base_env,
                        "SOURCING_APIFY_API_BASE_URL": fake_apify.base_url,
                        "SOURCING_APIFY_WEBHOOK_URL": f"http://127.0.0.1:{api_port}/api/providers/apify/webhook",
                    }
                    with mock.patch.dict(os.environ, env, clear=False):
                        store = ControlPlaneStore(runtime_dir / "sourcing_agent.db")
                        orchestrator = _build_orchestrator(
                            project_root=project_root,
                            runtime_dir=runtime_dir,
                            object_store_dir=object_store_dir,
                            store=store,
                        )
                        server = create_server(orchestrator, host="127.0.0.1", port=api_port)
                        api_thread = threading.Thread(target=server.serve_forever, daemon=True)
                        api_thread.start()
                        preview_process: subprocess.Popen[str] | None = None
                        try:
                            _wait_for_url(f"http://127.0.0.1:{api_port}/health", timeout_seconds=15)
                            start_payload = _post_json(
                                f"http://127.0.0.1:{api_port}/api/workflows",
                                {
                                    "raw_user_request": "帮我找Container Workflow Co做Agent方向的人",
                                    "target_company": _COMPANY,
                                    "skip_plan_review": True,
                                    "force_fresh": True,
                                    "execution_preferences": {
                                        "allow_local_bootstrap_fallback": False,
                                        "use_company_employees_lane": False,
                                    },
                                },
                            )
                            job_id = str(start_payload.get("job_id") or "").strip()
                            self.assertTrue(job_id, start_payload)

                            projection_id = _wait_for_workflow_projection_with_index(
                                orchestrator,
                                store,
                                job_id=job_id,
                                expected_count=_EXPECTED_COUNT,
                                timeout_seconds=75,
                            )
                            _build_frontend(api_port=api_port)
                            preview_process = _start_frontend_preview(
                                api_port=api_port,
                                frontend_port=frontend_port,
                            )
                            _wait_for_url(f"http://127.0.0.1:{frontend_port}/", timeout_seconds=30)
                            probe_payload = _run_workflow_projection_board_probe(
                                frontend_port=frontend_port,
                                projection_id=projection_id,
                            )
                            projection = store.repos.serving_projection.get(projection_id)
                            link = store.repos.serving_projection.get_run_link(job_id)
                        finally:
                            if preview_process is not None:
                                _terminate_process(preview_process)
                            server.shutdown()
                            server.server_close()
                            api_thread.join(timeout=5)
            finally:
                postgres.stop()

        self.assertEqual(probe_payload["status"], "ok")
        self.assertEqual(probe_payload["visibleCountText"], f"{_EXPECTED_COUNT}/{_EXPECTED_COUNT}")
        self.assertIn(_FIRST_NAME, probe_payload["firstPageNames"])
        self.assertIn(_PAGE_TWO_NAME, probe_payload["page2Names"])
        self.assertIn(_SEARCH_NAME, probe_payload["searchNames"])
        self.assertEqual(int(probe_payload["searchFilteredCandidateCount"]), 1)
        search_contract = dict(probe_payload["searchFilterContract"] or {})
        self.assertFalse(search_contract.get("fallback_used"))
        self.assertEqual(search_contract.get("source"), "projection_person_search_index")
        self.assertEqual(search_contract.get("row_filter_scope"), "projection_membership")
        self.assertEqual(str((projection or {}).get("source_run_id") or ""), str(link.get("run_id") or ""))
        provider_paths = [(request["method"], request["path"]) for request in fake_apify.requests]
        self.assertIn(("POST", "/v2/acts/Vb6LZkh4EqRlR0Ka9/runs"), provider_paths)
        self.assertIn(("POST", "/v2/acts/M2FMdjRVeF1HPGFcc/run-sync-get-dataset-items"), provider_paths)
        self.assertIn(("POST", "/v2/acts/LpVuK3Zozwuipa5bp/runs"), provider_paths)
        self.assertIn(("GET", "/v2/datasets/dataset-workflow-profiles/items"), provider_paths)


def _build_orchestrator(
    *,
    project_root: Path,
    runtime_dir: Path,
    object_store_dir: Path,
    store: ControlPlaneStore,
) -> SourcingOrchestrator:
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
                actor_id="LpVuK3Zozwuipa5bp",
                timeout_seconds=60,
                max_total_charge_usd=2.0,
                max_paid_items=100,
                default_mode="full",
            ),
            profile_search=HarvestActorSettings(
                enabled=True,
                api_token="fake-token",
                actor_id="M2FMdjRVeF1HPGFcc",
                timeout_seconds=60,
                max_total_charge_usd=2.0,
                max_paid_items=100,
                default_mode="short",
            ),
            company_employees=HarvestActorSettings(
                enabled=True,
                api_token="fake-token",
                actor_id="Vb6LZkh4EqRlR0Ka9",
                timeout_seconds=60,
                max_total_charge_usd=2.0,
                max_paid_items=100,
                default_mode="short",
            ),
        ),
        object_storage=ObjectStorageSettings(
            enabled=True,
            provider="filesystem",
            local_dir=str(object_store_dir),
            prefix="workflow-browser-gate",
        ),
    )
    catalog = AssetCatalog.discover()
    model_client = DeterministicModelClient()
    acquisition_engine = AcquisitionEngine(catalog, settings, store, model_client)
    return SourcingOrchestrator(
        catalog=catalog,
        store=store,
        jobs_dir=settings.jobs_dir,
        model_client=model_client,
        semantic_provider=LocalSemanticProvider(),
        acquisition_engine=acquisition_engine,
    )


def _build_fake_apify_fixture() -> FakeApifyProvider:
    fake_apify = FakeApifyProvider()
    candidates = _candidate_fixture_rows()
    fake_apify.add_actor_run(
        actor_id="Vb6LZkh4EqRlR0Ka9",
        run_id="run-workflow-company",
        dataset_id="dataset-workflow-company",
        dataset_items=[_company_employee_item(row) for row in candidates],
    )
    fake_apify.add_actor_run(
        actor_id="M2FMdjRVeF1HPGFcc",
        run_id="run-workflow-profile-search",
        dataset_id="dataset-workflow-profile-search",
        dataset_items=[_profile_search_item(row) for row in candidates],
    )
    fake_apify.add_actor_run(
        actor_id="LpVuK3Zozwuipa5bp",
        run_id="run-workflow-profiles",
        dataset_id="dataset-workflow-profiles",
        dataset_items=[_profile_scraper_item(row) for row in candidates],
    )
    return fake_apify


def _candidate_fixture_rows() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for index in range(1, _EXPECTED_COUNT + 1):
        if index == 7:
            name = _SEARCH_NAME
            slug = "container-workflow-needle-search-candidate"
            summary = "Needle search specialist for canonical workflow browser validation."
        elif index == 25:
            name = _PAGE_TWO_NAME
            slug = "container-workflow-probe-page-two-candidate"
            summary = "Pagination probe candidate for the workflow-driven projection gate."
        else:
            name = f"Container Workflow Candidate {index:02d}"
            slug = f"container-workflow-candidate-{index:02d}"
            summary = f"Agent workflow systems candidate {index:02d}."
        rows.append(
            {
                "index": str(index),
                "name": name,
                "slug": slug,
                "url": f"https://www.linkedin.com/in/{slug}/",
                "headline": f"Agent Systems Engineer at {_COMPANY}",
                "location": "San Francisco Bay Area",
                "summary": summary,
            }
        )
    return rows


def _pagination(row: dict[str, str]) -> dict[str, object]:
    index = int(row["index"])
    return {
        "totalElements": _EXPECTED_COUNT,
        "totalPages": 2,
        "pageNumber": 1 if index <= 25 else 2,
        "pageSize": 25,
    }


def _company_employee_item(row: dict[str, str]) -> dict[str, object]:
    return {
        "fullName": row["name"],
        "linkedinUrl": row["url"],
        "profileUrl": row["url"],
        "publicIdentifier": row["slug"],
        "headline": row["headline"],
        "currentCompany": _COMPANY,
        "location": row["location"],
        "photoUrl": f"https://cdn.example.com/{row['slug']}.jpg",
        "_meta": {"pagination": _pagination(row)},
    }


def _profile_search_item(row: dict[str, str]) -> dict[str, object]:
    return {
        "fullName": row["name"],
        "linkedinUrl": row["url"],
        "profileUrl": row["url"],
        "publicIdentifier": row["slug"],
        "headline": row["headline"],
        "currentCompany": _COMPANY,
        "location": row["location"],
        "summary": row["summary"],
        "item": {
            "fullName": row["name"],
            "linkedinUrl": row["url"],
            "profileUrl": row["url"],
            "publicIdentifier": row["slug"],
            "headline": row["headline"],
            "currentCompany": _COMPANY,
            "location": row["location"],
            "summary": row["summary"],
        },
        "_meta": {"pagination": _pagination(row)},
    }


def _profile_scraper_item(row: dict[str, str]) -> dict[str, object]:
    return {
        "_harvest_request": {"kind": "url", "value": row["url"], "profile_url": row["url"]},
        "originalQuery": {"url": row["url"]},
        "fullName": row["name"],
        "linkedinUrl": row["url"],
        "profileUrl": row["url"],
        "publicIdentifier": row["slug"],
        "headline": row["headline"],
        "currentCompany": _COMPANY,
        "location": row["location"],
        "photoUrl": f"https://cdn.example.com/{row['slug']}.jpg",
        "about": row["summary"],
        "experience": [
            {
                "companyName": _COMPANY,
                "title": "Agent Systems Engineer",
                "description": row["summary"],
            }
        ],
        "education": [{"schoolName": "Canonical University", "degreeName": "MS Computer Science"}],
        "skills": ["agent workflows", "projection serving", "provider integration"],
    }


def _post_json(url: str, payload: dict[str, object]) -> dict[str, object]:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def _wait_for_workflow_projection_with_index(
    orchestrator: SourcingOrchestrator,
    store: ControlPlaneStore,
    *,
    job_id: str,
    expected_count: int,
    timeout_seconds: float,
) -> str:
    deadline = time.monotonic() + timeout_seconds
    last_status: dict[str, object] = {}
    while time.monotonic() < deadline:
        link = store.repos.serving_projection.get_run_link(job_id)
        projection_id = str((link or {}).get("projection_id") or "").strip()
        if projection_id:
            member_count = store.repos.serving_projection.count_members(projection_id)
            index_count = store.repos.serving_projection.count_person_search_index(projection_id)
            last_status = {
                "job_id": job_id,
                "projection_id": projection_id,
                "member_count": member_count,
                "index_count": index_count,
                "job": store.get_job(job_id),
            }
            if member_count >= expected_count and index_count >= member_count:
                return projection_id
        orchestrator.run_worker_recovery_once(
            {
                "job_id": job_id,
                "projection_person_search_index_enabled": True,
                "projection_person_search_index_item_limit": 4,
                "recovery_tick_total_budget_ms": 10000,
                "worker_recovery_phase_budget_ms": 5000,
                "profile_prefetch_nonblocking_submit": False,
                "source": "testcontainers_workflow_browser_gate",
            }
        )
        time.sleep(0.25)
    raise AssertionError(f"timed out waiting for workflow projection/index readiness: {last_status}")


def _run_workflow_projection_board_probe(*, frontend_port: int, projection_id: str) -> dict[str, object]:
    screenshot_path = REPO_ROOT / "output" / "playwright" / "container-workflow-projection-board-probe.png"
    result = subprocess.run(
        [
            "node",
            "./scripts/run_projection_board_probe.mjs",
            "--frontend-url",
            f"http://127.0.0.1:{frontend_port}",
            "--projection-id",
            projection_id,
            "--expected-count",
            str(_EXPECTED_COUNT),
            "--expected-first-name",
            _FIRST_NAME,
            "--expected-page2-name",
            _PAGE_TWO_NAME,
            "--search-keyword",
            _SEARCH_KEYWORD,
            "--expected-search-name",
            _SEARCH_NAME,
            "--timeout-ms",
            "60000",
            "--screenshot",
            str(screenshot_path),
        ],
        cwd=FRONTEND_ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=90,
    )
    if result.returncode != 0:
        raise AssertionError(
            "workflow projection board probe failed\n"
            f"stdout:\n{result.stdout[-4000:]}\n"
            f"stderr:\n{result.stderr[-4000:]}"
        )
    return json.loads(result.stdout)


if __name__ == "__main__":
    unittest.main()
