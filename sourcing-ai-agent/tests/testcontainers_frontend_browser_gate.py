from __future__ import annotations

import json
import os
import socket
import subprocess
import tempfile
import threading
import time
import unittest
import urllib.request
from contextlib import closing
from pathlib import Path
from unittest import mock

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.api import create_server
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.orchestrator import SourcingOrchestrator
from sourcing_agent.person_asset_writer import PersonAssetWriter
from sourcing_agent.public_candidate_facets import public_facet_counts_from_records
from sourcing_agent.semantic_provider import LocalSemanticProvider
from sourcing_agent.serving_projection_writer import ServingProjectionWriter
from sourcing_agent.settings import AppSettings, QwenSettings, SemanticProviderSettings
from sourcing_agent.storage import ControlPlaneStore
from tests.testcontainers_helpers import (
    libpq_dsn_from_container,
    require_testcontainers_pg,
    start_postgres_container,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
FRONTEND_ROOT = REPO_ROOT / "frontend-demo"


class TestcontainersFrontendBrowserGateTest(unittest.TestCase):
    def test_projection_board_loads_pages_and_backend_filtered_search_without_fallback(self) -> None:
        require_testcontainers_pg()

        with tempfile.TemporaryDirectory() as tempdir:
            temp_path = Path(tempdir)
            project_root = temp_path / "project"
            runtime_dir = project_root / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            postgres = start_postgres_container()
            try:
                dsn = libpq_dsn_from_container(postgres)
                projection_id = "proj_container_browser"
                api_port = _find_free_port()
                frontend_port = _find_free_port()
                env = {
                    "SOURCING_CONTROL_PLANE_POSTGRES_DSN": dsn,
                    "SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA": "frontend_browser_gate",
                    "SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE": "postgres_only",
                    "SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES": "1",
                    "SOURCING_PG_ONLY_SQLITE_BACKEND": "shared_memory",
                    "SOURCING_RUNTIME_DIR": str(runtime_dir),
                    "SOURCING_RUNTIME_ENVIRONMENT": "test",
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "simulate",
                    "SOURCING_LIVE_PROVIDER_ACCESS_DISABLED": "1",
                }
                with mock.patch.dict(os.environ, env, clear=False):
                    store = ControlPlaneStore(runtime_dir / "sourcing_agent.db")
                    self._seed_projection(store, projection_id=projection_id)
                    orchestrator = self._build_orchestrator(project_root=project_root, runtime_dir=runtime_dir, store=store)
                    server = create_server(orchestrator, host="127.0.0.1", port=api_port)
                    api_thread = threading.Thread(target=server.serve_forever, daemon=True)
                    api_thread.start()
                    preview_process: subprocess.Popen[str] | None = None
                    try:
                        _wait_for_url(f"http://127.0.0.1:{api_port}/api/projections/{projection_id}", timeout_seconds=15)
                        _build_frontend(api_port=api_port)
                        preview_process = _start_frontend_preview(api_port=api_port, frontend_port=frontend_port)
                        _wait_for_url(f"http://127.0.0.1:{frontend_port}/", timeout_seconds=30)
                        probe_payload = _run_projection_board_probe(
                            frontend_port=frontend_port,
                            projection_id=projection_id,
                        )
                    finally:
                        if preview_process is not None:
                            _terminate_process(preview_process)
                        server.shutdown()
                        server.server_close()
                        api_thread.join(timeout=5)
            finally:
                postgres.stop()

        self.assertEqual(probe_payload["status"], "ok")
        self.assertEqual(probe_payload["visibleCountText"], "30/30")
        self.assertGreaterEqual(int(probe_payload["firstPageCardCount"]), 24)
        self.assertIn("Container Candidate 01", probe_payload["firstPageNames"])
        self.assertIn("Probe Page Two Candidate", probe_payload["page2Names"])
        self.assertIn("Needle Search Candidate", probe_payload["searchNames"])
        self.assertEqual(int(probe_payload["searchFilteredCandidateCount"]), 1)
        search_contract = dict(probe_payload["searchFilterContract"] or {})
        self.assertFalse(search_contract.get("fallback_used"))
        self.assertEqual(search_contract.get("source"), "projection_person_search_index")
        self.assertEqual(search_contract.get("row_filter_scope"), "projection_membership")

    @staticmethod
    def _build_orchestrator(
        *,
        project_root: Path,
        runtime_dir: Path,
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

    @staticmethod
    def _seed_projection(store: ControlPlaneStore, *, projection_id: str) -> None:
        records: list[dict[str, object]] = []
        members: list[dict[str, object]] = []
        for index in range(30):
            ordinal = index + 1
            if ordinal == 7:
                name = "Needle Search Candidate"
                summary = "Container browser gate candidate with unique needle keyword."
                matched_keywords = ["needle"]
            elif ordinal == 25:
                name = "Probe Page Two Candidate"
                summary = "First candidate on page two for projection pagination validation."
                matched_keywords = ["pagination"]
            else:
                name = f"Container Candidate {ordinal:02d}"
                summary = f"Canonical projection browser fixture candidate {ordinal:02d}."
                matched_keywords = ["projection"]
            employment_status = "former" if ordinal % 5 == 0 else "current"
            public_summary = {
                "candidate_id": f"cand_container_{ordinal:02d}",
                "id": f"cand_container_{ordinal:02d}",
                "name": name,
                "display_name": name,
                "linkedin_url": f"https://www.linkedin.com/in/container-candidate-{ordinal:02d}/",
                "profile_url": f"https://www.linkedin.com/in/container-candidate-{ordinal:02d}/",
                "headline": f"Staff Engineer at Container Browser Co {ordinal:02d}",
                "title": "Staff Engineer",
                "current_company": "Container Browser Co",
                "location": "San Francisco Bay Area",
                "employment_status": employment_status,
                "summary": summary,
                "experience_lines": ["Container Browser Co", "Projection Platform"],
                "education_lines": ["Canonical University"],
                "skills": ["projection", "browser", "workflow"],
                "matched_keywords": matched_keywords,
                "outreach_layer": ordinal % 4,
                "has_profile_detail": True,
                "needs_profile_completion": False,
                "low_profile_richness": False,
                "profile_fetched_at": "2026-05-20T08:00:00Z",
            }
            records.append(public_summary)
            members.append(
                {
                    "candidate_identity_key": f"linkedin:container-candidate-{ordinal:02d}",
                    "person_identity_key": f"linkedin:container-candidate-{ordinal:02d}",
                    "candidate_id": f"cand_container_{ordinal:02d}",
                    "rank_index": index,
                    "employment_scope": employment_status,
                    "source_shard_key": "container_browser_gate",
                    "source_run_id": "job-container-browser",
                    "row_readiness": "ready",
                    "profile_readiness": "ready",
                    "card_readiness": "ready",
                    "visibility_state": "visible",
                    "public_summary": public_summary,
                    "projection_metrics": {
                        "has_profile_detail": True,
                        "needs_profile_completion": False,
                        "low_profile_richness": False,
                    },
                    "metadata": {
                        "fixture": "testcontainers_frontend_browser_gate",
                    },
                }
            )
        facet_counts = public_facet_counts_from_records(records)
        facet_counts["count_scope"] = "exact_projection"
        writer = ServingProjectionWriter(store, writer_id="testcontainers_frontend_browser_gate")
        result = writer.publish_run_scope_projection(
            run_id="job-container-browser",
            collection_id="company:container-browser-co",
            projection_id=projection_id,
            members=members,
            replace_members=True,
            scope_label="Container Browser Projection",
            scope_spec={
                "target_company": "Container Browser Co",
                "keywords": ["projection", "browser"],
            },
            counts={
                "result_count": len(members),
                "candidate_count": len(members),
                "count_scope": "exact_projection",
                "public_facet_counts": facet_counts,
                "profile_fetch_required_count": len(members),
                "profile_fetched_count": len(members),
                "card_materialized_count": len(members),
            },
            readiness={
                "row_count": len(members),
                "profile_required_count": len(members),
                "profile_ready_count": len(members),
                "card_ready_count": len(members),
                "count_scope": "exact_projection",
            },
            state="serving",
        )
        assert result["member_count"] == len(members)
        index_result = PersonAssetWriter(store, writer_id="testcontainers_frontend_browser_gate").rebuild_projection_person_search_index(
            projection_id=projection_id,
            count_scope="exact_projection",
            member_page_size=100,
            max_members=100,
        )
        assert index_result["status"] == "indexed"
        assert index_result["indexed_count"] == len(members)


def _find_free_port() -> int:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _frontend_env(*, api_port: int) -> dict[str, str]:
    return {
        **os.environ,
        "VITE_API_BASE_URL": "same-origin",
        "VITE_DEV_PROXY_TARGET": f"http://127.0.0.1:{api_port}",
    }


def _build_frontend(*, api_port: int) -> None:
    result = subprocess.run(
        ["npm", "run", "build"],
        cwd=FRONTEND_ROOT,
        env=_frontend_env(api_port=api_port),
        check=False,
        capture_output=True,
        text=True,
        timeout=180,
    )
    if result.returncode != 0:
        raise AssertionError(
            "frontend build failed\n"
            f"stdout:\n{result.stdout[-4000:]}\n"
            f"stderr:\n{result.stderr[-4000:]}"
        )


def _start_frontend_preview(*, api_port: int, frontend_port: int) -> subprocess.Popen[str]:
    return subprocess.Popen(
        ["npm", "run", "preview", "--", "--host", "127.0.0.1", "--port", str(frontend_port)],
        cwd=FRONTEND_ROOT,
        env=_frontend_env(api_port=api_port),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _run_projection_board_probe(*, frontend_port: int, projection_id: str) -> dict[str, object]:
    screenshot_path = REPO_ROOT / "output" / "playwright" / "container-projection-board-probe.png"
    result = subprocess.run(
        [
            "node",
            "./scripts/run_projection_board_probe.mjs",
            "--frontend-url",
            f"http://127.0.0.1:{frontend_port}",
            "--projection-id",
            projection_id,
            "--expected-count",
            "30",
            "--expected-first-name",
            "Container Candidate 01",
            "--expected-page2-name",
            "Probe Page Two Candidate",
            "--search-keyword",
            "needle",
            "--expected-search-name",
            "Needle Search Candidate",
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
            "projection board probe failed\n"
            f"stdout:\n{result.stdout[-4000:]}\n"
            f"stderr:\n{result.stderr[-4000:]}"
        )
    return json.loads(result.stdout)


def _wait_for_url(url: str, *, timeout_seconds: float) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                if 200 <= int(response.status) < 500:
                    return
        except Exception as exc:  # pragma: no cover - failure message carries the last error.
            last_error = exc
        time.sleep(0.25)
    raise AssertionError(f"timed out waiting for {url}: {last_error}")


def _terminate_process(process: subprocess.Popen[str]) -> None:
    try:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
    finally:
        if process.stdout is not None:
            process.stdout.close()
        if process.stderr is not None:
            process.stderr.close()


if __name__ == "__main__":
    unittest.main()
