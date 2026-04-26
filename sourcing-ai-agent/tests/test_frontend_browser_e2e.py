import json
import os
import re
import socket
import subprocess
import time
import unittest
import zipfile
from pathlib import Path
from urllib import request as urllib_request

import tests.test_hosted_workflow_smoke as hosted_smoke_module
from sourcing_agent.domain import Candidate
from tests.test_excel_intake import _write_inline_workbook


def _env_flag(name: str) -> bool:
    return str(os.getenv(name) or "").strip().lower() in {"1", "true", "yes", "on"}


class _FrontendBrowserE2EBase(unittest.TestCase):
    FAST_SIMULATE_CASES = [
        {
            "name": "small_full_reuse_humansand_coding",
            "query": "我想了解Humans&里偏Coding agents方向的研究成员",
            "expected_target_company": "Humans&",
            "expected_strategy": "全量本地资产复用",
            "expected_project_scope": "目标公司全量范围",
            "expected_keywords_contains": ["Coding"],
            "expected_metadata": {
                "dispatchStrategy": "直接复用 snapshot",
                "requiresDeltaAcquisition": False,
                "organizationScaleBand": "小型组织",
            },
            "max_final_results_to_candidate_board_ms": 4000,
            "max_final_results_duration_seconds": 30,
        },
        {
            "name": "medium_reuse_anthropic_pretraining",
            "query": "帮我找Anthropic里做Pre-training方向的人",
            "expected_target_company": "Anthropic",
            "expected_project_scope": "目标公司全量范围",
            "expected_keywords_contains": ["Pre-train"],
            "expected_metadata": {
                "dispatchStrategy": "直接复用 snapshot",
                "requiresDeltaAcquisition": False,
                "currentLaneBehavior": "复用本地 baseline",
                "formerLaneBehavior": "复用本地 baseline",
                "organizationScaleBand": "中型组织",
            },
            "max_final_results_to_candidate_board_ms": 5000,
            "max_final_results_duration_seconds": 45,
        },
    ]
    SLOW_SIMULATE_CASES = [
        {
            "name": "large_scoped_delta_openai_pretrain",
            "query": "我想要OpenAI做Pre-train方向的人",
            "timeout_seconds": 180,
            "expected_target_company": "OpenAI",
            "expected_strategy": "Scoped search + Baseline 复用增量",
            "expected_project_scope": "目标公司全量范围",
            "expected_keywords_contains": ["Pre-train"],
            "expected_metadata": {
                "dispatchStrategy": "基于 snapshot 补 delta",
                "plannerMode": "delta_from_snapshot",
                "requiresDeltaAcquisition": True,
                "currentLaneBehavior": "只补缺口增量",
                "formerLaneBehavior": "只补缺口增量",
                "organizationScaleBand": "大型组织",
            },
            "max_final_results_to_candidate_board_ms": 7000,
            "max_final_results_duration_seconds": 120,
        },
        {
            "name": "large_partial_shard_google_multimodal_pretrain",
            "query": "帮我找Google里做多模态和Pre-train方向的人（包括Veo和Nano Banana相关）",
            "timeout_seconds": 240,
            "expected_target_company": "Google",
            "expected_strategy": "Scoped search + Baseline 复用增量",
            "expected_project_scope": "目标公司全量范围",
            "expected_keywords_contains": ["Multimodal", "Pre-train", "Veo", "Nano Banana"],
            "expected_metadata": {
                "dispatchStrategy": "基于 snapshot 补 delta",
                "plannerMode": "delta_from_snapshot",
                "requiresDeltaAcquisition": True,
                "currentLaneBehavior": "复用本地 baseline",
                "formerLaneBehavior": "只补缺口增量",
                "organizationScaleBand": "大型组织",
            },
            "max_final_results_to_candidate_board_ms": 7000,
            "max_final_results_duration_seconds": 150,
        },
    ]
    SCRIPTED_RUNTIME_IDENTITY_CASE = {
        "name": "new_org_runtime_identity_physical_intelligence_full_roster",
        "query": "给我Physical Intelligence的所有成员",
        "expected_target_company": "Physical Intelligence",
        "expected_strategy": "全量 live roster",
        "expected_project_scope": "目标公司全量范围",
        "expected_keywords_contains": [],
        "expected_metadata": {
            "dispatchStrategy": "新建 workflow",
            "requiresDeltaAcquisition": False,
            "currentLaneBehavior": "直接实时采集",
            "formerLaneBehavior": "直接实时采集",
        },
        "max_final_results_to_candidate_board_ms": 6000,
        "max_final_results_duration_seconds": 45,
    }
    LARGE_ORG_EXISTING_BASELINE_CASE = {
        "name": "large_org_existing_baseline_xai_full_roster",
        "query": "给我 xAI 的所有成员",
        "expected_target_company": "xAI",
        "expected_strategy": "全量本地资产复用",
        "expected_project_scope": "目标公司全量范围",
        "expected_keywords_contains": [],
        "expected_metadata": {
            "dispatchStrategy": "直接复用 snapshot",
            "requiresDeltaAcquisition": False,
            "currentLaneBehavior": "复用本地 baseline",
            "formerLaneBehavior": "复用本地 baseline",
            "organizationScaleBand": "大型组织",
        },
        "max_final_results_to_candidate_board_ms": 5000,
        "max_final_results_duration_seconds": 45,
    }

    @classmethod
    def setUpClass(cls) -> None:
        cls.repo_root = Path(__file__).resolve().parents[1]
        cls.frontend_dir = cls.repo_root / "frontend-demo"
        cls.playwright_env = cls._build_subprocess_env()
        if not _env_flag("SOURCING_RUN_FRONTEND_BROWSER_E2E"):
            raise unittest.SkipTest(
                "set SOURCING_RUN_FRONTEND_BROWSER_E2E=1 to run browser workflow e2e coverage"
            )
        try:
            subprocess.run(
                ["npm", "run", "browser:check"],
                cwd=cls.frontend_dir,
                env=cls.playwright_env,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=60,
            )
        except Exception as exc:  # pragma: no cover - environment dependent
            raise unittest.SkipTest(f"playwright browser unavailable: {exc}") from exc

    @classmethod
    def _build_subprocess_env(cls) -> dict[str, str]:
        repo_root = Path(__file__).resolve().parents[1]
        env = dict(os.environ)
        env["NO_PROXY"] = "*"
        env["PLAYWRIGHT_BROWSERS_PATH"] = env.get(
            "PLAYWRIGHT_BROWSERS_PATH",
            str(repo_root / ".cache" / "ms-playwright"),
        )
        env["PLAYWRIGHT_LD_LIBRARY_PATH"] = env.get(
            "PLAYWRIGHT_LD_LIBRARY_PATH",
            str(repo_root / ".cache" / "ubuntu-libs" / "root" / "usr" / "lib" / "x86_64-linux-gnu"),
        )
        env["NPM_CONFIG_CACHE"] = env.get("NPM_CONFIG_CACHE", str(repo_root / ".cache" / "npm"))
        return env

    def _reserve_free_port(self) -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return int(sock.getsockname()[1])

    def _wait_for_http_ready(self, url: str, *, timeout: float) -> None:
        opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))
        deadline = time.time() + timeout
        last_error: Exception | None = None
        while time.time() < deadline:
            try:
                with opener.open(url, timeout=5) as response:
                    if int(getattr(response, "status", 200) or 200) < 500:
                        return
            except Exception as exc:  # pragma: no cover - operational polling
                last_error = exc
            time.sleep(0.5)
        raise AssertionError(f"timed out waiting for {url}: {last_error}")

    def _terminate_process(self, process: subprocess.Popen[str], log_path: Path) -> None:
        if process.poll() is not None:
            return
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:  # pragma: no cover - cleanup fallback
            process.kill()
            process.wait(timeout=5)

    def _run_browser_case(
        self,
        *,
        frontend_url: str,
        query: str,
        screenshot_path: Path,
        timeout_seconds: int = 120,
        check_pagination_stability: bool = False,
        pagination_hydration_timeout_seconds: int = 20,
        start_url: str = "",
        restore_existing_results: bool = False,
    ) -> dict:
        command = [
            "node",
            str(self.frontend_dir / "scripts" / "run_workflow_e2e.mjs"),
            "--frontend-url",
            frontend_url,
            "--timeout-ms",
            str(timeout_seconds * 1000),
            "--screenshot",
            str(screenshot_path),
        ]
        if start_url:
            command.extend(["--start-url", start_url])
        if restore_existing_results:
            command.append("--restore-existing-results")
        else:
            command.extend(["--query", query])
        if check_pagination_stability:
            command.extend(
                [
                    "--check-pagination-stability",
                    "--pagination-target-page",
                    "2",
                    "--pagination-hydration-timeout-ms",
                    str(max(1, pagination_hydration_timeout_seconds) * 1000),
                ]
            )
        result = subprocess.run(
            command,
            cwd=self.frontend_dir,
            env=self.playwright_env,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout_seconds + 30,
        )
        return json.loads(result.stdout)

    def _run_excel_intake_browser_case(
        self,
        *,
        frontend_url: str,
        workbook_path: Path,
        screenshot_path: Path,
        timeout_seconds: int = 60,
    ) -> dict:
        command = [
            "node",
            str(self.frontend_dir / "scripts" / "run_excel_intake_e2e.mjs"),
            "--frontend-url",
            frontend_url,
            "--workbook",
            str(workbook_path),
            "--timeout-ms",
            str(timeout_seconds * 1000),
            "--screenshot",
            str(screenshot_path),
        ]
        result = subprocess.run(
            command,
            cwd=self.frontend_dir,
            env=self.playwright_env,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout_seconds + 30,
        )
        return json.loads(result.stdout)

    def _run_target_public_web_browser_case(
        self,
        *,
        frontend_url: str,
        screenshot_path: Path,
        timeout_seconds: int = 60,
    ) -> dict:
        command = [
            "node",
            str(self.frontend_dir / "scripts" / "run_target_public_web_e2e.mjs"),
            "--frontend-url",
            frontend_url,
            "--timeout-ms",
            str(timeout_seconds * 1000),
            "--screenshot",
            str(screenshot_path),
        ]
        result = subprocess.run(
            command,
            cwd=self.frontend_dir,
            env=self.playwright_env,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout_seconds + 30,
        )
        return json.loads(result.stdout)

    def _run_target_public_web_promotion_export_browser_case(
        self,
        *,
        frontend_url: str,
        screenshot_path: Path,
        download_path: Path,
        expected_email: str,
        timeout_seconds: int = 60,
    ) -> dict:
        command = [
            "node",
            str(self.frontend_dir / "scripts" / "run_target_public_web_promotion_export_e2e.mjs"),
            "--frontend-url",
            frontend_url,
            "--timeout-ms",
            str(timeout_seconds * 1000),
            "--screenshot",
            str(screenshot_path),
            "--download",
            str(download_path),
            "--expected-email",
            expected_email,
        ]
        result = subprocess.run(
            command,
            cwd=self.frontend_dir,
            env=self.playwright_env,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout_seconds + 30,
        )
        return json.loads(result.stdout)

    def _parse_duration_seconds(self, value: object) -> int | None:
        text = str(value or "").strip()
        if not text:
            return None
        if text == "<1s":
            return 0
        match = re.fullmatch(r"(\d+)s", text)
        if match is None:
            return None
        return int(match.group(1))

    def _build_large_org_candidate_specs(self, company_name: str, *, current_count: int, former_count: int) -> list[dict[str, str]]:
        specs: list[dict[str, str]] = [
            {
                "name": f"{company_name} Systems",
                "slug": f"{company_name.lower()}-systems".replace(" ", "-"),
                "role": "ML Systems Engineer",
                "focus_text": "Distributed systems, training infrastructure, and inference platform.",
            },
            {
                "name": f"{company_name} Research",
                "slug": f"{company_name.lower()}-research".replace(" ", "-"),
                "role": "Research Scientist",
                "focus_text": "Frontier-model research, reasoning, and post-training.",
            },
            {
                "name": f"{company_name} Former",
                "slug": f"{company_name.lower()}-former".replace(" ", "-"),
                "role": "Former Research Engineer",
                "employment_status": "former",
                "focus_text": "Former research engineer focused on evaluation and data systems.",
            },
        ]
        effective_total = 320
        for index in range(4, effective_total + 1):
            employment_status = "former" if index % 6 == 0 else "current"
            specs.append(
                {
                    "name": f"{company_name} Candidate {index:03d}",
                    "slug": f"{company_name.lower()}-{index:03d}".replace(" ", "-"),
                    "role": "Research Engineer" if index % 2 else "Research Scientist",
                    "employment_status": employment_status,
                    "focus_text": (
                        "Reasoning, Coding, inference systems, evaluation"
                        if index % 3
                        else "Pre-training, multimodal, data systems, scaling"
                    ),
                }
            )
        return specs

    def _assert_case_payload(self, case: dict, payload: dict) -> None:
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["plan"]["targetCompany"], case["expected_target_company"])
        if case.get("expected_strategy"):
            self.assertEqual(payload["plan"]["strategy"], case["expected_strategy"])
        self.assertEqual(payload["plan"]["projectScope"], case["expected_project_scope"])
        for expected_keyword in case["expected_keywords_contains"]:
            self.assertIn(expected_keyword, payload["plan"]["keywords"])
        for key, expected_value in dict(case.get("expected_metadata") or {}).items():
            self.assertEqual(payload["plan"]["metadata"].get(key), expected_value)
        self.assertGreater(payload["results"]["initialCount"], 0)
        self.assertGreater(payload["results"]["reloadedCount"], 0)
        self.assertEqual(payload["results"]["initialCount"], payload["results"]["reloadedCount"])
        self.assertTrue(payload["results"]["previewNames"])
        self.assertEqual(payload["results"]["previewNames"], payload["results"]["reloadedPreviewNames"])
        self.assertGreater(payload["results"]["openLinkedinActionCount"], 0)
        self.assertIn("history=", payload["historyUrl"])
        self.assertTrue(
            payload["manualReview"]["hasEmptyState"] or payload["manualReview"]["hasReviewCards"],
            payload,
        )
        timings = dict(payload["results"].get("timingsMs") or {})
        final_results_to_candidate_board_ms = timings.get("finalResultsToCandidateBoardMs")
        max_final_results_to_candidate_board_ms = case.get("max_final_results_to_candidate_board_ms")
        if max_final_results_to_candidate_board_ms is not None:
            self.assertIsNotNone(final_results_to_candidate_board_ms, payload)
            self.assertLessEqual(
                int(final_results_to_candidate_board_ms),
                int(max_final_results_to_candidate_board_ms),
                payload,
            )
        if (
            isinstance(final_results_to_candidate_board_ms, (int, float))
            and int(final_results_to_candidate_board_ms) > 2000
        ):
            self.assertTrue(payload["results"].get("sawInitialResultsLoadingCard"), payload)
        final_results_duration_seconds = self._parse_duration_seconds(
            dict(payload["results"].get("timelineDurations") or {}).get("Final Results")
        )
        max_final_results_duration_seconds = case.get("max_final_results_duration_seconds")
        if (
            final_results_duration_seconds is not None
            and max_final_results_duration_seconds is not None
        ):
            self.assertLessEqual(
                final_results_duration_seconds,
                int(max_final_results_duration_seconds),
                payload,
            )
        reloaded_final_results_duration_seconds = self._parse_duration_seconds(
            dict(payload["results"].get("reloadedTimelineDurations") or {}).get("Final Results")
        )
        if (
            reloaded_final_results_duration_seconds is not None
            and max_final_results_duration_seconds is not None
        ):
            self.assertLessEqual(
                reloaded_final_results_duration_seconds,
                int(max_final_results_duration_seconds),
                payload,
            )

    def _assert_pagination_stability_payload(self, payload: dict) -> None:
        pagination = dict(payload.get("pagination") or {})
        self.assertTrue(pagination.get("attempted"), payload)
        self.assertTrue(pagination.get("stableCurrentPage"), payload)
        self.assertTrue(pagination.get("stablePreviewNames"), payload)
        self.assertEqual(int(dict(pagination.get("before") or {}).get("pager", {}).get("currentPage") or 0), 2, payload)
        self.assertEqual(int(dict(pagination.get("after") or {}).get("pager", {}).get("currentPage") or 0), 2, payload)
        if not pagination.get("loadedCountIncreased"):
            before_visible = dict(dict(pagination.get("before") or {}).get("visibleCount") or {})
            after_visible = dict(dict(pagination.get("after") or {}).get("visibleCount") or {})
            self.assertEqual(
                int(before_visible.get("loadedCount") or 0),
                int(before_visible.get("totalCount") or 0),
                payload,
            )
            self.assertEqual(
                int(after_visible.get("loadedCount") or 0),
                int(after_visible.get("totalCount") or 0),
                payload,
            )

    def _run_seeded_hosted_cases(self, cases: list[dict]) -> list[tuple[dict, dict]]:
        helper = hosted_smoke_module.HostedWorkflowSmokeTest(
            methodName="test_default_smoke_matrix_covers_reference_orgs"
        )
        frontend_port = self._reserve_free_port()
        frontend_url = f"http://127.0.0.1:{frontend_port}"
        log_path = self.repo_root / "runtime" / "service_logs" / f"frontend-e2e-{frontend_port}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)

        with helper._hosted_harness(provider_mode="simulate") as harness:
            helper._seed_explain_matrix_assets(harness)

            frontend_env = dict(self.playwright_env)
            frontend_env["VITE_API_BASE_URL"] = harness.client.base_url
            with log_path.open("w", encoding="utf-8") as log_handle:
                frontend_process = subprocess.Popen(
                    ["npm", "run", "dev", "--", "--host", "127.0.0.1", "--port", str(frontend_port)],
                    cwd=self.frontend_dir,
                    env=frontend_env,
                    stdout=log_handle,
                    stderr=subprocess.STDOUT,
                    text=True,
                )
                try:
                    self._wait_for_http_ready(frontend_url, timeout=45)
                    payloads = []
                    for case in cases:
                        payloads.append(
                            (
                                case,
                                self._run_browser_case(
                                    frontend_url=frontend_url,
                                    query=case["query"],
                                    timeout_seconds=int(case.get("timeout_seconds") or 120),
                                    screenshot_path=self.repo_root
                                    / "output"
                                    / "playwright"
                                    / f"frontend-browser-e2e-{case['name']}.png",
                                ),
                            )
                        )
                except Exception as exc:
                    log_excerpt = ""
                    if log_path.exists():
                        log_excerpt = log_path.read_text(encoding="utf-8", errors="replace")[-2000:]
                    stderr_excerpt = ""
                    if isinstance(exc, subprocess.CalledProcessError):
                        stderr_excerpt = exc.stderr or ""
                    raise AssertionError(
                        f"frontend browser e2e failed.\nplaywright stderr:\n{stderr_excerpt}\nfrontend log tail:\n{log_excerpt}"
                    ) from exc
                finally:
                    self._terminate_process(frontend_process, log_path)

        return payloads

    def _run_scripted_runtime_identity_case(self) -> dict:
        helper = hosted_smoke_module.HostedWorkflowSmokeTest(
            methodName="test_default_explain_matrix_covers_reference_regressions"
        )
        frontend_port = self._reserve_free_port()
        frontend_url = f"http://127.0.0.1:{frontend_port}"
        log_path = self.repo_root / "runtime" / "service_logs" / f"frontend-e2e-scripted-{frontend_port}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        scenario_path = self.repo_root / "configs" / "scripted" / "physical_intelligence_full_roster.json"

        with helper._hosted_harness(provider_mode="scripted", scripted_scenario=str(scenario_path)) as harness:
            helper._write_runtime_identity(
                runtime_dir=harness.settings.runtime_dir,
                company_key="physicalintelligence",
                snapshot_id="20260415T010203",
                requested_name="Physical Intelligence",
                canonical_name="Physical Intelligence",
                linkedin_slug="physical-intelligence",
                aliases=["physical intelligence", "pi"],
            )
            harness.orchestrator.acquisition_engine.worker_runtime = None
            if hasattr(harness.orchestrator.acquisition_engine, "multi_source_enricher"):
                harness.orchestrator.acquisition_engine.multi_source_enricher.worker_runtime = None

            frontend_env = dict(self.playwright_env)
            frontend_env["VITE_API_BASE_URL"] = harness.client.base_url
            with log_path.open("w", encoding="utf-8") as log_handle:
                frontend_process = subprocess.Popen(
                    ["npm", "run", "dev", "--", "--host", "127.0.0.1", "--port", str(frontend_port)],
                    cwd=self.frontend_dir,
                    env=frontend_env,
                    stdout=log_handle,
                    stderr=subprocess.STDOUT,
                    text=True,
                )
                try:
                    self._wait_for_http_ready(frontend_url, timeout=45)
                    payload = self._run_browser_case(
                        frontend_url=frontend_url,
                        query=self.SCRIPTED_RUNTIME_IDENTITY_CASE["query"],
                        screenshot_path=self.repo_root
                        / "output"
                        / "playwright"
                        / f"frontend-browser-e2e-{self.SCRIPTED_RUNTIME_IDENTITY_CASE['name']}.png",
                        timeout_seconds=150,
                    )
                except Exception as exc:
                    log_excerpt = ""
                    if log_path.exists():
                        log_excerpt = log_path.read_text(encoding="utf-8", errors="replace")[-2000:]
                    stderr_excerpt = ""
                    if isinstance(exc, subprocess.CalledProcessError):
                        stderr_excerpt = exc.stderr or ""
                    raise AssertionError(
                        f"frontend scripted browser e2e failed.\nplaywright stderr:\n{stderr_excerpt}\nfrontend log tail:\n{log_excerpt}"
                    ) from exc
                finally:
                    self._terminate_process(frontend_process, log_path)

        return payload

    def _run_simulate_large_org_existing_baseline_case(self) -> dict:
        helper = hosted_smoke_module.HostedWorkflowSmokeTest(
            methodName="test_hosted_simulate_large_org_full_roster_existing_baseline_defaults_to_asset_population"
        )
        frontend_port = self._reserve_free_port()
        frontend_url = f"http://127.0.0.1:{frontend_port}"
        log_path = self.repo_root / "runtime" / "service_logs" / f"frontend-e2e-xai-baseline-{frontend_port}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)

        with unittest.mock.patch.dict(os.environ, {"SOURCING_TEST_CANDIDATE_PAGE_DELAY_MS": "3000"}, clear=False):
            with helper._hosted_harness(provider_mode="simulate") as harness:
                helper._seed_authoritative_company_asset(
                    harness=harness,
                    target_company="xAI",
                    snapshot_id="20260414T121000",
                    current_count=2600,
                    former_count=320,
                    candidate_specs=self._build_large_org_candidate_specs("xAI", current_count=2600, former_count=320),
                )

                frontend_env = dict(self.playwright_env)
                frontend_env["VITE_API_BASE_URL"] = harness.client.base_url
                with log_path.open("w", encoding="utf-8") as log_handle:
                    frontend_process = subprocess.Popen(
                        ["npm", "run", "dev", "--", "--host", "127.0.0.1", "--port", str(frontend_port)],
                        cwd=self.frontend_dir,
                        env=frontend_env,
                        stdout=log_handle,
                        stderr=subprocess.STDOUT,
                        text=True,
                    )
                    try:
                        self._wait_for_http_ready(frontend_url, timeout=45)
                        payload = self._run_browser_case(
                            frontend_url=frontend_url,
                            query=self.LARGE_ORG_EXISTING_BASELINE_CASE["query"],
                            screenshot_path=self.repo_root
                            / "output"
                            / "playwright"
                            / f"frontend-browser-e2e-{self.LARGE_ORG_EXISTING_BASELINE_CASE['name']}.png",
                            timeout_seconds=150,
                            check_pagination_stability=True,
                            pagination_hydration_timeout_seconds=30,
                        )
                    except Exception as exc:
                        log_excerpt = ""
                        if log_path.exists():
                            log_excerpt = log_path.read_text(encoding="utf-8", errors="replace")[-2000:]
                        stderr_excerpt = ""
                        if isinstance(exc, subprocess.CalledProcessError):
                            stderr_excerpt = exc.stderr or ""
                        raise AssertionError(
                            f"frontend large-org baseline browser e2e failed.\nplaywright stderr:\n{stderr_excerpt}\nfrontend log tail:\n{log_excerpt}"
                        ) from exc
                    finally:
                        self._terminate_process(frontend_process, log_path)

        return payload

    def _run_restored_history_large_org_hydration_case(self) -> dict:
        helper = hosted_smoke_module.HostedWorkflowSmokeTest(
            methodName="test_hosted_simulate_completed_history_round_trip_exposes_results_recovery"
        )
        frontend_port = self._reserve_free_port()
        frontend_url = f"http://127.0.0.1:{frontend_port}"
        log_path = self.repo_root / "runtime" / "service_logs" / f"frontend-e2e-xai-history-hydration-{frontend_port}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        history_id = "browser-e2e-xai-history-hydration"
        payload: dict | None = None

        with unittest.mock.patch.dict(os.environ, {"SOURCING_TEST_CANDIDATE_PAGE_DELAY_MS": "350"}, clear=False):
            with helper._hosted_harness(provider_mode="simulate") as harness:
                helper._seed_authoritative_company_asset(
                    harness=harness,
                    target_company="xAI",
                    snapshot_id="20260414T121000",
                    current_count=2600,
                    former_count=320,
                    candidate_specs=self._build_large_org_candidate_specs("xAI", current_count=2600, former_count=320),
                )
                smoke_record = hosted_smoke_module.run_hosted_smoke_case(
                    client=harness.client,
                    case_name="browser_e2e_xai_history_hydration",
                    payload={
                        "raw_user_request": "给我 xAI 的所有成员",
                        "planning_mode": "model_assisted",
                        "top_k": 30,
                        "slug_resolution_limit": 20,
                        "profile_detail_limit": 20,
                        "publication_scan_limit": 20,
                        "publication_lead_limit": 30,
                        "exploration_limit": 20,
                        "semantic_rerank_limit": 30,
                        "force_fresh_run": True,
                        "history_id": history_id,
                    },
                    reviewer="frontend-browser-history-hydration",
                    poll_seconds=0.1,
                    max_poll_seconds=60.0,
                    runtime_tuning_profile="fast_smoke",
                )
                job_id = str((smoke_record.get("start") or {}).get("job_id") or "")
                if str((smoke_record.get("final") or {}).get("job_status") or "") != "completed" or not job_id:
                    raise AssertionError(smoke_record)

                frontend_env = dict(self.playwright_env)
                frontend_env["VITE_API_BASE_URL"] = harness.client.base_url
                with log_path.open("w", encoding="utf-8") as log_handle:
                    frontend_process = subprocess.Popen(
                        ["npm", "run", "dev", "--", "--host", "127.0.0.1", "--port", str(frontend_port)],
                        cwd=self.frontend_dir,
                        env=frontend_env,
                        stdout=log_handle,
                        stderr=subprocess.STDOUT,
                        text=True,
                    )
                    try:
                        self._wait_for_http_ready(frontend_url, timeout=45)
                        payload = self._run_browser_case(
                            frontend_url=frontend_url,
                            start_url=f"{frontend_url}/?history={history_id}&job={job_id}",
                            restore_existing_results=True,
                            query="",
                            screenshot_path=self.repo_root
                            / "output"
                            / "playwright"
                            / "frontend-browser-e2e-xai-history-hydration.png",
                            timeout_seconds=120,
                            check_pagination_stability=True,
                            pagination_hydration_timeout_seconds=30,
                        )
                    except Exception as exc:
                        log_excerpt = ""
                        if log_path.exists():
                            log_excerpt = log_path.read_text(encoding="utf-8", errors="replace")[-2000:]
                        stderr_excerpt = ""
                        if isinstance(exc, subprocess.CalledProcessError):
                            stderr_excerpt = exc.stderr or ""
                        raise AssertionError(
                            f"frontend restored-history browser e2e failed.\nplaywright stderr:\n{stderr_excerpt}\nfrontend log tail:\n{log_excerpt}"
                        ) from exc
                    finally:
                        self._terminate_process(frontend_process, log_path)

        assert payload is not None
        return payload

    def _run_scripted_large_org_profile_tail_reuse_case(self) -> tuple[dict, dict]:
        helper = hosted_smoke_module.HostedWorkflowSmokeTest(
            methodName="test_hosted_scripted_large_org_profile_tail_completed_snapshot_skips_repeat_prefetch"
        )
        frontend_port = self._reserve_free_port()
        frontend_url = f"http://127.0.0.1:{frontend_port}"
        log_path = self.repo_root / "runtime" / "service_logs" / f"frontend-e2e-scripted-tail-{frontend_port}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        scenario_path = self.repo_root / "configs" / "scripted" / "large_org_profile_tail_reconcile.json"

        with helper._hosted_harness(provider_mode="scripted", scripted_scenario=str(scenario_path)) as harness:
            first_record = hosted_smoke_module.run_hosted_smoke_case(
                client=harness.client,
                case_name="browser_large_org_profile_tail_first_pass",
                payload={
                    "raw_user_request": "给我 xAI 的所有成员",
                    "planning_mode": "model_assisted",
                    "top_k": 30,
                    "slug_resolution_limit": 20,
                    "profile_detail_limit": 20,
                    "publication_scan_limit": 20,
                    "publication_lead_limit": 30,
                    "exploration_limit": 20,
                    "semantic_rerank_limit": 30,
                    "force_fresh_run": True,
                },
                reviewer="frontend-browser-scripted-large-org-profile-tail-first",
                poll_seconds=0.1,
                max_poll_seconds=60.0,
            )
            frontend_env = dict(self.playwright_env)
            frontend_env["VITE_API_BASE_URL"] = harness.client.base_url
            with log_path.open("w", encoding="utf-8") as log_handle:
                frontend_process = subprocess.Popen(
                    ["npm", "run", "dev", "--", "--host", "127.0.0.1", "--port", str(frontend_port)],
                    cwd=self.frontend_dir,
                    env=frontend_env,
                    stdout=log_handle,
                    stderr=subprocess.STDOUT,
                    text=True,
                )
                try:
                    self._wait_for_http_ready(frontend_url, timeout=45)
                    second_payload = self._run_browser_case(
                        frontend_url=frontend_url,
                        query="给我 xAI 的所有成员",
                        screenshot_path=self.repo_root / "output" / "playwright" / "frontend-browser-e2e-large-org-tail-second.png",
                        timeout_seconds=120,
                    )
                except Exception as exc:
                    log_excerpt = ""
                    if log_path.exists():
                        log_excerpt = log_path.read_text(encoding="utf-8", errors="replace")[-2000:]
                    stderr_excerpt = ""
                    if isinstance(exc, subprocess.CalledProcessError):
                        stderr_excerpt = exc.stderr or ""
                    raise AssertionError(
                        f"frontend large-org profile-tail browser e2e failed.\nplaywright stderr:\n{stderr_excerpt}\nfrontend log tail:\n{log_excerpt}"
                    ) from exc
                finally:
                    self._terminate_process(frontend_process, log_path)

        return first_record, second_payload


class FrontendBrowserFastE2ETest(_FrontendBrowserE2EBase):
    def test_browser_workflow_e2e_fast_simulate_matrix(self) -> None:
        for case, payload in self._run_seeded_hosted_cases(self.FAST_SIMULATE_CASES):
            with self.subTest(case=case["name"]):
                self._assert_case_payload(case, payload)

    def test_browser_workflow_e2e_covers_runtime_identity_new_org_full_roster(self) -> None:
        payload = self._run_scripted_runtime_identity_case()
        self._assert_case_payload(self.SCRIPTED_RUNTIME_IDENTITY_CASE, payload)

    def test_browser_workflow_e2e_covers_large_org_existing_baseline_asset_population(self) -> None:
        payload = self._run_simulate_large_org_existing_baseline_case()
        self._assert_case_payload(self.LARGE_ORG_EXISTING_BASELINE_CASE, payload)
        self._assert_pagination_stability_payload(payload)

    def test_browser_excel_intake_upload_splits_companies_via_same_origin(self) -> None:
        helper = hosted_smoke_module.HostedWorkflowSmokeTest(
            methodName="test_default_smoke_matrix_covers_reference_orgs"
        )
        frontend_port = self._reserve_free_port()
        frontend_url = f"http://127.0.0.1:{frontend_port}"
        log_path = self.repo_root / "runtime" / "service_logs" / f"frontend-e2e-excel-intake-{frontend_port}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)

        with helper._hosted_harness(provider_mode="simulate") as harness:
            harness.store.upsert_candidate(
                Candidate(
                    candidate_id="excel-openai-ada",
                    name_en="Ada Import",
                    display_name="Ada Import",
                    category="employee",
                    target_company="OpenAI",
                    organization="OpenAI",
                    employment_status="current",
                    role="Research Engineer",
                    linkedin_url="https://www.linkedin.com/in/ada-import/",
                )
            )
            harness.store.upsert_candidate(
                Candidate(
                    candidate_id="excel-anthropic-chris",
                    name_en="Chris Import",
                    display_name="Chris Import",
                    category="employee",
                    target_company="Anthropic",
                    organization="Anthropic",
                    employment_status="current",
                    role="Research Engineer",
                    linkedin_url="https://www.linkedin.com/in/chris-import/",
                )
            )
            workbook_path = Path(harness.tempdir.name) / "excel-intake-browser.xlsx"
            _write_inline_workbook(
                workbook_path,
                sheet_name="Contacts",
                headers=["Name", "Company", "Title", "Linkedin"],
                rows=[
                    ["Ada Import", "OpenAI", "Research Engineer", "https://www.linkedin.com/in/ada-import/"],
                    ["Chris Import", "Anthropic", "Research Engineer", "https://www.linkedin.com/in/chris-import/"],
                ],
            )

            frontend_env = dict(self.playwright_env)
            frontend_env["VITE_API_BASE_URL"] = "same-origin"
            frontend_env["VITE_DEV_PROXY_TARGET"] = harness.client.base_url
            frontend_env["VITE_ENABLE_EXCEL_INTAKE_WORKFLOW"] = "true"
            with log_path.open("w", encoding="utf-8") as log_handle:
                frontend_process = subprocess.Popen(
                    ["npm", "run", "dev", "--", "--host", "127.0.0.1", "--port", str(frontend_port)],
                    cwd=self.frontend_dir,
                    env=frontend_env,
                    stdout=log_handle,
                    stderr=subprocess.STDOUT,
                    text=True,
                )
                try:
                    self._wait_for_http_ready(frontend_url, timeout=45)
                    payload = self._run_excel_intake_browser_case(
                        frontend_url=frontend_url,
                        workbook_path=workbook_path,
                        screenshot_path=self.repo_root / "output" / "playwright" / "frontend-browser-e2e-excel-intake.png",
                        timeout_seconds=90,
                    )
                except Exception as exc:
                    log_excerpt = ""
                    if log_path.exists():
                        log_excerpt = log_path.read_text(encoding="utf-8", errors="replace")[-2000:]
                    stderr_excerpt = ""
                    if isinstance(exc, subprocess.CalledProcessError):
                        stderr_excerpt = exc.stderr or ""
                    raise AssertionError(
                        f"frontend Excel intake browser e2e failed.\nplaywright stderr:\n{stderr_excerpt}\nfrontend log tail:\n{log_excerpt}"
                    ) from exc
                finally:
                    self._terminate_process(frontend_process, log_path)

        self.assertEqual(payload.get("status"), "ok", payload)
        self.assertEqual(int(payload.get("groupCount") or 0), 2, payload)
        combined_groups = "\n".join(str(item) for item in list(payload.get("groups") or []))
        self.assertIn("OpenAI", combined_groups)
        self.assertIn("Anthropic", combined_groups)
        self.assertFalse(str(payload.get("errorText") or "").strip(), payload)

    def test_browser_target_candidate_public_web_selection_trigger_and_polling(self) -> None:
        helper = hosted_smoke_module.HostedWorkflowSmokeTest(
            methodName="test_default_smoke_matrix_covers_reference_orgs"
        )
        frontend_port = self._reserve_free_port()
        frontend_url = f"http://127.0.0.1:{frontend_port}"
        log_path = self.repo_root / "runtime" / "service_logs" / f"frontend-e2e-target-public-web-{frontend_port}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)

        with helper._hosted_harness(provider_mode="simulate") as harness:
            harness.store.upsert_target_candidate(
                {
                    "record_id": "browser-public-web-target-1",
                    "candidate_id": "browser-public-web-candidate-1",
                    "candidate_name": "Alice Public Web",
                    "headline": "Research Engineer",
                    "current_company": "Example AI",
                    "linkedin_url": "https://www.linkedin.com/in/alice-public-web/",
                }
            )
            frontend_env = dict(self.playwright_env)
            frontend_env["VITE_API_BASE_URL"] = harness.client.base_url
            with log_path.open("w", encoding="utf-8") as log_handle:
                frontend_process = subprocess.Popen(
                    ["npm", "run", "dev", "--", "--host", "127.0.0.1", "--port", str(frontend_port)],
                    cwd=self.frontend_dir,
                    env=frontend_env,
                    stdout=log_handle,
                    stderr=subprocess.STDOUT,
                    text=True,
                )
                try:
                    self._wait_for_http_ready(frontend_url, timeout=45)
                    payload = self._run_target_public_web_browser_case(
                        frontend_url=frontend_url,
                        screenshot_path=self.repo_root
                        / "output"
                        / "playwright"
                        / "frontend-browser-e2e-target-public-web.png",
                        timeout_seconds=90,
                    )
                except Exception as exc:
                    log_excerpt = ""
                    if log_path.exists():
                        log_excerpt = log_path.read_text(encoding="utf-8", errors="replace")[-2000:]
                    stderr_excerpt = ""
                    if isinstance(exc, subprocess.CalledProcessError):
                        stderr_excerpt = exc.stderr or ""
                    raise AssertionError(
                        f"frontend target Public Web browser e2e failed.\nplaywright stderr:\n{stderr_excerpt}\nfrontend log tail:\n{log_excerpt}"
                    ) from exc
                finally:
                    self._terminate_process(frontend_process, log_path)

        self.assertEqual(payload.get("status"), "ok", payload)
        self.assertGreaterEqual(int(payload.get("initialCandidateCount") or 0), 1, payload)
        self.assertTrue(payload.get("sawQueuedOrRunningStatus"), payload)
        self.assertIn("Web Search", str(payload.get("exportButtonText") or ""), payload)

    def test_browser_target_candidate_public_web_promotion_and_export(self) -> None:
        helper = hosted_smoke_module.HostedWorkflowSmokeTest(
            methodName="test_default_smoke_matrix_covers_reference_orgs"
        )
        frontend_port = self._reserve_free_port()
        frontend_url = f"http://127.0.0.1:{frontend_port}"
        log_path = (
            self.repo_root
            / "runtime"
            / "service_logs"
            / f"frontend-e2e-target-public-web-promotion-export-{frontend_port}.log"
        )
        log_path.parent.mkdir(parents=True, exist_ok=True)
        expected_email = "public.web.browser@example.edu"

        with helper._hosted_harness(provider_mode="simulate") as harness:
            record = harness.store.upsert_target_candidate(
                {
                    "record_id": "browser-public-web-promotion-target-1",
                    "candidate_id": "browser-public-web-promotion-candidate-1",
                    "candidate_name": "Alice Public Web Promotion",
                    "headline": "Research Engineer",
                    "current_company": "Example AI",
                    "linkedin_url": "https://www.linkedin.com/in/alice-public-web-promotion/",
                }
            )
            run = harness.store.upsert_target_candidate_public_web_run(
                {
                    "run_id": "browser-public-web-promotion-run-1",
                    "batch_id": "browser-public-web-promotion-batch-1",
                    "record_id": record["id"],
                    "candidate_id": record["candidate_id"],
                    "candidate_name": record["candidate_name"],
                    "current_company": record["current_company"],
                    "linkedin_url": record["linkedin_url"],
                    "linkedin_url_key": "alice-public-web-promotion",
                    "person_identity_key": "linkedin:alice-public-web-promotion",
                    "idempotency_key": "browser-public-web-promotion-run-key-1",
                    "status": "completed",
                    "phase": "completed",
                    "summary": {
                        "entry_link_count": 2,
                        "fetched_document_count": 2,
                        "email_candidate_count": 1,
                        "promotion_recommended_email_count": 1,
                    },
                    "analysis_checkpoint": {"stage": "completed", "status": "completed"},
                }
            )
            asset = harness.store.upsert_person_public_web_asset(
                {
                    "asset_id": "browser-public-web-promotion-asset-1",
                    "person_identity_key": "linkedin:alice-public-web-promotion",
                    "linkedin_url_key": "alice-public-web-promotion",
                    "latest_run_id": run["run_id"],
                    "target_candidate_record_id": record["id"],
                    "candidate_name": record["candidate_name"],
                    "current_company": record["current_company"],
                    "status": "completed",
                    "summary": {"email_candidate_count": 1, "profile_link_count": 1},
                    "source_run_ids": [run["run_id"]],
                }
            )
            harness.store.replace_person_public_web_signals_for_run(
                run_id=run["run_id"],
                signals=[
                    {
                        "signal_id": "browser-public-web-promotion-email-1",
                        "run_id": run["run_id"],
                        "asset_id": asset["asset_id"],
                        "person_identity_key": "linkedin:alice-public-web-promotion",
                        "record_id": record["id"],
                        "candidate_id": record["candidate_id"],
                        "candidate_name": record["candidate_name"],
                        "current_company": record["current_company"],
                        "linkedin_url_key": "alice-public-web-promotion",
                        "signal_kind": "email_candidate",
                        "signal_type": "academic",
                        "email_type": "academic",
                        "value": expected_email,
                        "normalized_value": expected_email,
                        "source_url": "https://alice-public-web-promotion.example.edu/",
                        "source_domain": "alice-public-web-promotion.example.edu",
                        "source_family": "profile_web_presence",
                        "confidence_label": "high",
                        "confidence_score": 0.94,
                        "identity_match_label": "likely_same_person",
                        "identity_match_score": 0.92,
                        "publishable": True,
                        "promotion_status": "promotion_recommended",
                        "evidence_excerpt": f"Contact Alice at {expected_email}",
                    },
                    {
                        "signal_id": "browser-public-web-promotion-github-1",
                        "run_id": run["run_id"],
                        "asset_id": asset["asset_id"],
                        "person_identity_key": "linkedin:alice-public-web-promotion",
                        "record_id": record["id"],
                        "candidate_id": record["candidate_id"],
                        "candidate_name": record["candidate_name"],
                        "current_company": record["current_company"],
                        "linkedin_url_key": "alice-public-web-promotion",
                        "signal_kind": "profile_link",
                        "signal_type": "github_url",
                        "value": "https://github.com/alice-public-web-promotion",
                        "normalized_value": "https://github.com/alice-public-web-promotion",
                        "url": "https://github.com/alice-public-web-promotion",
                        "source_url": "https://github.com/alice-public-web-promotion",
                        "source_domain": "github.com",
                        "source_family": "technical_presence",
                        "confidence_label": "high",
                        "confidence_score": 0.9,
                        "identity_match_label": "likely_same_person",
                        "identity_match_score": 0.88,
                        "publishable": True,
                        "metadata": {"clean_profile_link": True, "link_shape_warnings": []},
                    },
                ],
            )
            frontend_env = dict(self.playwright_env)
            frontend_env["VITE_API_BASE_URL"] = harness.client.base_url
            with log_path.open("w", encoding="utf-8") as log_handle:
                frontend_process = subprocess.Popen(
                    ["npm", "run", "dev", "--", "--host", "127.0.0.1", "--port", str(frontend_port)],
                    cwd=self.frontend_dir,
                    env=frontend_env,
                    stdout=log_handle,
                    stderr=subprocess.STDOUT,
                    text=True,
                )
                try:
                    self._wait_for_http_ready(frontend_url, timeout=45)
                    payload = self._run_target_public_web_promotion_export_browser_case(
                        frontend_url=frontend_url,
                        screenshot_path=self.repo_root
                        / "output"
                        / "playwright"
                        / "frontend-browser-e2e-target-public-web-promotion-export.png",
                        download_path=self.repo_root
                        / "output"
                        / "playwright"
                        / "frontend-browser-e2e-target-public-web-promoted-export.zip",
                        expected_email=expected_email,
                        timeout_seconds=90,
                    )
                except Exception as exc:
                    log_excerpt = ""
                    if log_path.exists():
                        log_excerpt = log_path.read_text(encoding="utf-8", errors="replace")[-2000:]
                    stderr_excerpt = ""
                    if isinstance(exc, subprocess.CalledProcessError):
                        stderr_excerpt = exc.stderr or ""
                    raise AssertionError(
                        "frontend target Public Web promotion/export browser e2e failed.\n"
                        f"playwright stderr:\n{stderr_excerpt}\nfrontend log tail:\n{log_excerpt}"
                    ) from exc
                finally:
                    self._terminate_process(frontend_process, log_path)

            updated_record = harness.store.get_target_candidate(record["id"])
            promotions = harness.store.list_target_candidate_public_web_promotions(record_id=record["id"])

        self.assertEqual(payload.get("status"), "ok", payload)
        self.assertTrue(payload.get("promotedEmailVisible"), payload)
        self.assertTrue(payload.get("promotedLinkVisible"), payload)
        self.assertGreater(int(payload.get("downloadedSize") or 0), 0, payload)
        download_path = Path(str(payload.get("downloadPath") or ""))
        self.assertTrue(zipfile.is_zipfile(download_path), payload)
        with zipfile.ZipFile(download_path, "r") as archive:
            archive_text = "\n".join(
                archive.read(name).decode("utf-8", errors="ignore")
                for name in archive.namelist()
                if name.endswith((".csv", ".json"))
            )
        self.assertIn(expected_email, archive_text)
        self.assertIn("https://github.com/alice-public-web-promotion", archive_text)
        self.assertNotIn("raw_path", archive_text)
        self.assertNotIn("raw_payload", archive_text)
        self.assertEqual((updated_record or {}).get("primary_email"), expected_email)
        self.assertEqual(len(promotions), 2)
        self.assertEqual(
            {promotion.get("promotion_status") for promotion in promotions},
            {"manually_promoted"},
        )

    def test_browser_results_recovery_hydration_preserves_second_page(self) -> None:
        payload = self._run_restored_history_large_org_hydration_case()
        self.assertEqual(payload.get("status"), "ok", payload)
        self._assert_pagination_stability_payload(payload)


class FrontendBrowserSlowE2ETest(_FrontendBrowserE2EBase):
    def test_browser_workflow_e2e_slow_large_org_matrix_when_enabled(self) -> None:
        enabled = _env_flag("SOURCING_RUN_SLOW_BROWSER_E2E") or _env_flag("SOURCING_RUN_FULL_BROWSER_E2E_MATRIX")
        if not enabled:
            self.skipTest(
                "set SOURCING_RUN_SLOW_BROWSER_E2E=1 to run the large-org browser e2e matrix"
            )
        for case in self.SLOW_SIMULATE_CASES:
            with self.subTest(case=case["name"]):
                payloads = self._run_seeded_hosted_cases([case])
                self.assertEqual(len(payloads), 1)
                returned_case, payload = payloads[0]
                self._assert_case_payload(returned_case, payload)

    def test_browser_workflow_e2e_large_org_profile_tail_second_run_reuses_completed_job(self) -> None:
        enabled = _env_flag("SOURCING_RUN_SLOW_BROWSER_E2E") or _env_flag("SOURCING_RUN_FULL_BROWSER_E2E_MATRIX")
        if not enabled:
            self.skipTest(
                "set SOURCING_RUN_SLOW_BROWSER_E2E=1 to run the large-org browser e2e matrix"
            )
        first_record, second_payload = self._run_scripted_large_org_profile_tail_reuse_case()
        self.assertEqual((first_record.get("final") or {}).get("job_status"), "completed")
        self.assertEqual(str((first_record.get("explain") or {}).get("dispatch_strategy") or ""), "new_job")
        self.assertEqual(second_payload["status"], "ok")
        self.assertEqual(second_payload["plan"]["targetCompany"], "xAI")
        self.assertEqual(second_payload["plan"]["metadata"].get("dispatchStrategy"), "复用历史完成结果")
        self.assertEqual(second_payload["plan"]["metadata"].get("currentLaneBehavior"), "复用本地 baseline")
        self.assertEqual(second_payload["plan"]["metadata"].get("formerLaneBehavior"), "只补缺口增量")
        self.assertGreater(second_payload["results"]["initialCount"], 0)
        self.assertGreater(second_payload["results"]["reloadedCount"], 0)
        self.assertIn("history=", second_payload["historyUrl"])


if __name__ == "__main__":
    unittest.main()
