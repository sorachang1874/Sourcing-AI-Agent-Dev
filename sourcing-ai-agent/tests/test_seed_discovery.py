import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

import sourcing_agent.seed_discovery as seed_discovery_module
from sourcing_agent.asset_logger import AssetLogger
from sourcing_agent.durable_runtime import legacy_job_workflow_run_id
from tests.pg_store_fixture import pg_backed_control_plane_store
from sourcing_agent.connectors import CompanyIdentity, RapidApiAccount, resolve_company_identity
from sourcing_agent.runtime_environment import LiveProviderAccessError
from sourcing_agent.search_provider import (
    SearchBatchFetchResult,
    SearchBatchFetchTask,
    SearchBatchReadyResult,
    SearchBatchReadyTask,
    SearchBatchSubmissionResult,
    SearchBatchSubmissionTask,
    SearchExecutionArtifact,
    SearchExecutionResult,
    SearchResponse,
    SearchResultItem,
    search_response_to_record,
)
from sourcing_agent.search_seed_registry import project_search_seed_snapshot_to_candidate_documents
from sourcing_agent.seed_discovery import (
    SearchSeedAcquirer,
    SearchSeedSnapshot,
    _lead_entries_from_public_result,
    _normalize_harvest_company_filters,
    _normalize_harvest_query_text,
    _provider_query_family_key,
    _resolve_provider_people_search_queries,
    _search_query_signature,
    _search_seed_worker_key,
    collect_search_seed_provider_retry_items,
    extract_linkedin_slug,
    extract_web_search_results,
    infer_name_from_result_title,
    infer_public_names_from_result_title,
)


class SeedDiscoveryTest(unittest.TestCase):
    def setUp(self) -> None:
        # Provider-mode default is now fail-closed (simulate). The live seed-discovery
        # dispatch tests here run with the network mocked, so opt into live + the
        # non-production dual-confirm. Non-live tests override the mode themselves.
        _live = mock.patch.dict(
            os.environ,
            {
                "SOURCING_EXTERNAL_PROVIDER_MODE": "live",
                "SOURCING_LIVE_PROVIDER_CONFIRM": "1",
                "SOURCING_ALLOW_ISOLATED_LIVE_PROVIDER_ACCESS": "1",
            },
            clear=False,
        )
        _live.start()
        self.addCleanup(_live.stop)

    def _write_seed_catalog(
        self,
        *,
        runtime_dir: Path,
        records: list[dict[str, object]],
    ) -> None:
        seed_path = runtime_dir / "company_identity_seed_catalog.json"
        seed_path.parent.mkdir(parents=True, exist_ok=True)
        seed_path.write_text(
            json.dumps(
                {
                    "updated_at": "2026-04-15T00:00:00+00:00",
                    "company_count": len(records),
                    "alias_count": 0,
                    "records": records,
                    "alias_index": {},
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

    def test_runtime_seed_discovery_poll_intervals_follow_env_without_reload(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "WEB_SEARCH_READY_POLL_MIN_INTERVAL_SECONDS": "0",
                "WEB_SEARCH_FETCH_MIN_INTERVAL_SECONDS": "0",
            },
            clear=False,
        ):
            self.assertEqual(seed_discovery_module._lane_ready_poll_min_interval_seconds(), 0)
            self.assertEqual(seed_discovery_module._lane_fetch_min_interval_seconds(), 0)

    def test_search_seed_discovery_query_item_normalizes_summary_to_query_summary(self) -> None:
        class FakeStore:
            def __init__(self) -> None:
                self.row: dict[str, object] = {}

            def upsert_job_materialization_item(self, **kwargs: object) -> dict[str, object]:
                self.row = dict(kwargs)
                return self.row

        class FakeRuntime:
            def __init__(self) -> None:
                self.store = FakeStore()

        runtime = FakeRuntime()
        summary = {
            "query": "Infra",
            "mode": "harvest_profile_search",
            "status": "completed",
            "zero_result_accepted": True,
        }

        row = seed_discovery_module._record_search_seed_discovery_query_item(
            worker_runtime=runtime,
            item_id="jdisc_test",
            job_id="job-test",
            identity=CompanyIdentity(canonical_name="OpenAI", requested_name="OpenAI", company_key="openai"),
            snapshot_id="20260504T000000",
            index=0,
            query_spec={"query": "Infra", "bundle_id": "infra"},
            employment_status="current",
            status="completed",
            phase="completed",
            reason="provider_result_persisted",
            metadata={"summary": summary},
        )

        metadata = dict(row.get("metadata") or {})
        self.assertEqual(metadata["summary"], summary)
        self.assertEqual(metadata["query_summary"], summary)

    def test_search_seed_candidate_document_projection_preserves_existing_profile_detail(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir) / "company_assets" / "openai" / "snapshot-search-seed"
            discovery_dir = snapshot_dir / "search_seed_discovery"
            discovery_dir.mkdir(parents=True, exist_ok=True)
            identity = CompanyIdentity(
                requested_name="OpenAI",
                canonical_name="OpenAI",
                company_key="openai",
                linkedin_slug="openai",
            )
            snapshot = SearchSeedSnapshot(
                snapshot_id=snapshot_dir.name,
                target_company="OpenAI",
                company_identity=identity,
                snapshot_dir=snapshot_dir,
                entries=[
                    {
                        "seed_key": "infra-builder",
                        "full_name": "Infra Builder",
                        "headline": "Seed Shell Role",
                        "source_type": "harvest_profile_search",
                        "source_query": "Agent",
                        "profile_url": "https://www.linkedin.com/in/infra-builder/",
                    }
                ],
                query_summaries=[{"query": "Agent", "status": "completed"}],
                accounts_used=["harvest_profile_search"],
                errors=[],
                stop_reason="provider_people_search_primary",
                summary_path=discovery_dir / "summary.json",
                entries_path=discovery_dir / "entries.json",
            )

            first = project_search_seed_snapshot_to_candidate_documents(snapshot)
            self.assertEqual(first["status"], "completed")
            candidate_doc_path = snapshot_dir / "candidate_documents.json"
            payload = json.loads(candidate_doc_path.read_text(encoding="utf-8"))
            payload["candidates"][0]["role"] = "Rich Profile Role"
            payload["candidates"][0]["focus_areas"] = "Profile detail from provider"
            payload["candidates"][0].setdefault("metadata", {})["skills"] = ["Kubernetes"]
            candidate_doc_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

            second = project_search_seed_snapshot_to_candidate_documents(snapshot)

            self.assertEqual(second["status"], "completed")
            refreshed = json.loads(candidate_doc_path.read_text(encoding="utf-8"))
            self.assertEqual(refreshed["candidate_count"], 1)
            candidate = refreshed["candidates"][0]
            self.assertEqual(candidate["role"], "Rich Profile Role")
            self.assertEqual(candidate["focus_areas"], "Profile detail from provider")
            self.assertEqual(candidate["metadata"]["skills"], ["Kubernetes"])
            self.assertEqual(
                refreshed["search_seed_candidate_documents_projection"]["search_seed_candidate_count"],
                1,
            )

    def test_runtime_seed_discovery_poll_intervals_prefer_seed_specific_env(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "WEB_SEARCH_READY_POLL_MIN_INTERVAL_SECONDS": "9",
                "WEB_SEARCH_FETCH_MIN_INTERVAL_SECONDS": "11",
                "SEED_DISCOVERY_READY_POLL_MIN_INTERVAL_SECONDS": "2",
                "SEED_DISCOVERY_FETCH_MIN_INTERVAL_SECONDS": "3",
            },
            clear=False,
        ):
            self.assertEqual(seed_discovery_module._lane_ready_poll_min_interval_seconds(), 2)
            self.assertEqual(seed_discovery_module._lane_fetch_min_interval_seconds(), 3)

    def test_extract_web_search_results_and_slug(self) -> None:
        html = """
        <a class="result__a" href="https://duckduckgo.com/l/?uddg=https%3A%2F%2Fwww.linkedin.com%2Fin%2Fyuntao-bai%2F">
        Yuntao Bai - Anthropic - LinkedIn
        </a>
        """
        results = extract_web_search_results(html)
        self.assertEqual(len(results), 1)
        self.assertEqual(extract_linkedin_slug(results[0]["url"]), "yuntao-bai")

    def test_infer_name_from_result_title(self) -> None:
        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropicresearch",
            aliases=["anthropicresearch"],
        )
        self.assertEqual(infer_name_from_result_title("Yuntao Bai - Anthropic - LinkedIn", identity), "Yuntao Bai")

    def test_infer_public_names_from_result_title(self) -> None:
        identity = CompanyIdentity(
            requested_name="Google",
            canonical_name="Google",
            company_key="google",
            linkedin_slug="google",
            aliases=["Google DeepMind", "Gemini"],
        )
        names = infer_public_names_from_result_title(
            "Interview with Logan Kilpatrick and Tulsee Doshi on Gemini research roadmap",
            identity,
        )
        self.assertIn("Logan Kilpatrick", names)
        self.assertIn("Tulsee Doshi", names)

    def test_public_media_lead_guard_rejects_non_person_titles(self) -> None:
        identity = CompanyIdentity(
            requested_name="Humans&",
            canonical_name="Humans&",
            company_key="humans",
            linkedin_slug="humans",
            aliases=["humansand"],
        )
        entries = _lead_entries_from_public_result(
            {
                "title": "Nature Biomedical",
                "url": "https://example.com/article",
            },
            identity,
            "humansand contributor",
            "current",
            analysis={"target_company_relation": "explicit", "confidence_label": "high"},
            source_family="publication_and_blog",
        )
        self.assertEqual(entries, [])

    def test_resolve_company_identity_prefers_builtin_mapping_over_model_assisted_observed_candidates(self) -> None:
        class _FakeModelClient:
            def judge_company_equivalence(self, payload):
                self.payload = payload
                return {
                    "decision": "same_company",
                    "matched_label": "Humans And AI",
                    "confidence_label": "high",
                    "rationale": "Observed LinkedIn company candidate matches the requested organization naming.",
                }

        client = _FakeModelClient()
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_dir = Path(temp_dir) / "runtime"
            self._write_seed_catalog(
                runtime_dir=runtime_dir,
                records=[
                    {
                        "company_key": "humansand",
                        "canonical_name": "Humans&",
                        "linkedin_slug": "humansand",
                        "aliases": ["humans and", "humansand"],
                        "confidence": "high",
                    }
                ],
            )
            with mock.patch.dict(os.environ, {"SOURCING_COMPANY_REGISTRY_RUNTIME_DIR": str(runtime_dir)}):
                identity = resolve_company_identity(
                    "Humans&",
                    model_client=client,
                    observed_companies=[
                        {
                            "label": "Humans And AI",
                            "linkedin_slug": "humansand",
                            "linkedin_company_url": "https://www.linkedin.com/company/humansand/",
                        }
                    ],
                )
        self.assertEqual(identity.linkedin_slug, "humansand")
        self.assertEqual(identity.linkedin_company_url, "https://www.linkedin.com/company/humansand/")
        self.assertEqual(identity.company_key, "humansand")
        self.assertEqual(identity.resolver, "seed_catalog")
        self.assertEqual(identity.confidence, "high")
        self.assertFalse(hasattr(client, "payload"))

    def test_resolve_company_identity_prefers_builtin_mapping_over_observed_exact_match_without_model_client(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_dir = Path(temp_dir) / "runtime"
            self._write_seed_catalog(
                runtime_dir=runtime_dir,
                records=[
                    {
                        "company_key": "humansand",
                        "canonical_name": "Humans&",
                        "linkedin_slug": "humansand",
                        "aliases": ["humans and", "humansand"],
                        "confidence": "high",
                    }
                ],
            )
            with mock.patch.dict(os.environ, {"SOURCING_COMPANY_REGISTRY_RUNTIME_DIR": str(runtime_dir)}):
                identity = resolve_company_identity(
                    "Humans&",
                    observed_companies=[
                        {
                            "label": "Humans&",
                            "linkedin_slug": "humansand",
                            "linkedin_company_url": "https://www.linkedin.com/company/humansand/",
                        }
                    ],
                )
        self.assertEqual(identity.linkedin_slug, "humansand")
        self.assertEqual(identity.linkedin_company_url, "https://www.linkedin.com/company/humansand/")
        self.assertEqual(identity.company_key, "humansand")
        self.assertEqual(identity.resolver, "seed_catalog")
        self.assertEqual(identity.confidence, "high")

    def test_resolve_company_identity_prefers_alias_mapped_linkedin_slug_for_company_key(self) -> None:
        identity = resolve_company_identity(
            "Thinking Machines Lab",
            observed_companies=[
                {
                    "label": "Thinking Machines Lab",
                    "linkedin_slug": "thinkingmachinesai",
                    "linkedin_company_url": "https://www.linkedin.com/company/thinkingmachinesai/",
                }
            ],
        )
        self.assertEqual(identity.linkedin_slug, "thinkingmachinesai")
        self.assertEqual(identity.company_key, "thinkingmachineslab")

    def test_resolve_company_identity_uses_bundled_seed_for_safe_superintelligence_inc(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_dir = Path(temp_dir) / "runtime"
            with mock.patch.dict(os.environ, {"SOURCING_COMPANY_REGISTRY_RUNTIME_DIR": str(runtime_dir)}):
                identity = resolve_company_identity("Safe Superintelligence Inc")

        self.assertEqual(identity.company_key, "safesuperintelligence")
        self.assertEqual(identity.canonical_name, "Safe Superintelligence")
        self.assertEqual(identity.linkedin_slug, "ssi-ai")
        self.assertEqual(identity.linkedin_company_url, "https://www.linkedin.com/company/ssi-ai/")
        self.assertEqual(identity.resolver, "seed_catalog")
        self.assertEqual(identity.confidence, "high")

    def test_resolve_company_identity_skips_model_for_irrelevant_observed_candidates(self) -> None:
        class _FailIfCalledModelClient:
            def judge_company_equivalence(self, payload):  # noqa: ARG002
                raise AssertionError("judge_company_equivalence should not be called for irrelevant observed candidates")

        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_dir = Path(temp_dir) / "runtime"
            self._write_seed_catalog(
                runtime_dir=runtime_dir,
                records=[
                    {
                        "company_key": "humansand",
                        "canonical_name": "Humans&",
                        "linkedin_slug": "humansand",
                        "aliases": ["humans and", "humansand"],
                        "confidence": "high",
                    }
                ],
            )
            with mock.patch.dict(os.environ, {"SOURCING_COMPANY_REGISTRY_RUNTIME_DIR": str(runtime_dir)}):
                identity = resolve_company_identity(
                    "Humans&",
                    model_client=_FailIfCalledModelClient(),
                    observed_companies=[
                        {
                            "label": "Completely Different Organization",
                            "linkedin_slug": "differentorg",
                            "linkedin_company_url": "https://www.linkedin.com/company/differentorg/",
                        }
                    ],
                )

        self.assertEqual(identity.resolver, "seed_catalog")
        self.assertEqual(identity.confidence, "high")

    def test_former_paid_fallback_skips_company_only_query_without_explicit_broad_strategy(self) -> None:
        class _FakeSettings:
            enabled = True
            max_paid_items = 25

        class _FakeHarvestConnector:
            settings = _FakeSettings()

            def __init__(self) -> None:
                self.queries = []
                self.filters = []
                self.tempdir = Path(".")

            def search_profiles(self, **kwargs):
                self.queries.append(kwargs.get("query_text", ""))
                self.filters.append(dict(kwargs.get("filter_hints") or {}))
                return {"raw_path": self.tempdir / "fake.json", "rows": []}

        identity = CompanyIdentity(
            requested_name="Thinking Machines Lab",
            canonical_name="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            linkedin_slug="thinkingmachinesai",
            linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            fake = _FakeHarvestConnector()
            fake.tempdir = Path(tempdir)
            acquirer = SearchSeedAcquirer([], harvest_search_connector=fake)
            entries, summaries, errors, accounts = acquirer._provider_people_search_fallback(
                identity=identity,
                discovery_dir=Path(tempdir),
                asset_logger=None,
                search_seed_queries=["Thinking Machines Lab Employee former"],
                filter_hints={"past_companies": ["https://www.linkedin.com/company/thinkingmachinesai/"]},
                employment_status="former",
                limit=25,
                cost_policy={},
            )
            self.assertEqual(entries, [])
            self.assertEqual(errors, [])
            self.assertEqual(accounts, [])
            self.assertGreaterEqual(len(summaries), 1)
            self.assertEqual(fake.queries, [])
            self.assertEqual(summaries[0]["status"], "skipped_degraded")
            self.assertEqual(
                summaries[0]["degraded_reason"],
                "former_broad_past_company_requires_explicit_strategy",
            )

    def test_discover_allows_harvest_former_fallback_without_rapidapi_accounts(self) -> None:
        class _FakeSettings:
            enabled = True
            max_paid_items = 100

        class _FakeHarvestConnector:
            settings = _FakeSettings()

            def search_profiles(self, **kwargs):
                return {
                    "raw_path": Path(kwargs["discovery_dir"]) / "fake_harvest.json",
                    "rows": [
                        {
                            "full_name": "Former Example",
                            "headline": "Research Engineer at NewCo",
                            "location": "San Francisco",
                            "profile_url": "https://www.linkedin.com/in/former-example/",
                            "username": "former-example",
                            "current_company": "NewCo",
                        }
                    ],
                    "payload": {},
                }

        class _NoopSearchProvider:
            def search(self, query, max_results=10):  # noqa: ARG002
                return SearchResponse(provider_name="noop", results=[])

        identity = CompanyIdentity(
            requested_name="Thinking Machines Lab",
            canonical_name="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            linkedin_slug="thinkingmachinesai",
            linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir) / "thinkingmachineslab" / "snapshot-01"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            acquirer = SearchSeedAcquirer(
                [],
                harvest_search_connector=_FakeHarvestConnector(),
                search_provider=_NoopSearchProvider(),
            )
            snapshot = acquirer.discover(
                identity,
                snapshot_dir,
                asset_logger=AssetLogger(snapshot_dir),
                search_seed_queries=[],
                query_bundles=[],
                filter_hints={"past_companies": ["https://www.linkedin.com/company/thinkingmachinesai/"]},
                cost_policy={
                    "provider_people_search_mode": "fallback_only",
                    "provider_people_search_min_expected_results": 1,
                    "provider_people_search_pages": 1,
                    "former_broad_past_company_only": True,
                },
                employment_status="former",
                worker_runtime=None,
                job_id="",
                request_payload={},
                plan_payload={},
                runtime_mode="maintenance",
            )
            self.assertEqual(snapshot.stop_reason, "provider_people_search_fallback")
            self.assertEqual(len(snapshot.entries), 1)
            self.assertEqual(snapshot.entries[0]["full_name"], "Former Example")

    def test_provider_people_search_fallback_skips_live_rapidapi_in_simulate_mode(self) -> None:
        identity = CompanyIdentity(
            requested_name="Thinking Machines Lab",
            canonical_name="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            linkedin_slug="thinkingmachinesai",
            linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
        )
        fake_account = RapidApiAccount(
            account_id="search_1",
            source="rapidapi",
            provider="fake",
            host="search/people",
            base_url="https://example.com",
            api_key="token",
            endpoint_search="/search/people",
        )
        with tempfile.TemporaryDirectory() as tempdir, mock.patch.dict(
            os.environ,
            {"SOURCING_EXTERNAL_PROVIDER_MODE": "simulate"},
            clear=False,
        ), mock.patch(
            "sourcing_agent.seed_discovery.request.urlopen",
            side_effect=AssertionError("live provider should not be called in simulate mode"),
        ):
            acquirer = SearchSeedAcquirer([fake_account])
            entries, summaries, errors, accounts = acquirer._provider_people_search_fallback(
                identity=identity,
                discovery_dir=Path(tempdir),
                asset_logger=AssetLogger(Path(tempdir)),
                search_seed_queries=["Thinking Machines Lab former employee"],
                filter_hints={"past_companies": ["https://www.linkedin.com/company/thinkingmachinesai/"]},
                employment_status="former",
                limit=25,
                cost_policy={},
            )
            self.assertEqual(entries, [])
            self.assertEqual(errors, [])
            self.assertEqual(accounts, [])
            self.assertEqual(len(summaries), 0)

    def test_provider_people_search_fallback_uses_runtime_scope_over_ambient_live_mode(self) -> None:
        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        fake_account = RapidApiAccount(
            account_id="search_1",
            source="rapidapi",
            provider="fake",
            host="search/people",
            base_url="https://example.com",
            api_key="token",
            endpoint_search="/search/people",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime" / "test_env" / "scripted_case"
            discovery_dir = runtime_dir / "company_assets" / "openai" / "snapshot-01" / "search_seed_discovery"
            discovery_dir.mkdir(parents=True)
            (runtime_dir / ".scripted-local-postgres.env").write_text(
                "\n".join(
                    [
                        "SOURCING_RUNTIME_ENVIRONMENT=scripted",
                        "SOURCING_EXTERNAL_PROVIDER_MODE=scripted",
                        "SOURCING_LIVE_PROVIDER_ACCESS_DISABLED=1",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            with mock.patch.dict(
                os.environ,
                {
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "live",
                    "SOURCING_RUNTIME_ENVIRONMENT": "local_dev",
                },
                clear=True,
            ), mock.patch(
                "sourcing_agent.seed_discovery.request.urlopen",
                side_effect=AssertionError("runtime-scoped scripted fallback must not call RapidAPI"),
            ) as urlopen_mock:
                acquirer = SearchSeedAcquirer([fake_account])
                entries, summaries, errors, accounts = acquirer._provider_people_search_fallback(
                    identity=identity,
                    discovery_dir=discovery_dir,
                    asset_logger=AssetLogger(discovery_dir),
                    search_seed_queries=["OpenAI Agent employee"],
                    filter_hints={"current_companies": ["https://www.linkedin.com/company/openai/"]},
                    employment_status="current",
                    limit=25,
                    cost_policy={"provider_people_search_mode": "primary_only"},
                )

            self.assertEqual(entries, [])
            self.assertEqual(errors, [])
            self.assertEqual(accounts, [])
            self.assertEqual(summaries, [])
            urlopen_mock.assert_not_called()

    def test_provider_people_search_live_rapidapi_rejects_isolated_runtime_without_confirm(self) -> None:
        fake_account = RapidApiAccount(
            account_id="search_1",
            source="rapidapi",
            provider="fake",
            host="search/people",
            base_url="https://example.com",
            api_key="token",
            endpoint_search="/search/people",
        )
        with tempfile.TemporaryDirectory() as tempdir, mock.patch.dict(
            os.environ,
            {
                "SOURCING_EXTERNAL_PROVIDER_MODE": "live",
                "SOURCING_RUNTIME_ENVIRONMENT": "test",
                "SOURCING_RUNTIME_DIR": str(Path(tempdir) / "runtime" / "test_env" / "scripted_case"),
            },
            clear=True,
        ), mock.patch("sourcing_agent.seed_discovery.request.urlopen") as urlopen_mock:
            acquirer = SearchSeedAcquirer([fake_account])
            with self.assertRaises(LiveProviderAccessError):
                acquirer._search_people("OpenAI Agent employees", limit=5)

        urlopen_mock.assert_not_called()

    def test_discover_uses_intent_view_queries_and_filters_when_task_metadata_is_sparse(self) -> None:
        class _FakeSettings:
            enabled = True
            max_paid_items = 100

        class _FakeHarvestConnector:
            settings = _FakeSettings()

            def __init__(self) -> None:
                self.calls: list[dict[str, object]] = []

            def search_profiles(self, **kwargs):
                self.calls.append(
                    {
                        "query_text": kwargs.get("query_text", ""),
                        "filter_hints": dict(kwargs.get("filter_hints") or {}),
                    }
                )
                return {
                    "raw_path": Path(kwargs["discovery_dir"]) / "fake_harvest.json",
                    "rows": [
                        {
                            "full_name": "Former Example",
                            "headline": "Pretraining Researcher at NewCo",
                            "location": "San Francisco",
                            "profile_url": "https://www.linkedin.com/in/former-example/",
                            "username": "former-example",
                            "current_company": "NewCo",
                        }
                    ],
                    "payload": {},
                }

        class _NoopSearchProvider:
            def search(self, query, max_results=10):  # noqa: ARG002
                return SearchResponse(provider_name="noop", results=[])

        identity = CompanyIdentity(
            requested_name="Thinking Machines Lab",
            canonical_name="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            linkedin_slug="thinkingmachinesai",
            linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir) / "thinkingmachineslab" / "snapshot-01"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            harvest_connector = _FakeHarvestConnector()
            acquirer = SearchSeedAcquirer(
                [],
                harvest_search_connector=harvest_connector,
                search_provider=_NoopSearchProvider(),
            )
            snapshot = acquirer.discover(
                identity,
                snapshot_dir,
                asset_logger=AssetLogger(snapshot_dir),
                search_seed_queries=[],
                query_bundles=[],
                filter_hints={},
                cost_policy={
                    "provider_people_search_mode": "fallback_only",
                    "provider_people_search_min_expected_results": 1,
                    "provider_people_search_pages": 1,
                },
                employment_status="former",
                worker_runtime=None,
                job_id="",
                request_payload={"target_company": "Thinking Machines Lab"},
                plan_payload={},
                runtime_mode="maintenance",
                intent_view={
                    "target_company": "Thinking Machines Lab",
                    "search_seed_queries": ["Pretraining"],
                    "filter_hints": {
                        "past_companies": ["https://www.linkedin.com/company/thinkingmachinesai/"],
                    },
                    "search_query_bundles": [
                        {
                            "bundle_id": "former_pretraining",
                            "label": "Pretraining",
                            "source_family": "former_employee_search",
                            "execution_mode": "paid_fallback",
                            "queries": ["Pretraining"],
                        }
                    ],
                },
            )
            self.assertEqual(snapshot.stop_reason, "provider_people_search_fallback")
            self.assertEqual(len(snapshot.entries), 1)
            self.assertEqual(snapshot.entries[0]["full_name"], "Former Example")
            self.assertEqual(harvest_connector.calls[0]["query_text"], "Pre-train")
            self.assertEqual(
                harvest_connector.calls[0]["filter_hints"].get("past_companies"),
                ["https://www.linkedin.com/company/thinkingmachinesai/"],
            )

    def test_former_paid_fallback_probes_then_expands_to_provider_total(self) -> None:
        class _FakeSettings:
            enabled = True
            max_paid_items = 25

        class _FakeHarvestConnector:
            settings = _FakeSettings()

            def __init__(self) -> None:
                self.calls: list[dict[str, object]] = []
                self.tempdir = Path(".")

            def search_profiles(self, **kwargs):
                self.calls.append(
                    {
                        "query_text": kwargs.get("query_text", ""),
                        "limit": kwargs.get("limit"),
                        "pages": kwargs.get("pages"),
                    }
                )
                if len(self.calls) == 1:
                    return {
                        "raw_path": self.tempdir / "probe.json",
                        "rows": [
                            {
                                "full_name": "Probe Example",
                                "headline": "Former Research Engineer",
                                "location": "San Francisco",
                                "profile_url": "https://www.linkedin.com/in/probe-example/",
                                "username": "probe-example",
                                "current_company": "NewCo",
                            }
                        ],
                        "pagination": {
                            "returned_count": 1,
                            "total_elements": 559,
                            "total_pages": 23,
                            "page_number": 1,
                            "page_size": 25,
                        },
                    }
                return {
                    "raw_path": self.tempdir / "full.json",
                    "rows": [
                        {
                            "full_name": "Former Example",
                            "headline": "Research Engineer at NewCo",
                            "location": "San Francisco",
                            "profile_url": "https://www.linkedin.com/in/former-example/",
                            "username": "former-example",
                            "current_company": "NewCo",
                        }
                    ],
                    "pagination": {
                        "returned_count": 1,
                        "total_elements": 559,
                        "total_pages": 23,
                        "page_number": 1,
                        "page_size": 25,
                    },
                }

        identity = CompanyIdentity(
            requested_name="Thinking Machines Lab",
            canonical_name="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            linkedin_slug="thinkingmachinesai",
            linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            fake = _FakeHarvestConnector()
            fake.tempdir = Path(tempdir)
            acquirer = SearchSeedAcquirer([], harvest_search_connector=fake)
            entries, summaries, errors, accounts = acquirer._provider_people_search_fallback(
                identity=identity,
                discovery_dir=Path(tempdir),
                asset_logger=None,
                search_seed_queries=[],
                filter_hints={"past_companies": ["https://www.linkedin.com/company/thinkingmachinesai/"]},
                employment_status="former",
                limit=50,
                cost_policy={"provider_people_search_pages": 2, "former_broad_past_company_only": True},
            )

        self.assertEqual(errors, [])
        self.assertEqual(accounts, ["harvest_profile_search"])
        self.assertEqual(len(entries), 1)
        self.assertEqual(len(fake.calls), 2)
        self.assertEqual(fake.calls[0], {"query_text": "", "limit": 25, "pages": 1})
        self.assertEqual(fake.calls[1], {"query_text": "", "limit": 559, "pages": 23})
        self.assertEqual(summaries[0]["effective_limit"], 559)
        self.assertEqual(summaries[0]["effective_pages"], 23)
        self.assertEqual(summaries[0]["probe"]["provider_total_count"], 559)

    def test_former_paid_fallback_keyword_only_skips_blank_query(self) -> None:
        class _FakeSettings:
            enabled = True
            max_paid_items = 25

        class _FakeHarvestConnector:
            settings = _FakeSettings()

            def __init__(self) -> None:
                self.queries: list[str] = []
                self.tempdir = Path(".")

            def search_profiles(self, **kwargs):
                query_text = str(kwargs.get("query_text") or "").strip()
                self.queries.append(query_text)
                return {
                    "raw_path": self.tempdir / f"{query_text or 'blank'}.json",
                    "rows": [
                        {
                            "full_name": "Keyword Former",
                            "headline": "Research Scientist",
                            "location": "United States",
                            "profile_url": f"https://www.linkedin.com/in/{query_text or 'blank'}-former/",
                            "username": f"{query_text or 'blank'}-former",
                            "current_company": "Google",
                        }
                    ],
                    "pagination": {
                        "returned_count": 1,
                        "total_elements": 1,
                        "total_pages": 1,
                        "page_number": 1,
                        "page_size": 25,
                    },
                }

        identity = CompanyIdentity(
            requested_name="Google",
            canonical_name="Google",
            company_key="google",
            linkedin_slug="google",
            linkedin_company_url="https://www.linkedin.com/company/google/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            fake = _FakeHarvestConnector()
            fake.tempdir = Path(tempdir)
            acquirer = SearchSeedAcquirer([], harvest_search_connector=fake)
            entries, summaries, errors, accounts = acquirer._provider_people_search_fallback(
                identity=identity,
                discovery_dir=Path(tempdir),
                asset_logger=None,
                search_seed_queries=["multimodal Veo", "Nano Banana"],
                filter_hints={"past_companies": ["https://www.linkedin.com/company/google/"]},
                employment_status="former",
                limit=25,
                cost_policy={
                    "provider_people_search_query_strategy": "all_queries_union",
                    "former_keyword_queries_only": True,
                },
            )

        self.assertEqual(errors, [])
        self.assertEqual(accounts, ["harvest_profile_search"])
        self.assertCountEqual(fake.queries, ["multimodal Veo", "Nano Banana"])
        self.assertEqual(len(entries), 2)
        self.assertGreaterEqual(len(summaries), 2)
        self.assertFalse(any(str(item.get("query") or "") == "__past_company_only__" for item in summaries))

    def test_former_paid_fallback_scoped_keyword_runs_query_without_broad_past_company(self) -> None:
        class _FakeSettings:
            enabled = True
            max_paid_items = 25

        class _FakeHarvestConnector:
            settings = _FakeSettings()

            def __init__(self) -> None:
                self.queries: list[str] = []
                self.filters: list[dict[str, list[str]]] = []
                self.tempdir = Path(".")

            def search_profiles(self, **kwargs):
                query_text = str(kwargs.get("query_text") or "").strip()
                self.queries.append(query_text)
                self.filters.append(dict(kwargs.get("filter_hints") or {}))
                return {
                    "raw_path": self.tempdir / f"{query_text or 'blank'}.json",
                    "rows": [
                        {
                            "full_name": "Scoped Former",
                            "headline": "Infrastructure Engineer at NewCo",
                            "location": "United States",
                            "profile_url": "https://www.linkedin.com/in/scoped-former/",
                            "username": "scoped-former",
                            "current_company": "NewCo",
                        }
                    ],
                    "pagination": {
                        "returned_count": 1,
                        "total_elements": 1,
                        "total_pages": 1,
                        "page_number": 1,
                        "page_size": 25,
                    },
                }

        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            fake = _FakeHarvestConnector()
            fake.tempdir = Path(tempdir)
            acquirer = SearchSeedAcquirer([], harvest_search_connector=fake)
            entries, summaries, errors, accounts = acquirer._provider_people_search_fallback(
                identity=identity,
                discovery_dir=Path(tempdir),
                asset_logger=None,
                search_seed_queries=["OpenAI Infra former"],
                filter_hints={
                    "past_companies": ["https://www.linkedin.com/company/openai/"],
                    "keywords": ["Infra"],
                },
                employment_status="former",
                limit=25,
                cost_policy={
                    "provider_people_search_query_strategy": "all_queries_union",
                    "former_broad_past_company_only": False,
                },
            )

        self.assertEqual(errors, [])
        self.assertEqual(accounts, ["harvest_profile_search"])
        self.assertEqual(fake.queries, ["Infra"])
        self.assertEqual(
            fake.filters[0].get("past_companies"),
            ["https://www.linkedin.com/company/openai/"],
        )
        self.assertEqual(len(entries), 1)
        self.assertEqual(summaries[0]["effective_query_text"], "Infra")

    def test_former_paid_fallback_full_roster_uses_broad_past_company_when_not_keyword_only(self) -> None:
        class _FakeSettings:
            enabled = True
            max_paid_items = 25

        class _FakeHarvestConnector:
            settings = _FakeSettings()

            def __init__(self) -> None:
                self.queries: list[str] = []
                self.filters: list[dict[str, list[str]]] = []
                self.tempdir = Path(".")

            def search_profiles(self, **kwargs):
                query_text = str(kwargs.get("query_text") or "").strip()
                self.queries.append(query_text)
                self.filters.append(dict(kwargs.get("filter_hints") or {}))
                return {
                    "raw_path": self.tempdir / f"{query_text or 'blank'}.json",
                    "rows": [
                        {
                            "full_name": "Former Perplexity Person",
                            "headline": "Research Engineer",
                            "location": "United States",
                            "profile_url": "https://www.linkedin.com/in/former-perplexity-person/",
                            "username": "former-perplexity-person",
                            "current_company": "Other AI",
                        }
                    ],
                    "pagination": {
                        "returned_count": 1,
                        "total_elements": 1,
                        "total_pages": 1,
                        "page_number": 1,
                        "page_size": 25,
                    },
                }

        identity = CompanyIdentity(
            requested_name="Perplexity",
            canonical_name="Perplexity",
            company_key="perplexity",
            linkedin_slug="perplexity-ai",
            linkedin_company_url="https://www.linkedin.com/company/perplexity-ai/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            fake = _FakeHarvestConnector()
            fake.tempdir = Path(tempdir)
            acquirer = SearchSeedAcquirer([], harvest_search_connector=fake)
            entries, summaries, errors, accounts = acquirer._provider_people_search_fallback(
                identity=identity,
                discovery_dir=Path(tempdir),
                asset_logger=None,
                search_seed_queries=["Agent"],
                filter_hints={
                    "past_companies": ["https://www.linkedin.com/company/perplexity-ai/"],
                    "keywords": ["Agent"],
                    "function_ids": ["24", "8"],
                },
                employment_status="former",
                limit=25,
                cost_policy={
                    "provider_people_search_query_strategy": "all_queries_union",
                    "former_broad_past_company_only": True,
                },
            )

        self.assertEqual(errors, [])
        self.assertEqual(accounts, ["harvest_profile_search"])
        self.assertEqual(fake.queries, [""])
        self.assertEqual(fake.filters[0]["past_companies"], ["https://www.linkedin.com/company/perplexity-ai/"])
        self.assertEqual(len(entries), 1)
        self.assertEqual(summaries[0]["query"], "__past_company_only__")
        self.assertEqual(summaries[0]["effective_query_text"], "")

    def test_former_paid_fallback_does_not_send_generic_full_roster_seed_query_to_harvest(self) -> None:
        class _FakeSettings:
            enabled = True
            max_paid_items = 25

        class _FakeHarvestConnector:
            settings = _FakeSettings()

            def __init__(self) -> None:
                self.queries: list[str] = []
                self.filters: list[dict[str, list[str]]] = []
                self.tempdir = Path(".")

            def search_profiles(self, **kwargs):
                query_text = str(kwargs.get("query_text") or "").strip()
                self.queries.append(query_text)
                self.filters.append(dict(kwargs.get("filter_hints") or {}))
                return {
                    "raw_path": self.tempdir / f"{query_text or 'blank'}.json",
                    "rows": [
                        {
                            "full_name": "Former Lovable Person",
                            "headline": "Builder at NewCo",
                            "location": "Sweden",
                            "profile_url": "https://www.linkedin.com/in/former-lovable-person/",
                            "username": "former-lovable-person",
                            "current_company": "NewCo",
                        }
                    ],
                    "pagination": {
                        "returned_count": 1,
                        "total_elements": 1,
                        "total_pages": 1,
                        "page_number": 1,
                        "page_size": 25,
                    },
                }

        identity = CompanyIdentity(
            requested_name="Lovable",
            canonical_name="Lovable",
            company_key="lovable",
            linkedin_slug="lovable-dev",
            linkedin_company_url="https://www.linkedin.com/company/lovable-dev/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            fake = _FakeHarvestConnector()
            fake.tempdir = Path(tempdir)
            acquirer = SearchSeedAcquirer([], harvest_search_connector=fake)
            entries, summaries, errors, accounts = acquirer._provider_people_search_fallback(
                identity=identity,
                discovery_dir=Path(tempdir),
                asset_logger=None,
                search_seed_queries=["Lovable Employee", "Lovable LinkedIn Employee"],
                filter_hints={"past_companies": ["https://www.linkedin.com/company/lovable-dev/"]},
                employment_status="former",
                limit=25,
                cost_policy={
                    "provider_people_search_query_strategy": "all_queries_union",
                    "former_broad_past_company_only": True,
                },
            )

        self.assertEqual(errors, [])
        self.assertEqual(accounts, ["harvest_profile_search"])
        self.assertEqual(fake.queries, [""])
        self.assertEqual(fake.filters[0]["past_companies"], ["https://www.linkedin.com/company/lovable-dev/"])
        self.assertEqual(len(entries), 1)
        self.assertEqual(summaries[0]["query"], "__past_company_only__")
        self.assertEqual(summaries[0]["effective_query_text"], "")

    def test_paid_fallback_all_queries_union_runs_all_harvest_queries(self) -> None:
        class _FakeSettings:
            enabled = True
            max_paid_items = 25

        class _FakeHarvestConnector:
            settings = _FakeSettings()

            def __init__(self) -> None:
                self.queries: list[str] = []
                self.tempdir = Path(".")

            def search_profiles(self, **kwargs):
                query_text = str(kwargs.get("query_text") or "").strip()
                self.queries.append(query_text)
                return {
                    "raw_path": self.tempdir / f"{query_text or 'blank'}.json",
                    "rows": [
                        {
                            "full_name": f"{query_text} Person".strip(),
                            "headline": "Research Engineer",
                            "location": "United States",
                            "profile_url": f"https://www.linkedin.com/in/{query_text or 'blank'}-person/",
                            "username": f"{query_text or 'blank'}-person",
                            "current_company": "Google",
                        }
                    ],
                }

        identity = CompanyIdentity(
            requested_name="Google",
            canonical_name="Google",
            company_key="google",
            linkedin_slug="google",
            linkedin_company_url="https://www.linkedin.com/company/google/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            fake = _FakeHarvestConnector()
            fake.tempdir = Path(tempdir)
            acquirer = SearchSeedAcquirer([], harvest_search_connector=fake)
            entries, summaries, errors, accounts = acquirer._provider_people_search_fallback(
                identity=identity,
                discovery_dir=Path(tempdir),
                asset_logger=None,
                search_seed_queries=["multimodal Veo", "Nano Banana"],
                filter_hints={"current_companies": ["https://www.linkedin.com/company/google/"]},
                employment_status="current",
                limit=25,
                cost_policy={"provider_people_search_query_strategy": "all_queries_union"},
            )

        self.assertEqual(errors, [])
        self.assertEqual(accounts, ["harvest_profile_search"])
        self.assertCountEqual(fake.queries, ["multimodal Veo", "Nano Banana"])
        self.assertEqual(len(entries), 2)
        self.assertTrue(any(item["source_query"] == "multimodal Veo" for item in entries))
        self.assertTrue(any(item["source_query"] == "Nano Banana" for item in entries))
        self.assertGreaterEqual(len(summaries), 2)

    def test_paid_fallback_prefers_filter_keywords_for_current_scoped_search(self) -> None:
        class _FakeSettings:
            enabled = True
            max_paid_items = 25

        class _FakeHarvestConnector:
            settings = _FakeSettings()

            def __init__(self) -> None:
                self.queries: list[str] = []
                self.filters: list[dict[str, list[str]]] = []
                self.tempdir = Path(".")

            def search_profiles(self, **kwargs):
                query_text = str(kwargs.get("query_text") or "").strip()
                self.queries.append(query_text)
                self.filters.append(dict(kwargs.get("filter_hints") or {}))
                return {
                    "raw_path": self.tempdir / f"{query_text or 'blank'}.json",
                    "rows": [
                        {
                            "full_name": "Reasoning Person",
                            "headline": "Research Scientist",
                            "location": "United States",
                            "profile_url": "https://www.linkedin.com/in/reasoning-person/",
                            "username": "reasoning-person",
                            "current_company": "OpenAI",
                        }
                    ],
                }

        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            fake = _FakeHarvestConnector()
            fake.tempdir = Path(tempdir)
            acquirer = SearchSeedAcquirer([], harvest_search_connector=fake)
            entries, summaries, errors, accounts = acquirer._provider_people_search_fallback(
                identity=identity,
                discovery_dir=Path(tempdir),
                asset_logger=None,
                search_seed_queries=["OpenAI Reasoning research Researcher"],
                filter_hints={
                    "current_companies": ["https://www.linkedin.com/company/openai/"],
                    "job_titles": ["Researcher"],
                    "keywords": ["Reasoning", "research"],
                },
                employment_status="current",
                limit=25,
                cost_policy={"provider_people_search_query_strategy": "all_queries_union"},
            )

        self.assertEqual(errors, [])
        self.assertEqual(accounts, ["harvest_profile_search"])
        self.assertEqual(fake.queries, ["Reasoning"])
        self.assertEqual(
            fake.filters[0]["current_companies"],
            ["https://www.linkedin.com/company/openai/"],
        )
        self.assertEqual(len(entries), 1)
        self.assertEqual(summaries[0]["effective_query_text"], "Reasoning")

    def test_provider_people_search_dedupes_keyword_alias_family(self) -> None:
        class _FakeSettings:
            enabled = True
            max_paid_items = 25

        class _FakeHarvestConnector:
            settings = _FakeSettings()

            def __init__(self) -> None:
                self.queries: list[str] = []
                self.tempdir = Path(".")

            def search_profiles(self, **kwargs):
                query_text = str(kwargs.get("query_text") or "").strip()
                self.queries.append(query_text)
                return {
                    "raw_path": self.tempdir / f"{query_text or 'blank'}.json",
                    "rows": [],
                }

        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            fake = _FakeHarvestConnector()
            fake.tempdir = Path(tempdir)
            acquirer = SearchSeedAcquirer([], harvest_search_connector=fake)
            acquirer._provider_people_search_fallback(
                identity=identity,
                discovery_dir=Path(tempdir),
                asset_logger=None,
                search_seed_queries=["Reasoning", "Reasoning model"],
                filter_hints={"current_companies": ["https://www.linkedin.com/company/openai/"]},
                employment_status="current",
                limit=25,
                cost_policy={"provider_people_search_query_strategy": "all_queries_union"},
            )

        self.assertEqual(fake.queries, ["Reasoning"])

    def test_provider_people_search_preserves_canonical_pretrain_label(self) -> None:
        class _FakeSettings:
            enabled = True
            max_paid_items = 25

        class _FakeHarvestConnector:
            settings = _FakeSettings()

            def __init__(self) -> None:
                self.queries: list[str] = []
                self.tempdir = Path(".")

            def search_profiles(self, **kwargs):
                query_text = str(kwargs.get("query_text") or "").strip()
                self.queries.append(query_text)
                return {
                    "raw_path": self.tempdir / f"{query_text or 'blank'}.json",
                    "rows": [],
                }

        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropicresearch",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            fake = _FakeHarvestConnector()
            fake.tempdir = Path(tempdir)
            acquirer = SearchSeedAcquirer([], harvest_search_connector=fake)
            acquirer._provider_people_search_fallback(
                identity=identity,
                discovery_dir=Path(tempdir),
                asset_logger=None,
                search_seed_queries=["pre_training", "Pre-training", "Pre-train"],
                filter_hints={"past_companies": ["https://www.linkedin.com/company/anthropicresearch/"]},
                employment_status="former",
                limit=25,
                cost_policy={"provider_people_search_query_strategy": "all_queries_union"},
            )

        self.assertEqual(fake.queries, ["Pre-train"])

    def test_provider_people_search_prefers_natural_posttrain_label(self) -> None:
        class _FakeSettings:
            enabled = True
            max_paid_items = 25

        class _FakeHarvestConnector:
            settings = _FakeSettings()

            def __init__(self) -> None:
                self.queries: list[str] = []
                self.tempdir = Path(".")

            def search_profiles(self, **kwargs):
                query_text = str(kwargs.get("query_text") or "").strip()
                self.queries.append(query_text)
                return {
                    "raw_path": self.tempdir / f"{query_text or 'blank'}.json",
                    "rows": [],
                }

        identity = CompanyIdentity(
            requested_name="Reflection AI",
            canonical_name="Reflection AI",
            company_key="reflectionai",
            linkedin_slug="reflectionai",
            linkedin_company_url="https://www.linkedin.com/company/reflectionai/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            fake = _FakeHarvestConnector()
            fake.tempdir = Path(tempdir)
            acquirer = SearchSeedAcquirer([], harvest_search_connector=fake)
            acquirer._provider_people_search_fallback(
                identity=identity,
                discovery_dir=Path(tempdir),
                asset_logger=None,
                search_seed_queries=["post_train", "Post-training", "Post-train"],
                filter_hints={"current_companies": ["https://www.linkedin.com/company/reflectionai/"]},
                employment_status="current",
                limit=25,
                cost_policy={"provider_people_search_query_strategy": "all_queries_union"},
            )

        self.assertEqual(fake.queries, ["Post-train"])

    def test_provider_people_search_prefers_natural_rl_eval_infra_labels(self) -> None:
        class _FakeSettings:
            enabled = True
            max_paid_items = 25

        class _FakeHarvestConnector:
            settings = _FakeSettings()

            def __init__(self) -> None:
                self.queries: list[str] = []
                self.tempdir = Path(".")

            def search_profiles(self, **kwargs):
                query_text = str(kwargs.get("query_text") or "").strip()
                self.queries.append(query_text)
                return {
                    "raw_path": self.tempdir / f"{query_text or 'blank'}.json",
                    "rows": [],
                }

        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            fake = _FakeHarvestConnector()
            fake.tempdir = Path(tempdir)
            acquirer = SearchSeedAcquirer([], harvest_search_connector=fake)
            acquirer._provider_people_search_fallback(
                identity=identity,
                discovery_dir=Path(tempdir),
                asset_logger=None,
                search_seed_queries=["RL", "Eval", "Infra"],
                filter_hints={"current_companies": ["https://www.linkedin.com/company/openai/"]},
                employment_status="current",
                limit=25,
                cost_policy={"provider_people_search_query_strategy": "all_queries_union"},
            )

        self.assertEqual(fake.queries, ["Reinforcement Learning", "Evaluation", "Infrastructure"])

    def test_provider_people_search_normalizes_world_model_and_text_labels(self) -> None:
        class _FakeSettings:
            enabled = True
            max_paid_items = 25

        class _FakeHarvestConnector:
            settings = _FakeSettings()

            def __init__(self) -> None:
                self.queries: list[str] = []
                self.tempdir = Path(".")

            def search_profiles(self, **kwargs):
                query_text = str(kwargs.get("query_text") or "").strip()
                self.queries.append(query_text)
                return {
                    "raw_path": self.tempdir / f"{query_text or 'blank'}.json",
                    "rows": [],
                }

        identity = CompanyIdentity(
            requested_name="Google",
            canonical_name="Google",
            company_key="google",
            linkedin_slug="google",
            linkedin_company_url="https://www.linkedin.com/company/google/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            fake = _FakeHarvestConnector()
            fake.tempdir = Path(tempdir)
            acquirer = SearchSeedAcquirer([], harvest_search_connector=fake)
            acquirer._provider_people_search_fallback(
                identity=identity,
                discovery_dir=Path(tempdir),
                asset_logger=None,
                search_seed_queries=["world_models", "Text"],
                filter_hints={"current_companies": ["https://www.linkedin.com/company/google/"]},
                employment_status="current",
                limit=25,
                cost_policy={"provider_people_search_query_strategy": "all_queries_union"},
            )

        self.assertEqual(fake.queries, ["World model", "Language Model"])

    def test_paid_fallback_first_hit_strategy_stops_after_first_harvest_match(self) -> None:
        class _FakeSettings:
            enabled = True
            max_paid_items = 25

        class _FakeHarvestConnector:
            settings = _FakeSettings()

            def __init__(self) -> None:
                self.queries: list[str] = []
                self.tempdir = Path(".")

            def search_profiles(self, **kwargs):
                query_text = str(kwargs.get("query_text") or "").strip()
                self.queries.append(query_text)
                return {
                    "raw_path": self.tempdir / f"{query_text or 'blank'}.json",
                    "rows": [
                        {
                            "full_name": "First Hit Person",
                            "headline": "Research Scientist",
                            "location": "United States",
                            "profile_url": "https://www.linkedin.com/in/first-hit-person/",
                            "username": "first-hit-person",
                            "current_company": "Google",
                        }
                    ],
                }

        identity = CompanyIdentity(
            requested_name="Google",
            canonical_name="Google",
            company_key="google",
            linkedin_slug="google",
            linkedin_company_url="https://www.linkedin.com/company/google/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            fake = _FakeHarvestConnector()
            fake.tempdir = Path(tempdir)
            acquirer = SearchSeedAcquirer([], harvest_search_connector=fake)
            entries, summaries, errors, accounts = acquirer._provider_people_search_fallback(
                identity=identity,
                discovery_dir=Path(tempdir),
                asset_logger=None,
                search_seed_queries=["multimodal Veo", "Nano Banana"],
                filter_hints={"current_companies": ["https://www.linkedin.com/company/google/"]},
                employment_status="current",
                limit=25,
                cost_policy={"provider_people_search_query_strategy": "first_hit"},
            )

        self.assertEqual(errors, [])
        self.assertEqual(accounts, ["harvest_profile_search"])
        self.assertEqual(fake.queries, ["multimodal Veo"])
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["source_query"], "multimodal Veo")
        self.assertEqual(len(summaries), 1)

    def test_normalize_harvest_company_filters_prefers_exact_company_url(self) -> None:
        identity = CompanyIdentity(
            requested_name="Thinking Machines Lab",
            canonical_name="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            linkedin_slug="thinkingmachinesai",
            linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
            aliases=["thinking machines ai", "tml"],
        )
        normalized = _normalize_harvest_company_filters(
            identity,
            {
                "past_companies": ["Thinking Machines Lab"],
                "exclude_current_companies": ["thinkingmachinesai"],
                "keywords": ["systems"],
            },
        )
        self.assertEqual(
            normalized["past_companies"],
            ["https://www.linkedin.com/company/thinkingmachinesai/"],
        )
        self.assertEqual(
            normalized["exclude_current_companies"],
            ["https://www.linkedin.com/company/thinkingmachinesai/"],
        )
        self.assertEqual(normalized["keywords"], ["systems"])

    def test_execute_query_spec_queues_worker_when_dataforseo_task_not_ready(self) -> None:
        class _PendingSearchProvider:
            provider_name = "dataforseo_google_organic"

            def execute_with_checkpoint(self, query_text, *, max_results=10, timeout=None, checkpoint=None):
                return SearchExecutionResult(
                    provider_name=self.provider_name,
                    query_text=query_text,
                    pending=True,
                    message="waiting for queue",
                    checkpoint={"provider_name": self.provider_name, "task_id": "task_123", "status": "submitted"},
                    artifacts=[
                        SearchExecutionArtifact(
                            label="task_post",
                            payload={"tasks": [{"id": "task_123"}]},
                            metadata={"task_id": "task_123"},
                        )
                    ],
                )

        class _WorkerHandle:
            worker_id = 1

        class _FakeItemStore:
            def __init__(self) -> None:
                self.items: dict[str, dict] = {}

            def upsert_job_materialization_item(self, **kwargs):
                item_id = str(kwargs["item_id"])
                existing = dict(self.items.get(item_id) or {})
                metadata = {**dict(existing.get("metadata") or {}), **dict(kwargs.get("metadata") or {})}
                item = {**existing, **kwargs, "metadata": metadata}
                self.items[item_id] = item
                return item

        class _FakeWorkerRuntime:
            def __init__(self) -> None:
                self.completed = []
                self.store = _FakeItemStore()

            def begin_worker(self, **kwargs):
                return _WorkerHandle()

            def list_workers(self, **kwargs):
                return []

            def get_worker(self, worker_id):
                return {"checkpoint": {}, "output": {}, "status": "queued"}

            def should_interrupt_worker(self, handle):
                return False

            def complete_worker(self, handle, **kwargs):
                self.completed.append(kwargs)
                return kwargs

            def checkpoint_worker(self, handle, **kwargs):
                return kwargs

        identity = CompanyIdentity(
            requested_name="Thinking Machines Lab",
            canonical_name="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            linkedin_slug="thinkingmachinesai",
            linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            discovery_dir = Path(tempdir)
            runtime = _FakeWorkerRuntime()
            # R-010 fixture migration (2026-07-22): item registration now goes
            # through DurableRuntimeWriter events/reducers — a real PG-backed
            # store replaces the retired _FakeItemStore write surface.
            runtime.store = self.enterContext(
                pg_backed_control_plane_store(schema_label="r010_queued_worker")
            )
            acquirer = SearchSeedAcquirer([], search_provider=_PendingSearchProvider())
            result = acquirer._execute_query_spec(
                index=1,
                query_spec={
                    "query": '"Kevin Lu" "Thinking Machines Lab" site:linkedin.com/in',
                    "bundle_id": "targeted_people_search",
                    "source_family": "targeted_people_search",
                    "execution_mode": "web_search",
                },
                identity=identity,
                discovery_dir=discovery_dir,
                logger=AssetLogger(discovery_dir.parent),
                employment_status="current",
                worker_runtime=runtime,
                job_id="job_1",
                request_payload={"target_company": "Thinking Machines Lab"},
                plan_payload={},
                runtime_mode="workflow",
                result_limit=10,
            )
            self.assertEqual(result["worker_status"], "queued")
            self.assertEqual(result["summary"]["status"], "queued")
            self.assertEqual(runtime.completed[0]["status"], "queued")
            self.assertTrue((discovery_dir / "web_query_01_task_post.json").exists())
            # Current registration contract (R-010 migration 2026-07-22): a
            # queued provider worker durably records DiscoveryQueryStateRecorded
            # workflow events via DurableRuntimeWriter — job_materialization_items
            # is no longer this path's write surface.
            run_events = runtime.store.repos.workflow_runtime.list_workflow_events(
                legacy_job_workflow_run_id("job_1"), limit=0
            )
            discovery_events = [
                dict(event.get("payload") or {})
                for event in run_events
                if str(event.get("event_type") or "") == "DiscoveryQueryStateRecorded"
            ]
            self.assertGreaterEqual(len(discovery_events), 1)
            # Events are per-call snapshots; fold them newest-last to mirror the
            # old item-row merge semantics.
            folded_metadata: dict = {}
            for event_payload in discovery_events:
                folded_metadata.update(dict(event_payload.get("materialization_metadata") or {}))
            latest = discovery_events[-1]
            self.assertEqual(latest["item_kind"], "search_seed_discovery_query")
            self.assertEqual(latest["status"], "running")
            self.assertEqual(latest["phase"], "provider_owned")
            self.assertTrue(any(event.get("source_worker_ids") == [1] for event in discovery_events))
            self.assertEqual(
                [(event.get("status"), event.get("phase")) for event in discovery_events],
                [("queued", "queued"), ("running", "provider_owned")],
            )
            self.assertEqual(folded_metadata.get("worker_id"), 1)
            self.assertTrue(str(folded_metadata.get("worker_key") or ""))

    def test_discover_batch_prefetches_dataforseo_tasks(self) -> None:
        class _BatchSearchProvider:
            provider_name = "dataforseo_google_organic"

            def __init__(self) -> None:
                self.batch_calls = []
                self.ready_calls = []
                self.fetch_calls = []
                self.execute_calls = []

            def submit_batch_queries(self, query_specs):
                self.batch_calls.append(list(query_specs))
                return SearchBatchSubmissionResult(
                    provider_name=self.provider_name,
                    tasks=[
                        SearchBatchSubmissionTask(
                            task_key=str(item["task_key"]),
                            query_text=str(item["query_text"]),
                            checkpoint={
                                "provider_name": self.provider_name,
                                "task_id": f"task_{index}",
                                "status": "submitted",
                            },
                            metadata={"artifact_label": "task_post_batch_01"},
                        )
                        for index, item in enumerate(query_specs, start=1)
                    ],
                    artifacts=[
                        SearchExecutionArtifact(
                            label="task_post_batch_01",
                            payload={"tasks": [{"id": "task_1"}, {"id": "task_2"}]},
                        )
                    ],
                )

            def poll_ready_batch(self, query_specs):
                self.ready_calls.append(list(query_specs))
                return SearchBatchReadyResult(
                    provider_name=self.provider_name,
                    tasks=[
                        SearchBatchReadyTask(
                            task_key=str(query_specs[0]["task_key"]),
                            task_id="task_1",
                            query_text=query_specs[0]["query_text"],
                            checkpoint={
                                **dict(query_specs[0].get("checkpoint") or {}),
                                "status": "ready_cached",
                            },
                        ),
                        SearchBatchReadyTask(
                            task_key=str(query_specs[1]["task_key"]),
                            task_id="task_2",
                            query_text=query_specs[1]["query_text"],
                            checkpoint={
                                **dict(query_specs[1].get("checkpoint") or {}),
                                "status": "waiting_for_ready_cached",
                            },
                        ),
                    ],
                    artifacts=[
                        SearchExecutionArtifact(
                            label="tasks_ready_batch",
                            payload={"tasks": [{"result": []}]},
                        )
                    ],
                )

            def fetch_ready_batch(self, query_specs):
                self.fetch_calls.append(list(query_specs))
                return SearchBatchFetchResult(
                    provider_name=self.provider_name,
                    tasks=[
                        SearchBatchFetchTask(
                            task_key=str(query_specs[0]["task_key"]),
                            task_id="task_1",
                            query_text=query_specs[0]["query_text"],
                            response=SearchResponse(
                                provider_name=self.provider_name,
                                query_text=query_specs[0]["query_text"],
                                results=[
                                    SearchResultItem(
                                        title="Jane Doe - LinkedIn",
                                        url="https://www.linkedin.com/in/jane-doe/",
                                        snippet="Thinking Machines Lab",
                                    )
                                ],
                                raw_payload={"tasks": [{"id": "task_1"}]},
                                raw_format="json",
                            ),
                            checkpoint={
                                "provider_name": self.provider_name,
                                "task_id": "task_1",
                                "status": "fetched_cached",
                            },
                        )
                    ],
                    artifacts=[
                        SearchExecutionArtifact(
                            label="task_get_batch_01",
                            payload={"tasks": [{"id": "task_1"}]},
                        )
                    ],
                )

            def execute_with_checkpoint(self, query_text, *, max_results=10, timeout=None, checkpoint=None):
                self.execute_calls.append({"query_text": query_text, "checkpoint": dict(checkpoint or {})})
                return SearchExecutionResult(
                    provider_name=self.provider_name,
                    query_text=query_text,
                    pending=True,
                    message="waiting for queue",
                    checkpoint=dict(checkpoint or {}),
                )

        class _WorkerHandle:
            def __init__(self, worker_id: int) -> None:
                self.worker_id = worker_id

        class _FakeWorkerRuntime:
            def __init__(self) -> None:
                self.completed = []
                self.checkpoints = []
                self._next_worker_id = 0

            def begin_worker(self, **kwargs):
                self._next_worker_id += 1
                return _WorkerHandle(self._next_worker_id)

            def list_workers(self, **kwargs):
                return []

            def get_worker(self, worker_id):
                return {"checkpoint": {}, "output": {}, "status": "queued"}

            def should_interrupt_worker(self, handle):
                return False

            def complete_worker(self, handle, **kwargs):
                self.completed.append(kwargs)
                return kwargs

            def checkpoint_worker(self, handle, **kwargs):
                self.checkpoints.append(kwargs)
                return kwargs

        identity = CompanyIdentity(
            requested_name="Thinking Machines Lab",
            canonical_name="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            linkedin_slug="thinkingmachinesai",
            linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir)
            runtime = _FakeWorkerRuntime()
            provider = _BatchSearchProvider()
            acquirer = SearchSeedAcquirer([], search_provider=provider)
            result = acquirer.discover(
                identity,
                snapshot_dir,
                asset_logger=AssetLogger(snapshot_dir),
                search_seed_queries=[
                    '"Jane Doe" "Thinking Machines Lab" site:linkedin.com/in',
                    '"John Smith" "Thinking Machines Lab" site:linkedin.com/in',
                ],
                filter_hints={},
                cost_policy={
                    "allow_stage1_web_seed_fallback": True,
                    "parallel_search_workers": 2,
                    "public_media_results_per_query": 10,
                },
                employment_status="current",
                worker_runtime=runtime,
                job_id="job_1",
                request_payload={"target_company": "Thinking Machines Lab"},
                plan_payload={},
                runtime_mode="workflow",
            )
            self.assertEqual(len(provider.batch_calls), 1)
            self.assertEqual(len(provider.ready_calls), 1)
            self.assertEqual(len(provider.fetch_calls), 1)
            self.assertEqual(len(provider.execute_calls), 1)
            self.assertEqual(provider.execute_calls[0]["checkpoint"]["task_id"], "task_2")
            self.assertEqual(provider.execute_calls[0]["checkpoint"]["status"], "waiting_for_ready_cached")
            self.assertEqual(result.stop_reason, "queued_background_search")
            self.assertEqual(len(result.entries), 1)
            self.assertTrue((snapshot_dir / "search_seed_discovery" / "web_query_01.json").exists())
            self.assertTrue((snapshot_dir / "search_seed_discovery" / "web_search_batch_manifest.json").exists())
            acquirer.discover(
                identity,
                snapshot_dir,
                asset_logger=AssetLogger(snapshot_dir),
                search_seed_queries=[
                    '"Jane Doe" "Thinking Machines Lab" site:linkedin.com/in',
                    '"John Smith" "Thinking Machines Lab" site:linkedin.com/in',
                ],
                filter_hints={},
                cost_policy={
                    "allow_stage1_web_seed_fallback": True,
                    "parallel_search_workers": 2,
                    "public_media_results_per_query": 10,
                },
                employment_status="current",
                worker_runtime=runtime,
                job_id="job_1",
                request_payload={"target_company": "Thinking Machines Lab"},
                plan_payload={},
                runtime_mode="workflow",
            )
            self.assertEqual(len(provider.ready_calls), 1)
            self.assertEqual(len(provider.fetch_calls), 1)

    def test_discover_still_runs_paid_fallback_while_low_cost_queue_is_pending(self) -> None:
        class _PendingSearchProvider:
            provider_name = "dataforseo_google_organic"

            def execute_with_checkpoint(self, query_text, *, max_results=10, timeout=None, checkpoint=None):
                return SearchExecutionResult(
                    provider_name=self.provider_name,
                    query_text=query_text,
                    pending=True,
                    message="waiting for queue",
                    checkpoint={"provider_name": self.provider_name, "task_id": "task_queued", "status": "submitted"},
                    artifacts=[
                        SearchExecutionArtifact(
                            label="task_post",
                            payload={"tasks": [{"id": "task_queued"}]},
                            metadata={"task_id": "task_queued"},
                        )
                    ],
                )

        class _FakeHarvestSettings:
            enabled = True
            max_paid_items = 25

        class _FakeHarvestConnector:
            settings = _FakeHarvestSettings()

            def __init__(self) -> None:
                self.calls = 0

            def search_profiles(self, **kwargs):
                self.calls += 1
                return {
                    "raw_path": Path(kwargs["discovery_dir"]) / "harvest.json",
                    "rows": [
                        {
                            "full_name": "Queued Fallback Candidate",
                            "headline": "Former Thinking Machines Lab",
                            "location": "San Francisco",
                            "profile_url": "https://www.linkedin.com/in/queued-fallback/",
                            "username": "queued-fallback",
                        }
                    ],
                }

        class _WorkerHandle:
            worker_id = 1

        class _FakeWorkerRuntime:
            def __init__(self) -> None:
                self.completed = []

            def begin_worker(self, **kwargs):
                return _WorkerHandle()

            def list_workers(self, **kwargs):
                return []

            def get_worker(self, worker_id):
                return {"checkpoint": {}, "output": {}, "status": "queued"}

            def should_interrupt_worker(self, handle):
                return False

            def complete_worker(self, handle, **kwargs):
                self.completed.append(kwargs)
                return kwargs

            def checkpoint_worker(self, handle, **kwargs):
                return kwargs

        identity = CompanyIdentity(
            requested_name="Thinking Machines Lab",
            canonical_name="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            linkedin_slug="thinkingmachinesai",
            linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
        )
        fake_account = RapidApiAccount(
            account_id="search_1",
            source="rapidapi",
            provider="fake",
            host="search/people",
            base_url="https://example.com",
            api_key="token",
            endpoint_search="/search/people",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir) / "thinkingmachineslab" / "snapshot-01"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            harvest_connector = _FakeHarvestConnector()
            runtime = _FakeWorkerRuntime()
            acquirer = SearchSeedAcquirer(
                [fake_account],
                harvest_search_connector=harvest_connector,
                search_provider=_PendingSearchProvider(),
            )
            snapshot = acquirer.discover(
                identity,
                snapshot_dir,
                asset_logger=AssetLogger(snapshot_dir),
                search_seed_queries=["Thinking Machines Lab former employee"],
                query_bundles=[],
                filter_hints={"past_companies": ["Thinking Machines Lab"]},
                cost_policy={
                    "provider_people_search_mode": "fallback_only",
                    "provider_people_search_min_expected_results": 1,
                    "allow_stage1_web_seed_fallback": True,
                    "parallel_search_workers": 1,
                    "public_media_results_per_query": 10,
                    # R-010 update (2026-07-22): broad former past-company
                    # recall now requires this explicit strategy authorization
                    # (paid-cost guard former_broad_past_company_requires_
                    # explicit_strategy) — the test predates the gate.
                    "former_broad_past_company_only": True,
                },
                employment_status="former",
                worker_runtime=runtime,
                job_id="job_queued",
                request_payload={"target_company": "Thinking Machines Lab"},
                plan_payload={},
                runtime_mode="workflow",
            )
            self.assertEqual(snapshot.stop_reason, "queued_background_search")
            self.assertEqual(len(snapshot.entries), 1)
            self.assertEqual(snapshot.entries[0]["full_name"], "Queued Fallback Candidate")
            self.assertGreaterEqual(harvest_connector.calls, 1)
            self.assertEqual(runtime.completed[0]["status"], "queued")

    def test_stage1_web_seed_fallback_is_disabled_by_default(self) -> None:
        class _FailingSearchProvider:
            provider_name = "dataforseo_google_organic"

            def __init__(self) -> None:
                self.calls = 0

            def execute_with_checkpoint(self, *args, **kwargs):  # noqa: ANN002, ANN003
                self.calls += 1
                raise AssertionError("Stage 1 must not call DataForSEO seed fallback by default.")

            def search(self, *args, **kwargs):  # noqa: ANN002, ANN003
                self.calls += 1
                raise AssertionError("Stage 1 must not call DataForSEO seed fallback by default.")

        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir)
            provider = _FailingSearchProvider()
            acquirer = SearchSeedAcquirer([], search_provider=provider)
            snapshot = acquirer.discover(
                identity,
                snapshot_dir,
                asset_logger=AssetLogger(snapshot_dir),
                search_seed_queries=["Infra"],
                query_bundles=[
                    {
                        "bundle_id": "relationship_web",
                        "source_family": "public_web_search",
                        "execution_mode": "low_cost_web_search",
                        "queries": ["OpenAI Infra LinkedIn"],
                    }
                ],
                filter_hints={},
                cost_policy={"provider_people_search_mode": "fallback_only"},
                employment_status="current",
                worker_runtime=None,
                job_id="job_no_web_seed_default",
                request_payload={"target_company": "OpenAI"},
                plan_payload={},
                runtime_mode="workflow",
            )

        self.assertEqual(provider.calls, 0)
        self.assertEqual(snapshot.entries, [])
        self.assertTrue(snapshot.summary_payload["web_seed_fallback_suppressed"])
        self.assertFalse(snapshot.summary_payload["web_seed_fallback_enabled"])

    def test_discover_emits_incremental_query_results_as_each_query_finishes(self) -> None:
        class _ParallelSearchProvider:
            provider_name = "test_parallel"

            def search(self, query, max_results=10):  # noqa: ARG002
                if "slow" in query.lower():
                    time.sleep(0.2)
                    person_name = "Slow Candidate"
                    slug = "slow-candidate"
                else:
                    time.sleep(0.02)
                    person_name = "Fast Candidate"
                    slug = "fast-candidate"
                return SearchResponse(
                    provider_name=self.provider_name,
                    query_text=query,
                    results=[
                        SearchResultItem(
                            title=f"{person_name} - Thinking Machines Lab - LinkedIn",
                            url=f"https://www.linkedin.com/in/{slug}/",
                            snippet=f"{person_name} at Thinking Machines Lab",
                        )
                    ],
                    raw_payload={},
                    raw_format="json",
                )

        identity = CompanyIdentity(
            requested_name="Thinking Machines Lab",
            canonical_name="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            linkedin_slug="thinkingmachinesai",
            linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
        )
        incremental_queries: list[str] = []
        incremental_urls: list[str] = []
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir) / "thinkingmachineslab" / "snapshot-incremental"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            acquirer = SearchSeedAcquirer([], search_provider=_ParallelSearchProvider())
            snapshot = acquirer.discover(
                identity,
                snapshot_dir,
                asset_logger=AssetLogger(snapshot_dir),
                search_seed_queries=["slow query", "fast query"],
                filter_hints={},
                cost_policy={
                    "allow_stage1_web_seed_fallback": True,
                    "parallel_search_workers": 2,
                    "public_media_results_per_query": 10,
                },
                employment_status="current",
                on_incremental_query_result=lambda result: (
                    incremental_queries.append(str(result.get("query") or "")),
                    incremental_urls.extend(
                        [
                            str(dict(entry or {}).get("profile_url") or "").strip()
                            for entry in list(result.get("entries") or [])
                            if str(dict(entry or {}).get("profile_url") or "").strip()
                        ]
                    ),
                ),
            )

        self.assertEqual(snapshot.stop_reason, "completed")
        self.assertEqual(len(snapshot.entries), 2)
        self.assertEqual(incremental_queries[0], "fast query")
        self.assertEqual(incremental_queries[1], "slow query")
        self.assertEqual(
            incremental_urls,
            [
                "https://www.linkedin.com/in/fast-candidate/",
                "https://www.linkedin.com/in/slow-candidate/",
            ],
        )

    def test_provider_people_search_fallback_emits_incremental_results_per_query(self) -> None:
        class _FakeSettings:
            enabled = True
            max_paid_items = 25

        class _FakeHarvestConnector:
            settings = _FakeSettings()

            def search_profiles(self, **kwargs):
                query_text = str(kwargs.get("query_text") or "").strip()
                if "slow" in query_text.lower():
                    time.sleep(0.2)
                    full_name = "Slow Former"
                    slug = "slow-former"
                else:
                    time.sleep(0.02)
                    full_name = "Fast Former"
                    slug = "fast-former"
                return {
                    "raw_path": Path(kwargs["discovery_dir"]) / f"{slug}.json",
                    "rows": [
                        {
                            "full_name": full_name,
                            "headline": "Research Engineer",
                            "location": "San Francisco",
                            "profile_url": f"https://www.linkedin.com/in/{slug}/",
                            "username": slug,
                            "current_company": "Another Co",
                        }
                    ],
                }

        identity = CompanyIdentity(
            requested_name="Thinking Machines Lab",
            canonical_name="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            linkedin_slug="thinkingmachinesai",
            linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
        )
        incremental_queries: list[str] = []
        with tempfile.TemporaryDirectory() as tempdir:
            discovery_dir = Path(tempdir)
            acquirer = SearchSeedAcquirer([], harvest_search_connector=_FakeHarvestConnector())
            entries, summaries, errors, accounts = acquirer._provider_people_search_fallback(
                identity=identity,
                discovery_dir=discovery_dir,
                asset_logger=AssetLogger(discovery_dir),
                search_seed_queries=["slow query", "fast query"],
                filter_hints={"past_companies": ["https://www.linkedin.com/company/thinkingmachinesai/"]},
                employment_status="former",
                limit=25,
                cost_policy={
                    "provider_people_search_query_strategy": "all_queries_union",
                    "provider_people_search_overlap_pruning": False,
                },
                on_incremental_query_result=lambda result: incremental_queries.append(str(result.get("query") or "")),
            )

        self.assertEqual(errors, [])
        self.assertEqual(accounts, ["harvest_profile_search"])
        self.assertEqual(len(entries), 2)
        self.assertEqual(len(summaries), 2)
        self.assertEqual(incremental_queries[0], "fast query")
        self.assertEqual(incremental_queries[1], "slow query")

    def test_provider_people_search_fallback_uses_page_chunks_when_scaled_harvest_result_is_empty(self) -> None:
        class _FakeSettings:
            enabled = True
            max_paid_items = 2500

        class _FakeHarvestConnector:
            settings = _FakeSettings()

            def __init__(self) -> None:
                self.calls: list[tuple[int, int, int]] = []

            def search_profiles(self, **kwargs):
                limit = int(kwargs.get("limit") or 0)
                pages = int(kwargs.get("pages") or 0)
                start_page = int(kwargs.get("start_page") or 1)
                self.calls.append((start_page, limit, pages))
                raw_path = Path(kwargs["discovery_dir"]) / f"harvest_{start_page}_{limit}_{pages}.json"
                if start_page == 1 and limit == 25 and pages == 1:
                    return {
                        "raw_path": raw_path,
                        "rows": [
                            {
                                "full_name": "Probe Candidate",
                                "headline": "Former Gemini researcher",
                                "location": "United States",
                                "profile_url": "https://www.linkedin.com/in/probe-candidate/",
                                "username": "probe-candidate",
                                "current_company": "NewCo",
                            }
                        ],
                        "pagination": {
                            "total_elements": 3,
                            "total_pages": 3,
                            "returned_count": 1,
                        },
                    }
                if start_page in {2, 3} and limit == 25 and pages == 1:
                    return {
                        "raw_path": raw_path,
                        "rows": [
                            {
                                "full_name": f"Chunk Candidate {start_page}",
                                "headline": "Former Gemini researcher",
                                "location": "United States",
                                "profile_url": f"https://www.linkedin.com/in/chunk-candidate-{start_page}/",
                                "username": f"chunk-candidate-{start_page}",
                                "current_company": "NewCo",
                            }
                        ],
                        "pagination": {
                            "total_elements": 3,
                            "total_pages": 3,
                            "returned_count": 1,
                        },
                    }
                return {
                    "raw_path": raw_path,
                    "rows": [],
                    "pagination": {
                        "total_elements": 3,
                        "total_pages": 3,
                        "returned_count": 0,
                    },
                }

        identity = CompanyIdentity(
            requested_name="Google",
            canonical_name="Google",
            company_key="google",
            linkedin_slug="google",
            linkedin_company_url="https://www.linkedin.com/company/google/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            discovery_dir = Path(tempdir)
            harvest_connector = _FakeHarvestConnector()
            acquirer = SearchSeedAcquirer([], harvest_search_connector=harvest_connector)
            entries, summaries, errors, accounts = acquirer._provider_people_search_fallback(
                identity=identity,
                discovery_dir=discovery_dir,
                asset_logger=AssetLogger(discovery_dir),
                search_seed_queries=["Gemini"],
                filter_hints={
                    "past_companies": ["https://www.linkedin.com/company/google/"],
                    "keywords": ["Gemini"],
                },
                employment_status="former",
                limit=75,
                cost_policy={
                    "provider_people_search_pages": 3,
                    "provider_people_search_scale_chunk_pages": 1,
                    "provider_people_search_query_strategy": "all_queries_union",
                    "provider_people_search_overlap_pruning": False,
                },
            )

        self.assertEqual(errors, [])
        self.assertEqual(accounts, ["harvest_profile_search"])
        self.assertEqual(len(entries), 3)
        self.assertEqual(entries[0]["full_name"], "Probe Candidate")
        self.assertEqual(entries[1]["full_name"], "Chunk Candidate 2")
        self.assertEqual(entries[2]["full_name"], "Chunk Candidate 3")
        self.assertEqual(summaries[0]["result_source"], "chunked_scale_fallback")
        self.assertEqual(summaries[0]["fallback_reason"], "scaled_harvest_profile_search_returned_no_rows")
        self.assertEqual(summaries[0]["seed_entry_count"], 3)
        self.assertEqual(summaries[0]["chunked_scale_fallback"]["returned_count"], 3)
        self.assertFalse(summaries[0]["chunked_scale_fallback"]["incomplete"])
        self.assertIn((1, 25, 1), harvest_connector.calls)
        self.assertIn((1, 3, 3), harvest_connector.calls)
        self.assertIn((2, 25, 1), harvest_connector.calls)
        self.assertIn((3, 25, 1), harvest_connector.calls)

    def test_provider_people_search_retries_empty_page_chunk_as_single_pages(self) -> None:
        class _FakeSettings:
            enabled = True
            max_paid_items = 2500

        class _FakeHarvestConnector:
            settings = _FakeSettings()

            def __init__(self) -> None:
                self.calls: list[tuple[int, int, int]] = []

            def search_profiles(self, **kwargs):
                limit = int(kwargs.get("limit") or 0)
                pages = int(kwargs.get("pages") or 0)
                start_page = int(kwargs.get("start_page") or 1)
                self.calls.append((start_page, limit, pages))
                raw_path = Path(kwargs["discovery_dir"]) / f"harvest_{start_page}_{limit}_{pages}.json"
                if start_page == 1 and limit == 25 and pages == 1:
                    rows = [
                        {
                            "full_name": "Probe Candidate",
                            "headline": "Gemini researcher",
                            "location": "United States",
                            "profile_url": "https://www.linkedin.com/in/probe-candidate/",
                            "username": "probe-candidate",
                        }
                    ]
                elif start_page in {2, 3} and limit == 25 and pages == 1:
                    rows = [
                        {
                            "full_name": f"Recovered Candidate {start_page}",
                            "headline": "Gemini researcher",
                            "location": "United States",
                            "profile_url": f"https://www.linkedin.com/in/recovered-candidate-{start_page}/",
                            "username": f"recovered-candidate-{start_page}",
                        }
                    ]
                else:
                    rows = []
                return {
                    "raw_path": raw_path,
                    "rows": rows,
                    "pagination": {
                        "total_elements": 3,
                        "total_pages": 3,
                        "returned_count": len(rows),
                    },
                }

        identity = CompanyIdentity(
            requested_name="Google",
            canonical_name="Google",
            company_key="google",
            linkedin_slug="google",
            linkedin_company_url="https://www.linkedin.com/company/google/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            discovery_dir = Path(tempdir)
            harvest_connector = _FakeHarvestConnector()
            acquirer = SearchSeedAcquirer([], harvest_search_connector=harvest_connector)
            entries, summaries, _errors, _accounts = acquirer._provider_people_search_fallback(
                identity=identity,
                discovery_dir=discovery_dir,
                asset_logger=AssetLogger(discovery_dir),
                search_seed_queries=["Gemini"],
                filter_hints={"current_companies": ["https://www.linkedin.com/company/google/"]},
                employment_status="current",
                limit=75,
                cost_policy={
                    "provider_people_search_pages": 3,
                    "provider_people_search_scale_chunk_pages": 2,
                    "provider_people_search_query_strategy": "all_queries_union",
                    "provider_people_search_overlap_pruning": False,
                },
            )

        self.assertEqual(len(entries), 3)
        self.assertEqual(summaries[0]["result_source"], "chunked_scale_fallback")
        self.assertEqual(summaries[0]["chunked_scale_fallback"]["single_page_retry_count"], 2)
        self.assertFalse(summaries[0]["chunked_scale_fallback"]["incomplete"])
        self.assertIn((2, 50, 2), harvest_connector.calls)
        self.assertIn((2, 25, 1), harvest_connector.calls)
        self.assertIn((3, 25, 1), harvest_connector.calls)

    def test_provider_people_search_marks_chunked_page_coverage_drift_degraded(self) -> None:
        class _FakeSettings:
            enabled = True
            max_paid_items = 2500

        class _FakeHarvestConnector:
            settings = _FakeSettings()

            def search_profiles(self, **kwargs):
                limit = int(kwargs.get("limit") or 0)
                pages = int(kwargs.get("pages") or 0)
                start_page = int(kwargs.get("start_page") or 1)
                raw_path = Path(kwargs["discovery_dir"]) / f"harvest_{start_page}_{limit}_{pages}.json"
                if start_page == 1 and limit == 25 and pages == 1:
                    rows = [
                        {
                            "full_name": "Probe Candidate",
                            "headline": "Gemini researcher",
                            "location": "United States",
                            "profile_url": "https://www.linkedin.com/in/probe-candidate/",
                            "username": "probe-candidate",
                        }
                    ]
                elif start_page == 2 and limit == 25 and pages == 1:
                    rows = [
                        {
                            "full_name": "Recovered Candidate 2",
                            "headline": "Gemini researcher",
                            "location": "United States",
                            "profile_url": "https://www.linkedin.com/in/recovered-candidate-2/",
                            "username": "recovered-candidate-2",
                        }
                    ]
                else:
                    rows = []
                return {
                    "raw_path": raw_path,
                    "rows": rows,
                    "pagination": {
                        "total_elements": 3,
                        "total_pages": 3,
                        "returned_count": len(rows),
                    },
                }

        identity = CompanyIdentity(
            requested_name="Google",
            canonical_name="Google",
            company_key="google",
            linkedin_slug="google",
            linkedin_company_url="https://www.linkedin.com/company/google/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            discovery_dir = Path(tempdir)
            acquirer = SearchSeedAcquirer([], harvest_search_connector=_FakeHarvestConnector())
            entries, summaries, _errors, _accounts = acquirer._provider_people_search_fallback(
                identity=identity,
                discovery_dir=discovery_dir,
                asset_logger=AssetLogger(discovery_dir),
                search_seed_queries=["Gemini"],
                filter_hints={"current_companies": ["https://www.linkedin.com/company/google/"]},
                employment_status="current",
                limit=75,
                cost_policy={
                    "provider_people_search_pages": 3,
                    "provider_people_search_scale_chunk_pages": 2,
                    "provider_people_search_query_strategy": "all_queries_union",
                    "provider_people_search_overlap_pruning": False,
                },
            )

        self.assertEqual(len(entries), 2)
        self.assertEqual(summaries[0]["result_source"], "chunked_scale_partial_fallback")
        self.assertEqual(summaries[0]["status"], "degraded")
        self.assertEqual(
            summaries[0]["degraded_reason"],
            "provider_reported_variable_or_unreliable_page_coverage_after_probe",
        )
        self.assertTrue(summaries[0]["chunked_scale_fallback"]["coverage_degraded"])
        self.assertTrue(summaries[0]["chunked_scale_fallback"]["incomplete"])
        self.assertEqual(summaries[0]["chunked_scale_fallback"]["single_page_retry_count"], 2)
        self.assertEqual(summaries[0]["chunked_scale_fallback"]["empty_page_ranges"], [{"start_page": 3, "pages": 1}])

    def test_provider_people_search_probe_fallback_without_chunks_is_marked_degraded(self) -> None:
        class _FakeSettings:
            enabled = True
            max_paid_items = 2500

        class _FakeHarvestConnector:
            settings = _FakeSettings()

            def search_profiles(self, **kwargs):
                limit = int(kwargs.get("limit") or 0)
                pages = int(kwargs.get("pages") or 0)
                start_page = int(kwargs.get("start_page") or 1)
                raw_path = Path(kwargs["discovery_dir"]) / f"harvest_{start_page}_{limit}_{pages}.json"
                if start_page == 1 and limit == 25 and pages == 1:
                    return {
                        "raw_path": raw_path,
                        "rows": [
                            {
                                "full_name": "Probe Candidate",
                                "headline": "Gemini researcher",
                                "location": "United States",
                                "profile_url": "https://www.linkedin.com/in/probe-candidate/",
                                "username": "probe-candidate",
                                "current_company": "Google",
                            }
                        ],
                        "pagination": {
                            "total_elements": 75,
                            "total_pages": 3,
                            "returned_count": 25,
                        },
                    }
                return {
                    "raw_path": raw_path,
                    "rows": [],
                    "pagination": {
                        "total_elements": 75,
                        "total_pages": 3,
                        "returned_count": 0,
                    },
                }

        identity = CompanyIdentity(
            requested_name="Google",
            canonical_name="Google",
            company_key="google",
            linkedin_slug="google",
            linkedin_company_url="https://www.linkedin.com/company/google/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir)
            acquirer = SearchSeedAcquirer([], harvest_search_connector=_FakeHarvestConnector())
            snapshot = acquirer.discover(
                identity,
                snapshot_dir,
                asset_logger=AssetLogger(snapshot_dir),
                search_seed_queries=["Gemini"],
                query_bundles=[],
                filter_hints={
                    "current_companies": ["https://www.linkedin.com/company/google/"],
                    "keywords": ["Gemini"],
                },
                cost_policy={
                    "provider_people_search_mode": "primary_only",
                    "provider_people_search_pages": 3,
                    "provider_people_search_scale_chunk_pages": 1,
                    "provider_people_search_query_strategy": "all_queries_union",
                    "provider_people_search_overlap_pruning": False,
                },
                employment_status="current",
            )

        # stop_reason carries lane provenance since the primary/fallback split
        # (R-010 vocabulary update 2026-07-22).
        self.assertEqual(snapshot.stop_reason, "provider_people_search_primary")
        self.assertEqual(len(snapshot.entries), 1)
        self.assertEqual(snapshot.summary_payload["incomplete_provider_query_count"], 0)
        self.assertEqual(snapshot.query_summaries[0]["status"], "degraded")
        self.assertTrue(snapshot.query_summaries[0]["provider_search_degraded"])
        self.assertEqual(
            snapshot.query_summaries[0]["degraded_reason"],
            "provider_reported_variable_or_unreliable_page_coverage_after_probe",
        )

    def test_provider_people_search_zero_result_after_retry_is_marked_incomplete(self) -> None:
        class _FakeSettings:
            enabled = True
            max_paid_items = 2500

        class _FakeHarvestConnector:
            settings = _FakeSettings()

            def search_profiles(self, **kwargs):
                raw_path = Path(kwargs["discovery_dir"]) / "harvest_zero_after_retry.json"
                return {
                    "raw_path": raw_path,
                    "rows": [],
                    "pagination": {
                        "total_elements": 0,
                        "total_pages": 0,
                        "returned_count": 0,
                    },
                    "zero_result_retry": {
                        "attempts": int(kwargs.get("zero_result_retry_attempts") or 0),
                        "retry_count": int(kwargs.get("zero_result_retry_attempts") or 0),
                        "exhausted": True,
                    },
                }

        identity = CompanyIdentity(
            requested_name="Google",
            canonical_name="Google",
            company_key="google",
            linkedin_slug="google",
            linkedin_company_url="https://www.linkedin.com/company/google/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir)
            acquirer = SearchSeedAcquirer([], harvest_search_connector=_FakeHarvestConnector())
            snapshot = acquirer.discover(
                identity,
                snapshot_dir,
                asset_logger=AssetLogger(snapshot_dir),
                search_seed_queries=["Gemini"],
                query_bundles=[],
                filter_hints={
                    "current_companies": ["https://www.linkedin.com/company/google/"],
                    "keywords": ["Gemini"],
                },
                cost_policy={
                    "provider_people_search_mode": "primary_only",
                    "provider_people_search_pages": 1,
                    "provider_people_search_zero_result_retry_attempts": 2,
                    "provider_people_search_query_strategy": "all_queries_union",
                    "provider_people_search_overlap_pruning": False,
                },
                employment_status="current",
            )

        self.assertEqual(snapshot.stop_reason, "provider_people_search_incomplete")
        self.assertEqual(len(snapshot.entries), 0)
        self.assertEqual(snapshot.summary_payload["incomplete_provider_query_count"], 1)
        self.assertEqual(snapshot.query_summaries[0]["status"], "incomplete")
        self.assertEqual(snapshot.query_summaries[0]["incomplete_reason"], "provider_zero_results_after_retry")
        self.assertTrue(snapshot.query_summaries[0]["provider_search_incomplete"])
        self.assertEqual(snapshot.query_summaries[0]["zero_result_retry"]["result"]["attempts"], 2)
        provider_retry_items = collect_search_seed_provider_retry_items(snapshot)
        self.assertEqual(snapshot.summary_payload["provider_retry_item_count"], 1)
        self.assertEqual(snapshot.summary_payload["provider_retry_exhausted_count"], 1)
        self.assertEqual(len(provider_retry_items), 1)
        self.assertEqual(provider_retry_items[0]["item_kind"], "provider_search_retry")
        self.assertEqual(provider_retry_items[0]["provider_retry_type"], "harvest_people_search_zero_result_retry")
        self.assertEqual(provider_retry_items[0]["provider"], "harvest_profile_search")
        self.assertEqual(provider_retry_items[0]["query"], "Gemini")
        self.assertEqual(provider_retry_items[0]["employment_status"], "current")
        self.assertEqual(provider_retry_items[0]["queue_status"], "failed")
        self.assertEqual(provider_retry_items[0]["status"], "exhausted")
        self.assertTrue(provider_retry_items[0]["item_key"])
        self.assertEqual(
            snapshot.query_summaries[0]["provider_retry_items"][0]["item_key"],
            provider_retry_items[0]["item_key"],
        )

    def test_provider_people_search_zero_result_can_be_accepted_for_scoped_lane(self) -> None:
        class _FakeSettings:
            enabled = True
            max_paid_items = 2500

        class _FakeHarvestConnector:
            settings = _FakeSettings()

            def search_profiles(self, **kwargs):
                raw_path = Path(kwargs["discovery_dir"]) / "harvest_zero_after_retry.json"
                return {
                    "raw_path": raw_path,
                    "rows": [],
                    "pagination": {
                        "total_elements": 0,
                        "total_pages": 0,
                        "returned_count": 0,
                    },
                    "zero_result_retry": {
                        "attempts": int(kwargs.get("zero_result_retry_attempts") or 0),
                        "retry_count": int(kwargs.get("zero_result_retry_attempts") or 0),
                        "exhausted": True,
                    },
                }

        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir)
            acquirer = SearchSeedAcquirer([], harvest_search_connector=_FakeHarvestConnector())
            snapshot = acquirer.discover(
                identity,
                snapshot_dir,
                asset_logger=AssetLogger(snapshot_dir),
                search_seed_queries=["Infra"],
                query_bundles=[],
                filter_hints={
                    "current_companies": ["https://www.linkedin.com/company/openai/"],
                    "keywords": ["Infra"],
                },
                cost_policy={
                    "provider_people_search_mode": "primary_only",
                    "provider_people_search_pages": 1,
                    "provider_people_search_zero_result_retry_attempts": 2,
                    "provider_people_search_accept_zero_results": True,
                    "provider_people_search_query_strategy": "all_queries_union",
                    "provider_people_search_overlap_pruning": False,
                },
                employment_status="current",
            )

        self.assertEqual(snapshot.stop_reason, "completed")
        self.assertEqual(len(snapshot.entries), 0)
        self.assertEqual(snapshot.summary_payload["incomplete_provider_query_count"], 0)
        self.assertEqual(snapshot.summary_payload["provider_retry_item_count"], 0)
        self.assertEqual(snapshot.query_summaries[0]["status"], "completed")
        self.assertTrue(snapshot.query_summaries[0]["zero_result_accepted"])
        self.assertEqual(
            snapshot.query_summaries[0]["zero_result_reason"],
            "accepted_scoped_lane_zero_result_after_retry",
        )
        self.assertEqual(collect_search_seed_provider_retry_items(snapshot), [])

    def test_provider_people_search_zero_result_exhaustion_updates_discovery_query_item_owner(self) -> None:
        class _FakeSettings:
            enabled = True
            max_paid_items = 2500

        class _FakeHarvestConnector:
            settings = _FakeSettings()

            def search_profiles(self, **kwargs):
                raw_path = Path(kwargs["discovery_dir"]) / "harvest_zero_after_retry.json"
                return {
                    "raw_path": raw_path,
                    "rows": [],
                    "pagination": {"total_elements": 0, "total_pages": 0, "returned_count": 0},
                    "zero_result_retry": {
                        "attempts": int(kwargs.get("zero_result_retry_attempts") or 0),
                        "retry_count": int(kwargs.get("zero_result_retry_attempts") or 0),
                        "exhausted": True,
                    },
                }

        class _FakeItemStore:
            def __init__(self) -> None:
                self.items: dict[str, dict] = {}

            def upsert_job_materialization_item(self, **kwargs):
                item_id = str(kwargs["item_id"])
                existing = dict(self.items.get(item_id) or {})
                metadata = {**dict(existing.get("metadata") or {}), **dict(kwargs.get("metadata") or {})}
                item = {**existing, **kwargs, "metadata": metadata}
                self.items[item_id] = item
                return item

        class _FakeWorkerRuntime:
            def __init__(self) -> None:
                self.store = _FakeItemStore()

        identity = CompanyIdentity(
            requested_name="Google",
            canonical_name="Google",
            company_key="google",
            linkedin_slug="google",
            linkedin_company_url="https://www.linkedin.com/company/google/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir)
            runtime = _FakeWorkerRuntime()
            # R-010 migration (2026-07-22): retry-wait registration plans a
            # durable workflow command via DurableRuntimeWriter — real store.
            runtime.store = self.enterContext(
                pg_backed_control_plane_store(schema_label="r010_zero_exhaustion")
            )
            acquirer = SearchSeedAcquirer([], harvest_search_connector=_FakeHarvestConnector())
            snapshot = acquirer.discover(
                identity,
                snapshot_dir,
                asset_logger=AssetLogger(snapshot_dir),
                search_seed_queries=["Gemini"],
                query_bundles=[],
                filter_hints={
                    "current_companies": ["https://www.linkedin.com/company/google/"],
                    "keywords": ["Gemini"],
                },
                cost_policy={
                    "provider_people_search_mode": "primary_only",
                    "provider_people_search_pages": 1,
                    "provider_people_search_zero_result_retry_attempts": 2,
                    "provider_people_search_query_strategy": "all_queries_union",
                    "provider_people_search_overlap_pruning": False,
                },
                employment_status="current",
                worker_runtime=runtime,
                job_id="job_google_gemini",
            )

        # R-010 migration (2026-07-22): exhaustion is durably recorded as
        # workflow events (CompletionProofRecorded for terminal statuses).
        run_events = runtime.store.repos.workflow_runtime.list_workflow_events(
            legacy_job_workflow_run_id("job_google_gemini"), limit=0
        )
        exhausted_events = [
            dict(event.get("payload") or {})
            for event in run_events
            if str(dict(event.get("payload") or {}).get("status") or "") == "exhausted"
        ]
        self.assertEqual(len(exhausted_events), 1)
        self.assertEqual(exhausted_events[0]["phase"], "exhausted")
        item_id = str(exhausted_events[0]["item_id"])
        folded_metadata = dict(exhausted_events[0].get("materialization_metadata") or {})
        self.assertTrue(folded_metadata.get("linked_provider_search_retry_required"))
        provider_retry_items = collect_search_seed_provider_retry_items(snapshot)
        self.assertEqual(len(provider_retry_items), 1)
        self.assertEqual(provider_retry_items[0]["owner"], "search_seed_discovery_query")
        self.assertEqual(provider_retry_items[0]["owner_item_id"], item_id)
        self.assertEqual(snapshot.query_summaries[0]["discovery_query_item_id"], item_id)

    def test_provider_people_search_retryable_failure_enters_discovery_query_retry_wait(self) -> None:
        class _FakeSettings:
            enabled = True
            max_paid_items = 2500

        class _FakeHarvestConnector:
            settings = _FakeSettings()

            def search_profiles(self, **kwargs):  # noqa: ARG002
                raise RuntimeError("provider temporary timeout")

        class _FakeItemStore:
            def __init__(self) -> None:
                self.items: dict[str, dict] = {}

            def upsert_job_materialization_item(self, **kwargs):
                item_id = str(kwargs["item_id"])
                existing = dict(self.items.get(item_id) or {})
                metadata = {**dict(existing.get("metadata") or {}), **dict(kwargs.get("metadata") or {})}
                item = {**existing, **kwargs, "metadata": metadata}
                self.items[item_id] = item
                return item

        class _FakeWorkerRuntime:
            def __init__(self) -> None:
                self.store = _FakeItemStore()

        identity = CompanyIdentity(
            requested_name="Google",
            canonical_name="Google",
            company_key="google",
            linkedin_slug="google",
            linkedin_company_url="https://www.linkedin.com/company/google/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir)
            runtime = _FakeWorkerRuntime()
            # R-010 migration (2026-07-22): retry-wait registration plans a
            # durable workflow command via DurableRuntimeWriter — real store.
            runtime.store = self.enterContext(
                pg_backed_control_plane_store(schema_label="r010_retry_wait")
            )
            acquirer = SearchSeedAcquirer([], harvest_search_connector=_FakeHarvestConnector())
            snapshot = acquirer.discover(
                identity,
                snapshot_dir,
                asset_logger=AssetLogger(snapshot_dir),
                search_seed_queries=["Gemini"],
                query_bundles=[],
                filter_hints={"current_companies": ["https://www.linkedin.com/company/google/"]},
                cost_policy={
                    "provider_people_search_mode": "primary_only",
                    "provider_people_search_pages": 1,
                    "provider_people_search_query_strategy": "all_queries_union",
                    "provider_people_search_overlap_pruning": False,
                    "provider_people_search_retry_delay_seconds": 1,
                },
                employment_status="current",
                worker_runtime=runtime,
                job_id="job_google_gemini_retry",
            )

        retry_commands = [
            dict(command)
            for command in runtime.store.list_workflow_commands(limit=50)
            if str(command.get("command_type") or "") == "linkedin.discovery_query.run"
        ]
        self.assertEqual(len(retry_commands), 1)
        self.assertTrue(retry_commands[0]["not_before_at"])
        command_payload = dict(retry_commands[0].get("payload") or {})
        self.assertEqual(command_payload.get("item_kind"), "search_seed_discovery_query")
        self.assertEqual(snapshot.stop_reason, "provider_people_search_incomplete")
        self.assertEqual(snapshot.query_summaries[0]["status"], "retry_wait")
        self.assertTrue(snapshot.query_summaries[0]["provider_search_retryable"])
        self.assertEqual(snapshot.query_summaries[0]["incomplete_reason"], "provider_retry_wait")

    def test_provider_people_search_dedupes_after_effective_harvest_query_normalization(self) -> None:
        class _FakeSettings:
            enabled = True
            max_paid_items = 2500

        class _FakeHarvestConnector:
            settings = _FakeSettings()

            def __init__(self) -> None:
                self.calls: list[tuple[str, int, int]] = []

            def search_profiles(self, **kwargs):
                query_text = str(kwargs.get("query_text") or "").strip()
                limit = int(kwargs.get("limit") or 0)
                pages = int(kwargs.get("pages") or 0)
                self.calls.append((query_text, limit, pages))
                raw_path = Path(kwargs["discovery_dir"]) / f"harvest_{len(self.calls)}.json"
                if limit == 25 and pages == 1:
                    return {
                        "raw_path": raw_path,
                        "rows": [
                            {
                                "full_name": "Probe Candidate",
                                "headline": "Former Gemini researcher",
                                "location": "United States",
                                "profile_url": "https://www.linkedin.com/in/probe-candidate/",
                                "username": "probe-candidate",
                                "current_company": "NewCo",
                            }
                        ],
                        "pagination": {
                            "total_elements": 991,
                            "total_pages": 40,
                            "returned_count": 25,
                        },
                    }
                return {
                    "raw_path": raw_path,
                    "rows": [
                        {
                            "full_name": "Scaled Candidate",
                            "headline": "Former Gemini researcher",
                            "location": "United States",
                            "profile_url": "https://www.linkedin.com/in/scaled-candidate/",
                            "username": "scaled-candidate",
                            "current_company": "NewCo",
                        }
                    ],
                    "pagination": {
                        "total_elements": 991,
                        "total_pages": 40,
                        "returned_count": 1,
                    },
                }

        identity = CompanyIdentity(
            requested_name="Google",
            canonical_name="Google",
            company_key="google",
            linkedin_slug="google",
            linkedin_company_url="https://www.linkedin.com/company/google/",
            aliases=["Google DeepMind"],
        )
        with tempfile.TemporaryDirectory() as tempdir:
            discovery_dir = Path(tempdir)
            harvest_connector = _FakeHarvestConnector()
            acquirer = SearchSeedAcquirer([], harvest_search_connector=harvest_connector)
            entries, summaries, errors, accounts = acquirer._provider_people_search_fallback(
                identity=identity,
                discovery_dir=discovery_dir,
                asset_logger=AssetLogger(discovery_dir),
                search_seed_queries=["Gemini", "Google DeepMind"],
                filter_hints={
                    "past_companies": ["https://www.linkedin.com/company/google/"],
                    "keywords": ["Gemini"],
                    "scope_keywords": ["Gemini", "Google DeepMind"],
                    "function_ids": ["24", "8"],
                },
                employment_status="former",
                limit=991,
                cost_policy={
                    "provider_people_search_pages": 40,
                    "provider_people_search_query_strategy": "all_queries_union",
                },
            )

        self.assertEqual(errors, [])
        self.assertEqual(accounts, ["harvest_profile_search"])
        self.assertEqual(len(entries), 1)
        self.assertEqual([call[0] for call in harvest_connector.calls], ["Gemini", "Gemini"])
        self.assertEqual(harvest_connector.calls, [("Gemini", 25, 1), ("Gemini", 991, 40)])
        self.assertEqual(len([item for item in summaries if item.get("mode") == "harvest_profile_search"]), 1)

    def test_discover_does_not_repoll_ready_cached_entries(self) -> None:
        class _BatchSearchProvider:
            provider_name = "dataforseo_google_organic"

            def __init__(self) -> None:
                self.batch_calls = []
                self.ready_calls = []
                self.fetch_calls = []
                self.execute_calls = []

            def submit_batch_queries(self, query_specs):
                self.batch_calls.append(list(query_specs))
                return SearchBatchSubmissionResult(
                    provider_name=self.provider_name,
                    tasks=[
                        SearchBatchSubmissionTask(
                            task_key=str(item["task_key"]),
                            query_text=str(item["query_text"]),
                            checkpoint={
                                "provider_name": self.provider_name,
                                "task_id": f"task_{index}",
                                "status": "submitted",
                            },
                            metadata={"artifact_label": "task_post_batch_01"},
                        )
                        for index, item in enumerate(query_specs, start=1)
                    ],
                    artifacts=[
                        SearchExecutionArtifact(
                            label="task_post_batch_01",
                            payload={"tasks": [{"id": "task_1"}, {"id": "task_2"}]},
                        )
                    ],
                )

            def poll_ready_batch(self, query_specs):
                self.ready_calls.append([str(item["task_key"]) for item in query_specs])
                if len(self.ready_calls) == 1:
                    return SearchBatchReadyResult(
                        provider_name=self.provider_name,
                        tasks=[
                            SearchBatchReadyTask(
                                task_key=str(query_specs[0]["task_key"]),
                                task_id="task_1",
                                query_text=query_specs[0]["query_text"],
                                checkpoint={
                                    **dict(query_specs[0].get("checkpoint") or {}),
                                    "status": "ready_cached",
                                },
                            ),
                            SearchBatchReadyTask(
                                task_key=str(query_specs[1]["task_key"]),
                                task_id="task_2",
                                query_text=query_specs[1]["query_text"],
                                checkpoint={
                                    **dict(query_specs[1].get("checkpoint") or {}),
                                    "status": "waiting_for_ready_cached",
                                },
                            ),
                        ],
                        artifacts=[
                            SearchExecutionArtifact(
                                label="tasks_ready_batch",
                                payload={"tasks": [{"result": []}]},
                            )
                        ],
                    )
                self.assertEqual(query_specs[0]["task_key"], self.ready_calls[0][1])
                self.assertEqual(len(query_specs), 1)
                return SearchBatchReadyResult(
                    provider_name=self.provider_name,
                    tasks=[
                        SearchBatchReadyTask(
                            task_key=str(query_specs[0]["task_key"]),
                            task_id="task_2",
                            query_text=query_specs[0]["query_text"],
                            checkpoint={
                                **dict(query_specs[0].get("checkpoint") or {}),
                                "status": "waiting_for_ready_cached",
                            },
                        )
                    ],
                    artifacts=[
                        SearchExecutionArtifact(
                            label="tasks_ready_batch",
                            payload={"tasks": [{"result": []}]},
                        )
                    ],
                )

            def fetch_ready_batch(self, query_specs):
                self.fetch_calls.append([str(item["task_key"]) for item in query_specs])
                return SearchBatchFetchResult(
                    provider_name=self.provider_name,
                    tasks=[],
                    artifacts=[],
                    message="no fetched tasks yet",
                )

            def execute_with_checkpoint(self, query_text, *, max_results=10, timeout=None, checkpoint=None):
                self.execute_calls.append({"query_text": query_text, "checkpoint": dict(checkpoint or {})})
                return SearchExecutionResult(
                    provider_name=self.provider_name,
                    query_text=query_text,
                    pending=True,
                    message="waiting for queue",
                    checkpoint=dict(checkpoint or {}),
                )

        class _WorkerHandle:
            def __init__(self, worker_id: int) -> None:
                self.worker_id = worker_id

        class _FakeWorkerRuntime:
            def __init__(self) -> None:
                self._next_worker_id = 0

            def begin_worker(self, **kwargs):
                self._next_worker_id += 1
                return _WorkerHandle(self._next_worker_id)

            def list_workers(self, **kwargs):
                return []

            def get_worker(self, worker_id):
                return {"checkpoint": {}, "output": {}, "status": "queued"}

            def should_interrupt_worker(self, handle):
                return False

            def complete_worker(self, handle, **kwargs):
                return kwargs

            def checkpoint_worker(self, handle, **kwargs):
                return kwargs

        identity = CompanyIdentity(
            requested_name="Thinking Machines Lab",
            canonical_name="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            linkedin_slug="thinkingmachinesai",
            linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir)
            runtime = _FakeWorkerRuntime()
            provider = _BatchSearchProvider()
            acquirer = SearchSeedAcquirer([], search_provider=provider)
            acquirer.discover(
                identity,
                snapshot_dir,
                asset_logger=AssetLogger(snapshot_dir),
                search_seed_queries=[
                    '"Jane Doe" "Thinking Machines Lab" site:linkedin.com/in',
                    '"John Smith" "Thinking Machines Lab" site:linkedin.com/in',
                ],
                filter_hints={},
                cost_policy={
                    "allow_stage1_web_seed_fallback": True,
                    "parallel_search_workers": 2,
                    "public_media_results_per_query": 10,
                },
                employment_status="current",
                worker_runtime=runtime,
                job_id="job_1",
                request_payload={"target_company": "Thinking Machines Lab"},
                plan_payload={},
                runtime_mode="workflow",
            )
            manifest_path = snapshot_dir / "search_seed_discovery" / "web_search_batch_manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            for item in manifest["entries"]:
                search_state = dict(item.get("search_state") or {})
                search_state["ready_attempted_at"] = "2026-01-01T00:00:00+00:00"
                search_state["fetch_attempted_at"] = "2026-01-01T00:00:00+00:00"
                item["search_state"] = search_state
            manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

            acquirer.discover(
                identity,
                snapshot_dir,
                asset_logger=AssetLogger(snapshot_dir),
                search_seed_queries=[
                    '"Jane Doe" "Thinking Machines Lab" site:linkedin.com/in',
                    '"John Smith" "Thinking Machines Lab" site:linkedin.com/in',
                ],
                filter_hints={},
                cost_policy={
                    "allow_stage1_web_seed_fallback": True,
                    "parallel_search_workers": 2,
                    "public_media_results_per_query": 10,
                },
                employment_status="current",
                worker_runtime=runtime,
                job_id="job_1",
                request_payload={"target_company": "Thinking Machines Lab"},
                plan_payload={},
                runtime_mode="workflow",
            )
            expected_keys = [
                _search_seed_worker_key("seed_queries", index, "current", query_text=query)
                for index, query in enumerate(
                    [
                        '"Jane Doe" "Thinking Machines Lab" site:linkedin.com/in',
                        '"John Smith" "Thinking Machines Lab" site:linkedin.com/in',
                    ],
                    start=1,
                )
            ]
            self.assertEqual(provider.ready_calls[0], expected_keys)
            self.assertEqual(provider.ready_calls[1], expected_keys[1:])

    def test_discover_worker_direct_fetch_updates_batch_manifest(self) -> None:
        class _BatchSearchProvider:
            provider_name = "dataforseo_google_organic"

            def __init__(self) -> None:
                self.ready_calls = []
                self.fetch_calls = 0
                self.execute_calls = []

            def submit_batch_queries(self, query_specs):
                return SearchBatchSubmissionResult(
                    provider_name=self.provider_name,
                    tasks=[
                        SearchBatchSubmissionTask(
                            task_key=str(query_specs[0]["task_key"]),
                            query_text=str(query_specs[0]["query_text"]),
                            checkpoint={
                                "provider_name": self.provider_name,
                                "task_id": "task_1",
                                "status": "submitted",
                            },
                            metadata={"artifact_label": "task_post_batch_01"},
                        )
                    ],
                    artifacts=[
                        SearchExecutionArtifact(
                            label="task_post_batch_01",
                            payload={"tasks": [{"id": "task_1"}]},
                        )
                    ],
                )

            def poll_ready_batch(self, query_specs):
                self.ready_calls.append([str(item["task_key"]) for item in query_specs])
                return SearchBatchReadyResult(
                    provider_name=self.provider_name,
                    tasks=[
                        SearchBatchReadyTask(
                            task_key=str(query_specs[0]["task_key"]),
                            task_id="task_1",
                            query_text=str(query_specs[0]["query_text"]),
                            checkpoint={
                                **dict(query_specs[0].get("checkpoint") or {}),
                                "status": "ready_cached",
                            },
                        )
                    ],
                    artifacts=[
                        SearchExecutionArtifact(
                            label="tasks_ready_batch",
                            payload={"tasks": [{"result": []}]},
                        )
                    ],
                )

            def fetch_ready_batch(self, query_specs):
                self.fetch_calls += 1
                return None

            def execute_with_checkpoint(self, query_text, *, max_results=10, timeout=None, checkpoint=None):
                self.execute_calls.append({"query_text": query_text, "checkpoint": dict(checkpoint or {})})
                return SearchExecutionResult(
                    provider_name=self.provider_name,
                    query_text=query_text,
                    response=SearchResponse(
                        provider_name=self.provider_name,
                        query_text=query_text,
                        results=[
                            SearchResultItem(
                                title="Jane Doe - LinkedIn",
                                url="https://www.linkedin.com/in/jane-doe/",
                                snippet="Thinking Machines Lab",
                            )
                        ],
                        raw_payload={"tasks": [{"id": "task_1"}]},
                        raw_format="json",
                    ),
                    checkpoint={
                        "provider_name": self.provider_name,
                        "task_id": "task_1",
                        "status": "completed",
                    },
                )

        class _WorkerHandle:
            def __init__(self, worker_id: int) -> None:
                self.worker_id = worker_id

        class _FakeWorkerRuntime:
            def __init__(self) -> None:
                self._next_worker_id = 0

            def begin_worker(self, **kwargs):
                self._next_worker_id += 1
                return _WorkerHandle(self._next_worker_id)

            def list_workers(self, **kwargs):
                return []

            def get_worker(self, worker_id):
                return {"checkpoint": {}, "output": {}, "status": "queued"}

            def should_interrupt_worker(self, handle):
                return False

            def complete_worker(self, handle, **kwargs):
                return kwargs

            def checkpoint_worker(self, handle, **kwargs):
                return kwargs

        identity = CompanyIdentity(
            requested_name="Thinking Machines Lab",
            canonical_name="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            linkedin_slug="thinkingmachinesai",
            linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir)
            provider = _BatchSearchProvider()
            runtime = _FakeWorkerRuntime()
            acquirer = SearchSeedAcquirer([], search_provider=provider)
            snapshot = acquirer.discover(
                identity,
                snapshot_dir,
                asset_logger=AssetLogger(snapshot_dir),
                search_seed_queries=['"Jane Doe" "Thinking Machines Lab" site:linkedin.com/in'],
                filter_hints={},
                cost_policy={
                    "allow_stage1_web_seed_fallback": True,
                    "parallel_search_workers": 1,
                    "public_media_results_per_query": 10,
                },
                employment_status="current",
                worker_runtime=runtime,
                job_id="job_direct_fetch",
                request_payload={"target_company": "Thinking Machines Lab"},
                plan_payload={},
                runtime_mode="workflow",
            )
            manifest = json.loads(
                (snapshot_dir / "search_seed_discovery" / "web_search_batch_manifest.json").read_text(encoding="utf-8")
            )
            entry = manifest["entries"][0]
            self.assertEqual(provider.execute_calls[0]["checkpoint"]["status"], "ready_cached")
            self.assertEqual(entry["search_state"]["status"], "fetched_cached")
            self.assertTrue(str(entry["search_state"]["fetch_token"]).startswith("worker_direct_"))
            self.assertTrue(str(entry["raw_path"]).endswith("web_query_01.json"))
            self.assertEqual(len(snapshot.entries), 1)

    def test_discover_cached_raw_path_updates_summary_path(self) -> None:
        class _UnusedSearchProvider:
            provider_name = "dataforseo_google_organic"

            def __init__(self) -> None:
                self.execute_calls = 0

            def submit_batch_queries(self, query_specs):
                return SearchBatchSubmissionResult(
                    provider_name=self.provider_name,
                    tasks=[
                        SearchBatchSubmissionTask(
                            task_key=str(query_specs[0]["task_key"]),
                            query_text=str(query_specs[0]["query_text"]),
                            checkpoint={
                                "provider_name": self.provider_name,
                                "task_id": "task_cached",
                                "status": "submitted",
                            },
                            metadata={"artifact_label": "task_post_batch_01"},
                        )
                    ],
                    artifacts=[
                        SearchExecutionArtifact(
                            label="task_post_batch_01",
                            payload={"tasks": [{"id": "task_cached"}]},
                        )
                    ],
                )

            def poll_ready_batch(self, query_specs):
                return SearchBatchReadyResult(
                    provider_name=self.provider_name,
                    tasks=[
                        SearchBatchReadyTask(
                            task_key=str(query_specs[0]["task_key"]),
                            task_id="task_cached",
                            query_text=str(query_specs[0]["query_text"]),
                            checkpoint={
                                **dict(query_specs[0].get("checkpoint") or {}),
                                "status": "ready_cached",
                            },
                        )
                    ],
                    artifacts=[
                        SearchExecutionArtifact(
                            label="tasks_ready_batch",
                            payload={"tasks": [{"result": []}]},
                        )
                    ],
                )

            def fetch_ready_batch(self, query_specs):
                return None

            def execute_with_checkpoint(self, query_text, *, max_results=10, timeout=None, checkpoint=None):
                self.execute_calls += 1
                raise AssertionError("execute_with_checkpoint should not be called when cached raw_path exists")

        class _WorkerHandle:
            def __init__(self, worker_id: int) -> None:
                self.worker_id = worker_id

        class _FakeWorkerRuntime:
            def __init__(self) -> None:
                self._next_worker_id = 0

            def begin_worker(self, **kwargs):
                self._next_worker_id += 1
                return _WorkerHandle(self._next_worker_id)

            def list_workers(self, **kwargs):
                return []

            def get_worker(self, worker_id):
                return {"checkpoint": {}, "output": {}, "status": "queued"}

            def should_interrupt_worker(self, handle):
                return False

            def complete_worker(self, handle, **kwargs):
                return kwargs

            def checkpoint_worker(self, handle, **kwargs):
                return kwargs

        identity = CompanyIdentity(
            requested_name="Thinking Machines Lab",
            canonical_name="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            linkedin_slug="thinkingmachinesai",
            linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
        )
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_dir = Path(tempdir)
            provider = _UnusedSearchProvider()
            runtime = _FakeWorkerRuntime()
            discovery_dir = snapshot_dir / "search_seed_discovery"
            discovery_dir.mkdir(parents=True, exist_ok=True)
            logger = AssetLogger(snapshot_dir)
            cached_raw_path = discovery_dir / "web_query_01.json"
            logger.write_json(
                cached_raw_path,
                search_response_to_record(
                    SearchResponse(
                        provider_name=provider.provider_name,
                        query_text='"Jane Doe" "Thinking Machines Lab"',
                        results=[
                            SearchResultItem(
                                title="Jane Doe - LinkedIn",
                                url="https://www.linkedin.com/in/jane-doe/",
                                snippet="Thinking Machines Lab",
                            )
                        ],
                        raw_payload={"tasks": [{"id": "task_cached"}]},
                        raw_format="json",
                    )
                ),
                asset_type="web_search_payload",
                source_kind="search_seed_discovery",
                is_raw_asset=True,
                model_safe=False,
            )
            manifest_path = discovery_dir / "web_search_batch_manifest.json"
            logger.write_json(
                manifest_path,
                {
                    "provider_name": provider.provider_name,
                    "submitted_query_count": 1,
                    "artifact_paths": {},
                    "entries": [
                        {
                            "task_key": _search_seed_worker_key(
                                "seed_queries", 1, "", query_text='"Jane Doe" "Thinking Machines Lab"'
                            ),
                            "query": '"Jane Doe" "Thinking Machines Lab"',
                            "search_state": {
                                "provider_name": provider.provider_name,
                                "task_id": "task_cached",
                                "status": "ready_cached",
                            },
                            "artifact_paths": {},
                            "raw_path": str(cached_raw_path),
                            "metadata": {},
                        }
                    ],
                    "message": "",
                },
                asset_type="web_search_batch_manifest",
                source_kind="search_seed_discovery",
                is_raw_asset=False,
                model_safe=False,
            )
            acquirer = SearchSeedAcquirer([], search_provider=provider)
            result = acquirer._execute_query_spec(
                index=1,
                query_spec={
                    "bundle_id": "seed_queries",
                    "source_family": "public_web_search",
                    "execution_mode": "low_cost_web_search",
                    "query": '"Jane Doe" "Thinking Machines Lab"',
                },
                identity=identity,
                discovery_dir=discovery_dir,
                logger=logger,
                employment_status="current",
                worker_runtime=runtime,
                job_id="job_cached",
                request_payload={"target_company": "Thinking Machines Lab"},
                plan_payload={},
                runtime_mode="workflow",
                result_limit=10,
                prefetched_search_state={
                    "provider_name": provider.provider_name,
                    "task_id": "task_cached",
                    "status": "ready_cached",
                },
                prefetched_search_artifact_paths={},
                prefetched_search_raw_path=str(cached_raw_path),
                prefetched_search_manifest_path=str(manifest_path),
                prefetched_search_manifest_key=_search_seed_worker_key(
                    "seed_queries", 1, "", query_text='"Jane Doe" "Thinking Machines Lab"'
                ),
            )
            self.assertEqual(provider.execute_calls, 0)
            self.assertEqual(result["worker_status"], "completed")
            self.assertEqual(result["summary"]["raw_path"], str(cached_raw_path))

    def test_provider_query_signature_collapses_hyphen_underscore_case_variants(self) -> None:
        """`_search_query_signature` is the dedupe key. Hyphen / underscore / whitespace /
        case variants must collapse to the same signature so a paid provider is not invoked
        twice for what is the same query family."""

        equivalence_classes = [
            {"Reasoning-Model", "Reasoning_Model", "reasoning model", "REASONING-MODEL"},
            {"Chain-of-thought", "chain of thought", "Chain_of_Thought"},
            {"Vision-language", "vision language", "Vision_Language"},
            {"Inference-time compute", "inference time compute", "INFERENCE-TIME-COMPUTE"},
        ]
        for variants in equivalence_classes:
            signatures = {_search_query_signature(value) for value in variants}
            self.assertEqual(
                len(signatures),
                1,
                f"signatures must collapse for variants={sorted(variants)}, got={signatures}",
            )

    def test_search_seed_worker_key_uses_query_identity_not_order_ordinal(self) -> None:
        first = _search_seed_worker_key(
            "seed_queries",
            1,
            "current",
            query_text='"Jane Doe" "Thinking Machines Lab"',
        )
        reordered = _search_seed_worker_key(
            "seed_queries",
            9,
            "current",
            query_text='"Jane Doe" "Thinking Machines Lab"',
        )
        different_query = _search_seed_worker_key(
            "seed_queries",
            1,
            "current",
            query_text='"Jane Doe" "Thinking Machines Lab" publications',
        )

        self.assertEqual(first, reordered)
        self.assertNotEqual(first, different_query)
        self.assertTrue(first.startswith("current::seed_queries::q_"))
        self.assertNotIn("::01", first)

    def test_provider_query_family_key_aliases_canonicalize_synonyms(self) -> None:
        """`_provider_query_family_key` is what the dedupe set in `_resolve_provider_people_search_queries`
        keys on. Alias-mappable forms (`Reasoning-Model` → `Reasoning`, `post training` → `Post-train`)
        must collapse to a single family key so a paid provider is not invoked twice for what is
        the same canonical term."""

        equivalence_classes = [
            {"Reasoning Model", "Reasoning Models", "reasoning model"},
            {"post train", "post-training"},
            {"pre train", "pre-training"},
            {"chain of thought", "chain-of-thought"},
            {"inference time compute", "inference-time compute"},
            {"vision language", "vision-language"},
        ]
        for variants in equivalence_classes:
            family_keys = {_provider_query_family_key(value) for value in variants}
            self.assertEqual(
                len(family_keys),
                1,
                f"alias-mapped variants must collapse: variants={sorted(variants)}, family_keys={family_keys}",
            )

    def test_resolve_provider_people_search_queries_dedupes_alias_and_case_variants(self) -> None:
        """End-to-end invariant for `_resolve_provider_people_search_queries`: feeding it
        keywords + scope_keywords + search_seed_queries that are all variants of the same
        canonical term must yield at most one provider query."""

        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        filter_hints = {
            "keywords": ["Reasoning-Model"],
            "scope_keywords": ["reasoning_model", "Reasoning Models"],
            "past_companies": [],
        }
        queries = _resolve_provider_people_search_queries(
            identity=identity,
            filter_hints=filter_hints,
            search_seed_queries=["REASONING MODEL"],
        )
        self.assertEqual(
            len(queries),
            1,
            f"alias-equivalent inputs must yield exactly one provider query, got={queries}",
        )

    def test_resolve_provider_people_search_queries_dedupes_hyphen_underscore_variants(self) -> None:
        """Hyphen vs underscore vs whitespace variants of the same scope keyword must collapse
        to a single provider query when they have no canonical alias entry."""

        identity = CompanyIdentity(
            requested_name="Meta",
            canonical_name="Meta",
            company_key="meta",
            linkedin_slug="meta",
            linkedin_company_url="https://www.linkedin.com/company/meta/",
        )
        filter_hints = {
            "keywords": ["Speech-to-text"],
            "scope_keywords": ["speech_to_text", "speech to text"],
            "past_companies": [],
        }
        queries = _resolve_provider_people_search_queries(
            identity=identity,
            filter_hints=filter_hints,
            search_seed_queries=["SPEECH-TO-TEXT"],
        )
        self.assertEqual(
            len(queries),
            1,
            f"hyphen/underscore/whitespace variants must collapse: got={queries}",
        )

    def test_resolve_provider_people_search_queries_ignores_target_company_scope_keyword(self) -> None:
        """The target company filter is not a paid profile-search query.

        A scoped-search task can carry `scope_keywords` for attribution or legacy planning
        metadata. If that value is just the target company, sending it through Harvest as
        another query makes `_normalize_harvest_query_text` strip the company name and fall
        back to `filter_hints.keywords`, which previously produced duplicate paid probes like
        `Coding research` after the real `Coding` current/former probes.
        """

        identity = CompanyIdentity(
            requested_name="PostHog",
            canonical_name="PostHog",
            company_key="posthog",
            linkedin_slug="posthog",
            linkedin_company_url="https://www.linkedin.com/company/posthog/",
        )
        filter_hints = {
            "current_companies": ["PostHog"],
            "keywords": ["Coding", "research"],
            "scope_keywords": ["PostHog", "https://www.linkedin.com/company/posthog/"],
        }
        queries = _resolve_provider_people_search_queries(
            identity=identity,
            filter_hints=filter_hints,
            search_seed_queries=["Coding"],
        )
        self.assertEqual(queries, ["Coding"])

    def test_resolve_provider_people_search_queries_ignores_generic_full_roster_seed_queries(self) -> None:
        """Generic full-roster seed labels are not Harvest profile-search keywords.

        Historical plans can still contain values such as `Lovable Employee` from the
        old low-cost seed-discovery field. For a full-company roster, company identity
        and company filters own provider execution; these labels must not become
        `searchQuery` / search keyword payload values.
        """

        identity = CompanyIdentity(
            requested_name="Lovable",
            canonical_name="Lovable",
            company_key="lovable",
            linkedin_slug="lovable-dev",
            linkedin_company_url="https://www.linkedin.com/company/lovable-dev/",
        )
        queries = _resolve_provider_people_search_queries(
            identity=identity,
            filter_hints={
                "current_companies": ["https://www.linkedin.com/company/lovable-dev/"],
                "past_companies": ["https://www.linkedin.com/company/lovable-dev/"],
            },
            search_seed_queries=["Lovable Employee", "Lovable LinkedIn Employee"],
        )

        self.assertEqual(queries, [])

    def test_normalize_harvest_query_text_keyword_fallback_drops_generic_role_terms(self) -> None:
        """When a query normalizes to the target company, fallback keywords must stay thematic.

        `research` / `researcher` are role filters backed by functionIds and scoring; they
        should not be appended to the provider-facing search text for an explicit user shard
        such as `Coding`.
        """

        identity = CompanyIdentity(
            requested_name="PostHog",
            canonical_name="PostHog",
            company_key="posthog",
            linkedin_slug="posthog",
            linkedin_company_url="https://www.linkedin.com/company/posthog/",
        )
        normalized = _normalize_harvest_query_text(
            query_text="PostHog",
            filter_hints={
                "current_companies": ["PostHog"],
                "keywords": ["Coding", "research"],
            },
            identity=identity,
        )
        self.assertEqual(normalized, "Coding")

    def test_normalize_harvest_query_text_is_alias_canonical(self) -> None:
        """The provider-facing query text emitted by `_normalize_harvest_query_text` must use
        the canonical alias form when one exists. Without this, the provider sees raw variants
        like `Reasoning-Model` even though dedupe collapsed them — wasting any per-variant
        provider response cache and making query attribution noisy in summaries."""

        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        filter_hints = {"keywords": ["Reasoning"]}
        cases = [
            ("Reasoning-Model", "Reasoning"),
            ("Reasoning_Model", "Reasoning"),
            ("Reasoning Models", "Reasoning"),
            ("reasoning model", "Reasoning"),
            ("post-training", "Post-train"),
            ("post train", "Post-train"),
            ("chain of thought", "Chain-of-thought"),
        ]
        for raw, expected in cases:
            normalized = _normalize_harvest_query_text(
                query_text=raw,
                filter_hints=filter_hints,
                identity=identity,
            )
            self.assertEqual(
                normalized,
                expected,
                f"raw={raw!r} should normalize to canonical {expected!r}, got {normalized!r}",
            )
