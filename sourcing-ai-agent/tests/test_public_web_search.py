import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from sourcing_agent.asset_logger import AssetLogger
from sourcing_agent.document_extraction import extract_page_signals
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.public_web_search import (
    ClassifiedEntryLink,
    PublicWebCandidateContext,
    PublicWebExperimentOptions,
    _primary_links,
    adjudicate_public_web_candidate_evidence,
    adjudicate_public_web_signals,
    apply_email_adjudication,
    apply_link_adjudication,
    build_candidate_adjudication_context,
    build_diversified_fetch_queue,
    candidate_context_from_target_candidate,
    classify_entry_links_from_document_signals,
    classify_public_web_url,
    extract_email_candidate_signals,
    fetch_candidate_public_web_documents,
    is_clean_profile_link,
    load_target_candidates_from_json,
    normalize_model_link_signal_type,
    plan_candidate_public_web_queries,
    public_web_link_shape_warnings,
    rank_entry_links,
    run_target_candidate_public_web_experiment,
    sanitize_public_web_adjudication_for_payload,
    select_entry_links_for_adjudication,
    should_prioritize_discovered_fetch_link,
)
from sourcing_agent.search_provider import (
    BaseSearchProvider,
    SearchBatchFetchResult,
    SearchBatchFetchTask,
    SearchBatchReadyResult,
    SearchBatchReadyTask,
    SearchBatchSubmissionResult,
    SearchBatchSubmissionTask,
    SearchResponse,
    SearchResultItem,
)
from sourcing_agent.web_fetch import FetchedTextAsset


class _FakeSearchProvider(BaseSearchProvider):
    provider_name = "fake_search"

    def search(self, query_text: str, *, max_results: int = 10, timeout: int | None = None) -> SearchResponse:
        del timeout
        results = [
            SearchResultItem(
                title="Ada Lovelace homepage",
                url="https://ada.example.edu/~ada/",
                snippet="Ada Lovelace research engineer at Example AI. Contact and publications.",
            ),
            SearchResultItem(
                title="Ada Lovelace CV",
                url="https://ada.example.edu/~ada/cv.pdf",
                snippet="Curriculum vitae for Ada Lovelace.",
            ),
            SearchResultItem(
                title="Ada Lovelace GitHub",
                url="https://github.com/ada-lovelace",
                snippet="Research code and projects.",
            ),
        ]
        return SearchResponse(
            provider_name=self.provider_name,
            query_text=query_text,
            results=results[:max_results],
            raw_payload={"query": query_text, "results": [item.to_record() for item in results[:max_results]]},
            raw_format="json",
        )


class _GitHubOnlySearchProvider(BaseSearchProvider):
    provider_name = "fake_github_search"

    def search(self, query_text: str, *, max_results: int = 10, timeout: int | None = None) -> SearchResponse:
        del timeout
        results = [
            SearchResultItem(
                title="Noah Yonack · GitHub",
                url="https://github.com/noahyonack",
                snippet="Noah Yonack. Data Scientist. Harvard '17. San Francisco, CA.",
            )
        ]
        return SearchResponse(
            provider_name=self.provider_name,
            query_text=query_text,
            results=results[:max_results],
            raw_payload={"query": query_text, "results": [item.to_record() for item in results[:max_results]]},
            raw_format="json",
        )


class _BatchFakeSearchProvider(BaseSearchProvider):
    provider_name = "fake_batch_search"

    def __init__(self, *, fail_record_id: str = "") -> None:
        self.fail_record_id = fail_record_id
        self.search_calls = 0
        self.submit_calls = 0
        self.poll_calls = 0
        self.fetch_calls = 0

    def search(self, query_text: str, *, max_results: int = 10, timeout: int | None = None) -> SearchResponse:
        del query_text, max_results, timeout
        self.search_calls += 1
        raise AssertionError("batch execution should not call synchronous search")

    def submit_batch_queries(self, query_specs: list[dict]) -> SearchBatchSubmissionResult | None:
        self.submit_calls += 1
        tasks = [
            SearchBatchSubmissionTask(
                task_key=str(spec.get("task_key") or ""),
                query_text=str(spec.get("query_text") or ""),
                checkpoint={"status": "submitted", "task_id": f"task-{index:02d}"},
                metadata=dict(spec.get("metadata") or {}),
            )
            for index, spec in enumerate(query_specs, start=1)
            if str(spec.get("task_key") or "").strip()
        ]
        return SearchBatchSubmissionResult(provider_name=self.provider_name, tasks=tasks)

    def poll_ready_batch(self, query_specs: list[dict]) -> SearchBatchReadyResult | None:
        self.poll_calls += 1
        tasks = [
            SearchBatchReadyTask(
                task_key=str(spec.get("task_key") or ""),
                task_id=str(dict(spec.get("checkpoint") or {}).get("task_id") or ""),
                query_text=str(spec.get("query_text") or ""),
                checkpoint={**dict(spec.get("checkpoint") or {}), "status": "ready_cached"},
                metadata={"ready": True, **dict(spec.get("metadata") or {})},
            )
            for spec in query_specs
        ]
        return SearchBatchReadyResult(provider_name=self.provider_name, tasks=tasks)

    def fetch_ready_batch(self, query_specs: list[dict]) -> SearchBatchFetchResult | None:
        self.fetch_calls += 1
        tasks: list[SearchBatchFetchTask] = []
        for spec in query_specs:
            metadata = dict(spec.get("metadata") or {})
            task_key = str(spec.get("task_key") or "")
            query_text = str(spec.get("query_text") or "")
            response = None
            if metadata.get("record_id") != self.fail_record_id:
                response = SearchResponse(
                    provider_name=self.provider_name,
                    query_text=query_text,
                    results=[
                        SearchResultItem(
                            title=f"{metadata.get('record_id')} homepage",
                            url=f"https://{metadata.get('record_id')}.example.edu/",
                            snippet="Candidate homepage with publications and contact.",
                        )
                    ],
                    raw_payload={"task_key": task_key},
                    raw_format="json",
                )
            tasks.append(
                SearchBatchFetchTask(
                    task_key=task_key,
                    task_id=str(dict(spec.get("checkpoint") or {}).get("task_id") or ""),
                    query_text=query_text,
                    response=response,
                    checkpoint={**dict(spec.get("checkpoint") or {}), "status": "fetched_cached"},
                    metadata=metadata,
                )
            )
        return SearchBatchFetchResult(provider_name=self.provider_name, tasks=tasks)


class _ExplodingLiveModelClient:
    def provider_name(self) -> str:
        return "live_fake_model"

    def analyze_public_web_candidate_signals(self, payload: dict) -> dict:
        del payload
        raise AssertionError("auto mode should not call AI for entry-link-only evidence")


class _CountingPublicWebModelClient:
    def __init__(self) -> None:
        self.page_calls = 0
        self.candidate_calls = 0
        self.payloads: list[dict] = []

    def provider_name(self) -> str:
        return "live_fake_model"

    def analyze_page_asset(self, payload: dict) -> dict:
        del payload
        self.page_calls += 1
        raise AssertionError("Public Web fetch should not call per-document analyze_page_asset")

    def analyze_public_web_candidate_signals(self, payload: dict) -> dict:
        self.candidate_calls += 1
        self.payloads.append(payload)
        return {
            "summary": "candidate-level adjudication",
            "email_assessments": [],
            "link_assessments": [],
            "notes": [],
        }


class PublicWebSearchTest(unittest.TestCase):
    def test_query_planning_uses_target_candidate_source_families(self) -> None:
        candidate = PublicWebCandidateContext(
            record_id="target-1",
            candidate_id="cand-1",
            candidate_name="Ada Lovelace",
            current_company="Example AI",
            headline="Research Engineer",
        )

        queries = plan_candidate_public_web_queries(candidate, max_queries=10)

        self.assertEqual(len(queries), 10)
        self.assertEqual(queries[0].source_family, "profile_web_presence")
        self.assertEqual(queries[1].source_family, "profile_web_presence")
        self.assertEqual(queries[2].source_family, "profile_web_presence")
        self.assertEqual(queries[3].source_family, "scholar_profile_discovery")
        self.assertEqual(queries[4].source_family, "technical_presence")
        self.assertEqual(queries[7].source_family, "social_presence")
        self.assertIn('"Example AI" site:github.com', queries[4].query_text)
        self.assertIn('site:github.com', queries[5].query_text)
        self.assertFalse(any(query.source_family == "resume_and_documents" for query in queries))
        self.assertTrue(any("site:scholar.google.com/citations" in query.query_text for query in queries))
        self.assertTrue(any("site:github.com" in query.query_text for query in queries))
        self.assertTrue(any("site:x.com" in query.query_text for query in queries))
        self.assertTrue(any("site:substack.com" in query.query_text for query in queries))
        self.assertFalse(any("newsletter" in query.query_text.lower() for query in queries))
        self.assertFalse(any(" OR blog" in query.query_text for query in queries))
        self.assertFalse(any(query.source_family == "contact_signals" for query in queries))
        self.assertEqual(len({query.query_text.lower() for query in queries}), len(queries))

    def test_target_candidate_context_preserves_profile_context_for_ai(self) -> None:
        candidate = candidate_context_from_target_candidate(
            {
                "id": "target-1",
                "candidate_id": "cand-1",
                "candidate_name": "Ada Lovelace",
                "current_company": "Example AI",
                "headline": "Research Engineer",
                "linkedin_url": "https://www.linkedin.com/in/ada-lovelace/",
                "experience_lines": ["2024~Present, Example AI, Research Engineer"],
                "education_lines": ["2018~2022, BS, MIT, Computer Science"],
            }
        )

        context = build_candidate_adjudication_context(candidate)

        self.assertEqual(
            context["known_profile_context"]["work_history"],
            ["2024~Present, Example AI, Research Engineer"],
        )
        self.assertEqual(
            context["known_profile_context"]["education"],
            ["2018~2022, BS, MIT, Computer Science"],
        )
        self.assertEqual(
            context["profile_identity"]["linkedin_url_key"],
            "https://www.linkedin.com/in/ada-lovelace",
        )

    def test_classify_and_rank_entry_links(self) -> None:
        candidate = PublicWebCandidateContext(
            record_id="target-1",
            candidate_id="cand-1",
            candidate_name="Ada Lovelace",
            current_company="Example AI",
        )
        links = [
            classify_public_web_url(
                url="https://scholar.google.com/citations?user=abc",
                title="Ada Lovelace - Google Scholar",
                snippet="Example AI publications",
                candidate=candidate,
                result_rank=1,
            ),
            classify_public_web_url(
                url="https://github.com/ada-lovelace",
                title="ada-lovelace",
                snippet="Ada Lovelace research code",
                candidate=candidate,
                result_rank=2,
            ),
            classify_public_web_url(
                url="https://ada.example.edu/cv.pdf",
                title="Ada Lovelace CV",
                snippet="Curriculum vitae",
                candidate=candidate,
                result_rank=3,
            ),
        ]

        ranked = rank_entry_links([link for link in links if link is not None])

        self.assertEqual(ranked[0].entry_type, "resume_url")
        self.assertTrue(any(link.entry_type == "scholar_url" and link.fetchable for link in ranked))
        self.assertTrue(any(link.entry_type == "github_url" and link.fetchable for link in ranked))

    def test_fetch_queue_diversifies_source_types_before_fetch_limit(self) -> None:
        candidate = PublicWebCandidateContext(
            record_id="target-1",
            candidate_id="cand-1",
            candidate_name="Ada Lovelace",
            current_company="Example AI",
        )
        links = [
            classify_public_web_url(
                url=f"https://scholar.google.com/citations?user={index}",
                title="Ada Lovelace - Google Scholar",
                snippet="Example AI publications",
                candidate=candidate,
                result_rank=index,
            )
            for index in range(5)
        ]
        links.extend(
            [
                classify_public_web_url(
                    url="https://github.com/ada-lovelace",
                    title="Ada Lovelace",
                    snippet="Example AI projects",
                    candidate=candidate,
                    result_rank=6,
                ),
                classify_public_web_url(
                    url="https://adalovelace.ai/",
                    title="Ada Lovelace",
                    snippet="Example AI homepage",
                    candidate=candidate,
                    result_rank=7,
                ),
            ]
        )
        ranked = rank_entry_links([link for link in links if link is not None], limit=10)
        fetch_queue = build_diversified_fetch_queue(ranked, max_fetches=4)

        self.assertLessEqual([link.entry_type for link in fetch_queue[:4]].count("scholar_url"), 2)
        self.assertIn("github_url", [link.entry_type for link in fetch_queue[:4]])
        self.assertIn("personal_homepage", [link.entry_type for link in fetch_queue[:4]])

    def test_fetch_queue_respects_zero_fetch_budget(self) -> None:
        candidate = PublicWebCandidateContext(
            record_id="target-1",
            candidate_id="cand-1",
            candidate_name="Ada Lovelace",
            current_company="Example AI",
        )
        link = classify_public_web_url(
            url="https://github.com/ada-lovelace",
            title="Ada Lovelace",
            snippet="Example AI projects",
            candidate=candidate,
            result_rank=1,
        )
        self.assertIsNotNone(link)

        self.assertEqual(build_diversified_fetch_queue([link], max_fetches=0), [])

    def test_fetch_documents_uses_bounded_concurrency(self) -> None:
        candidate = PublicWebCandidateContext(
            record_id="target-1",
            candidate_id="cand-1",
            candidate_name="Ada Lovelace",
            current_company="Example AI",
        )
        links = [
            ClassifiedEntryLink(
                url=f"https://ada.example.edu/{index}",
                normalized_url=f"https://ada.example.edu/{index}",
                title="Ada Lovelace",
                snippet="Example AI homepage.",
                source_domain="ada.example.edu",
                entry_type="personal_homepage",
                source_family="profile_web_presence",
                score=10 - index,
                fetchable=True,
            )
            for index in range(3)
        ]
        active = 0
        max_active = 0
        lock = threading.Lock()

        def fake_analyze_remote_document(**kwargs):
            nonlocal active, max_active
            with lock:
                active += 1
                max_active = max(max_active, active)
            time.sleep(0.03)
            with lock:
                active -= 1
            source_url = str(kwargs.get("source_url") or "")
            return SimpleNamespace(
                source_url=source_url,
                final_url=source_url,
                document_type="html",
                content_type="text/html",
                raw_path="",
                extracted_text_path="",
                analysis_input_path="",
                analysis_path="",
                evidence_slice_path="",
                title="Ada Lovelace",
                signals={"descriptions": []},
                analysis={},
                evidence_slice={},
            )

        with tempfile.TemporaryDirectory() as tempdir:
            with patch("sourcing_agent.public_web_search.analyze_remote_document", side_effect=fake_analyze_remote_document):
                result = fetch_candidate_public_web_documents(
                    ranked_links=links,
                    candidate=candidate,
                    candidate_dir=Path(tempdir),
                    logger=AssetLogger(Path(tempdir)),
                    model_client=None,
                    options=PublicWebExperimentOptions(
                        max_fetches_per_candidate=3,
                        max_concurrent_fetches_per_candidate=2,
                        extract_contact_signals=False,
                    ),
                )

        self.assertEqual(len(result["fetched_documents"]), 3)
        self.assertEqual(max_active, 2)

    def test_fetch_queue_prefers_one_per_available_media_type_before_duplicates(self) -> None:
        candidate = PublicWebCandidateContext(
            record_id="target-1",
            candidate_id="cand-1",
            candidate_name="Ada Lovelace",
            current_company="Example AI",
        )
        raw_links = [
            ("https://adalovelace.dev/", "Ada Lovelace", "Example AI homepage"),
            ("https://adalovelace.dev/blog", "Ada Lovelace", "Example AI homepage"),
            ("https://scholar.google.com/citations?user=ada", "Ada Lovelace - Google Scholar", "Example AI"),
            ("https://github.com/ada-lovelace", "Ada Lovelace", "Example AI projects"),
            ("https://x.com/ada_lovelace", "Ada Lovelace (@ada_lovelace) / X", "Example AI"),
            ("https://ada-lovelace.substack.com/", "Ada Lovelace | Substack", "Example AI"),
            ("https://ada.example.edu/cv.pdf", "Ada Lovelace CV", "Curriculum vitae"),
            ("https://profiles.example.edu/people/ada-lovelace", "Ada Lovelace", "University profile"),
            ("https://arxiv.org/abs/1234.5678", "Ada Lovelace publication", "Example AI publication"),
        ]
        ranked = rank_entry_links(
            [
                link
                for rank, (url, title, snippet) in enumerate(raw_links, start=1)
                if (
                    link := classify_public_web_url(
                        url=url,
                        title=title,
                        snippet=snippet,
                        candidate=candidate,
                        result_rank=rank,
                    )
                )
                is not None
            ],
            limit=20,
        )

        fetch_queue = build_diversified_fetch_queue(ranked, max_fetches=8)
        front_types = [link.entry_type for link in fetch_queue[:8]]

        self.assertEqual(
            front_types,
            [
                "personal_homepage",
                "scholar_url",
                "github_url",
                "x_url",
                "substack_url",
                "resume_url",
                "academic_profile",
                "publication_url",
            ],
        )

    def test_model_link_signal_type_aliases_are_normalized(self) -> None:
        self.assertEqual(normalize_model_link_signal_type("scholar_profile", fallback="other"), "scholar_url")
        self.assertEqual(normalize_model_link_signal_type("github_repository", fallback="other"), "github_url")
        self.assertEqual(normalize_model_link_signal_type("personal_website", fallback="other"), "personal_homepage")
        self.assertEqual(normalize_model_link_signal_type("unexpected", fallback="github_url"), "github_url")

    def test_url_shape_warnings_distinguish_profiles_from_deep_links(self) -> None:
        self.assertEqual(public_web_link_shape_warnings("x_url", "https://x.com/ada_ai"), [])
        self.assertIn(
            "x_link_not_profile",
            public_web_link_shape_warnings("x_url", "https://x.com/ada_ai/status/123"),
        )
        self.assertEqual(public_web_link_shape_warnings("substack_url", "https://ada.substack.com/"), [])
        self.assertIn(
            "substack_link_not_profile_or_publication",
            public_web_link_shape_warnings("substack_url", "https://ada.substack.com/p/post-title"),
        )
        self.assertEqual(
            public_web_link_shape_warnings("scholar_url", "https://scholar.google.com/citations?user=abc"),
            [],
        )
        self.assertIn(
            "scholar_link_not_profile",
            public_web_link_shape_warnings(
                "scholar_url",
                "https://scholar.google.com/citations?view_op=view_citation&citation_for_view=abc:def",
            ),
        )

    def test_link_adjudication_keeps_identity_label_but_marks_dirty_url_shape(self) -> None:
        link = ClassifiedEntryLink(
            url="https://x.com/ada_ai/status/123",
            normalized_url="https://x.com/ada_ai/status/123",
            title="Ada AI on X",
            snippet="Ada Lovelace post about Example AI.",
            source_domain="x.com",
            entry_type="x_url",
            source_family="social_presence",
            score=50,
            fetchable=True,
        )

        [updated] = apply_link_adjudication(
            [link],
            {
                "link_assessments": [
                    {
                        "url": "https://x.com/ada_ai/status/123",
                        "signal_type": "x_url",
                        "identity_match_label": "likely_same_person",
                        "identity_match_score": 0.78,
                    }
                ]
            },
        )

        self.assertEqual(updated.identity_match_label, "likely_same_person")
        self.assertFalse(updated.adjudication["clean_profile_link"])
        self.assertIn("x_link_not_profile", updated.adjudication["link_shape_warnings"])

    def test_primary_links_exclude_likely_same_person_deep_links(self) -> None:
        dirty_x_post = ClassifiedEntryLink(
            url="https://x.com/ada_ai/status/123",
            normalized_url="https://x.com/ada_ai/status/123",
            title="Ada AI on X",
            snippet="Ada Lovelace post about Example AI.",
            source_domain="x.com",
            entry_type="x_url",
            source_family="social_presence",
            score=90,
            identity_match_label="likely_same_person",
            identity_match_score=0.8,
            fetchable=True,
        )
        clean_x_profile = ClassifiedEntryLink(
            url="https://x.com/ada_ai",
            normalized_url="https://x.com/ada_ai",
            title="Ada AI on X",
            snippet="Ada Lovelace Example AI profile.",
            source_domain="x.com",
            entry_type="x_url",
            source_family="social_presence",
            score=80,
            identity_match_label="likely_same_person",
            identity_match_score=0.8,
            fetchable=True,
        )

        primary = _primary_links([dirty_x_post, clean_x_profile], {})

        self.assertFalse(is_clean_profile_link(dirty_x_post.entry_type, dirty_x_post.normalized_url))
        self.assertEqual(primary["x_url"], "https://x.com/ada_ai")

    def test_public_web_adjudication_sanitizer_drops_model_invented_email_and_normalizes_links(self) -> None:
        payload = {
            "email_candidates": [],
            "entry_links": [
                {
                    "url": "https://scholar.google.com/citations?user=abc",
                    "normalized_url": "https://scholar.google.com/citations?user=abc",
                }
            ],
        }
        adjudication = {
            "summary": "Qwen result",
            "email_assessments": [
                {
                    "email": "email@periodic.com",
                    "email_type": "company",
                    "publishable": True,
                    "promotion_status": "promotion_recommended",
                }
            ],
            "link_assessments": [
                {
                    "url": "https://scholar.google.com/citations?user=abc",
                    "signal_type": "scholar_profile",
                    "identity_match_label": "likely_same_person",
                },
                {
                    "url": "https://github.com/unrelated",
                    "signal_type": "github_profile",
                    "identity_match_label": "confirmed",
                },
            ],
        }

        sanitized = sanitize_public_web_adjudication_for_payload(adjudication, payload)

        self.assertEqual(sanitized["email_assessments"], [])
        self.assertEqual(len(sanitized["link_assessments"]), 1)
        self.assertEqual(sanitized["link_assessments"][0]["signal_type"], "scholar_url")

    def test_discovered_homepage_links_are_only_prioritized_from_scholar(self) -> None:
        candidate = PublicWebCandidateContext(
            record_id="target-1",
            candidate_id="cand-1",
            candidate_name="Ada Lovelace",
            current_company="Example AI",
        )
        homepage = classify_public_web_url(
            url="https://adalovelace.ai/",
            title="Ada Lovelace",
            snippet="Discovered from https://scholar.google.com/citations?user=abc",
            candidate=candidate,
            result_rank=0,
        )
        blog = classify_public_web_url(
            url="https://adalovelace.ai/blog",
            title="Ada Lovelace Blog",
            snippet="Discovered from https://adalovelace.ai/",
            candidate=candidate,
            result_rank=0,
        )
        self.assertIsNotNone(homepage)
        self.assertIsNotNone(blog)

        self.assertTrue(
            should_prioritize_discovered_fetch_link(
                homepage,
                source_url="https://scholar.google.com/citations?user=abc",
            )
        )
        self.assertFalse(
            should_prioritize_discovered_fetch_link(
                blog,
                source_url="https://adalovelace.ai/",
            )
        )
        github = classify_public_web_url(
            url="https://github.com/adalovelace",
            title="Ada Lovelace",
            snippet="Discovered from https://adalovelace.ai/",
            candidate=candidate,
            result_rank=0,
        )
        self.assertIsNotNone(github)
        self.assertFalse(
            should_prioritize_discovered_fetch_link(
                github,
                source_url="https://adalovelace.ai/",
            )
        )

    def test_entry_link_adjudication_payload_is_source_balanced(self) -> None:
        links: list[ClassifiedEntryLink] = []
        for index in range(40):
            links.append(
                ClassifiedEntryLink(
                    url=f"https://github.com/example/repo-{index}",
                    normalized_url=f"https://github.com/example/repo-{index}",
                    title=f"GitHub repo {index}",
                    snippet="Ada Lovelace mentioned in a repository.",
                    source_domain="github.com",
                    entry_type="github_url",
                    source_family="technical_presence",
                    score=100 - index,
                    provider_name="dataforseo_google_organic",
                    result_rank=index + 1,
                    fetchable=True,
                )
            )
        for entry_type, url in [
            ("x_url", "https://x.com/ada_ai"),
            ("substack_url", "https://ada.substack.com/"),
            ("scholar_url", "https://scholar.google.com/citations?user=abc"),
            ("personal_homepage", "https://ada.example.edu/"),
        ]:
            links.append(
                ClassifiedEntryLink(
                    url=url,
                    normalized_url=url,
                    title="Ada Lovelace",
                    snippet="Ada Lovelace Example AI public profile.",
                    source_domain=url.split("/")[2],
                    entry_type=entry_type,
                    source_family="social_presence" if entry_type in {"x_url", "substack_url"} else "profile_web_presence",
                    score=20,
                    provider_name="dataforseo_google_organic",
                    result_rank=50,
                    fetchable=True,
                )
            )

        selected = select_entry_links_for_adjudication(links, limit=20)
        selected_types = {link.entry_type for link in selected}

        self.assertLessEqual(len(selected), 20)
        self.assertIn("x_url", selected_types)
        self.assertIn("substack_url", selected_types)
        self.assertIn("scholar_url", selected_types)
        self.assertIn("personal_homepage", selected_types)
        self.assertLess(sum(1 for link in selected if link.entry_type == "github_url"), len(selected))

    def test_x_search_pages_are_not_candidate_social_links(self) -> None:
        candidate = PublicWebCandidateContext(
            record_id="target-1",
            candidate_id="cand-1",
            candidate_name="Ada Lovelace",
            current_company="Example AI",
        )
        profile = classify_public_web_url(
            url="https://x.com/ada_lovelace",
            title="Ada Lovelace (@ada_lovelace) / X",
            snippet="Example AI research engineer.",
            candidate=candidate,
            result_rank=1,
        )
        search_page = classify_public_web_url(
            url="https://x.com/search?q=Ada%20Lovelace&src=typed_query&f=user",
            title='"Ada Lovelace" - Results on X',
            snippet="Results for Ada Lovelace on X.",
            candidate=candidate,
            result_rank=2,
        )

        self.assertIsNotNone(profile)
        self.assertEqual(profile.entry_type, "x_url")
        self.assertIsNotNone(search_page)
        self.assertEqual(search_page.entry_type, "other")

    def test_plain_pdf_is_not_classified_as_resume_without_resume_signal(self) -> None:
        candidate = PublicWebCandidateContext(
            record_id="target-1",
            candidate_id="cand-1",
            candidate_name="Ada Lovelace",
            current_company="Example AI",
        )

        paper = classify_public_web_url(
            url="https://aclanthology.org/2020.emnlp-main.437.pdf",
            title="A paper by Ada Lovelace",
            snippet="Example AI publication",
            candidate=candidate,
            result_rank=1,
        )
        cv = classify_public_web_url(
            url="https://ada.example.edu/cv.pdf",
            title="Ada Lovelace CV",
            snippet="Curriculum vitae",
            candidate=candidate,
            result_rank=2,
        )

        self.assertIsNotNone(paper)
        self.assertEqual(paper.entry_type, "publication_url")
        self.assertIsNotNone(cv)
        self.assertEqual(cv.entry_type, "resume_url")

    def test_company_and_contact_aggregator_pages_are_not_personal_homepages(self) -> None:
        candidate = PublicWebCandidateContext(
            record_id="target-1",
            candidate_id="cand-1",
            candidate_name="Costa Huang",
            current_company="Periodic Labs",
        )

        company_page = classify_public_web_url(
            url="https://periodic.com/",
            title="Periodic Labs",
            snippet="Costa Huang, Dogus Cubuk, Dzmitry Bahdanau. Periodic Labs.",
            candidate=candidate,
            result_rank=1,
        )
        contact_page = classify_public_web_url(
            url="https://rocketreach.co/costa-huang-email_69757629",
            title="Costa Huang Email & Phone Number | Periodic Labs",
            snippet="Costa Huang is currently a Member of Technical Staff at Periodic Labs.",
            candidate=candidate,
            result_rank=1,
        )

        self.assertIsNotNone(company_page)
        self.assertEqual(company_page.entry_type, "company_page")
        self.assertFalse(company_page.fetchable)
        self.assertIsNotNone(contact_page)
        self.assertNotEqual(contact_page.entry_type, "personal_homepage")
        self.assertFalse(contact_page.fetchable)

    def test_third_party_company_mentions_are_not_company_pages(self) -> None:
        candidate = PublicWebCandidateContext(
            record_id="target-1",
            candidate_id="cand-1",
            candidate_name="Aaron Wilkowitz",
            current_company="OpenAI",
        )

        official_page = classify_public_web_url(
            url="https://academy.openai.com/public/content",
            title="Content | OpenAI Academy",
            snippet="OpenAI Academy content.",
            candidate=candidate,
            result_rank=1,
        )
        third_party_article = classify_public_web_url(
            url="https://www.techradar.com/pro/openai-launch-a-version-of-chatgpt-just-for-governments",
            title="OpenAI launches a version of ChatGPT just for governments",
            snippet="OpenAI news mention.",
            candidate=candidate,
            result_rank=2,
        )

        self.assertIsNotNone(official_page)
        self.assertEqual(official_page.entry_type, "company_page")
        self.assertIsNotNone(third_party_article)
        self.assertNotEqual(third_party_article.entry_type, "company_page")

    def test_personal_domain_beats_company_text_and_low_value_pages_stay_other(self) -> None:
        candidate = PublicWebCandidateContext(
            record_id="target-1",
            candidate_id="cand-1",
            candidate_name="Xiang Fu",
            current_company="Periodic Labs",
        )

        homepage = classify_public_web_url(
            url="https://xiangfu.co/",
            title="Xiang Fu",
            snippet="Xiang Fu is a researcher at Periodic Labs.",
            candidate=candidate,
            result_rank=1,
        )
        linkedin_post = classify_public_web_url(
            url="https://www.linkedin.com/posts/example_periodic-labs-activity-123",
            title="Periodic Labs update",
            snippet="Xiang Fu and Periodic Labs.",
            candidate=candidate,
            result_rank=2,
        )
        event_page = classify_public_web_url(
            url="https://luma.com/p9i679us",
            title="Boba & Science with Periodic Labs @ NeurIPS",
            snippet="Event with Xiang Fu and Periodic Labs.",
            candidate=candidate,
            result_rank=3,
        )
        social_mirror = classify_public_web_url(
            url="https://mobile.twstalker.com/xiangfu_ml",
            title="Xiang Fu Twitter Profile",
            snippet="Periodic Labs",
            candidate=candidate,
            result_rank=4,
        )
        article = classify_public_web_url(
            url="https://example-news.com/openai-launching-specialized-chatgpt",
            title="OpenAI is launching specialized ChatGPT",
            snippet="Article mentioning Xiang Fu and Periodic Labs.",
            candidate=candidate,
            result_rank=5,
        )

        self.assertIsNotNone(homepage)
        self.assertEqual(homepage.entry_type, "personal_homepage")
        self.assertIsNotNone(linkedin_post)
        self.assertEqual(linkedin_post.entry_type, "other")
        self.assertFalse(linkedin_post.fetchable)
        self.assertIsNotNone(event_page)
        self.assertEqual(event_page.entry_type, "other")
        self.assertFalse(event_page.fetchable)
        self.assertIsNotNone(social_mirror)
        self.assertEqual(social_mirror.entry_type, "other")
        self.assertFalse(social_mirror.fetchable)
        self.assertIsNotNone(article)
        self.assertNotEqual(article.entry_type, "personal_homepage")

    def test_email_extraction_types_and_suppresses_generic_inboxes(self) -> None:
        candidate = PublicWebCandidateContext(
            record_id="target-1",
            candidate_id="cand-1",
            candidate_name="Ada Lovelace",
            current_company="Example AI",
        )
        signals = extract_email_candidate_signals(
            text=(
                "Ada Lovelace can be reached at ada [at] stanford [dot] edu. "
                "For the lab, contact contact@exampleai.com."
            ),
            source_url="https://ada.stanford.edu/",
            source_family="profile_web_presence",
            source_title="Ada Lovelace",
            candidate=candidate,
        )

        by_email = {signal.normalized_value: signal for signal in signals}
        self.assertEqual(by_email["ada@stanford.edu"].email_type, "academic")
        self.assertEqual(by_email["ada@stanford.edu"].promotion_status, "promotion_recommended")
        self.assertFalse(by_email["contact@exampleai.com"].publishable)
        self.assertEqual(by_email["contact@exampleai.com"].suppression_reason, "generic_inbox")

    def test_email_extraction_suppresses_pdf_title_artifacts(self) -> None:
        signals = extract_email_candidate_signals(
            text=(
                "BridgeData V2: A Dataset for Robot Learning@Scale.Conference on Robot Learning. "
                "RT-1: Robotics Transformer for Real-World Control@Scale.Robotics. "
                "Contact cbfinn@cs.stanford.edu."
            ),
            source_url="https://ai.stanford.edu/~cbfinn/_files/cv.pdf",
            source_family="resume_and_documents",
            source_title="cv.pdf",
        )

        self.assertEqual([signal.normalized_value for signal in signals], ["cbfinn@cs.stanford.edu"])

    def test_email_extraction_expands_group_email_patterns(self) -> None:
        signals = extract_email_candidate_signals(
            text="Author contact: {barryz, lesli}@cs.stanford.edu.",
            source_url="https://example.edu/paper.pdf",
            source_family="candidate_publication_presence",
            source_title="paper.pdf",
        )

        self.assertEqual(
            [signal.normalized_value for signal in signals],
            ["barryz@cs.stanford.edu", "lesli@cs.stanford.edu"],
        )

    def test_scholar_verified_domain_is_not_extracted_as_email_and_homepage_is_signal(self) -> None:
        html = """
        <html><head><title>Noam Shazeer - Google Scholar</title></head>
        <body>
          <div id="gsc_prf_i">
            <div id="gsc_prf_in">Noam Shazeer</div>
            <div class="gsc_prf_il"><a class="gsc_prf_ila">Google</a></div>
            <div class="gsc_prf_il" id="gsc_prf_ivh">
              Verified email at google.com -
              <a href="https://www.noamshazeer.com/" rel="nofollow" class="gsc_prf_ila">Homepage</a>
            </div>
            <div class="gsc_prf_il" id="gsc_prf_int">
              <a class="gsc_prf_inta">Deep Learning</a>
            </div>
          </div></div></div><div id="gsc_prf_t_wrp"></div>
        </body></html>
        """
        signals = extract_page_signals(html, "https://scholar.google.com/citations?user=abc&hl=en")
        emails = extract_email_candidate_signals(
            text=html,
            source_url="https://scholar.google.com/citations?user=abc&hl=en",
            source_family="scholar_profile_discovery",
            source_title="Noam Shazeer - Google Scholar",
            candidate=PublicWebCandidateContext(
                record_id="target-1",
                candidate_id="cand-1",
                candidate_name="Noam Shazeer",
                current_company="Google",
            ),
        )

        self.assertEqual(emails, [])
        self.assertEqual(signals["personal_urls"], ["https://www.noamshazeer.com/"])
        self.assertEqual(signals["scholar_verified_domains"], ["google.com"])
        self.assertEqual(signals["research_interests"], ["Deep Learning"])
        self.assertEqual(signals["affiliation_signals"][0]["organization"], "Google")

        discovered_links = classify_entry_links_from_document_signals(
            signals=signals,
            candidate=PublicWebCandidateContext(
                record_id="target-1",
                candidate_id="cand-1",
                candidate_name="Noam Shazeer",
                current_company="Google",
            ),
            source_url="https://scholar.google.com/citations?user=abc&hl=en",
        )
        self.assertEqual(discovered_links[0].entry_type, "personal_homepage")
        self.assertEqual(discovered_links[0].normalized_url, "https://www.noamshazeer.com/")

    def test_publication_multi_email_keeps_candidate_local_part_and_suppresses_coauthors(self) -> None:
        candidate = PublicWebCandidateContext(
            record_id="target-1",
            candidate_id="cand-1",
            candidate_name="Noam Shazeer",
            current_company="Google",
        )
        signals = extract_email_candidate_signals(
            text=(
                "Adam Roberts Google adarob@google.com "
                "Colin Raffel Google craffel@gmail.com "
                "Noam Shazeer Google noam@google.com"
            ),
            source_url="https://aclanthology.org/example.pdf",
            source_family="candidate_publication_presence",
            source_title="paper.pdf",
            candidate=candidate,
        )

        by_email = {signal.normalized_value: signal for signal in signals}
        self.assertEqual(by_email["noam@google.com"].promotion_status, "promotion_recommended")
        self.assertEqual(by_email["adarob@google.com"].suppression_reason, "coauthor_email_needs_ai_review")
        self.assertFalse(by_email["adarob@google.com"].publishable)
        self.assertEqual(by_email["craffel@gmail.com"].suppression_reason, "coauthor_email_needs_ai_review")

    def test_ai_adjudication_can_mark_weak_identity_email_ambiguous(self) -> None:
        candidate = PublicWebCandidateContext(
            record_id="target-1",
            candidate_id="cand-1",
            candidate_name="Ada Lovelace",
            current_company="Example AI",
        )
        signals = extract_email_candidate_signals(
            text="Unrelated page lists bob@stanford.edu as a contact.",
            source_url="https://stanford.edu/people/bob",
            source_family="contact_signals",
            candidate=candidate,
        )

        adjudicated, result = adjudicate_public_web_signals(
            candidate=candidate,
            email_candidates=signals,
            entry_links=[],
            model_client=DeterministicModelClient(),
            ai_extraction="on",
        )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(adjudicated[0].identity_match_label, "ambiguous_identity")
        self.assertFalse(adjudicated[0].publishable)

    def test_email_adjudication_needs_review_low_score_is_not_publishable(self) -> None:
        signals = extract_email_candidate_signals(
            text="Contact Tian Xie at txie@csail.mit.edu for this repository.",
            source_url="https://github.com/txie-93/cdvae",
            source_family="technical_presence",
            source_title="CDVAE GitHub repository",
            candidate=PublicWebCandidateContext(
                record_id="target-1",
                candidate_id="cand-1",
                candidate_name="Xiang Fu",
                current_company="Periodic Labs",
            ),
        )

        adjudicated = apply_email_adjudication(
            signals,
            {
                "email_assessments": [
                    {
                        "email": "txie@csail.mit.edu",
                        "publishable": True,
                        "promotion_status": "not_promoted",
                        "identity_match_label": "needs_review",
                        "identity_match_score": 0.0,
                    }
                ]
            },
        )

        self.assertFalse(adjudicated[0].publishable)
        self.assertEqual(adjudicated[0].promotion_status, "not_promoted")

    def test_ai_adjudication_marks_weak_identity_social_link_ambiguous(self) -> None:
        candidate = PublicWebCandidateContext(
            record_id="target-1",
            candidate_id="cand-1",
            candidate_name="Ada Lovelace",
            current_company="Example AI",
        )
        weak_link = classify_public_web_url(
            url="https://github.com/unrelated-engineer",
            title="unrelated-engineer",
            snippet="Open source projects.",
            candidate=candidate,
            result_rank=1,
        )
        self.assertIsNotNone(weak_link)

        _emails, links, result = adjudicate_public_web_candidate_evidence(
            candidate=candidate,
            email_candidates=[],
            entry_links=[weak_link],
            fetched_documents=[],
            model_client=DeterministicModelClient(),
            ai_extraction="on",
        )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(links[0].identity_match_label, "ambiguous_identity")
        self.assertLess(links[0].identity_match_score, 0.35)

    def test_ai_adjudication_returns_academic_summary_from_scholar_evidence(self) -> None:
        candidate = PublicWebCandidateContext(
            record_id="target-1",
            candidate_id="cand-1",
            candidate_name="Noam Shazeer",
            current_company="Google",
            headline="Research Scientist",
            linkedin_url="https://www.linkedin.com/in/noam-shazeer/",
        )
        scholar_link = classify_public_web_url(
            url="https://scholar.google.com/citations?user=abc",
            title="Noam Shazeer - Google Scholar",
            snippet="Google. Deep Learning. Natural Language Processing.",
            candidate=candidate,
            result_rank=1,
        )
        self.assertIsNotNone(scholar_link)

        _emails, _links, result = adjudicate_public_web_candidate_evidence(
            candidate=candidate,
            email_candidates=[],
            entry_links=[scholar_link],
            fetched_documents=[
                {
                    "source_url": "https://scholar.google.com/citations?user=abc",
                    "final_url": "https://scholar.google.com/citations?user=abc",
                    "entry_type": "scholar_url",
                    "source_family": "scholar_profile_discovery",
                    "document_type": "social_profile",
                    "title": "Noam Shazeer - Google Scholar",
                    "evidence_slice": {
                        "source_url": "https://scholar.google.com/citations?user=abc",
                        "final_url": "https://scholar.google.com/citations?user=abc",
                        "source_type": "google_scholar_profile",
                        "selected_text": "Noam Shazeer Google Verified email at google.com Homepage Deep Learning Natural Language Processing Attention Is All You Need",
                        "structured_signals": {
                            "research_interests": ["Deep Learning", "Natural Language Processing"],
                            "affiliation_signals": [{"organization": "Google", "relation": "scholar_profile_affiliation"}],
                            "scholar_publications": [
                                {
                                    "title": "Attention Is All You Need",
                                    "authors": "A Vaswani, N Shazeer, N Parmar",
                                    "venue": "NeurIPS",
                                    "year": "2017",
                                    "citations": "100000",
                                }
                            ],
                        },
                    },
                }
            ],
            model_client=DeterministicModelClient(),
            ai_extraction="on",
        )

        self.assertEqual(result["status"], "completed")
        academic_summary = result["result"]["academic_summary"]
        self.assertIn("Deep Learning", academic_summary["research_directions"])
        self.assertEqual(academic_summary["academic_affiliations"], ["Google"])
        self.assertEqual(academic_summary["notable_work"][0]["title"], "Attention Is All You Need")
        self.assertTrue(academic_summary["outreach_angles"])

    def test_auto_ai_adjudication_skips_entry_link_only_evidence(self) -> None:
        candidate = PublicWebCandidateContext(
            record_id="target-1",
            candidate_id="cand-1",
            candidate_name="Ada Lovelace",
            current_company="Example AI",
        )
        link = classify_public_web_url(
            url="https://github.com/ada-lovelace",
            title="Ada Lovelace",
            snippet="Example AI projects.",
            candidate=candidate,
            result_rank=1,
        )
        self.assertIsNotNone(link)

        _emails, links, result = adjudicate_public_web_candidate_evidence(
            candidate=candidate,
            email_candidates=[],
            entry_links=[link],
            fetched_documents=[],
            model_client=_ExplodingLiveModelClient(),
            ai_extraction="auto",
        )

        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["reason"], "auto_mode_entry_link_only")
        self.assertEqual(links[0].identity_match_label, "unreviewed")

    def test_runner_discovers_entry_links_and_writes_artifacts_without_fetch(self) -> None:
        records = [
            {
                "id": "target-1",
                "candidate_id": "cand-1",
                "candidate_name": "Ada Lovelace",
                "current_company": "Example AI",
                "headline": "Research Engineer",
            }
        ]
        with tempfile.TemporaryDirectory() as tempdir:
            summary = run_target_candidate_public_web_experiment(
                target_candidates=records,
                search_provider=_FakeSearchProvider(),
                output_dir=tempdir,
                options=PublicWebExperimentOptions(
                    max_queries_per_candidate=2,
                    max_results_per_query=3,
                    max_fetches_per_candidate=0,
                    fetch_content=False,
                ),
                run_id="test-run",
            )
            root = Path(tempdir) / "test-run"
            self.assertEqual(summary["status"], "completed")
            self.assertEqual(summary["candidate_count"], 1)
            self.assertEqual(summary["query_count"], 2)
            self.assertGreaterEqual(summary["entry_link_count"], 3)
            self.assertTrue((root / "run_summary.json").exists())
            self.assertTrue((root / "candidates" / "01_target-1" / "entry_links.json").exists())

    def test_runner_uses_batch_queue_and_persists_per_candidate_status(self) -> None:
        records = [
            {
                "id": "target-1",
                "candidate_id": "cand-1",
                "candidate_name": "Ada Lovelace",
                "current_company": "Example AI",
            },
            {
                "id": "target-2",
                "candidate_id": "cand-2",
                "candidate_name": "Grace Hopper",
                "current_company": "Example AI",
            },
        ]
        provider = _BatchFakeSearchProvider()
        with tempfile.TemporaryDirectory() as tempdir:
            summary = run_target_candidate_public_web_experiment(
                target_candidates=records,
                search_provider=provider,
                output_dir=tempdir,
                options=PublicWebExperimentOptions(
                    max_queries_per_candidate=2,
                    max_results_per_query=1,
                    max_fetches_per_candidate=0,
                    fetch_content=False,
                    use_batch_search=True,
                    batch_ready_poll_interval_seconds=0,
                    max_batch_ready_polls=1,
                ),
                run_id="batch-run",
            )
            root = Path(tempdir) / "batch-run"

            self.assertEqual(summary["status"], "completed")
            self.assertEqual(summary["candidate_count"], 2)
            self.assertEqual(summary["query_count"], 4)
            self.assertEqual(provider.submit_calls, 1)
            self.assertGreaterEqual(provider.poll_calls, 1)
            self.assertEqual(provider.search_calls, 0)
            self.assertTrue((root / "search_batches" / "batch_execution_summary.json").exists())
            self.assertTrue((root / "candidates" / "01_target-1" / "candidate_status.json").exists())
            self.assertTrue((root / "candidates" / "02_target-2" / "candidate_status.json").exists())
            self.assertTrue(all(item["search_mode"] == "batch" for item in summary["candidate_summaries"]))
            self.assertTrue(all(item["primary_links"] == {} for item in summary["candidate_summaries"]))

    def test_fetch_run_uses_evidence_slices_and_single_candidate_adjudication(self) -> None:
        records = [
            {
                "id": "target-1",
                "candidate_id": "cand-1",
                "candidate_name": "Noah Yonack",
                "current_company": "Perplexity",
                "headline": "Data Scientist",
                "linkedin_url": "https://www.linkedin.com/in/noah-yonack/",
                "education_lines": ["Harvard, 2017"],
            }
        ]
        html = """
        <html><head><title>Noah Yonack · GitHub</title></head>
        <body><main>
          <div>Noah Yonack</div>
          <div>noahyonack</div>
          <div>Data Scientist. Harvard '17.</div>
          <div>San Francisco, CA</div>
          <div>noah.yonack@gmail.com</div>
        </main></body></html>
        """
        model = _CountingPublicWebModelClient()
        with tempfile.TemporaryDirectory() as tempdir:
            with patch(
                "sourcing_agent.document_extraction.fetch_text_url",
                return_value=FetchedTextAsset(
                    url="https://github.com/noahyonack",
                    final_url="https://github.com/noahyonack",
                    content_type="text/html",
                    text=html,
                    source_label="test",
                ),
            ):
                summary = run_target_candidate_public_web_experiment(
                    target_candidates=records,
                    search_provider=_GitHubOnlySearchProvider(),
                    output_dir=tempdir,
                    model_client=model,
                    options=PublicWebExperimentOptions(
                        max_queries_per_candidate=1,
                        max_results_per_query=1,
                        max_fetches_per_candidate=1,
                        fetch_content=True,
                        ai_extraction="auto",
                        use_batch_search=False,
                    ),
                    run_id="fetch-slices",
                )
            signals_path = Path(tempdir) / "fetch-slices" / "candidates" / "01_target-1" / "signals.json"
            signals = json.loads(signals_path.read_text())

        self.assertEqual(summary["fetched_document_count"], 1)
        self.assertEqual(summary["email_candidate_count"], 1)
        self.assertEqual(model.page_calls, 0)
        self.assertEqual(model.candidate_calls, 1)
        self.assertEqual(model.payloads[0]["evidence_slices"][0]["source_type"], "github_profile")
        self.assertEqual(model.payloads[0]["search_evidence"][0]["entry_type"], "github_url")
        self.assertIn("noah.yonack@gmail.com", model.payloads[0]["evidence_slices"][0]["selected_text"])
        self.assertIn("evidence_slice", signals["fetched_documents"][0])
        self.assertIn("noah.yonack@gmail.com", signals["fetched_documents"][0]["evidence_slice"]["selected_text"])

    def test_ai_evidence_document_budget_is_configurable(self) -> None:
        candidate = PublicWebCandidateContext(
            record_id="target-1",
            candidate_id="cand-1",
            candidate_name="Noah Yonack",
            current_company="Perplexity",
        )
        link = classify_public_web_url(
            url="https://github.com/noahyonack",
            title="Noah Yonack",
            snippet="Data Scientist. Harvard '17.",
            candidate=candidate,
            provider_name="test",
        )
        self.assertIsNotNone(link)
        fetched_documents = [
            {
                "source_url": f"https://example.edu/{index}",
                "evidence_slice": {
                    "source_url": f"https://example.edu/{index}",
                    "source_type": "academic_profile",
                    "title": f"Evidence {index}",
                    "selected_text": f"candidate evidence slice {index}",
                },
            }
            for index in range(4)
        ]
        model = _CountingPublicWebModelClient()

        _, _, result = adjudicate_public_web_candidate_evidence(
            candidate=candidate,
            email_candidates=[],
            entry_links=[link],
            fetched_documents=fetched_documents,
            model_client=model,
            ai_extraction="auto",
            max_ai_evidence_documents=2,
        )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(model.candidate_calls, 1)
        self.assertEqual(len(model.payloads[0]["evidence_slices"]), 2)
        self.assertEqual(len(model.payloads[0]["fetched_documents"]), 2)
        self.assertEqual(result["input_counts"]["max_ai_evidence_documents"], 2)

    def test_batch_fetch_failure_does_not_block_other_candidates(self) -> None:
        records = [
            {
                "id": "target-1",
                "candidate_id": "cand-1",
                "candidate_name": "Ada Lovelace",
                "current_company": "Example AI",
            },
            {
                "id": "target-2",
                "candidate_id": "cand-2",
                "candidate_name": "Grace Hopper",
                "current_company": "Example AI",
            },
        ]
        provider = _BatchFakeSearchProvider(fail_record_id="target-2")
        with tempfile.TemporaryDirectory() as tempdir:
            summary = run_target_candidate_public_web_experiment(
                target_candidates=records,
                search_provider=provider,
                output_dir=tempdir,
                options=PublicWebExperimentOptions(
                    max_queries_per_candidate=1,
                    max_results_per_query=1,
                    max_fetches_per_candidate=0,
                    fetch_content=False,
                    use_batch_search=True,
                    batch_ready_poll_interval_seconds=0,
                    max_batch_ready_polls=1,
                ),
                run_id="batch-partial-failure",
            )

        by_record_id = {item["record_id"]: item for item in summary["candidate_summaries"]}
        self.assertEqual(by_record_id["target-1"]["status"], "completed")
        self.assertEqual(by_record_id["target-1"]["entry_link_count"], 1)
        self.assertEqual(by_record_id["target-2"]["status"], "completed_with_errors")
        self.assertEqual(by_record_id["target-2"]["entry_link_count"], 0)
        self.assertTrue(any("empty_batch_response" in error for error in by_record_id["target-2"]["errors"]))

    def test_load_target_candidates_from_json_accepts_wrapped_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            path = Path(tempdir) / "targets.json"
            path.write_text(
                json.dumps({"target_candidates": [{"id": "target-1", "candidate_name": "Ada Lovelace"}]}),
                encoding="utf-8",
            )

            records = load_target_candidates_from_json(path)

        self.assertEqual(records[0]["id"], "target-1")


if __name__ == "__main__":
    unittest.main()
