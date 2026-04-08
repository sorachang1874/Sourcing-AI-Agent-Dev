import unittest
from unittest.mock import patch

from sourcing_agent.search_provider import (
    BaseSearchProvider,
    BingHtmlSearchProvider,
    BrowserGoogleSearchProvider,
    DataForSeoGoogleOrganicSearchProvider,
    SearchProviderChain,
    SearchProviderError,
    SearchResponse,
    SearchResultItem,
    build_search_provider,
    parse_bing_html_results,
    parse_dataforseo_google_organic_results,
    parse_duckduckgo_html_results,
    parse_serper_search_results,
    search_response_from_record,
    search_response_to_record,
)
from sourcing_agent.settings import SearchProviderSettings


class SearchProviderTest(unittest.TestCase):
    def test_parse_duckduckgo_html_results(self) -> None:
        html = """
        <div class="result">
          <a class="result__a" href="https://duckduckgo.com/l/?uddg=https%3A%2F%2Fwww.linkedin.com%2Fin%2Fkzl%2F">
            Kevin Lu - Thinking Machines Lab - LinkedIn
          </a>
          <div class="result__snippet">Research Engineer at Thinking Machines Lab.</div>
        </div>
        """
        results = parse_duckduckgo_html_results(html)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].url, "https://www.linkedin.com/in/kzl/")
        self.assertIn("Kevin Lu", results[0].title)

    def test_parse_bing_html_results(self) -> None:
        html = """
        <li class="b_algo">
          <h2><a href="https://www.bing.com/ck/a?u=a1aHR0cHM6Ly93d3cubGlua2VkaW4uY29tL2luL2t6bC8=">Kevin Lu - LinkedIn</a></h2>
          <div class="b_caption"><p>Research Engineer at Thinking Machines Lab.</p></div>
        </li>
        """
        results = parse_bing_html_results(html)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].url, "https://www.linkedin.com/in/kzl/")
        self.assertIn("Kevin Lu", results[0].title)
        self.assertIn("Thinking Machines Lab", results[0].snippet)

    def test_parse_serper_search_results(self) -> None:
        payload = {
            "organic": [
                {
                    "title": "Kevin Lu - LinkedIn",
                    "link": "https://www.linkedin.com/in/kzl/",
                    "snippet": "Research Engineer at Thinking Machines Lab",
                    "position": 1,
                    "displayLink": "www.linkedin.com",
                }
            ]
        }
        results = parse_serper_search_results(payload)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].metadata["position"], 1)
        self.assertEqual(results[0].metadata["display_link"], "www.linkedin.com")

    def test_parse_dataforseo_google_organic_results(self) -> None:
        payload = {
            "tasks": [
                {
                    "status_code": 20000,
                    "result": [
                        {
                            "check_url": "https://www.google.com/search?q=Kevin+Lu",
                            "items": [
                                {
                                    "type": "organic",
                                    "rank_group": 1,
                                    "rank_absolute": 1,
                                    "page": 1,
                                    "domain": "www.linkedin.com",
                                    "title": "Kevin Lu - LinkedIn",
                                    "description": "Research Engineer at Thinking Machines Lab.",
                                    "url": "https://www.linkedin.com/in/kzl/",
                                    "breadcrumb": "https://www.linkedin.com",
                                }
                            ],
                        }
                    ],
                }
            ]
        }
        results = parse_dataforseo_google_organic_results(payload)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].url, "https://www.linkedin.com/in/kzl/")
        self.assertEqual(results[0].metadata["rank_absolute"], 1)
        self.assertEqual(results[0].metadata["domain"], "www.linkedin.com")

    def test_search_response_roundtrip(self) -> None:
        response = SearchResponse(
            provider_name="serper_google",
            query_text="Kevin Lu Thinking Machines Lab LinkedIn",
            results=[
                SearchResultItem(
                    title="Kevin Lu - LinkedIn",
                    url="https://www.linkedin.com/in/kzl/",
                    snippet="Research Engineer at Thinking Machines Lab",
                    metadata={"position": 1},
                )
            ],
            raw_payload={"organic": [{"title": "Kevin Lu - LinkedIn"}]},
            raw_format="json",
            final_url="https://google.serper.dev/search",
            content_type="application/json",
            metadata={"source_label": "serper"},
        )
        record = search_response_to_record(response)
        restored = search_response_from_record(record)
        self.assertEqual(restored.provider_name, "serper_google")
        self.assertEqual(restored.query_text, response.query_text)
        self.assertEqual(restored.results[0].url, "https://www.linkedin.com/in/kzl/")
        self.assertEqual(restored.metadata["source_label"], "serper")

    def test_provider_chain_falls_back(self) -> None:
        class _FailingProvider(BaseSearchProvider):
            provider_name = "failing"

            def search(self, query_text: str, *, max_results: int = 10, timeout: int | None = None) -> SearchResponse:
                raise RuntimeError("provider down")

        class _WorkingProvider(BaseSearchProvider):
            provider_name = "working"

            def search(self, query_text: str, *, max_results: int = 10, timeout: int | None = None) -> SearchResponse:
                return SearchResponse(
                    provider_name=self.provider_name,
                    query_text=query_text,
                    results=[SearchResultItem(title="Kevin Lu - LinkedIn", url="https://www.linkedin.com/in/kzl/")],
                    raw_payload="ok",
                    raw_format="html",
                )

        chain = SearchProviderChain([_FailingProvider(), _WorkingProvider()])
        response = chain.search("Kevin Lu Thinking Machines Lab LinkedIn")
        self.assertEqual(response.provider_name, "working")
        self.assertEqual(response.results[0].url, "https://www.linkedin.com/in/kzl/")

    def test_provider_chain_raises_error_with_attempts(self) -> None:
        class _FailingProvider(BaseSearchProvider):
            provider_name = "failing"

            def search(self, query_text: str, *, max_results: int = 10, timeout: int | None = None) -> SearchResponse:
                raise RuntimeError("provider down")

        chain = SearchProviderChain([_FailingProvider()])
        with self.assertRaises(SearchProviderError) as exc_info:
            chain.search("Kevin Lu")
        self.assertEqual(exc_info.exception.attempts[0]["provider_name"], "failing")

    def test_browser_google_provider_parses_script_output(self) -> None:
        provider = BrowserGoogleSearchProvider(
            script_path="/tmp/google_search_browser.cjs",
            npx_package="playwright@1.59.1",
            node_modules_dir="/tmp/sourcing-playwright-node/node_modules",
            npm_cache_dir="/tmp/.npm-cache",
            browsers_path="/tmp/playwright-browsers",
            headless=True,
            locale="en-US",
            timeout_seconds=30,
        )
        with patch("sourcing_agent.search_provider.shutil.which", return_value="/usr/bin/node"):
            with patch("sourcing_agent.search_provider.Path.exists", return_value=True):
                with patch("sourcing_agent.search_provider.subprocess.run") as run_mock:
                    run_mock.return_value.returncode = 0
                    run_mock.return_value.stdout = """{"provider_name":"google_browser","final_url":"https://www.google.com/search?q=Kevin+Lu","results":[{"title":"Kevin Lu - LinkedIn","url":"https://www.linkedin.com/in/kzl/","snippet":"Research Engineer at Thinking Machines Lab.","metadata":{"source_domain":"www.linkedin.com"}}],"metadata":{"blocked":false}}"""
                    response = provider.search("Kevin Lu Thinking Machines Lab LinkedIn")
        self.assertEqual(response.provider_name, "google_browser")
        self.assertEqual(response.results[0].url, "https://www.linkedin.com/in/kzl/")

    def test_browser_google_provider_raises_when_blocked(self) -> None:
        provider = BrowserGoogleSearchProvider(
            script_path="/tmp/google_search_browser.cjs",
            npx_package="playwright@1.59.1",
            node_modules_dir="/tmp/sourcing-playwright-node/node_modules",
            npm_cache_dir="/tmp/.npm-cache",
            browsers_path="/tmp/playwright-browsers",
            headless=True,
            locale="en-US",
            timeout_seconds=30,
        )
        with patch("sourcing_agent.search_provider.shutil.which", return_value="/usr/bin/node"):
            with patch("sourcing_agent.search_provider.Path.exists", return_value=True):
                with patch("sourcing_agent.search_provider.subprocess.run") as run_mock:
                    run_mock.return_value.returncode = 0
                    run_mock.return_value.stdout = (
                        '{"provider_name":"google_browser","final_url":"https://www.google.com/sorry/index",'
                        '"results":[],"metadata":{"blocked":true}}'
                    )
                    with self.assertRaises(SearchProviderError) as exc_info:
                        provider.search("Kevin Lu Thinking Machines Lab LinkedIn")
        self.assertIn("blocked by Google CAPTCHA", str(exc_info.exception))

    def test_browser_google_provider_surfaces_missing_shared_library_hint(self) -> None:
        provider = BrowserGoogleSearchProvider(
            script_path="/tmp/google_search_browser.cjs",
            npx_package="playwright@1.59.1",
            node_modules_dir="/tmp/sourcing-playwright-node/node_modules",
            npm_cache_dir="/tmp/.npm-cache",
            browsers_path="/tmp/playwright-browsers",
            headless=True,
            locale="en-US",
            timeout_seconds=30,
        )
        with patch("sourcing_agent.search_provider.shutil.which", return_value="/usr/bin/node"):
            with patch("sourcing_agent.search_provider.Path.exists", return_value=True):
                with patch("sourcing_agent.search_provider.subprocess.run") as run_mock:
                    run_mock.return_value.returncode = 1
                    run_mock.return_value.stdout = ""
                    run_mock.return_value.stderr = (
                        "chromium: error while loading shared libraries: "
                        "libnspr4.so: cannot open shared object file: No such file or directory"
                    )
                    with self.assertRaises(SearchProviderError) as exc_info:
                        provider.search("Kevin Lu Thinking Machines Lab LinkedIn")
        self.assertIn("libnspr4.so", str(exc_info.exception))
        self.assertIn("sudo apt-get install -y libnspr4", str(exc_info.exception))

    def test_bing_html_provider_parses_results(self) -> None:
        provider = BingHtmlSearchProvider(timeout_seconds=30)
        html = """
        <html><body>
          <li class="b_algo">
            <h2><a href="https://www.bing.com/ck/a?u=a1aHR0cHM6Ly93d3cubGlua2VkaW4uY29tL2luL2t6bC8=">Kevin Lu - LinkedIn</a></h2>
            <div class="b_caption"><p>Research Engineer at Thinking Machines Lab.</p></div>
          </li>
        </body></html>
        """
        with patch("sourcing_agent.search_provider.requests.get") as get_mock:
            get_mock.return_value.status_code = 200
            get_mock.return_value.text = html
            get_mock.return_value.url = "https://www.bing.com/search?q=Kevin+Lu"
            get_mock.return_value.headers = {"Content-Type": "text/html"}
            get_mock.return_value.raise_for_status.return_value = None
            response = provider.search("Kevin Lu Thinking Machines Lab LinkedIn")
        self.assertEqual(response.provider_name, "bing_html")
        self.assertEqual(response.results[0].url, "https://www.linkedin.com/in/kzl/")

    def test_dataforseo_provider_parses_results(self) -> None:
        provider = DataForSeoGoogleOrganicSearchProvider(
            login="login",
            password="password",
            location_name="United States",
            language_name="English",
            device="desktop",
            os="windows",
            depth=10,
            timeout_seconds=30,
        )
        payload = {
            "tasks": [
                {
                    "status_code": 20000,
                    "result": [
                        {
                            "check_url": "https://www.google.com/search?q=Kevin+Lu",
                            "se_results_count": 123,
                            "pages_count": 1,
                            "items_count": 10,
                            "items": [
                                {
                                    "type": "organic",
                                    "rank_group": 1,
                                    "rank_absolute": 1,
                                    "page": 1,
                                    "domain": "www.linkedin.com",
                                    "title": "Kevin Lu - LinkedIn",
                                    "description": "Research Engineer at Thinking Machines Lab.",
                                    "url": "https://www.linkedin.com/in/kzl/",
                                    "breadcrumb": "https://www.linkedin.com",
                                }
                            ],
                        }
                    ],
                }
            ]
        }
        with patch.object(provider.client, "live_regular", return_value=payload):
            response = provider.search("Kevin Lu Thinking Machines Lab LinkedIn")
        self.assertEqual(response.provider_name, "dataforseo_google_organic")
        self.assertEqual(response.results[0].url, "https://www.linkedin.com/in/kzl/")
        self.assertEqual(response.metadata["items_count"], 10)

    def test_dataforseo_provider_execute_with_checkpoint_submits_queue_task(self) -> None:
        provider = DataForSeoGoogleOrganicSearchProvider(
            login="login",
            password="password",
            location_name="United States",
            language_name="English",
            device="desktop",
            os="windows",
            depth=10,
            timeout_seconds=30,
        )
        payload = {
            "tasks": [
                {
                    "id": "task_123",
                    "status_code": 20100,
                    "result": None,
                }
            ]
        }
        with patch.object(provider.client, "task_post", return_value=payload):
            execution = provider.execute_with_checkpoint("Kevin Lu Thinking Machines Lab LinkedIn")
        self.assertTrue(execution.pending)
        self.assertEqual(execution.checkpoint["task_id"], "task_123")
        self.assertEqual(execution.artifacts[0].label, "task_post")

    def test_dataforseo_provider_execute_with_checkpoint_waits_until_ready(self) -> None:
        provider = DataForSeoGoogleOrganicSearchProvider(
            login="login",
            password="password",
            location_name="United States",
            language_name="English",
            device="desktop",
            os="windows",
            depth=10,
            timeout_seconds=30,
        )
        ready_payload = {
            "tasks": [
                {
                    "result": [
                        {
                            "id": "other_task",
                        }
                    ]
                }
            ]
        }
        with patch.object(provider.client, "tasks_ready", return_value=ready_payload):
            execution = provider.execute_with_checkpoint(
                "Kevin Lu Thinking Machines Lab LinkedIn",
                checkpoint={"provider_name": "dataforseo_google_organic", "task_id": "task_123"},
            )
        self.assertTrue(execution.pending)
        self.assertEqual(execution.checkpoint["status"], "waiting_for_ready")
        self.assertEqual(execution.artifacts[0].label, "tasks_ready")

    def test_dataforseo_provider_execute_with_checkpoint_fetches_ready_task(self) -> None:
        provider = DataForSeoGoogleOrganicSearchProvider(
            login="login",
            password="password",
            location_name="United States",
            language_name="English",
            device="desktop",
            os="windows",
            depth=10,
            timeout_seconds=30,
        )
        ready_payload = {
            "tasks": [
                {
                    "result": [
                        {
                            "id": "task_123",
                        }
                    ]
                }
            ]
        }
        task_payload = {
            "tasks": [
                {
                    "status_code": 20000,
                    "result": [
                        {
                            "check_url": "https://www.google.com/search?q=Kevin+Lu",
                            "items_count": 10,
                            "items": [
                                {
                                    "type": "organic",
                                    "rank_group": 1,
                                    "rank_absolute": 1,
                                    "page": 1,
                                    "domain": "www.linkedin.com",
                                    "title": "Kevin Lu - LinkedIn",
                                    "description": "Research Engineer at Thinking Machines Lab.",
                                    "url": "https://www.linkedin.com/in/kzl/",
                                    "breadcrumb": "https://www.linkedin.com",
                                }
                            ],
                        }
                    ],
                }
            ]
        }
        with patch.object(provider.client, "tasks_ready", return_value=ready_payload):
            with patch.object(provider.client, "task_get_regular", return_value=task_payload):
                execution = provider.execute_with_checkpoint(
                    "Kevin Lu Thinking Machines Lab LinkedIn",
                    checkpoint={"provider_name": "dataforseo_google_organic", "task_id": "task_123"},
                )
        self.assertFalse(execution.pending)
        self.assertIsNotNone(execution.response)
        self.assertEqual(execution.response.metadata["search_mode"], "standard_queue")
        self.assertEqual(execution.response.results[0].url, "https://www.linkedin.com/in/kzl/")

    def test_build_search_provider_includes_google_browser_when_enabled(self) -> None:
        settings = SearchProviderSettings(
            provider_order=("google_browser", "bing_html", "duckduckgo_html"),
            enable_google_browser=True,
            google_browser_node_modules_dir="/tmp/sourcing-playwright-node/node_modules",
            google_browser_script_path="/tmp/google_search_browser.cjs",
            enable_bing_html=True,
            enable_duckduckgo_html=False,
        )
        chain = build_search_provider(settings)
        provider_names = [provider.provider_name for provider in chain.providers]
        self.assertIn("google_browser", provider_names)
        self.assertIn("bing_html", provider_names)

    def test_build_search_provider_includes_dataforseo_when_configured(self) -> None:
        settings = SearchProviderSettings(
            provider_order=("dataforseo_google_organic", "bing_html"),
            dataforseo_login="login",
            dataforseo_password="password",
            enable_dataforseo_google_organic=True,
            enable_bing_html=True,
            enable_duckduckgo_html=False,
        )
        chain = build_search_provider(settings)
        provider_names = [provider.provider_name for provider in chain.providers]
        self.assertEqual(provider_names, ["dataforseo_google_organic", "bing_html"])

    def test_build_search_provider_inserts_bing_for_legacy_order(self) -> None:
        settings = SearchProviderSettings(
            provider_order=("google_browser", "duckduckgo_html"),
            enable_google_browser=True,
            google_browser_node_modules_dir="/tmp/sourcing-playwright-node/node_modules",
            google_browser_script_path="/tmp/google_search_browser.cjs",
            enable_bing_html=True,
            enable_duckduckgo_html=True,
        )
        chain = build_search_provider(settings)
        provider_names = [provider.provider_name for provider in chain.providers]
        self.assertEqual(provider_names, ["google_browser", "bing_html", "duckduckgo_html"])


if __name__ == "__main__":
    unittest.main()
