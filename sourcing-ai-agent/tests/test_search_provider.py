import unittest

from sourcing_agent.search_provider import (
    BaseSearchProvider,
    SearchProviderChain,
    SearchProviderError,
    SearchResponse,
    SearchResultItem,
    parse_duckduckgo_html_results,
    parse_serper_search_results,
    search_response_from_record,
    search_response_to_record,
)


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


if __name__ == "__main__":
    unittest.main()
