import unittest

from sourcing_agent.asset_policy import DEFAULT_MODEL_CONTEXT_CHAR_LIMIT, RAW_ASSET_POLICY_SUMMARY
from sourcing_agent.domain import Candidate
from sourcing_agent.exploratory_enrichment import build_page_analysis_input, extract_page_signals


class ExploratoryEnrichmentTest(unittest.TestCase):
    def test_extract_page_signals(self) -> None:
        html = """
        <html>
          <head>
            <title>Jane Doe</title>
            <meta name="description" content="Research engineer at xAI. Ex OpenAI.">
          </head>
          <body>
            <a href="https://www.linkedin.com/in/jane-doe/">LinkedIn</a>
            <a href="https://x.com/janedoe">X</a>
            <a href="https://github.com/janedoe">GitHub</a>
            <a href="https://janedoe.dev">Homepage</a>
            <a href="https://janedoe.dev/Jane_Doe_CV.pdf">CV</a>
          </body>
        </html>
        """
        signals = extract_page_signals(html, "https://janedoe.dev/about")
        self.assertEqual(signals["linkedin_urls"][0], "https://www.linkedin.com/in/jane-doe/")
        self.assertEqual(signals["x_urls"][0], "https://x.com/janedoe")
        self.assertEqual(signals["github_urls"][0], "https://github.com/janedoe")
        self.assertIn("https://janedoe.dev", signals["personal_urls"][0])
        self.assertEqual(signals["resume_urls"][0], "https://janedoe.dev/Jane_Doe_CV.pdf")
        self.assertIn("Research engineer at xAI.", signals["descriptions"][1])
        self.assertEqual(signals["education_signals"], [])

    def test_build_page_analysis_input_truncates_excerpt(self) -> None:
        candidate = Candidate(candidate_id="c1", name_en="Jane Doe", display_name="Jane Doe")
        html = "<html><head><title>Jane Doe</title><meta name=\"description\" content=\"Research engineer\"></head><body>" + ("signal " * 1000) + "</body></html>"
        payload = build_page_analysis_input(
            candidate=candidate,
            target_company="xAI",
            source_url="https://janedoe.dev",
            html_text=html,
            extracted_links={"linkedin_urls": [], "x_urls": [], "github_urls": [], "personal_urls": [], "resume_urls": []},
        )
        self.assertLessEqual(payload["excerpt_char_count"], DEFAULT_MODEL_CONTEXT_CHAR_LIMIT)
        self.assertEqual(payload["raw_asset_policy"], RAW_ASSET_POLICY_SUMMARY)
        self.assertIn("document_type", payload)
        self.assertIn("text_blocks", payload)
