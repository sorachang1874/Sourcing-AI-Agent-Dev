import json
import tempfile
import unittest
from pathlib import Path

from sourcing_agent.public_web_quality import (
    evaluate_public_web_quality_paths,
    evaluate_public_web_signals_payload,
    write_public_web_quality_report,
)


class PublicWebQualityTest(unittest.TestCase):
    def test_quality_report_flags_email_and_media_identity_risks(self) -> None:
        payload = {
            "candidate": {
                "record_id": "target-1",
                "candidate_id": "cand-1",
                "candidate_name": "Ada Lovelace",
            },
            "email_candidates": [
                {
                    "value": "ada@example.edu",
                    "normalized_value": "ada@example.edu",
                    "email_type": "academic",
                    "confidence_label": "high",
                    "confidence_score": 0.91,
                    "publishable": True,
                    "promotion_status": "promotion_recommended",
                    "source_url": "https://ada.example.edu/",
                    "source_domain": "ada.example.edu",
                    "source_family": "profile_web_presence",
                    "evidence_excerpt": "Ada Lovelace can be reached at ada@example.edu.",
                    "identity_match_label": "likely_same_person",
                    "identity_match_score": 0.9,
                },
                {
                    "value": "Learning@Scale.Conference",
                    "normalized_value": "learning@scale.conference",
                    "email_type": "unknown",
                    "confidence_label": "high",
                    "confidence_score": 0.85,
                    "publishable": True,
                    "promotion_status": "promotion_recommended",
                    "source_url": "https://ada.example.edu/cv.pdf",
                    "source_domain": "ada.example.edu",
                    "source_family": "resume_and_documents",
                    "identity_match_label": "likely_same_person",
                    "identity_match_score": 0.8,
                },
            ],
            "entry_links": [
                {
                    "url": "https://x.com/ada_ai",
                    "normalized_url": "https://x.com/ada_ai",
                    "entry_type": "x_url",
                    "source_domain": "x.com",
                    "source_family": "social_presence",
                    "identity_match_label": "ambiguous_identity",
                    "identity_match_score": 0.4,
                    "confidence_label": "medium",
                    "score": 50,
                },
                {
                    "url": "https://x.com/i/web/status/123",
                    "normalized_url": "https://x.com/i/web/status/123",
                    "entry_type": "x_url",
                    "source_domain": "x.com",
                    "source_family": "social_presence",
                    "identity_match_label": "likely_same_person",
                    "identity_match_score": 0.75,
                    "confidence_label": "medium",
                    "score": 45,
                },
                {
                    "url": "https://substack.com/home/post/p-123",
                    "normalized_url": "https://substack.com/home/post/p-123",
                    "entry_type": "substack_url",
                    "source_domain": "substack.com",
                    "source_family": "social_presence",
                    "identity_match_label": "likely_same_person",
                    "identity_match_score": 0.75,
                    "confidence_label": "medium",
                    "score": 45,
                },
                {
                    "url": "https://scholar.google.com/citations?user=abc",
                    "normalized_url": "https://scholar.google.com/citations?user=abc",
                    "entry_type": "scholar_url",
                    "source_domain": "scholar.google.com",
                    "source_family": "scholar_profile_discovery",
                    "identity_match_label": "likely_same_person",
                    "identity_match_score": 0.8,
                    "confidence_label": "medium",
                    "score": 90,
                },
            ],
        }

        report = evaluate_public_web_signals_payload(payload, artifact_path="/tmp/signals.json")

        self.assertEqual(report["promotion_recommended_email_count"], 2)
        self.assertEqual(report["trusted_media_link_count"], 1)
        issue_codes = {issue["code"] for issue in report["issues"]}
        self.assertIn("promotion_recommended_generic_or_unknown_email", issue_codes)
        self.assertIn("media_link_without_trusted_identity", issue_codes)
        self.assertIn("x_link_not_profile", issue_codes)
        self.assertIn("substack_link_not_profile_or_publication", issue_codes)
        substack_row = next(row for row in report["signals"] if row["value"] == "https://substack.com/home/post/p-123")
        self.assertFalse(substack_row["publishable"])
        self.assertEqual(report["signals"][0]["source_family"], "profile_web_presence")
        self.assertTrue(report["signals"][0]["evidence_present"])
        self.assertGreaterEqual(report["high_issue_count"], 1)

    def test_quality_report_writes_json_csv_and_markdown(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            candidate_dir = root / "experiment" / "candidates" / "01_ada"
            candidate_dir.mkdir(parents=True)
            (candidate_dir / "signals.json").write_text(
                json.dumps(
                    {
                        "candidate": {"record_id": "target-1", "candidate_name": "Ada Lovelace"},
                        "email_candidates": [],
                        "entry_links": [
                            {
                                "url": "https://github.com/ada/project",
                                "normalized_url": "https://github.com/ada/project",
                                "entry_type": "github_repository",
                                "source_domain": "github.com",
                                "identity_match_label": "likely_same_person",
                            }
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            report = evaluate_public_web_quality_paths([root / "experiment"])
            written = write_public_web_quality_report(report, root / "quality")

            self.assertEqual(report["status"], "ok")
            self.assertEqual(report["summary"]["candidate_count"], 1)
            self.assertIn("github_repository_or_deep_link_not_profile", report["summary"]["issue_code_counts"])
            self.assertIn("non_canonical_profile_link_type", report["summary"]["issue_code_counts"])
            self.assertEqual(report["candidates"][0]["signals"][0]["signal_type"], "github_url")
            self.assertEqual(report["candidates"][0]["signals"][0]["raw_signal_type"], "github_repository")
            self.assertTrue(Path(written["json"]).exists())
            self.assertTrue(Path(written["csv"]).exists())
            self.assertTrue(Path(written["markdown"]).exists())


if __name__ == "__main__":
    unittest.main()
