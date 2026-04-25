from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from sourcing_agent.domain import Candidate
from sourcing_agent.outreach_layering import build_outreach_layer_analysis


class _FakeModelClient:
    def __init__(self, supported_ids: set[str]) -> None:
        self.supported_ids = set(supported_ids)
        self.calls = 0

    def evaluate_outreach_profile(self, payload: dict) -> dict:
        self.calls += 1
        candidate_id = str((payload.get("candidate") or {}).get("candidate_id") or "").strip()
        if candidate_id in self.supported_ids:
            return {
                "final_layer": 3,
                "confidence_label": "high",
                "evidence_clues": ["mocked"],
                "rationale": "mocked",
            }
        return {
            "final_layer": 0,
            "confidence_label": "low",
            "evidence_clues": [],
            "rationale": "mocked-negative",
        }


class OutreachLayeringTests(unittest.TestCase):
    def test_name_signal_hits_pinyin_surname(self) -> None:
        candidate = Candidate(
            candidate_id="c_name",
            name_en="Li Wei",
            display_name="Li Wei",
            target_company="Thinking Machines Lab",
            organization="Thinking Machines Lab",
            employment_status="current",
            role="Researcher",
        )
        result = build_outreach_layer_analysis(candidates=[candidate], query="", model_client=None, max_ai_verifications=0)
        self.assertEqual(result["layers"]["layer_0_roster"]["count"], 1)
        self.assertEqual(result["layers"]["layer_1_name_signal"]["count"], 1)
        self.assertEqual(result["layers"]["layer_2_greater_china_region_experience"]["count"], 0)
        self.assertEqual(result["layers"]["layer_3_mainland_china_experience_or_chinese_language"]["count"], 0)
        self.assertEqual(result["layers"]["layer_2_greater_china_experience"]["count"], 0)
        self.assertEqual(result["layers"]["layer_3_mainland_or_chinese_language"]["count"], 0)
        self.assertEqual(result["final_layer_distribution"]["layer_1"], 1)
        self.assertEqual(
            (result.get("ai_prompt_template") or {}).get("version"),
            "outreach_layering_v3_explicit_greater_china_scope",
        )

    def test_name_signal_hits_common_english_surname_variant(self) -> None:
        candidate = Candidate(
            candidate_id="c_name_variant",
            name_en="Aaron Chen",
            display_name="Aaron Chen",
            target_company="Thinking Machines Lab",
            organization="Thinking Machines Lab",
            employment_status="current",
            role="Research Engineer",
        )
        result = build_outreach_layer_analysis(candidates=[candidate], query="", model_client=None, max_ai_verifications=0)
        self.assertEqual(result["layers"]["layer_1_name_signal"]["count"], 1)
        self.assertEqual(result["final_layer_distribution"]["layer_1"], 1)

    def test_region_and_language_signals_drive_layer3_layer4(self) -> None:
        candidate = Candidate(
            candidate_id="c_region",
            name_en="Jane Doe",
            display_name="Jane Doe",
            target_company="Thinking Machines Lab",
            organization="Thinking Machines Lab",
            employment_status="current",
            role="Member of Technical Staff",
            education="Peking University, Computer Science",
            notes="Languages: Mandarin, English. Based in Shanghai.",
        )
        result = build_outreach_layer_analysis(candidates=[candidate], query="", model_client=None, max_ai_verifications=0)
        self.assertEqual(result["layers"]["layer_1_name_signal"]["count"], 0)
        self.assertEqual(result["layers"]["layer_2_greater_china_region_experience"]["count"], 0)
        self.assertEqual(result["layers"]["layer_3_mainland_china_experience_or_chinese_language"]["count"], 1)
        self.assertEqual(result["final_layer_distribution"]["layer_3"], 1)

    def test_hong_kong_taiwan_university_signals_raise_layer2(self) -> None:
        candidate = Candidate(
            candidate_id="c_hk",
            name_en="Jane HK",
            display_name="Jane HK",
            target_company="Thinking Machines Lab",
            organization="Thinking Machines Lab",
            employment_status="current",
            role="Researcher",
            education="Hong Kong University of Science and Technology, Computer Science",
        )
        result = build_outreach_layer_analysis(candidates=[candidate], query="", model_client=None, max_ai_verifications=0)
        self.assertEqual(result["layers"]["layer_2_greater_china_region_experience"]["count"], 1)
        self.assertEqual(result["final_layer_distribution"]["layer_2"], 1)

    def test_sinophone_university_signals_raise_layer3(self) -> None:
        candidate = Candidate(
            candidate_id="c_fdu",
            name_en="Jane FDU",
            display_name="Jane FDU",
            target_company="Thinking Machines Lab",
            organization="Thinking Machines Lab",
            employment_status="current",
            role="Researcher",
            education="FDU / Fu Jen Catholic University",
        )
        result = build_outreach_layer_analysis(candidates=[candidate], query="", model_client=None, max_ai_verifications=0)
        self.assertEqual(result["layers"]["layer_3_mainland_china_experience_or_chinese_language"]["count"], 1)
        self.assertEqual(result["final_layer_distribution"]["layer_3"], 1)

    def test_ai_verification_populates_layer2(self) -> None:
        candidate_supported = Candidate(
            candidate_id="c_supported",
            name_en="Zhang San",
            display_name="Zhang San",
            target_company="Thinking Machines Lab",
            organization="Thinking Machines Lab",
            employment_status="current",
            role="Research Engineer",
            education="Tsinghua University, Computer Science",
        )
        candidate_unsupported = Candidate(
            candidate_id="c_unsupported",
            name_en="Li Ming",
            display_name="Li Ming",
            target_company="Thinking Machines Lab",
            organization="Thinking Machines Lab",
            employment_status="current",
            role="Research Engineer",
            work_history="Research Engineer at Example Labs / Visiting researcher in Hong Kong and Singapore",
        )
        fake_model = _FakeModelClient({"c_supported"})
        result = build_outreach_layer_analysis(
            candidates=[candidate_supported, candidate_unsupported],
            query="找华人成员",
            model_client=fake_model,
            max_ai_verifications=10,
        )
        self.assertGreaterEqual(fake_model.calls, 2)
        self.assertEqual(result["layers"]["layer_3_mainland_china_experience_or_chinese_language"]["count"], 1)
        self.assertEqual(result["layers"]["layer_2_greater_china_region_experience"]["count"], 0)
        self.assertEqual(result["final_layer_distribution"]["layer_0"], 1)
        self.assertEqual(result["final_layer_distribution"]["layer_3"], 1)
        self.assertEqual(result["ai_verification"]["successful"], 2)

    def test_zero_ai_budget_disables_model_verification(self) -> None:
        candidate = Candidate(
            candidate_id="c_zero_budget",
            name_en="Zhang San",
            display_name="Zhang San",
            target_company="Thinking Machines Lab",
            organization="Thinking Machines Lab",
            employment_status="current",
            role="Research Engineer",
            education="Tsinghua University, Computer Science",
        )
        fake_model = _FakeModelClient({"c_zero_budget"})
        result = build_outreach_layer_analysis(
            candidates=[candidate],
            query="找华人成员",
            model_client=fake_model,
            max_ai_verifications=0,
        )
        self.assertEqual(fake_model.calls, 0)
        self.assertEqual(result["ai_verification"]["requested"], 0)
        self.assertEqual(result["ai_verification"]["successful"], 0)
        self.assertEqual(result["final_layer_distribution"]["layer_3"], 1)

    def test_profile_excerpt_and_notes_strip_roster_boilerplate(self) -> None:
        candidate = Candidate(
            candidate_id="c_clean",
            name_en="Benny Zhang",
            display_name="Benny Zhang",
            target_company="Reflection AI",
            organization="Reflection AI",
            employment_status="current",
            role="Recruiting Coordinator",
            notes="LinkedIn company roster baseline. Location: San Francisco Bay Area. Source account: harvest_company_employees.",
        )
        result = build_outreach_layer_analysis(candidates=[candidate], query="", model_client=None, max_ai_verifications=0)
        record = (result.get("candidates") or [])[0]
        notes = str(record.get("notes") or "")
        excerpt = str(record.get("profile_text_excerpt") or "")
        self.assertNotIn("LinkedIn company roster baseline", notes)
        self.assertNotIn("Location:", notes)
        self.assertNotIn("Source account:", notes)
        self.assertNotIn("LinkedIn company roster baseline", excerpt)
        self.assertNotIn("Location:", excerpt)
        self.assertNotIn("Source account:", excerpt)

    def test_layer_membership_only_contains_primary_keys(self) -> None:
        candidate = Candidate(
            candidate_id="c_layer",
            name_en="Li Wei",
            display_name="Li Wei",
            target_company="Reflection AI",
            organization="Reflection AI",
            employment_status="current",
            role="Member of Technical Staff",
        )
        result = build_outreach_layer_analysis(candidates=[candidate], query="", model_client=None, max_ai_verifications=0)
        membership = ((result.get("candidates") or [])[0].get("layer_membership") or {})
        self.assertEqual(
            set(membership.keys()),
            {
                "layer_0_roster",
                "layer_1_name_signal",
                "layer_2_greater_china_region_experience",
                "layer_3_mainland_china_experience_or_chinese_language",
            },
        )

    def test_registry_raw_path_fallback_populates_profile_details(self) -> None:
        candidate_url = "https://www.linkedin.com/in/ACwAAExample"
        payload = {
            "_harvest_request": {"url": candidate_url},
            "item": {
                "linkedinUrl": candidate_url,
                "fullName": "Test Person",
                "headline": "ML Researcher",
                "location": "New York, United States",
                "experience": [
                    {"companyName": "Reflection AI", "title": "Research Engineer"},
                ],
                "education": [
                    {"schoolName": "Tsinghua University", "degreeName": "MS", "fieldOfStudy": "Computer Science"},
                ],
                "languages": [{"name": "Mandarin"}],
            },
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            raw_path = Path(temp_dir) / "harvest_profiles" / "fallback.json"
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            raw_path.write_text(json.dumps(payload, ensure_ascii=False))
            candidate = Candidate(
                candidate_id="c_registry",
                name_en="Test Person",
                display_name="Test Person",
                target_company="Reflection AI",
                organization="Reflection AI",
                employment_status="current",
                role="Research Engineer",
                linkedin_url=candidate_url,
                source_path=str(Path(temp_dir) / "not_profile_source.json"),
            )
            result = build_outreach_layer_analysis(
                candidates=[candidate],
                query="",
                model_client=None,
                max_ai_verifications=0,
                registry_raw_paths={candidate_url.lower(): str(raw_path)},
            )
        record = (result.get("candidates") or [])[0]
        self.assertIn("Tsinghua University", str(record.get("education") or ""))
        self.assertIn("Reflection AI", str(record.get("work_history") or ""))


if __name__ == "__main__":
    unittest.main()
