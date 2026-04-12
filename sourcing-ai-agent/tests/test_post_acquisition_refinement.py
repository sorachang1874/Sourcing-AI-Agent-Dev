import unittest

from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.post_acquisition_refinement import (
    apply_refinement_patch,
    compile_refinement_patch_from_instruction,
    parse_refinement_instruction,
)


class PostAcquisitionRefinementTest(unittest.TestCase):
    def test_parse_refinement_instruction_extracts_core_filters(self) -> None:
        patch = parse_refinement_instruction("只看 multimodal researcher，top 5，strict roster only，不要 recruiting。")

        self.assertEqual(patch["asset_view"], "strict_roster_only")
        self.assertEqual(patch["top_k"], 5)
        self.assertEqual(patch["categories"], ["researcher"])
        self.assertEqual(patch["must_have_facets"], ["multimodal"])
        self.assertIn("recruiting", patch["exclude_keywords"])

    def test_compile_refinement_patch_prefers_model_and_supplements_deterministic(self) -> None:
        class _ModelClient(DeterministicModelClient):
            def provider_name(self) -> str:
                return "test-model"

            def normalize_refinement_instruction(self, payload: dict[str, object]) -> dict[str, object]:  # noqa: ARG002
                return {
                    "patch": {
                        "must_have_primary_role_buckets": ["infra systems"],
                    }
                }

        compiled = compile_refinement_patch_from_instruction(
            instruction="只看 infra systems，top 2",
            base_request={"target_company": "Acme", "categories": ["employee"], "employment_statuses": ["current"]},
            model_client=_ModelClient(),
        )

        self.assertEqual(
            compiled["request_patch"],
            {
                "must_have_primary_role_buckets": ["infra_systems"],
                "top_k": 2,
            },
        )
        self.assertEqual(compiled["instruction_compiler"]["source"], "model")
        self.assertEqual(compiled["instruction_compiler"]["supplemented_keys"], ["top_k"])

    def test_apply_refinement_patch_overrides_request_fields(self) -> None:
        merged = apply_refinement_patch(
            {
                "target_company": "Acme",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "top_k": 10,
            },
            {
                "categories": ["researcher"],
                "must_have_facets": ["multimodal"],
                "top_k": 3,
            },
        )

        self.assertEqual(merged["target_company"], "Acme")
        self.assertEqual(merged["categories"], ["researcher"])
        self.assertEqual(merged["must_have_facets"], ["multimodal"])
        self.assertEqual(merged["top_k"], 3)

    def test_compile_refinement_patch_rewrites_shorthand_into_must_have_keywords(self) -> None:
        compiled = compile_refinement_patch_from_instruction(
            instruction="只看更偏华人的成员，top 5",
            base_request={
                "target_company": "Anthropic",
                "categories": ["employee"],
                "employment_statuses": ["current"],
            },
        )

        self.assertEqual(
            compiled["request_patch"]["must_have_keywords"],
            ["Greater China experience", "Chinese bilingual outreach"],
        )
        self.assertEqual(compiled["request_patch"]["top_k"], 5)
        self.assertEqual(
            compiled["merged_request"]["must_have_keywords"],
            ["Greater China experience", "Chinese bilingual outreach"],
        )

    def test_compile_refinement_patch_maps_model_keyword_families_into_shared_fields(self) -> None:
        class _ModelClient(DeterministicModelClient):
            def normalize_refinement_instruction(self, payload: dict[str, object]) -> dict[str, object]:  # noqa: ARG002
                return {
                    "patch": {
                        "team_keywords": ["TBD"],
                        "model_keywords": ["o1"],
                        "research_direction_keywords": ["reasoning"],
                    }
                }

        compiled = compile_refinement_patch_from_instruction(
            instruction="聚焦 TBD reasoning",
            base_request={
                "target_company": "OpenAI",
                "categories": ["employee"],
                "employment_statuses": ["current", "former"],
            },
            model_client=_ModelClient(),
        )

        self.assertEqual(compiled["request_patch"]["organization_keywords"], ["TBD"])
        self.assertEqual(compiled["request_patch"]["keywords"], ["reasoning", "o1"])


if __name__ == "__main__":
    unittest.main()
