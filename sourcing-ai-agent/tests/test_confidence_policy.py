import unittest
from datetime import datetime, timezone

from sourcing_agent.confidence_policy import apply_policy_control, build_confidence_policy, default_confidence_policy


class ConfidencePolicyTest(unittest.TestCase):
    def test_default_policy_uses_default_thresholds(self) -> None:
        policy = default_confidence_policy("xAI")
        self.assertEqual(policy["high_threshold"], 0.75)
        self.assertEqual(policy["medium_threshold"], 0.45)

    def test_false_positive_feedback_raises_thresholds(self) -> None:
        policy = build_confidence_policy(
            target_company="xAI",
            feedback_items=[
                {"feedback_type": "false_positive_pattern", "metadata": {}},
                {"feedback_type": "exclude_signal", "metadata": {}},
            ],
        )
        self.assertGreater(policy["high_threshold"], 0.75)
        self.assertGreater(policy["medium_threshold"], 0.45)

    def test_false_negative_feedback_lowers_thresholds(self) -> None:
        policy = build_confidence_policy(
            target_company="xAI",
            feedback_items=[
                {"feedback_type": "false_negative_pattern", "metadata": {}},
                {"feedback_type": "must_have_signal", "metadata": {}},
            ],
        )
        self.assertLess(policy["high_threshold"], 0.75)
        self.assertLess(policy["medium_threshold"], 0.45)

    def test_request_family_scope_ignores_unrelated_feedback(self) -> None:
        current_request = {
            "target_company": "xAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["rl", "research"],
        }
        unrelated_request = {
            "target_company": "xAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["multimodal", "vision"],
        }
        policy = build_confidence_policy(
            target_company="xAI",
            request_payload=current_request,
            feedback_items=[
                {
                    "feedback_type": "false_negative_pattern",
                    "metadata": {"request_payload": current_request},
                    "created_at": "2026-04-05 00:00:00",
                },
                {
                    "feedback_type": "false_positive_pattern",
                    "metadata": {"request_payload": unrelated_request},
                    "created_at": "2026-04-05 00:00:00",
                },
            ],
            now=datetime(2026, 4, 6, tzinfo=timezone.utc),
        )
        self.assertEqual(policy["scope_kind"], "request_family")
        self.assertLess(policy["high_threshold"], 0.75)
        self.assertEqual(policy["summary"]["exact_family_feedback_count"], 1)

    def test_old_feedback_decays_over_time(self) -> None:
        request_payload = {
            "target_company": "xAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["systems"],
        }
        recent_policy = build_confidence_policy(
            target_company="xAI",
            request_payload=request_payload,
            feedback_items=[
                {
                    "feedback_type": "false_positive_pattern",
                    "metadata": {"request_payload": request_payload},
                    "created_at": "2026-04-05 00:00:00",
                }
            ],
            now=datetime(2026, 4, 6, tzinfo=timezone.utc),
        )
        stale_policy = build_confidence_policy(
            target_company="xAI",
            request_payload=request_payload,
            feedback_items=[
                {
                    "feedback_type": "false_positive_pattern",
                    "metadata": {"request_payload": request_payload},
                    "created_at": "2025-10-01 00:00:00",
                }
            ],
            now=datetime(2026, 4, 6, tzinfo=timezone.utc),
        )
        self.assertGreater(recent_policy["high_threshold"], stale_policy["high_threshold"])
        self.assertGreater(recent_policy["medium_threshold"], stale_policy["medium_threshold"])

    def test_manual_override_replaces_thresholds(self) -> None:
        policy = build_confidence_policy(
            target_company="xAI",
            request_payload={"target_company": "xAI", "keywords": ["systems"]},
            feedback_items=[],
        )
        overridden = apply_policy_control(
            policy,
            {
                "control_id": 1,
                "scope_kind": "request_family",
                "control_mode": "override",
                "high_threshold": 0.68,
                "medium_threshold": 0.4,
                "reviewer": "human",
                "notes": "Lock thresholds for this family.",
                "selection_reason": "request_family_control",
            },
        )
        self.assertEqual(overridden["high_threshold"], 0.68)
        self.assertEqual(overridden["medium_threshold"], 0.4)
        self.assertEqual(overridden["control"]["control_mode"], "override")
