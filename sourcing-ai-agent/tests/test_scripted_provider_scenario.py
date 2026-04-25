import json
from pathlib import Path

from sourcing_agent.scripted_provider_scenario import (
    summarize_scripted_provider_scenario,
    validate_scripted_provider_scenario,
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_scripted_provider_scenario_summary_detects_required_behavior_categories() -> None:
    scenario = {
        "search": {
            "rules": [
                {
                    "name": "search_staged_partial",
                    "poll_pending_rounds": 2,
                    "estimated_total_count": 3,
                    "results": [
                        {"title": "A", "url": "https://example.com/a"},
                    ],
                }
            ]
        },
        "harvest": {
            "rules": [
                {
                    "name": "harvest_retry_timeout",
                    "errors": [
                        {"phase": "execute", "round": 1, "kind": "retryable", "status": "429"},
                        {"phase": "execute", "round": 2, "kind": "timeout", "message": "Provider timed out"},
                    ],
                    "body": [],
                }
            ]
        },
    }

    summary = summarize_scripted_provider_scenario(scenario)
    validation = validate_scripted_provider_scenario(scenario)

    assert summary["complete"] is True
    assert summary["coverage"]["retryable_error"] == 1
    assert summary["coverage"]["timeout_error"] == 1
    assert summary["coverage"]["partial_result"] == 1
    assert summary["coverage"]["staged_ready_fetch"] == 1
    assert validation["status"] == "valid"


def test_scripted_provider_scenario_validation_reports_missing_categories() -> None:
    validation = validate_scripted_provider_scenario({"harvest": {"default": {"body": []}}})

    assert validation["status"] == "incomplete"
    assert set(validation["missing_categories"]) == {
        "retryable_error",
        "timeout_error",
        "partial_result",
        "staged_ready_fetch",
    }


def test_provider_behavior_matrix_fixture_covers_required_scenarios() -> None:
    payload = json.loads((_repo_root() / "configs" / "scripted" / "provider_behavior_matrix.json").read_text())
    validation = validate_scripted_provider_scenario(payload)

    assert validation["status"] == "valid"
