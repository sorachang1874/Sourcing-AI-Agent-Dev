from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read_doc(relative_path: str) -> str:
    return (REPO_ROOT / relative_path).read_text(encoding="utf-8")


def _compact(value: str) -> str:
    return " ".join(value.split())


def test_s1e2b_create_wake_contract_is_cross_document_consistent() -> None:
    plan = _compact(_read_doc("docs/TRACK_D_D1N_REMAINING_ACTION_AND_AGENT_TOOL_SERVING_PLAN.md"))
    implementation = _compact(_read_doc("docs/TRACK_D_D1N_S1E2B_START_CREATE_UOW_IMPLEMENTATION.md"))
    response = _compact(_read_doc("docs/TRACK_D_D1N_S1E2B_START_CREATE_UOW_REVIEW_RESPONSE.md"))

    for document in (plan, implementation, response):
        assert (
            "Create performs no recovery wake" in document
            or "create performs no wake" in document
            or "Create intentionally does not call `DurableRuntimeWriter.signal_recovery_for_committed_commands`"
            in document
        )
        assert "S1e2c owns" in document or "result acceptance owns clearing the hold and waking" in document
        assert "post-accept wake" in document or "waking the owner" in document

    assert "best-effort and post-commit" not in plan
