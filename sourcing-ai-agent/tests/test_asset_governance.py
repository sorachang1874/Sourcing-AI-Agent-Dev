from sourcing_agent.asset_governance import (
    build_canonical_asset_replacement_plan,
    build_default_asset_pointer,
    default_asset_pointer_key,
    normalize_asset_lifecycle_status,
)
from sourcing_agent.storage import ControlPlaneStore


def test_default_asset_pointer_key_is_unique_per_scope() -> None:
    assert (
        default_asset_pointer_key(
            company_key="Google",
            scope_kind="team",
            scope_key="Veo",
            asset_kind="scoped_asset",
        )
        == "google/team/veo/scoped_asset"
    )


def test_canonical_replacement_plan_supersedes_previous_default_pointer() -> None:
    current = build_default_asset_pointer(
        company_key="OpenAI",
        snapshot_id="snap-old",
        coverage_proof={"coverage_scope": "company"},
    )

    plan = build_canonical_asset_replacement_plan(
        current_pointer=current,
        company_key="OpenAI",
        snapshot_id="snap-new",
        coverage_proof={"coverage_scope": "company", "lane_coverage": ["current", "former"]},
        promoted_by_job_id="job-1",
    )

    assert plan["status"] == "promoted"
    assert plan["default_pointer"]["snapshot_id"] == "snap-new"
    assert plan["default_pointer"]["previous_snapshot_id"] == "snap-old"
    assert plan["default_pointer"]["coverage_proof"]["coverage_scope"] == "company"
    assert plan["superseded_pointer"]["snapshot_id"] == "snap-old"
    assert plan["superseded_pointer"]["lifecycle_status"] == "superseded"
    assert plan["historical_retention"]["retain_snapshot_ids"] == ["snap-new", "snap-old"]


def test_canonical_replacement_plan_rejects_partial_snapshot_as_default() -> None:
    plan = build_canonical_asset_replacement_plan(
        current_pointer={},
        company_key="Meta",
        snapshot_id="excel-small-import",
        lifecycle_status="partial",
    )

    assert plan["status"] == "rejected"
    assert plan["reason"] == "lifecycle_status_not_promotable"
    assert normalize_asset_lifecycle_status("unexpected") == "draft"


def test_promote_asset_default_pointer_persists_current_pointer_and_history(tmp_path) -> None:
    store = ControlPlaneStore(str(tmp_path / "test.db"))

    first = store.promote_asset_default_pointer(
        {
            "company_key": "OpenAI",
            "snapshot_id": "snap-1",
            "coverage_proof": {"coverage_scope": "company"},
            "promoted_by_job_id": "job-1",
        }
    )
    second = store.promote_asset_default_pointer(
        {
            "company_key": "OpenAI",
            "snapshot_id": "snap-2",
            "coverage_proof": {"coverage_scope": "company", "lane_coverage": ["current", "former"]},
            "promoted_by_job_id": "job-2",
        }
    )

    assert first["status"] == "promoted"
    assert second["status"] == "promoted"
    pointer = store.get_asset_default_pointer(company_key="OpenAI")
    assert pointer is not None
    assert pointer["snapshot_id"] == "snap-2"
    assert pointer["previous_snapshot_id"] == "snap-1"
    history = store.list_asset_default_pointer_history(pointer_key=pointer["pointer_key"])
    event_types = [item["event_type"] for item in history]
    assert event_types.count("promoted") == 2
    assert event_types.count("superseded") == 1


def test_promote_asset_default_pointer_rejects_partial_without_writing(tmp_path) -> None:
    store = ControlPlaneStore(str(tmp_path / "test.db"))

    result = store.promote_asset_default_pointer(
        {
            "company_key": "Meta",
            "snapshot_id": "excel-import",
            "lifecycle_status": "partial",
        }
    )

    assert result["status"] == "rejected"
    assert store.get_asset_default_pointer(company_key="Meta") is None
