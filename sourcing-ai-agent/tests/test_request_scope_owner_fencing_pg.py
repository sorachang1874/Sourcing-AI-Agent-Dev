"""C2.7 permanent PostgreSQL owner-CAS race regressions."""

import pytest

from sourcing_agent.crm_writer import CRMWriter
from tests.pg_store_fixture import pg_backed_control_plane_store


def _crm_record_payload(record_id: str, *, workspace: str, owner: str) -> dict[str, str]:
    return {
        "crm_record_id": record_id,
        "workspace_id": workspace,
        "owner_user_id": owner,
        "person_identity_key": f"linkedin:{record_id}",
    }


def _promotion_payload(
    promotion_id: str,
    *,
    record_id: str,
    workspace: str,
    signal_id: str,
    run_id: str = "run-1",
    action: str = "promote",
) -> dict[str, object]:
    return {
        "promotion_id": promotion_id,
        "crm_record_id": record_id,
        "workspace_id": workspace,
        "signal_id": signal_id,
        "run_id": run_id,
        "action": action,
        "signal_kind": "email_candidate",
        "signal_type": "work_email",
        "normalized_value": "alice@example.com",
        "new_value": "alice@example.com",
        "promoted_field": "primary_email",
    }


def test_postgres_owner_cas_rejects_stale_job_and_crm_snapshots() -> None:
    with pg_backed_control_plane_store(schema_label="c2_7_owner_cas") as store:
        store.save_job(
            job_id="job-owner-cas",
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload={"target_company": "OpenAI"},
            requester_id="alice",
            tenant_id="user-alice",
        )
        applied = store.save_job_if_owned(
            expected_requester_id="alice",
            expected_tenant_id="user-alice",
            job_id="job-owner-cas",
            job_type="workflow",
            status="blocked",
            stage="retrieving",
            request_payload={"target_company": "OpenAI"},
            requester_id="alice",
            tenant_id="user-alice",
        )
        assert applied["status"] == "applied"
        stale_job = dict(store.get_job("job-owner-cas") or {})
        store._control_plane_postgres.execute_non_query(  # noqa: SLF001
            "UPDATE jobs SET requester_id = %s, tenant_id = %s WHERE job_id = %s",
            ("bob", "user-bob", "job-owner-cas"),
        )
        owner_miss = store.save_job_if_owned(
            expected_requester_id="alice",
            expected_tenant_id="user-alice",
            job_id="job-owner-cas",
            job_type="workflow",
            status="cancelled",
            stage="completed",
            request_payload=dict(stale_job.get("request") or {}),
            requester_id="alice",
            tenant_id="user-alice",
        )
        assert owner_miss == {"status": "owner_miss"}
        current_job = dict(store.get_job("job-owner-cas") or {})
        assert (current_job["requester_id"], current_job["tenant_id"], current_job["status"]) == (
            "bob",
            "user-bob",
            "blocked",
        )

        stale_record = store.upsert_crm_record(
            {
                "crm_record_id": "crm-owner-cas",
                "workspace_id": "user-alice",
                "owner_user_id": "alice",
                "person_identity_key": "linkedin:owner-cas",
            }
        )
        store._control_plane_postgres.execute_non_query(  # noqa: SLF001
            "UPDATE crm_records SET workspace_id = %s, owner_user_id = %s WHERE crm_record_id = %s",
            ("user-bob", "bob", "crm-owner-cas"),
        )
        crm_result = store.apply_owned_crm_record_update(
            record_payload={**stale_record, "metadata": {"attempted_by": "alice"}},
            engagement_payload={"stage": "contacted_waiting"},
            event_payload={"event_type": "crm_engagement_updated", "actor_type": "user"},
            expected_workspace_id="user-alice",
            expected_owner_user_id="alice",
        )
        assert crm_result == {"status": "not_found", "reason": "crm_record_not_found"}
        current_record = store.get_crm_record("crm-owner-cas")
        assert (current_record["workspace_id"], current_record["owner_user_id"]) == ("user-bob", "bob")
        assert store.list_crm_engagements(crm_record_id="crm-owner-cas") == []

        promotion = store.upsert_crm_public_web_promotion_if_owned(
            {
                "promotion_id": "promotion-owner-cas",
                "crm_record_id": "crm-owner-cas",
                "workspace_id": "user-alice",
                "signal_id": "signal-owner-cas",
                "action": "reject",
            },
            crm_record_id="crm-owner-cas",
            expected_workspace_id="user-alice",
            expected_owner_user_id="alice",
        )
        assert promotion == {"status": "not_found", "reason": "crm_record_not_found"}
        assert store.get_crm_public_web_promotion("promotion-owner-cas") is None


def test_postgres_stage2_cas_preserves_terminal_interleaving() -> None:
    with pg_backed_control_plane_store(schema_label="c2_7_stage2_terminal_interleaving") as store:
        store.save_job(
            job_id="job-stage2-race",
            job_type="workflow",
            status="blocked",
            stage="retrieving",
            request_payload={"target_company": "OpenAI"},
            summary_payload={"awaiting_user_action": "continue_stage2", "stage2_transition_state": ""},
            requester_id="alice",
            tenant_id="user-alice",
        )
        stale = dict(store.get_job("job-stage2-race") or {})
        store._control_plane_postgres.execute_non_query(  # noqa: SLF001
            "UPDATE jobs SET status = %s, stage = %s, summary_json = %s WHERE job_id = %s",
            ("completed", "completed", '{"terminal_marker":"winner"}', "job-stage2-race"),
        )

        result = store.save_job_if_owned(
            expected_requester_id="alice",
            expected_tenant_id="user-alice",
            expected_job_type="workflow",
            expected_statuses=("blocked",),
            expected_stage="retrieving",
            expected_summary_fields={"awaiting_user_action": "continue_stage2"},
            forbidden_summary_values={"stage2_transition_state": ("queued", "running")},
            job_id="job-stage2-race",
            job_type="workflow",
            status="blocked",
            stage="retrieving",
            request_payload=dict(stale.get("request") or {}),
            summary_payload={
                **dict(stale.get("summary") or {}),
                "stage2_transition_state": "queued",
            },
            requester_id="alice",
            tenant_id="user-alice",
        )

        assert result["status"] == "state_conflict"
        assert result["job"]["status"] == "completed"
        current = dict(store.get_job("job-stage2-race") or {})
        assert (current["status"], current["stage"], current["summary"]) == (
            "completed",
            "completed",
            {"terminal_marker": "winner"},
        )


def test_postgres_cancel_cas_preserves_terminal_interleaving() -> None:
    with pg_backed_control_plane_store(schema_label="c2_7_cancel_terminal_interleaving") as store:
        store.save_job(
            job_id="job-cancel-race",
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload={"target_company": "OpenAI"},
            summary_payload={"progress": "working"},
            requester_id="alice",
            tenant_id="user-alice",
        )
        stale = dict(store.get_job("job-cancel-race") or {})
        store._control_plane_postgres.execute_non_query(  # noqa: SLF001
            "UPDATE jobs SET status = %s, stage = %s, summary_json = %s WHERE job_id = %s",
            ("failed", "failed", '{"terminal_marker":"winner"}', "job-cancel-race"),
        )

        result = store.save_job_if_owned(
            expected_requester_id="alice",
            expected_tenant_id="user-alice",
            expected_job_type="workflow",
            forbidden_statuses=("completed", "failed", "superseded", "cancelled", "canceled"),
            job_id="job-cancel-race",
            job_type="workflow",
            status="cancelled",
            stage="completed",
            request_payload=dict(stale.get("request") or {}),
            summary_payload={**dict(stale.get("summary") or {}), "cancelled_reason": "operator"},
            requester_id="alice",
            tenant_id="user-alice",
        )

        assert result["status"] == "state_conflict"
        assert result["job"]["status"] == "failed"
        current = dict(store.get_job("job-cancel-race") or {})
        assert (current["status"], current["stage"], current["summary"]) == (
            "failed",
            "failed",
            {"terminal_marker": "winner"},
        )


def test_postgres_owned_crm_uow_preserves_blank_owner_compatibility() -> None:
    with pg_backed_control_plane_store(schema_label="c2_7_blank_crm_owner") as store:
        record = store.upsert_crm_record(
            {
                "crm_record_id": "crm-blank-owner",
                "workspace_id": "user-alice",
                "owner_user_id": "",
                "person_identity_key": "linkedin:blank-owner",
            }
        )
        result = CRMWriter(store).update_crm_record(
            crm_record_id=record["crm_record_id"],
            workspace_id="user-alice",
            expected_workspace_id="user-alice",
            expected_owner_user_id="alice",
            stage="contacted_waiting",
            comment="owner-compatible update",
            comment_present=True,
        )

        assert result["status"] == "updated"
        assert result["crm_record"]["owner_user_id"] == ""
        assert result["crm_engagement"]["stage"] == "contacted_waiting"
        assert result["crm_event"]["event_type"] == "crm_engagement_updated"
        assert len(store.list_crm_engagements(crm_record_id=record["crm_record_id"])) == 1


def test_postgres_owned_promotion_rejects_cross_workspace_id_collision() -> None:
    with pg_backed_control_plane_store(schema_label="c2_7_promotion_cross_workspace") as store:
        store.upsert_crm_record(_crm_record_payload("crm-alice", workspace="user-alice", owner="alice"))
        store.upsert_crm_record(_crm_record_payload("crm-bob", workspace="user-bob", owner="bob"))
        original = store.upsert_crm_public_web_promotion(
            _promotion_payload(
                "promotion-shared",
                record_id="crm-bob",
                workspace="user-bob",
                signal_id="signal-bob",
            )
        )

        result = store.upsert_crm_public_web_promotion_if_owned(
            _promotion_payload(
                "promotion-shared",
                record_id="crm-alice",
                workspace="user-alice",
                signal_id="signal-alice",
            ),
            crm_record_id="crm-alice",
            expected_workspace_id="user-alice",
            expected_owner_user_id="alice",
        )

        assert result == {
            "status": "conflict",
            "reason": "crm_public_web_promotion_idempotency_conflict",
        }
        assert store.get_crm_public_web_promotion("promotion-shared") == original


def test_postgres_owned_promotion_rejects_same_workspace_cross_record_collision() -> None:
    with pg_backed_control_plane_store(schema_label="c2_7_promotion_cross_record") as store:
        store.upsert_crm_record(_crm_record_payload("crm-alice-a", workspace="user-alice", owner="alice"))
        store.upsert_crm_record(_crm_record_payload("crm-alice-b", workspace="user-alice", owner="alice"))
        original = store.upsert_crm_public_web_promotion(
            _promotion_payload(
                "promotion-shared",
                record_id="crm-alice-a",
                workspace="user-alice",
                signal_id="signal-a",
            )
        )

        result = store.upsert_crm_public_web_promotion_if_owned(
            _promotion_payload(
                "promotion-shared",
                record_id="crm-alice-b",
                workspace="user-alice",
                signal_id="signal-b",
            ),
            crm_record_id="crm-alice-b",
            expected_workspace_id="user-alice",
            expected_owner_user_id="alice",
        )

        assert result["status"] == "conflict"
        assert store.get_crm_public_web_promotion("promotion-shared") == original


@pytest.mark.parametrize(
    ("signal_id", "action"),
    (("signal-other", "promote"), ("signal-original", "reject")),
)
def test_postgres_owned_promotion_rejects_same_record_identity_drift(signal_id: str, action: str) -> None:
    with pg_backed_control_plane_store(schema_label=f"c2_7_promotion_drift_{action}") as store:
        store.upsert_crm_record(_crm_record_payload("crm-alice", workspace="user-alice", owner="alice"))
        original_payload = _promotion_payload(
            "promotion-stable",
            record_id="crm-alice",
            workspace="user-alice",
            signal_id="signal-original",
        )
        original = store.upsert_crm_public_web_promotion(original_payload)
        changed_payload = _promotion_payload(
            "promotion-stable",
            record_id="crm-alice",
            workspace="user-alice",
            signal_id=signal_id,
            action=action,
        )

        result = store.upsert_crm_public_web_promotion_if_owned(
            changed_payload,
            crm_record_id="crm-alice",
            expected_workspace_id="user-alice",
            expected_owner_user_id="alice",
        )

        assert result["status"] == "conflict"
        assert store.get_crm_public_web_promotion("promotion-stable") == original


def test_postgres_owned_promotion_exact_replay_returns_existing_row_without_rewrite() -> None:
    with pg_backed_control_plane_store(schema_label="c2_7_promotion_exact_replay") as store:
        store.upsert_crm_record(_crm_record_payload("crm-alice", workspace="user-alice", owner="alice"))
        payload = _promotion_payload(
            "promotion-replay",
            record_id="crm-alice",
            workspace="user-alice",
            signal_id="signal-original",
        )
        first = store.upsert_crm_public_web_promotion_if_owned(
            payload,
            crm_record_id="crm-alice",
            expected_workspace_id="user-alice",
            expected_owner_user_id="alice",
        )
        replay = store.upsert_crm_public_web_promotion_if_owned(
            payload,
            crm_record_id="crm-alice",
            expected_workspace_id="user-alice",
            expected_owner_user_id="alice",
        )

        assert replay == first
        assert store.get_crm_public_web_promotion("promotion-replay") == first
