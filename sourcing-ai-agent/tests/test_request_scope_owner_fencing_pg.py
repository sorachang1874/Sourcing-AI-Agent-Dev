"""C2.7 permanent PostgreSQL owner-CAS race regressions."""

from sourcing_agent.crm_writer import CRMWriter
from tests.pg_store_fixture import pg_backed_control_plane_store


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
        assert store.save_job_if_owned(
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
        stale_job = dict(store.get_job("job-owner-cas") or {})
        store._control_plane_postgres.execute_non_query(  # noqa: SLF001
            "UPDATE jobs SET requester_id = %s, tenant_id = %s WHERE job_id = %s",
            ("bob", "user-bob", "job-owner-cas"),
        )
        assert not store.save_job_if_owned(
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
