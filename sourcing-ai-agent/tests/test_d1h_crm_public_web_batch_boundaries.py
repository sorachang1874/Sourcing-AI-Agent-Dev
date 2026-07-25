from __future__ import annotations

from typing import Any

import pytest

from sourcing_agent.crm_public_web_owner import CrmPublicWebOwner
from sourcing_agent.crm_public_web_runtime import (
    CRM_PUBLIC_WEB_EXECUTION_BACKEND,
    build_crm_public_web_batch_idempotency_key,
    public_web_options_from_record,
    start_crm_public_web_batch,
    sync_crm_public_web_batch_summary,
)
from sourcing_agent.durable_runtime import CRM_PUBLIC_WEB_QUEUE_BATCH_OWNER

BOUNDARY_BATCH_SIZES = (501, 1000)


class _TruncatingBatchStore:
    """Return only the requested prefix so a stale default limit stays visible."""

    def __init__(self, *, batch: dict[str, Any], runs: list[dict[str, Any]]) -> None:
        self.batch = dict(batch)
        self.runs = [dict(run) for run in runs]
        self.list_limits: list[int] = []

    def get_crm_public_web_batch(
        self,
        *,
        batch_id: str = "",
        idempotency_key: str = "",
    ) -> dict[str, Any] | None:
        if batch_id and batch_id != self.batch.get("batch_id"):
            return None
        if idempotency_key and idempotency_key != self.batch.get("idempotency_key"):
            return None
        return dict(self.batch)

    def list_crm_public_web_runs(
        self,
        *,
        batch_id: str,
        workspace_id: str,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        assert batch_id == self.batch["batch_id"]
        assert workspace_id == self.batch["workspace_id"]
        self.list_limits.append(limit)
        return [dict(run) for run in self.runs[:limit]]

    def upsert_crm_public_web_batch(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.batch = dict(payload)
        return dict(self.batch)


def _record_ids(batch_size: int) -> list[str]:
    return [f"crm-{index:04d}" for index in range(batch_size)]


def _minimal_runs(record_ids: list[str], *, batch_id: str) -> list[dict[str, Any]]:
    return [
        {
            "run_id": f"run-{record_id}",
            "batch_id": batch_id,
            "crm_record_id": record_id,
            "status": "queued",
        }
        for record_id in record_ids
    ]


@pytest.mark.parametrize("batch_size", BOUNDARY_BATCH_SIZES)
def test_existing_batch_join_reads_and_returns_the_exact_requested_batch(
    tmp_path: Any,
    batch_size: int,
) -> None:
    workspace_id = "boundary-workspace"
    record_ids = _record_ids(batch_size)
    records = [{"crm_record_id": record_id} for record_id in record_ids]
    request_payload = {"workspace_id": workspace_id, "requested_by": CRM_PUBLIC_WEB_QUEUE_BATCH_OWNER}
    idempotency_key = build_crm_public_web_batch_idempotency_key(
        workspace_id=workspace_id,
        requested_record_ids=record_ids,
        options=public_web_options_from_record(request_payload),
    )
    batch_id = f"batch-{batch_size}"
    store = _TruncatingBatchStore(
        batch={
            "batch_id": batch_id,
            "idempotency_key": idempotency_key,
            "workspace_id": workspace_id,
            "requested_crm_record_ids": record_ids,
        },
        runs=_minimal_runs(record_ids, batch_id=batch_id),
    )

    result = start_crm_public_web_batch(
        store=store,
        crm_records=records,
        runtime_dir=tmp_path,
        payload=request_payload,
    )

    assert result["status"] == "joined"
    assert store.list_limits == [batch_size + 1]
    assert len(result["runs"]) == batch_size
    assert result["summary"]["run_count"] == batch_size
    assert [run["run_id"] for run in result["runs"]] == [f"run-{record_id}" for record_id in record_ids]


@pytest.mark.parametrize("batch_size", BOUNDARY_BATCH_SIZES)
def test_summary_sync_reads_and_persists_the_exact_requested_batch(batch_size: int) -> None:
    workspace_id = "boundary-workspace"
    record_ids = _record_ids(batch_size)
    batch_id = f"batch-{batch_size}"
    expected_run_ids = [f"run-{record_id}" for record_id in record_ids]
    store = _TruncatingBatchStore(
        batch={
            "batch_id": batch_id,
            "idempotency_key": f"idem-{batch_size}",
            "workspace_id": workspace_id,
            "requested_crm_record_ids": record_ids,
            "run_ids": expected_run_ids,
            "metadata": {},
        },
        runs=_minimal_runs(record_ids, batch_id=batch_id),
    )

    result = sync_crm_public_web_batch_summary(store, batch_id, workspace_id=workspace_id)

    assert result["status"] == "updated"
    assert store.list_limits == [batch_size + 1]
    assert result["batch"]["run_ids"] == expected_run_ids
    assert result["batch"]["summary"]["run_count"] == batch_size
    assert result["summary"]["run_count"] == batch_size


def _materialization_fixture(
    batch_size: int,
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    workspace_id = "boundary-workspace"
    batch_id = f"batch-{batch_size}"
    record_ids = _record_ids(batch_size)
    expected_options = {"max_queries_per_candidate": 2}
    expected_source_families = ["official_bio"]
    expected_runs_by_record_id: dict[str, dict[str, Any]] = {}
    runs: list[dict[str, Any]] = []
    for record_id in record_ids:
        run_id = f"run-{record_id}"
        expected = {
            "run_id": run_id,
            "batch_id": batch_id,
            "crm_record_id": record_id,
            "workspace_id": workspace_id,
            "linkedin_url_key": f"linkedin.com/in/{record_id}",
            "person_identity_key": f"person::{record_id}",
            "idempotency_key": f"idem::{record_id}",
            "worker_key": f"worker::{record_id}",
        }
        expected_runs_by_record_id[record_id] = expected
        runs.append(
            {
                **expected,
                "options": expected_options,
                "source_families": expected_source_families,
                "execution_backend": CRM_PUBLIC_WEB_EXECUTION_BACKEND,
                "source_target_run_id": "",
            }
        )
    expectation = {
        "status": "ready",
        "workspace_id": workspace_id,
        "record_ids": record_ids,
        "batch_id": batch_id,
        "idempotency_key": f"batch-idem-{batch_size}",
        "expected_options": expected_options,
        "expected_source_families": expected_source_families,
        "force_refresh": False,
        "refresh_nonce": "",
        "request_metadata": {},
        "expected_runs_by_record_id": expected_runs_by_record_id,
    }
    batch = {
        "batch_id": batch_id,
        "idempotency_key": expectation["idempotency_key"],
        "workspace_id": workspace_id,
        "requested_crm_record_ids": record_ids,
        "run_ids": [run["run_id"] for run in runs],
        "options": expected_options,
        "source_families": expected_source_families,
        "force_refresh": False,
        "requested_by": CRM_PUBLIC_WEB_QUEUE_BATCH_OWNER,
        "execution_backend": CRM_PUBLIC_WEB_EXECUTION_BACKEND,
        "source_target_batch_id": "",
        "metadata": {
            "refresh_nonce": "",
            "owner": "crm_public_web_v1",
            "execution_backend": CRM_PUBLIC_WEB_EXECUTION_BACKEND,
        },
    }
    return expectation, batch, runs


@pytest.mark.parametrize("batch_size", BOUNDARY_BATCH_SIZES)
def test_action_materialization_validator_accepts_exact_boundary_batches_and_rejects_last_run_drift(
    batch_size: int,
) -> None:
    owner = object.__new__(CrmPublicWebOwner)
    expectation, batch, runs = _materialization_fixture(batch_size)

    validated = owner._revalidate_crm_public_web_operation_action_materialization(  # noqa: SLF001
        expectation=expectation,
        batch=batch,
        runs=runs,
    )

    assert validated["status"] == "ready"
    assert len(validated["runs"]) == batch_size

    wrong_batch_runs = [dict(run) for run in runs]
    wrong_batch_runs[-1]["batch_id"] = "foreign-batch"
    rejected = owner._revalidate_crm_public_web_operation_action_materialization(  # noqa: SLF001
        expectation=expectation,
        batch=batch,
        runs=wrong_batch_runs,
    )

    assert rejected == {
        "status": "invalid",
        "reason": "crm_record_batch_command_materialization_invalid",
    }
