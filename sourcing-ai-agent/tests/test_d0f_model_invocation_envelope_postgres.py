from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import pytest

from sourcing_agent.repositories.model_invocation_envelopes import (
    MODEL_INVOCATION_ENVELOPE_COLUMN_MANIFEST,
    ModelInvocationEnvelopeCollisionError,
    ModelInvocationEnvelopePurgedError,
)
from tests.pg_store_fixture import pg_backed_control_plane_store
from tests.test_d0f_model_invocation_envelope_repository import _digest, _envelope, _pfx


def test_d0f_real_pg_migration_exact_replay_isolation_concurrency_tamper_and_tombstone() -> None:
    with pg_backed_control_plane_store(schema_label="d0f_model_invocation_envelopes") as store:
        repository = store.repos.model_invocation_envelopes
        adapter = store._control_plane_postgres  # noqa: SLF001

        migration = adapter.execute_returning_one(
            "SELECT version FROM schema_migrations WHERE version = %s",
            ("0007_model_invocation_envelopes",),
        )
        assert migration == {"version": "0007_model_invocation_envelopes"}
        columns = adapter.execute_returning_one(
            "SELECT json_agg(column_name ORDER BY ordinal_position) AS column_names, "
            "json_agg(data_type ORDER BY ordinal_position) AS data_types "
            "FROM information_schema.columns WHERE table_schema = %s AND table_name = %s",
            (adapter.schema, "model_invocation_envelopes"),
        )
        assert columns is not None
        assert tuple(columns["column_names"]) == tuple(item[0] for item in MODEL_INVOCATION_ENVELOPE_COLUMN_MANIFEST)
        assert tuple(columns["data_types"][11:14]) == (
            "timestamp with time zone",
            "timestamp with time zone",
            "timestamp with time zone",
        )
        constraints = adapter.execute_returning_one(
            "SELECT json_agg(conname ORDER BY conname) AS constraint_names "
            "FROM pg_constraint AS constraint_row "
            "JOIN pg_class AS table_row ON table_row.oid = constraint_row.conrelid "
            "JOIN pg_namespace AS namespace_row ON namespace_row.oid = table_row.relnamespace "
            "WHERE namespace_row.nspname = %s AND table_row.relname = %s",
            (adapter.schema, "model_invocation_envelopes"),
        )
        assert constraints is not None
        assert {
            "model_invocation_envelopes_pkey",
            "model_invocation_envelopes_ref_digest_uk",
            "model_invocation_envelopes_digest_uk",
            "model_invocation_envelopes_retention_pair_ck",
            "model_invocation_envelopes_retained_until_ck",
        }.issubset(set(constraints["constraint_names"]))
        key_definitions = adapter.execute_returning_one(
            "SELECT json_object_agg(constraint_row.conname, pg_get_constraintdef(constraint_row.oid)) "
            "AS definitions FROM pg_constraint AS constraint_row "
            "JOIN pg_class AS table_row ON table_row.oid = constraint_row.conrelid "
            "JOIN pg_namespace AS namespace_row ON namespace_row.oid = table_row.relnamespace "
            "WHERE namespace_row.nspname = %s AND table_row.relname = %s "
            "AND constraint_row.conname IN (%s, %s, %s)",
            (
                adapter.schema,
                "model_invocation_envelopes",
                "model_invocation_envelopes_pkey",
                "model_invocation_envelopes_ref_digest_uk",
                "model_invocation_envelopes_digest_uk",
            ),
        )
        assert key_definitions is not None
        assert key_definitions["definitions"] == {
            "model_invocation_envelopes_pkey": (
                "PRIMARY KEY (runtime_namespace, provider_mode, workspace_id, scope_digest, "
                "coordination_plan_review_id, model_invocation_envelope_ref)"
            ),
            "model_invocation_envelopes_ref_digest_uk": (
                "UNIQUE (runtime_namespace, provider_mode, workspace_id, scope_digest, "
                "coordination_plan_review_id, model_invocation_envelope_ref, envelope_digest)"
            ),
            "model_invocation_envelopes_digest_uk": (
                "UNIQUE (runtime_namespace, provider_mode, workspace_id, scope_digest, "
                "coordination_plan_review_id, envelope_digest)"
            ),
        }

        persisted_by_mode = {}
        for ordinal, mode in enumerate(("live", "simulate", "scripted"), start=1):
            envelope = replace(
                _envelope(mode),
                turn_id=f"turn-{mode}",
                step_id=f"step-{mode}",
                provider_call_id=f"provider-call-{mode}",
                canonical_result_digest=_digest(f"result-{mode}"),
            )
            pfx = {**_pfx(mode), "coordination_plan_review_id": ordinal}
            first = repository.persist(**pfx, envelope=envelope)  # type: ignore[arg-type]
            replay = repository.persist(**pfx, envelope=envelope)  # type: ignore[arg-type]
            assert replay["model_invocation_envelope_ref"] == first["model_invocation_envelope_ref"]
            assert replay["created_at"] == first["created_at"]
            assert replay["state_version"] == 0
            persisted_by_mode[mode] = (pfx, envelope, first)

        scripted_pfx, scripted_envelope, scripted_row = persisted_by_mode["scripted"]
        assert (
            adapter.get_model_invocation_envelope(
                table_name="model_invocation_envelopes",
                **{**scripted_pfx, "workspace_id": "foreign-workspace"},
                model_invocation_envelope_ref=scripted_row["model_invocation_envelope_ref"],
                envelope_digest=scripted_row["envelope_digest"],
            )
            is None
        )

        concurrent_envelope = replace(
            scripted_envelope,
            turn_id="turn-concurrent",
            step_id="step-concurrent",
            provider_call_id="provider-call-concurrent",
            canonical_result_digest=_digest("result-concurrent"),
        )
        concurrent_pfx = {
            **scripted_pfx,
            "scope_digest": _digest("scope-concurrent"),
            "coordination_plan_review_id": 99,
        }

        def persist_concurrently() -> dict:
            return repository.persist(**concurrent_pfx, envelope=concurrent_envelope)  # type: ignore[arg-type]

        with ThreadPoolExecutor(max_workers=8) as executor:
            concurrent_rows = list(executor.map(lambda _index: persist_concurrently(), range(16)))
        assert len({row["model_invocation_envelope_ref"] for row in concurrent_rows}) == 1
        assert len({row["created_at"] for row in concurrent_rows}) == 1
        count = adapter.execute_returning_one(
            "SELECT count(*) AS row_count FROM model_invocation_envelopes",
            (),
        )
        assert count == {"row_count": 4}

        tampered_json = replace(
            scripted_envelope,
            canonical_result_digest=_digest("tampered-result"),
        ).to_canonical_json()
        adapter.execute_non_query(
            "UPDATE model_invocation_envelopes SET envelope_record_json = %s "
            "WHERE runtime_namespace = %s AND provider_mode = %s AND workspace_id = %s "
            "AND scope_digest = %s AND coordination_plan_review_id = %s "
            "AND model_invocation_envelope_ref = %s AND envelope_digest = %s",
            (
                tampered_json,
                *(
                    scripted_pfx[field_name]
                    for field_name in (
                        "runtime_namespace",
                        "provider_mode",
                        "workspace_id",
                        "scope_digest",
                        "coordination_plan_review_id",
                    )
                ),
                scripted_row["model_invocation_envelope_ref"],
                scripted_row["envelope_digest"],
            ),
        )
        with pytest.raises(ModelInvocationEnvelopeCollisionError, match="digest_record"):
            repository.get(
                **scripted_pfx,  # type: ignore[arg-type]
                model_invocation_envelope_ref=scripted_row["model_invocation_envelope_ref"],
                envelope_digest=scripted_row["envelope_digest"],
            )
        adapter.execute_non_query(
            "UPDATE model_invocation_envelopes SET envelope_record_json = %s "
            "WHERE runtime_namespace = %s AND provider_mode = %s AND workspace_id = %s "
            "AND scope_digest = %s AND coordination_plan_review_id = %s "
            "AND model_invocation_envelope_ref = %s AND envelope_digest = %s",
            (
                scripted_envelope.to_canonical_json(),
                *(
                    scripted_pfx[field_name]
                    for field_name in (
                        "runtime_namespace",
                        "provider_mode",
                        "workspace_id",
                        "scope_digest",
                        "coordination_plan_review_id",
                    )
                ),
                scripted_row["model_invocation_envelope_ref"],
                scripted_row["envelope_digest"],
            ),
        )

        with pytest.raises(Exception, match="model_invocation_envelopes_provider_mode_shape_ck"):
            adapter.execute_non_query(
                "UPDATE model_invocation_envelopes SET provider_mode = 'replay' "
                "WHERE runtime_namespace = %s AND provider_mode = %s AND workspace_id = %s "
                "AND scope_digest = %s AND coordination_plan_review_id = %s "
                "AND model_invocation_envelope_ref = %s",
                (
                    *(
                        scripted_pfx[field_name]
                        for field_name in (
                            "runtime_namespace",
                            "provider_mode",
                            "workspace_id",
                            "scope_digest",
                            "coordination_plan_review_id",
                        )
                    ),
                    scripted_row["model_invocation_envelope_ref"],
                ),
            )

        adapter.execute_non_query(
            "UPDATE model_invocation_envelopes "
            "SET created_at = transaction_timestamp() - interval '31 days', "
            "retained_until = transaction_timestamp() - interval '1 day' "
            "WHERE runtime_namespace = %s AND provider_mode = %s AND workspace_id = %s "
            "AND scope_digest = %s AND coordination_plan_review_id = %s "
            "AND model_invocation_envelope_ref = %s",
            (
                *(
                    scripted_pfx[field_name]
                    for field_name in (
                        "runtime_namespace",
                        "provider_mode",
                        "workspace_id",
                        "scope_digest",
                        "coordination_plan_review_id",
                    )
                ),
                scripted_row["model_invocation_envelope_ref"],
            ),
        )
        purged = repository.purge_expired(limit=10)
        assert len(purged) == 1
        assert purged[0]["retention_state"] == "purged_tombstone"
        assert purged[0]["envelope_record_json"] is None
        assert purged[0]["state_version"] == 1
        assert purged[0]["envelope"] is None

        with pytest.raises(ModelInvocationEnvelopePurgedError, match="purged_tombstone"):
            repository.persist(**scripted_pfx, envelope=scripted_envelope)  # type: ignore[arg-type]
        after_replay = repository.get(
            **scripted_pfx,  # type: ignore[arg-type]
            model_invocation_envelope_ref=scripted_row["model_invocation_envelope_ref"],
            envelope_digest=scripted_row["envelope_digest"],
        )
        assert after_replay is not None
        assert after_replay["retention_state"] == "purged_tombstone"
        assert after_replay["envelope_record_json"] is None
        assert after_replay["state_version"] == 1
        count_after_replay = adapter.execute_returning_one(
            "SELECT count(*) AS row_count FROM model_invocation_envelopes",
            (),
        )
        assert count_after_replay == {"row_count": 4}
