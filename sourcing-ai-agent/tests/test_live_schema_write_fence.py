"""Live-schema write fence for offline provider rows (incident 2026-07-22).

Observed failure: a worker daemon restarted against the live Postgres schema
(sourcing_live_tml_path_20260719) WITHOUT the live provider triple-gate resumed
test-root jobs, executed harvest fail-closed simulate, and upserted 40-item
placeholder shard rows (run ids simulate_probe_*) over real paid lineage —
google/openai acquisition_shard_registry rows and the authoritative serving
chain ended up 100% simulate. Mechanism: nothing between the connector's
offline branch and storage.upsert_acquisition_shard_registry distinguished a
live schema from a test schema. This fence refuses offline provider rows
(run ids matching ^(simulate|replay|scripted)_probe_) in any sourcing_live_*
schema unless SOURCING_ALLOW_OFFLINE_PROVIDER_ROWS_IN_LIVE_SCHEMA=1.
"""

import os
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sourcing_agent.storage import ControlPlaneStore


def _bare_store(schema: str) -> ControlPlaneStore:
    store = ControlPlaneStore.__new__(ControlPlaneStore)
    store._control_plane_postgres = SimpleNamespace(schema=schema)
    return store


SIMULATE_PAYLOAD = {
    "shard_key": "deadbeefdeadbeefdeadbeef",
    "target_company": "Google",
    "snapshot_id": "20260722T000000",
    "metadata": {
        "probe_summary": {"run_id": "simulate_probe_36c1d5eed955875a"},
    },
}


class OfflineMarkerDetectionTest(unittest.TestCase):
    def test_detects_offline_probe_run_ids_at_any_depth(self) -> None:
        for run_id in ("simulate_probe_root", "replay_probe_abc123", "scripted_probe_x"):
            payload = {"metadata": {"shards": [{"probe": {"run_id": run_id}}]}}
            self.assertEqual(
                ControlPlaneStore._find_offline_provider_run_marker(payload), run_id
            )

    def test_free_text_mentioning_simulate_does_not_trip(self) -> None:
        # The OpenAI former shard carries "simulate" inside a candidate's own
        # summary prose — that row is REAL paid data and must never be fenced.
        payload = {
            "metadata": {
                "summary": "I simulate market dynamics and simulated annealing.",
                "note": "simulation-driven research",
            },
            "search_query": "simulate",
        }
        self.assertEqual(ControlPlaneStore._find_offline_provider_run_marker(payload), "")

    def test_live_run_ids_do_not_trip(self) -> None:
        payload = {"metadata": {"run_id": "hbFYgTkamkP9GaXCv", "mode": "live_probe_1"}}
        self.assertEqual(ControlPlaneStore._find_offline_provider_run_marker(payload), "")


class LiveSchemaFenceTest(unittest.TestCase):
    def test_live_schema_refuses_offline_row_without_db_touch(self) -> None:
        store = _bare_store("sourcing_live_fence_test")
        result = store.upsert_acquisition_shard_registry(dict(SIMULATE_PAYLOAD))
        refusal = result.get("live_schema_write_refused")
        self.assertIsNotNone(refusal, result)
        self.assertEqual(refusal["reason"], "offline_provider_row_in_live_schema")
        self.assertEqual(refusal["table"], "acquisition_shard_registry")
        self.assertEqual(refusal["marker"], "simulate_probe_36c1d5eed955875a")

    def test_non_live_schema_is_not_fenced(self) -> None:
        store = _bare_store("sourcing_test_env_abc")
        self.assertEqual(
            store._refuse_offline_provider_row_in_live_schema(
                dict(SIMULATE_PAYLOAD), table="acquisition_shard_registry"
            ),
            {},
        )

    def test_override_env_disables_the_fence(self) -> None:
        store = _bare_store("sourcing_live_fence_test")
        with patch.dict(
            os.environ, {"SOURCING_ALLOW_OFFLINE_PROVIDER_ROWS_IN_LIVE_SCHEMA": "1"}
        ):
            self.assertEqual(
                store._refuse_offline_provider_row_in_live_schema(
                    dict(SIMULATE_PAYLOAD), table="acquisition_shard_registry"
                ),
                {},
            )

    def test_clean_live_row_passes_the_fence_check(self) -> None:
        store = _bare_store("sourcing_live_fence_test")
        clean = {
            "shard_key": "cafecafecafecafecafecafe",
            "target_company": "Google",
            "snapshot_id": "20260722T113432",
            "metadata": {"probe_summary": {"run_id": "hbFYgTkamkP9GaXCv"}},
        }
        self.assertEqual(
            store._refuse_offline_provider_row_in_live_schema(
                clean, table="acquisition_shard_registry"
            ),
            {},
        )


if __name__ == "__main__":
    unittest.main()
