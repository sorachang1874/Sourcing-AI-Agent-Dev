"""Former-lane merge routes through the shared roster completion contract.

WS1 Step 2b (2026-07-22, master plan docs/REFACTOR_MASTER_PLAN.md): the former
per-function merge previously used a private all-or-blocked verdict with NO
truncation check — a provider-capped former shard was reported as full former
coverage, exactly the dishonesty class resolve_segmented_roster_completion was
built to prevent on the roster lane. The merge now derives its verdict through
that shared contract: failed shards count as missing (blocked, unchanged),
truncated/incomplete shards keep the lane PARTIAL.
"""

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sourcing_agent.acquisition import AcquisitionExecution, _merge_former_function_shard_executions
from sourcing_agent.connectors import CompanyIdentity
from sourcing_agent.seed_discovery import SearchSeedSnapshot


_TMP = Path(tempfile.mkdtemp(prefix="former_merge_test_"))
_IDENTITY = CompanyIdentity(
    requested_name="TML",
    canonical_name="TML",
    company_key="thinkingmachineslab",
    linkedin_slug="thinkingmachinesai",
    linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
)


def _snapshot(stop_reason: str = "", entries=None, incomplete_count: int = 0) -> SearchSeedSnapshot:
    return SearchSeedSnapshot(
        snapshot_id="snap",
        target_company="TML",
        company_identity=_IDENTITY,
        snapshot_dir=_TMP,
        entries=list(entries or []),
        query_summaries=[],
        accounts_used=[],
        errors=[],
        stop_reason=stop_reason,
        summary_path=_TMP / "summary.json",
        entries_path=_TMP / "entries.json",
        summary_payload={"incomplete_provider_query_count": incomplete_count},
        lane_payloads={},
        lane_entries={},
    )


def _shard_result(shard_id: str, *, status: str = "completed", snapshot=None, error: str = "") -> dict:
    execution = AcquisitionExecution(
        task_id="t",
        status=status,
        detail="",
        payload={},
        state_updates={"search_seed_snapshot": snapshot} if snapshot is not None else {},
    )
    return {"shard": {"shard_id": shard_id, "function_ids": ["8"]}, "execution": execution, "error": error}


PLAN = {"shards": [{"shard_id": "former_function_8"}, {"shard_id": "former_function_24"}]}


class FormerMergeCompletionContractTest(unittest.TestCase):
    def test_all_clean_shards_complete_fully(self) -> None:
        result = _merge_former_function_shard_executions(
            "task",
            [
                _shard_result("former_function_8", snapshot=_snapshot(entries=[{"profile_url": "https://x/in/a"}])),
                _shard_result("former_function_24", snapshot=_snapshot()),
            ],
            shard_plan=PLAN,
            cost_policy={},
        )
        self.assertEqual(result.status, "completed")
        completion = result.payload["former_function_shard_completion"]
        self.assertEqual(completion["completion_status"], "completed")
        self.assertEqual(completion["missing_shard_ids"], [])
        self.assertEqual(completion["truncated_shard_ids"], [])

    def test_failed_shard_still_blocks_and_counts_as_missing(self) -> None:
        result = _merge_former_function_shard_executions(
            "task",
            [
                _shard_result("former_function_8", snapshot=_snapshot()),
                _shard_result("former_function_24", status="blocked", error="provider down"),
            ],
            shard_plan=PLAN,
            cost_policy={},
        )
        self.assertEqual(result.status, "blocked")
        completion = result.payload["former_function_shard_completion"]
        self.assertEqual(completion["completion_status"], "partial")
        self.assertEqual(completion["missing_shard_ids"], ["former_function_24"])

    def test_truncated_shard_keeps_the_lane_partial_never_full_coverage(self) -> None:
        # Before Step 2b this scenario reported full former coverage: every
        # shard "completed", but one scan was provider-incomplete.
        result = _merge_former_function_shard_executions(
            "task",
            [
                _shard_result("former_function_8", snapshot=_snapshot()),
                _shard_result(
                    "former_function_24",
                    snapshot=_snapshot(stop_reason="provider_people_search_incomplete", incomplete_count=1),
                ),
            ],
            shard_plan=PLAN,
            cost_policy={},
        )
        self.assertEqual(result.status, "completed")
        completion = result.payload["former_function_shard_completion"]
        self.assertEqual(completion["completion_status"], "partial")
        self.assertEqual(completion["truncated_shard_ids"], ["former_function_24"])
        self.assertIn("PARTIAL", result.detail)
        self.assertEqual(result.payload["stop_reason"], "former_function_shards_incomplete")


if __name__ == "__main__":
    unittest.main()
