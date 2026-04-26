import json
import tempfile
import unittest
from pathlib import Path

from sourcing_agent.acquisition import _merge_search_seed_snapshots
from sourcing_agent.asset_reuse_planning import (
    _query_filters_cover_requested,
    ensure_acquisition_shard_registry_for_snapshot,
)
from sourcing_agent.connectors import CompanyIdentity
from sourcing_agent.search_seed_registry import persist_search_seed_snapshot
from sourcing_agent.seed_discovery import SearchSeedSnapshot
from sourcing_agent.storage import ControlPlaneStore


class SearchSeedRegistryContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.runtime_dir = Path(self.tempdir.name)
        self.store = ControlPlaneStore(self.runtime_dir / "test.db")

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _write_snapshot_identity(self, *, target_company: str, snapshot_id: str) -> tuple[Path, CompanyIdentity]:
        company_key = target_company.strip().lower()
        company_dir = self.runtime_dir / "company_assets" / company_key
        snapshot_dir = company_dir / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity_payload = {
            "requested_name": target_company,
            "canonical_name": target_company,
            "company_key": company_key,
            "linkedin_slug": company_key,
            "linkedin_company_url": f"https://www.linkedin.com/company/{company_key}/",
            "aliases": [target_company],
        }
        (snapshot_dir / "identity.json").write_text(
            json.dumps(identity_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (company_dir / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_id,
                    "company_identity": identity_payload,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        identity = CompanyIdentity(
            requested_name=target_company,
            canonical_name=target_company,
            company_key=company_key,
            linkedin_slug=company_key,
            linkedin_company_url=identity_payload["linkedin_company_url"],
        )
        return snapshot_dir, identity

    def _make_lane_snapshot(
        self,
        *,
        snapshot_dir: Path,
        identity: CompanyIdentity,
        employment_scope: str,
        query: str,
        result_count: int,
    ) -> SearchSeedSnapshot:
        discovery_dir = snapshot_dir / "search_seed_discovery"
        summary_path = discovery_dir / "summary.json"
        entries_path = discovery_dir / "entries.json"
        filter_hints = {
            ("past_companies" if employment_scope == "former" else "current_companies"): [
                str(identity.linkedin_company_url or "")
            ],
            "function_ids": ["24", "8"],
            "keywords": [query, "research"],
        }
        query_summary = {
            "query": query,
            "effective_query_text": query,
            "mode": "harvest_profile_search",
            "raw_path": str(discovery_dir / f"{employment_scope}_{query}.json"),
            "seed_entry_count": result_count,
            "employment_scope": employment_scope,
            "employment_status": employment_scope,
            "strategy_type": "former_employee_search" if employment_scope == "former" else "scoped_search_roster",
            "filter_hints": filter_hints,
        }
        lane_payload = {
            "lane": "profile_search",
            "employment_scope": employment_scope,
            "employment_status": employment_scope,
            "strategy_type": query_summary["strategy_type"],
            "requested_filter_hints": filter_hints,
            "effective_filter_hints": filter_hints,
            "query_summaries": [query_summary],
        }
        entries = [
            {
                "seed_key": f"{employment_scope}_{index}",
                "full_name": f"{employment_scope.title()} Member {index}",
                "source_type": "harvest_profile_search",
                "employment_status": employment_scope,
                "profile_url": f"https://www.linkedin.com/in/{employment_scope}-{index}/",
            }
            for index in range(result_count)
        ]
        return SearchSeedSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company=identity.canonical_name,
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=entries,
            query_summaries=[query_summary],
            accounts_used=["harvest_profile_search"],
            errors=[],
            stop_reason="completed",
            summary_path=summary_path,
            entries_path=entries_path,
            summary_payload={
                "requested_filter_hints": filter_hints,
                "effective_filter_hints": filter_hints,
            },
            lane_payloads={employment_scope: lane_payload},
            lane_entries={employment_scope: entries},
        )

    def test_merge_persists_per_lane_search_seed_summaries(self) -> None:
        snapshot_dir, identity = self._write_snapshot_identity(target_company="OpenAI", snapshot_id="20260422T171923")
        current_snapshot = self._make_lane_snapshot(
            snapshot_dir=snapshot_dir,
            identity=identity,
            employment_scope="current",
            query="Math",
            result_count=52,
        )
        former_snapshot = self._make_lane_snapshot(
            snapshot_dir=snapshot_dir,
            identity=identity,
            employment_scope="former",
            query="Math",
            result_count=15,
        )

        merged = _merge_search_seed_snapshots(current_snapshot, former_snapshot)
        assert merged is not None

        self.assertTrue((snapshot_dir / "search_seed_discovery" / "current" / "summary.json").exists())
        self.assertTrue((snapshot_dir / "search_seed_discovery" / "former" / "summary.json").exists())

        ensure_acquisition_shard_registry_for_snapshot(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company="OpenAI",
            snapshot_id="20260422T171923",
        )

        rows = [
            row
            for row in self.store.list_acquisition_shard_registry(
                target_company="OpenAI",
                snapshot_ids=["20260422T171923"],
                limit=20,
            )
            if str(row.get("lane") or "") == "profile_search" and str(row.get("search_query") or "") == "Math"
        ]
        self.assertEqual(len(rows), 2)
        self.assertEqual({str(row.get("employment_scope") or "") for row in rows}, {"current", "former"})
        self.assertEqual(
            {str(row.get("employment_scope") or ""): int(row.get("result_count") or 0) for row in rows},
            {"current": 52, "former": 15},
        )

    def test_persist_single_lane_snapshot_materializes_current_lane_files(self) -> None:
        snapshot_dir, identity = self._write_snapshot_identity(target_company="OpenAI", snapshot_id="20260422T171922")
        current_snapshot = self._make_lane_snapshot(
            snapshot_dir=snapshot_dir,
            identity=identity,
            employment_scope="current",
            query="Math",
            result_count=52,
        )

        persisted = persist_search_seed_snapshot(current_snapshot)

        self.assertEqual(sorted(persisted.lane_payloads.keys()), ["current"])
        self.assertTrue((snapshot_dir / "search_seed_discovery" / "current" / "summary.json").exists())
        self.assertTrue((snapshot_dir / "search_seed_discovery" / "current" / "entries.json").exists())
        self.assertEqual(
            int(dict(persisted.lane_payloads.get("current") or {}).get("entry_count") or 0),
            52,
        )

    def test_legacy_aggregate_summary_can_recover_both_lanes_when_query_entries_are_explicit(self) -> None:
        snapshot_dir, identity = self._write_snapshot_identity(target_company="OpenAI", snapshot_id="20260422T171924")
        discovery_dir = snapshot_dir / "search_seed_discovery"
        discovery_dir.mkdir(parents=True, exist_ok=True)
        current_filter_hints = {
            "current_companies": [str(identity.linkedin_company_url or "")],
            "function_ids": ["24", "8"],
            "keywords": ["Math", "research"],
        }
        former_filter_hints = {
            "past_companies": [str(identity.linkedin_company_url or "")],
            "function_ids": ["24", "8"],
            "keywords": ["Math", "research"],
        }
        (discovery_dir / "summary.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_dir.name,
                    "target_company": identity.canonical_name,
                    "company_identity": identity.to_record(),
                    "query_summaries": [
                        {
                            "query": "Math",
                            "effective_query_text": "Math",
                            "mode": "harvest_profile_search",
                            "seed_entry_count": 52,
                            "employment_scope": "current",
                            "employment_status": "current",
                            "strategy_type": "scoped_search_roster",
                            "filter_hints": current_filter_hints,
                        },
                        {
                            "query": "Math",
                            "effective_query_text": "Math",
                            "mode": "harvest_profile_search",
                            "seed_entry_count": 15,
                            "employment_scope": "former",
                            "employment_status": "former",
                            "strategy_type": "former_employee_search",
                            "filter_hints": former_filter_hints,
                        },
                    ],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        ensure_acquisition_shard_registry_for_snapshot(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company="OpenAI",
            snapshot_id="20260422T171924",
        )

        rows = [
            row
            for row in self.store.list_acquisition_shard_registry(
                target_company="OpenAI",
                snapshot_ids=["20260422T171924"],
                limit=20,
            )
            if str(row.get("lane") or "") == "profile_search" and str(row.get("search_query") or "") == "Math"
        ]
        self.assertEqual(len(rows), 2)

    def test_language_model_rows_persist_text_query_family_metadata(self) -> None:
        snapshot_dir, identity = self._write_snapshot_identity(target_company="OpenAI", snapshot_id="20260423T165904")
        current_snapshot = self._make_lane_snapshot(
            snapshot_dir=snapshot_dir,
            identity=identity,
            employment_scope="current",
            query="Language Model",
            result_count=52,
        )

        persist_search_seed_snapshot(current_snapshot)
        ensure_acquisition_shard_registry_for_snapshot(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company="OpenAI",
            snapshot_id="20260423T165904",
        )

        rows = [
            row
            for row in self.store.list_acquisition_shard_registry(
                target_company="OpenAI",
                snapshot_ids=["20260423T165904"],
                limit=20,
            )
            if str(row.get("lane") or "") == "profile_search"
            and str(row.get("search_query") or "") == "Language Model"
        ]
        self.assertEqual(len(rows), 1)
        query_family = dict(dict(rows[0].get("metadata") or {}).get("query_family") or {})
        self.assertEqual(query_family.get("canonical_label"), "Text")
        self.assertIn("text", list(query_family.get("signatures") or []))

        compatible, _ = _query_filters_cover_requested(
            desired_query="Text",
            desired_metadata=None,
            existing_query=rows[0].get("search_query"),
            existing_metadata=dict(rows[0].get("metadata") or {}),
        )
        self.assertTrue(compatible)
        multimodal_compatible, _ = _query_filters_cover_requested(
            desired_query="Multimodal",
            desired_metadata=None,
            existing_query=rows[0].get("search_query"),
            existing_metadata=dict(rows[0].get("metadata") or {}),
        )
        self.assertFalse(multimodal_compatible)
        self.assertTrue((snapshot_dir / "search_seed_discovery" / "current" / "summary.json").exists())


if __name__ == "__main__":
    unittest.main()
