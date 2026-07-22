import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "live_apify_dataset_salvage.py"


def _roster_item(slug: str, first: str, last: str) -> dict:
    return {
        "linkedinUrl": f"https://www.linkedin.com/in/{slug}",
        "firstName": first,
        "lastName": last,
        "location": {"linkedinText": "United States"},
        "currentPositions": [{"title": "Engineer"}],
    }


class SalvageAdapterFunctionAttributionTest(unittest.TestCase):
    """Function-sharded roster adoption must preserve per-shard function
    attribution (the GDM fn8/fn24 salvage, 2026-07-22): the earlier merged-query
    salvage note must never be stamped onto sharded lanes."""

    def _run(self, workdir: Path, extra_args: list[str]) -> subprocess.CompletedProcess:
        base = [
            sys.executable,
            str(SCRIPT),
            "--company-assets-root",
            str(workdir / "company_assets"),
            "--snapshot-id",
            "20260722T000000",
            "--identity-from",
            str(workdir / "identity.json"),
            "--apply",
        ]
        return subprocess.run(base + extra_args, capture_output=True, text=True, check=False)

    def _setup(self, workdir: Path) -> None:
        (workdir / "identity.json").write_text(
            json.dumps(
                {
                    "requested_name": "Google DeepMind",
                    "canonical_name": "Google DeepMind",
                    "company_key": "google",
                }
            ),
            encoding="utf-8",
        )

    def test_sharded_rosters_keep_function_attribution(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            workdir = Path(root)
            self._setup(workdir)
            fn8 = workdir / "ds_fn8.json"
            fn8.write_text(json.dumps([_roster_item("alice-a", "Alice", "A")]), encoding="utf-8")
            fn24 = workdir / "ds_fn24.json"
            fn24.write_text(json.dumps([_roster_item("bob-b", "Bob", "B")]), encoding="utf-8")

            proc = self._run(
                workdir,
                [
                    "--current-roster-dataset", str(fn8),
                    "--current-roster-function", "8",
                    "--current-roster-run-id", "runA",
                    "--current-roster-dataset", str(fn24),
                    "--current-roster-function", "24",
                    "--current-roster-run-id", "runB",
                ],
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)

            snapshot = workdir / "company_assets" / "20260722T000000"
            docs = json.loads((snapshot / "candidate_documents.json").read_text(encoding="utf-8"))
            by_slug = {
                c["metadata"]["seed_slug"]: c for c in docs["candidates"]
            }
            self.assertEqual(by_slug["alice-a"]["metadata"]["salvage_function_id"], "8")
            self.assertEqual(by_slug["bob-b"]["metadata"]["salvage_function_id"], "24")
            self.assertEqual(by_slug["alice-a"]["metadata"]["salvage_run_id"], "runA")
            self.assertEqual(by_slug["bob-b"]["metadata"]["salvage_run_id"], "runB")
            self.assertIn("function 8", by_slug["alice-a"]["metadata"]["salvage_note"])
            self.assertNotIn("untagged", by_slug["alice-a"]["metadata"]["salvage_note"])

            shards = docs["acquisition_sources"]["current_roster_salvage"]["shards"]
            self.assertEqual(
                [(s["function"], s["run_id"], s["dataset_id"]) for s in shards],
                [("8", "runA", "ds_fn8"), ("24", "runB", "ds_fn24")],
            )

    def test_unsharded_roster_keeps_untagged_semantics(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            workdir = Path(root)
            self._setup(workdir)
            roster = workdir / "ds_merged.json"
            roster.write_text(json.dumps([_roster_item("carol-c", "Carol", "C")]), encoding="utf-8")

            proc = self._run(
                workdir,
                ["--current-roster-dataset", str(roster), "--current-run-id", "runX"],
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)

            snapshot = workdir / "company_assets" / "20260722T000000"
            docs = json.loads((snapshot / "candidate_documents.json").read_text(encoding="utf-8"))
            (candidate,) = docs["candidates"]
            self.assertNotIn("salvage_function_id", candidate["metadata"])
            self.assertIn("untagged", candidate["metadata"]["salvage_note"])
            self.assertEqual(candidate["metadata"]["salvage_run_id"], "runX")
            note = docs["acquisition_sources"]["current_roster_salvage"]["note"]
            self.assertIn("untagged", note)


class SalvagePassthroughPreservationTest(unittest.TestCase):
    """Pass-through candidate documents that fail candidate_from_payload
    re-validation (the TML empty-name former rows, paid coverage pending a name
    resolve) must survive the rebuild verbatim instead of silently shrinking
    the union."""

    def test_invalid_passthrough_docs_are_preserved_with_marker(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            workdir = Path(root)
            (workdir / "identity.json").write_text(
                json.dumps({"canonical_name": "TML", "company_key": "thinkingmachineslab"}),
                encoding="utf-8",
            )
            prior_docs = workdir / "prior_candidate_documents.json"
            prior_docs.write_text(
                json.dumps(
                    {
                        "candidates": [
                            {
                                "candidate_id": "aaaaaaaaaaaaaaaa",
                                "name_en": "Dana D",
                                "display_name": "Dana D",
                                "category": "employee",
                                "target_company": "TML",
                                "employment_status": "current",
                                "linkedin_url": "https://www.linkedin.com/in/dana-d",
                            },
                            {
                                "candidate_id": "bbbbbbbbbbbbbbbb",
                                "name_en": "",
                                "display_name": "",
                                "category": "former_employee",
                                "target_company": "TML",
                                "employment_status": "former",
                                "linkedin_url": "https://www.linkedin.com/in/ACwAAAcgPQIBTNw3",
                            },
                        ]
                    }
                ),
                encoding="utf-8",
            )
            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--company-assets-root", str(workdir / "company_assets"),
                    "--snapshot-id", "20260722T000000",
                    "--identity-from", str(workdir / "identity.json"),
                    "--former-candidate-documents", str(prior_docs),
                    "--apply",
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)

            snapshot = workdir / "company_assets" / "20260722T000000"
            docs = json.loads((snapshot / "candidate_documents.json").read_text(encoding="utf-8"))
            self.assertEqual(docs["candidate_count"], 2)
            preserved = [
                c
                for c in docs["candidates"]
                if dict(c.get("metadata") or {}).get("salvage_passthrough_invalid")
            ]
            self.assertEqual(len(preserved), 1)
            self.assertEqual(preserved[0]["employment_status"], "former")
            self.assertEqual(
                preserved[0]["metadata"]["salvage_passthrough_origin"], str(prior_docs)
            )
            self.assertNotIn("_salvage_passthrough_origin", preserved[0])

            manifest = json.loads((snapshot / "salvage_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["counts"]["passthrough_preserved_invalid"], 1)
            self.assertEqual(manifest["counts"]["union_candidates"], 2)


if __name__ == "__main__":
    unittest.main()
