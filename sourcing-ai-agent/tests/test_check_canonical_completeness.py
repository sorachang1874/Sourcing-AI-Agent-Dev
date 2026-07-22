import hashlib
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

_SPEC = importlib.util.spec_from_file_location(
    "check_canonical_completeness", REPO_ROOT / "scripts" / "check_canonical_completeness.py"
)
ccc = importlib.util.module_from_spec(_SPEC)
assert _SPEC.loader is not None
_SPEC.loader.exec_module(ccc)

from sourcing_agent.seed_discovery import normalize_name_token


def _sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def _alias_entry(company: str, name: str, url: str) -> dict:
    return {
        "canonical_id": _sha1("|".join([normalize_name_token(company), normalize_name_token(name), url]))[:16],
        "name": name,
        "linkedin_url": url,
        "aliases": {
            "harvest_url_hash_16hex": _sha1(url)[:16],
            "v1_seed_name_hash_12hex": _sha1(name)[:12],
        },
    }


class AliasMapCheckTest(unittest.TestCase):
    def test_consistent_map_passes_and_drift_fails(self) -> None:
        entry = _alias_entry("openai", "Ada Example", "https://www.linkedin.com/in/ada")
        alias_map = {"company_key": "openai", "entries": [entry]}
        self.assertEqual(ccc.check_alias_map(alias_map), [])

        entry["aliases"]["harvest_url_hash_16hex"] = "0" * 16
        drift = ccc.check_alias_map(alias_map)
        self.assertEqual(len(drift), 1)
        self.assertEqual(drift[0]["check"], "alias_map_hash_drift")
        self.assertEqual(drift[0]["field"], "harvest_url_hash_16hex")


class DocumentsCheckTest(unittest.TestCase):
    def test_alias_documented_pair_is_allowed_but_unknown_dup_is_flagged(self) -> None:
        url = "https://www.linkedin.com/in/ada"
        entry = _alias_entry("openai", "Ada Example", url)
        alias_map = {"company_key": "openai", "entries": [entry]}
        documents = [
            {"candidate_id": entry["canonical_id"], "name_en": "Ada Example", "linkedin_url": url},
            {"candidate_id": entry["aliases"]["harvest_url_hash_16hex"], "name_en": "Ada Example", "linkedin_url": url},
            {"candidate_id": "a" * 16, "name_en": "Bob Twin", "linkedin_url": "https://www.linkedin.com/in/bob"},
            {"candidate_id": "b" * 16, "name_en": "Bob Twin", "linkedin_url": "https://www.linkedin.com/in/bob"},
        ]
        violations = ccc.check_documents(documents, alias_map)
        self.assertEqual([v["check"] for v in violations], ["duplicate_identity"])
        self.assertEqual(violations[0]["name"], "Bob Twin")

    def test_bad_id_format_is_flagged(self) -> None:
        violations = ccc.check_documents(
            [{"candidate_id": "XYZ", "name_en": "Cara", "linkedin_url": "https://l.example/c"}], None
        )
        self.assertEqual([v["check"] for v in violations], ["candidate_id_format"])


class PointerCheckTest(unittest.TestCase):
    def test_pointer_resolves_by_snapshot_id_not_absolute_dir(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            company_dir = Path(root) / "openai"
            snapshot_dir = company_dir / "20260101T000000"
            snapshot_dir.mkdir(parents=True)
            (snapshot_dir / "candidate_documents.json").write_text("[]")
            (company_dir / "latest_snapshot.json").write_text(
                json.dumps({"snapshot_id": "20260101T000000", "snapshot_dir": "/some/other/machine/path"})
            )
            violations, documents_path = ccc.check_pointer(company_dir)
            self.assertEqual(violations, [])
            self.assertEqual(documents_path, snapshot_dir / "candidate_documents.json")

    def test_dangling_pointer_is_flagged(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            company_dir = Path(root) / "openai"
            company_dir.mkdir(parents=True)
            (company_dir / "latest_snapshot.json").write_text(json.dumps({"snapshot_id": "20991231T000000"}))
            violations, documents_path = ccc.check_pointer(company_dir)
            self.assertIsNone(documents_path)
            self.assertEqual([v["check"] for v in violations], ["pointer_dangling"])


class CommittedOpenAiAliasMapTest(unittest.TestCase):
    def test_committed_alias_map_is_hash_consistent(self) -> None:
        alias_map = json.loads(
            (REPO_ROOT / "configs" / "identity" / "openai_identity_alias_map_v1.json").read_text(encoding="utf-8")
        )
        self.assertEqual(len(alias_map["entries"]), 40)
        self.assertEqual(ccc.check_alias_map(alias_map), [])


if __name__ == "__main__":
    unittest.main()
