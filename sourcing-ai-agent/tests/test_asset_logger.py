import tempfile
import unittest

from sourcing_agent.asset_logger import AssetLogger


class AssetLoggerTest(unittest.TestCase):
    def test_registry_tracks_json_and_text_assets(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            logger = AssetLogger(tempdir)
            json_path = logger.write_json(
                "raw/payload.json",
                {"hello": "world"},
                asset_type="test_json",
                source_kind="unit_test",
                is_raw_asset=True,
                model_safe=False,
                metadata={"stage": "raw"},
            )
            text_path = logger.write_text(
                "notes/summary.txt",
                "compact summary",
                asset_type="test_text",
                source_kind="unit_test",
                content_type="text/plain",
                is_raw_asset=False,
                model_safe=True,
                metadata={"stage": "summary"},
            )

            self.assertTrue(json_path.exists())
            self.assertTrue(text_path.exists())

            entries = logger.list_entries()
            self.assertEqual(len(entries), 2)
            by_path = {entry["relative_path"]: entry for entry in entries}
            self.assertIn("raw/payload.json", by_path)
            self.assertIn("notes/summary.txt", by_path)
            self.assertTrue(by_path["raw/payload.json"]["is_raw_asset"])
            self.assertFalse(by_path["raw/payload.json"]["model_safe"])
            self.assertFalse(by_path["notes/summary.txt"]["is_raw_asset"])
            self.assertTrue(by_path["notes/summary.txt"]["model_safe"])
