import os
import tempfile
import unittest
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sourcing_agent.artifact_cache import materialize_link_first_file, mirror_tree_link_first


class MaterializeLinkFirstFileTest(unittest.TestCase):
    def test_same_path_call_preserves_content_and_never_self_symlinks(self) -> None:
        # Regression: hot-cache sync callers can be handed a snapshot_dir that
        # IS the hot-cache destination (registry source_path pointing into the
        # cache). Without the same-file guard the destination removal deletes
        # the only copy and the symlink fallback leaves `X.json -> X.json`.
        with tempfile.TemporaryDirectory() as root:
            target = Path(root) / "candidates" / "a.json"
            target.parent.mkdir(parents=True)
            target.write_text('{"k": 1}')

            mode = materialize_link_first_file(target, target)

            self.assertEqual(mode, "same_path")
            self.assertFalse(target.is_symlink())
            self.assertEqual(target.read_text(), '{"k": 1}')

    def test_same_inode_alias_is_kept_without_relinking(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            source = Path(root) / "src.json"
            source.write_text("payload")
            alias = Path(root) / "alias.json"
            os.link(source, alias)

            mode = materialize_link_first_file(source, alias)

            self.assertEqual(mode, "already_linked")
            self.assertEqual(alias.read_text(), "payload")

    def test_existing_self_symlink_destination_is_repaired_from_real_source(self) -> None:
        # A destination corrupted into a dangling self-loop (the observed
        # 2026-07-21 hot-cache damage) must be replaced, not tripped over.
        with tempfile.TemporaryDirectory() as root:
            source = Path(root) / "src.json"
            source.write_text("good")
            broken = Path(root) / "view" / "b.json"
            broken.parent.mkdir(parents=True)
            broken.symlink_to("b.json")

            mode = materialize_link_first_file(source, broken)

            self.assertIn(mode, {"hardlink", "symlink", "copy"})
            self.assertEqual(broken.read_text(), "good")

    def test_distinct_paths_still_link(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            source = Path(root) / "src.json"
            source.write_text("data")
            destination = Path(root) / "dest" / "src.json"

            mode = materialize_link_first_file(source, destination)

            self.assertIn(mode, {"hardlink", "symlink", "copy"})
            self.assertEqual(destination.read_text(), "data")


class MirrorTreeLinkFirstTest(unittest.TestCase):
    def test_same_directory_mirror_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            snapshot = Path(root) / "snap"
            snapshot.mkdir()
            (snapshot / "identity.json").write_text("{}")

            summary = mirror_tree_link_first(snapshot, snapshot)

            self.assertEqual(summary["status"], "same_path")
            self.assertEqual((snapshot / "identity.json").read_text(), "{}")


if __name__ == "__main__":
    unittest.main()
