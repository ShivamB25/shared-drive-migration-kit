from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
APP_PATH = REPO_ROOT / "adapters" / "modal_volume" / "modal_shared_drive_app.py"
SPEC = importlib.util.spec_from_file_location("modal_shared_drive_app", APP_PATH)
assert SPEC is not None and SPEC.loader is not None
APP = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(APP)


class ArchivePlanningTests(unittest.TestCase):
    def test_contiguous_batches_preserve_order_and_limits(self) -> None:
        batches = APP.pack_contiguous_archive_members(
            [
                {"source_path": "en/a", "bytes": 60, "entries": 2},
                {"source_path": "en/b", "bytes": 40, "entries": 2},
                {"source_path": "en/c", "bytes": 50, "entries": 2},
            ],
            max_bytes=100,
            max_entries=10,
            max_roots=10,
        )

        self.assertEqual(
            [[member["source_path"] for member in batch] for batch in batches],
            [["en/a", "en/b"], ["en/c"]],
        )

    def test_batch_paths_are_human_readable_and_indexes_are_adjacent(self) -> None:
        archive, package_index, files_index = APP.batch_target_paths("MassivePodCasts", "en", 2)

        self.assertEqual(archive, "MassivePodCasts/en/batches/en-batch-00002.tar.zst")
        self.assertEqual(package_index, "MassivePodCasts/en/batches/en-batch-00002.package.index.json")
        self.assertEqual(files_index, "MassivePodCasts/en/batches/en-batch-00002.files.index.jsonl.zst")

    @unittest.skipUnless(shutil.which("tar") and shutil.which("zstd"), "tar and zstd are required")
    def test_multi_root_archive_restores_original_paths(self) -> None:
        original_mount = APP.SOURCE_MOUNT
        try:
            with tempfile.TemporaryDirectory() as tmp_text:
                tmp = Path(tmp_text)
                source = tmp / "src"
                (source / "en" / "podcast one" / "audio").mkdir(parents=True)
                (source / "en" / "podcast-two").mkdir(parents=True)
                (source / "en" / "podcast one" / "audio" / "episode.mp3").write_bytes(b"audio-data")
                (source / "en" / "podcast-two" / "feed.xml").write_text("<rss/>")
                (source / "metadata file.txt").write_text("metadata")
                APP.SOURCE_MOUNT = source

                roots = ["en/podcast one", "en/podcast-two", "metadata file.txt"]
                archive = tmp / "package.tar.zst"
                APP.create_archive_staged_many(roots, archive, 3)

                index_dir = tmp / "indexes"
                index_dir.mkdir()
                index_info = APP.write_indexes_many(
                    [source / root for root in roots],
                    "test-volume",
                    roots,
                    "target/package.tar.zst",
                    "target/package.index.json",
                    "target/files.index.jsonl.zst",
                    index_dir,
                    1024**3,
                    900 * 1024**2,
                )
                finalized = APP.finalize_package_index(index_dir / "package.index.json", archive)

                restore = tmp / "restore"
                restore.mkdir()
                subprocess.run(["tar", "-xf", str(archive), "-C", str(restore)], check=True)
                package_index = json.loads((index_dir / "package.index.json").read_text())

                self.assertEqual(
                    (restore / "en" / "podcast one" / "audio" / "episode.mp3").read_bytes(),
                    b"audio-data",
                )
                self.assertEqual((restore / "en" / "podcast-two" / "feed.xml").read_text(), "<rss/>")
                self.assertEqual((restore / "metadata file.txt").read_text(), "metadata")
                self.assertEqual(package_index["source"]["paths"], roots)
                self.assertTrue(package_index["package"]["independently_extractable"])
                self.assertEqual(package_index["package"]["archive_sha256"], finalized["archive_sha256"])
                self.assertEqual(package_index["package"]["archive_bytes"], archive.stat().st_size)
                self.assertEqual(index_info["records"], 6)
        finally:
            APP.SOURCE_MOUNT = original_mount


if __name__ == "__main__":
    unittest.main()
