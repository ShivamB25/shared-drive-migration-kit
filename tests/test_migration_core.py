from __future__ import annotations

import unittest

from migration_core import (
    archive_triplet_paths,
    clean_relative_path,
    pack_contiguous_members,
    parse_bytes,
    retry_with_exponential_backoff,
    row_belongs_to_worker,
)


class MigrationCoreTests(unittest.TestCase):
    def test_size_and_path_helpers_reject_unsafe_values(self) -> None:
        self.assertEqual(parse_bytes("200GiB"), 200 * 1024**3)
        self.assertEqual(clean_relative_path("/archive/part-001").as_posix(), "archive/part-001")
        with self.assertRaises(ValueError):
            clean_relative_path("../../outside")

    def test_contiguous_pack_keeps_source_roots_and_limit(self) -> None:
        batches = pack_contiguous_members(
            [
                {"source_path": "en/b", "bytes": 40, "entries": 2},
                {"source_path": "en/a", "bytes": 60, "entries": 2},
                {"source_path": "en/c", "bytes": 50, "entries": 2},
            ],
            max_bytes=100,
            max_entries=10,
            max_roots=10,
        )
        self.assertEqual([[member["source_path"] for member in batch] for batch in batches], [["en/a", "en/b"], ["en/c"]])

    def test_archive_triplet_is_adjacent_and_extractable_by_convention(self) -> None:
        self.assertEqual(
            archive_triplet_paths("dataset", "en", 2),
            (
                "dataset/en/batches/en-batch-00002.tar.zst",
                "dataset/en/batches/en-batch-00002.package.index.json",
                "dataset/en/batches/en-batch-00002.files.index.jsonl.zst",
            ),
        )

    def test_worker_assignment_is_deterministic(self) -> None:
        self.assertTrue(row_belongs_to_worker(3, 10, 1, 3, "contiguous"))
        self.assertFalse(row_belongs_to_worker(3, 10, 0, 3, "contiguous"))

    def test_retry_preserves_failure_and_backs_off_only_for_transient_error(self) -> None:
        attempts = 0
        sleeps: list[float] = []

        def operation() -> str:
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise RuntimeError("transient")
            return "done"

        result = retry_with_exponential_backoff(
            operation,
            should_retry=lambda exc: str(exc) == "transient",
            retries=3,
            base_delay_seconds=2,
            sleep=sleeps.append,
        )

        self.assertEqual(result, "done")
        self.assertEqual(sleeps, [2, 4])


if __name__ == "__main__":
    unittest.main()
