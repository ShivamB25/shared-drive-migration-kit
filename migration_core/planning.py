"""Deterministic package and worker planning primitives."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from .paths import posix_join


def package_strategy(bytes_total: int | None, max_package_bytes: int, warn_package_bytes: int) -> str:
    """Classify a package row without taking any source or target action."""
    if bytes_total is None:
        return "unknown"
    if bytes_total > max_package_bytes:
        return "split_required"
    if bytes_total > warn_package_bytes:
        return "warn_large_single_tar_zst"
    return "single_tar_zst"


def row_belongs_to_worker(
    row_index: int,
    total_rows: int,
    worker_index: int,
    worker_count: int,
    assignment_mode: str,
) -> bool:
    """Assign an ordered plan row deterministically to one worker."""
    if worker_count <= 1:
        return worker_index == 0
    if assignment_mode == "modulo":
        return row_index % worker_count == worker_index
    if assignment_mode == "contiguous":
        start = (total_rows * worker_index) // worker_count
        end = (total_rows * (worker_index + 1)) // worker_count
        return start <= row_index < end
    raise ValueError("assignment_mode must be 'modulo' or 'contiguous'")


def pack_contiguous_members(
    members: Iterable[dict[str, Any]],
    max_bytes: int,
    max_entries: int,
    max_roots: int,
) -> list[list[dict[str, Any]]]:
    """Pack ordered rows without breaking a source root across package batches."""
    if max_bytes <= 0 or max_entries <= 0 or max_roots <= 0:
        raise ValueError("package limits must be positive")

    batches: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    current_bytes = 0
    current_entries = 0
    for member in sorted(members, key=lambda item: str(item["source_path"])):
        member_bytes = int(member.get("bytes", 0) or 0)
        member_entries = int(member.get("entries", 1) or 1)
        if member_bytes > max_bytes or member_entries > max_entries:
            raise ValueError(f"oversized archive member was not recursively split: {member['source_path']}")
        would_exceed = current and (
            current_bytes + member_bytes > max_bytes
            or current_entries + member_entries > max_entries
            or len(current) >= max_roots
        )
        if would_exceed:
            batches.append(current)
            current = []
            current_bytes = 0
            current_entries = 0
        current.append(member)
        current_bytes += member_bytes
        current_entries += member_entries
    if current:
        batches.append(current)
    return batches


def archive_triplet_paths(dest_prefix: str, top_unit: str, batch_number: int) -> tuple[str, str, str]:
    """Return adjacent archive, package-index, and compressed-file-index paths."""
    if batch_number < 1:
        raise ValueError("batch_number must be >= 1")
    unit_name = top_unit.rstrip("/").split("/")[-1]
    safe_name = "".join(char if char.isalnum() or char in "-_." else "-" for char in unit_name).strip("-.")
    batch_stem = f"{safe_name or 'package'}-batch-{batch_number:05d}"
    base = posix_join(dest_prefix, top_unit, "batches", batch_stem)
    return (
        f"{base}.tar.zst",
        f"{base}.package.index.json",
        f"{base}.files.index.jsonl.zst",
    )
