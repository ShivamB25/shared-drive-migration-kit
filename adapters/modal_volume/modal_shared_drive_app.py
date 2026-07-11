from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import hashlib
import json
import os
import shutil
import shlex
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

# Allow direct ``modal run adapters/modal_volume/...py`` execution while keeping
# provider-neutral migration primitives at the repository root.
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from migration_core import (  # noqa: E402
    archive_triplet_paths as batch_target_paths,
    clean_relative_path,
    ensure_inside,
    package_strategy,
    pack_contiguous_members as pack_contiguous_archive_members,
    parse_bytes,
    posix_join,
    relative_path_under_prefix,
    retry_with_exponential_backoff,
    row_belongs_to_worker,
)

import modal  # noqa: E402


APP_NAME = os.environ.get("MODAL_APP_NAME", "shared-drive-migration-modal-volume")
SOURCE_VOLUME_NAME = os.environ.get("MODAL_SOURCE_VOLUME_NAME", "source-volume-name")
CREDS_VOLUME_NAME = os.environ.get("MODAL_CREDS_VOLUME_NAME", "sdmig-credentials")
STATE_VOLUME_NAME = os.environ.get("MODAL_STATE_VOLUME_NAME", "sdmig-state")
CACHE_VOLUME_NAME = os.environ.get("MODAL_CACHE_VOLUME_NAME", "sdmig-cache")

SOURCE_MOUNT = Path("/src")
CREDS_MOUNT = Path("/creds")
STATE_MOUNT = Path("/state")
CACHE_MOUNT = Path("/cache")

RCLONE_CONFIG = CREDS_MOUNT / "modal-rclone-bundle" / "rclone.conf"
RCLONE_MANIFEST = CREDS_MOUNT / "modal-rclone-bundle" / "rclone.manifest.csv"

RCLONE_DRIVE_CHUNK_SIZE = os.environ.get("RCLONE_DRIVE_CHUNK_SIZE", "512M")
RCLONE_TRANSFERS = os.environ.get("RCLONE_TRANSFERS", "1")
RCLONE_CHECKERS = os.environ.get("RCLONE_CHECKERS", "4")
RCLONE_TPSLIMIT = os.environ.get("RCLONE_TPSLIMIT", "5")
RCLONE_TPSLIMIT_BURST = os.environ.get("RCLONE_TPSLIMIT_BURST", "0")
RCLONE_RETRIES = os.environ.get("RCLONE_RETRIES", "3")
RCLONE_LOW_LEVEL_RETRIES = os.environ.get("RCLONE_LOW_LEVEL_RETRIES", "20")
RCLONE_RETRIES_SLEEP = os.environ.get("RCLONE_RETRIES_SLEEP", "30s")
RCLONE_CONTIMEOUT = os.environ.get("RCLONE_CONTIMEOUT", "60s")
RCLONE_TIMEOUT = os.environ.get("RCLONE_TIMEOUT", "5m")
RCLONE_STATS = os.environ.get("RCLONE_STATS", "30s")
RCLONE_STATS_FILE_NAME_LENGTH = os.environ.get("RCLONE_STATS_FILE_NAME_LENGTH", "0")
RCLONE_LOG_LEVEL = os.environ.get("RCLONE_LOG_LEVEL", "INFO")
# Keep transient Drive user-rate limits retryable. Per-remote byte budgets
# enforce our daily-upload guard without converting 403 pacing responses into
# fatal worker failures.
RCLONE_DRIVE_STOP_ON_UPLOAD_LIMIT = os.environ.get("RCLONE_DRIVE_STOP_ON_UPLOAD_LIMIT", "0")
RCLONE_RATE_LIMIT_RETRIES = max(0, int(os.environ.get("RCLONE_RATE_LIMIT_RETRIES", "6")))
RCLONE_RATE_LIMIT_BACKOFF_SECONDS = max(1, int(os.environ.get("RCLONE_RATE_LIMIT_BACKOFF_SECONDS", "15")))
MODAL_MAX_UPLOADS_PER_REMOTE = int(os.environ.get("MODAL_MAX_UPLOADS_PER_REMOTE", "0"))
MODAL_CACHE_COMMIT_EVERY = max(1, int(os.environ.get("MODAL_CACHE_COMMIT_EVERY", "25")))
MODAL_DISCOVER_THREADS = int(os.environ.get("MODAL_DISCOVER_THREADS", "16"))
MODAL_DISCOVER_SHARD_DEPTH = int(os.environ.get("MODAL_DISCOVER_SHARD_DEPTH", "1"))
MODAL_MOUNTED_FIND_THREADS = int(os.environ.get("MODAL_MOUNTED_FIND_THREADS", "16"))


def env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if not value:
        return default
    return int(value)


app = modal.App(APP_NAME)

image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install(
        "bash",
        "ca-certificates",
        "coreutils",
        "curl",
        "findutils",
        "tar",
        "unzip",
        "zstd",
    )
    .run_commands("curl https://rclone.org/install.sh | bash")
    .add_local_python_source("migration_core")
)

source_volume = modal.Volume.from_name(SOURCE_VOLUME_NAME).with_mount_options(read_only=True)
creds_volume = modal.Volume.from_name(CREDS_VOLUME_NAME).with_mount_options(read_only=True)
state_volume = modal.Volume.from_name(STATE_VOLUME_NAME, create_if_missing=True)
cache_volume = modal.Volume.from_name(CACHE_VOLUME_NAME, create_if_missing=True)


def iter_unit_dirs(base: Path, depth: int):
    if depth < 0:
        raise ValueError("unit depth must be >= 0")
    if depth == 0:
        if base.is_dir():
            yield base
        return

    stack: list[tuple[Path, int]] = [(base, 0)]
    while stack:
        current, current_depth = stack.pop()
        if current_depth == depth:
            if current.is_dir():
                yield current
            continue
        try:
            children = sorted(
                [entry for entry in current.iterdir() if entry.is_dir() and not entry.is_symlink()],
                key=lambda item: item.name,
                reverse=True,
            )
        except FileNotFoundError:
            continue
        for child in children:
            stack.append((child, current_depth + 1))


def count_tree(path: Path) -> tuple[int, int, int]:
    files = 0
    dirs = 0
    bytes_total = 0
    for root, dirnames, filenames in os.walk(path):
        dirs += len(dirnames)
        for filename in filenames:
            file_path = Path(root) / filename
            try:
                stat = file_path.lstat()
            except FileNotFoundError:
                continue
            files += 1
            if file_path.is_file():
                bytes_total += stat.st_size
    return files, dirs, bytes_total


def entry_type_name(entry: Any) -> str:
    return getattr(getattr(entry, "type", None), "name", str(getattr(entry, "type", ""))).upper()


def entry_is_file(entry: Any) -> bool:
    return entry_type_name(entry).endswith("FILE")


def entry_is_dir(entry: Any) -> bool:
    return entry_type_name(entry).endswith("DIRECTORY")


def serialize_volume_entry(entry: Any) -> dict[str, Any]:
    public_attrs: dict[str, str] = {}
    for name in sorted(name for name in dir(entry) if not name.startswith("_")):
        try:
            value = getattr(entry, name)
        except Exception as exc:  # pragma: no cover - defensive metadata probe
            public_attrs[name] = f"<error: {exc}>"
            continue
        if callable(value):
            continue
        if hasattr(value, "name"):
            public_attrs[name] = getattr(value, "name")
        else:
            public_attrs[name] = repr(value)

    return {
        "path": str(getattr(entry, "path", "")),
        "type": entry_type_name(entry),
        "size": getattr(entry, "size", None),
        "mtime": str(getattr(entry, "mtime", "")),
        "repr": repr(entry),
        "public_attrs": public_attrs,
    }


def parse_inspect_paths(paths_csv: str) -> list[str]:
    paths = [part.strip().strip("/") for part in paths_csv.split(",") if part.strip()]
    return paths or [""]


def discover_unit_paths_api(volume: modal.Volume, source_prefix: str, unit_depth: int, limit: int) -> list[str]:
    source = source_prefix.strip("/")
    if unit_depth < 0:
        raise ValueError("unit depth must be >= 0")
    if unit_depth == 0:
        return [source]

    frontier = [source]
    for _depth in range(unit_depth):
        next_frontier: list[str] = []
        for current in frontier:
            entries = volume.listdir(current or "/", recursive=False)
            children = sorted(entry.path.strip("/") for entry in entries if entry_is_dir(entry))
            next_frontier.extend(children)
            if limit > 0 and len(next_frontier) >= limit and _depth == unit_depth - 1:
                return next_frontier[:limit]
        frontier = next_frontier
        if not frontier:
            break
    return frontier[:limit] if limit > 0 else frontier


def summarize_unit_api(volume: modal.Volume, unit_path: str) -> tuple[int, int, int]:
    files = 0
    dirs = 0
    bytes_total = 0
    entries = volume.listdir(unit_path or "/", recursive=True)
    for entry in entries:
        if entry_is_file(entry):
            files += 1
            bytes_total += int(getattr(entry, "size", 0) or 0)
        elif entry_is_dir(entry):
            dirs += 1
    return files, dirs, bytes_total


def write_api_plan_to_state_limited(
    source_volume_name: str,
    source_prefix: str,
    unit_depth: int,
    dest_prefix: str,
    plan_path: str,
    limit: int,
    max_package_bytes: str,
    warn_package_bytes: str,
) -> dict[str, Any]:
    volume = modal.Volume.from_name(source_volume_name)
    state = modal.Volume.from_name(STATE_VOLUME_NAME, create_if_missing=True)
    max_bytes = parse_bytes(max_package_bytes)
    warn_bytes = parse_bytes(warn_package_bytes)
    unit_paths = discover_unit_paths_api(volume, source_prefix, unit_depth, limit)

    total_files = 0
    total_dirs = 0
    total_bytes = 0
    strategy_counts: dict[str, int] = {}
    remote_plan_path = clean_relative_path(plan_path).as_posix()
    remote_summary_path = str(Path(remote_plan_path).with_suffix(".summary.json"))

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        local_plan = tmp_path / "plan.jsonl"
        local_summary = tmp_path / "summary.json"

        with local_plan.open("w") as handle:
            for index, unit_path in enumerate(unit_paths):
                files, dirs, bytes_total = summarize_unit_api(volume, unit_path)
                strategy = package_strategy(bytes_total, max_bytes, warn_bytes)
                archive_dest, package_index_dest, files_index_dest = target_paths(dest_prefix, unit_path)
                row = {
                    "index": index,
                    "source_volume": source_volume_name,
                    "source_path": unit_path,
                    "archive_dest": archive_dest,
                    "package_index_dest": package_index_dest,
                    "files_index_dest": files_index_dest,
                    "package_format": "tar.zst",
                    "files": files,
                    "directories": dirs,
                    "bytes": bytes_total,
                    "package_strategy": strategy,
                    "max_package_bytes": max_bytes,
                    "warn_package_bytes": warn_bytes,
                    "planner": "modal-volume-api",
                }
                handle.write(json.dumps(row, sort_keys=True) + "\n")
                total_files += files
                total_dirs += dirs
                total_bytes += bytes_total
                strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1

        summary = {
            "source_volume": source_volume_name,
            "source_prefix": source_prefix,
            "unit_depth": unit_depth,
            "dest_prefix": dest_prefix,
            "plan_path": f"/state/{remote_plan_path}",
            "planner": "modal-volume-api",
            "units": len(unit_paths),
            "files": total_files,
            "directories": total_dirs,
            "bytes": total_bytes,
            "max_package_bytes": max_bytes,
            "warn_package_bytes": warn_bytes,
            "strategy_counts": strategy_counts,
        }
        local_summary.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")

        with state.batch_upload(force=True) as batch:
            batch.put_file(local_plan, remote_plan_path)
            batch.put_file(local_summary, remote_summary_path)

    return summary


def inventory_path_for_plan(plan_path: str) -> str:
    path = Path(clean_relative_path(plan_path))
    return path.with_suffix(".inventory.jsonl").as_posix()


def aggregate_unit_path(source_prefix: str, relative_to_source: str, unit_depth: int) -> str | None:
    if unit_depth < 0:
        raise ValueError("unit depth must be >= 0")
    source = source_prefix.strip("/")
    if unit_depth == 0:
        return source
    parts = [part for part in relative_to_source.strip("/").split("/") if part]
    if len(parts) < unit_depth:
        return None
    return posix_join(source, *parts[:unit_depth])


def mounted_unit_path(source_prefix: str, relative_to_source: str, unit_depth: int, is_dir: bool) -> str | None:
    if unit_depth < 0:
        raise ValueError("unit depth must be >= 0")
    source = source_prefix.strip("/")
    if unit_depth == 0:
        return source
    parts = [part for part in relative_to_source.strip("/").split("/") if part]
    if is_dir and len(parts) >= unit_depth:
        return posix_join(source, *parts[:unit_depth])
    if not is_dir and len(parts) > unit_depth:
        return posix_join(source, *parts[:unit_depth])
    return None


def discover_shard_prefixes_api(volume: modal.Volume, source_prefix: str, shard_depth: int) -> list[str]:
    source = source_prefix.strip("/")
    if shard_depth <= 0:
        return [source]

    frontier = [source]
    for _depth in range(shard_depth):
        next_frontier: list[str] = []
        for current in frontier:
            entries = volume.listdir(current or "/", recursive=False)
            next_frontier.extend(sorted(entry.path.strip("/") for entry in entries if entry_is_dir(entry)))
        frontier = next_frontier
        if not frontier:
            break
    return frontier or [source]


def scan_prefix_to_inventory(
    source_volume_name: str,
    scan_prefix: str,
    source_prefix: str,
    unit_depth: int,
    inventory_path: Path,
) -> dict[str, Any]:
    volume = modal.Volume.from_name(source_volume_name)
    units: dict[str, dict[str, int]] = {}
    entries = 0
    inventory_bytes = 0
    source = source_prefix.strip("/")
    started_at = time.time()

    with inventory_path.open("w") as inventory_handle:
        for entry in volume.listdir(scan_prefix or "/", recursive=True):
            entry_path = entry.path.strip("/")
            rel_to_source = relative_path_under_prefix(entry_path, source)
            unit_path = aggregate_unit_path(source, rel_to_source, unit_depth)
            entry_kind = entry_type_name(entry)
            size = int(getattr(entry, "size", 0) or 0)
            inventory_record = {
                "path": entry_path,
                "relative_path": rel_to_source,
                "type": entry_kind,
                "size": size,
                "mtime": str(getattr(entry, "mtime", "")),
                "unit_path": unit_path,
            }
            encoded = json.dumps(inventory_record, sort_keys=True)
            inventory_handle.write(encoded + "\n")
            inventory_bytes += len(encoded) + 1
            entries += 1

            if unit_path is not None:
                unit = units.setdefault(unit_path, {"files": 0, "directories": 0, "bytes": 0})
                if entry_is_file(entry):
                    unit["files"] += 1
                    unit["bytes"] += size
                elif entry_is_dir(entry) and entry_path != unit_path:
                    unit["directories"] += 1

    return {
        "scan_prefix": scan_prefix,
        "entries": entries,
        "inventory_bytes": inventory_bytes,
        "units": units,
        "elapsed_seconds": round(time.time() - started_at, 3),
    }


def write_sharded_api_plan_to_state(
    source_volume_name: str,
    source_prefix: str,
    unit_depth: int,
    dest_prefix: str,
    plan_path: str,
    max_package_bytes: str,
    warn_package_bytes: str,
    shard_depth: int,
    threads: int,
) -> dict[str, Any]:
    volume = modal.Volume.from_name(source_volume_name)
    state = modal.Volume.from_name(STATE_VOLUME_NAME, create_if_missing=True)
    max_bytes = parse_bytes(max_package_bytes)
    warn_bytes = parse_bytes(warn_package_bytes)
    remote_plan_path = clean_relative_path(plan_path).as_posix()
    remote_summary_path = str(Path(remote_plan_path).with_suffix(".summary.json"))
    remote_inventory_path = inventory_path_for_plan(remote_plan_path)
    started_at = time.time()

    shard_prefixes = discover_shard_prefixes_api(volume, source_prefix, shard_depth)
    worker_count = max(1, min(threads, len(shard_prefixes)))
    print(
        json.dumps(
            {
                "planner": "modal-volume-api-sharded",
                "shards": len(shard_prefixes),
                "threads": worker_count,
                "shard_depth": shard_depth,
            },
            sort_keys=True,
        ),
        flush=True,
    )

    units: dict[str, dict[str, int]] = {}
    total_entries = 0
    inventory_bytes = 0
    completed = 0

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        inventory_dir = tmp_path / "inventory-shards"
        inventory_dir.mkdir()
        local_inventory = tmp_path / "inventory.jsonl"
        local_plan = tmp_path / "plan.jsonl"
        local_summary = tmp_path / "summary.json"

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {
                executor.submit(
                    scan_prefix_to_inventory,
                    source_volume_name,
                    shard_prefix,
                    source_prefix,
                    unit_depth,
                    inventory_dir / f"shard-{index:05d}.jsonl",
                ): shard_prefix
                for index, shard_prefix in enumerate(shard_prefixes)
            }
            for future in as_completed(futures):
                result = future.result()
                completed += 1
                total_entries += int(result["entries"])
                inventory_bytes += int(result["inventory_bytes"])
                for unit_path, unit_counts in result["units"].items():
                    unit = units.setdefault(unit_path, {"files": 0, "directories": 0, "bytes": 0})
                    unit["files"] += int(unit_counts["files"])
                    unit["directories"] += int(unit_counts["directories"])
                    unit["bytes"] += int(unit_counts["bytes"])

                elapsed = max(time.time() - started_at, 0.001)
                print(
                    json.dumps(
                        {
                            "planner": "modal-volume-api-sharded",
                            "completed_shards": completed,
                            "shards": len(shard_prefixes),
                            "latest_prefix": result["scan_prefix"],
                            "entries": total_entries,
                            "units": len(units),
                            "entries_per_second": round(total_entries / elapsed, 2),
                        },
                        sort_keys=True,
                    ),
                    flush=True,
                )

        with local_inventory.open("w") as inventory_handle:
            for shard_file in sorted(inventory_dir.glob("shard-*.jsonl")):
                with shard_file.open() as shard_handle:
                    for line in shard_handle:
                        inventory_handle.write(line)

        total_files = 0
        total_dirs = 0
        total_bytes = 0
        strategy_counts: dict[str, int] = {}
        with local_plan.open("w") as plan_handle:
            for index, unit_path in enumerate(sorted(units)):
                unit = units[unit_path]
                files = int(unit["files"])
                dirs = int(unit["directories"])
                bytes_total = int(unit["bytes"])
                archive_dest, package_index_dest, files_index_dest = target_paths(dest_prefix, unit_path)
                strategy = package_strategy(bytes_total, max_bytes, warn_bytes)
                row = {
                    "index": index,
                    "source_volume": source_volume_name,
                    "source_path": unit_path,
                    "archive_dest": archive_dest,
                    "package_index_dest": package_index_dest,
                    "files_index_dest": files_index_dest,
                    "package_format": "tar.zst",
                    "files": files,
                    "directories": dirs,
                    "bytes": bytes_total,
                    "package_strategy": strategy,
                    "max_package_bytes": max_bytes,
                    "warn_package_bytes": warn_bytes,
                    "planner": "modal-volume-api-sharded",
                    "shard_depth": shard_depth,
                }
                plan_handle.write(json.dumps(row, sort_keys=True) + "\n")
                total_files += files
                total_dirs += dirs
                total_bytes += bytes_total
                strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1

        summary = {
            "source_volume": source_volume_name,
            "source_prefix": source_prefix,
            "unit_depth": unit_depth,
            "dest_prefix": dest_prefix,
            "plan_path": f"/state/{remote_plan_path}",
            "summary_path": f"/state/{remote_summary_path}",
            "inventory_path": f"/state/{remote_inventory_path}",
            "planner": "modal-volume-api-sharded",
            "shard_depth": shard_depth,
            "threads": worker_count,
            "shards": len(shard_prefixes),
            "entries": total_entries,
            "inventory_bytes": inventory_bytes,
            "units": len(units),
            "files": total_files,
            "directories": total_dirs,
            "bytes": total_bytes,
            "max_package_bytes": max_bytes,
            "warn_package_bytes": warn_bytes,
            "strategy_counts": strategy_counts,
            "elapsed_seconds": round(time.time() - started_at, 3),
        }
        local_summary.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")

        with state.batch_upload(force=True) as batch:
            batch.put_file(local_plan, remote_plan_path)
            batch.put_file(local_summary, remote_summary_path)
            batch.put_file(local_inventory, remote_inventory_path)

    return summary


def write_api_plan_to_state(
    source_volume_name: str,
    source_prefix: str,
    unit_depth: int,
    dest_prefix: str,
    plan_path: str,
    limit: int,
    max_package_bytes: str,
    warn_package_bytes: str,
) -> dict[str, Any]:
    if limit > 0:
        return write_api_plan_to_state_limited(
            source_volume_name=source_volume_name,
            source_prefix=source_prefix,
            unit_depth=unit_depth,
            dest_prefix=dest_prefix,
            plan_path=plan_path,
            limit=limit,
            max_package_bytes=max_package_bytes,
            warn_package_bytes=warn_package_bytes,
        )
    if MODAL_DISCOVER_THREADS > 1 and MODAL_DISCOVER_SHARD_DEPTH > 0 and MODAL_DISCOVER_SHARD_DEPTH < unit_depth:
        return write_sharded_api_plan_to_state(
            source_volume_name=source_volume_name,
            source_prefix=source_prefix,
            unit_depth=unit_depth,
            dest_prefix=dest_prefix,
            plan_path=plan_path,
            max_package_bytes=max_package_bytes,
            warn_package_bytes=warn_package_bytes,
            shard_depth=MODAL_DISCOVER_SHARD_DEPTH,
            threads=MODAL_DISCOVER_THREADS,
        )

    volume = modal.Volume.from_name(source_volume_name)
    state = modal.Volume.from_name(STATE_VOLUME_NAME, create_if_missing=True)
    max_bytes = parse_bytes(max_package_bytes)
    warn_bytes = parse_bytes(warn_package_bytes)
    source = source_prefix.strip("/")
    remote_plan_path = clean_relative_path(plan_path).as_posix()
    remote_summary_path = str(Path(remote_plan_path).with_suffix(".summary.json"))
    remote_inventory_path = inventory_path_for_plan(remote_plan_path)

    units: dict[str, dict[str, Any]] = {}
    total_entries = 0
    inventory_bytes = 0
    started_at = time.time()

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        local_inventory = tmp_path / "inventory.jsonl"
        local_plan = tmp_path / "plan.jsonl"
        local_summary = tmp_path / "summary.json"

        with local_inventory.open("w") as inventory_handle:
            for entry in volume.listdir(source or "/", recursive=True):
                entry_path = entry.path.strip("/")
                rel_to_source = relative_path_under_prefix(entry_path, source)
                unit_path = aggregate_unit_path(source, rel_to_source, unit_depth)
                entry_kind = entry_type_name(entry)
                size = int(getattr(entry, "size", 0) or 0)
                inventory_record = {
                    "path": entry_path,
                    "relative_path": rel_to_source,
                    "type": entry_kind,
                    "size": size,
                    "mtime": str(getattr(entry, "mtime", "")),
                    "unit_path": unit_path,
                }
                encoded = json.dumps(inventory_record, sort_keys=True)
                inventory_handle.write(encoded + "\n")
                inventory_bytes += len(encoded) + 1
                total_entries += 1

                if unit_path is not None:
                    unit = units.setdefault(
                        unit_path,
                        {
                            "files": 0,
                            "directories": 0,
                            "bytes": 0,
                        },
                    )
                    if entry_is_file(entry):
                        unit["files"] += 1
                        unit["bytes"] += size
                    elif entry_is_dir(entry) and entry_path != unit_path:
                        unit["directories"] += 1

                if total_entries % 100000 == 0:
                    elapsed = max(time.time() - started_at, 0.001)
                    print(
                        json.dumps(
                            {
                                "planner": "modal-volume-api-single-pass",
                                "entries": total_entries,
                                "units": len(units),
                                "entries_per_second": round(total_entries / elapsed, 2),
                            },
                            sort_keys=True,
                        ),
                        flush=True,
                    )

        total_files = 0
        total_dirs = 0
        total_bytes = 0
        strategy_counts: dict[str, int] = {}
        with local_plan.open("w") as plan_handle:
            for index, unit_path in enumerate(sorted(units)):
                unit = units[unit_path]
                files = int(unit["files"])
                dirs = int(unit["directories"])
                bytes_total = int(unit["bytes"])
                archive_dest, package_index_dest, files_index_dest = target_paths(dest_prefix, unit_path)
                strategy = package_strategy(bytes_total, max_bytes, warn_bytes)
                row = {
                    "index": index,
                    "source_volume": source_volume_name,
                    "source_path": unit_path,
                    "archive_dest": archive_dest,
                    "package_index_dest": package_index_dest,
                    "files_index_dest": files_index_dest,
                    "package_format": "tar.zst",
                    "files": files,
                    "directories": dirs,
                    "bytes": bytes_total,
                    "package_strategy": strategy,
                    "max_package_bytes": max_bytes,
                    "warn_package_bytes": warn_bytes,
                    "planner": "modal-volume-api-single-pass",
                }
                plan_handle.write(json.dumps(row, sort_keys=True) + "\n")
                total_files += files
                total_dirs += dirs
                total_bytes += bytes_total
                strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1

        summary = {
            "source_volume": source_volume_name,
            "source_prefix": source_prefix,
            "unit_depth": unit_depth,
            "dest_prefix": dest_prefix,
            "plan_path": f"/state/{remote_plan_path}",
            "summary_path": f"/state/{remote_summary_path}",
            "inventory_path": f"/state/{remote_inventory_path}",
            "planner": "modal-volume-api-single-pass",
            "entries": total_entries,
            "inventory_bytes": inventory_bytes,
            "units": len(units),
            "files": total_files,
            "directories": total_dirs,
            "bytes": total_bytes,
            "max_package_bytes": max_bytes,
            "warn_package_bytes": warn_bytes,
            "strategy_counts": strategy_counts,
            "elapsed_seconds": round(time.time() - started_at, 3),
        }
        local_summary.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")

        with state.batch_upload(force=True) as batch:
            batch.put_file(local_plan, remote_plan_path)
            batch.put_file(local_summary, remote_summary_path)
            batch.put_file(local_inventory, remote_inventory_path)

    return summary


def unit_name_for(unit_rel: str) -> str:
    return unit_rel.rstrip("/").split("/")[-1]


def target_paths(dest_prefix: str, unit_rel: str) -> tuple[str, str, str]:
    prefix_parts = [part for part in [dest_prefix.strip("/"), unit_rel.strip("/")] if part]
    unit_prefix = "/".join(prefix_parts)
    unit_name = unit_name_for(unit_rel)
    archive = "/".join([unit_prefix, f"{unit_name}.tar.zst"])
    package_index = "/".join([unit_prefix, f"{unit_name}.package.index.json"])
    files_index = "/".join([unit_prefix, f"{unit_name}.files.index.jsonl.zst"])
    return archive, package_index, files_index


def row_source_paths(row: dict[str, Any]) -> list[str]:
    raw_paths = row.get("source_paths")
    if raw_paths is None:
        raw_paths = [row["source_path"]]
    if not isinstance(raw_paths, list) or not raw_paths:
        raise ValueError("package row source_paths must be a non-empty list")

    paths: list[str] = []
    seen: set[str] = set()
    for raw_path in raw_paths:
        if not isinstance(raw_path, str):
            raise ValueError("package row source_paths entries must be strings")
        clean_path = clean_relative_path(raw_path).as_posix()
        if not clean_path or clean_path in seen:
            continue
        seen.add(clean_path)
        paths.append(clean_path)
    if not paths:
        raise ValueError("package row contains no usable source paths")
    return paths


def raw_dest_path(dest_prefix: str, source_rel: str) -> str:
    return "/".join(part for part in [dest_prefix.strip("/"), source_rel.strip("/")] if part)


def scan_api_stream(
    source_volume_name: str,
    source_prefix: str,
    unit_depth: int,
    dest_prefix: str,
    plan_path: str,
    max_package_bytes: str,
    warn_package_bytes: str,
    local_plan: Path,
    local_summary: Path,
    local_inventory: Path,
) -> dict[str, Any]:
    volume = modal.Volume.from_name(source_volume_name)
    max_bytes = parse_bytes(max_package_bytes)
    warn_bytes = parse_bytes(warn_package_bytes)
    source = source_prefix.strip("/")
    remote_plan_path = clean_relative_path(plan_path).as_posix()
    remote_summary_path = str(Path(remote_plan_path).with_suffix(".summary.json"))
    remote_inventory_path = inventory_path_for_plan(remote_plan_path)

    units: dict[str, dict[str, int]] = {}
    raw_files: list[dict[str, Any]] = []
    entries = 0
    files = 0
    dirs = 0
    bytes_total = 0
    started_at = time.time()

    with local_inventory.open("w") as inventory_handle:
        for entry in volume.iterdir(source or "/", recursive=True):
            source_path = entry.path.strip("/")
            rel_to_source = relative_path_under_prefix(source_path, source)
            kind_name = entry_type_name(entry)
            is_file = entry_is_file(entry)
            is_dir = entry_is_dir(entry)
            size = int(getattr(entry, "size", 0) or 0)
            unit_path = mounted_unit_path(source, rel_to_source, unit_depth, is_dir)
            inventory_record = {
                "path": source_path,
                "relative_path": rel_to_source,
                "type": kind_name,
                "size": size if is_file else 0,
                "mtime": str(getattr(entry, "mtime", "")),
                "unit_path": unit_path,
            }
            inventory_handle.write(json.dumps(inventory_record, sort_keys=True) + "\n")

            entries += 1
            if is_file:
                files += 1
                bytes_total += size
            elif is_dir:
                dirs += 1

            if unit_path is None:
                if is_file:
                    raw_files.append(
                        {
                            "source_path": source_path,
                            "dest_path": raw_dest_path(dest_prefix, source_path),
                            "bytes": size,
                            "mtime": str(getattr(entry, "mtime", "")),
                        }
                    )
            else:
                unit = units.setdefault(unit_path, {"files": 0, "directories": 0, "bytes": 0})
                if is_file:
                    unit["files"] += 1
                    unit["bytes"] += size
                elif is_dir and source_path != unit_path:
                    unit["directories"] += 1

            if entries % 100000 == 0:
                elapsed = max(time.time() - started_at, 0.001)
                print(
                    json.dumps(
                        {
                            "planner": "modal-volume-api-stream",
                            "entries": entries,
                            "files": files,
                            "directories": dirs,
                            "units": len(units),
                            "raw_files": len(raw_files),
                            "entries_per_second": round(entries / elapsed, 2),
                        },
                        sort_keys=True,
                    ),
                    flush=True,
                )

    total_package_files = 0
    total_package_dirs = 0
    total_package_bytes = 0
    strategy_counts: dict[str, int] = {}
    rows = 0
    with local_plan.open("w") as plan_handle:
        for raw_index, raw_file in enumerate(sorted(raw_files, key=lambda item: item["source_path"])):
            row = {
                "index": rows,
                "kind": "raw_file",
                "source_volume": source_volume_name,
                "source_path": raw_file["source_path"],
                "dest_path": raw_file["dest_path"],
                "archive_dest": raw_file["dest_path"],
                "package_format": "raw",
                "files": 1,
                "directories": 0,
                "bytes": raw_file["bytes"],
                "planner": "modal-volume-api-stream",
                "raw_index": raw_index,
            }
            plan_handle.write(json.dumps(row, sort_keys=True) + "\n")
            rows += 1

        for unit_path in sorted(units):
            unit = units[unit_path]
            unit_files = int(unit["files"])
            unit_dirs = int(unit["directories"])
            unit_bytes = int(unit["bytes"])
            archive_dest, package_index_dest, files_index_dest = target_paths(dest_prefix, unit_path)
            strategy = package_strategy(unit_bytes, max_bytes, warn_bytes)
            row = {
                "index": rows,
                "kind": "package",
                "source_volume": source_volume_name,
                "source_path": unit_path,
                "archive_dest": archive_dest,
                "package_index_dest": package_index_dest,
                "files_index_dest": files_index_dest,
                "package_format": "tar.zst",
                "files": unit_files,
                "directories": unit_dirs,
                "bytes": unit_bytes,
                "package_strategy": strategy,
                "max_package_bytes": max_bytes,
                "warn_package_bytes": warn_bytes,
                "planner": "modal-volume-api-stream",
            }
            plan_handle.write(json.dumps(row, sort_keys=True) + "\n")
            rows += 1
            total_package_files += unit_files
            total_package_dirs += unit_dirs
            total_package_bytes += unit_bytes
            strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1

    summary = {
        "source_volume": source_volume_name,
        "source_prefix": source_prefix,
        "unit_depth": unit_depth,
        "dest_prefix": dest_prefix,
        "plan_path": f"/state/{remote_plan_path}",
        "summary_path": f"/state/{remote_summary_path}",
        "inventory_path": f"/state/{remote_inventory_path}",
        "planner": "modal-volume-api-stream",
        "entries": entries,
        "files": files,
        "directories": dirs,
        "bytes": bytes_total,
        "package_rows": len(units),
        "raw_file_rows": len(raw_files),
        "plan_rows": rows,
        "package_files": total_package_files,
        "package_directories": total_package_dirs,
        "package_bytes": total_package_bytes,
        "max_package_bytes": max_bytes,
        "warn_package_bytes": warn_bytes,
        "strategy_counts": strategy_counts,
        "elapsed_seconds": round(time.time() - started_at, 3),
    }
    local_summary.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    return summary


def write_api_stream_plan_to_state(
    source_volume_name: str,
    source_prefix: str,
    unit_depth: int,
    dest_prefix: str,
    plan_path: str,
    max_package_bytes: str,
    warn_package_bytes: str,
) -> dict[str, Any]:
    state = modal.Volume.from_name(STATE_VOLUME_NAME, create_if_missing=True)
    remote_plan_path = clean_relative_path(plan_path).as_posix()
    remote_summary_path = str(Path(remote_plan_path).with_suffix(".summary.json"))
    remote_inventory_path = inventory_path_for_plan(remote_plan_path)

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        local_plan = tmp_path / "plan.jsonl"
        local_summary = tmp_path / "summary.json"
        local_inventory = tmp_path / "inventory.jsonl"
        summary = scan_api_stream(
            source_volume_name,
            source_prefix,
            unit_depth,
            dest_prefix,
            plan_path,
            max_package_bytes,
            warn_package_bytes,
            local_plan,
            local_summary,
            local_inventory,
        )
        with state.batch_upload(force=True) as batch:
            batch.put_file(local_plan, remote_plan_path)
            batch.put_file(local_summary, remote_summary_path)
            batch.put_file(local_inventory, remote_inventory_path)
    return summary


def write_structure_plan_to_state(
    source_volume_name: str,
    source_prefix: str,
    unit_depth: int,
    dest_prefix: str,
    plan_path: str,
    max_package_bytes: str,
    warn_package_bytes: str,
) -> dict[str, Any]:
    volume = modal.Volume.from_name(source_volume_name)
    state = modal.Volume.from_name(STATE_VOLUME_NAME, create_if_missing=True)
    max_bytes = parse_bytes(max_package_bytes)
    warn_bytes = parse_bytes(warn_package_bytes)
    source = source_prefix.strip("/")
    remote_plan_path = clean_relative_path(plan_path).as_posix()
    remote_summary_path = str(Path(remote_plan_path).with_suffix(".summary.json"))

    raw_files: list[dict[str, Any]] = []
    package_units: list[str] = []
    listed_dirs = 0
    started_at = time.time()

    frontier: list[tuple[str, int]] = [(source, 0)]
    while frontier:
        current, depth = frontier.pop(0)
        listed_dirs += 1
        for entry in volume.listdir(current or "/", recursive=False):
            entry_path = entry.path.strip("/")
            if entry_is_file(entry):
                raw_files.append(
                    {
                        "source_path": entry_path,
                        "dest_path": raw_dest_path(dest_prefix, entry_path),
                        "bytes": int(getattr(entry, "size", 0) or 0),
                        "mtime": str(getattr(entry, "mtime", "")),
                    }
                )
            elif entry_is_dir(entry):
                next_depth = depth + 1
                if next_depth == unit_depth:
                    package_units.append(entry_path)
                elif next_depth < unit_depth:
                    frontier.append((entry_path, next_depth))

        if listed_dirs % 100 == 0:
            elapsed = max(time.time() - started_at, 0.001)
            print(
                json.dumps(
                    {
                        "planner": "modal-volume-structure",
                        "listed_dirs": listed_dirs,
                        "frontier": len(frontier),
                        "package_units": len(package_units),
                        "raw_files": len(raw_files),
                        "listings_per_second": round(listed_dirs / elapsed, 2),
                    },
                    sort_keys=True,
                ),
                flush=True,
            )

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        local_plan = tmp_path / "plan.jsonl"
        local_summary = tmp_path / "summary.json"

        rows = 0
        raw_bytes = 0
        with local_plan.open("w") as plan_handle:
            for raw_index, raw_file in enumerate(sorted(raw_files, key=lambda item: item["source_path"])):
                row = {
                    "index": rows,
                    "kind": "raw_file",
                    "source_volume": source_volume_name,
                    "source_path": raw_file["source_path"],
                    "dest_path": raw_file["dest_path"],
                    "archive_dest": raw_file["dest_path"],
                    "package_format": "raw",
                    "files": 1,
                    "directories": 0,
                    "bytes": raw_file["bytes"],
                    "planner": "modal-volume-structure",
                    "raw_index": raw_index,
                }
                plan_handle.write(json.dumps(row, sort_keys=True) + "\n")
                rows += 1
                raw_bytes += int(raw_file["bytes"])

            for unit_path in sorted(package_units):
                archive_dest, package_index_dest, files_index_dest = target_paths(dest_prefix, unit_path)
                row = {
                    "index": rows,
                    "kind": "package",
                    "source_volume": source_volume_name,
                    "source_path": unit_path,
                    "archive_dest": archive_dest,
                    "package_index_dest": package_index_dest,
                    "files_index_dest": files_index_dest,
                    "package_format": "tar.zst",
                    "files": None,
                    "directories": None,
                    "bytes": None,
                    "package_strategy": "worker_index_required",
                    "max_package_bytes": max_bytes,
                    "warn_package_bytes": warn_bytes,
                    "planner": "modal-volume-structure",
                }
                plan_handle.write(json.dumps(row, sort_keys=True) + "\n")
                rows += 1

        summary = {
            "source_volume": source_volume_name,
            "source_prefix": source_prefix,
            "unit_depth": unit_depth,
            "dest_prefix": dest_prefix,
            "plan_path": f"/state/{remote_plan_path}",
            "summary_path": f"/state/{remote_summary_path}",
            "planner": "modal-volume-structure",
            "listed_dirs": listed_dirs,
            "package_rows": len(package_units),
            "raw_file_rows": len(raw_files),
            "raw_bytes": raw_bytes,
            "plan_rows": rows,
            "max_package_bytes": max_bytes,
            "warn_package_bytes": warn_bytes,
            "elapsed_seconds": round(time.time() - started_at, 3),
            "note": "Package byte/file counts are computed by upload workers before archiving.",
        }
        local_summary.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")

        with state.batch_upload(force=True) as batch:
            batch.put_file(local_plan, remote_plan_path)
            batch.put_file(local_summary, remote_summary_path)

    return summary


def write_hybrid_structure_plan_to_state(
    source_volume_name: str,
    source_prefix: str,
    top_depth: int,
    fallback_depth: int,
    max_children_per_top_unit: int,
    dest_prefix: str,
    plan_path: str,
    max_package_bytes: str,
    warn_package_bytes: str,
) -> dict[str, Any]:
    if top_depth < 1:
        raise ValueError("top_depth must be >= 1")
    if fallback_depth <= top_depth:
        raise ValueError("fallback_depth must be greater than top_depth")
    volume = modal.Volume.from_name(source_volume_name)
    state = modal.Volume.from_name(STATE_VOLUME_NAME, create_if_missing=True)
    max_bytes = parse_bytes(max_package_bytes)
    warn_bytes = parse_bytes(warn_package_bytes)
    source = source_prefix.strip("/")
    remote_plan_path = clean_relative_path(plan_path).as_posix()
    remote_summary_path = str(Path(remote_plan_path).with_suffix(".summary.json"))
    started_at = time.time()

    raw_files: list[dict[str, Any]] = []
    package_units: list[dict[str, Any]] = []
    top_units = discover_unit_paths_api(volume, source, top_depth, 0)
    listed_dirs = 1

    if not source and top_depth == 1:
        for entry in volume.listdir("/", recursive=False):
            if entry_is_file(entry):
                entry_path = entry.path.strip("/")
                raw_files.append(
                    {
                        "source_path": entry_path,
                        "dest_path": raw_dest_path(dest_prefix, entry_path),
                        "bytes": int(getattr(entry, "size", 0) or 0),
                        "mtime": str(getattr(entry, "mtime", "")),
                    }
                )

    for top_unit in sorted(top_units):
        child_units = discover_unit_paths_api(volume, top_unit, fallback_depth - top_depth, 0)
        listed_dirs += 1
        if len(child_units) <= max_children_per_top_unit:
            package_units.append(
                {
                    "source_path": top_unit,
                    "hybrid_level": "top",
                    "child_package_rows": len(child_units),
                    "package_strategy": "worker_index_required",
                }
            )
        else:
            for child_unit in sorted(child_units):
                package_units.append(
                    {
                        "source_path": child_unit,
                        "hybrid_level": "fallback",
                        "top_source_path": top_unit,
                        "child_package_rows": 1,
                        "package_strategy": "worker_index_required",
                    }
                )

        if listed_dirs % 50 == 0:
            elapsed = max(time.time() - started_at, 0.001)
            print(
                json.dumps(
                    {
                        "planner": "modal-volume-hybrid-structure",
                        "top_units_seen": listed_dirs - 1,
                        "package_units": len(package_units),
                        "top_depth": top_depth,
                        "fallback_depth": fallback_depth,
                        "listings_per_second": round(listed_dirs / elapsed, 2),
                    },
                    sort_keys=True,
                ),
                flush=True,
            )

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        local_plan = tmp_path / "plan.jsonl"
        local_summary = tmp_path / "summary.json"

        rows = 0
        raw_bytes = 0
        top_package_rows = 0
        fallback_package_rows = 0
        with local_plan.open("w") as plan_handle:
            for raw_index, raw_file in enumerate(sorted(raw_files, key=lambda item: item["source_path"])):
                row = {
                    "index": rows,
                    "kind": "raw_file",
                    "source_volume": source_volume_name,
                    "source_path": raw_file["source_path"],
                    "dest_path": raw_file["dest_path"],
                    "archive_dest": raw_file["dest_path"],
                    "package_format": "raw",
                    "files": 1,
                    "directories": 0,
                    "bytes": raw_file["bytes"],
                    "planner": "modal-volume-hybrid-structure",
                    "raw_index": raw_index,
                }
                plan_handle.write(json.dumps(row, sort_keys=True) + "\n")
                rows += 1
                raw_bytes += int(raw_file["bytes"])

            for package_unit in sorted(package_units, key=lambda item: item["source_path"]):
                unit_path = package_unit["source_path"]
                archive_dest, package_index_dest, files_index_dest = target_paths(dest_prefix, unit_path)
                hybrid_level = package_unit["hybrid_level"]
                if hybrid_level == "top":
                    top_package_rows += 1
                else:
                    fallback_package_rows += 1
                row = {
                    "index": rows,
                    "kind": "package",
                    "source_volume": source_volume_name,
                    "source_path": unit_path,
                    "archive_dest": archive_dest,
                    "package_index_dest": package_index_dest,
                    "files_index_dest": files_index_dest,
                    "package_format": "tar.zst",
                    "files": None,
                    "directories": None,
                    "bytes": None,
                    "package_strategy": package_unit["package_strategy"],
                    "max_package_bytes": max_bytes,
                    "warn_package_bytes": warn_bytes,
                    "planner": "modal-volume-hybrid-structure",
                    "hybrid_level": hybrid_level,
                    "top_source_path": package_unit.get("top_source_path", unit_path),
                    "child_package_rows": package_unit["child_package_rows"],
                }
                plan_handle.write(json.dumps(row, sort_keys=True) + "\n")
                rows += 1

        summary = {
            "source_volume": source_volume_name,
            "source_prefix": source_prefix,
            "top_depth": top_depth,
            "fallback_depth": fallback_depth,
            "max_children_per_top_unit": max_children_per_top_unit,
            "dest_prefix": dest_prefix,
            "plan_path": f"/state/{remote_plan_path}",
            "summary_path": f"/state/{remote_summary_path}",
            "planner": "modal-volume-hybrid-structure",
            "listed_dirs": listed_dirs,
            "top_units": len(top_units),
            "top_package_rows": top_package_rows,
            "fallback_package_rows": fallback_package_rows,
            "package_rows": len(package_units),
            "raw_file_rows": len(raw_files),
            "raw_bytes": raw_bytes,
            "plan_rows": rows,
            "estimated_drive_items": (len(package_units) * 3) + len(raw_files),
            "max_package_bytes": max_bytes,
            "warn_package_bytes": warn_bytes,
            "elapsed_seconds": round(time.time() - started_at, 3),
            "note": "Top-level packages are chosen by child count. Workers still compute indexes and skip packages above max_package_bytes.",
        }
        local_summary.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")

        with state.batch_upload(force=True) as batch:
            batch.put_file(local_plan, remote_plan_path)
            batch.put_file(local_summary, remote_summary_path)

    return summary


@app.function(
    image=image,
    timeout=env_int("MODAL_INSPECT_TIMEOUT", 600),
    cpu=float(os.environ.get("MODAL_INSPECT_CPU", "1")),
    memory=env_int("MODAL_INSPECT_MEMORY", 1024),
    max_containers=1,
)
def inspect_volume_metadata(
    source_volume_name: str,
    paths_csv: str,
    limit: int = 20,
    recursive: bool = False,
) -> dict[str, Any]:
    volume = modal.Volume.from_name(source_volume_name)
    paths = parse_inspect_paths(paths_csv)
    effective_limit = limit if limit > 0 else 20
    result: dict[str, Any] = {
        "source_volume": source_volume_name,
        "recursive": recursive,
        "limit_per_path": effective_limit,
        "paths": [],
    }

    for path in paths:
        lookup_path = path or "/"
        path_result: dict[str, Any] = {
            "path": path,
            "lookup_path": lookup_path,
            "entries": [],
        }
        try:
            entries = volume.listdir(lookup_path, recursive=recursive)
            for index, entry in enumerate(entries):
                if index >= effective_limit:
                    path_result["truncated"] = True
                    break
                path_result["entries"].append(serialize_volume_entry(entry))
        except Exception as exc:
            path_result["error"] = str(exc)
        result["paths"].append(path_result)

    return result


@app.function(
    image=image,
    volumes={str(STATE_MOUNT): state_volume},
    timeout=env_int("MODAL_TIMEOUT", 43200),
    cpu=float(os.environ.get("MODAL_CPU", "2")),
    memory=env_int("MODAL_MEMORY", 4096),
    ephemeral_disk=env_int("MODAL_EPHEMERAL_DISK", 524288),
    max_containers=1,
)
def write_hybrid_inventory_plan(
    source_volume_name: str,
    source_prefix: str,
    top_depth: int,
    fallback_depth: int,
    dest_prefix: str,
    plan_path: str,
    inventory_path: str = "",
    max_package_bytes: str = "200GiB",
    warn_package_bytes: str = "180GiB",
) -> dict[str, Any]:
    if top_depth < 1:
        raise ValueError("top_depth must be >= 1")
    if fallback_depth <= top_depth:
        raise ValueError("fallback_depth must be greater than top_depth")

    max_bytes = parse_bytes(max_package_bytes)
    warn_bytes = parse_bytes(warn_package_bytes)
    source = source_prefix.strip("/")
    remote_plan_path = clean_relative_path(plan_path).as_posix()
    remote_summary_path = str(Path(remote_plan_path).with_suffix(".summary.json"))
    remote_inventory_path = clean_relative_path(inventory_path or inventory_path_for_plan(plan_path)).as_posix()
    inventory = STATE_MOUNT / remote_inventory_path
    output = STATE_MOUNT / remote_plan_path
    summary_path = STATE_MOUNT / remote_summary_path
    if not inventory.exists():
        raise FileNotFoundError(f"missing inventory: {inventory}")

    top_units: dict[str, dict[str, int]] = {}
    fallback_units: dict[str, dict[str, Any]] = {}
    raw_files: list[dict[str, Any]] = []
    entries = 0
    files = 0
    directories = 0
    bytes_total = 0
    started_at = time.time()

    with inventory.open() as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            source_path = str(record.get("path", "")).strip("/")
            relative_path = str(record.get("relative_path") or relative_path_under_prefix(source_path, source))
            kind_name = str(record.get("type", "")).upper()
            is_file = kind_name.endswith("FILE") or kind_name == "F"
            is_dir = kind_name.endswith("DIRECTORY") or kind_name == "D"
            size = int(record.get("size", 0) or 0) if is_file else 0
            top_unit = aggregate_unit_path(source, relative_path, top_depth)
            fallback_unit = aggregate_unit_path(source, relative_path, fallback_depth)

            entries += 1
            if is_file:
                files += 1
                bytes_total += size
            elif is_dir:
                directories += 1

            if top_unit is None:
                if is_file:
                    raw_files.append(
                        {
                            "source_path": source_path,
                            "dest_path": raw_dest_path(dest_prefix, source_path),
                            "bytes": size,
                            "mtime": str(record.get("mtime", "")),
                        }
                    )
                continue

            top_stats = top_units.setdefault(top_unit, {"files": 0, "directories": 0, "bytes": 0})
            if is_file:
                top_stats["files"] += 1
                top_stats["bytes"] += size
            elif is_dir and source_path != top_unit:
                top_stats["directories"] += 1

            if fallback_unit is not None:
                fallback_stats = fallback_units.setdefault(
                    fallback_unit,
                    {"files": 0, "directories": 0, "bytes": 0, "top_source_path": top_unit},
                )
                if is_file:
                    fallback_stats["files"] += 1
                    fallback_stats["bytes"] += size
                elif is_dir and source_path != fallback_unit:
                    fallback_stats["directories"] += 1

    output.parent.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    rows = 0
    raw_bytes = 0
    top_package_rows = 0
    fallback_package_rows = 0
    split_required_rows = 0
    strategy_counts: dict[str, int] = {}

    with output.open("w") as plan_handle:
        for raw_index, raw_file in enumerate(sorted(raw_files, key=lambda item: item["source_path"])):
            row = {
                "index": rows,
                "kind": "raw_file",
                "source_volume": source_volume_name,
                "source_path": raw_file["source_path"],
                "dest_path": raw_file["dest_path"],
                "archive_dest": raw_file["dest_path"],
                "package_format": "raw",
                "files": 1,
                "directories": 0,
                "bytes": raw_file["bytes"],
                "planner": "modal-volume-hybrid-inventory",
                "raw_index": raw_index,
            }
            plan_handle.write(json.dumps(row, sort_keys=True) + "\n")
            rows += 1
            raw_bytes += int(raw_file["bytes"])

        for top_unit in sorted(top_units):
            top_stats = top_units[top_unit]
            selected_units: list[tuple[str, dict[str, Any], str]]
            if int(top_stats["bytes"]) <= max_bytes:
                selected_units = [(top_unit, top_stats, "top")]
            else:
                selected_units = [
                    (unit_path, unit_stats, "fallback")
                    for unit_path, unit_stats in sorted(fallback_units.items())
                    if unit_stats.get("top_source_path") == top_unit
                ]

            for unit_path, unit_stats, hybrid_level in selected_units:
                unit_bytes = int(unit_stats["bytes"])
                strategy = package_strategy(unit_bytes, max_bytes, warn_bytes)
                if strategy == "split_required":
                    split_required_rows += 1
                if hybrid_level == "top":
                    top_package_rows += 1
                else:
                    fallback_package_rows += 1
                strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1
                archive_dest, package_index_dest, files_index_dest = target_paths(dest_prefix, unit_path)
                row = {
                    "index": rows,
                    "kind": "package",
                    "source_volume": source_volume_name,
                    "source_path": unit_path,
                    "archive_dest": archive_dest,
                    "package_index_dest": package_index_dest,
                    "files_index_dest": files_index_dest,
                    "package_format": "tar.zst",
                    "files": int(unit_stats["files"]),
                    "directories": int(unit_stats["directories"]),
                    "bytes": unit_bytes,
                    "package_strategy": strategy,
                    "max_package_bytes": max_bytes,
                    "warn_package_bytes": warn_bytes,
                    "planner": "modal-volume-hybrid-inventory",
                    "hybrid_level": hybrid_level,
                    "top_source_path": unit_stats.get("top_source_path", unit_path),
                }
                plan_handle.write(json.dumps(row, sort_keys=True) + "\n")
                rows += 1

    summary = {
        "source_volume": source_volume_name,
        "source_prefix": source_prefix,
        "top_depth": top_depth,
        "fallback_depth": fallback_depth,
        "dest_prefix": dest_prefix,
        "plan_path": f"/state/{remote_plan_path}",
        "summary_path": f"/state/{remote_summary_path}",
        "inventory_path": f"/state/{remote_inventory_path}",
        "planner": "modal-volume-hybrid-inventory",
        "entries": entries,
        "files": files,
        "directories": directories,
        "bytes": bytes_total,
        "top_units": len(top_units),
        "top_package_rows": top_package_rows,
        "fallback_package_rows": fallback_package_rows,
        "split_required_rows": split_required_rows,
        "package_rows": top_package_rows + fallback_package_rows,
        "raw_file_rows": len(raw_files),
        "raw_bytes": raw_bytes,
        "plan_rows": rows,
        "estimated_drive_items": ((top_package_rows + fallback_package_rows) * 3) + len(raw_files),
        "max_package_bytes": max_bytes,
        "warn_package_bytes": warn_bytes,
        "strategy_counts": strategy_counts,
        "elapsed_seconds": round(time.time() - started_at, 3),
        "note": "Top-level packages are selected by real inventory bytes. Oversized top units fall back to lower-level units.",
    }
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    state_volume.commit()
    return summary


@app.function(
    image=image,
    volumes={str(STATE_MOUNT): state_volume},
    timeout=env_int("MODAL_ANALYSIS_TIMEOUT", 43200),
    cpu=float(os.environ.get("MODAL_ANALYSIS_CPU", "4")),
    memory=env_int("MODAL_ANALYSIS_MEMORY", 8192),
    ephemeral_disk=env_int("MODAL_ANALYSIS_EPHEMERAL_DISK", 524288),
    max_containers=1,
)
def analyze_find_json_inventory(
    inventory_path: str,
    source_prefix: str = "",
    top_depth: int = 1,
    fallback_depth: int = 2,
    dest_prefix: str = "",
    max_package_bytes: str = "200GiB",
    shared_drive_item_limit: int = 400000,
) -> dict[str, Any]:
    """Estimate Drive objects from the resumable find-json inventory shards.

    The inventory is deliberately read inside Modal.  Its final JSON array is
    large; the JSONL shards are both safer to stream and already durable.
    """
    if top_depth < 1:
        raise ValueError("top_depth must be >= 1")
    if fallback_depth <= top_depth:
        raise ValueError("fallback_depth must be greater than top_depth")
    if shared_drive_item_limit < 1:
        raise ValueError("shared_drive_item_limit must be >= 1")

    source = source_prefix.strip("/")
    max_bytes = parse_bytes(max_package_bytes)
    inventory_rel = clean_relative_path(inventory_path)
    inventory = STATE_MOUNT / inventory_rel
    shard_dir = inventory.with_suffix("").with_name(inventory.with_suffix("").name + ".shards")
    if not shard_dir.is_dir():
        raise FileNotFoundError(f"missing find-json shard directory: {shard_dir}")

    top_units: dict[str, dict[str, int]] = {}
    fallback_units: dict[str, dict[str, Any]] = {}
    raw_files: list[str] = []
    entries = 0
    files = 0
    directories = 0
    bytes_total = 0

    # Only final shards are considered; durable split batch parts are inputs
    # used to assemble one final shard and would otherwise double-count data.
    shard_paths = sorted(shard_dir.glob("shard-*.jsonl"))
    for shard_path in shard_paths:
        if ".batch-parts" in shard_path.parts:
            continue
        with shard_path.open() as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                source_path = str(record.get("Path", "")).strip("/")
                if not source_path:
                    continue
                is_dir = bool(record.get("IsDir", False))
                size = 0 if is_dir else int(record.get("Size", 0) or 0)
                relative_path = relative_path_under_prefix(source_path, source)
                top_unit = aggregate_unit_path(source, relative_path, top_depth)
                fallback_unit = aggregate_unit_path(source, relative_path, fallback_depth)

                entries += 1
                if is_dir:
                    directories += 1
                else:
                    files += 1
                    bytes_total += size

                if top_unit is None:
                    if not is_dir:
                        raw_files.append(raw_dest_path(dest_prefix, source_path))
                    continue

                top_stats = top_units.setdefault(top_unit, {"files": 0, "directories": 0, "bytes": 0})
                if is_dir:
                    if source_path != top_unit:
                        top_stats["directories"] += 1
                else:
                    top_stats["files"] += 1
                    top_stats["bytes"] += size

                if fallback_unit is not None:
                    fallback_stats = fallback_units.setdefault(
                        fallback_unit,
                        {"files": 0, "directories": 0, "bytes": 0, "top_source_path": top_unit},
                    )
                    if is_dir:
                        if source_path != fallback_unit:
                            fallback_stats["directories"] += 1
                    else:
                        fallback_stats["files"] += 1
                        fallback_stats["bytes"] += size

    selected_units: list[str] = []
    top_package_rows = 0
    fallback_package_rows = 0
    split_required_rows = 0
    oversized_top_units = 0
    oversized_fallback_units = 0
    for top_unit in sorted(top_units):
        top_stats = top_units[top_unit]
        if int(top_stats["bytes"]) <= max_bytes:
            selected_units.append(top_unit)
            top_package_rows += 1
            continue

        oversized_top_units += 1
        children = [
            (unit_path, unit_stats)
            for unit_path, unit_stats in fallback_units.items()
            if unit_stats.get("top_source_path") == top_unit
        ]
        if not children:
            selected_units.append(top_unit)
            split_required_rows += 1
            continue
        for unit_path, unit_stats in sorted(children):
            selected_units.append(unit_path)
            fallback_package_rows += 1
            if int(unit_stats["bytes"]) > max_bytes:
                oversized_fallback_units += 1
                split_required_rows += 1

    # Existing target_paths stores the three package files under the unit path.
    # Count every unique parent folder rclone would need to create as a Drive item.
    destination_folders: set[str] = set()
    for unit_path in selected_units:
        destination = posix_join(dest_prefix.strip("/"), unit_path)
        parts = [part for part in destination.split("/") if part]
        for depth in range(1, len(parts) + 1):
            destination_folders.add("/".join(parts[:depth]))
    for raw_file_dest in raw_files:
        parent = str(Path(raw_file_dest).parent).replace("\\", "/").strip(".")
        if parent:
            parts = [part for part in parent.split("/") if part]
            for depth in range(1, len(parts) + 1):
                destination_folders.add("/".join(parts[:depth]))

    package_rows = len(selected_units)
    existing_layout_items = (package_rows * 3) + len(raw_files) + len(destination_folders)
    flat_layout_items = (package_rows * 3) + len(raw_files) + (1 if package_rows and dest_prefix else 0)
    theoretical_min_packages = (bytes_total + max_bytes - 1) // max_bytes
    theoretical_min_items = (theoretical_min_packages * 3) + len(raw_files) + (1 if theoretical_min_packages and dest_prefix else 0)
    summary = {
        "planner": "modal-volume-find-json-drive-item-analysis",
        "inventory_path": str(inventory),
        "shard_dir": str(shard_dir),
        "source_prefix": source_prefix,
        "top_depth": top_depth,
        "fallback_depth": fallback_depth,
        "dest_prefix": dest_prefix,
        "entries": entries,
        "files": files,
        "directories": directories,
        "bytes": bytes_total,
        "max_package_bytes": max_bytes,
        "top_units": len(top_units),
        "oversized_top_units": oversized_top_units,
        "top_package_rows": top_package_rows,
        "fallback_package_rows": fallback_package_rows,
        "package_rows": package_rows,
        "oversized_fallback_units": oversized_fallback_units,
        "split_required_rows": split_required_rows,
        "raw_file_rows": len(raw_files),
        "existing_layout_folders": len(destination_folders),
        "existing_layout_estimated_drive_items": existing_layout_items,
        "flat_layout_estimated_drive_items": flat_layout_items,
        "theoretical_min_packages_by_bytes": theoretical_min_packages,
        "theoretical_min_drive_items_by_bytes": theoretical_min_items,
        "shared_drive_item_limit": shared_drive_item_limit,
        "existing_layout_fits_limit": existing_layout_items <= shared_drive_item_limit,
        "flat_layout_fits_limit": flat_layout_items <= shared_drive_item_limit,
        "note": "The theoretical minimum assumes archives may bundle multiple source units. Oversized fallback rows require deeper splitting before they can be packaged safely.",
    }
    analysis_path = inventory.with_suffix(".drive-item-analysis.json")
    analysis_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    state_volume.commit()
    summary["analysis_path"] = str(analysis_path)
    return summary


@app.function(
    image=image,
    volumes={str(STATE_MOUNT): state_volume},
    timeout=env_int("MODAL_SANE_PLAN_TIMEOUT", 43200),
    cpu=float(os.environ.get("MODAL_SANE_PLAN_CPU", "4")),
    memory=env_int("MODAL_SANE_PLAN_MEMORY", 16384),
    max_containers=1,
)
def write_sane_archive_plan(
    inventory_path: str,
    source_volume_name: str,
    source_prefix: str = "",
    dest_prefix: str = "",
    completed_manifest_path: str = "",
    plan_path: str = "plans/sane-archives.jsonl",
    max_package_bytes: str = "200GiB",
    max_archive_entries: int = 100000,
    max_roots_per_archive: int = 1000,
    shared_drive_item_limit: int = 400000,
) -> dict[str, Any]:
    """Build independently extractable, complete-folder archive batches."""
    if max_archive_entries < 1:
        raise ValueError("max_archive_entries must be >= 1")
    if max_roots_per_archive < 1:
        raise ValueError("max_roots_per_archive must be >= 1")
    if shared_drive_item_limit < 1:
        raise ValueError("shared_drive_item_limit must be >= 1")

    max_bytes = parse_bytes(max_package_bytes)
    source = source_prefix.strip("/")
    inventory = STATE_MOUNT / clean_relative_path(inventory_path)
    shard_dir = inventory.with_suffix("").with_name(inventory.with_suffix("").name + ".shards")
    if not shard_dir.is_dir():
        raise FileNotFoundError(f"missing find-json shard directory: {shard_dir}")

    shard_paths = sorted(shard_dir.glob("shard-*.jsonl"))
    if not shard_paths:
        raise FileNotFoundError(f"find-json shard directory is empty: {shard_dir}")

    completed_paths: set[str] = set()
    if completed_manifest_path:
        manifest = STATE_MOUNT / clean_relative_path(completed_manifest_path)
        if not manifest.is_file():
            raise FileNotFoundError(f"missing completed package manifest: {manifest}")
        payload = json.loads(manifest.read_text())
        raw_paths = payload.get("completed_source_paths", [])
        if not isinstance(raw_paths, list):
            raise ValueError("completed package manifest has invalid completed_source_paths")
        completed_paths = {
            clean_relative_path(str(path)).as_posix() for path in raw_paths if str(path).strip()
        }

    def is_completed_path(path: str) -> bool:
        parts = [part for part in path.strip("/").split("/") if part]
        return any("/".join(parts[:depth]) in completed_paths for depth in range(1, len(parts) + 1))

    directory_paths: set[str] = set()
    directory_stats: dict[str, dict[str, int]] = {}
    entries = 0
    source_files = 0
    source_directories = 0
    source_bytes = 0
    started_at = time.time()

    def ensure_directory(path: str) -> dict[str, int]:
        directory_paths.add(path)
        return directory_stats.setdefault(path, {"files": 0, "directories": 0, "bytes": 0})

    # First pass calculates recursive directory totals. The final JSON array is
    # intentionally ignored; durable JSONL shards are streamed in-place.
    for shard_path in shard_paths:
        with shard_path.open() as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                source_path = str(record.get("Path", "")).strip("/")
                if not source_path or is_completed_path(source_path):
                    continue
                relative = relative_path_under_prefix(source_path, source)
                parts = [part for part in relative.split("/") if part]
                if not parts:
                    continue
                is_dir = bool(record.get("IsDir", False))
                size = 0 if is_dir else int(record.get("Size", 0) or 0)
                parent_depth = len(parts) if is_dir else len(parts) - 1
                ancestors = [posix_join(source, *parts[:depth]) for depth in range(1, parent_depth + 1)]

                entries += 1
                if is_dir:
                    source_directories += 1
                    for ancestor in ancestors:
                        ensure_directory(ancestor)
                    for ancestor in ancestors[:-1]:
                        directory_stats[ancestor]["directories"] += 1
                else:
                    source_files += 1
                    source_bytes += size
                    for ancestor in ancestors:
                        stats = ensure_directory(ancestor)
                        stats["files"] += 1
                        stats["bytes"] += size

    children: dict[str, list[str]] = {}
    for directory in directory_paths:
        relative = relative_path_under_prefix(directory, source)
        parts = [part for part in relative.split("/") if part]
        parent = posix_join(source, *parts[:-1]) if parts else source
        children.setdefault(parent, []).append(directory)
    for child_paths in children.values():
        child_paths.sort()

    split_directories = {
        path
        for path, stats in directory_stats.items()
        if int(stats["bytes"]) > max_bytes
        or int(stats["files"]) + int(stats["directories"]) + 1 > max_archive_entries
    }
    direct_files: dict[str, list[dict[str, Any]]] = {}
    collect_file_parents = split_directories | {source}

    # The second pass retains exact file names only for directories that must be
    # split. Files under a complete package root never need individual planning.
    for shard_path in shard_paths:
        with shard_path.open() as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                if bool(record.get("IsDir", False)):
                    continue
                source_path = str(record.get("Path", "")).strip("/")
                if is_completed_path(source_path):
                    continue
                relative = relative_path_under_prefix(source_path, source)
                parts = [part for part in relative.split("/") if part]
                if not parts:
                    continue
                parent = posix_join(source, *parts[:-1]) if len(parts) > 1 else source
                if parent not in collect_file_parents:
                    continue
                direct_files.setdefault(parent, []).append(
                    {
                        "source_path": source_path,
                        "bytes": int(record.get("Size", 0) or 0),
                        "files": 1,
                        "directories": 0,
                        "entries": 1,
                    }
                )
    for file_members in direct_files.values():
        file_members.sort(key=lambda item: str(item["source_path"]))

    plan_rows: list[dict[str, Any]] = []
    oversized_files: list[dict[str, Any]] = []
    oversized_file_count = 0
    batch_numbers: dict[str, int] = {}

    def directory_member(path: str) -> dict[str, Any]:
        stats = directory_stats[path]
        return {
            "source_path": path,
            "bytes": int(stats["bytes"]),
            "files": int(stats["files"]),
            "directories": int(stats["directories"]) + 1,
            "entries": int(stats["files"]) + int(stats["directories"]) + 1,
        }

    def append_package(top_unit: str, members: list[dict[str, Any]], whole_top: bool) -> None:
        source_paths = [str(member["source_path"]) for member in members]
        package_bytes = sum(int(member["bytes"]) for member in members)
        package_files = sum(int(member["files"]) for member in members)
        package_directories = sum(int(member["directories"]) for member in members)
        if whole_top:
            archive_dest, package_index_dest, files_index_dest = target_paths(dest_prefix, top_unit)
            package_id = unit_name_for(top_unit)
            batch_number = None
        else:
            batch_number = batch_numbers.get(top_unit, 0) + 1
            batch_numbers[top_unit] = batch_number
            archive_dest, package_index_dest, files_index_dest = batch_target_paths(
                dest_prefix, top_unit, batch_number
            )
            package_id = Path(archive_dest).name.removesuffix(".tar.zst")
        plan_rows.append(
            {
                "kind": "package",
                "source_volume": source_volume_name,
                "source_path": source_paths[0],
                "source_paths": source_paths,
                "source_root_count": len(source_paths),
                "top_source_path": top_unit,
                "package_id": package_id,
                "batch_number": batch_number,
                "archive_dest": archive_dest,
                "package_index_dest": package_index_dest,
                "files_index_dest": files_index_dest,
                "package_format": "tar.zst",
                "archive_layout": "whole_top" if whole_top else "complete_roots_batch",
                "files": package_files,
                "directories": package_directories,
                "bytes": package_bytes,
                "entries": package_files + package_directories,
                "package_strategy": "single_tar_zst",
                "max_package_bytes": max_bytes,
                "max_archive_entries": max_archive_entries,
                "max_roots_per_archive": max_roots_per_archive,
                "planner": "modal-volume-sane-archive-plan",
            }
        )

    def append_member_batches(top_unit: str, members: list[dict[str, Any]]) -> None:
        if not members:
            return
        for batch in pack_contiguous_archive_members(
            members,
            max_bytes=max_bytes,
            max_entries=max_archive_entries,
            max_roots=max_roots_per_archive,
        ):
            append_package(top_unit, batch, whole_top=False)

    def plan_split_directory(directory: str, top_unit: str) -> None:
        nonlocal oversized_file_count
        pending_members: list[dict[str, Any]] = []

        def flush_pending() -> None:
            nonlocal pending_members
            append_member_batches(top_unit, pending_members)
            pending_members = []

        child_members: list[tuple[str, str, dict[str, Any] | None]] = []
        for child in children.get(directory, []):
            child_members.append((child, "directory", None))
        for file_member in direct_files.get(directory, []):
            child_members.append((str(file_member["source_path"]), "file", file_member))

        for _path, member_type, file_member in sorted(child_members, key=lambda item: item[0]):
            if member_type == "directory":
                child = _path
                if child in split_directories:
                    flush_pending()
                    plan_split_directory(child, top_unit)
                else:
                    pending_members.append(directory_member(child))
                continue

            assert file_member is not None
            if int(file_member["bytes"]) > max_bytes:
                flush_pending()
                oversized_file_count += 1
                if len(oversized_files) < 100:
                    oversized_files.append(file_member)
            else:
                pending_members.append(file_member)
        flush_pending()

    top_units = children.get(source, [])
    for top_unit in top_units:
        if top_unit in split_directories:
            plan_split_directory(top_unit, top_unit)
        else:
            append_package(top_unit, [directory_member(top_unit)], whole_top=True)

    root_files = direct_files.get(source, [])
    normal_root_files = []
    for file_member in root_files:
        if int(file_member["bytes"]) > max_bytes:
            oversized_file_count += 1
            if len(oversized_files) < 100:
                oversized_files.append(file_member)
        else:
            normal_root_files.append(file_member)
    if normal_root_files:
        root_package_path = posix_join(source, "_root-files") if source else "_root-files"
        append_member_batches(root_package_path, normal_root_files)

    for index, row in enumerate(plan_rows):
        row["index"] = index

    output = STATE_MOUNT / clean_relative_path(plan_path)
    summary_path = output.with_suffix(".summary.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w") as handle:
        for row in plan_rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")

    drive_folders: set[str] = set()
    for row in plan_rows:
        parent = str(Path(str(row["archive_dest"])).parent).replace("\\", "/").strip("./")
        parts = [part for part in parent.split("/") if part]
        for depth in range(1, len(parts) + 1):
            drive_folders.add("/".join(parts[:depth]))
    estimated_drive_items = (len(plan_rows) * 3) + len(drive_folders)
    packaged_bytes = sum(int(row["bytes"]) for row in plan_rows)
    packaged_files = sum(int(row["files"]) for row in plan_rows)
    summary = {
        "planner": "modal-volume-sane-archive-plan",
        "source_volume": source_volume_name,
        "source_prefix": source_prefix,
        "dest_prefix": dest_prefix,
        "completed_manifest_path": completed_manifest_path,
        "completed_source_paths": len(completed_paths),
        "inventory_path": str(inventory),
        "shard_dir": str(shard_dir),
        "plan_path": str(output),
        "summary_path": str(summary_path),
        "inventory_entries": entries,
        "source_files": source_files,
        "source_directories": source_directories,
        "source_bytes": source_bytes,
        "top_units": len(top_units),
        "split_directories": len(split_directories),
        "packages": len(plan_rows),
        "whole_top_packages": sum(row["archive_layout"] == "whole_top" for row in plan_rows),
        "batched_packages": sum(row["archive_layout"] == "complete_roots_batch" for row in plan_rows),
        "packaged_files": packaged_files,
        "packaged_bytes": packaged_bytes,
        "unpackaged_oversized_file_count": oversized_file_count,
        "unpackaged_oversized_file_examples": oversized_files,
        "max_package_bytes": max_bytes,
        "max_archive_entries": max_archive_entries,
        "max_roots_per_archive": max_roots_per_archive,
        "drive_folder_items": len(drive_folders),
        "estimated_drive_items": estimated_drive_items,
        "shared_drive_item_limit": shared_drive_item_limit,
        "fits_shared_drive_item_limit": estimated_drive_items <= shared_drive_item_limit,
        "plan_complete": (
            oversized_file_count == 0
            and packaged_bytes == source_bytes
            and packaged_files == source_files
        ),
        "elapsed_seconds": round(time.time() - started_at, 3),
        "extraction_command": "tar --zstd -xf PACKAGE.tar.zst -C RESTORE_ROOT",
        "note": "Every package is independently extractable and contains complete source roots with original paths relative to the Modal volume root.",
    }
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    state_volume.commit()
    return summary


def iter_find_records(find_root: Path) -> Iterator[tuple[str, int, str, str]]:
    command = ["find", str(find_root), "-mindepth", "1", "-printf", "%y\\0%s\\0%T@\\0%P\\0"]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert process.stdout is not None

    buffer = b""
    fields: list[bytes] = []
    while True:
        # Keep reads below a typical pipe buffer so `find` cannot block while
        # waiting for a large buffered read to be satisfied.
        chunk = process.stdout.read(64 * 1024)
        if not chunk:
            break
        buffer += chunk
        parts = buffer.split(b"\0")
        buffer = parts.pop()
        for part in parts:
            fields.append(part)
            if len(fields) == 4:
                kind = fields[0].decode("utf-8", errors="surrogateescape")
                size_text = fields[1].decode("ascii", errors="ignore") or "0"
                mtime = fields[2].decode("ascii", errors="ignore")
                rel_path = fields[3].decode("utf-8", errors="surrogateescape")
                fields = []
                yield kind, int(size_text), mtime, rel_path

    stderr = process.stderr.read().decode("utf-8", errors="replace") if process.stderr else ""
    return_code = process.wait()
    if return_code != 0:
        raise RuntimeError(f"find failed with exit {return_code}: {stderr.strip()}")
    if buffer or fields:
        raise RuntimeError("find output ended with a partial record")


def iter_find_records_many(find_roots: list[Path]) -> Iterator[tuple[str, int, str, str]]:
    """Yield records for several roots, keeping absolute paths in the output."""
    if not find_roots:
        return

    command = [
        "find",
        *(str(root) for root in find_roots),
        "-mindepth",
        "1",
        "-printf",
        "%y\\0%s\\0%T@\\0%p\\0",
    ]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert process.stdout is not None

    buffer = b""
    fields: list[bytes] = []
    while True:
        # Keep reads below a typical pipe buffer so `find` cannot block while
        # waiting for a large buffered read to be satisfied.
        chunk = process.stdout.read(64 * 1024)
        if not chunk:
            break
        buffer += chunk
        parts = buffer.split(b"\0")
        buffer = parts.pop()
        for part in parts:
            fields.append(part)
            if len(fields) == 4:
                kind = fields[0].decode("utf-8", errors="surrogateescape")
                size_text = fields[1].decode("ascii", errors="ignore") or "0"
                mtime = fields[2].decode("ascii", errors="ignore")
                absolute_path = fields[3].decode("utf-8", errors="surrogateescape")
                fields = []
                yield kind, int(size_text), mtime, absolute_path

    stderr = process.stderr.read().decode("utf-8", errors="replace") if process.stderr else ""
    return_code = process.wait()
    if return_code != 0:
        raise RuntimeError(f"find failed with exit {return_code}: {stderr.strip()}")
    if buffer or fields:
        raise RuntimeError("find output ended with a partial record")


def find_kind_name(kind: str) -> str:
    return {
        "d": "DIRECTORY",
        "f": "FILE",
        "l": "SYMLINK",
    }.get(kind, "OTHER")


def mounted_scan_roots(find_root: Path, source: str) -> tuple[list[Path], list[Path]]:
    if source:
        return [find_root], []
    scan_dirs: list[Path] = []
    shallow_files: list[Path] = []
    for entry in sorted(find_root.iterdir(), key=lambda path: path.name):
        if entry.is_dir():
            scan_dirs.append(entry)
        elif entry.is_file():
            shallow_files.append(entry)
    return scan_dirs, shallow_files


def scan_mounted_dir_to_inventory(
    scan_dir: Path,
    scan_source: str,
    source: str,
    unit_depth: int,
    dest_prefix: str,
    inventory_path: Path,
) -> dict[str, Any]:
    units: dict[str, dict[str, int]] = {}
    raw_files: list[dict[str, Any]] = []
    entries = 0
    files = 0
    dirs = 0
    bytes_total = 0
    started_at = time.time()

    with inventory_path.open("w") as inventory_handle:
        for kind, size, mtime, rel_under_scan in iter_find_records(scan_dir):
            source_path = posix_join(scan_source, rel_under_scan)
            rel_to_source = relative_path_under_prefix(source_path, source)
            kind_name = find_kind_name(kind)
            is_file = kind == "f"
            is_dir = kind == "d"
            unit_path = mounted_unit_path(source, rel_to_source, unit_depth, is_dir)

            inventory_record = {
                "path": source_path,
                "relative_path": rel_to_source,
                "type": kind_name,
                "size": size if is_file else 0,
                "mtime": mtime,
                "unit_path": unit_path,
            }
            inventory_handle.write(json.dumps(inventory_record, sort_keys=True) + "\n")

            entries += 1
            if is_file:
                files += 1
                bytes_total += size
            elif is_dir:
                dirs += 1

            if unit_path is None:
                if is_file:
                    raw_files.append(
                        {
                            "source_path": source_path,
                            "dest_path": raw_dest_path(dest_prefix, source_path),
                            "bytes": size,
                            "mtime": mtime,
                        }
                    )
            else:
                unit = units.setdefault(unit_path, {"files": 0, "directories": 0, "bytes": 0})
                if is_file:
                    unit["files"] += 1
                    unit["bytes"] += size
                elif is_dir and source_path != unit_path:
                    unit["directories"] += 1

    return {
        "scan_prefix": scan_source,
        "entries": entries,
        "files": files,
        "directories": dirs,
        "bytes": bytes_total,
        "units": units,
        "raw_files": raw_files,
        "elapsed_seconds": round(time.time() - started_at, 3),
    }


def aggregate_inventory_file(inventory_path: Path, dest_prefix: str) -> dict[str, Any]:
    units: dict[str, dict[str, int]] = {}
    raw_files: list[dict[str, Any]] = []
    entries = 0
    files = 0
    dirs = 0
    bytes_total = 0

    with inventory_path.open() as inventory_handle:
        for line in inventory_handle:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            entries += 1
            kind = str(record.get("type", "")).upper()
            is_file = kind == "FILE"
            is_dir = kind == "DIRECTORY"
            size = int(record.get("size", 0) or 0)
            unit_path = record.get("unit_path")
            source_path = str(record.get("path", ""))

            if is_file:
                files += 1
                bytes_total += size
            elif is_dir:
                dirs += 1

            if unit_path is None:
                if is_file:
                    raw_files.append(
                        {
                            "source_path": source_path,
                            "dest_path": raw_dest_path(dest_prefix, source_path),
                            "bytes": size,
                            "mtime": str(record.get("mtime", "")),
                        }
                    )
                continue

            unit = units.setdefault(str(unit_path), {"files": 0, "directories": 0, "bytes": 0})
            if is_file:
                unit["files"] += 1
                unit["bytes"] += size
            elif is_dir and source_path != unit_path:
                unit["directories"] += 1

    return {
        "entries": entries,
        "files": files,
        "directories": dirs,
        "bytes": bytes_total,
        "units": units,
        "raw_files": raw_files,
    }


def scan_find_json_shard(
    scan_dir: Path,
    scan_source: str,
    source: str,
    shard_path: Path,
    include_scan_root: bool = False,
) -> dict[str, Any]:
    entries = 0
    files = 0
    dirs = 0
    bytes_total = 0
    started_at = time.time()

    with shard_path.open("w") as handle:
        if include_scan_root:
            handle.write(
                json.dumps(
                    {
                        "Path": scan_source,
                        "Name": Path(scan_source).name,
                        "Size": 0,
                        "IsDir": True,
                    },
                    separators=(",", ":"),
                    ensure_ascii=False,
                )
                + "\n"
            )
            entries += 1
            dirs += 1
        for kind, size, _mtime, rel_under_scan in iter_find_records(scan_dir):
            source_path = posix_join(scan_source, rel_under_scan)
            is_file = kind == "f"
            is_dir = kind == "d"
            record = {
                "Path": source_path,
                "Name": Path(source_path).name,
                "Size": size if is_file else 0,
                "IsDir": is_dir,
            }
            handle.write(json.dumps(record, separators=(",", ":"), ensure_ascii=False) + "\n")
            entries += 1
            if is_file:
                files += 1
                bytes_total += size
            elif is_dir:
                dirs += 1

    return {
        "scan_prefix": scan_source,
        "entries": entries,
        "files": files,
        "directories": dirs,
        "bytes": bytes_total,
        "elapsed_seconds": round(time.time() - started_at, 3),
    }


def scan_find_json_batch(scan_dirs: list[Path], shard_path: Path) -> dict[str, Any]:
    """Scan a bounded group of sibling directories into one durable part."""
    entries = 0
    files = 0
    dirs = 0
    bytes_total = 0
    started_at = time.time()

    with shard_path.open("w") as handle:
        for scan_dir in scan_dirs:
            source_path = scan_dir.relative_to(SOURCE_MOUNT).as_posix()
            handle.write(
                json.dumps(
                    {
                        "Path": source_path,
                        "Name": scan_dir.name,
                        "Size": 0,
                        "IsDir": True,
                    },
                    separators=(",", ":"),
                    ensure_ascii=False,
                )
                + "\n"
            )
            entries += 1
            dirs += 1

        for kind, size, _mtime, absolute_path in iter_find_records_many(scan_dirs):
            source_path = Path(absolute_path).relative_to(SOURCE_MOUNT).as_posix()
            is_file = kind == "f"
            is_dir = kind == "d"
            record = {
                "Path": source_path,
                "Name": Path(source_path).name,
                "Size": size if is_file else 0,
                "IsDir": is_dir,
            }
            handle.write(json.dumps(record, separators=(",", ":"), ensure_ascii=False) + "\n")
            entries += 1
            if is_file:
                files += 1
                bytes_total += size
            elif is_dir:
                dirs += 1

    return {
        "entries": entries,
        "files": files,
        "directories": dirs,
        "bytes": bytes_total,
        "elapsed_seconds": round(time.time() - started_at, 3),
    }


@app.function(
    image=image,
    volumes={str(SOURCE_MOUNT): source_volume, str(STATE_MOUNT): state_volume},
    timeout=env_int("MODAL_TIMEOUT", 43200),
    cpu=float(os.environ.get("MODAL_FIND_JSON_CPU", os.environ.get("MODAL_CPU", "8"))),
    memory=env_int("MODAL_FIND_JSON_MEMORY", env_int("MODAL_MEMORY", 8192)),
    ephemeral_disk=env_int("MODAL_FIND_JSON_EPHEMERAL_DISK", 524288),
    retries=5,
    max_containers=1,
)
def write_find_json_split_prefix(
    source_prefix: str,
    output_path: str,
    shard_index: int,
    threads: int = 16,
) -> dict[str, Any]:
    """Build one inventory shard from resumable immediate-child subshards.

    This is for an unusually large top-level prefix that cannot reliably finish
    in a single worker lifetime. Child subshards are copied to the state Volume
    only after their local scan completes, so retries never consume partial data.
    """
    source_rel = clean_relative_path(source_prefix)
    source = source_rel.as_posix()
    if not source:
        raise ValueError("split inventory requires a non-root source prefix")
    if shard_index < 0:
        raise ValueError("shard_index must be non-negative")

    find_root = SOURCE_MOUNT / source_rel
    ensure_inside(find_root, SOURCE_MOUNT)
    if not find_root.exists() or not find_root.is_dir():
        raise FileNotFoundError(f"source prefix does not exist or is not a directory: {find_root}")

    output = STATE_MOUNT / clean_relative_path(output_path)
    shard_dir = output.with_suffix("").with_name(output.with_suffix("").name + ".shards")
    final_shard = shard_dir / f"shard-{shard_index:05d}.jsonl"
    parts_dir = shard_dir / f"shard-{shard_index:05d}.batch-parts"
    output.parent.mkdir(parents=True, exist_ok=True)
    shard_dir.mkdir(parents=True, exist_ok=True)
    parts_dir.mkdir(parents=True, exist_ok=True)
    completed_part_names = {
        entry.name
        for entry in parts_dir.iterdir()
        if entry.is_file() and entry.stat().st_size > 0
    }

    if final_shard.exists() and final_shard.stat().st_size > 0:
        return {"status": "already_complete", "source_prefix": source, "shard": str(final_shard)}

    scan_dirs, shallow_files = mounted_scan_roots(find_root, "")
    children_per_part = max(1, env_int("MODAL_FIND_SPLIT_CHILDREN_PER_PART", 512))
    scan_batches = [
        scan_dirs[index : index + children_per_part]
        for index in range(0, len(scan_dirs), children_per_part)
    ]
    root_files_part = parts_dir / "part-root-files.jsonl"
    with root_files_part.open("w") as handle:
        for shallow_file in shallow_files:
            stat = shallow_file.stat()
            source_path = posix_join(source, shallow_file.name)
            handle.write(
                json.dumps(
                    {
                        "Path": source_path,
                        "Name": shallow_file.name,
                        "Size": stat.st_size,
                        "IsDir": False,
                    },
                    separators=(",", ":"),
                    ensure_ascii=False,
                )
                + "\n"
            )
    state_volume.commit()

    started_at = time.time()
    resumed = 0
    completed = 0
    commit_every = max(1, env_int("MODAL_FIND_SPLIT_COMMIT_EVERY", 64))
    uncommitted_parts = 0
    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        worker_count = max(1, min(threads, len(scan_batches)))
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {}
            for batch_index, scan_batch in enumerate(scan_batches):
                state_part = parts_dir / f"part-{batch_index:05d}.jsonl"
                if state_part.name in completed_part_names:
                    resumed += len(scan_batch)
                    continue
                local_part = tmp_dir / state_part.name
                futures[
                    executor.submit(
                        scan_find_json_batch,
                        scan_batch,
                        local_part,
                    )
                ] = (scan_batch, local_part, state_part)

            print(
                json.dumps(
                    {
                        "planner": "modal-volume-find-json-split-prefix",
                        "source_prefix": source,
                        "shard_index": shard_index,
                        "child_dirs": len(scan_dirs),
                        "children_per_part": children_per_part,
                        "parts": len(scan_batches),
                        "resumed_children": resumed,
                        "missing_parts": len(futures),
                    },
                    sort_keys=True,
                ),
                flush=True,
            )

            for future in as_completed(futures):
                result = future.result()
                scan_batch, local_part, state_part = futures[future]
                shutil.copyfile(local_part, state_part)
                completed += len(scan_batch)
                uncommitted_parts += 1
                if uncommitted_parts >= commit_every:
                    state_volume.commit()
                    uncommitted_parts = 0
                print(
                    json.dumps(
                        {
                            "planner": "modal-volume-find-json-split-prefix",
                            "source_prefix": source,
                            "shard_index": shard_index,
                            "completed_children": completed,
                            "resumed_children": resumed,
                            "part": state_part.name,
                            "part_children": len(scan_batch),
                            "prefix_entries": result["entries"],
                            "status": "part_finished",
                        },
                        sort_keys=True,
                    ),
                    flush=True,
                )

        if uncommitted_parts:
            state_volume.commit()

        local_final = tmp_dir / final_shard.name
        with local_final.open("w") as out:
            if root_files_part.exists():
                out.write(root_files_part.read_text())
            for batch_index in range(len(scan_batches)):
                state_part = parts_dir / f"part-{batch_index:05d}.jsonl"
                if not state_part.exists() or state_part.stat().st_size == 0:
                    raise RuntimeError(f"missing completed child part: {state_part}")
                with state_part.open() as part:
                    shutil.copyfileobj(part, out)

        pending_final = final_shard.with_suffix(".jsonl.pending")
        shutil.copyfile(local_final, pending_final)
        state_volume.commit()
        os.replace(pending_final, final_shard)
        state_volume.commit()

    stats = summarize_json_shard(final_shard)
    return {
        "status": "complete",
        "source_prefix": source,
        "shard_index": shard_index,
        "shard": str(final_shard),
        "child_dirs": len(scan_dirs),
        "children_per_part": children_per_part,
        "parts": len(scan_batches),
        "resumed_children": resumed,
        "completed_children": completed,
        "entries": stats["entries"],
        "files": stats["files"],
        "directories": stats["directories"],
        "bytes": stats["bytes"],
        "elapsed_seconds": round(time.time() - started_at, 3),
    }


def summarize_json_shard(shard_path: Path) -> dict[str, int]:
    entries = 0
    files = 0
    dirs = 0
    bytes_total = 0
    with shard_path.open() as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            entries += 1
            if record.get("IsDir"):
                dirs += 1
            else:
                files += 1
                bytes_total += int(record.get("Size", 0) or 0)
    return {"entries": entries, "files": files, "directories": dirs, "bytes": bytes_total}


def write_json_array_from_shards(output: Path, shard_paths: list[Path]) -> int:
    written = 0
    first = True
    with output.open("w") as out:
        out.write("[\n")
        for shard_path in shard_paths:
            with shard_path.open() as shard:
                for line in shard:
                    line = line.strip()
                    if not line:
                        continue
                    if not first:
                        out.write(",\n")
                    out.write(line)
                    first = False
                    written += 1
        out.write("\n]\n")
    return written


@app.function(
    image=image,
    volumes={str(SOURCE_MOUNT): source_volume, str(STATE_MOUNT): state_volume},
    timeout=env_int("MODAL_TIMEOUT", 43200),
    cpu=float(os.environ.get("MODAL_FIND_JSON_CPU", os.environ.get("MODAL_CPU", "8"))),
    memory=env_int("MODAL_FIND_JSON_MEMORY", env_int("MODAL_MEMORY", 16384)),
    ephemeral_disk=env_int("MODAL_FIND_JSON_EPHEMERAL_DISK", 524288),
    retries=5,
    max_containers=1,
)
def write_find_json_inventory(
    source_prefix: str = "",
    source_volume_name: str = SOURCE_VOLUME_NAME,
    output_path: str = "plans/find-output.json",
    threads: int = 32,
    limit: int = 0,
) -> dict[str, Any]:
    source_rel = clean_relative_path(source_prefix)
    find_root = SOURCE_MOUNT / source_rel
    ensure_inside(find_root, SOURCE_MOUNT)
    if not find_root.exists() or not find_root.is_dir():
        raise FileNotFoundError(f"source prefix does not exist or is not a directory: {find_root}")

    source = source_prefix.strip("/")
    output = STATE_MOUNT / clean_relative_path(output_path)
    summary_path = output.with_suffix(".summary.json")
    shard_dir = output.with_suffix("").with_name(output.with_suffix("").name + ".shards")
    output.parent.mkdir(parents=True, exist_ok=True)
    shard_dir.mkdir(parents=True, exist_ok=True)
    started_at = time.time()

    scan_dirs, shallow_files = mounted_scan_roots(find_root, source)
    if limit > 0:
        scan_dirs = scan_dirs[:limit]

    shallow_shard = shard_dir / "shard-root-files.jsonl"
    entries = 0
    files = 0
    dirs = 0
    bytes_total = 0
    with shallow_shard.open("w") as handle:
        for shallow_file in shallow_files:
            stat = shallow_file.stat()
            rel_to_source = shallow_file.relative_to(find_root).as_posix()
            source_path = posix_join(source, rel_to_source)
            record = {
                "Path": source_path,
                "Name": shallow_file.name,
                "Size": stat.st_size,
                "IsDir": False,
            }
            handle.write(json.dumps(record, separators=(",", ":"), ensure_ascii=False) + "\n")
            entries += 1
            files += 1
            bytes_total += stat.st_size
    state_volume.commit()

    worker_count = max(1, min(threads, len(scan_dirs)))
    print(
        json.dumps(
            {
                "planner": "modal-volume-find-json-fast",
                "scan_dirs": len(scan_dirs),
                "shallow_files": len(shallow_files),
                "threads": worker_count,
                "output_path": str(output),
                "shard_dir": str(shard_dir),
            },
            sort_keys=True,
        ),
        flush=True,
    )

    completed = 0
    resumed = 0
    shard_paths: list[Path] = [shallow_shard]
    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {}
            for index, scan_dir in enumerate(scan_dirs):
                scan_source = posix_join(source, scan_dir.relative_to(find_root).as_posix()) if not source else source
                state_shard = shard_dir / f"shard-{index:05d}.jsonl"
                shard_paths.append(state_shard)
                if state_shard.exists() and state_shard.stat().st_size > 0:
                    resumed += 1
                    continue

                local_shard = tmp_dir / f"shard-{index:05d}.jsonl"
                futures[
                    executor.submit(
                        scan_find_json_shard,
                        scan_dir,
                        scan_source,
                        source,
                        local_shard,
                    )
                ] = (scan_source, local_shard, state_shard)

            print(
                json.dumps(
                    {
                        "planner": "modal-volume-find-json-fast",
                        "scan_dirs": len(scan_dirs),
                        "resumed_prefixes": resumed,
                        "missing_prefixes": len(futures),
                        "status": "resume-check-complete",
                    },
                    sort_keys=True,
                ),
                flush=True,
            )

            for future in as_completed(futures):
                result = future.result()
                completed += 1
                entries += int(result["entries"])
                files += int(result["files"])
                dirs += int(result["directories"])
                bytes_total += int(result["bytes"])
                scan_source, local_shard, state_shard = futures[future]
                shutil.copyfile(local_shard, state_shard)
                state_volume.commit()
                elapsed = max(time.time() - started_at, 0.001)
                print(
                    json.dumps(
                        {
                            "planner": "modal-volume-find-json-fast",
                            "completed_prefixes": completed,
                            "resumed_prefixes": resumed,
                            "scan_dirs": len(scan_dirs),
                            "scan_prefix": scan_source,
                            "status": "finished",
                            "prefix_entries": result["entries"],
                            "prefix_entries_per_second": round(
                                int(result["entries"]) / max(float(result["elapsed_seconds"]), 0.001),
                                2,
                            ),
                            "entries": entries,
                            "files": files,
                            "directories": dirs,
                            "bytes": bytes_total,
                            "entries_per_second": round(entries / elapsed, 2),
                        },
                        sort_keys=True,
                    ),
                    flush=True,
                )

    entries = 0
    files = 0
    dirs = 0
    bytes_total = 0
    for shard_path in shard_paths:
        if not shard_path.exists():
            continue
        stats = summarize_json_shard(shard_path)
        entries += stats["entries"]
        files += stats["files"]
        dirs += stats["directories"]
        bytes_total += stats["bytes"]

    output_entries = write_json_array_from_shards(output, [path for path in shard_paths if path.exists()])
    summary = {
        "source_volume": source_volume_name,
        "source_prefix": source_prefix,
        "source_path": str(find_root),
        "output_path": str(output),
        "summary_path": str(summary_path),
        "shard_dir": str(shard_dir),
        "planner": "modal-volume-find-json-fast",
        "threads": worker_count,
        "scan_dirs": len(scan_dirs),
        "resumed_prefixes": resumed,
        "entries": entries,
        "output_entries": output_entries,
        "files": files,
        "directories": dirs,
        "bytes": bytes_total,
        "output_bytes": output.stat().st_size,
        "elapsed_seconds": round(time.time() - started_at, 3),
        "note": "Output is a single JSON array merged from resumable JSONL shards.",
    }
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    state_volume.commit()
    return summary


@app.function(
    image=image,
    volumes={str(SOURCE_MOUNT): source_volume, str(STATE_MOUNT): state_volume},
    timeout=env_int("MODAL_TIMEOUT", 43200),
    cpu=float(os.environ.get("MODAL_CPU", "2")),
    memory=env_int("MODAL_MEMORY", 4096),
    ephemeral_disk=env_int("MODAL_EPHEMERAL_DISK", 524288),
    max_containers=1,
)
def discover_units_mounted_find(
    source_prefix: str = "",
    source_volume_name: str = SOURCE_VOLUME_NAME,
    unit_depth: int = 2,
    dest_prefix: str = "",
    plan_path: str = "plans/modal-volume-units.jsonl",
    max_package_bytes: str = "200GiB",
    warn_package_bytes: str = "180GiB",
) -> dict[str, Any]:
    source_rel = clean_relative_path(source_prefix)
    find_root = SOURCE_MOUNT / source_rel
    ensure_inside(find_root, SOURCE_MOUNT)
    if not find_root.exists() or not find_root.is_dir():
        raise FileNotFoundError(f"source prefix does not exist or is not a directory: {find_root}")

    max_bytes = parse_bytes(max_package_bytes)
    warn_bytes = parse_bytes(warn_package_bytes)
    remote_plan_path = clean_relative_path(plan_path).as_posix()
    remote_summary_path = str(Path(remote_plan_path).with_suffix(".summary.json"))
    remote_inventory_path = inventory_path_for_plan(remote_plan_path)
    remote_shard_dir_path = str(Path(remote_inventory_path).with_suffix("")) + ".shards"

    units: dict[str, dict[str, int]] = {}
    raw_files: list[dict[str, Any]] = []
    entries = 0
    files = 0
    dirs = 0
    bytes_total = 0
    started_at = time.time()

    output = STATE_MOUNT / remote_plan_path
    inventory = STATE_MOUNT / remote_inventory_path
    summary_path = STATE_MOUNT / remote_summary_path
    state_shard_dir = STATE_MOUNT / remote_shard_dir_path
    output.parent.mkdir(parents=True, exist_ok=True)
    inventory.parent.mkdir(parents=True, exist_ok=True)
    state_shard_dir.mkdir(parents=True, exist_ok=True)

    source = source_prefix.strip("/")
    scan_dirs, shallow_files = mounted_scan_roots(find_root, source)
    print(
        json.dumps(
            {
                "planner": "modal-volume-mounted-find",
                "scan_dirs": len(scan_dirs),
                "shallow_files": len(shallow_files),
                "source_prefix": source_prefix,
            },
            sort_keys=True,
        ),
        flush=True,
    )
    shallow_inventory_records: list[str] = []
    for shallow_file in shallow_files:
        stat = shallow_file.stat()
        rel_to_source = shallow_file.relative_to(find_root).as_posix()
        source_path = posix_join(source, rel_to_source)
        inventory_record = {
            "path": source_path,
            "relative_path": rel_to_source,
            "type": "FILE",
            "size": stat.st_size,
            "mtime": str(stat.st_mtime),
            "unit_path": None,
        }
        shallow_inventory_records.append(json.dumps(inventory_record, sort_keys=True) + "\n")
        raw_files.append(
            {
                "source_path": source_path,
                "dest_path": raw_dest_path(dest_prefix, source_path),
                "bytes": stat.st_size,
                "mtime": str(stat.st_mtime),
            }
        )
        entries += 1
        files += 1
        bytes_total += stat.st_size

    worker_count = max(1, min(MODAL_MOUNTED_FIND_THREADS, len(scan_dirs)))
    queued = 0
    resumed = 0
    with tempfile.TemporaryDirectory() as tmp:
        shard_dir = Path(tmp) / "mounted-find-shards"
        shard_dir.mkdir()
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {}
            for index, scan_dir in enumerate(scan_dirs):
                scan_source = posix_join(source, scan_dir.relative_to(find_root).as_posix()) if not source else source
                local_shard = shard_dir / f"shard-{index:05d}.jsonl"
                state_shard = state_shard_dir / f"shard-{index:05d}.jsonl"
                if state_shard.exists() and state_shard.stat().st_size > 0:
                    shard_result = aggregate_inventory_file(state_shard, dest_prefix)
                    resumed += 1
                    entries += int(shard_result["entries"])
                    files += int(shard_result["files"])
                    dirs += int(shard_result["directories"])
                    bytes_total += int(shard_result["bytes"])
                    raw_files.extend(shard_result["raw_files"])
                    for unit_path, unit_counts in shard_result["units"].items():
                        unit = units.setdefault(unit_path, {"files": 0, "directories": 0, "bytes": 0})
                        unit["files"] += int(unit_counts["files"])
                        unit["directories"] += int(unit_counts["directories"])
                        unit["bytes"] += int(unit_counts["bytes"])
                    print(
                        json.dumps(
                            {
                                "planner": "modal-volume-mounted-find",
                                "scan_prefix": scan_source,
                                "status": "resumed",
                                "shard": str(state_shard),
                                "entries": shard_result["entries"],
                            },
                            sort_keys=True,
                        ),
                        flush=True,
                    )
                    continue

                queued += 1
                print(
                    json.dumps(
                        {
                            "planner": "modal-volume-mounted-find",
                            "scan_prefix": scan_source,
                            "status": "queued",
                        },
                        sort_keys=True,
                    ),
                    flush=True,
                )
                futures[
                    executor.submit(
                        scan_mounted_dir_to_inventory,
                        scan_dir,
                        scan_source,
                        source,
                        unit_depth,
                        dest_prefix,
                        local_shard,
                    )
                ] = (scan_source, local_shard, state_shard)

            completed = 0
            for future in as_completed(futures):
                result = future.result()
                completed += 1
                entries += int(result["entries"])
                files += int(result["files"])
                dirs += int(result["directories"])
                bytes_total += int(result["bytes"])
                raw_files.extend(result["raw_files"])
                for unit_path, unit_counts in result["units"].items():
                    unit = units.setdefault(unit_path, {"files": 0, "directories": 0, "bytes": 0})
                    unit["files"] += int(unit_counts["files"])
                    unit["directories"] += int(unit_counts["directories"])
                    unit["bytes"] += int(unit_counts["bytes"])

                _scan_source, local_shard, state_shard = futures[future]
                shutil.copyfile(local_shard, state_shard)
                state_volume.commit()

                elapsed = max(time.time() - started_at, 0.001)
                print(
                    json.dumps(
                        {
                            "planner": "modal-volume-mounted-find",
                            "completed_prefixes": completed,
                            "scan_dirs": len(scan_dirs),
                            "queued_prefixes": queued,
                            "resumed_prefixes": resumed,
                            "scan_prefix": result["scan_prefix"],
                            "status": "finished",
                            "shard": str(state_shard),
                            "prefix_entries": result["entries"],
                            "prefix_entries_per_second": round(
                                int(result["entries"]) / max(float(result["elapsed_seconds"]), 0.001),
                                2,
                            ),
                            "entries": entries,
                            "files": files,
                            "directories": dirs,
                            "units": len(units),
                            "raw_files": len(raw_files),
                            "entries_per_second": round(entries / elapsed, 2),
                        },
                        sort_keys=True,
                    ),
                    flush=True,
                )

        with inventory.open("w") as inventory_handle:
            inventory_handle.writelines(shallow_inventory_records)
            for shard_file in sorted(state_shard_dir.glob("shard-*.jsonl")):
                with shard_file.open() as shard_handle:
                    for line in shard_handle:
                        inventory_handle.write(line)

    total_package_files = 0
    total_package_dirs = 0
    total_package_bytes = 0
    strategy_counts: dict[str, int] = {}
    rows = 0
    with output.open("w") as plan_handle:
        for raw_index, raw_file in enumerate(sorted(raw_files, key=lambda item: item["source_path"])):
            row = {
                "index": rows,
                "kind": "raw_file",
                "source_volume": source_volume_name,
                "source_path": raw_file["source_path"],
                "dest_path": raw_file["dest_path"],
                "archive_dest": raw_file["dest_path"],
                "package_format": "raw",
                "files": 1,
                "directories": 0,
                "bytes": raw_file["bytes"],
                "planner": "modal-volume-mounted-find",
                "raw_index": raw_index,
            }
            plan_handle.write(json.dumps(row, sort_keys=True) + "\n")
            rows += 1

        for unit_path in sorted(units):
            unit = units[unit_path]
            unit_files = int(unit["files"])
            unit_dirs = int(unit["directories"])
            unit_bytes = int(unit["bytes"])
            archive_dest, package_index_dest, files_index_dest = target_paths(dest_prefix, unit_path)
            strategy = package_strategy(unit_bytes, max_bytes, warn_bytes)
            row = {
                "index": rows,
                "kind": "package",
                "source_volume": source_volume_name,
                "source_path": unit_path,
                "archive_dest": archive_dest,
                "package_index_dest": package_index_dest,
                "files_index_dest": files_index_dest,
                "package_format": "tar.zst",
                "files": unit_files,
                "directories": unit_dirs,
                "bytes": unit_bytes,
                "package_strategy": strategy,
                "max_package_bytes": max_bytes,
                "warn_package_bytes": warn_bytes,
                "planner": "modal-volume-mounted-find",
            }
            plan_handle.write(json.dumps(row, sort_keys=True) + "\n")
            rows += 1
            total_package_files += unit_files
            total_package_dirs += unit_dirs
            total_package_bytes += unit_bytes
            strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1

    summary = {
        "source_volume": source_volume_name,
        "source_prefix": source_prefix,
        "unit_depth": unit_depth,
        "dest_prefix": dest_prefix,
        "plan_path": str(output),
        "summary_path": str(summary_path),
        "inventory_path": str(inventory),
        "inventory_shard_dir": str(state_shard_dir),
        "planner": "modal-volume-mounted-find",
        "queued_prefixes": queued,
        "resumed_prefixes": resumed,
        "entries": entries,
        "files": files,
        "directories": dirs,
        "bytes": bytes_total,
        "package_rows": len(units),
        "raw_file_rows": len(raw_files),
        "plan_rows": rows,
        "package_files": total_package_files,
        "package_directories": total_package_dirs,
        "package_bytes": total_package_bytes,
        "max_package_bytes": max_bytes,
        "warn_package_bytes": warn_bytes,
        "strategy_counts": strategy_counts,
        "elapsed_seconds": round(time.time() - started_at, 3),
    }
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    state_volume.commit()
    return summary


def load_remotes(worker_index: int, remote_group_size: int) -> list[str]:
    if not RCLONE_MANIFEST.exists():
        raise FileNotFoundError(f"missing rclone manifest in Modal credentials volume: {RCLONE_MANIFEST}")
    rows: list[dict[str, str]] = []
    with RCLONE_MANIFEST.open(newline="") as handle:
        rows = [row for row in csv.DictReader(handle) if row.get("remote")]

    assigned = [row["remote"] for row in rows if row.get("worker_index") == str(worker_index)]
    if len(assigned) >= remote_group_size:
        return assigned

    start = worker_index * remote_group_size
    fallback = [row["remote"] for row in rows[start : start + remote_group_size]]
    if fallback:
        return fallback

    all_remotes = [row["remote"] for row in rows]
    if not all_remotes:
        raise RuntimeError("no remotes in rclone manifest")
    return [all_remotes[worker_index % len(all_remotes)]]


def default_spool_name(plan_path: str) -> str:
    return Path(clean_relative_path(plan_path)).stem


def spool_base(spool_name: str, plan_path: str) -> Path:
    name = clean_relative_path(spool_name or default_spool_name(plan_path)).as_posix()
    return CACHE_MOUNT / "archive-spool" / name


def spool_file_path(spool_root: Path, dest_path: str) -> Path:
    return spool_root / clean_relative_path(dest_path)


def file_record(path: Path, base: Path) -> dict[str, Any]:
    stat = path.lstat()
    rel = path.relative_to(base).as_posix()
    record: dict[str, Any] = {
        "path": rel,
        "mode": oct(stat.st_mode),
        "mtime_ns": stat.st_mtime_ns,
        "size": stat.st_size,
    }
    if path.is_symlink():
        record["type"] = "symlink"
        record["target"] = os.readlink(path)
    elif path.is_dir():
        record["type"] = "directory"
    elif path.is_file():
        record["type"] = "file"
    else:
        record["type"] = "other"
    return record


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def finalize_package_index(package_index_path: Path, archive_path: Path) -> dict[str, Any]:
    archive_bytes = archive_path.stat().st_size
    archive_sha256 = sha256_file(archive_path)
    return apply_archive_metadata(package_index_path, archive_bytes, archive_sha256)


def apply_archive_metadata(
    package_index_path: Path, archive_bytes: int, archive_sha256: str
) -> dict[str, Any]:
    payload = json.loads(package_index_path.read_text())
    payload["package"]["archive_bytes"] = archive_bytes
    payload["package"]["archive_sha256"] = archive_sha256
    payload["package"]["archive_hash_algorithm"] = "sha256"
    encoded = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode()
    package_index_path.write_bytes(encoded)
    return {
        "archive_bytes": archive_bytes,
        "archive_sha256": archive_sha256,
        "package_index_sha256": hashlib.sha256(encoded).hexdigest(),
    }


def write_indexes_many(
    source_abs_paths: list[Path],
    source_volume_name: str,
    source_rel_paths: list[str],
    archive_dest: str,
    package_index_dest: str,
    files_index_dest: str,
    output_dir: Path,
    max_package_bytes: int,
    warn_package_bytes: int,
) -> dict[str, Any]:
    if not source_abs_paths or len(source_abs_paths) != len(source_rel_paths):
        raise ValueError("source paths must be non-empty and have matching absolute/relative entries")

    clean_rel_paths = [clean_relative_path(path).as_posix() for path in source_rel_paths]
    ordered_paths = sorted(set(clean_rel_paths), key=lambda item: Path(item).parts)
    if len(ordered_paths) != len(clean_rel_paths):
        raise ValueError("duplicate archive roots are not allowed")
    for path, next_path in zip(ordered_paths, ordered_paths[1:]):
        if next_path.startswith(f"{path}/"):
            raise ValueError(f"overlapping archive roots are not allowed: {path} and {next_path}")

    files_index_jsonl = output_dir / "files.index.jsonl"
    files_index_zst = output_dir / "files.index.jsonl.zst"
    package_index = output_dir / "package.index.json"
    file_count = 0
    dir_count = 0
    byte_count = 0

    with files_index_jsonl.open("w") as handle:
        for source_abs in source_abs_paths:
            ensure_inside(source_abs, SOURCE_MOUNT)
            if source_abs.is_symlink() or source_abs.is_file():
                record = file_record(source_abs, SOURCE_MOUNT)
                handle.write(json.dumps(record, sort_keys=True) + "\n")
                if record["type"] == "file":
                    file_count += 1
                    byte_count += int(record["size"])
                continue
            if not source_abs.is_dir():
                raise FileNotFoundError(f"missing archive source root: {source_abs}")

            root_record = file_record(source_abs, SOURCE_MOUNT)
            handle.write(json.dumps(root_record, sort_keys=True) + "\n")
            dir_count += 1
            for root, dirnames, filenames in os.walk(source_abs):
                root_path = Path(root)
                for dirname in sorted(dirnames):
                    path = root_path / dirname
                    handle.write(json.dumps(file_record(path, SOURCE_MOUNT), sort_keys=True) + "\n")
                    dir_count += 1
                for filename in sorted(filenames):
                    path = root_path / filename
                    record = file_record(path, SOURCE_MOUNT)
                    handle.write(json.dumps(record, sort_keys=True) + "\n")
                    if record["type"] == "file":
                        file_count += 1
                        byte_count += int(record["size"])

    subprocess.run(["zstd", "-q", "-T0", "-3", "-f", str(files_index_jsonl), "-o", str(files_index_zst)], check=True)
    files_index_jsonl.unlink()
    strategy = package_strategy(byte_count, max_package_bytes, warn_package_bytes)

    payload = {
        "schema": "shared-drive-migration/modal-volume-index/v1",
        "created_at_unix": int(time.time()),
        "source": {
            "adapter": "modal-volume",
            "volume": source_volume_name,
            "path": clean_rel_paths[0] if len(clean_rel_paths) == 1 else None,
            "paths": clean_rel_paths,
            "root_count": len(clean_rel_paths),
        },
        "package": {
            "format": "tar.zst",
            "archive_path": archive_dest,
            "package_index_path": package_index_dest,
            "files_index_path": files_index_dest,
            "strategy": strategy,
            "max_package_bytes": max_package_bytes,
            "warn_package_bytes": warn_package_bytes,
            "independently_extractable": True,
            "paths_relative_to": "modal-volume-root",
            "extract_command": "tar --zstd -xf PACKAGE.tar.zst -C RESTORE_ROOT",
        },
        "summary": {
            "files": file_count,
            "directories": dir_count,
            "bytes": byte_count,
            "records": file_count + dir_count,
        },
    }
    encoded = json.dumps(payload, indent=2, sort_keys=True).encode()
    package_index.write_bytes(encoded)
    return {
        "package_index_path": str(package_index),
        "files_index_path": str(files_index_zst),
        "files": file_count,
        "directories": dir_count,
        "bytes": byte_count,
        "records": file_count + dir_count,
        "package_strategy": strategy,
        "package_index_sha256": hashlib.sha256(encoded).hexdigest(),
        "files_index_sha256": sha256_file(files_index_zst),
    }


def write_indexes(
    unit_abs: Path,
    source_volume_name: str,
    unit_rel: str,
    archive_dest: str,
    package_index_dest: str,
    files_index_dest: str,
    output_dir: Path,
    max_package_bytes: int,
    warn_package_bytes: int,
) -> dict[str, Any]:
    return write_indexes_many(
        [unit_abs],
        source_volume_name,
        [unit_rel],
        archive_dest,
        package_index_dest,
        files_index_dest,
        output_dir,
        max_package_bytes,
        warn_package_bytes,
    )


def run_checked(command: str) -> None:
    subprocess.run(command, shell=True, executable="/bin/bash", check=True)


class RcloneError(RuntimeError):
    def __init__(self, message: str, returncode: int) -> None:
        super().__init__(message)
        self.returncode = returncode


def raise_for_rclone_failure(result: subprocess.CompletedProcess[str], command_label: str) -> None:
    if result.returncode == 0:
        return
    output = "\n".join(part for part in [result.stdout, result.stderr] if part)
    raise RcloneError(f"{command_label} failed with exit code {result.returncode}\n{output}".strip(), result.returncode)


def is_drive_rate_limit_error(exc: Exception) -> bool:
    message = repr(exc).lower()
    return any(
        marker in message
        for marker in (
            "userratelimitexceeded",
            "user rate limit exceeded",
            "received upload limit error",
            "daily upload limit",
            "upload limit",
        )
    )


def truthy(value: str) -> bool:
    return value.strip().lower() not in {"", "0", "false", "no", "off"}


def positive_float(value: str) -> bool:
    try:
        return float(value) > 0
    except ValueError:
        return False


def rclone_common_args() -> list[str]:
    args = [
        "--drive-chunk-size",
        RCLONE_DRIVE_CHUNK_SIZE,
        "--transfers",
        RCLONE_TRANSFERS,
        "--checkers",
        RCLONE_CHECKERS,
        "--retries",
        RCLONE_RETRIES,
        "--low-level-retries",
        RCLONE_LOW_LEVEL_RETRIES,
        "--retries-sleep",
        RCLONE_RETRIES_SLEEP,
        "--contimeout",
        RCLONE_CONTIMEOUT,
        "--timeout",
        RCLONE_TIMEOUT,
        "--stats",
        RCLONE_STATS,
        "--stats-file-name-length",
        RCLONE_STATS_FILE_NAME_LENGTH,
        "--log-level",
        RCLONE_LOG_LEVEL,
    ]
    if positive_float(RCLONE_TPSLIMIT):
        args.extend(["--tpslimit", RCLONE_TPSLIMIT, "--tpslimit-burst", RCLONE_TPSLIMIT_BURST])
    if truthy(RCLONE_DRIVE_STOP_ON_UPLOAD_LIMIT):
        args.append("--drive-stop-on-upload-limit")
    return args


def rclone_shell_flags() -> str:
    return " ".join(shlex.quote(part) for part in rclone_common_args())


def retry_transient_drive_rate_limit(operation: Callable[[], None], label: str) -> None:
    """Keep a staged package in place while Drive asks us to slow down."""
    retry_with_exponential_backoff(
        operation,
        should_retry=is_drive_rate_limit_error,
        retries=RCLONE_RATE_LIMIT_RETRIES,
        base_delay_seconds=RCLONE_RATE_LIMIT_BACKOFF_SECONDS,
    )


def rclone_rcat(remote: str, dest_path: str, local_path: Path) -> None:
    def operation() -> None:
        command = [
            "rclone",
            "--config",
            str(RCLONE_CONFIG),
            "rcat",
            f"{remote}:{dest_path}",
        ]
        command.extend(rclone_common_args())
        with local_path.open("rb") as handle:
            result = subprocess.run(command, stdin=handle, text=False, capture_output=True)
        decoded = subprocess.CompletedProcess(
            args=result.args,
            returncode=result.returncode,
            stdout=result.stdout.decode(errors="replace") if result.stdout else "",
            stderr=result.stderr.decode(errors="replace") if result.stderr else "",
        )
        raise_for_rclone_failure(decoded, f"rclone rcat {remote}:{dest_path}")

    retry_transient_drive_rate_limit(operation, f"rclone rcat {remote}:{dest_path}")


def rclone_copyto(remote: str, dest_path: str, local_path: Path) -> None:
    def operation() -> None:
        command = [
            "rclone",
            "--config",
            str(RCLONE_CONFIG),
            "copyto",
            str(local_path),
            f"{remote}:{dest_path}",
        ]
        command.extend(rclone_common_args())
        result = subprocess.run(command, text=True, capture_output=True)
        raise_for_rclone_failure(result, f"rclone copyto {remote}:{dest_path}")

    retry_transient_drive_rate_limit(operation, f"rclone copyto {remote}:{dest_path}")


def rclone_remote_file_size(remote: str, dest_path: str) -> int | None:
    command = [
        "rclone",
        "--config",
        str(RCLONE_CONFIG),
        "lsjson",
        f"{remote}:{dest_path}",
        "--stat",
        "--files-only",
    ]
    result = subprocess.run(command, text=True, capture_output=True)
    if result.returncode != 0:
        output = f"{result.stdout}\n{result.stderr}".lower()
        if "directory not found" in output or "object not found" in output or "not found" in output:
            return None
        raise_for_rclone_failure(result, f"rclone lsjson --stat {remote}:{dest_path}")
    payload = json.loads(result.stdout)
    if not isinstance(payload, dict) or bool(payload.get("IsDir", False)):
        return None
    return int(payload.get("Size", 0) or 0)


def rclone_list_files(remote: str, dest_prefix: str) -> dict[str, int]:
    """Return files below a Drive prefix, keyed relative to that prefix."""
    command = [
        "rclone",
        "--config",
        str(RCLONE_CONFIG),
        "lsjson",
        f"{remote}:{dest_prefix.strip('/')}",
        "--recursive",
        "--files-only",
    ]
    command.extend(rclone_common_args())
    result = subprocess.run(command, text=True, capture_output=True)
    raise_for_rclone_failure(result, f"rclone lsjson {remote}:{dest_prefix}")
    payload = json.loads(result.stdout)
    if not isinstance(payload, list):
        raise ValueError("rclone lsjson recursive response was not a list")
    return {
        str(entry["Path"]).strip("/"): int(entry.get("Size", 0) or 0)
        for entry in payload
        if isinstance(entry, dict) and not bool(entry.get("IsDir", False)) and entry.get("Path")
    }


def rclone_read_text(remote: str, dest_path: str) -> str:
    command = ["rclone", "--config", str(RCLONE_CONFIG), "cat", f"{remote}:{dest_path}"]
    command.extend(rclone_common_args())
    result = subprocess.run(command, text=True, capture_output=True)
    raise_for_rclone_failure(result, f"rclone cat {remote}:{dest_path}")
    return result.stdout


def rclone_deletefile(remote: str, dest_path: str) -> None:
    command = ["rclone", "--config", str(RCLONE_CONFIG), "deletefile", f"{remote}:{dest_path}"]
    command.extend(rclone_common_args())
    result = subprocess.run(command, text=True, capture_output=True)
    raise_for_rclone_failure(result, f"rclone deletefile {remote}:{dest_path}")


def rclone_moveto(remote: str, dest_path: str, local_path: Path) -> None:
    command = [
        "rclone",
        "--config",
        str(RCLONE_CONFIG),
        "moveto",
        str(local_path),
        f"{remote}:{dest_path}",
    ]
    command.extend(rclone_common_args())
    result = subprocess.run(command, text=True, capture_output=True)
    raise_for_rclone_failure(result, f"rclone moveto {remote}:{dest_path}")


def rclone_purge(remote: str, dest_prefix: str, dry_run: bool) -> None:
    command = [
        "rclone",
        "--config",
        str(RCLONE_CONFIG),
        "purge",
        f"{remote}:{dest_prefix.strip('/')}",
    ]
    command.extend(rclone_common_args())
    if dry_run:
        command.append("--dry-run")
    subprocess.run(command, check=True)


def write_tar_roots_file(source_rel_paths: list[str], output_path: Path) -> None:
    if not source_rel_paths:
        raise ValueError("archive requires at least one source path")
    with output_path.open("wb") as handle:
        for source_rel in source_rel_paths:
            clean_path = clean_relative_path(source_rel).as_posix()
            if not clean_path:
                raise ValueError("archiving the entire source mount is not supported")
            handle.write(os.fsencode(clean_path))
            handle.write(b"\0")


def upload_archive_stream_many(
    source_rel_paths: list[str],
    remote: str,
    archive_dest: str,
    compression_level: int,
    compression_threads: int = 2,
) -> None:
    config = shlex.quote(str(RCLONE_CONFIG))
    target = shlex.quote(f"{remote}:{archive_dest}")
    level = max(1, min(19, int(compression_level)))
    threads = max(1, int(compression_threads))
    rclone_flags = rclone_shell_flags()
    with tempfile.TemporaryDirectory() as tmp:
        roots_file = Path(tmp) / "archive-roots.nul"
        write_tar_roots_file(source_rel_paths, roots_file)
        command = (
            "set -o pipefail; "
            f"tar -C {shlex.quote(str(SOURCE_MOUNT))} --null "
            f"-T {shlex.quote(str(roots_file))} -cf - "
            f"| zstd -q -T{threads} -{level} "
            f"| rclone --config {config} rcat {target} "
            f"{rclone_flags}"
        )
        result = subprocess.run(command, shell=True, executable="/bin/bash", text=True, capture_output=True)
    raise_for_rclone_failure(result, f"stream upload {remote}:{archive_dest}")


def upload_archive_stream(
    unit_abs: Path,
    remote: str,
    archive_dest: str,
    compression_level: int,
    compression_threads: int = 2,
) -> None:
    ensure_inside(unit_abs, SOURCE_MOUNT)
    upload_archive_stream_many(
        [unit_abs.relative_to(SOURCE_MOUNT).as_posix()],
        remote,
        archive_dest,
        compression_level,
        compression_threads,
    )


def create_archive_staged_many(
    source_rel_paths: list[str],
    archive_path: Path,
    compression_level: int,
    compression_threads: int = 2,
) -> None:
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    out = shlex.quote(str(archive_path))
    level = max(1, min(19, int(compression_level)))
    threads = max(1, int(compression_threads))
    with tempfile.TemporaryDirectory() as tmp:
        roots_file = Path(tmp) / "archive-roots.nul"
        write_tar_roots_file(source_rel_paths, roots_file)
        command = (
            "set -o pipefail; "
            f"tar -C {shlex.quote(str(SOURCE_MOUNT))} --null "
            f"-T {shlex.quote(str(roots_file))} -cf - "
            f"| zstd -q -T{threads} -{level} -f -o {out}"
        )
        run_checked(command)


def create_archive_staged(
    unit_abs: Path,
    archive_path: Path,
    compression_level: int,
    compression_threads: int = 2,
) -> None:
    ensure_inside(unit_abs, SOURCE_MOUNT)
    create_archive_staged_many(
        [unit_abs.relative_to(SOURCE_MOUNT).as_posix()],
        archive_path,
        compression_level,
        compression_threads,
    )


def load_plan(plan_path: str) -> list[dict[str, Any]]:
    path = STATE_MOUNT / clean_relative_path(plan_path)
    if not path.exists():
        raise FileNotFoundError(f"missing plan: {path}")
    rows: list[dict[str, Any]] = []
    with path.open() as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


@app.function(
    image=image,
    volumes={str(CREDS_MOUNT): creds_volume, str(STATE_MOUNT): state_volume},
    timeout=env_int("MODAL_TIMEOUT", 43200),
    cpu=float(os.environ.get("MODAL_CPU", "2")),
    memory=env_int("MODAL_MEMORY", 4096),
    max_containers=env_int("MODAL_MAX_CONTAINERS", 10),
    retries=1,
)
def smoke_worker(
    worker_index: int,
    run_id: str,
    dest_prefix: str = "_sdmig_smoke",
    remote_group_size: int = 10,
    dry_run: bool = True,
    compression_level: int = 3,
) -> dict[str, Any]:
    remotes = load_remotes(worker_index, remote_group_size)
    remote = remotes[0]
    smoke_prefix = "/".join([dest_prefix.strip("/"), run_id, f"worker-{worker_index:03d}"])
    archive_dest = f"{smoke_prefix}/worker-{worker_index:03d}.tar.zst"
    index_dest = f"{smoke_prefix}/worker-{worker_index:03d}.package.index.json"

    result: dict[str, Any] = {
        "worker_index": worker_index,
        "remote": remote,
        "archive_dest": archive_dest,
        "index_dest": index_dest,
        "dry_run": dry_run,
    }

    with tempfile.TemporaryDirectory() as tmp:
        unit_dir = Path(tmp) / f"worker-{worker_index:03d}"
        unit_dir.mkdir()
        payload = {
            "schema": "shared-drive-migration/modal-volume-smoke/v1",
            "worker_index": worker_index,
            "run_id": run_id,
            "remote": remote,
            "created_at_unix": int(time.time()),
        }
        (unit_dir / "payload.json").write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        index_path = Path(tmp) / "index.json"
        index_path.write_text(
            json.dumps(
                {
                    "schema": "shared-drive-migration/modal-volume-index/v1",
                    "source": {"adapter": "modal-volume-smoke", "path": unit_dir.name},
                    "package": {"format": "tar.zst", "archive_path": archive_dest, "package_index_path": index_dest},
                    "entries": [{"path": "payload.json", "type": "file", "size": (unit_dir / "payload.json").stat().st_size}],
                },
                indent=2,
                sort_keys=True,
            )
            + "\n"
        )
        if not dry_run:
            upload_archive_stream(unit_dir, remote, archive_dest, compression_level)
            rclone_rcat(remote, index_dest, index_path)
    result["status"] = "planned" if dry_run else "uploaded"
    return result


@app.function(
    image=image,
    volumes={str(CREDS_MOUNT): creds_volume},
    timeout=env_int("MODAL_TIMEOUT", 43200),
    cpu=float(os.environ.get("MODAL_CPU", "2")),
    memory=env_int("MODAL_MEMORY", 4096),
    ephemeral_disk=env_int("MODAL_EPHEMERAL_DISK", 524288),
    max_containers=1,
)
def cleanup_remote_prefix(
    dest_prefix: str,
    remote: str = "",
    dry_run: bool = True,
    allow_unsafe_delete: bool = False,
) -> dict[str, Any]:
    prefix = dest_prefix.strip("/")
    if not prefix:
        raise ValueError("refusing to clean an empty destination prefix")
    if not allow_unsafe_delete and not prefix.startswith("_sdmig_smoke/"):
        raise ValueError("cleanup is only allowed for _sdmig_smoke/... unless allow_unsafe_delete=True")

    selected_remote = remote or load_remotes(0, 10)[0]
    rclone_purge(selected_remote, prefix, dry_run=dry_run)
    return {"remote": selected_remote, "dest_prefix": prefix, "dry_run": dry_run, "status": "planned" if dry_run else "deleted"}


@app.function(
    image=image,
    volumes={str(CREDS_MOUNT): creds_volume, str(STATE_MOUNT): state_volume},
    timeout=env_int("MODAL_TIMEOUT", 43200),
    cpu=float(os.environ.get("MODAL_CPU", "2")),
    memory=env_int("MODAL_MEMORY", 4096),
    max_containers=1,
)
def audit_drive_packages(
    dest_prefix: str,
    manifest_path: str,
    remote: str = "",
) -> dict[str, Any]:
    """Record complete package triplets and isolated partial artifacts on Drive."""
    prefix = dest_prefix.strip("/")
    if not prefix:
        raise ValueError("dest_prefix is required for a Drive package audit")
    selected_remote = remote or load_remotes(0, 10)[0]
    files = rclone_list_files(selected_remote, prefix)

    bases: set[str] = set()
    for path in files:
        if path.endswith(".tar.zst") and not path.endswith(".files.index.jsonl.zst"):
            bases.add(path.removesuffix(".tar.zst"))
        elif path.endswith(".package.index.json"):
            bases.add(path.removesuffix(".package.index.json"))
        elif path.endswith(".files.index.jsonl.zst"):
            bases.add(path.removesuffix(".files.index.jsonl.zst"))

    def drive_path(relative_path: str) -> str:
        return posix_join(prefix, relative_path)

    completed_packages: list[dict[str, Any]] = []
    incomplete_artifacts: set[str] = set()
    completed_source_paths: set[str] = set()
    invalid_indexes: list[dict[str, str]] = []
    for base in sorted(bases):
        archive_relative = f"{base}.tar.zst"
        package_index_relative = f"{base}.package.index.json"
        files_index_relative = f"{base}.files.index.jsonl.zst"
        triplet = (archive_relative, package_index_relative, files_index_relative)
        if not all(path in files for path in triplet):
            incomplete_artifacts.update(path for path in triplet if path in files)
            continue
        try:
            package_index = json.loads(rclone_read_text(selected_remote, drive_path(package_index_relative)))
            source = package_index.get("source", {})
            source_paths = source.get("paths", [])
            if not isinstance(source_paths, list) or not source_paths:
                raise ValueError("package index has no source.paths")
            normalized_paths = [clean_relative_path(str(path)).as_posix() for path in source_paths]
            if len(set(normalized_paths)) != len(normalized_paths):
                raise ValueError("package index source.paths contains duplicates")
        except Exception as exc:  # noqa: BLE001 - keep corrupt indexes out of resume exclusions.
            invalid_indexes.append({"package_index": drive_path(package_index_relative), "error": repr(exc)})
            incomplete_artifacts.update(triplet)
            continue
        completed_source_paths.update(normalized_paths)
        completed_packages.append(
            {
                "archive": drive_path(archive_relative),
                "package_index": drive_path(package_index_relative),
                "files_index": drive_path(files_index_relative),
                "archive_bytes": files[archive_relative],
                "source_paths": normalized_paths,
            }
        )

    payload = {
        "schema": "shared-drive-migration/drive-package-audit/v1",
        "created_at_unix": int(time.time()),
        "remote": selected_remote,
        "dest_prefix": prefix,
        "completed_packages": completed_packages,
        "completed_source_paths": sorted(completed_source_paths),
        "incomplete_artifacts": [drive_path(path) for path in sorted(incomplete_artifacts)],
        "invalid_indexes": invalid_indexes,
        "drive_file_count": len(files),
    }
    output = STATE_MOUNT / clean_relative_path(manifest_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    state_volume.commit()
    return {
        "manifest_path": str(output),
        "completed_packages": len(completed_packages),
        "completed_source_paths": len(completed_source_paths),
        "incomplete_artifacts": len(incomplete_artifacts),
        "invalid_indexes": len(invalid_indexes),
        "drive_file_count": len(files),
    }


@app.function(
    image=image,
    volumes={str(CREDS_MOUNT): creds_volume, str(STATE_MOUNT): state_volume},
    timeout=env_int("MODAL_TIMEOUT", 43200),
    cpu=float(os.environ.get("MODAL_CPU", "2")),
    memory=env_int("MODAL_MEMORY", 4096),
    max_containers=1,
)
def cleanup_incomplete_drive_packages(
    manifest_path: str,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Delete only artifacts that a preceding audit proved are not a complete triplet."""
    manifest = STATE_MOUNT / clean_relative_path(manifest_path)
    payload = json.loads(manifest.read_text())
    remote = str(payload["remote"])
    dest_prefix = str(payload["dest_prefix"]).strip("/")
    artifacts = [str(path).strip("/") for path in payload.get("incomplete_artifacts", [])]
    if not dest_prefix or any(not path.startswith(f"{dest_prefix}/") for path in artifacts):
        raise ValueError("refusing to delete artifacts outside the audited destination prefix")
    if not dry_run:
        for artifact in artifacts:
            rclone_deletefile(remote, artifact)
    return {
        "manifest_path": str(manifest),
        "remote": remote,
        "deleted_artifacts": len(artifacts),
        "dry_run": dry_run,
        "status": "planned" if dry_run else "deleted",
    }


@app.function(
    image=image,
    volumes={str(SOURCE_MOUNT): source_volume, str(STATE_MOUNT): state_volume},
    timeout=env_int("MODAL_TIMEOUT", 43200),
    cpu=float(os.environ.get("MODAL_CPU", "2")),
    memory=env_int("MODAL_MEMORY", 4096),
    ephemeral_disk=env_int("MODAL_EPHEMERAL_DISK", 524288),
    max_containers=1,
)
def discover_units(
    source_prefix: str = "",
    source_volume_name: str = SOURCE_VOLUME_NAME,
    unit_depth: int = 2,
    dest_prefix: str = "",
    plan_path: str = "plans/modal-volume-units.jsonl",
    limit: int = 0,
    include_stats: bool = False,
    max_package_bytes: str = "200GiB",
    warn_package_bytes: str = "180GiB",
) -> dict[str, Any]:
    source_rel = clean_relative_path(source_prefix)
    base = SOURCE_MOUNT / source_rel
    ensure_inside(base, SOURCE_MOUNT)
    if not base.exists() or not base.is_dir():
        raise FileNotFoundError(f"source prefix does not exist or is not a directory: {base}")

    output = STATE_MOUNT / clean_relative_path(plan_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    max_bytes = parse_bytes(max_package_bytes)
    warn_bytes = parse_bytes(warn_package_bytes)

    count = 0
    total_files = 0
    total_dirs = 0
    total_bytes = 0
    with output.open("w") as handle:
        for unit_abs in iter_unit_dirs(base, unit_depth):
            unit_rel = unit_abs.relative_to(SOURCE_MOUNT).as_posix()
            archive_dest, package_index_dest, files_index_dest = target_paths(dest_prefix, unit_rel)
            row: dict[str, Any] = {
                "index": count,
                "source_volume": source_volume_name,
                "source_path": unit_rel,
                "archive_dest": archive_dest,
                "package_index_dest": package_index_dest,
                "files_index_dest": files_index_dest,
                "package_format": "tar.zst",
                "max_package_bytes": max_bytes,
                "warn_package_bytes": warn_bytes,
            }
            if include_stats:
                files, dirs, bytes_total = count_tree(unit_abs)
                row.update(
                    {
                        "files": files,
                        "directories": dirs,
                        "bytes": bytes_total,
                        "package_strategy": package_strategy(bytes_total, max_bytes, warn_bytes),
                    }
                )
                total_files += files
                total_dirs += dirs
                total_bytes += bytes_total
            handle.write(json.dumps(row, sort_keys=True) + "\n")
            count += 1
            if limit > 0 and count >= limit:
                break

    summary = {
        "source_volume": source_volume_name,
        "source_prefix": source_prefix,
        "unit_depth": unit_depth,
        "dest_prefix": dest_prefix,
        "plan_path": str(output),
        "units": count,
        "include_stats": include_stats,
        "max_package_bytes": max_bytes,
        "warn_package_bytes": warn_bytes,
        "files": total_files if include_stats else None,
        "directories": total_dirs if include_stats else None,
        "bytes": total_bytes if include_stats else None,
    }
    summary_path = output.with_suffix(".summary.json")
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    state_volume.commit()
    return summary


@app.function(
    image=image,
    volumes={str(SOURCE_MOUNT): source_volume, str(STATE_MOUNT): state_volume},
    timeout=env_int("MODAL_TIMEOUT", 43200),
    cpu=float(os.environ.get("MODAL_RCLONE_LSJSON_CPU", os.environ.get("MODAL_CPU", "4"))),
    memory=env_int("MODAL_RCLONE_LSJSON_MEMORY", env_int("MODAL_MEMORY", 8192)),
    ephemeral_disk=env_int("MODAL_RCLONE_LSJSON_EPHEMERAL_DISK", 524288),
    max_containers=1,
)
def write_rclone_lsjson_inventory(
    source_prefix: str = "",
    output_path: str = "plans/rclone-output.json",
    files_only: bool = False,
) -> dict[str, Any]:
    source_rel = clean_relative_path(source_prefix)
    source_path = SOURCE_MOUNT / source_rel
    ensure_inside(source_path, SOURCE_MOUNT)
    if not source_path.exists() or not source_path.is_dir():
        raise FileNotFoundError(f"source prefix does not exist or is not a directory: {source_path}")

    output = STATE_MOUNT / clean_relative_path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    summary_path = output.with_suffix(".summary.json")
    started_at = time.time()

    command = [
        "rclone",
        "lsjson",
        "-R",
        "--no-mimetype",
        "--no-modtime",
        str(source_path),
    ]
    if files_only:
        command.insert(3, "--files-only")

    with output.open("w") as stdout:
        result = subprocess.run(command, text=True, stdout=stdout, stderr=subprocess.PIPE)
    if result.returncode != 0:
        raise RuntimeError(f"rclone lsjson failed with exit {result.returncode}: {result.stderr.strip()}")

    output_size = output.stat().st_size
    summary = {
        "source_volume": SOURCE_VOLUME_NAME,
        "source_prefix": source_prefix,
        "source_path": str(source_path),
        "output_path": str(output),
        "summary_path": str(summary_path),
        "output_bytes": output_size,
        "files_only": files_only,
        "command": command,
        "elapsed_seconds": round(time.time() - started_at, 3),
        "note": "Raw rclone lsjson output is a JSON array. It is not a package plan until reduced.",
    }
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    state_volume.commit()
    return summary


@app.function(
    image=image,
    volumes={
        str(SOURCE_MOUNT): source_volume,
        str(CREDS_MOUNT): creds_volume,
        str(STATE_MOUNT): state_volume,
        str(CACHE_MOUNT): cache_volume,
    },
    timeout=env_int("MODAL_TIMEOUT", 43200),
    cpu=float(os.environ.get("MODAL_CPU", "2")),
    memory=env_int("MODAL_MEMORY", 4096),
    max_containers=env_int("MODAL_MAX_CONTAINERS", 10),
    retries=1,
)
def upload_worker(
    worker_index: int,
    worker_count: int = 10,
    plan_path: str = "plans/modal-volume-units.jsonl",
    remote_group_size: int = 10,
    assignment_mode: str = "modulo",
    dry_run: bool = True,
    limit: int = 0,
    compression_level: int = 3,
    compression_threads: int = 2,
    upload_mode: str = "stream",
    max_package_bytes: str = "200GiB",
    warn_package_bytes: str = "180GiB",
    max_uploads_per_remote: int = MODAL_MAX_UPLOADS_PER_REMOTE,
    max_bytes_per_remote: str = "0",
    remote_start_offset: int = 0,
) -> dict[str, Any]:
    rows = load_plan(plan_path)
    remotes = load_remotes(worker_index, remote_group_size)
    if remotes:
        offset = int(remote_start_offset) % len(remotes)
        remotes = remotes[offset:] + remotes[:offset]
    active_remotes = list(remotes)
    retired_remotes: dict[str, str] = {}
    remote_upload_counts = {remote: 0 for remote in remotes}
    remote_upload_bytes = {remote: 0 for remote in remotes}
    remote_byte_budget = parse_bytes(max_bytes_per_remote)
    if upload_mode not in {"stream", "staged"}:
        raise ValueError("upload_mode must be 'stream' or 'staged'")

    status_dir = STATE_MOUNT / "runs" / f"worker-{worker_index:03d}"
    status_dir.mkdir(parents=True, exist_ok=True)
    status_path = status_dir / f"{int(time.time())}.jsonl"

    processed = 0
    uploaded = 0
    skipped = 0
    deferred = 0
    failed = 0
    max_bytes = parse_bytes(max_package_bytes)
    warn_bytes = parse_bytes(warn_package_bytes)
    with status_path.open("w") as status_handle:
        for row in rows:
            row_index = int(row["index"])
            if not row_belongs_to_worker(row_index, len(rows), worker_index, worker_count, assignment_mode):
                continue
            if not active_remotes:
                break
            row_kind = row.get("kind", "package")
            source_rel_paths = row_source_paths(row)
            source_abs_paths = [SOURCE_MOUNT / clean_relative_path(path) for path in source_rel_paths]
            unit_rel = source_rel_paths[0]
            unit_abs = source_abs_paths[0]
            row_max_bytes = min(max_bytes, int(row.get("max_package_bytes", max_bytes) or max_bytes))
            row_warn_bytes = min(warn_bytes, row_max_bytes)
            row_planned_bytes = int(row.get("bytes", 0) or 0)
            eligible_remotes = [
                candidate
                for candidate in active_remotes
                if remote_byte_budget <= 0
                or remote_upload_bytes[candidate] + row_planned_bytes <= remote_byte_budget
            ]
            if not eligible_remotes:
                deferred += 1
                status_handle.write(
                    json.dumps(
                        {
                            "worker_index": worker_index,
                            "row_index": row_index,
                            "source_path": unit_rel,
                            "archive_dest": row["archive_dest"],
                            "bytes": row_planned_bytes,
                            "status": "deferred_remote_byte_budget",
                            "remote_byte_budget": remote_byte_budget,
                            "finished_at_unix": int(time.time()),
                        },
                        sort_keys=True,
                    )
                    + "\n"
                )
                status_handle.flush()
                processed += 1
                continue
            remote = eligible_remotes[processed % len(eligible_remotes)]
            archive_dest = row["archive_dest"]
            package_index_dest = row.get("package_index_dest") or row.get("index_dest") or f"{archive_dest}.package.index.json"
            files_index_dest = row.get("files_index_dest") or f"{archive_dest}.files.index.jsonl.zst"
            result: dict[str, Any] = {
                "worker_index": worker_index,
                "assignment_mode": assignment_mode,
                "row_index": row_index,
                "remote": remote,
                "source_path": unit_rel,
                "source_root_count": len(source_rel_paths),
                "archive_dest": archive_dest,
                "package_index_dest": package_index_dest,
                "files_index_dest": files_index_dest,
                "kind": row_kind,
                "dry_run": dry_run,
                "max_package_bytes": row_max_bytes,
                "remote_byte_budget": remote_byte_budget,
                "upload_mode": upload_mode,
                "started_at_unix": int(time.time()),
            }
            try:
                for source_abs in source_abs_paths:
                    ensure_inside(source_abs, SOURCE_MOUNT)
                if row_kind == "raw_file":
                    if len(source_abs_paths) != 1:
                        raise ValueError("raw_file rows must contain exactly one source path")
                    if not unit_abs.exists() or not unit_abs.is_file():
                        raise FileNotFoundError(f"missing raw source file: {unit_abs}")
                    result["bytes"] = row.get("bytes", unit_abs.stat().st_size)
                    if dry_run:
                        result["status"] = "planned"
                    else:
                        rclone_copyto(remote, row.get("dest_path", archive_dest), unit_abs)
                        result["status"] = "uploaded"
                        uploaded += 1
                        remote_upload_counts[remote] += 1
                        remote_upload_bytes[remote] += int(result["bytes"])
                        if max_uploads_per_remote > 0 and remote_upload_counts[remote] >= max_uploads_per_remote:
                            retired_remotes[remote] = f"max_uploads_per_remote={max_uploads_per_remote}"
                            active_remotes = [candidate for candidate in active_remotes if candidate != remote]
                    status_handle.write(json.dumps(result | {"finished_at_unix": int(time.time())}, sort_keys=True) + "\n")
                    status_handle.flush()
                    processed += 1
                    if limit > 0 and processed >= limit:
                        break
                    continue

                missing_paths = [str(path) for path in source_abs_paths if not path.exists() and not path.is_symlink()]
                if missing_paths:
                    raise FileNotFoundError(f"missing source package roots: {missing_paths}")
                if not dry_run:
                    existing_archive_bytes = rclone_remote_file_size(remote, archive_dest)
                    existing_package_index_bytes = rclone_remote_file_size(remote, package_index_dest)
                    existing_files_index_bytes = rclone_remote_file_size(remote, files_index_dest)
                    result["existing_archive_bytes"] = existing_archive_bytes
                    if (
                        existing_archive_bytes is not None
                        and existing_package_index_bytes is not None
                        and existing_files_index_bytes is not None
                    ):
                        result["status"] = "already_uploaded"
                        skipped += 1
                        result["finished_at_unix"] = int(time.time())
                        status_handle.write(json.dumps(result, sort_keys=True) + "\n")
                        status_handle.flush()
                        processed += 1
                        if limit > 0 and processed >= limit:
                            break
                        continue
                if dry_run:
                    row_bytes = row.get("bytes")
                    result["bytes"] = row_bytes
                    result["package_strategy"] = (
                        package_strategy(int(row_bytes), row_max_bytes, row_warn_bytes)
                        if row_bytes is not None
                        else "unknown"
                    )
                    result["status"] = "planned"
                else:
                    with tempfile.TemporaryDirectory() as tmp:
                        tmp_path = Path(tmp)
                        index_info = write_indexes_many(
                            source_abs_paths,
                            row.get("source_volume", SOURCE_VOLUME_NAME),
                            source_rel_paths,
                            archive_dest,
                            package_index_dest,
                            files_index_dest,
                            tmp_path,
                            row_max_bytes,
                            row_warn_bytes,
                        )
                        result.update(index_info)
                        if index_info["package_strategy"] == "split_required":
                            result["status"] = "skipped_split_required"
                            skipped += 1
                        elif upload_mode == "staged":
                            cache_dir = CACHE_MOUNT / "workers" / f"{worker_index:03d}" / f"row-{row_index:012d}"
                            archive_path = cache_dir / Path(archive_dest).name
                            ready_path = archive_path.with_suffix(archive_path.suffix + ".ready.json")
                            result["archive_staged_path"] = str(archive_path)
                            if archive_path.exists() and ready_path.exists():
                                ready = json.loads(ready_path.read_text())
                                if archive_path.stat().st_size != int(ready["archive_bytes"]):
                                    archive_path.unlink(missing_ok=True)
                                    ready_path.unlink(missing_ok=True)
                                    raise RuntimeError(f"staged archive size does not match ready marker: {archive_path}")
                                result.update(
                                    apply_archive_metadata(
                                        Path(index_info["package_index_path"]),
                                        int(ready["archive_bytes"]),
                                        str(ready["archive_sha256"]),
                                    )
                                )
                                result["reused_staged_archive"] = True
                            else:
                                archive_path.unlink(missing_ok=True)
                                ready_path.unlink(missing_ok=True)
                                create_archive_staged_many(
                                    source_rel_paths, archive_path, compression_level, compression_threads
                                )
                                result.update(
                                    finalize_package_index(Path(index_info["package_index_path"]), archive_path)
                                )
                                ready_path.write_text(
                                    json.dumps(
                                        {
                                            "archive_bytes": result["archive_bytes"],
                                            "archive_sha256": result["archive_sha256"],
                                            "archive_dest": archive_dest,
                                            "row_index": row_index,
                                        },
                                        indent=2,
                                        sort_keys=True,
                                    )
                                    + "\n"
                                )
                            if int(result["archive_bytes"]) > row_max_bytes:
                                archive_path.unlink(missing_ok=True)
                                ready_path.unlink(missing_ok=True)
                                result["status"] = "skipped_archive_exceeds_limit"
                                skipped += 1
                            else:
                                cache_volume.commit()
                                rclone_copyto(remote, archive_dest, archive_path)
                                rclone_rcat(remote, package_index_dest, Path(index_info["package_index_path"]))
                                rclone_rcat(remote, files_index_dest, Path(index_info["files_index_path"]))
                                archive_path.unlink(missing_ok=True)
                                ready_path.unlink(missing_ok=True)
                                cache_volume.commit()
                                result["status"] = "uploaded"
                                uploaded += 1
                                remote_upload_counts[remote] += 1
                                remote_upload_bytes[remote] += row_planned_bytes
                        else:
                            if result.get("existing_archive_bytes") is None:
                                upload_archive_stream_many(
                                    source_rel_paths,
                                    remote,
                                    archive_dest,
                                    compression_level,
                                    compression_threads,
                                )
                            rclone_rcat(remote, package_index_dest, Path(index_info["package_index_path"]))
                            rclone_rcat(remote, files_index_dest, Path(index_info["files_index_path"]))
                            result["status"] = (
                                "uploaded_missing_indexes"
                                if result.get("existing_archive_bytes") is not None
                                else "uploaded"
                            )
                            uploaded += 1
                            remote_upload_counts[remote] += 1
                            if result.get("existing_archive_bytes") is None:
                                remote_upload_bytes[remote] += row_planned_bytes
                        if max_uploads_per_remote > 0 and remote_upload_counts[remote] >= max_uploads_per_remote:
                            retired_remotes[remote] = f"max_uploads_per_remote={max_uploads_per_remote}"
                            active_remotes = [candidate for candidate in active_remotes if candidate != remote]
            except Exception as exc:  # noqa: BLE001 - worker status should record any package failure.
                result["status"] = "failed"
                result["error"] = repr(exc)
                failed += 1
                if is_drive_rate_limit_error(exc):
                    result["status"] = "remote_rate_limited"
                    retired_remotes[remote] = "drive_rate_limit"
                    active_remotes = [candidate for candidate in active_remotes if candidate != remote]
            result["finished_at_unix"] = int(time.time())
            status_handle.write(json.dumps(result, sort_keys=True) + "\n")
            status_handle.flush()
            processed += 1
            if limit > 0 and processed >= limit:
                break

    state_volume.commit()
    return {
        "worker_index": worker_index,
        "worker_count": worker_count,
        "assignment_mode": assignment_mode,
        "remotes": remotes,
        "active_remotes": active_remotes,
        "retired_remotes": retired_remotes,
        "remote_upload_counts": remote_upload_counts,
        "remote_upload_bytes": remote_upload_bytes,
        "max_bytes_per_remote": remote_byte_budget,
        "max_uploads_per_remote": max_uploads_per_remote,
        "processed": processed,
        "uploaded": uploaded,
        "skipped": skipped,
        "deferred": deferred,
        "failed": failed,
        "dry_run": dry_run,
        "status_path": str(status_path),
    }


@app.function(
    image=image,
    volumes={str(STATE_MOUNT): state_volume},
    timeout=env_int("MODAL_COORDINATOR_TIMEOUT", 900),
    cpu=float(os.environ.get("MODAL_COORDINATOR_CPU", "0.25")),
    memory=env_int("MODAL_COORDINATOR_MEMORY", 512),
    max_containers=1,
)
def submit_prepare_workers(
    plan_path: str,
    worker_count: int = 10,
    assignment_mode: str = "modulo",
    spool_name: str = "",
    compression_level: int = 3,
    compression_threads: int = 2,
    cache_commit_every: int = 1,
    max_package_bytes: str = "200GiB",
    warn_package_bytes: str = "180GiB",
    dry_run: bool = False,
    limit: int = 0,
    run_id: str = "",
) -> dict[str, Any]:
    if worker_count < 1:
        raise ValueError("worker_count must be >= 1")
    effective_run_id = run_id or f"prepare-{int(time.time())}"
    calls = [
        prepare_archives_worker.spawn(
            worker_index=index,
            worker_count=worker_count,
            plan_path=plan_path,
            assignment_mode=assignment_mode,
            spool_name=spool_name,
            dry_run=dry_run,
            limit=limit,
            compression_level=compression_level,
            compression_threads=compression_threads,
            cache_commit_every=cache_commit_every,
            max_package_bytes=max_package_bytes,
            warn_package_bytes=warn_package_bytes,
        )
        for index in range(worker_count)
    ]
    manifest = {
        "schema": "shared-drive-migration/modal-prepare-submission/v1",
        "run_id": effective_run_id,
        "submitted_at_unix": int(time.time()),
        "plan_path": plan_path,
        "spool_name": spool_name or default_spool_name(plan_path),
        "worker_count": worker_count,
        "assignment_mode": assignment_mode,
        "compression_level": compression_level,
        "compression_threads": compression_threads,
        "cache_commit_every": cache_commit_every,
        "max_package_bytes": max_package_bytes,
        "warn_package_bytes": warn_package_bytes,
        "dry_run": dry_run,
        "limit": limit,
        "worker_calls": [
            {"worker_index": index, "function_call_id": call.object_id}
            for index, call in enumerate(calls)
        ],
        "status": "submitted",
    }
    manifest_path = STATE_MOUNT / "runs" / "coordinators" / f"{effective_run_id}.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    state_volume.commit()
    return manifest | {"manifest_path": str(manifest_path)}


@app.function(
    image=image,
    volumes={str(STATE_MOUNT): state_volume},
    timeout=env_int("MODAL_COORDINATOR_TIMEOUT", 900),
    cpu=float(os.environ.get("MODAL_COORDINATOR_CPU", "0.25")),
    memory=env_int("MODAL_COORDINATOR_MEMORY", 512),
    max_containers=1,
)
def submit_migration_workers(
    plan_path: str,
    worker_count: int = 10,
    remote_group_size: int = 10,
    assignment_mode: str = "modulo",
    compression_level: int = 3,
    compression_threads: int = 2,
    upload_mode: str = "staged",
    max_package_bytes: str = "200GiB",
    warn_package_bytes: str = "180GiB",
    max_bytes_per_remote: str = "700GiB",
    remote_start_offset: int = 0,
    dry_run: bool = False,
    limit: int = 0,
    run_id: str = "",
) -> dict[str, Any]:
    if worker_count < 1 or remote_group_size < 1:
        raise ValueError("worker_count and remote_group_size must be >= 1")
    if upload_mode not in {"stream", "staged"}:
        raise ValueError("upload_mode must be 'stream' or 'staged'")
    effective_run_id = run_id or f"migration-{int(time.time())}"
    calls = [
        upload_worker.spawn(
            worker_index=index,
            worker_count=worker_count,
            plan_path=plan_path,
            remote_group_size=remote_group_size,
            assignment_mode=assignment_mode,
            dry_run=dry_run,
            limit=limit,
            compression_level=compression_level,
            compression_threads=compression_threads,
            upload_mode=upload_mode,
            max_package_bytes=max_package_bytes,
            warn_package_bytes=warn_package_bytes,
            max_uploads_per_remote=0,
            max_bytes_per_remote=max_bytes_per_remote,
            remote_start_offset=remote_start_offset,
        )
        for index in range(worker_count)
    ]
    manifest = {
        "schema": "shared-drive-migration/modal-migration-submission/v1",
        "run_id": effective_run_id,
        "submitted_at_unix": int(time.time()),
        "plan_path": plan_path,
        "worker_count": worker_count,
        "remote_group_size": remote_group_size,
        "assignment_mode": assignment_mode,
        "compression_level": compression_level,
        "compression_threads": compression_threads,
        "upload_mode": upload_mode,
        "max_package_bytes": max_package_bytes,
        "warn_package_bytes": warn_package_bytes,
        "max_bytes_per_remote": max_bytes_per_remote,
        "remote_start_offset": remote_start_offset,
        "dry_run": dry_run,
        "limit": limit,
        "worker_remote_ranges": [
            {
                "worker_index": index,
                "remote_start": (index * remote_group_size) + 1,
                "remote_end": (index + 1) * remote_group_size,
            }
            for index in range(worker_count)
        ],
        "worker_calls": [
            {"worker_index": index, "function_call_id": call.object_id}
            for index, call in enumerate(calls)
        ],
        "status": "submitted",
    }
    manifest_path = STATE_MOUNT / "runs" / "coordinators" / f"{effective_run_id}.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    state_volume.commit()
    return manifest | {"manifest_path": str(manifest_path)}


@app.function(
    image=image,
    volumes={str(STATE_MOUNT): state_volume},
    timeout=env_int("MODAL_COORDINATOR_TIMEOUT", 43200),
    cpu=float(os.environ.get("MODAL_COORDINATOR_CPU", "0.25")),
    memory=env_int("MODAL_COORDINATOR_MEMORY", 512),
    max_containers=1,
)
def plan_resume_and_submit(
    inventory_path: str,
    source_volume_name: str,
    completed_manifest_path: str,
    plan_path: str,
    dest_prefix: str,
    worker_count: int = 10,
    remote_group_size: int = 10,
    assignment_mode: str = "modulo",
    compression_level: int = 3,
    compression_threads: int = 2,
    max_package_bytes: str = "200GiB",
    max_archive_entries: int = 100000,
    max_roots_per_archive: int = 1000,
    shared_drive_item_limit: int = 400000,
    max_bytes_per_remote: str = "650GiB",
    remote_start_offset: int = 0,
    run_id: str = "",
) -> dict[str, Any]:
    """Build a collision-free resume plan, then submit workers from Modal."""
    plan = write_sane_archive_plan.remote(
        inventory_path=inventory_path,
        source_volume_name=source_volume_name,
        source_prefix="",
        dest_prefix=dest_prefix,
        completed_manifest_path=completed_manifest_path,
        plan_path=plan_path,
        max_package_bytes=max_package_bytes,
        max_archive_entries=max_archive_entries,
        max_roots_per_archive=max_roots_per_archive,
        shared_drive_item_limit=shared_drive_item_limit,
    )
    if not bool(plan.get("plan_complete")):
        raise RuntimeError("resume plan is incomplete; workers were not submitted")
    submission = submit_migration_workers.remote(
        plan_path=plan_path,
        worker_count=worker_count,
        remote_group_size=remote_group_size,
        assignment_mode=assignment_mode,
        compression_level=compression_level,
        compression_threads=compression_threads,
        upload_mode="staged",
        max_package_bytes=max_package_bytes,
        warn_package_bytes=max_package_bytes,
        max_bytes_per_remote=max_bytes_per_remote,
        remote_start_offset=remote_start_offset,
        dry_run=False,
        run_id=run_id,
    )
    return {"plan": plan, "submission": submission}


@app.function(
    image=image,
    volumes={
        str(SOURCE_MOUNT): source_volume,
        str(STATE_MOUNT): state_volume,
        str(CACHE_MOUNT): cache_volume,
    },
    timeout=env_int("MODAL_TIMEOUT", 43200),
    cpu=float(os.environ.get("MODAL_CPU", "2")),
    memory=env_int("MODAL_MEMORY", 4096),
    max_containers=env_int("MODAL_MAX_CONTAINERS", 10),
    retries=1,
)
def prepare_archives_worker(
    worker_index: int,
    worker_count: int = 10,
    plan_path: str = "plans/modal-volume-units.jsonl",
    assignment_mode: str = "contiguous",
    spool_name: str = "",
    dry_run: bool = True,
    limit: int = 0,
    compression_level: int = 3,
    compression_threads: int = 2,
    cache_commit_every: int = 1,
    max_package_bytes: str = "200GiB",
    warn_package_bytes: str = "180GiB",
) -> dict[str, Any]:
    rows = load_plan(plan_path)
    spool_root = spool_base(spool_name, plan_path)
    status_dir = STATE_MOUNT / "runs" / f"prepare-worker-{worker_index:03d}"
    status_dir.mkdir(parents=True, exist_ok=True)
    status_path = status_dir / f"{int(time.time())}.jsonl"

    processed = 0
    prepared = 0
    skipped = 0
    failed = 0
    uncommitted = 0
    max_bytes = parse_bytes(max_package_bytes)
    warn_bytes = parse_bytes(warn_package_bytes)
    commit_every = max(1, int(cache_commit_every))

    with status_path.open("w") as status_handle:
        for row in rows:
            row_index = int(row["index"])
            if not row_belongs_to_worker(row_index, len(rows), worker_index, worker_count, assignment_mode):
                continue
            row_kind = row.get("kind", "package")
            source_rel_paths = row_source_paths(row)
            source_abs_paths = [SOURCE_MOUNT / clean_relative_path(path) for path in source_rel_paths]
            unit_rel = source_rel_paths[0]
            unit_abs = source_abs_paths[0]
            row_max_bytes = min(max_bytes, int(row.get("max_package_bytes", max_bytes) or max_bytes))
            row_warn_bytes = min(warn_bytes, row_max_bytes)
            archive_dest = row["archive_dest"]
            package_index_dest = row.get("package_index_dest") or row.get("index_dest") or f"{archive_dest}.package.index.json"
            files_index_dest = row.get("files_index_dest") or f"{archive_dest}.files.index.jsonl.zst"
            archive_path = spool_file_path(spool_root, archive_dest)
            package_index_path = spool_file_path(spool_root, package_index_dest)
            files_index_path = spool_file_path(spool_root, files_index_dest)
            manifest_path = archive_path.with_suffix(archive_path.suffix + ".spool.json")
            result: dict[str, Any] = {
                "worker_index": worker_index,
                "worker_count": worker_count,
                "assignment_mode": assignment_mode,
                "row_index": row_index,
                "source_path": unit_rel,
                "source_root_count": len(source_rel_paths),
                "archive_dest": archive_dest,
                "package_index_dest": package_index_dest,
                "files_index_dest": files_index_dest,
                "spool_archive_path": str(archive_path),
                "spool_package_index_path": str(package_index_path),
                "spool_files_index_path": str(files_index_path),
                "spool_manifest_path": str(manifest_path),
                "kind": row_kind,
                "dry_run": dry_run,
                "max_package_bytes": row_max_bytes,
                "started_at_unix": int(time.time()),
            }
            try:
                for source_abs in source_abs_paths:
                    ensure_inside(source_abs, SOURCE_MOUNT)
                if row_kind == "raw_file":
                    if len(source_abs_paths) != 1:
                        raise ValueError("raw_file rows must contain exactly one source path")
                    if not unit_abs.exists() or not unit_abs.is_file():
                        raise FileNotFoundError(f"missing raw source file: {unit_abs}")
                    result["bytes"] = unit_abs.stat().st_size
                    if archive_path.exists():
                        result["status"] = "already_prepared"
                        skipped += 1
                    elif dry_run:
                        result["status"] = "planned"
                    else:
                        archive_path.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(unit_abs, archive_path)
                        manifest_path.write_text(json.dumps(result | {"status": "prepared"}, indent=2, sort_keys=True) + "\n")
                        prepared += 1
                        uncommitted += 1
                        if uncommitted >= commit_every:
                            cache_volume.commit()
                            uncommitted = 0
                        result["status"] = "prepared"
                    status_handle.write(json.dumps(result | {"finished_at_unix": int(time.time())}, sort_keys=True) + "\n")
                    status_handle.flush()
                    processed += 1
                    if limit > 0 and processed >= limit:
                        break
                    continue

                missing_paths = [str(path) for path in source_abs_paths if not path.exists() and not path.is_symlink()]
                if missing_paths:
                    raise FileNotFoundError(f"missing source package roots: {missing_paths}")
                if archive_path.exists() and package_index_path.exists() and files_index_path.exists():
                    result["status"] = "already_prepared"
                    skipped += 1
                elif dry_run:
                    result["status"] = "planned"
                else:
                    with tempfile.TemporaryDirectory() as tmp:
                        tmp_path = Path(tmp)
                        index_info = write_indexes_many(
                            source_abs_paths,
                            row.get("source_volume", SOURCE_VOLUME_NAME),
                            source_rel_paths,
                            archive_dest,
                            package_index_dest,
                            files_index_dest,
                            tmp_path,
                            row_max_bytes,
                            row_warn_bytes,
                        )
                        result.update(index_info)
                        if index_info["package_strategy"] == "split_required":
                            result["status"] = "skipped_split_required"
                            skipped += 1
                        else:
                            create_archive_staged_many(
                                source_rel_paths, archive_path, compression_level, compression_threads
                            )
                            result.update(
                                finalize_package_index(Path(index_info["package_index_path"]), archive_path)
                            )
                            if int(result["archive_bytes"]) > row_max_bytes:
                                archive_path.unlink(missing_ok=True)
                                result["status"] = "skipped_archive_exceeds_limit"
                                skipped += 1
                            else:
                                package_index_path.parent.mkdir(parents=True, exist_ok=True)
                                files_index_path.parent.mkdir(parents=True, exist_ok=True)
                                shutil.copy2(index_info["package_index_path"], package_index_path)
                                shutil.copy2(index_info["files_index_path"], files_index_path)
                                manifest_path.write_text(
                                    json.dumps(result | {"status": "prepared"}, indent=2, sort_keys=True) + "\n"
                                )
                                prepared += 1
                                result["status"] = "prepared"
                                uncommitted += 1
                                if uncommitted >= commit_every:
                                    cache_volume.commit()
                                    uncommitted = 0
            except Exception as exc:  # noqa: BLE001 - keep per-package resume data.
                result["status"] = "failed"
                result["error"] = repr(exc)
                failed += 1
            result["finished_at_unix"] = int(time.time())
            status_handle.write(json.dumps(result, sort_keys=True) + "\n")
            status_handle.flush()
            processed += 1
            if limit > 0 and processed >= limit:
                break

    state_volume.commit()
    cache_volume.commit()
    return {
        "worker_index": worker_index,
        "worker_count": worker_count,
        "assignment_mode": assignment_mode,
        "spool_root": str(spool_root),
        "processed": processed,
        "prepared": prepared,
        "skipped": skipped,
        "failed": failed,
        "dry_run": dry_run,
        "cache_commit_every": commit_every,
        "status_path": str(status_path),
    }


@app.function(
    image=image,
    volumes={
        str(CREDS_MOUNT): creds_volume,
        str(STATE_MOUNT): state_volume,
        str(CACHE_MOUNT): cache_volume,
    },
    timeout=env_int("MODAL_TIMEOUT", 43200),
    cpu=float(os.environ.get("MODAL_CPU", "2")),
    memory=env_int("MODAL_MEMORY", 4096),
    max_containers=env_int("MODAL_MAX_CONTAINERS", 10),
    retries=1,
)
def upload_prepared_worker(
    worker_index: int,
    worker_count: int = 10,
    plan_path: str = "plans/modal-volume-units.jsonl",
    remote_group_size: int = 10,
    assignment_mode: str = "contiguous",
    spool_name: str = "",
    dry_run: bool = True,
    limit: int = 0,
    max_uploads_per_remote: int = MODAL_MAX_UPLOADS_PER_REMOTE,
) -> dict[str, Any]:
    rows = load_plan(plan_path)
    remotes = load_remotes(worker_index, remote_group_size)
    active_remotes = list(remotes)
    retired_remotes: dict[str, str] = {}
    remote_upload_counts = {remote: 0 for remote in remotes}
    spool_root = spool_base(spool_name, plan_path)
    status_dir = STATE_MOUNT / "runs" / f"upload-prepared-worker-{worker_index:03d}"
    status_dir.mkdir(parents=True, exist_ok=True)
    status_path = status_dir / f"{int(time.time())}.jsonl"

    processed = 0
    uploaded = 0
    skipped = 0
    failed = 0
    with status_path.open("w") as status_handle:
        for row in rows:
            row_index = int(row["index"])
            if not row_belongs_to_worker(row_index, len(rows), worker_index, worker_count, assignment_mode):
                continue
            if not active_remotes:
                break
            remote = active_remotes[processed % len(active_remotes)]
            row_kind = row.get("kind", "package")
            archive_dest = row["archive_dest"]
            package_index_dest = row.get("package_index_dest") or row.get("index_dest") or f"{archive_dest}.package.index.json"
            files_index_dest = row.get("files_index_dest") or f"{archive_dest}.files.index.jsonl.zst"
            archive_path = spool_file_path(spool_root, archive_dest)
            package_index_path = spool_file_path(spool_root, package_index_dest)
            files_index_path = spool_file_path(spool_root, files_index_dest)
            result: dict[str, Any] = {
                "worker_index": worker_index,
                "worker_count": worker_count,
                "assignment_mode": assignment_mode,
                "row_index": row_index,
                "remote": remote,
                "archive_dest": archive_dest,
                "package_index_dest": package_index_dest,
                "files_index_dest": files_index_dest,
                "spool_archive_path": str(archive_path),
                "spool_package_index_path": str(package_index_path),
                "spool_files_index_path": str(files_index_path),
                "kind": row_kind,
                "dry_run": dry_run,
                "started_at_unix": int(time.time()),
            }
            try:
                if not archive_path.exists():
                    result["status"] = "missing_prepared_archive"
                    skipped += 1
                elif row_kind != "raw_file" and (not package_index_path.exists() or not files_index_path.exists()):
                    result["status"] = "missing_prepared_indexes"
                    skipped += 1
                elif dry_run:
                    result["status"] = "planned"
                else:
                    rclone_copyto(remote, archive_dest, archive_path)
                    if row_kind != "raw_file":
                        rclone_copyto(remote, package_index_dest, package_index_path)
                        rclone_copyto(remote, files_index_dest, files_index_path)
                        package_index_path.unlink(missing_ok=True)
                        files_index_path.unlink(missing_ok=True)
                    archive_path.unlink(missing_ok=True)
                    uploaded += 1
                    remote_upload_counts[remote] += 1
                    result["status"] = "uploaded_and_removed_from_spool"
                    if max_uploads_per_remote > 0 and remote_upload_counts[remote] >= max_uploads_per_remote:
                        retired_remotes[remote] = f"max_uploads_per_remote={max_uploads_per_remote}"
                        active_remotes = [candidate for candidate in active_remotes if candidate != remote]
                    cache_volume.commit()
            except Exception as exc:  # noqa: BLE001 - keep per-package resume data.
                result["status"] = "failed"
                result["error"] = repr(exc)
                failed += 1
                if is_drive_rate_limit_error(exc):
                    result["status"] = "remote_rate_limited"
                    retired_remotes[remote] = "drive_rate_limit"
                    active_remotes = [candidate for candidate in active_remotes if candidate != remote]
            result["finished_at_unix"] = int(time.time())
            status_handle.write(json.dumps(result, sort_keys=True) + "\n")
            status_handle.flush()
            processed += 1
            if limit > 0 and processed >= limit:
                break

    state_volume.commit()
    cache_volume.commit()
    return {
        "worker_index": worker_index,
        "worker_count": worker_count,
        "assignment_mode": assignment_mode,
        "spool_root": str(spool_root),
        "remotes": remotes,
        "active_remotes": active_remotes,
        "retired_remotes": retired_remotes,
        "remote_upload_counts": remote_upload_counts,
        "max_uploads_per_remote": max_uploads_per_remote,
        "processed": processed,
        "uploaded": uploaded,
        "skipped": skipped,
        "failed": failed,
        "dry_run": dry_run,
        "status_path": str(status_path),
    }


@app.function(
    image=image,
    volumes={str(STATE_MOUNT): state_volume},
    timeout=env_int("MODAL_COORDINATOR_TIMEOUT", 900),
    cpu=float(os.environ.get("MODAL_COORDINATOR_CPU", "0.25")),
    memory=env_int("MODAL_COORDINATOR_MEMORY", 512),
    max_containers=1,
)
def submit_upload_prepared_workers(
    plan_path: str,
    worker_count: int = 10,
    remote_group_size: int = 10,
    assignment_mode: str = "modulo",
    spool_name: str = "",
    max_uploads_per_remote: int = 0,
    dry_run: bool = False,
    limit: int = 0,
    run_id: str = "",
) -> dict[str, Any]:
    if worker_count < 1 or remote_group_size < 1:
        raise ValueError("worker_count and remote_group_size must be >= 1")
    effective_run_id = run_id or f"upload-{int(time.time())}"
    calls = [
        upload_prepared_worker.spawn(
            worker_index=index,
            worker_count=worker_count,
            plan_path=plan_path,
            remote_group_size=remote_group_size,
            assignment_mode=assignment_mode,
            spool_name=spool_name,
            dry_run=dry_run,
            limit=limit,
            max_uploads_per_remote=max_uploads_per_remote,
        )
        for index in range(worker_count)
    ]
    manifest = {
        "schema": "shared-drive-migration/modal-upload-submission/v1",
        "run_id": effective_run_id,
        "submitted_at_unix": int(time.time()),
        "plan_path": plan_path,
        "spool_name": spool_name or default_spool_name(plan_path),
        "worker_count": worker_count,
        "remote_group_size": remote_group_size,
        "assignment_mode": assignment_mode,
        "max_uploads_per_remote": max_uploads_per_remote,
        "dry_run": dry_run,
        "limit": limit,
        "worker_remote_ranges": [
            {
                "worker_index": index,
                "remote_start": (index * remote_group_size) + 1,
                "remote_end": (index + 1) * remote_group_size,
            }
            for index in range(worker_count)
        ],
        "worker_calls": [
            {"worker_index": index, "function_call_id": call.object_id}
            for index, call in enumerate(calls)
        ],
        "status": "submitted",
    }
    manifest_path = STATE_MOUNT / "runs" / "coordinators" / f"{effective_run_id}.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    state_volume.commit()
    return manifest | {"manifest_path": str(manifest_path)}


@app.local_entrypoint()
def main(
    command: str = "discover",
    source_prefix: str = "",
    unit_depth: int = 2,
    top_depth: int = 1,
    fallback_depth: int = 2,
    max_children_per_top_unit: int = env_int("MODAL_MAX_CHILDREN_PER_TOP_UNIT", 1000),
    shared_drive_item_limit: int = env_int("MODAL_SHARED_DRIVE_ITEM_LIMIT", 400000),
    max_archive_entries: int = env_int("MODAL_MAX_ARCHIVE_ENTRIES", 100000),
    max_roots_per_archive: int = env_int("MODAL_MAX_ROOTS_PER_ARCHIVE", 1000),
    dest_prefix: str = "",
    plan_path: str = "plans/modal-volume-units.jsonl",
    inventory_path: str = "",
    completed_manifest_path: str = "",
    worker_count: int = 10,
    remote_group_size: int = 10,
    assignment_mode: str = os.environ.get("MODAL_ASSIGNMENT_MODE", "contiguous"),
    spool_name: str = os.environ.get("MODAL_SPOOL_NAME", ""),
    dry_run: bool = True,
    limit: int = 0,
    include_stats: bool = False,
    compression_level: int = env_int("MODAL_COMPRESSION_LEVEL", 3),
    compression_threads: int = env_int("MODAL_COMPRESSION_THREADS", 2),
    cache_commit_every: int = env_int("MODAL_CACHE_COMMIT_EVERY", 1),
    run_id: str = "",
    worker_index: int = -1,
    upload_mode: str = os.environ.get("MODAL_UPLOAD_MODE", "staged"),
    max_package_bytes: str = os.environ.get("MODAL_MAX_PACKAGE_BYTES", "200GiB"),
    warn_package_bytes: str = os.environ.get("MODAL_WARN_PACKAGE_BYTES", "180GiB"),
    max_uploads_per_remote: int = MODAL_MAX_UPLOADS_PER_REMOTE,
    max_bytes_per_remote: str = os.environ.get("MODAL_MAX_BYTES_PER_REMOTE", "0"),
    allow_unsafe_delete: bool = False,
    shard_index: int = -1,
):
    if command == "inspect-metadata":
        result = inspect_volume_metadata.remote(
            source_volume_name=SOURCE_VOLUME_NAME,
            paths_csv=source_prefix,
            limit=limit,
            recursive=include_stats,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return

    if command in {"rclone-lsjson", "lsjson"}:
        result = write_rclone_lsjson_inventory.remote(
            source_prefix=source_prefix,
            output_path=plan_path,
            files_only=include_stats,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return

    if command in {"find-json-fast", "find-json", "fast-json"}:
        result = write_find_json_inventory.remote(
            source_prefix=source_prefix,
            source_volume_name=SOURCE_VOLUME_NAME,
            output_path=plan_path,
            threads=worker_count,
            limit=limit,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return

    if command in {"find-json-split", "find-json-split-prefix"}:
        result = write_find_json_split_prefix.remote(
            source_prefix=source_prefix,
            output_path=plan_path,
            shard_index=shard_index,
            threads=worker_count,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return

    if command in {"analyze-find-json", "analyze-drive-items", "analyze-item-limit"}:
        result = analyze_find_json_inventory.remote(
            inventory_path=inventory_path or plan_path,
            source_prefix=source_prefix,
            top_depth=top_depth,
            fallback_depth=fallback_depth,
            dest_prefix=dest_prefix,
            max_package_bytes=max_package_bytes,
            shared_drive_item_limit=shared_drive_item_limit,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return

    if command in {"plan-sane-archives", "plan-sane", "sane-zip-plan"}:
        if not inventory_path:
            raise ValueError("--inventory-path must point to the completed find-json inventory")
        result = write_sane_archive_plan.remote(
            inventory_path=inventory_path,
            source_volume_name=SOURCE_VOLUME_NAME,
            source_prefix=source_prefix,
            dest_prefix=dest_prefix,
            completed_manifest_path=completed_manifest_path,
            plan_path=plan_path,
            max_package_bytes=max_package_bytes,
            max_archive_entries=max_archive_entries,
            max_roots_per_archive=max_roots_per_archive,
            shared_drive_item_limit=shared_drive_item_limit,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return

    if command in {"audit-drive-packages", "audit-drive", "resume-audit"}:
        if not dest_prefix:
            raise ValueError("--dest-prefix is required for audit-drive-packages")
        if not completed_manifest_path:
            raise ValueError("--completed-manifest-path is required for audit-drive-packages")
        result = audit_drive_packages.remote(
            dest_prefix=dest_prefix,
            manifest_path=completed_manifest_path,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return

    if command in {"cleanup-incomplete-drive-packages", "cleanup-incomplete-packages"}:
        if not completed_manifest_path:
            raise ValueError("--completed-manifest-path is required for cleanup-incomplete-drive-packages")
        result = cleanup_incomplete_drive_packages.remote(
            manifest_path=completed_manifest_path,
            dry_run=dry_run,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return

    if command == "discover":
        result = write_api_plan_to_state(
            source_volume_name=SOURCE_VOLUME_NAME,
            source_prefix=source_prefix,
            unit_depth=unit_depth,
            dest_prefix=dest_prefix,
            plan_path=plan_path,
            limit=limit,
            max_package_bytes=max_package_bytes,
            warn_package_bytes=warn_package_bytes,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return

    if command == "discover-mounted":
        result = discover_units.remote(
            source_prefix=source_prefix,
            source_volume_name=SOURCE_VOLUME_NAME,
            unit_depth=unit_depth,
            dest_prefix=dest_prefix,
            plan_path=plan_path,
            limit=limit,
            include_stats=include_stats,
            max_package_bytes=max_package_bytes,
            warn_package_bytes=warn_package_bytes,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return

    if command == "discover-mounted-fast":
        result = discover_units_mounted_find.remote(
            source_prefix=source_prefix,
            source_volume_name=SOURCE_VOLUME_NAME,
            unit_depth=unit_depth,
            dest_prefix=dest_prefix,
            plan_path=plan_path,
            max_package_bytes=max_package_bytes,
            warn_package_bytes=warn_package_bytes,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return

    if command == "discover-api-stream":
        result = write_api_stream_plan_to_state(
            source_volume_name=SOURCE_VOLUME_NAME,
            source_prefix=source_prefix,
            unit_depth=unit_depth,
            dest_prefix=dest_prefix,
            plan_path=plan_path,
            max_package_bytes=max_package_bytes,
            warn_package_bytes=warn_package_bytes,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return

    if command == "discover-structure":
        result = write_structure_plan_to_state(
            source_volume_name=SOURCE_VOLUME_NAME,
            source_prefix=source_prefix,
            unit_depth=unit_depth,
            dest_prefix=dest_prefix,
            plan_path=plan_path,
            max_package_bytes=max_package_bytes,
            warn_package_bytes=warn_package_bytes,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return

    if command in {"discover-hybrid-structure", "discover-hybrid", "plan-hybrid"}:
        result = write_hybrid_structure_plan_to_state(
            source_volume_name=SOURCE_VOLUME_NAME,
            source_prefix=source_prefix,
            top_depth=top_depth,
            fallback_depth=fallback_depth,
            max_children_per_top_unit=max_children_per_top_unit,
            dest_prefix=dest_prefix,
            plan_path=plan_path,
            max_package_bytes=max_package_bytes,
            warn_package_bytes=warn_package_bytes,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return

    if command in {"discover-hybrid-inventory", "plan-hybrid-inventory"}:
        result = write_hybrid_inventory_plan.remote(
            source_volume_name=SOURCE_VOLUME_NAME,
            source_prefix=source_prefix,
            top_depth=top_depth,
            fallback_depth=fallback_depth,
            dest_prefix=dest_prefix,
            plan_path=plan_path,
            inventory_path=inventory_path,
            max_package_bytes=max_package_bytes,
            warn_package_bytes=warn_package_bytes,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return

    if command == "upload":
        worker_indexes = [worker_index] if worker_index >= 0 else list(range(worker_count))
        calls = [
            upload_worker.spawn(
                worker_index=index,
                worker_count=worker_count,
                plan_path=plan_path,
                remote_group_size=remote_group_size,
                assignment_mode=assignment_mode,
                dry_run=dry_run,
                limit=limit,
                compression_level=compression_level,
                compression_threads=compression_threads,
                upload_mode=upload_mode,
                max_package_bytes=max_package_bytes,
                warn_package_bytes=warn_package_bytes,
                max_uploads_per_remote=max_uploads_per_remote,
                max_bytes_per_remote=max_bytes_per_remote,
            )
            for index in worker_indexes
        ]
        results = [call.get() for call in calls]
        print(json.dumps(results, indent=2, sort_keys=True))
        return

    if command in {"prepare-archives", "zip", "zip-mode"}:
        worker_indexes = [worker_index] if worker_index >= 0 else list(range(worker_count))
        calls = [
            prepare_archives_worker.spawn(
                worker_index=index,
                worker_count=worker_count,
                plan_path=plan_path,
                assignment_mode=assignment_mode,
                spool_name=spool_name,
                dry_run=dry_run,
                limit=limit,
                compression_level=compression_level,
                compression_threads=compression_threads,
                cache_commit_every=cache_commit_every,
                max_package_bytes=max_package_bytes,
                warn_package_bytes=warn_package_bytes,
            )
            for index in worker_indexes
        ]
        results = [call.get() for call in calls]
        print(json.dumps(results, indent=2, sort_keys=True))
        return

    if command == "upload-prepared":
        worker_indexes = [worker_index] if worker_index >= 0 else list(range(worker_count))
        calls = [
            upload_prepared_worker.spawn(
                worker_index=index,
                worker_count=worker_count,
                plan_path=plan_path,
                remote_group_size=remote_group_size,
                assignment_mode=assignment_mode,
                spool_name=spool_name,
                dry_run=dry_run,
                limit=limit,
                max_uploads_per_remote=max_uploads_per_remote,
            )
            for index in worker_indexes
        ]
        results = [call.get() for call in calls]
        print(json.dumps(results, indent=2, sort_keys=True))
        return

    if command == "smoke":
        smoke_run_id = run_id or str(int(time.time()))
        worker_indexes = [worker_index] if worker_index >= 0 else list(range(worker_count))
        calls = [
            smoke_worker.spawn(
                worker_index=index,
                run_id=smoke_run_id,
                dest_prefix=dest_prefix or "_sdmig_smoke",
                remote_group_size=remote_group_size,
                dry_run=dry_run,
                compression_level=compression_level,
            )
            for index in worker_indexes
        ]
        results = [call.get() for call in calls]
        print(json.dumps({"run_id": smoke_run_id, "results": results}, indent=2, sort_keys=True))
        return

    if command == "cleanup":
        result = cleanup_remote_prefix.remote(
            dest_prefix=dest_prefix,
            dry_run=dry_run,
            allow_unsafe_delete=allow_unsafe_delete,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return

    raise ValueError(f"unknown command: {command}")
