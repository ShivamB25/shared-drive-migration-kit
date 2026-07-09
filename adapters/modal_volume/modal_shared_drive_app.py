from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import hashlib
import json
import os
import shutil
import shlex
import subprocess
import tempfile
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import modal


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
RCLONE_DRIVE_STOP_ON_UPLOAD_LIMIT = os.environ.get("RCLONE_DRIVE_STOP_ON_UPLOAD_LIMIT", "1")
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


def parse_bytes(value: str | int) -> int:
    if isinstance(value, int):
        return value
    text = value.strip()
    if not text:
        raise ValueError("empty byte size")
    units = {
        "b": 1,
        "kb": 1000,
        "mb": 1000**2,
        "gb": 1000**3,
        "tb": 1000**4,
        "kib": 1024,
        "mib": 1024**2,
        "gib": 1024**3,
        "tib": 1024**4,
    }
    number = ""
    suffix = ""
    for char in text:
        if char.isdigit() or char == ".":
            number += char
        elif not char.isspace():
            suffix += char.lower()
    if not number:
        raise ValueError(f"invalid byte size: {value}")
    suffix = suffix or "b"
    if suffix not in units:
        raise ValueError(f"unknown byte unit in {value}")
    return int(float(number) * units[suffix])


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
)

source_volume = modal.Volume.from_name(SOURCE_VOLUME_NAME).with_mount_options(read_only=True)
creds_volume = modal.Volume.from_name(CREDS_VOLUME_NAME).with_mount_options(read_only=True)
state_volume = modal.Volume.from_name(STATE_VOLUME_NAME, create_if_missing=True)
cache_volume = modal.Volume.from_name(CACHE_VOLUME_NAME)


def clean_relative_path(value: str) -> Path:
    value = value.strip().strip("/")
    if not value:
        return Path()
    path = Path(value)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError(f"path must be relative and stay inside the volume: {value}")
    return path


def ensure_inside(path: Path, root: Path) -> None:
    path.resolve().relative_to(root.resolve())


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


def posix_join(*parts: str) -> str:
    return "/".join(part.strip("/") for part in parts if part and part.strip("/"))


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


def relative_path_under_prefix(path: str, prefix: str) -> str:
    clean_path = path.strip("/")
    clean_prefix = prefix.strip("/")
    if not clean_prefix:
        return clean_path
    if clean_path == clean_prefix:
        return ""
    prefix_with_slash = f"{clean_prefix}/"
    if clean_path.startswith(prefix_with_slash):
        return clean_path[len(prefix_with_slash) :]
    return clean_path


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


def iter_find_records(find_root: Path) -> Iterator[tuple[str, int, str, str]]:
    command = ["find", str(find_root), "-mindepth", "1", "-printf", "%y\\0%s\\0%T@\\0%P\\0"]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert process.stdout is not None

    buffer = b""
    fields: list[bytes] = []
    while True:
        chunk = process.stdout.read(1024 * 1024)
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
    max_package_bytes: str = "700GiB",
    warn_package_bytes: str = "650GiB",
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
    output.parent.mkdir(parents=True, exist_ok=True)
    inventory.parent.mkdir(parents=True, exist_ok=True)

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
    with tempfile.TemporaryDirectory() as tmp:
        shard_dir = Path(tmp) / "mounted-find-shards"
        shard_dir.mkdir()
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {}
            for index, scan_dir in enumerate(scan_dirs):
                scan_source = posix_join(source, scan_dir.relative_to(find_root).as_posix()) if not source else source
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
                        shard_dir / f"shard-{index:05d}.jsonl",
                    )
                ] = scan_source

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

                elapsed = max(time.time() - started_at, 0.001)
                print(
                    json.dumps(
                        {
                            "planner": "modal-volume-mounted-find",
                            "completed_prefixes": completed,
                            "scan_dirs": len(scan_dirs),
                            "scan_prefix": result["scan_prefix"],
                            "status": "finished",
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
            for shard_file in sorted(shard_dir.glob("shard-*.jsonl")):
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
        "planner": "modal-volume-mounted-find",
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


def row_belongs_to_worker(row_index: int, total_rows: int, worker_index: int, worker_count: int, assignment_mode: str) -> bool:
    if worker_count <= 1:
        return worker_index == 0
    if assignment_mode == "modulo":
        return row_index % worker_count == worker_index
    if assignment_mode == "contiguous":
        start = (total_rows * worker_index) // worker_count
        end = (total_rows * (worker_index + 1)) // worker_count
        return start <= row_index < end
    raise ValueError("assignment_mode must be 'modulo' or 'contiguous'")


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


def package_strategy(bytes_total: int | None, max_package_bytes: int, warn_package_bytes: int) -> str:
    if bytes_total is None:
        return "unknown"
    if bytes_total > max_package_bytes:
        return "split_required"
    if bytes_total > warn_package_bytes:
        return "warn_large_single_tar_zst"
    return "single_tar_zst"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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
    files_index_jsonl = output_dir / "files.index.jsonl"
    files_index_zst = output_dir / "files.index.jsonl.zst"
    package_index = output_dir / "package.index.json"
    file_count = 0
    dir_count = 0
    byte_count = 0

    with files_index_jsonl.open("w") as handle:
        for root, dirnames, filenames in os.walk(unit_abs):
            root_path = Path(root)
            for dirname in sorted(dirnames):
                path = root_path / dirname
                handle.write(json.dumps(file_record(path, unit_abs), sort_keys=True) + "\n")
                dir_count += 1
            for filename in sorted(filenames):
                path = root_path / filename
                record = file_record(path, unit_abs)
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
            "path": unit_rel,
        },
        "package": {
            "format": "tar.zst",
            "archive_path": archive_dest,
            "package_index_path": package_index_dest,
            "files_index_path": files_index_dest,
            "strategy": strategy,
            "max_package_bytes": max_package_bytes,
            "warn_package_bytes": warn_package_bytes,
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


def rclone_rcat(remote: str, dest_path: str, local_path: Path) -> None:
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


def rclone_copyto(remote: str, dest_path: str, local_path: Path) -> None:
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


def upload_archive_stream(unit_abs: Path, remote: str, archive_dest: str, compression_level: int) -> None:
    parent = shlex.quote(str(unit_abs.parent))
    name = shlex.quote(unit_abs.name)
    config = shlex.quote(str(RCLONE_CONFIG))
    target = shlex.quote(f"{remote}:{archive_dest}")
    level = max(1, min(19, int(compression_level)))
    rclone_flags = rclone_shell_flags()
    command = (
        "set -o pipefail; "
        f"tar -C {parent} -cf - {name} "
        f"| zstd -q -T0 -{level} "
        f"| rclone --config {config} rcat {target} "
        f"{rclone_flags}"
    )
    result = subprocess.run(command, shell=True, executable="/bin/bash", text=True, capture_output=True)
    raise_for_rclone_failure(result, f"stream upload {remote}:{archive_dest}")


def create_archive_staged(unit_abs: Path, archive_path: Path, compression_level: int) -> None:
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    parent = shlex.quote(str(unit_abs.parent))
    name = shlex.quote(unit_abs.name)
    out = shlex.quote(str(archive_path))
    level = max(1, min(19, int(compression_level)))
    command = (
        "set -o pipefail; "
        f"tar -C {parent} -cf - {name} "
        f"| zstd -q -T0 -{level} -o {out}"
    )
    run_checked(command)


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
    ephemeral_disk=env_int("MODAL_EPHEMERAL_DISK", 524288),
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
    max_package_bytes: str = "700GiB",
    warn_package_bytes: str = "650GiB",
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
    volumes={
        str(SOURCE_MOUNT): source_volume,
        str(CREDS_MOUNT): creds_volume,
        str(STATE_MOUNT): state_volume,
        str(CACHE_MOUNT): cache_volume,
    },
    timeout=env_int("MODAL_TIMEOUT", 43200),
    cpu=float(os.environ.get("MODAL_CPU", "2")),
    memory=env_int("MODAL_MEMORY", 4096),
    ephemeral_disk=env_int("MODAL_EPHEMERAL_DISK", 524288),
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
    upload_mode: str = "stream",
    max_package_bytes: str = "700GiB",
    warn_package_bytes: str = "650GiB",
    max_uploads_per_remote: int = MODAL_MAX_UPLOADS_PER_REMOTE,
) -> dict[str, Any]:
    rows = load_plan(plan_path)
    remotes = load_remotes(worker_index, remote_group_size)
    active_remotes = list(remotes)
    retired_remotes: dict[str, str] = {}
    remote_upload_counts = {remote: 0 for remote in remotes}
    if upload_mode not in {"stream", "staged"}:
        raise ValueError("upload_mode must be 'stream' or 'staged'")

    status_dir = STATE_MOUNT / "runs" / f"worker-{worker_index:03d}"
    status_dir.mkdir(parents=True, exist_ok=True)
    status_path = status_dir / f"{int(time.time())}.jsonl"

    processed = 0
    uploaded = 0
    skipped = 0
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
            remote = active_remotes[processed % len(active_remotes)]
            row_kind = row.get("kind", "package")
            unit_rel = row["source_path"]
            unit_abs = SOURCE_MOUNT / clean_relative_path(unit_rel)
            archive_dest = row["archive_dest"]
            package_index_dest = row.get("package_index_dest") or row.get("index_dest") or f"{archive_dest}.package.index.json"
            files_index_dest = row.get("files_index_dest") or f"{archive_dest}.files.index.jsonl.zst"
            result: dict[str, Any] = {
                "worker_index": worker_index,
                "assignment_mode": assignment_mode,
                "row_index": row_index,
                "remote": remote,
                "source_path": unit_rel,
                "archive_dest": archive_dest,
                "package_index_dest": package_index_dest,
                "files_index_dest": files_index_dest,
                "kind": row_kind,
                "dry_run": dry_run,
                "upload_mode": upload_mode,
                "started_at_unix": int(time.time()),
            }
            try:
                ensure_inside(unit_abs, SOURCE_MOUNT)
                if row_kind == "raw_file":
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
                        if max_uploads_per_remote > 0 and remote_upload_counts[remote] >= max_uploads_per_remote:
                            retired_remotes[remote] = f"max_uploads_per_remote={max_uploads_per_remote}"
                            active_remotes = [candidate for candidate in active_remotes if candidate != remote]
                    status_handle.write(json.dumps(result | {"finished_at_unix": int(time.time())}, sort_keys=True) + "\n")
                    status_handle.flush()
                    processed += 1
                    if limit > 0 and processed >= limit:
                        break
                    continue

                if not unit_abs.exists() or not unit_abs.is_dir():
                    raise FileNotFoundError(f"missing source package directory: {unit_abs}")
                if dry_run:
                    row_bytes = row.get("bytes")
                    result["bytes"] = row_bytes
                    result["package_strategy"] = (
                        package_strategy(int(row_bytes), max_bytes, warn_bytes) if row_bytes is not None else "unknown"
                    )
                    result["status"] = "planned"
                else:
                    with tempfile.TemporaryDirectory() as tmp:
                        tmp_path = Path(tmp)
                        index_info = write_indexes(
                            unit_abs,
                            row.get("source_volume", SOURCE_VOLUME_NAME),
                            unit_rel,
                            archive_dest,
                            package_index_dest,
                            files_index_dest,
                            tmp_path,
                            max_bytes,
                            warn_bytes,
                        )
                        result.update(index_info)
                        if index_info["package_strategy"] == "split_required":
                            result["status"] = "skipped_split_required"
                            skipped += 1
                        elif upload_mode == "staged":
                            cache_dir = CACHE_MOUNT / "workers" / f"{worker_index:03d}" / f"row-{row_index:012d}"
                            archive_path = cache_dir / Path(archive_dest).name
                            create_archive_staged(unit_abs, archive_path, compression_level)
                            result["archive_staged_path"] = str(archive_path)
                            result["archive_bytes"] = archive_path.stat().st_size
                            result["archive_sha256"] = sha256_file(archive_path)
                            cache_volume.commit()
                            rclone_copyto(remote, archive_dest, archive_path)
                            rclone_rcat(remote, package_index_dest, Path(index_info["package_index_path"]))
                            rclone_rcat(remote, files_index_dest, Path(index_info["files_index_path"]))
                            archive_path.unlink(missing_ok=True)
                            cache_volume.commit()
                            result["status"] = "uploaded"
                            uploaded += 1
                            remote_upload_counts[remote] += 1
                        else:
                            upload_archive_stream(unit_abs, remote, archive_dest, compression_level)
                            rclone_rcat(remote, package_index_dest, Path(index_info["package_index_path"]))
                            rclone_rcat(remote, files_index_dest, Path(index_info["files_index_path"]))
                            result["status"] = "uploaded"
                            uploaded += 1
                            remote_upload_counts[remote] += 1
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
    volumes={
        str(SOURCE_MOUNT): source_volume,
        str(STATE_MOUNT): state_volume,
        str(CACHE_MOUNT): cache_volume,
    },
    timeout=env_int("MODAL_TIMEOUT", 43200),
    cpu=float(os.environ.get("MODAL_CPU", "2")),
    memory=env_int("MODAL_MEMORY", 4096),
    ephemeral_disk=env_int("MODAL_EPHEMERAL_DISK", 524288),
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
    max_package_bytes: str = "700GiB",
    warn_package_bytes: str = "650GiB",
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

    with status_path.open("w") as status_handle:
        for row in rows:
            row_index = int(row["index"])
            if not row_belongs_to_worker(row_index, len(rows), worker_index, worker_count, assignment_mode):
                continue
            row_kind = row.get("kind", "package")
            unit_rel = row["source_path"]
            unit_abs = SOURCE_MOUNT / clean_relative_path(unit_rel)
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
                "archive_dest": archive_dest,
                "package_index_dest": package_index_dest,
                "files_index_dest": files_index_dest,
                "spool_archive_path": str(archive_path),
                "spool_package_index_path": str(package_index_path),
                "spool_files_index_path": str(files_index_path),
                "spool_manifest_path": str(manifest_path),
                "kind": row_kind,
                "dry_run": dry_run,
                "started_at_unix": int(time.time()),
            }
            try:
                ensure_inside(unit_abs, SOURCE_MOUNT)
                if row_kind == "raw_file":
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
                        if uncommitted >= MODAL_CACHE_COMMIT_EVERY:
                            cache_volume.commit()
                            uncommitted = 0
                        result["status"] = "prepared"
                    status_handle.write(json.dumps(result | {"finished_at_unix": int(time.time())}, sort_keys=True) + "\n")
                    status_handle.flush()
                    processed += 1
                    if limit > 0 and processed >= limit:
                        break
                    continue

                if not unit_abs.exists() or not unit_abs.is_dir():
                    raise FileNotFoundError(f"missing source package directory: {unit_abs}")
                if archive_path.exists() and package_index_path.exists() and files_index_path.exists():
                    result["status"] = "already_prepared"
                    skipped += 1
                elif dry_run:
                    result["status"] = "planned"
                else:
                    with tempfile.TemporaryDirectory() as tmp:
                        tmp_path = Path(tmp)
                        index_info = write_indexes(
                            unit_abs,
                            row.get("source_volume", SOURCE_VOLUME_NAME),
                            unit_rel,
                            archive_dest,
                            package_index_dest,
                            files_index_dest,
                            tmp_path,
                            max_bytes,
                            warn_bytes,
                        )
                        result.update(index_info)
                        if index_info["package_strategy"] == "split_required":
                            result["status"] = "skipped_split_required"
                            skipped += 1
                        else:
                            create_archive_staged(unit_abs, archive_path, compression_level)
                            package_index_path.parent.mkdir(parents=True, exist_ok=True)
                            files_index_path.parent.mkdir(parents=True, exist_ok=True)
                            shutil.copy2(index_info["package_index_path"], package_index_path)
                            shutil.copy2(index_info["files_index_path"], files_index_path)
                            result["archive_bytes"] = archive_path.stat().st_size
                            result["archive_sha256"] = sha256_file(archive_path)
                            manifest_path.write_text(json.dumps(result | {"status": "prepared"}, indent=2, sort_keys=True) + "\n")
                            prepared += 1
                            result["status"] = "prepared"
                            uncommitted += 1
                            if uncommitted >= MODAL_CACHE_COMMIT_EVERY:
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
        "cache_commit_every": MODAL_CACHE_COMMIT_EVERY,
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
    ephemeral_disk=env_int("MODAL_EPHEMERAL_DISK", 524288),
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


@app.local_entrypoint()
def main(
    command: str = "discover",
    source_prefix: str = "",
    unit_depth: int = 2,
    dest_prefix: str = "",
    plan_path: str = "plans/modal-volume-units.jsonl",
    worker_count: int = 10,
    remote_group_size: int = 10,
    assignment_mode: str = os.environ.get("MODAL_ASSIGNMENT_MODE", "contiguous"),
    spool_name: str = os.environ.get("MODAL_SPOOL_NAME", ""),
    dry_run: bool = True,
    limit: int = 0,
    include_stats: bool = False,
    compression_level: int = env_int("MODAL_COMPRESSION_LEVEL", 3),
    run_id: str = "",
    worker_index: int = -1,
    upload_mode: str = os.environ.get("MODAL_UPLOAD_MODE", "stream"),
    max_package_bytes: str = os.environ.get("MODAL_MAX_PACKAGE_BYTES", "700GiB"),
    warn_package_bytes: str = os.environ.get("MODAL_WARN_PACKAGE_BYTES", "650GiB"),
    max_uploads_per_remote: int = MODAL_MAX_UPLOADS_PER_REMOTE,
    allow_unsafe_delete: bool = False,
):
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
                upload_mode=upload_mode,
                max_package_bytes=max_package_bytes,
                warn_package_bytes=warn_package_bytes,
                max_uploads_per_remote=max_uploads_per_remote,
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
