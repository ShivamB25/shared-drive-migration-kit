from __future__ import annotations

import csv
import hashlib
import json
import os
import shlex
import subprocess
import tempfile
import time
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


def load_remotes(worker_index: int, remote_group_size: int) -> list[str]:
    if not RCLONE_MANIFEST.exists():
        raise FileNotFoundError(f"missing rclone manifest in Modal credentials volume: {RCLONE_MANIFEST}")
    rows: list[dict[str, str]] = []
    with RCLONE_MANIFEST.open(newline="") as handle:
        rows = [row for row in csv.DictReader(handle) if row.get("remote")]

    assigned = [row["remote"] for row in rows if row.get("worker_index") == str(worker_index)]
    if assigned:
        return assigned

    start = worker_index * remote_group_size
    fallback = [row["remote"] for row in rows[start : start + remote_group_size]]
    if fallback:
        return fallback

    all_remotes = [row["remote"] for row in rows]
    if not all_remotes:
        raise RuntimeError("no remotes in rclone manifest")
    return [all_remotes[worker_index % len(all_remotes)]]


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

    subprocess.run(["zstd", "-T0", "-3", "-f", str(files_index_jsonl), "-o", str(files_index_zst)], check=True)
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
        subprocess.run(command, stdin=handle, check=True)


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
    subprocess.run(command, check=True)


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
        f"| zstd -T0 -{level} "
        f"| rclone --config {config} rcat {target} "
        f"{rclone_flags}"
    )
    run_checked(command)


def create_archive_staged(unit_abs: Path, archive_path: Path, compression_level: int) -> None:
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    parent = shlex.quote(str(unit_abs.parent))
    name = shlex.quote(unit_abs.name)
    out = shlex.quote(str(archive_path))
    level = max(1, min(19, int(compression_level)))
    command = (
        "set -o pipefail; "
        f"tar -C {parent} -cf - {name} "
        f"| zstd -T0 -{level} -o {out}"
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
    dry_run: bool = True,
    limit: int = 0,
    compression_level: int = 3,
    upload_mode: str = "stream",
    max_package_bytes: str = "700GiB",
    warn_package_bytes: str = "650GiB",
) -> dict[str, Any]:
    rows = load_plan(plan_path)
    remotes = load_remotes(worker_index, remote_group_size)
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
            if row_index % worker_count != worker_index:
                continue
            remote = remotes[processed % len(remotes)]
            unit_rel = row["source_path"]
            unit_abs = SOURCE_MOUNT / clean_relative_path(unit_rel)
            archive_dest = row["archive_dest"]
            package_index_dest = row.get("package_index_dest") or row.get("index_dest") or f"{archive_dest}.package.index.json"
            files_index_dest = row.get("files_index_dest") or f"{archive_dest}.files.index.jsonl.zst"
            result: dict[str, Any] = {
                "worker_index": worker_index,
                "row_index": row_index,
                "remote": remote,
                "source_path": unit_rel,
                "archive_dest": archive_dest,
                "package_index_dest": package_index_dest,
                "files_index_dest": files_index_dest,
                "dry_run": dry_run,
                "upload_mode": upload_mode,
                "started_at_unix": int(time.time()),
            }
            try:
                ensure_inside(unit_abs, SOURCE_MOUNT)
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
                        else:
                            upload_archive_stream(unit_abs, remote, archive_dest, compression_level)
                            rclone_rcat(remote, package_index_dest, Path(index_info["package_index_path"]))
                            rclone_rcat(remote, files_index_dest, Path(index_info["files_index_path"]))
                            result["status"] = "uploaded"
                            uploaded += 1
            except Exception as exc:  # noqa: BLE001 - worker status should record any package failure.
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
    return {
        "worker_index": worker_index,
        "worker_count": worker_count,
        "remotes": remotes,
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
    dry_run: bool = True,
    limit: int = 0,
    include_stats: bool = False,
    compression_level: int = env_int("MODAL_COMPRESSION_LEVEL", 3),
    run_id: str = "",
    worker_index: int = -1,
    upload_mode: str = os.environ.get("MODAL_UPLOAD_MODE", "stream"),
    max_package_bytes: str = os.environ.get("MODAL_MAX_PACKAGE_BYTES", "700GiB"),
    warn_package_bytes: str = os.environ.get("MODAL_WARN_PACKAGE_BYTES", "650GiB"),
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

    if command == "upload":
        worker_indexes = [worker_index] if worker_index >= 0 else list(range(worker_count))
        calls = [
            upload_worker.spawn(
                worker_index=index,
                worker_count=worker_count,
                plan_path=plan_path,
                remote_group_size=remote_group_size,
                dry_run=dry_run,
                limit=limit,
                compression_level=compression_level,
                upload_mode=upload_mode,
                max_package_bytes=max_package_bytes,
                warn_package_bytes=warn_package_bytes,
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
