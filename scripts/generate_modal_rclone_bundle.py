#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
from pathlib import Path


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def read_manifest(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as handle:
        rows = [row for row in csv.DictReader(handle) if row.get("remote") and row.get("key_file")]
    if not rows:
        raise SystemExit(f"no remotes found in manifest: {path}")
    return rows


def client_email(path: Path) -> str:
    with path.open() as handle:
        data = json.load(handle)
    email = data.get("client_email")
    if not email:
        raise SystemExit(f"{path} does not contain client_email")
    return email


def main() -> int:
    root_dir = Path(__file__).resolve().parents[1]
    load_env_file(root_dir / ".env")

    parser = argparse.ArgumentParser(
        description="Build a Modal-mounted rclone credential bundle from the local service-account manifest."
    )
    parser.add_argument("--manifest", default=os.environ.get("RCLONE_MANIFEST", "generated/rclone.manifest.csv"))
    parser.add_argument("--out", default=os.environ.get("MODAL_RCLONE_BUNDLE_DIR", "generated/modal-rclone-bundle"))
    parser.add_argument(
        "--mount-path",
        default=os.environ.get("MODAL_RCLONE_BUNDLE_MOUNT", "/creds/modal-rclone-bundle"),
    )
    parser.add_argument("--shared-drive-id", default=os.environ.get("SHARED_DRIVE_ID", ""))
    parser.add_argument("--root-folder-id", default=os.environ.get("ROOT_FOLDER_ID", ""))
    parser.add_argument("--worker-count", type=int, default=int(os.environ.get("MODAL_WORKER_COUNT", "10")))
    parser.add_argument("--remote-group-size", type=int, default=int(os.environ.get("MODAL_REMOTE_GROUP_SIZE", "10")))
    args = parser.parse_args()

    if not args.shared_drive_id:
        raise SystemExit("SHARED_DRIVE_ID is required.")
    if args.worker_count <= 0:
        raise SystemExit("--worker-count must be positive")
    if args.remote_group_size <= 0:
        raise SystemExit("--remote-group-size must be positive")

    manifest = Path(args.manifest)
    if not manifest.exists():
        raise SystemExit(f"missing manifest: {manifest}. Run scripts/generate_rclone_config.py first.")

    rows = read_manifest(manifest)
    out = Path(args.out)
    keys_dir = out / "service-accounts"
    if out.exists():
        shutil.rmtree(out)
    keys_dir.mkdir(parents=True, exist_ok=True)

    mount_path = args.mount_path.rstrip("/")
    rclone_lines: list[str] = []
    bundle_rows: list[dict[str, str]] = []
    group_rows: list[dict[str, str]] = []

    for index, row in enumerate(rows):
        remote = row["remote"]
        source_key = Path(row["key_file"]).expanduser()
        if not source_key.exists():
            raise SystemExit(f"missing key file for {remote}: {source_key}")

        email = row.get("email") or client_email(source_key)
        bundle_key = keys_dir / f"{remote}.json"
        shutil.copy2(source_key, bundle_key)
        bundle_key.chmod(0o600)

        modal_key_path = f"{mount_path}/service-accounts/{bundle_key.name}"
        rclone_lines.extend(
            [
                f"[{remote}]",
                "type = drive",
                "scope = drive",
                f"service_account_file = {modal_key_path}",
                f"team_drive = {args.shared_drive_id}",
            ]
        )
        if args.root_folder_id:
            rclone_lines.append(f"root_folder_id = {args.root_folder_id}")
        rclone_lines.append("")

        worker_index = min(index // args.remote_group_size, args.worker_count - 1)
        slot = index % args.remote_group_size
        bundle_rows.append(
            {
                "remote": remote,
                "email": email,
                "key_file": modal_key_path,
                "worker_index": str(worker_index),
                "slot": str(slot),
            }
        )
        group_rows.append(
            {
                "worker_index": str(worker_index),
                "slot": str(slot),
                "remote": remote,
                "email": email,
            }
        )

    rclone_conf = out / "rclone.conf"
    rclone_conf.write_text("\n".join(rclone_lines))
    rclone_conf.chmod(0o600)

    with (out / "rclone.manifest.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["remote", "email", "key_file", "worker_index", "slot"])
        writer.writeheader()
        writer.writerows(bundle_rows)

    with (out / "remote-groups.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["worker_index", "slot", "remote", "email"])
        writer.writeheader()
        writer.writerows(group_rows)

    metadata = {
        "mount_path": mount_path,
        "shared_drive_id": args.shared_drive_id,
        "root_folder_id": args.root_folder_id,
        "worker_count": args.worker_count,
        "remote_group_size": args.remote_group_size,
        "remote_count": len(rows),
    }
    (out / "bundle.json").write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n")

    print(f"wrote Modal rclone bundle: {out}")
    print(f"remotes: {len(rows)}")
    print(f"worker count: {args.worker_count}")
    print(f"remote group size: {args.remote_group_size}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
