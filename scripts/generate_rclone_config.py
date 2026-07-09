#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key.strip(), value)


def key_email(key_file: Path) -> str:
    with key_file.open() as handle:
        data = json.load(handle)
    email = data.get("client_email")
    if not email:
        raise ValueError(f"{key_file} does not contain client_email")
    return email


def inventory_key_files(inventory_file: Path) -> list[Path]:
    with inventory_file.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    key_files = [Path(row["key_file"]).expanduser() for row in rows if row.get("key_file")]
    return key_files


def discover_key_files(keys_dir: Path) -> list[Path]:
    return sorted(keys_dir.expanduser().glob("*.json"))


def main() -> int:
    root_dir = Path(__file__).resolve().parents[1]
    load_env_file(root_dir / ".env")

    parser = argparse.ArgumentParser(description="Generate an rclone config for Google shared drive service-account remotes.")
    parser.add_argument("--inventory-file", default=os.environ.get("INVENTORY_FILE", "generated/service_accounts.csv"))
    parser.add_argument("--keys-dir", default=os.environ.get("KEY_DIR", "secrets/service-accounts"))
    parser.add_argument("--out", default=os.environ.get("RCLONE_CONFIG_OUT", "generated/rclone.conf"))
    parser.add_argument("--remote-prefix", default=os.environ.get("RCLONE_REMOTE_PREFIX", "gdrive-sa"))
    parser.add_argument("--shared-drive-id", default=os.environ.get("SHARED_DRIVE_ID", ""))
    parser.add_argument("--root-folder-id", default=os.environ.get("ROOT_FOLDER_ID", ""))
    args = parser.parse_args()

    if not args.shared_drive_id:
        raise SystemExit("SHARED_DRIVE_ID is required.")

    inventory_file = Path(args.inventory_file)
    if inventory_file.exists():
        key_files = inventory_key_files(inventory_file)
    else:
        key_files = discover_key_files(Path(args.keys_dir))

    existing_key_files = [path.resolve() for path in key_files if path.exists()]
    if not existing_key_files:
        raise SystemExit("No service account key files found. Run bootstrap_gcp_service_accounts.sh with APPLY=1 first.")

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    lines: list[str] = []
    manifest_rows: list[dict[str, str]] = []
    for number, key_file in enumerate(existing_key_files, start=1):
        remote = f"{args.remote_prefix}{number:03d}"
        email = key_email(key_file)
        lines.extend(
            [
                f"[{remote}]",
                "type = drive",
                "scope = drive",
                f"service_account_file = {key_file}",
                f"team_drive = {args.shared_drive_id}",
            ]
        )
        if args.root_folder_id:
            lines.append(f"root_folder_id = {args.root_folder_id}")
        lines.append("")
        manifest_rows.append({"remote": remote, "email": email, "key_file": str(key_file)})

    out.write_text("\n".join(lines))
    out.chmod(0o600)

    manifest = out.with_suffix(".manifest.csv")
    with manifest.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["remote", "email", "key_file"])
        writer.writeheader()
        writer.writerows(manifest_rows)

    print(f"wrote rclone config: {out}")
    print(f"wrote manifest: {manifest}")
    print(f"remotes: {len(manifest_rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

