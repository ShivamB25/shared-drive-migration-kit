#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import urllib.error
import urllib.request
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


def access_token() -> str:
    result = subprocess.run(
        ["gcloud", "auth", "print-access-token"],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return result.stdout.strip()


def create_permission(token: str, drive_id: str, email: str, member_type: str, role: str) -> dict:
    url = (
        f"https://www.googleapis.com/drive/v3/files/{drive_id}/permissions"
        "?supportsAllDrives=true&sendNotificationEmail=false"
    )
    body = json.dumps({"type": member_type, "role": role, "emailAddress": email}).encode()
    request = urllib.request.Request(
        url,
        data=body,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request) as response:
            return json.loads(response.read().decode())
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        raise RuntimeError(f"Drive permission create failed for {email}: HTTP {exc.code}: {detail}") from exc


def main() -> int:
    root_dir = Path(__file__).resolve().parents[1]
    load_env_file(root_dir / ".env")

    parser = argparse.ArgumentParser(description="Grant a Workspace group or service account access to a shared drive.")
    parser.add_argument("--shared-drive-id", default=os.environ.get("SHARED_DRIVE_ID", ""))
    parser.add_argument("--email", default=os.environ.get("GROUP_EMAIL", ""))
    parser.add_argument("--type", choices=["group", "user"], default="group")
    parser.add_argument(
        "--role",
        choices=["reader", "commenter", "writer", "fileOrganizer", "organizer"],
        default=os.environ.get("SHARED_DRIVE_ROLE", "fileOrganizer"),
        help="fileOrganizer maps to Content manager in shared drives.",
    )
    parser.add_argument("--apply", action="store_true", default=os.environ.get("APPLY", "0") == "1")
    args = parser.parse_args()

    if not args.shared_drive_id:
        raise SystemExit("SHARED_DRIVE_ID is required.")
    if not args.email:
        raise SystemExit("--email or GROUP_EMAIL is required.")

    print(f"shared drive: {args.shared_drive_id}")
    print(f"principal:    {args.email}")
    print(f"type:         {args.type}")
    print(f"role:         {args.role}")
    print(f"apply:        {int(args.apply)}")

    if not args.apply:
        print("dry run only. Re-run with --apply or APPLY=1 to create the Drive permission.")
        print("If gcloud tokens do not have Drive scope, run: gcloud auth login --enable-gdrive-access --force")
        return 0

    token = access_token()
    response = create_permission(token, args.shared_drive_id, args.email, args.type, args.role)
    print(json.dumps(response, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

