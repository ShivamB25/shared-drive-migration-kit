#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import urllib.error
import urllib.parse
import urllib.request
import uuid
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


def drive_request(token: str, method: str, url: str, body: dict | None = None) -> dict:
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(
        url,
        data=data,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        method=method,
    )
    try:
        with urllib.request.urlopen(request) as response:
            content = response.read().decode()
            return json.loads(content) if content else {}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        raise RuntimeError(f"Drive API request failed: {method} {url}: HTTP {exc.code}: {detail}") from exc


def escape_drive_query(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def find_shared_drives(token: str, name: str) -> list[dict]:
    params = {
        "q": f"name = '{escape_drive_query(name)}'",
        "pageSize": "10",
        "fields": "drives(id,name),nextPageToken",
    }
    url = "https://www.googleapis.com/drive/v3/drives?" + urllib.parse.urlencode(params)
    response = drive_request(token, "GET", url)
    return response.get("drives", [])


def create_shared_drive(token: str, name: str, request_id: str) -> dict:
    params = {
        "requestId": request_id,
        "fields": "id,name",
    }
    url = "https://www.googleapis.com/drive/v3/drives?" + urllib.parse.urlencode(params)
    return drive_request(token, "POST", url, {"name": name})


def update_env(path: Path, updates: dict[str, str]) -> None:
    existing = path.read_text().splitlines() if path.exists() else []
    seen: set[str] = set()
    output: list[str] = []

    for line in existing:
        if "=" not in line or line.lstrip().startswith("#"):
            output.append(line)
            continue
        key = line.split("=", 1)[0]
        if key in updates:
            output.append(f'{key}="{updates[key]}"')
            seen.add(key)
        else:
            output.append(line)

    for key, value in updates.items():
        if key not in seen:
            output.append(f'{key}="{value}"')

    path.write_text("\n".join(output) + "\n")


def main() -> int:
    root_dir = Path(__file__).resolve().parents[1]
    env_path = root_dir / ".env"
    load_env_file(env_path)

    parser = argparse.ArgumentParser(description="Create or find a Google shared drive using gcloud auth and the Drive API.")
    parser.add_argument("--name", default=os.environ.get("SHARED_DRIVE_NAME", "Shared Drive Migration Target"))
    parser.add_argument("--request-id", default=os.environ.get("SHARED_DRIVE_REQUEST_ID", ""))
    parser.add_argument("--allow-duplicate", action="store_true", default=os.environ.get("ALLOW_DUPLICATE_SHARED_DRIVE", "0") == "1")
    parser.add_argument("--write-env", action="store_true", default=os.environ.get("WRITE_ENV", "0") == "1")
    parser.add_argument("--apply", action="store_true", default=os.environ.get("APPLY", "0") == "1")
    args = parser.parse_args()

    if not args.name:
        raise SystemExit("--name or SHARED_DRIVE_NAME is required.")

    request_id = args.request_id or str(uuid.uuid4())

    print(f"shared drive name: {args.name}")
    print(f"request id:        {request_id}")
    print(f"allow duplicate:   {int(args.allow_duplicate)}")
    print(f"write .env:        {int(args.write_env)}")
    print(f"apply:             {int(args.apply)}")

    if not args.apply:
        print("dry run only. Re-run with --apply or APPLY=1 to create/find the shared drive.")
        print("If gcloud tokens do not have Drive scope, run: gcloud auth login --enable-gdrive-access --force")
        return 0

    token = access_token()

    if not args.allow_duplicate:
        matches = find_shared_drives(token, args.name)
        if matches:
            drive = matches[0]
            print("found existing shared drive:")
            print(json.dumps(drive, indent=2, sort_keys=True))
        else:
            drive = create_shared_drive(token, args.name, request_id)
            print("created shared drive:")
            print(json.dumps(drive, indent=2, sort_keys=True))
    else:
        drive = create_shared_drive(token, args.name, request_id)
        print("created shared drive:")
        print(json.dumps(drive, indent=2, sort_keys=True))

    drive_id = drive.get("id")
    if not drive_id:
        raise SystemExit("Drive API response did not include an id.")

    if args.write_env:
        update_env(env_path, {"SHARED_DRIVE_ID": drive_id, "SHARED_DRIVE_NAME": args.name})
        print(f"updated: {env_path}")

    print(f"SHARED_DRIVE_ID={drive_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

