#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
import shlex
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


def remotes_from_manifest(path: Path) -> list[str]:
    with path.open(newline="") as handle:
        return [row["remote"] for row in csv.DictReader(handle) if row.get("remote")]


def main() -> int:
    root_dir = Path(__file__).resolve().parents[1]
    load_env_file(root_dir / ".env")

    parser = argparse.ArgumentParser(description="Assign top-level source entries to generated rclone remotes.")
    parser.add_argument("--source-path", default=os.environ.get("SOURCE_PATH", ""))
    parser.add_argument("--dest-path", default=os.environ.get("DEST_PATH", ""))
    parser.add_argument("--rclone-config", default=os.environ.get("RCLONE_CONFIG_OUT", "generated/rclone.conf"))
    parser.add_argument("--manifest", default=os.environ.get("RCLONE_MANIFEST", "generated/rclone.manifest.csv"))
    parser.add_argument("--out", default="generated/upload_plan.csv")
    parser.add_argument("--commands-out", default="generated/upload_commands.sh")
    parser.add_argument("--remote-limit", type=int, default=int(os.environ.get("REMOTE_LIMIT", "0")))
    args = parser.parse_args()

    if not args.source_path:
        raise SystemExit("SOURCE_PATH is required.")

    source = Path(args.source_path).expanduser().resolve()
    if not source.exists() or not source.is_dir():
        raise SystemExit(f"SOURCE_PATH must be an existing directory: {source}")

    manifest = Path(args.manifest)
    if not manifest.exists():
        raise SystemExit(f"missing rclone manifest: {manifest}. Run generate_rclone_config.py first.")

    remotes = remotes_from_manifest(manifest)
    if args.remote_limit > 0:
        remotes = remotes[: args.remote_limit]
    if not remotes:
        raise SystemExit("no remotes available")

    entries = sorted(source.iterdir(), key=lambda path: path.name)
    if not entries:
        raise SystemExit(f"source directory is empty: {source}")

    rows: list[dict[str, str]] = []
    for index, entry in enumerate(entries):
        remote = remotes[index % len(remotes)]
        relative_dest = "/".join(part for part in [args.dest_path.strip("/"), entry.name] if part)
        rows.append(
            {
                "source": str(entry),
                "remote": remote,
                "destination": f"{remote}:{relative_dest}",
            }
        )

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["source", "remote", "destination"])
        writer.writeheader()
        writer.writerows(rows)

    commands_out = Path(args.commands_out)
    commands_out.parent.mkdir(parents=True, exist_ok=True)
    with commands_out.open("w") as handle:
        handle.write("#!/usr/bin/env bash\nset -Eeuo pipefail\n\n")
        handle.write("# Review this file before running. Commands default to dry-run.\n")
        for row in rows:
            command = [
                "rclone",
                "--config",
                args.rclone_config,
                "copy",
                row["source"],
                row["destination"],
                "--dry-run",
                "--transfers",
                "${TRANSFERS:-4}",
                "--checkers",
                "${CHECKERS:-8}",
                "--drive-chunk-size",
                "256M",
                "--progress",
                "--stats",
                "30s",
            ]
            rendered = " ".join(shlex.quote(part) for part in command)
            rendered = rendered.replace("'${TRANSFERS:-4}'", "${TRANSFERS:-4}")
            rendered = rendered.replace("'${CHECKERS:-8}'", "${CHECKERS:-8}")
            handle.write(rendered)
            handle.write("\n")
    commands_out.chmod(0o755)

    print(f"wrote upload plan: {out}")
    print(f"wrote command script: {commands_out}")
    print(f"entries: {len(rows)}")
    print(f"remotes used: {min(len(remotes), len(rows))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

