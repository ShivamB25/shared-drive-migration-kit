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
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def main() -> int:
    root_dir = Path(__file__).resolve().parents[1]
    load_env_file(root_dir / ".env")

    parser = argparse.ArgumentParser(description="Generate Workspace group membership artifacts from service account inventory.")
    parser.add_argument("--inventory-file", default=os.environ.get("INVENTORY_FILE", "generated/service_accounts.csv"))
    parser.add_argument("--group-email", default=os.environ.get("GROUP_EMAIL", ""))
    parser.add_argument("--out-dir", default="generated")
    parser.add_argument("--batch-size", type=int, default=int(os.environ.get("GROUP_BATCH_SIZE", "10")))
    args = parser.parse_args()

    if not args.group_email:
        raise SystemExit("GROUP_EMAIL is required.")
    if args.batch_size < 1:
        raise SystemExit("--batch-size must be at least 1")

    inventory_file = Path(args.inventory_file)
    if not inventory_file.exists():
        raise SystemExit(f"missing inventory file: {inventory_file}")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    with inventory_file.open(newline="") as handle:
        rows = list(csv.DictReader(handle))

    emails = [row["email"] for row in rows if row.get("email")]
    if not emails:
        raise SystemExit("inventory has no service account emails")

    batch_dir = out_dir / "group-email-batches"
    batch_dir.mkdir(parents=True, exist_ok=True)
    for stale_file in batch_dir.glob("batch-*.csv"):
        stale_file.unlink()

    batches = [emails[index : index + args.batch_size] for index in range(0, len(emails), args.batch_size)]
    for batch_number, batch in enumerate(batches, start=1):
        batch_file = batch_dir / f"batch-{batch_number:03d}.csv"
        batch_file.write_text(",".join(batch) + "\n")

    all_emails_out = out_dir / "workspace_group_emails_one_per_line.txt"
    all_emails_out.write_text("\n".join(emails) + "\n")

    csv_out = out_dir / "workspace_group_members.csv"
    with csv_out.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["group_email", "member_email", "role"])
        writer.writeheader()
        for email in emails:
            writer.writerow({"group_email": args.group_email, "member_email": email, "role": "MEMBER"})

    jsonl_out = out_dir / "admin_directory_members.jsonl"
    with jsonl_out.open("w") as handle:
        for email in emails:
            payload = {"email": email, "role": "MEMBER", "delivery_settings": "NONE"}
            handle.write(json.dumps({"groupKey": args.group_email, "body": payload}, sort_keys=True))
            handle.write("\n")

    txt_out = out_dir / "workspace_group_next_steps.txt"
    txt_out.write_text(
        "\n".join(
            [
                f"Group: {args.group_email}",
                "",
                "Use one of these artifacts with your Workspace admin tooling:",
                f"- {csv_out}",
                f"- {jsonl_out}",
                f"- {all_emails_out}",
                "",
                "For manual Google Group member entry, use these comma-separated batch files:",
                f"- {batch_dir}/batch-001.csv through batch-{len(batches):03d}.csv",
                "",
                "Admin SDK endpoint for each jsonl row:",
                "POST https://admin.googleapis.com/admin/directory/v1/groups/{groupKey}/members",
                "",
                "After members are added, grant the group access to the target shared drive.",
                "Use Contributor or Content manager depending on whether the migration must move/organize files after upload.",
            ]
        )
    )

    print(f"wrote: {csv_out}")
    print(f"wrote: {jsonl_out}")
    print(f"wrote: {all_emails_out}")
    print(f"wrote batches: {batch_dir} ({len(batches)} files, batch size {args.batch_size})")
    print(f"wrote: {txt_out}")
    print(f"members: {len(emails)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
