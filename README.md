# Shared Drive Migration Kit

This repository sets up a Google shared drive as a large-scale migration target using Google Cloud service accounts and rclone.

The design is shared-drive-only:

- no My Drive upload target
- no domain-wide delegation for upload
- no required Workspace Admin SDK automation
- one Google Group grants all service accounts access to the shared drive
- one rclone Drive remote is generated per service-account key

The source side is intentionally adapter-shaped. A source adapter can upload from a local path, a Modal Volume worker, object storage, a remote host, or any other source that can produce a migration plan and feed rclone-compatible uploads.

For the deeper operator/agent runbook, see [AGENTS.md](AGENTS.md).

## Proven Setup

This workflow has been tested end-to-end in a private Google Workspace. Real project IDs, group emails, shared-drive IDs, local paths, and credential paths are intentionally omitted from this public guide.

```text
Google Cloud project: dedicated migration project
Billing: not linked, when organization policy allows it
Workspace group: migration-uploaders@example.com
Shared drive ID: configured in .env
Service accounts created: 100
Service-account keys created: 100
Group members added: 100
Reference rclone remote: gdrive_target:
```

Confirmed working commands:

```bash
rclone --config generated/gdrive_target.reference.conf lsf gdrive_target:
REMOTE_LIMIT=5 scripts/validate_rclone_remotes.sh
```

The Modal Volume source adapter has also been tested in a private environment:

```text
Modal auth: existing local Modal CLI profile
Planner: Modal SDK recursive Volume metadata listing
Worker shape: 10-worker plan, worker 0 launched alone
Package format: tar.zst
Metadata files: package.index.json and files.index.jsonl.zst
Upload mode tested: staged
Real test package: one package uploaded and verified with rclone
```

## Architecture

Target access flow:

```text
Google Cloud project
  -> 100 service accounts
  -> service-account JSON keys
  -> Google Group membership
  -> shared drive Content manager access
  -> rclone remotes using team_drive
```

Source adapter flow:

```text
source adapter
  -> planning manifest
  -> optional package step
  -> rclone upload
  -> shared drive target remotes
```

The target side is stable: service accounts, group access, rclone remotes, and shared-drive permissions. The source side is intentionally pluggable. A source adapter can be a local path, Modal Volume, object storage bucket, remote server, or any future source that can emit the same kind of package/upload plan.

## Source Adapter Decisions

Before running any source adapter, answer these questions and record the answers in `.env` or in the run notes:

- What source adapter is being used: `local-path`, `modal-volume`, or another adapter?
- What is the source root or prefix?
- What is the migration unit: one top-level folder, a fixed-depth folder, one file, or a custom manifest row?
- Should the target receive raw files, one archive per unit, or another package format?
- If packaging, should the package be `tar.zst`, `zip`, uncompressed `tar`, or source-native?
- Should each package include a small package index and a compressed file index next to the archive?
- What destination prefix should be used inside the shared drive?
- What is the maximum concurrency the source account allows?
- How many rclone/service-account remotes should each worker rotate through?
- Is the first run a dry-run, smoke run, single-worker run, or full run?

For very large sources with millions of files, prefer packaging units into archives instead of uploading raw files. This keeps the shared drive below item-count limits and makes migration verification easier.

Recommended migration operating style:

1. Trust the operator on source semantics and desired folder/package boundaries.
2. Verify platform behavior that can silently break a long migration.
3. Prefer API metadata for planning before reading file contents.
4. Run a synthetic smoke upload before touching source data.
5. Run one real worker with `--limit 1` before launching wider concurrency.
6. Keep cleanup narrow and prefix-scoped.

Why use a Google Group:

- shared drives allow far fewer direct members than effective group members
- a single group counts as one direct shared-drive member
- adding/removing migration service accounts becomes centralized
- future migrations can reuse the same pattern

## Required Tools

Verify these are installed:

```bash
gcloud version
rclone version
jq --version
python3 --version
```

On macOS, the scripts are written to work with the default older Bash where possible.

## Permissions Needed

The operator should be able to:

- create or manage a Google Cloud project
- enable APIs in that project
- create service accounts and service-account keys
- create/manage a Google Group, or have the group created manually
- add the Google Group to the destination shared drive

If the operator can manage Google Group memberships, this repo can add all service-account emails to the group with CLI.

If the operator can manage the shared drive, this repo can grant the group shared-drive access with CLI.

Google Cloud CLI does not expose a first-class `gcloud drive shared-drives ...` command group. Shared-drive automation in this repo uses the active gcloud OAuth token with the Google Drive API. From the operator's point of view it is still CLI-driven.

## Google Limits That Matter

Current important shared-drive limits:

- shared drive item cap: `500,000` total items
- max folder nesting: `100` levels
- upload/copy limit: `750 GB` per user per 24 hours
- max individual upload/sync file size: `5 TB`
- direct shared-drive groups: `100`
- direct shared-drive members, groups plus individual accounts: `600`
- effective individuals through groups/direct users: `50,000`
- one group can be a member of up to `30,000` shared drives

Design implication: keep using a group for service accounts. Do not directly add all 100 service accounts to the shared drive unless there is a specific reason.

Service-account count is a Google Cloud project quota. Historically the common limit was 100 per project, and this repo has successfully created 100 in the confirmed project. For new organizations, verify the project quota named `Service Account Count`.

Primary references:

- https://support.google.com/a/users/answer/7338880
- https://support.google.com/a/users/answer/7212025
- https://developers.google.com/workspace/drive/api/guides/enable-shareddrives
- https://developers.google.com/workspace/drive/api/guides/limits
- https://docs.cloud.google.com/iam/docs/service-accounts-create

## Setup From Scratch

### 0. Agent-Friendly Naming And Bootstrap

For future automated runs, the human should first authenticate locally:

```bash
gcloud auth login --enable-gdrive-access --force
gcloud auth list
gcloud organizations list
gcloud billing accounts list
```

After that, a coding agent can infer most safe defaults from the active account and organization list. The only values it normally still needs from the human are:

- the destination `SHARED_DRIVE_ID`, or approval to create a new shared drive
- the source adapter path, `SOURCE_PATH`
- whether billing may be linked if organization policy blocks unbilled API enablement

Suggested discovery commands:

```bash
ACTIVE_ACCOUNT="$(gcloud auth list --filter=status:ACTIVE --format='value(account)' | head -n 1)"
ACCOUNT_DOMAIN="${ACTIVE_ACCOUNT#*@}"
gcloud organizations list --format='table(displayName,name,domain,directoryCustomerId)'
```

Pick the Workspace domain this way:

1. If `gcloud organizations list` shows exactly one organization domain, use that.
2. If one organization domain matches `ACCOUNT_DOMAIN`, use that.
3. If there are multiple plausible domains, ask the human to choose.

Suggested names:

```bash
WORKSPACE_DOMAIN="example.com"
DOMAIN_SLUG="$(printf '%s' "$WORKSPACE_DOMAIN" | tr '[:upper:]' '[:lower:]' | tr '._' '--' | tr -cd 'a-z0-9-' | cut -c1-16)"
RAND="$(LC_ALL=C tr -dc 'a-z0-9' < /dev/urandom | head -c 5)"

GOOGLE_CLOUD_PROJECT="sdmig-${DOMAIN_SLUG}-${RAND}"
GROUP_EMAIL="drive-migration-uploaders@${WORKSPACE_DOMAIN}"
GROUP_DISPLAY_NAME="Drive Migration Uploaders"
SHARED_DRIVE_NAME="Shared Drive Migration Target"
SA_PREFIX="drive-migrate"
RCLONE_REMOTE_PREFIX="gdrive-sa"
```

Why these names:

- `sdmig-...` keeps the project ID short enough for Google Cloud's 30-character project ID limit.
- `drive-migration-uploaders@...` describes the group purpose and is reusable for one shared-drive migration target.
- `Shared Drive Migration Target` is explicit enough for a new destination shared drive; teams should rename it to match their project or dataset.
- `drive-migrate-001` through `drive-migrate-100` stay under the service-account ID length limit.
- `gdrive-sa001:` through `gdrive-sa100:` are short rclone remote names.

Suggested `.env` update flow for an agent:

```bash
cp -n .env.example .env

python3 - <<'PY'
from pathlib import Path

updates = {
    "GOOGLE_CLOUD_PROJECT": "sdmig-example-abc12",
    "SA_PREFIX": "drive-migrate",
    "SA_COUNT": "100",
    "SHARED_DRIVE_ID": "0Axxxxxxxxxxxxxxxx",
    "SHARED_DRIVE_NAME": "Shared Drive Migration Target",
    "GROUP_EMAIL": "drive-migration-uploaders@example.com",
    "GROUP_DISPLAY_NAME": "Drive Migration Uploaders",
    "SOURCE_PATH": "/path/to/source-export",
    "RCLONE_REMOTE_PREFIX": "gdrive-sa",
}

path = Path(".env")
lines = path.read_text().splitlines()
out = []
for line in lines:
    if "=" not in line or line.lstrip().startswith("#"):
        out.append(line)
        continue
    key = line.split("=", 1)[0]
    if key in updates:
        out.append(f'{key}="{updates[key]}"')
    else:
        out.append(line)
path.write_text("\n".join(out) + "\n")
PY
```

Replace the example values before running that snippet. Do not write tokens, service-account JSON contents, or private keys into `.env`.

### 1. Login

Login with Drive scope:

```bash
gcloud auth login --enable-gdrive-access --force
gcloud auth list
gcloud auth print-access-token
```

Do not paste access tokens into chat, commits, logs, or docs.

Set the active account if needed:

```bash
gcloud config set account you@example.com
```

### 2. Create Or Select A GCP Project

Preferred: create a dedicated unbilled migration project.

Find organization and billing context:

```bash
gcloud organizations list
gcloud billing accounts list
```

Create a project:

```bash
PROJECT_ID="shared-drive-mig-$(LC_ALL=C tr -dc 'a-z0-9' < /dev/urandom | head -c 5)"
ORG_ID="1234567890"

gcloud projects create "$PROJECT_ID" \
  --organization="$ORG_ID" \
  --name="Shared Drive Migration" \
  --set-as-default
```

Check billing is not linked:

```bash
gcloud billing projects describe "$PROJECT_ID" \
  --format='json(projectId,billingEnabled,billingAccountName)'
```

Enable required APIs:

```bash
gcloud services enable \
  serviceusage.googleapis.com \
  iam.googleapis.com \
  cloudresourcemanager.googleapis.com \
  drive.googleapis.com \
  cloudidentity.googleapis.com \
  --project "$PROJECT_ID"
```

If an organization policy blocks API enablement without billing, stop and decide whether linking billing is acceptable. Do not link billing accidentally.

### 3. Configure `.env`

Copy the template:

```bash
cp .env.example .env
```

Fill these values:

```bash
GOOGLE_CLOUD_PROJECT="your-project-id"
SA_PREFIX="drive-migrate"
SA_COUNT="100"
SA_START_INDEX="1"
SHARED_DRIVE_ID="your-shared-drive-id"
GROUP_EMAIL="migration-uploaders@your-domain.com"
SOURCE_PATH="/path/to/source-export"
DEST_PATH=""
APPLY="0"
```

`SHARED_DRIVE_ID` is the ID from the shared drive URL, not the visible drive name.

### 4. Preflight

```bash
scripts/preflight.sh
```

This checks local tools, active gcloud account, project, and shared-drive config.

### 5. Create Service Accounts And Keys

Dry-run first:

```bash
scripts/bootstrap_gcp_service_accounts.sh
```

Apply:

```bash
APPLY=1 scripts/bootstrap_gcp_service_accounts.sh
```

Outputs:

```text
secrets/service-accounts/drive-migrate-001.json
secrets/service-accounts/drive-migrate-002.json
...
generated/service_accounts.csv
```

The script is rerunnable. It skips existing service accounts and existing key files. It also retries common Google IAM propagation delays and service-account creation rate limits.

Keep `secrets/` private. These JSON files are credentials.

### 6. Create Or Use A Google Group

Create a Google Group in the Workspace domain, for example:

```text
migration-uploaders@example.com
```

Set it in `.env`:

```bash
GROUP_EMAIL="migration-uploaders@example.com"
```

If your active gcloud user can create Cloud Identity groups, create it with CLI:

```bash
scripts/create_google_group.sh
APPLY=1 scripts/create_google_group.sh
```

By default, the script derives the organization domain from `GROUP_EMAIL`. You can override the domain or use a customer ID:

```bash
GROUP_ORGANIZATION="example.com" APPLY=1 scripts/create_google_group.sh
GROUP_CUSTOMER="C012abcde" APPLY=1 scripts/create_google_group.sh
```

Manual group creation is still fine when Workspace policy or permissions block CLI creation.

Generate group-member artifacts:

```bash
scripts/generate_workspace_group_artifacts.py
```

Important outputs:

```text
generated/group-email-batches/batch-001.csv
generated/group-email-batches/batch-002.csv
...
generated/workspace_group_emails_one_per_line.txt
generated/workspace_group_members.csv
generated/admin_directory_members.jsonl
```

Each `batch-*.csv` contains up to 10 comma-separated service-account emails for manual Google Group entry.

### 7. Add Service Accounts To The Google Group

Preferred CLI path, if your active gcloud user can manage the group:

```bash
scripts/add_service_accounts_to_group.sh
```

Test only a few first:

```bash
LIMIT=5 scripts/add_service_accounts_to_group.sh
```

The command treats existing members as non-fatal. In the confirmed setup:

```text
processed: 100
added:     99
already:   1
failed:    0
```

Manual fallback:

1. Open Google Group member management.
2. Open `generated/group-email-batches/batch-001.csv`.
3. Copy the comma-separated line.
4. Paste it into the add-member UI.
5. Repeat through the last batch file.

### 8. Add Group To Shared Drive

If you already have a shared drive, set `SHARED_DRIVE_ID` in `.env`.

If your active gcloud user can create shared drives, create or find one from CLI:

```bash
scripts/create_shared_drive.py
APPLY=1 WRITE_ENV=1 scripts/create_shared_drive.py
```

This writes the created/found ID back to `.env`.

Then grant the group access. Preferred CLI path, if your active gcloud user can manage the shared drive:

```bash
scripts/grant_shared_drive_access.py
APPLY=1 scripts/grant_shared_drive_access.py
```

Default role is `fileOrganizer`, which maps to Content manager.

Manual fallback:

1. Open the destination shared drive.
2. Manage members.
3. Add `GROUP_EMAIL`.
4. Give it Content manager access.

### 9. Create A Single Reference rclone Remote

For debugging and handoff, keep one simple rclone config named `gdrive_target:`.

Current confirmed file:

```text
generated/gdrive_target.reference.conf
```

Example:

```ini
[gdrive_target]
type = drive
scope = drive
service_account_file = /absolute/path/to/secrets/service-accounts/drive-migrate-001.json
team_drive = your-shared-drive-id
```

Validate:

```bash
rclone --config generated/gdrive_target.reference.conf listremotes
rclone --config generated/gdrive_target.reference.conf lsf gdrive_target:
```

If `lsf` exits successfully with no output, the shared drive root may simply be empty.

### 10. Generate Full rclone Config

```bash
scripts/generate_rclone_config.py
```

Outputs:

```text
generated/rclone.conf
generated/rclone.manifest.csv
```

Remote names:

```text
gdrive-sa001:
gdrive-sa002:
...
gdrive-sa100:
```

Each remote uses:

```ini
type = drive
scope = drive
service_account_file = /absolute/path/to/service-account.json
team_drive = SHARED_DRIVE_ID
```

### 11. Validate Remotes

Validate first five:

```bash
REMOTE_LIMIT=5 scripts/validate_rclone_remotes.sh
```

Validate all:

```bash
scripts/validate_rclone_remotes.sh
```

If validation fails:

- confirm the service account is in the Google Group
- confirm the Google Group is on the shared drive
- wait for group/shared-drive permission propagation
- confirm `SHARED_DRIVE_ID`
- confirm the key file is valid JSON and not zero bytes

Check keys:

```bash
find secrets/service-accounts -name '*.json' | wc -l
jq -r '.client_email' secrets/service-accounts/drive-migrate-001.json
```

Recreate a bad key:

```bash
rm -f secrets/service-accounts/drive-migrate-001.json
gcloud iam service-accounts keys create secrets/service-accounts/drive-migrate-001.json \
  --iam-account "drive-migrate-001@$GOOGLE_CLOUD_PROJECT.iam.gserviceaccount.com" \
  --project "$GOOGLE_CLOUD_PROJECT"
chmod 600 secrets/service-accounts/drive-migrate-001.json
```

## Upload Workflow

Set source path in `.env`:

```bash
SOURCE_PATH="/path/to/source-export"
DEST_PATH="optional-folder-inside-shared-drive"
```

Dry-run one remote:

```bash
DRY_RUN=1 scripts/rclone_copy_shared_drive.sh gdrive-sa001
```

Run one remote for real:

```bash
DRY_RUN=0 scripts/rclone_copy_shared_drive.sh gdrive-sa001
```

Create a top-level round-robin upload plan:

```bash
scripts/plan_round_robin_upload.py
```

Outputs:

```text
generated/upload_plan.csv
generated/upload_commands.sh
```

The generated commands include `--dry-run` by default. Review before removing `--dry-run`.

## Rclone Large Archive Defaults

The repo defaults are tuned for large `tar.zst` package uploads to Google shared drives, not many small-file copies. References: [rclone Google Drive docs](https://rclone.org/drive/) and [rclone install docs](https://rclone.org/install/).

```bash
RCLONE_DRIVE_CHUNK_SIZE="512M"
RCLONE_TRANSFERS="1"
RCLONE_CHECKERS="4"
RCLONE_TPSLIMIT="5"
RCLONE_TPSLIMIT_BURST="0"
RCLONE_RETRIES="3"
RCLONE_LOW_LEVEL_RETRIES="20"
RCLONE_RETRIES_SLEEP="30s"
RCLONE_DRIVE_STOP_ON_UPLOAD_LIMIT="1"
MODAL_MAX_UPLOADS_PER_REMOTE="0"
```

Reasoning:

- rclone's Google Drive backend documents `--drive-chunk-size` as the upload chunk size and notes that larger chunks can improve upload performance at the cost of memory per transfer.
- Each Modal worker uploads one package at a time, so `RCLONE_TRANSFERS=1` keeps memory predictable while still allowing concurrency across workers and service accounts.
- `512M` is the default package-upload chunk size. Try `1G` only after confirming container memory and network behavior.
- `RCLONE_TPSLIMIT=5` and burst `0` reduce API spikes. Lower this to `2` or `3` if Drive returns rate-limit errors.
- `--drive-stop-on-upload-limit` makes Google's daily upload-limit response fatal instead of repeatedly wasting time on a package that cannot complete that day.
- `MODAL_MAX_UPLOADS_PER_REMOTE=0` means unlimited successful packages per service-account remote. Set it to a small number when testing Drive behavior.
- Use direct rclone commands (`copy`, `copyto`, or `rcat`) for migration uploads. Avoid uploading through an rclone mount for this workflow.

The Modal image installs rclone with the official rclone install script because distribution packages can lag the current stable release.

## Modal Volume Adapter

Use the Modal adapter when source data lives in a Modal Volume and should be packaged inside Modal before upload. This avoids downloading the volume to the local machine.

Default shape:

```text
Modal Volume
  -> fixed-depth package units
  -> tar.zst archive per unit
  -> package.index.json per unit
  -> files.index.jsonl.zst per unit
  -> rclone rcat or copyto to shared drive
```

Example for a structure like `source-volume/language/podcast-folder/...`:

```bash
MODAL_SOURCE_VOLUME_NAME="source-volume-name"
MODAL_SOURCE_PREFIX="aa"
MODAL_UNIT_DEPTH="1"
MODAL_DEST_PREFIX="source-volume-name"
```

Generate and upload the private Modal rclone bundle:

```bash
scripts/generate_modal_rclone_bundle.py
scripts/upload_modal_rclone_bundle.sh
```

Discover a small package plan first:

```bash
MODAL_SOURCE_VOLUME_NAME="source-volume-name" \
  scripts/run_modal_volume_adapter.sh discover \
  --source-prefix aa \
  --unit-depth 1 \
  --dest-prefix source-volume-name \
  --plan-path plans/source-aa-smoke.jsonl \
  --limit 3
```

`discover` uses the authenticated Modal SDK/CLI profile and `Volume.listdir(..., recursive=True)` to read file metadata. No Modal token should be pasted into the repo or chat if `modal volume list` already works. The planner sums file-entry sizes from Modal metadata; it does not download file contents. A fallback `discover-mounted` command exists for comparing mounted filesystem behavior.

Run only worker 0 against the 10-worker plan:

```bash
MODAL_SOURCE_VOLUME_NAME="source-volume-name" \
  scripts/run_modal_volume_adapter.sh upload \
  --plan-path plans/source-aa-smoke.jsonl \
  --worker-count 10 \
  --worker-index 0 \
  --remote-group-size 10 \
  --upload-mode staged \
  --max-package-bytes 700GiB \
  --warn-package-bytes 650GiB \
  --dry-run \
  --limit 1
```

Run worker 0 for one real package after the dry-run is reviewed:

```bash
MODAL_SOURCE_VOLUME_NAME="source-volume-name" \
  scripts/run_modal_volume_adapter.sh upload \
  --plan-path plans/source-aa-smoke.jsonl \
  --worker-count 10 \
  --worker-index 0 \
  --remote-group-size 10 \
  --upload-mode staged \
  --max-package-bytes 700GiB \
  --warn-package-bytes 650GiB \
  --no-dry-run \
  --limit 1
```

Verify the destination package folder:

```bash
rclone --config generated/rclone.conf lsf \
  "gdrive-sa001:source-volume-name/aa/example-package-folder" \
  --max-depth 1
```

Run a tiny synthetic upload test instead of touching source data:

```bash
MODAL_SOURCE_VOLUME_NAME="source-volume-name" \
  scripts/run_modal_volume_adapter.sh smoke \
  --worker-count 1 \
  --worker-index 0 \
  --dest-prefix _sdmig_smoke \
  --no-dry-run
```

Clean only the exact smoke prefix after testing:

```bash
MODAL_SOURCE_VOLUME_NAME="source-volume-name" \
  scripts/run_modal_volume_adapter.sh cleanup \
  --dest-prefix _sdmig_smoke/<run-id> \
  --no-dry-run
```

The adapter defaults to `MODAL_MAX_CONTAINERS=10`, `MODAL_WORKER_COUNT=10`, and `MODAL_REMOTE_GROUP_SIZE=10`. That means each Modal worker gets a lane of the plan and rotates through 10 rclone remotes/service accounts. Keep `--dry-run` until the plan, destination paths, and package format are confirmed.

For Drive-sensitive runs, prefer fewer upload workers with more service accounts per worker. This keeps package uploads serial or near-serial while still rotating through many service-account remotes:

```bash
RCLONE_TPSLIMIT=1 \
MODAL_MAX_CONTAINERS=1 \
MODAL_SOURCE_VOLUME_NAME="source-volume-name" \
  scripts/run_modal_volume_adapter.sh upload \
  --plan-path plans/source-units.jsonl \
  --worker-count 1 \
  --worker-index 0 \
  --remote-group-size 100 \
  --upload-mode stream \
  --max-uploads-per-remote 1 \
  --no-dry-run \
  --limit 100
```

In this mode, one Modal container processes all plan rows, rotates across up to 100 remotes, and retires a remote when rclone reports a Drive upload/rate-limit error such as `userRateLimitExceeded`. `--limit` caps total package attempts for the run; `--max-uploads-per-remote` caps successful packages per service account. Raise both only after checking the worker status JSONL under `/state/runs/worker-000/...`.

Modal Volume size note: `modal volume ls --json` exposes direct file sizes, but directories report `0 B`; Modal's dashboard exposes whole-volume size, not recursive package-unit size. The adapter therefore uses the Modal SDK recursive listing API to calculate package-unit size from file metadata. Real uploads also calculate the package index before archiving and skip units larger than `--max-package-bytes`.

Package naming:

```text
<unit>/<unit>.tar.zst
<unit>/<unit>.package.index.json
<unit>/<unit>.files.index.jsonl.zst
```

Size guards:

```bash
--max-package-bytes 700GiB
--warn-package-bytes 650GiB
```

Upload modes:

```text
stream
  /src -> tar -> zstd -> rclone rcat

staged
  /src -> /cache/workers/<worker>/...tar.zst -> rclone copyto
```

`staged` mode uses `MODAL_CACHE_VOLUME_NAME`, default `sdmig-cache`, and writes worker-specific paths under `/cache/workers/000`, `/cache/workers/001`, etc. The source volume remains read-only.

## Script Reference

```text
scripts/preflight.sh
  Checks local tools and required env values.

scripts/bootstrap_gcp_service_accounts.sh
  Enables APIs, creates service accounts, creates JSON keys, writes inventory.

scripts/generate_workspace_group_artifacts.py
  Produces batch files and CSV/JSONL group-member artifacts.

scripts/create_google_group.sh
  Optionally creates GROUP_EMAIL through gcloud identity groups.

scripts/add_service_accounts_to_group.sh
  Adds service-account emails to GROUP_EMAIL through gcloud identity groups.

scripts/create_shared_drive.py
  Optionally creates or finds SHARED_DRIVE_NAME through the Drive API using gcloud auth.

scripts/grant_shared_drive_access.py
  Grants GROUP_EMAIL access to SHARED_DRIVE_ID with Drive API permissions.create.

scripts/generate_rclone_config.py
  Generates one rclone remote per service-account key.

scripts/validate_rclone_remotes.sh
  Runs rclone lsf against each generated remote.

scripts/rclone_copy_shared_drive.sh
  Runs a guarded rclone copy to one remote.

scripts/plan_round_robin_upload.py
  Assigns top-level source entries across generated remotes.

scripts/generate_modal_rclone_bundle.py
  Creates an ignored rclone/key bundle for Modal workers.

scripts/upload_modal_rclone_bundle.sh
  Uploads the ignored Modal rclone bundle into a private Modal Volume.

scripts/run_modal_volume_adapter.sh
  Runs the Modal Volume source adapter.
```

## Generated Files

Important generated files:

```text
generated/service_accounts.csv
generated/rclone.conf
generated/rclone.manifest.csv
generated/gdrive_target.reference.conf
generated/group-email-batches/batch-*.csv
generated/workspace_group_emails_one_per_line.txt
generated/upload_plan.csv
generated/upload_commands.sh
generated/modal-rclone-bundle/
logs/*.log
```

Important secret files:

```text
secrets/service-accounts/*.json
```

These paths are ignored by git.

## Safety

- Do not commit `.env`.
- Do not commit `secrets/`.
- Do not paste service-account JSON contents anywhere.
- Do not paste access tokens anywhere.
- Keep generated rclone configs private because they point to credential files.
- Prefer a dedicated unbilled migration project.
- Do not unlink billing from an existing project unless you know it has no unrelated workloads.
- Do not delete service accounts or keys until migration verification is complete.

## Cleanup

After migration and verification, you can remove local keys:

```bash
rm -rf secrets/service-accounts
```

If the project is disposable:

```bash
gcloud projects delete "$GOOGLE_CLOUD_PROJECT"
```

Deleting the project deletes its service accounts and invalidates their keys.

## Troubleshooting

### `gcloud` token refresh fails

Run:

```bash
gcloud auth login --enable-gdrive-access --force
gcloud config set account you@example.com
```

### `PERMISSION_DENIED` enabling APIs

Check:

```bash
gcloud config get-value project
gcloud projects describe "$GOOGLE_CLOUD_PROJECT"
```

You need permission to enable services in that project.

### Service account key creation says account does not exist

Google IAM can lag immediately after account creation. Rerun:

```bash
APPLY=1 scripts/bootstrap_gcp_service_accounts.sh
```

The script retries and skips completed work.

### Service account creation hits 429 quota

Google can throttle service-account creation per minute. Rerun:

```bash
APPLY=1 scripts/bootstrap_gcp_service_accounts.sh
```

The script has retry handling for this.

### rclone validates but upload fails

Check:

- group membership has propagated
- group has Content manager access to the shared drive
- `SHARED_DRIVE_ID` is correct
- destination path is valid
- shared drive is not near the 500,000 item cap

### `rclone lsf` succeeds with no output

That can mean the shared drive root is empty. Success exit code is what matters.

### Shared drive creation fails

The active Google account may not be allowed to create shared drives, or the Workspace admin may have disabled shared-drive creation. Create the shared drive manually, set `SHARED_DRIVE_ID` in `.env`, and continue from the group grant step.
