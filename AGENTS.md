# AGENTS.md

This file is the operator runbook for turning a Google Workspace shared drive into a migration target using many Google Cloud service accounts and rclone.

The intended workflow is repeatable for a new person, new Google Workspace organization, or new shared drive. It is deliberately shared-drive-only on the target side, with source adapters producing migration plans and feeding rclone-compatible uploads.

## Scope

This project handles:

- creating or configuring a Google Cloud project for migration service accounts
- enabling the required Google APIs
- creating service accounts and JSON keys
- producing copy-paste batches of service-account emails for manual Google Group membership
- generating rclone remotes for a Google shared drive
- validating service-account access to the shared drive
- running guarded rclone copy jobs
- documenting source-adapter handoff through plans, packages, or `SOURCE_PATH`

This project does not require:

- My Drive uploads
- domain-wide delegation for upload
- Google Workspace Admin SDK automation
- billing-linked compute resources

## Preferred Architecture

Use one migration-only Google Cloud project per migration/org. Prefer creating it without billing linked unless some organization policy blocks API usage without billing.

Access model:

1. Create service accounts in the migration GCP project.
2. Add those service-account emails to one Google Group in the Workspace domain.
3. Add that Google Group to the destination shared drive.
4. Generate one rclone Drive remote per service-account key using `team_drive = SHARED_DRIVE_ID`.

This avoids adding 100 service accounts directly to the shared drive and keeps future cleanup simple.

Source model:

1. A source adapter discovers source units and produces a plan.
2. The adapter either exposes raw files or packages each unit.
3. The shared-drive target scripts upload with rclone remotes that point at `SHARED_DRIVE_ID`.

Modal volumes are one possible source adapter. The Google shared-drive target setup should remain independent from Modal-specific assumptions.

## Source Adapter Questions

Before writing or running an adapter, ask the operator how the source should be migrated:

- Which adapter is this run using: `local-path`, `modal-volume`, or something else?
- What source root/prefix is in scope?
- What is one migration unit: a file, top-level folder, fixed-depth folder, or manifest row?
- Should upload preserve raw files, or package each unit?
- If packaging, which format: `tar.zst`, `zip`, `tar`, or another format?
- Should each package have a small package index and a compressed file index next to it?
- Where should packages land inside the shared drive?
- What source-side concurrency limit should be respected?
- How many Google Drive remotes/service accounts should each worker rotate through?
- Should the first execution be dry-run, synthetic smoke, one-worker, limited real run, or full run?

For sources with millions of files, prefer package-per-unit output plus `<unit>.package.index.json` and `<unit>.files.index.jsonl.zst`; raw file upload can hit shared-drive item limits even when storage capacity is not the issue.

## Operating Style

Trust the human operator on source semantics: source layout, package boundaries, account limits they know from their provider, and what should land in the shared drive. Verify platform behavior before relying on it: Modal API metadata shape, Drive/rclone behavior, mounted paths, auth state, and cleanup scope.

Default escalation path:

1. Ask the adapter-shape questions above.
2. Use existing CLI auth when it works; do not ask for pasted tokens.
3. Discover with API metadata where available.
4. Smoke upload synthetic data.
5. Launch one worker with `--dry-run --limit 1`.
6. Launch one worker with `--no-dry-run --limit 1`.
7. Verify with rclone listing.
8. Only then widen concurrency.

## Confirmed Working Path

The following has been confirmed in a private Google Workspace. Do not commit real project IDs, group emails, shared-drive IDs, local paths, tokens, or credential paths into public docs.

- a dedicated unbilled project can be used for this flow
- the required APIs can be enabled without linking billing
- 100 service accounts can be created in one project
- service-account JSON keys can be generated locally
- service-account emails can be added directly to a Google Group with `gcloud identity groups memberships add`
- the Google Group can be granted shared-drive access from CLI with the Drive API
- rclone can authenticate to the shared drive using a service-account key and `team_drive`
- Modal Volume planning can use the local authenticated Modal SDK/CLI profile without pasted tokens
- Modal recursive Volume metadata listing returns direct file sizes, but directories report `0 B`
- a Modal staged upload can package one source unit as `tar.zst`, upload package indexes, and verify through rclone

Note: `gcloud` has Cloud Identity group commands, but no first-class Google Drive shared-drive command group. Shared-drive creation and permission grants should be automated with the Drive API using `gcloud auth print-access-token`.

Public example values:

```text
Project: shared-drive-migration-example
Workspace group: migration-uploaders@example.com
Shared drive ID: 0Axxxxxxxxxxxxxxxx
Reference remote: gdrive_target:
```

Treat those values as examples for future organizations. New operators should replace them with their own project ID, Workspace group, and shared-drive ID.

## Shared Drive And Quota Limits

These are the limits that matter for this migration design. Verify them again before a new long-running migration, because Google can change product quotas.

Official Google Workspace shared-drive limits:

- A shared drive can contain up to 500,000 items total, including files, folders, shortcuts, and trashed items.
- A folder in a shared drive can be nested up to 100 levels deep.
- A file in a shared drive can be directly shared with up to 100 groups.
- Each user can upload and copy 750 GB to Drive within 24 hours.
- Individual uploads and syncs can be up to 5 TB.
- Files larger than 750 GB cannot be copied; download then upload instead.
- A shared drive can have up to 100 groups as direct members.
- A shared drive can have up to 600 total direct members, counting groups plus individual accounts.
- A shared drive can effectively include up to 50,000 individuals through direct users and group members.
- Each group can be a member of up to 30,000 shared drives.
- Drive UI lists up to 1,000 shared drives in the left navigation; other shared drives remain accessible by URL/search.
- Shared drives can be hidden by default for large groups: directly shared groups over 1,000 people and indirect membership over 2,500 people can trigger hidden-by-default behavior for later members.

Drive API shared-drive constraints:

- API calls that touch shared-drive files need shared-drive support, for example `supportsAllDrives=true`.
- For listing a specific shared drive with the Drive API, use parameters like `corpora=drive`, `driveId=...`, `includeItemsFromAllDrives=true`, and `supportsAllDrives=true`.
- rclone handles the shared-drive flags when the Drive remote is configured with `team_drive = SHARED_DRIVE_ID`.

Google Cloud IAM/service-account constraints:

- Service accounts per project are controlled by the project quota named `Service Account Count`; check the quota for each project instead of assuming it is always the same.
- The historical AutoRclone ecosystem assumed 100 service accounts per project and rotated under the 750 GB/day upload ceiling.
- In the confirmed private run, one unbilled project successfully created 100 service accounts.

Migration design implications:

- Use a Google Group for service accounts. One group counts as one direct shared-drive member and avoids adding 100 service accounts directly to the shared drive.
- Keep the shared drive far below 500,000 items when possible. Many small files are more likely to hit item/API/search pain than storage pain.
- Avoid extremely deep folder trees; keep nesting well below 100 levels.
- Do not assume 100 service accounts means a guaranteed 75 TB/day plan. The 750 GB/day limit is official per user, but service-account behavior and enforcement can change. Treat multi-service-account upload scaling as best-effort and validate with small batches.
- For very large migrations, shard top-level folders across remotes and preserve logs so failed shards can be retried.

Primary docs:

- Google Workspace shared drive limits: https://support.google.com/a/users/answer/7338880
- Google Workspace shared drive overview and group membership guidance: https://support.google.com/a/users/answer/7212025
- Drive API shared-drive support: https://developers.google.com/workspace/drive/api/guides/enable-shareddrives
- Drive API limits/errors: https://developers.google.com/workspace/drive/api/guides/limits and https://developers.google.com/workspace/drive/api/guides/handle-errors
- IAM service account quotas: https://docs.cloud.google.com/iam/docs/service-accounts-create

## Required Local CLI Tools

Install and verify:

```bash
gcloud version
rclone version
jq --version
python3 --version
```

On macOS, typical install paths are fine. This repo already expects `gcloud`, `rclone`, `jq`, and `python3` to be on `PATH`.

Shell compatibility note: macOS ships an older Bash. Scripts should avoid Bash 4-only features like `mapfile` unless `/usr/local/bin/bash` or another modern Bash is explicitly required.

## Google Permissions Needed

The human operator should have:

- permission to create or manage a Google Cloud project
- permission to enable APIs in that project
- permission to create service accounts and service-account keys
- permission to create/manage a Google Group, or access to someone who can do this manually
- permission to add the group to the target shared drive

For the simple/manual group workflow, Workspace admin API access is not needed.

## Auth

Login with Drive scope included:

```bash
gcloud auth login --enable-gdrive-access --force
gcloud auth list
gcloud auth print-access-token
```

Do not paste access tokens into chats, issues, logs, or docs. If a token is exposed, it is short-lived, but a clean re-login/revoke is still reasonable after setup.

Set the active account if needed:

```bash
gcloud config set account you@example.com
```

## Agent-Friendly Naming And Bootstrap

For future fully automated runs, assume the human has already completed:

```bash
gcloud auth login --enable-gdrive-access --force
gcloud auth list
gcloud organizations list
gcloud billing accounts list
```

The agent should then infer safe defaults before asking questions.

Discovery:

```bash
ACTIVE_ACCOUNT="$(gcloud auth list --filter=status:ACTIVE --format='value(account)' | head -n 1)"
ACCOUNT_DOMAIN="${ACTIVE_ACCOUNT#*@}"
gcloud organizations list --format='table(displayName,name,domain,directoryCustomerId)'
```

Domain selection:

1. Use the only organization domain if there is exactly one.
2. Otherwise use the organization domain matching `ACCOUNT_DOMAIN`.
3. If several domains are plausible, ask the human to choose.

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

Naming rationale:

- `sdmig-...` is short and leaves room for a random suffix under the 30-character project ID limit.
- `drive-migration-uploaders@...` is explicit and reusable for the service-account group.
- `Shared Drive Migration Target` should be renamed by the operator or source adapter to match the dataset/project.
- `drive-migrate-001` etc. are short enough for service-account IDs.
- `gdrive-sa001:` etc. are compact rclone remote names.

Agent behavior:

- Ask the human for `SHARED_DRIVE_ID` if it is not already present.
- If the human wants a new shared drive and the account has permission, run `scripts/create_shared_drive.py` with `WRITE_ENV=1`.
- Ask the human for `SOURCE_PATH` or run the relevant source adapter that creates it.
- Prefer an unbilled dedicated project.
- Do not link billing unless the human explicitly approves it.
- Prefer CLI group creation with `scripts/create_google_group.sh`; fall back to manual group creation if permissions fail.
- After creating or selecting names, update `.env` with placeholders replaced, never with tokens or JSON key contents.
- Continue with preflight, service-account creation, group creation, group membership, shared-drive grant, rclone generation, and validation.

Suggested `.env` update pattern:

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

## Create A New Unbilled Migration Project

List organization and billing context:

```bash
gcloud organizations list
gcloud billing accounts list
```

Create a dedicated project under the Workspace organization:

```bash
PROJECT_ID="shared-drive-mig-$(LC_ALL=C tr -dc 'a-z0-9' < /dev/urandom | head -c 5)"
ORG_ID="1234567890"

gcloud projects create "$PROJECT_ID" \
  --organization="$ORG_ID" \
  --name="Shared Drive Migration" \
  --set-as-default
```

Confirm billing is not linked:

```bash
gcloud billing projects describe "$PROJECT_ID" \
  --format='json(projectId,billingEnabled,billingAccountName)'
```

Expected:

```json
{
  "billingAccountName": "",
  "billingEnabled": false
}
```

Enable only the APIs needed for this workflow:

```bash
gcloud services enable \
  serviceusage.googleapis.com \
  iam.googleapis.com \
  cloudresourcemanager.googleapis.com \
  drive.googleapis.com \
  cloudidentity.googleapis.com \
  --project "$PROJECT_ID"
```

If an organization policy requires billing for API enablement, stop and decide whether to link billing. Do not link billing automatically.

## Configure This Repo

Copy the example env file:

```bash
cp .env.example .env
```

Edit `.env`:

```bash
GOOGLE_CLOUD_PROJECT="shared-drive-mig-abc12"
SA_PREFIX="drive-migrate"
SA_COUNT="100"
SA_START_INDEX="1"
SHARED_DRIVE_ID="0Axxxxxxxxxxxxxxxx"
GROUP_EMAIL="migration-uploaders@example.com"
KEY_DIR="secrets/service-accounts"
INVENTORY_FILE="generated/service_accounts.csv"
RCLONE_CONFIG_OUT="generated/rclone.conf"
RCLONE_REMOTE_PREFIX="gdrive-sa"
APPLY="0"
```

`SHARED_DRIVE_ID` is the ID from the shared drive URL, not the visible drive name.

## Preflight

Run:

```bash
scripts/preflight.sh
```

This checks local tools, active `gcloud` account, project, and shared-drive config.

## Create Service Accounts

Dry-run first:

```bash
scripts/bootstrap_gcp_service_accounts.sh
```

Apply:

```bash
APPLY=1 scripts/bootstrap_gcp_service_accounts.sh
```

Outputs:

- `secrets/service-accounts/drive-migrate-001.json`
- `secrets/service-accounts/drive-migrate-002.json`
- ...
- `generated/service_accounts.csv`

The script is idempotent enough to rerun. It skips existing service accounts and existing key files.

Keep `secrets/` private. These JSON files are credentials.

## Manual Google Group Workflow

Create the group manually in Google Workspace, for example:

```text
migration-uploaders@example.com
```

## CLI Google Group Creation Workflow

If the active `gcloud` user can create Cloud Identity groups, the Google Group can be created automatically:

```bash
scripts/create_google_group.sh
APPLY=1 scripts/create_google_group.sh
```

The script uses:

```bash
gcloud identity groups create "$GROUP_EMAIL" \
  --organization="$GROUP_ORGANIZATION" \
  --display-name="$GROUP_DISPLAY_NAME" \
  --description="$GROUP_DESCRIPTION" \
  --group-type="$GROUP_TYPE" \
  --with-initial-owner="$WITH_INITIAL_OWNER"
```

Defaults:

- `GROUP_ORGANIZATION` is derived from the domain part of `GROUP_EMAIL`.
- `GROUP_TYPE` defaults to `discussion`.
- `WITH_INITIAL_OWNER` defaults to `with-initial-owner`.
- Existing groups are described and treated as already complete.

Use `GROUP_CUSTOMER` instead of `GROUP_ORGANIZATION` if a customer ID is preferred.

Manual creation is still a valid fallback when Workspace policy or permissions block CLI creation.

Then generate member artifacts:

```bash
scripts/generate_workspace_group_artifacts.py
```

Important output:

```text
generated/group-email-batches/batch-001.csv
generated/group-email-batches/batch-002.csv
...
```

Each `batch-*.csv` contains up to 10 comma-separated service-account emails:

```text
drive-migrate-001@project.iam.gserviceaccount.com,drive-migrate-002@project.iam.gserviceaccount.com
```

Use these files to manually add members to the Google Group in batches.

Other outputs:

- `generated/workspace_group_emails_one_per_line.txt`
- `generated/workspace_group_members.csv`
- `generated/admin_directory_members.jsonl`
- `generated/workspace_group_next_steps.txt`

## CLI Google Group Membership Workflow

If the active `gcloud` user can manage group memberships and the Workspace group accepts service-account members, add them directly:

```bash
scripts/add_service_accounts_to_group.sh
```

Test only the first few:

```bash
LIMIT=5 scripts/add_service_accounts_to_group.sh
```

The script reads `generated/service_accounts.csv` and runs:

```bash
gcloud identity groups memberships add \
  --group-email="$GROUP_EMAIL" \
  --member-email="SERVICE_ACCOUNT_EMAIL" \
  --roles=MEMBER
```

Existing members are treated as non-fatal.

In the confirmed private run, this added 99 new members and treated the first test service account as already present:

```text
processed: 100
added:     99
already:   1
failed:    0
```

## Add Group To Shared Drive

Manual path:

1. Open the destination shared drive in Google Drive.
2. Manage members.
3. Add `GROUP_EMAIL`.
4. Use Content manager access for normal migration writes.

Create/find shared drive from CLI, if the active `gcloud` user can create shared drives:

```bash
scripts/create_shared_drive.py
APPLY=1 WRITE_ENV=1 scripts/create_shared_drive.py
```

The script:

- uses `gcloud auth print-access-token`
- searches for an existing shared drive with `SHARED_DRIVE_NAME`
- creates one with Drive API `drives.create` if no match exists
- writes `SHARED_DRIVE_ID` and `SHARED_DRIVE_NAME` to `.env` when `WRITE_ENV=1`

CLI permission grant, if the active `gcloud` user can manage the shared drive:

```bash
scripts/grant_shared_drive_access.py
APPLY=1 scripts/grant_shared_drive_access.py
```

Default role is `fileOrganizer`, which maps to Content manager.

The CLI grant calls the Drive API `permissions.create` endpoint with `supportsAllDrives=true`. A successful response looks like:

```json
{
  "kind": "drive#permission",
  "role": "fileOrganizer",
  "type": "group"
}
```

If this fails, the active `gcloud` user probably cannot manage that shared drive. Add the group manually in Drive instead.

## Create A Single Reference rclone Remote

For debugging or handoff, it is useful to create one simple rclone config named `gdrive_target:` before generating all 100 remotes.

Example file:

```text
generated/gdrive_target.reference.conf
```

Example contents:

```ini
[gdrive_target]
type = drive
scope = drive
service_account_file = /absolute/path/to/secrets/service-accounts/drive-migrate-001.json
team_drive = 0Axxxxxxxxxxxxxxxx
```

Validate the reference remote:

```bash
rclone --config generated/gdrive_target.reference.conf listremotes
rclone --config generated/gdrive_target.reference.conf lsf gdrive_target:
```

If `lsf` exits `0` with no output, that can simply mean the shared drive root is empty. The important part is the command succeeds.

Also verify the key file is not empty or malformed:

```bash
ls -l secrets/service-accounts/drive-migrate-001.json
jq -r '.client_email' secrets/service-accounts/drive-migrate-001.json
```

If a key file is zero bytes because an earlier key creation was interrupted, recreate that key:

```bash
rm -f secrets/service-accounts/drive-migrate-001.json
gcloud iam service-accounts keys create secrets/service-accounts/drive-migrate-001.json \
  --iam-account "drive-migrate-001@$PROJECT_ID.iam.gserviceaccount.com" \
  --project "$PROJECT_ID"
chmod 600 secrets/service-accounts/drive-migrate-001.json
```

## Generate rclone Config

After the group membership is done and has had time to propagate:

```bash
scripts/generate_rclone_config.py
```

Outputs:

- `generated/rclone.conf`
- `generated/rclone.manifest.csv`

Each remote looks like:

```ini
[gdrive-sa001]
type = drive
scope = drive
service_account_file = /absolute/path/to/secrets/service-accounts/drive-migrate-001.json
team_drive = 0Axxxxxxxxxxxxxxxx
```

## Validate Shared Drive Access

Check all remotes:

```bash
scripts/validate_rclone_remotes.sh
```

Check only first few:

```bash
REMOTE_LIMIT=5 scripts/validate_rclone_remotes.sh
```

If validation fails:

- confirm service-account emails were added to the group
- confirm the group is a member of the shared drive
- wait for Google Group propagation
- confirm `SHARED_DRIVE_ID` is correct

## Upload

Set source and destination in `.env`:

```bash
SOURCE_PATH="/path/to/source-export"
DEST_PATH="optional-folder-inside-shared-drive"
```

Test one remote:

```bash
DRY_RUN=1 scripts/rclone_copy_shared_drive.sh gdrive-sa001
```

Run for real:

```bash
DRY_RUN=0 scripts/rclone_copy_shared_drive.sh gdrive-sa001
```

For large top-level sharding:

```bash
scripts/plan_round_robin_upload.py
```

Outputs:

- `generated/upload_plan.csv`
- `generated/upload_commands.sh`

The generated commands default to `--dry-run`. Review them before removing `--dry-run`.

## Rclone Large Archive Defaults

Use these defaults for Google shared-drive uploads when the source adapter produces large `tar.zst` package files. References: [rclone Google Drive docs](https://rclone.org/drive/) and [rclone install docs](https://rclone.org/install/).

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

Operator notes:

- rclone's Google Drive docs define `--drive-chunk-size` as the upload chunk size and warn that bigger chunks use more memory per transfer.
- `512M` is the starting value for package uploads. `1G` can be tested later only if worker memory and retry behavior are acceptable.
- Keep `RCLONE_TRANSFERS=1` for Modal package workers. Concurrency should come from separate workers/service accounts, not multiple huge uploads inside one container.
- `RCLONE_TPSLIMIT=5` with burst `0` is intentionally conservative. Lower it if Google returns rate-limit errors.
- Keep `RCLONE_DRIVE_STOP_ON_UPLOAD_LIMIT=1`; a worker should stop when Drive reports the daily upload limit.
- `MODAL_MAX_UPLOADS_PER_REMOTE=0` means no per-remote success cap. Use a small cap for Drive-sensitive tests.
- Prefer direct rclone operations (`copy`, `copyto`, `rcat`) over rclone mount for migration uploads.
- The Modal image should install rclone from the official rclone installer, not the Debian package, because distro packages can be old.

## Modal Volume Adapter

Use the Modal adapter only when the source data is already in a Modal Volume or can be read efficiently from one. Keep the target side generic: the adapter emits packages and uses the generated shared-drive rclone remotes.

Default Modal adapter behavior:

- mount source volume read-only at `/src`
- mount private rclone/key bundle at `/creds`
- mount writable cache Volume v2 at `/cache`
- write run plans and status to a separate Modal state volume
- package units as `tar.zst`
- upload archives with `rclone rcat` in stream mode or `rclone copyto` in staged mode
- upload `<unit>.package.index.json` next to each archive
- upload `<unit>.files.index.jsonl.zst` next to each archive
- default to `10` Modal workers and `10` rclone remotes per worker

Do not rely on `modal volume ls --json` for recursive directory sizes. It reports direct file sizes but directories as `0 B`. Normal adapter discovery uses `Volume.listdir(..., recursive=True)` through the authenticated Modal SDK and sums file-entry sizes from metadata. Real upload workers still recalculate package indexes and must skip units above `--max-package-bytes` instead of producing an archive that can exceed the Drive daily upload ceiling.

Prepare Modal credentials:

```bash
scripts/generate_modal_rclone_bundle.py
scripts/upload_modal_rclone_bundle.sh
```

Discover a limited plan:

```bash
MODAL_SOURCE_VOLUME_NAME="source-volume-name" \
  scripts/run_modal_volume_adapter.sh discover \
  --source-prefix aa \
  --unit-depth 1 \
  --dest-prefix source-volume-name \
  --plan-path plans/source-aa-smoke.jsonl \
  --limit 3
```

Normal `discover` uses the local authenticated Modal SDK/CLI profile and `Volume.listdir(..., recursive=True)` metadata. Do not ask the human to paste Modal tokens if `modal volume list` already works. Direct file entries include byte sizes; directory entries report `0 B`, so aggregate unit size must be calculated by summing recursive file entries. Use `discover-mounted` only as a fallback comparison path.

Launch only worker 0 from the 10-worker plan:

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

Launch one real worker only after the dry-run output is reviewed:

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

If Drive returns `userRateLimitExceeded` or broad upload-limit errors, do not scale by adding more Modal containers. Switch to serial remote rotation first:

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

In this mode, one worker owns the full plan and rotates across up to 100 service-account remotes. The worker retires a remote from its active rotation when rclone reports a Drive upload/rate-limit response. The returned summary includes `active_remotes`, `retired_remotes`, and `remote_upload_counts`; inspect those before raising `--limit`, `--max-uploads-per-remote`, or worker count.

Verify the package folder with rclone before increasing workers:

```bash
rclone --config generated/rclone.conf lsf \
  "gdrive-sa001:source-volume-name/aa/example-package-folder" \
  --max-depth 1
```

Use `--no-dry-run` only after confirming the package unit, destination prefix, and cleanup plan. For real source uploads, avoid broad cleanup commands; cleanup automation is intentionally restricted to `_sdmig_smoke/...` unless an unsafe-delete override is explicitly passed.

## Safety Rules

- Do not commit `.env`, `generated/`, `secrets/`, or logs.
- Do not paste access tokens or service-account JSON contents anywhere.
- Prefer unbilled dedicated projects for this workflow.
- Do not unlink billing from an existing project unless you know it has no unrelated workloads.
- Do not delete service accounts until migration verification is complete.
- If service-account keys are exposed, delete and recreate them.

## Cleanup

After migration and verification:

```bash
gcloud iam service-accounts list --project "$PROJECT_ID"
```

Delete local key files only when no longer needed:

```bash
rm -rf secrets/service-accounts
```

If the whole migration project is disposable:

```bash
gcloud projects delete "$PROJECT_ID"
```

Deleting the project deletes its service accounts and invalidates their keys.
