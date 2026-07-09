# Shared Drive Migration Kit

This repository sets up a Google shared drive as a large-scale migration target using Google Cloud service accounts and rclone.

The design is shared-drive-only:

- no My Drive upload target
- no domain-wide delegation for upload
- no required Workspace Admin SDK automation
- one Google Group grants all service accounts access to the shared drive
- one rclone Drive remote is generated per service-account key

The source side is intentionally adapter-shaped. A Modal volume export can be one source adapter, but the core Google Drive functionality works with any local source path that rclone can read.

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
  -> local export or mount path
  -> rclone copy
  -> shared drive target remotes
```

Today the scripts assume `SOURCE_PATH` is local. Future adapters can prepare that local path from Modal volumes, object storage, network mounts, or another export mechanism.

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

Preferred CLI path, if your active gcloud user can manage the shared drive:

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

## Script Reference

```text
scripts/preflight.sh
  Checks local tools and required env values.

scripts/bootstrap_gcp_service_accounts.sh
  Enables APIs, creates service accounts, creates JSON keys, writes inventory.

scripts/generate_workspace_group_artifacts.py
  Produces batch files and CSV/JSONL group-member artifacts.

scripts/add_service_accounts_to_group.sh
  Adds service-account emails to GROUP_EMAIL through gcloud identity groups.

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
