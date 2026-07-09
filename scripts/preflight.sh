#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# shellcheck disable=SC1091
source "$ROOT_DIR/scripts/env.sh"
load_env_file .env

need_command() {
  local name="$1"
  if ! command -v "$name" >/dev/null 2>&1; then
    echo "missing required command: $name" >&2
    exit 1
  fi
}

need_command gcloud
need_command rclone
need_command jq
need_command python3

active_account="$(gcloud auth list --filter=status:ACTIVE --format='value(account)' 2>/dev/null || true)"
if [[ -z "$active_account" ]]; then
  echo "gcloud has no active account. Run: gcloud auth login" >&2
  exit 1
fi

project="${GOOGLE_CLOUD_PROJECT:-$(gcloud config get-value project 2>/dev/null || true)}"
if [[ -z "$project" || "$project" == "(unset)" ]]; then
  echo "GOOGLE_CLOUD_PROJECT is not set and gcloud has no default project." >&2
  exit 1
fi

if [[ -z "${SHARED_DRIVE_ID:-}" ]]; then
  echo "SHARED_DRIVE_ID is not set. Fill it in .env before generating rclone config." >&2
  exit 1
fi

echo "gcloud account: $active_account"
echo "gcloud project: $project"
echo "shared drive:   $SHARED_DRIVE_ID"
echo "rclone:         $(rclone version | head -n 1)"
echo "preflight ok"
