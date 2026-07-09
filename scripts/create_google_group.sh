#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# shellcheck disable=SC1091
source "$ROOT_DIR/scripts/env.sh"
load_env_file .env

GROUP_EMAIL="${GROUP_EMAIL:-}"
GROUP_ORGANIZATION="${GROUP_ORGANIZATION:-}"
GROUP_CUSTOMER="${GROUP_CUSTOMER:-}"
GROUP_DISPLAY_NAME="${GROUP_DISPLAY_NAME:-}"
GROUP_DESCRIPTION="${GROUP_DESCRIPTION:-Service account uploaders for shared drive migration.}"
GROUP_TYPE="${GROUP_TYPE:-discussion}"
WITH_INITIAL_OWNER="${WITH_INITIAL_OWNER:-with-initial-owner}"
APPLY="${APPLY:-0}"

if [[ -z "$GROUP_EMAIL" ]]; then
  echo "GROUP_EMAIL is required." >&2
  exit 1
fi

if [[ -z "$GROUP_ORGANIZATION" && -z "$GROUP_CUSTOMER" ]]; then
  GROUP_ORGANIZATION="${GROUP_EMAIL#*@}"
fi

if [[ -z "$GROUP_DISPLAY_NAME" ]]; then
  local_part="${GROUP_EMAIL%@*}"
  GROUP_DISPLAY_NAME="${local_part//-/ }"
fi

if gcloud identity groups describe "$GROUP_EMAIL" --format=json >/dev/null 2>&1; then
  echo "group exists: $GROUP_EMAIL"
  gcloud identity groups describe "$GROUP_EMAIL" --format='json(name,groupKey,displayName,labels)'
  exit 0
fi

args=(identity groups create "$GROUP_EMAIL")

if [[ -n "$GROUP_CUSTOMER" ]]; then
  args+=(--customer="$GROUP_CUSTOMER")
else
  args+=(--organization="$GROUP_ORGANIZATION")
fi

args+=(
  --display-name="$GROUP_DISPLAY_NAME"
  --description="$GROUP_DESCRIPTION"
  --group-type="$GROUP_TYPE"
  --with-initial-owner="$WITH_INITIAL_OWNER"
)

echo "group:        $GROUP_EMAIL"
echo "organization: ${GROUP_ORGANIZATION:-}"
echo "customer:     ${GROUP_CUSTOMER:-}"
echo "display name: $GROUP_DISPLAY_NAME"
echo "group type:   $GROUP_TYPE"
echo "apply:        $APPLY"

printf '+ gcloud '
printf '%q ' "${args[@]}"
printf '\n'

if [[ "$APPLY" != "1" ]]; then
  echo "dry run only. Re-run with APPLY=1 to create the group."
  exit 0
fi

gcloud "${args[@]}"

