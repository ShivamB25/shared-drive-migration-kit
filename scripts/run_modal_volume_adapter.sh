#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# shellcheck disable=SC1091
source "$ROOT_DIR/scripts/env.sh"
load_env_file .env

COMMAND="${1:-discover}"
shift || true

export MODAL_SOURCE_VOLUME_NAME="${MODAL_SOURCE_VOLUME_NAME:-${MODAL_VOLUME_NAME:-}}"
export MODAL_CREDS_VOLUME_NAME="${MODAL_CREDS_VOLUME_NAME:-sdmig-credentials}"
export MODAL_STATE_VOLUME_NAME="${MODAL_STATE_VOLUME_NAME:-sdmig-state}"
export MODAL_CACHE_VOLUME_NAME="${MODAL_CACHE_VOLUME_NAME:-sdmig-cache}"
export MODAL_MAX_CONTAINERS="${MODAL_MAX_CONTAINERS:-10}"
export MODAL_WORKER_COUNT="${MODAL_WORKER_COUNT:-10}"
export MODAL_REMOTE_GROUP_SIZE="${MODAL_REMOTE_GROUP_SIZE:-10}"
export MODAL_CPU="${MODAL_CPU:-2}"
export MODAL_MEMORY="${MODAL_MEMORY:-4096}"
export MODAL_EPHEMERAL_DISK="${MODAL_EPHEMERAL_DISK:-524288}"
export MODAL_TIMEOUT="${MODAL_TIMEOUT:-43200}"

if [[ -z "$MODAL_SOURCE_VOLUME_NAME" ]]; then
  echo "MODAL_SOURCE_VOLUME_NAME or MODAL_VOLUME_NAME is required." >&2
  exit 1
fi

if ! modal volume list 2>/dev/null | grep -Fq "$MODAL_STATE_VOLUME_NAME"; then
  modal volume create --version=2 "$MODAL_STATE_VOLUME_NAME"
fi

if ! modal volume list 2>/dev/null | grep -Fq "$MODAL_CACHE_VOLUME_NAME"; then
  modal volume create --version=2 "$MODAL_CACHE_VOLUME_NAME"
fi

modal run adapters/modal_volume/modal_shared_drive_app.py \
  --command "$COMMAND" \
  "$@"
