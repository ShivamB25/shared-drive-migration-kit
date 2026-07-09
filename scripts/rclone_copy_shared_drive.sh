#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# shellcheck disable=SC1091
source "$ROOT_DIR/scripts/env.sh"
load_env_file .env

RCLONE_CONFIG="${RCLONE_CONFIG_OUT:-generated/rclone.conf}"
SOURCE_PATH="${SOURCE_PATH:-}"
DEST_PATH="${DEST_PATH:-}"
DRY_RUN="${DRY_RUN:-1}"
TRANSFERS="${TRANSFERS:-8}"
CHECKERS="${CHECKERS:-16}"
LOG_FILE="${LOG_FILE:-logs/rclone_copy_shared_drive.log}"
REMOTE="${1:-${RCLONE_REMOTE:-}}"

if [[ -z "$REMOTE" ]]; then
  echo "usage: $0 <rclone-remote-name>" >&2
  exit 1
fi

if [[ -z "$SOURCE_PATH" || ! -e "$SOURCE_PATH" ]]; then
  echo "SOURCE_PATH must point to an existing local source export or mount." >&2
  exit 1
fi

if [[ ! -f "$RCLONE_CONFIG" ]]; then
  echo "missing rclone config: $RCLONE_CONFIG" >&2
  exit 1
fi

mkdir -p "$(dirname "$LOG_FILE")"

args=(
  --config "$RCLONE_CONFIG"
  copy "$SOURCE_PATH" "${REMOTE}:${DEST_PATH}"
  --transfers "$TRANSFERS"
  --checkers "$CHECKERS"
  --drive-chunk-size 256M
  --progress
  --stats 30s
  --log-file "$LOG_FILE"
  --log-level INFO
)

if [[ "$DRY_RUN" != "0" ]]; then
  args+=(--dry-run)
fi

echo "remote: $REMOTE"
echo "source: $SOURCE_PATH"
echo "dest:   ${REMOTE}:${DEST_PATH}"
echo "dry-run: $DRY_RUN"
echo "log: $LOG_FILE"

rclone "${args[@]}"
