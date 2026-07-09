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
RCLONE_TRANSFERS="${RCLONE_TRANSFERS:-${TRANSFERS:-1}}"
RCLONE_CHECKERS="${RCLONE_CHECKERS:-${CHECKERS:-4}}"
RCLONE_DRIVE_CHUNK_SIZE="${RCLONE_DRIVE_CHUNK_SIZE:-512M}"
RCLONE_TPSLIMIT="${RCLONE_TPSLIMIT:-5}"
RCLONE_TPSLIMIT_BURST="${RCLONE_TPSLIMIT_BURST:-0}"
RCLONE_RETRIES="${RCLONE_RETRIES:-3}"
RCLONE_LOW_LEVEL_RETRIES="${RCLONE_LOW_LEVEL_RETRIES:-20}"
RCLONE_RETRIES_SLEEP="${RCLONE_RETRIES_SLEEP:-30s}"
RCLONE_CONTIMEOUT="${RCLONE_CONTIMEOUT:-60s}"
RCLONE_TIMEOUT="${RCLONE_TIMEOUT:-5m}"
RCLONE_STATS="${RCLONE_STATS:-30s}"
RCLONE_STATS_FILE_NAME_LENGTH="${RCLONE_STATS_FILE_NAME_LENGTH:-0}"
RCLONE_LOG_LEVEL="${RCLONE_LOG_LEVEL:-INFO}"
RCLONE_DRIVE_STOP_ON_UPLOAD_LIMIT="${RCLONE_DRIVE_STOP_ON_UPLOAD_LIMIT:-1}"
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
  --transfers "$RCLONE_TRANSFERS"
  --checkers "$RCLONE_CHECKERS"
  --drive-chunk-size "$RCLONE_DRIVE_CHUNK_SIZE"
  --retries "$RCLONE_RETRIES"
  --low-level-retries "$RCLONE_LOW_LEVEL_RETRIES"
  --retries-sleep "$RCLONE_RETRIES_SLEEP"
  --contimeout "$RCLONE_CONTIMEOUT"
  --timeout "$RCLONE_TIMEOUT"
  --progress
  --stats "$RCLONE_STATS"
  --stats-file-name-length "$RCLONE_STATS_FILE_NAME_LENGTH"
  --log-file "$LOG_FILE"
  --log-level "$RCLONE_LOG_LEVEL"
)

if [[ "$RCLONE_TPSLIMIT" != "0" && -n "$RCLONE_TPSLIMIT" ]]; then
  args+=(--tpslimit "$RCLONE_TPSLIMIT" --tpslimit-burst "$RCLONE_TPSLIMIT_BURST")
fi

if [[ "$RCLONE_DRIVE_STOP_ON_UPLOAD_LIMIT" != "0" ]]; then
  args+=(--drive-stop-on-upload-limit)
fi

if [[ "$DRY_RUN" != "0" ]]; then
  args+=(--dry-run)
fi

echo "remote: $REMOTE"
echo "source: $SOURCE_PATH"
echo "dest:   ${REMOTE}:${DEST_PATH}"
echo "dry-run: $DRY_RUN"
echo "log: $LOG_FILE"

rclone "${args[@]}"
