#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# shellcheck disable=SC1091
source "$ROOT_DIR/scripts/env.sh"
load_env_file .env

RCLONE_CONFIG="${RCLONE_CONFIG_OUT:-generated/rclone.conf}"
REMOTE_LIMIT="${REMOTE_LIMIT:-0}"
LOG_FILE="${LOG_FILE:-logs/validate_rclone_remotes.log}"

if [[ ! -f "$RCLONE_CONFIG" ]]; then
  echo "missing rclone config: $RCLONE_CONFIG" >&2
  exit 1
fi

mkdir -p "$(dirname "$LOG_FILE")"
: > "$LOG_FILE"

remotes=()
while IFS= read -r remote; do
  remotes+=("${remote%:}")
done < <(rclone --config "$RCLONE_CONFIG" listremotes)
if (( ${#remotes[@]} == 0 )); then
  echo "no remotes found in $RCLONE_CONFIG" >&2
  exit 1
fi

failures=0
checked=0
for remote in "${remotes[@]}"; do
  if (( REMOTE_LIMIT > 0 && checked >= REMOTE_LIMIT )); then
    break
  fi
  checked=$((checked + 1))
  echo "validating ${remote}:"
  if rclone --config "$RCLONE_CONFIG" lsf "${remote}:" --max-depth 1 --log-file "$LOG_FILE" --log-level INFO >/dev/null; then
    echo "  ok"
  else
    echo "  failed"
    failures=$((failures + 1))
  fi
done

echo "checked: $checked"
echo "failures: $failures"
echo "log: $LOG_FILE"

if (( failures > 0 )); then
  exit 1
fi
