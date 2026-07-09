#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# shellcheck disable=SC1091
source "$ROOT_DIR/scripts/env.sh"
load_env_file .env

INVENTORY_FILE="${INVENTORY_FILE:-generated/service_accounts.csv}"
GROUP_EMAIL="${GROUP_EMAIL:-}"
ROLE="${GROUP_ROLE:-MEMBER}"
LOG_FILE="${LOG_FILE:-logs/add_service_accounts_to_group.log}"
LIMIT="${LIMIT:-0}"

if [[ -z "$GROUP_EMAIL" ]]; then
  echo "GROUP_EMAIL is required." >&2
  exit 1
fi

if [[ ! -f "$INVENTORY_FILE" ]]; then
  echo "missing inventory file: $INVENTORY_FILE" >&2
  exit 1
fi

mkdir -p "$(dirname "$LOG_FILE")"
: > "$LOG_FILE"

count=0
added=0
already=0
failed=0

while IFS=, read -r index account_id email key_file; do
  [[ "$index" == "index" ]] && continue
  [[ -z "$email" ]] && continue

  count=$((count + 1))
  if (( LIMIT > 0 && count > LIMIT )); then
    break
  fi

  echo "adding $email to $GROUP_EMAIL"
  tmp_err="$(mktemp)"
  if gcloud identity groups memberships add \
    --group-email="$GROUP_EMAIL" \
    --member-email="$email" \
    --roles="$ROLE" \
    --format=json >>"$LOG_FILE" 2>"$tmp_err"; then
    added=$((added + 1))
    rm -f "$tmp_err"
    continue
  fi

  if grep -qiE 'already|ALREADY_EXISTS|duplicate' "$tmp_err"; then
    echo "already member: $email"
    cat "$tmp_err" >> "$LOG_FILE"
    already=$((already + 1))
    rm -f "$tmp_err"
    continue
  fi

  echo "failed: $email" >&2
  cat "$tmp_err" >&2
  cat "$tmp_err" >> "$LOG_FILE"
  rm -f "$tmp_err"
  failed=$((failed + 1))
done < "$INVENTORY_FILE"

echo "processed: $((added + already + failed))"
echo "added:     $added"
echo "already:   $already"
echo "failed:    $failed"
echo "log:       $LOG_FILE"

if (( failed > 0 )); then
  exit 1
fi

