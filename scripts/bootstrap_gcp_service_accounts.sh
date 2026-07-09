#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# shellcheck disable=SC1091
source "$ROOT_DIR/scripts/env.sh"
load_env_file .env

PROJECT_ID="${GOOGLE_CLOUD_PROJECT:-}"
SA_PREFIX="${SA_PREFIX:-drive-migrate}"
SA_COUNT="${SA_COUNT:-100}"
SA_START_INDEX="${SA_START_INDEX:-1}"
KEY_DIR="${KEY_DIR:-secrets/service-accounts}"
INVENTORY_FILE="${INVENTORY_FILE:-generated/service_accounts.csv}"
APPLY="${APPLY:-0}"

if [[ -z "$PROJECT_ID" ]]; then
  echo "GOOGLE_CLOUD_PROJECT is required." >&2
  exit 1
fi

if ! [[ "$SA_COUNT" =~ ^[0-9]+$ ]] || (( SA_COUNT < 1 )); then
  echo "SA_COUNT must be a positive integer." >&2
  exit 1
fi

if ! [[ "$SA_START_INDEX" =~ ^[0-9]+$ ]] || (( SA_START_INDEX < 1 )); then
  echo "SA_START_INDEX must be a positive integer." >&2
  exit 1
fi

if ! [[ "$SA_PREFIX" =~ ^[a-z][a-z0-9-]*[a-z0-9]$ ]]; then
  echo "SA_PREFIX must start with a lowercase letter and contain only lowercase letters, digits, and hyphens." >&2
  exit 1
fi

mkdir -p "$KEY_DIR" "$(dirname "$INVENTORY_FILE")"
chmod 700 "$KEY_DIR"

run() {
  printf '+ '
  printf '%q ' "$@"
  printf '\n'
  if [[ "$APPLY" == "1" ]]; then
    "$@"
  fi
}

echo "project: $PROJECT_ID"
echo "service accounts: $SA_COUNT starting at $SA_START_INDEX"
echo "apply mode: $APPLY"

run gcloud config set project "$PROJECT_ID"

for api in serviceusage.googleapis.com iam.googleapis.com cloudresourcemanager.googleapis.com drive.googleapis.com; do
  run gcloud services enable "$api" --project "$PROJECT_ID"
done

tmp_inventory="$(mktemp)"
trap 'rm -f "$tmp_inventory"' EXIT
echo "index,account_id,email,key_file" > "$tmp_inventory"

last_index=$((SA_START_INDEX + SA_COUNT - 1))
for index in $(seq "$SA_START_INDEX" "$last_index"); do
  suffix="$(printf '%03d' "$index")"
  account_id="${SA_PREFIX}-${suffix}"
  if (( ${#account_id} > 30 )); then
    echo "service account id '$account_id' is longer than the 30 character limit." >&2
    exit 1
  fi

  email="${account_id}@${PROJECT_ID}.iam.gserviceaccount.com"
  key_file="${KEY_DIR}/${account_id}.json"

  if [[ "$APPLY" == "1" ]] && gcloud iam service-accounts describe "$email" --project "$PROJECT_ID" >/dev/null 2>&1; then
    echo "service account exists: $email"
  else
    if [[ "$APPLY" == "1" ]]; then
      printf '+ '
      printf '%q ' gcloud iam service-accounts create "$account_id" --project "$PROJECT_ID" --display-name "Shared drive migration ${suffix}"
      printf '\n'
      created_account=0
      for attempt in 1 2 3 4 5 6; do
        if gcloud iam service-accounts create "$account_id" \
          --project "$PROJECT_ID" \
          --display-name "Shared drive migration ${suffix}"; then
          created_account=1
          break
        fi
        if gcloud iam service-accounts describe "$email" --project "$PROJECT_ID" >/dev/null 2>&1; then
          created_account=1
          break
        fi
        echo "service account create failed for $email; retrying in 65s (${attempt}/6)" >&2
        sleep 65
      done
      if [[ "$created_account" != "1" ]]; then
        echo "failed to create service account $email after retries" >&2
        exit 1
      fi
    else
      run gcloud iam service-accounts create "$account_id" \
        --project "$PROJECT_ID" \
        --display-name "Shared drive migration ${suffix}"
    fi
  fi

  if [[ -f "$key_file" ]]; then
    echo "key exists: $key_file"
  else
    if [[ "$APPLY" == "1" ]]; then
      printf '+ '
      printf '%q ' gcloud iam service-accounts keys create "$key_file" --iam-account "$email" --project "$PROJECT_ID"
      printf '\n'
      created_key=0
      for attempt in 1 2 3 4 5 6; do
        if gcloud iam service-accounts keys create "$key_file" \
          --iam-account "$email" \
          --project "$PROJECT_ID"; then
          created_key=1
          break
        fi
        echo "key create failed for $email; retrying in 5s (${attempt}/6)" >&2
        sleep 5
      done
      if [[ "$created_key" != "1" ]]; then
        echo "failed to create key for $email after retries" >&2
        exit 1
      fi
      chmod 600 "$key_file"
    else
      run gcloud iam service-accounts keys create "$key_file" \
        --iam-account "$email" \
        --project "$PROJECT_ID"
    fi
  fi

  echo "${index},${account_id},${email},${key_file}" >> "$tmp_inventory"
done

cp "$tmp_inventory" "$INVENTORY_FILE"
echo "wrote inventory: $INVENTORY_FILE"

if [[ "$APPLY" != "1" ]]; then
  echo "dry run complete. Re-run with APPLY=1 to create resources and keys."
fi
