#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# shellcheck disable=SC1091
source "$ROOT_DIR/scripts/env.sh"
load_env_file .env

BUNDLE_DIR="${MODAL_RCLONE_BUNDLE_DIR:-generated/modal-rclone-bundle}"
CREDS_VOLUME="${MODAL_CREDS_VOLUME_NAME:-sdmig-credentials}"

if [[ ! -d "$BUNDLE_DIR" ]]; then
  echo "missing bundle directory: $BUNDLE_DIR" >&2
  echo "run: scripts/generate_modal_rclone_bundle.py" >&2
  exit 1
fi

for required in rclone.conf rclone.manifest.csv bundle.json; do
  if [[ ! -s "$BUNDLE_DIR/$required" ]]; then
    echo "missing or empty bundle file: $BUNDLE_DIR/$required" >&2
    echo "run: scripts/generate_modal_rclone_bundle.py" >&2
    exit 1
  fi
done

if ! find "$BUNDLE_DIR/service-accounts" -maxdepth 1 -name '*.json' -type f -print -quit 2>/dev/null | grep -q .; then
  echo "bundle has no service-account JSON files: $BUNDLE_DIR/service-accounts" >&2
  echo "run: scripts/generate_modal_rclone_bundle.py" >&2
  exit 1
fi

if ! modal volume list 2>/dev/null | grep -Fq "$CREDS_VOLUME"; then
  modal volume create --version=2 "$CREDS_VOLUME"
fi

modal volume put --force "$CREDS_VOLUME" "$BUNDLE_DIR" /

echo "uploaded $BUNDLE_DIR to Modal volume: $CREDS_VOLUME"
echo "expected worker mount path: /creds"
