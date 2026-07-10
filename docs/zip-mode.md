# Zip Mode

Zip mode is the reusable pattern for turning any structured source into portable archive packages before uploading them somewhere else.

It is not Google Drive specific. Google Drive is only one possible drain target.

## Mental Model

```text
source adapter
  -> migration plan
  -> zip mode / archive spool
  -> optional upload/drain adapter
```

For Modal Volume sources:

```text
Modal source volume
  -> tar.zst package per planned unit
  -> package index JSON
  -> files index JSONL.zst
  -> Modal cache volume
```

Then later:

```text
Modal cache volume
  -> rclone / upload adapter / copy adapter
  -> final storage target
```

## Unit Boundary

The user or source adapter decides the unit boundary. Examples:

```text
source-prefix=""
unit-depth=2
unit="aa/62563-Psychiatry-Psychotherapy-Podcast"
```

```text
source-prefix="customers/acme"
unit-depth=1
unit="customers/acme/export-2026-07-09"
```

```text
source-prefix="datasets"
unit-depth=2
unit="datasets/language/en"
```

Zip mode should not guess business structure. It should accept a plan produced by discovery or by a custom adapter.

## Hybrid Folder Planning

For very large trees, use inventory-based hybrid planning instead of one fixed depth:

```text
small top-level folder
  -> one archive for the whole folder

large top-level folder
  -> fallback archives for lower-level child folders
```

This keeps Google Drive item counts low without creating archives that are likely to exceed a target upload limit. The inventory pass is the expensive metadata step; after it exists, planning should reuse it instead of making every zip worker rediscover sizes.

Example:

```text
aa/
  70 child folders
  -> aa.tar.zst
  -> aa.package.index.json
  -> aa.files.index.jsonl.zst

en/
  145525 child folders
  -> en/<podcast-1>/<podcast-1>.tar.zst
  -> en/<podcast-2>/<podcast-2>.tar.zst
```

The inventory planner chooses top-level archives by real byte totals. If a top-level folder is above `--max-package-bytes`, it falls back to lower-level child folders. Workers still compute the final package index before writing an archive and skip any row above `--max-package-bytes`.

Create or reuse a full inventory first. For Modal Volume sources, `discover-mounted-fast` writes a full inventory JSONL next to the plan:

```bash
MODAL_SOURCE_VOLUME_NAME="source-volume-name" \
scripts/run_modal_volume_adapter.sh discover-mounted-fast \
  --source-prefix "" \
  --unit-depth 2 \
  --dest-prefix SourceName \
  --plan-path plans/source-inventory-seed.jsonl \
  --max-package-bytes 700GiB \
  --warn-package-bytes 650GiB
```

Then create a size-based hybrid plan from the inventory:

```bash
MODAL_SOURCE_VOLUME_NAME="source-volume-name" \
scripts/run_modal_volume_adapter.sh plan-hybrid-inventory \
  --source-prefix "" \
  --top-depth 1 \
  --fallback-depth 2 \
  --dest-prefix SourceName \
  --plan-path plans/source-hybrid-units.jsonl \
  --inventory-path plans/source-inventory-seed.inventory.jsonl \
  --max-package-bytes 700GiB \
  --warn-package-bytes 650GiB
```

Then zip from the hybrid plan:

```bash
MODAL_SOURCE_VOLUME_NAME="source-volume-name" \
MODAL_MAX_CONTAINERS=100 \
MODAL_CACHE_COMMIT_EVERY=25 \
modal run --detach adapters/modal_volume/modal_shared_drive_app.py \
  --command zip \
  --plan-path plans/source-hybrid-units.jsonl \
  --worker-count 100 \
  --assignment-mode contiguous \
  --spool-name source-hybrid-spool \
  --no-dry-run
```

## Package Layout

Each package produces:

```text
<unit>/<unit-name>.tar.zst
<unit>/<unit-name>.package.index.json
<unit>/<unit-name>.files.index.jsonl.zst
```

The package index records source and package paths:

```json
{
  "source": {
    "adapter": "modal-volume",
    "volume": "source-volume-name",
    "path": "aa/62563-Psychiatry-Psychotherapy-Podcast"
  },
  "package": {
    "format": "tar.zst",
    "archive_path": "SourceName/aa/62563-Psychiatry-Psychotherapy-Podcast/62563-Psychiatry-Psychotherapy-Podcast.tar.zst",
    "package_index_path": "SourceName/aa/62563-Psychiatry-Psychotherapy-Podcast/62563-Psychiatry-Psychotherapy-Podcast.package.index.json",
    "files_index_path": "SourceName/aa/62563-Psychiatry-Psychotherapy-Podcast/62563-Psychiatry-Psychotherapy-Podcast.files.index.jsonl.zst"
  }
}
```

The files index records simple paths relative to the package root:

```json
{"path":"audio","type":"directory","size":0}
{"path":"feed.xml","type":"file","size":253422}
{"path":"audio/example.mp3","type":"file","size":58308836}
```

The full logical source path is:

```text
<source.path>/<entry.path>
```

Do not write absolute paths like `/src/...` into indexes. Relative paths keep the package portable.

## Modal Command

Prepare a small spool first:

```bash
MODAL_SOURCE_VOLUME_NAME="source-volume-name" \
MODAL_MAX_CONTAINERS=10 \
MODAL_CACHE_COMMIT_EVERY=25 \
scripts/run_modal_volume_adapter.sh zip \
  --plan-path plans/source-units.jsonl \
  --worker-count 10 \
  --assignment-mode contiguous \
  --spool-name source-spool \
  --max-package-bytes 700GiB \
  --warn-package-bytes 650GiB \
  --no-dry-run \
  --limit 1000
```

Full run after checking cache growth and status files:

```bash
MODAL_SOURCE_VOLUME_NAME="source-volume-name" \
MODAL_MAX_CONTAINERS=100 \
MODAL_CACHE_COMMIT_EVERY=25 \
modal run --detach adapters/modal_volume/modal_shared_drive_app.py \
  --command zip \
  --plan-path plans/source-units.jsonl \
  --worker-count 100 \
  --assignment-mode contiguous \
  --spool-name source-spool \
  --max-package-bytes 700GiB \
  --warn-package-bytes 650GiB \
  --no-dry-run
```

Monitor:

```bash
modal app list
modal app logs <app-id>
modal volume ls <cache-volume-name> /archive-spool/<spool-name>
```

## Operational Lessons

- Separate archive creation from final upload when the final target has rate limits.
- Keep source volumes read-only.
- Use a cache/spool volume as the durable handoff point.
- Use contiguous worker assignment when plan rows are sorted by source path.
- Keep indexes relative and portable.
- Commit cache writes in batches for throughput. Lower `MODAL_CACHE_COMMIT_EVERY` if redoing work after preemption is more expensive than commit overhead.
- Expect Modal preemption. Workers should skip already prepared files on retry.
- Start with a limit, inspect cache growth, then scale.
