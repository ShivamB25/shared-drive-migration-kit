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

## Sane Folder Planning

For very large trees, use inventory-based hybrid planning instead of one fixed depth:

```text
small top-level folder
  -> one archive for the whole folder

large top-level folder
  -> contiguous batches of complete child folders
  -> recurse only when one child is itself too large
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
  -> en/batches/en-batch-00001.tar.zst
  -> en/batches/en-batch-00002.tar.zst
```

Every batch contains complete, path-contiguous child folders and is independently extractable. The planner limits source bytes, archive entries, and source roots at the same time. It never uses multipart tar for ordinary folders. Original paths are stored relative to the Modal Volume root, so extracting at a restore root recreates paths such as `en/<podcast>/...`.

Create or reuse a full inventory first. Modal does not expose recursive folder sizes for package planning: `modal volume ls --json` and `Volume.listdir()` both report directories as `0 B`, while direct file entries have real sizes. `find-json-fast` writes resumable JSONL shards plus a compatibility JSON array; the sane planner streams the shards directly.

```bash
MODAL_SOURCE_VOLUME_NAME="source-volume-name" \
modal run adapters/modal_volume/modal_shared_drive_app.py \
  --command find-json-fast \
  --source-prefix "" \
  --plan-path plans/source-find-fast.json \
  --worker-count 32
```

Then create the sane package plan from the completed `find-json-fast` inventory:

```bash
MODAL_SOURCE_VOLUME_NAME="source-volume-name" \
modal run adapters/modal_volume/modal_shared_drive_app.py \
  --command plan-sane-archives \
  --source-prefix "" \
  --dest-prefix SourceName \
  --inventory-path plans/source-find-fast.json \
  --plan-path plans/source-sane-archives.jsonl \
  --max-package-bytes 200GiB \
  --max-archive-entries 100000 \
  --max-roots-per-archive 1000 \
  --shared-drive-item-limit 400000
```

This command is a planning dry run. It reads inventory shards and writes only the plan and summary to the Modal state volume. Require `plan_complete=true`, `fits_shared_drive_item_limit=true`, zero oversized files, and exact source/package byte coverage before preparing archives.

Then zip from the sane plan:

```bash
MODAL_SOURCE_VOLUME_NAME="source-volume-name" \
MODAL_MAX_CONTAINERS=10 \
MODAL_CACHE_COMMIT_EVERY=25 \
modal run --detach adapters/modal_volume/modal_shared_drive_app.py \
  --command zip \
  --plan-path plans/source-sane-archives.jsonl \
  --worker-count 10 \
  --assignment-mode contiguous \
  --spool-name source-sane-spool \
  --no-dry-run
```

## Package Layout

Each whole-top package produces:

```text
<unit>/<unit-name>.tar.zst
<unit>/<unit-name>.package.index.json
<unit>/<unit-name>.files.index.jsonl.zst
```

An oversized top-level folder uses adjacent batch files without creating one Drive folder per source child:

```text
<top>/batches/<top>-batch-00001.tar.zst
<top>/batches/<top>-batch-00001.package.index.json
<top>/batches/<top>-batch-00001.files.index.jsonl.zst
```

The package index uses `source.paths` for multi-root batches and records the archive SHA-256 after staged creation. Extract any package independently:

```bash
tar --zstd -xf en-batch-00001.tar.zst -C /restore-root
```

The package index records source and package paths:

```json
{
  "source": {
    "adapter": "modal-volume",
    "volume": "source-volume-name",
    "path": "aa/62563-Psychiatry-Psychotherapy-Podcast",
    "paths": ["aa/62563-Psychiatry-Psychotherapy-Podcast"]
  },
  "package": {
    "format": "tar.zst",
    "archive_path": "SourceName/aa/62563-Psychiatry-Psychotherapy-Podcast/62563-Psychiatry-Psychotherapy-Podcast.tar.zst",
    "package_index_path": "SourceName/aa/62563-Psychiatry-Psychotherapy-Podcast/62563-Psychiatry-Psychotherapy-Podcast.package.index.json",
    "files_index_path": "SourceName/aa/62563-Psychiatry-Psychotherapy-Podcast/62563-Psychiatry-Psychotherapy-Podcast.files.index.jsonl.zst"
  }
}
```

The files index records paths relative to the Modal Volume root:

```json
{"path":"aa/62563-Psychiatry-Psychotherapy-Podcast/audio","type":"directory","size":0}
{"path":"aa/62563-Psychiatry-Psychotherapy-Podcast/feed.xml","type":"file","size":253422}
{"path":"aa/62563-Psychiatry-Psychotherapy-Podcast/audio/example.mp3","type":"file","size":58308836}
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
  --max-package-bytes 200GiB \
  --warn-package-bytes 180GiB \
  --no-dry-run \
  --limit 1000
```

Full run after checking cache growth and status files:

```bash
MODAL_SOURCE_VOLUME_NAME="source-volume-name" \
MODAL_MAX_CONTAINERS=10 \
MODAL_CACHE_COMMIT_EVERY=25 \
modal run --detach adapters/modal_volume/modal_shared_drive_app.py \
  --command zip \
  --plan-path plans/source-units.jsonl \
  --worker-count 10 \
  --assignment-mode contiguous \
  --spool-name source-spool \
  --max-package-bytes 200GiB \
  --warn-package-bytes 180GiB \
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
- Start `staged` packages at `200GiB` maximum on Modal's usual 512 GiB ephemeral disk. Source reads/FUSE cache and the archive share that local disk; increase package or worker-disk limits only after a representative measurement.
- Start with a small number of workers. More containers do not guarantee faster Volume reads or Drive uploads, and many concurrent archive finalizations can trigger target-side pacing.
- Treat archive, package index, and file index as one completion unit. Audit all three before deleting a spool object or marking a source path complete.

For reusable source/target adapter rules and the pure-Python helper module, see [operational learnings](operational-learnings.md).
- Use contiguous worker assignment when plan rows are sorted by source path.
- Keep indexes relative and portable.
- Commit cache writes in batches for throughput. Lower `MODAL_CACHE_COMMIT_EVERY` if redoing work after preemption is more expensive than commit overhead.
- Expect Modal preemption. Workers should skip already prepared files on retry.
- Start with a limit, inspect cache growth, then scale.
