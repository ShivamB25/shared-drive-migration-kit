# Operational Learnings

This document records reusable rules from a large archive migration. It is not
a throughput promise, a provider quota guarantee, or a substitute for a small
real smoke test in the operator's own organization.

## Portable Adapter Contract

Keep source and target concerns separate.

1. The source adapter inventories source paths and byte counts.
2. A deterministic planner turns that inventory into ordered package rows.
3. A package row names one independently extractable archive and its adjacent
   metadata files.
4. The target adapter uploads and verifies the complete package triplet.
5. Recovery reads the target audit before deciding what can be retried or
   cleaned.

A normal package produces exactly three adjacent target objects:

```text
<package>.tar.zst
<package>.package.index.json
<package>.files.index.jsonl.zst
```

The package index identifies source paths, the target paths, summary counts,
archive SHA-256, and an extraction command. The compressed file index records
every archived path. Treat the three objects as one logical transaction: an
archive alone is not complete.

`migration_core/` contains pure-Python helpers for this contract:

- safe relative paths and byte parsing
- deterministic contiguous package packing
- deterministic worker assignment
- adjacent archive/index triplet naming
- bounded exponential retry with caller-supplied error classification

Adapters import these helpers; they must not embed credentials, provider IDs,
or source-specific discovery behavior into the reusable module.

## Inventory Before Packaging

Use a persisted inventory as the planning source of truth when exact package
sizes, item counts, or retry coverage matter. Do not use a provider's total
volume size as a substitute for recursive per-directory sizes.

Some providers expose file sizes but report directories as zero bytes. Modal
Volume metadata has this behavior. In that case, compute a unit size by summing
recursive file records once, persist the inventory, and derive all later plans
from it. Repeated `du` calls only repeat the same filesystem walk.

Plan small units whole. For an oversized unit, descend to a lower source depth
and pack complete adjacent child roots under independent byte, entry, and root
limits. Avoid multipart archives for ordinary recovery: an independently
extractable package is easier to verify, move, and restore.

## Staged Archive Sizing

Staged archive preparation uses local worker disk for both source reads or
FUSE cache and the completed compressed archive. The configured disk quota is
not fully available for the final archive.

For a Modal worker with the usual 512 GiB ephemeral disk, start staged archive
plans at `200GiB` maximum with a warning below that value, then measure the
real peak disk usage. Larger packages require an explicitly larger worker disk
and a successful representative test. Compression ratio is not a safety
guarantee: incompressible media can leave the archive close to source size.

Use one archive transfer per worker. Benchmark compression threads against the
actual mounted source before increasing CPU or memory; mounted-volume I/O can
be the bottleneck, so more CPU is not automatically faster or cheaper.

## Drive Pacing And Service Accounts

Many service accounts provide identity lanes and per-account budget lanes, but
they do not guarantee independent throughput. Google Drive can still pace the
shared destination, Cloud project, or backend when too many large resumable
uploads finalize together.

Treat `userRateLimitExceeded` as a pacing signal first:

1. Keep the staged archive and ready marker in place.
2. Retry the same object with bounded exponential backoff.
3. Retire an account only after the retry budget is exhausted or a local
   per-account byte budget has been reached.
4. Reduce active upload streams before adding workers.

For resumable package workers, keep rclone's
`--drive-stop-on-upload-limit` disabled. The target adapter has its own
per-account byte guard and retry policy; making a transient rate response fatal
causes duplicate preparation work and can fill local cache.

Scale in measured gates, not by account count:

1. synthetic target smoke test
2. one real package with one worker
3. one worker rotating all assigned remotes
4. a small number of workers with disjoint contiguous remote lanes
5. larger concurrency only after each prior stage completes without sustained
   Drive pacing errors

A `10 workers x 10 remotes` design means ten active package streams, not one
hundred concurrent large uploads. Preserve that distinction in plans and
operator expectations.

## Recovery And Cleanup

Persist state separately from archive spool data:

- state: inventories, plans, summaries, status JSONL, Drive audits
- cache/spool: archive, ready marker, and local indexes
- target: complete package triplets

Write a ready marker only after archive bytes and SHA-256 are final. On a
retry, reuse that ready archive instead of preparing another copy. Delete local
spool objects only after the archive and both indexes are confirmed uploaded.

Audit the target before cleanup. A source path is complete only if its archive,
package index, and file index all exist. Cleanup may remove incomplete objects
under an explicitly scoped destination prefix; it must never infer completion
from a single archive object or delete a broad shared-drive root.

## Provider-Specific Validation

Every new adapter should document and smoke-test:

- what metadata is cheap versus computed
- read-only source mount behavior
- worker disk and memory limits
- source read concurrency limits
- package creation and extraction command
- target item, file size, API, and daily-account limits
- which errors are transient, terminal, or operator-actionable

Use the generic helpers for deterministic behavior, but keep provider-specific
quotas and error parsing in the adapter that owns them.
