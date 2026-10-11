# R1 history retirement implementation (2026-10-11)

The offline `gtron db migrate-history-cdc` command now rewrites existing R1 or
V2 containers into the existing CDC V3 format. It preserves every logical byte
and copies the original index and accessor bytes to the new content-addressed
stem. This adds no history format. No deployment or production migration was
performed as part of this implementation.

Default execution publishes only a strictly smaller history file meeting
`--min-savings-percent` (default 5). `--r1-only` restricts candidate selection to
R1; any unfinished journal is resumed first. `--allow-growth` explicitly bypasses
the savings requirement, allowing complete R1 retirement even where CDC is larger.
Bounds and exclusive offline database locking still apply.

Example plan for the existing deployment layout:

```sh
gtron db migrate-history-cdc --datadir /data/gtron/main/datadir \
  --r1-only --max-trios 1
```

Add `--yes` to execute. Add `--allow-growth` only when format unification is the
chosen objective. `--max-trios=0` processes the selected inventory; a size rejection
consumes a batch slot. A rejected R1 remains readable by the existing reader and
can be reconsidered with `--allow-growth`.

The rewrite reads source logical bytes once while encoding and hashing them,
then decodes the CDC output once and compares its logical digest. Source physical
checksums are checked before and after conversion, and exact source identities
are checked again before unlink. Unchanged companions retain their original
checksums. There are no repeated semantic scans or verification ETL passes.

The existing small durable journal and atomic manifest publication are reused.
Recovery verifies the exact installed output checksums instead of recompressing
an already completed conversion. Publication keeps chain identity, visible range,
progress, and unrelated references unchanged; cleanup removes only the replaced
old paths with matching identities and no leases. Cancellation removes private
scratch while retaining installed immutable files and any recovery journal.

The old 2 GiB logical/20 million record admission limits were removed. Existing
container bounds, checked CDC directory/ordinal capacity, and per-trio disk
admission remain. The conservative work estimate is twice logical bytes plus
twice companion bytes and 1 GiB slack; the CLI adds the live free-space reserve.
Large logical files may therefore require raising `--max-work-gib` even when
actual encoded output is small. Encoder memory and dictionaries remain bounded.

Ordinary compaction now honors `HistoryReferenceContainer=false`: it reads R1
inputs and emits the configured streaming codec. With compression format `3`
this emits CDC. Explicit R1 output remains an opt-in compaction configuration.

Same-input tests measure actual encoded file lengths, rather than comparing
history from different ages. A deliberately fragmented-span fixture measured
R1 7,869,151 bytes, V2 3,147,935 bytes, and CDC 1,502,445 bytes, with the same
3,146,740 logical bytes. This synthetic fixture proves span removal and byte
preservation; its ratio is not a production savings forecast.

A valid production R1 builder fixture with shared values measured R1 1,013,106
bytes and CDC 1,077,465 bytes (+6.35%). Default migration keeps the smaller R1;
explicit growth migration emits CDC while preserving the complete history digest.
Thus the R1 span directory size cannot be treated as guaranteed reclaimable space.

Tests cover unchanged logical digests and indexed queries, unchanged accessor
bytes, size rejection, explicit growth, R1-only selection, prepublication recovery,
postpublication recovery with partial source deletion, corruption, leases, database
locking, disk admission, and both busy/aligned compaction with R1 disabled.
