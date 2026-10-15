# Ownership costs in complete manifest publication and first load

Complete publication continues to normalize, sort, validate, encode compact
JSON, write a temporary file, fsync, close, rename and fsync its directory.
Only successful durable publications may seed the decode cache. Public mutable
loaders and publishers retain their existing behavior. All current file bytes
are authenticated on every load, including cache hits; externally replaced,
malformed or stale files cannot be approved by their timestamp or inode.

## Publication strings

The public publisher owns its mutable catalog containers and may use small
substrings of arbitrarily large caller buffers. Cache seeding therefore still
clones both catalog arrays and every string, after full validation. Instead of
four separately allocated strings per ref, it packs strings into completed
immutable blocks of at most 64 KiB of logical string data. An oversized field
occupies a separate block. Blocks never combine active and retired catalogs.
Chain identity strings keep their independent clones. No unsafe conversion is
used, and no field is assigned until its whole backing block is complete.

A small retained field may keep one block alive, rather than the complete
manifest arena. This is bounded to 64 KiB of logical data for ordinary fields,
plus allocator rounding; an oversized field keeps its own allocation only.
There is no additional pointer graph or interning table in the generation.
The existing cache accounting remains a conservative retained charge, not an
RSS measurement: it charges byte/slice capacities and all logical string bytes,
then doubles the subtotal for allocation rounding and metadata, plus 4096.
Additionally, the cache measures each completed builder's actual capacity
minus logical string bytes and adds twice that slack to its charge. It checks
the final charge after construction and rejects a generation that cannot fit.
Progress publications carry this charge along with their shared immutable
catalog; clearing or decoding a fresh generation clears this arena slack.

## Private read-only first load

On a cache miss, decoding creates an exclusively owned generation. A private
read-only load can transfer that generation directly to the cache because it
has no public mutable alias. The charge includes the actual decoder slice
capacities, including growth slack. Public loads continue to receive detached
catalog containers, and old borrowed generations are never modified when a
publisher or another reader replaces the resident entry.

The transfer may require more resident budget than the former compact clone.
At a budget that admits a compact clone but cannot admit the actual decoder
capacity, this optimization rejects caching and returns the same validated
read-only result. No capacity is hidden, no old resident is retained as a
fallback, and disabled/invalid/rejected cache configuration keeps the normal
load and publication error behavior. Production validation errors are retained
and surfaced identically. No new trusted public publication interface exists.

## Verification and measurement

`BenchmarkManifestFullMaintenance` compiles unchanged on baseline c1e77d5. It
covers complete durable Publish+public first Load, Integrate+public first Load+
advancing completed-prune publication, and a cache-cleared private first Load.
Small and production-shaped fixtures exercise enabled, disabled and rejected
caches; an optional SHA-pinned native JSON fixture can be replayed. Integration
replaces an event-log companion pair and lets the real active/retired and
progress logic run. These metadata benchmarks do not open segment files or
delete hot rows and do not claim sustained live-node throughput.

Correctness tests cover exact strings across block boundaries, escaped and
oversized fields, public mutation isolation, transferred decoder ownership and
exact capacity-budget boundaries, old borrowed generations, and a same-length
invalid current-file replacement. Existing full manifest, cache, progress,
directory-fsync retry, failure non-seeding, reset epoch, identity and concurrency
tests remain required, together with snapshots/pruning package and race tests.
