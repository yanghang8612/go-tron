# Hot history pack query-key directories and whole-query cold acceptance

The active task remains first full HTTP eth_call latency at 9M/10M/15M/20M on
the identical H45,836,827 snapshot. No production deployment, restart, database
reset, or eviction of shared production file pages is authorized.

Extend the existing optional owner-span sidecar to version 4 (GTHOS004).
Versions 2 and 3 remain readable and strict reconstruction uses their original
encoders. Canonical history and MuHash are unchanged. The source binding is the
exact encoded seq-zero pack BLAKE2b digest; the entire sidecar payload is checksummed.

The measured V3 exact-key directory is superseded: its 16 KiB/1024-key account
limit often made large packs unknown, causing repeated decode when a filtered
scan could not publish a complete owner cache. It also allocated unrelated key
strings. Retain its parser for old optional indexes, using borrowed visitors.

V4 uses conservative fixed bitsets over full canonical latest keys: 16384 bits
for all account/generation envelopes, 4096 bits for all system dynamic-property
keys, and 1024 bits for system fork-vote keys. Four probes use xxhash64 seed zero
and odd double-hash steps. Insert every matching source row, including deletes
and repeats. There is no key-count cutoff; oversized/unencodable keys make only
their scope unknown. Emit complete flags only after scanning the whole pack.
A checksummed GTHQBF04 payload tag distinguishes bitsets from V3 exact keys even
if only the outer version byte is corrupted. All lengths are exact and bounded.

A missing filter bit proves absence. A collision, saturation, or any remaining
wanted key that may be present requires ordinary canonical fallback. False
positives may cost extra reads; they cannot prove false absence. Complete-empty
filters differ from unknown. Normal atomic writes and fixed-copy conversion
must produce deterministic identical metadata. Record actual all-negative rates
for real remaining-wanted sets in a diagnostic-only excluded discovery process;
the timed candidate contains no observer hook.

Only specialized first-change calls may use the directory. In the seq-zero
tail branch, before materializing/decompressing chunks, skip the pack only if
every remaining requested full key is proved absent in its complete scope.
Positive/unknown/missing/stale/corrupt sidecars follow the canonical path.
Cancellation propagates. Continue the same block iterator through every positive
seq repair and flush only after repair precedence is resolved. A repair can add
a previously absent key or shadow a pack hit by changing owner/key/generation.

Filtered scans cannot publish complete owner or first-owner caches. They may
reuse an already complete pinned cache; successful high-level first-key memo
publication remains request-owned and unchanged. Generic iterators still expose
all repaired canonical rows.

Tests must compare the logical oracle across windows and positive repairs,
confirm zero chunk reads on negative packs, preserve later different-key reads,
and exercise stale/corrupt/version-2/empty/oversize scopes and cancellation.
Measure byte overhead and native writer/read cost, not only synthetic lookups.

Continue bounded account refresh on a private clone of the partially repaired
input until all exact missing IDs are covered. Preserve canonical refs, head,
Tx and root. Build new hot sidecars only in a private history database whose
mutable metadata/WAL files are independently copied before RW open.

The host is ext4 without reflink; a full physical copy is about 1.50 TB.
The initial all-open-files closure included 482 GB of ancient payload files
opened/read for startup catalogs. That copy experiment was superseded; its
partial copies are not performance evidence. No shared production file pages
were evicted.

Separate mandatory startup from timed HTTP. Trace startup plus all four heights
on both exact binaries with strace read/pread/readv/preadv variants and mmap.
Independently reconcile those traces with filesystem access events and require
zero database-backed mmap; static freezer/snapshots/Pebble source audit likewise
finds no database mmap path. Record constructor-only accesses explicitly.

For the timed HTTP data-path closure, copy entire actually read files to
independent inodes across all four stores. Before each fresh process and again
at HTTP dispatch, fsync/DONTNEED only files proved private (nlink 1) and require
mincore zero for every isolated file. Observe HTTP IN_ACCESS events, reject any
new non-isolated read and rerun after copying it, and verify stable inode/size
identities after each run. This measures full HTTP with cold database data-path
file pages; mandatory startup/application metadata caches remain. Do not call
it all application/device caches cold, include process startup in HTTP timing,
or claim untouched constructor files had cold pages.

The fixed input actually has posting watermark == HEAD. Its four-height matrix
cannot establish hot-tail benefit. A separately labelled retained-block watermark
ablation must prove that every canonical seq-zero pack remains present before
lowering only the private derived posting watermark. Record each encoded pack
size/digest, preserve HEAD and canonical segments, derive versioned sidecars, and flush
private memtables. Compare the normal candidate with the same versioned reader whose
key-directory path alone is disabled; retain owner-span support in the control.
Do not attribute an old reader falling back on newer sidecars solely to the new
key-directory mechanism.

Use the fixed cut's historical original lag of 1418 blocks (posting watermark
45,835,409 at HEAD 45,836,827), in addition to the shorter 256-block experiment.
The encoded pack must be present at every block and each source digest recorded.
Keep unsuccessful V3 measurements separate; do not average different candidates.

The V4 native 1418-block experiment proves all four actual remaining-wanted
scopes negative, but still regresses against the full planned-owner cache path.
Repeated per-family reads/checksum/decode of the owner directory are not free.
Retain only validated V4 filters in a request-owned 4 MiB charged cache, separate
from complete owner rows. Copy pooled bytes; bind every cache hit to the encoded
canonical pack digest and pinned view; never share between requests. Unknown
scopes remain canonical fallbacks. The retained 1418-block fixture fits this
budget; overflow simply keeps the uncached path. Positive-sequence repair scans
and complete-owner publication rules remain unchanged. Re-run the identical
cold data-path HTTP matrix; an all-negative diagnostic alone is not a speedup.

Even the shared filter cache remains slower than planned-owner reads because
the first filter access still loads and validates large owner/run tables. Add a
small separately checksummed GTHQPF01 record under the same optional index
family: schema key parent(block) + qf01. Its bounded Snappy payload contains only
block, canonical encoded-pack digest and the three versioned filter scopes.
The existing atomic writer and explicit fixed-copy builder derive it from the
same complete source pack; charge its key/value bytes to encoding statistics.
A pinned query validates this small record and pack digest before use; missing
records retain the previous reader fallback and corrupt/stale records never
prove absence. No canonical exported history or commitment elements change.

Both sparse and bounded point-retirement paths must recognize exact parent and
child keys independently, including orphan children. Existing coarse ranges
already include the child through the same block boundary. Strict validation
checks either record even when its counterpart is absent, rebuilding from the
canonical pack. Directory cost hints also recognize a standalone child without
loading the big parent. Restore does not require the optional record. Test
missing/corrupt/stale source, child-only hints/strict checks, maximum block key
bounds and more than 512 orphan records followed by live parents. The request
cache uses a conservative 256-byte per-entry accounting charge (not an RSS cap).
