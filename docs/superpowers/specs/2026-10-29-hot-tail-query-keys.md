# Hot history pack query-key directories and whole-query cold acceptance

The active task remains first full HTTP eth_call latency at 9M/10M/15M/20M on
the identical H45,836,827 snapshot. No production deployment, restart, database
reset, or eviction of shared production file pages is authorized.

Extend the existing optional owner-span sidecar to version 3. Retain version 2
as an unknown key-directory fallback. The sidecar is bound to the exact encoded
seq-zero canonical pack by its existing BLAKE2b digest, and its entire payload
checksum includes the new directory. It cannot change canonical history or
MuHash. Normal atomic pack writes and explicit pinned-copy conversion both
produce the same deterministic sidecar.

Three independently complete scopes store sorted unique full canonical latest
keys: account/generation envelopes for all owners; all system dynamic-property
keys across generations; and all system fork-vote keys across generations.
Presence/deletion rows and repeated changes all contribute. Each scope has a
separate 16 KiB/1024-key cap. Exceeding a cap makes only that scope unknown;
complete-empty and unknown are different states. Source rows are fully scanned
before any complete flag can be emitted. Old small/dense-owner profitability
conditions must not prevent useful query directories for supported packs.

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

The host is ext4 without reflink; a full physical copy is about 1.50 TB. For
cold acceptance, compute the union of every opened database file, including
startup, across old/new processes and all four requested heights. Copy whole
files from that union to independent inodes across chaindata/history/ancient/
state-snapshots. Before every new process fsync/DONTNEED and verify zero resident
pages for all isolated files. Observe opens before process startup, including
mmap-opened indexes. An accepted run must open no non-isolated database file;
any new file invalidates the sample, is copied, and requires a fresh rerun.
Thus all actual database access paths are cold without copying unused terabytes.
Record startup warming separately; do not describe post-startup engine caches
as empty. Never fadvise an inode shared with the original or production.
