# Posting cleanup under persistent compaction debt

The mainnet posting worker made zero callbacks during a ten-minute capture on
2026-10-18. Combined hot/history debt remained above the fixed 32 GiB veto;
write stalls were zero and device latency was low. This absolute threshold can
starve deletion on a growing database even when foreground I/O remains healthy.

Keep the normal admission below 32 GiB unchanged. At or above that threshold,
permit the same bounded posting-only chunk only when fresh engine/device
observations show no hard limit, fewer than eight L0 sublevels, device await at
most 2 ms, and average queue at most eight requests. The normal device limits
are 5 ms and 32 requests. Reject negative device latency in either mode. Debt
remains a reason for stricter admission, but is no longer an unconditional veto.
This grants progress opportunities during healthy I/O windows, not a guarantee
under hard pressure, active reservations or exclusive maintenance ownership.

Apply this identical classification in the scheduling precheck and after
acquiring the chain lock. The latter still obtains the process-wide maintenance
gate, whose own pressure check, reservations and cooldown remain authoritative.
Do not hold the gate while waiting for the chain lock. Observe debt-relief
callbacks/chunks/deleted bytes separately, using the actual locked admission.

Keep existing limits (4096 scanned frames, 1 MiB scanned KV, 256 KiB deleted KV,
10 ms cooperative scan budget, one-second interval), cursor/error semantics,
fixed canonical proof/anchor and cold-covered hot-prune authorization. Individual
iterator I/O and batch writes can exceed the cooperative scan deadline; this
change must not describe it as a hard wall-clock bound. Do not delete crossing
frames, directories, history changesets or transaction ranges. Do not force
compaction on production or modify the legacy full-sweep watermark.

Before quantifying reclaim, acquire a coherent immutable history image. The
census must validate every frame, count exact visible uncompressed KV bytes,
and retain length-framed digests for eligible, crossing, newer and directory
classes. Eligibility is last(frame) <= captured H; H still needs the normal
canonical/cold proof before any production deletion. Compressed table estimates
are not exact reclaim. A separate, explicitly marked private copy may be pruned
and compacted to measure its before/after physical allocation and verify all
retained classes and non-posting keyspaces. Hardlink only immutable SST files;
copy WAL, manifests, CURRENT and options independently. Production must be
restored to its exact original executable before the long census begins.

Validate pressure boundaries and stale/hard/queue cases, locked rechecks after
queued admission, real Pebble census/deletion parity, corrupt-frame and iterator
failures, preserved directory digests, plus existing core proof/reorg/cursor
tests. Compare actual online deletion progress, chain hold time, sync/cold rates
and write-stall/device metrics before expanding any cleanup budget.
