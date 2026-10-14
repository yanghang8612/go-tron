# Mainnet random-read cache budget, 2026-10-14

The selected change is one capacity deployment: raise `--db.cache` from
4096 to 8192 MiB, then collect before/after metrics. Keep the existing Go,
application cache, GC, prefetch, concurrency and database format settings.
Rollback is for failure or a serious sustained regression, not repeated
capacity switching. This is a capacity decision, not proof of a node speedup.

## Why capacity

The production point-read profile spends CPU locating SST blocks and reading
random data. A warm 1 GiB commitment application cache reduced durable
gets per update, while the remaining gets have a different key mix; their
mean latency does not establish a same-key storage regression. Recent
prefetch samples were 100% present and about 98.5% useful. Dropping prefetch
or adding a presence filter is unsupported by this evidence.

Finite, fixed-input native tests of 2 KiB data blocks and 16 KiB index blocks
showed workload-dependent tradeoffs, without a robust overall improvement.
Smaller blocks reduced bytes but could increase read calls or CPU. Table
properties also showed that small average values in mixed production SSTs
do not describe the commitment-only payload distribution. No SST layout,
keyspace, encoding, iterator semantics or Pebble fork is changed.

## The actual budgets

For the mainnet direct-history runtime, `historyStagingRuntimeCacheSplit`
caps the history share at 512 MiB. Thus the configured total changes from
3584 MiB hot + 512 MiB history to **7680 MiB hot + 512 MiB history**. It is
not a 7 GiB / 1 GiB split. The service keeps explicit `--state.trie.cache 128`,
so increasing `db.cache` does not increase that application setting.

A configured Pebble cache is a total reservation budget, not an independent
read-only payload allowance. In Pebble v1.1.5, memtable arenas and flushable
batches call `Cache.Reserve`; recycled and zombie memtables can retain these
reservations. Reservations reduce the actual block-cache target. Hot and
history memtable sizes remain 256 and 128 MiB, with the existing threshold
of eight; do not add their maximum sizes again as fully independent caches.
Block-cache entry/value headers, maps, ghost entries, allocator overhead and
fragmentation are additional memory costs. The block/live/zombie metrics do
not include all such overhead.

Unchanged mainnet controls are the 32 GiB cgroup hard limit, `GOMEMLIMIT=8GiB`,
`GOGC=100`, commitment application cache 512 MiB base + 512 MiB tail,
`MALLOC_ARENA_MAX=4`, and the existing read/prefetch concurrency. Go's limit
is soft and excludes native allocations; neither it nor a Pebble budget is
an RSS bound. The effective 32 GiB hard limit comes from the final
`zzzz-mainnet-memory-32g.conf` override. The older memory drop-in still says
20G; changing its cache argument must leave that line and the final override
unchanged.

## Observed headroom and its limits

The read-only production snapshot at 2026-10-14 07:07:53 UTC recorded:

| Measurement | GiB, approximately |
| --- | ---: |
| Cgroup usage | 25.56 |
| Cgroup peak | 27.40 |
| Cgroup anonymous RSS | 11.01 |
| Cgroup file cache | 14.56 |
| Inactive file cache | 7.28 |
| Active file cache | 7.28 |
| Process RSS / process historical high-water RSS | 11.03 / 11.91 |
| Cgroup hard limit | 32.00 |

Cgroup swap, unevictable and fail count were zero. This cgroup interface did
not expose dirty/writeback/shmem fields. Host-wide Dirty was about 10.42 MiB,
Writeback zero and Shmem about 3.82 GiB; these are conservative substitutes,
not measurements of this service's dirty memory. Taking half of inactive
file bytes after subtracting those host-wide amounts yields about 1.73 GiB
of provisional reclaim margin. It is not guaranteed available memory.

The additional configured native budget is 4 GiB. With a nominal million
additional 4 KiB blocks, entry/value headers alone are roughly 128 MiB;
0.25–0.75 GiB extra metadata/allocator overhead is a planning allowance,
not a bound. Holding file cache constant, current usage plus the added
budget and this allowance would reach roughly 29.81–30.31 GiB. Applying the
same arithmetic to the recorded peak gives 31.65–32.15 GiB. Activation
therefore relies on reclaim of clean file cache and requires monitoring;
RSS near 11 GiB is not evidence of 21 GiB of free cgroup capacity.

## Deployment and acceptance

The repository service template selects 8192. The installed guarded release
helper preserves the effective argument list during ordinary binary release,
so updating the template alone does not change the existing memory drop-in.
Its binary/source/hash guards do not pin the cache argument itself.

First push the audited revision and wait for automatic tests and binary release
to succeed. That release restarts with the preserved 4096 argument. Then use the separately reviewed, one-shot
maintenance script under `/run/lock/gtron-mainnet-release.lock`. Recapture the
new release source (also equal to repository HEAD), binary path/hash, and current memory-file SHA before
invoking it; an old prepared candidate must not replace a newly pinned binary
path. Default invocation only prints the plan. Applying it must:

1. Validate the active release and all existing guards with the installed
   release helper, the unique old 4096 argument, and effective 32 GiB limit.
2. Save the exact old memory drop-in bytes and a candidate in a new root-only
   backup directory, while holding the same lock as automatic deployment.
3. Stop once, atomically replace only the unique `--db.cache 4096` token with
   `--db.cache 8192`, reload systemd and start. Leave binary identity,
   environment, guard files, markers and all other bytes unchanged.
4. Require the exact new configured and live argv, helper verification,
   unchanged guard/marker and 32 GiB override bytes, and a healthy Wallet API.
   If activation fails, restore the original memory bytes and restart the
   original argv. Surface any rollback failure.

Both planned restarts precede sampling. Warm for five minutes, then collect a
six-minute after window against the saved stable before window. After activation, confirm configured hot/history capacities and reservation-
adjusted block targets, process/native memory, cgroup usage/peak/cache,
fail count, swap, dirty/writeback where available, storage pressure, throughput,
and durable read calls/bytes/latency per update. Allow warmup before interpreting
cache misses. Compare stable before/after windows with workload/mix recorded;
this is not a controlled estimate of capacity's causal speedup. Restore the
saved configuration only if service health or a serious sustained resource or
performance regression requires it.

Read-only evidence and the maintenance candidate are retained in ignored
`build/benchmarks/20261014-native-cache-plan/`; finite native layout evidence is
in `build/benchmarks/20261014-random-read/` and production SST properties in
`build/benchmarks/20261014-sst-properties/`. These local paths are audit inputs,
not runtime dependencies. No server configuration is changed by this document.
