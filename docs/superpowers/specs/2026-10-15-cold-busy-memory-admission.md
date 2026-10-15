# Separate bounded cold progress from optional parallel work

The 2026-10-15 eight-minute capture found an approximately 80-second cold
frontier pause while the importer had buffered work, 9–11 CPUs were idle, and
the runtime memory estimate was between approximately 1.38 and 1.97 GiB. The
node wired BusyHistoryBuildReady and ParallelHistoryEventReady to the same
2 GiB predicate. This suppressed the whole opportunity between the existing
soft and busy backlog watermarks, rather than just optional event pairing.

Keep the existing CPU sampler and evidence policy: two comparable observations
5–30 seconds apart, bounded successful reads, unchanged CPU affinity and
cgroup scope, unlimited CPU quota, GOMAXPROCS at least eight, at least 25%
idle and two proven spare cores. A stopped, failed, backwards, stale or reset
sampler cannot admit a build. The serial opportunity additionally requires
known, inactive OOM state at every cgroup ancestor and unchanged OOM event
counters across the pair. This applies even when raw limit-minus-usage alone
would satisfy its target. Existing conservative clean inactive file credit
remains subject to the same fresh pair and OOM checks; failcnt by itself is
not an OOM event and does not grant headroom.

Give the existing bounded busy opportunity its own memory target:
max(1 GiB, twice the configured per-collector ETL buffer plus 512 MiB).
Production's two 256 MiB accessor collectors therefore retain a 1 GiB target;
explicit larger ETL buffers raise that target. The additional 512 MiB is an
opportunity allowance for metadata, identity/replay caches, codec and sorting
scratch, and GC overlap. It is a scheduling target, not an RSS reservation or
proof of a hard peak. Existing adaptive complete-block/transaction ranges and
ETL spill behavior remain authoritative. An unusually large individual block
or record retains its existing limitations and can exceed a nominal target.

Parallel history/event pairing and enhanced shared reads retain their original
2 GiB checks and original CPU/topology requirements. A successful serial
opportunity does not enable either optional feature. The Runner's storage
freshness, hard/L0/stall/device guards, shared heavy-work lease, disk space,
range validation, busy recovery and complete-maintenance accounting do not
change. The independent above-busy-watermark liveness path does not change.

Expose busy_ready and busy_memory_minimum_bytes alongside the existing runtime
resource gauges so live verification distinguishes serial admission from
parallel readiness. Verification covers both memory boundaries, larger ETL
budgets, CPU/quota limits, read/clock/scope/counter failures, cgroup v1/v2 clean
credit and OOM changes, and sampler shutdown. Retain the Runner's existing
busy resource/storage/lease/recovery tests. Fixed inputs should produce the
same authenticated history bytes and roots; this change only schedules work.

A separate operational proposal raises only mainnet's MemoryLimit from 32 GiB
to 40 GiB while retaining GOMEMLIMIT=8GiB and db.cache=8192. The separately
measured commitment tail budget changes from 512 to 1024 MiB (see
2026-10-15-commitment-tail-budget.md), with unchanged
64 partitions/32 readers and prefetch overlap zero. Host RAM is about 61.3 GiB;
a read-only observation found 26.4 GiB MemAvailable and Java RSS 5.3 GiB.
The extra 8 GiB provides room for page cache and maintenance, not a larger Go
heap/cache setting. Recheck host/Java and preserve startup guards before
activation. Change the existing zz-memory-budget-20260918.conf in place,
backing up its exact bytes; mainnet_release.plan preserves all its bytes except
the release binary token. Do not install the historical, unused
zzzz-mainnet-memory-32g.conf override. Persist 40 GiB in the repository service
template. Configuration activation and restart require the reviewed deployment
phase, then compare sustained frontier progress, memory, failures and latency.
