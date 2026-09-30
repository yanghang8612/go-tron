# Mainnet memory budget candidate (2026-09-30)

The six-minute sync observation and independent host sample are in
`build/benchmarks/20260930-resource-bottleneck/`. The running `9a1ac520`
release has a **20 GiB** cgroup v1 hard limit, `GOMEMLIMIT=8GiB`, `GOGC=100`,
and `--db.cache 4096`. Between 14:50:01 and 14:53:03 UTC its cgroup usage
rose from 21,123,579,904 to 21,368,516,608 bytes, leaving 351,256,576 to
106,319,872 bytes (335 to 101 MiB). `memory.failcnt`
rose by 440,945; these are failed charge attempts, not OOM kills. Host
`MemAvailable` at the latter point was about 35.6 GiB. The cold parallel
probe reported `ready=0` throughout; its measured headroom had a 69,632-byte
median and 1,479,581,696-byte (about 1.38 GiB) maximum. The enhanced reader
needs 2 GiB base headroom
plus 256 MiB decoded pipeline and 64 MiB chunk cache, or **2.3125 GiB**.
Published cold coverage grew 5.78 blocks/s while eligibility grew 8.93
blocks/s in that window, increasing the lag by 1,131 blocks.

The probe in `cmd/gtron/history_parallel_ready.go` takes the minimum of host
`MemAvailable` and every cgroup ancestor's limit minus charged usage. This
correctly refuses to equate host free memory with capacity inside the service.
Cgroup usage includes file cache; 4,637,265,920 bytes (4.32 GiB) was
`inactive_file` at the final host sample. The revised probe reads each memory
cgroup ancestor's `memory.stat`. It credits at most half of inactive file pages
after subtracting dirty, writeback, and shmem; missing cgroup-local unsafe
fields use host-wide upper bounds, while missing inactive or host bounds yield
zero credit. The candidate is clamped by the layer's own charge and limit,
then capped at 3 GiB. Every ancestor and host `MemAvailable` still clamp the
result. The existing 2.3125 GiB reader threshold is unchanged. The probe
requires a fresh pair of stable-scope samples and OOM evidence; actual
`under_oom` or a new OOM event removes credit for that sample. Cgroup v1 on
this host has no sampled OOM-kill counter, so an unseen kill between samples
cannot be ruled out. `memory.failcnt` alone does not veto credit because file
cache reclaim can increment it without an OOM. The new
`memory_uncredited_bytes`, `memory_clean_credit_bytes`, `memory_under_oom`,
and `memory_oom_known` gauges distinguish raw hard headroom from provisional
clean-file credit.

## Staged candidate

The repository's `deploy/systemd/gtron.service` template now represents a
**24 GiB candidate** while retaining the deployed 8 GiB Go soft limit,
`GOGC=100`, and 4 GiB Pebble cache. It also drops `MemorySoftLimit`: the
2026-09-18 live investigation in `docs/dev/mainnet-oom-status-20260918.md`
records systemd 219 rejecting it as `unknown lvalue`, with the kernel soft
limit still unlimited. The live service has a later
`zz-memory-budget-20260918.conf` drop-in; editing the template alone cannot
change the running service. The separately reviewable
`deploy/systemd/zzzz-mainnet-memory-24g.conf` only overrides `MemoryLimit` and
sorts after the existing drop-in. Confirm the effective unit and actual cgroup
limit after any activation; a source-file diff is not deployment verification.

The operator can review the single-setting file, copy it to
`/etc/systemd/system/gtron.service.d/zzzz-mainnet-memory-24g.conf`, run
`systemctl daemon-reload`, and restart `gtron.service` through the existing
guarded procedure. Verify `systemctl show gtron -p MemoryLimit -p ExecStart`
and the cgroup's `memory.limit_in_bytes` equals 25,769,803,776 bytes. Removing
that one drop-in, reloading, and restarting restores the prior effective 20G
limit. This does not alter the existing `zz-memory-budget-20260918.conf`,
`ExecStart`, environment, or reader guards.

This is a staged candidate, not a guaranteed four GiB of durable probe
headroom. The added budget may fill with page cache as syncing continues; the
bounded clean-file credit is intended to permit cold bursts in that state
without counting all cache as unused memory. A 20 GiB limit with roughly 4.3
GiB inactive file and virtually no raw headroom would usually remain below
the reader threshold, while a 24 GiB limit with more clean cache may cross it.
Neither is an observed production outcome. The earlier
40 GiB service setting coincided with a host-wide OOM when other processes
used much more RAM; the 20 GiB setting was chosen to leave room for tracker,
Java, and the OS. Recheck those processes and host `MemAvailable` immediately
before any trial. Do not extrapolate today's 35.6 GiB host availability to a
future workload.

After activation, use a post-warmup observation window of at least 30 minutes,
recording the same process start, cgroup usage/limit, `rss`, `cache`,
`active_file`, `inactive_file`, `memory.failcnt`, host `MemAvailable`, OOM
messages, Pebble cache occupancy/hit rate, physical read rate, cold probe
raw headroom, credited headroom, ready, cold read fallback, head and cold publication rates, and cold
lag. The trial succeeds only if the enhanced reader gains sustained admission
and cold lag trend improves without host memory or importer regressions. A
brief `ready=1` after restart is insufficient. Restore 20G if host memory
falls toward the historical OOM margin, the process is killed, or the extra
budget simply refills with cache while the cold probe remains below 2.3125
GiB. No production change, restart, or database write is made by this note.
