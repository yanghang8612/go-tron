# Mainnet Pebble data-cache capacity proposal, 2026-10-15

Increase only the explicit `--db.cache` argument from 8192 to 12288 MiB. The
history-staging split remains capped at 512 MiB, making the hot Pebble cache
budget 7680 → 11776 MiB. Keep the live explicit `--state.trie.cache 128`,
`GOMEMLIMIT=8GiB`, commitment base 512 MiB and tail 1024 MiB, concurrency,
data format, and the 40 GiB cgroup limit unchanged. The trie auto-derived
1024 MiB cap does not apply to this service because its argv sets 128.

## Evidence and limit

The 2026-10-15 06:56–07:04 UTC production before window recorded a stable
process, 28.93 blocks/s and no cgroup fail-count change. Hot block-cache
occupancy was about 7.48–7.50 billion bytes against an 8,053,063,680-byte
configured capacity (7680 MiB), while hot SST ReadAt traffic continued. This
supports testing a larger data-block working set, not a claim of an exact
4 GiB miss reduction. Pebble memtable reservations reduce the available block
cache; allocator and cache-entry overhead are not included in configured
capacity. The freezer, stage store, and native buffers still use memory.

The same window's cgroup maximum current charge was 32.599 GiB; its
historical peak was 33.972 GiB under a 40 GiB hard limit. Adding the full
4 GiB to that historical peak gives 37.972 GiB, leaving 2.028 GiB before
metadata and future workload variation. At the first sample, anonymous RSS
was 18.925 GiB, file cache 12.485 GiB, and inactive file cache 6.240 GiB.
Host MemAvailable was 26,254 MiB at the same collection; this is host-wide
and cannot substitute for the cgroup headroom. Clean file cache can be
reclaimed as native cache grows, possibly trading away OS page-cache hits.
Go's 8 GiB soft limit does not cap Pebble native memory or total RSS.
The 2.028 GiB arithmetic margin is not a guaranteed floor: paused freezer
buffers and admitted parallel cold work can overlap a hot-cache high-water
mark. Preserve the existing pressure admission gates; do not use a low
instantaneous RSS sample to override them.
The downloaded collector did not record Java RSS; the deployment owner must
capture it before activation rather than assume host-wide availability belongs
to gtron. This proposal adds no soft-limit directive or override and keeps the
40 GiB `MemoryLimit`; the old systemd cannot use `MemoryHigh` or `MemoryMax`.

In a fixed 8-layer, 8192-key, random-Get local fixture, scaling the cache
from 2 to 3 MiB (ratio 1.5 versus the proposed 1.533) reduced measured
VFS SST reads from 0.01848 to 0 per Get and bytes from 74.89 to 0 per Get;
value oracle and allocation count stayed equal. A separate same-input
8-block full fold+flush pipeline on seeded SSTs reported exactly 36 VFS
ReadAt calls and 127636 bytes in both arms for hot, scan, and delayed-read
cases. Each root was checked against the reference fixture. Timings and
allocations varied across three one-operation trials and do not support a
pipeline speedup claim. These small Darwin fixtures do not approximate the
production multi-terabyte working set or Linux I/O. Raw commands/results are
in `build/benchmarks/20261015-io-manifest-gate/random-read/cache8192to12288/`.

## Activation and acceptance

The repository template changes only the `db.cache` argument and its adjacent
explanatory comment. A previously saved memory drop-in pinned an older binary.
Replacing solely that path with the last observed c1 release path reproduces
the prior live post-check SHA exactly:
`eab29e0c38b270b0b95ebef794cefca66ff9b5c02021c94625ce1d84f4bab09f`.
The observed source is `c1e77d5a729683d17191cabcc395232acad6931f`
and its running binary SHA was
`2bd64f96104f72c5bec8b248b6ea5b6fd227284127754c367cc46d569d37ccdd`.
The one-token 8192→12288 proposal from those c1-pinned bytes has SHA
`9d9c88db44121c3edea66c43d9e054bfe004299a47b4c03bfa48a204e96331c7`.
This cryptographically matches the **prior** post-check, not a fresh reading
of the currently installed file. The deployment owner must capture the
latest live drop-in and executable identity; apply this candidate only if
the live bytes still match that exact SHA and the pinned binary remains the
observed c1 release. Otherwise generate and review a fresh byte-only
candidate. The ignored
`prepare_memory_candidate.py` only performs this local transformation and
never invokes systemd. Avoid a second overriding drop-in.

After joint Astra/root approval, push the audited code with the existing
8192 MiB memory argument and let the normal guarded build/test/release helper
start that new binary. Its `inspect` contract requires configured and running
argv to agree, so do not preload 12288 MiB with a reload while an 8192 MiB
process is active. Once the new release is healthy, capture its exact source,
binary path and SHA, MainPID, helper SHA, and freshly installed memory bytes.
Review a one-token 8192→12288 candidate against those new inputs. Under the
same release lock with the deploy timer stopped, call helper `inspect` while
the 8192 MiB configuration and process still agree; then stop gtron and
verify it inactive with MainPID 0, atomically replace the drop-in, reload
systemd, and start the **same new binary** with 12288 MiB. Require helper
`wait_healthy` and `inspect` to prove configured/running identity, exact argv,
executable SHA, explicit trie 128 MiB, unchanged Go/commitment budgets, and
the 40 GiB cgroup hard limit. Keep the original drop-in for rollback. If
any step fails, restore its exact bytes while stopped, reload, restart the
same new binary at 8192 MiB and verify health/identity; record primary and
rollback errors independently. A timed-out stop may already have stopped the
service, so recovery must check state and restore service health before
releasing the timer. This requires a second controlled restart, rather than
silently violating the helper's running/effective argv contract.
Accept only if Wallet/API and sync
remain healthy and a warmed, comparable workload window shows useful SST
read-call/byte or latency reduction without sustained cgroup or host-memory
pressure. Record cgroup current/peak/anon/file/inactive_file/failcnt,
process RSS, host MemAvailable, Java RSS, cache occupancy/reservations,
SST ReadAt calls/bytes and Get latency, import BPS and block/tx mix.
Rollback on OOM/fail-count growth, unsafe headroom, or serious sustained
health/performance regression. A natural before/after window is operational
evidence, not an isolated causal A/B test.
