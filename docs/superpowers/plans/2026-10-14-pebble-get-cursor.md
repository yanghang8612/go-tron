# Bounded snapshot Get cursor validation plan

Baseline: `8bf634fb15eca46804544c59a672bd3aa18dbacf`. Design and ownership are
specified in `../specs/2026-10-14-pebble-get-cursor.md`. Keep production8192MiB,
32GiB hard limit, Go8GiB, application caches and all concurrency unchanged.
Root owns auditing, commit/push, deployment and online sampling; implementation
work does not authorize changing guards, configuration, DB format or old WIP.

Completed implementation and local checks:

1. Copy the v1.1.5 runtime dependency closure with all platform source variants,
   LICENSE/module metadata and per-file original SHA. Add only snapshot-scoped
   bounded GetCursor and root go.mod replace; preserve upstream Get/Iterator.
2. Retain at most four table pairs, lease all entries touched during one lookup,
   own obsolete-filter backing, fallback when all slots are leased, and release
   every readState/table/value resource. Use upstream per-read pool ownership.
3. Lazily enable GetCursor only for in-prefix commitment reads; preserve callback
   lifetime and timing. Distinguish exact missing from joined failures; a failed
   speculative operation cannot fail session Close a second time.
4. Run root-module Snapshot.Get differential, real speculative session failure,
   callback lifetime, delayed snapshot/flush/compaction, value-block fetch failure
   and resource checks. Explicitly run the nested-module fault/cleanup tests.
5. Run full pebbledb/rawdb/blockbuffer/domains packages, and focused root/nested
   race. Astra independently verified forced two-level/value-block fixtures and
   the joined-NotFound failure overlay.
6. Compare isolated baseline and candidate on the same fork/input/cache/toolchain.
   Include 0/1/4/16/256-read lifecycle cost, sorted/random keys, four-slot overflow,
   bare timing and separate VFS observation. Complete8-block publication/flush/
   read benchmarks preserve expected roots, including seedMem and seedSST cases.

Reproducible commands use the absolute Go1.25.5 binary with its matching GOROOT
and GOTOOLCHAIN=local. The command shapes are:

```
go test ./core/rawdb/pebbledb ./core/rawdb ./core/blockbuffer ./core/state/domains -count=1 -timeout 180s
go test -C third_party/pebble . -run TestGetCursor -count=1 -timeout 60s
go test -C third_party/pebble . -race -run TestGetCursor -count=1 -timeout 90s
go test ./core/rawdb/pebbledb ./core/blockbuffer ./core/state/domains -race -run 'Test(GetCursor|PointGet|PointReadSnapshot|CommitmentParentReadSessionForegroundRetriesFailedPrefetchFlight|OrderedCommitmentPrefetchError)' -count=1 -timeout 120s
```

Final gates are native results, the exact final manifest and Astra review. Local
pipeline timing is mixed and cannot establish a universal improvement; retain
negative cases and report ranges. The corpus configures64 partitions but uses
one active partition. VFS ReadAt is not necessarily device IO, and resident
handle count is not a byte/RSS bound.

If accepted, root performs the guarded binary release with existing cache8192
and all memory/environment settings preserved, waits for warmup and captures a
stable before/after window. Compare actual cursor reuse, lookup calls/time,
throughput, errors, SST/memtable zombies, process/native memory and cgroup
pressure/failcnt. The baseline already touched32GiB through file-cache pressure;
do not classify its existing zombie/reclaim counters as new cursor leaks, or
attribute changing workloads/capacity warmup to this interface alone. Restore the
prior executable only for a material health/resource/performance regression.
