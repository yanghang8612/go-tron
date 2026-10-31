# Implementation and frozen validation protocol

Base: `52aea62ef56799b7e6c1365293123dd029286935`.
Worktree: `/Users/asuka/.codex/worktrees/sync-state-demand-oct31/go-tron`.
Evidence: `build/benchmarks/20261031-state-demand/` (ignored, local artifacts).
Existing dirty workspaces remain untouched. Full-content preservation manifests
were first recorded after isolated implementation began, and explicitly document
that limitation; they are not pre-edit exact-content proof.

1. Implement consumed resource projections, independent bounded point results,
   explicit provenance and one-shot prefix value ownership.
2. Pass adversarial tests and exact upstream oracle; retain every failed fixture
   iteration log. Freeze fixture/source/binary hashes, exact commands, environment
   and acceptance thresholds in `protocol.json` before formal timing.
3. Compile baseline from an exact git-archive source artifact plus IDENTICAL
   API-only lifecycle fixtures. Compile candidate from the isolated worktree.
   Use pinned Darwin native Sapling archive SHA256
   `deb1c6b067b8ba52d3aca010bfbb083c2b6cf5df75b90bf502b38fec17340012`.
4. Run five fixed alternating baseline/candidate rounds, one process per workload,
   one-second Go benchmark duration and all raw results retained. Fixed settings:
   `GOMAXPROCS=4`, `GOGC=100`, `GOMEMLIMIT=6GiB`, `CGO_ENABLED=1`, tags `sapling`.
   `/usr/bin/time -l` records whole-process CPU and peak RSS. `getrusage` around
   each timed benchmark loop reports aggregate process `cpu-ns/op`, including
   background GC/compaction; startup and calibration are outside that CPU delta.
5. Acceptance: BOTH delegation mixed and high-churn full lifecycles must improve
   median ns/op >=5%. No measured control (including frozen first-use/minimal,
   write-only, pending-prefix small/few-query/dense shapes) may slow >5% in median.
   A borderline result within twice the maximum arm MAD is INCONCLUSIVE, not a
   pass or grounds for selecting another window. Allocation bytes/op and counts
   must not increase >5% on representative workloads; controls may grow at most
   max(5%, 1 KiB and 8 allocations per lifecycle) for bounded cache bookkeeping.
   Median normalized process CPU may not regress >5% on representative workloads
   or >10% controls. Median peak RSS may not grow by more than max(10%,16 MiB).
   Any violated threshold holds rollout; redesign is a new frozen iteration,
   retaining all previous evidence. No best-of retries or selective exclusion.
6. Run related race tests, full repository Sapling tests, production native Linux
   validation/release build with pinned server archives, then Astra final audit.
   No push/deploy/restart until final GO. After GO use ordinary master FF push,
   current deployment lock/config/reader guards and normal release flow; measure
   sync throughput, state cold backlog and archive/freezer backlog together.
