# Implementation and frozen validation protocol

Base: `52aea62ef56799b7e6c1365293123dd029286935`.
Worktree: `/Users/asuka/.codex/worktrees/sync-state-demand-oct31/go-tron`.
Evidence: `build/benchmarks/20261031-state-demand/` (ignored, local artifacts).
Existing dirty workspaces remain untouched. Full-content preservation manifests
were first recorded after isolated implementation began, and explicitly document
that limitation; they are not pre-edit exact-content proof.

1. Implement consumed resource projections, independent bounded point results
   and explicit provenance. Keep prefix iteration identical to upstream after
   withdrawing the ownership-transfer experiment described below.
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


## Second candidate validation boundary

Candidate `6991ec78cf19fff5c7693ef6263facf5cff3ee0f` is not released: a
public-API differential test found point-only warm -> DeleteAccount -> frozen
materializer returning a cached row where exact upstream returned absence.
Its successful local component timings and full/race logs remain original v1
evidence, not proof for the final candidate. The v1 native validation was
intentionally stopped with `ABORTED_KNOWN_DELETED_REGRESSION`; this is neither a
product test pass nor a spontaneous product failure. The private process group
was proved and terminated without touching the production node.

The minimal second candidate excludes deleted objects from both point-cache
lookup and population while retaining the existing generic reader/dependencies.
After related local correctness/race passes, freeze new source and an independent
native v2 plan for Astra review. Keep the SAME three lifecycle fixture files,
16 workloads, five alternating rounds, one-second timing and thresholds. Reuse
the v1 private Go build cache only with the SAME Go binary, compiler, flags and
native archive; Go validates action IDs. Retain every v1 artifact. Final exact
v2 Linux full Sapling, related race, clean ELF metadata and all 160 benchmark
records are mandatory. The prior lengthy Mac full run is not relabeled v2;
Linux full validation covers the final source without another redundant Mac
full run. No push/deploy until final exact-source release GO.

## Prefix experiment withdrawn after native v2 hold

Candidate `c1f72d2c6200e40e3b219f51fc637f75a860b896` passed all 64 Linux
Sapling package tests, related race tests, exact clean ELF metadata and all
15 lifecycle trace comparisons. Its native fixed 160-record benchmark passed
79 of 80 strict checks. The sole miss was prefix many-groups median wall time:
19,297,780ns baseline versus 20,338,813ns candidate (+5.3946%, limit +5%).
Its 76,144ns excess above the boundary is within twice maximum arm MAD
(938,368ns), so the frozen rule classifies it INCONCLUSIVE/HOLD. All five
paired wall and CPU results were slower; no selective retry or relaxed limit
is used to seek a pass. All original and independently sealed evidence remains.

The next candidate restores the upstream callback value clone and leaves
`iterateLatestPrefix` byte-identical to upstream. It retains the account demand
projections, frozen point reuse, deleted-object guard and reset invalidation.
Related full-state, four-package race and 15-trace checks precede a separately
frozen native plan using the same pinned Go/compiler/native archive and owned
Go cache. The exact new candidate must pass full Linux Sapling `./... -count=1`,
related race, clean ELF metadata and all 16 shapes in five alternating rounds.
Each new benchmark process uses a three-second duration; all original thresholds
and the MAD/HOLD rule remain unchanged. Every prefix control remains included;
c1f's full test results remain facts about c1f rather than proof for a different
candidate. No source is pushed or deployed without final exact-source GO.
