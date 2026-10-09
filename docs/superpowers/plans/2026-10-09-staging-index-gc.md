# Online staging history-index GC implementation plan

Follow `../specs/2026-10-09-staging-index-gc.md`. Work on master with exact file
staging, preserving unrelated CDC/reference WIP. Do not push or deploy.

1. Audit directory/posting writers, async inflight mutation, whole-layer flush,
   pinned snapshots and prefix seek. Resolve nonblocking snapshot capture with
   the independent reviewer before asserting a coherent directory absence.
2. Add schema-owned bounded staging posting and directory primitives in
   `core/rawdb/accessors_state_change_staging_gc.go`, with real Pebble and fault
   tests in its matching `_test.go`. Keep byte/row/time bounds and cursors local;
   require a coherent exact prefix seeker for directory checks.
3. Add an independent worker in `core/history_staging_index_gc.go`, with private
   epoch/prefix/anchor authority, bounded route certification, guard/admission,
   separate cursors, lifecycle and metrics. Add matching tests, including
   source holes that later become COLD, resource/lock deferrals, stale proofs,
   failed chunks and overlay-protected directories.
4. In `cmd/gtron/main.go`, register the independent worker only with staging
   and exclude old full-index/posting workers in that mode. Keep non-staging
   wiring and existing retention unchanged. Add focused wiring coverage if
   a small testable selection helper is needed.
5. Run focused rawdb/core tests, meaningful race checks, affected package tests
   and diff checks. Record exact results and known cooperative-latency limits.
   Obtain independent code audit before a precise-path commit.

Expected files: this spec and plan; the new core worker and its tests; the new
rawdb primitive and its tests; main.go wiring; a narrowly scoped blockbuffer
TryNewReadSnapshot addition in buffer.go with a separate maintenance test file.
The flushMu-only nonblocking capture and writer-order proof were independently reviewed and
explicitly assigned. Existing startup receipt files belong
to the separate startup-memory task and are not modified here.

Use cancellable post-pass delay max(interval,9*elapsed) to yield after slow
proofs; allow at least one metadata candidate or one raw row after necessary
proof/capture exceeds the cooperative budget. Test delay arithmetic without
wall-clock sleeps.
