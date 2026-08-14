# go-tron logging audit — 2026-08-14

This audit is scoped to operational logging. It does not change consensus,
wire protocol, RPC responses, or database formats. The production server and
its databases were not accessed or modified.

## Method and inventory

The static inventory scans production `*.go` files (excluding tests) for
structured `Trace`, `Debug`, `Info`, `Warn`, `Error`, and `Crit` calls on the
project/geth logger receivers. Matches were then reviewed by call site for
lifecycle, event type, loop location, and expected production frequency.
Standard-library `http.Error`, returned `error` values, and test assertions are
not counted as logs. Direct `gtronlog` calls, instance `logger` calls, and one
dynamic-message helper are included; comment-only regex matches are excluded.

Baseline: the starting worktree at HEAD
`e8849466272394ad99e66c9d34bc921a49b534a7`, including its inherited
uncommitted snapshot/prune/sync work:

| Area | Trace | Debug | Info | Warn | Error | Crit | Total |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| cmd/gtron + node | 0 | 0 | 33 | 4 | 1 | 0 | 38 |
| net, p2p, sync | 2 | 26 | 17 | 53 | 3 | 0 | 101 |
| core chain, freezer, rawdb | 1 | 6 | 28 | 25 | 8 | 0 | 68 |
| state, pruning, snapshots | 0 | 2 | 46 | 17 | 1 | 0 | 66 |
| consensus/dpos | 0 | 0 | 0 | 4 | 0 | 0 | 4 |
| actuator + VM | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| txpool + producer | 0 | 1 | 3 | 3 | 0 | 0 | 7 |
| HTTP, JSON-RPC, gRPC | 3 | 10 | 2 | 7 | 1 | 0 | 23 |
| metrics endpoint | 0 | 0 | 1 | 0 | 1 | 0 | 2 |
| other CLI commands | 0 | 0 | 7 | 2 | 1 | 43 | 53 |
| other | 0 | 0 | 3 | 0 | 0 | 3 | 6 |
| **Total** | **6** | **45** | **140** | **115** | **16** | **46** | **368** |

The implementation adds explicit recovery/aggregation events and ends at 378
static calls (Trace 6 / Debug 53 / Info 140 / Warn 117 / Error 16 / Crit 46).
The higher static count is not higher runtime volume: several per-message,
per-retry, and duplicate Info/Warn paths are now Debug, rate-limited, or
state-aggregated.

Frequency classes found during review:

- Per block: `core/blockchain.go` is Trace/Debug and therefore opt-in. Normal
  Info does not log every imported block.
- Per sync window: compact import progress moved from 8 seconds to 30 seconds;
  wall-clock summaries remain 5m/30m/1h/1d.
- Per peer/message: handshake and expected light-node disconnects are Debug;
  write-queue and protocol-rate-limit drops are aggregated.
- Per RPC: success is Debug. Standard client errors are now Debug; server and
  application failures remain Warn with bounded fields.
- Long maintenance: history snapshot build and compaction report every 30
  seconds. Offline DB tools generally report every 5–30 seconds as human text.
- Lifecycle/configuration: cmd startup and subsystem start/stop events are
  one-shot Info records.

## Priority findings

### P0 — implemented

1. JSON-RPC log amplification and sensitive error data.

   `internal/rpc/handler.go` previously emitted a Warn for every error response,
   copied up to 1 KiB of arbitrary error data, and accepted an unbounded request
   ID representation. A large batch could expand remote-controlled input into
   much larger Warn output. Logs now hash/length-represent request IDs over 128
   bytes, cap error text at 512 bytes, never copy error data into normal logs,
   classify standard client errors as Debug, and aggregate server/application
   Warn events globally to at most one every 30 seconds. Suppressed errors stay
   available at Debug and are counted on the next Warn. RPC responses are
   unchanged.

No storage, consensus, or network P0 was found.

### P1 — implemented

1. History snapshot silence and ambiguous completion.

   `core/state/snapshots/cold_builder.go` now emits a stable start event, a
   30-second phase watchdog, and one rich final publication event after manifest,
   trust-cache, and stage writes succeed. It reports block/tx ranges, bytes,
   phase durations, throughput, backlog, smoothed net catch-up rate, and bounded
   ETA. Standalone wrapper success messages were demoted to Debug to avoid a
   second Info completion.

2. Prune cursor semantics.

   `core/state/pruning/worker.go` carries actual start/pruned-through block and
   tx cursors. `pruner.go` distinguishes `headBlock`, `solidifiedBlock`,
   policy-derived `targetPruneThrough`, and actual `prunedThroughBlock`. Cursor
   and history rate fields appear only when history rows changed. Atomic stats
   and metrics include the tx cursor.

3. Sync operator signal/noise.

   `net/sync.go` emits compact 30-second `Sync import progress` at Info and
   detailed execution/state diagnostics only at Debug. Field names use
   `*PerSec`, `*Pct`, and `remaining`. Wall-clock ETA is suppressed until at
   least 80% of the requested time window is covered. Historical peers outside
   the required range are Debug per peer and Info once per minute in aggregate.

4. Peer drop storms.

   `net/handler.go` and `p2p/peer.go` keep per-event detail at Debug while Warn
   reports are emitted at most once per minute with
   `droppedSinceLastReport` and a sample message/code. Counters and report
   timestamps are atomic because send/admission paths can be concurrent.

5. Producer retry storms and missing recovery.

   `core/producer/producer.go` keeps every 500 ms production retry unchanged,
   but low-participation and production-failure logs are state based: first
   entry, at most one 10-minute continuation summary, then recovery. Summaries
   report skipped/failed slots and suppressed retry attempts. Scheduled-witness
   lookup now records recovery and permits a later recurrence to warn again.
   Slot number and slot timestamp milliseconds are no longer conflated.

6. Unified maintenance blind spots.

   `core/state/pruning/lifecycle.go` adds one change-only completion summary for
   latest snapshots, chain-freezer snapshots, chain lookup/bloom/balance prune,
   state-change posting reclamation, and retired files. No-op passes stay quiet.

### P1 — open, intentionally not expanded in this patch

1. Latest full-keyspace and chain-freezer/event-log builds still lack an
   internal phase-aware 30-second watchdog. The unified lifecycle now records
   final success, but a single long pass can still be silent. Adding progress
   callbacks inside the builders should be a separate, measured change because
   it crosses ETL/verification loops and the shared heavy-work gate.

2. Freezer base passes and online V2 promotion need consistent start/range,
   throughput, backlog, and ETA. The lower layers expose some callbacks, but
   wiring them changes multiple maintenance schedulers. Generic wrapper
   `V2 compaction complete` / `transaction-index maintenance complete` records
   also duplicate richer inner completions.

3. Pebble write-stall warnings are already limited to once per minute, but lack
   stall reason, duration, compaction debt, L0 pressure, and a recovery event.
   The callback state is not currently atomic; fields must not be added by
   reading it unsafely.

4. Wallet HTTP, JSON-RPC, and gRPC have incomplete request/status/duration
   metrics. Wallet/JSON-RPC/debug HTTP `Serve` failures are also discarded.
   The safe design is fixed-route/status-class metrics and rate-limited 5xx
   summaries without body, transaction, signature, calldata, revert data, or
   client IP.

5. DPoS verification warns once per rejected block and the outer network path
   may warn again for the same failure. A reason-keyed limiter needs to preserve
   evidence of consensus rejection and is deferred rather than silently
   downgrading it.

6. Txpool full/reject states have neither admission counters nor enter/recover
   events. These should be added with pool gauges and reason counters, not
   per-transaction logs.

7. Node lifecycle rollback and shutdown discard component `Stop` errors, and
   the main process has no final shutdown-complete event. Adding component type,
   phase, and elapsed context is safe, but changing the lifecycle error contract
   or shutdown ordering is outside a logging-only patch.

8. Snapshot/prune failures after a durable partial step need explicit phase and
   committed-boundary context. A manifest may already be integrated before a
   later cache/stage write fails, and a hot-history batch may commit before a
   later code/checkpoint step fails. Returning partial stats or changing retry
   behavior touches recovery semantics, so this needs a dedicated design and
   fault-injection tests.

### P2

- Actuator/VM having no per-transaction logger calls is desirable. Reverts,
  out-of-energy, and validation failures are transaction outcomes; aggregate
  them at block/sync/metrics layers instead of logging payloads.
- `core/block_builder.go` can emit one Debug per invalid pending transaction and
  then producer emits an aggregate eviction Debug. Per-tx details should move
  to Trace after an error-kind aggregation is available.
- Snapshot compaction `percent` was a string and ETA was always present as
  `0s`; it is now numeric `progressPct`, and ETA is omitted until meaningful.
- Most modules still use the human message as the event identifier rather than
  a dedicated stable `event` field. The modified paths keep messages stable and
  fields structured; a repository-wide event taxonomy should be introduced
  with dashboard/alert migration rather than as a blind bulk rewrite.
- Raw freezer/Pebble and vendored RPC loggers do not consistently carry the
  project `module` field. A bulk migration would affect filtering and should be
  reviewed separately.
- Metrics names mix durations with implicit nanoseconds and use some gauges as
  counters. Renaming/removing them would break dashboards; introduce compatible
  `_seconds`, `_bytes`, and `_total` metrics before deprecation.
- Shielded historical compatibility bypasses and exact mainnet state repairs
  need non-sensitive one-shot/aggregate observability. Never log proof,
  nullifier, ciphertext, address, calldata, or private key material.

## Expected production change

- Normal sync drops from one broad Info record every 8 seconds to one compact
  progress record every 30 seconds; heavy execution diagnostics require
  `net/sync=debug`.
- Expected peer handshakes/light-node range failures no longer dominate Info.
  Persistent peer overload produces at most one Warn per peer/category/minute.
- A stuck history snapshot shows its current phase every 30 seconds and ends in
  exactly one rich publish record. Catch-up ETA is absent while unmeasurable and
  protected from duration overflow.
- Prune alerts can distinguish policy target from committed cursor and no longer
  misread code-only reclamation as history block zero.
- Persistent producer degradation produces an entry event, ten-minute summaries,
  and recovery rather than up to six warnings per three-second slot.
- Remote JSON-RPC IDs/error data cannot create unbounded normal log fields;
  routine client mistakes disappear from production Warn volume, and remotely
  triggerable server/application errors produce at most one Warn per 30 seconds.

## Verification

All final gates passed:

- `go test ./... -count=1 -timeout 300s`
- `go test -race ./core/state/snapshots ./core/state/pruning ./net ./p2p ./core/producer ./internal/rpc -count=1 -timeout 300s`
- after the final snapshot/RPC refinements,
  `go test -race ./core/state/snapshots ./internal/rpc -count=1 -timeout 300s`
- `go vet ./...`
- `git diff --check`

The focused tests cover structured field semantics, numeric units, no-op/change
summaries, cursor resume, watchdog stop concurrency, rate-limit accounting,
producer degradation/recovery, ETA warm-up/overflow, and RPC field bounding.

No commit or push is part of this audit.
