# SyncService refactor — plan

**Spec:** [2026-05-19-sync-split-design.md](../specs/2026-05-19-sync-split-design.md)

## Slice 1 — Package skeleton + pure helpers

- [ ] Create `net/sync/` directory + `net/sync/downloader/`,
      `net/sync/fetcher/`
- [ ] `net/sync/service.go` skeleton: empty `SyncService` struct
      embedding today's fields; constructor + `Start/Stop` shells
- [ ] Move `BuildChainSummary`, `FindCommonBlock` to
      `net/sync/downloader/chain_summary.go` (pure functions, no state)
- [ ] Move free constants (`maxChainInventorySize`, `maxFetchBatch`,
      `maxParallelSyncPeers`, `minFetchRequestInterval`,
      `syncFetchTimeout`, `statsReportInterval`) to a single
      `net/sync/constants.go`
- [ ] `make test ./net/...` green; **no behaviour change**

## Slice 2 — Extract PauseGate

- [ ] `net/sync/pause.go`: `PauseGate` struct + `Enter / Paused / Status`
      methods (mirroring today's `paused / pausedAtNum / pausedAtTime /
      pausedErr` fields)
- [ ] Replace `ss.paused / ss.pausedAt*` field reads with
      `ss.pause.X()` shims
- [ ] Move `IsPaused` / `PausedStatus` shims onto the façade,
      delegating to `ss.pause`
- [ ] New `net/sync/pause_test.go` covering: idempotent re-entry, status
      round-trip
- [ ] Existing `sync_stall_test.go::TestInsertFailurePausesSync` passes
      unchanged

## Slice 3 — Extract Watchdog + Stats

- [ ] `net/sync/watchdog.go`: `Watchdog` struct, `checkIsolation` loop,
      ticker management
- [ ] Watchdog takes `Downloader`, `PauseGate`, `*core.BlockChain`, and
      `*TronHandler` (peer source). Calls `Downloader.Start(...)` on stall.
- [ ] `net/sync/stats.go`: `Stats` struct, `syncStats` accumulator, the
      rolling "Imported chain segment" emitter goroutine
- [ ] Tests: `watchdog_test.go` (stall detection), `stats_test.go`
      (window emission boundary)

## Slice 4 — Extract Downloader

This is the big one — split into sub-steps:

- [ ] 4a — `net/sync/downloader/peer_state.go`: move `syncPeerState`
      struct + its lifecycle methods (`armFetchTimer`,
      `onFetchTimeout`, retry list mgmt)
- [ ] 4b — `net/sync/downloader/queue.go`: fetchList + retryList +
      blockBuffer (the per-Downloader cross-peer queue)
- [ ] 4c — `net/sync/downloader/inventory.go`:
      `HandleChainInventory`, `HandleSyncBlockChain`, KhaosDB orphan
      filter, `requestedHashes` dedup
- [ ] 4d — `net/sync/downloader/downloader.go`: top-level Downloader
      that wires Peer + Queue + Inventory + Block handler. Manages
      target head, parallel peer fan-out
- [ ] Move all `sync_*_test.go` files into the downloader package and
      retarget to internal Downloader API
- [ ] One new integration test that exercises Downloader end-to-end
      with two fake peers, asserts byte-for-byte identical message
      traffic compared to a pre-refactor capture
- [ ] `go test -race ./net/sync/...` clean

## Slice 5 — Extract Fetcher (gossip path)

- [ ] `net/sync/fetcher/fetcher.go`: handle adv-broadcast blocks (the
      path in `net/handler.go::handleBlock` that currently calls
      `chain.InsertBlock` after `IsSyncing()` returns false)
- [ ] Dedup recent broadcast hashes (mirror geth's `fetcher.go` LRU
      of size ~512)
- [ ] Test: spam the same hash 1000× from N peers, assert single
      InsertBlock call
- [ ] Adv-block hook + cheat-detector wiring preserved

## Slice 6 — Delete old file + final sweep

- [ ] Remove `net/sync.go`
- [ ] Update `net/handler.go`: `import "github.com/tronprotocol/go-tron/net/sync"`
      + use `sync.SyncService` (rename type)
- [ ] Update `cmd/gtron/main.go`: same import / type changes
- [ ] Update `net/pbft_producer.go`: `sync.SyncService` for the
      `IsSyncing`/`IsPaused` reads
- [ ] `make test ./...` green
- [ ] `go test -race -count=3 ./net/...` clean

## Acceptance criteria

- [ ] `net/sync.go` deleted
- [ ] No file in `net/sync/` exceeds 400 LOC
- [ ] All pre-refactor sync regression tests preserved and green
- [ ] Wire-format capture from new binary vs old binary byte-identical
      on a 100-block Nile-segment sync
- [ ] Race detector clean across full test sweep
