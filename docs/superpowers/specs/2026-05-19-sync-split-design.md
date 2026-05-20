# SyncService refactor — split downloader / fetcher / peer-state — design

**Status:** Proposed
**Author:** yanghang8612
**Date:** 2026-05-19
**Inspiration:** [go-ethereum/eth/downloader](../../../../ethereum/go-ethereum/eth/downloader), [eth/fetcher](../../../../ethereum/go-ethereum/eth/fetcher)
**Related plan:** [2026-05-19-sync-split.md](../plans/2026-05-19-sync-split.md)

## Background

[`net/sync.go`](../../../net/sync.go) is **1323 lines** in a single file
mixing five distinct concerns:

1. **Watchdog** — periodic isolation check, starts sync when chain is stalled
2. **Per-peer download driver** — `syncPeerState`, fetch batches, timer
   rearm, retry list, request dedup via `requestedHashes`
3. **Chain summary / inventory** — build summary, find common block, handle
   inbound `ChainInventory` (with KhaosDB-orphan dedup + the recent
   "sticky pause" flag for InsertBlock failures)
4. **Parallel orchestration** — `maxParallelSyncPeers`, target head tracking,
   peer pool, retry redistribution
5. **Stats reporting** — rolling-window `Imported chain segment` log,
   per-window block/tx counters

The file has grown organically through three bug-fix waves (partial-batch
timer rearm, KhaosDB orphan filter, sticky pause on InsertBlock failure,
parallel peer fan-out). Every new sync bug requires reading the whole file
to find the right place to fix. The `syncPeerState` struct has 16 fields;
the `SyncService` struct has 30+. Test files (`sync_test.go`,
`sync_failover_test.go`, `sync_parallel_test.go`, `sync_stall_test.go`,
`sync_logging_test.go`) each pre-seed different subsets of internal state
to exercise one slice — making them simultaneously brittle and incomplete.

go-ethereum solved the same problem long ago by splitting into:

- **`eth/downloader/`** — master driver; queues; per-peer scheduling
- **`eth/fetcher/`** — block/tx **announcement** path (separate from sync)
- **`eth/protocols/eth/`** — wire codec

This spec proposes the gtron equivalent: split `net/sync.go` into focused
sub-packages with clear interfaces, without changing wire behaviour.

## Goals

- One file per concern; each ≤ 400 LOC
- One test file per concern, each driving its own narrow fixture
- No behaviour change visible on the wire (byte-for-byte same protocol)
- Sticky-pause / KhaosDB-orphan-filter / parallel-peer fan-out and every
  other recent fix preserved
- Easier to add: per-peer reputation, adaptive batch sizing, new sync modes
  (e.g. snap-sync if we ever add state sync)

## Non-goals

- Do NOT change the TRON wire protocol (libp2p framing, message types,
  rate limits)
- Do NOT change PBFT broadcast or `pbft_*` files
- Do NOT introduce a new sync mode in this slice (state sync is separate work)
- Do NOT touch `handler.go`'s dispatch into the sync service beyond the
  one-line type rename

## Target structure

```
net/sync/
  service.go          # SyncService façade — public API the rest of net/ calls
  watchdog.go         # checkIsolation loop, StartSync trigger
  downloader/
    downloader.go     # orchestrates the active sync session
    queue.go          # fetchList / retryList / blockBuffer
    peer_state.go     # per-peer state machine (the old syncPeerState)
    inventory.go      # HandleChainInventory, HandleSyncBlockChain, KhaosDB orphan filter
  fetcher/
    fetcher.go        # gossip path: NEW_BLOCK / FETCH_INV_DATA non-sync inflow
  stats.go            # syncStats, "Imported chain segment" emitter
  pause.go            # sticky-pause state + IsPaused / PausedStatus / EnterPause
```

`net/sync.go` ceases to exist; `net/handler.go` imports `net/sync` and
calls into `service.SyncService` as before.

### Public API (preserved)

```go
type SyncService struct { ... }

func NewSyncService(chain *core.BlockChain, handler *TronHandler) *SyncService
func (ss *SyncService) Start()
func (ss *SyncService) Stop()
func (ss *SyncService) StartSync(peer *p2p.Peer)
func (ss *SyncService) HandleBlock(peer *p2p.Peer, block *types.Block) bool
func (ss *SyncService) HandleChainInventory(peer *p2p.Peer, payload []byte)
func (ss *SyncService) HandleSyncBlockChain(peer *p2p.Peer, payload []byte)
func (ss *SyncService) PeerDisconnected(peer *p2p.Peer)
func (ss *SyncService) IsSyncing() bool
func (ss *SyncService) IsPaused() bool
func (ss *SyncService) PausedStatus() (paused bool, atNum uint64, at time.Time, err error)
```

These are the only methods called from outside the package (mostly from
`handler.go` and `pbft_producer.go`). The split is purely internal.

### Internal interfaces

```go
// Downloader manages an active sync session.
type Downloader interface {
    Start(targetPeer *p2p.Peer, targetHead uint64) error
    Stop()
    HandleBlock(peer *p2p.Peer, block *types.Block) bool
    HandleChainInventory(peer *p2p.Peer, ids []types.BlockID, remain int64)
    Status() (running bool, peers []string, headNum uint64)
}

// Fetcher handles gossip-broadcast blocks (non-sync inflow).
type Fetcher interface {
    Start()
    Stop()
    NotifyBlockBroadcast(peer *p2p.Peer, block *types.Block)
}

// PauseGate is the sticky-pause flag, consulted by Downloader & Watchdog.
type PauseGate interface {
    Paused() bool
    Enter(blockNum uint64, err error)
    Status() (bool, uint64, time.Time, error)
}
```

### Concurrency model

Same as today: one `sync.Mutex` per logical state (downloader has its own,
pause has its own, fetcher has its own). The service façade holds **no
mutex** — callers always go through one of the sub-components which
serialize their own state. This drops one source of lock contention.

## Migration strategy

The refactor is mechanical but touches every test file under `net/`. To
keep it reviewable, we **don't** do "big-bang move." Instead:

1. **Slice 1**: create the new package skeleton, move pure functions
   (`BuildChainSummary`, `FindCommonBlock`) — no behaviour change
2. **Slice 2**: extract `PauseGate` (smallest, well-isolated) into
   `net/sync/pause.go`; existing tests pass through unchanged shim
3. **Slice 3**: extract `Watchdog` + `Stats`
4. **Slice 4**: extract `Downloader` (largest); move tests slice-by-slice
5. **Slice 5**: extract `Fetcher` (currently embedded in `handler.go`'s
   adv-block path)
6. **Slice 6**: delete `net/sync.go`; rename `net/sync/service.go` exposes
   the same names

Each slice is independently reviewable; `make test ./net/...` stays green
after every slice.

## Tests

Today's sync tests live in:

- `net/sync_test.go` — happy path
- `net/sync_failover_test.go` — peer disconnect / retry
- `net/sync_parallel_test.go` — multi-peer fan-out
- `net/sync_stall_test.go` — partial-batch timer rearm, KhaosDB-orphan
  filter, insert-failure pause
- `net/sync_logging_test.go` — `Imported chain segment` emission

Post-refactor:

- `net/sync/downloader/peer_state_test.go` — single-peer state machine
- `net/sync/downloader/queue_test.go` — fetchList eviction, retry order
- `net/sync/downloader/inventory_test.go` — orphan filter, common-block
- `net/sync/downloader/parallel_test.go` — multi-peer
- `net/sync/pause_test.go` — sticky flag
- `net/sync/stats_test.go` — emitter
- `net/sync/fetcher/fetcher_test.go` — gossip dedup
- `net/sync/integration_test.go` — full-flow smoke (one test that
  exercises the service façade end-to-end)

## Compatibility / observability

- All existing log lines preserved (operators grep for these — don't break
  ops monitoring)
- All exported metric names preserved
- `net/sync.go`'s public types kept as type aliases in
  `net/sync/service.go` for one release cycle, then removed

## Risks

- Refactors that touch sync behavior usually introduce subtle races.
  Mitigation: incremental slicing + `go test -race ./net/...` at each step.
- The `bc.buffer`-aware paths inside downloader (fetch-list dedup vs
  KhaosDB orphans) are subtle — slice 4 will need extra care when moving
  `HandleChainInventory`.

## Acceptance criteria

- `net/sync.go` deleted; longest sub-file ≤ 400 LOC
- `make test ./net/...` green after each slice (no big-bang)
- All existing sync-bug regression tests preserved (partial-batch,
  KhaosDB-orphan, last-block-insert-pause)
- Live Nile sync resumes & completes from genesis on the refactored
  binary, byte-for-byte identical wire traffic to today's binary
- Race detector clean (`go test -race -count=3 ./net/...`)

## Out of scope / future

- **Snap-sync (state sync)** — separate future spec when state-history
  index lands and we can build snapshot chunks
- **Adaptive batch sizing** — per-peer latency-aware tuning. Worth doing
  but on the refactored code, not in this slice
- **Peer scoring** — track per-peer success/failure rate, demote bad peers.
  Same — easier post-split
