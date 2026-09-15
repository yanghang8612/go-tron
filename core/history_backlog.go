package core

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

// HistoryBacklogSample is a scheduling observation, not permission to delete
// history. SnapshotBuild is written after cold catalog publication; actual
// pruning must still establish its independent manifest and canonical proofs.
type HistoryBacklogSample struct {
	SampledAt                                        time.Time
	Head, Finish, Solidified, Eligible, Covered, Lag uint64
}

var ErrHistoryBacklogBusy = errors.New("history backlog: chain writer busy")

// SampleHistoryBacklog reads hash-verified durable watermarks without opening
// history packs or loading the catalog. It never waits for session or chain
// locks. Individual storage reads are not interruptible, so ctx requests
// cooperative cancellation, not a hard latency bound. Call only outside an
// active insert session.
func (bc *BlockChain) SampleHistoryBacklog(ctx context.Context, historyWindow uint64) (HistoryBacklogSample, error) {
	var out HistoryBacklogSample
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	if bc == nil || bc.db == nil || bc.chaindb == nil {
		return out, errors.New("history backlog: unavailable database")
	}
	// An async insert worker can publish head/DP without chainmu. Refuse the
	// entire live-session lifetime, using the same lock order as Close.
	if !bc.insertSessionGate.TryLock() {
		return out, ErrHistoryBacklogBusy
	}
	defer bc.insertSessionGate.Unlock()
	if !bc.chainmu.TryLock() {
		return out, ErrHistoryBacklogBusy
	}
	defer bc.chainmu.Unlock()
	if err := ctx.Err(); err != nil {
		return out, err
	}
	if bc.closed.Load() || bc.config == nil || !bc.config.HistoryEnabled {
		return out, errors.New("history backlog: closed chain or history disabled")
	}
	// Cold publication does not take chainmu and can follow a background
	// durable flush. Read its dependent watermark first, then Finish, so a
	// normal forward publication cannot appear to precede its prerequisite.
	covered, _, err := rawdb.ReadVerifiedStageProgressBlockWithHashLookup(bc.db, rawdb.StageSnapshotBuild, bc.readCanonicalHashStrict)
	if err != nil {
		return HistoryBacklogSample{}, fmt.Errorf("history backlog: snapshot build: %w", err)
	}
	finish, found, err := rawdb.ReadVerifiedStageProgressBlockWithHashLookup(bc.db, rawdb.StageFinish, bc.readCanonicalHashStrict)
	if err != nil {
		return HistoryBacklogSample{}, fmt.Errorf("history backlog: finish: %w", err)
	}
	head := bc.CurrentBlock()
	if head == nil {
		return HistoryBacklogSample{}, errors.New("history backlog: missing canonical head")
	}
	out.Head = head.Number()
	if !found {
		// A new database can start before its first Finish row. Do not turn a
		// missing watermark on an existing chain into apparent empty backlog.
		if out.Head != 0 {
			return HistoryBacklogSample{}, errors.New("history backlog: missing finish progress")
		}
		hash, ok, err := bc.readCanonicalHashStrict(0)
		if err != nil || !ok || hash != head.Hash() {
			return HistoryBacklogSample{}, fmt.Errorf("history backlog: unverified genesis: %w", errors.Join(err, errors.New("canonical genesis unavailable or different")))
		}
	}
	if finish > out.Head {
		return HistoryBacklogSample{}, errors.New("history backlog: finish ahead of canonical head")
	}
	out.Finish = finish
	if covered > finish {
		return HistoryBacklogSample{}, errors.New("history backlog: cold watermark ahead of durable finish")
	}
	out.Covered = covered
	// The published DP cache is immutable. Avoid copying all dynamic
	// properties or loading rooted state while holding the chain lock.
	bc.dynPropsCacheMu.RLock()
	var solidified int64
	if bc.dynPropsCache != nil {
		solidified = bc.dynPropsCache.LatestSolidifiedBlockNum()
	}
	known := bc.dynPropsCache != nil || out.Head == 0
	bc.dynPropsCacheMu.RUnlock()
	if !known || solidified < 0 || uint64(solidified) > out.Head {
		return HistoryBacklogSample{}, errors.New("history backlog: unavailable or invalid solidified height")
	}
	out.Solidified = uint64(solidified)
	if out.Solidified > historyWindow {
		out.Eligible = min(out.Solidified-historyWindow, finish)
	}
	if out.Eligible > covered {
		out.Lag = out.Eligible - covered
	}
	if err := ctx.Err(); err != nil {
		return HistoryBacklogSample{}, err
	}
	out.SampledAt = time.Now()
	return out, nil
}
