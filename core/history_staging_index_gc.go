package core

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// HistoryStagingIndexGCConfig controls optional derived-index maintenance.
// Pressure must be a memory-only observation; admission runs under writer
// guards. Limits are cooperative, not a bound on an individual DB operation.
type HistoryStagingIndexGCConfig struct {
	Interval      time.Duration
	Limits        rawdb.StateChangePostingPruneLimits
	HeavyWorkGate *maintenance.HeavyWorkGate
	Pressure      func() maintenance.StoragePressure
}

// stagingIndexAuthority is private to one worker after runtime receipt
// authentication. A bare height supplied by another subsystem is never
// accepted as a hot-prune or staging-GC permission.
type stagingIndexAuthority struct {
	epoch, through uint64
	anchor         common.Hash
}

type HistoryStagingIndexGC struct {
	bc       *BlockChain
	manager  *rawdb.HistoryStagingManager
	identity rawdb.HistoryStagingIdentity
	cfg      HistoryStagingIndexGCConfig
	pass     sync.Mutex
	mu       sync.Mutex
	cancel   context.CancelFunc
	done     chan struct{}
	stopped  bool

	prefix                         stagingIndexAuthority
	sweep                          stagingIndexAuthority // fixed through both posting and directory phases
	postingCursor, directoryCursor []byte
	directoryPhase                 bool
	completed                      uint64
	counts                         map[string]uint64
	gauges                         map[string]*metrics.Gauge
}

func NewHistoryStagingIndexGC(bc *BlockChain, cfg HistoryStagingIndexGCConfig) (*HistoryStagingIndexGC, error) {
	if bc == nil || bc.HistoryStagingManager() == nil || cfg.Pressure == nil || cfg.HeavyWorkGate == nil {
		return nil, errors.New("staging index GC: incomplete configuration")
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 100 * time.Millisecond
	}
	if cfg.Limits == (rawdb.StateChangePostingPruneLimits{}) {
		cfg.Limits = rawdb.StateChangePostingPruneLimits{MaxScannedRows: 4096, MaxScannedBytes: 1 << 20,
			MaxDeleteBytes: 256 << 10, MaxDuration: 10 * time.Millisecond}
	}
	if cfg.Limits.MaxScannedRows == 0 || cfg.Limits.MaxScannedBytes == 0 || cfg.Limits.MaxDeleteBytes == 0 || cfg.Limits.MaxDuration <= 0 {
		return nil, errors.New("staging index GC: invalid limits")
	}
	manager := bc.HistoryStagingManager()
	if err := manager.VerifyIdentity(); err != nil {
		return nil, fmt.Errorf("staging index GC: manager identity: %w", err)
	}
	identity, present, err := rawdb.ReadHistoryStagingIdentity(bc.db)
	if err != nil || !present {
		return nil, errors.Join(err, rawdb.ErrHistoryStagingUninitialized)
	}
	w := &HistoryStagingIndexGC{bc: bc, manager: manager, identity: identity, cfg: cfg, counts: make(map[string]uint64), gauges: make(map[string]*metrics.Gauge)}
	for _, name := range []string{"enabled", "epoch", "prefix", "cutoff", "completed", "invalidations", "errors", "route_rows", "posting/chunks", "posting/sweeps", "posting/scanned_rows", "posting/scanned_bytes", "posting/deleted_rows", "posting/deleted_bytes", "posting/scan_ns", "posting/write_ns", "directory/chunks", "directory/sweeps", "directory/scanned_rows", "directory/scanned_bytes", "directory/deleted_rows", "directory/deleted_bytes", "directory/scan_ns", "directory/write_ns", "last/guard_held_ns", "max/guard_held_ns", "deferred/readiness", "deferred/writer", "deferred/pressure", "deferred/gate", "deferred/prefix", "deferred/stages", "deferred/unsettled", "deferred/snapshot_busy", "deferred/snapshot_unsupported", "budget/first_row"} {
		w.gauges[name] = metrics.GetOrRegisterGauge("core/history_staging/index_gc/"+name, nil)
		w.gauges[name].Update(0)
	}
	return w, nil
}

func (w *HistoryStagingIndexGC) add(name string, n uint64) {
	w.counts[name] += n
	w.gauges[name].Update(int64(w.counts[name]))
}

func (w *HistoryStagingIndexGC) Start() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopped || w.done != nil {
		return errors.New("staging index GC: already started or stopped")
	}
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel, w.done = cancel, make(chan struct{})
	w.gauges["enabled"].Update(1)
	go func() {
		defer close(w.done)
		var lastLog time.Time
		for {
			started := time.Now()
			if err := w.RunOnce(ctx); err != nil && ctx.Err() == nil && time.Since(lastLog) >= time.Minute {
				log.Warn("Staging history-index GC retained its cursor", "err", err)
				lastLog = time.Now()
			}
			// Optional work must yield proportionally after slow proofs/IO.
			// The elapsed pass includes the guarded work, so waiting nine
			// times as long bounds steady-loop guard duty to about ten percent.
			timer := time.NewTimer(historyStagingIndexGCDelay(w.cfg.Interval, time.Since(started)))
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
	return nil
}

func (w *HistoryStagingIndexGC) Stop() error {
	w.mu.Lock()
	w.stopped = true
	cancel, done := w.cancel, w.done
	w.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
	w.gauges["enabled"].Update(0)
	return nil
}

func (w *HistoryStagingIndexGC) invalidate(epoch uint64) {
	if w.prefix.through != 0 || w.sweep.through != 0 {
		w.add("invalidations", 1)
	}
	w.prefix, w.sweep = stagingIndexAuthority{epoch: epoch}, stagingIndexAuthority{}
	w.postingCursor, w.directoryCursor = nil, nil
	w.completed, w.directoryPhase = 0, false
	w.gauges["epoch"].Update(int64(epoch))
	for _, name := range []string{"prefix", "cutoff", "completed"} {
		w.gauges[name].Update(0)
	}
}

var errStagingIndexAuthorityChanged = errors.New("staging index GC: authority changed")

// RunOnce certifies a bounded metadata page and submits at most one bounded
// index batch. All authority and cursors belong to this process only.
func (w *HistoryStagingIndexGC) RunOnce(ctx context.Context) (err error) {
	if ctx == nil {
		return errors.New("staging index GC: missing context")
	}
	w.pass.Lock()
	defer w.pass.Unlock()
	defer func() {
		if errors.Is(err, errStagingIndexAuthorityChanged) {
			w.invalidate(0)
			err = nil
		} else if err != nil && ctx.Err() == nil {
			w.add("errors", 1)
		}
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	bc := w.bc
	if !bc.historyStagingReady.Load() {
		w.invalidate(0)
		w.add("deferred/readiness", 1)
		return nil
	}
	if classifyHistoryStagingPressure(w.cfg.Pressure(), time.Now()) > historyStagingBusy {
		w.add("deferred/pressure", 1)
		return nil
	}
	if !bc.stateHistoryIndexMu.TryLock() {
		w.add("deferred/writer", 1)
		return nil
	}
	defer bc.stateHistoryIndexMu.Unlock()
	if !bc.chainmu.TryLock() {
		w.add("deferred/writer", 1)
		return nil
	}
	started := time.Now()
	defer func() {
		held := time.Since(started).Nanoseconds()
		w.gauges["last/guard_held_ns"].Update(held)
		if held > w.gauges["max/guard_held_ns"].Snapshot().Value() {
			w.gauges["max/guard_held_ns"].Update(held)
		}
		bc.chainmu.Unlock()
	}()
	if bc.closed.Load() || !bc.historyStagingReady.Load() || bc.historyStagingReplayEpoch.Load() != 0 {
		w.invalidate(0)
		w.add("deferred/readiness", 1)
		return nil
	}
	if e := bc.commitErr.Load(); e != nil {
		return fmt.Errorf("staging index GC: failed canonical commit: %w", *e)
	}
	if e := bc.flushErr.Load(); e != nil {
		return fmt.Errorf("staging index GC: failed canonical flush: %w", *e)
	}
	manager := bc.HistoryStagingManager()
	// Certificates belong to the constructor's manager and source identity.
	// A replacement must construct a fresh worker after authentication.
	if manager == nil || manager != w.manager {
		return errStagingIndexAuthorityChanged
	}
	identity, present, err := rawdb.ReadHistoryStagingIdentity(bc.db)
	if err != nil {
		return err
	}
	if !present || identity != w.identity {
		return errStagingIndexAuthorityChanged
	}
	if err := manager.VerifyIdentity(); err != nil {
		w.invalidate(0)
		return err
	}
	epoch, err := manager.CurrentEpoch()
	if err != nil {
		return err
	}
	intent, present, err := manager.ReadResetIntent()
	if err != nil {
		return err
	}
	if present && (!intent.Complete || intent.NewEpoch != epoch) {
		w.invalidate(0)
		w.add("deferred/readiness", 1)
		return nil
	}
	if w.prefix.epoch != epoch {
		w.invalidate(epoch)
	}
	// The precheck is only a hint. Actual pressure and heavy admission occur
	// after index -> chain acquisition, with no waiting for the heavy token.
	if classifyHistoryStagingPressure(w.cfg.Pressure(), time.Now()) > historyStagingBusy {
		w.add("deferred/pressure", 1)
		return nil
	}
	release, ok := w.cfg.HeavyWorkGate.TryAcquire()
	if !ok {
		w.add("deferred/gate", 1)
		return nil
	}
	defer release()
	boundary, err := w.availableBoundary()
	if err != nil || boundary == 0 {
		return err
	}
	if w.prefix.through != 0 {
		if ok, err := w.verifyAuthority(ctx, w.prefix, boundary); err != nil || !ok {
			return err
		}
	}
	caughtUp, err := w.extendPrefix(ctx, boundary, started)
	if err != nil {
		return err
	}
	if w.prefix.through == 0 || (!caughtUp && w.sweep.through == 0) {
		return nil
	}
	if w.sweep.through == 0 {
		if w.prefix.through <= w.completed {
			return nil
		}
		w.sweep = w.prefix
		w.gauges["cutoff"].Update(int64(w.sweep.through))
	}
	if ok, err := w.verifyAuthority(ctx, w.sweep, boundary); err != nil || !ok {
		return err
	}
	limits := w.remainingLimits(started)
	phase := "posting"
	var result rawdb.StateChangePostingPruneChunkResult
	if w.directoryPhase {
		phase = "directory"
		// All posting writes finish in foreground FlushFinal before the async
		// fold enqueue. Async folding changes commitment/stage/metadata only.
		// chainmu therefore freezes the families inspected here, even while
		// inflight topology can be promoted or atomically flushed in parallel.
		// IndexMu also spans the complete directory-first ETL load.
		view, snapshotErr := bc.buffer.TryNewReadSnapshot()
		if errors.Is(snapshotErr, blockbuffer.ErrReadSnapshotBusy) {
			w.add("deferred/snapshot_busy", 1)
			return nil
		}
		if errors.Is(snapshotErr, blockbuffer.ErrReadSnapshotUnsupported) {
			w.add("deferred/snapshot_unsupported", 1)
			return nil
		}
		if snapshotErr != nil {
			return snapshotErr
		}
		defer view.Close()
		limits = w.remainingLimits(started)
		result, err = rawdb.PruneStagingStateChangeDirectoryChunk(ctx, bc.db, view, w.directoryCursor, limits)
	} else {
		result, err = rawdb.PruneStagingStateChangePostingChunk(ctx, bc.db, w.sweep.through, w.postingCursor, limits)
	}
	w.add(phase+"/scan_ns", uint64(result.ScanDuration))
	w.add(phase+"/write_ns", uint64(result.WriteDuration))
	w.add(phase+"/scanned_rows", result.RowsScanned)
	w.add(phase+"/scanned_bytes", result.BytesScanned)
	if err != nil {
		return err
	}
	w.add(phase+"/chunks", 1)
	w.add(phase+"/deleted_rows", result.RowsDeleted)
	w.add(phase+"/deleted_bytes", result.BytesDeleted)
	if w.directoryPhase {
		w.directoryCursor = result.NextCursor
		if result.Complete {
			w.add("directory/sweeps", 1)
			w.completed = w.sweep.through
			w.gauges["completed"].Update(int64(w.completed))
			w.sweep = stagingIndexAuthority{}
			w.gauges["cutoff"].Update(0)
			w.postingCursor, w.directoryCursor = nil, nil
			w.directoryPhase = false
		}
	} else {
		w.postingCursor = result.NextCursor
		if result.Complete {
			w.add("posting/sweeps", 1)
			w.directoryPhase = true
		}
	}
	return nil
}

func (w *HistoryStagingIndexGC) availableBoundary() (uint64, error) {
	bc := w.bc
	head := bc.CurrentBlock()
	props := bc.cachedDynProps()
	if head == nil || props == nil || props.LatestSolidifiedBlockNum() <= 0 {
		w.add("deferred/stages", 1)
		return 0, nil
	}
	boundary := min(head.Number(), uint64(props.LatestSolidifiedBlockNum()))
	for _, stage := range []rawdb.StageID{rawdb.StageFinish, rawdb.StageStateHistoryIndex} {
		height, exists, err := rawdb.ReadVerifiedStageProgressBlockWithHashLookup(bc.db, stage, bc.readCanonicalHashStrict)
		if err != nil {
			return 0, err
		}
		if !exists {
			w.add("deferred/stages", 1)
			return 0, nil
		}
		boundary = min(boundary, height)
	}
	return boundary, nil
}

func (w *HistoryStagingIndexGC) coldBucket(ctx context.Context, bucket uint64) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	m := w.manager
	route, exists, err := m.ReadRoute(bucket)
	if err != nil || !exists || route.Epoch != w.prefix.epoch || route.Owner != rawdb.HistoryStagingOwnerCold {
		return false, err
	}
	claim, claimed, err := m.ReadClaim(bucket)
	if err != nil || (claimed && claim.Epoch == w.prefix.epoch) {
		return false, err
	}
	binding, exists, err := m.ReadColdBindingAt(w.prefix.epoch, bucket)
	first, last, boundErr := rawdb.StateHistoryChunkBucketBounds(bucket)
	if err != nil || boundErr != nil {
		return false, errors.Join(err, boundErr)
	}
	if !exists || binding.Version != rawdb.HistoryStagingFormatVersion || binding.Epoch != w.prefix.epoch || binding.Bucket != bucket || binding.BindingEpoch != route.ColdBindingEpoch || binding.ManifestEpoch == 0 || !historyStagingBindingFull(binding, first, last) {
		return false, nil
	}
	for _, span := range binding.Spans {
		if span.ContentID == ([32]byte{}) || span.SemanticHash == ([32]byte{}) || span.TxRangeDigest == ([32]byte{}) {
			return false, nil
		}
	}
	return true, nil
}

func (w *HistoryStagingIndexGC) verifyAuthority(ctx context.Context, auth stagingIndexAuthority, available uint64) (bool, error) {
	if auth.epoch != w.prefix.epoch || auth.through == 0 || auth.through > w.prefix.through || auth.anchor == (common.Hash{}) {
		return false, errStagingIndexAuthorityChanged
	}
	if auth.through > available {
		return false, errStagingIndexAuthorityChanged
	}
	hash, exists, err := w.bc.readCanonicalHashStrict(auth.through)
	if err != nil {
		return false, err
	}
	if !exists || hash != auth.anchor {
		return false, errStagingIndexAuthorityChanged
	}
	if ok, err := w.coldBucket(ctx, auth.through/rawdb.StateHistoryChunkBucketBlocks); err != nil || !ok {
		if err != nil {
			return false, err
		}
		return false, errStagingIndexAuthorityChanged
	}
	if !w.bc.buffer.HistoryPrefixSettled(auth.through) {
		w.add("deferred/unsettled", 1)
		return false, nil
	}
	return true, nil
}

func (w *HistoryStagingIndexGC) extendPrefix(ctx context.Context, available uint64, started time.Time) (bool, error) {
	next := uint64(1)
	if w.prefix.through != 0 {
		next = w.prefix.through/rawdb.StateHistoryChunkBucketBlocks + 1
	}
	candidate := w.prefix
	caughtUp := false
	for examined := 0; examined < 256; examined++ {
		if examined > 0 && time.Since(started) >= w.cfg.Limits.MaxDuration {
			break
		}
		_, last, err := rawdb.StateHistoryChunkBucketBounds(next)
		if err != nil {
			return false, err
		}
		if last > available {
			caughtUp = true
			break
		}
		w.add("route_rows", 1)
		ok, err := w.coldBucket(ctx, next)
		if err != nil {
			return false, err
		}
		if !ok {
			w.add("deferred/prefix", 1)
			caughtUp = true
			break
		}
		if !w.bc.buffer.HistoryPrefixSettled(last) {
			w.add("deferred/unsettled", 1)
			caughtUp = true
			break
		}
		candidate.through = last
		next++
	}
	if candidate.through != w.prefix.through {
		hash, exists, err := w.bc.readCanonicalHashStrict(candidate.through)
		if err != nil || !exists {
			return false, errors.Join(err, rawdb.ErrHistoryStagingIncomplete)
		}
		candidate.anchor = hash
		w.prefix = candidate
		w.gauges["prefix"].Update(int64(candidate.through))
	}
	return caughtUp, nil
}

// Required proof/snapshot I/O can itself exceed a cooperative budget. Preserve
// liveness by admitting only one complete raw row in that case, matching the
// schema scanner's first-row policy; never authorize a larger unbounded scan.
func (w *HistoryStagingIndexGC) remainingLimits(started time.Time) rawdb.StateChangePostingPruneLimits {
	limits := w.cfg.Limits
	limits.MaxDuration -= time.Since(started)
	if limits.MaxDuration <= 0 {
		limits.MaxScannedRows = 1
		limits.MaxDuration = time.Nanosecond
		w.add("budget/first_row", 1)
	}
	return limits
}

func historyStagingIndexGCDelay(interval, elapsed time.Duration) time.Duration {
	const maxDuration = time.Duration(1<<63 - 1)
	if elapsed > maxDuration/9 {
		return maxDuration
	}
	return max(interval, 9*max(elapsed, 0))
}
