package pruning

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// PostingPruneBoundary is a process-local permission from a successful cold-
// covered hot prune. The proof is captured before pruning, not rebound to the
// current chain when a later posting chunk happens to run.
type PostingPruneBoundary struct {
	PrunedThrough uint64
	ProofHead     uint64
	ProofHash     common.Hash
}

type PostingPruneChunkOutcome struct {
	Result          rawdb.StateChangePostingPruneChunkResult
	Anchor          common.Hash
	Deferred        bool
	BoundaryChanged bool
}

type PostingPruneChunkFunc func(context.Context, PostingPruneBoundary, common.Hash, []byte, rawdb.StateChangePostingPruneLimits) (PostingPruneChunkOutcome, error)

type PostingPruneWorkerConfig struct {
	Boundary         func() PostingPruneBoundary
	Chunk            PostingPruneChunkFunc
	LoadProbe        func() maintenance.StoragePressure
	HeavyWorkGate    *maintenance.HeavyWorkGate
	Interval         time.Duration
	Limits           rawdb.StateChangePostingPruneLimits
	MinAdvanceBlocks uint64
	MetricsNamespace string
}

// PostingPruneWorker owns a single in-memory sweep. It never writes a manifest
// or directory and must stop before the blockchain/database lifecycle.
type PostingPruneWorker struct {
	cfg          PostingPruneWorkerConfig
	ctx          context.Context
	cancel       context.CancelFunc
	done         chan struct{}
	mu           sync.Mutex
	started      bool
	stopped      bool
	target       PostingPruneBoundary
	rejected     PostingPruneBoundary
	anchor       common.Hash
	cursor       []byte
	completed    uint64
	gauges       map[string]*metrics.Gauge
	counts       map[string]uint64
	lastErrorLog time.Time
}

func NewPostingPruneWorker(cfg PostingPruneWorkerConfig) *PostingPruneWorker {
	if cfg.Interval <= 0 {
		cfg.Interval = time.Second
	}
	if cfg.Limits == (rawdb.StateChangePostingPruneLimits{}) {
		cfg.Limits = rawdb.StateChangePostingPruneLimits{MaxScannedRows: 4096, MaxScannedBytes: 1 << 20, MaxDeleteBytes: 256 << 10, MaxDuration: 10 * time.Millisecond}
	}
	if cfg.MinAdvanceBlocks == 0 {
		cfg.MinAdvanceBlocks = DefaultStateChangeIndexPruneIntervalBlocks
	}
	if cfg.MetricsNamespace == "" {
		cfg.MetricsNamespace = "state/prune/posting/"
	}
	ctx, cancel := context.WithCancel(context.Background())
	w := &PostingPruneWorker{cfg: cfg, ctx: ctx, cancel: cancel, done: make(chan struct{}), gauges: make(map[string]*metrics.Gauge), counts: make(map[string]uint64)}
	for _, name := range []string{"enabled", "chunks", "sweeps", "errors", "deferred/boundary", "deferred/pressure", "deferred/gate", "deferred/chain", "resets", "scanned/rows", "scanned/bytes", "deleted/rows", "deleted/bytes", "target/block", "completed/block", "last/duration_ns", "max/duration_ns", "last/scanned_rows", "last/deleted_bytes"} {
		w.gauges[name] = metrics.GetOrRegisterGauge(normalizePrunerMetricNamespace(cfg.MetricsNamespace)+name, nil)
		w.gauges[name].Update(0)
	}
	return w
}

func (w *PostingPruneWorker) Start() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopped {
		return errors.New("posting prune: worker already stopped")
	}
	if w.started {
		return nil
	}
	if w.cfg.Boundary == nil || w.cfg.Chunk == nil || w.cfg.LoadProbe == nil {
		return errors.New("posting prune: missing safety callback")
	}
	limits := w.cfg.Limits
	if limits.MaxScannedRows == 0 || limits.MaxScannedBytes == 0 || limits.MaxDeleteBytes == 0 || limits.MaxDuration <= 0 {
		return errors.New("posting prune: invalid chunk limits")
	}
	w.started = true
	w.gauges["enabled"].Update(1)
	go w.loop()
	return nil
}

func (w *PostingPruneWorker) Stop() error {
	w.mu.Lock()
	w.stopped = true
	w.cancel()
	started := w.started
	w.mu.Unlock()
	if started {
		<-w.done
	}
	w.gauges["enabled"].Update(0)
	return nil
}

func (w *PostingPruneWorker) loop() {
	defer close(w.done)
	for {
		if err := w.runChunk(w.ctx); err != nil && w.ctx.Err() == nil && time.Since(w.lastErrorLog) >= time.Minute {
			log.Warn("Posting prune chunk failed; retaining cursor", "err", err)
			w.lastErrorLog = time.Now()
		}
		timer := time.NewTimer(w.cfg.Interval)
		select {
		case <-w.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (w *PostingPruneWorker) add(name string, n uint64) {
	w.counts[name] += n
	w.gauges[name].Update(prunerUintGauge(w.counts[name]))
}

func postingPrunePressureReady(p maintenance.StoragePressure, now time.Time) bool {
	fresh := func(t time.Time) bool {
		age := now.Sub(t)
		return !t.IsZero() && age >= -time.Second && age <= 15*time.Second
	}
	return p.Available && fresh(p.SampledAt) && p.DeviceAvailable && fresh(p.DeviceSampledAt) &&
		!p.HardLimitReached(now) && p.L0Sublevels < 8 && p.CompactionDebt < 32<<30 &&
		p.DeviceAwait <= 5*time.Millisecond && p.DeviceQueueMilli <= 32_000
}

func (w *PostingPruneWorker) clearSweep() {
	w.target = PostingPruneBoundary{}
	w.anchor = common.Hash{}
	w.cursor = nil
	w.gauges["target/block"].Update(0)
}

// runChunk is owned by loop; tests call it only without a running lifecycle.
func (w *PostingPruneWorker) runChunk(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	boundary := w.cfg.Boundary()
	if boundary.PrunedThrough < w.target.PrunedThrough || boundary.PrunedThrough < w.completed {
		w.clearSweep()
		w.completed = 0
		w.gauges["completed/block"].Update(0)
		w.add("resets", 1)
		w.rejected = boundary
		w.add("deferred/boundary", 1)
		return nil
	}
	if boundary.PrunedThrough == 0 || boundary.ProofHead < boundary.PrunedThrough || boundary.ProofHash == (common.Hash{}) || boundary == w.rejected {
		if w.target.PrunedThrough != 0 || w.completed != 0 {
			w.clearSweep()
			w.completed = 0
			w.gauges["completed/block"].Update(0)
			w.add("resets", 1)
		}
		w.rejected = boundary
		w.add("deferred/boundary", 1)
		return nil
	}
	if w.target.PrunedThrough == 0 {
		if boundary.PrunedThrough <= w.completed || (w.completed > 0 && boundary.PrunedThrough-w.completed < w.cfg.MinAdvanceBlocks) {
			return nil
		}
		w.target = boundary
		w.gauges["target/block"].Update(prunerUintGauge(boundary.PrunedThrough))
	}
	if !postingPrunePressureReady(w.cfg.LoadProbe(), time.Now()) {
		w.add("deferred/pressure", 1)
		return nil
	}
	release, ok := w.cfg.HeavyWorkGate.TryAcquire()
	if !ok {
		w.add("deferred/gate", 1)
		return nil
	}
	defer release()
	started := time.Now()
	out, err := w.cfg.Chunk(ctx, w.target, w.anchor, w.cursor, w.cfg.Limits)
	elapsed := time.Since(started)
	w.gauges["last/duration_ns"].Update(int64(elapsed))
	if uint64(elapsed) > w.counts["max/duration_ns"] {
		w.counts["max/duration_ns"] = uint64(elapsed)
		w.gauges["max/duration_ns"].Update(int64(elapsed))
	}
	w.add("scanned/rows", out.Result.RowsScanned)
	w.add("scanned/bytes", out.Result.BytesScanned)
	w.gauges["last/scanned_rows"].Update(prunerUintGauge(out.Result.RowsScanned))
	w.gauges["last/deleted_bytes"].Update(0)
	if err != nil {
		if ctx.Err() == nil {
			w.add("errors", 1)
		}
		return err
	}
	if out.BoundaryChanged {
		w.rejected = w.target
		w.clearSweep()
		w.completed = 0
		w.gauges["completed/block"].Update(0)
		w.add("resets", 1)
		return nil
	}
	if out.Deferred {
		w.add("deferred/chain", 1)
		return nil
	}
	w.anchor = out.Anchor
	w.cursor = append(w.cursor[:0], out.Result.NextCursor...)
	w.add("chunks", 1)
	w.add("deleted/rows", out.Result.RowsDeleted)
	w.add("deleted/bytes", out.Result.BytesDeleted)
	w.gauges["last/deleted_bytes"].Update(prunerUintGauge(out.Result.BytesDeleted))
	if out.Result.Complete {
		w.completed = w.target.PrunedThrough
		w.gauges["completed/block"].Update(prunerUintGauge(w.completed))
		w.add("sweeps", 1)
		log.Info("Bounded posting-only sweep completed", "prunedThrough", w.completed, "deletedLogicalBytes", w.counts["deleted/bytes"], "chunks", w.counts["chunks"])
		w.clearSweep()
	}
	return nil
}
