package domains

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/common"
	gtronlog "github.com/tronprotocol/go-tron/common/log"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

const commitmentRebuildProgressInterval = 30 * time.Second

var commitmentRebuildLog = gtronlog.NewModule("core/state/commitment")

var (
	commitmentRebuildStartedCounter   = metrics.NewRegisteredCounter("state/commitment/rebuild/started", nil)
	commitmentRebuildCompletedCounter = metrics.NewRegisteredCounter("state/commitment/rebuild/completed", nil)
	commitmentRebuildFailedCounter    = metrics.NewRegisteredCounter("state/commitment/rebuild/failed", nil)
	commitmentRebuildRowsCounter      = metrics.NewRegisteredCounter("state/commitment/rebuild/rows_scanned", nil)
	commitmentRebuildBytesCounter     = metrics.NewRegisteredCounter("state/commitment/rebuild/bytes_scanned", nil)
	commitmentRebuildBatchesCounter   = metrics.NewRegisteredCounter("state/commitment/rebuild/batches_folded", nil)
	commitmentRebuildWallNanosCounter = metrics.NewRegisteredCounter("state/commitment/rebuild/wall_nanos", nil)

	commitmentRebuildActiveGauge       = metrics.NewRegisteredGauge("state/commitment/rebuild/active", nil)
	commitmentRebuildCurrentRowsGauge  = metrics.NewRegisteredGauge("state/commitment/rebuild/current_rows_scanned", nil)
	commitmentRebuildCurrentBytesGauge = metrics.NewRegisteredGauge("state/commitment/rebuild/current_bytes_scanned", nil)
	commitmentRebuildCurrentBatchGauge = metrics.NewRegisteredGauge("state/commitment/rebuild/current_batches_folded", nil)
	commitmentRebuildElapsedGauge      = metrics.NewRegisteredGauge("state/commitment/rebuild/elapsed_seconds", nil)
	commitmentRebuildLastProgressGauge = metrics.NewRegisteredGauge("state/commitment/rebuild/last_progress_unix", nil)
)

var commitmentRebuildActiveCount atomic.Int64

// CommitmentRebuildActive reports whether this process is currently rebuilding
// a complete commitment branch index. Sync recovery uses it to avoid treating a
// known, observable maintenance operation as a wedged fetch scheduler.
func CommitmentRebuildActive() bool { return commitmentRebuildActiveCount.Load() > 0 }

type commitmentRebuildContext struct {
	reason            string
	trigger           string
	snapshotTxNum     uint64
	markerRoot        common.Hash
	snapshotRoot      common.Hash
	snapshotRootFound bool
	hasSnapshotRoots  bool
}

type commitmentRebuildProgress struct {
	context  commitmentRebuildContext
	mode     string
	started  time.Time
	interval time.Duration

	phase  atomic.Value
	source atomic.Value

	rows             atomic.Uint64
	sourceRows       atomic.Uint64
	bytes            atomic.Uint64
	batches          atomic.Uint64
	lastProgressNano atomic.Int64

	stopOnce sync.Once
	done     chan struct{}
	wg       sync.WaitGroup
}

func startCommitmentRebuildProgress(context commitmentRebuildContext, mode string, maxEntries, maxBytes int, interval time.Duration) *commitmentRebuildProgress {
	if context.reason == "" {
		context.reason = "bootstrap"
	}
	if mode == "" {
		mode = "buffered"
	}
	if interval <= 0 {
		interval = commitmentRebuildProgressInterval
	}
	now := time.Now()
	p := &commitmentRebuildProgress{
		context:  context,
		mode:     mode,
		started:  now,
		interval: interval,
		done:     make(chan struct{}),
	}
	p.phase.Store("clear")
	p.source.Store("none")
	p.lastProgressNano.Store(now.UnixNano())

	active := commitmentRebuildActiveCount.Add(1)
	commitmentRebuildActiveGauge.Update(active)
	commitmentRebuildCurrentRowsGauge.Update(0)
	commitmentRebuildCurrentBytesGauge.Update(0)
	commitmentRebuildCurrentBatchGauge.Update(0)
	commitmentRebuildElapsedGauge.Update(0)
	commitmentRebuildLastProgressGauge.Update(now.Unix())
	commitmentRebuildStartedCounter.Inc(1)

	ctx := append(p.baseLogContext(),
		"phase", "clear",
		"maxBatchEntries", maxEntries,
		"maxBatchBytes", maxBytes)
	if context.reason == "branch_base_root_mismatch" {
		commitmentRebuildLog.Warn("Commitment branch rebuild started", ctx...)
	} else {
		commitmentRebuildLog.Info("Commitment branch rebuild started", ctx...)
	}

	p.wg.Add(1)
	go p.run()
	return p
}

func (p *commitmentRebuildProgress) baseLogContext() []any {
	ctx := []any{
		"reason", p.context.reason,
		"mode", p.mode,
	}
	if p.context.trigger != "" {
		ctx = append(ctx, "trigger", p.context.trigger)
	}
	if p.context.hasSnapshotRoots {
		ctx = append(ctx,
			"snapshotTxNum", p.context.snapshotTxNum,
			"markerRoot", p.context.markerRoot,
			"snapshotRootFound", p.context.snapshotRootFound)
		if p.context.snapshotRootFound {
			ctx = append(ctx, "snapshotRoot", p.context.snapshotRoot)
		}
	}
	return ctx
}

func (p *commitmentRebuildProgress) run() {
	defer p.wg.Done()
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.logProgress()
		case <-p.done:
			return
		}
	}
}

func (p *commitmentRebuildProgress) stop() {
	if p == nil {
		return
	}
	p.stopOnce.Do(func() { close(p.done) })
	p.wg.Wait()
}

func (p *commitmentRebuildProgress) setPhase(phase string) {
	if p == nil || phase == "" {
		return
	}
	p.phase.Store(phase)
	p.recordProgressTime(time.Now())
}

func (p *commitmentRebuildProgress) setSource(source rawdb.LatestDomainCommitmentSource, rows, bytes uint64) {
	if p == nil {
		return
	}
	name := string(source)
	previous, _ := p.source.Load().(string)
	if previous == name {
		return
	}
	p.source.Store(name)
	p.sourceRows.Store(0)
	p.observeScan(rows, 0, bytes)
	commitmentRebuildLog.Info("Commitment branch rebuild source started",
		"reason", p.context.reason,
		"mode", p.mode,
		"source", name,
		"rowsScanned", rows,
		"bytesScanned", bytes,
		"elapsed", time.Since(p.started).Round(time.Second))
}

func (p *commitmentRebuildProgress) observeScan(rows, sourceRows, bytes uint64) {
	if p == nil {
		return
	}
	p.rows.Store(rows)
	p.sourceRows.Store(sourceRows)
	p.bytes.Store(bytes)
	p.recordProgressTime(time.Now())
	p.publishCurrentMetrics(time.Now())
}

func (p *commitmentRebuildProgress) beginFold(rows, sourceRows, bytes uint64) {
	if p == nil {
		return
	}
	p.observeScan(rows, sourceRows, bytes)
	p.setPhase("fold")
}

func (p *commitmentRebuildProgress) finishFold(rows, sourceRows, bytes uint64) {
	if p == nil {
		return
	}
	p.batches.Add(1)
	p.phase.Store("scan")
	p.observeScan(rows, sourceRows, bytes)
}

func (p *commitmentRebuildProgress) recordProgressTime(now time.Time) {
	p.lastProgressNano.Store(now.UnixNano())
	commitmentRebuildLastProgressGauge.Update(now.Unix())
}

func (p *commitmentRebuildProgress) publishCurrentMetrics(now time.Time) {
	commitmentRebuildCurrentRowsGauge.Update(int64(p.rows.Load()))
	commitmentRebuildCurrentBytesGauge.Update(int64(p.bytes.Load()))
	commitmentRebuildCurrentBatchGauge.Update(int64(p.batches.Load()))
	commitmentRebuildElapsedGauge.Update(int64(now.Sub(p.started) / time.Second))
}

func (p *commitmentRebuildProgress) logProgress() {
	if p == nil {
		return
	}
	now := time.Now()
	p.publishCurrentMetrics(now)
	phase, _ := p.phase.Load().(string)
	source, _ := p.source.Load().(string)
	rows := p.rows.Load()
	bytes := p.bytes.Load()
	elapsed := now.Sub(p.started)
	seconds := elapsed.Seconds()
	rowsPerSecond, bytesPerSecond := float64(0), float64(0)
	if seconds > 0 {
		rowsPerSecond = roundCommitmentRebuildRate(float64(rows) / seconds)
		bytesPerSecond = roundCommitmentRebuildRate(float64(bytes) / seconds)
	}
	lastProgress := time.Unix(0, p.lastProgressNano.Load())
	commitmentRebuildLog.Info("Commitment branch rebuild progress",
		"reason", p.context.reason,
		"mode", p.mode,
		"phase", phase,
		"source", source,
		"rowsScanned", rows,
		"sourceRowsScanned", p.sourceRows.Load(),
		"bytesScanned", bytes,
		"batchesFolded", p.batches.Load(),
		"rowsPerSecond", rowsPerSecond,
		"bytesPerSecond", bytesPerSecond,
		"elapsed", elapsed.Round(time.Second),
		"sinceLastProgress", now.Sub(lastProgress).Round(time.Second))
}

func roundCommitmentRebuildRate(value float64) float64 {
	return math.Round(value*100) / 100
}

func (p *commitmentRebuildProgress) finish(root common.Hash, rebuildErr error) {
	if p == nil {
		return
	}
	p.stop()
	now := time.Now()
	p.publishCurrentMetrics(now)
	p.recordProgressTime(now)
	elapsed := now.Sub(p.started)
	rows := p.rows.Load()
	bytes := p.bytes.Load()
	batches := p.batches.Load()
	commitmentRebuildRowsCounter.Inc(int64(rows))
	commitmentRebuildBytesCounter.Inc(int64(bytes))
	commitmentRebuildBatchesCounter.Inc(int64(batches))
	commitmentRebuildWallNanosCounter.Inc(elapsed.Nanoseconds())

	active := commitmentRebuildActiveCount.Add(-1)
	if active < 0 {
		commitmentRebuildActiveCount.Store(0)
		active = 0
	}
	commitmentRebuildActiveGauge.Update(active)

	phase, _ := p.phase.Load().(string)
	source, _ := p.source.Load().(string)
	ctx := append(p.baseLogContext(),
		"phase", phase,
		"source", source,
		"rowsScanned", rows,
		"bytesScanned", bytes,
		"batchesFolded", batches,
		"elapsed", elapsed.Round(time.Second))
	if rebuildErr != nil {
		commitmentRebuildFailedCounter.Inc(1)
		ctx = append(ctx, "err", rebuildErr)
		commitmentRebuildLog.Error("Commitment branch rebuild failed", ctx...)
		return
	}
	commitmentRebuildCompletedCounter.Inc(1)
	ctx = append(ctx, "root", root)
	commitmentRebuildLog.Info("Commitment branch rebuild completed", ctx...)
}
