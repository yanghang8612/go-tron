package snapshots

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
)

const historyCompactionProgressInterval = 30 * time.Second

const (
	historyCompactionPhaseIdle int64 = iota
	historyCompactionPhaseValidate
	historyCompactionPhaseCollectKeys
	historyCompactionPhaseBuildDictionary
	historyCompactionPhaseWriteTxRanges
	historyCompactionPhaseCopyRecords
	historyCompactionPhaseFinalizeHistory
	historyCompactionPhaseVerifyHistory
	historyCompactionPhaseBuildAccessor
)

var historyCompactionPhaseNames = map[int64]string{
	historyCompactionPhaseIdle:            "idle",
	historyCompactionPhaseValidate:        "validate-sources",
	historyCompactionPhaseCollectKeys:     "collect-keys",
	historyCompactionPhaseBuildDictionary: "build-dictionary",
	historyCompactionPhaseWriteTxRanges:   "write-tx-ranges",
	historyCompactionPhaseCopyRecords:     "copy-records",
	historyCompactionPhaseFinalizeHistory: "finalize-history",
	historyCompactionPhaseVerifyHistory:   "verify-history",
	historyCompactionPhaseBuildAccessor:   "build-accessor",
}

type historyCompactionLiveMetrics struct {
	active           *metrics.Gauge
	phase            *metrics.Gauge
	recordsProcessed *metrics.Gauge
	recordsTotal     *metrics.Gauge
	sourcesProcessed *metrics.Gauge
	sourcesTotal     *metrics.Gauge
	elapsed          *metrics.Gauge
	remapRows        *metrics.Gauge
}

var productionHistoryCompactionMetrics = historyCompactionLiveMetrics{
	active:           metrics.GetOrRegisterGauge(defaultColdSnapshotMetrics+"compaction/current/active", nil),
	phase:            metrics.GetOrRegisterGauge(defaultColdSnapshotMetrics+"compaction/current/phase", nil),
	recordsProcessed: metrics.GetOrRegisterGauge(defaultColdSnapshotMetrics+"compaction/current/records/processed", nil),
	recordsTotal:     metrics.GetOrRegisterGauge(defaultColdSnapshotMetrics+"compaction/current/records/total", nil),
	sourcesProcessed: metrics.GetOrRegisterGauge(defaultColdSnapshotMetrics+"compaction/current/sources/processed", nil),
	sourcesTotal:     metrics.GetOrRegisterGauge(defaultColdSnapshotMetrics+"compaction/current/sources/total", nil),
	elapsed:          metrics.GetOrRegisterGauge(defaultColdSnapshotMetrics+"compaction/current/elapsed", nil),
	remapRows:        metrics.GetOrRegisterGauge(defaultColdSnapshotMetrics+"compaction/current/key_remap/rows", nil),
}

type historyCompactionProgress struct {
	dataset SegmentDataset
	fromTx  uint64
	toTx    uint64
	started time.Time
	metrics historyCompactionLiveMetrics

	phase            atomic.Int64
	recordsProcessed atomic.Uint64
	recordsTotal     atomic.Uint64
	sourcesProcessed atomic.Uint64
	sourcesTotal     atomic.Uint64
	remapRows        atomic.Uint64
	copyStarted      atomic.Int64

	stop chan struct{}
	done chan struct{}
	once sync.Once
}

func newHistoryCompactionProgress(dataset SegmentDataset, fromTx, toTx uint64, sources int) *historyCompactionProgress {
	return newHistoryCompactionProgressWithMetrics(dataset, fromTx, toTx, sources, productionHistoryCompactionMetrics, historyCompactionProgressInterval)
}

func newHistoryCompactionProgressWithMetrics(dataset SegmentDataset, fromTx, toTx uint64, sources int, liveMetrics historyCompactionLiveMetrics, interval time.Duration) *historyCompactionProgress {
	p := &historyCompactionProgress{
		dataset: dataset,
		fromTx:  fromTx,
		toTx:    toTx,
		started: time.Now(),
		metrics: liveMetrics,
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	p.sourcesTotal.Store(uint64(max(sources, 0)))
	p.metrics.active.Update(1)
	p.metrics.phase.Update(historyCompactionPhaseIdle)
	p.metrics.recordsProcessed.Update(0)
	p.metrics.recordsTotal.Update(0)
	p.metrics.sourcesProcessed.Update(0)
	p.metrics.sourcesTotal.Update(coldSnapshotUintGauge(p.sourcesTotal.Load()))
	p.metrics.elapsed.Update(0)
	p.metrics.remapRows.Update(0)
	coldSnapshotLog.Info("History cold snapshot compaction started",
		"dataset", dataset,
		"fromTx", fromTx,
		"toTx", toTx,
		"sources", sources)
	go p.loop(interval)
	return p
}

func (p *historyCompactionProgress) loop(interval time.Duration) {
	defer close(p.done)
	if interval <= 0 {
		interval = historyCompactionProgressInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.logProgress()
		case <-p.stop:
			return
		}
	}
}

func (p *historyCompactionProgress) setPhase(phase int64) {
	if p == nil {
		return
	}
	p.phase.Store(phase)
	if phase == historyCompactionPhaseCopyRecords {
		p.copyStarted.Store(time.Now().UnixNano())
	}
	p.sourcesProcessed.Store(0)
	p.metrics.phase.Update(phase)
	p.metrics.sourcesProcessed.Update(0)
	p.metrics.elapsed.Update(time.Since(p.started).Nanoseconds())
	coldSnapshotLog.Info("History cold snapshot compaction phase started",
		"dataset", p.dataset,
		"phase", historyCompactionPhaseName(phase),
		"fromTx", p.fromTx,
		"toTx", p.toTx,
		"records", p.recordsProcessed.Load(),
		"totalRecords", p.recordsTotal.Load(),
		"elapsed", time.Since(p.started).Round(time.Millisecond))
}

func (p *historyCompactionProgress) setRecordTotal(total uint64) {
	if p == nil {
		return
	}
	p.recordsTotal.Store(total)
	p.metrics.recordsTotal.Update(coldSnapshotUintGauge(total))
}

func (p *historyCompactionProgress) setRecordsProcessed(processed uint64) {
	if p == nil {
		return
	}
	p.recordsProcessed.Store(processed)
	p.metrics.recordsProcessed.Update(coldSnapshotUintGauge(processed))
}

func (p *historyCompactionProgress) setSourcesProcessed(processed uint64) {
	if p == nil {
		return
	}
	p.sourcesProcessed.Store(processed)
	p.metrics.sourcesProcessed.Update(coldSnapshotUintGauge(processed))
}

func (p *historyCompactionProgress) addRemapRows(rows uint64) {
	if p == nil || rows == 0 {
		return
	}
	total := p.remapRows.Add(rows)
	p.metrics.remapRows.Update(coldSnapshotUintGauge(total))
}

func (p *historyCompactionProgress) logProgress() {
	if p == nil {
		return
	}
	elapsed := time.Since(p.started)
	processed := p.recordsProcessed.Load()
	total := p.recordsTotal.Load()
	var recordsPerSecond uint64
	copyElapsed := elapsed
	if started := p.copyStarted.Load(); started > 0 {
		copyElapsed = time.Since(time.Unix(0, started))
	}
	if p.phase.Load() == historyCompactionPhaseCopyRecords && processed > 0 && copyElapsed > 0 {
		recordsPerSecond = uint64(float64(processed) / copyElapsed.Seconds())
	}
	p.metrics.elapsed.Update(elapsed.Nanoseconds())
	ctx := []any{
		"dataset", p.dataset,
		"phase", historyCompactionPhaseName(p.phase.Load()),
		"fromTx", p.fromTx,
		"toTx", p.toTx,
		"records", processed,
		"totalRecords", total,
		"progressPct", historyCompactionPercent(processed, total),
		"recordsPerSecond", recordsPerSecond,
		"sources", p.sourcesProcessed.Load(),
		"totalSources", p.sourcesTotal.Load(),
		"remapRows", p.remapRows.Load(),
		"elapsed", elapsed.Round(time.Millisecond),
	}
	if recordsPerSecond > 0 && total > processed {
		ctx = append(ctx, "eta", (time.Duration((total-processed)/recordsPerSecond) * time.Second).Round(time.Second))
	}
	coldSnapshotLog.Info("History cold snapshot compaction progress", ctx...)
}

func (p *historyCompactionProgress) finish(err error) {
	if p == nil {
		return
	}
	p.once.Do(func() {
		close(p.stop)
		<-p.done
		elapsed := time.Since(p.started)
		p.metrics.elapsed.Update(elapsed.Nanoseconds())
		p.metrics.active.Update(0)
		p.metrics.phase.Update(historyCompactionPhaseIdle)
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			coldSnapshotLog.Info("History cold snapshot compaction canceled",
				"dataset", p.dataset, "phase", historyCompactionPhaseName(p.phase.Load()),
				"fromTx", p.fromTx, "toTx", p.toTx,
				"records", p.recordsProcessed.Load(), "totalRecords", p.recordsTotal.Load(),
				"elapsed", elapsed.Round(time.Millisecond), "err", err)
			return
		}
		if err != nil {
			coldSnapshotLog.Warn("History cold snapshot compaction failed",
				"dataset", p.dataset,
				"phase", historyCompactionPhaseName(p.phase.Load()),
				"fromTx", p.fromTx,
				"toTx", p.toTx,
				"records", p.recordsProcessed.Load(),
				"totalRecords", p.recordsTotal.Load(),
				"elapsed", elapsed.Round(time.Millisecond),
				"err", err)
			return
		}
		coldSnapshotLog.Info("History cold snapshot compaction completed",
			"dataset", p.dataset,
			"fromTx", p.fromTx,
			"toTx", p.toTx,
			"records", p.recordsProcessed.Load(),
			"totalRecords", p.recordsTotal.Load(),
			"remapRows", p.remapRows.Load(),
			"elapsed", elapsed.Round(time.Millisecond))
	})
}

func historyCompactionPhaseName(phase int64) string {
	if name, ok := historyCompactionPhaseNames[phase]; ok {
		return name
	}
	return fmt.Sprintf("unknown-%d", phase)
}

func historyCompactionPercent(processed, total uint64) float64 {
	if total == 0 {
		return 0
	}
	return math.Round(float64(processed)*10_000/float64(total)) / 100
}
