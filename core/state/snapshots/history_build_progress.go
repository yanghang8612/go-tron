package snapshots

import (
	"sync"
	"sync/atomic"
	"time"
)

const coldSnapshotBuildProgressInterval = 30 * time.Second

type coldSnapshotBuildProgress struct {
	dataset            SegmentDataset
	fromTx, toTx       uint64
	fromBlock, toBlock uint64
	eligible           uint64
	started            time.Time
	interval           time.Duration
	phase              atomic.Value
	stopOnce           sync.Once
	done               chan struct{}
	wg                 sync.WaitGroup
}

func startColdSnapshotBuildProgress(dataset SegmentDataset, fromTx, toTx, fromBlock, toBlock, eligible uint64, interval time.Duration) *coldSnapshotBuildProgress {
	if interval <= 0 {
		interval = coldSnapshotBuildProgressInterval
	}
	p := &coldSnapshotBuildProgress{
		dataset:   dataset,
		fromTx:    fromTx,
		toTx:      toTx,
		fromBlock: fromBlock,
		toBlock:   toBlock,
		eligible:  eligible,
		started:   time.Now(),
		interval:  interval,
		done:      make(chan struct{}),
	}
	p.phase.Store("history")
	p.wg.Add(1)
	go p.run()
	return p
}

func (p *coldSnapshotBuildProgress) SetPhase(phase string) {
	if p == nil || phase == "" {
		return
	}
	p.phase.Store(phase)
}

func (p *coldSnapshotBuildProgress) Stop() {
	if p == nil {
		return
	}
	p.stopOnce.Do(func() { close(p.done) })
	p.wg.Wait()
}

func (p *coldSnapshotBuildProgress) run() {
	defer p.wg.Done()
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.log()
		case <-p.done:
			return
		}
	}
}

func (p *coldSnapshotBuildProgress) log() {
	if p == nil {
		return
	}
	phase, _ := p.phase.Load().(string)
	backlogBlocks := uint64(0)
	if p.eligible > p.toBlock {
		backlogBlocks = p.eligible - p.toBlock
	}
	coldSnapshotLog.Info("History cold snapshot build progress",
		"dataset", p.dataset,
		"phase", phase,
		"fromTx", p.fromTx,
		"toTx", p.toTx,
		"fromBlock", p.fromBlock,
		"toBlock", p.toBlock,
		"eligibleCutoffBlock", p.eligible,
		"backlogBlocks", backlogBlocks,
		"elapsed", time.Since(p.started).Round(time.Second))
}
