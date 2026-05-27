package snapshots

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	gtronlog "github.com/tronprotocol/go-tron/common/log"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

var coldSnapshotLog = gtronlog.NewModule("core/state/snapshots")

const (
	defaultColdSnapshotInterval    = time.Minute
	defaultColdSnapshotBatchBlocks = uint64(5_000)
)

// DefaultLatestBuildBlocks is the default latest-snapshot build cadence
// (~33h of TRON blocks). Coarse because latest builds full-scan every latest
// keyspace; CommitmentBranch shares this cadence too.
const DefaultLatestBuildBlocks = defaultColdSnapshotBatchBlocks * 8 // 40_000

// ChainSource is the narrow chain surface needed by the cold snapshot builder.
type ChainSource interface {
	DB() AggregatorDB
	LatestSolidifiedBlockNum() int64
}

// Config controls the cold history snapshot builder lifecycle.
type Config struct {
	Dir                    string
	Enabled                bool
	HistoryDataset         SegmentDataset
	Interval               time.Duration
	HistoryWindow          uint64
	BatchBlocks            uint64
	CompactMinSegments     int
	CompactMaxTxSpan       uint64
	RetainObsoleteSegments bool
	// LatestBuildBlocks is the minimum number of solidified blocks that must
	// elapse between production latest-snapshot builds. 0 disables the latest
	// build pass entirely. Latest builds are full-keyspace scans, so all latest
	// datasets share this single coarse cadence rather than rebuilding every tick.
	LatestBuildBlocks uint64
}

// PassResult describes a single cold snapshot builder pass.
type PassResult struct {
	Built             bool
	LatestBuilt       bool
	Compaction        HistoryCompactionResult
	FromTxNum         uint64
	ToTxNum           uint64
	CutoffBlock       uint64
	SolidifiedBlock   uint64
	PreviousVisibleTx uint64
	Segment           SegmentRef
	Segments          []SegmentRef
	Manifest          *Manifest
}

// Stats is a thread-safe snapshot of lifecycle progress.
type Stats struct {
	PassesCompleted   uint64
	SegmentsBuilt     uint64
	SegmentsCompacted uint64
	LastSolidified    uint64
	LastCutoffBlock   uint64
	LastVisibleTxEnd  uint64
	LastFromTxNum     uint64
	LastToTxNum       uint64
	LastPassDuration  time.Duration
}

// Runner builds registered history snapshot segments in the background.
type Runner struct {
	chain ChainSource
	cfg   Config

	quit chan struct{}
	done chan struct{}

	startOnce sync.Once
	stopOnce  sync.Once
	passMu    sync.Mutex
	startErr  error

	passesCompleted      atomic.Uint64
	segmentsBuilt        atomic.Uint64
	segmentsCompacted    atomic.Uint64
	lastSolidified       atomic.Uint64
	lastCutoffBlock      atomic.Uint64
	lastVisibleTxEnd     atomic.Uint64
	lastFromTxNum        atomic.Uint64
	lastToTxNum          atomic.Uint64
	lastPassDuration     atomic.Int64
	lastLatestBuildBlock atomic.Uint64
}

func NewRunner(chain ChainSource, cfg Config) *Runner {
	cfg = cfg.applyDefaults()
	return &Runner{
		chain: chain,
		cfg:   cfg,
		quit:  make(chan struct{}),
		done:  make(chan struct{}),
	}
}

func (c Config) applyDefaults() Config {
	if c.HistoryDataset == "" {
		c.HistoryDataset = SegmentDatasetStateDomainChange
	}
	if c.Interval <= 0 {
		c.Interval = defaultColdSnapshotInterval
	}
	if c.BatchBlocks == 0 {
		c.BatchBlocks = defaultColdSnapshotBatchBlocks
	}
	if c.CompactMinSegments == 0 {
		c.CompactMinSegments = 8
	}
	if c.CompactMaxTxSpan == 0 {
		c.CompactMaxTxSpan = c.BatchBlocks * uint64(c.CompactMinSegments)
	}
	return c
}

func (c Config) validate() error {
	if !c.Enabled {
		return nil
	}
	if c.Dir == "" {
		return errors.New("snapshots: cold builder directory is empty")
	}
	if c.Interval <= 0 {
		return fmt.Errorf("snapshots: invalid cold builder interval %s", c.Interval)
	}
	if c.BatchBlocks == 0 {
		return errors.New("snapshots: cold builder batch blocks is zero")
	}
	if c.HistoryDataset == "" {
		c.HistoryDataset = SegmentDatasetStateDomainChange
	}
	cfg, ok := DefaultDomainRegistry().Dataset(c.HistoryDataset)
	if !ok || !cfg.HasHistory {
		return fmt.Errorf("snapshots: unknown cold builder history dataset %q", c.HistoryDataset)
	}
	if cfg.BuildHistory == nil {
		return fmt.Errorf("snapshots: history domain %s has no builder", c.HistoryDataset)
	}
	return nil
}

// Start implements node.Lifecycle.
func (r *Runner) Start() error {
	if r == nil {
		return nil
	}
	r.startOnce.Do(func() {
		if r.quit == nil {
			r.quit = make(chan struct{})
		}
		if r.done == nil {
			r.done = make(chan struct{})
		}
		if err := r.cfg.validate(); err != nil {
			close(r.done)
			r.startErr = err
			return
		}
		if !r.cfg.Enabled {
			close(r.done)
			return
		}
		if r.chain == nil || r.chain.DB() == nil {
			close(r.done)
			r.startErr = errors.New("snapshots: nil cold builder chain or database")
			return
		}
		// Seed lastLatestBuildBlock to the current solidified block so the first
		// production latest build happens one interval later, avoiding a startup
		// full-scan. Persisting this across restarts is a follow-up.
		if r.cfg.LatestBuildBlocks > 0 {
			if block, _, ok, err := r.latestBuildWatermark(); err == nil && ok {
				r.lastLatestBuildBlock.Store(block)
			}
		}
		go r.loop()
		coldSnapshotLog.Info("History cold snapshot builder started",
			"dir", r.cfg.Dir,
			"dataset", r.cfg.HistoryDataset,
			"interval", r.cfg.Interval,
			"historyWindow", r.cfg.HistoryWindow,
			"batchBlocks", r.cfg.BatchBlocks)
	})
	return r.startErr
}

// Stop implements node.Lifecycle.
func (r *Runner) Stop() error {
	if r == nil {
		return nil
	}
	r.stopOnce.Do(func() {
		if r.quit != nil {
			close(r.quit)
		}
	})
	if r.done != nil {
		<-r.done
	}
	coldSnapshotLog.Info("History cold snapshot builder stopped",
		"dataset", r.cfg.HistoryDataset,
		"passes", r.passesCompleted.Load(),
		"segments", r.segmentsBuilt.Load())
	return nil
}

// Snapshot returns a thread-safe copy of runner progress.
func (r *Runner) Snapshot() Stats {
	if r == nil {
		return Stats{}
	}
	return Stats{
		PassesCompleted:   r.passesCompleted.Load(),
		SegmentsBuilt:     r.segmentsBuilt.Load(),
		SegmentsCompacted: r.segmentsCompacted.Load(),
		LastSolidified:    r.lastSolidified.Load(),
		LastCutoffBlock:   r.lastCutoffBlock.Load(),
		LastVisibleTxEnd:  r.lastVisibleTxEnd.Load(),
		LastFromTxNum:     r.lastFromTxNum.Load(),
		LastToTxNum:       r.lastToTxNum.Load(),
		LastPassDuration:  time.Duration(r.lastPassDuration.Load()),
	}
}

// OnePass builds at most one registered history segment, then compacts history,
// then runs one latest-snapshot build pass if the cadence interval has elapsed.
func (r *Runner) OnePass() (PassResult, error) {
	if r == nil {
		return PassResult{}, nil
	}
	r.passMu.Lock()
	defer r.passMu.Unlock()

	start := time.Now()
	result, err := r.onePass()
	if err == nil {
		result.Compaction, err = r.compactHistory()
	}
	if err == nil {
		built, perr := r.latestPass()
		if perr != nil {
			err = perr
		} else {
			result.LatestBuilt = built
		}
	}
	r.recordPass(result, start)
	return result, err
}

func (r *Runner) onePass() (PassResult, error) {
	if !r.cfg.Enabled {
		return PassResult{}, nil
	}
	if err := r.cfg.validate(); err != nil {
		return PassResult{}, err
	}
	if r.chain == nil {
		return PassResult{}, errors.New("snapshots: nil cold builder chain")
	}
	db := r.chain.DB()
	if db == nil {
		return PassResult{}, errors.New("snapshots: nil cold builder database")
	}
	historyCfg, ok := DefaultDomainRegistry().Dataset(r.cfg.HistoryDataset)
	if !ok || historyCfg.BuildHistory == nil {
		return PassResult{}, fmt.Errorf("snapshots: history domain %s is not registered", r.cfg.HistoryDataset)
	}

	solidified := r.chain.LatestSolidifiedBlockNum()
	if solidified <= 0 || uint64(solidified) < r.cfg.HistoryWindow {
		return PassResult{}, nil
	}
	cutoffBlock := uint64(solidified) - r.cfg.HistoryWindow
	result := PassResult{
		SolidifiedBlock: uint64(solidified),
		CutoffBlock:     cutoffBlock,
	}

	cutoffRange, ok, err := historyCfg.HotHistoryTxRangeForBlock(db, cutoffBlock)
	if err != nil {
		return PassResult{}, err
	}
	if !ok {
		return result, nil
	}
	if cutoffRange.EndTxNum < cutoffRange.BeginTxNum {
		return PassResult{}, fmt.Errorf("snapshots: state tx range for block %d is inverted", cutoffBlock)
	}

	visibleEnd, err := coldSnapshotVisibleTxEnd(r.cfg.Dir, r.cfg.HistoryDataset)
	if err != nil {
		return PassResult{}, err
	}
	result.PreviousVisibleTx = visibleEnd
	if visibleEnd == ^uint64(0) {
		return result, nil
	}
	fromTxNum := visibleEnd + 1
	if fromTxNum > cutoffRange.EndTxNum {
		return result, nil
	}

	toTxNum := cutoffRange.EndTxNum
	if r.cfg.BatchBlocks > 0 {
		startBlock, ok, err := firstHotHistoryTxRangeBlockAtOrAfterTx(historyCfg, db, fromTxNum, cutoffBlock)
		if err != nil {
			return PassResult{}, err
		}
		if !ok {
			return result, nil
		}
		batchCutoffBlock := startBlock + r.cfg.BatchBlocks - 1
		if batchCutoffBlock < startBlock || batchCutoffBlock > cutoffBlock {
			batchCutoffBlock = cutoffBlock
		}
		if batchCutoffBlock != cutoffBlock {
			cutoffBlock = batchCutoffBlock
			result.CutoffBlock = cutoffBlock
			cutoffRange, ok, err = historyCfg.HotHistoryTxRangeForBlock(db, cutoffBlock)
			if err != nil {
				return PassResult{}, err
			}
			if !ok {
				return result, nil
			}
			if cutoffRange.EndTxNum < cutoffRange.BeginTxNum {
				return PassResult{}, fmt.Errorf("snapshots: state tx range for block %d is inverted", cutoffBlock)
			}
			toTxNum = cutoffRange.EndTxNum
		}
	}
	if fromTxNum > toTxNum {
		return result, nil
	}

	refs, err := historyCfg.BuildHistory(db, r.cfg.Dir, fromTxNum, toTxNum, historyCfg.HistoryPath(fromTxNum, toTxNum))
	if err != nil {
		return PassResult{}, err
	}
	if len(refs) == 0 {
		return result, nil
	}
	manifest, err := NewAggregator(r.cfg.Dir).Integrate(fromTxNum, toTxNum, refs)
	if err != nil {
		return PassResult{}, err
	}
	result.Built = true
	result.FromTxNum = fromTxNum
	result.ToTxNum = toTxNum
	result.Segment = refs[0]
	result.Segments = append([]SegmentRef(nil), refs...)
	result.Manifest = manifest
	if writer, ok := db.(ethdb.KeyValueWriter); ok {
		stageProgress := newRawDBStageProgressStore(writer)
		if err := stageProgress.Write(rawdb.StageSnapshotBuild, cutoffBlock); err != nil {
			return PassResult{}, err
		}
		if err := writeManifestProgressStages(stageProgress, manifest.Progress); err != nil {
			return PassResult{}, err
		}
	}
	return result, nil
}

func (r *Runner) recordPass(result PassResult, start time.Time) {
	r.passesCompleted.Add(1)
	if result.Built {
		r.segmentsBuilt.Add(1)
	}
	if result.Compaction.Merged {
		r.segmentsCompacted.Add(uint64(result.Compaction.SegmentsMerged))
	}
	r.lastSolidified.Store(result.SolidifiedBlock)
	r.lastCutoffBlock.Store(result.CutoffBlock)
	r.lastVisibleTxEnd.Store(result.PreviousVisibleTx)
	r.lastFromTxNum.Store(result.FromTxNum)
	r.lastToTxNum.Store(result.ToTxNum)
	r.lastPassDuration.Store(int64(time.Since(start)))
}

func (r *Runner) compactHistory() (HistoryCompactionResult, error) {
	if r == nil || !r.cfg.Enabled {
		return HistoryCompactionResult{}, nil
	}
	return CompactHistoryDomain(r.cfg.Dir, r.cfg.HistoryDataset, CompactionConfig{
		MinSegments:    r.cfg.CompactMinSegments,
		MaxTxSpan:      r.cfg.CompactMaxTxSpan,
		DeleteObsolete: !r.cfg.RetainObsoleteSegments,
	})
}

func (r *Runner) latestBuildWatermark() (block uint64, txNum uint64, ok bool, err error) {
	solidified := r.chain.LatestSolidifiedBlockNum()
	if solidified <= 0 {
		return 0, 0, false, nil
	}
	tx, err := StateDomainHistoryTxNumAtBlockEnd(r.chain.DB(), uint64(solidified))
	if err != nil {
		return 0, 0, false, err
	}
	return uint64(solidified), tx, tx > 0, nil
}

func (r *Runner) latestPass() (bool, error) {
	if r == nil || !r.cfg.Enabled || r.cfg.LatestBuildBlocks == 0 {
		return false, nil
	}
	db := r.chain.DB()
	if db == nil {
		return false, errors.New("snapshots: nil cold builder database")
	}
	block, txNum, ok, err := r.latestBuildWatermark()
	if err != nil || !ok {
		return false, err
	}
	prevBlock := r.lastLatestBuildBlock.Load()
	if prevBlock != 0 && block < prevBlock+r.cfg.LatestBuildBlocks {
		return false, nil // not enough blocks elapsed
	}
	res, err := NewAggregator(r.cfg.Dir).BuildLatest(db, AggregatorBuildOptions{FromTxNum: 1, ToTxNum: txNum})
	if err != nil {
		return false, err
	}
	r.lastLatestBuildBlock.Store(block)
	return res != nil && len(res.Segments) > 0, nil
}

func (r *Runner) loop() {
	defer close(r.done)

	if result, err := r.OnePass(); err != nil {
		coldSnapshotLog.Warn("History cold snapshot initial pass failed", "dataset", r.cfg.HistoryDataset, "err", err)
	} else if result.Built {
		coldSnapshotLog.Info("History cold snapshot initial pass built",
			"dataset", r.cfg.HistoryDataset,
			"fromTx", result.FromTxNum,
			"toTx", result.ToTxNum,
			"cutoffBlock", result.CutoffBlock)
	} else if result.Compaction.Merged {
		coldSnapshotLog.Info("History cold snapshot initial pass compacted",
			"dataset", result.Compaction.Dataset,
			"fromTx", result.Compaction.FromTxNum,
			"toTx", result.Compaction.ToTxNum,
			"segments", result.Compaction.SegmentsMerged)
	} else if result.LatestBuilt {
		coldSnapshotLog.Info("Latest cold snapshot pass built", "dataset", "all-latest", "toBlock", r.lastLatestBuildBlock.Load())
	}

	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			result, err := r.OnePass()
			if err != nil {
				coldSnapshotLog.Warn("History cold snapshot pass failed", "dataset", r.cfg.HistoryDataset, "err", err)
			} else if result.Built {
				coldSnapshotLog.Info("History cold snapshot pass built",
					"dataset", r.cfg.HistoryDataset,
					"fromTx", result.FromTxNum,
					"toTx", result.ToTxNum,
					"cutoffBlock", result.CutoffBlock)
			} else if result.Compaction.Merged {
				coldSnapshotLog.Info("History cold snapshot pass compacted",
					"dataset", result.Compaction.Dataset,
					"fromTx", result.Compaction.FromTxNum,
					"toTx", result.Compaction.ToTxNum,
					"segments", result.Compaction.SegmentsMerged)
			} else if result.LatestBuilt {
				coldSnapshotLog.Info("Latest cold snapshot pass built", "dataset", "all-latest", "toBlock", r.lastLatestBuildBlock.Load())
			}
		case <-r.quit:
			return
		}
	}
}

func coldSnapshotVisibleTxEnd(dir string, dataset SegmentDataset) (uint64, error) {
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	visibleEnd := ContiguousHistoryVisibleTxEnd(manifest, dataset, 1)
	if manifest.Progress != nil && manifest.Progress.HistoryBuildTxNum > visibleEnd {
		return 0, fmt.Errorf("snapshots: history progress %d exceeds visible %s coverage %d", manifest.Progress.HistoryBuildTxNum, dataset, visibleEnd)
	}
	return visibleEnd, nil
}

func firstHotHistoryTxRangeBlockAtOrAfterTx(cfg DomainCfg, db AggregatorDB, txNum, cutoffBlock uint64) (uint64, bool, error) {
	if cfg.IterateHotHistoryTxRanges == nil {
		return 0, false, fmt.Errorf("snapshots: %s missing hot history tx-range iterator", cfg.Dataset)
	}
	var block uint64
	var found bool
	if err := cfg.IterateHotHistoryTxRanges(db, func(row *rawdb.StateTxRange) (bool, error) {
		if row.EndTxNum < row.BeginTxNum {
			return false, fmt.Errorf("snapshots: state tx range for block %d is inverted", row.BlockNum)
		}
		if row.BlockNum > cutoffBlock {
			return false, nil
		}
		if row.EndTxNum < txNum {
			return true, nil
		}
		block = row.BlockNum
		found = true
		return false, nil
	}); err != nil {
		return 0, false, err
	}
	return block, found, nil
}

func stateDomainChangeHistorySegmentPath(fromTxNum, toTxNum uint64) string {
	if cfg, ok := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange); ok {
		return cfg.HistoryPath(fromTxNum, toTxNum)
	}
	return fmt.Sprintf("history/state-domain-change-%d-%d.seg", fromTxNum, toTxNum)
}
