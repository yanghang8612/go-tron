package snapshots

import (
	"os"
	"sync"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

const defaultSectionBloomPruneInterval = time.Minute

type SectionBloomPruneLifecycleConfig struct {
	Dir      string
	Interval time.Duration
}

// SectionBloomPruneLifecycle periodically prunes hot section-bloom rows after
// verified section-bloom snapshot coverage and canonical block history are
// visible.
type SectionBloomPruneLifecycle struct {
	db  *rawdb.ChainDB
	cfg SectionBloomPruneLifecycleConfig

	quit chan struct{}
	done chan struct{}
	once sync.Once
}

func NewSectionBloomPruneLifecycle(db *rawdb.ChainDB, cfg SectionBloomPruneLifecycleConfig) *SectionBloomPruneLifecycle {
	if cfg.Interval <= 0 {
		cfg.Interval = defaultSectionBloomPruneInterval
	}
	return &SectionBloomPruneLifecycle{
		db:   db,
		cfg:  cfg,
		quit: make(chan struct{}),
		done: make(chan struct{}),
	}
}

func (l *SectionBloomPruneLifecycle) Start() error {
	if l == nil {
		return nil
	}
	if l.db == nil || l.cfg.Dir == "" {
		close(l.done)
		return nil
	}
	go l.loop()
	coldSnapshotLog.Info("Section bloom prune lifecycle started", "dir", l.cfg.Dir, "interval", l.cfg.Interval)
	return nil
}

func (l *SectionBloomPruneLifecycle) Stop() error {
	if l == nil {
		return nil
	}
	l.once.Do(func() { close(l.quit) })
	<-l.done
	coldSnapshotLog.Info("Section bloom prune lifecycle stopped")
	return nil
}

func (l *SectionBloomPruneLifecycle) OnePass() (*PruneHotSectionBloomResult, error) {
	if l == nil || l.db == nil || l.cfg.Dir == "" {
		return nil, nil
	}
	manifest, err := LoadProductionManifest(l.cfg.Dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return PruneHotSectionBloomsWithProgress(l.db, l.cfg.Dir, manifest)
}

func (l *SectionBloomPruneLifecycle) loop() {
	defer close(l.done)
	if result, err := l.OnePass(); err != nil {
		coldSnapshotLog.Warn("Section bloom prune initial pass failed", "err", err)
	} else if result != nil && result.HasRange {
		coldSnapshotLog.Info("Section bloom prune initial pass completed",
			"fromSection", result.FromSection,
			"toSection", result.ToSection,
			"coldBloomSegments", result.ColdBloomSegments,
			"rows", result.RowsDeleted)
	}

	ticker := time.NewTicker(l.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			result, err := l.OnePass()
			if err != nil {
				coldSnapshotLog.Warn("Section bloom prune pass failed", "err", err)
			} else if result != nil && result.HasRange {
				coldSnapshotLog.Info("Section bloom prune pass completed",
					"fromSection", result.FromSection,
					"toSection", result.ToSection,
					"coldBloomSegments", result.ColdBloomSegments,
					"rows", result.RowsDeleted)
			}
		case <-l.quit:
			return
		}
	}
}
