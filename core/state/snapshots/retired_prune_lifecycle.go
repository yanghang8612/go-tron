package snapshots

import (
	"os"
	"sync"
	"time"
)

const defaultRetiredPruneInterval = time.Minute

type RetiredPruneLifecycleConfig struct {
	Dir      string
	Interval time.Duration
}

// RetiredPruneLifecycle periodically reclaims immutable snapshot files that
// have been replaced in the active manifest view.
type RetiredPruneLifecycle struct {
	cfg RetiredPruneLifecycleConfig

	quit chan struct{}
	done chan struct{}
	once sync.Once
}

func NewRetiredPruneLifecycle(cfg RetiredPruneLifecycleConfig) *RetiredPruneLifecycle {
	if cfg.Interval <= 0 {
		cfg.Interval = defaultRetiredPruneInterval
	}
	return &RetiredPruneLifecycle{
		cfg:  cfg,
		quit: make(chan struct{}),
		done: make(chan struct{}),
	}
}

func (l *RetiredPruneLifecycle) Start() error {
	if l == nil {
		return nil
	}
	if l.cfg.Dir == "" {
		close(l.done)
		return nil
	}
	go l.loop()
	coldSnapshotLog.Info("Retired snapshot segment prune lifecycle started", "dir", l.cfg.Dir, "interval", l.cfg.Interval)
	return nil
}

func (l *RetiredPruneLifecycle) Stop() error {
	if l == nil {
		return nil
	}
	l.once.Do(func() { close(l.quit) })
	<-l.done
	coldSnapshotLog.Info("Retired snapshot segment prune lifecycle stopped")
	return nil
}

func (l *RetiredPruneLifecycle) OnePass() (*PruneRetiredSegmentFilesResult, error) {
	if l == nil || l.cfg.Dir == "" {
		return nil, nil
	}
	if _, err := LoadProductionManifest(l.cfg.Dir); err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return PruneRetiredSegmentFiles(l.cfg.Dir)
}

func (l *RetiredPruneLifecycle) loop() {
	defer close(l.done)
	if result, err := l.OnePass(); err != nil {
		coldSnapshotLog.Warn("Retired snapshot segment prune initial pass failed", "err", err)
	} else if result != nil && result.FilesDeleted > 0 {
		logRetiredPruneResult("Retired snapshot segment prune initial pass completed", result)
	}

	ticker := time.NewTicker(l.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			result, err := l.OnePass()
			if err != nil {
				coldSnapshotLog.Warn("Retired snapshot segment prune pass failed", "err", err)
			} else if result != nil && result.FilesDeleted > 0 {
				logRetiredPruneResult("Retired snapshot segment prune pass completed", result)
			}
		case <-l.quit:
			return
		}
	}
}

func logRetiredPruneResult(msg string, result *PruneRetiredSegmentFilesResult) {
	coldSnapshotLog.Info(msg,
		"retired", result.RetiredSegments,
		"deleted", result.FilesDeleted,
		"missing", result.FilesMissing,
		"skippedActive", result.FilesSkippedActive,
		"bytesDeleted", result.BytesDeleted)
}
