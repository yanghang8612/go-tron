package main

import (
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

// historyStagingCLIProgress writes human-readable, non-authoritative status to
// stderr. The stdout JSONL record remains the only durable phase result.
type historyStagingCLIProgress struct {
	phase     string
	writer    io.Writer
	started   time.Time
	stage     atomic.Value // string
	bucket    atomic.Uint64
	completed atomic.Uint64
	total     atomic.Uint64
	stop      chan struct{}
	stopped   chan struct{}
}

func startHistoryStagingCLIProgress(writer io.Writer, phase, stage string, interval time.Duration) *historyStagingCLIProgress {
	if writer == nil {
		writer = io.Discard
	}
	p := &historyStagingCLIProgress{phase: phase, writer: writer, started: time.Now(),
		stop: make(chan struct{}), stopped: make(chan struct{})}
	p.stage.Store(stage)
	p.report()
	go func() {
		defer close(p.stopped)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-p.stop:
				return
			case <-ticker.C:
				p.report()
			}
		}
	}()
	return p
}

func (p *historyStagingCLIProgress) report() {
	_, _ = fmt.Fprintf(p.writer,
		"history-staging progress phase=%s stage=%s bucket=%d completed=%d total=%d elapsed=%s\n",
		p.phase, p.stage.Load().(string), p.bucket.Load(), p.completed.Load(),
		p.total.Load(), time.Since(p.started).Round(time.Second))
}

func (p *historyStagingCLIProgress) close() {
	close(p.stop)
	<-p.stopped
}
