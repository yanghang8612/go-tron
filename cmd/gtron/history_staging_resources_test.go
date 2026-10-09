package main

import (
	"testing"
	"time"
)

func TestStagingMemoryUsesCachedMemoryIndependentOfCPUAdmission(t *testing.T) {
	now := time.Now()
	p := &runtimeHistoryResources{parallel: &runtimeHistoryParallelProbe{
		known: false, idlePPM: 0,
		previous: &historyParallelObservation{at: now, cpuLimitMilli: 1000, memoryAvailable: 3 << 30, memoryUncredited: 3 << 30, memoryOOMKnown: true},
		read: func() (historyParallelObservation, error) {
			t.Fatal("memory helper performed a second proc read")
			return historyParallelObservation{}, nil
		},
	}}
	p.running.Store(true)
	got := p.stagingMemoryHeadroom()
	if !got.Available || got.SampledAt != now || got.HeadroomBytes != 3<<30 {
		t.Fatalf("memory-only observation=%+v", got)
	}
	// The existing sampler grants credit only after its own fresh-pair/OOM
	// audit. Consume that result even when CPU quota blocks parallel reads.
	p.parallel.known = true
	p.parallel.previous.memoryCredit = 5 << 30
	p.parallel.previous.memoryAvailable = 8 << 30
	if got := p.stagingMemoryHeadroom(); got.HeadroomBytes != 8<<30 {
		t.Fatalf("audited clean credit lost: %+v", got)
	}
	p.parallel.known = false
	p.parallel.previous.memoryCredit = 0
	p.parallel.previous.memoryAvailable = p.parallel.previous.memoryUncredited
	if got := p.stagingMemoryHeadroom(); got.HeadroomBytes != 3<<30 {
		t.Fatalf("unaudited credit invented: %+v", got)
	}
	p.parallel.previous.memoryOOMKnown = false
	if p.stagingMemoryHeadroom().Available {
		t.Fatal("unknown OOM state granted memory capacity")
	}
	p.parallel.previous.memoryOOMKnown = true
	p.parallel.previous.memoryUnderOOM = true
	if got := p.stagingMemoryHeadroom(); got.Available || !got.UnderOOM {
		t.Fatalf("under OOM observation=%+v", got)
	}
	p.running.Store(false)
	if got := p.stagingMemoryHeadroom(); got.Available || !got.SampledAt.IsZero() {
		t.Fatalf("stopped sampler exposed capacity=%+v", got)
	}
}
