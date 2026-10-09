package main

import "github.com/tronprotocol/go-tron/core"

// stagingMemoryHeadroom consumes the existing background observation only.
// CPU quotas/idle counts and strict parallel storage admission do not decide
// whether this serial bounded maintenance task has memory headroom.
func (p *runtimeHistoryResources) stagingMemoryHeadroom() core.HistoryStagingMemory {
	if !p.running.Load() {
		return core.HistoryStagingMemory{}
	}
	p.parallel.mu.Lock()
	defer p.parallel.mu.Unlock()
	if !p.running.Load() || p.parallel.previous == nil {
		return core.HistoryStagingMemory{}
	}
	observed := p.parallel.previous
	return core.HistoryStagingMemory{Available: observed.memoryOOMKnown && !observed.memoryUnderOOM, SampledAt: observed.at, HeadroomBytes: observed.memoryAvailable, UnderOOM: observed.memoryUnderOOM}
}
