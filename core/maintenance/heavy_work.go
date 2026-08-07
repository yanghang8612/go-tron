// Package maintenance coordinates optional background work which competes
// with canonical block import for CPU and storage bandwidth.
package maintenance

import "sync"

// HeavyWorkGate admits at most one optional heavy task at a time. Admission is
// deliberately non-blocking: a lifecycle which loses the race records a
// deferral and retries on its normal cadence instead of adding an invisible
// queue behind another long-running maintenance task.
type HeavyWorkGate struct {
	token chan struct{}
}

// NewHeavyWorkGate constructs an idle process-wide maintenance gate.
func NewHeavyWorkGate() *HeavyWorkGate {
	return &HeavyWorkGate{token: make(chan struct{}, 1)}
}

// TryAcquire returns an idempotent release callback when the gate is idle.
// A nil gate is treated as unlimited so package-level tests and deployments
// which do not wire the coordinator preserve their previous behavior.
func (g *HeavyWorkGate) TryAcquire() (release func(), ok bool) {
	if g == nil {
		return func() {}, true
	}
	select {
	case g.token <- struct{}{}:
		var once sync.Once
		return func() {
			once.Do(func() { <-g.token })
		}, true
	default:
		return nil, false
	}
}
