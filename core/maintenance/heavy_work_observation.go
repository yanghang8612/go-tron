package maintenance

import (
	"github.com/ethereum/go-ethereum/metrics"
	"time"
)

// HeavyWorkOwner is a bounded diagnostic identity, not an admission priority.
type HeavyWorkOwner uint32

const (
	HeavyWorkUnspecified HeavyWorkOwner = iota
	HeavyWorkDirectV2
	HeavyWorkHistory
	HeavyWorkTransactionIndex
)

type HeavyWorkObservation struct {
	Owner                                             HeavyWorkOwner
	Hold                                              time.Duration
	TotalHold                                         time.Duration
	MaxHold                                           time.Duration
	Acquisitions, HistoryAcquisitions                 uint64
	RejectedHeld, RejectedCooldown, RejectedAdmission uint64
}

func (g *HeavyWorkGate) Observation() HeavyWorkObservation {
	if g == nil {
		return HeavyWorkObservation{}
	}
	o := HeavyWorkObservation{Owner: HeavyWorkOwner(g.owner.Load()), TotalHold: time.Duration(g.totalHold.Load()), MaxHold: time.Duration(g.maxHold.Load()), Acquisitions: g.acquisitions.Load(), HistoryAcquisitions: g.historyAcquisitions.Load(), RejectedHeld: g.rejectedHeld.Load(), RejectedCooldown: g.rejectedCooldown.Load(), RejectedAdmission: g.rejectedAdmission.Load()}
	if at := g.acquiredAt.Load(); at > 0 {
		o.Hold = max(0, g.currentTime().Sub(time.Unix(0, at)))
	}
	return o
}

func (g *HeavyWorkGate) HistoryContended(maxAge time.Duration) bool {
	if g == nil {
		return false
	}
	at := g.historyContendedAt.Load()
	age := g.currentTime().Sub(time.Unix(0, at))
	return at > 0 && age >= 0 && age <= maxAge
}

func (g *HeavyWorkGate) TryAcquireNamed(owner HeavyWorkOwner) (func(), bool) {
	if g == nil {
		return func() {}, true
	}
	return g.tryAcquireNamed(g.cooldown, owner)
}
func (g *HeavyWorkGate) TryAcquireNamedWithCooldown(owner HeavyWorkOwner, cooldown time.Duration) (func(), bool) {
	if g == nil {
		return func() {}, true
	}
	return g.tryAcquireNamed(max(0, cooldown), owner)
}
func (g *HeavyWorkGate) tryAcquireNamed(cooldown time.Duration, owner HeavyWorkOwner) (func(), bool) {
	release, ok := g.tryAcquireReservedPolicy(cooldown, nil, nil, owner)
	if owner == HeavyWorkHistory && !ok {
		g.historyContendedAt.Store(g.currentTime().UnixNano())
	}
	return release, ok
}

// Observation gauges describe coordination, not CPU/IO attribution. The busy
// rejection counter also includes reservations and reservation mutex contention.
func (g *HeavyWorkGate) publishObservation() {
	if !metrics.Enabled() {
		return
	}
	o := g.Observation()
	for name, value := range map[string]int64{"owner": int64(o.Owner), "acquired_at_unix_ns": g.acquiredAt.Load(), "hold_total_ns": int64(o.TotalHold), "hold_max_ns": int64(o.MaxHold), "acquisitions": int64(o.Acquisitions), "history_acquisitions": int64(o.HistoryAcquisitions), "reject_busy_or_reserved": int64(o.RejectedHeld), "reject_cooldown": int64(o.RejectedCooldown), "reject_admission": int64(o.RejectedAdmission)} {
		metrics.GetOrRegisterGauge("maintenance/heavy_work/"+name, nil).Update(value)
	}
}
