package snapshots

import (
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/maintenance"
)

func TestHistoryAndEventRetryActiveHeavyLease(t *testing.T) {
	for _, event := range []bool{false, true} {
		name := "history"
		if event {
			name = "event"
		}
		t.Run(name, func(t *testing.T) {
			var r *Runner
			if event {
				r, _, _, _ = historyEventBudgetFixture(t)
			} else {
				r = throughputFixture(t)
			}
			gate := maintenance.NewHeavyWorkGate()
			r.cfg.HeavyWorkGate = gate
			release, admitted := gate.TryAcquire()
			if !admitted {
				t.Fatal("acquire test lease")
			}
			defer release()
			deadline := r.historyNotBefore.Load()
			started := time.Now()
			result, err := r.OnePass()
			if err != nil || !result.HistoryGateDeferred || !result.HistoryDeferred || result.Built || result.EventLogBuilt || result.HistoryBuildAttempted || result.HistoryEventAttempted {
				t.Fatalf("active lease did not defer: %+v %v", result, err)
			}
			if result.HistoryRetryDeadline.Before(started.Add(3*time.Second)) || result.MaintenanceRetryRemaining(time.Now()) <= 0 {
				t.Fatalf("active lease lost bounded retry: %+v", result)
			}
			if r.historyNotBefore.Load() != deadline {
				t.Fatal("wake hint changed admission deadline")
			}
			release()
			resumed, err := r.OnePass()
			if err != nil || event && !resumed.EventLogBuilt || !event && !resumed.Built {
				t.Fatalf("released lease cannot resume: %+v %v", resumed, err)
			}
		})
	}
}

func TestHeavyWorkRetryPreservesKnownCooldown(t *testing.T) {
	r := throughputFixture(t)
	r.cfg.HeavyWorkGate = maintenance.NewHeavyWorkGateWithCooldown(15 * time.Second)
	release, _ := r.cfg.HeavyWorkGate.TryAcquire()
	release()
	started := time.Now()
	result, err := r.OnePass()
	if err != nil || !result.HistoryGateDeferred || result.HistoryRetryDeadline.Before(started.Add(14*time.Second)) {
		t.Fatalf("known cooldown shortened to polling fallback: %+v %v", result, err)
	}
}

func TestHistoryEventUnknownFreezerTargetDefersWithoutPublication(t *testing.T) {
	r, _, address, topic := historyEventBudgetFixture(t)
	busy := true
	r.cfg.SyncEventLogTargetBlock = func() (uint64, bool, bool) { return 4, !busy, busy }
	deadline := time.Now().Add(time.Minute).UnixNano()
	r.historyNotBefore.Store(deadline)
	before, err := LoadProductionManifest(r.cfg.Dir)
	if err != nil {
		t.Fatal(err)
	}
	result, err := r.OnePass()
	if err != nil || !result.HistoryGateDeferred || !result.DerivedSidecarsDeferred || result.HistoryBuildAttempted || result.HistoryEventAttempted || result.EventLogBuilt || result.Built {
		t.Fatalf("unknown freezer layout was treated as an uncapped target: %+v %v", result, err)
	}
	if r.historyNotBefore.Load() != deadline || result.HistoryRetryDeadline.UnixNano() != deadline || result.MaintenanceRetryRemaining(time.Now()) <= 0 {
		t.Fatalf("unknown target lost existing admission deadline/retry: %+v", result)
	}
	after, err := LoadProductionManifest(r.cfg.Dir)
	if err != nil || len(after.Segments) != len(before.Segments) || len(eventLogRefs(after)) != 0 {
		t.Fatalf("unknown target published output: %+v %v", after, err)
	}
	busy = false
	r.historyNotBefore.Store(time.Now().Add(-time.Second).UnixNano())
	resumed, err := r.OnePass()
	if err != nil || !resumed.EventLogBuilt || resumed.Built || resumed.HistoryGateDeferred {
		t.Fatalf("known target did not resume bounded event publication: %+v %v", resumed, err)
	}
	assertHistoryEventBudgetQuery(t, r, address, topic)
}

func TestPublishedEventKeepsUnknownNextTargetPending(t *testing.T) {
	r, _, address, topic := historyEventBudgetFixture(t)
	calls := 0
	r.cfg.SyncEventLogTargetBlock = func() (uint64, bool, bool) {
		calls++
		if calls == 1 {
			return 1, true, false
		}
		return 0, false, true
	}
	result, err := r.OnePass()
	if err != nil || !result.EventLogBuilt || !result.DerivedSidecarsPending || result.EventLogFreezerHandoff {
		t.Fatalf("unknown next target falsely completed event handoff: %+v %v", result, err)
	}
	assertHistoryEventBudgetQuery(t, r, address, topic)
}
