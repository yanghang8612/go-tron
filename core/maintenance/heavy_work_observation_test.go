package maintenance

import (
	"testing"
	"time"
)

func TestHeavyWorkGateNamedObservationAndRejectedReasons(t *testing.T) {
	now := time.Unix(10, 0)
	g := NewHeavyWorkGateWithCooldown(time.Second)
	g.now = func() time.Time { return now }
	r, ok := g.TryAcquireNamed(HeavyWorkDirectV2)
	if !ok {
		t.Fatal("first")
	}
	now = now.Add(2 * time.Second)
	if _, ok := g.TryAcquireNamed(HeavyWorkHistory); ok {
		t.Fatal("overlap")
	}
	o := g.Observation()
	if o.Owner != HeavyWorkDirectV2 || o.Hold != 2*time.Second || o.RejectedHeld != 1 || !g.HistoryContended(time.Second) {
		t.Fatalf("%+v", o)
	}
	r()
	r()
	if _, ok := g.TryAcquire(); ok {
		t.Fatal("cooldown bypass")
	}
	now = now.Add(time.Second)
	g.SetAdmissionCheck(func() bool { return false })
	if _, ok := g.TryAcquireNamed(HeavyWorkHistory); ok {
		t.Fatal("pressure bypass")
	}
	o = g.Observation()
	if o.RejectedCooldown != 1 || o.RejectedAdmission != 1 || o.TotalHold != 2*time.Second || o.MaxHold != 2*time.Second {
		t.Fatalf("%+v", o)
	}
	g.SetAdmissionCheck(nil)
	r, ok = g.TryAcquireNamed(HeavyWorkHistory)
	if !ok || g.HistoryContended(time.Second) || g.Observation().HistoryAcquisitions != 1 {
		t.Fatal("history acquisition not observed")
	}
	r()
}
