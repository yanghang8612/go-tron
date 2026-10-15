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

func TestHistoryContentionNotificationIsIntentOnly(t *testing.T) {
	var nilGate *HeavyWorkGate
	nilGate.NotifyHistoryContention()
	if nilGate.HistoryContended(time.Second) {
		t.Fatal("nil gate reported demand")
	}
	now := time.Unix(100, 0)
	g := NewHeavyWorkGateWithCooldown(time.Minute)
	g.now = func() time.Time { return now }
	calls := 0
	g.SetAdmissionCheck(func() bool { calls++; return true })
	release, _ := g.TryAcquireNamed(HeavyWorkDirectV2)
	defer release()
	reservation := g.ReserveNext(10 * time.Second)
	defer reservation.Cancel()
	before := g.Observation()
	deadline := g.nextAllowed.Load()
	owner := g.reservation
	g.NotifyHistoryContention()
	g.NotifyHistoryContention()
	if !g.HistoryContended(time.Second) || g.Observation() != before || g.nextAllowed.Load() != deadline || g.reservation != owner || calls != 1 {
		t.Fatal("notification acquired/rejected work or changed admission/owner/reservation/recovery")
	}
	now = now.Add(11 * time.Second)
	if g.HistoryContended(10 * time.Second) {
		t.Fatal("stale notification remained fresh")
	}
	now = now.Add(-12 * time.Second)
	if g.HistoryContended(10 * time.Second) {
		t.Fatal("future notification remained fresh")
	}
	now = time.Unix(100, 0)
	release()
	deadline = g.nextAllowed.Load()
	before = g.Observation()
	g.NotifyHistoryContention()
	if g.nextAllowed.Load() != deadline || g.Observation() != before || calls != 1 {
		t.Fatal("notification changed cooldown")
	}
}
