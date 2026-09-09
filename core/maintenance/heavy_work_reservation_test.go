package maintenance

import (
	"testing"
	"time"
)

func TestHeavyWorkReservationPreservesActiveLeaseCooldownAndYield(t *testing.T) {
	now := time.Unix(5000, 0)
	g := NewHeavyWorkGateWithCooldown(time.Second)
	g.now = func() time.Time { return now }
	for round := 0; round < 5; round++ {
		background, ok := g.TryAcquire()
		if !ok {
			t.Fatal("background work was starved between index quanta")
		}
		r := g.ReserveNext(10 * time.Second)
		if r == nil || !r.Active() {
			t.Fatal("ready job could not reserve its next turn")
		}
		if _, ok := r.TryAcquire(3 * time.Second); ok {
			t.Fatal("reservation interrupted background lease")
		}
		background()
		if _, ok := r.TryAcquire(3 * time.Second); ok {
			t.Fatal("reservation bypassed background recovery")
		}
		now = now.Add(time.Second)
		if _, ok := g.TryAcquire(); ok {
			t.Fatal("ordinary contender stole a reserved eligible turn")
		}
		index, ok := r.TryAcquire(3 * time.Second)
		if !ok || r.Active() {
			t.Fatal("ready index failed to consume exactly one reservation")
		}
		index()
		if _, ok := g.TryAcquire(); ok {
			t.Fatal("index did not leave its recovery window")
		}
		now = now.Add(3 * time.Second)
	}
}

func TestHeavyWorkReservationCancelExpiryAndPressure(t *testing.T) {
	now := time.Unix(5000, 0)
	g := NewHeavyWorkGate()
	g.now = func() time.Time { return now }
	for _, mode := range []string{"cancel", "expiry", "pressure"} {
		t.Run(mode, func(t *testing.T) {
			r := g.ReserveNext(time.Hour)
			if g.ReserveNext(time.Second) != nil {
				t.Fatal("second reservation replaced live owner")
			}
			switch mode {
			case "cancel":
				r.Cancel()
			case "expiry":
				now = now.Add(10 * time.Second)
			case "pressure":
				g.SetAdmissionCheck(func() bool { return false })
				if _, ok := r.TryAcquire(time.Minute); ok {
					t.Fatal("reservation bypassed hard pressure")
				}
				g.SetAdmissionCheck(nil)
			}
			if r.Active() {
				t.Fatal("invalid reservation remained active")
			}
			if _, ok := r.TryAcquire(time.Second); ok {
				t.Fatal("invalid owner reacquired a lease")
			}
			if release, ok := g.TryAcquire(); !ok {
				t.Fatal("invalid reservation blocked ordinary work")
			} else {
				release()
			}
		})
	}
}
