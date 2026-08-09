package maintenance

import (
	"testing"
	"time"
)

func TestHeavyWorkGateIsNonBlockingAndReleaseIsIdempotent(t *testing.T) {
	gate := NewHeavyWorkGate()
	release, ok := gate.TryAcquire()
	if !ok {
		t.Fatal("first acquisition failed")
	}
	if _, ok := gate.TryAcquire(); ok {
		t.Fatal("concurrent acquisition succeeded")
	}
	release()
	release()
	if releaseAgain, ok := gate.TryAcquire(); !ok {
		t.Fatal("acquisition after release failed")
	} else {
		releaseAgain()
	}
}

func TestNilHeavyWorkGateIsUnlimited(t *testing.T) {
	var gate *HeavyWorkGate
	release, ok := gate.TryAcquire()
	if !ok {
		t.Fatal("nil gate rejected work")
	}
	release()
}

func TestHeavyWorkGateCooldownStartsAtRelease(t *testing.T) {
	now := time.Unix(1_000, 0)
	gate := NewHeavyWorkGateWithCooldown(15 * time.Second)
	gate.now = func() time.Time { return now }

	release, ok := gate.TryAcquire()
	if !ok {
		t.Fatal("initial acquisition failed")
	}
	now = now.Add(time.Minute)
	if _, ok := gate.TryAcquire(); ok {
		t.Fatal("busy gate admitted concurrent work")
	}
	release()
	if _, ok := gate.TryAcquire(); ok {
		t.Fatal("gate admitted work at cooldown start")
	}
	now = now.Add(15*time.Second - time.Nanosecond)
	if _, ok := gate.TryAcquire(); ok {
		t.Fatal("gate admitted work before cooldown elapsed")
	}
	now = now.Add(time.Nanosecond)
	release, ok = gate.TryAcquire()
	if !ok {
		t.Fatal("gate rejected work after cooldown elapsed")
	}
	release()
}

func TestHeavyWorkGateShortLeaseDoesNotStartCooldown(t *testing.T) {
	now := time.Unix(2_000, 0)
	gate := NewHeavyWorkGateWithCooldownAfter(15*time.Second, time.Second)
	gate.now = func() time.Time { return now }
	release, ok := gate.TryAcquire()
	if !ok {
		t.Fatal("initial acquisition failed")
	}
	now = now.Add(time.Second - time.Nanosecond)
	release()
	release, ok = gate.TryAcquire()
	if !ok {
		t.Fatal("short no-op lease unexpectedly started cooldown")
	}
	now = now.Add(time.Second)
	release()
	if _, ok := gate.TryAcquire(); ok {
		t.Fatal("long lease did not start cooldown")
	}
}
