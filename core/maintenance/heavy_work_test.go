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

func TestHeavyWorkGateReportsCooldownRemaining(t *testing.T) {
	now := time.Unix(500, 0)
	gate := NewHeavyWorkGateWithCooldown(15 * time.Second)
	gate.now = func() time.Time { return now }

	release, ok := gate.TryAcquire()
	if !ok {
		t.Fatal("initial acquire rejected")
	}
	release()
	if got := gate.CooldownRemaining(); got != 15*time.Second {
		t.Fatalf("initial cooldown remaining = %s, want 15s", got)
	}
	now = now.Add(9 * time.Second)
	if got := gate.CooldownRemaining(); got != 6*time.Second {
		t.Fatalf("advanced cooldown remaining = %s, want 6s", got)
	}
	now = now.Add(6 * time.Second)
	if got := gate.CooldownRemaining(); got != 0 {
		t.Fatalf("expired cooldown remaining = %s, want 0", got)
	}
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

func TestHeavyWorkGatePerLeaseCooldownOverride(t *testing.T) {
	now := time.Unix(3_000, 0)
	gate := NewHeavyWorkGateWithCooldown(15 * time.Second)
	gate.now = func() time.Time { return now }

	release, ok := gate.TryAcquireWithCooldown(3 * time.Second)
	if !ok {
		t.Fatal("override acquisition failed")
	}
	release()
	if got := gate.CooldownRemaining(); got != 3*time.Second {
		t.Fatalf("override cooldown remaining = %s, want 3s", got)
	}
	if _, ok := gate.TryAcquire(); ok {
		t.Fatal("default acquisition bypassed existing override cooldown")
	}
	now = now.Add(3 * time.Second)
	release, ok = gate.TryAcquire()
	if !ok {
		t.Fatal("default acquisition failed after override cooldown")
	}
	release()
	if got := gate.CooldownRemaining(); got != 15*time.Second {
		t.Fatalf("default cooldown remaining = %s, want 15s", got)
	}
}

func TestHeavyWorkGateOverrideWorksWithNoDefaultCooldown(t *testing.T) {
	now := time.Unix(4_000, 0)
	gate := NewHeavyWorkGate()
	gate.now = func() time.Time { return now }

	release, ok := gate.TryAcquireWithCooldown(time.Second)
	if !ok {
		t.Fatal("override acquisition failed")
	}
	release()
	if got := gate.CooldownRemaining(); got != time.Second {
		t.Fatalf("override cooldown remaining = %s, want 1s", got)
	}
	if _, ok := gate.TryAcquire(); ok {
		t.Fatal("gate ignored override cooldown without a default")
	}
}
