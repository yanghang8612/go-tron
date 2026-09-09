package maintenance

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHeavyWorkMeasuredRecoveryUsesCurrentLeaseAndPreservesEarlierDeadline(t *testing.T) {
	for _, held := range []time.Duration{100 * time.Millisecond, 300 * time.Millisecond, 5 * time.Second} {
		t.Run(held.String(), func(t *testing.T) {
			now := time.Unix(5000, 0)
			g := NewHeavyWorkGateWithCooldownAfter(15*time.Second, 250*time.Millisecond)
			g.now = func() time.Time { return now }
			calls := 0
			policy := func(work, configured time.Duration) time.Duration {
				calls++
				if work != held || configured != 15*time.Second {
					t.Fatalf("release inputs = %s/%s", work, configured)
				}
				return min(3*time.Second, work)
			}
			release, ok := g.TryAcquireWithReleaseCooldown(policy)
			if !ok {
				t.Fatal("initial lease rejected")
			}
			now = now.Add(held)
			release()
			release()
			want := min(3*time.Second, held)
			if held < 250*time.Millisecond {
				want = 0
			}
			if got := g.CooldownRemaining(); got != want || calls != 1 {
				t.Fatalf("recovery=%s calls=%d, want %s/1", got, calls, want)
			}
			if want > 0 {
				if _, ok := g.TryAcquireWithReleaseCooldown(func(time.Duration, time.Duration) time.Duration { return 0 }); ok {
					t.Fatal("new policy bypassed installed recovery")
				}
				if got := g.CooldownRemaining(); got != want {
					t.Fatalf("rejected new policy shortened prior recovery to %s", got)
				}
			}
			now = now.Add(want)
			if next, ok := g.TryAcquire(); !ok {
				t.Fatal("other maintenance did not receive the measured recovery window")
			} else {
				next()
			}
		})
	}
}

func TestHeavyWorkReleasePolicyPanicKeepsDefaultAndReturnsToken(t *testing.T) {
	now := time.Unix(5000, 0)
	g := NewHeavyWorkGateWithCooldownAfter(15*time.Second, time.Second)
	g.now = func() time.Time { return now }
	release, ok := g.TryAcquireWithReleaseCooldown(func(time.Duration, time.Duration) time.Duration { panic("injected release-policy panic") })
	if !ok {
		t.Fatal("initial lease rejected")
	}
	func() {
		defer func() {
			if got := recover(); got != "injected release-policy panic" {
				t.Fatalf("panic changed: %v", got)
			}
		}()
		release()
	}()
	if got := g.CooldownRemaining(); got != 15*time.Second {
		t.Fatalf("failed policy lost conservative fallback: %s", got)
	}
	release() // Idempotent even after a propagated panic.
	now = now.Add(15 * time.Second)
	if next, ok := g.TryAcquire(); !ok {
		t.Fatal("panicking policy leaked its token")
	} else {
		next()
	}
}

func TestHeavyWorkReservedReleasePolicyHonorsPressureAndOwnership(t *testing.T) {
	now := time.Unix(5000, 0)
	g := NewHeavyWorkGateWithCooldown(15 * time.Second)
	g.now = func() time.Time { return now }
	policy := func(held, _ time.Duration) time.Duration { return min(3*time.Second, held) }
	background, _ := g.TryAcquireWithCooldown(time.Second)
	r := g.ReserveNext(10 * time.Second)
	if _, ok := r.TryAcquireWithReleaseCooldown(policy); ok {
		t.Fatal("reserved measured job interrupted its predecessor")
	}
	background()
	if _, ok := r.TryAcquireWithReleaseCooldown(policy); ok {
		t.Fatal("reservation bypassed predecessor recovery")
	}
	now = now.Add(time.Second)
	if _, ok := g.TryAcquireWithReleaseCooldown(policy); ok {
		t.Fatal("unreserved job stole the pending turn")
	}
	g.SetAdmissionCheck(func() bool { return false })
	if _, ok := r.TryAcquireWithReleaseCooldown(policy); ok || r.Active() {
		t.Fatal("pressure did not reject and consume the reservation")
	}
	g.SetAdmissionCheck(nil)
	r = g.ReserveNext(10 * time.Second)
	release, ok := r.TryAcquireWithReleaseCooldown(policy)
	if !ok {
		t.Fatal("ready reserved job was starved")
	}
	now = now.Add(300 * time.Millisecond)
	release()
	if got := g.CooldownRemaining(); got != 300*time.Millisecond {
		t.Fatalf("reserved leaf retained fixed recovery: %s", got)
	}
	now = now.Add(300 * time.Millisecond)
	if next, ok := g.TryAcquire(); !ok {
		t.Fatal("reserved job did not yield to ordinary maintenance")
	} else {
		next()
	}
}

func TestHeavyWorkReleasePolicyConcurrentCloseStillOwnsToken(t *testing.T) {
	g := NewHeavyWorkGate()
	entered, finish := make(chan struct{}), make(chan struct{})
	var calls atomic.Uint64
	release, _ := g.TryAcquireWithReleaseCooldown(func(time.Duration, time.Duration) time.Duration {
		calls.Add(1)
		close(entered)
		<-finish // Controlled test barrier; production policies must not block.
		return 0
	})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); release() }()
	}
	<-entered
	_, overlapping := g.TryAcquire()
	close(finish)
	wg.Wait()
	if overlapping || calls.Load() != 1 {
		t.Fatalf("overlap=%t policy calls=%d", overlapping, calls.Load())
	}
	if next, ok := g.TryAcquire(); !ok {
		t.Fatal("concurrent closes leaked the token")
	} else {
		next()
	}
}
