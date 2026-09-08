package maintenance

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHeavyWorkGateAdmissionRejectDoesNotCoolDown(t *testing.T) {
	now := time.Unix(5000, 0)
	gate := NewHeavyWorkGateWithCooldown(time.Minute)
	gate.now = func() time.Time { return now }
	gate.SetAdmissionCheck(func() bool {
		now = now.Add(time.Minute)
		return false
	})
	for _, acquire := range []func() (func(), bool){gate.TryAcquire, func() (func(), bool) { return gate.TryAcquireWithCooldown(time.Hour) }} {
		if release, ok := acquire(); ok || release != nil {
			t.Fatal("pressure check admitted work")
		}
		if got := gate.CooldownRemaining(); got != 0 {
			t.Fatalf("rejected check started cooldown: %s", got)
		}
	}
	gate.SetAdmissionCheck(func() bool { return true })
	release, ok := gate.TryAcquire()
	if !ok {
		t.Fatal("pressure recovery did not release admission")
	}
	release()
	if got := gate.CooldownRemaining(); got != time.Minute {
		t.Fatalf("successful lease lost normal cooldown: %s", got)
	}
	now = now.Add(time.Minute)
	gate.SetAdmissionCheck(nil)
	release, ok = gate.TryAcquire()
	if !ok {
		t.Fatal("nil check did not restore normal admission")
	}
	release()
	var nilGate *HeavyWorkGate
	nilGate.SetAdmissionCheck(func() bool { return false })
}

func TestHeavyWorkGateAdmissionOwnsLeaseDuringCheck(t *testing.T) {
	gate := NewHeavyWorkGate()
	entered, finish, done := make(chan struct{}), make(chan struct{}), make(chan bool, 1)
	gate.SetAdmissionCheck(func() bool {
		close(entered)
		<-finish
		return false
	})
	go func() {
		_, ok := gate.TryAcquire()
		done <- ok
	}()
	<-entered
	// Replacing the check cannot let a second job pass an in-flight check.
	gate.SetAdmissionCheck(nil)
	if _, ok := gate.TryAcquire(); ok {
		t.Fatal("second job bypassed in-flight pressure check")
	}
	close(finish)
	if <-done {
		t.Fatal("already running rejected check changed its result")
	}
	if release, ok := gate.TryAcquire(); !ok {
		t.Fatal("rejected check leaked the token")
	} else {
		release()
	}
}

func TestHeavyWorkGateAdmissionConcurrentReplacement(t *testing.T) {
	gate := NewHeavyWorkGate()
	var checks, leases atomic.Int32
	var overlap atomic.Bool
	check := func() bool {
		if checks.Add(1) != 1 {
			overlap.Store(true)
		}
		runtime.Gosched()
		checks.Add(-1)
		return true
	}
	var group sync.WaitGroup
	group.Add(5)
	go func() {
		defer group.Done()
		for i := 0; i < 1000; i++ {
			gate.SetAdmissionCheck(check)
			gate.SetAdmissionCheck(func() bool { return false })
			gate.SetAdmissionCheck(nil)
		}
	}()
	for i := 0; i < 4; i++ {
		go func() {
			defer group.Done()
			for j := 0; j < 1000; j++ {
				if release, ok := gate.TryAcquire(); ok {
					if leases.Add(1) != 1 {
						overlap.Store(true)
					}
					runtime.Gosched()
					leases.Add(-1)
					release()
				}
			}
		}()
	}
	group.Wait()
	if overlap.Load() {
		t.Fatal("concurrent replacement admitted overlapping work/checks")
	}
	gate.SetAdmissionCheck(nil)
	if release, ok := gate.TryAcquire(); !ok {
		t.Fatal("concurrent admission leaked the token")
	} else {
		release()
	}
}
