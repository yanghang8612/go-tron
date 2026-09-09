package maintenance

import "time"

// HeavyWorkReservation gives an already eligible bounded job one next-lease
// opportunity. It never interrupts an active lease or bypasses recovery or
// pressure checks. The maximum lifetime limits interference if its owner stops
// retrying; owners must cancel on loss of eligibility and after each quantum.
type HeavyWorkReservation struct {
	gate    *HeavyWorkGate
	expires time.Time
}

func (g *HeavyWorkGate) ReserveNext(ttl time.Duration) *HeavyWorkReservation {
	if g == nil || ttl <= 0 {
		return nil
	}
	g.reservationMu.Lock()
	defer g.reservationMu.Unlock()
	now := g.currentTime()
	if g.reservation != nil && now.Before(g.reservation.expires) {
		return nil
	}
	r := &HeavyWorkReservation{gate: g, expires: now.Add(min(ttl, 10*time.Second))}
	g.reservation = r
	return r
}

func (r *HeavyWorkReservation) Cancel() {
	if r == nil || r.gate == nil {
		return
	}
	r.gate.reservationMu.Lock()
	if r.gate.reservation == r {
		r.gate.reservation = nil
	}
	r.gate.reservationMu.Unlock()
}

func (r *HeavyWorkReservation) Active() bool {
	if r == nil || r.gate == nil {
		return false
	}
	r.gate.reservationMu.Lock()
	defer r.gate.reservationMu.Unlock()
	return r.gate.reservation == r && r.gate.currentTime().Before(r.expires)
}

func (r *HeavyWorkReservation) TryAcquire(cooldown time.Duration) (func(), bool) {
	if r == nil || r.gate == nil {
		return nil, false
	}
	return r.gate.tryAcquireReserved(max(0, cooldown), r)
}

// TryAcquireWithReleaseCooldown consumes this reservation under the same
// measured-recovery contract as HeavyWorkGate.TryAcquireWithReleaseCooldown.
func (r *HeavyWorkReservation) TryAcquireWithReleaseCooldown(policy ReleaseCooldownPolicy) (func(), bool) {
	if r == nil || r.gate == nil {
		return nil, false
	}
	return r.gate.tryAcquireReservedPolicy(r.gate.cooldown, r, policy)
}
