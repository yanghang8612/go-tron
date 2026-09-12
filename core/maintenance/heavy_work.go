// Package maintenance coordinates optional background work which competes
// with canonical block import for CPU and storage bandwidth.
package maintenance

import (
	"sync"
	"sync/atomic"
	"time"
)

// HeavyWorkGate admits at most one optional heavy task at a time. Admission is
// deliberately non-blocking: a lifecycle which loses the race records a
// deferral and retries on its normal cadence instead of adding an invisible
// queue behind another long-running maintenance task.
type HeavyWorkGate struct {
	token          chan struct{}
	cooldown       time.Duration
	cooldownAfter  time.Duration
	nextAllowed    atomic.Int64
	admissionCheck atomic.Pointer[heavyWorkAdmission]
	now            func() time.Time
	reservationMu  sync.Mutex
	reservation    *HeavyWorkReservation
}

type heavyWorkAdmission struct {
	check func() bool
}

// SetAdmissionCheck installs an optional process-wide pressure check. The check
// runs after the gate is exclusively owned, must be bounded and concurrency
// safe with its data sources, and must not reenter this gate. Returning false
// rejects the acquisition without starting a cooldown. A nil check restores
// ordinary gate behavior. Replacing a check is safe during an acquisition; an
// already running check may finish with the previously installed callback.
// This is protection at admission, not a limit on I/O already in flight.
func (g *HeavyWorkGate) SetAdmissionCheck(check func() bool) {
	if g == nil {
		return
	}
	if check == nil {
		g.admissionCheck.Store(nil)
		return
	}
	g.admissionCheck.Store(&heavyWorkAdmission{check: check})
}

// NewHeavyWorkGate constructs an idle process-wide maintenance gate.
func NewHeavyWorkGate() *HeavyWorkGate {
	return NewHeavyWorkGateWithCooldown(0)
}

// NewHeavyWorkGateWithCooldown constructs a gate which leaves an importer-only
// recovery window after every admitted task. The cooldown starts when work
// releases the gate, preventing individually bounded snapshot/freezer jobs from
// becoming one long back-to-back maintenance burst.
func NewHeavyWorkGateWithCooldown(cooldown time.Duration) *HeavyWorkGate {
	return NewHeavyWorkGateWithCooldownAfter(cooldown, 0)
}

// NewHeavyWorkGateWithCooldownAfter applies the recovery window only when an
// admitted lease lasts at least cooldownAfter. Production uses this to keep
// cheap readiness/no-op checks from continuously renewing the cooldown while
// still spacing out jobs which materially consume CPU or storage bandwidth.
func NewHeavyWorkGateWithCooldownAfter(cooldown, cooldownAfter time.Duration) *HeavyWorkGate {
	if cooldown < 0 {
		cooldown = 0
	}
	if cooldownAfter < 0 {
		cooldownAfter = 0
	}
	return &HeavyWorkGate{
		token:         make(chan struct{}, 1),
		cooldown:      cooldown,
		cooldownAfter: cooldownAfter,
		now:           time.Now,
	}
}

// TryAcquire returns an idempotent release callback when the gate is idle.
// A nil gate is treated as unlimited so package-level tests and deployments
// which do not wire the coordinator preserve their previous behavior.
func (g *HeavyWorkGate) TryAcquire() (release func(), ok bool) {
	if g == nil {
		return func() {}, true
	}
	return g.tryAcquire(g.cooldown)
}

// CanTryAcquire is a non-binding, non-blocking scheduling hint for ordinary
// contenders. It does not call admission checks, consume a reservation, own a
// lease or start recovery. Callers must still TryAcquire before doing work:
// another owner, reservation or pressure change may intervene after this read.
func (g *HeavyWorkGate) CanTryAcquire() bool {
	if g == nil {
		return true
	}
	if !g.reservationMu.TryLock() {
		return false
	}
	defer g.reservationMu.Unlock()
	now := g.currentTime()
	if g.reservation != nil && now.Before(g.reservation.expires) {
		return false
	}
	return !g.coolingDown(now) && len(g.token) == 0
}

// TryAcquireWithCooldown is TryAcquire with a per-lease recovery window. It
// is intended for bounded catch-up work whose backlog is large enough to use a
// shorter recovery window than ordinary background maintenance. The override
// only controls the cooldown installed when this lease is released: an active
// lease and any recovery window already in force are still honored.
func (g *HeavyWorkGate) TryAcquireWithCooldown(cooldown time.Duration) (release func(), ok bool) {
	if g == nil {
		return func() {}, true
	}
	if cooldown < 0 {
		cooldown = 0
	}
	return g.tryAcquire(cooldown)
}

// ReleaseCooldownPolicy chooses recovery from this lease's measured hold time
// and the gate's configured default. It must be pure, bounded and must not
// reenter the gate. Negative results are treated as zero. If it panics, release
// propagates the panic after installing the default cooldown and returning the
// token, so a failed policy cannot permanently block maintenance.
type ReleaseCooldownPolicy func(held, defaultCooldown time.Duration) time.Duration

// TryAcquireWithReleaseCooldown changes only the recovery installed by this
// lease. Existing cooldowns, active owners and pressure admission still apply.
// The normal minimum-work threshold remains in effect after a successful
// policy evaluation. A nil policy keeps the configured default behavior.
func (g *HeavyWorkGate) TryAcquireWithReleaseCooldown(policy ReleaseCooldownPolicy) (release func(), ok bool) {
	if g == nil {
		return func() {}, true
	}
	return g.tryAcquireReservedPolicy(g.cooldown, nil, policy)
}

func (g *HeavyWorkGate) tryAcquire(recoveryCooldown time.Duration) (release func(), ok bool) {
	return g.tryAcquireReserved(recoveryCooldown, nil)
}

func (g *HeavyWorkGate) tryAcquireReserved(recoveryCooldown time.Duration, owner *HeavyWorkReservation) (release func(), ok bool) {
	return g.tryAcquireReservedPolicy(recoveryCooldown, owner, nil)
}

func (g *HeavyWorkGate) tryAcquireReservedPolicy(recoveryCooldown time.Duration, owner *HeavyWorkReservation, policy ReleaseCooldownPolicy) (release func(), ok bool) {
	if !g.reservationMu.TryLock() {
		return nil, false
	}
	locked := true
	defer func() {
		if locked {
			g.reservationMu.Unlock()
		}
	}()
	now := g.currentTime()
	if g.reservation != nil && !now.Before(g.reservation.expires) {
		g.reservation = nil
	}
	if owner != nil && g.reservation != owner {
		return nil, false
	}
	if g.reservation != nil && g.reservation != owner {
		return nil, false
	}
	if g.coolingDown(now) {
		return nil, false
	}
	select {
	case g.token <- struct{}{}:
		// Recheck after owning the token. A release may have installed a
		// cooldown between the optimistic timestamp check and admission.
		if g.coolingDown(g.currentTime()) {
			<-g.token
			return nil, false
		}
		g.reservation = nil
		g.reservationMu.Unlock()
		locked = false
		if admission := g.admissionCheck.Load(); admission != nil && !admission.check() {
			<-g.token
			return nil, false
		}
		acquiredAt := g.currentTime()
		var once sync.Once
		return func() {
			once.Do(func() {
				defer func() { <-g.token }()
				now := g.currentTime()
				held := max(0, now.Sub(acquiredAt))
				if policy == nil {
					if recoveryCooldown > 0 && held >= g.cooldownAfter {
						g.nextAllowed.Store(now.Add(recoveryCooldown).UnixNano())
					}
					return
				}
				// The token remains exclusively held while replacing this lease's
				// fallback with its measured recovery. This cannot shorten another
				// lease's cooldown: admission already waited for it to expire.
				if g.cooldown > 0 {
					g.nextAllowed.Store(now.Add(g.cooldown).UnixNano())
				}
				cooldown := max(0, policy(held, g.cooldown))
				if held < g.cooldownAfter {
					cooldown = 0
				}
				g.nextAllowed.Store(now.Add(cooldown).UnixNano())
			})
		}, true
	default:
		return nil, false
	}
}

func (g *HeavyWorkGate) currentTime() time.Time {
	if g != nil && g.now != nil {
		return g.now()
	}
	return time.Now()
}

func (g *HeavyWorkGate) coolingDown(now time.Time) bool {
	if g == nil {
		return false
	}
	return now.UnixNano() < g.nextAllowed.Load()
}

// CooldownRemaining reports how long callers should wait before retrying an
// admission rejected by the post-work recovery window. It returns zero when
// the gate is not cooling down; an independently active lease has no known
// completion time and should still be retried on the caller's normal cadence.
func (g *HeavyWorkGate) CooldownRemaining() time.Duration {
	if g == nil {
		return 0
	}
	remaining := time.Unix(0, g.nextAllowed.Load()).Sub(g.currentTime())
	if remaining <= 0 {
		return 0
	}
	return remaining
}
