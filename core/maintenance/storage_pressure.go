package maintenance

import "time"

// StoragePressure is a bounded snapshot of the foreground database's write
// pressure. It describes the storage engine, not the utilization or spare
// bandwidth of its underlying (possibly shared) device. An unavailable sample
// must not be interpreted as an idle database.
type StoragePressure struct {
	Available bool
	SampledAt time.Time

	WriteStalled bool
	StallCount   uint64
	// StallDuration accumulates completed write stalls. A stall still in
	// progress is represented by WriteStalled and is not included here yet.
	StallDuration time.Duration

	L0Sublevels                 int
	L0CompactionThreshold       int
	L0StopWritesThreshold       int
	MemTableCount               int64
	MemTableStopWritesThreshold int
	CompactionDebt              uint64

	// Device observations are optional and independent of engine availability.
	// A shared device includes the I/O of other databases and processes.
	DeviceAvailable  bool
	DeviceSampledAt  time.Time
	DeviceBusyPPM    uint64
	DeviceQueueMilli uint64
	DeviceAwait      time.Duration
}

// HardLimitReached rejects new optional heavy work when a fresh engine sample
// shows foreground writes stalled or approaching their engine stop thresholds.
// Missing or stale engine measurements and device utilization alone do not
// prove a hard limit. This is an instantaneous admission check, not a limit on
// work or I/O already in flight; callers must budget those jobs separately.
func (p StoragePressure) HardLimitReached(now time.Time) bool {
	if !p.Available || p.SampledAt.IsZero() {
		return false
	}
	age := now.Sub(p.SampledAt)
	if age > 15*time.Second || age < -time.Second {
		return false
	}
	if p.WriteStalled {
		return true
	}
	if p.MemTableStopWritesThreshold > 0 && p.MemTableCount >= int64(max(1, p.MemTableStopWritesThreshold-1)) {
		return true
	}
	if p.L0StopWritesThreshold > 0 {
		// Compute floor(3*stop/4) without overflowing an otherwise valid int.
		stop := p.L0StopWritesThreshold
		limit := max(1, (stop/4)*3+(stop%4)*3/4)
		return p.L0Sublevels >= limit
	}
	return false
}
