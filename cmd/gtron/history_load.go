package main

import (
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tronprotocol/go-tron/core/maintenance"
)

const (
	historyDeviceSampleInterval = 5 * time.Second
	historyDeviceMaxSampleSpan  = 30 * time.Second
	historyDiskstatsReadLimit   = 1 << 20
)

type historyStoragePressureReader interface {
	MaintenancePressure() maintenance.StoragePressure
}

type historyDeviceID struct{ major, minor uint32 }

type historyDiskCounters struct {
	reads, writes, readMillis, writeMillis, busyMillis, weightedMillis uint64
}

type historyDeviceObservation struct {
	device   historyDeviceID
	at       time.Time
	counters historyDiskCounters
}

// runtimeHistoryLoadProbe observes the device that contains the data directory.
// Its counters include competing processes; spare CPU does not imply spare I/O.
// Reads are bounded and cached for five seconds. The node resource sampler keeps
// this baseline current independently of maintenance admission calls.
type runtimeHistoryLoadProbe struct {
	mu          sync.Mutex
	engine      historyStoragePressureReader
	path        string
	now         func() time.Time
	locate      func(string) (historyDeviceID, error)
	read        func() ([]byte, error)
	lastAttempt time.Time
	previous    *historyDeviceObservation
	cached      maintenance.StoragePressure
}

func newRuntimeHistoryLoadProbe(db any, path string) *runtimeHistoryLoadProbe {
	engine, _ := db.(historyStoragePressureReader)
	p := &runtimeHistoryLoadProbe{
		engine: engine, path: path, now: time.Now,
		locate: historyDataDevice, read: readHistoryDiskstats,
	}
	return p
}

func (p *runtimeHistoryLoadProbe) reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastAttempt, p.previous, p.cached = time.Time{}, nil, maintenance.StoragePressure{}
}

func (p *runtimeHistoryLoadProbe) sample() maintenance.StoragePressure {
	p.mu.Lock()
	defer p.mu.Unlock()

	var out maintenance.StoragePressure
	if p.engine != nil {
		out = p.engine.MaintenancePressure()
	}
	now := p.now()
	age := now.Sub(p.lastAttempt)
	if p.lastAttempt.IsZero() || age < 0 || age >= historyDeviceSampleInterval {
		p.refresh(now)
	}
	// Never cache Pebble pressure along with the device sample: a write stall
	// beginning between device reads must be visible immediately.
	out.DeviceAvailable = p.cached.DeviceAvailable
	out.DeviceSampledAt = p.cached.DeviceSampledAt
	out.DeviceBusyPPM = p.cached.DeviceBusyPPM
	out.DeviceQueueMilli = p.cached.DeviceQueueMilli
	out.DeviceAwait = p.cached.DeviceAwait
	return out
}

func (p *runtimeHistoryLoadProbe) refresh(now time.Time) {
	p.lastAttempt = now
	p.cached = maintenance.StoragePressure{}
	previous := p.previous
	p.previous = nil // Failed reads invalidate the baseline, not just the result.
	device, err := p.locate(p.path)
	if err != nil {
		return
	}
	raw, err := p.read()
	if err != nil {
		return
	}
	counters, err := parseHistoryDiskstats(raw, device)
	if err != nil {
		return
	}
	p.previous = &historyDeviceObservation{device: device, at: now, counters: counters}
	if previous == nil || previous.device != device {
		return
	}
	elapsed := now.Sub(previous.at)
	if elapsed < historyDeviceSampleInterval || elapsed > historyDeviceMaxSampleSpan {
		return
	}
	p.cached = historyDeviceDelta(previous.counters, counters, elapsed, now)
}

func readHistoryDiskstats() ([]byte, error) {
	f, err := os.Open("/proc/diskstats")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, historyDiskstatsReadLimit+1))
	if err == nil && len(raw) > historyDiskstatsReadLimit {
		return nil, errors.New("history diskstats exceeds read limit")
	}
	return raw, err
}

func parseHistoryDiskstats(raw []byte, device historyDeviceID) (historyDiskCounters, error) {
	if len(raw) > historyDiskstatsReadLimit {
		return historyDiskCounters{}, errors.New("history diskstats exceeds read limit")
	}
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		major, majorErr := strconv.ParseUint(fields[0], 10, 32)
		minor, minorErr := strconv.ParseUint(fields[1], 10, 32)
		if majorErr != nil || minorErr != nil || uint32(major) != device.major || uint32(minor) != device.minor {
			continue
		}
		if len(fields) < 14 {
			return historyDiskCounters{}, errors.New("history diskstats device line is truncated")
		}
		var values [6]uint64
		for i, field := range [...]int{3, 7, 6, 10, 12, 13} {
			value, err := strconv.ParseUint(fields[field], 10, 64)
			if err != nil {
				return historyDiskCounters{}, fmt.Errorf("history diskstats counter: %w", err)
			}
			values[i] = value
		}
		return historyDiskCounters{values[0], values[1], values[2], values[3], values[4], values[5]}, nil
	}
	return historyDiskCounters{}, errors.New("history data device absent from diskstats")
}

func historyDeviceDelta(before, after historyDiskCounters, elapsed time.Duration, now time.Time) maintenance.StoragePressure {
	var out maintenance.StoragePressure
	if elapsed <= 0 || after.reads < before.reads || after.writes < before.writes ||
		after.readMillis < before.readMillis || after.writeMillis < before.writeMillis ||
		after.busyMillis < before.busyMillis || after.weightedMillis < before.weightedMillis {
		return out
	}
	reads, writes := after.reads-before.reads, after.writes-before.writes
	readMillis, writeMillis := after.readMillis-before.readMillis, after.writeMillis-before.writeMillis
	// Without completed operations, await is unknown (possibly all I/O is
	// still queued), not an observed zero. Treat the composite as unavailable.
	if reads > math.MaxUint64-writes || reads+writes == 0 || readMillis > math.MaxUint64-writeMillis {
		return out
	}
	out.DeviceAvailable = true
	out.DeviceSampledAt = now
	out.DeviceBusyPPM = min(uint64(1_000_000), historyLoadRatio(after.busyMillis-before.busyMillis, 1_000_000_000_000, uint64(elapsed)))
	out.DeviceQueueMilli = historyLoadRatio(after.weightedMillis-before.weightedMillis, 1_000_000_000, uint64(elapsed))
	out.DeviceAwait = time.Duration(min(uint64(math.MaxInt64), historyLoadRatio(readMillis+writeMillis, uint64(time.Millisecond), reads+writes)))
	return out
}

// Counter multiplication can overflow before division even when the resulting
// ratio fits. Saturate only the final result, never wrap a busy disk into idle.
func historyLoadRatio(value, scale, divisor uint64) uint64 {
	hi, lo := bits.Mul64(value, scale)
	if divisor == 0 || hi >= divisor {
		return math.MaxUint64
	}
	q, _ := bits.Div64(hi, lo, divisor)
	return q
}
