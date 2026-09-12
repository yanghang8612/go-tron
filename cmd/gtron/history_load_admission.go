package main

import "github.com/tronprotocol/go-tron/core/maintenance"

// enginePressure preserves instantaneous hard-pressure admission without
// acquiring the device-sampler mutex or refreshing proc/filesystem telemetry.
// Pebble's metadata/lifecycle locks may still wait; this is not a hard timeout.
func (p *runtimeHistoryResources) enginePressure() maintenance.StoragePressure {
	if p == nil || p.load == nil || p.load.engine == nil {
		return maintenance.StoragePressure{}
	}
	return p.load.engine.MaintenancePressure()
}

// postingPressure is safe to call after acquiring the canonical chain lock:
// it never starts device I/O and declines a busy sampler. The caller applies
// the ordinary freshness/pressure thresholds to the cached device timestamp.
func (p *runtimeHistoryResources) postingPressure() maintenance.StoragePressure {
	if p == nil || p.load == nil || !p.running.Load() {
		return maintenance.StoragePressure{}
	}
	out := p.load.sampleCachedDevice()
	if !p.running.Load() {
		return maintenance.StoragePressure{}
	}
	return out
}

func (p *runtimeHistoryLoadProbe) sampleCachedDevice() maintenance.StoragePressure {
	if !p.mu.TryLock() {
		return maintenance.StoragePressure{}
	}
	device := p.cached
	p.mu.Unlock()
	var out maintenance.StoragePressure
	if p.engine != nil {
		out = p.engine.MaintenancePressure()
	}
	out.DeviceAvailable = device.DeviceAvailable
	out.DeviceSampledAt = device.DeviceSampledAt
	out.DeviceBusyPPM = device.DeviceBusyPPM
	out.DeviceQueueMilli = device.DeviceQueueMilli
	out.DeviceAwait = device.DeviceAwait
	return out
}
