// Copyright 2026 The Pebble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package pebble

// MaintenanceMetrics is the engine state needed to schedule external
// maintenance. It excludes cache, table, WAL and object-provider statistics.
type MaintenanceMetrics struct {
	L0Sublevels    int32
	MemTableCount  int64
	CompactionDebt uint64
}

// MaintenanceMetrics returns a current scheduling observation under the DB
// mutex, using the same sources as Metrics. It does not collect cache metrics
// or walk table properties. Like Metrics, the caller must exclude Close.
func (d *DB) MaintenanceMetrics() MaintenanceMetrics {
	d.mu.Lock()
	defer d.mu.Unlock()
	return MaintenanceMetrics{
		L0Sublevels:    d.mu.versions.metrics.Levels[0].Sublevels,
		MemTableCount:  int64(len(d.mu.mem.queue)),
		CompactionDebt: d.mu.versions.picker.estimatedCompactionDebt(0),
	}
}
