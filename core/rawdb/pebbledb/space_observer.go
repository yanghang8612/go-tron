package pebbledb

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
)

const (
	diskSpaceObservationInterval = 30 * time.Second
	maxDiskSpaceRanges           = 16
)

// DiskSpaceRange selects a named part of the physical keyspace. Pebble treats
// End as inclusive and may count entire boundary data blocks. Even disjoint
// logical ranges are not additive physical accounting, nor reclaimable bytes.
type DiskSpaceRange struct {
	Name       string
	Start, End []byte
}

func copyDiskSpaceRanges(ranges []DiskSpaceRange) ([]DiskSpaceRange, error) {
	if len(ranges) > maxDiskSpaceRanges {
		return nil, fmt.Errorf("disk space observer: at most %d ranges", maxDiskSpaceRanges)
	}
	out := make([]DiskSpaceRange, len(ranges))
	names := make(map[string]bool, len(ranges))
	for i, r := range ranges {
		if len(r.Name) == 0 || len(r.Name) > 40 || names[r.Name] {
			return nil, fmt.Errorf("disk space observer: invalid or duplicate range name %q", r.Name)
		}
		for _, c := range r.Name {
			if c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_' {
				continue
			}
			return nil, fmt.Errorf("disk space observer: invalid range name %q", r.Name)
		}
		if len(r.Start) == 0 || len(r.End) == 0 || bytes.Compare(r.Start, r.End) >= 0 {
			return nil, fmt.Errorf("disk space observer: %s requires explicit increasing bounds", r.Name)
		}
		names[r.Name] = true
		out[i] = DiskSpaceRange{r.Name, bytes.Clone(r.Start), bytes.Clone(r.End)}
	}
	return out, nil
}

type diskSpaceRangeMetrics struct {
	rangeSpec                               DiskSpaceRange
	bytes, known, sampledAt                 *metrics.Gauge
	attemptedAt, duration, attempts, errors *metrics.Gauge
	attemptCount, errorCount                int64
}

type diskSpaceObserver struct {
	estimate                 func(start, end []byte) (uint64, error)
	ranges                   []diskSpaceRangeMetrics
	enabled, owner, inFlight *metrics.Gauge
	interval                 time.Duration
	stopCh, done             chan struct{}
	next                     int
}

var diskSpaceOwner atomic.Int64

// Rawdb uses an empty metrics namespace for its primary database. Auxiliary
// opens can share it. Only the first open DB publishes the new space metrics;
// secondary (including read-only) opens must not reset or interleave its data.
// A lease lasts through worker/meter shutdown and Pebble Close. Secondary DBs
// do not take over automatically when the owner closes.
var spaceMetricsOwners = struct {
	sync.Mutex
	byNamespace map[string]*Database
}{byNamespace: make(map[string]*Database)}

func acquireSpaceMetrics(d *Database) bool {
	spaceMetricsOwners.Lock()
	defer spaceMetricsOwners.Unlock()
	if spaceMetricsOwners.byNamespace[d.namespace] != nil {
		return false
	}
	spaceMetricsOwners.byNamespace[d.namespace] = d
	return true
}

func releaseSpaceMetrics(d *Database) {
	spaceMetricsOwners.Lock()
	defer spaceMetricsOwners.Unlock()
	if spaceMetricsOwners.byNamespace[d.namespace] == d {
		delete(spaceMetricsOwners.byNamespace, d.namespace)
	}
}

// The caller holds the namespace lease and validates/copies ranges before DB
// open. Each observer owns one worker, and Close joins it before closing the
// underlying Pebble DB. Estimates
// must use Pebble directly, not Database.EstimateDiskUsage: Close holds quitLock
// while joining, so trying to acquire that lock in this worker could deadlock.
func newDiskSpaceObserver(namespace string, ranges []DiskSpaceRange, estimate func([]byte, []byte) (uint64, error)) *diskSpaceObserver {
	prefix := namespace + "storage/keyspace/"
	gauge := func(name string) *metrics.Gauge {
		g := metrics.GetOrRegisterGauge(prefix+name, nil)
		g.Update(0)
		return g
	}
	o := &diskSpaceObserver{
		estimate: estimate, interval: diskSpaceObservationInterval,
		enabled: gauge("enabled"), owner: gauge("owner"), inFlight: gauge("inflight"),
	}
	gauge("range_count").Update(int64(len(ranges)))
	for _, r := range ranges {
		base := r.Name + "/"
		o.ranges = append(o.ranges, diskSpaceRangeMetrics{
			rangeSpec: r,
			bytes:     gauge(base + "sst_bytes"), known: gauge(base + "known"),
			sampledAt: gauge(base + "sampled_at"), attemptedAt: gauge(base + "attempted_at"),
			duration: gauge(base + "duration_ns"), attempts: gauge(base + "attempts/total"), errors: gauge(base + "errors/total"),
		})
	}
	if len(ranges) != 0 {
		o.stopCh, o.done = make(chan struct{}), make(chan struct{})
		o.owner.Update(diskSpaceOwner.Add(1))
		o.enabled.Update(1)
	}
	return o
}

func (o *diskSpaceObserver) start() {
	if o.stopCh != nil {
		go o.run()
	}
}

func (o *diskSpaceObserver) stop() {
	if o.stopCh != nil {
		close(o.stopCh)
		<-o.done
	}
	o.enabled.Update(0)
}

func (o *diskSpaceObserver) run() {
	defer close(o.done)
	for {
		select {
		case <-o.stopCh:
			return
		default:
		}
		o.sampleNext()
		// Wait after completion, rather than catching up missed ticker events.
		// A single native estimate cannot be cancelled. Never detach it on a
		// timeout and start another one; shutdown joins this in-flight call.
		timer := time.NewTimer(o.interval)
		select {
		case <-o.stopCh:
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (o *diskSpaceObserver) sampleNext() {
	r := &o.ranges[o.next]
	o.next = (o.next + 1) % len(o.ranges)
	start := time.Now()
	r.attemptedAt.Update(start.Unix())
	o.inFlight.Update(1)
	value, err := o.estimate(r.rangeSpec.Start, r.rangeSpec.End)
	o.inFlight.Update(0)
	r.duration.Update(time.Since(start).Nanoseconds())
	r.attemptCount++
	r.attempts.Update(r.attemptCount)
	if err != nil {
		r.errorCount++
		r.errors.Update(r.errorCount)
		return // Keep the previous successful value, known flag and timestamp.
	}
	r.bytes.Update(int64(value))
	r.sampledAt.Update(time.Now().Unix())
	r.known.Update(1)
}
