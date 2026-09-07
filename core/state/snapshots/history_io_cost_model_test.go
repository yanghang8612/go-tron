package snapshots

import (
	"errors"
	"math"
	"math/bits"
	"testing"
	"time"
)

// Test-only discrete-event model. This is not a filesystem interceptor or a
// claim about device IOPS, caches, write amplification or mainnet throughput.
// Each bounded producer completes one artifact's observed factory work, then
// submits its byte cost to one shared disk server. Full queue => producer
// remains occupied. CPU and I/O can overlap across artifacts, not within one.
type historyIOCost struct {
	SourceRead, FinalWrite, SpoolRead, SpoolWrite         uint64
	WALWrite, FlushWrite, CompactionRead, CompactionWrite uint64
}

func (c historyIOCost) bytes() (uint64, error) {
	var n uint64
	for _, v := range []uint64{c.SourceRead, c.FinalWrite, c.SpoolRead, c.SpoolWrite, c.WALWrite, c.FlushWrite, c.CompactionRead, c.CompactionWrite} {
		if v > math.MaxUint64-n {
			return 0, errors.New("history I/O byte cost overflow")
		}
		n += v
	}
	return n, nil
}

type historyIOJob struct {
	Work time.Duration
	Cost historyIOCost
}

type historyIOModelResult struct {
	Wall, DiskBusy, ProducerBlocked time.Duration
	TotalDiskBytes                  uint64
	MaxQueued, MaxOccupiedProducers int
}

func historyIOService(bytes, bytesPerSecond uint64) (time.Duration, error) {
	if bytesPerSecond == 0 {
		return 0, errors.New("history I/O bandwidth must be positive")
	}
	hi, lo := bits.Mul64(bytes, uint64(time.Second))
	if hi >= bytesPerSecond {
		return 0, errors.New("history I/O service duration overflow")
	}
	ns, rem := bits.Div64(hi, lo, bytesPerSecond)
	if ns > math.MaxInt64 || ns == math.MaxInt64 && rem != 0 {
		return 0, errors.New("history I/O service duration overflow")
	}
	if rem != 0 {
		ns++
	}
	return time.Duration(ns), nil
}

func historyIOTimeAdd(a, b time.Duration) (time.Duration, error) {
	if a < 0 || b < 0 || a > time.Duration(math.MaxInt64)-b {
		return 0, errors.New("history I/O timeline overflow")
	}
	return a + b, nil
}

func modelHistoryIO(jobs []historyIOJob, workers, queueCapacity int, bytesPerSecond uint64) (historyIOModelResult, error) {
	var out historyIOModelResult
	if workers < 1 || workers > 32 || queueCapacity < 1 || queueCapacity > 32 || bytesPerSecond == 0 {
		return out, errors.New("invalid bounded history I/O model configuration")
	}
	services := make([]time.Duration, len(jobs))
	for i, job := range jobs {
		if job.Work <= 0 {
			return out, errors.New("history I/O work must be positive")
		}
		n, err := job.Cost.bytes()
		if err != nil || n == 0 {
			return out, errors.New("history I/O cost must be positive and bounded")
		}
		if n > math.MaxUint64-out.TotalDiskBytes {
			return out, errors.New("history I/O total bytes overflow")
		}
		out.TotalDiskBytes += n
		services[i], err = historyIOService(n, bytesPerSecond)
		if err != nil {
			return out, err
		}
		out.DiskBusy, err = historyIOTimeAdd(out.DiskBusy, services[i])
		if err != nil {
			return out, err
		}
	}
	if len(jobs) == 0 {
		return out, nil
	}
	type producer struct {
		job               int
		ready             time.Duration
		occupied, blocked bool
	}
	producers := make([]producer, workers)
	queue := make([]int, 0, queueCapacity)
	next, finished := 0, 0
	now, diskEnd := time.Duration(0), time.Duration(0)
	diskActive := false
	for finished < len(jobs) {
		// Drain a finished disk event before admitting blocked producers.
		if diskActive && diskEnd == now {
			finished++
			diskActive = false
		}
		for i := range producers {
			p := &producers[i]
			if p.occupied && p.ready <= now {
				p.blocked = true
			}
		}
		// At most queueCapacity waiting artifacts plus one disk-service artifact.
		// Blocked producers use fixed worker-index priority for reproducibility,
		// not FIFO ordering across different completion times.
		for {
			changed := false
			if !diskActive && len(queue) > 0 {
				job := queue[0]
				queue = queue[1:]
				var err error
				diskEnd, err = historyIOTimeAdd(now, services[job])
				if err != nil {
					return out, err
				}
				diskActive, changed = true, true
			}
			for i := range producers {
				p := &producers[i]
				if !p.occupied || !p.blocked || len(queue) >= queueCapacity {
					continue
				}
				queue = append(queue, p.job)
				out.MaxQueued = max(out.MaxQueued, len(queue))
				var err error
				out.ProducerBlocked, err = historyIOTimeAdd(out.ProducerBlocked, now-p.ready)
				if err != nil {
					return out, err
				}
				p.occupied, p.blocked, changed = false, false, true
			}
			if !changed {
				break
			}
		}
		occupied := 0
		for i := range producers {
			p := &producers[i]
			if !p.occupied && next < len(jobs) {
				ready, err := historyIOTimeAdd(now, jobs[next].Work)
				if err != nil {
					return out, err
				}
				*p = producer{job: next, ready: ready, occupied: true}
				next++
			}
			if p.occupied {
				occupied++
			}
		}
		out.MaxOccupiedProducers = max(out.MaxOccupiedProducers, occupied)
		if finished == len(jobs) {
			break
		}
		event := time.Duration(math.MaxInt64)
		if diskActive {
			event = diskEnd
		}
		for _, p := range producers {
			if p.occupied && !p.blocked && p.ready < event {
				event = p.ready
			}
		}
		if event <= now || event == time.Duration(math.MaxInt64) {
			return out, errors.New("history I/O model cannot advance")
		}
		now = event
	}
	out.Wall = now
	return out, nil
}

func TestHistoryIOModelSharesBandwidthAndBoundsQueue(t *testing.T) {
	job := historyIOJob{Work: time.Millisecond, Cost: historyIOCost{FinalWrite: 1 << 20}}
	jobs := []historyIOJob{job, job, job, job, job}
	out, err := modelHistoryIO(jobs, 1, 1, 100<<20)
	if err != nil || out.Wall != 51*time.Millisecond || out.DiskBusy != 50*time.Millisecond || out.ProducerBlocked <= 0 || out.MaxQueued != 1 || out.MaxOccupiedProducers != 1 {
		t.Fatalf("bounded serial producer: %+v %v", out, err)
	}
	out, err = modelHistoryIO(jobs, 4, 2, 100<<20)
	if err != nil || out.Wall != 51*time.Millisecond || out.MaxQueued > 2 || out.MaxOccupiedProducers > 4 {
		t.Fatalf("workers acquired separate disks: %+v %v", out, err)
	}
	job.Work = 10 * time.Millisecond
	out, err = modelHistoryIO([]historyIOJob{job, job}, 2, 2, 100<<20)
	if err != nil || out.Wall != 30*time.Millisecond {
		t.Fatalf("overlapped producer work: %+v %v", out, err)
	}
}

func TestHistoryIOServiceExactArithmeticAndAllByteFamilies(t *testing.T) {
	for _, rate := range []uint64{50, 100, 250} {
		got, err := historyIOService(rate<<20, rate<<20)
		if err != nil || got != time.Second {
			t.Fatalf("rate %d: %v %v", rate, got, err)
		}
	}
	got, err := historyIOService(1, 3)
	if err != nil || got != 333333334*time.Nanosecond {
		t.Fatalf("rounded service %v %v", got, err)
	}
	cost := historyIOCost{1, 2, 3, 4, 5, 6, 7, 8}
	if got, err := cost.bytes(); err != nil || got != 36 {
		t.Fatalf("cost=%d %v", got, err)
	}
	if _, err := (historyIOCost{SourceRead: math.MaxUint64, FinalWrite: 1}).bytes(); err == nil {
		t.Fatal("byte overflow accepted")
	}
	for _, pair := range [][2]uint64{{1, 0}, {math.MaxUint64, 1}, {math.MaxUint64, 1_000_000_000}} {
		if _, err := historyIOService(pair[0], pair[1]); err == nil {
			t.Fatal("duration overflow/zero bandwidth accepted", pair)
		}
	}
	for _, workers := range []int{0, 33} {
		if _, err := modelHistoryIO(nil, workers, 2, 100<<20); err == nil {
			t.Fatal("unbounded workers accepted")
		}
	}
}
