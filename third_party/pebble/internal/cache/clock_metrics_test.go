package cache

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
)

func TestClockMetricsEmpty(t *testing.T) {
	c := newShards(4096, 4)
	defer c.Unref()
	m := c.Metrics()
	if m.Clock.Shards != 4 || m.Clock.ColdTargetBytes != 4096 || m.Clock.ColdCountBuckets != [6]int64{4} {
		t.Fatalf("empty cache clock snapshot: %+v", m.Clock)
	}
	if m.Size != 0 || m.Count != 0 || m.Clock.HotCount != 0 || m.Clock.ColdCount != 0 || m.Clock.TestCount != 0 ||
		m.Clock.HotBytes != 0 || m.Clock.ColdBytes != 0 || m.Clock.TestBytes != 0 || m.Clock.ColdCountMin != 0 || m.Clock.ColdCountMax != 0 {
		t.Fatalf("empty cache has occupancy: %+v", m)
	}
	if got := (&Cache{}).Metrics(); got != (Metrics{}) {
		t.Fatalf("zero-shard cache snapshot: %+v", got)
	}
}

func TestClockMetricsColdCountDistribution(t *testing.T) {
	// Generate real cold entries, including both sides of every bucket boundary.
	// No eviction is necessary at this size, so the input counts are an
	// independent distribution oracle, not copies of the Metrics aggregation.
	counts := [...]int{0, 1, 2, 7, 8, 63, 64, 511, 512, 513}
	c := newShards(16<<20, len(counts))
	defer c.Unref()
	var bytes int64
	for i, count := range counts {
		for j := 0; j < count; j++ {
			size := 1 + j%31
			v := newValue(size)
			c.shards[i].Set(1, base.FileNum(1).DiskFileNum(), uint64(j+1), v).Release()
			bytes += int64(size)
		}
	}
	m := c.Metrics()
	if m.Clock.Shards != 10 || m.Clock.ColdCount != 1681 || m.Clock.ColdCountMin != 0 || m.Clock.ColdCountMax != 513 ||
		m.Clock.ColdCountBuckets != [6]int64{1, 1, 2, 2, 2, 2} {
		t.Fatalf("cold distribution = %+v", m.Clock)
	}
	if m.Clock.HotCount != 0 || m.Clock.TestCount != 0 || m.Size != bytes || m.Clock.ColdBytes != bytes || m.Count != 1681 {
		t.Fatalf("cold-only payload/count = %+v, want bytes=%d", m, bytes)
	}
}

type clockEntrySnapshot struct {
	key                            key
	value                          *Value
	next, prev, fileNext, filePrev *entry
	size                           int64
	kind                           entryType
	referenced                     bool
}

type clockShardSnapshot struct {
	hot, cold, test                                 *entry
	counts                                          [3]int64
	bytes                                           [3]int64
	coldTarget, reservedSize, maxSize, hits, misses int64
	blockCount, fileCount                           int
	entries                                         map[*entry]clockEntrySnapshot
}

func snapshotClockShards(c *Cache) []clockShardSnapshot {
	out := make([]clockShardSnapshot, len(c.shards))
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.RLock()
		snap := clockShardSnapshot{
			hot: s.handHot, cold: s.handCold, test: s.handTest,
			counts:     [3]int64{s.countHot, s.countCold, s.countTest},
			bytes:      [3]int64{s.sizeHot, s.sizeCold, s.sizeTest},
			coldTarget: s.coldTarget, reservedSize: s.reservedSize, maxSize: s.maxSize,
			hits: s.hits.Load(), misses: s.misses.Load(),
			blockCount: s.blocks.Count(), fileCount: s.files.Count(),
			entries: make(map[*entry]clockEntrySnapshot),
		}
		for e := s.handCold; e != nil; {
			snap.entries[e] = clockEntrySnapshot{e.key, e.val, e.next(), e.prev(), e.fileLink.next, e.fileLink.prev, e.size, e.ptype, e.referenced.Load()}
			e = e.next()
			if e == s.handCold {
				break
			}
		}
		out[i] = snap
		s.mu.RUnlock()
	}
	return out
}

func TestClockMetricsReadOnlyWithHotColdAndGhostEntries(t *testing.T) {
	c := newShards(512, 4)
	defer c.Unref()
	for i := range c.shards {
		s := &c.shards[i]
		for j := uint64(1); j <= 12; j++ {
			s.Set(1, base.FileNum(1).DiskFileNum(), j, newValue(16)).Release()
		}
		// Refault a real ghost, causing the ordinary algorithm to admit a hot
		// entry. Leave a referenced bit set so a metrics walk cannot clear it.
		var ghost *entry
		for e := s.handCold; e != nil; {
			if e.ptype == etTest {
				ghost = e
				break
			}
			e = e.next()
			if e == s.handCold {
				break
			}
		}
		if ghost == nil {
			t.Fatal("fixture did not create a ghost")
		}
		k := ghost.key
		s.Set(k.id, k.fileNum, k.offset, newValue(16)).Release()
		s.Get(k.id, k.fileNum, k.offset).Release()
	}
	before := snapshotClockShards(c)
	m := c.Metrics()
	if again := c.Metrics(); again != m {
		t.Fatal("repeated Metrics changed the sample without mutations")
	}
	after := snapshotClockShards(c)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("Metrics mutated accounting, clock hands, links, entries or reference bits")
	}
	// Independently walk the real clock rings and classify entry types/values;
	// do not derive expected occupancy from shard accounting counters.
	var counts, bytes [3]int64
	for _, s := range before {
		for _, e := range s.entries {
			counts[e.kind]++
			bytes[e.kind] += e.size
			if (e.kind == etTest) != (e.value == nil) {
				t.Fatal("fixture entry/value mismatch")
			}
		}
	}
	if counts[etHot] == 0 || counts[etCold] == 0 || counts[etTest] == 0 {
		t.Fatalf("missing entry class: %v", counts)
	}
	if m.Clock.HotCount != counts[etHot] || m.Clock.ColdCount != counts[etCold] || m.Clock.TestCount != counts[etTest] ||
		m.Clock.HotBytes != bytes[etHot] || m.Clock.ColdBytes != bytes[etCold] || m.Clock.TestBytes != bytes[etTest] ||
		m.Size != bytes[etHot]+bytes[etCold] || m.Count != counts[etHot]+counts[etCold]+counts[etTest] {
		t.Fatalf("clock sample %+v disagrees with ring oracle counts=%v bytes=%v", m, counts, bytes)
	}
}

// Exercise the existing sampling lock concurrently with real cache mutations.
// Equalities join fields sampled under each same shard lock, so remain valid
// even though the aggregate does not represent one cross-shard instant.
func TestClockMetricsConcurrentSampling(t *testing.T) {
	c := newShards(4096, 4)
	defer c.Unref()
	start := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-start
		for i := uint64(0); i < 12000; i++ {
			file := base.FileNum(i%7 + 1).DiskFileNum()
			c.Set(1, file, i%113, newValue(int(i%31+1))).Release()
			c.Get(1, file, i%113).Release()
			if i%17 == 0 {
				c.Delete(1, file, i%113)
			}
			if i%101 == 0 {
				c.EvictFile(1, file)
			}
			if i%211 == 0 {
				release := c.Reserve(512)
				release()
			}
		}
	}()
	close(start)
	for i := 0; i < 4000; i++ {
		m := c.Metrics()
		var buckets int64
		for _, n := range m.Clock.ColdCountBuckets {
			buckets += n
		}
		if m.Clock.Shards != 4 || buckets != 4 || m.Size != m.Clock.HotBytes+m.Clock.ColdBytes || m.Count != m.Clock.HotCount+m.Clock.ColdCount+m.Clock.TestCount || m.Clock.ColdCountMin > m.Clock.ColdCountMax {
			<-done
			t.Fatalf("incoherent per-shard accounting while mutating: %+v", m)
		}
	}
	<-done
}
