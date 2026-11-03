package cache

import (
	"github.com/cockroachdb/pebble/internal/base"
	"sync"
	"testing"
)

func assertResidentConservation(t *testing.T, c *Cache) Metrics {
	t.Helper()
	m := c.Metrics()
	if m.Size != m.Activity.Balance() || m.ConservationErrorBytes != 0 {
		t.Fatalf("aggregate conservation: %+v", m)
	}
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.RLock()
		var resident int64
		if s.handHot != nil {
			start := s.handHot
			for e := start; ; {
				if e.peekValue() != nil {
					resident += e.size
				}
				e = e.next()
				if e == start {
					break
				}
			}
		}
		if resident != s.sizeHot+s.sizeCold || resident != s.activity.Balance() {
			s.mu.RUnlock()
			t.Fatalf("shard%d actual entries=%d counters=%d", i, resident, s.activity.Balance())
		}
		s.mu.RUnlock()
	}
	return m
}
func TestResidentResizeDetachedAndReserve(t *testing.T) {
	c := newShards(1024, 1)
	defer c.Unref()
	file := base.FileNum(1).DiskFileNum()
	old := c.Set(1, file, 0, newValue(128))
	c.Set(1, file, 0, newValue(192)).Release()
	c.Set(1, file, 0, newValue(96)).Release()
	m := assertResidentConservation(t, c)
	if m.Size != 96 || m.Activity.AddedBytes != 128 || m.Activity.ResizeGrowBytes != 64 || m.Activity.ResizeShrinkBytes != 96 || len(old.Get()) != 128 {
		t.Fatalf("resize or detached value: %+v", m)
	}
	old.Release()
	prior := m
	release := c.Reserve(4096)
	m = assertResidentConservation(t, c)
	if m.EffectiveTargetBytes != 1 || m.Activity.RemovedReserveBytes-prior.Activity.RemovedReserveBytes != prior.Size || m.Activity.RemovedClockBytes != prior.Activity.RemovedClockBytes {
		t.Fatalf("reserve clamp/double count: %+v", m)
	}
	release()
	m = assertResidentConservation(t, c)
	if m.ReservedBytes != 0 || m.EffectiveTargetBytes != 1024 || m.Size != 0 {
		t.Fatalf("release refilled cache: %+v", m)
	}
}
func TestResidentGhostNoopAndDelete(t *testing.T) {
	c := newShards(512, 1)
	defer c.Unref()
	for i := 1; i < 20; i++ {
		c.Set(1, base.FileNum(i).DiskFileNum(), 0, newValue(128)).Release()
		assertResidentConservation(t, c)
	}
	s := &c.shards[0]
	var ghostFile base.DiskFileNum
	var ghostBytes int64
	for e := s.handHot; ; {
		if e.ptype == etTest {
			ghostFile = e.key.fileNum
			ghostBytes = e.size
			break
		}
		e = e.next()
		if e == s.handHot {
			t.Fatal("fixture has no ghost")
		}
	}
	before := c.Metrics()
	c.EvictFile(1, ghostFile)
	after := assertResidentConservation(t, c)
	if after.Size != before.Size || after.Activity.RemovedFileBytes != before.Activity.RemovedFileBytes || after.Activity.FileGhostRemovedBytes-before.Activity.FileGhostRemovedBytes != ghostBytes {
		t.Fatalf("ghost counted as resident: %+v", after)
	}
	before = after
	c.EvictFile(1, base.FileNum(999).DiskFileNum())
	after = assertResidentConservation(t, c)
	if before.Size != after.Size || before.Activity.RemovedFileBytes != after.Activity.RemovedFileBytes {
		t.Fatal("no-op invalidation removed resident bytes")
	}
	resident := s.handHot
	for resident.peekValue() == nil {
		resident = resident.next()
	}
	size := resident.size
	before = after
	c.Delete(1, resident.key.fileNum, resident.key.offset)
	after = assertResidentConservation(t, c)
	if after.Activity.RemovedDeleteBytes-before.Activity.RemovedDeleteBytes != size {
		t.Fatal("explicit delete reason lost")
	}
}
func TestResidentConcurrentBatchFileEvictionAndSnapshot(t *testing.T) {
	c := newShards(1<<20, 4)
	defer c.Unref()
	file := base.FileNum(5).DiskFileNum()
	for i := 0; i < 128; i++ {
		c.Set(1, file, uint64(i), newValue(512)).Release()
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 128; i++ {
			c.Set(1, file, uint64(i), newValue(256+(i%3)*128)).Release()
		}
	}()
	go func() { defer wg.Done(); c.EvictFile(1, file) }()
	for i := 0; i < 100; i++ {
		m := c.Metrics()
		if m.ConservationErrorBytes != 0 || m.Size != m.Activity.Balance() {
			t.Fatalf("concurrent cut: %+v", m)
		}
	}
	wg.Wait()
	assertResidentConservation(t, c)
	c.EvictFile(1, file)
	m := assertResidentConservation(t, c)
	if m.Size != 0 || m.Activity.RemovedFileBytes == 0 {
		t.Fatalf("file batch not accounted: %+v", m)
	}
}
func TestResidentDestroyAndNewSource(t *testing.T) {
	c := newShards(1024, 1)
	id := c.Metrics().SourceID
	c.Set(1, base.FileNum(1).DiskFileNum(), 0, newValue(64)).Release()
	c.Unref()
	s := &c.shards[0]
	if s.activity.RemovedOtherBytes != 64 || s.activity.Balance() != 0 || s.sizeHot+s.sizeCold != 0 {
		t.Fatal("destroy not counted separately")
	}
	fresh := newShards(1024, 1)
	defer fresh.Unref()
	m := assertResidentConservation(t, fresh)
	if id == 0 || m.SourceID == id || m.Activity != (ResidentActivity{}) {
		t.Fatal("cache source identity/counters reused")
	}
}

func TestResidentPromotionOversizeAndSnapshots(t *testing.T) {
	c := newShards(512, 1)
	defer c.Unref()
	for i := 1; i < 20; i++ {
		c.Set(1, base.FileNum(i).DiskFileNum(), 0, newValue(128)).Release()
	}
	s := &c.shards[0]
	ghost := s.handHot
	for ghost.ptype != etTest {
		ghost = ghost.next()
	}
	file, offset, size := ghost.key.fileNum, ghost.key.offset, ghost.size
	before := c.Metrics()
	c.Set(1, file, offset, newValue(128)).Release()
	after := assertResidentConservation(t, c)
	if after.Activity.PromotionGhostRemovedBytes-before.Activity.PromotionGhostRemovedBytes != size {
		t.Fatal("ghost promotion not distinguished")
	}
	before = after
	h := c.Set(1, base.FileNum(100).DiskFileNum(), 0, newValue(2048))
	if len(h.Get()) != 2048 {
		t.Fatal("oversized handle broken")
	}
	h.Release()
	after = assertResidentConservation(t, c)
	if after.Activity.AddedBytes != before.Activity.AddedBytes {
		t.Fatal("uncached oversized value counted resident")
	}
	again := c.Metrics()
	if again != after {
		t.Fatal("snapshot changed cache")
	}
}

func TestResidentShardErrorsCannotCancelInAggregate(t *testing.T) {
	c := newShards(4096, 2)
	defer c.Unref()
	c.shards[0].activity.AddedBytes = 1
	c.shards[1].activity.AddedBytes = -1
	m := c.Metrics()
	if m.ConservationErrorBytes != 0 || m.ConservationMismatchShards != 2 {
		t.Fatalf("opposing shard errors hidden: %+v", m)
	}
	c.shards[0].activity = ResidentActivity{}
	c.shards[1].activity = ResidentActivity{}
}
