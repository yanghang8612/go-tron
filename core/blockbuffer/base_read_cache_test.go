package blockbuffer

import (
	"fmt"
	"testing"
)

func testBaseReadCacheSet(c *baseReadCache, key, value []byte) {
	_, _, epoch := c.getWithEpoch(key)
	_, _ = c.setIfEpoch(key, value, epoch)
}

func TestBaseReadCache_BoundedPayloadAndInvalidationQueue(t *testing.T) {
	const size = 64 * 256
	c := newBaseReadCache(size)
	totalLimit := 0
	for i := range c.shards {
		totalLimit += c.shards[i].limit
	}
	if totalLimit != size {
		t.Fatalf("shard limits sum to %d, want exact configured budget %d", totalLimit, size)
	}
	value := make([]byte, 96)
	for i := 0; i < 10_000; i++ {
		key := []byte(fmt.Sprintf("branch-%08d", i))
		testBaseReadCacheSet(c, key, value)
	}
	for i := range c.shards {
		s := &c.shards[i]
		if s.used > s.limit {
			t.Fatalf("shard %d retained %d bytes above limit %d", i, s.used, s.limit)
		}
	}

	// Repeated populate→flush-invalidate cycles must not accumulate one stale
	// FIFO token per block for the lifetime of a long sync session.
	key := []byte("repeated-hot-branch")
	shard := &c.shards[baseReadCacheShardIndex(key)]
	for i := 0; i < 10_000; i++ {
		testBaseReadCacheSet(c, key, value)
		c.del(key)
	}
	if live := len(shard.queue) - shard.head; live >= 2048 {
		t.Fatalf("invalidation queue retained %d stale tokens, want <2048", live)
	}
}

func TestBaseReadCache_SetFlushedRefreshesOnlyCachedKeys(t *testing.T) {
	c := newBaseReadCache(1 << 20)
	cachedKey := []byte("cached-commitment-branch")
	uncachedKey := "unrelated-block-metadata"
	oldValue := []byte("old")
	newValue := []byte("new")
	testBaseReadCacheSet(c, cachedKey, oldValue)

	c.setFlushed(string(cachedKey), newValue)
	got, ok, _ := c.getWithEpoch(cachedKey)
	if !ok || string(got) != "new" {
		t.Fatalf("flushed cached value = (%q,%v), want (new,true)", got, ok)
	}
	if len(got) == 0 || &got[0] != &newValue[0] {
		t.Fatal("flushed immutable layer value was copied instead of retained")
	}

	c.setFlushed(uncachedKey, []byte("metadata"))
	if _, ok, _ := c.getWithEpoch([]byte(uncachedKey)); ok {
		t.Fatal("flush admitted a key that was never read through the cache")
	}

	shard := &c.shards[baseReadCacheShardIndex(cachedKey)]
	for i := 0; i < 10_000; i++ {
		c.setFlushed(string(cachedKey), newValue)
	}
	if live := len(shard.queue) - shard.head; live != 1 {
		t.Fatalf("flush refresh queue retained %d tokens, want the original 1", live)
	}
}

func TestBaseReadCache_SetFlushedRejectsLateOldGenerationFill(t *testing.T) {
	c := newBaseReadCache(1 << 20)
	key := []byte("racing-commitment-branch")
	_, _, oldEpoch := c.getWithEpoch(key)

	// There is no resident entry to refresh, but the flush must still advance
	// the generation so a read that began before it cannot publish stale bytes.
	c.setFlushed(string(key), []byte("new"))
	if _, stored := c.setIfEpoch(key, []byte("old"), oldEpoch); stored {
		t.Fatal("pre-flush read populated stale bytes after the flush")
	}
	if _, ok, _ := c.getWithEpoch(key); ok {
		t.Fatal("uncached flush should invalidate without admitting the key")
	}
}

func TestBaseReadCache_SetFlushedDropsOversizedReplacement(t *testing.T) {
	// 256 bytes per shard: the original row fits, the replacement does not.
	c := newBaseReadCache(baseReadCacheShardCount * 256)
	key := []byte("hot-branch")
	testBaseReadCacheSet(c, key, []byte("old"))
	if _, ok, _ := c.getWithEpoch(key); !ok {
		t.Fatal("test setup did not cache original value")
	}

	c.setFlushed(string(key), make([]byte, 512))
	if _, ok, _ := c.getWithEpoch(key); ok {
		t.Fatal("oversized flushed replacement retained a stale or over-budget entry")
	}
}

func BenchmarkBaseReadCacheFlushedHotKey(b *testing.B) {
	key := []byte("state-commitment-branch-v1-hot-prefix")
	keyString := string(key)
	value := make([]byte, 1500)

	b.Run("invalidate_and_refill", func(b *testing.B) {
		c := newBaseReadCache(1 << 20)
		testBaseReadCacheSet(c, key, value)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			c.del(key)
			_, _, epoch := c.getWithEpoch(key)
			if _, stored := c.setIfEpoch(key, value, epoch); !stored {
				b.Fatal("refill rejected")
			}
		}
	})

	b.Run("refresh_from_layer", func(b *testing.B) {
		c := newBaseReadCache(1 << 20)
		testBaseReadCacheSet(c, key, value)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			c.setFlushed(keyString, value)
		}
	})
}
