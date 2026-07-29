package blockbuffer

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"unsafe"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type prefixBucketIteratorEntry struct {
	key   []byte
	value []byte
}

func prefixBucketAccountID(bucket, tail byte) common.AccountID {
	var id common.AccountID
	id[0] = bucket
	id[len(id)-1] = tail
	return id
}

func collectPrefixBucketIterator(it ethdb.Iterator) ([]prefixBucketIteratorEntry, error) {
	defer it.Release()
	var entries []prefixBucketIteratorEntry
	for it.Next() {
		entries = append(entries, prefixBucketIteratorEntry{
			key:   append([]byte(nil), it.Key()...),
			value: append([]byte(nil), it.Value()...),
		})
	}
	return entries, it.Error()
}

func assertPrefixBucketIteratorEquivalent(t *testing.T, b *Buffer, schema []byte, accountID common.AccountID, generation uint64, domain uint16, logicalPrefix []byte) {
	t.Helper()
	physical := appendStateKVLatestKey(nil, schema, accountID, generation, domain, logicalPrefix)
	structured, err := collectPrefixBucketIterator(b.NewStateKVLatestIterator(schema, accountID, physical))
	if err != nil {
		t.Fatalf("structured iterator: %v", err)
	}
	generic, err := collectPrefixBucketIterator(b.NewIterator(physical, nil))
	if err != nil {
		t.Fatalf("generic iterator: %v", err)
	}
	if !slices.EqualFunc(structured, generic, func(a, b prefixBucketIteratorEntry) bool {
		return bytes.Equal(a.key, b.key) && bytes.Equal(a.value, b.value)
	}) {
		t.Fatalf("structured entries = %x, generic entries = %x", structured, generic)
	}
}

func prefixBucketIteratorValue(t *testing.T, b *Buffer, schema []byte, accountID common.AccountID, generation uint64, domain uint16, logicalKey []byte) ([]byte, bool) {
	t.Helper()
	physical := appendStateKVLatestKey(nil, schema, accountID, generation, domain, logicalKey)
	entries, err := collectPrefixBucketIterator(b.NewStateKVLatestIterator(schema, accountID, physical))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if bytes.Equal(entry.key, physical) {
			return entry.value, true
		}
	}
	return nil, false
}

func allPrefixBucketsBuilt(index *layerPrefixBucketIndex) bool {
	if index == nil {
		return false
	}
	for _, word := range index.built {
		if word != ^uint64(0) {
			return false
		}
	}
	return true
}

func TestLayerPrefixBucketIndexPackedBuild(t *testing.T) {
	const schema = "state\x00kv/"
	writes := map[string][]byte{
		schema + "\x00write-a": []byte("a"),
		schema + "\x7fwrite-b": []byte("b"),
		schema + "\xffwrite-c": []byte("c"),
		"other\x7fwrite":       []byte("ignored"),
	}
	deletes := map[string]struct{}{
		schema + "\x00delete-a": {},
		schema + "\xffdelete-c": {},
		"other\x00delete":       {},
	}
	index := newLayerPrefixBucketIndex(schema)
	index.ensureBucket(0x7f, writes, deletes)
	if allPrefixBucketsBuilt(index) {
		t.Fatal("first bucket query eagerly built the full index")
	}
	index.ensureBucket(0x00, writes, deletes)
	if !allPrefixBucketsBuilt(index) {
		t.Fatal("packed build did not mark every bucket built")
	}

	want := make(map[string]int)
	for key := range writes {
		if strings.HasPrefix(key, schema) {
			want[key]++
		}
	}
	for key := range deletes {
		if strings.HasPrefix(key, schema) {
			want[key]++
		}
	}
	got := make(map[string]int)
	for bucket, keys := range index.buckets {
		if cap(keys) != len(keys) {
			t.Fatalf("bucket %#02x cap=%d len=%d, want cap == len", bucket, cap(keys), len(keys))
		}
		for _, key := range keys {
			if !strings.HasPrefix(key, schema) || key[len(schema)] != bucket {
				t.Fatalf("key %q stored in wrong bucket %#02x", key, bucket)
			}
			got[key]++
		}
	}
	if !mapsEqualStringInt(got, want) {
		t.Fatalf("packed keys = %v, want %v", got, want)
	}
	adjacentBefore := append([]string(nil), index.buckets[0x7f]...)
	lateHead := schema + "\x00late"
	writes[lateHead] = []byte("late")
	index.addToBucket(0x00, lateHead)
	if !slices.Equal(index.buckets[0x7f], adjacentBefore) {
		t.Fatalf("append to bucket 0 overwrote adjacent arena segment: got %q want %q", index.buckets[0x7f], adjacentBefore)
	}
	tailBefore := append([]string(nil), index.buckets[0xff]...)
	lateMiddle := schema + "\x7flate"
	deletes[lateMiddle] = struct{}{}
	index.addToBucket(0x7f, lateMiddle)
	if !slices.Equal(index.buckets[0xff], tailBefore) {
		t.Fatalf("append to bucket 127 overwrote tail arena segment: got %q want %q", index.buckets[0xff], tailBefore)
	}
}

func mapsEqualStringInt(a, b map[string]int) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if b[key] != value {
			return false
		}
	}
	return true
}

func TestLayerPrefixBucketIndexEmptyBuildAndIncrementalMutations(t *testing.T) {
	schema := []byte("state-kv/")
	accountID := prefixBucketAccountID(0xa5, 0x01)
	const (
		generation = uint64(7)
		domain     = uint16(0x0201)
	)
	base := rawdb.NewMemoryDatabase()
	baseKey := appendStateKVLatestKey(nil, schema, accountID, generation, domain, []byte("base"))
	if err := base.Put(baseKey, []byte("base-value")); err != nil {
		t.Fatal(err)
	}
	b := New(base)
	b.BeginBlock(bufHash(0xa1), 1)

	physical := appendStateKVLatestKey(nil, schema, accountID, generation, domain, nil)
	entries, err := collectPrefixBucketIterator(b.NewStateKVLatestIterator(schema, accountID, physical))
	if err != nil || len(entries) != 1 {
		t.Fatalf("initial entries=%v err=%v, want one base row", entries, err)
	}
	for shard := range b.inflight[0].shards {
		index := b.inflight[0].shards[shard].prefixBucketIndex
		if index == nil || allPrefixBucketsBuilt(index) || len(index.buckets) != 0 {
			t.Fatalf("shard %d empty index = %+v", shard, index)
		}
	}
	otherID := prefixBucketAccountID(accountID[0]+1, 0x02)
	otherPhysical := appendStateKVLatestKey(nil, schema, otherID, generation, domain, nil)
	entries, err = collectPrefixBucketIterator(b.NewStateKVLatestIterator(schema, otherID, otherPhysical))
	if err != nil || len(entries) != 0 {
		t.Fatalf("second empty-bucket entries=%v err=%v", entries, err)
	}
	for shard := range b.inflight[0].shards {
		index := b.inflight[0].shards[shard].prefixBucketIndex
		if !allPrefixBucketsBuilt(index) || len(index.buckets) != 0 {
			t.Fatalf("shard %d packed empty index = %+v", shard, index)
		}
	}

	putKey := appendStateKVLatestKey(nil, schema, accountID, generation, domain, []byte{'p', 0x00, 0xff})
	if err := b.Put(putKey, []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := b.Put(putKey, []byte("two")); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete(baseKey); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete(putKey); err != nil {
		t.Fatal(err)
	}
	if err := b.Put(putKey, []byte("three")); err != nil {
		t.Fatal(err)
	}

	entries, err = collectPrefixBucketIterator(b.NewStateKVLatestIterator(schema, accountID, physical))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || !bytes.Equal(entries[0].key, putKey) || !bytes.Equal(entries[0].value, []byte("three")) {
		t.Fatalf("incremental entries = %x, want binary put=three", entries)
	}
	shard := b.inflight[0].shardForBytes(putKey)
	count := 0
	for _, key := range shard.prefixBucketIndex.buckets[accountID[0]] {
		if key == string(putKey) {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("incrementally tracked key count = %d, want 1", count)
	}
}

func TestStateKVLatestPackedIteratorEquivalentAcrossVersionsAndSchemas(t *testing.T) {
	schema := []byte{'s', 0x00, 'a', '/'}
	otherSchema := []byte{'s', 0x00, 'b', '/'}
	const (
		generation = uint64(3)
		domain     = uint16(0x0202)
	)
	accounts := []common.AccountID{
		prefixBucketAccountID(0x00, 0x01),
		prefixBucketAccountID(0x7f, 0x02),
		prefixBucketAccountID(0xff, 0x03),
	}
	base := rawdb.NewMemoryDatabase()
	for n, accountID := range accounts {
		for _, logical := range [][]byte{[]byte("base/a"), {'b', 0x00, 0xff}} {
			key := appendStateKVLatestKey(nil, schema, accountID, generation, domain, logical)
			if err := base.Put(key, []byte(fmt.Sprintf("base-%d-%x", n, logical))); err != nil {
				t.Fatal(err)
			}
		}
	}
	b := New(base)
	b.BeginBlock(bufHash(0xb1), 1)
	if err := b.PutStateKVLatest(schema, accounts[0], generation, domain, []byte("base/a"), []byte("layer-1")); err != nil {
		t.Fatal(err)
	}
	if err := b.DeleteStateKVLatest(schema, accounts[1], generation, domain, []byte("base/a")); err != nil {
		t.Fatal(err)
	}
	if err := b.PutStateKVLatest(schema, accounts[2], generation, domain, []byte{'n', 0x00, 0xff}, []byte("binary")); err != nil {
		t.Fatal(err)
	}
	if err := b.PutStateKVLatest(otherSchema, accounts[0], generation, domain, []byte("other"), []byte("schema-b")); err != nil {
		t.Fatal(err)
	}
	b.CommitBlock()

	b.BeginBlock(bufHash(0xb2), 2)
	if err := b.DeleteStateKVLatest(schema, accounts[0], generation, domain, []byte("base/a")); err != nil {
		t.Fatal(err)
	}
	if err := b.PutStateKVLatest(schema, accounts[1], generation, domain, []byte("base/a"), []byte("layer-2")); err != nil {
		t.Fatal(err)
	}
	for _, accountID := range accounts {
		assertPrefixBucketIteratorEquivalent(t, b, schema, accountID, generation, domain, nil)
		assertPrefixBucketIteratorEquivalent(t, b, schema, accountID, generation, domain, []byte("base/"))
	}
	if value, exists := prefixBucketIteratorValue(t, b, schema, accounts[0], generation, domain, []byte("base/a")); exists {
		t.Fatalf("newer tombstone exposed base/a=%q", value)
	}
	if value, exists := prefixBucketIteratorValue(t, b, schema, accounts[1], generation, domain, []byte("base/a")); !exists || !bytes.Equal(value, []byte("layer-2")) {
		t.Fatalf("newer overwrite base/a=%q exists=%v, want layer-2", value, exists)
	}
	// The first schema owns the per-shard index. A different schema must retain
	// exact generic-iterator behavior through the established fallback.
	assertPrefixBucketIteratorEquivalent(t, b, otherSchema, accounts[0], generation, domain, nil)
	if err := b.PutStateKVLatest(otherSchema, accounts[0], generation, domain, []byte{'l', 0x00, 0xff}, []byte("late-schema-b")); err != nil {
		t.Fatal(err)
	}
	assertPrefixBucketIteratorEquivalent(t, b, otherSchema, accounts[0], generation, domain, nil)

	b.DiscardActive()
	for _, accountID := range accounts {
		assertPrefixBucketIteratorEquivalent(t, b, schema, accountID, generation, domain, nil)
	}
	if value, exists := prefixBucketIteratorValue(t, b, schema, accounts[0], generation, domain, []byte("base/a")); !exists || !bytes.Equal(value, []byte("layer-1")) {
		t.Fatalf("discard did not reveal older overwrite: value=%q exists=%v", value, exists)
	}
	b.BeginBlock(bufHash(0xb3), 3)
	if err := b.PutStateKVLatest(schema, accounts[0], generation, domain, []byte("base/a"), []byte("layer-3")); err != nil {
		t.Fatal(err)
	}
	b.CommitBlock()
	assertPrefixBucketIteratorEquivalent(t, b, schema, accounts[0], generation, domain, nil)
	if value, exists := prefixBucketIteratorValue(t, b, schema, accounts[0], generation, domain, []byte("base/a")); !exists || !bytes.Equal(value, []byte("layer-3")) {
		t.Fatalf("committed overwrite: value=%q exists=%v", value, exists)
	}
	b.DiscardBlock(bufHash(0xb3))
	if value, exists := prefixBucketIteratorValue(t, b, schema, accounts[0], generation, domain, []byte("base/a")); !exists || !bytes.Equal(value, []byte("layer-1")) {
		t.Fatalf("committed discard did not reveal layer-1: value=%q exists=%v", value, exists)
	}
	b.BeginBlock(bufHash(0xb4), 4)
	if err := b.PutStateKVLatest(schema, accounts[0], generation, domain, []byte("base/a"), []byte("layer-4")); err != nil {
		t.Fatal(err)
	}
	b.CommitBlock()
	if err := b.FlushUpTo(4, base); err != nil {
		t.Fatal(err)
	}
	if value, exists := prefixBucketIteratorValue(t, b, schema, accounts[0], generation, domain, []byte("base/a")); !exists || !bytes.Equal(value, []byte("layer-4")) {
		t.Fatalf("flushed overwrite: value=%q exists=%v", value, exists)
	}
}

func TestStateKVLatestPackedIteratorConcurrentMutation(t *testing.T) {
	schema := []byte("state-kv-race/")
	const (
		generation = uint64(11)
		domain     = uint16(0x0203)
		operations = 2000
		readers    = 4
	)
	b := New(nil)
	b.BeginBlock(bufHash(0xc1), 1)
	seedID := prefixBucketAccountID(0, 0)
	seedPrefix := appendStateKVLatestKey(nil, schema, seedID, generation, domain, nil)
	if it := b.NewStateKVLatestIterator(schema, seedID, seedPrefix); it.Next() {
		t.Fatal("empty seed iterator returned a row")
	} else {
		it.Release()
	}

	done := make(chan struct{})
	errCh := make(chan error, readers)
	var readWG sync.WaitGroup
	for reader := 0; reader < readers; reader++ {
		readWG.Add(1)
		go func(offset int) {
			defer readWG.Done()
			for n := 0; ; n++ {
				select {
				case <-done:
					return
				default:
				}
				accountID := prefixBucketAccountID(byte((n+offset*17)&0xff), byte(offset))
				physical := appendStateKVLatestKey(nil, schema, accountID, generation, domain, nil)
				entries, err := collectPrefixBucketIterator(b.NewStateKVLatestIterator(schema, accountID, physical))
				if err != nil {
					errCh <- err
					return
				}
				seen := make(map[string]struct{}, len(entries))
				for _, entry := range entries {
					if !bytes.HasPrefix(entry.key, physical) {
						errCh <- fmt.Errorf("iterator returned out-of-prefix key %x for %x", entry.key, physical)
						return
					}
					if _, duplicate := seen[string(entry.key)]; duplicate {
						errCh <- fmt.Errorf("iterator returned duplicate key %x", entry.key)
						return
					}
					seen[string(entry.key)] = struct{}{}
				}
				runtime.Gosched()
			}
		}(reader)
	}

	expected := make(map[string][]byte)
	for n := 0; n < operations; n++ {
		accountID := prefixBucketAccountID(byte(n&0xff), byte(n>>8))
		var logical [4]byte
		binary.BigEndian.PutUint32(logical[:], uint32(n%512))
		key := appendStateKVLatestKey(nil, schema, accountID, generation, domain, logical[:])
		if n%5 == 0 {
			if err := b.DeleteStateKVLatest(schema, accountID, generation, domain, logical[:]); err != nil {
				t.Fatal(err)
			}
			delete(expected, string(key))
		} else {
			value := []byte(strconv.Itoa(n))
			if err := b.PutStateKVLatest(schema, accountID, generation, domain, logical[:], value); err != nil {
				t.Fatal(err)
			}
			expected[string(key)] = append([]byte(nil), value...)
		}
	}
	close(done)
	readWG.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	for bucket := 0; bucket < 256; bucket++ {
		for tail := 0; tail < 8; tail++ {
			accountID := prefixBucketAccountID(byte(bucket), byte(tail))
			physical := appendStateKVLatestKey(nil, schema, accountID, generation, domain, nil)
			entries, err := collectPrefixBucketIterator(b.NewStateKVLatestIterator(schema, accountID, physical))
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				want, exists := expected[string(entry.key)]
				if !exists || !bytes.Equal(entry.value, want) {
					t.Fatalf("final entry %x=%q, want %q exists=%v", entry.key, entry.value, want, exists)
				}
				delete(expected, string(entry.key))
			}
		}
	}
	if len(expected) != 0 {
		t.Fatalf("final iterator missed %d expected entries", len(expected))
	}
}

var prefixBucketBenchmarkSink int

func benchmarkPrefixBucketMaps(keys int) (map[string][]byte, map[string]struct{}) {
	const schema = "state-kv-latest-v2-"
	writes := make(map[string][]byte, keys)
	deletes := make(map[string]struct{}, keys/8)
	for n := 0; n < keys; n++ {
		bucket := byte((n * 131) & 0xff)
		key := schema + string([]byte{bucket}) + strconv.Itoa(n) + strings.Repeat("x", 32)
		if n%8 == 0 {
			deletes[key] = struct{}{}
		} else {
			writes[key] = []byte("value")
		}
	}
	return writes, deletes
}

func benchmarkEnsurePrefixBucketLegacy(index *layerPrefixBucketIndex, bucket byte, writes map[string][]byte, deletes map[string]struct{}) {
	if index == nil || index.bucketBuilt(bucket) {
		return
	}
	for key := range writes {
		index.addToBucket(bucket, key)
	}
	for key := range deletes {
		index.addToBucket(bucket, key)
	}
	index.built[bucket>>6] |= uint64(1) << (bucket & 63)
}

func walkPrefixBucketLegacyForBenchmark(overlay *overlayState, layer *layer, schema string, bucket byte, physical string) {
	if layer == nil {
		return
	}
	for shardNum := range layer.shards {
		shard := &layer.shards[shardNum]
		shard.mu.Lock()
		if shard.prefixBucketIndex == nil {
			shard.prefixBucketIndex = newLayerPrefixBucketIndex(schema)
		}
		index := shard.prefixBucketIndex
		if index.prefix != schema {
			shard.mu.Unlock()
			overlay.walk(layer, []byte(physical), nil)
			return
		}
		benchmarkEnsurePrefixBucketLegacy(index, bucket, shard.writes, shard.deletes)
		for _, key := range index.buckets[bucket] {
			if !strings.HasPrefix(key, physical) {
				continue
			}
			if _, resolved := overlay.m[key]; resolved {
				continue
			}
			if value, exists := shard.writes[key]; exists {
				overlay.m[key] = overlayOp{value: append([]byte(nil), value...)}
				continue
			}
			if _, deleted := shard.deletes[key]; deleted {
				overlay.m[key] = overlayOp{deleted: true}
			}
		}
		shard.mu.Unlock()
	}
}

func newStateKVLatestIteratorLegacyForBenchmark(b *Buffer, schemaPrefix []byte, accountID common.AccountID, physicalPrefix []byte) ethdb.Iterator {
	view := b.loadReadView()
	overlay := newOverlayState()
	schema := string(schemaPrefix)
	physical := unsafe.String(unsafe.SliceData(physicalPrefix), len(physicalPrefix))
	for index := len(view.inflight) - 1; index >= 0; index-- {
		walkPrefixBucketLegacyForBenchmark(overlay, view.inflight[index], schema, accountID[0], physical)
	}
	for index := len(view.layers) - 1; index >= 0; index-- {
		walkPrefixBucketLegacyForBenchmark(overlay, view.layers[index], schema, accountID[0], physical)
	}
	return b.finishIterator(overlay, physicalPrefix, nil)
}

func resetPrefixBucketIndexesForBenchmark(b *Buffer) {
	view := b.loadReadView()
	reset := func(layer *layer) {
		for shardNum := range layer.shards {
			shard := &layer.shards[shardNum]
			shard.mu.Lock()
			shard.prefixBucketIndex = nil
			shard.mu.Unlock()
		}
	}
	for _, layer := range view.inflight {
		reset(layer)
	}
	for _, layer := range view.layers {
		reset(layer)
	}
}

func benchmarkProductionShapePrefixBuffer(b *testing.B) (*Buffer, []byte, []common.AccountID, uint64, uint16) {
	b.Helper()
	schema := []byte("state-kv-latest-v2-")
	const (
		liveLayers = 4
		// Production counters average about 1,514 operations per layer, or
		// 94.6 per shard. Six rows for each of 256 first-byte buckets gives
		// 1,536 rows/layer and closely matches that observed sync shape.
		keysPerAccount = 6
		generation     = uint64(5)
		domain         = uint16(0x0201)
	)
	accounts := make([]common.AccountID, 256)
	for bucket := range accounts {
		accounts[bucket] = prefixBucketAccountID(byte(bucket), byte(bucket^0xa5))
	}
	buffer := New(rawdb.NewMemoryDatabase())
	for layerNum := 0; layerNum < liveLayers; layerNum++ {
		buffer.BeginBlock(bufHash(byte(0xd0+layerNum)), uint64(layerNum+1))
		for bucket, accountID := range accounts {
			for keyNum := 0; keyNum < keysPerAccount; keyNum++ {
				var logical [36]byte
				binary.BigEndian.PutUint16(logical[:2], uint16(bucket))
				binary.BigEndian.PutUint16(logical[2:4], uint16(keyNum))
				for n := 4; n < len(logical); n++ {
					logical[n] = byte(n + keyNum)
				}
				desiredShard := uint32((bucket*keysPerAccount + keyNum) % layerShardCount)
				for candidate := 0; candidate < 256; candidate++ {
					logical[len(logical)-1] = byte(candidate)
					physical := appendStateKVLatestKey(nil, schema, accountID, generation, domain, logical[:])
					if layerShardIndexBytes(physical) == desiredShard {
						break
					}
				}
				value := []byte{byte(layerNum), byte(keyNum)}
				if err := buffer.PutStateKVLatest(schema, accountID, generation, domain, logical[:], value); err != nil {
					b.Fatal(err)
				}
			}
		}
		buffer.CommitBlock()
	}
	for layerNum, layer := range buffer.layers {
		for shardNum := range layer.shards {
			shard := &layer.shards[shardNum]
			if len(shard.writes)+len(shard.deletes) == 0 {
				b.Fatalf("production shape layer %d shard %d is empty", layerNum, shardNum)
			}
		}
	}
	return buffer, schema, accounts, generation, domain
}

func consumePrefixBucketIteratorForBenchmark(b *testing.B, iterator ethdb.Iterator) {
	b.Helper()
	count := 0
	for iterator.Next() {
		prefixBucketBenchmarkSink += len(iterator.Key()) + len(iterator.Value())
		count++
	}
	if err := iterator.Error(); err != nil {
		iterator.Release()
		b.Fatal(err)
	}
	iterator.Release()
	if count == 0 {
		b.Fatal("production-shape iterator returned no rows")
	}
}

func BenchmarkStateKVLatestIteratorPrefixIndex(b *testing.B) {
	for _, queried := range []int{1, 8, 32, 128} {
		b.Run("legacy/cold-buckets="+strconv.Itoa(queried), func(b *testing.B) {
			buffer, schema, accounts, generation, domain := benchmarkProductionShapePrefixBuffer(b)
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				resetPrefixBucketIndexesForBenchmark(buffer)
				for bucket := 0; bucket < queried; bucket++ {
					physical := appendStateKVLatestKey(nil, schema, accounts[bucket], generation, domain, nil)
					consumePrefixBucketIteratorForBenchmark(b, newStateKVLatestIteratorLegacyForBenchmark(buffer, schema, accounts[bucket], physical))
				}
			}
		})
		b.Run("packed/cold-buckets="+strconv.Itoa(queried), func(b *testing.B) {
			buffer, schema, accounts, generation, domain := benchmarkProductionShapePrefixBuffer(b)
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				resetPrefixBucketIndexesForBenchmark(buffer)
				for bucket := 0; bucket < queried; bucket++ {
					physical := appendStateKVLatestKey(nil, schema, accounts[bucket], generation, domain, nil)
					consumePrefixBucketIteratorForBenchmark(b, buffer.NewStateKVLatestIterator(schema, accounts[bucket], physical))
				}
			}
		})
	}

	for _, implementation := range []string{"legacy", "packed"} {
		b.Run(implementation+"/steady", func(b *testing.B) {
			buffer, schema, accounts, generation, domain := benchmarkProductionShapePrefixBuffer(b)
			physical := appendStateKVLatestKey(nil, schema, accounts[17], generation, domain, nil)
			var iterator ethdb.Iterator
			if implementation == "legacy" {
				iterator = newStateKVLatestIteratorLegacyForBenchmark(buffer, schema, accounts[17], physical)
			} else {
				iterator = buffer.NewStateKVLatestIterator(schema, accounts[17], physical)
			}
			consumePrefixBucketIteratorForBenchmark(b, iterator)
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				if implementation == "legacy" {
					iterator = newStateKVLatestIteratorLegacyForBenchmark(buffer, schema, accounts[17], physical)
				} else {
					iterator = buffer.NewStateKVLatestIterator(schema, accounts[17], physical)
				}
				consumePrefixBucketIteratorForBenchmark(b, iterator)
			}
		})
	}
}

func BenchmarkLayerPrefixBucketIndexBuild(b *testing.B) {
	const schema = "state-kv-latest-v2-"
	for _, keys := range []int{1024, 4096} {
		writes, deletes := benchmarkPrefixBucketMaps(keys)
		for _, queried := range []int{8, 32, 128} {
			name := "keys=" + strconv.Itoa(keys) + "/buckets=" + strconv.Itoa(queried)
			b.Run("legacy/"+name, func(b *testing.B) {
				b.ReportAllocs()
				for n := 0; n < b.N; n++ {
					index := newLayerPrefixBucketIndex(schema)
					for bucket := 0; bucket < queried; bucket++ {
						benchmarkEnsurePrefixBucketLegacy(index, byte(bucket), writes, deletes)
					}
					prefixBucketBenchmarkSink = len(index.buckets)
				}
			})
			b.Run("packed/"+name, func(b *testing.B) {
				b.ReportAllocs()
				for n := 0; n < b.N; n++ {
					index := newLayerPrefixBucketIndex(schema)
					for bucket := 0; bucket < queried; bucket++ {
						index.ensureBucket(byte(bucket), writes, deletes)
					}
					prefixBucketBenchmarkSink = len(index.buckets)
				}
			})
		}
	}
}
