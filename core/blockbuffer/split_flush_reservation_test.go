package blockbuffer

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type splitFlushProbe struct {
	*rawdb.DirectHistoryStore
	split   [][2]int
	generic []int
}

func (p *splitFlushProbe) NewSplitBatchWithSize(hot, history int) ethdb.Batch {
	p.split = append(p.split, [2]int{hot, history})
	return p.DirectHistoryStore.NewSplitBatchWithSize(hot, history)
}
func (p *splitFlushProbe) NewBatchWithSize(size int) ethdb.Batch {
	p.generic = append(p.generic, size)
	return p.DirectHistoryStore.NewBatchWithSize(size)
}
func splitFlushRecordSize(key string, value []byte, deleted bool) int {
	n := 1 + uvarintSize(len(key)) + len(key)
	if !deleted {
		n += uvarintSize(len(value)) + len(value)
	}
	return n
}
func splitFlushSize(records int) int {
	if records == 0 {
		return 0
	}
	return records + pebbleBatchHeaderSize + pebbleBatchRecordSlack
}
func TestSplitFlushReservationFinalOperations(t *testing.T) {
	for _, merged := range []bool{false, true} {
		for _, mode := range []string{"hot", "history", "mixed"} {
			t.Run(fmt.Sprintf("merged=%t/%s", merged, mode), func(t *testing.T) {
				hot, history := rawdb.NewMemoryDatabase(), rawdb.NewMemoryDatabase()
				p := &splitFlushProbe{DirectHistoryStore: rawdb.NewDirectHistoryStore(hot, history)}
				defer p.Close()
				buf := New(p)
				values := map[string][]byte{}
				deleted := map[string]bool{}
				keys := []string{"state-commitment-branch-test", "state-changeset-v2-test"}
				for block := 1; block <= 2; block++ {
					if !merged && block == 1 {
						continue
					}
					buf.BeginBlock(bufHash(byte(block)), uint64(block))
					for side, key := range keys {
						if (mode == "hot" && side == 1) || (mode == "history" && side == 0) {
							continue
						}
						value := bytes.Repeat([]byte{byte(block)}, 127*block)
						if err := buf.Put([]byte(key), value); err != nil {
							t.Fatal(err)
						}
						values[key] = value
						deleteKey := key + "-deleted"
						if block == 1 {
							if err := buf.Put([]byte(deleteKey), value); err != nil {
								t.Fatal(err)
							}
						} else {
							if err := buf.Delete([]byte(deleteKey)); err != nil {
								t.Fatal(err)
							}
							deleted[deleteKey] = true
						}
					}
					buf.CommitBlock()
				}
				if err := buf.FlushUpTo(2, p); err != nil {
					t.Fatal(err)
				}
				if len(p.split) != 1 || len(p.generic) != 0 {
					t.Fatalf("split=%v generic=%v", p.split, p.generic)
				}
				var want [2]int
				for key, value := range values {
					side := 0
					if rawdb.IsDirectHistoryKey([]byte(key)) {
						side = 1
					}
					want[side] += splitFlushRecordSize(key, value, false)
					got, err := p.Get([]byte(key))
					if err != nil || !bytes.Equal(got, value) {
						t.Fatalf("%q got=%x err=%v", key, got, err)
					}
				}
				for key := range deleted {
					side := 0
					if rawdb.IsDirectHistoryKey([]byte(key)) {
						side = 1
					}
					want[side] += splitFlushRecordSize(key, nil, true)
					if found, err := p.Has([]byte(key)); err != nil || found {
						t.Fatalf("deleted %q found=%t err=%v", key, found, err)
					}
				}
				want = [2]int{splitFlushSize(want[0]), splitFlushSize(want[1])}
				if p.split[0] != want {
					t.Fatalf("sizes=%v want=%v", p.split[0], want)
				}
			})
		}
	}
}
func TestSplitFlushReservationRangeFallback(t *testing.T) {
	p := &splitFlushProbe{DirectHistoryStore: rawdb.NewDirectHistoryStore(rawdb.NewMemoryDatabase(), rawdb.NewMemoryDatabase())}
	defer p.Close()
	buf := New(p)
	buf.BeginBlock(bufHash(1), 1)
	if err := buf.DeleteRange([]byte("state"), []byte("state~")); err != nil {
		t.Fatal(err)
	}
	if err := buf.Put([]byte("state-changeset-v2-kept"), []byte("kept")); err != nil {
		t.Fatal(err)
	}
	buf.CommitBlock()
	if err := buf.FlushUpTo(2, p); err != nil {
		t.Fatal(err)
	}
	if len(p.split) != 0 || len(p.generic) != 1 {
		t.Fatalf("split=%v generic=%v", p.split, p.generic)
	}
	value, err := p.Get([]byte("state-changeset-v2-kept"))
	if err != nil || string(value) != "kept" {
		t.Fatalf("kept value=%q err=%v", value, err)
	}
}

// The interface wrapper hides only split sizing. Both arms execute the same
// layer publication, coalescing, sorting, two Pebble batches and durable writes.
type genericSplitFlushStore struct{ ethdb.KeyValueStore }

func BenchmarkDirectHistorySplitFlush(b *testing.B) {
	for _, tc := range []struct {
		name         string
		hot, history int
	}{
		{"hot-only", 2048, 0}, {"history-only", 0, 2048},
		{"hot-dominant", 1792, 256}, {"history-dominant", 256, 1792}, {"balanced", 1024, 1024},
	} {
		keys := make([][]byte, 0, tc.hot+tc.history)
		for side, count := range []int{tc.hot, tc.history} {
			prefix := "state-commitment-branch-"
			if side == 1 {
				prefix = "state-changeset-v2-"
			}
			for i := 0; i < count; i++ {
				keys = append(keys, []byte(fmt.Sprintf("%s%08d", prefix, i)))
			}
		}
		values := [4][]byte{}
		for block := range values {
			values[block] = bytes.Repeat([]byte{byte(block + 1)}, 768+block*17)
		}
		for _, split := range []bool{false, true} {
			label := "baseline"
			if split {
				label = "split"
			}
			b.Run(tc.name+"/"+label, func(b *testing.B) {
				hot, err := rawdb.NewPebbleDB(b.TempDir(), 16, 16)
				if err != nil {
					b.Fatal(err)
				}
				history, err := rawdb.NewPebbleDB(b.TempDir(), 16, 16)
				if err != nil {
					b.Fatal(err)
				}
				routed := rawdb.NewDirectHistoryStore(hot, history)
				defer routed.Close()
				var dst ethdb.KeyValueStore = routed
				if !split {
					dst = genericSplitFlushStore{dst}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					buf := New(dst)
					for block, value := range values {
						buf.BeginBlock(bufHash(byte(block+1)), uint64(block+1))
						for i, key := range keys {
							if block == 3 && i%7 == 0 {
								if err := buf.Delete(key); err != nil {
									b.Fatal(err)
								}
							} else if err := buf.Put(key, value); err != nil {
								b.Fatal(err)
							}
						}
						buf.CommitBlock()
					}
					if err := buf.FlushUpTo(4, dst); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// The initial source window fills with overwritten points, then extension
// admits another point layer before reaching a range layer. The range must
// remain a separate group and delete durable keys absent from all point maps.
func TestSplitFlushExtensionStopsBeforeRangeTombstone(t *testing.T) {
	for _, split := range []bool{false, true} {
		t.Run(fmt.Sprint(split), func(t *testing.T) {
			p := &splitFlushProbe{DirectHistoryStore: rawdb.NewDirectHistoryStore(rawdb.NewMemoryDatabase(), rawdb.NewMemoryDatabase())}
			defer p.Close()
			if err := p.Put([]byte("state-unseen"), []byte("old")); err != nil {
				t.Fatal(err)
			}
			var dst ethdb.KeyValueStore = p
			if !split {
				dst = genericSplitFlushStore{dst}
			}
			buf := New(dst)
			value := bytes.Repeat([]byte{0x61}, maxFlushBatchValueSize/2+1)
			for block := 1; block <= 2; block++ {
				buf.BeginBlock(bufHash(byte(block)), uint64(block))
				if err := buf.PutOwnedValue([]byte("state-victim"), value); err != nil {
					t.Fatal(err)
				}
				buf.CommitBlock()
			}
			buf.BeginBlock(bufHash(3), 3)
			if err := buf.DeleteRange([]byte("state"), []byte("state~")); err != nil {
				t.Fatal(err)
			}
			if err := buf.Put([]byte("state-survivor"), []byte("new")); err != nil {
				t.Fatal(err)
			}
			buf.CommitBlock()
			if err := buf.FlushUpTo(3, dst); err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"state-unseen", "state-victim"} {
				if has, err := p.Has([]byte(key)); err != nil || has {
					t.Fatalf("range deleted %q: found=%t err=%v", key, has, err)
				}
			}
			got, err := p.Get([]byte("state-survivor"))
			if err != nil || string(got) != "new" {
				t.Fatalf("survivor=%q err=%v", got, err)
			}
			if split && (len(p.split) != 1 || len(p.generic) != 1) {
				t.Fatalf("range must be separate generic group: split=%v generic=%v", p.split, p.generic)
			}
		})
	}
}
