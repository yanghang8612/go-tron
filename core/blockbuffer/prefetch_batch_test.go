package blockbuffer

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestPrefetchBatchPebbleOverlayCacheAndExactMissing(t *testing.T) {
	disk, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := disk.Close(); err != nil {
			t.Error(err)
		}
	}()
	for _, key := range []string{"a", "b", "c", "e", "empty"} {
		value := []byte("disk-" + key)
		if key == "empty" {
			value = []byte{}
		}
		if err := disk.Put([]byte(key), value); err != nil {
			t.Fatal(err)
		}
	}
	b := New(disk)
	b.SetBaseReadCacheSize(1 << 20)
	if _, err := b.Prefetch([]byte("c")); err != nil {
		t.Fatal(err)
	}
	b.BeginBlock(bufHash(1), 1)
	if err := b.Put([]byte("a"), []byte("overlay-a")); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete([]byte("b")); err != nil {
		t.Fatal(err)
	}
	b.CommitBlock()
	keys := [][]byte{[]byte("e"), []byte("b"), []byte("d"), []byte("a"), []byte("c"), []byte("e"), []byte("empty")}
	want := []string{"disk-e", "", "", "overlay-a", "disk-c", "disk-e", ""}
	seen := make([]int, len(keys))
	err = b.PrefetchBatch(keys, func(i int, value []byte, present bool, err error) error {
		seen[i]++
		if err != nil || present != (i != 1 && i != 2) || string(value) != want[i] {
			t.Fatalf("row %d = %q/%v/%v", i, value, present, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, n := range seen {
		if n != 1 {
			t.Fatalf("row %d visited %d times", i, n)
		}
	}
	for i, key := range keys {
		value, err := b.GetNoCopyCached(key)
		if i == 1 || i == 2 {
			if !b.IsKeyNotFound(err) {
				t.Fatalf("missing %q: %v", key, err)
			}
			continue
		}
		if err != nil || string(value) != want[i] {
			t.Fatalf("canonical %q: %q/%v", key, value, err)
		}
	}
	stop := errors.New("stop")
	calls := 0
	err = b.PrefetchBatch([][]byte{[]byte("uncached-1"), []byte("uncached-2")}, func(_ int, _ []byte, _ bool, _ error) error { calls++; return stop })
	if !errors.Is(err, stop) || calls != 1 {
		t.Fatalf("cancel = %v/%d", err, calls)
	}
}

type prefetchSnapshotGate struct {
	ethdb.KeyValueStore
	before           bool
	started, release chan struct{}
	lookups          int
}

func (d *prefetchSnapshotGate) NewPointReadSnapshot() (pointread.Snapshot, error) {
	if d.before {
		close(d.started)
		<-d.release
	}
	s, err := d.KeyValueStore.(pointread.Snapshotter).NewPointReadSnapshot()
	if !d.before {
		close(d.started)
		<-d.release
	}
	if err != nil {
		return nil, err
	}
	return &prefetchCountedSnapshot{s, &d.lookups}, nil
}

func TestPrefetchBatchFlushCannotPublishStaleSnapshot(t *testing.T) {
	for _, before := range []bool{false, true} {
		for _, deletion := range []bool{false, true} {
			t.Run(fmt.Sprintf("beforeSnapshot=%t/delete=%t", before, deletion), func(t *testing.T) {
				disk, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := disk.Close(); err != nil {
						t.Error(err)
					}
				}()
				key := prefetchTestStateKey()
				if err := disk.Put(key, []byte("old")); err != nil {
					t.Fatal(err)
				}
				gate := &prefetchSnapshotGate{KeyValueStore: disk, before: before, started: make(chan struct{}), release: make(chan struct{})}
				b := New(gate)
				b.SetBaseReadCacheSize(1 << 20)
				done := make(chan error, 1)
				go func() {
					done <- b.PrefetchBatch([][]byte{key}, func(_ int, value []byte, present bool, err error) error {
						want, wantPresent := "old", true
						if before {
							want, wantPresent = "new", !deletion
							if deletion {
								want = ""
							}
						}
						if err != nil || present != wantPresent || string(value) != want {
							return fmt.Errorf("pinned row = %q/%t/%v, want %q/%t", value, present, err, want, wantPresent)
						}
						return nil
					})
				}()
				<-gate.started
				b.BeginBlock(bufHash(1), 1)
				if deletion {
					err = b.Delete(key)
				} else {
					err = b.Put(key, []byte("new"))
				}
				if err != nil {
					t.Fatal(err)
				}
				b.CommitBlock()
				if err := b.FlushUpTo(1, disk); err != nil {
					t.Fatal(err)
				}
				if !before {
					// Offer a newer cache value to the older pinned snapshot.
					if _, err := b.Prefetch(key); err != nil && (!deletion || !b.IsKeyNotFound(err)) {
						t.Error(err)
					}
				}
				close(gate.release)
				if err := <-done; err != nil {
					t.Fatal(err)
				}
				if gate.lookups != 1 {
					t.Fatalf("concurrent flush issued %d cursor views, want 1", gate.lookups)
				}
				value, err := b.GetNoCopyCached(key)
				if deletion {
					if !b.IsKeyNotFound(err) {
						t.Fatalf("deleted row resurrected: %q/%v", value, err)
					}
				} else if err != nil || string(value) != "new" {
					t.Fatalf("stale fill: %q/%v", value, err)
				}
			})
		}
	}
}

// Compare identical exact-key batches on real Pebble SSTs. Cache admission and
// copies are included; rebuilding the bounded cache between batches is not.
func BenchmarkStatePrefetchBatchPebble(b *testing.B) {
	disk, err := rawdb.NewPebbleDB(b.TempDir(), 16, 16)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := disk.Close(); err != nil {
			b.Error(err)
		}
	}()
	const rows = 32768
	keys := make([][]byte, rows)
	value := bytes.Repeat([]byte{0x73}, 192)
	batch := disk.NewBatch()
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("state-kv-latest-%08x", i))
		if err := batch.Put(keys[i], value); err != nil {
			b.Fatal(err)
		}
	}
	if err := batch.Write(); err != nil {
		b.Fatal(err)
	}
	if err := disk.Compact(nil, nil); err != nil {
		b.Fatal(err)
	}
	for _, size := range []int{128, 512} {
		for _, ordered := range []bool{false, true} {
			b.Run(fmt.Sprintf("rows=%d/batch=%t", size, ordered), func(b *testing.B) {
				buffer := New(disk)
				requests := make([][]byte, size)
				b.ReportAllocs()
				for n := 0; n < b.N; n++ {
					b.StopTimer()
					buffer.SetBaseReadCacheSize(1 << 20)
					for j := range requests {
						requests[j] = keys[(n*size+j*7919)%rows]
					}
					b.StartTimer()
					if ordered {
						err = buffer.PrefetchBatch(requests, func(_ int, _ []byte, present bool, err error) error {
							if !present && err == nil {
								return errors.New("missing benchmark row")
							}
							return err
						})
					} else {
						for _, key := range requests {
							if _, err = buffer.Prefetch(key); err != nil {
								break
							}
						}
					}
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

type prefetchCountedSnapshot struct {
	pointread.Snapshot
	lookups *int
}

func (s *prefetchCountedSnapshot) NewCursor(prefix []byte) (pointread.Cursor, error) {
	cursor, err := s.Snapshot.NewCursor(prefix)
	if err != nil {
		return nil, err
	}
	return &prefetchCountedCursor{cursor, s.lookups}, nil
}

type prefetchCountedCursor struct {
	pointread.Cursor
	lookups *int
}

func (c *prefetchCountedCursor) View(key []byte, fn func([]byte) error) (bool, error) {
	(*c.lookups)++
	return c.Cursor.View(key, fn)
}

func prefetchTestStateKey() []byte {
	return append([]byte("state-code-v1-"), make([]byte, common.HashLength)...)
}

func TestPrefetchBatchRechecksSameSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name                                  string
		value                                 []byte
		fill, busyBefore, busyAfter, stateKey bool
		wantLookups                           int
	}{
		{name: "present", value: []byte("cached"), fill: true, stateKey: true},
		{name: "empty", value: []byte{}, fill: true, stateKey: true},
		{name: "negative", fill: true, stateKey: true},
		{name: "uncached", value: []byte("disk"), stateKey: true, wantLookups: 1},
		{name: "unsupported-domain", value: []byte("disk"), fill: true, wantLookups: 1},
		{name: "busy-at-capture", value: []byte("disk"), fill: true, busyBefore: true, stateKey: true, wantLookups: 1},
		{name: "busy-at-hit", value: []byte("disk"), fill: true, busyAfter: true, stateKey: true, wantLookups: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			disk, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := disk.Close(); err != nil {
					t.Error(err)
				}
			})
			key := []byte("other-prefetch-key")
			if tc.stateKey {
				key = prefetchTestStateKey()
			}
			if tc.value != nil {
				if err := disk.Put(key, tc.value); err != nil {
					t.Fatal(err)
				}
			}
			gate := &prefetchSnapshotGate{KeyValueStore: disk, started: make(chan struct{}), release: make(chan struct{})}
			b := New(gate)
			b.SetBaseReadCacheSize(1 << 20)
			if tc.busyBefore {
				b.flushMu.Lock()
				defer b.flushMu.Unlock()
			}
			stop := errors.New("stop prefetch")
			done := make(chan error, 1)
			calls := 0
			go func() {
				done <- b.PrefetchBatch([][]byte{key}, func(i int, value []byte, present bool, err error) error {
					calls++
					if err != nil || i != 0 || present != (tc.value != nil) || !bytes.Equal(value, tc.value) {
						return fmt.Errorf("row %d = %q/%t/%v", i, value, present, err)
					}
					return stop
				})
			}()
			<-gate.started
			if tc.fill {
				_, err = b.Prefetch(key)
			}
			if tc.busyAfter {
				b.flushMu.Lock()
				defer b.flushMu.Unlock()
			}
			close(gate.release)
			if err != nil && (tc.value != nil || !b.IsKeyNotFound(err)) {
				t.Error(err)
			}
			if err := <-done; !errors.Is(err, stop) || calls != 1 || gate.lookups != tc.wantLookups {
				t.Fatalf("batch = %v, callbacks = %d, cursor views = %d (want %d)", err, calls, gate.lookups, tc.wantLookups)
			}
		})
	}
}
