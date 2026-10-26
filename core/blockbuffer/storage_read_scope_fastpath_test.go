package blockbuffer

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type storageFastpathCursor struct {
	pointread.Cursor
	calls int
}

func (c *storageFastpathCursor) View(key []byte, fn func([]byte) error) (bool, error) {
	c.calls++
	return c.Cursor.View(key, fn)
}

// The memory-hit allocation test needs an immutable empty base, but never
// reads it. Use the public snapshot/cursor capabilities without Pebble's
// background goroutines: AllocsPerRun counts process-wide allocations.
// Actual Pebble cursor ownership and IO remain covered below and by scope
// lifecycle/interleaving tests. Any unexpected durable access fails closed.
type storageFastpathMemoryStore struct{ ethdb.KeyValueStore }

type storageFastpathMemorySnapshot struct{ ethdb.KeyValueStore }

func (s *storageFastpathMemoryStore) NewKeyValueSnapshot() (pointread.KeyValueSnapshot, error) {
	return &storageFastpathMemorySnapshot{KeyValueStore: s.KeyValueStore}, nil
}
func (*storageFastpathMemorySnapshot) Close() error { return nil }
func (*storageFastpathMemorySnapshot) NewPointReadCursor() (pointread.Cursor, error) {
	return storageFastpathUnexpectedCursor{}, nil
}

type storageFastpathUnexpectedCursor struct{}

func (storageFastpathUnexpectedCursor) View([]byte, func([]byte) error) (bool, error) {
	return false, errors.New("memory-hit fixture reached durable cursor")
}
func (storageFastpathUnexpectedCursor) Close() error { return nil }

func TestStorageReadScopeStructuredMemoryHits(t *testing.T) {
	// Both the ordinary stack key and the oversized fallback must preserve
	// active > captured committed > versioned base-cache precedence. A zero
	// length physical value is present; a nil cache value is a cached miss.
	for _, size := range []int{32, splitReadKeyStackSize} {
		for _, source := range []string{"active", "captured", "cache"} {
			t.Run(fmt.Sprintf("%s/%d", source, size), func(t *testing.T) {
				disk := &storageFastpathMemoryStore{KeyValueStore: memorydb.New()}
				defer disk.Close()
				b := New(disk)
				var err error
				b.SetBaseReadCacheSize(1 << 20)
				prefix, owner := []byte("scope-"), common.AccountID{19: 7}
				logical := bytes.Repeat([]byte{0x51}, size)
				physical := appendStateKVLatestKey(nil, prefix, owner, 17, 2, logical)
				b.BeginBlock(bufHash(1), 1)
				if source == "captured" {
					if err := b.Put(physical, []byte("captured")); err != nil {
						t.Fatal(err)
					}
					b.CommitBlock()
					b.BeginBlock(bufHash(2), 2)
				}
				number := uint64(1)
				if source == "captured" {
					number = 2
				}
				s, err := b.TryStorageReadScope(number)
				if err != nil || s == nil {
					t.Fatalf("scope %v/%v", s, err)
				}
				defer s.Close()
				cursor := &storageFastpathCursor{Cursor: s.cursor}
				s.cursor = cursor
				read := func() ([]byte, error) { return s.GetNoCopyCachedStateKVLatest(prefix, owner, 17, 2, logical) }
				for _, want := range [][]byte{[]byte("value"), {}, nil} {
					if source == "cache" {
						c := s.snapshot.view.baseReadCache
						_, _, epoch, _ := c.getAtVersion(physical, s.snapshot.cacheVersion)
						// Keep production two-hit admission; prime probation once.
						c.setSnapshotEntry(physical, want, want == nil, epoch, s.snapshot.cacheVersion)
						if _, ok := c.setSnapshotEntry(physical, want, want == nil, epoch, s.snapshot.cacheVersion); !ok {
							t.Fatal("cache setup rejected")
						}
					} else if source == "captured" {
						l := s.snapshot.view.layers[0]
						v := &LayerView{b: b, l: l}
						if want == nil {
							err = v.Delete(physical)
						} else {
							err = v.Put(physical, want)
						}
						if err != nil {
							t.Fatal(err)
						}
					} else {
						if want == nil {
							err = b.Delete(physical)
						} else {
							err = b.Put(physical, want)
						}
						if err != nil {
							t.Fatal(err)
						}
					}
					check := func() {
						got, err := read()
						if want == nil {
							if err != ErrNotFound {
								t.Fatalf("missing: %q/%v", got, err)
							}
						} else if err != nil || !bytes.Equal(got, want) {
							t.Fatalf("present: %q/%v want %q", got, err, want)
						}
					}
					check()
					if size == 32 {
						if allocs := testing.AllocsPerRun(100, check); allocs != 0 {
							t.Fatalf("memory-hit temporary allocations: %v", allocs)
						}
					}
					logical[0]++
					physical = appendStateKVLatestKey(nil, prefix, owner, 17, 2, logical)
				}
				if cursor.calls != 0 {
					t.Fatalf("memory hits reached durable cursor %d times", cursor.calls)
				}
				// Current writes must override the older tombstone/cache miss.
				logical[0]--
				physical = appendStateKVLatestKey(nil, prefix, owner, 17, 2, logical)
				if err := b.Put(physical, []byte("current")); err != nil {
					t.Fatal(err)
				}
				if got, err := read(); err != nil || string(got) != "current" {
					t.Fatalf("current %q/%v", got, err)
				}
				if err := b.Delete(physical); err != nil {
					t.Fatal(err)
				}
				if _, err := read(); err != ErrNotFound {
					t.Fatalf("current delete: %v", err)
				}
			})
		}
	}
}

func TestStorageReadScopeStructuredDurableOwnership(t *testing.T) {
	for _, size := range []int{32, splitReadKeyStackSize} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			disk, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
			if err != nil {
				t.Fatal(err)
			}
			defer disk.Close()
			prefix, owner := []byte("scope-"), common.AccountID{19: 3}
			logical := bytes.Repeat([]byte{0x11}, size)
			physical := appendStateKVLatestKey(nil, prefix, owner, 23, 2, logical)
			if err := disk.Put(physical, []byte("durable")); err != nil {
				t.Fatal(err)
			}
			b := New(disk)
			b.SetBaseReadCacheSize(1 << 20)
			b.BeginBlock(bufHash(1), 1)
			s, err := b.TryStorageReadScope(1)
			if err != nil || s == nil {
				t.Fatalf("scope %v/%v", s, err)
			}
			defer s.Close()
			got, err := s.GetNoCopyCachedStateKVLatest(prefix, owner, 23, 2, logical)
			if err != nil || string(got) != "durable" {
				t.Fatalf("durable %q/%v", got, err)
			}
			// Reuse the scratch path for a different key and release its value.
			logical[0]++
			if _, err := s.GetNoCopyCachedStateKVLatest(prefix, owner, 23, 2, logical); err != ErrNotFound {
				t.Fatalf("absent %v", err)
			}
			logical[0]--
			if string(got) != "durable" {
				t.Fatalf("borrowed result changed: %q", got)
			}
			if got, err := s.GetNoCopyCachedStateKVLatest(prefix, owner, 23, 2, logical); err != nil || string(got) != "durable" {
				t.Fatalf("reused key %q/%v", got, err)
			}
		})
	}
}
