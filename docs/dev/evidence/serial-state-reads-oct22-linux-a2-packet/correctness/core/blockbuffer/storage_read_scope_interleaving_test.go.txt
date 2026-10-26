package blockbuffer

import (
	"bytes"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func storageWaitFor(t *testing.T, description string, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatal("timeout: " + description)
		}
		runtime.Gosched()
	}
}

// The real fill first owns the snapshot guard, then waits on the shard. This
// exposes the former check/tag gap deterministically without a production hook.
func TestSnapshotFillPublishSerializesWithVersionAdvance(t *testing.T) {
	c := newBaseReadCache(1 << 20)
	key := []byte("snapshot-guard-key")
	version := c.version.Load()
	_, _, epoch, _ := c.getAtVersion(key, version)
	// Prime normal two-hit admission; the blocked operation must publish a row.
	c.setSnapshotEntry(key, []byte("old"), false, epoch, version)
	shard := &c.shards[baseReadCacheShardIndex(key)]
	shard.mu.Lock()
	locked := true
	defer func() {
		if locked {
			shard.mu.Unlock()
		}
	}()
	filled := make(chan bool, 1)
	go func() { _, ok := c.setSnapshotEntry(key, []byte("old"), false, epoch, version); filled <- ok }()
	storageWaitFor(t, "fill owns snapshot guard", func() bool {
		if c.snapshotFillMu.TryLock() {
			c.snapshotFillMu.Unlock()
			return false
		}
		return true
	})
	advanced := make(chan uint64, 1)
	go func() { advanced <- c.advanceVersion() }()
	storageWaitFor(t, "advance queued behind fill", func() bool {
		if c.snapshotFillMu.TryRLock() {
			c.snapshotFillMu.RUnlock()
			return false
		}
		return true
	})
	if got := c.version.Load(); got != version {
		t.Fatalf("version advanced before publish: %d", got)
	}
	select {
	case <-advanced:
		t.Fatal("advance did not wait for fill")
	default:
	}
	shard.mu.Unlock()
	locked = false
	if !<-filled {
		t.Fatal("guarded fill failed admission")
	}
	if got := <-advanced; got != version+1 {
		t.Fatalf("version %d", got)
	}
	if got, ok, _ := c.getWithEpoch(key); !ok || string(got) != "old" {
		t.Fatalf("published API value %q/%v", got, ok)
	}
	// Refresh belongs to the new generation. An old captured fill may never
	// replace it, including the existing-entry fast path.
	c.setFlushed(string(key), []byte("new"))
	if _, ok := c.setSnapshotEntry(key, []byte("stale"), false, epoch, version); ok {
		t.Fatal("old fill accepted after advance")
	}
	if got, ok, _ := c.getWithEpoch(key); !ok || string(got) != "new" {
		t.Fatalf("live API value %q/%v", got, ok)
	}
}

type storageBarrierCursor struct {
	pointread.Cursor
	entered chan struct{}
	release chan struct{}
	first   bool
}

func (c *storageBarrierCursor) View(key []byte, fn func([]byte) error) (bool, error) {
	if c.first {
		return c.Cursor.View(key, fn)
	}
	c.first = true
	// Preserve the actual snapshot result, but release engine-owned bytes before
	// allowing the concurrent durable mutation. This barrier adds no fake value.
	var owned []byte
	found, err := c.Cursor.View(key, func(v []byte) error { owned = bytes.Clone(v); return nil })
	close(c.entered)
	<-c.release
	if err != nil || !found {
		return found, err
	}
	return true, fn(owned)
}

// Direct durable mutation is a fault-injection component test, not a supported
// canonical write route. It proves old-cut misses/values cannot poison a newer
// live cache once the version boundary has been advanced.
func TestStorageReadScopeLateSnapshotFillKeepsLiveValues(t *testing.T) {
	cases := []struct {
		name                   string
		old, new               []byte
		oldPresent, newPresent bool
	}{
		{"missing-to-present", nil, []byte("new"), false, true},
		{"present-to-new", []byte("old"), []byte("new"), true, true},
		{"present-to-deleted", []byte("old"), nil, true, false},
		{"physical-empty-to-new", []byte{}, []byte("new"), true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			key := []byte("barrier-key")
			if tc.oldPresent {
				if err := db.Put(key, tc.old); err != nil {
					t.Fatal(err)
				}
			}
			b := New(db)
			b.SetBaseReadCacheSize(1 << 20)
			b.BeginBlock(bufHash(1), 1)
			scope, err := b.TryStorageReadScope(1)
			if err != nil || scope == nil {
				t.Fatalf("scope %v/%v", scope, err)
			}
			defer scope.Close()
			gate := &storageBarrierCursor{Cursor: scope.cursor, entered: make(chan struct{}), release: make(chan struct{})}
			scope.cursor = gate
			released := false
			defer func() {
				if !released {
					close(gate.release)
				}
			}()
			type result struct {
				value []byte
				err   error
			}
			done := make(chan result, 1)
			go func() { v, e := scope.Get(key); done <- result{v, e} }()
			select {
			case <-gate.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("snapshot read did not reach barrier")
			}
			if tc.newPresent {
				err = db.Put(key, tc.new)
			} else {
				err = db.Delete(key)
			}
			if err != nil {
				t.Fatal(err)
			}
			b.baseReadCache.advanceVersion()
			check := func(label string, value []byte, err error, want []byte, present bool) {
				t.Helper()
				if present {
					if err != nil || !bytes.Equal(value, want) {
						t.Fatalf("%s %q/%v want present %q", label, value, err, want)
					}
				} else if !errors.Is(err, ErrNotFound) {
					t.Fatalf("%s %q/%v want missing", label, value, err)
				}
			}
			for i := 0; i < 3; i++ {
				v, e := b.GetNoCopyCached(key)
				check("new live before release", v, e, tc.new, tc.newPresent)
			}
			close(gate.release)
			released = true
			got := <-done
			check("captured snapshot", got.value, got.err, tc.old, tc.oldPresent)
			for i := 0; i < 3; i++ {
				v, e := b.GetNoCopyCached(key)
				check("new live after release", v, e, tc.new, tc.newPresent)
			}
			v, e := scope.Get(key)
			check("snapshot bypasses newer cache", v, e, tc.old, tc.oldPresent)
			if present, err := scope.Has(key); err != nil || present != tc.oldPresent {
				t.Fatalf("snapshot Has %v/%v want %v", present, err, tc.oldPresent)
			}
			if present, err := b.Has(key); err != nil || present != tc.newPresent {
				t.Fatalf("live Has %v/%v want %v", present, err, tc.newPresent)
			}
		})
	}
}
