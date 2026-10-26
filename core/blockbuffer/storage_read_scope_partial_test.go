package blockbuffer

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// This oracle is a materialized map of fixture writes, never another buffer,
// snapshot, iterator, or overlay lookup. Has distinguishes empty from absent.
func storagePartialCheck(t *testing.T, label string, reader ethdb.KeyValueReader, want map[string][]byte, keys []string) {
	t.Helper()
	for _, key := range keys {
		expected, present := want[key]
		got, err := reader.Get([]byte(key))
		has, hasErr := reader.Has([]byte(key))
		if hasErr != nil || has != present {
			t.Fatalf("%s %s: Has %v/%v, want %v", label, key, has, hasErr, present)
		}
		if present {
			if err != nil || !bytes.Equal(got, expected) {
				t.Fatalf("%s %s: Get %q/%v, want present %q", label, key, got, err, expected)
			}
		} else if err == nil {
			t.Fatalf("%s %s: Get %q without absence error", label, key, got)
		}
	}
}

func TestStorageReadScopePartialFlushLateOwnedBatch(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		old, next              []byte
		oldPresent, newPresent bool
	}{
		{"missing-to-present", nil, []byte("late-new"), false, true},
		{"present-to-new", []byte("late-old"), []byte("late-new"), true, true},
		{"present-to-deleted", []byte("late-old"), nil, true, false},
		{"empty-to-new", []byte{}, []byte("late-new"), true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
			if err != nil {
				t.Fatal(err)
			}
			// Close is bounded: a leaked snapshot/cursor lease must fail this
			// test instead of blocking the entire package during cleanup.
			t.Cleanup(func() {
				done := make(chan error, 1)
				go func() { done <- db.Close() }()
				select {
				case err := <-done:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(5 * time.Second):
					t.Error("database Close blocked: scope retained a durable lease")
				}
			})
			const prefix = "partial-prefix"
			const late = "partial-late-fill"
			const cached = "partial-cache-warm"
			const activeDelete = "partial-active-delete"
			const empty = "partial-active-empty"
			const absent = "partial-never-present"
			first, second := "partial-suffix-a", "partial-suffix-b"
			// One key per shard makes the failed suffix's durable prefix
			// deterministic, independent of Go map iteration order.
			i, j := layerShardIndexBytes([]byte(first)), layerShardIndexBytes([]byte(second))
			if i == j {
				t.Fatal("fixture suffix keys must use different shards")
			}
			if i > j {
				first, second = second, first
			}
			base := map[string][]byte{
				prefix: []byte("prefix-base"), first: []byte("first-base"), second: []byte("second-base"),
				cached: []byte("warm-base"), activeDelete: []byte("delete-base"),
			}
			if tc.oldPresent {
				base[late] = bytes.Clone(tc.old)
			}
			for key, value := range base {
				if err := db.Put([]byte(key), value); err != nil {
					t.Fatal(err)
				}
			}
			b := New(db)
			b.SetBaseReadCacheSize(1 << 20)
			for i := 0; i < 3; i++ {
				if _, err := b.GetNoCopyCached([]byte(cached)); err != nil {
					t.Fatal(err)
				}
			}
			if _, ok, _ := b.baseReadCache.getWithEpoch([]byte(cached)); !ok {
				t.Fatal("fixture did not prime cache")
			}
			b.BeginBlock(bufHash(1), 1)
			if err := b.Put([]byte(prefix), []byte("prefix-new")); err != nil {
				t.Fatal(err)
			}
			b.CommitBlock()
			b.BeginBlock(bufHash(2), 2)
			for key, value := range map[string][]byte{first: []byte("first-layer"), second: []byte("second-layer")} {
				if err := b.Put([]byte(key), value); err != nil {
					t.Fatal(err)
				}
			}
			batch := b.NewBatch().(*bufferBatch)
			defer batch.Close()
			if err := batch.PutOwnedKeyValue([]byte(first), []byte("first-late-owned")); err != nil {
				t.Fatal(err)
			}
			if err := batch.Delete([]byte(second)); err != nil {
				t.Fatal(err)
			}
			if tc.newPresent {
				err = batch.PutOwnedKeyValue([]byte(late), bytes.Clone(tc.next))
			} else {
				err = batch.Delete([]byte(late))
			}
			if err != nil {
				t.Fatal(err)
			}
			b.CommitBlock()
			b.BeginBlock(bufHash(3), 3)
			for key, value := range map[string][]byte{first: []byte("active-wins"), empty: {}} {
				if err := b.Put([]byte(key), value); err != nil {
					t.Fatal(err)
				}
			}
			if err := b.Delete([]byte(activeDelete)); err != nil {
				t.Fatal(err)
			}
			scope, err := b.TryStorageReadScope(3)
			if err != nil || scope == nil {
				t.Fatalf("scope %v/%v", scope, err)
			}
			defer scope.Close()
			keys := []string{prefix, first, second, cached, activeDelete, empty, absent}
			logical := map[string][]byte{prefix: []byte("prefix-new"), first: []byte("active-wins"), second: []byte("second-layer"), cached: []byte("warm-base"), empty: {}}
			storagePartialCheck(t, "scope before pending publication", scope, logical, keys)
			storagePartialCheck(t, "live before pending publication", b, logical, keys)

			gate := &storageBarrierCursor{Cursor: scope.cursor, entered: make(chan struct{}), release: make(chan struct{})}
			scope.cursor = gate
			type result struct {
				value []byte
				err   error
			}
			readDone := make(chan result, 1)
			go func() { value, err := scope.Get([]byte(late)); readDone <- result{value, err} }()
			released, received := false, false
			defer func() {
				if !released {
					close(gate.release)
				}
				if !received {
					select {
					case <-readDone:
					case <-time.After(5 * time.Second):
						t.Error("snapshot read did not finish after release")
					}
				}
			}()
			select {
			case <-gate.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("snapshot read did not reach barrier")
			}
			forced := errors.New("partial suffix point failure")
			// Reuse the no-Batcher failure fixture, starting at -1 so its
			// failure is Put #3: all of L1, then one L2 point, then failure.
			writer := &failSecondPointFlush{target: db, entered: make(chan struct{}), release: make(chan struct{}), puts: -1, err: forced}
			defer writer.unblock()
			flushDone := make(chan error, 1)
			go func() { flushDone <- b.FlushUpTo(2, writer) }()
			select {
			case <-writer.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("flush did not reach partially written suffix")
			}
			partial := map[string][]byte{prefix: []byte("prefix-new"), first: []byte("first-layer"), second: []byte("second-base"), cached: []byte("warm-base"), activeDelete: []byte("delete-base")}
			storagePartialCheck(t, "actual partial durable prefix", db, partial, keys)
			wait, remaining, err := batch.writeFilteredAttempt(func(target *layer) bool { return target.number <= 2 }, false)
			if err != nil || wait == nil || remaining != 3 {
				t.Fatalf("pending batch preflight wait=%v remaining=%d err=%v", wait != nil, remaining, err)
			}
			storagePartialCheck(t, "pending batch still hidden", b, logical, keys)
			writeDone := make(chan error, 1)
			go func() { _, err := batch.WriteUpTo(2); writeDone <- err }()
			writer.unblock()
			if err := <-flushDone; !errors.Is(err, forced) {
				t.Fatalf("flush error %v", err)
			}
			if err := <-writeDone; err != nil {
				t.Fatalf("late batch publication %v", err)
			}
			if pending := b.PendingBlocks(); len(pending) != 1 || pending[0] != bufHash(2) {
				t.Fatalf("pending after partial failure %v", pending)
			}
			for _, key := range []string{cached, prefix} {
				if _, found, _ := b.baseReadCache.getWithEpoch([]byte(key)); found {
					t.Fatalf("partial failure retained cached %s", key)
				}
			}
			delete(logical, second)
			keys = append(keys, late)
			if tc.newPresent {
				logical[late] = bytes.Clone(tc.next)
			}
			storagePartialCheck(t, "live retained suffix plus active", b, logical, keys)
			if err := b.FlushUpTo(2, db); err != nil {
				t.Fatalf("retry %v", err)
			}
			if pending := b.PendingBlocks(); len(pending) != 0 {
				t.Fatalf("retry retained committed layers %v", pending)
			}
			durable := map[string][]byte{prefix: []byte("prefix-new"), first: []byte("first-late-owned"), cached: []byte("warm-base"), activeDelete: []byte("delete-base")}
			if tc.newPresent {
				durable[late] = bytes.Clone(tc.next)
			}
			storagePartialCheck(t, "durable after retry", db, durable, keys)
			// Only now release the old pinned result: it must not fill the
			// cache cleared by failure or replace the retry's promoted value.
			close(gate.release)
			released = true
			got := <-readDone
			received = true
			if tc.oldPresent {
				if got.err != nil || !bytes.Equal(got.value, tc.old) {
					t.Fatalf("in-flight captured value %q/%v, want %q", got.value, got.err, tc.old)
				}
			} else if !errors.Is(got.err, ErrNotFound) {
				t.Fatalf("in-flight captured miss %q/%v", got.value, got.err)
			}
			storagePartialCheck(t, "scope captured layers after retry", scope, logical, keys)
			for i := 0; i < 3; i++ {
				storagePartialCheck(t, "live after stale fill", b, logical, keys)
				value, err := b.GetNoCopyCached([]byte(late))
				if tc.newPresent {
					if err != nil || !bytes.Equal(value, tc.next) {
						t.Fatalf("cached retry value %q/%v", value, err)
					}
				} else if !errors.Is(err, ErrNotFound) {
					t.Fatalf("cached retry absence %q/%v", value, err)
				}
			}
			if err := scope.Close(); err != nil {
				t.Fatal(err)
			}
			if err := scope.Close(); err != nil {
				t.Fatalf("double close %v", err)
			}
			if _, err := scope.Get([]byte(late)); !errors.Is(err, errStorageReadScope) {
				t.Fatalf("closed scope read %v", err)
			}
		})
	}
}
