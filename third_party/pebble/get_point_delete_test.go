package pebble

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/vfs"
)

type pointDeleteCountIter struct {
	internalIterator
	nexts, closes     int
	nextErr, closeErr error
}

func TestPointDeleteCursorAndGetCloseErrors(t *testing.T) {
	for _, cursorMode := range []bool{false, true} {
		t.Run(fmt.Sprint(cursorMode), func(t *testing.T) {
			db, err := Open("close", &Options{FS: vfs.NewMem(), DisableAutomaticCompactions: true})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			}()
			if err := db.Set([]byte("hot"), []byte("old"), NoSync); err != nil {
				t.Fatal(err)
			}
			old := db.NewSnapshot()
			defer old.Close()
			if err := db.Flush(); err != nil {
				t.Fatal(err)
			}
			if err := db.Delete([]byte("hot"), NoSync); err != nil {
				t.Fatal(err)
			}
			if err := db.Flush(); err != nil {
				t.Fatal(err)
			}
			closeErr := errors.New("active tombstone table close")
			newIters := db.newIters
			var opened []*pointDeleteCountIter
			db.newIters = func(ctx context.Context, f *manifest.FileMetadata, o *IterOptions, internal internalIterOpts) (internalIterator, keyspan.FragmentIterator, error) {
				p, r, e := newIters(ctx, f, o, internal)
				if e != nil {
					return p, r, e
				}
				counted := &pointDeleteCountIter{internalIterator: p, closeErr: closeErr}
				opened = append(opened, counted)
				return counted, r, nil
			}
			defer func() { db.newIters = newIters }()
			if cursorMode {
				s := db.NewSnapshot()
				defer s.Close()
				c, e := s.NewGetCursor()
				if e != nil {
					t.Fatal(e)
				}
				if _, e := c.Get([]byte("hot")); e != ErrNotFound {
					t.Fatal(e)
				}
				if e := c.ReleaseValue(); e != nil {
					t.Fatal(e)
				}
				if e := c.Close(); !errors.Is(e, closeErr) {
					t.Fatal("close error lost", e)
				}
				if e := c.Close(); e != nil {
					t.Fatal("error repeated", e)
				}
			} else {
				if _, _, e := db.Get([]byte("hot")); !errors.Is(e, closeErr) {
					t.Fatal("Get close error became miss", e)
				}
			}
			if len(opened) != 1 || opened[0].nexts != 0 || opened[0].closes != 1 {
				t.Fatalf("table opens=%d, resources=%+v", len(opened), opened)
			}
		})
	}
}

func (i *pointDeleteCountIter) Next() (*InternalKey, LazyValue) {
	i.nexts++
	if i.nextErr != nil {
		return nil, LazyValue{}
	}
	return i.internalIterator.Next()
}
func (i *pointDeleteCountIter) Error() error {
	if i.nexts > 0 && i.nextErr != nil {
		return i.nextErr
	}
	return i.internalIterator.Error()
}
func (i *pointDeleteCountIter) Close() error {
	i.closes++
	return errors.Join(i.internalIterator.Close(), i.Error(), i.closeErr)
}

func TestPointDeleteDoesNotAdvanceOlderVersions(t *testing.T) {
	for _, n := range []int{1, 65536} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			db := pointDeleteHistoryDB(t, n, true)
			state := db.loadReadState()
			buf := &getIterAlloc{}
			get := &buf.get
			*get = getIter{logger: db.opts.Logger, comparer: db.opts.Comparer, newIters: db.newIters,
				snapshot: db.mu.versions.visibleSeqNum.Load(), key: []byte("hot"),
				mem: state.memtables, l0: state.current.L0SublevelFiles, version: state.current}
			key, value := get.First()
			if key == nil || key.Kind() != InternalKeyKindDelete {
				t.Fatal("expected real memtable deletion")
			}
			// Any attempt to read an obsolete version is independently observable
			// and erroneous. The visible tombstone is sufficient to answer Get.
			olderErr := errors.New("obsolete Next called")
			closeErr := errors.New("active iterator close failed")
			counted := &pointDeleteCountIter{internalIterator: get.iter, nextErr: olderErr, closeErr: closeErr}
			get.iter = counted
			i := &buf.dbi
			*i = Iterator{ctx: context.Background(), getIterAlloc: buf, iter: get, pointIter: get,
				merge: db.merge, comparer: *db.opts.Comparer, readState: state, iterKey: key, iterValue: value}
			i.findNextEntry(nil)
			valid := i.Valid()
			err := i.Close()
			if valid || counted.nexts != 0 || counted.closes != 1 || !errors.Is(err, closeErr) || errors.Is(err, olderErr) {
				t.Fatalf("valid=%v Next=%d Close=%d error=%v", valid, counted.nexts, counted.closes, err)
			}
		})
	}
}

// Retain N overwritten versions in one real mutable memtable. The normal
// initial 256KiB ramp would otherwise flush/compact away benchmark input.
// Increasing the next arena is fixture setup, not a production option change.
func pointDeleteHistoryDB(tb testing.TB, versions int, deleted bool) *DB {
	tb.Helper()
	db, err := Open("history", &Options{FS: vfs.NewMem(), MemTableSize: 64 << 20, DisableAutomaticCompactions: true})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		if err := db.Close(); err != nil {
			tb.Error(err)
		}
	})
	db.mu.Lock()
	db.mu.mem.nextSize = 64 << 20
	db.mu.Unlock()
	if err := db.Set([]byte("sizing"), []byte("seed"), NoSync); err != nil {
		tb.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		tb.Fatal(err)
	}
	batch := db.NewBatch()
	for n := 0; n < versions; n++ {
		if err := batch.Set([]byte("hot"), []byte("value"), nil); err != nil {
			tb.Fatal(err)
		}
	}
	if deleted {
		err = batch.Delete([]byte("hot"), nil)
	} else {
		err = batch.Set([]byte("hot"), []byte("latest"), nil)
	}
	if err != nil {
		tb.Fatal(err)
	}
	if err = batch.Commit(NoSync); err != nil {
		tb.Fatal(err)
	}
	if err = batch.Close(); err != nil {
		tb.Fatal(err)
	}
	// Verify actual versions, rather than infer retained history from writes.
	db.mu.Lock()
	iter := db.mu.mem.mutable.newIter(nil)
	db.mu.Unlock()
	count := 0
	for k, _ := iter.First(); k != nil; k, _ = iter.Next() {
		if bytes.Equal(k.UserKey, []byte("hot")) {
			count++
		}
	}
	if err := iter.Close(); err != nil {
		tb.Fatal(err)
	}
	if count != versions+1 {
		tb.Fatalf("retained hot versions=%d, want %d", count, versions+1)
	}
	return db
}

func BenchmarkPointDeleteHistory(b *testing.B) {
	for _, versions := range []int{1, 128, 4096, 65536} {
		for _, deleted := range []bool{true, false} {
			b.Run(fmt.Sprintf("versions=%d/deleted=%t", versions, deleted), func(b *testing.B) {
				db := pointDeleteHistoryDB(b, versions, deleted)
				key := []byte("hot")
				b.ReportAllocs()
				b.ReportMetric(float64(versions+1), "versions")
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					value, closer, err := db.Get(key)
					if deleted {
						if err != ErrNotFound {
							b.Fatal(err)
						}
					} else {
						if err != nil || !bytes.Equal(value, []byte("latest")) {
							b.Fatalf("%q/%v", value, err)
						}
						if err := closer.Close(); err != nil {
							b.Fatal(err)
						}
					}
				}
			})
		}
	}
}

func TestPointDeleteViewsAndOrdinaryIteration(t *testing.T) {
	for _, storage := range []string{"mutable", "flushed", "compacted"} {
		for _, kind := range []string{"delete", "single-delete", "delete-sized"} {
			t.Run(storage+"/"+kind, func(t *testing.T) {
				db, err := Open("views", &Options{FS: vfs.NewMem(), FormatMajorVersion: FormatNewest, DisableAutomaticCompactions: true})
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := db.Close(); err != nil {
						t.Error(err)
					}
				}()
				for _, key := range []string{"a", "b", "c", "hot", "m", "z"} {
					if err := db.Set([]byte(key), []byte("base-"+key), NoSync); err != nil {
						t.Fatal(err)
					}
				}
				before := db.NewSnapshot()
				defer before.Close()
				switch kind {
				case "delete":
					err = db.Delete([]byte("hot"), NoSync)
				case "single-delete":
					err = db.SingleDelete([]byte("hot"), NoSync)
				case "delete-sized":
					err = db.DeleteSized([]byte("hot"), uint32(len("base-hot")), NoSync)
				}
				if err != nil {
					t.Fatal(err)
				}
				if err := db.DeleteRange([]byte("b"), []byte("c"), NoSync); err != nil {
					t.Fatal(err)
				}
				if err := db.Delete([]byte("m"), NoSync); err != nil {
					t.Fatal(err)
				}
				if err := db.Merge([]byte("m"), []byte("new"), NoSync); err != nil {
					t.Fatal(err)
				}
				if storage != "mutable" {
					if err := db.Flush(); err != nil {
						t.Fatal(err)
					}
				}
				if storage == "compacted" {
					if err := db.Compact([]byte("a"), []byte("zz"), false); err != nil {
						t.Fatal(err)
					}
				}
				after := db.NewSnapshot()
				defer after.Close()
				// Newer point tombstones are invisible to these snapshot readers.
				if err := db.Delete([]byte("a"), NoSync); err != nil {
					t.Fatal(err)
				}
				check := func(label, key, want string, get func([]byte) ([]byte, io.Closer, error)) {
					t.Helper()
					v, c, e := get([]byte(key))
					if want == "" {
						if e != ErrNotFound {
							t.Fatalf("%s/%s=%q/%v", label, key, v, e)
						}
						return
					}
					if e != nil || string(v) != want {
						t.Fatalf("%s/%s=%q/%v want%q", label, key, v, e, want)
					}
					if e := c.Close(); e != nil {
						t.Fatal(e)
					}
				}
				check("old", "hot", "base-hot", before.Get)
				check("old", "b", "base-b", before.Get)
				check("new", "hot", "", after.Get)
				check("new", "b", "", after.Get)
				check("new", "c", "base-c", after.Get)
				check("new", "a", "base-a", after.Get)
				check("merge-delete", "m", "new", after.Get)
				check("DB", "hot", "", db.Get)
				batch := db.NewIndexedBatch()
				defer batch.Close()
				if err := batch.Set([]byte("hot"), []byte("batch"), nil); err != nil {
					t.Fatal(err)
				}
				check("batch-set", "hot", "batch", batch.Get)
				if err := batch.Delete([]byte("hot"), nil); err != nil {
					t.Fatal(err)
				}
				check("batch-delete", "hot", "", batch.Get)
				cursor, err := after.NewGetCursor()
				if err != nil {
					t.Fatal(err)
				}
				defer cursor.Close()
				for repeat := 0; repeat < 3; repeat++ {
					for _, key := range []string{"hot", "a", "b", "c", "m"} {
						want := map[string]string{"a": "base-a", "c": "base-c", "m": "new"}[key]
						v, e := cursor.Get([]byte(key))
						if want == "" {
							if e != ErrNotFound {
								t.Fatal(key, e)
							}
						} else if e != nil || string(v) != want {
							t.Fatalf("cursor %s=%q/%v", key, v, e)
						}
						if err := cursor.ReleaseValue(); err != nil {
							t.Fatal(err)
						}
					}
				}
				// Independent ordinary forward/reverse iterator oracle must move
				// past deleted keys, unlike the specialized single-key lookup.
				iter, err := after.NewIter(nil)
				if err != nil {
					t.Fatal(err)
				}
				var forward, reverse []string
				for ok := iter.First(); ok; ok = iter.Next() {
					forward = append(forward, string(iter.Key()))
				}
				for ok := iter.Last(); ok; ok = iter.Prev() {
					reverse = append(reverse, string(iter.Key()))
				}
				if err := iter.Close(); err != nil {
					t.Fatal(err)
				}
				if fmt.Sprint(forward) != "[a c m z]" || fmt.Sprint(reverse) != "[z m c a]" {
					t.Fatal(forward, reverse)
				}
			})
		}
	}
}
