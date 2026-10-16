package pebbledb

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/tronprotocol/go-tron/core/pointread"
)

func unboundedFaultFixture(t *testing.T, get bool) (*Database, *pebble.DB, pointread.Snapshot, pointread.Cursor, *pointGetFailFS) {
	t.Helper()
	fs := &pointGetFailFS{FS: vfs.NewMem(), readErr: errors.Join(pebble.ErrNotFound, errPointGetIO)}
	cache := pebble.NewCache(0)
	p, err := pebble.Open("unbounded", &pebble.Options{FS: fs, Cache: cache, Comparer: exactPointComparer, DisableAutomaticCompactions: true})
	cache.Unref()
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"a", "b"} {
		if err := p.Set([]byte(key), bytes.Repeat([]byte(key), 8192), pebble.NoSync); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Flush(); err != nil {
		t.Fatal(err)
	}
	// Seed reader/index metadata, but leave data uncached and fail only reads
	// performed by the cursor under test.
	for _, key := range []string{"a", "b"} {
		_, closer, err := p.Get([]byte(key))
		if err != nil {
			t.Fatal(err)
		}
		if err := closer.Close(); err != nil {
			t.Fatal(err)
		}
	}
	d := &Database{db: p, boundedPointGet: get}
	s, err := d.NewPointReadSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	c, err := s.NewCursor(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { fs.fail.Store(false); _ = c.Close(); _ = s.Close(); _ = d.Close() })
	return d, p, s, c, fs
}

// The old iterator and new Get strategy must agree on last-error Close and on
// clearing that error at the next positioning attempt, including a miss.
func TestUnboundedGetReadFailureRetryAndClose(t *testing.T) {
	for _, get := range []bool{false, true} {
		for _, next := range []string{"close", "miss", "old-value"} {
			name := "iterator/"
			if get {
				name = "get/"
			}
			t.Run(name+next, func(t *testing.T) {
				_, p, _, c, fs := unboundedFaultFixture(t, get)
				fs.fail.Store(true)
				called := false
				found, err := c.View([]byte("a"), func([]byte) error { called = true; return nil })
				if found || called || !errors.Is(err, errPointGetIO) || !errors.Is(err, pebble.ErrNotFound) {
					t.Fatalf("failed read: %t/%t/%v", found, called, err)
				}
				fs.fail.Store(false)
				if next == "close" {
					if err := c.Close(); !errors.Is(err, errPointGetIO) {
						t.Fatalf("Close lost last error: %v", err)
					}
				} else {
					if next == "miss" {
						if found, err := c.View([]byte("absent"), func([]byte) error { t.Fatal("miss callback"); return nil }); found || err != nil {
							t.Fatalf("miss retry: %t/%v", found, err)
						}
					} else {
						if err := p.Set([]byte("a"), []byte("future"), pebble.NoSync); err != nil {
							t.Fatal(err)
						}
						if err := p.Flush(); err != nil {
							t.Fatal(err)
						}
						if err := p.Compact([]byte("a"), []byte("z"), false); err != nil {
							t.Fatal(err)
						}
						found, err := c.View([]byte("a"), func(value []byte) error {
							if !bytes.Equal(value, bytes.Repeat([]byte("a"), 8192)) {
								t.Fatal("retry changed snapshot cut")
							}
							return nil
						})
						if !found || err != nil {
							t.Fatalf("old snapshot retry: %t/%v", found, err)
						}
					}
					if err := c.Close(); err != nil {
						t.Fatalf("retry did not clear last error: %v", err)
					}
				}
				if err := c.Close(); err != nil {
					t.Fatalf("double Close repeated error: %v", err)
				}
			})
		}
	}
}

func TestUnboundedGetCallbackAndMetrics(t *testing.T) {
	_, _, _, c, fs := unboundedFaultFixture(t, true)
	wrapper := c.(*pointReadCursor)
	logical := statePrefetchPointGetMetrics.cursors.Snapshot().Count()
	inners := statePrefetchGetCursorInners.Snapshot().Count()
	bounded := commitmentPointGetMetrics.cursors.Snapshot().Count()
	legacyIterator := statePrefetchPointReadMetrics.cursors.Snapshot().Count()
	fs.fail.Store(true)
	if _, err := c.View([]byte("a"), func([]byte) error { t.Fatal("failed callback"); return nil }); !errors.Is(err, errPointGetIO) {
		t.Fatal(err)
	}
	if wrapper.getCursor != nil || statePrefetchGetCursorInners.Snapshot().Count()-inners != 1 {
		t.Fatal("failed inner was not detached/published once")
	}
	if statePrefetchPointGetMetrics.cursors.Snapshot().Count() != logical {
		t.Fatal("discard published a logical cursor")
	}
	fs.fail.Store(false)
	if _, err := c.View([]byte("a"), func([]byte) error { return nil }); err != nil {
		t.Fatal(err)
	}
	inner := wrapper.getCursor
	callbackErr := errors.New("consumer error")
	if found, err := c.View([]byte("a"), func([]byte) error { return callbackErr }); !found || err != callbackErr {
		t.Fatalf("callback error: %t/%v", found, err)
	}
	if wrapper.getCursor != inner {
		t.Fatal("business error discarded an engine cursor")
	}
	panicValue := errors.New("consumer panic")
	func() {
		defer func() {
			if got := recover(); got != panicValue {
				t.Fatalf("callback panic changed: %v", got)
			}
		}()
		_, _ = c.View([]byte("b"), func([]byte) error { panic(panicValue) })
	}()
	if wrapper.getCursor != inner {
		t.Fatal("consumer panic discarded a healthy cursor")
	}
	if found, err := c.View([]byte("b"), func(value []byte) error {
		if len(value) != 8192 {
			t.Fatal("invalid value after panic")
		}
		return nil
	}); !found || err != nil {
		t.Fatalf("post-panic retry: %t/%v", found, err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if statePrefetchGetCursorInners.Snapshot().Count()-inners != 2 || statePrefetchPointGetMetrics.cursors.Snapshot().Count()-logical != 1 {
		t.Fatal("inner/wrapper close counted more than once")
	}
	if commitmentPointGetMetrics.cursors.Snapshot().Count() != bounded {
		t.Fatal("state prefetch published commitment counters")
	}
	if statePrefetchPointReadMetrics.cursors.Snapshot().Count() != legacyIterator {
		t.Fatal("Get published synthetic iterator samples")
	}
}

type unboundedMergeCloser struct {
	value   []byte
	failure *bool
	calls   *int
	err     error
}

func (c *unboundedMergeCloser) Close() error {
	*c.calls++
	clear(c.value)
	if *c.failure {
		return c.err
	}
	return nil
}

type unboundedMerger struct {
	pebble.ValueMerger
	failure *bool
	calls   *int
	err     error
}

func (m *unboundedMerger) Finish(base bool) ([]byte, io.Closer, error) {
	value, closer, err := m.ValueMerger.Finish(base)
	if closer != nil || err != nil {
		return value, closer, err
	}
	return value, &unboundedMergeCloser{value: value, failure: m.failure, calls: m.calls, err: m.err}, nil
}

func TestUnboundedGetReleaseErrorPreservesCallbackAndRetry(t *testing.T) {
	for _, closeOnly := range []bool{false, true} {
		name := "retry"
		if closeOnly {
			name = "close"
		}
		t.Run(name, func(t *testing.T) {
			fail := false
			calls := 0
			releaseErr, callbackErr := errors.New("merge value close"), errors.New("business callback")
			merger := &pebble.Merger{Name: "unbounded-release-test", Merge: func(key, value []byte) (pebble.ValueMerger, error) {
				m, err := pebble.DefaultMerger.Merge(key, value)
				return &unboundedMerger{ValueMerger: m, failure: &fail, calls: &calls, err: releaseErr}, err
			}}
			p, err := pebble.Open("merge", &pebble.Options{FS: vfs.NewMem(), Merger: merger, Comparer: exactPointComparer, DisableAutomaticCompactions: true})
			if err != nil {
				t.Fatal(err)
			}
			d := &Database{db: p, boundedPointGet: true}
			defer d.Close()
			if err := p.Set([]byte("key"), []byte("base"), pebble.NoSync); err != nil {
				t.Fatal(err)
			}
			if err := p.Merge([]byte("key"), []byte("+merge"), pebble.NoSync); err != nil {
				t.Fatal(err)
			}
			s, err := d.NewPointReadSnapshot()
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			c, err := s.NewCursor(nil)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			fail = true
			var borrowed []byte
			found, err := c.View([]byte("key"), func(value []byte) error {
				if string(value) != "base+merge" || calls != 0 {
					t.Fatal("merge value was released before callback")
				}
				borrowed = value
				return callbackErr
			})
			if !found || !errors.Is(err, releaseErr) || !errors.Is(err, callbackErr) || calls != 1 || !bytes.Equal(borrowed, make([]byte, len(borrowed))) {
				t.Fatalf("release error/lifetime: %t/%v calls=%d", found, err, calls)
			}
			if c.(*pointReadCursor).getCursor != nil {
				t.Fatal("release failure retained poisoned inner")
			}
			if closeOnly {
				if err := c.Close(); !errors.Is(err, releaseErr) || errors.Is(err, callbackErr) {
					t.Fatalf("Close must repeat only the engine error: %v", err)
				}
				if err := c.Close(); err != nil {
					t.Fatalf("double Close: %v", err)
				}
				return
			}
			fail = false
			found, err = c.View([]byte("key"), func(value []byte) error {
				if string(value) != "base+merge" {
					t.Fatal("release retry changed value")
				}
				return nil
			})
			if !found || err != nil || calls != 2 {
				t.Fatalf("release retry: %t/%v calls=%d", found, err, calls)
			}
			if err := c.Close(); err != nil {
				t.Fatalf("old release error repeated after retry: %v", err)
			}
		})
	}
}
