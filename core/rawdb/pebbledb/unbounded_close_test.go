package pebbledb

import (
	"bytes"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

var errUnboundedSequentialClose = errors.New("sequential read handle Close")

type unboundedCloseFS struct {
	vfs.FS
	fail                 atomic.Bool
	opened, failedCloses atomic.Int64
	duplicateCloses      atomic.Int64
}
type unboundedCloseFile struct {
	vfs.File
	fs         *unboundedCloseFS
	sequential bool
	closed     atomic.Bool
}

func (fs *unboundedCloseFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	f, err := fs.FS.Open(name, opts...)
	if err != nil {
		return nil, err
	}
	sequential := false
	for _, opt := range opts {
		sequential = sequential || physicalReadSameOpenOption(opt, vfs.SequentialReadsOption)
	}
	if sequential {
		fs.opened.Add(1)
	}
	return &unboundedCloseFile{File: f, fs: fs, sequential: sequential}, nil
}
func (f *unboundedCloseFile) Close() error {
	if f.sequential && f.closed.Swap(true) {
		f.fs.duplicateCloses.Add(1)
	}
	err := f.File.Close()
	if f.sequential && f.fs.fail.Load() {
		f.fs.failedCloses.Add(1)
		return errors.Join(err, errUnboundedSequentialClose)
	}
	return err
}

// Exercise actual SST ReadHandle.Close failures, rather than replacing the
// resolver with a mock. Sequential reads promote the retained table iterator's
// read handle to a separate descriptor; closing that descriptor can fail.
func TestUnboundedGetFreshCleanupErrorAndRetry(t *testing.T) {
	for _, name := range []string{"final-close", "read-and-cleanup-then-retry", "callback-release-cleanup"} {
		readFailure := name == "read-and-cleanup-then-retry"
		combined := name == "callback-release-cleanup"
		t.Run(name, func(t *testing.T) {
			closeFS := &unboundedCloseFS{FS: vfs.NewMem()}
			readFS := &pointGetFailFS{FS: closeFS}
			valueFail, valueCloses := false, 0
			releaseErr, callbackErr := errors.New("merge value close"), errors.New("business callback")
			merger := &pebble.Merger{Name: "unbounded-cleanup-test", Merge: func(key, value []byte) (pebble.ValueMerger, error) {
				m, err := pebble.DefaultMerger.Merge(key, value)
				return &unboundedMerger{ValueMerger: m, failure: &valueFail, calls: &valueCloses, err: releaseErr}, err
			}}
			cache := pebble.NewCache(0)
			p, err := pebble.Open("close", &pebble.Options{FS: readFS, Cache: cache, Merger: merger, Comparer: exactPointComparer, DisableAutomaticCompactions: true,
				Levels: []pebble.LevelOptions{{Compression: pebble.NoCompression}}})
			cache.Unref()
			if err != nil {
				t.Fatal(err)
			}
			d := &Database{db: p, boundedPointGet: true}
			defer d.Close()
			value := bytes.Repeat([]byte{0x73}, 8192)
			for i := 0; i < 512; i++ {
				if err := p.Set([]byte(fmt.Sprintf("k%04d", i)), value, pebble.NoSync); err != nil {
					t.Fatal(err)
				}
			}
			if err := p.Set([]byte("z-fail"), value, pebble.NoSync); err != nil {
				t.Fatal(err)
			}
			if err := p.Flush(); err != nil {
				t.Fatal(err)
			}
			if combined {
				if err := p.Merge([]byte("k0511"), []byte("+merge"), pebble.NoSync); err != nil {
					t.Fatal(err)
				}
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
			opened := closeFS.opened.Load()
			for i := 0; i < 512; i++ {
				found, err := c.View([]byte(fmt.Sprintf("k%04d", i)), func(got []byte) error {
					expected := value
					if combined && i == 511 {
						expected = append(bytes.Clone(value), []byte("+merge")...)
					}
					if !bytes.Equal(got, expected) {
						t.Fatal("sequential value changed")
					}
					return nil
				})
				if !found || err != nil {
					t.Fatalf("sequential read: %t/%v", found, err)
				}
			}
			if closeFS.opened.Load() <= opened {
				t.Fatal("fixture did not open a sequential SST descriptor")
			}
			closed := closeFS.failedCloses.Load()
			closeFS.fail.Store(true)
			if combined {
				valueFail = true
				found, err := c.View([]byte("k0511"), func([]byte) error { return callbackErr })
				if !found || !errors.Is(err, callbackErr) || !errors.Is(err, releaseErr) || !errors.Is(err, errUnboundedSequentialClose) {
					t.Fatalf("callback/release/fresh cleanup join: %t/%v", found, err)
				}
				valueFail = false
				if err := c.Close(); !errors.Is(err, releaseErr) || !errors.Is(err, errUnboundedSequentialClose) || errors.Is(err, callbackErr) {
					t.Fatalf("Close must repeat only engine errors: %v", err)
				}
			} else if !readFailure {
				if err := c.Close(); !errors.Is(err, errUnboundedSequentialClose) {
					t.Fatalf("fresh cleanup failure lost: %v", err)
				}
			} else {
				readFS.fail.Store(true)
				found, err := c.View([]byte("z-fail"), func([]byte) error { t.Fatal("failed read callback"); return nil })
				if found || !errors.Is(err, errPointGetIO) || !errors.Is(err, errUnboundedSequentialClose) {
					t.Fatalf("read/cleanup join: %t/%v", found, err)
				}
				readFS.fail.Store(false)
				closeFS.fail.Store(false)
				if err := p.Set([]byte("k0000"), []byte("future"), pebble.NoSync); err != nil {
					t.Fatal(err)
				}
				if err := p.Flush(); err != nil {
					t.Fatal(err)
				}
				if err := p.Compact([]byte("k"), []byte("zz"), false); err != nil {
					t.Fatal(err)
				}
				found, err = c.View([]byte("k0000"), func(got []byte) error {
					if !bytes.Equal(got, value) {
						t.Fatal("cleanup retry changed snapshot")
					}
					return nil
				})
				if !found || err != nil {
					t.Fatalf("cleanup retry: %t/%v", found, err)
				}
				if err := c.Close(); err != nil {
					t.Fatalf("retry retained old cleanup error: %v", err)
				}
			}
			closeFS.fail.Store(false)
			if closeFS.failedCloses.Load()-closed < 1 || closeFS.duplicateCloses.Load() != 0 {
				t.Fatalf("cleanup failure missing or a descriptor closed twice: failures=%d duplicates=%d", closeFS.failedCloses.Load()-closed, closeFS.duplicateCloses.Load())
			}
			failedCloses := closeFS.failedCloses.Load()
			if err := c.Close(); err != nil {
				t.Fatalf("double Close repeated cleanup: %v", err)
			}
			if closeFS.failedCloses.Load() != failedCloses || closeFS.duplicateCloses.Load() != 0 {
				t.Fatal("double Close repeated descriptor cleanup")
			}
		})
	}
}
