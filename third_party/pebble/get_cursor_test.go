package pebble

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/vfs"
)

type cursorFaultPoint struct {
	internalIterator
	closeErr error
	closed   int
}

func (i *cursorFaultPoint) Close() error { i.closed++; return i.closeErr }

type cursorFaultRange struct {
	keyspan.FragmentIterator
	closeErr error
	closed   int
}

func (i *cursorFaultRange) Close() error { i.closed++; return i.closeErr }

type cursorValueCloser struct {
	err    error
	closed int
}

func (c *cursorValueCloser) Close() error { c.closed++; return c.err }

var _ io.Closer = (*cursorValueCloser)(nil)

func TestGetCursorLeaseBoundAndCleanupErrors(t *testing.T) {
	pointErr := errors.New("point close")
	rangeErr := errors.New("range close")
	var points []*cursorFaultPoint
	var ranges []*cursorFaultRange
	db := &DB{}
	db.newIters = func(context.Context, *manifest.FileMetadata, *IterOptions, internalIterOpts) (internalIterator, keyspan.FragmentIterator, error) {
		p := &cursorFaultPoint{internalIterator: newErrorIter(nil), closeErr: pointErr}
		r := &cursorFaultRange{FragmentIterator: keyspan.NewIter(DefaultComparer.Compare, nil), closeErr: rangeErr}
		points = append(points, p)
		ranges = append(ranges, r)
		return p, r, nil
	}
	c := &GetCursor{db: db, seqNum: 10}
	opts := &IterOptions{snapshotForHideObsoletePoints: 10, level: manifest.Level(1)}
	files := make([]manifest.FileMetadata, 5)
	for i := 0; i < 4; i++ {
		p, r, err := c.newIters(context.Background(), &files[i], opts, internalIterOpts{})
		if err != nil {
			t.Fatal(err)
		}
		if err := p.Close(); err != nil {
			t.Fatal(err)
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
	}
	// Logical Close cannot release an entry's per-Get lease: getIter may still
	// retain the returned Span. All slots remain leased, including a repeat file.
	p, r, err := c.newIters(context.Background(), &files[0], opts, internalIterOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if c.Stats().TableFallbacks != 1 || c.Stats().MaxRetained != 4 || len(points) != 5 {
		t.Fatalf("bounds %+v opens%d", c.Stats(), len(points))
	}
	for i := 0; i < 4; i++ {
		if points[i].closed != 0 || ranges[i].closed != 0 {
			t.Fatal("evicted a live lease")
		}
	}
	if !errors.Is(p.Close(), pointErr) || !errors.Is(r.Close(), rangeErr) {
		t.Fatal("uncached ownership/errors lost")
	}
	err = c.Close()
	if !errors.Is(err, pointErr) || !errors.Is(err, rangeErr) {
		t.Fatalf("fresh cleanup errors lost: %v", err)
	}
	for i := range points {
		if points[i].closed != 1 || ranges[i].closed != 1 {
			t.Fatalf("pair%d closed%d/%d", i, points[i].closed, ranges[i].closed)
		}
	}
	if err := c.Close(); err != nil {
		t.Fatalf("already-reported Close error repeated: %v", err)
	}
}

func TestGetCursorOperationErrorCleansOnceAndStaysPoisoned(t *testing.T) {
	db, err := Open("fault", &Options{FS: vfs.NewMem(), DisableAutomaticCompactions: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := db.Set([]byte("key"), []byte("value"), NoSync); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	s := db.NewSnapshot()
	defer s.Close()
	readErr := errors.New("read operation")
	pointErr := errors.New("new point cleanup")
	rangeErr := errors.New("new range cleanup")
	p := &cursorFaultPoint{internalIterator: newErrorIter(readErr), closeErr: pointErr}
	r := &cursorFaultRange{FragmentIterator: keyspan.NewIter(DefaultComparer.Compare, nil), closeErr: rangeErr}
	original := db.newIters
	db.newIters = func(context.Context, *manifest.FileMetadata, *IterOptions, internalIterOpts) (internalIterator, keyspan.FragmentIterator, error) {
		return p, r, nil
	}
	c, err := s.NewGetCursor()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = c.Get([]byte("key"))
	if !errors.Is(err, readErr) || !errors.Is(err, pointErr) || !errors.Is(err, rangeErr) {
		t.Fatalf("operation cleanup errors lost: %v", err)
	}
	if p.closed != 1 || r.closed != 1 {
		t.Fatal("operation did not cleanup")
	}
	if _, err := c.Get([]byte("key")); !errors.Is(err, readErr) {
		t.Fatalf("poison lost: %v", err)
	}
	if err := c.ReleaseValue(); err != nil {
		t.Fatalf("reported operation repeated: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("reported operation reappeared at Close: %v", err)
	}
	db.newIters = original
	fg, err := s.NewGetCursor()
	if err != nil {
		t.Fatal(err)
	}
	defer fg.Close()
	v, err := fg.Get([]byte("key"))
	if err != nil || string(v) != "value" {
		t.Fatalf("independent retry: %q/%v", v, err)
	}
}

func TestGetCursorValueCloserExactlyOnce(t *testing.T) {
	for _, release := range []bool{false, true} {
		want := errors.New("value closer")
		closer := &cursorValueCloser{err: want}
		c := &GetCursor{active: true, iter: &Iterator{valueCloser: closer}}
		if release {
			if err := c.ReleaseValue(); !errors.Is(err, want) {
				t.Fatal(err)
			}
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := c.Close(); !errors.Is(err, want) {
				t.Fatal(err)
			}
		}
		if closer.closed != 1 {
			t.Fatalf("value closer called%d times", closer.closed)
		}
	}
}

type cursorFinishErrorMerger struct {
	finishErr error
	closer    io.Closer
}

func (m cursorFinishErrorMerger) MergeNewer([]byte) error { return nil }
func (m cursorFinishErrorMerger) MergeOlder([]byte) error { return nil }
func (m cursorFinishErrorMerger) Finish(bool) ([]byte, io.Closer, error) {
	return nil, m.closer, m.finishErr
}

func TestGetCursorInheritsIteratorMergeErrorPrecedence(t *testing.T) {
	primary := errors.New("finish failure")
	secondary := errors.New("secondary merge value close")
	closer := &cursorValueCloser{err: secondary}
	db, err := Open("merge-error", &Options{FS: vfs.NewMem(), Merger: &Merger{Name: "cursor-finish-error", Merge: func([]byte, []byte) (ValueMerger, error) { return cursorFinishErrorMerger{primary, closer}, nil }}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Merge([]byte("k"), []byte("v"), NoSync); err != nil {
		t.Fatal(err)
	}
	s := db.NewSnapshot()
	defer s.Close()
	c, err := s.NewGetCursor()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = c.Get([]byte("k"))
	if !errors.Is(err, primary) || errors.Is(err, secondary) {
		t.Fatalf("upstream error precedence changed: %v", err)
	}
	if closer.closed != 1 {
		t.Fatalf("merge closer called%d times", closer.closed)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("reported error repeated: %v", err)
	}
}
