package pebbledb

import (
	"bytes"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func testSpaceRanges() []DiskSpaceRange {
	return []DiskSpaceRange{
		{Name: "alpha", Start: []byte("a-"), End: []byte("a.")},
		{Name: "zeta", Start: []byte("z-"), End: []byte("z.")},
	}
}

func TestDiskSpaceRangesRejectUnboundedAndCopy(t *testing.T) {
	in := testSpaceRanges()
	out, err := copyDiskSpaceRanges(in)
	if err != nil {
		t.Fatal(err)
	}
	in[0].Name = "changed"
	in[0].Start[0], in[0].End[0] = 'x', 'x'
	if out[0].Name != "alpha" || string(out[0].Start) != "a-" || string(out[0].End) != "a." {
		t.Fatalf("caller mutated copied ranges: %+v", out)
	}
	for _, r := range []DiskSpaceRange{
		{Name: "all"}, {Name: "no_end", Start: []byte("a")},
		{Name: "no_start", End: []byte("z")},
		{Name: "equal", Start: []byte("a"), End: []byte("a")},
		{Name: "reverse", Start: []byte("z"), End: []byte("a")},
		{Name: "unbounded/label", Start: []byte("a"), End: []byte("z")},
	} {
		if _, err := copyDiskSpaceRanges([]DiskSpaceRange{r}); err == nil {
			t.Fatalf("accepted %+v", r)
		}
	}
	if _, err := copyDiskSpaceRanges(append(out, out[0])); err == nil {
		t.Fatal("accepted duplicate name")
	}
	tooMany := make([]DiskSpaceRange, maxDiskSpaceRanges+1)
	for i := range tooMany {
		tooMany[i] = DiskSpaceRange{Name: fmt.Sprintf("range_%d", i), Start: []byte("a"), End: []byte("b")}
	}
	if _, err := copyDiskSpaceRanges(tooMany); err == nil {
		t.Fatal("accepted too many estimates")
	}
}

func TestDiskSpaceObservationRotatesAndPreservesFailedValue(t *testing.T) {
	calls := 0
	o := newDiskSpaceObserver(t.Name()+"/", testSpaceRanges(), func(start, end []byte) (uint64, error) {
		want := testSpaceRanges()[calls%2]
		if !bytes.Equal(start, want.Start) || !bytes.Equal(end, want.End) {
			t.Fatalf("call %d: wrong range %q..%q", calls, start, end)
		}
		calls++
		switch calls {
		case 2:
			return 0, nil // A measured empty range is distinct from unknown.
		case 3:
			return 42, nil
		default:
			return 0, errors.New("estimate failed")
		}
	})
	o.sampleNext()
	a, z := &o.ranges[0], &o.ranges[1]
	if a.known.Snapshot().Value() != 0 || a.sampledAt.Snapshot().Value() != 0 || a.errorCount != 1 {
		t.Fatal("initial failure became a known empty sample")
	}
	o.sampleNext()
	if z.known.Snapshot().Value() != 1 || z.bytes.Snapshot().Value() != 0 {
		t.Fatal("valid empty estimate not published")
	}
	o.sampleNext()
	when := a.sampledAt.Snapshot().Value()
	if when == 0 || a.bytes.Snapshot().Value() != 42 {
		t.Fatal("successful estimate missing")
	}
	o.sampleNext()
	o.sampleNext()
	if a.known.Snapshot().Value() != 1 || a.bytes.Snapshot().Value() != 42 || a.sampledAt.Snapshot().Value() != when {
		t.Fatal("failed estimate replaced previous success")
	}
	if a.attemptCount != 3 || a.errorCount != 2 || z.attemptCount != 2 || z.errorCount != 1 {
		t.Fatal("attempt/error accounting lost during rotation")
	}
	// Reading any cached gauge must never invoke an estimate.
	for i := 0; i < 100; i++ {
		_ = a.bytes.Snapshot().Value()
		_ = a.sampledAt.Snapshot().Value()
	}
	if calls != 5 {
		t.Fatal("scraping caused estimates")
	}
}

func TestDiskSpaceObserverStopJoinsInFlight(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int64
	o := newDiskSpaceObserver(t.Name()+"/", testSpaceRanges(), func(_, _ []byte) (uint64, error) {
		calls.Add(1)
		close(started)
		<-release
		return 1, nil
	})
	o.interval = time.Millisecond
	o.start()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("estimate not started")
	}
	stopped := make(chan struct{})
	go func() { o.stop(); close(stopped) }()
	<-o.stopCh // The shutdown request has been accepted, but IO is in flight.
	select {
	case <-stopped:
		t.Fatal("stop returned before in-flight estimate completed")
	default:
	}
	close(release)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("stop did not join the completed estimate")
	}
	if calls.Load() != 1 || o.inFlight.Snapshot().Value() != 0 || o.enabled.Snapshot().Value() != 0 {
		t.Fatal("shutdown leaked work or started the next range")
	}
}

func TestDiskSpaceObserverWaitsAfterCompletion(t *testing.T) {
	starts := make(chan time.Time, 2)
	release := make(chan struct{})
	var calls atomic.Int64
	o := newDiskSpaceObserver(t.Name()+"/", testSpaceRanges(), func(_, _ []byte) (uint64, error) {
		if calls.Add(1) == 1 {
			starts <- time.Now()
			<-release
		} else {
			select {
			case starts <- time.Now():
			default:
			}
		}
		return 0, nil
	})
	o.interval = 20 * time.Millisecond
	o.start()
	<-starts
	// Let a tick-based implementation accumulate missed work while blocked.
	time.Sleep(2 * o.interval)
	completed := time.Now()
	close(release)
	select {
	case next := <-starts:
		if next.Sub(completed) < o.interval {
			t.Fatal("estimate caught up missed ticks rather than waiting after completion")
		}
	case <-time.After(time.Second):
		t.Fatal("second range did not run")
	}
	o.stop()
}

func TestDatabaseSpaceObserverCloseAndReadOnly(t *testing.T) {
	path := t.TempDir()
	db, err := New(path, 16, 32, t.Name()+"/", false, Options{DiskSpaceRanges: testSpaceRanges()})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 16; i++ {
		if err := db.Put([]byte(fmt.Sprintf("a-%02d", i)), bytes.Repeat([]byte{byte(i)}, 1024)); err != nil {
			t.Fatal(err)
		}
	}
	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("database close deadlocked with observer")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	ro, err := New(path, 16, 32, t.Name()+"/readonly/", true, Options{DiskSpaceRanges: []DiskSpaceRange{{Name: "invalid", Start: nil}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := ro.Close(); err != nil {
			t.Error(err)
		}
	})
	if ro.diskSpace.stopCh != nil || ro.diskSpace.enabled.Snapshot().Value() != 0 {
		t.Fatal("read-only database started observation")
	}
	disabled := newDiskSpaceObserver(t.Name()+"/disabled/", nil, func(_, _ []byte) (uint64, error) {
		t.Fatal("disabled observer called estimator")
		return 0, nil
	})
	disabled.start()
	disabled.stop()
}

func TestDatabaseSpaceMetricsNamespaceOwnership(t *testing.T) {
	namespace := t.Name() + "/"
	first, err := New(t.TempDir(), 16, 32, namespace, false, Options{DiskSpaceRanges: testSpaceRanges()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })
	owner := first.diskSpace.owner.Snapshot().Value()
	if owner <= 0 {
		t.Fatal("first database did not acquire observation")
	}
	for _, enabled := range []bool{false, true} {
		tune := Options{}
		if enabled {
			tune.DiskSpaceRanges = testSpaceRanges()
		}
		second, err := New(t.TempDir(), 16, 32, namespace, false, tune)
		if err != nil {
			t.Fatal(err)
		}
		if second.diskSpace != nil || second.engineSpace != nil {
			t.Fatal("secondary open can overwrite active space metrics")
		}
		if err := second.Close(); err != nil {
			t.Fatal(err)
		}
		if first.diskSpace.enabled.Snapshot().Value() != 1 || first.diskSpace.owner.Snapshot().Value() != owner {
			t.Fatal("secondary construction/close changed active owner")
		}
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(t.TempDir(), 16, 32, namespace, false, Options{DiskSpaceRanges: testSpaceRanges()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	if reopened.diskSpace == nil || reopened.diskSpace.owner.Snapshot().Value() == owner {
		t.Fatal("closed namespace lease was not released to a new owner")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if reopened.diskSpace.enabled.Snapshot().Value() != 1 {
		t.Fatal("old owner's repeated close stopped new owner publication")
	}
}
