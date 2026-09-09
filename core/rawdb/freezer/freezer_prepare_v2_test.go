package freezer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestV2PreparedReaderOrdersParallelResultsAndJoins(t *testing.T) {
	ctx := context.Background()
	var active atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	r := newV2PreparedReader(ctx, 0, 300, 4, func(n uint64) ([]byte, []byte, error) {
		return []byte{byte(n)}, nil, nil
	}, func(n uint64, data, _ []byte) ([]byte, error) {
		active.Add(1)
		defer active.Add(-1)
		if n == 0 {
			close(started)
			<-release
		}
		if n == 1 {
			<-started
			close(release)
		}
		return data, nil
	})
	defer r.Close()
	for n := uint64(0); n < 300; n++ {
		got, err := r.Read(n)
		if err != nil || !bytes.Equal(got, []byte{byte(n)}) {
			t.Fatalf("record %d: %x / %v", n, got, err)
		}
	}
	if active.Load() != 0 {
		t.Fatal("transform outlived final Read")
	}
	if _, err := r.Read(300); err == nil {
		t.Fatal("accepted read beyond end")
	}
}

func TestV2PreparedReaderBoundsRowsAndOversizedInput(t *testing.T) {
	for _, mode := range []string{"rows", "capacity"} {
		t.Run(mode, func(t *testing.T) {
			var reads atomic.Int32
			blocked := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			r := newV2PreparedReader(context.Background(), 0, 300, 4, func(n uint64) ([]byte, []byte, error) {
				reads.Add(1)
				data := []byte{byte(n)}
				if mode == "capacity" {
					data = make([]byte, 1, v2PreparationBatchBytes/2+1)
					data[0] = byte(n)
					if n == 1 {
						close(blocked) // The one carry exceeds the remaining budget.
					}
				}
				return data, nil, nil
			}, func(n uint64, data, _ []byte) ([]byte, error) {
				if n == 0 {
					<-release
				}
				if mode == "rows" && n == v2PreparationBatchRecords-1 {
					close(blocked)
				}
				return data, nil
			})
			defer func() { unblock(); r.Close() }()
			done := make(chan error, 1)
			go func() { _, err := r.Read(0); done <- err }()
			waitPreparationSignal(t, blocked)
			r.mu.Lock()
			rows, owned := r.ownedRows, r.ownedBytes
			r.mu.Unlock()
			if mode == "rows" && (rows != v2PreparationBatchRecords || reads.Load() != v2PreparationBatchRecords) {
				t.Fatalf("row budget: owned=%d reads=%d", rows, reads.Load())
			}
			if mode == "capacity" && (rows != 1 || owned != v2PreparationBatchBytes/2+1 || reads.Load() != 2) {
				t.Fatalf("allocation budget: rows=%d bytes=%d reads=%d", rows, owned, reads.Load())
			}
			unblock()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			// Close under backpressure, without consuming the remaining 299 rows.
			r.Close()
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.ownedRows != 0 || r.ownedBytes != 0 {
				t.Fatalf("Close retained inputs: %d/%d", r.ownedRows, r.ownedBytes)
			}
		})
	}
}

func waitPreparationSignal(t *testing.T, ready <-chan struct{}) {
	t.Helper()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("preparation phase did not start")
	}
}

func TestV2PreparedReaderFailureAndCancellationJoin(t *testing.T) {
	wantErr := errors.New("injected preparation failure")
	for _, mode := range []string{"source", "transform", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var active atomic.Int32
			r := newV2PreparedReader(ctx, 0, 256, 4, func(n uint64) ([]byte, []byte, error) {
				if mode == "source" && n == 5 {
					return nil, nil, wantErr
				}
				return []byte{byte(n)}, nil, nil
			}, func(n uint64, data, _ []byte) ([]byte, error) {
				active.Add(1)
				defer active.Add(-1)
				if n == 5 {
					if mode == "cancel" {
						cancel()
					}
					return nil, wantErr
				}
				return data, nil
			})
			var err error
			for n := uint64(0); n < 256; n++ {
				if _, err = r.Read(n); err != nil {
					break
				}
			}
			if err == nil || (mode == "cancel" && !errors.Is(err, context.Canceled)) || (mode != "cancel" && !errors.Is(err, wantErr)) {
				t.Fatalf("lost %s failure: %v", mode, err)
			}
			if active.Load() != 0 {
				t.Fatal("worker survived failure")
			}
		})
	}
}

func TestV2PreparationCancellationWaitsForInFlightTransform(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	blocked := make(chan struct{})
	release := make(chan struct{})
	r := newV2PreparedReader(ctx, 0, 2, 2, func(n uint64) ([]byte, []byte, error) {
		return []byte{byte(n)}, nil, nil
	}, func(n uint64, data, _ []byte) ([]byte, error) {
		if n == 0 {
			close(blocked)
			<-release
		} else {
			<-blocked
			cancel()
		}
		return data, nil
	})
	done := make(chan error, 1)
	go func() { _, err := r.Read(0); done <- err }()
	<-ctx.Done()
	select {
	case err := <-done:
		close(release)
		t.Fatalf("Read returned before its transform exited: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
}

func TestV2PreparationMigrationProtectsBorrowedSourcesAndAllRows(t *testing.T) {
	for _, workers := range []int{1, 4} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			store, err := NewFreezer(t.TempDir(), "", false, 2049, map[string]TableConfig{
				"bodies": {Prunable: true}, "tx_infos": {Prunable: true}, "state_roots": {Prunable: true},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			borrowed := make([]byte, 128)
			opts := V2MigrationOptions{Tables: []string{"bodies", "tx_infos", "state_roots"}, SegmentBlocks: 256, FrameBlocks: 8,
				MaxSegments: 1, Online: true, SourceHead: 256, PreparationWorkers: workers,
				Source: func(kind string, n uint64) ([]byte, error) {
					for i := range borrowed {
						borrowed[i] = byte(n)
					}
					borrowed[0] = kind[0]
					return borrowed, nil
				}, Transform: func(kind string, n uint64, data, body []byte) ([]byte, error) {
					if data[0] != kind[0] || data[1] != byte(n) || (kind == "tx_infos" && (body[0] != 'b' || body[1] != byte(n))) {
						return nil, fmt.Errorf("borrowed buffer changed for %s[%d]", kind, n)
					}
					return bytes.Clone(data), nil
				}}
			if _, err := store.MigrateV2(opts); err != nil {
				t.Fatal(err)
			}
			for _, kind := range opts.Tables {
				for n := uint64(0); n < 256; n++ {
					got, err := store.Ancient(kind, n)
					want := bytes.Repeat([]byte{byte(n)}, 128)
					want[0] = kind[0]
					if err != nil || !bytes.Equal(got, want) {
						t.Fatalf("%s[%d]: %v", kind, n, err)
					}
				}
			}
		})
	}
}

func TestV2PreparationOverlapsSerialSourceWithTransform(t *testing.T) {
	loadedNext := make(chan struct{})
	preparedFirst := make(chan struct{})
	var sourceActive, transformActive atomic.Int32
	r := newV2PreparedReader(context.Background(), 0, 4, 4, func(n uint64) ([]byte, []byte, error) {
		if sourceActive.Add(1) != 1 {
			return nil, nil, errors.New("concurrent Source calls")
		}
		defer sourceActive.Add(-1)
		if n == 1 {
			close(loadedNext)
			select {
			case <-preparedFirst:
			case <-time.After(5 * time.Second):
				return nil, nil, errors.New("source did not overlap preparation")
			}
		}
		return []byte{byte(n)}, nil, nil
	}, func(n uint64, data, _ []byte) ([]byte, error) {
		transformActive.Add(1)
		defer transformActive.Add(-1)
		if n == 0 {
			select {
			case <-loadedNext:
			case <-time.After(5 * time.Second):
				return nil, errors.New("preparation did not overlap source")
			}
			close(preparedFirst)
		}
		return data, nil
	})
	defer r.Close()
	for n := uint64(0); n < 4; n++ {
		if got, err := r.Read(n); err != nil || !bytes.Equal(got, []byte{byte(n)}) {
			t.Fatalf("read %d: %x / %v", n, got, err)
		}
	}
	if sourceActive.Load() != 0 || transformActive.Load() != 0 {
		t.Fatal("final Read left work running")
	}
}

func TestV2PreparationOversizedInputRunsAlone(t *testing.T) {
	var reads atomic.Int32
	firstRelease, giantRelease := make(chan struct{}), make(chan struct{})
	carryLoaded, giantStarted, thirdLoaded := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var firstOnce, giantOnce sync.Once
	unblock := func() { firstOnce.Do(func() { close(firstRelease) }); giantOnce.Do(func() { close(giantRelease) }) }
	r := newV2PreparedReader(context.Background(), 0, 3, 4, func(n uint64) ([]byte, []byte, error) {
		reads.Add(1)
		data := []byte{byte(n)}
		if n == 1 {
			data = make([]byte, 1, v2PreparationBatchBytes+1)
			data[0] = byte(n)
			close(carryLoaded)
		}
		if n == 2 {
			close(thirdLoaded)
		}
		return data, nil, nil
	}, func(n uint64, data, _ []byte) ([]byte, error) {
		if n == 0 {
			<-firstRelease
		}
		if n == 1 {
			close(giantStarted)
			<-giantRelease
		}
		return data[:1], nil
	})
	defer func() { unblock(); r.Close() }()
	firstDone := make(chan error, 1)
	go func() { _, err := r.Read(0); firstDone <- err }()
	waitPreparationSignal(t, carryLoaded)
	select {
	case <-giantStarted:
		t.Fatal("oversized transform overlapped the preceding input")
	default:
	}
	firstOnce.Do(func() { close(firstRelease) })
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	waitPreparationSignal(t, giantStarted)
	r.mu.Lock()
	rows, owned := r.ownedRows, r.ownedBytes
	r.mu.Unlock()
	if rows != 1 || owned != v2PreparationBatchBytes+1 {
		t.Fatalf("oversized input was not exclusive: %d/%d", rows, owned)
	}
	select {
	case <-thirdLoaded:
		t.Fatal("source read ahead while an oversized input was active")
	case <-time.After(20 * time.Millisecond):
	}
	giantOnce.Do(func() { close(giantRelease) })
	for n := uint64(1); n < 3; n++ {
		if got, err := r.Read(n); err != nil || !bytes.Equal(got, []byte{byte(n)}) {
			t.Fatalf("read %d: %x / %v", n, got, err)
		}
	}
	if reads.Load() != 3 {
		t.Fatalf("reads=%d", reads.Load())
	}
}

func TestV2PreparationReturnsEarliestOrderedFailure(t *testing.T) {
	early, late := errors.New("early transform failure"), errors.New("later source failure")
	laterLoaded := make(chan struct{})
	r := newV2PreparedReader(context.Background(), 0, 20, 4, func(n uint64) ([]byte, []byte, error) {
		if n == 5 {
			close(laterLoaded)
			return nil, nil, late
		}
		return []byte{byte(n)}, nil, nil
	}, func(n uint64, data, _ []byte) ([]byte, error) {
		if n == 2 {
			<-laterLoaded
			return nil, early
		}
		return data, nil
	})
	defer r.Close()
	for n := uint64(0); n < 2; n++ {
		if _, err := r.Read(n); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.Read(2); !errors.Is(err, early) {
		t.Fatalf("earliest source-order error lost: %v", err)
	}
}

func TestV2PreparationCloseJoinsInFlightSource(t *testing.T) {
	blocked, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	var active atomic.Int32
	wantErr := errors.New("first transform failed")
	r := newV2PreparedReader(context.Background(), 0, 10, 4, func(n uint64) ([]byte, []byte, error) {
		active.Add(1)
		defer active.Add(-1)
		if n == 1 {
			close(blocked)
			<-release
		}
		return []byte{byte(n)}, nil, nil
	}, func(n uint64, data, _ []byte) ([]byte, error) {
		if n == 0 {
			<-blocked
			return nil, wantErr
		}
		return data, nil
	})
	defer func() { unblock(); r.Close() }()
	done := make(chan error, 1)
	go func() { _, err := r.Read(0); done <- err }()
	waitPreparationSignal(t, blocked)
	select {
	case err := <-done:
		t.Fatalf("error returned while Source was running: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	unblock()
	if err := <-done; !errors.Is(err, wantErr) || active.Load() != 0 {
		t.Fatalf("source survived failed Read: err=%v active=%d", err, active.Load())
	}
}

func TestV2PreparationMigrationJoinsAfterWriterCallbackError(t *testing.T) {
	store, err := NewFreezer(t.TempDir(), "", false, 2049, map[string]TableConfig{
		"bodies": {Prunable: true}, "tx_infos": {Prunable: true}, "state_roots": {Prunable: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	blocked, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var zeros, sourceActive, transformActive atomic.Int32
	wantErr := errors.New("writer transaction-index callback failed")
	opts := V2MigrationOptions{
		Tables: []string{"bodies", "tx_infos", "state_roots"}, SegmentBlocks: 256, FrameBlocks: 8,
		MaxSegments: 1, Online: true, SourceHead: 256, PreparationWorkers: 4,
		Source: func(kind string, n uint64) ([]byte, error) {
			if sourceActive.Add(1) != 1 {
				sourceActive.Add(-1)
				return nil, errors.New("dictionary/source calls overlapped")
			}
			defer sourceActive.Add(-1)
			if kind == "bodies" && n == 0 {
				zeros.Add(1) // First is dictionary sampling, second is the writer.
			}
			if kind == "bodies" && n == 1 && zeros.Load() > 1 {
				close(blocked)
				<-release
			}
			return bytes.Repeat([]byte{byte(n)}, 128), nil
		},
		Transform: func(kind string, n uint64, data, _ []byte) ([]byte, error) {
			transformActive.Add(1)
			defer transformActive.Add(-1)
			if kind == "bodies" && n == 0 && zeros.Load() > 1 {
				<-blocked
			}
			return data, nil
		},
		TransactionIndexEntries: func(uint64, []byte) ([]TransactionIndexEntry, error) { return nil, wantErr },
	}
	done := make(chan error, 1)
	go func() { _, err := store.MigrateV2(opts); done <- err }()
	waitPreparationSignal(t, blocked)
	select {
	case err := <-done:
		t.Fatalf("MigrateV2 returned while Source was running: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	unblock()
	if err := <-done; !errors.Is(err, wantErr) || sourceActive.Load() != 0 || transformActive.Load() != 0 {
		t.Fatalf("writer error lifecycle: %v source=%d prepare=%d", err, sourceActive.Load(), transformActive.Load())
	}
	if store.V2Coverage() != 0 {
		t.Fatal("failed writer published coverage")
	}
	// Retry the same range after the joined failed attempt. Immutable rows must
	// still be complete, with no stale worker touching the retry's sources.
	opts.Source = func(kind string, n uint64) ([]byte, error) { return bytes.Repeat([]byte{byte(n)}, 128), nil }
	opts.Transform = func(_ string, _ uint64, data, _ []byte) ([]byte, error) { return data, nil }
	opts.TransactionIndexEntries = nil
	if _, err := store.MigrateV2(opts); err != nil {
		t.Fatalf("retry after writer error: %v", err)
	}
	for _, kind := range opts.Tables {
		for n := uint64(0); n < 256; n++ {
			got, err := store.Ancient(kind, n)
			if err != nil || !bytes.Equal(got, bytes.Repeat([]byte{byte(n)}, 128)) {
				t.Fatalf("retry %s[%d]: %v", kind, n, err)
			}
		}
	}
}

func TestV2PreparationDictionaryCompletesBeforeSourceStarts(t *testing.T) {
	const count = uint64(256)
	var samples atomic.Int32
	load := func(n uint64) ([]byte, []byte, error) {
		if got := samples.Load(); got != int32(count) {
			return nil, nil, fmt.Errorf("producer started with %d/%d dictionary samples", got, count)
		}
		return bytes.Repeat([]byte{byte(n)}, 128), nil, nil
	}
	prepared := newV2PreparedReader(context.Background(), 0, count, 4, load,
		func(_ uint64, data, _ []byte) ([]byte, error) { return data, nil })
	defer prepared.Close()
	dictionaryRead := func(n uint64) ([]byte, error) {
		samples.Add(1)
		return bytes.Repeat([]byte{byte(n)}, 128), nil
	}
	if samples.Load() != 0 {
		t.Fatal("constructor started source work")
	}
	path := t.TempDir() + "/bodies.gtv2"
	if err := writeV2TableSegmentWithWorkers(path, "bodies", 0, count, 8, prepared.Read, dictionaryRead, 4); err != nil {
		t.Fatal(err)
	}
	if samples.Load() != int32(count) {
		t.Fatalf("dictionary samples=%d", samples.Load())
	}
}

func TestV2PreparationCloseBeforeReadDoesNotStartSource(t *testing.T) {
	r := newV2PreparedReader(context.Background(), 0, 10, 4,
		func(uint64) ([]byte, []byte, error) { t.Error("Source after Close"); return nil, nil, nil },
		func(uint64, []byte, []byte) ([]byte, error) { t.Error("Transform after Close"); return nil, nil })
	r.Close()
	r.Close()
	if _, err := r.Read(0); !errors.Is(err, context.Canceled) {
		t.Fatalf("read closed reader: %v", err)
	}
}

func TestV2PreparationConcurrentCloseAndReadJoin(t *testing.T) {
	blocked, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	r := newV2PreparedReader(context.Background(), 0, 256, 4,
		func(n uint64) ([]byte, []byte, error) { return []byte{byte(n)}, nil, nil },
		func(n uint64, data, _ []byte) ([]byte, error) {
			if n == 0 {
				close(blocked)
				<-release
			}
			return data, nil
		})
	defer func() { unblock(); r.Close() }()
	readDone := make(chan error, 1)
	go func() { _, err := r.Read(0); readDone <- err }()
	waitPreparationSignal(t, blocked)
	closeDone := make(chan struct{}, 8)
	for range cap(closeDone) {
		go func() { r.Close(); closeDone <- struct{}{} }()
	}
	waitPreparationSignal(t, r.ctx.Done())
	select {
	case <-closeDone:
		t.Fatal("Close returned before the active transform exited")
	case <-time.After(20 * time.Millisecond):
	}
	unblock()
	for range cap(closeDone) {
		waitPreparationSignal(t, closeDone)
	}
	if err := <-readDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("concurrent Read: %v", err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ownedRows != 0 || r.ownedBytes != 0 || len(r.free) != v2PreparationBatchRecords {
		t.Fatalf("Close leaked slots: rows=%d bytes=%d free=%d", r.ownedRows, r.ownedBytes, len(r.free))
	}
}
