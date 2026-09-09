package freezer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
	for n := uint64(0); n < 300; n++ {
		got, err := r.Read(n)
		if err != nil || !bytes.Equal(got, []byte{byte(n)}) {
			t.Fatalf("record %d: %x / %v", n, got, err)
		}
		if active.Load() != 0 {
			t.Fatal("transform outlived Read")
		}
	}
	if _, err := r.Read(300); err == nil {
		t.Fatal("accepted read beyond end")
	}
}

func TestV2PreparedReaderBoundsRowsAndOversizedInput(t *testing.T) {
	var reads int
	r := newV2PreparedReader(context.Background(), 0, 300, 4, func(n uint64) ([]byte, []byte, error) {
		reads++
		size := 1
		if n == 1 {
			size = v2PreparationBatchBytes + 1
		}
		return bytes.Repeat([]byte{byte(n)}, size), nil, nil
	}, func(n uint64, data, _ []byte) ([]byte, error) { return data[:1], nil })
	for n := uint64(0); n < 300; n++ {
		got, err := r.Read(n)
		if err != nil || !bytes.Equal(got, []byte{byte(n)}) {
			t.Fatalf("record %d: %x / %v", n, got, err)
		}
		if n == 0 && (reads != 2 || len(r.batch) != 1 || r.carry == nil) {
			t.Fatalf("lookahead not bounded: reads=%d batch=%d carry=%t", reads, len(r.batch), r.carry != nil)
		}
		if n == 1 && (reads != 2 || len(r.batch) != 1) {
			t.Fatal("oversized input did not run alone")
		}
		if len(r.batch) > v2PreparationBatchRecords {
			t.Fatal("batch record cap exceeded")
		}
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
