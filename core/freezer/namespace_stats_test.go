package freezer

import (
	"bytes"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type namespaceStatsDB struct {
	ethdb.KeyValueStore
	iterator func() ethdb.Iterator
}

func (db *namespaceStatsDB) NewIterator(prefix, start []byte) ethdb.Iterator {
	if bytes.Equal(prefix, blockNamespacePrefix) {
		return db.iterator()
	}
	return db.KeyValueStore.NewIterator(prefix, start)
}

// This lazy iterator does not allocate the backlog up front, so counting Next
// calls also verifies that the scanner's cost cannot grow with backlog size.
type namespaceStatsIterator struct {
	total       uint64
	value       []byte
	visited     uint64
	nextCalls   uint64
	errorAtCall uint64
	err         error
	onNext      func()
	released    bool
}

func (it *namespaceStatsIterator) Next() bool {
	it.nextCalls++
	if it.onNext != nil {
		it.onNext()
	}
	if it.Error() != nil || it.visited >= it.total {
		return false
	}
	it.visited++
	return true
}

func (it *namespaceStatsIterator) Error() error {
	if it.err != nil && it.nextCalls >= it.errorAtCall {
		return it.err
	}
	return nil
}

func (it *namespaceStatsIterator) Key() []byte   { return []byte("b-12345678") }
func (it *namespaceStatsIterator) Value() []byte { return it.value }
func (it *namespaceStatsIterator) Release()      { it.released = true }

func newNamespaceStatsRunner(t *testing.T, db ethdb.KeyValueStore) *Runner {
	t.Helper()
	namespace := normalizeMetricNamespace("test/chain/freezer/" + strings.ReplaceAll(t.Name(), "/", "_"))
	t.Cleanup(func() { unregisterRunnerMetricNamespace(namespace) })
	return New(&fakeChain{db: db}, wrapFreezer(newFreezer(t)), Config{Enabled: true, MetricsNamespace: namespace})
}

func TestHotBlockSizeSampleCompleteAndEmpty(t *testing.T) {
	for _, rows := range []uint64{0, 3} {
		t.Run(strconv.FormatUint(rows, 10), func(t *testing.T) {
			t.Parallel()
			fc := newFakeChain()
			defer fc.db.Close()
			var want uint64
			for n := uint64(0); n < rows; n++ {
				fc.plantBlock(t, n)
				raw, ok, err := rawdb.ReadBlockRawStrict(fc.db, n)
				if err != nil || !ok {
					t.Fatalf("ReadBlockRawStrict(%d): ok=%t err=%v", n, ok, err)
				}
				want += uint64(10 + len(raw))
			}
			r := newNamespaceStatsRunner(t, fc.db)
			before := r.Snapshot()
			if before.PebbleSizeAfterComplete || !before.PebbleSizeAfterSampledAt.IsZero() {
				t.Fatalf("uninitialized sample looks complete: %+v", before)
			}
			if err := r.sampleHotBlockNamespaceSize(); err != nil {
				t.Fatal(err)
			}
			got := r.Snapshot()
			if !got.PebbleSizeAfterComplete || got.PebbleSizeAfterRows != rows || got.PebbleSizeAfter != want || got.PebbleSizeAfterSampledAt.IsZero() {
				t.Fatalf("sample=%+v, want complete rows=%d bytes=%d", got, rows, want)
			}
			r.updateMetrics()
			if runnerGaugeValue(t, r.cfg.MetricsNamespace+"pebble/size/complete") != 1 || runnerGaugeValue(t, r.cfg.MetricsNamespace+"pebble/size/rows") != int64(rows) || runnerGaugeValue(t, r.cfg.MetricsNamespace+"pebble/size/sampled_at") != got.PebbleSizeAfterSampledAt.Unix() {
				t.Fatal("sample metadata metrics do not match snapshot")
			}
		})
	}
}

func TestHotBlockSizeScanBudgets(t *testing.T) {
	for _, tc := range []struct {
		name      string
		total     uint64
		valueSize int
		budget    namespaceSizeBudget
		wantRows  uint64
		wantCalls uint64
	}{
		{"rows", 1_000_000_000, 32, namespaceSizeBudget{rows: 7, bytes: 1 << 20, duration: time.Second}, 7, 7},
		{"exact_rows_is_conservatively_incomplete", 7, 32, namespaceSizeBudget{rows: 7, bytes: 1 << 20, duration: time.Second}, 7, 7},
		{"bytes", 1_000_000_000, 32, namespaceSizeBudget{rows: 100, bytes: 80, duration: time.Second}, 2, 2},
		{"oversized_row", 100, 1000, namespaceSizeBudget{rows: 100, bytes: 80, duration: time.Second}, 1, 1},
		{"time", 1_000_000_000, 32, namespaceSizeBudget{rows: 100, bytes: 1 << 20, duration: 0}, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			it := &namespaceStatsIterator{total: tc.total, value: make([]byte, tc.valueSize)}
			db := &namespaceStatsDB{KeyValueStore: memorydb.New(), iterator: func() ethdb.Iterator { return it }}
			defer db.Close()
			r := newNamespaceStatsRunner(t, db)
			got, err := r.scanHotBlockNamespaceSize(tc.budget)
			if err != nil {
				t.Fatal(err)
			}
			if got.complete || got.rows != tc.wantRows || it.nextCalls != tc.wantCalls || got.bytes != tc.wantRows*uint64(10+tc.valueSize) || !it.released {
				t.Fatalf("sample=%+v calls=%d released=%t, want rows=%d calls=%d incomplete", got, it.nextCalls, it.released, tc.wantRows, tc.wantCalls)
			}
		})
	}
}

func TestHotBlockSizeSampleErrorAndCancellationDoNotPublish(t *testing.T) {
	for _, mode := range []string{"initial_error", "iteration_error", "cancel_during_next", "cancel_before_scan"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			readErr := errors.New("injected iterator read failure")
			it := &namespaceStatsIterator{total: 10, value: []byte("value")}
			created := false
			db := &namespaceStatsDB{KeyValueStore: memorydb.New(), iterator: func() ethdb.Iterator { created = true; return it }}
			defer db.Close()
			r := newNamespaceStatsRunner(t, db)
			old := &hotBlockSizeSample{bytes: 999, rows: 9, complete: true, sampledAt: time.Unix(100, 0)}
			r.pebbleSizeSample.Store(old)
			wantErr := readErr
			switch mode {
			case "initial_error":
				it.err = readErr
			case "iteration_error":
				it.err, it.errorAtCall = readErr, 2
			case "cancel_during_next":
				it.onNext = r.BeginStop
				wantErr = errRunnerStopping
			case "cancel_before_scan":
				r.BeginStop()
				wantErr = errRunnerStopping
			}
			if err := r.sampleHotBlockNamespaceSize(); !errors.Is(err, wantErr) {
				t.Fatalf("err=%v, want %v", err, wantErr)
			}
			if r.pebbleSizeSample.Load() != old {
				t.Fatal("failed scan published a fresh or partial sample")
			}
			if mode == "cancel_before_scan" {
				if created {
					t.Fatal("cancelled scan opened an iterator")
				}
			} else if !it.released {
				t.Fatal("iterator was not released")
			}
		})
	}
}

func TestHotBlockSizeSampleSnapshotCoherent(t *testing.T) {
	t.Parallel()
	db := memorydb.New()
	defer db.Close()
	r := newNamespaceStatsRunner(t, db)
	first := &hotBlockSizeSample{bytes: 100, rows: 10, complete: true, sampledAt: time.Unix(100, 0)}
	second := &hotBlockSizeSample{bytes: 200, rows: 20, complete: false, sampledAt: time.Unix(200, 0)}
	r.pebbleSizeSample.Store(first)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10000; i++ {
			r.pebbleSizeSample.Store(second)
			r.pebbleSizeSample.Store(first)
		}
	}()
	for i := 0; i < 10000; i++ {
		s := r.Snapshot()
		want := first
		if s.PebbleSizeAfter == second.bytes {
			want = second
		}
		if s.PebbleSizeAfter != want.bytes || s.PebbleSizeAfterRows != want.rows || s.PebbleSizeAfterComplete != want.complete || !s.PebbleSizeAfterSampledAt.Equal(want.sampledAt) {
			t.Errorf("torn sample: %+v", s)
			break
		}
	}
	wg.Wait()
}

func TestHotBlockSizeFailurePreservesCompletedFreezeCount(t *testing.T) {
	t.Parallel()
	fc := newFakeChain()
	defer fc.db.Close()
	for n := uint64(0); n < 10; n++ {
		fc.plantBlock(t, n)
	}
	fc.setSolidified(5)
	readErr := errors.New("injected namespace stats failure")
	fc.db = &namespaceStatsDB{KeyValueStore: fc.db, iterator: func() ethdb.Iterator { return &namespaceStatsIterator{err: readErr} }}
	namespace := normalizeMetricNamespace("test/chain/freezer/" + t.Name())
	t.Cleanup(func() { unregisterRunnerMetricNamespace(namespace) })
	r := New(fc, wrapFreezer(newFreezer(t)), Config{Enabled: true, BatchBlocks: 5, MetricsNamespace: namespace})
	frozen, err := r.OnePass()
	if !errors.Is(err, readErr) || frozen != 5 {
		t.Fatalf("OnePass=(%d,%v), want completed=5 and stats error", frozen, err)
	}
	if s := r.Snapshot(); s.BlocksFrozen != 5 || !s.HasFrozen || s.FrozenMax != 4 || !s.PebbleSizeAfterSampledAt.IsZero() {
		t.Fatalf("durable progress or sample metadata lost: %+v", s)
	}
	progress, ok, err := rawdb.ReadStageProgress(fc.db, rawdb.StageChainFreezer)
	if err != nil || !ok || progress != 4 {
		t.Fatalf("freezer stage=(%d,%t,%v), want 4", progress, ok, err)
	}
}
