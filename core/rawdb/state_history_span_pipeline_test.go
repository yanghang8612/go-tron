package rawdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
)

func spanPipelineFixture(t *testing.T, modes ...string) (ethdb.KeyValueStore, [][]byte) {
	t.Helper()
	db, keys := pipelineFixture(t, modes...)
	for i := range modes {
		block := uint64(i + 1)
		if err := WriteStateTxRange(db, block, common.Hash{byte(block)}, block*10, block*10+9); err != nil {
			t.Fatal(err)
		}
	}
	return db, keys
}

type spanPipelineObservation struct {
	Info   StateHistorySpanBlockInfo
	Rows   []*StateDomainChange
	Chunks []StateHistorySpanChunk
}

func spanPipelineCollect(t *testing.T, view StateHistoryReadView, workers int, bounds [4]uint64, stop bool) ([]spanPipelineObservation, error) {
	t.Helper()
	var result []spanPipelineObservation
	var held []*StateHistorySpanBlock
	visit := func(b *StateHistorySpanBlock) (bool, error) {
		item := spanPipelineObservation{Info: b.Info(), Rows: spanRows(t, b, true)}
		if !reflect.DeepEqual(item.Rows, spanRows(t, b, false)) {
			t.Fatal("callback mutation leaked into block")
		}
		for i := 0; i < b.ChunkCount(); i++ {
			meta, e := b.Chunk(i)
			if e != nil {
				t.Fatal(e)
			}
			item.Chunks = append(item.Chunks, meta)
		}
		result = append(result, item)
		held = append(held, b)
		return !stop, nil
	}
	var err error
	if workers == 0 {
		err = IterateStateHistorySpanBlocks(context.Background(), view, bounds[0], bounds[1], bounds[2], bounds[3], visit)
	} else {
		err = IterateStateHistorySpanBlocksWithWorkers(context.Background(), view, bounds[0], bounds[1], bounds[2], bounds[3], workers, visit)
	}
	for _, b := range held {
		if b.raw != nil || b.ownRows != nil || b.pooled != nil || b.ChunkCount() != 0 {
			t.Fatal("callback payload retained")
		}
		if _, e := b.Chunk(0); !errors.Is(e, ErrStateHistorySpanExpired) {
			t.Fatal(e)
		}
	}
	return result, err
}

func TestStateHistorySpanPipelineSerialOracle(t *testing.T) {
	for _, workers := range []int{2, 4, 8} {
		for _, mode := range []string{"shared", "snappy-shared", "nested-snappy", "missing", "bad-chunk", "chunk-hash", "bad-digest", "bad-rlp", "nested"} {
			t.Run(fmt.Sprintf("workers%d/%s", workers, mode), func(t *testing.T) {
				db, _ := spanPipelineFixture(t, "shared", mode, "shared")
				view := spanView(t, db)
				bounds := [4]uint64{1, 3, 0, 99}
				if !stateHistorySpanCanPipeline(context.Background(), view, 1, 3, 0, 99) {
					t.Fatal("fixture did not exercise parallel source")
				}
				a, b := &pipelineTestView{StateHistoryReadView: view}, &pipelineTestView{StateHistoryReadView: view}
				want, we := spanPipelineCollect(t, a, 0, bounds, false)
				got, ge := spanPipelineCollect(t, b, workers, bounds, false)
				if fmt.Sprint(we) != fmt.Sprint(ge) || !reflect.DeepEqual(want, got) {
					t.Fatalf("rows/error differ: %d/%d %v/%v", len(want), len(got), we, ge)
				}
				// Preflight adds iterator metadata reads, never point reads. Successful
				// authentication still performs the same Has/Get sequence per physical key.
				for key, ops := range a.trace {
					if !reflect.DeepEqual(ops, b.trace[key]) {
						t.Fatalf("per-key trace %x: %v/%v", key, ops, b.trace[key])
					}
				}
				if we == nil && !reflect.DeepEqual(a.trace, b.trace) {
					t.Fatal("successful read coverage differs")
				}
			})
		}
	}
}

func TestStateHistorySpanPipelineSerialFallback(t *testing.T) {
	for _, mode := range []string{"raw", "snappy", "bad-header", "repair", "empty", "missing-range", "gap"} {
		t.Run(mode, func(t *testing.T) {
			use := mode
			if mode == "repair" || mode == "empty" || mode == "missing-range" || mode == "gap" {
				use = "shared"
			}
			db, _ := spanPipelineFixture(t, "shared", use, "shared")
			switch mode {
			case "repair":
				for _, seq := range []uint64{1, 99} {
					row := borrowedStateDomainChangeTestRow(2, seq, 20)
					row.Prev = []byte{byte(seq)}
					raw, e := encodePersistedStateDomainChange(row)
					if e != nil {
						t.Fatal(e)
					}
					if e = db.Put(stateChangeSetKey(2, seq), raw); e != nil {
						t.Fatal(e)
					}
				}
			case "empty":
				if e := db.Delete(stateChangeSetKey(2, 0)); e != nil {
					t.Fatal(e)
				}
			case "missing-range":
				if e := db.Delete(stateTxRangeKey(2)); e != nil {
					t.Fatal(e)
				}
			case "gap":
				if e := WriteStateTxRange(db, 2, common.Hash{2}, 21, 29); e != nil {
					t.Fatal(e)
				}
			}
			view := spanView(t, db)
			for _, bounds := range [][4]uint64{{1, 3, 0, 99}, {1, 3, 20, 20}} {
				if stateHistorySpanCanPipeline(context.Background(), view, bounds[0], bounds[1], bounds[2], bounds[3]) {
					t.Fatal("compatibility shape was admitted")
				}
				for _, stop := range []bool{false, true} {
					want, we := spanPipelineCollect(t, view, 0, bounds, stop)
					for _, workers := range []int{2, 4, 8} {
						got, ge := spanPipelineCollect(t, view, workers, bounds, stop)
						if fmt.Sprint(we) != fmt.Sprint(ge) || !reflect.DeepEqual(want, got) {
							t.Fatalf("workers%d stop%v errors %v/%v", workers, stop, we, ge)
						}
					}
				}
			}
		})
	}
}

func TestStateHistorySpanPipelineQueueOwnershipAndCancelJoin(t *testing.T) {
	for _, workers := range []int{2, 4, 8} {
		for _, cancelRun := range []bool{false, true} {
			t.Run(fmt.Sprintf("workers%d/cancel%v", workers, cancelRun), func(t *testing.T) {
				modes := make([]string, workers+1)
				for i := range modes {
					modes[i] = "shared"
				}
				db, keys := spanPipelineFixture(t, modes...)
				view, release, err := AcquireStateHistoryReadView(db)
				if err != nil {
					t.Fatal(err)
				}
				defer release()
				started := make([]chan struct{}, workers)
				onces := make([]sync.Once, workers)
				for i := range started {
					started[i] = make(chan struct{})
				}
				allowReads, extraRead := make(chan struct{}), make(chan struct{})
				var extraOnce sync.Once
				v := &pipelineClosingView{pipelineTestView: &pipelineTestView{StateHistoryReadView: view, hook: func(op string, key []byte) error {
					if op != "Get" {
						return nil
					}
					for i := range started {
						if bytes.Equal(key, keys[i]) {
							onces[i].Do(func() { close(started[i]) })
							<-allowReads
							return nil
						}
					}
					if bytes.Equal(key, keys[workers]) {
						extraOnce.Do(func() { close(extraRead) })
					}
					return nil
				}}, closeFn: func() error { return nil }}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				inCallback, allowCallback := make(chan struct{}), make(chan struct{})
				seen := 0
				var held *StateHistorySpanBlock
				done := make(chan error, 1)
				go func() {
					done <- IterateStateHistorySpanBlocksWithWorkers(ctx, v, 1, uint64(workers+1), 0, 999, workers, func(b *StateHistorySpanBlock) (bool, error) {
						seen++
						if seen == 1 {
							held = b
							close(inCallback)
							<-allowCallback
						}
						return true, nil
					})
				}()
				for _, ch := range started {
					pipelineAwait(t, ch)
				}
				if int(v.active.Load()) != workers {
					t.Fatalf("active %d want %d", v.active.Load(), workers)
				}
				select {
				case <-extraRead:
					t.Fatal("slot limit exceeded")
				default:
				}
				if cancelRun {
					cancel()
					select {
					case e := <-done:
						t.Fatalf("returned without join: %v", e)
					default:
					}
					close(allowReads)
					close(allowCallback)
				} else {
					close(allowReads)
					pipelineAwait(t, inCallback)
					select {
					case <-extraRead:
						t.Fatal("callback released its charge early")
					default:
					}
					close(allowCallback)
				}
				err = <-done
				if cancelRun {
					if !errors.Is(err, context.Canceled) || seen != 0 {
						t.Fatalf("error %v seen%d", err, seen)
					}
				} else if err != nil || seen != workers+1 {
					t.Fatalf("error%v seen%d", err, seen)
				}
				if held != nil && (held.raw != nil || held.ChunkCount() != 0) {
					t.Fatal("expired callback retained output")
				}
				if v.active.Load() != 0 || v.closes.Load() != 0 {
					t.Fatal("read API closed caller snapshot or left active jobs")
				}
				if e := v.Close(); e != nil || v.closeActive.Load() != 0 || v.closes.Load() != 1 {
					t.Fatal("caller close before join", e)
				}
			})
		}
	}
}

func TestStateHistorySpanPipelineFutureErrorOrder(t *testing.T) {
	for _, workers := range []int{2, 4, 8} {
		for _, behavior := range []string{"stop", "callback-error", "continue"} {
			t.Run(fmt.Sprintf("workers%d/%s", workers, behavior), func(t *testing.T) {
				modes := make([]string, workers)
				for i := range modes {
					modes[i] = "shared"
				}
				db, keys := spanPipelineFixture(t, modes...)
				view := spanView(t, db)
				first, future, allow := make(chan struct{}), make(chan struct{}), make(chan struct{})
				var firstOnce, futureOnce sync.Once
				readErr, callbackErr := errors.New("future read failure"), errors.New("earlier callback failure")
				v := &pipelineTestView{StateHistoryReadView: view, hook: func(op string, key []byte) error {
					if op == "Get" && bytes.Equal(key, keys[0]) {
						firstOnce.Do(func() { close(first) })
						<-allow
					}
					if op == "Get" && bytes.Equal(key, keys[workers-1]) {
						futureOnce.Do(func() { close(future) })
						return readErr
					}
					return nil
				}}
				seen := 0
				done := make(chan error, 1)
				go func() {
					done <- IterateStateHistorySpanBlocksWithWorkers(context.Background(), v, 1, uint64(workers), 0, 999, workers, func(*StateHistorySpanBlock) (bool, error) {
						seen++
						if behavior == "callback-error" {
							return false, callbackErr
						}
						return behavior == "continue", nil
					})
				}()
				pipelineAwait(t, first)
				pipelineAwait(t, future)
				select {
				case e := <-done:
					t.Fatalf("future error overtook predecessor: %v", e)
				default:
				}
				close(allow)
				err := <-done
				want := error(nil)
				count := 1
				if behavior == "continue" {
					want = readErr
					count = workers - 1
				}
				if behavior == "callback-error" {
					want = callbackErr
				}
				if !errors.Is(err, want) || seen != count || v.active.Load() != 0 {
					t.Fatalf("error%v want%v seen%d/%d", err, want, seen, count)
				}
			})
		}
	}
}

// Inject only on the second iterator pair to exercise errors discovered after
// a successful preflight; no physical data change is involved.
type spanPipelineIteratorFailures struct {
	*pipelineTestView
	calls               int
	skipPreflight       bool
	changeErr, rangeErr error
}

func (v *spanPipelineIteratorFailures) NewIterator(prefix, start []byte) ethdb.Iterator {
	v.calls++
	it := v.StateHistoryReadView.NewIterator(prefix, start)
	if v.skipPreflight && v.calls <= 2 {
		return it
	}
	if bytes.Equal(prefix, stateChangeSetPrefix) {
		return pipelineErrorIterator{it, v.changeErr}
	}
	return pipelineErrorIterator{it, v.rangeErr}
}
func TestStateHistorySpanPipelineIteratorErrorOracle(t *testing.T) {
	changeErr, rangeErr := errors.New("changes EOF failure"), errors.New("ranges final failure")
	for _, tc := range []struct {
		name, mode            string
		changes, ranges, stop bool
	}{
		{"change", "shared", true, false, false}, {"range", "shared", false, true, false}, {"both", "shared", true, true, false},
		{"early-stop", "shared", true, true, true}, {"final-stop", "shared", false, true, true}, {"pack-before-change", "bad-digest", true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, _ := spanPipelineFixture(t, "shared", tc.mode)
			view := spanView(t, db)
			for _, skip := range []bool{false, true} {
				a := &spanPipelineIteratorFailures{pipelineTestView: &pipelineTestView{StateHistoryReadView: view}}
				b := &spanPipelineIteratorFailures{pipelineTestView: &pipelineTestView{StateHistoryReadView: view}, skipPreflight: skip}
				if tc.changes {
					a.changeErr = changeErr
					b.changeErr = changeErr
				}
				if tc.ranges {
					a.rangeErr = rangeErr
					b.rangeErr = rangeErr
				}
				bounds := [4]uint64{1, 2, 0, 99}
				if tc.name == "final-stop" {
					bounds[0] = 2
				}
				want, we := spanPipelineCollect(t, a, 0, bounds, tc.stop)
				got, ge := spanPipelineCollect(t, b, 4, bounds, tc.stop)
				if fmt.Sprint(we) != fmt.Sprint(ge) || !reflect.DeepEqual(want, got) {
					t.Fatalf("skip%v errors%v/%v blocks%d/%d", skip, we, ge, len(want), len(got))
				}
			}
		})
	}
}

func TestStateHistorySpanPipelineValidationAndBounds(t *testing.T) {
	db, _ := spanPipelineFixture(t, "shared")
	view := spanView(t, db)
	fn := func(*StateHistorySpanBlock) (bool, error) { return true, nil }
	for _, n := range []int{-1, 0, 1, 3, 5, 9} {
		if e := IterateStateHistorySpanBlocksWithWorkers(context.Background(), view, 1, 1, 0, 99, n, fn); !errors.Is(e, ErrStateHistoryPipelineWorkers) {
			t.Fatal(n, e)
		}
	}
	for _, v := range []StateHistoryReadView{pipelineFalseView{StateHistoryReadView: view, owned: false, concurrent: true}, pipelineFalseView{StateHistoryReadView: view, owned: true, concurrent: false}, pipelinePresenceView{&pipelineTestView{StateHistoryReadView: view}}} {
		if e := IterateStateHistorySpanBlocksWithWorkers(context.Background(), v, 1, 1, 0, 99, 4, fn); !errors.Is(e, ErrStateHistoryPipelineView) {
			t.Fatal(e)
		}
	}
	if e := IterateStateHistorySpanBlocksWithWorkers(context.Background(), nil, 1, 1, 0, 99, 4, fn); !errors.Is(e, ErrStateHistoryReadViewUnpinned) {
		t.Fatal(e)
	}
	for _, bounds := range [][4]uint64{{2, 1, 0, 99}, {1, 1, 99, 0}, {math.MaxUint64, math.MaxUint64, 0, 99}, {1, 5001, 0, math.MaxUint64}} {
		a, we := spanPipelineCollect(t, view, 0, bounds, false)
		b, ge := spanPipelineCollect(t, view, 4, bounds, false)
		if fmt.Sprint(we) != fmt.Sprint(ge) || !reflect.DeepEqual(a, b) {
			t.Fatal(bounds, we, ge)
		}
	}
	// Actual queues retain slots through callbacks above. The shared arithmetic
	// also rejects overflow/oversize and admits exactly one maximum legal pack.
	for _, n := range []int{2, 4, 8} {
		if !historyPipelineCanAdmit(0, 0, 128<<20, 256<<20, n, false) || historyPipelineCanAdmit(128<<20, 1, 1, 256<<20, n, true) || historyPipelineCanAdmit(math.MaxUint64, 1, 1, 256<<20, n, false) || historyPipelineCanAdmit(0, 0, (128<<20)+1, 256<<20, n, false) {
			t.Fatal("budget", n)
		}
	}
}
