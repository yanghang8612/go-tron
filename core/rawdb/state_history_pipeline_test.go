package rawdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/pointread"
)

type pipelineTestView struct {
	StateHistoryReadView
	hook   func(string, []byte) error
	mu     sync.Mutex
	trace  map[string][]string
	active atomic.Int32
}

func (*pipelineTestView) ConcurrentOwnedHistoryReads() bool { return true }
func (*pipelineTestView) GetReturnsOwnedBytes() bool        { return true }
func (v *pipelineTestView) operation(op string, key []byte) error {
	v.mu.Lock()
	if v.trace == nil {
		v.trace = make(map[string][]string)
	}
	v.trace[string(key)] = append(v.trace[string(key)], op)
	v.mu.Unlock()
	if v.hook != nil {
		return v.hook(op, key)
	}
	return nil
}
func (v *pipelineTestView) Has(key []byte) (bool, error) {
	v.active.Add(1)
	defer v.active.Add(-1)
	if err := v.operation("Has", key); err != nil {
		return false, err
	}
	return v.StateHistoryReadView.Has(key)
}
func (v *pipelineTestView) Get(key []byte) ([]byte, error) {
	v.active.Add(1)
	defer v.active.Add(-1)
	if err := v.operation("Get", key); err != nil {
		return nil, err
	}
	return v.StateHistoryReadView.Get(key)
}

func pipelineFixture(t testing.TB, modes ...string) (ethdb.KeyValueStore, [][]byte) {
	t.Helper()
	db, err := NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	keys := make([][]byte, len(modes))
	for i, mode := range modes {
		block := uint64(i + 1)
		row := borrowedStateDomainChangeTestRow(block, 1, block*10)
		row.Prev = bytes.Repeat([]byte{byte(i + 1)}, 80<<10)
		raw := encodeBorrowedStateDomainChangeTestBlock(t, []*StateDomainChange{row})
		if mode == "nested-snappy" {
			raw, _ = encodeStateDomainChangeBlockStorage(raw)
		}
		if mode == "nested" {
			raw = append(bytes.Clone(stateDomainChangeBlockEnvelopeMagic[:]), stateDomainChangeBlockSharedVersion)
		}
		if mode == "bad-rlp" {
			raw = []byte{0xff}
		}
		pack, values := ownedReadBenchmarkPack(raw, block, mode == "snappy-shared")
		refs, _, _, _, err := sharedStateHistoryPackHeader(pack, block)
		if err != nil {
			t.Fatal(err)
		}
		_, n := binary.Uvarint(refs)
		var hash [32]byte
		copy(hash[:], refs[n:n+32])
		keys[i] = stateHistoryChunkKey(stateHistoryChunkBucket(block), hash)
		for key, val := range values {
			if err := db.Put([]byte(key), val); err != nil {
				t.Fatal(err)
			}
		}
		switch mode {
		case "raw":
			pack = raw
		case "snappy":
			pack, _ = encodeStateDomainChangeBlockStorage(raw)
		case "bad-digest":
			p := len(stateDomainChangeBlockEnvelopeMagic) + 1
			_, n := binary.Uvarint(pack[p:])
			p += n
			_, n = binary.Uvarint(pack[p:])
			p += n
			pack[p] ^= 1
		case "missing":
			if err := db.Delete(keys[i]); err != nil {
				t.Fatal(err)
			}
		case "bad-chunk":
			if err := db.Put(keys[i], []byte{0xff}); err != nil {
				t.Fatal(err)
			}
		case "chunk-hash":
			value, err := db.Get(keys[i])
			if err != nil {
				t.Fatal(err)
			}
			value[len(value)-1] ^= 1
			if err := db.Put(keys[i], value); err != nil {
				t.Fatal(err)
			}
		case "bad-header":
			pack = pack[:len(pack)-1]
		}
		if err := db.Put(stateChangeSetKey(block, 0), pack); err != nil {
			t.Fatal(err)
		}
		if err := WriteStateTxRange(db, block, common.Hash{byte(i + 1)}, block*10, block*10); err != nil {
			t.Fatal(err)
		}
	}
	return db, keys
}

func pipelineCollect(t *testing.T, view StateHistoryReadView, candidate bool, bounds [4]uint64) ([]*StateDomainChange, error) {
	t.Helper()
	var rows []*StateDomainChange
	fn := func(row *StateDomainChange) (bool, error) {
		rows = append(rows, cloneStateDomainChange(row))
		return true, nil
	}
	if candidate {
		err := IterateStateDomainChangesByBlockTxRangePipelined(context.Background(), view, bounds[0], bounds[1], bounds[2], bounds[3], fn)
		return rows, err
	}
	err := frozen3fdIterateHistoryRange(view, bounds[0], bounds[1], bounds[2], bounds[3], fn)
	return rows, err
}

func TestStateHistoryPipelineFrozenOracleAndPerReferenceReads(t *testing.T) {
	for _, modes := range [][]string{{"shared", "shared", "shared"}, {"snappy-shared", "raw", "snappy", "shared"}, {"shared", "nested-snappy", "shared"}, {"shared", "bad-digest", "shared"}, {"shared", "missing", "shared"}, {"shared", "bad-chunk", "shared"}, {"shared", "chunk-hash", "shared"}, {"shared", "bad-header", "shared"}, {"shared", "nested", "shared"}, {"shared", "bad-rlp", "shared"}} {
		t.Run(fmt.Sprint(modes), func(t *testing.T) {
			db, _ := pipelineFixture(t, modes...)
			view, closeView, err := AcquireStateHistoryReadView(db)
			if err != nil {
				t.Fatal(err)
			}
			defer closeView()
			for _, bounds := range [][4]uint64{{1, uint64(len(modes)), 0, math.MaxUint64}, {1, uint64(len(modes)), 11, 30}, {2, 1, 0, 99}, {1, 2, 99, 0}, {math.MaxUint64, math.MaxUint64, 0, math.MaxUint64}} {
				a, b := &pipelineTestView{StateHistoryReadView: view}, &pipelineTestView{StateHistoryReadView: view}
				want, we := pipelineCollect(t, a, false, bounds)
				got, ge := pipelineCollect(t, b, true, bounds)
				if fmt.Sprint(we) != fmt.Sprint(ge) || !reflect.DeepEqual(want, got) {
					t.Fatalf("bounds=%v error %v != %v; rows %d != %d", bounds, we, ge, len(want), len(got))
				}
				// Future speculative reads are allowed only on the opt-in path.
				// Every key reached by serial still has identical Has/Get counts
				// and order; complete successful scans have no extra reads at all.
				for key, ops := range a.trace {
					if !reflect.DeepEqual(ops, b.trace[key]) {
						t.Fatalf("per-reference reads %x: %v != %v", key, ops, b.trace[key])
					}
				}
				if we == nil && !reflect.DeepEqual(a.trace, b.trace) {
					t.Fatal("successful scan changed read coverage")
				}
			}
		})
	}
}

func TestStateHistoryPipelineSourceAndFirstErrorOracle(t *testing.T) {
	for _, fault := range []string{"Has", "Get", "range", "legacy", "missing-range", "source-order"} {
		t.Run(fault, func(t *testing.T) {
			db, keys := pipelineFixture(t, "shared", "shared", "shared")
			sentinel := errors.New("injected " + fault)
			switch fault {
			case "range":
				if err := db.Put(stateTxRangeKey(2), []byte{0xff}); err != nil {
					t.Fatal(err)
				}
			case "legacy":
				if err := db.Put(stateChangeSetKey(2, 1), []byte{0xff}); err != nil {
					t.Fatal(err)
				}
			case "missing-range":
				if err := db.Delete(stateTxRangeKey(2)); err != nil {
					t.Fatal(err)
				}
			case "source-order":
				row := borrowedStateDomainChangeTestRow(2, 1, 5)
				raw := encodeBorrowedStateDomainChangeTestBlock(t, []*StateDomainChange{row})
				if err := db.Put(stateChangeSetKey(2, 0), raw); err != nil {
					t.Fatal(err)
				}
			}
			view, release, err := AcquireStateHistoryReadView(db)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			hook := func(op string, key []byte) error {
				if op == fault && bytes.Equal(key, keys[1]) {
					return sentinel
				}
				return nil
			}
			a, b := &pipelineTestView{StateHistoryReadView: view, hook: hook}, &pipelineTestView{StateHistoryReadView: view, hook: hook}
			want, we := pipelineCollect(t, a, false, [4]uint64{1, 3, 0, 99})
			got, ge := pipelineCollect(t, b, true, [4]uint64{1, 3, 0, 99})
			if fmt.Sprint(we) != fmt.Sprint(ge) || !reflect.DeepEqual(want, got) {
				t.Fatalf("%v != %v; rows %d != %d", we, ge, len(want), len(got))
			}
		})
	}
}

func pipelineAwait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("deterministic pipeline barrier timed out")
	}
}

func TestStateHistoryPipelineFutureFailureDoesNotCancelPredecessor(t *testing.T) {
	for _, callbackErr := range []error{nil, errors.New("earlier callback")} {
		t.Run(fmt.Sprint(callbackErr), func(t *testing.T) {
			db, keys := pipelineFixture(t, "shared", "shared", "shared")
			view, release, err := AcquireStateHistoryReadView(db)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			first, allowFirst, future := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var firstOnce, futureOnce sync.Once
			v := &pipelineTestView{StateHistoryReadView: view, hook: func(op string, key []byte) error {
				if op == "Get" && bytes.Equal(key, keys[0]) {
					firstOnce.Do(func() { close(first) })
					<-allowFirst
				}
				if op == "Get" && bytes.Equal(key, keys[1]) {
					futureOnce.Do(func() { close(future) })
					return errors.New("future failure")
				}
				return nil
			}}
			done := make(chan error, 1)
			seen := 0
			go func() {
				done <- IterateStateDomainChangesByBlockTxRangePipelined(context.Background(), v, 1, 3, 0, 99, func(row *StateDomainChange) (bool, error) { seen++; return false, callbackErr })
			}()
			pipelineAwait(t, first)
			pipelineAwait(t, future)
			select {
			case err := <-done:
				t.Fatalf("returned ahead of predecessor: %v", err)
			default:
			}
			close(allowFirst)
			if err := <-done; !errors.Is(err, callbackErr) || seen != 1 || v.active.Load() != 0 {
				t.Fatalf("err=%v seen=%d active=%d", err, seen, v.active.Load())
			}
		})
	}
}

type pipelineFactory struct {
	ethdb.Iteratee
	snapshot *pipelineClosingView
	err      error
}

func (f *pipelineFactory) NewKeyValueSnapshot() (pointread.KeyValueSnapshot, error) {
	return f.snapshot, f.err
}

type pipelineClosingView struct {
	*pipelineTestView
	closeFn     func() error
	closes      atomic.Int32
	closeActive atomic.Int32
}

func (v *pipelineClosingView) Close() error {
	v.closeActive.Store(v.active.Load())
	v.closes.Add(1)
	return v.closeFn()
}

func TestStateHistoryPipelineCancellationJoinsBeforeOwnedSnapshotClose(t *testing.T) {
	db, keys := pipelineFixture(t, "shared", "shared")
	view, release, err := AcquireStateHistoryReadView(db)
	if err != nil {
		t.Fatal(err)
	}
	started, unblock := make(chan struct{}), make(chan struct{})
	var once sync.Once
	v := &pipelineClosingView{pipelineTestView: &pipelineTestView{StateHistoryReadView: view, hook: func(op string, key []byte) error {
		if op == "Get" && bytes.Equal(key, keys[0]) {
			once.Do(func() { close(started) })
			<-unblock
		}
		return nil
	}}, closeFn: release}
	f := &pipelineFactory{Iteratee: db, snapshot: v}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- IterateStateDomainChangesByBlockTxRangePipelined(ctx, f, 1, 2, 0, 99, borrowedStateDomainChangeNoop)
	}()
	pipelineAwait(t, started)
	cancel()
	select {
	case err := <-done:
		t.Fatalf("did not join: %v", err)
	default:
	}
	if v.closes.Load() != 0 {
		t.Fatal("closed active snapshot")
	}
	close(unblock)
	if err := <-done; !errors.Is(err, context.Canceled) || v.closes.Load() != 1 || v.closeActive.Load() != 0 || v.active.Load() != 0 {
		t.Fatalf("err=%v closes=%d active=%d atClose=%d", err, v.closes.Load(), v.active.Load(), v.closeActive.Load())
	}
}

type pipelineFalseView struct {
	StateHistoryReadView
	owned, concurrent bool
}

type pipelinePresenceView struct{ *pipelineTestView }

func (v pipelinePresenceView) GetWithPresence(key []byte) ([]byte, bool, error) {
	if err := v.operation("GetWithPresence", key); err != nil {
		return nil, false, err
	}
	value, err := v.StateHistoryReadView.Get(key)
	return value, err == nil, err
}

func (v pipelineFalseView) GetReturnsOwnedBytes() bool        { return v.owned }
func (v pipelineFalseView) ConcurrentOwnedHistoryReads() bool { return v.concurrent }

func TestStateHistoryPipelinePinnedCapabilityAndSnapshotOwnership(t *testing.T) {
	db, keys := pipelineFixture(t, "shared", "shared")
	view, release, err := AcquireStateHistoryReadView(db)
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range []ethdb.Iteratee{struct{ StateHistoryReadView }{view}, pipelineFalseView{view, true, false}, pipelineFalseView{view, false, true}, struct{ ethdb.KeyValueStore }{db}} {
		if err := IterateStateDomainChangesByBlockTxRangePipelined(context.Background(), source, 1, 2, 0, 99, borrowedStateDomainChangeNoop); !errors.Is(err, ErrStateHistoryPipelineView) {
			t.Fatalf("unaudited view accepted: %T %v", source, err)
		}
	}
	presence := pipelinePresenceView{&pipelineTestView{StateHistoryReadView: view}}
	if err := IterateStateDomainChangesByBlockTxRangePipelined(context.Background(), presence, 1, 2, 0, 99, borrowedStateDomainChangeNoop); !errors.Is(err, ErrStateHistoryPipelineView) || len(presence.trace) != 0 {
		t.Fatal("silently changed coupled-presence path", err)
	}
	want, we := pipelineCollect(t, view, false, [4]uint64{1, 2, 0, 99})
	if we != nil {
		t.Fatal(we)
	}
	// Both iterator and point reads must stay at the old sequence after writes.
	if err := db.Delete(stateChangeSetKey(1, 0)); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete(keys[0]); err != nil {
		t.Fatal(err)
	}
	got, ge := pipelineCollect(t, view, true, [4]uint64{1, 2, 0, 99})
	if ge != nil || !reflect.DeepEqual(want, got) {
		t.Fatal("snapshot moved", ge)
	}
	if err := release(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatal("retained callback-owned copies changed after view close")
	}
	failure := errors.New("snapshot factory")
	if err := IterateStateDomainChangesByBlockTxRangePipelined(context.Background(), &pipelineFactory{Iteratee: db, err: failure}, 1, 2, 0, 99, borrowedStateDomainChangeNoop); !errors.Is(err, failure) {
		t.Fatal(err)
	}
}

func TestStateHistoryPipelineBudgetAndOverflow(t *testing.T) {
	for _, tt := range []struct {
		used         uint64
		count        int
		next, budget uint64
		want         bool
	}{
		{0, 0, 1, 256, true}, {127, 1, 127, 256, true}, {128, 1, 1, 256, false}, {1, 1, 128, 256, false}, {0, 0, 128, 256, true},
		{0, 0, 0, 256, false}, {0, 2, 1, 256, false}, {0, -1, 1, 256, false}, {257, 1, 1, 256, false}, {255, 1, 2, 256, false},
		{math.MaxUint64, 1, 1, math.MaxUint64, false}, {1, 1, math.MaxUint64, math.MaxUint64, false},
	} {
		if got := historyPipelineCanAdmit(tt.used, tt.count, tt.next, tt.budget, 2, tt.used >= tt.budget/2); got != tt.want {
			t.Fatalf("%+v got %v", tt, got)
		}
	}
	if StateHistoryPipelineDecodedBudget != 2*uint64(stateDomainChangeBlockMaxDecodedBytes) {
		t.Fatal("budget no longer aligned to existing format bound")
	}
	// Completed-but-not-consumed output must continue to occupy a slot/charge.
	done := make(chan struct{})
	close(done)
	queue := []*historyPipelineBlock{{done: done, decoded: 128}}
	if historyPipelineKnownFailure(queue) || historyPipelineCanAdmit(queue[0].decoded, len(queue), 1, 256, 2, true) {
		t.Fatal("ready result released before consumption")
	}
}

func TestStateHistoryPipelinePreflightBoundsWithoutReads(t *testing.T) {
	for _, size := range []uint64{0, stateDomainChangeBlockMaxDecodedBytes + 1, math.MaxUint64} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			db, _ := pipelineFixture(t, "shared")
			pack := append(bytes.Clone(stateDomainChangeBlockEnvelopeMagic[:]), stateDomainChangeBlockSharedVersion)
			pack = binary.AppendUvarint(pack, 1)
			pack = binary.AppendUvarint(pack, size)
			if err := db.Put(stateChangeSetKey(1, 0), pack); err != nil {
				t.Fatal(err)
			}
			view, release, err := AcquireStateHistoryReadView(db)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			a, b := &pipelineTestView{StateHistoryReadView: view}, &pipelineTestView{StateHistoryReadView: view}
			want, we := pipelineCollect(t, a, false, [4]uint64{1, 1, 0, 99})
			got, ge := pipelineCollect(t, b, true, [4]uint64{1, 1, 0, 99})
			if we == nil || fmt.Sprint(we) != fmt.Sprint(ge) || len(want) != 0 || len(got) != 0 || len(a.trace) != 0 || len(b.trace) != 0 {
				t.Fatalf("size=%d errors=%v/%v traces=%v/%v", size, we, ge, a.trace, b.trace)
			}
		})
	}
}

func TestStateHistoryPipelineConsumerRetainsSlotUntilCallbackReturns(t *testing.T) {
	db, keys := pipelineFixture(t, "shared", "shared", "shared")
	view, release, err := AcquireStateHistoryReadView(db)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	inCallback, allowCallback, thirdStarted := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var thirdOnce sync.Once
	v := &pipelineTestView{StateHistoryReadView: view, hook: func(op string, key []byte) error {
		if op == "Get" && bytes.Equal(key, keys[2]) {
			thirdOnce.Do(func() { close(thirdStarted) })
		}
		return nil
	}}
	var scratch *StateDomainChange
	seen := 0
	done := make(chan error, 1)
	go func() {
		done <- IterateStateDomainChangesByBlockTxRangePipelined(context.Background(), v, 1, 3, 0, 99, func(row *StateDomainChange) (bool, error) {
			seen++
			scratch = row
			if seen == 1 {
				close(inCallback)
				<-allowCallback
			}
			return true, nil
		})
	}()
	pipelineAwait(t, inCallback)
	select {
	case <-thirdStarted:
		t.Fatal("consuming block freed its slot before callback returned")
	default:
	}
	close(allowCallback)
	if err := <-done; err != nil || seen != 3 {
		t.Fatalf("error=%v rows=%d", err, seen)
	}
	pipelineAwait(t, thirdStarted)
	// White-box ownership assertion: the operation's reused row may not retain
	// a decoded output after that output's admission charge is released.
	if scratch == nil || scratch.Key != nil || scratch.Prev != nil {
		t.Fatal("scratch retained a decoded output after charge release")
	}
}

type pipelineErrorIterator struct {
	ethdb.Iterator
	err error
}

func (i pipelineErrorIterator) Error() error {
	if i.err != nil {
		return i.err
	}
	return i.Iterator.Error()
}

type pipelineIteratorErrors struct {
	*pipelineTestView
	changeErr, rangeErr error
}

func (v pipelineIteratorErrors) NewIterator(prefix, start []byte) ethdb.Iterator {
	i := v.StateHistoryReadView.NewIterator(prefix, start)
	if bytes.Equal(prefix, stateChangeSetPrefix) {
		return pipelineErrorIterator{i, v.changeErr}
	}
	return pipelineErrorIterator{i, v.rangeErr}
}

func TestStateHistoryPipelineIteratorErrorPrecedenceOracle(t *testing.T) {
	changeErr, rangeErr := errors.New("change iterator failure"), errors.New("range iterator failure")
	for _, tc := range []struct {
		name                                 string
		change, rangeFailure, missing, early bool
	}{
		{"change", true, false, false, false}, {"range", false, true, false, false}, {"both", true, true, false, false},
		{"range-eof", false, true, true, false}, {"both-eof", true, true, true, false}, {"early-stop", true, true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, _ := pipelineFixture(t, "shared", "shared")
			if tc.missing {
				if err := db.Delete(stateTxRangeKey(2)); err != nil {
					t.Fatal(err)
				}
			}
			view, release, err := AcquireStateHistoryReadView(db)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			var expectedRows []*StateDomainChange
			var expectedErr error
			for _, candidate := range []bool{false, true} {
				v := pipelineIteratorErrors{pipelineTestView: &pipelineTestView{StateHistoryReadView: view}}
				if tc.change {
					v.changeErr = changeErr
				}
				if tc.rangeFailure {
					v.rangeErr = rangeErr
				}
				var rows []*StateDomainChange
				visit := func(row *StateDomainChange) (bool, error) {
					rows = append(rows, cloneStateDomainChange(row))
					return !tc.early, nil
				}
				var err error
				if candidate {
					err = IterateStateDomainChangesByBlockTxRangePipelined(context.Background(), v, 1, 2, 0, 99, visit)
				} else {
					err = frozen3fdIterateHistoryRange(v, 1, 2, 0, 99, visit)
				}
				if !candidate {
					expectedRows, expectedErr = rows, err
				} else if !errors.Is(err, expectedErr) || fmt.Sprint(err) != fmt.Sprint(expectedErr) || !reflect.DeepEqual(rows, expectedRows) {
					t.Fatalf("rows=%d/%d errors=%v/%v", len(rows), len(expectedRows), err, expectedErr)
				}
			}
			if tc.early && expectedErr != nil {
				t.Fatal("early callback did not win")
			}
			if !tc.early && tc.change && !errors.Is(expectedErr, changeErr) {
				t.Fatal("change iterator did not take precedence")
			}
		})
	}
}
