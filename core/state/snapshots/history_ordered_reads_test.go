package snapshots

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestHistoryOrderedReadsIgnoreUnneededSpeculativeErrors(t *testing.T) {
	p := newHistoryOrderedReads(context.Background(), func(_ []byte, c *rawdb.StateDomainChange) bool { return c != nil })
	defer p.close()
	key := []byte("key")
	ready := make(chan struct{})
	p.submit(&historyReadAheadTask{keys: [][]byte{key}, read: func(_ context.Context, _ [][]byte, emit func([]byte, *rawdb.StateDomainChange) (bool, error)) error {
		<-ready // The later source finishes first; consumption must stay ordered.
		_, err := emit(key, &rawdb.StateDomainChange{TxNum: 1, Prev: []byte("first")})
		return err
	}})
	p.submit(&historyReadAheadTask{keys: [][]byte{key}, read: func(context.Context, [][]byte, func([]byte, *rawdb.StateDomainChange) (bool, error)) error {
		close(ready)
		return errors.New("unneeded later source")
	}})
	var first *rawdb.StateDomainChange
	active := func([]byte) bool { return first == nil }
	emit := func(_ []byte, c *rawdb.StateDomainChange) (bool, error) { first = c; return false, nil }
	if err := p.consume(active, emit); err != nil || first == nil || string(first.Prev) != "first" {
		t.Fatal(first, err)
	}
	if err := p.consume(active, emit); err != nil {
		t.Fatal("speculative error escaped for a resolved key", err)
	}
}

func TestHistoryOrderedReadsRetryPrunedAndOversizedResults(t *testing.T) {
	for _, large := range []bool{false, true} {
		t.Run(map[bool]string{false: "pruned", true: "large"}[large], func(t *testing.T) {
			p := newHistoryOrderedReads(context.Background(), func(_ []byte, c *rawdb.StateDomainChange) bool { return c != nil })
			defer p.close()
			calls := 0 // The consumer runs only after the worker's result channel.
			keys := [][]byte{[]byte("old"), []byte("needed")}
			p.submit(&historyReadAheadTask{keys: keys, read: func(_ context.Context, keys [][]byte, emit func([]byte, *rawdb.StateDomainChange) (bool, error)) error {
				calls++
				if !large && len(keys) == 2 {
					return errors.New("corruption while reading an already resolved key")
				}
				value := []byte("value")
				if large {
					value = make([]byte, historyReadAheadResultBytes+1)
				}
				_, err := emit(keys[len(keys)-1], &rawdb.StateDomainChange{TxNum: 2, Prev: value})
				return err
			}})
			got := 0
			if err := p.consume(func(k []byte) bool { return large || string(k) == "needed" }, func(k []byte, c *rawdb.StateDomainChange) (bool, error) {
				if string(k) != "needed" || c.TxNum != 2 {
					t.Fatal(string(k), c)
				}
				got++
				return false, nil
			}); err != nil || calls != 2 || got != 1 {
				t.Fatal(calls, got, err)
			}
		})
	}
}

func TestHistoryOrderedReadsCancelJoinsWorkers(t *testing.T) {
	p := newHistoryOrderedReads(context.Background(), func([]byte, *rawdb.StateDomainChange) bool { return true })
	started := make(chan struct{})
	finished := make(chan struct{})
	p.submit(&historyReadAheadTask{keys: [][]byte{[]byte("key")}, read: func(ctx context.Context, _ [][]byte, _ func([]byte, *rawdb.StateDomainChange) (bool, error)) error {
		close(started)
		<-ctx.Done()
		close(finished)
		return ctx.Err()
	}})
	<-started
	p.close()
	select {
	case <-finished:
	default:
		t.Fatal("worker survived request close")
	}
}

func TestHistoryPhysicalPagesCoalesceDescendingFooters(t *testing.T) {
	data := bytes.Repeat([]byte{7}, 12*historyDirectoryPageSize+317)
	file, err := os.CreateTemp(t.TempDir(), "physical")
	if err != nil {
		t.Fatal(err)
	}
	r := newHistoryAccessorPages(context.Background(), file, uint64(len(data)))
	defer r.Close()
	source := &historyPageProbe{data: data}
	r.pages.source = source
	var b [64]byte
	for _, delta := range []int64{64, 128, 192, 3000} {
		if _, err := r.ReadAt(b[:], int64(len(data))-delta); err != nil || b[0] != 7 {
			t.Fatal(delta, err)
		}
	}
	if source.calls != 1 {
		t.Fatal("descending footer reads did not share their final page", source.calls)
	}
}
