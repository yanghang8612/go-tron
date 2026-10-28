package snapshots

import (
	"context"
	"errors"
	"sync"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

const historyReadAheadWindow = 32
const historyReadAheadResultBytes = 1 << 20

// Bound active descriptor pairs across requests as well as within a request.
var historyReadAheadAdmission = make(chan struct{}, historyReadAheadWindow)
var errHistoryReadAheadResultBudget = errors.New("snapshots: speculative history result budget")

type historyPointReadKey struct{}

type historyReadAheadRow struct {
	key    []byte
	change *rawdb.StateDomainChange
}

type historyReadAheadResult struct {
	rows  []historyReadAheadRow
	err   error
	retry bool
}

type historyReadAheadTask struct {
	keys [][]byte
	read func(context.Context, [][]byte, func([]byte, *rawdb.StateDomainChange) (bool, error)) error
	done chan historyReadAheadResult
}

type historyOrderedReads struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	queue  []*historyReadAheadTask
	match  func([]byte, *rawdb.StateDomainChange) bool
}

func newHistoryOrderedReads(ctx context.Context, match func([]byte, *rawdb.StateDomainChange) bool) *historyOrderedReads {
	ctx, cancel := context.WithCancel(ctx)
	return &historyOrderedReads{ctx: ctx, cancel: cancel, match: match}
}

func (p *historyOrderedReads) close() {
	p.cancel()
	p.wg.Wait()
}

func (p *historyOrderedReads) submit(t *historyReadAheadTask) {
	t.done = make(chan historyReadAheadResult, 1)
	p.queue = append(p.queue, t)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		select {
		case historyReadAheadAdmission <- struct{}{}:
			defer func() { <-historyReadAheadAdmission }()
		case <-p.ctx.Done():
			t.done <- historyReadAheadResult{err: p.ctx.Err()}
			return
		}
		var result historyReadAheadResult
		cost := 0
		result.err = t.read(p.ctx, t.keys, func(key []byte, change *rawdb.StateDomainChange) (bool, error) {
			if !p.match(key, change) {
				return true, nil
			}
			n := len(key) + len(change.Key) + len(change.Prev) + len(change.Next) + 256
			if n > historyReadAheadResultBytes-cost {
				return false, errHistoryReadAheadResultBudget
			}
			cost += n
			result.rows = append(result.rows, historyReadAheadRow{key, cloneStateDomainChangeForSegment(change)})
			return false, nil
		})
		if errors.Is(result.err, errHistoryReadAheadResultBudget) {
			result = historyReadAheadResult{retry: true}
		}
		t.done <- result
	}()
}

// Only the consumer observes/resolves the query state. Workers never race with
// entry.first and speculative errors are invisible for already resolved keys.
func (p *historyOrderedReads) consume(active func([]byte) bool, emit func([]byte, *rawdb.StateDomainChange) (bool, error)) error {
	t := p.queue[0]
	p.queue[0] = nil
	p.queue = p.queue[1:]
	var result historyReadAheadResult
	select {
	case result = <-t.done:
	case <-p.ctx.Done():
		return p.ctx.Err()
	}
	keys := make([][]byte, 0, len(t.keys))
	for _, key := range t.keys {
		if active(key) {
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return nil
	}
	if result.retry || len(keys) != len(t.keys) {
		select {
		case historyReadAheadAdmission <- struct{}{}:
			defer func() { <-historyReadAheadAdmission }()
		case <-p.ctx.Done():
			return p.ctx.Err()
		}
		return t.read(p.ctx, keys, emit)
	}
	if result.err != nil {
		return result.err
	}
	for _, row := range result.rows {
		if _, err := emit(row.key, row.change); err != nil {
			return err
		}
	}
	return p.ctx.Err()
}
