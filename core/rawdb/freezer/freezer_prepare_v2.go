package freezer

import (
	"context"
	"fmt"
	"sync"
)

const (
	v2PreparationBatchRecords = 128
	v2PreparationBatchBytes   = 16 << 20
)

type v2PreparationInput struct {
	number     uint64
	data, body []byte
	result     []byte
	err        error
	footprint  uint64
	ready      chan struct{}
}

// v2PreparedReader overlaps one serial source producer with CPU-only prepare
// workers and delivers results in source order. load must return owned bytes;
// prepare may borrow those bytes for its result. Dictionary sampling must finish
// before the first Read. Close joins all source/prepare work and is mandatory
// before verification, another table or returning from the enclosing writer.
// Read also closes on error, cancellation and the final record.
//
// Pending inputs/results use at most 128 reusable slots. Admitted input
// allocations are capped at 16 MiB, plus one already-read lookahead input whose
// size could not be known in advance. An oversized input waits for the other
// slots to drain and runs alone, without reading ahead. Results and transient
// decoder allocations are additional; this is not a process RSS limit.
// Read has one consumer. Close may run concurrently with it.
type v2PreparedReader struct {
	ctx     context.Context
	cancel  context.CancelFunc
	next    uint64
	end     uint64
	workers int
	load    func(uint64) ([]byte, []byte, error)
	prepare func(uint64, []byte, []byte) ([]byte, error)
	jobs    chan *v2PreparationInput
	ordered chan *v2PreparationInput
	free    chan *v2PreparationInput
	changed chan struct{}
	wg      sync.WaitGroup

	mu         sync.Mutex
	started    bool
	closed     bool
	ownedRows  int
	ownedBytes uint64
}

func newV2PreparedReader(ctx context.Context, start, count uint64, workers int, load func(uint64) ([]byte, []byte, error), prepare func(uint64, []byte, []byte) ([]byte, error)) *v2PreparedReader {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	r := &v2PreparedReader{
		ctx: ctx, cancel: cancel, next: start, end: start + count,
		workers: max(1, min(workers, 8)), load: load, prepare: prepare,
		jobs:    make(chan *v2PreparationInput, v2PreparationBatchRecords),
		ordered: make(chan *v2PreparationInput, v2PreparationBatchRecords),
		free:    make(chan *v2PreparationInput, v2PreparationBatchRecords), changed: make(chan struct{}, 1),
	}
	for range v2PreparationBatchRecords {
		r.free <- &v2PreparationInput{ready: make(chan struct{}, 1)}
	}
	return r
}

func (r *v2PreparedReader) start() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started || r.closed {
		return
	}
	r.started = true
	// Add before exposing started to Close, so Wait cannot race a future Add.
	r.wg.Add(r.workers + 1)
	for range r.workers {
		go r.work()
	}
	go r.produce(r.next)
}

func (r *v2PreparedReader) Read(number uint64) ([]byte, error) {
	fail := func(err error) ([]byte, error) { r.Close(); return nil, err }
	if err := r.ctx.Err(); err != nil {
		return fail(err)
	}
	if number != r.next || number >= r.end {
		return fail(fmt.Errorf("ancient V2 preparation: read %d, expected %d below %d", number, r.next, r.end))
	}
	r.start()
	var item *v2PreparationInput
	select {
	case <-r.ctx.Done():
		return fail(r.ctx.Err())
	case next, ok := <-r.ordered:
		if !ok {
			if err := r.ctx.Err(); err != nil {
				return fail(err)
			}
			return fail(fmt.Errorf("ancient V2 preparation: source ended before %d", number))
		}
		item = next
	}
	select {
	case <-r.ctx.Done():
		r.Close()
		<-item.ready // Close joined its worker, including an in-flight prepare.
		r.release(item)
		return nil, r.ctx.Err()
	case <-item.ready:
	}
	data, err := item.result, item.err
	r.release(item)
	if ctxErr := r.ctx.Err(); ctxErr != nil {
		return fail(ctxErr)
	}
	r.next++
	if err != nil {
		return fail(err)
	}
	if r.next == r.end {
		r.Close()
	}
	return data, nil
}

// Close cancels prefetch and waits even for a source/transform already running.
// Such callbacks cannot be forcibly interrupted; they must eventually return.
func (r *v2PreparedReader) Close() {
	r.mu.Lock()
	r.closed = true
	r.cancel()
	started := r.started
	r.mu.Unlock()
	if !started {
		return
	}
	r.wg.Wait()
	for item := range r.ordered {
		<-item.ready
		r.release(item)
	}
}

// room waits before loading another record, preventing lookahead while a giant
// input is admitted. reserve separately handles an unexpectedly large input.
func (r *v2PreparedReader) room() bool {
	for r.ctx.Err() == nil {
		r.mu.Lock()
		ok := r.ownedRows < v2PreparationBatchRecords && r.ownedBytes < v2PreparationBatchBytes
		r.mu.Unlock()
		if ok {
			return true
		}
		select {
		case <-r.ctx.Done():
		case <-r.changed:
		}
	}
	return false
}

func (r *v2PreparedReader) reserve(item *v2PreparationInput) bool {
	for r.ctx.Err() == nil {
		r.mu.Lock()
		ok := r.ownedRows == 0 || (r.ownedRows < v2PreparationBatchRecords && r.ownedBytes <= v2PreparationBatchBytes && item.footprint <= v2PreparationBatchBytes-r.ownedBytes)
		if ok {
			r.ownedRows++
			r.ownedBytes += item.footprint
		}
		r.mu.Unlock()
		if ok {
			return true
		}
		select {
		case <-r.ctx.Done():
		case <-r.changed:
		}
	}
	return false
}

func (r *v2PreparedReader) recycle(item *v2PreparationInput) {
	*item = v2PreparationInput{ready: item.ready}
	r.free <- item
}

func (r *v2PreparedReader) release(item *v2PreparationInput) {
	r.mu.Lock()
	r.ownedRows--
	r.ownedBytes -= item.footprint
	r.mu.Unlock()
	r.recycle(item)
	select {
	case r.changed <- struct{}{}:
	default:
	}
}

func (r *v2PreparedReader) produce(start uint64) {
	defer r.wg.Done()
	defer close(r.ordered)
	defer close(r.jobs)
	for number := start; number < r.end && r.room(); number++ {
		var item *v2PreparationInput
		select {
		case <-r.ctx.Done():
			return
		case item = <-r.free:
		}
		if r.ctx.Err() != nil {
			r.recycle(item)
			return
		}
		item.number = number
		item.data, item.body, item.err = r.load(number)
		item.footprint = uint64(cap(item.data)) + uint64(cap(item.body))
		if !r.reserve(item) {
			r.recycle(item)
			return
		}
		select {
		case <-r.ctx.Done():
			r.release(item)
			return
		case r.ordered <- item:
		}
		if item.err != nil {
			item.ready <- struct{}{}
			return
		}
		select {
		case <-r.ctx.Done():
			item.err = r.ctx.Err()
			item.data, item.body = nil, nil
			item.ready <- struct{}{}
			return
		case r.jobs <- item:
		}
	}
}

func (r *v2PreparedReader) work() {
	defer r.wg.Done()
	for item := range r.jobs {
		if err := r.ctx.Err(); err != nil {
			item.err = err
		} else {
			item.result, item.err = r.prepare(item.number, item.data, item.body)
		}
		item.data, item.body = nil, nil
		item.ready <- struct{}{}
	}
}
