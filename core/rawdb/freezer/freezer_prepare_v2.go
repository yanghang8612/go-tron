package freezer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
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
}

// v2PreparedReader keeps source reads and delivery ordered. Only prepare runs
// concurrently, over private input bytes. Workers join before Read returns, so
// dictionary sampling, verification and source errors cannot leave background
// readers or transforms alive. It deliberately does not retain decoded graphs.
//
// A batch owns at most 128 inputs and 16 MiB of input allocations, plus one
// lookahead record (whose size cannot be known before reading it). An input
// larger than the byte budget runs alone. Transform results and transient
// decoder allocations are additional; this is not a process RSS limit.
type v2PreparedReader struct {
	ctx     context.Context
	next    uint64
	end     uint64
	workers int
	load    func(uint64) ([]byte, []byte, error)
	prepare func(uint64, []byte, []byte) ([]byte, error)
	batch   []v2PreparationInput
	index   int
	carry   *v2PreparationInput
}

func newV2PreparedReader(ctx context.Context, start, count uint64, workers int, load func(uint64) ([]byte, []byte, error), prepare func(uint64, []byte, []byte) ([]byte, error)) *v2PreparedReader {
	return &v2PreparedReader{ctx: ctx, next: start, end: start + count, workers: max(1, min(workers, 8)), load: load, prepare: prepare}
}

func (r *v2PreparedReader) Read(number uint64) ([]byte, error) {
	if err := r.ctx.Err(); err != nil {
		return nil, err
	}
	if number != r.next || number >= r.end {
		return nil, fmt.Errorf("ancient V2 preparation: read %d, expected %d below %d", number, r.next, r.end)
	}
	if r.index == len(r.batch) {
		if err := r.fill(); err != nil {
			return nil, err
		}
	}
	item := &r.batch[r.index]
	data, err := item.result, item.err
	*item = v2PreparationInput{}
	r.index++
	r.next++
	return data, err
}

func (r *v2PreparedReader) fill() error {
	r.batch = r.batch[:0]
	r.index = 0
	var owned uint64
	for number := r.next; number < r.end && len(r.batch) < v2PreparationBatchRecords; number++ {
		if err := r.ctx.Err(); err != nil {
			return err
		}
		var item v2PreparationInput
		if r.carry != nil {
			item, r.carry = *r.carry, nil
		} else {
			data, body, err := r.load(number)
			if err != nil {
				return err
			}
			item = v2PreparationInput{number: number, data: data, body: body}
		}
		footprint := uint64(cap(item.data)) + uint64(cap(item.body))
		if len(r.batch) != 0 && owned+footprint > v2PreparationBatchBytes {
			r.carry = &item
			break
		}
		r.batch = append(r.batch, item)
		owned += footprint
		if owned >= v2PreparationBatchBytes {
			break
		}
	}
	var next atomic.Uint32
	run := func() {
		for {
			i := int(next.Add(1) - 1)
			if i >= len(r.batch) {
				return
			}
			item := &r.batch[i]
			if err := r.ctx.Err(); err != nil {
				item.err = err
			} else {
				item.result, item.err = r.prepare(item.number, item.data, item.body)
			}
			item.data, item.body = nil, nil
		}
	}
	var wg sync.WaitGroup
	for i := 1; i < min(r.workers, len(r.batch)); i++ {
		wg.Add(1)
		go func() { defer wg.Done(); run() }()
	}
	run()
	wg.Wait()
	return r.ctx.Err()
}
