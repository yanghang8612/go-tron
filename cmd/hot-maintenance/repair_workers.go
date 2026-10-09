package main

import (
	"context"
	"errors"
	"sync"
)

// repairParallelOrdered runs only independent, preplanned read/build work.
// Results keep their original ordinal; cancellation joins every worker before
// callers can publish a journal, manifest, binding, or sidecar.
func repairParallelOrdered[T any](ctx context.Context, workers, count int, work func(context.Context, int) (T, error)) ([]T, error) {
	if ctx == nil || workers < 1 || work == nil {
		return nil, errors.New("repair: invalid bounded worker plan")
	}
	results := make([]T, count)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if count == 0 {
		return results, nil
	}
	if workers == 1 {
		for index := range count {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			value, err := work(ctx, index)
			if err != nil {
				return nil, err
			}
			results[index] = value
		}
		return results, ctx.Err()
	}
	if workers > count {
		workers = count
	}
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int)
	var group sync.WaitGroup
	var once sync.Once
	var firstErr error
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			for index := range jobs {
				if err := workCtx.Err(); err != nil {
					return
				}
				value, err := work(workCtx, index)
				if err != nil {
					once.Do(func() { firstErr = err; cancel() })
					return
				}
				results[index] = value
			}
		}()
	}
dispatch:
	for index := range count {
		select {
		case <-workCtx.Done():
			break dispatch
		case jobs <- index:
		}
	}
	close(jobs)
	group.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return results, nil
}
