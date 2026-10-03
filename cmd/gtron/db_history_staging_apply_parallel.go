package main

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"

	"github.com/tronprotocol/go-tron/core/rawdb"
	statesnapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/urfave/cli/v2"
)

const historyStagingMaxApplyWorkers = 8

func historyStagingApplyWorkerCount(ctx *cli.Context) (int, error) {
	requested := ctx.Uint("apply-workers")
	if requested > historyStagingMaxApplyWorkers {
		return 0, fmt.Errorf("history staging --apply-workers exceeds %d", historyStagingMaxApplyWorkers)
	}
	workers := int(requested)
	if workers == 0 {
		workers = historyStagingMaxApplyWorkers
	}
	if cores := runtime.GOMAXPROCS(0); workers > cores {
		workers = cores
	}
	return workers, nil
}

type historyStagingApplyResult struct {
	sequence uint64
	row      historyStagingPlanBucket
	check    func(context.Context) error
	err      error
}

// The complete dispatch/result window is bounded by workers. Only the caller
// reads the plan and consumes results (and therefore mutates stores), in order.
// Cancellation joins every worker before any worker-local view is released.
func pipelineHistoryStagingRows(ctx context.Context, workers int,
	next func(context.Context) (historyStagingPlanBucket, bool, error),
	prepare func(context.Context, int, historyStagingPlanBucket) (func(context.Context) error, error),
	consume func(historyStagingPlanBucket, func(context.Context) error) error) error {
	if ctx == nil || workers < 1 || workers > historyStagingMaxApplyWorkers || next == nil || prepare == nil || consume == nil {
		return errors.New("history staging invalid bounded apply pipeline")
	}
	workCtx, cancel := context.WithCancel(ctx)
	tasks := make(chan historyStagingApplyResult, workers)
	results := make(chan historyStagingApplyResult, workers)
	var group sync.WaitGroup
	for id := 0; id < workers; id++ {
		group.Add(1)
		go func(id int) {
			defer group.Done()
			for {
				select {
				case <-workCtx.Done():
					return
				case task, ok := <-tasks:
					if !ok {
						return
					}
					task.check, task.err = prepare(workCtx, id, task.row)
					select {
					case results <- task:
					case <-workCtx.Done():
						return
					}
				}
			}
		}(id)
	}
	defer func() { cancel(); close(tasks); group.Wait() }()
	pending := make(map[uint64]historyStagingApplyResult, workers)
	var dispatched, consumed uint64
	active, ended := 0, false
	for {
		for !ended && active+len(pending) < workers {
			row, more, err := next(workCtx)
			if err != nil {
				return err
			}
			if !more {
				ended = true
				break
			}
			select {
			case <-workCtx.Done():
				return workCtx.Err()
			case tasks <- historyStagingApplyResult{sequence: dispatched, row: row}:
				dispatched++
				active++
			}
		}
		if active == 0 && len(pending) == 0 {
			return workCtx.Err()
		}
		var result historyStagingApplyResult
		select {
		case <-workCtx.Done():
			return workCtx.Err()
		case result = <-results:
		}
		active--
		pending[result.sequence] = result
		if result.err != nil {
			ended = true
		}
		for {
			ready, ok := pending[consumed]
			if !ok {
				break
			}
			if ready.err != nil {
				return fmt.Errorf("history staging bucket %d: %w", ready.row.Proof.Bucket, ready.err)
			}
			if err := workCtx.Err(); err != nil {
				return err
			}
			if err := consume(ready.row, ready.check); err != nil {
				return fmt.Errorf("history staging bucket %d: %w", ready.row.Proof.Bucket, err)
			}
			delete(pending, consumed)
			consumed++
		}
	}
}

func (s *historyStagingApplySession) coldWorkers(count int) ([]*statesnapshots.HistoryStagingColdProver, func(), error) {
	workers := make([]*statesnapshots.HistoryStagingColdProver, 0, count)
	closeWorkers := func() {
		for _, p := range workers {
			_ = p.Close()
		}
	}
	for i := 0; i < count; i++ {
		p, err := statesnapshots.NewHistoryStagingColdProver(s.cli.paths.Cold, s.manifest)
		if err != nil {
			closeWorkers()
			return nil, nil, err
		}
		p.EnableReaderReuse()
		workers = append(workers, p)
	}
	return workers, closeWorkers, nil
}

func coldProofBinding(proof rawdb.HistoryStagingProof) rawdb.HistoryStagingColdBinding {
	return rawdb.HistoryStagingColdBinding{Bucket: proof.Bucket, Spans: proof.ColdSpans}
}
