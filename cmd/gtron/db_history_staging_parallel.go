package main

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	statesnapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/urfave/cli/v2"
)

const historyStagingMaxPlanWorkers = 8

func historyStagingPlanWorkerCount(ctx *cli.Context) (int, error) {
	requested := ctx.Uint("plan-workers")
	if requested > historyStagingMaxPlanWorkers {
		return 0, fmt.Errorf("history staging --plan-workers exceeds %d", historyStagingMaxPlanWorkers)
	}
	workers := int(requested)
	if workers == 0 {
		workers = historyStagingMaxPlanWorkers
	}
	if cores := runtime.GOMAXPROCS(0); workers > cores {
		workers = cores
	}
	return workers, nil
}

type historyStagingPlanWorker struct {
	hotView rawdb.StateHistoryReadView
	release func() error
	prover  *statesnapshots.HistoryStagingColdProver
	chain   *rawdb.ChainDB
}

func (w *historyStagingPlanWorker) close() {
	if w == nil {
		return
	}
	if w.prover != nil {
		_ = w.prover.Close()
	}
	if w.release != nil {
		_ = w.release()
	}
}

func (w *historyStagingPlanWorker) build(ctx context.Context, bucket, eligible, pruneTx uint64, boundary offlineChainBoundary,
	index rawdb.StageProgress, limits rawdb.HistoryStagingLimits) (historyStagingPlanBucket, error) {
	var row historyStagingPlanBucket
	first, last, err := rawdb.StateHistoryChunkBucketBounds(bucket)
	if err != nil {
		return row, err
	}
	proof := rawdb.HistoryStagingProof{Bucket: bucket, Epoch: 1,
		EligibleThrough: eligible, FinishBlock: boundary.HeadBlock,
		FinishHash: boundary.HeadHash, IndexBlock: index.BlockNum,
		IndexHash: index.BlockHash,
		Blocks:    make([]rawdb.HistoryStagingBlockProof, 0, rawdb.StateHistoryChunkBucketBlocks)}
	missing := make([]bool, rawdb.StateHistoryChunkBucketBlocks)
	for number := first; number <= last; number++ {
		if err := ctx.Err(); err != nil {
			return row, err
		}
		canonicalHash, present, err := rawdb.ReadBlockHashByNumberStrict(w.chain, number)
		if err != nil || !present || canonicalHash == (common.Hash{}) {
			return row, fmt.Errorf("history staging canonical block %d unavailable: %w", number, err)
		}
		rangeRow, present, err := rawdb.ReadStateTxRange(w.hotView, number)
		if err != nil || !present || rangeRow == nil || rangeRow.BlockHash != canonicalHash {
			return row, fmt.Errorf("history staging tx range %d unavailable or noncanonical: %w", number, err)
		}
		proof.Blocks = append(proof.Blocks, rawdb.HistoryStagingBlockProof{
			Number: number, Hash: canonicalHash,
			BeginTxNum: rangeRow.BeginTxNum, EndTxNum: rangeRow.EndTxNum})
		missing[number-first] = rangeRow.EndTxNum <= pruneTx
	}
	proof.ColdSpans, err = w.prover.Build(ctx, proof.Blocks, missing)
	if err != nil {
		return row, fmt.Errorf("history staging bucket %d cold proof: %w", bucket, err)
	}
	if err := rawdb.VerifyHistoryStagingProof(proof); err != nil {
		return row, err
	}
	physical, err := rawdb.InspectHistoryStagingPhysicalBucket(ctx, w.hotView, proof, limits)
	if err != nil {
		return row, fmt.Errorf("history staging bucket %d physical inventory: %w", bucket, err)
	}
	if physical.Bytes > rawdb.HistoryStagingMaxCopyPhysicalBytes(limits.MaxWorkBytes) {
		return row, fmt.Errorf("history staging bucket %d exceeds bounded scan/copy/verify work budget", bucket)
	}
	return historyStagingPlanBucket{Proof: proof, Physical: physical}, nil
}

type historyStagingPlanResult struct {
	bucket uint64
	row    historyStagingPlanBucket
	err    error
}

// writeHistoryStagingPlanBuckets bounds all dispatched or buffered results to
// workers, while the caller alone writes complete rows in bucket order. Every
// return path cancels and joins workers before their snapshots may be closed.
func writeHistoryStagingPlanBuckets(ctx context.Context, lastBucket uint64, workers int,
	build func(context.Context, int, uint64) (historyStagingPlanBucket, error),
	write func(historyStagingPlanBucket) error, progress *historyStagingCLIProgress) error {
	if ctx == nil || workers < 1 || workers > historyStagingMaxPlanWorkers || build == nil || write == nil {
		return errors.New("history staging invalid bounded planner configuration")
	}
	workCtx, cancel := context.WithCancel(ctx)
	tasks := make(chan uint64, workers)
	results := make(chan historyStagingPlanResult, workers)
	var group sync.WaitGroup
	for id := 0; id < workers; id++ {
		group.Add(1)
		go func(id int) {
			defer group.Done()
			for {
				select {
				case <-workCtx.Done():
					return
				case bucket, ok := <-tasks:
					if !ok {
						return
					}
					row, err := build(workCtx, id, bucket)
					select {
					case results <- historyStagingPlanResult{bucket, row, err}:
					case <-workCtx.Done():
						return
					}
				}
			}
		}(id)
	}
	defer func() {
		cancel()
		close(tasks)
		group.Wait()
	}()
	pending := make(map[uint64]historyStagingPlanResult, workers)
	nextDispatch, nextWrite := uint64(1), uint64(1)
	active := 0 // dispatched buckets not yet received
	stopDispatch := false
	for nextWrite <= lastBucket {
		for !stopDispatch && nextDispatch <= lastBucket && active+len(pending) < workers {
			select {
			case <-workCtx.Done():
				return workCtx.Err()
			case tasks <- nextDispatch:
				active++
				nextDispatch++
			}
		}
		if progress != nil {
			progress.bucket.Store(nextWrite)
		}
		var result historyStagingPlanResult
		select {
		case <-workCtx.Done():
			return workCtx.Err()
		case result = <-results:
		}
		active--
		pending[result.bucket] = result
		if result.err != nil {
			stopDispatch = true
		}
		for {
			ready, ok := pending[nextWrite]
			if !ok {
				break
			}
			if ready.err != nil {
				return ready.err
			}
			if err := workCtx.Err(); err != nil {
				return err
			}
			if err := write(ready.row); err != nil {
				return err
			}
			delete(pending, nextWrite)
			nextWrite++
			if progress != nil {
				progress.completed.Store(nextWrite - 1)
			}
		}
	}
	return nil
}
