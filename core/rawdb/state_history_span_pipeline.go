package rawdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/pointread"
)

const stateHistorySpanPipelinePreflightBlocks = 5000

// IterateStateHistorySpanBlocksWithWorkers is an explicit 2/4/8-slot variant
// for an audited concurrent, owned, immutable pinned view. The original serial
// API is unchanged. A bounded metadata-only preflight (at most 5000 blocks)
// selects the serial API for repairs, legacy encodings, empty blocks or unusual
// range shapes; it does not materialize or authenticate any pack. Preflight I/O
// errors select serial replay so future failures cannot override earlier stops.
//
// Only complete shared-pack authentication runs in parallel. Every block still
// has its complete chunk and pack checks, with original per-pack Has/Get order.
// Global speculative reads may interleave. RLP validation and callbacks remain
// ordered; future errors cannot cancel predecessors. Parent cancellation joins
// all workers before returning to the caller that owns the view. Results waiting
// for callbacks share the existing 256MiB materialization budget; a 128MiB pack
// runs alone. Encoded ref tables, per-read codec buffers and a compatibility
// inner Snappy buffer (up to 128MiB during consumption) are additional, not RSS.
func IterateStateHistorySpanBlocksWithWorkers(ctx context.Context, view StateHistoryReadView, fromBlock, toBlock, fromTx, toTx uint64, workers int, fn func(*StateHistorySpanBlock) (bool, error)) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if view == nil || !view.IsPinnedKeyValueView() {
		return ErrStateHistoryReadViewUnpinned
	}
	if workers != 2 && workers != 4 && workers != 8 {
		return ErrStateHistoryPipelineWorkers
	}
	capability, ok := view.(pointread.ConcurrentOwnedKeyValueView)
	if !ok || !capability.GetReturnsOwnedBytes() || !capability.ConcurrentOwnedHistoryReads() {
		return ErrStateHistoryPipelineView
	}
	if _, ok := view.(interface {
		GetWithPresence([]byte) ([]byte, bool, error)
	}); ok {
		return ErrStateHistoryPipelineView
	}
	if fn == nil {
		return errors.New("rawdb: nil history span block callback")
	}
	if fromBlock > toBlock || fromTx > toTx {
		return errors.New("rawdb: inverted history span range")
	}
	if !stateHistorySpanCanPipeline(ctx, view, fromBlock, toBlock, fromTx, toTx) {
		return IterateStateHistorySpanBlocks(ctx, view, fromBlock, toBlock, fromTx, toTx, fn)
	}
	source := newStateHistorySpanPipelineSource(ctx, view, fromBlock, toBlock, fromTx, toTx)
	defer source.close()
	workCtx, cancel := context.WithCancel(ctx)
	reader := historyPipelineReader{StateHistoryReadView: view, ctx: workCtx}
	var queue []*stateHistorySpanPipelineJob
	defer func() {
		cancel()
		for _, job := range queue {
			<-job.done
			job.raw = nil
			job.encoded = nil
		}
	}()
	var pending *stateHistorySpanPipelineJob
	var sourceErr error
	var ended, exclusive bool
	var retained uint64
	knownFailure := func() bool {
		for _, job := range queue {
			select {
			case <-job.done:
				if job.err != nil {
					return true
				}
			default:
			}
		}
		return false
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		for len(queue) < workers && !ended && !knownFailure() {
			if pending == nil {
				pending, sourceErr = source.next(true)
				if pending == nil {
					ended = true
					break
				}
			}
			if !historyPipelineCanAdmit(retained, len(queue), pending.decoded, StateHistoryPipelineDecodedBudget, workers, exclusive) {
				break
			}
			job := pending
			pending = nil
			retained += job.decoded
			exclusive = job.decoded >= StateHistoryPipelineDecodedBudget/2
			job.done = make(chan struct{})
			queue = append(queue, job)
			go func() {
				defer close(job.done)
				job.raw, job.err = materializeStateHistorySharedPack(job.encoded, job.info.BlockNum, []ethdb.KeyValueReader{reader})
			}()
		}
		if len(queue) == 0 {
			if pending != nil {
				return errors.New("rawdb: span pipeline source changed after preflight")
			}
			if ended {
				return sourceErr
			}
			continue
		}
		job := queue[0]
		select {
		case <-job.done:
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		cont, err := consumeStateHistorySpanPipelineJob(ctx, job, fromTx, toTx, fn)
		// consume invalidates all block aliases before releasing the accounting
		// reservation; a callback never outlives its charged materialized bytes.
		job.raw = nil
		job.encoded = nil
		retained -= job.decoded
		if job.decoded >= StateHistoryPipelineDecodedBudget/2 {
			exclusive = false
		}
		queue[0] = nil
		queue = queue[1:]
		if err != nil || !cont {
			return err
		}
	}
}

func consumeStateHistorySpanPipelineJob(ctx context.Context, job *stateHistorySpanPipelineJob, fromTx, toTx uint64, fn func(*StateHistorySpanBlock) (bool, error)) (bool, error) {
	if job.err != nil {
		return false, job.err
	}
	b := &StateHistorySpanBlock{ctx: ctx, info: job.info, active: true, fromTx: fromTx, toTx: toTx}
	defer b.discard()
	if err := b.initializeAuthenticatedPack(job.encoded, job.raw, true); err != nil {
		return false, err
	}
	// Serial add authenticates/decodes this pack before advancing its changeset
	// iterator. Preserve that precedence even when the look-ahead read failed.
	if job.boundaryErr != nil {
		return false, job.boundaryErr
	}
	if err := b.finish(); err != nil {
		return false, err
	}
	cont, err := fn(b)
	if err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if !cont {
		return false, nil
	}
	return true, job.afterCallbackErr
}

type stateHistorySpanPipelineJob struct {
	info                               StateHistorySpanBlockInfo
	decoded                            uint64
	encoded, raw                       []byte
	done                               chan struct{}
	err, boundaryErr, afterCallbackErr error
}

type stateHistorySpanPipelineSource struct {
	ctx                                                     context.Context
	ranges, changes                                         ethdb.Iterator
	fromBlock, toBlock, fromTx, toTx, expected, previousEnd uint64
	started, haveChange, first, ended                       bool
}

func newStateHistorySpanPipelineSource(ctx context.Context, view StateHistoryReadView, fromBlock, toBlock, fromTx, toTx uint64) *stateHistorySpanPipelineSource {
	var start [8]byte
	binary.BigEndian.PutUint64(start[:], fromBlock)
	return &stateHistorySpanPipelineSource{ctx: ctx, ranges: view.NewIterator(stateTxRangePrefix, start[:]), changes: view.NewIterator(stateChangeSetPrefix, start[:]), fromBlock: fromBlock, toBlock: toBlock, fromTx: fromTx, toTx: toTx, expected: fromBlock, first: true}
}
func (s *stateHistorySpanPipelineSource) close() { s.changes.Release(); s.ranges.Release() }
func (s *stateHistorySpanPipelineSource) advance() error {
	if err := s.ctx.Err(); err != nil {
		return err
	}
	s.haveChange = s.changes.Next()
	if err := s.ctx.Err(); err != nil {
		return err
	}
	if !s.haveChange {
		return s.changes.Error()
	}
	return nil
}
func (s *stateHistorySpanPipelineSource) next(own bool) (*stateHistorySpanPipelineJob, error) {
	if err := s.ctx.Err(); err != nil {
		return nil, err
	}
	if s.ended {
		return nil, nil
	}
	if !s.started {
		s.started = true
		if err := s.advance(); err != nil {
			return nil, err
		}
	}
	if !s.ranges.Next() {
		s.ended = true
		if err := s.ranges.Error(); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("rawdb: missing history span tx range at block %d", s.expected)
	}
	key := s.ranges.Key()
	if !bytes.HasPrefix(key, stateTxRangePrefix) || len(key) != len(stateTxRangePrefix)+8 {
		return nil, errors.New("rawdb: invalid history span tx-range key")
	}
	block := binary.BigEndian.Uint64(key[len(stateTxRangePrefix):])
	if block != s.expected {
		return nil, fmt.Errorf("rawdb: missing history span tx range at block %d", s.expected)
	}
	hash, begin, end, err := decodeBorrowedStateTxRange(s.ranges.Value(), block)
	if err != nil {
		return nil, err
	}
	if !s.first && (s.previousEnd == ^uint64(0) || begin != s.previousEnd+1) {
		return nil, errors.New("rawdb: history span tx ranges are not contiguous")
	}
	if end < s.fromTx || begin > s.toTx || !s.haveChange {
		return nil, errors.New("rawdb: span pipeline requires serial range fallback")
	}
	key = s.changes.Key()
	if !bytes.HasPrefix(key, stateChangeSetPrefix) || len(key) != len(stateChangeSetPrefix)+16 || binary.BigEndian.Uint64(key[len(stateChangeSetPrefix):]) != block || binary.BigEndian.Uint64(key[len(stateChangeSetPrefix)+8:]) != 0 {
		return nil, errors.New("rawdb: span pipeline requires serial changeset fallback")
	}
	encoded := s.changes.Value()
	if !isStateHistorySharedPack(encoded) || len(encoded) > stateDomainChangeBlockMaxDecodedBytes {
		return nil, errors.New("rawdb: span pipeline requires serial codec fallback")
	}
	_, decoded, _, _, err := sharedStateHistoryPackHeader(encoded, block)
	if err != nil {
		return nil, err
	}
	job := &stateHistorySpanPipelineJob{info: StateHistorySpanBlockInfo{BlockNum: block, BlockHash: hash, BeginTxNum: begin, EndTxNum: end}, decoded: uint64(decoded)}
	if own {
		job.encoded = bytes.Clone(encoded)
	}
	job.boundaryErr = s.advance()
	if job.boundaryErr == nil && s.haveChange {
		key = s.changes.Key()
		if !bytes.HasPrefix(key, stateChangeSetPrefix) || len(key) != len(stateChangeSetPrefix)+16 {
			job.boundaryErr = errors.New("rawdb: invalid history span changeset key")
		} else if binary.BigEndian.Uint64(key[len(stateChangeSetPrefix):]) <= block {
			job.boundaryErr = errors.New("rawdb: span pipeline requires serial repair fallback")
		}
	}
	if job.boundaryErr != nil {
		s.ended = true
	}
	s.first = false
	s.previousEnd = end
	if block == s.toBlock {
		s.ended = true
		job.afterCallbackErr = s.ranges.Error()
	} else {
		s.expected = block + 1
	}
	return job, nil
}

func stateHistorySpanCanPipeline(ctx context.Context, view StateHistoryReadView, fromBlock, toBlock, fromTx, toTx uint64) bool {
	if toBlock-fromBlock >= stateHistorySpanPipelinePreflightBlocks {
		return false
	}
	source := newStateHistorySpanPipelineSource(ctx, view, fromBlock, toBlock, fromTx, toTx)
	defer source.close()
	for {
		job, err := source.next(false)
		if err != nil {
			return false
		}
		if job == nil {
			return true
		}
		if job.boundaryErr != nil || job.afterCallbackErr != nil || job.decoded == 0 || job.decoded > StateHistoryPipelineDecodedBudget/2 {
			return false
		}
	}
}
