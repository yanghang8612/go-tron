package rawdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/pointread"
)

const StateHistoryPipelineDecodedBudget = uint64(256 << 20)
const stateHistoryPipelineSlots = 2

var ErrStateHistoryPipelineView = errors.New("rawdb: history pipeline requires explicit concurrent pinned owned reads")
var ErrStateHistoryPipelineWorkers = errors.New("rawdb: history pipeline workers must be 2, 4 or 8")

// IterateStateDomainChangesByBlockTxRangePipelined is an opt-in cold-build
// experiment. Default public readers remain serial. Only shared-pack byte
// materialization is parallel: each block retains every original Has/Get and
// chunk/pack SHA, while RLP rows and callbacks are consumed in original order.
// Global speculative read interleaving differs from the serial reader. Later
// failures never cancel predecessors; early-stop/callback errors take precedence
// over future work. Parent cancellation joins all workers before releasing the
// view. At most two blocks and 256MiB declared decoded output are in flight,
// including completed results awaiting consumption. A maximum-sized block runs
// alone. This bounds shared materialization only: a compatibility inner Snappy
// envelope can additionally expand up to the original 128MiB limit during serial
// consumption. Encoded envelopes, per-read chunk bytes and runtime overhead are
// extra; this is not a total heap/RSS cap. Non-shared inputs retain serial semantics.
func IterateStateDomainChangesByBlockTxRangePipelined(ctx context.Context, db ethdb.Iteratee, fromBlock, toBlock, fromTxNum, toTxNum uint64, fn func(*StateDomainChange) (bool, error)) (resultErr error) {
	return IterateStateDomainChangesByBlockTxRangePipelinedWithWorkers(ctx, db, fromBlock, toBlock, fromTxNum, toTxNum, stateHistoryPipelineSlots, fn)
}

// IterateStateDomainChangesByBlockTxRangePipelinedWithWorkers is the explicit
// offline 2/4/8-slot variant. The default wrapper remains two slots. All slots,
// including results waiting for ordered consumption, share the same 256MiB
// declared-output budget. Increasing slots permits more speculative reads;
// chunk/encoded/runtime memory is additional. It adds no production enablement.
func IterateStateDomainChangesByBlockTxRangePipelinedWithWorkers(ctx context.Context, db ethdb.Iteratee, fromBlock, toBlock, fromTxNum, toTxNum uint64, workers int, fn func(*StateDomainChange) (bool, error)) (resultErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if workers != 2 && workers != 4 && workers != 8 {
		return ErrStateHistoryPipelineWorkers
	}
	view, release, err := AcquireStateHistoryReadView(db)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, release()) }()
	capability, ok := view.(pointread.ConcurrentOwnedKeyValueView)
	if !ok || !capability.IsPinnedKeyValueView() || !capability.GetReturnsOwnedBytes() || !capability.ConcurrentOwnedHistoryReads() {
		return ErrStateHistoryPipelineView
	}
	// This experiment audits the private Pebble snapshot's Has/Get path only.
	// Presence-coupled adapters may have different ownership/lifetime contracts;
	// reject them explicitly instead of silently replacing their read semantics.
	if _, ok := view.(interface {
		GetWithPresence([]byte) ([]byte, bool, error)
	}); ok {
		return ErrStateHistoryPipelineView
	}
	if fn == nil {
		return errors.New("rawdb: nil borrowed state domain change callback")
	}
	if toBlock < fromBlock {
		return fmt.Errorf("rawdb: inverted state domain change block range [%d,%d]", fromBlock, toBlock)
	}
	if toTxNum < fromTxNum {
		return fmt.Errorf("rawdb: inverted state domain change tx range [%d,%d]", fromTxNum, toTxNum)
	}
	var start [8]byte
	binary.BigEndian.PutUint64(start[:], fromBlock)
	rangeIt := view.NewIterator(stateTxRangePrefix, start[:])
	defer rangeIt.Release()
	changeIt := view.NewIterator(stateChangeSetPrefix, start[:])
	defer changeIt.Release()
	source := historyPipelineSource{ctx: ctx, rangeIt: rangeIt, changeIt: changeIt, fromBlock: fromBlock, toBlock: toBlock, fromTx: fromTxNum, toTx: toTxNum}
	workCtx, cancel := context.WithCancel(ctx)
	reader := historyPipelineReader{StateHistoryReadView: view, ctx: workCtx}
	var queue []*historyPipelineBlock
	defer func() {
		cancel()
		for _, job := range queue {
			<-job.done
			job.raw = nil
		}
	}()
	var retained uint64
	var exclusive bool
	var pending *historyPipelineBlock
	var sourceErr error
	var ended bool
	var scratch StateDomainChange
	var previousTx, previousSeq, previousBlock uint64
	var havePrevious bool
	consume := func(job *historyPipelineBlock) (bool, error) {
		visit := func(change *StateDomainChange) (bool, error) {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			if change.TxNum < fromTxNum || change.TxNum > toTxNum {
				return true, nil
			}
			if havePrevious && (change.TxNum < previousTx || change.TxNum == previousTx && (change.Seq < previousSeq || change.Seq == previousSeq && change.BlockNum <= previousBlock)) {
				return false, fmt.Errorf("rawdb: borrowed state domain changes are not ordered at block %d sequence %d txNum %d", change.BlockNum, change.Seq, change.TxNum)
			}
			change.BlockHash = job.hash
			cont, err := fn(change)
			if err == nil && cont {
				havePrevious, previousTx, previousSeq, previousBlock = true, change.TxNum, change.Seq, change.BlockNum
			}
			return cont, err
		}
		if job.done != nil {
			if job.err != nil {
				return false, job.err
			}
			return iterateMaterializedStateHistoryBlock(job.raw, job.block, &scratch, visit)
		}
		// Legacy/raw/Snappy and malformed shared envelopes stay at the serial
		// consumption boundary, preserving fallback and exact decoder errors.
		cont, err := iteratePersistedStateDomainChangeBlockBorrowedWithScratch(job.encoded, job.block, &scratch, visit, reader)
		if err != nil && !isStateHistorySharedPack(job.encoded) {
			if _, legacyErr := decodePersistedStateDomainChange(job.encoded, job.block, 0); legacyErr == nil {
				return false, fmt.Errorf("%w at block %d sequence 0", ErrStateDomainChangeBorrowedLegacyRows, job.block)
			}
		}
		return cont, err
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Only this goroutine touches source iterators. A pending serial block
		// borrows its iterator bytes without advancing until earlier work drains.
		for len(queue) < workers && !ended && !historyPipelineKnownFailure(queue) {
			if pending == nil {
				pending, sourceErr = source.next()
				if pending == nil {
					ended = true
					break
				}
			}
			if !pending.shared || !historyPipelineCanAdmit(retained, len(queue), pending.decoded, StateHistoryPipelineDecodedBudget, workers, exclusive) {
				break
			}
			job := pending
			job.encoded = bytes.Clone(job.encoded)
			job.done = make(chan struct{})
			retained += job.decoded
			exclusive = job.decoded >= StateHistoryPipelineDecodedBudget/2
			queue = append(queue, job)
			pending = nil
			go func() {
				defer close(job.done)
				job.raw, job.err = materializeStateHistorySharedPack(job.encoded, job.block, []ethdb.KeyValueReader{reader})
				job.encoded = nil
			}()
		}
		if len(queue) > 0 {
			job := queue[0]
			select {
			case <-job.done:
			case <-ctx.Done():
				return ctx.Err()
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			cont, err := consume(job)
			// The reused row also aliases this decoded pack. Drop that alias
			// before returning the pack's declared bytes to the admission budget.
			scratch = StateDomainChange{}
			job.raw = nil
			retained -= job.decoded
			// A maximum-sized job is admitted only to an empty queue, so
			// consuming it is the only way an exclusive reservation ends.
			if job.decoded >= StateHistoryPipelineDecodedBudget/2 {
				exclusive = false
			}
			queue[0] = nil
			queue = queue[1:]
			if err != nil || !cont {
				return err
			}
			continue
		}
		if pending != nil {
			// A legal maximum-sized shared pack is admitted alone; only serial
			// cases reach this branch with no queued predecessors.
			cont, err := consume(pending)
			scratch = StateDomainChange{}
			pending = nil
			if err != nil || !cont {
				return err
			}
			continue
		}
		if ended {
			return sourceErr
		}
	}
}

func historyPipelineCanAdmit(retained uint64, count int, next, budget uint64, workers int, exclusive bool) bool {
	if count < 0 || count >= workers || retained > budget || next == 0 || next > budget/2 || next > budget-retained {
		return false
	}
	// Do not overlap either side of a large-block admission. The normal 128MiB
	// format maximum is half the fixed 256MiB output budget.
	return count == 0 || !exclusive && next < budget/2
}

type historyPipelineBlock struct {
	block, decoded uint64
	hash           common.Hash
	encoded, raw   []byte
	shared         bool
	done           chan struct{}
	err            error
}

func historyPipelineKnownFailure(queue []*historyPipelineBlock) bool {
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

// Preserve the audited snapshot's Has/Get order and ownership while checking
// parent cancellation between existing operations; no presence API is added.
type historyPipelineReader struct {
	StateHistoryReadView
	ctx context.Context
}

func (r historyPipelineReader) GetReturnsOwnedBytes() bool { return true }
func (r historyPipelineReader) historyChunkCacheState() (*StateHistoryChunkCache, context.Context) {
	if provider, ok := r.StateHistoryReadView.(historyChunkCacheProvider); ok {
		cache, _ := provider.historyChunkCacheState()
		return cache, r.ctx
	}
	return nil, r.ctx
}
func (r historyPipelineReader) Has(key []byte) (bool, error) {
	if err := r.ctx.Err(); err != nil {
		return false, err
	}
	return r.StateHistoryReadView.Has(key)
}
func (r historyPipelineReader) Get(key []byte) ([]byte, error) {
	if err := r.ctx.Err(); err != nil {
		return nil, err
	}
	return r.StateHistoryReadView.Get(key)
}

type historyPipelineSource struct {
	ctx                              context.Context
	rangeIt, changeIt                ethdb.Iterator
	fromBlock, toBlock, fromTx, toTx uint64
	haveRange                        bool
	rangeBlock, rangeBegin, rangeEnd uint64
	rangeHash                        common.Hash
}

func (s *historyPipelineSource) finish() (*historyPipelineBlock, error) {
	if err := s.changeIt.Error(); err != nil {
		return nil, err
	}
	return nil, s.rangeIt.Error()
}

// next mirrors the unchanged serial iterator's range matching and schema-key
// handling. It returns borrowed encoded bytes; the caller must copy or consume
// them before advancing again. Source errors are delivered after prior jobs.
func (s *historyPipelineSource) next() (*historyPipelineBlock, error) {
	for s.changeIt.Next() {
		if err := s.ctx.Err(); err != nil {
			return nil, err
		}
		key := s.changeIt.Key()
		if !bytes.HasPrefix(key, stateChangeSetPrefix) || len(key) != len(stateChangeSetPrefix)+16 {
			continue
		}
		block := binary.BigEndian.Uint64(key[len(stateChangeSetPrefix):])
		if block < s.fromBlock {
			continue
		}
		if block > s.toBlock {
			return s.finish()
		}
		seq := binary.BigEndian.Uint64(key[len(stateChangeSetPrefix)+8:])
		if seq != 0 {
			return nil, fmt.Errorf("%w at block %d sequence %d", ErrStateDomainChangeBorrowedLegacyRows, block, seq)
		}
		for !s.haveRange || s.rangeBlock < block {
			if err := s.ctx.Err(); err != nil {
				return nil, err
			}
			if !s.rangeIt.Next() {
				return s.finish()
			}
			rangeKey := s.rangeIt.Key()
			if !bytes.HasPrefix(rangeKey, stateTxRangePrefix) || len(rangeKey) != len(stateTxRangePrefix)+8 {
				continue
			}
			candidate := binary.BigEndian.Uint64(rangeKey[len(stateTxRangePrefix):])
			if candidate < s.fromBlock {
				continue
			}
			if candidate > s.toBlock {
				s.rangeBlock, s.rangeHash, s.rangeBegin, s.rangeEnd, s.haveRange = candidate, common.Hash{}, 0, 0, true
				break
			}
			hash, begin, end, err := decodeBorrowedStateTxRange(s.rangeIt.Value(), candidate)
			if err != nil {
				return nil, err
			}
			s.rangeBlock, s.rangeHash, s.rangeBegin, s.rangeEnd, s.haveRange = candidate, hash, begin, end, true
		}
		if s.rangeBlock != block || s.rangeEnd < s.fromTx || s.rangeBegin > s.toTx {
			continue
		}
		job := &historyPipelineBlock{block: block, hash: s.rangeHash, encoded: s.changeIt.Value()}
		if isStateHistorySharedPack(job.encoded) {
			if _, decoded, _, _, err := sharedStateHistoryPackHeader(job.encoded, block); err == nil {
				job.shared, job.decoded = true, uint64(decoded)
			}
		}
		return job, nil
	}
	return s.finish()
}
