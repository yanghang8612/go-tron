package pruning

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

type Store interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
	ethdb.Iteratee
}

type Worker struct {
	DB          Store
	Policy      Policy
	MaxBlocks   int
	SnapshotDir string

	// HistoryRangePrune changes only the expression of already-authorized hot
	// history deletes. Cold coverage and progress publication remain unchanged.
	HistoryRangePrune bool
	// HistoryRangeGuard optionally admits the range scan and its batch flush
	// under the live writer locks. Busy admission retains the original point
	// deletion path. Proof failures must return an error, never busy admission.
	HistoryRangeGuard func(context.Context, uint64, func() error) (bool, error)

	// ShouldDeferStateCodePrune skips the optional full CodeDomain reference
	// scan when it returns true at the stage boundary. Hot code is immutable and
	// remains authoritative while retained; a later pass scans every hot code
	// hash again, so no separate resume cursor is required. Production uses this
	// only during active historical sync. Evaluating it here, rather than when
	// the pass starts, closes the race where sync begins during hot-history prune.
	ShouldDeferStateCodePrune func() bool

	// PruneHeadHash, when set by the live pruner, binds SnapshotPrune
	// progress to the canonical block hash that capped this prune pass.
	PruneHeadHash    common.Hash
	PruneHeadHasHash bool

	coverageVerificationCache   *snapshotCoverageVerificationCache
	coverageVerificationContext context.Context
	coverageVerificationDone    func()
}

type Stats struct {
	DeletedTxRanges              int
	DeletedDomainChangeBlocks    int
	DeletedCommitmentCheckpoints int
	DeletedStateCodeRows         int
	StateCodePruneDeferred       bool
	DomainChangeStartBlock       uint64
	DomainChangePrunedThrough    uint64
	DomainChangePrunedThroughTx  uint64
	// HistoryMetadataDuration covers manifest planning/progress work only.
	// Lazy content verification, hot row reads/deletes and every batch flush
	// remain outside it and therefore in the adaptive per-row work estimate.
	HistoryMetadataDuration time.Duration
	// HistoryDeletes counts accepted logical deletes from a completely
	// successful pass. It is not reclaimed filesystem space.
	HistoryDeletes               rawdb.StateDomainChangeDeleteStats
	HistoryRangeFallbackBlocks   uint64
	HistoryRangeGuardDuration    time.Duration
	HistoryRangeGuardMaxDuration time.Duration
}

const maxPruneBatchValueSize = 32 << 20

// Limit each optional critical section independently of the adaptive pass size.
// This bounds rows, not individual storage call latency or unusually large packs.
const maxHistoryRangeGuardBlocks = 256

// pruneBatchStore keeps scans on the committed store while directing writes to
// bounded batches. Pruning is idempotent and advances progress only after all
// deletes finish, so committing bounded chunks is safe and avoids Pebble's hard
// 4 GiB batch limit on unusually large historical passes.
type pruneBatchStore struct {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
	ethdb.Iteratee
}

// Expose range deletion only for the opted-in hot-history phase. Promoting it
// on every prune store would change capability-based behavior in other phases.
type rangePruneBatchStore struct {
	pruneBatchStore
	deleteRange func([]byte, []byte) error
}

func (s rangePruneBatchStore) DeleteRange(start, end []byte) error {
	return s.deleteRange(start, end)
}

// GetWithPresence preserves the atomic point-read capability of the committed
// store. The bounded prune writer intentionally exposes committed reads only:
// prune deletes are idempotent and callers must not make correctness depend on
// whether an earlier bounded batch has already been flushed. Without this
// forwarding method, hot-history pruning falls back to Has+Get for every live
// posting row and doubles Pebble point reads on the success path.
func (s pruneBatchStore) GetWithPresence(key []byte) ([]byte, bool, error) {
	if reader, ok := s.KeyValueReader.(interface {
		GetWithPresence([]byte) ([]byte, bool, error)
	}); ok {
		return reader.GetWithPresence(key)
	}
	exists, err := s.KeyValueReader.Has(key)
	if err != nil || !exists {
		return nil, false, err
	}
	value, err := s.KeyValueReader.Get(key)
	if err != nil {
		return nil, false, err
	}
	return value, true, nil
}

func newPruneBatchStore(store Store) (Store, func() error) {
	return newPruneBatchStoreWithLimit(store, maxPruneBatchValueSize)
}

func newPruneBatchStoreWithLimit(store Store, limit int) (Store, func() error) {
	return newPruneBatchStoreWithRanges(store, limit, false)
}

func newPruneBatchStoreWithRanges(store Store, limit int, ranges bool) (Store, func() error) {
	batcher, ok := store.(ethdb.Batcher)
	if !ok {
		return store, func() error { return nil }
	}
	writer := &boundedPruneBatchWriter{
		batch: batcher.NewBatch(),
		limit: limit,
	}
	wrapped := pruneBatchStore{
		KeyValueReader: store,
		KeyValueWriter: writer,
		Iteratee:       store,
	}
	if ranges {
		return rangePruneBatchStore{pruneBatchStore: wrapped, deleteRange: writer.DeleteRange}, writer.Flush
	}
	return wrapped, writer.Flush
}

type boundedPruneBatchWriter struct {
	batch ethdb.Batch
	limit int
}

func (w *boundedPruneBatchWriter) Put(key, value []byte) error {
	if err := w.flushBefore(len(key) + len(value)); err != nil {
		return err
	}
	return w.batch.Put(key, value)
}

func (w *boundedPruneBatchWriter) Delete(key []byte) error {
	if err := w.flushBefore(len(key)); err != nil {
		return err
	}
	return w.batch.Delete(key)
}

func (w *boundedPruneBatchWriter) DeleteRange(start, end []byte) error {
	if err := w.flushBefore(len(start) + len(end)); err != nil {
		return err
	}
	return w.batch.DeleteRange(start, end)
}

func (w *boundedPruneBatchWriter) flushBefore(nextSize int) error {
	if w == nil || w.batch == nil || w.limit <= 0 || w.batch.ValueSize() == 0 || w.batch.ValueSize()+nextSize <= w.limit {
		return nil
	}
	return w.flush()
}

func (w *boundedPruneBatchWriter) flush() error {
	if w == nil || w.batch == nil || w.batch.ValueSize() == 0 {
		return nil
	}
	err := w.batch.Write()
	w.batch.Reset()
	return err
}

func (w *boundedPruneBatchWriter) Flush() error {
	if w == nil {
		return nil
	}
	defer func() {
		if w.batch != nil {
			w.batch.Reset()
		}
	}()
	return w.flush()
}

func Run(db Store, policy Policy, headNum uint64) (Stats, error) {
	return Worker{DB: db, Policy: policy}.PruneTo(headNum)
}

func (w Worker) PruneTo(headNum uint64) (Stats, error) {
	return w.PruneToContext(context.Background(), headNum)
}

// PruneToContext runs one bounded prune pass and stops before starting the
// next mutation phase when ctx is cancelled. The expensive CodeDomain
// reference scan also observes ctx while iterating account and history rows,
// which keeps graceful shutdown from waiting for a full-chain scan.
func (w Worker) PruneToContext(ctx context.Context, headNum uint64) (Stats, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return Stats{}, err
	}
	if w.DB == nil {
		return Stats{}, errors.New("pruning: nil database")
	}
	if err := w.Policy.Validate(); err != nil {
		return Stats{}, err
	}
	if w.Policy.Mode == ModeArchive && w.Policy.HistoryWindow == 0 {
		return Stats{}, nil
	}
	var stats Stats
	coverageCtx := ctx
	if w.coverageVerificationContext != nil {
		coverageCtx = w.coverageVerificationContext
	}
	coverageDone := w.coverageVerificationDone
	defer func() {
		if coverageDone != nil {
			coverageDone()
		}
	}()
	metadataStarted := time.Now()
	coverage, err := w.newSnapshotStateDomainChangeCoverageGate(coverageCtx)
	stats.HistoryMetadataDuration += time.Since(metadataStarted)
	if err != nil {
		return Stats{}, err
	}
	if len(coverage.segments) == 0 && coverageDone != nil {
		coverageDone()
		coverageDone = nil
	}
	historyCfg, err := w.hotHistoryDomainConfig()
	if err != nil {
		return Stats{}, err
	}
	var historyDeletes rawdb.StateDomainChangeDeleteStats
	historyStore, flushHistory := newPruneBatchStoreWithRanges(w.DB, maxPruneBatchValueSize, w.HistoryRangePrune)
	if w.HistoryRangePrune {
		if w.Policy.Mode != ModeSnap || w.SnapshotDir == "" {
			return Stats{}, errors.New("pruning: history range pruning requires snap mode with cold snapshots")
		}
		if w.HistoryRangeGuard == nil {
			return Stats{}, errors.New("pruning: history range pruning requires a writer guard")
		}
		historyCfg.DeleteHotHistoryBlocks = func(store rawdb.StateKVLatestStore, blocks []uint64) error {
			for len(blocks) > 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
				n := min(len(blocks), maxHistoryRangeGuardBlocks)
				part := blocks[:n]
				admitted, err := w.HistoryRangeGuard(ctx, part[n-1], func() error {
					started := time.Now()
					defer func() {
						elapsed := time.Since(started)
						stats.HistoryRangeGuardDuration += elapsed
						stats.HistoryRangeGuardMaxDuration = max(stats.HistoryRangeGuardMaxDuration, elapsed)
					}()
					deleted, err := rawdb.DeleteStateDomainChangeBlocksWithOptions(store, part, rawdb.StateDomainChangeDeleteOptions{EnableRangeDelete: true})
					if err != nil {
						return err
					}
					if err := ctx.Err(); err != nil {
						return err
					}
					if err := flushHistory(); err != nil {
						return err
					}
					historyDeletes.RangeRuns += deleted.RangeRuns
					historyDeletes.RangeRows += deleted.RangeRows
					historyDeletes.RangeBytes += deleted.RangeBytes
					historyDeletes.PointRows += deleted.PointRows
					historyDeletes.PointBytes += deleted.PointBytes
					return nil
				})
				if err != nil {
					return err
				}
				if !admitted {
					// Busy admission is not permission for a range. Preserve the
					// pre-existing key-only point path without new value reads.
					if err := rawdb.DeleteStateDomainChangeBlocks(store, part); err != nil {
						return err
					}
					if err := ctx.Err(); err != nil {
						return err
					}
					if err := flushHistory(); err != nil {
						return err
					}
					stats.HistoryRangeFallbackBlocks += uint64(n)
				}
				blocks = blocks[n:]
			}
			return nil
		}
	}
	metadataStarted = time.Now()
	hotPruneStartBlock, err := w.hotHistoryPruneStartBlock()
	stats.HistoryMetadataDuration += time.Since(metadataStarted)
	if err != nil {
		return Stats{}, err
	}
	stats.DomainChangeStartBlock = hotPruneStartBlock
	hotStats, err := historyCfg.PruneHotHistory(historyStore, snapshots.HotHistoryPruneOptions{
		MaxBlocks:  w.MaxBlocks,
		StartBlock: hotPruneStartBlock,
		Decide: func(row rawdb.StateTxRange) (snapshots.HotHistoryPruneDecision, error) {
			if err := ctx.Err(); err != nil {
				return snapshots.HotHistoryPruneDecision{}, err
			}
			if w.Policy.RetainHotHistory(row.BlockNum, headNum) {
				return snapshots.HotHistoryPruneDecision{}, nil
			}
			switch w.Policy.Mode {
			case ModeFull, ModeBlocks, ModeMinimal:
				return snapshots.HotHistoryPruneDecision{DeleteTxRange: true, DeleteHistoryBlock: true}, nil
			case ModeSnap, ModeArchive:
				covered, err := coverage.covers(row.BeginTxNum, row.EndTxNum)
				if err != nil {
					return snapshots.HotHistoryPruneDecision{}, err
				}
				if !covered {
					return snapshots.HotHistoryPruneDecision{Stop: true}, nil
				}
				return snapshots.HotHistoryPruneDecision{DeleteHistoryBlock: true}, nil
			}
			return snapshots.HotHistoryPruneDecision{}, nil
		},
	})
	if coverageDone != nil {
		coverageDone()
		coverageDone = nil
	}
	if err != nil {
		return Stats{}, err
	}
	if err := flushHistory(); err != nil {
		return Stats{}, fmt.Errorf("pruning: flush hot history delete batch: %w", err)
	}
	stats.HistoryDeletes = historyDeletes
	stats.DeletedTxRanges = hotStats.DeletedTxRanges
	stats.DeletedDomainChangeBlocks = hotStats.DeletedHistoryBlocks
	stats.DomainChangePrunedThrough = hotStats.MaxDeletedHistoryBlock
	stats.DomainChangePrunedThroughTx = hotStats.MaxDeletedHistoryBlockTx
	if hotStats.MaxDeletedHistoryBlockTx != 0 && w.SnapshotDir != "" {
		metadataStarted = time.Now()
		progressErr := snapshots.UpdateHotPruneProgress(w.SnapshotDir, hotStats.MaxDeletedHistoryBlock, hotStats.MaxDeletedHistoryBlockTx)
		stats.HistoryMetadataDuration += time.Since(metadataStarted)
		if progressErr != nil {
			return Stats{}, progressErr
		}
		if err := newRawDBStageProgressStore(w.DB).Write(rawdb.StageSnapshotHotPrune, hotStats.MaxDeletedHistoryBlockTx); err != nil {
			return Stats{}, fmt.Errorf("pruning: write snapshot/hot-prune stage progress: %w", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return Stats{}, err
	}
	deferStateCodePrune := w.ShouldDeferStateCodePrune != nil && w.ShouldDeferStateCodePrune()
	if deferStateCodePrune && w.Policy.Mode == ModeSnap && w.SnapshotDir != "" {
		stats.StateCodePruneDeferred = true
	} else {
		codeStore, flushCode := newPruneBatchStore(w.DB)
		deletedCodeRows, err := w.pruneStateCodeRowsContext(ctx, codeStore, headNum)
		if err != nil {
			return Stats{}, err
		}
		if err := flushCode(); err != nil {
			return Stats{}, fmt.Errorf("pruning: flush CodeDomain delete batch: %w", err)
		}
		stats.DeletedStateCodeRows = deletedCodeRows
	}

	checkpointCfg, err := latestDomainConfig(snapshots.SegmentDatasetCommitmentCheckpoint)
	if err != nil {
		return Stats{}, err
	}
	if checkpointCfg.IterateHotCommitmentCheckpoints == nil || checkpointCfg.DeleteHotCommitmentCheckpoint == nil {
		return Stats{}, errors.New("pruning: missing commitment checkpoint lifecycle hooks")
	}
	checkpointStore, flushCheckpoints := newPruneBatchStore(w.DB)
	var commitmentBlocks []uint64
	if err := checkpointCfg.IterateHotCommitmentCheckpoints(checkpointStore, func(cp *rawdb.StateCommitmentCheckpoint) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if !w.Policy.RetainReorgData(cp.BlockNum, headNum) {
			commitmentBlocks = append(commitmentBlocks, cp.BlockNum)
		}
		return true, nil
	}); err != nil {
		return Stats{}, err
	}
	for _, blockNum := range commitmentBlocks {
		if err := checkpointCfg.DeleteHotCommitmentCheckpoint(checkpointStore, blockNum); err != nil {
			return Stats{}, err
		}
		stats.DeletedCommitmentCheckpoints++
	}
	if err := flushCheckpoints(); err != nil {
		return Stats{}, fmt.Errorf("pruning: flush commitment checkpoint delete batch: %w", err)
	}
	if err := w.writeSnapshotPruneProgress(headNum); err != nil {
		return Stats{}, fmt.Errorf("pruning: write snapshot/prune stage progress: %w", err)
	}
	return stats, nil
}

func (w Worker) hotHistoryPruneStartBlock() (uint64, error) {
	if (w.Policy.Mode != ModeSnap && w.Policy.Mode != ModeArchive) || w.SnapshotDir == "" {
		return 0, nil
	}
	manifest, err := snapshots.LoadProductionManifest(w.SnapshotDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	if manifest.Progress == nil || manifest.Progress.HotPruneBlockNum == 0 {
		return 0, nil
	}
	if manifest.Progress.HotPruneBlockNum == ^uint64(0) {
		return manifest.Progress.HotPruneBlockNum, nil
	}
	return manifest.Progress.HotPruneBlockNum + 1, nil
}

func (w Worker) writeSnapshotPruneProgress(headNum uint64) error {
	store := newRawDBStageProgressStore(w.DB)
	if w.PruneHeadHasHash {
		return store.WriteWithHash(rawdb.StageSnapshotPrune, headNum, w.PruneHeadHash)
	}
	return store.Write(rawdb.StageSnapshotPrune, headNum)
}

func (w Worker) hotHistoryDomainConfig() (snapshots.DomainCfg, error) {
	cfg, ok := snapshots.DefaultDomainRegistry().Dataset(snapshots.SegmentDatasetStateDomainChange)
	if !ok || cfg.DeleteHotHistoryBlock == nil {
		return snapshots.DomainCfg{}, errors.New("pruning: missing state-domain hot history deleter")
	}
	return cfg, nil
}

func (w Worker) pruneStateCodeRows(db Store, headNum uint64) (int, error) {
	return w.pruneStateCodeRowsContext(context.Background(), db, headNum)
}

func (w Worker) pruneStateCodeRowsContext(ctx context.Context, db Store, headNum uint64) (int, error) {
	if w.Policy.Mode != ModeSnap || w.SnapshotDir == "" {
		return 0, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	mgr, err := snapshots.OpenManager(w.SnapshotDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	manifest := mgr.Manifest()
	if manifest == nil {
		return 0, nil
	}
	headTxNum, err := snapshots.StateDomainHistoryTxNumAtBlockEnd(db, headNum)
	if err != nil {
		return 0, err
	}
	accountCfg, err := latestDomainConfig(snapshots.SegmentDatasetAccountLatest)
	if err != nil {
		return 0, err
	}
	if accountCfg.IterateHotAccountLatest == nil {
		return 0, errors.New("pruning: missing account latest iterator")
	}
	codeCfg, err := latestDomainConfig(snapshots.SegmentDatasetCode)
	if err != nil {
		return 0, err
	}
	if codeCfg.IterateHotCodeHashes == nil || codeCfg.DeleteHotCode == nil {
		return 0, errors.New("pruning: missing CodeDomain lifecycle hooks")
	}
	// Code references only matter for hashes that still have a hot CodeDomain
	// row. Once all eligible rows have been moved behind snapshot coverage,
	// avoid rescanning the entire account history on every lifecycle pass.
	var hotCodeHashes []common.Hash
	hotCodeSet := make(map[common.Hash]struct{})
	if err := codeCfg.IterateHotCodeHashes(db, func(hash common.Hash) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if isMeaningfulCodeHash(hash) {
			hotCodeHashes = append(hotCodeHashes, hash)
			hotCodeSet[hash] = struct{}{}
		}
		return true, nil
	}); err != nil {
		return 0, err
	}
	if len(hotCodeHashes) == 0 {
		return 0, nil
	}
	// A hot code row with no CodeDomain snapshot visible at the current head
	// cannot be deleted regardless of its account-history references. Gate the
	// expensive full history scan on that necessary condition. This matters in
	// VM-heavy sync ranges where new bytecode arrives continuously but latest
	// snapshots lag behind the head: without the gate, every lifecycle pass
	// rescans all hot history only to reach the same uncovered result.
	coveredHotCodeHashes := hotCodeHashes[:0]
	coveredHotCodeSet := make(map[common.Hash]struct{})
	for _, hash := range hotCodeHashes {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		covered, err := codeHashAvailableInSnapshot(mgr, hash, headTxNum)
		if err != nil {
			return 0, err
		}
		if covered {
			coveredHotCodeHashes = append(coveredHotCodeHashes, hash)
			coveredHotCodeSet[hash] = struct{}{}
		}
	}
	if len(coveredHotCodeHashes) == 0 {
		return 0, nil
	}
	hotCodeHashes = coveredHotCodeHashes
	hotCodeSet = coveredHotCodeSet
	refs := make(codeHashRefs)
	if err := accountCfg.IterateHotAccountLatest(db, nil, func(row rawdb.StateAccountLatestRow) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		hash, err := decodeAccountEnvelopeCodeHash(row.Value)
		if err != nil {
			return false, fmt.Errorf("pruning: decode account latest %x: %w", row.Owner, err)
		}
		if _, ok := hotCodeSet[hash]; ok {
			refs.add(hash, headTxNum)
		}
		return true, nil
	}); err != nil {
		return 0, err
	}
	collector := Checker{DB: db, SnapshotDir: w.SnapshotDir}
	historyCfg, ok := snapshots.DefaultDomainRegistry().Dataset(snapshots.SegmentDatasetStateDomainChange)
	if !ok {
		return 0, errors.New("pruning: missing state-domain history config")
	}
	if err := collectHotHistoryCodeHashesFiltered(ctx, historyCfg, db, refs, hotCodeSet); err != nil {
		return 0, err
	}

	// CodeDomain is immutable and GetCodeAtOrBefore keeps older segments
	// visible. Coverage at the earliest possible reference therefore covers
	// every later reference and lets the common case skip cold-history decode.
	fullyCovered := make(map[common.Hash]struct{})
	coldWanted := make(map[common.Hash]struct{})
	for _, hash := range hotCodeHashes {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		earliest := manifest.VisibleTxStart
		if hotTxNum, ok := refs[hash]; ok && hotTxNum < earliest {
			earliest = hotTxNum
		}
		covered, err := codeHashAvailableInSnapshot(mgr, hash, earliest)
		if err != nil {
			return 0, err
		}
		if covered {
			fullyCovered[hash] = struct{}{}
		} else {
			coldWanted[hash] = struct{}{}
		}
	}
	if len(coldWanted) != 0 {
		if err := collector.collectColdHistoryCodeHashesFilteredContext(ctx, refs, coldWanted, mgr); err != nil {
			return 0, err
		}
	}

	deleteHashes := hotCodeHashes[:0]
	for _, hash := range hotCodeHashes {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if _, covered := fullyCovered[hash]; covered {
			deleteHashes = append(deleteHashes, hash)
			continue
		}
		earliestTxNum, referenced := refs[hash]
		if !referenced {
			earliestTxNum = headTxNum
		}
		covered, err := codeHashAvailableInSnapshot(mgr, hash, earliestTxNum)
		if err != nil {
			return 0, err
		}
		if covered {
			deleteHashes = append(deleteHashes, hash)
		}
	}
	for _, hash := range deleteHashes {
		if err := codeCfg.DeleteHotCode(db, hash); err != nil {
			return 0, err
		}
	}
	return len(deleteHashes), nil
}

type snapshotTxRange struct {
	from uint64
	to   uint64
}

type snapshotTxCoverage []snapshotTxRange

type snapshotStateDomainCoverageSegment struct {
	ref      snapshots.SegmentRef
	key      snapshotHistoryVerificationKey
	verified bool
}

// snapshotStateDomainCoverageGate binds one production manifest view to the
// exact immutable file identities it names. Construction checks every active
// companion's presence and size and retains all active durable cache records,
// but deliberately postpones SHA-256 and semantic verification until a hot row
// is actually eligible for deletion.
type snapshotStateDomainCoverageGate struct {
	ctx      context.Context
	dir      string
	manifest *snapshots.Manifest
	cache    *snapshotCoverageVerificationCache
	segments []snapshotStateDomainCoverageSegment
}

func (w Worker) snapshotStateDomainChangeCoverage() (snapshotTxCoverage, error) {
	return w.snapshotStateDomainChangeCoverageContext(context.Background())
}

func (w Worker) snapshotStateDomainChangeCoverageContext(ctx context.Context) (snapshotTxCoverage, error) {
	gate, err := w.newSnapshotStateDomainChangeCoverageGate(ctx)
	if err != nil {
		return nil, err
	}
	coverage := make(snapshotTxCoverage, 0, len(gate.segments))
	for i := range gate.segments {
		if err := gate.verify(i); err != nil {
			return nil, err
		}
		ref := gate.segments[i].ref
		coverage = append(coverage, snapshotTxRange{from: ref.FromTxNum, to: ref.ToTxNum})
	}
	return coverage, nil
}

func (w Worker) newSnapshotStateDomainChangeCoverageGate(ctx context.Context) (*snapshotStateDomainCoverageGate, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	gate := &snapshotStateDomainCoverageGate{ctx: ctx, dir: w.SnapshotDir, cache: w.coverageVerificationCache}
	if (w.Policy.Mode != ModeSnap && w.Policy.Mode != ModeArchive) || w.SnapshotDir == "" {
		return gate, nil
	}
	manifest, err := snapshots.LoadProductionManifest(w.SnapshotDir)
	if err != nil {
		if os.IsNotExist(err) {
			return gate, nil
		}
		return nil, err
	}
	gate.manifest = manifest
	activeCacheKeys := make(map[snapshotHistoryVerificationKey]struct{})
	for _, ref := range manifest.Segments {
		if ref.NormalizedDataset() != snapshots.SegmentDatasetStateDomainChange || ref.Kind != snapshots.SegmentHistory {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cacheKey, err := snapshotHistoryVerificationKeyFor(w.SnapshotDir, manifest, ref)
		if err != nil {
			return nil, fmt.Errorf("hot prune coverage: identify state-domain history %q: %w", ref.Path, err)
		}
		activeCacheKeys[cacheKey] = struct{}{}
		gate.segments = append(gate.segments, snapshotStateDomainCoverageSegment{ref: ref, key: cacheKey})
	}
	if w.coverageVerificationCache != nil {
		w.coverageVerificationCache.setActiveManifest(activeCacheKeys)
		if err := w.coverageVerificationCache.retain(activeCacheKeys); err != nil {
			return nil, fmt.Errorf("pruning: retain active state-domain history verifications: %w", err)
		}
	}
	sort.Slice(gate.segments, func(i, j int) bool {
		if gate.segments[i].ref.FromTxNum != gate.segments[j].ref.FromTxNum {
			return gate.segments[i].ref.FromTxNum < gate.segments[j].ref.FromTxNum
		}
		return gate.segments[i].ref.ToTxNum < gate.segments[j].ref.ToTxNum
	})
	return gate, nil
}

func (g *snapshotStateDomainCoverageGate) verify(index int) error {
	if g == nil || index < 0 || index >= len(g.segments) {
		return errors.New("pruning: invalid state-domain coverage segment")
	}
	segment := &g.segments[index]
	if segment.verified {
		return nil
	}
	key, _, err := g.cache.verifyHistory(g.ctx, g.dir, g.manifest, segment.ref, false, "hot prune coverage")
	if err != nil {
		return err
	}
	if key != segment.key {
		return fmt.Errorf("hot prune coverage: state-domain history %q identity changed before verification", segment.ref.Path)
	}
	segment.verified = true
	return nil
}

func (g *snapshotStateDomainCoverageGate) covers(from, to uint64) (bool, error) {
	if g == nil || to < from {
		return false, nil
	}
	if g.ctx != nil {
		if err := g.ctx.Err(); err != nil {
			return false, err
		}
	}
	if !g.coversMetadata(from, to) {
		return false, nil
	}
	next := from
	for i := range g.segments {
		ref := g.segments[i].ref
		if ref.ToTxNum < next {
			continue
		}
		if err := g.verify(i); err != nil {
			return false, err
		}
		if ref.ToTxNum >= to {
			return true, nil
		}
		next = ref.ToTxNum + 1
	}
	return false, nil
}

func (g *snapshotStateDomainCoverageGate) coversMetadata(from, to uint64) bool {
	next := from
	for i := range g.segments {
		ref := g.segments[i].ref
		if ref.ToTxNum < next {
			continue
		}
		if ref.FromTxNum > next {
			return false
		}
		if ref.ToTxNum >= to {
			return true
		}
		next = ref.ToTxNum + 1
	}
	return false
}

func (c snapshotTxCoverage) covers(from, to uint64) bool {
	if to < from {
		return false
	}
	next := from
	for _, r := range c {
		if r.to < next {
			continue
		}
		if r.from > next {
			return false
		}
		if r.to >= to {
			return true
		}
		next = r.to + 1
	}
	return false
}
