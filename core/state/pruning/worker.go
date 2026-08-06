package pruning

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

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

	// PruneHeadHash, when set by the live pruner, binds SnapshotPrune
	// progress to the canonical block hash that capped this prune pass.
	PruneHeadHash    common.Hash
	PruneHeadHasHash bool

	coverageVerificationCache *snapshotCoverageVerificationCache
}

type Stats struct {
	DeletedTxRanges              int
	DeletedDomainChangeBlocks    int
	DeletedCommitmentCheckpoints int
	DeletedStateCodeRows         int
}

const maxPruneBatchValueSize = 32 << 20

// pruneBatchStore keeps scans on the committed store while directing writes to
// bounded batches. Pruning is idempotent and advances progress only after all
// deletes finish, so committing bounded chunks is safe and avoids Pebble's hard
// 4 GiB batch limit on unusually large historical passes.
type pruneBatchStore struct {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
	ethdb.Iteratee
}

func newPruneBatchStore(store Store) (Store, func() error) {
	return newPruneBatchStoreWithLimit(store, maxPruneBatchValueSize)
}

func newPruneBatchStoreWithLimit(store Store, limit int) (Store, func() error) {
	batcher, ok := store.(ethdb.Batcher)
	if !ok {
		return store, func() error { return nil }
	}
	writer := &boundedPruneBatchWriter{
		batch: batcher.NewBatch(),
		limit: limit,
	}
	return pruneBatchStore{
		KeyValueReader: store,
		KeyValueWriter: writer,
		Iteratee:       store,
	}, writer.Flush
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
	coverage, err := w.snapshotStateDomainChangeCoverageContext(ctx)
	if err != nil {
		return Stats{}, err
	}
	historyCfg, err := w.hotHistoryDomainConfig()
	if err != nil {
		return Stats{}, err
	}
	historyStore, flushHistory := newPruneBatchStore(w.DB)
	hotStats, err := historyCfg.PruneHotHistory(historyStore, snapshots.HotHistoryPruneOptions{
		MaxBlocks: w.MaxBlocks,
		Decide: func(row *rawdb.StateTxRange) (snapshots.HotHistoryPruneDecision, error) {
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
				if !coverage.covers(row.BeginTxNum, row.EndTxNum) {
					return snapshots.HotHistoryPruneDecision{}, nil
				}
				return snapshots.HotHistoryPruneDecision{DeleteHistoryBlock: true}, nil
			}
			return snapshots.HotHistoryPruneDecision{}, nil
		},
	})
	if err != nil {
		return Stats{}, err
	}
	if err := flushHistory(); err != nil {
		return Stats{}, fmt.Errorf("pruning: flush hot history delete batch: %w", err)
	}
	stats.DeletedTxRanges = hotStats.DeletedTxRanges
	stats.DeletedDomainChangeBlocks = hotStats.DeletedHistoryBlocks
	if hotStats.MaxDeletedHistoryBlockTx != 0 && w.SnapshotDir != "" {
		if err := snapshots.UpdateHotPruneProgress(w.SnapshotDir, hotStats.MaxDeletedHistoryBlockTx); err != nil {
			return Stats{}, err
		}
		if err := newRawDBStageProgressStore(w.DB).Write(rawdb.StageSnapshotHotPrune, hotStats.MaxDeletedHistoryBlockTx); err != nil {
			return Stats{}, fmt.Errorf("pruning: write snapshot/hot-prune stage progress: %w", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return Stats{}, err
	}
	codeStore, flushCode := newPruneBatchStore(w.DB)
	deletedCodeRows, err := w.pruneStateCodeRowsContext(ctx, codeStore, headNum)
	if err != nil {
		return Stats{}, err
	}
	if err := flushCode(); err != nil {
		return Stats{}, fmt.Errorf("pruning: flush CodeDomain delete batch: %w", err)
	}
	stats.DeletedStateCodeRows = deletedCodeRows

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
	if mgr.Manifest() == nil {
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
	if codeCfg.IterateHotCode == nil || codeCfg.DeleteHotCode == nil {
		return 0, errors.New("pruning: missing CodeDomain lifecycle hooks")
	}
	// Code references only matter for hashes that still have a hot CodeDomain
	// row. Once all eligible rows have been moved behind snapshot coverage,
	// avoid rescanning the entire account history on every lifecycle pass.
	hotCodeRows := 0
	if err := codeCfg.IterateHotCode(db, func(row rawdb.StateCodeRow) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if isMeaningfulCodeHash(row.Hash) {
			hotCodeRows++
		}
		return true, nil
	}); err != nil {
		return 0, err
	}
	if hotCodeRows == 0 {
		return 0, nil
	}
	refs := make(codeHashRefs)
	if err := accountCfg.IterateHotAccountLatest(db, nil, func(row rawdb.StateAccountLatestRow) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		hash, err := decodeAccountEnvelopeCodeHash(row.Value, fmt.Sprintf("account latest %x", row.Owner))
		if err != nil {
			return false, err
		}
		refs.add(hash, headTxNum)
		return true, nil
	}); err != nil {
		return 0, err
	}
	if err := (Checker{DB: db, SnapshotDir: w.SnapshotDir}).collectHistoryCodeHashesContext(ctx, refs); err != nil {
		return 0, err
	}

	var deleteHashes []common.Hash
	if err := codeCfg.IterateHotCode(db, func(row rawdb.StateCodeRow) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if !isMeaningfulCodeHash(row.Hash) {
			return true, nil
		}
		txNums := refs[row.Hash]
		if len(txNums) == 0 {
			covered, err := codeHashAvailableInSnapshot(mgr, row.Hash, headTxNum)
			if err != nil {
				return false, err
			}
			if covered {
				deleteHashes = append(deleteHashes, row.Hash)
			}
			return true, nil
		}
		for txNum := range txNums {
			covered, err := codeHashAvailableInSnapshot(mgr, row.Hash, txNum)
			if err != nil {
				return false, err
			}
			if !covered {
				return true, nil
			}
		}
		deleteHashes = append(deleteHashes, row.Hash)
		return true, nil
	}); err != nil {
		return 0, err
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

func (w Worker) snapshotStateDomainChangeCoverage() (snapshotTxCoverage, error) {
	return w.snapshotStateDomainChangeCoverageContext(context.Background())
}

func (w Worker) snapshotStateDomainChangeCoverageContext(ctx context.Context) (snapshotTxCoverage, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if (w.Policy.Mode != ModeSnap && w.Policy.Mode != ModeArchive) || w.SnapshotDir == "" {
		return nil, nil
	}
	manifest, err := snapshots.LoadProductionManifest(w.SnapshotDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	coverage := make(snapshotTxCoverage, 0)
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
			return nil, fmt.Errorf("pruning: identify state-domain history coverage %q: %w", ref.Path, err)
		}
		activeCacheKeys[cacheKey] = struct{}{}
		if w.coverageVerificationCache == nil || !w.coverageVerificationCache.contains(cacheKey) {
			if err := snapshots.VerifyHistorySegmentWithCompanionsContext(ctx, w.SnapshotDir, manifest, ref); err != nil {
				return nil, fmt.Errorf("pruning: verify state-domain history coverage %q: %w", ref.Path, err)
			}
			verifiedKey, err := snapshotHistoryVerificationKeyFor(w.SnapshotDir, manifest, ref)
			if err != nil {
				return nil, fmt.Errorf("pruning: re-identify state-domain history coverage %q: %w", ref.Path, err)
			}
			if verifiedKey != cacheKey {
				return nil, fmt.Errorf("pruning: state-domain history coverage %q changed while being verified", ref.Path)
			}
			if w.coverageVerificationCache != nil {
				w.coverageVerificationCache.add(cacheKey)
			}
		}
		coverage = append(coverage, snapshotTxRange{from: ref.FromTxNum, to: ref.ToTxNum})
	}
	if w.coverageVerificationCache != nil {
		w.coverageVerificationCache.retain(activeCacheKeys)
	}
	sort.Slice(coverage, func(i, j int) bool {
		if coverage[i].from != coverage[j].from {
			return coverage[i].from < coverage[j].from
		}
		return coverage[i].to < coverage[j].to
	})
	return coverage, nil
}

type snapshotFileIdentity struct {
	size        int64
	modUnixNano int64
}

type snapshotHistoryVerificationKey struct {
	history      snapshots.SegmentRef
	index        snapshots.SegmentRef
	accessor     snapshots.SegmentRef
	historyFile  snapshotFileIdentity
	indexFile    snapshotFileIdentity
	accessorFile snapshotFileIdentity
}

type snapshotCoverageVerificationCache struct {
	mu       sync.Mutex
	verified map[snapshotHistoryVerificationKey]struct{}
}

func newSnapshotCoverageVerificationCache() *snapshotCoverageVerificationCache {
	return &snapshotCoverageVerificationCache{verified: make(map[snapshotHistoryVerificationKey]struct{})}
}

func (c *snapshotCoverageVerificationCache) contains(key snapshotHistoryVerificationKey) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	_, ok := c.verified[key]
	c.mu.Unlock()
	return ok
}

func (c *snapshotCoverageVerificationCache) add(key snapshotHistoryVerificationKey) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.verified[key] = struct{}{}
	c.mu.Unlock()
}

func (c *snapshotCoverageVerificationCache) retain(active map[snapshotHistoryVerificationKey]struct{}) {
	if c == nil {
		return
	}
	c.mu.Lock()
	for key := range c.verified {
		if _, ok := active[key]; !ok {
			delete(c.verified, key)
		}
	}
	c.mu.Unlock()
}

func snapshotHistoryVerificationKeyFor(dir string, manifest *snapshots.Manifest, history snapshots.SegmentRef) (snapshotHistoryVerificationKey, error) {
	key := snapshotHistoryVerificationKey{history: history}
	var err error
	if key.historyFile, err = snapshotVerificationFileIdentity(dir, history); err != nil {
		return snapshotHistoryVerificationKey{}, err
	}
	cfg, ok := snapshots.DefaultDomainRegistry().ConfigForRef(history)
	if !ok || !cfg.IsHistoryBinarySegmentPath(history.Path) {
		return key, nil
	}
	if cfg.HasHistoryInvertedIndex {
		var found bool
		key.index, found = cfg.HistoryIndexRef(manifest, history)
		if !found {
			return snapshotHistoryVerificationKey{}, fmt.Errorf("missing history index companion")
		}
		if key.indexFile, err = snapshotVerificationFileIdentity(dir, key.index); err != nil {
			return snapshotHistoryVerificationKey{}, err
		}
	}
	if cfg.HasHistoryAccessor {
		var found bool
		key.accessor, found = cfg.HistoryAccessorRef(manifest, history)
		if !found {
			return snapshotHistoryVerificationKey{}, fmt.Errorf("missing history accessor companion")
		}
		if key.accessorFile, err = snapshotVerificationFileIdentity(dir, key.accessor); err != nil {
			return snapshotHistoryVerificationKey{}, err
		}
	}
	return key, nil
}

func snapshotVerificationFileIdentity(dir string, ref snapshots.SegmentRef) (snapshotFileIdentity, error) {
	info, err := os.Stat(filepath.Join(dir, ref.Path))
	if err != nil {
		return snapshotFileIdentity{}, err
	}
	return snapshotFileIdentity{size: info.Size(), modUnixNano: info.ModTime().UnixNano()}, nil
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
