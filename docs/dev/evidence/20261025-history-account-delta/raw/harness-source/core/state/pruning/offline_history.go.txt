package pruning

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

// OfflineHistoryOptions describes one history-only maintenance transaction.
// The caller must hold exclusive ownership of a stopped datadir for the entire
// call, including the snapshot directory. No node or sync service is started.
type OfflineHistoryOptions struct {
	SnapshotDir     string
	ExpectedChain   snapshots.ChainIdentity
	Policy          Policy
	SolidifiedBlock uint64
	MaxBlocks       uint64
	MaxTxNums       uint64
	MaxInputBytes   uint64
	ETL             etl.Options
	CanonicalHash   func(uint64) (common.Hash, bool, error)
	// LegacyManifestSHA256 authorizes only this exact unbound manifest after
	// canonical genesis and every state-history boundary have been checked.
	// It never assigns an identity to the manifest or its other datasets.
	LegacyManifestSHA256 string
	// Sync must durably sync all earlier database writes, including delete
	// batches. A memtable flush is unnecessary if the WAL is durably synced.
	// Planning does not call Sync and permits it to be nil.
	Sync func() error
	// BeforeWrite is called after the read-only source scan and before either
	// building or semantic verification (which can use ETL scratch). It is a
	// mandatory admission gate, not a replacement for monitoring free space
	// and cancelling ctx during the pass.
	BeforeWrite func(OfflineHistoryPlan) error
}

type OfflineHistoryPlan struct {
	ManifestSHA256         string                   `json:"manifestSHA256"`
	ManifestGeneration     uint64                   `json:"manifestGeneration"`
	LegacyManifestVerified bool                     `json:"legacyManifestVerified"`
	Mode                   Mode                     `json:"mode"`
	HistoryWindow          uint64                   `json:"historyWindow"`
	SolidifiedBlock        uint64                   `json:"solidifiedBlock"`
	FinishBlock            uint64                   `json:"finishBlock"`
	FinishHash             common.Hash              `json:"finishHash"`
	PruneHead              uint64                   `json:"pruneHead"`
	PruneHeadHash          common.Hash              `json:"pruneHeadHash"`
	EligibleCutoffBlock    uint64                   `json:"eligibleCutoffBlock"`
	PreviousPruneBlock     uint64                   `json:"previousPruneBlock"`
	PreviousPruneTxNum     uint64                   `json:"previousPruneTxNum"`
	HistoryFrontierTxNum   uint64                   `json:"historyFrontierTxNum"`
	FromBlock              uint64                   `json:"fromBlock"`
	ToBlock                uint64                   `json:"toBlock"`
	FromTxNum              uint64                   `json:"fromTxNum"`
	ToTxNum                uint64                   `json:"toTxNum"`
	Blocks                 uint64                   `json:"blocks"`
	TxNums                 uint64                   `json:"txNums"`
	BuildNeeded            bool                     `json:"buildNeeded"`
	CoverageRefs           []snapshots.SegmentRef   `json:"coverageRefs,omitempty"`
	VerificationBytes      uint64                   `json:"verificationBytes"`
	Input                  OfflineHistoryInputStats `json:"input"`
	rows                   []rawdb.StateTxRange
}

type OfflineHistoryResult struct {
	Plan                         OfflineHistoryPlan     `json:"plan"`
	Built                        bool                   `json:"built"`
	Segments                     []snapshots.SegmentRef `json:"segments,omitempty"`
	BytesBuilt                   uint64                 `json:"bytesBuilt"`
	PublicationAttempted         bool                   `json:"publicationAttempted"`
	Published                    bool                   `json:"published"`
	DeletedBlocks                uint64                 `json:"deletedBlocks"`
	ComparedRecords              uint64                 `json:"comparedRecords"`
	DeletesDurable               bool                   `json:"deletesDurable"`
	ProgressPublicationAttempted bool                   `json:"progressPublicationAttempted"`
	ProgressPublished            bool                   `json:"progressPublished"`
	ProgressDurable              bool                   `json:"progressDurable"`
	ElapsedSeconds               float64                `json:"elapsedSeconds"`
}

// PlanOfflineHistoryContext reads a bounded range of authoritative hot records
// without writing files, opening ETL collectors or changing database progress.
// Already-covered blocks are selected first, so retries do not rebuild a trio
// whose publication succeeded before an interrupted deletion/progress phase.
func PlanOfflineHistoryContext(ctx context.Context, db ethdb.KeyValueStore, opts OfflineHistoryOptions) (OfflineHistoryPlan, error) {
	plan, _, err := planOfflineHistory(ctx, db, opts)
	return plan, err
}

func planOfflineHistory(ctx context.Context, db ethdb.KeyValueStore, opts OfflineHistoryOptions) (OfflineHistoryPlan, *snapshots.Manifest, error) {
	var plan OfflineHistoryPlan
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return plan, nil, err
	}
	if db == nil || opts.SnapshotDir == "" || opts.CanonicalHash == nil {
		return plan, nil, errors.New("pruning: offline history requires database, snapshot directory and canonical hash lookup")
	}
	if opts.Policy.Mode != ModeSnap && opts.Policy.Mode != ModeArchive || opts.Policy.HistoryWindow == 0 {
		return plan, nil, errors.New("pruning: offline history requires snap/archive with a positive hot retention window")
	}
	if err := opts.Policy.Validate(); err != nil {
		return plan, nil, err
	}
	if opts.MaxBlocks == 0 || opts.MaxBlocks > 5000 || opts.MaxTxNums == 0 || opts.MaxInputBytes == 0 {
		return plan, nil, errors.New("pruning: offline history requires max-blocks in [1,5000] and positive transaction/input byte limits")
	}
	mode, ok, err := rawdb.ReadHistoryPruneMode(db)
	if err != nil || !ok || mode != string(opts.Policy.Mode) {
		return plan, nil, fmt.Errorf("pruning: offline history persisted mode %q (present=%t) differs from %q: %w", mode, ok, opts.Policy.Mode, offlineHistoryCause(err))
	}
	plan.ManifestSHA256, err = offlineHistoryManifestDigest(opts.SnapshotDir)
	if err != nil {
		return plan, nil, err
	}
	manifest, err := snapshots.LoadProductionManifest(opts.SnapshotDir)
	if err != nil {
		return plan, nil, err
	}
	if err := offlineHistoryRequireManifest(opts.SnapshotDir, plan.ManifestSHA256); err != nil {
		return plan, nil, err
	}
	if opts.LegacyManifestSHA256 != "" && opts.LegacyManifestSHA256 != plan.ManifestSHA256 {
		return plan, nil, errors.New("pruning: offline legacy manifest SHA256 does not match the explicit pin")
	}
	if manifest.Chain != nil {
		if err := manifest.ValidateChainIdentity(opts.ExpectedChain); err != nil {
			return plan, nil, err
		}
	} else {
		if opts.LegacyManifestSHA256 == "" {
			return plan, nil, errors.New("pruning: unbound manifest requires its exact legacy manifest SHA256 and canonical boundary proof")
		}
		if err := snapshots.VerifyLegacyStateDomainHistoryBoundariesContext(ctx, db, opts.SnapshotDir, manifest, opts.ExpectedChain, opts.CanonicalHash); err != nil {
			return plan, nil, err
		}
		plan.LegacyManifestVerified = true
	}
	plan.ManifestGeneration = manifest.Generation
	plan.Mode, plan.HistoryWindow = opts.Policy.Mode, opts.Policy.HistoryWindow
	plan.SolidifiedBlock = opts.SolidifiedBlock
	finish, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageFinish)
	if err != nil || !ok || !finish.HasBlockHash {
		return plan, nil, fmt.Errorf("pruning: offline history requires a hash-bound Finish stage: %w", offlineHistoryCause(err))
	}
	if err := offlineHistoryCheckHash(opts, finish.BlockNum, finish.BlockHash); err != nil {
		return plan, nil, err
	}
	plan.FinishBlock, plan.FinishHash = finish.BlockNum, finish.BlockHash
	plan.PruneHead = min(opts.SolidifiedBlock, finish.BlockNum)
	plan.PruneHeadHash, ok, err = opts.CanonicalHash(plan.PruneHead)
	if err != nil || !ok || plan.PruneHeadHash == (common.Hash{}) {
		return plan, nil, fmt.Errorf("pruning: offline history prune head %d has no canonical hash: %w", plan.PruneHead, offlineHistoryCause(err))
	}
	if manifest.Progress != nil {
		plan.PreviousPruneBlock = manifest.Progress.HotPruneBlockNum
		plan.PreviousPruneTxNum = manifest.Progress.HotPruneTxNum
	}
	if plan.PreviousPruneBlock == 0 && plan.PreviousPruneTxNum != 0 {
		return plan, nil, errors.New("pruning: offline history cursor has a transaction without a block")
	}
	if plan.PreviousPruneBlock != 0 {
		row, ok, err := rawdb.ReadStateTxRange(db, plan.PreviousPruneBlock)
		if err != nil || !ok || row.EndTxNum != plan.PreviousPruneTxNum {
			return plan, nil, fmt.Errorf("pruning: offline history hot-prune cursor has no matching retained tx range: %w", offlineHistoryCause(err))
		}
		if err := offlineHistoryCheckHash(opts, row.BlockNum, row.BlockHash); err != nil {
			return plan, nil, err
		}
	}
	histories := offlineHistoryRefs(manifest)
	for i, ref := range histories {
		if i > 0 && (histories[i-1].ToTxNum == math.MaxUint64 || ref.FromTxNum != histories[i-1].ToTxNum+1) {
			return plan, nil, errors.New("pruning: offline history refuses a gap in active cold history")
		}
		plan.HistoryFrontierTxNum = ref.ToTxNum
	}
	if plan.PreviousPruneTxNum > plan.HistoryFrontierTxNum {
		return plan, nil, errors.New("pruning: offline history hot-prune cursor exceeds cold history")
	}
	for _, stage := range []rawdb.StageID{rawdb.StageSnapshotHistory, rawdb.StageSnapshotAccessor, rawdb.StageSnapshotHotPrune} {
		row, ok, err := rawdb.ReadStageProgressRow(db, stage)
		if err != nil {
			return plan, nil, err
		}
		limit := plan.HistoryFrontierTxNum
		if stage == rawdb.StageSnapshotAccessor && manifest.Progress != nil {
			limit = max(limit, manifest.Progress.AccessorBuildTxNum)
		}
		if stage == rawdb.StageSnapshotHotPrune {
			limit = plan.PreviousPruneTxNum
		}
		if ok && row.BlockNum > limit {
			return plan, nil, fmt.Errorf("pruning: offline history %s stage %d exceeds manifest boundary %d", stage, row.BlockNum, limit)
		}
	}
	if row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSnapshotBuild); err != nil {
		return plan, nil, err
	} else if ok && row.BlockNum != 0 {
		if !row.HasBlockHash {
			return plan, nil, errors.New("pruning: offline history requires a hash-bound SnapshotBuild stage")
		}
		if err := offlineHistoryCheckHash(opts, row.BlockNum, row.BlockHash); err != nil {
			return plan, nil, err
		}
		source, exists, err := rawdb.ReadStateTxRange(db, row.BlockNum)
		if err != nil || !exists || source.EndTxNum > plan.HistoryFrontierTxNum {
			return plan, nil, fmt.Errorf("pruning: SnapshotBuild exceeds authenticated cold frontier: %w", offlineHistoryCause(err))
		}
	}
	if plan.PruneHead < opts.Policy.HistoryWindow {
		return plan, manifest, nil
	}
	plan.EligibleCutoffBlock = plan.PruneHead - opts.Policy.HistoryWindow
	if plan.PreviousPruneBlock >= plan.EligibleCutoffBlock {
		return plan, manifest, nil
	}
	start := plan.PreviousPruneBlock + 1
	cfg, _ := snapshots.DefaultDomainRegistry().Dataset(snapshots.SegmentDatasetStateDomainChange)
	err = cfg.IterateHotHistoryTxRangeBorrowed(db, start, plan.EligibleCutoffBlock, func(row *rawdb.StateTxRange) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if row == nil || row.EndTxNum < row.BeginTxNum || row.BlockNum != start+plan.Blocks {
			return false, errors.New("pruning: offline history requires contiguous complete block tx ranges")
		}
		if err := offlineHistoryCheckHash(opts, row.BlockNum, row.BlockHash); err != nil {
			return false, err
		}
		if plan.Blocks > 0 && (plan.ToTxNum == math.MaxUint64 || row.BeginTxNum != plan.ToTxNum+1) {
			return false, errors.New("pruning: offline history transaction range is not contiguous")
		}
		covered := offlineHistoryCovered(histories, row.BeginTxNum, row.EndTxNum)
		if plan.Blocks == 0 {
			if plan.PreviousPruneTxNum == math.MaxUint64 || row.BeginTxNum != plan.PreviousPruneTxNum+1 {
				return false, errors.New("pruning: offline history first tx range does not follow the hot-prune cursor")
			}
			plan.BuildNeeded = !covered
			if !covered && row.BeginTxNum != plan.HistoryFrontierTxNum+1 {
				return false, errors.New("pruning: offline history build does not continue the cold frontier")
			}
		} else if covered == plan.BuildNeeded {
			return false, nil // Never mix prune-only and newly-built ranges.
		}
		txNums := row.EndTxNum - row.BeginTxNum + 1
		if txNums == 0 || txNums > opts.MaxTxNums {
			return false, fmt.Errorf("pruning: block %d exceeds offline transaction limit %d", row.BlockNum, opts.MaxTxNums)
		}
		if plan.TxNums > opts.MaxTxNums-txNums {
			return false, nil
		}
		input, err := scanOfflineHistoryBlock(ctx, db, *row, opts.MaxInputBytes)
		if err != nil {
			return false, err
		}
		if input.InputBytes > opts.MaxInputBytes {
			return false, fmt.Errorf("pruning: block %d requires %d input bytes, above offline limit %d", row.BlockNum, input.InputBytes, opts.MaxInputBytes)
		}
		if plan.Input.InputBytes > opts.MaxInputBytes-input.InputBytes {
			return false, nil
		}
		if err := plan.Input.Add(input); err != nil {
			return false, err
		}
		if plan.Blocks == 0 {
			plan.FromBlock, plan.FromTxNum = row.BlockNum, row.BeginTxNum
		}
		plan.ToBlock, plan.ToTxNum = row.BlockNum, row.EndTxNum
		plan.Blocks++
		plan.TxNums += txNums
		plan.rows = append(plan.rows, *row)
		return plan.Blocks < opts.MaxBlocks, nil
	})
	if err != nil {
		return plan, nil, err
	}
	if plan.Blocks == 0 && start <= plan.EligibleCutoffBlock {
		return plan, nil, errors.New("pruning: offline history has no source tx range at the hot-prune frontier")
	}
	if !plan.BuildNeeded && plan.Blocks > 0 {
		plan.CoverageRefs, err = offlineHistoryCoverageRefs(manifest, plan.FromTxNum, plan.ToTxNum)
		if err != nil {
			return plan, nil, err
		}
		plan.VerificationBytes, err = snapshots.InspectStateDomainHistoryVerificationContext(ctx, opts.SnapshotDir, plan.CoverageRefs)
		if err != nil {
			return plan, nil, err
		}
	} else if plan.Blocks > 0 {
		if plan.Input.Records > (math.MaxUint64-(1<<20))/51 {
			return plan, nil, errors.New("pruning: offline verification size overflows")
		}
		plan.VerificationBytes = 51*plan.Input.Records + (1 << 20)
	}
	publications := uint64(1)
	if plan.BuildNeeded {
		publications++
	}
	if plan.ManifestGeneration > math.MaxUint64-publications {
		return plan, nil, errors.New("pruning: offline history manifest generation overflows")
	}
	if err := offlineHistoryRequireManifest(opts.SnapshotDir, plan.ManifestSHA256); err != nil {
		return plan, nil, err
	}
	return plan, manifest, nil
}

// OfflineHistoryPassContext executes at most one bounded plan. It deliberately
// bypasses the online lifecycle: no latest scan, derived sidecar, code prune,
// posting sweep or cold merge is invoked. Output files are never unlinked after
// attempting manifest publication, including an error after rename/fsync.
func OfflineHistoryPassContext(ctx context.Context, db ethdb.KeyValueStore, opts OfflineHistoryOptions) (OfflineHistoryResult, error) {
	return offlineHistoryPassContext(ctx, db, opts, snapshots.PublishManifest)
}

func offlineHistoryPassContext(ctx context.Context, db ethdb.KeyValueStore, opts OfflineHistoryOptions, publish func(string, *snapshots.Manifest) error) (result OfflineHistoryResult, err error) {
	started := time.Now()
	defer func() { result.ElapsedSeconds = time.Since(started).Seconds() }()
	if ctx == nil {
		ctx = context.Background()
	}
	if opts.Sync == nil || opts.BeforeWrite == nil {
		return result, errors.New("pruning: offline history requires durable Sync and BeforeWrite admission callbacks")
	}
	plan, manifest, err := planOfflineHistory(ctx, db, opts)
	result.Plan = plan
	if err != nil || plan.Blocks == 0 {
		return result, err
	}
	if err := opts.BeforeWrite(plan); err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := offlineHistoryRecheck(db, opts, plan); err != nil {
		return result, err
	}
	refs := plan.CoverageRefs
	if plan.BuildNeeded {
		cfg, _ := snapshots.DefaultDomainRegistry().Dataset(snapshots.SegmentDatasetStateDomainChange)
		refs, err = snapshots.BuildStateDomainChangeHistorySegmentsFromDBByBlockRangeContext(ctx, db, opts.SnapshotDir,
			plan.FromTxNum, plan.ToTxNum, plan.FromBlock, plan.ToBlock, cfg.HistoryPath(plan.FromTxNum, plan.ToTxNum), opts.ETL)
		if err != nil {
			return result, err
		}
		result.Built, result.Segments = true, refs
		for _, ref := range refs {
			result.BytesBuilt += ref.Size
		}
	}
	// Exhaustive source/companion validation is mandatory even for newly built
	// trios; the builder's fast self-check is not this deletion gate.
	verifyManifest := snapshots.NewManifestForChain(refs[0].FromTxNum, refs[len(refs)-1].ToTxNum, refs, opts.ExpectedChain)
	for _, ref := range refs {
		verifyManifest.VisibleTxStart = min(verifyManifest.VisibleTxStart, ref.FromTxNum)
		verifyManifest.VisibleTxEnd = max(verifyManifest.VisibleTxEnd, ref.ToTxNum)
	}
	if _, err := snapshots.VerifyLoadedManifestFiles(opts.SnapshotDir, verifyManifest, snapshots.VerifyManifestOptions{
		Context: ctx, ExpectedChain: &opts.ExpectedChain, RequireRegistered: true, RequireChecksums: true,
	}); err != nil {
		return result, err
	}
	result.ComparedRecords, err = compareOfflineHistoryToHot(ctx, db, opts.SnapshotDir, verifyManifest, plan)
	if err != nil {
		return result, err
	}
	if err := snapshots.SyncHistorySegmentDirectories(opts.SnapshotDir, refs); err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := offlineHistoryRecheck(db, opts, plan); err != nil {
		return result, err
	}
	if plan.BuildNeeded {
		manifest = offlineHistoryAppendManifest(manifest, refs, plan.ToTxNum)
		if err := manifest.ValidateProduction(); err != nil {
			return result, err
		}
		result.PublicationAttempted = true
		if err := publish(opts.SnapshotDir, manifest); err != nil {
			return result, err // Publication may already be visible; keep every output.
		}
		result.Published = true
		plan.ManifestSHA256, err = offlineHistoryManifestDigest(opts.SnapshotDir)
		if err != nil {
			return result, err
		}
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := offlineHistoryRecheck(db, opts, plan); err != nil {
		return result, err
	}
	store, flush := newPruneBatchStore(db)
	store = pruneBatchStore{KeyValueReader: store, KeyValueWriter: offlineHistoryContextWriter{ctx: ctx, KeyValueWriter: store}, Iteratee: store}
	blocks := make([]uint64, len(plan.rows))
	for i, row := range plan.rows {
		blocks[i] = row.BlockNum
	}
	cfg, _ := snapshots.DefaultDomainRegistry().Dataset(snapshots.SegmentDatasetStateDomainChange)
	if err := cfg.DeleteHotHistoryBlocks(store, blocks); err != nil {
		return result, err
	}
	if err := flush(); err != nil {
		return result, err
	}
	result.DeletedBlocks = uint64(len(blocks))
	if err := opts.Sync(); err != nil {
		return result, fmt.Errorf("pruning: offline history delete durability barrier: %w", err)
	}
	result.DeletesDurable = true
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := offlineHistoryRequireManifest(opts.SnapshotDir, plan.ManifestSHA256); err != nil {
		return result, err
	}
	if manifest.Progress == nil {
		manifest.Progress = new(snapshots.Progress)
	}
	if manifest.Generation == math.MaxUint64 {
		return result, errors.New("pruning: offline history manifest generation overflows")
	}
	manifest.Generation++
	manifest.PublishedUnix = time.Now().Unix()
	manifest.Progress.HotPruneBlockNum = plan.ToBlock
	manifest.Progress.HotPruneTxNum = plan.ToTxNum
	result.ProgressPublicationAttempted = true
	if err := publish(opts.SnapshotDir, manifest); err != nil {
		return result, err // Deletes were durable before this cursor became visible.
	}
	result.ProgressPublished = true
	batch := db.NewBatch()
	if err := rawdb.WriteStageProgress(batch, rawdb.StageSnapshotHotPrune, plan.ToTxNum); err != nil {
		return result, err
	}
	// Repair manifest-first/stage-second interruption during prune-only retries
	// as well. The accessor stage can also represent other datasets, so never
	// overwrite a higher existing position with this smaller history batch.
	for _, stage := range []rawdb.StageID{rawdb.StageSnapshotHistory, rawdb.StageSnapshotAccessor} {
		row, ok, err := rawdb.ReadStageProgressRow(db, stage)
		if err != nil {
			return result, err
		}
		if !ok || row.BlockNum < plan.ToTxNum {
			if err := rawdb.WriteStageProgress(batch, stage, plan.ToTxNum); err != nil {
				return result, err
			}
		}
	}
	buildRow, buildExists, err := rawdb.ReadStageProgressRow(db, rawdb.StageSnapshotBuild)
	if err != nil {
		return result, err
	}
	if !buildExists || buildRow.BlockNum < plan.ToBlock {
		if err := rawdb.WriteStageProgressWithHash(batch, rawdb.StageSnapshotBuild, plan.ToBlock, plan.rows[len(plan.rows)-1].BlockHash); err != nil {
			return result, err
		}
	}
	if err := batch.Write(); err != nil {
		return result, err
	}
	if err := opts.Sync(); err != nil {
		return result, fmt.Errorf("pruning: offline history progress durability barrier: %w", err)
	}
	result.ProgressDurable = true
	return result, nil
}

func offlineHistoryAppendManifest(old *snapshots.Manifest, refs []snapshots.SegmentRef, end uint64) *snapshots.Manifest {
	manifest := *old
	manifest.Segments = append(append([]snapshots.SegmentRef(nil), old.Segments...), refs...)
	manifest.Generation++
	manifest.PublishedUnix = time.Now().Unix()
	manifest.VisibleTxEnd = max(manifest.VisibleTxEnd, end)
	manifest.Progress = new(snapshots.Progress)
	if old.Progress != nil {
		*manifest.Progress = *old.Progress
	}
	manifest.Progress.HistoryBuildTxNum = end
	manifest.Progress.AccessorBuildTxNum = max(manifest.Progress.AccessorBuildTxNum, end)
	return &manifest
}

func offlineHistoryRefs(manifest *snapshots.Manifest) []snapshots.SegmentRef {
	var refs []snapshots.SegmentRef
	for _, ref := range manifest.Segments {
		if ref.NormalizedDataset() == snapshots.SegmentDatasetStateDomainChange && ref.Kind == snapshots.SegmentHistory {
			refs = append(refs, ref)
		}
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].FromTxNum < refs[j].FromTxNum })
	return refs
}

func offlineHistoryCovered(refs []snapshots.SegmentRef, from, to uint64) bool {
	next := from
	for _, ref := range refs {
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

func offlineHistoryCoverageRefs(manifest *snapshots.Manifest, from, to uint64) ([]snapshots.SegmentRef, error) {
	cfg, _ := snapshots.DefaultDomainRegistry().Dataset(snapshots.SegmentDatasetStateDomainChange)
	var refs []snapshots.SegmentRef
	for _, ref := range offlineHistoryRefs(manifest) {
		if ref.ToTxNum < from || ref.FromTxNum > to {
			continue
		}
		index, ok := cfg.HistoryIndexRef(manifest, ref)
		if !ok {
			return nil, errors.New("pruning: offline history missing index companion")
		}
		accessor, ok := cfg.HistoryAccessorRef(manifest, ref)
		if !ok {
			return nil, errors.New("pruning: offline history missing accessor companion")
		}
		refs = append(refs, ref, index, accessor)
	}
	if len(refs) == 0 {
		return nil, errors.New("pruning: offline history has no verified range candidates")
	}
	return refs, nil
}

func offlineHistoryRecheck(db ethdb.KeyValueStore, opts OfflineHistoryOptions, plan OfflineHistoryPlan) error {
	if err := offlineHistoryRequireManifest(opts.SnapshotDir, plan.ManifestSHA256); err != nil {
		return err
	}
	finish, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageFinish)
	if err != nil || !ok || !finish.HasBlockHash || finish.BlockNum != plan.FinishBlock || finish.BlockHash != plan.FinishHash {
		return errors.New("pruning: offline history Finish boundary changed")
	}
	if err := offlineHistoryCheckHash(opts, plan.PruneHead, plan.PruneHeadHash); err != nil {
		return err
	}
	for _, expected := range plan.rows {
		row, ok, err := rawdb.ReadStateTxRange(db, expected.BlockNum)
		if err != nil || !ok || *row != expected {
			return fmt.Errorf("pruning: offline history source tx range changed at block %d", expected.BlockNum)
		}
		if err := offlineHistoryCheckHash(opts, row.BlockNum, row.BlockHash); err != nil {
			return err
		}
	}
	return nil
}

func offlineHistoryCheckHash(opts OfflineHistoryOptions, block uint64, expected common.Hash) error {
	hash, ok, err := opts.CanonicalHash(block)
	if err != nil || !ok || expected == (common.Hash{}) || hash != expected {
		return fmt.Errorf("pruning: offline history canonical hash mismatch/unavailable at block %d: %w", block, offlineHistoryCause(err))
	}
	return nil
}

func offlineHistoryCause(err error) error {
	if err != nil {
		return err
	}
	return errors.New("required boundary is absent or inconsistent")
}

func offlineHistoryManifestDigest(dir string) (string, error) {
	data, err := os.ReadFile(filepath.Join(dir, snapshots.ManifestFile))
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

func offlineHistoryRequireManifest(dir, expected string) error {
	actual, err := offlineHistoryManifestDigest(dir)
	if err != nil {
		return err
	}
	if actual != expected {
		return errors.New("pruning: offline history manifest changed; exclusive ownership is required")
	}
	return nil
}

type offlineHistoryContextWriter struct {
	ctx context.Context
	ethdb.KeyValueWriter
}

func (w offlineHistoryContextWriter) Put(key, value []byte) error {
	if err := w.ctx.Err(); err != nil {
		return err
	}
	return w.KeyValueWriter.Put(key, value)
}

func (w offlineHistoryContextWriter) Delete(key []byte) error {
	if err := w.ctx.Err(); err != nil {
		return err
	}
	return w.KeyValueWriter.Delete(key)
}
