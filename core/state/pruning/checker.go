package pruning

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	statepkg "github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

type CheckReport struct {
	LatestRows            int
	AccountLatestRows     int
	KVLatestRows          int
	KVGenerationRows      int
	CommitmentDomainRows  int
	CommitmentRootPresent bool
	ReferencedCodeHashes  int
	RetainedTxRanges      int
	RetainedDomainChanges int
	CommitmentCheckpoints int
	SnapshotSegments      int
	MuHashSnapshotPresent bool
	Warnings              []string
}

type Checker struct {
	DB          Store
	Policy      Policy
	SnapshotDir string
}

func latestDomainConfig(dataset snapshots.SegmentDataset) (snapshots.DomainCfg, error) {
	cfg, ok := snapshots.DefaultDomainRegistry().Dataset(dataset)
	if !ok || !cfg.HasLatest {
		return snapshots.DomainCfg{}, fmt.Errorf("pruning: missing latest domain config for %s", dataset)
	}
	return cfg, nil
}

func Check(db Store, policy Policy, headNum uint64, snapshotDir string) (CheckReport, error) {
	return Checker{DB: db, Policy: policy, SnapshotDir: snapshotDir}.Check(headNum)
}

func (c Checker) Check(headNum uint64) (CheckReport, error) {
	if c.DB == nil {
		return CheckReport{}, errors.New("pruning: nil database")
	}
	if err := c.Policy.Validate(); err != nil {
		return CheckReport{}, err
	}
	var report CheckReport
	codeHashes := make(codeHashRefs)
	accountCfg, err := latestDomainConfig(snapshots.SegmentDatasetAccountLatest)
	if err != nil {
		return CheckReport{}, err
	}
	if accountCfg.IterateHotAccountLatest == nil {
		return CheckReport{}, errors.New("pruning: missing account latest iterator")
	}
	if err := accountCfg.IterateHotAccountLatest(c.DB, nil, func(row rawdb.StateAccountLatestRow) (bool, error) {
		report.AccountLatestRows++
		report.LatestRows++
		txNum, err := snapshots.StateDomainHistoryTxNumAtBlockEnd(c.DB, headNum)
		if err != nil {
			return false, err
		}
		hash, err := decodeAccountEnvelopeCodeHash(row.Value)
		if err != nil {
			return false, fmt.Errorf("pruning: decode account latest %x: %w", row.Owner, err)
		}
		codeHashes.add(hash, txNum)
		return true, nil
	}); err != nil {
		return CheckReport{}, err
	}
	kvCfg, err := latestDomainConfig(snapshots.SegmentDatasetKVLatest)
	if err != nil {
		return CheckReport{}, err
	}
	if kvCfg.IterateHotKVLatestRows == nil {
		return CheckReport{}, errors.New("pruning: missing account-KV latest iterator")
	}
	if err := kvCfg.IterateHotKVLatestRows(c.DB, func(row rawdb.StateKVLatestRow) (bool, error) {
		report.KVLatestRows++
		report.LatestRows++
		return true, nil
	}); err != nil {
		return CheckReport{}, err
	}
	generationCfg, err := latestDomainConfig(snapshots.SegmentDatasetKVGeneration)
	if err != nil {
		return CheckReport{}, err
	}
	if generationCfg.IterateHotKVGeneration == nil {
		return CheckReport{}, errors.New("pruning: missing KV generation iterator")
	}
	if err := generationCfg.IterateHotKVGeneration(c.DB, nil, func(row rawdb.StateKVGenerationRow) (bool, error) {
		report.KVGenerationRows++
		report.LatestRows++
		return true, nil
	}); err != nil {
		return CheckReport{}, err
	}
	checkpointCfg, err := latestDomainConfig(snapshots.SegmentDatasetCommitmentCheckpoint)
	if err != nil {
		return CheckReport{}, err
	}
	if checkpointCfg.ReadHotLatestCommitmentCheckpoint == nil {
		return CheckReport{}, errors.New("pruning: missing latest commitment checkpoint reader")
	}
	if err := rawdb.IterateStateCommitmentDomain(c.DB, nil, func(logicalKey, value []byte) (bool, error) {
		report.CommitmentDomainRows++
		switch {
		case rawdb.IsLatestDomainCommitmentRootLogicalKey(logicalKey):
			report.CommitmentRootPresent = true
			if len(value) != common.HashLength {
				return false, fmt.Errorf("pruning: latest commitment root has bad length %d", len(value))
			}
		case rawdb.IsLatestStateCommitmentCheckpointLogicalKey(logicalKey):
			cp, ok, err := checkpointCfg.ReadHotLatestCommitmentCheckpoint(c.DB)
			if err != nil || !ok {
				return false, fmt.Errorf("pruning: latest commitment checkpoint pointer unreadable ok=%v err=%v", ok, err)
			}
			if cp.Scheme == "" {
				return false, fmt.Errorf("pruning: latest commitment checkpoint pointer for block %d has empty scheme", cp.BlockNum)
			}
		case rawdb.IsStateCommitmentCheckpointLogicalKey(logicalKey):
		default:
			report.Warnings = append(report.Warnings, fmt.Sprintf("unknown commitment-domain row %x", logicalKey))
		}
		return true, nil
	}); err != nil {
		return CheckReport{}, err
	}
	if report.LatestRows > 0 && !report.CommitmentRootPresent {
		return CheckReport{}, errors.New("pruning: flat latest rows present without latest CommitmentDomain root")
	}
	if err := c.collectHistoryCodeHashes(codeHashes); err != nil {
		return CheckReport{}, err
	}
	report.ReferencedCodeHashes = len(codeHashes)
	if err := c.checkReferencedCodeHashes(codeHashes); err != nil {
		return CheckReport{}, err
	}
	coverage, err := (Worker{Policy: c.Policy, SnapshotDir: c.SnapshotDir}).snapshotStateDomainChangeCoverage()
	if err != nil {
		return CheckReport{}, err
	}
	historyCfg, ok := snapshots.DefaultDomainRegistry().Dataset(snapshots.SegmentDatasetStateDomainChange)
	if !ok || historyCfg.IterateHotHistoryTxRanges == nil {
		return CheckReport{}, errors.New("pruning: missing state-domain hot history tx-range iterator")
	}
	if err := historyCfg.IterateHotHistoryTxRanges(c.DB, func(row *rawdb.StateTxRange) (bool, error) {
		if row.EndTxNum < row.BeginTxNum {
			return false, fmt.Errorf("pruning: state tx range for block %d is inverted", row.BlockNum)
		}
		if !c.Policy.RetainHotHistory(row.BlockNum, headNum) {
			if c.Policy.Mode == ModeFull || c.Policy.Mode == ModeBlocks || c.Policy.Mode == ModeMinimal {
				report.Warnings = append(report.Warnings, fmt.Sprintf("state tx range for prunable block %d is still present", row.BlockNum))
				return true, nil
			}
			if c.Policy.Mode == ModeSnap || c.Policy.Mode == ModeArchive {
				report.RetainedTxRanges++
				blockChanges, err := countHotHistoryChangesInTxRange(historyCfg, c.DB, row.BeginTxNum, row.EndTxNum, func() {
					report.RetainedDomainChanges++
				})
				if err != nil {
					return false, err
				}
				if blockChanges > 0 && coverage.covers(row.BeginTxNum, row.EndTxNum) {
					report.Warnings = append(report.Warnings, fmt.Sprintf("state domain changes for snapshot-covered prunable block %d are still present", row.BlockNum))
				}
			}
			return true, nil
		}
		report.RetainedTxRanges++
		if _, err := countHotHistoryChangesInTxRange(historyCfg, c.DB, row.BeginTxNum, row.EndTxNum, func() {
			report.RetainedDomainChanges++
		}); err != nil {
			return false, err
		}
		return true, nil
	}); err != nil {
		return CheckReport{}, err
	}
	if checkpointCfg.IterateHotCommitmentCheckpoints == nil {
		return CheckReport{}, errors.New("pruning: missing commitment checkpoint iterator")
	}
	if err := checkpointCfg.IterateHotCommitmentCheckpoints(c.DB, func(cp *rawdb.StateCommitmentCheckpoint) (bool, error) {
		if cp.Scheme == "" {
			return false, fmt.Errorf("pruning: commitment checkpoint for block %d has empty scheme", cp.BlockNum)
		}
		if !c.Policy.RetainReorgData(cp.BlockNum, headNum) {
			report.Warnings = append(report.Warnings, fmt.Sprintf("commitment checkpoint for prunable block %d is still present", cp.BlockNum))
			return true, nil
		}
		report.CommitmentCheckpoints++
		return true, nil
	}); err != nil {
		return CheckReport{}, err
	}
	if c.SnapshotDir != "" {
		if err := c.checkSnapshots(&report); err != nil {
			return CheckReport{}, err
		}
	}
	return report, nil
}

func countHotHistoryChangesInTxRange(cfg snapshots.DomainCfg, db ethdb.Iteratee, fromTxNum, toTxNum uint64, onChange func()) (int, error) {
	count := 0
	err := cfg.IterateHotHistoryChangesByTxRange(db, fromTxNum, toTxNum, func(change *rawdb.StateDomainChange) (bool, error) {
		if change == nil {
			return true, nil
		}
		count++
		if onChange != nil {
			onChange()
		}
		return true, nil
	})
	return count, err
}

func CheckCodeHashes(db ethdb.KeyValueReader, hashes []common.Hash) error {
	if db == nil {
		return errors.New("pruning: nil database")
	}
	codeCfg, err := latestDomainConfig(snapshots.SegmentDatasetCode)
	if err != nil {
		return err
	}
	if codeCfg.ReadHotCode == nil {
		return errors.New("pruning: missing CodeDomain hot reader")
	}
	for _, hash := range hashes {
		if hash == (common.Hash{}) || hash == emptyCodeHash {
			continue
		}
		if code, ok, err := codeCfg.ReadHotCode(db, hash); err != nil {
			return err
		} else if !ok || len(code) == 0 {
			return fmt.Errorf("pruning: missing state code for hash %x", hash)
		}
	}
	return nil
}

// codeHashRefs records the earliest state txNum that references each immutable,
// content-addressed code hash. CodeDomain snapshot visibility is monotonic: a
// segment visible at the earliest reference remains visible at every later
// reference, while coverage that starts later cannot satisfy that earliest
// historical account image.
type codeHashRefs map[common.Hash]uint64

var emptyCodeHash = common.Keccak256(nil)

func (refs codeHashRefs) add(hash common.Hash, txNum uint64) {
	if !isMeaningfulCodeHash(hash) {
		return
	}
	if earliest, ok := refs[hash]; !ok || txNum < earliest {
		refs[hash] = txNum
	}
}

func (refs codeHashRefs) merge(other codeHashRefs) {
	for hash, txNum := range other {
		refs.add(hash, txNum)
	}
}

func (c Checker) collectHistoryCodeHashes(refs codeHashRefs) error {
	return c.collectHistoryCodeHashesContext(context.Background(), refs)
}

func (c Checker) collectHistoryCodeHashesContext(ctx context.Context, refs codeHashRefs) error {
	return c.collectHistoryCodeHashesFilteredContext(ctx, refs, nil)
}

func (c Checker) collectHistoryCodeHashesFilteredContext(ctx context.Context, refs codeHashRefs, wanted map[common.Hash]struct{}) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	cfg, ok := snapshots.DefaultDomainRegistry().Dataset(snapshots.SegmentDatasetStateDomainChange)
	if !ok {
		return errors.New("pruning: missing state-domain history config")
	}
	if err := collectHotHistoryCodeHashesFiltered(ctx, cfg, c.DB, refs, wanted); err != nil {
		return err
	}
	return c.collectColdHistoryCodeHashesFilteredContext(ctx, refs, wanted, nil)
}

func (c Checker) collectColdHistoryCodeHashesFilteredContext(ctx context.Context, refs codeHashRefs, wanted map[common.Hash]struct{}, mgr *snapshots.Manager) error {
	if c.SnapshotDir == "" {
		return nil
	}
	if mgr == nil {
		var err error
		mgr, err = snapshots.OpenManager(c.SnapshotDir)
		if err != nil {
			return err
		}
	}
	manifest := mgr.Manifest()
	if manifest == nil {
		return nil
	}
	return mgr.IterateStateDomainChanges(manifest.VisibleTxStart, manifest.VisibleTxEnd, func(change *rawdb.StateDomainChange) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		return true, collectStateDomainChangeCodeHashesFiltered(refs, wanted, change)
	})
}

// collectHotHistoryCodeHashes uses the allocation-bounded current block-pack
// reader for the common case. Databases written by the retired standalone-row
// schema retain the owning reader's overwrite and compatibility semantics.
// Borrowed results stay isolated until the scan succeeds, so falling back after
// a mixed block-pack/repair block cannot retain a shadowed code reference.
func collectHotHistoryCodeHashes(ctx context.Context, cfg snapshots.DomainCfg, db ethdb.Iteratee, refs codeHashRefs) error {
	return collectHotHistoryCodeHashesFiltered(ctx, cfg, db, refs, nil)
}

func collectHotHistoryCodeHashesFiltered(ctx context.Context, cfg snapshots.DomainCfg, db ethdb.Iteratee, refs codeHashRefs, wanted map[common.Hash]struct{}) error {
	collectInto := func(dst codeHashRefs) func(*rawdb.StateDomainChange) (bool, error) {
		return func(change *rawdb.StateDomainChange) (bool, error) {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			return true, collectStateDomainChangeCodeHashesFiltered(dst, wanted, change)
		}
	}
	if cfg.IterateHotHistoryBlockTxBorrowed != nil {
		borrowedRefs := make(codeHashRefs)
		err := cfg.IterateHotHistoryBlockTxBorrowed(db, 0, ^uint64(0), 0, ^uint64(0), collectInto(borrowedRefs))
		if err == nil {
			refs.merge(borrowedRefs)
			return nil
		}
		if !errors.Is(err, rawdb.ErrStateDomainChangeBorrowedLegacyRows) {
			return err
		}
	}
	return cfg.IterateHotHistoryChangesByTxRange(db, 0, ^uint64(0), collectInto(refs))
}

func collectStateDomainChangeCodeHashes(refs codeHashRefs, change *rawdb.StateDomainChange) error {
	return collectStateDomainChangeCodeHashesFiltered(refs, nil, change)
}

func collectStateDomainChangeCodeHashesFiltered(refs codeHashRefs, wanted map[common.Hash]struct{}, change *rawdb.StateDomainChange) error {
	if change == nil || change.FlatDomain != rawdb.StateFlatDomainAccountLatest {
		return nil
	}
	if change.PrevExists {
		hash, err := decodeAccountEnvelopeCodeHash(change.Prev)
		if err != nil {
			return fmt.Errorf("pruning: decode state-domain-change prev block=%d seq=%d: %w", change.BlockNum, change.Seq, err)
		}
		if wanted == nil {
			refs.add(hash, change.TxNum)
		} else if _, ok := wanted[hash]; ok {
			refs.add(hash, change.TxNum)
		}
	}
	return nil
}

func decodeAccountEnvelopeCodeHash(data []byte) (common.Hash, error) {
	return statepkg.DecodeStateAccountCodeHash(data)
}

func (c Checker) checkReferencedCodeHashes(hashes codeHashRefs) error {
	if len(hashes) == 0 {
		return nil
	}
	var codeSnapshots *snapshots.Manager
	if c.SnapshotDir != "" {
		mgr, err := snapshots.OpenManager(c.SnapshotDir)
		if err != nil {
			return err
		}
		codeSnapshots = mgr
	}
	codeCfg, err := latestDomainConfig(snapshots.SegmentDatasetCode)
	if err != nil {
		return err
	}
	if codeCfg.ReadHotCode == nil {
		return errors.New("pruning: missing CodeDomain hot reader")
	}
	for hash, earliestTxNum := range hashes {
		covered, err := codeHashAvailableInSnapshot(codeSnapshots, hash, earliestTxNum)
		if err != nil {
			return err
		}
		if covered {
			continue
		}
		if code, ok, err := codeCfg.ReadHotCode(c.DB, hash); err != nil {
			return err
		} else if ok && len(code) > 0 {
			continue
		}
		return fmt.Errorf("pruning: account history references missing code hash %x", hash)
	}
	return nil
}

func codeHashAvailableInSnapshot(mgr *snapshots.Manager, hash common.Hash, txNum uint64) (bool, error) {
	if mgr == nil {
		return false, nil
	}
	code, ok, err := mgr.GetCodeAtOrBefore(hash, txNum)
	if err != nil {
		return false, err
	}
	return ok && len(code) > 0, nil
}

func isMeaningfulCodeHash(hash common.Hash) bool {
	return hash != (common.Hash{}) && hash != emptyCodeHash
}

func (c Checker) checkSnapshots(report *CheckReport) error {
	manifest, err := snapshots.LoadProductionManifest(c.SnapshotDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, ref := range manifest.Segments {
		report.SnapshotSegments++
		if cfg, ok := snapshots.DefaultDomainRegistry().ConfigForRef(ref); ok && cfg.HasHistory && ref.Kind == snapshots.SegmentHistory {
			if err := snapshots.VerifyHistorySegmentWithCompanions(c.SnapshotDir, manifest, ref); err != nil {
				return err
			}
			continue
		}
		checked, err := snapshots.CheckRegisteredSegment(c.SnapshotDir, ref)
		if err != nil {
			return err
		}
		if checked {
			continue
		}
		if err := c.checkSnapshotFile(ref); err != nil {
			return err
		}
	}
	return c.checkMuHashSnapshot(report, manifest)
}

func (c Checker) checkSnapshotFile(ref snapshots.SegmentRef) error {
	st, err := os.Stat(filepath.Join(c.SnapshotDir, ref.Path))
	if err != nil {
		return err
	}
	if st.IsDir() {
		return fmt.Errorf("pruning: snapshot segment %q is a directory", ref.Path)
	}
	if ref.Size != 0 && uint64(st.Size()) != ref.Size {
		return fmt.Errorf("pruning: snapshot segment %q size %d, want %d", ref.Path, st.Size(), ref.Size)
	}
	return nil
}

func (c Checker) checkMuHashSnapshot(report *CheckReport, manifest *snapshots.Manifest) error {
	if manifest.LatestStateTxNum == 0 {
		return nil
	}
	mgr, err := snapshots.OpenPinnedManager(c.SnapshotDir, manifest)
	if err != nil {
		return err
	}
	head, ok, err := mgr.GetCommitmentHead(manifest.LatestStateTxNum)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("pruning: snapshot lacks complete MuHash HEAD")
	}
	root, ok, err := mgr.GetCommitmentRoot(manifest.LatestStateTxNum)
	if err != nil {
		return err
	}
	if !ok || root != head.Root {
		return errors.New("pruning: snapshot HEAD/root cut mismatch")
	}
	report.MuHashSnapshotPresent = true
	return nil
}
