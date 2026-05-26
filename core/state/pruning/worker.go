package pruning

import (
	"errors"
	"fmt"
	"os"

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
}

type Stats struct {
	DeletedTxRanges              int
	DeletedDomainChangeBlocks    int
	DeletedCommitmentCheckpoints int
	DeletedStateCodeRows         int
}

func Run(db Store, policy Policy, headNum uint64) (Stats, error) {
	return Worker{DB: db, Policy: policy}.PruneTo(headNum)
}

func (w Worker) PruneTo(headNum uint64) (Stats, error) {
	if w.DB == nil {
		return Stats{}, errors.New("pruning: nil database")
	}
	if err := w.Policy.Validate(); err != nil {
		return Stats{}, err
	}
	if w.Policy.Mode == ModeArchive {
		return Stats{}, nil
	}
	var stats Stats
	coverage, err := w.snapshotStateDomainChangeCoverage()
	if err != nil {
		return Stats{}, err
	}
	historyCfg, err := w.hotHistoryDomainConfig()
	if err != nil {
		return Stats{}, err
	}
	hotStats, err := historyCfg.PruneHotHistory(w.DB, snapshots.HotHistoryPruneOptions{
		MaxBlocks: w.MaxBlocks,
		Decide: func(row *rawdb.StateTxRange) (snapshots.HotHistoryPruneDecision, error) {
			if w.Policy.RetainHistory(row.BlockNum, headNum) {
				return snapshots.HotHistoryPruneDecision{}, nil
			}
			switch w.Policy.Mode {
			case ModeFull:
				return snapshots.HotHistoryPruneDecision{DeleteTxRange: true, DeleteHistoryBlock: true}, nil
			case ModeSnap:
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
	deletedCodeRows, err := w.pruneStateCodeRows(headNum)
	if err != nil {
		return Stats{}, err
	}
	stats.DeletedStateCodeRows = deletedCodeRows

	checkpointCfg, err := latestDomainConfig(snapshots.SegmentDatasetCommitmentCheckpoint)
	if err != nil {
		return Stats{}, err
	}
	if checkpointCfg.IterateHotCommitmentCheckpoints == nil || checkpointCfg.DeleteHotCommitmentCheckpoint == nil {
		return Stats{}, errors.New("pruning: missing commitment checkpoint lifecycle hooks")
	}
	var commitmentBlocks []uint64
	if err := checkpointCfg.IterateHotCommitmentCheckpoints(w.DB, func(cp *rawdb.StateCommitmentCheckpoint) (bool, error) {
		if !w.Policy.RetainReorgData(cp.BlockNum, headNum) {
			commitmentBlocks = append(commitmentBlocks, cp.BlockNum)
		}
		return true, nil
	}); err != nil {
		return Stats{}, err
	}
	for _, blockNum := range commitmentBlocks {
		if err := checkpointCfg.DeleteHotCommitmentCheckpoint(w.DB, blockNum); err != nil {
			return Stats{}, err
		}
		stats.DeletedCommitmentCheckpoints++
	}
	if err := newRawDBStageProgressStore(w.DB).Write(rawdb.StageSnapshotPrune, headNum); err != nil {
		return Stats{}, fmt.Errorf("pruning: write snapshot/prune stage progress: %w", err)
	}
	return stats, nil
}

func (w Worker) hotHistoryDomainConfig() (snapshots.DomainCfg, error) {
	cfg, ok := snapshots.DefaultDomainRegistry().Dataset(snapshots.SegmentDatasetStateDomainChange)
	if !ok || cfg.DeleteHotHistoryBlock == nil {
		return snapshots.DomainCfg{}, errors.New("pruning: missing state-domain hot history deleter")
	}
	return cfg, nil
}

func (w Worker) pruneStateCodeRows(headNum uint64) (int, error) {
	if w.Policy.Mode != ModeSnap || w.SnapshotDir == "" {
		return 0, nil
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
	headTxNum, err := snapshots.StateDomainHistoryTxNumAtBlockEnd(w.DB, headNum)
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
	refs := make(codeHashRefs)
	if err := accountCfg.IterateHotAccountLatest(w.DB, nil, func(row rawdb.StateAccountLatestRow) (bool, error) {
		hash, err := decodeAccountEnvelopeCodeHash(row.Value, fmt.Sprintf("account latest %x", row.Owner))
		if err != nil {
			return false, err
		}
		refs.add(hash, headTxNum)
		return true, nil
	}); err != nil {
		return 0, err
	}
	if err := (Checker{DB: w.DB, SnapshotDir: w.SnapshotDir}).collectHistoryCodeHashes(refs); err != nil {
		return 0, err
	}

	var deleteHashes []common.Hash
	if err := codeCfg.IterateHotCode(w.DB, func(row rawdb.StateCodeRow) (bool, error) {
		if !isMeaningfulCodeHash(row.Hash) {
			return true, nil
		}
		txNums := refs[row.Hash]
		if len(txNums) == 0 {
			if codeHashAvailableInSnapshot(mgr, row.Hash, headTxNum) {
				deleteHashes = append(deleteHashes, row.Hash)
			}
			return true, nil
		}
		for txNum := range txNums {
			if !codeHashAvailableInSnapshot(mgr, row.Hash, txNum) {
				return true, nil
			}
		}
		deleteHashes = append(deleteHashes, row.Hash)
		return true, nil
	}); err != nil {
		return 0, err
	}
	for _, hash := range deleteHashes {
		if err := codeCfg.DeleteHotCode(w.DB, hash); err != nil {
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
	if w.Policy.Mode != ModeSnap || w.SnapshotDir == "" {
		return nil, nil
	}
	manifest, err := snapshots.LoadProductionManifest(w.SnapshotDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	ranges := snapshots.HistoryTxRanges(manifest, snapshots.SegmentDatasetStateDomainChange)
	coverage := make(snapshotTxCoverage, 0, len(ranges))
	for _, r := range ranges {
		coverage = append(coverage, snapshotTxRange{from: r.From, to: r.To})
	}
	return coverage, nil
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
