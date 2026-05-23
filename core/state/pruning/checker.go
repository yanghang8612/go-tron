package pruning

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

type CheckReport struct {
	LatestRows            int
	RetainedTxRanges      int
	RetainedDomainChanges int
	CommitmentCheckpoints int
	SnapshotSegments      int
	Warnings              []string
}

type Checker struct {
	DB          Store
	Policy      Policy
	SnapshotDir string
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
	if err := rawdb.IterateStateKVLatestRows(c.DB, func(row rawdb.StateKVLatestRow) (bool, error) {
		report.LatestRows++
		return true, nil
	}); err != nil {
		return CheckReport{}, err
	}
	if err := rawdb.IterateStateTxRanges(c.DB, func(row *rawdb.StateTxRange) (bool, error) {
		if row.EndTxNum < row.BeginTxNum {
			return false, fmt.Errorf("pruning: state tx range for block %d is inverted", row.BlockNum)
		}
		if !c.Policy.RetainHistory(row.BlockNum, headNum) {
			report.Warnings = append(report.Warnings, fmt.Sprintf("state tx range for prunable block %d is still present", row.BlockNum))
			return true, nil
		}
		report.RetainedTxRanges++
		if err := rawdb.IterateStateDomainChanges(c.DB, row.BlockNum, func(change *rawdb.StateDomainChange) (bool, error) {
			report.RetainedDomainChanges++
			return true, nil
		}); err != nil {
			return false, err
		}
		return true, nil
	}); err != nil {
		return CheckReport{}, err
	}
	if err := rawdb.IterateStateCommitmentCheckpoints(c.DB, func(cp *rawdb.StateCommitmentCheckpoint) (bool, error) {
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

func CheckCodeHashes(db ethdb.KeyValueReader, hashes []common.Hash) error {
	if db == nil {
		return errors.New("pruning: nil database")
	}
	for _, hash := range hashes {
		if hash == (common.Hash{}) || hash == common.Keccak256(nil) {
			continue
		}
		if len(rawdb.ReadStateCode(db, hash)) == 0 {
			return fmt.Errorf("pruning: missing state code for hash %x", hash)
		}
	}
	return nil
}

func (c Checker) checkSnapshots(report *CheckReport) error {
	manifest, err := snapshots.LoadManifest(c.SnapshotDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, ref := range manifest.Segments {
		report.SnapshotSegments++
		switch ref.Kind {
		case snapshots.SegmentLatest:
			if _, err := snapshots.OpenLatestSegment(c.SnapshotDir, ref); err != nil {
				return err
			}
		default:
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
		}
	}
	return nil
}
