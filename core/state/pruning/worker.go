package pruning

import (
	"errors"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type Store interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
	ethdb.Iteratee
}

type Worker struct {
	DB        Store
	Policy    Policy
	MaxBlocks int
}

type Stats struct {
	DeletedTxRanges              int
	DeletedDomainChangeBlocks    int
	DeletedCommitmentCheckpoints int
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
	var txRangeBlocks []uint64
	if err := rawdb.IterateStateTxRanges(w.DB, func(row *rawdb.StateTxRange) (bool, error) {
		if !w.Policy.RetainHistory(row.BlockNum, headNum) {
			txRangeBlocks = append(txRangeBlocks, row.BlockNum)
			if w.MaxBlocks > 0 && len(txRangeBlocks) >= w.MaxBlocks {
				return false, nil
			}
		}
		return true, nil
	}); err != nil {
		return Stats{}, err
	}
	for _, blockNum := range txRangeBlocks {
		if err := rawdb.DeleteStateDomainChanges(w.DB, blockNum); err != nil {
			return Stats{}, err
		}
		stats.DeletedDomainChangeBlocks++
		if err := rawdb.DeleteStateTxRange(w.DB, blockNum); err != nil {
			return Stats{}, err
		}
		stats.DeletedTxRanges++
	}

	var commitmentBlocks []uint64
	if err := rawdb.IterateStateCommitmentCheckpoints(w.DB, func(cp *rawdb.StateCommitmentCheckpoint) (bool, error) {
		if !w.Policy.RetainReorgData(cp.BlockNum, headNum) {
			commitmentBlocks = append(commitmentBlocks, cp.BlockNum)
		}
		return true, nil
	}); err != nil {
		return Stats{}, err
	}
	for _, blockNum := range commitmentBlocks {
		if err := rawdb.DeleteStateCommitmentCheckpoint(w.DB, blockNum); err != nil {
			return Stats{}, err
		}
		stats.DeletedCommitmentCheckpoints++
	}
	return stats, nil
}
