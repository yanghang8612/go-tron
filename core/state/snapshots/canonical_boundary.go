package snapshots

import (
	"crypto/ed25519"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type RestoreCanonicalBoundaryResult struct {
	TxNum     uint64
	BlockNum  uint64
	BlockHash common.Hash
}

type canonicalBoundaryDB interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
	ethdb.Iteratee
}

// InstallCanonicalBoundaryFromVerifiedCatalog verifies the signed snapshot
// catalog and chain identity before advancing canonical chain stages to the
// snapshot boundary. Callers that already hold a verified manifest can use
// InstallCanonicalBoundaryFromVerifiedSnapshot directly.
func InstallCanonicalBoundaryFromVerifiedCatalog(db canonicalBoundaryDB, chainDB *rawdb.ChainDB, dir string, expected ChainIdentity, trustedKeys []ed25519.PublicKey) (*RestoreCanonicalBoundaryResult, error) {
	if _, _, err := VerifySignedSnapshotCatalog(dir, expected, trustedKeys); err != nil {
		return nil, err
	}
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		return nil, err
	}
	return InstallCanonicalBoundaryFromVerifiedSnapshot(db, chainDB, manifest)
}

func InstallCanonicalBoundaryFromVerifiedSnapshot(db canonicalBoundaryDB, chainDB *rawdb.ChainDB, manifest *Manifest) (*RestoreCanonicalBoundaryResult, error) {
	if db == nil {
		return nil, errors.New("snapshots: nil canonical boundary database")
	}
	if chainDB == nil {
		return nil, errors.New("snapshots: nil canonical boundary chain database")
	}
	if manifest == nil {
		return nil, errors.New("snapshots: nil manifest")
	}
	txNum := manifest.VisibleTxEnd
	row, ok, err := stateTxRangeAtTxNum(db, txNum)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("snapshots: no restored state tx range contains snapshot txNum %d", txNum)
	}
	if row.BlockHash == (common.Hash{}) {
		return nil, fmt.Errorf("snapshots: state tx range for block %d has zero block hash", row.BlockNum)
	}
	block := rawdb.ReadBlock(chainDB, row.BlockNum)
	if block == nil {
		return nil, fmt.Errorf("snapshots: canonical boundary block %d not found in chain data", row.BlockNum)
	}
	if block.Hash() != row.BlockHash {
		return nil, fmt.Errorf("snapshots: canonical boundary block %d hash %x, want state tx range hash %x", row.BlockNum, block.Hash(), row.BlockHash)
	}
	if err := rawdb.WriteBlockNumber(db, row.BlockHash, row.BlockNum); err != nil {
		return nil, err
	}
	if err := rawdb.WriteCanonicalStageProgressWithHash(db, row.BlockNum, row.BlockHash); err != nil {
		return nil, err
	}
	rawdb.WriteHeadBlockHash(db, row.BlockHash)
	return &RestoreCanonicalBoundaryResult{
		TxNum:     txNum,
		BlockNum:  row.BlockNum,
		BlockHash: row.BlockHash,
	}, nil
}

func stateTxRangeAtTxNum(db ethdb.Iteratee, txNum uint64) (*rawdb.StateTxRange, bool, error) {
	var out *rawdb.StateTxRange
	if err := rawdb.IterateStateTxRanges(db, func(row *rawdb.StateTxRange) (bool, error) {
		if row == nil || row.EndTxNum < txNum || row.BeginTxNum > txNum {
			return true, nil
		}
		if out != nil {
			return false, fmt.Errorf("snapshots: multiple restored state tx ranges contain txNum %d", txNum)
		}
		out = row
		return true, nil
	}); err != nil {
		return nil, false, err
	}
	if out == nil {
		return nil, false, nil
	}
	return cloneStateTxRangeForSegment(out), true, nil
}
