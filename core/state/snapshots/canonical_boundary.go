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
	_, manifest, _, err := VerifySignedSnapshotCatalogManifest(dir, expected, trustedKeys)
	if err != nil {
		return nil, err
	}
	return InstallCanonicalBoundaryFromVerifiedSnapshot(db, chainDB, manifest)
}

func InstallCanonicalBoundaryFromVerifiedSnapshot(db canonicalBoundaryDB, chainDB *rawdb.ChainDB, manifest *Manifest) (*RestoreCanonicalBoundaryResult, error) {
	boundary, err := VerifyCanonicalBoundaryFromRestoredSnapshot(db, chainDB, manifest)
	if err != nil {
		return nil, err
	}
	batcher, ok := db.(ethdb.Batcher)
	if !ok {
		return nil, errors.New("snapshots: canonical publication requires batch-capable target")
	}
	batch := batcher.NewBatch()
	defer batch.Close()
	if err := rawdb.WriteBlockNumber(batch, boundary.BlockHash, boundary.BlockNum); err != nil {
		return nil, err
	}
	if err := rawdb.WriteCanonicalStageProgressWithHash(batch, boundary.BlockNum, boundary.BlockHash); err != nil {
		return nil, err
	}
	capture := new(keyCapture)
	rawdb.WriteHeadBlockHash(capture, boundary.BlockHash)
	if err := batch.Put(capture.key, boundary.BlockHash.Bytes()); err != nil {
		return nil, err
	}
	if err := batch.Write(); err != nil {
		return nil, err
	}
	return boundary, nil
}

// VerifyCanonicalBoundaryFromRestoredSnapshot never publishes current state.
// It is used before fresh restore's final HEAD/root/current publication batch.
func VerifyCanonicalBoundaryFromRestoredSnapshot(db canonicalBoundaryDB, chainDB *rawdb.ChainDB, manifest *Manifest) (*RestoreCanonicalBoundaryResult, error) {
	if db == nil {
		return nil, errors.New("snapshots: nil canonical boundary database")
	}
	if chainDB == nil {
		return nil, errors.New("snapshots: nil canonical boundary chain database")
	}
	if manifest == nil {
		return nil, errors.New("snapshots: nil manifest")
	}
	txNum := manifest.LatestStateTxNum
	if txNum == 0 {
		return nil, errors.New("snapshots: no authenticated latest state cut")
	}
	head, headPresent, err := rawdb.ReadStateCommitmentHead(db)
	if err != nil {
		return nil, err
	}
	if !headPresent || head.TxNum != txNum {
		return nil, errors.New("snapshots: missing same-cut HEAD")
	}
	row, ok, err := rawdb.ReadStateTxRange(db, head.BlockNum)
	if err != nil {
		return nil, err
	}
	if !ok || row.EndTxNum != txNum || row.BlockHash != head.BlockHash {
		return nil, fmt.Errorf("snapshots: missing or mismatched current allocator range identity/hash for snapshot txNum %d", txNum)
	}
	if row.BlockHash == (common.Hash{}) {
		return nil, fmt.Errorf("snapshots: state tx range for block %d has zero block hash", row.BlockNum)
	}
	block, ok, err := rawdb.ReadBlockStrict(chainDB, row.BlockNum)
	if err != nil {
		return nil, fmt.Errorf("snapshots: canonical boundary block %d unreadable: %w", row.BlockNum, err)
	}
	if !ok {
		return nil, fmt.Errorf("snapshots: canonical boundary block %d not found in chain data", row.BlockNum)
	}
	if block.Hash() != row.BlockHash {
		return nil, fmt.Errorf("snapshots: canonical boundary block %d hash %x, want state tx range hash %x", row.BlockNum, block.Hash(), row.BlockHash)
	}
	if err := verifyCanonicalBoundaryCommitmentCheckpoint(db, row.BlockNum, row.BlockHash, row.EndTxNum); err != nil {
		return nil, err
	}
	properties, err := rawdb.ReadStateExecutionProperties(db)
	if err != nil {
		return nil, err
	}
	if properties.BlockNum != int64(block.Number()) || properties.BlockHash != block.Hash() || properties.Timestamp != block.Timestamp() {
		return nil, errors.New("snapshots: execution properties differ from cut block")
	}
	head, _, err = rawdb.ReadStateCommitmentHead(db)
	if err != nil {
		return nil, err
	}
	if head.ParentHash != block.ParentHash() {
		return nil, errors.New("snapshots: canonical boundary parent hash / HEAD mismatch")
	}
	chainRoot, present, err := rawdb.ReadBlockStateRootStrict(chainDB, row.BlockHash)
	if err != nil {
		return nil, err
	}
	if !present || chainRoot != head.Root {
		return nil, errors.New("snapshots: canonical boundary chain internal root / HEAD mismatch")
	}
	return &RestoreCanonicalBoundaryResult{
		TxNum:     txNum,
		BlockNum:  row.BlockNum,
		BlockHash: row.BlockHash,
	}, nil
}

func verifyCanonicalBoundaryCommitmentCheckpoint(db ethdb.KeyValueReader, blockNum uint64, blockHash common.Hash, txNum uint64) error {
	head, present, err := rawdb.ReadStateCommitmentHead(db)
	if err != nil {
		return err
	}
	if !present {
		return errors.New("snapshots: canonical boundary requires complete MuHash HEAD")
	}
	if head.BlockNum != blockNum || head.BlockHash != blockHash || head.TxNum != txNum {
		return errors.New("snapshots: canonical boundary HEAD identity/source cut mismatch")
	}

	checkpoint, ok, err := rawdb.ReadLatestStateCommitmentCheckpoint(db)
	if err != nil {
		return fmt.Errorf("snapshots: canonical boundary commitment checkpoint unreadable: %w", err)
	}
	if !ok {
		return errors.New("snapshots: canonical boundary missing commitment checkpoint")
	}
	if checkpoint.BlockNum != blockNum {
		return fmt.Errorf("snapshots: canonical boundary commitment checkpoint block %d, want %d", checkpoint.BlockNum, blockNum)
	}
	if checkpoint.BlockHash != blockHash {
		return fmt.Errorf("snapshots: canonical boundary commitment checkpoint hash %x, want boundary hash %x", checkpoint.BlockHash, blockHash)
	}
	restoredRoot, ok, err := rawdb.ReadLatestDomainCommitmentRoot(db)
	if err != nil {
		return fmt.Errorf("snapshots: canonical boundary restored commitment root unreadable: %w", err)
	}
	if !ok || checkpoint.Root != restoredRoot || head.Root != restoredRoot {
		return fmt.Errorf("snapshots: canonical boundary commitment checkpoint root %x, want latest root %x", checkpoint.Root, restoredRoot)
	}
	return nil
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
