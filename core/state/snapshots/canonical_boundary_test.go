package snapshots

import (
	"crypto/ed25519"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestInstallCanonicalBoundaryFromVerifiedSnapshotUsesAncientBlock(t *testing.T) {
	root := t.TempDir()
	fz := openChainFreezerTestStore(t, root)
	defer fz.Close()
	block := canonicalBoundaryTestBlock(t, 3)
	appendCanonicalBoundaryBlock(t, fz, block)

	db := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteStateTxRange(db, block.Number(), block.Hash(), 10, 12); err != nil {
		t.Fatalf("WriteStateTxRange: %v", err)
	}
	manifest := NewManifest(10, 12, nil)
	result, err := InstallCanonicalBoundaryFromVerifiedSnapshot(db, rawdb.NewChainDB(db, rawdb.NewFreezerReader(fz)), manifest)
	if err != nil {
		t.Fatalf("InstallCanonicalBoundaryFromVerifiedSnapshot: %v", err)
	}
	if result.TxNum != 12 || result.BlockNum != block.Number() || result.BlockHash != block.Hash() {
		t.Fatalf("result = %+v, want txNum=12 block=%d hash=%x", result, block.Number(), block.Hash())
	}
	if got := rawdb.ReadHeadBlockHash(db); got != block.Hash() {
		t.Fatalf("LastBlock = %x, want %x", got, block.Hash())
	}
	chainDB := rawdb.NewChainDB(db, rawdb.NewFreezerReader(fz))
	num := rawdb.ReadBlockNumber(chainDB, block.Hash())
	if num == nil || *num != block.Number() {
		t.Fatalf("ReadBlockNumber = %v, want %d", num, block.Number())
	}
	if got := rawdb.ReadBlockHashByNumber(chainDB, block.Number()); got != block.Hash() {
		t.Fatalf("ReadBlockHashByNumber = %x, want %x", got, block.Hash())
	}
	for _, stage := range rawdb.CanonicalExecutionStages() {
		row, ok, err := rawdb.ReadStageProgressRow(db, stage)
		if err != nil || !ok || row.BlockNum != block.Number() || !row.HasBlockHash || row.BlockHash != block.Hash() {
			t.Fatalf("%s progress = %+v ok=%v err=%v, want block %d hash %x", stage, row, ok, err, block.Number(), block.Hash())
		}
	}
}

func TestInstallCanonicalBoundaryFromVerifiedCatalogRequiresTrustedCatalog(t *testing.T) {
	root := t.TempDir()
	fz := openChainFreezerTestStore(t, filepath.Join(root, "freezer"))
	defer fz.Close()
	block := canonicalBoundaryTestBlock(t, 3)
	appendCanonicalBoundaryBlock(t, fz, block)

	snapshotDir := filepath.Join(root, "snapshot")
	ref, err := BuildChainFreezerSegmentFromAncient(fz, snapshotDir, "", 0, block.Number())
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	identity := chainFreezerTestIdentity()
	if err := PublishManifest(snapshotDir, NewManifestForChain(10, 12, []SegmentRef{ref}, identity)); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	db := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteStateTxRange(db, block.Number(), block.Hash(), 10, 12); err != nil {
		t.Fatalf("WriteStateTxRange: %v", err)
	}
	chainDB := rawdb.NewChainDB(db, rawdb.NewFreezerReader(fz))

	if _, err := InstallCanonicalBoundaryFromVerifiedCatalog(db, chainDB, snapshotDir, identity, nil); err == nil {
		t.Fatal("unsigned catalog advanced canonical boundary")
	}
	if got := rawdb.ReadHeadBlockHash(db); got != (common.Hash{}) {
		t.Fatalf("LastBlock advanced without signed catalog: %x", got)
	}
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	if _, err := PublishSignedSnapshotCatalog(snapshotDir, priv); err != nil {
		t.Fatalf("PublishSignedSnapshotCatalog: %v", err)
	}
	result, err := InstallCanonicalBoundaryFromVerifiedCatalog(db, chainDB, snapshotDir, identity, []ed25519.PublicKey{pub})
	if err != nil {
		t.Fatalf("InstallCanonicalBoundaryFromVerifiedCatalog: %v", err)
	}
	if result.TxNum != 12 || result.BlockNum != block.Number() || result.BlockHash != block.Hash() {
		t.Fatalf("result = %+v, want txNum=12 block=%d hash=%x", result, block.Number(), block.Hash())
	}
	if got := rawdb.ReadHeadBlockHash(db); got != block.Hash() {
		t.Fatalf("LastBlock = %x, want %x", got, block.Hash())
	}
}

func TestInstallCanonicalBoundaryRejectsHashMismatch(t *testing.T) {
	root := t.TempDir()
	fz := openChainFreezerTestStore(t, root)
	defer fz.Close()
	block := canonicalBoundaryTestBlock(t, 4)
	appendCanonicalBoundaryBlock(t, fz, block)

	db := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteStateTxRange(db, block.Number(), common.Hash{0xee}, 20, 20); err != nil {
		t.Fatalf("WriteStateTxRange: %v", err)
	}
	err := func() error {
		_, err := InstallCanonicalBoundaryFromVerifiedSnapshot(db, rawdb.NewChainDB(db, rawdb.NewFreezerReader(fz)), NewManifest(20, 20, nil))
		return err
	}()
	if err == nil || !strings.Contains(err.Error(), "hash") {
		t.Fatalf("error = %v, want hash mismatch rejection", err)
	}
	if got := rawdb.ReadHeadBlockHash(db); got != (common.Hash{}) {
		t.Fatalf("LastBlock advanced on mismatch: %x", got)
	}
}

func canonicalBoundaryTestBlock(t *testing.T, number uint64) *types.Block {
	t.Helper()
	return types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    int64(number),
				Timestamp: int64(1_000 + number),
			},
		},
	})
}

func appendCanonicalBoundaryBlock(t *testing.T, fz rawdb.AncientWriter, block *types.Block) {
	t.Helper()
	if _, err := fz.ModifyAncients(func(op rawdb.AncientWriteOp) error {
		for n := uint64(0); n <= block.Number(); n++ {
			rowBlock := block
			if n != block.Number() {
				rowBlock = canonicalBoundaryTestBlock(t, n)
			}
			data, err := rowBlock.Marshal()
			if err != nil {
				return err
			}
			if err := op.AppendRaw(rawdb.AncientBlocksTable, n, data); err != nil {
				return err
			}
			if err := op.AppendRaw(rawdb.AncientTxInfosTable, n, nil); err != nil {
				return err
			}
			if err := op.AppendRaw(rawdb.AncientStateRootsTable, n, nil); err != nil {
				return err
			}
			if n == block.Number() {
				break
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("ModifyAncients: %v", err)
	}
	if err := fz.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
}
