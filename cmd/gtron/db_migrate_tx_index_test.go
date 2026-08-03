package main

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	chainfreezer "github.com/tronprotocol/go-tron/core/freezer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"github.com/urfave/cli/v2"
	"google.golang.org/protobuf/proto"
)

func TestDBMigrateTxIndexCommandPublishesBeforeDeletingAndResumes(t *testing.T) {
	datadir := t.TempDir()
	ancientPath := ancientDataDir(datadir)
	tables := chainfreezer.FreezerTableSet()
	ancient, err := rawdbfreezer.NewFreezer(ancientPath, "", false, 2049, tables)
	if err != nil {
		t.Fatal(err)
	}
	const blocks = 6
	hashes := make([][32]byte, blocks)
	if _, err := ancient.ModifyAncients(func(op rawdbfreezer.AncientWriteOp) error {
		for number := uint64(0); number < blocks; number++ {
			pb := &corepb.Block{
				BlockHeader:  &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: int64(number), Timestamp: int64(number + 1)}},
				Transactions: []*corepb.Transaction{{RawData: &corepb.TransactionRaw{Timestamp: int64(number + 1), Data: []byte{byte(number + 1)}}}},
			}
			block := types.NewBlockFromPB(pb)
			body, err := block.Marshal()
			if err != nil {
				return err
			}
			hashes[number] = block.Transactions()[0].Hash()
			ret, err := proto.Marshal(&corepb.TransactionRet{
				BlockNumber: int64(number),
				Transactioninfo: []*corepb.TransactionInfo{{
					Id:          append([]byte(nil), hashes[number][:]...),
					Fee:         int64(100 + number),
					BlockNumber: int64(number),
				}},
			})
			if err != nil {
				return err
			}
			if err := op.AppendRaw("bodies", number, body); err != nil {
				return err
			}
			if err := op.AppendRaw("tx_infos", number, ret); err != nil {
				return err
			}
			if err := op.AppendRaw("state_roots", number, make([]byte, 32)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := ancient.Sync(); err != nil {
		t.Fatal(err)
	}
	result, err := ancient.MigrateV2(rawdbfreezer.V2MigrationOptions{
		Tables:        []string{"bodies", "tx_infos", "state_roots"},
		SegmentBlocks: 4,
		FrameBlocks:   2,
		MaxSegments:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.End != 4 {
		t.Fatalf("V2 coverage = %d, want 4", result.End)
	}
	if err := ancient.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := rawdb.NewPebbleDB(chainDataDir(datadir), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	for number, hash := range hashes {
		if err := rawdb.WriteTransactionLocation(db, hash[:], uint64(number), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	output := runMigrateTxIndexTestCommand(t, datadir)
	if output.StartBlock != 0 || output.EndBlock != 4 || output.IndexedTransactions != 4 || output.DeletedHotRows != 4 || output.RetainedHotRows != 2 || output.ScannedHotRows != blocks || output.RunBytes == 0 {
		t.Fatalf("migration output = %+v", output)
	}
	db, err = rawdb.NewPebbleDBReadOnly(chainDataDir(datadir), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	hotOnly := rawdb.NewChainDB(db, rawdb.NoopAncient{})
	if progress, ok, err := rawdb.ReadStageProgress(db, rawdb.StageFreezerTxIndexPrune); err != nil || !ok || progress != 4 {
		t.Fatalf("transaction-index prune progress=%d ok=%v err=%v", progress, ok, err)
	}
	for number, hash := range hashes {
		got := rawdb.ReadTransactionIndex(hotOnly, hash[:])
		if number < 4 && got != nil {
			t.Fatalf("covered hot index %d remains: %v", number, got)
		}
		if number >= 4 && (got == nil || *got != uint64(number)) {
			t.Fatalf("uncovered hot index %d = %v", number, got)
		}
	}
	ancient, err = rawdbfreezer.NewFreezer(ancientPath, "", true, 2049, tables)
	if err != nil {
		t.Fatal(err)
	}
	chainDB := rawdb.NewChainDB(db, rawdb.NewFreezerReader(ancient))
	for number, hash := range hashes {
		if got := rawdb.ReadTransactionIndex(chainDB, hash[:]); got == nil || *got != uint64(number) {
			t.Fatalf("combined transaction index %d = %v", number, got)
		}
		if got := rawdb.ReadTransactionInfo(chainDB, hash[:]); got == nil || got.Fee != int64(100+number) {
			t.Fatalf("combined transaction info %d = %+v", number, got)
		}
	}
	if err := ancient.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	resumed := runMigrateTxIndexTestCommand(t, datadir)
	if resumed.StartBlock != 4 || resumed.EndBlock != 4 || resumed.IndexedTransactions != 0 || resumed.DeletedHotRows != 0 || resumed.RetainedHotRows != 2 || resumed.ScannedHotRows != 2 {
		t.Fatalf("resumed output = %+v", resumed)
	}
}

type txIndexBatchProbeDB struct {
	ethdb.KeyValueStore
	rangeDeletes int
	pointDeletes int
	puts         int
}

func (db *txIndexBatchProbeDB) NewBatchWithSize(size int) ethdb.Batch {
	return ethdb.HookedBatch{
		Batch: db.KeyValueStore.NewBatchWithSize(size),
		OnPut: func([]byte, []byte) {
			db.puts++
		},
		OnDelete: func([]byte) {
			db.pointDeletes++
		},
		OnDeleteRange: func([]byte, []byte) {
			db.rangeDeletes++
		},
	}
}

func TestReplaceCoveredTransactionIndexesUsesAtomicRangeRewrite(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	db := &txIndexBatchProbeDB{KeyValueStore: base}
	var hashes [6][32]byte
	for number := range hashes {
		hashes[number][0] = byte(number + 1)
		if err := rawdb.WriteTransactionLocation(db, hashes[number][:], uint64(number), 0); err != nil {
			t.Fatal(err)
		}
	}
	db.puts = 0 // Count only writes made by the replacement batch.
	scanned, deleted, retained, err := replaceCoveredTransactionIndexes(db, 4, 0, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if scanned != 6 || deleted != 4 || retained != 2 {
		t.Fatalf("rewrite stats scanned=%d deleted=%d retained=%d", scanned, deleted, retained)
	}
	if db.rangeDeletes != 1 || db.pointDeletes != 0 || db.puts != 2 {
		t.Fatalf("batch ops rangeDeletes=%d pointDeletes=%d puts=%d", db.rangeDeletes, db.pointDeletes, db.puts)
	}
	chainDB := rawdb.NewChainDB(base, rawdb.NoopAncient{})
	for number, hash := range hashes {
		got := rawdb.ReadTransactionIndex(chainDB, hash[:])
		if number < 4 && got != nil {
			t.Fatalf("covered index %d remains: %v", number, got)
		}
		if number >= 4 && (got == nil || *got != uint64(number)) {
			t.Fatalf("retained index %d=%v", number, got)
		}
	}
}

func runMigrateTxIndexTestCommand(t *testing.T, datadir string) txIndexMigrationOutput {
	t.Helper()
	var stdout, stderr bytes.Buffer
	app := &cli.App{Writer: &stdout, ErrWriter: &stderr, Commands: []*cli.Command{dbCommand()}}
	if err := app.Run([]string{
		"gtron", "db", "migrate-tx-index",
		"--datadir", datadir,
		"--db.cache", "16",
		"--db.handles", "16",
		"--db.memtable", "4",
		"--prefix-bits", "8",
		"--progress", "0s",
		"--yes",
		"--json",
	}); err != nil {
		t.Fatalf("migrate tx index: %v\nstderr: %s", err, stderr.String())
	}
	var output txIndexMigrationOutput
	if err := json.Unmarshal(stdout.Bytes(), &output); err != nil {
		t.Fatalf("decode output: %v\n%s", err, stdout.String())
	}
	return output
}
