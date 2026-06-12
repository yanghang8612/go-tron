package main

import (
	"crypto/ed25519"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	corepkg "github.com/tronprotocol/go-tron/core"
	chainfreezer "github.com/tronprotocol/go-tron/core/freezer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	corestate "github.com/tronprotocol/go-tron/core/state"
	statesnapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	coretypes "github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"github.com/urfave/cli/v2"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestDBRebuildTxIndexesCmdRebuildsHotIndexes(t *testing.T) {
	dataDir := t.TempDir()
	db, txInfos := seedDBRebuildTxIndexDatadir(t, dataDir, false)
	db.Close()

	ctx := makeDBTestContext(t, []string{
		"--datadir", dataDir,
		"--db.from-block", "1",
		"--db.to-block", "2",
		"--db.etl.tempdir", filepath.Join(t.TempDir(), "etl"),
		"--db.etl.buffer", "1",
	})
	if err := dbRebuildTxIndexesCmd(ctx); err != nil {
		t.Fatalf("dbRebuildTxIndexesCmd: %v", err)
	}

	reopened, err := rawdb.NewPebbleDB(chainDataDir(dataDir), 256, 500)
	if err != nil {
		t.Fatalf("reopen pebble: %v", err)
	}
	defer reopened.Close()
	chainDB := rawdb.NewChainDB(reopened, rawdb.NoopAncient{})
	txID := txInfos[1].Id
	if got := rawdb.ReadTransactionIndex(chainDB, txID); got == nil || *got != 1 {
		t.Fatalf("ReadTransactionIndex = %v, want 1", got)
	}
	if got := rawdb.ReadTransactionInfo(chainDB, txID); got == nil || got.Fee != txInfos[1].Fee {
		t.Fatalf("ReadTransactionInfo = %+v, want fee %d", got, txInfos[1].Fee)
	}
}

func TestDBRebuildTxIndexesCmdDefaultsToHead(t *testing.T) {
	dataDir := t.TempDir()
	db, txInfos := seedDBRebuildTxIndexDatadir(t, dataDir, true)
	db.Close()

	ctx := makeDBTestContext(t, []string{
		"--datadir", dataDir,
		"--db.from-block", "1",
	})
	if err := dbRebuildTxIndexesCmd(ctx); err != nil {
		t.Fatalf("dbRebuildTxIndexesCmd default head: %v", err)
	}

	reopened, err := rawdb.NewPebbleDB(chainDataDir(dataDir), 256, 500)
	if err != nil {
		t.Fatalf("reopen pebble: %v", err)
	}
	defer reopened.Close()
	chainDB := rawdb.NewChainDB(reopened, rawdb.NoopAncient{})
	txID := txInfos[2].Id
	if got := rawdb.ReadTransactionIndex(chainDB, txID); got == nil || *got != 2 {
		t.Fatalf("ReadTransactionIndex default head = %v, want 2", got)
	}
	if got := rawdb.ReadTransactionInfo(chainDB, txID); got == nil || got.Fee != txInfos[2].Fee {
		t.Fatalf("ReadTransactionInfo default head = %+v, want fee %d", got, txInfos[2].Fee)
	}
}

func TestDBRebuildSectionBloomsCmd(t *testing.T) {
	dataDir := t.TempDir()
	db, txInfos := seedDBRebuildTxIndexDatadir(t, dataDir, false)
	txInfos[0].Log = []*corepb.TransactionInfo_Log{{
		Address: []byte{0x11, 0x22, 0x33, 0x44},
		Topics: [][]byte{
			{0xaa, 0xbb, 0xcc},
			{0x01, 0x02, 0x03, 0x04},
		},
	}}
	if err := rawdb.WriteTransactionInfosByBlock(db, 1, txInfos[:2]); err != nil {
		t.Fatalf("rewrite block1 tx infos with logs: %v", err)
	}
	db.Close()

	ctx := makeDBTestContext(t, []string{
		"--datadir", dataDir,
		"--db.from-block", "1",
		"--db.to-block", "2",
		"--db.etl.tempdir", filepath.Join(t.TempDir(), "etl"),
		"--db.etl.buffer", "1",
	})
	if err := dbRebuildSectionBloomsCmd(ctx); err != nil {
		t.Fatalf("dbRebuildSectionBloomsCmd: %v", err)
	}

	reopened, err := rawdb.NewPebbleDB(chainDataDir(dataDir), 256, 500)
	if err != nil {
		t.Fatalf("reopen pebble: %v", err)
	}
	defer reopened.Close()
	rows := 0
	for bitIndex := uint64(0); bitIndex < rawdb.SectionBloomBitSize; bitIndex++ {
		bitset, ok, err := rawdb.ReadSectionBloomBitSet(reopened, 0, bitIndex)
		if err != nil {
			t.Fatalf("ReadSectionBloomBitSet %d: %v", bitIndex, err)
		}
		if !ok {
			continue
		}
		rows++
		if !dbTestSectionBloomBitSetHas(bitset, 1) {
			t.Fatalf("section bloom row %d does not include block offset 1: %x", bitIndex, bitset)
		}
	}
	if rows == 0 {
		t.Fatal("section bloom rebuild wrote no rows")
	}
}

func TestDBRebuildAccountTracesCmd(t *testing.T) {
	dataDir := t.TempDir()
	db, txInfos := seedDBRebuildTxIndexDatadir(t, dataDir, false)
	chainDB := rawdb.NewChainDB(db, rawdb.NoopAncient{})
	block1 := rawdb.ReadBlock(chainDB, 1)
	block2 := rawdb.ReadBlock(chainDB, 2)
	if block1 == nil || block2 == nil {
		t.Fatal("seeded blocks missing")
	}
	a := dbRebuildTraceAddress(0xa0)
	b := dbRebuildTraceAddress(0xb0)
	if err := rawdb.WriteBlockBalanceTrace(db, 1, &contractpb.BlockBalanceTrace{
		BlockIdentifier: dbRebuildBlockBalanceID(block1),
		TransactionBalanceTrace: []*contractpb.TransactionBalanceTrace{
			{
				TransactionIdentifier: append([]byte(nil), txInfos[0].Id...),
				Operation: []*contractpb.TransactionBalanceTrace_Operation{
					dbRebuildBalanceOp(0, a, 100),
					dbRebuildBalanceOp(1, b, 50),
				},
				Type:   "TransferContract",
				Status: "SUCCESS",
			},
			{
				TransactionIdentifier: append([]byte(nil), txInfos[1].Id...),
				Operation: []*contractpb.TransactionBalanceTrace_Operation{
					dbRebuildBalanceOp(0, a, -10),
				},
				Type:   "TransferContract",
				Status: "SUCCESS",
			},
		},
	}); err != nil {
		t.Fatalf("WriteBlockBalanceTrace block1: %v", err)
	}
	if err := rawdb.WriteBlockBalanceTrace(db, 2, &contractpb.BlockBalanceTrace{
		BlockIdentifier: dbRebuildBlockBalanceID(block2),
		TransactionBalanceTrace: []*contractpb.TransactionBalanceTrace{
			{
				TransactionIdentifier: append([]byte(nil), txInfos[2].Id...),
				Operation: []*contractpb.TransactionBalanceTrace_Operation{
					dbRebuildBalanceOp(0, a, 3),
				},
				Type:   "TransferContract",
				Status: "SUCCESS",
			},
		},
	}); err != nil {
		t.Fatalf("WriteBlockBalanceTrace block2: %v", err)
	}
	db.Close()

	ctx := makeDBTestContext(t, []string{
		"--datadir", dataDir,
		"--db.from-block", "1",
		"--db.to-block", "2",
		"--db.etl.tempdir", filepath.Join(t.TempDir(), "etl"),
		"--db.etl.buffer", "1",
	})
	if err := dbRebuildAccountTracesCmd(ctx); err != nil {
		t.Fatalf("dbRebuildAccountTracesCmd: %v", err)
	}

	reopened, err := rawdb.NewPebbleDB(chainDataDir(dataDir), 256, 500)
	if err != nil {
		t.Fatalf("reopen pebble: %v", err)
	}
	defer reopened.Close()
	for _, tc := range []struct {
		addr  []byte
		block int64
		want  int64
	}{
		{a, 1, 90},
		{b, 1, 50},
		{a, 2, 93},
	} {
		got, ok := rawdb.ReadAccountTrace(reopened, tc.addr, tc.block)
		if !ok || got != tc.want {
			t.Fatalf("ReadAccountTrace addr=%x block=%d = %d/%v, want %d/true", tc.addr, tc.block, got, ok, tc.want)
		}
	}
}

func TestDBAuditBalanceTracesCmd(t *testing.T) {
	dataDir := t.TempDir()
	db, txInfos := seedDBRebuildTxIndexDatadir(t, dataDir, false)
	chainDB := rawdb.NewChainDB(db, rawdb.NoopAncient{})
	block1 := rawdb.ReadBlock(chainDB, 1)
	block2 := rawdb.ReadBlock(chainDB, 2)
	if block1 == nil || block2 == nil {
		t.Fatal("seeded blocks missing")
	}
	addr := dbRebuildTraceAddress(0xc0)
	if err := rawdb.WriteBlockBalanceTrace(db, 1, &contractpb.BlockBalanceTrace{
		BlockIdentifier: dbRebuildBlockBalanceID(block1),
		TransactionBalanceTrace: []*contractpb.TransactionBalanceTrace{{
			TransactionIdentifier: append([]byte(nil), txInfos[0].Id...),
			Operation: []*contractpb.TransactionBalanceTrace_Operation{
				dbRebuildBalanceOp(0, addr, 1),
			},
			Type:   "TransferContract",
			Status: "SUCCESS",
		}},
	}); err != nil {
		t.Fatalf("WriteBlockBalanceTrace block1: %v", err)
	}
	if err := rawdb.WriteAccountTrace(db, addr, 1, 1); err != nil {
		t.Fatalf("WriteAccountTrace block1: %v", err)
	}
	if err := rawdb.WriteBlockBalanceTrace(db, 2, &contractpb.BlockBalanceTrace{
		BlockIdentifier:         dbRebuildBlockBalanceID(block2),
		TransactionBalanceTrace: []*contractpb.TransactionBalanceTrace{},
	}); err != nil {
		t.Fatalf("WriteBlockBalanceTrace block2: %v", err)
	}
	db.Close()

	ctx := makeDBTestContext(t, []string{
		"--datadir", dataDir,
		"--db.from-block", "1",
		"--db.to-block", "2",
	})
	if err := dbAuditBalanceTracesCmd(ctx); err != nil {
		t.Fatalf("dbAuditBalanceTracesCmd: %v", err)
	}
}

func TestDBAuditBalanceTracesCmdRejectsIncompleteCoverage(t *testing.T) {
	dataDir := t.TempDir()
	db, _ := seedDBRebuildTxIndexDatadir(t, dataDir, false)
	chainDB := rawdb.NewChainDB(db, rawdb.NoopAncient{})
	block1 := rawdb.ReadBlock(chainDB, 1)
	if block1 == nil {
		t.Fatal("seeded block1 missing")
	}
	if err := rawdb.WriteBlockBalanceTrace(db, 1, &contractpb.BlockBalanceTrace{
		BlockIdentifier: dbRebuildBlockBalanceID(block1),
	}); err != nil {
		t.Fatalf("WriteBlockBalanceTrace block1: %v", err)
	}
	db.Close()

	ctx := makeDBTestContext(t, []string{
		"--datadir", dataDir,
		"--db.from-block", "1",
		"--db.to-block", "2",
	})
	err := dbAuditBalanceTracesCmd(ctx)
	if err == nil || !strings.Contains(err.Error(), "coverage incomplete") {
		t.Fatalf("dbAuditBalanceTracesCmd error = %v, want coverage incomplete", err)
	}
}

func TestDBBackfillBalanceTracesCmd(t *testing.T) {
	dataDir := t.TempDir()
	genesisPath, sender, receiver, block1 := seedDBBackfillBalanceTraceDatadir(t, dataDir)
	replayDir := filepath.Join(t.TempDir(), "replay")

	ctx := makeDBTestContext(t, []string{
		"--datadir", dataDir,
		"--genesis", genesisPath,
		"--db.from-block", "1",
		"--db.to-block", "1",
		"--db.replay.dir", replayDir,
		"--db.etl.tempdir", t.TempDir(),
		"--db.etl.buffer", "1",
	})
	if err := dbBackfillBalanceTracesCmd(ctx); err != nil {
		t.Fatalf("dbBackfillBalanceTracesCmd: %v", err)
	}

	reopened, err := rawdb.NewPebbleDB(chainDataDir(dataDir), 256, 500)
	if err != nil {
		t.Fatalf("reopen pebble: %v", err)
	}
	trace := rawdb.ReadBlockBalanceTrace(reopened, 1)
	if trace == nil {
		t.Fatal("BlockBalanceTrace missing after backfill")
	}
	if trace.GetBlockIdentifier().GetNumber() != 1 || string(trace.GetBlockIdentifier().GetHash()) != string(block1.Hash().Bytes()) {
		t.Fatalf("trace id = %+v, want block 1 %x", trace.GetBlockIdentifier(), block1.Hash())
	}
	if _, ok := rawdb.ReadAccountTrace(reopened, sender.Bytes(), 1); !ok {
		t.Fatal("sender AccountTrace missing after backfill")
	}
	if _, ok := rawdb.ReadAccountTrace(reopened, receiver.Bytes(), 1); !ok {
		t.Fatal("receiver AccountTrace missing after backfill")
	}
	if err := rawdb.DeleteBlockBalanceTrace(reopened, 1); err != nil {
		t.Fatalf("DeleteBlockBalanceTrace: %v", err)
	}
	if err := rawdb.DeleteAccountTrace(reopened, sender.Bytes(), 1); err != nil {
		t.Fatalf("DeleteAccountTrace sender: %v", err)
	}
	if err := rawdb.DeleteAccountTrace(reopened, receiver.Bytes(), 1); err != nil {
		t.Fatalf("DeleteAccountTrace receiver: %v", err)
	}
	reopened.Close()

	ctx = makeDBTestContext(t, []string{
		"--datadir", dataDir,
		"--genesis", genesisPath,
		"--db.from-block", "1",
		"--db.to-block", "1",
		"--db.replay.dir", replayDir,
		"--db.etl.tempdir", t.TempDir(),
		"--db.etl.buffer", "1",
	})
	if err := dbBackfillBalanceTracesCmd(ctx); err != nil {
		t.Fatalf("resume dbBackfillBalanceTracesCmd: %v", err)
	}
	reopened, err = rawdb.NewPebbleDB(chainDataDir(dataDir), 256, 500)
	if err != nil {
		t.Fatalf("reopen pebble after resume: %v", err)
	}
	defer reopened.Close()
	if trace := rawdb.ReadBlockBalanceTrace(reopened, 1); trace == nil {
		t.Fatal("BlockBalanceTrace missing after replay-dir resume")
	}
}

func TestDBBackfillBalanceTracesCmdSeedsReplayFromSnapshot(t *testing.T) {
	dataDir := t.TempDir()
	genesisPath, sender, receiver, block1 := seedDBBackfillBalanceTraceDatadir(t, dataDir)
	snapshotDir := filepath.Join(t.TempDir(), "snapshot")
	replayDir := filepath.Join(t.TempDir(), "replay")

	sourceDB, err := rawdb.NewPebbleDB(chainDataDir(dataDir), 256, 500)
	if err != nil {
		t.Fatalf("open source pebble: %v", err)
	}
	if err := rawdb.WriteStateTxRange(sourceDB, block1.Number(), block1.Hash(), 1, 1); err != nil {
		t.Fatalf("WriteStateTxRange block1: %v", err)
	}
	trustedKey := writeDBBackfillReplaySeedSnapshot(t, sourceDB, genesisPath, snapshotDir, block1)

	genesis, err := loadGenesisFile(genesisPath)
	if err != nil {
		t.Fatalf("loadGenesisFile: %v", err)
	}
	bc, err := corepkg.NewBlockChain(sourceDB, corestate.NewDatabase(rawdb.WrapKeyValueStore(sourceDB)), genesis.Config)
	if err != nil {
		t.Fatalf("NewBlockChain source: %v", err)
	}
	block2 := dbBackfillTransferBlock(t, 2, 6000, block1.Hash(), sender, receiver, 7_000_000)
	if err := bc.InsertBlock(block2); err != nil {
		t.Fatalf("InsertBlock block2: %v", err)
	}
	if got := rawdb.ReadBlockBalanceTrace(sourceDB, 2); got != nil {
		t.Fatalf("pre-backfill BlockBalanceTrace 2 = %+v, want nil", got)
	}
	if err := bc.Close(); err != nil {
		t.Fatalf("close source chain: %v", err)
	}
	sourceDB.Close()

	rejectCtx := makeDBTestContext(t, []string{
		"--datadir", dataDir,
		"--genesis", genesisPath,
		"--db.from-block", "1",
		"--db.to-block", "2",
		"--db.replay.dir", filepath.Join(t.TempDir(), "reject-replay"),
		"--snapshot.dir", snapshotDir,
		"--snapshot.trusted-key", trustedKey,
	})
	if err := dbBackfillBalanceTracesCmd(rejectCtx); err == nil || !strings.Contains(err.Error(), "can only backfill from block 2 or later") {
		t.Fatalf("dbBackfillBalanceTracesCmd boundary error = %v, want from-block rejection", err)
	}

	ctx := makeDBTestContext(t, []string{
		"--datadir", dataDir,
		"--genesis", genesisPath,
		"--db.from-block", "2",
		"--db.to-block", "2",
		"--db.replay.dir", replayDir,
		"--db.etl.tempdir", t.TempDir(),
		"--db.etl.buffer", "1",
		"--snapshot.dir", snapshotDir,
		"--snapshot.trusted-key", trustedKey,
	})
	if err := dbBackfillBalanceTracesCmd(ctx); err != nil {
		t.Fatalf("dbBackfillBalanceTracesCmd snapshot seed: %v", err)
	}

	reopened, err := rawdb.NewPebbleDB(chainDataDir(dataDir), 256, 500)
	if err != nil {
		t.Fatalf("reopen target pebble: %v", err)
	}
	defer reopened.Close()
	if got := rawdb.ReadBlockBalanceTrace(reopened, 1); got != nil {
		t.Fatalf("target BlockBalanceTrace 1 = %+v, want untouched", got)
	}
	if got := rawdb.ReadBlockBalanceTrace(reopened, 2); got == nil {
		t.Fatal("target BlockBalanceTrace 2 missing after snapshot-seeded backfill")
	}

	replayDB, err := rawdb.NewPebbleDB(replayDir, 256, 500)
	if err != nil {
		t.Fatalf("open replay pebble: %v", err)
	}
	defer replayDB.Close()
	if got := rawdb.ReadBlockBalanceTrace(replayDB, 1); got != nil {
		t.Fatalf("replay BlockBalanceTrace 1 = %+v, want no genesis-prefix replay", got)
	}
	if got := rawdb.ReadBlockBalanceTrace(replayDB, 2); got == nil {
		t.Fatal("replay BlockBalanceTrace 2 missing after replay from snapshot boundary")
	}
}

func TestDBFreezerStatusCmd(t *testing.T) {
	dataDir := t.TempDir()
	f, err := rawdbfreezer.NewFreezer(ancientDataDir(dataDir), "", false, 50, chainfreezer.FreezerTableSet())
	if err != nil {
		t.Fatalf("NewFreezer: %v", err)
	}
	_, err = f.ModifyAncients(func(op rawdbfreezer.AncientWriteOp) error {
		for i := uint64(0); i < 5; i++ {
			if err := op.AppendRaw(rawdb.AncientBlocksTable, i, []byte{byte(i), byte(i + 1)}); err != nil {
				return err
			}
			if err := op.AppendRaw(rawdb.AncientTxInfosTable, i, []byte{byte(i + 2)}); err != nil {
				return err
			}
			if err := op.AppendRaw(rawdb.AncientStateRootsTable, i, []byte{byte(i + 3)}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ModifyAncients: %v", err)
	}
	if _, err := f.TruncateTail(3); err != nil {
		t.Fatalf("TruncateTail: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close freezer: %v", err)
	}

	ctx := makeDBTestContext(t, []string{"--datadir", dataDir})
	output, err := captureDBCmdStdout(t, func() error {
		return dbFreezerStatusCmd(ctx)
	})
	if err != nil {
		t.Fatalf("dbFreezerStatusCmd: %v", err)
	}
	for _, want := range []string{
		"Freezer status:",
		"readonly=true",
		"head=5",
		"tail=3",
		"repairApplied=false",
		"name=" + rawdb.AncientBlocksTable,
		"name=" + rawdb.AncientTxInfosTable,
		"name=" + rawdb.AncientStateRootsTable,
		"hiddenTail=3",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("freezer status output missing %q:\n%s", want, output)
		}
	}
}

func TestDBStageStatusCmd(t *testing.T) {
	dataDir := t.TempDir()
	db, err := rawdb.NewPebbleDB(chainDataDir(dataDir), 256, 500)
	if err != nil {
		t.Fatalf("open pebble: %v", err)
	}
	block1, _ := dbRebuildTxIndexBlock(t, 1, 0)
	if err := rawdb.WriteBlock(db, block1); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageHeaders, block1.Number(), block1.Hash()); err != nil {
		t.Fatalf("WriteStageProgress Headers: %v", err)
	}
	mismatchHash := common.Hash{0xee}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncBodies, block1.Number(), mismatchHash); err != nil {
		t.Fatalf("WriteStageProgress SyncBodies: %v", err)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncImport, block1.Number(), mismatchHash); err != nil {
		t.Fatalf("WriteStageProgress SyncImport: %v", err)
	}
	if err := rawdb.WriteStageProgress(db, rawdb.StageSnapshotHistory, 11); err != nil {
		t.Fatalf("WriteStageProgress SnapshotHistory: %v", err)
	}
	if err := rawdb.WriteStageProgress(db, rawdb.StageID("FutureStage"), 77); err != nil {
		t.Fatalf("WriteStageProgress FutureStage: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close pebble: %v", err)
	}

	ctx := makeDBTestContext(t, []string{"--datadir", dataDir})
	output, err := captureDBCmdStdout(t, func() error {
		return dbStageStatusCmd(ctx)
	})
	if err != nil {
		t.Fatalf("dbStageStatusCmd: %v", err)
	}
	for _, want := range []string{
		"Stage status:",
		"known=",
		fmt.Sprintf("group=canonical name=%s value=1 hash=%x verified=canonical", rawdb.StageHeaders, block1.Hash()),
		fmt.Sprintf("group=sync name=%s value=1 hash=%x verified=mismatch canonicalHash=%x", rawdb.StageSyncBodies, mismatchHash, block1.Hash()),
		fmt.Sprintf("group=sync name=%s value=1 hash=%x verified=mismatch canonicalHash=%x", rawdb.StageSyncImport, mismatchHash, block1.Hash()),
		fmt.Sprintf("group=snapshot name=%s value=11 hash=none verified=unbound", rawdb.StageSnapshotHistory),
		fmt.Sprintf("group=freezer name=%s status=missing", rawdb.StageChainFreezer),
		"group=unknown name=FutureStage value=77 hash=none verified=unbound",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("stage status output missing %q:\n%s", want, output)
		}
	}

	verifyCtx := makeDBTestContext(t, []string{"--datadir", dataDir, "--db.stage.verify"})
	verifyOutput, err := captureDBCmdStdout(t, func() error {
		return dbStageStatusCmd(verifyCtx)
	})
	if err == nil || !strings.Contains(err.Error(), "stage status verification failed") || !strings.Contains(err.Error(), string(rawdb.StageSyncImport)) || !strings.Contains(err.Error(), "mismatch") {
		t.Fatalf("dbStageStatusCmd verify err = %v, want SyncImport mismatch", err)
	}
	if strings.Contains(err.Error(), string(rawdb.StageSyncBodies)) {
		t.Fatalf("dbStageStatusCmd verify err = %v, downloader SyncBodies should not fail verification", err)
	}
	if !strings.Contains(verifyOutput, fmt.Sprintf("group=sync name=%s", rawdb.StageSyncBodies)) {
		t.Fatalf("verify output missing SyncBodies line:\n%s", verifyOutput)
	}
}

func TestDBStageStatusPipelineOrderIssues(t *testing.T) {
	rows := []dbStageStatusRow{
		{
			stage:   rawdb.StageBodies,
			group:   "canonical",
			present: true,
			progress: rawdb.StageProgress{
				Stage:    rawdb.StageBodies,
				BlockNum: 5,
			},
		},
		{
			stage:   rawdb.StageExecution,
			group:   "canonical",
			present: true,
			progress: rawdb.StageProgress{
				Stage:    rawdb.StageExecution,
				BlockNum: 6,
			},
		},
		{
			stage:   rawdb.StageFinish,
			group:   "canonical",
			present: true,
			progress: rawdb.StageProgress{
				Stage:    rawdb.StageFinish,
				BlockNum: 30,
			},
		},
		{
			stage:   rawdb.StageSnapshotBuild,
			group:   "snapshot",
			present: true,
			progress: rawdb.StageProgress{
				Stage:    rawdb.StageSnapshotBuild,
				BlockNum: 31,
			},
		},
		{
			stage:   rawdb.StageSnapshotPrune,
			group:   "prune",
			present: true,
			progress: rawdb.StageProgress{
				Stage:    rawdb.StageSnapshotPrune,
				BlockNum: 32,
			},
		},
		{
			stage:   rawdb.StageSnapshotEventLogBuild,
			group:   "snapshot",
			present: true,
			progress: rawdb.StageProgress{
				Stage:    rawdb.StageSnapshotEventLogBuild,
				BlockNum: 33,
			},
		},
		{
			stage:   rawdb.StageSnapshotSectionBloomPrune,
			group:   "prune",
			present: true,
			progress: rawdb.StageProgress{
				Stage:    rawdb.StageSnapshotSectionBloomPrune,
				BlockNum: 34,
			},
		},
		{
			stage:   rawdb.StageSnapshotBalanceTracePrune,
			group:   "prune",
			present: true,
			progress: rawdb.StageProgress{
				Stage:    rawdb.StageSnapshotBalanceTracePrune,
				BlockNum: 35,
			},
		},
		{
			stage:   rawdb.StageSyncBodies,
			group:   "sync",
			present: true,
			progress: rawdb.StageProgress{
				Stage:    rawdb.StageSyncBodies,
				BlockNum: 7,
			},
		},
		{
			stage:   rawdb.StageSyncBodiesReady,
			group:   "sync",
			present: true,
			progress: rawdb.StageProgress{
				Stage:    rawdb.StageSyncBodiesReady,
				BlockNum: 8,
			},
		},
		{
			stage:   rawdb.StageSyncImport,
			group:   "sync",
			present: true,
			progress: rawdb.StageProgress{
				Stage:    rawdb.StageSyncImport,
				BlockNum: 3,
			},
		},
		{
			stage:   rawdb.StageSyncExecution,
			group:   "sync",
			present: true,
			progress: rawdb.StageProgress{
				Stage:    rawdb.StageSyncExecution,
				BlockNum: 4,
			},
		},
		{
			stage:   rawdb.StageChainFreezer,
			group:   "freezer",
			present: true,
			progress: rawdb.StageProgress{
				Stage:    rawdb.StageChainFreezer,
				BlockNum: 20,
			},
		},
		{
			stage:   rawdb.StageSnapshotChainLookupPrune,
			group:   "prune",
			present: true,
			progress: rawdb.StageProgress{
				Stage:    rawdb.StageSnapshotChainLookupPrune,
				BlockNum: 21,
			},
		},
		{
			stage:   rawdb.StageSnapshotChainFreezerTailPrune,
			group:   "prune",
			present: true,
			progress: rawdb.StageProgress{
				Stage:    rawdb.StageSnapshotChainFreezerTailPrune,
				BlockNum: 34,
			},
		},
	}

	issues := dbStageStatusPipelineOrderIssues(rows)
	for _, want := range []string{
		"Execution=6 ahead of Bodies=5",
		"SnapshotBuild=31 ahead of Finish=30",
		"SnapshotPrune=32 ahead of Finish=30",
		"SnapshotEventLogBuild=33 ahead of Finish=30",
		"SnapshotSectionBloomPrune=34 ahead of Finish=30",
		"SnapshotBalanceTracePrune=35 ahead of Finish=30",
		"SyncBodiesReady=8 ahead of SyncBodies=7",
		"SyncExecution=4 ahead of SyncImport=3",
		"SnapshotChainLookupPrune=21 ahead of ChainFreezer=20",
		"SnapshotChainFreezerTailPrune=34 ahead of SnapshotChainLookupPrune=21",
		"SnapshotChainFreezerTailPrune=34 ahead of SnapshotEventLogBuild=33",
	} {
		found := false
		for _, issue := range issues {
			if issue == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("pipeline order issues missing %q in %#v", want, issues)
		}
	}

	rows = []dbStageStatusRow{
		{
			stage:   rawdb.StageSyncExecution,
			group:   "sync",
			present: true,
			progress: rawdb.StageProgress{
				Stage:    rawdb.StageSyncExecution,
				BlockNum: 4,
			},
		},
	}
	if issues := dbStageStatusPipelineOrderIssues(rows); len(issues) != 0 {
		t.Fatalf("pipeline order issues with missing upstream = %#v, want none", issues)
	}

	rows = []dbStageStatusRow{
		{
			stage:   rawdb.StageSnapshotBuild,
			group:   "snapshot",
			present: true,
			progress: rawdb.StageProgress{
				Stage:    rawdb.StageSnapshotBuild,
				BlockNum: 12,
			},
		},
		{
			stage:   rawdb.StageSnapshotChainLookupPrune,
			group:   "prune",
			present: true,
			progress: rawdb.StageProgress{
				Stage:    rawdb.StageSnapshotChainLookupPrune,
				BlockNum: 8,
			},
		},
		{
			stage:   rawdb.StageSnapshotSectionBloomPrune,
			group:   "prune",
			present: true,
			progress: rawdb.StageProgress{
				Stage:    rawdb.StageSnapshotSectionBloomPrune,
				BlockNum: 9,
			},
		},
		{
			stage:   rawdb.StageSnapshotEventLogBuild,
			group:   "snapshot",
			present: true,
			progress: rawdb.StageProgress{
				Stage:    rawdb.StageSnapshotEventLogBuild,
				BlockNum: 10,
			},
		},
	}
	issues = dbStageStatusPipelineOrderIssues(rows)
	for _, want := range []string{
		"SnapshotBuild requires Finish",
		"SnapshotChainLookupPrune requires ChainFreezer",
		"SnapshotSectionBloomPrune requires Finish",
		"SnapshotEventLogBuild requires Finish",
	} {
		found := false
		for _, issue := range issues {
			if issue == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("pipeline order issues missing %q in %#v", want, issues)
		}
	}

	rows = []dbStageStatusRow{
		{
			stage:   rawdb.StageSnapshotChainFreezerTailPrune,
			group:   "prune",
			present: true,
			progress: rawdb.StageProgress{
				Stage:    rawdb.StageSnapshotChainFreezerTailPrune,
				BlockNum: 20,
			},
		},
	}
	issues = dbStageStatusPipelineOrderIssues(rows)
	for _, want := range []string{
		"SnapshotChainFreezerTailPrune requires SnapshotChainLookupPrune",
		"SnapshotChainFreezerTailPrune requires SnapshotEventLogBuild",
	} {
		found := false
		for _, issue := range issues {
			if issue == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("pipeline order issues missing %q in %#v", want, issues)
		}
	}
}

func TestDBStageStatusSnapshotCoverageIssues(t *testing.T) {
	rows := []dbStageStatusRow{
		{
			stage:   rawdb.StageSnapshotEventLogBuild,
			group:   "snapshot",
			present: true,
			progress: rawdb.StageProgress{
				Stage:    rawdb.StageSnapshotEventLogBuild,
				BlockNum: 12,
			},
		},
		{
			stage:   rawdb.StageSnapshotChainLookupPrune,
			group:   "prune",
			present: true,
			progress: rawdb.StageProgress{
				Stage:    rawdb.StageSnapshotChainLookupPrune,
				BlockNum: 12,
			},
		},
		{
			stage:   rawdb.StageSnapshotSectionBloomPrune,
			group:   "prune",
			present: true,
			progress: rawdb.StageProgress{
				Stage:    rawdb.StageSnapshotSectionBloomPrune,
				BlockNum: rawdb.SectionBloomBlockPerSection - 1,
			},
		},
		{
			stage:   rawdb.StageSnapshotBalanceTracePrune,
			group:   "prune",
			present: true,
			progress: rawdb.StageProgress{
				Stage:    rawdb.StageSnapshotBalanceTracePrune,
				BlockNum: 12,
			},
		},
		{
			stage:   rawdb.StageSnapshotChainFreezerTailPrune,
			group:   "prune",
			present: true,
			progress: rawdb.StageProgress{
				Stage:    rawdb.StageSnapshotChainFreezerTailPrune,
				BlockNum: 12,
			},
		},
	}
	issues := dbStageStatusSnapshotCoverageIssues(rows, filepath.Join(t.TempDir(), "missing-snapshots"))
	for _, want := range []string{
		"SnapshotEventLogBuild=12 missing cold indexed event-log coverage [1,12]",
		"SnapshotChainLookupPrune=12 missing cold chain-index coverage [0,12]",
		fmt.Sprintf("SnapshotSectionBloomPrune=%d missing cold section-bloom coverage [0,%d]", rawdb.SectionBloomBlockPerSection-1, rawdb.SectionBloomBlockPerSection-1),
		"SnapshotBalanceTracePrune=12 missing cold balance-trace coverage [0,12]",
		"SnapshotChainFreezerTailPrune=12 missing cold chain-freezer coverage [0,12]",
		"SnapshotChainFreezerTailPrune=12 missing cold indexed event-log coverage [1,12]",
	} {
		found := false
		for _, issue := range issues {
			if issue == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("snapshot coverage issues missing %q in %#v", want, issues)
		}
	}
}

func TestDBStageStatusSnapshotCoverageIssuesChecksManifestProgress(t *testing.T) {
	snapshotDir := t.TempDir()
	manifest := statesnapshots.NewManifest(1, 12, nil)
	manifest.Progress = &statesnapshots.Progress{
		LatestBuildTxNum:     10,
		HistoryBuildTxNum:    10,
		AccessorBuildTxNum:   13,
		CommitmentFlushTxNum: 0,
		HotPruneTxNum:        5,
	}
	if err := statesnapshots.PublishManifest(snapshotDir, manifest); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}

	rows := []dbStageStatusRow{
		{
			stage:   rawdb.StageSnapshotLatest,
			group:   "snapshot",
			present: true,
			progress: rawdb.StageProgress{
				Stage:    rawdb.StageSnapshotLatest,
				BlockNum: 8,
			},
		},
		{
			stage:   rawdb.StageSnapshotHistory,
			group:   "snapshot",
			present: true,
			progress: rawdb.StageProgress{
				Stage:    rawdb.StageSnapshotHistory,
				BlockNum: 12,
			},
		},
		{
			stage:   rawdb.StageSnapshotAccessor,
			group:   "snapshot",
			present: true,
			progress: rawdb.StageProgress{
				Stage:    rawdb.StageSnapshotAccessor,
				BlockNum: 12,
			},
		},
		{
			stage:   rawdb.StageSnapshotCommitmentFlush,
			group:   "snapshot",
			present: true,
			progress: rawdb.StageProgress{
				Stage:    rawdb.StageSnapshotCommitmentFlush,
				BlockNum: 1,
			},
		},
		{
			stage:   rawdb.StageSnapshotHotPrune,
			group:   "prune",
			present: true,
			progress: rawdb.StageProgress{
				Stage:    rawdb.StageSnapshotHotPrune,
				BlockNum: 6,
			},
		},
	}
	issues := dbStageStatusSnapshotCoverageIssues(rows, snapshotDir)
	for _, want := range []string{
		"SnapshotHistory=12 ahead of snapshot manifest history progress 10",
		"SnapshotCommitmentFlush=1 missing snapshot manifest commitment-flush progress",
		"SnapshotHotPrune=6 ahead of snapshot manifest hot-prune progress 5",
	} {
		found := false
		for _, issue := range issues {
			if issue == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("snapshot coverage issues missing %q in %#v", want, issues)
		}
	}
	if len(issues) != 3 {
		t.Fatalf("snapshot coverage issues = %#v, want only manifest progress issues", issues)
	}
}

func TestDBStageStatusSnapshotCoverageIssuesRequiresManifestProgress(t *testing.T) {
	rows := []dbStageStatusRow{
		{
			stage:   rawdb.StageSnapshotLatest,
			group:   "snapshot",
			present: true,
			progress: rawdb.StageProgress{
				Stage:    rawdb.StageSnapshotLatest,
				BlockNum: 7,
			},
		},
	}
	issues := dbStageStatusSnapshotCoverageIssues(rows, filepath.Join(t.TempDir(), "missing-snapshots"))
	want := "SnapshotLatest=7 missing snapshot manifest latest progress"
	for _, issue := range issues {
		if issue == want {
			return
		}
	}
	t.Fatalf("snapshot coverage issues missing %q in %#v", want, issues)
}

func writeDBBackfillReplaySeedSnapshot(t *testing.T, sourceDB ethdb.KeyValueStore, genesisPath string, snapshotDir string, boundary *coretypes.Block) string {
	t.Helper()
	genesis, err := loadGenesisFile(genesisPath)
	if err != nil {
		t.Fatalf("loadGenesisFile: %v", err)
	}
	if root, ok, err := rawdb.ReadLatestDomainCommitmentRoot(sourceDB); err != nil || !ok || root == (common.Hash{}) {
		t.Fatalf("ReadLatestDomainCommitmentRoot = %x/%v/%v, want restored latest root", root, ok, err)
	} else if err := rawdb.WriteStateCommitmentCheckpoint(sourceDB, &rawdb.StateCommitmentCheckpoint{
		BlockNum:  boundary.Number(),
		BlockHash: boundary.Hash(),
		Root:      root,
		Scheme:    rawdb.LatestDomainCommitmentScheme,
	}); err != nil {
		t.Fatalf("WriteStateCommitmentCheckpoint: %v", err)
	}
	refs, err := statesnapshots.NewAggregator(snapshotDir).BuildSegments(sourceDB, statesnapshots.AggregatorBuildOptions{
		FromTxNum: 1,
		ToTxNum:   1,
	})
	if err != nil {
		t.Fatalf("BuildSegments: %v", err)
	}
	identity, err := snapshotExpectedChainIdentityFromGenesis(genesis, "")
	if err != nil {
		t.Fatalf("snapshotExpectedChainIdentityFromGenesis: %v", err)
	}
	if err := statesnapshots.PublishManifest(snapshotDir, statesnapshots.NewManifestForChain(1, 1, refs, identity)); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	if _, err := statesnapshots.PublishSignedSnapshotCatalog(snapshotDir, priv); err != nil {
		t.Fatalf("PublishSignedSnapshotCatalog: %v", err)
	}
	return hex.EncodeToString(pub)
}

func seedDBRebuildTxIndexDatadir(t *testing.T, dataDir string, writeHead bool) (ethdb.KeyValueStore, []*corepb.TransactionInfo) {
	t.Helper()
	db, err := rawdb.NewPebbleDB(chainDataDir(dataDir), 256, 500)
	if err != nil {
		t.Fatalf("open pebble: %v", err)
	}
	block1, infos1 := dbRebuildTxIndexBlock(t, 1, 2)
	block2, infos2 := dbRebuildTxIndexBlock(t, 2, 1)
	if err := rawdb.WriteBlock(db, block1); err != nil {
		t.Fatalf("WriteBlock block1: %v", err)
	}
	if err := rawdb.WriteBlock(db, block2); err != nil {
		t.Fatalf("WriteBlock block2: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(db, 1, infos1); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock block1: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(db, 2, infos2); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock block2: %v", err)
	}
	if writeHead {
		rawdb.WriteHeadBlockHash(db, block2.Hash())
	}
	chainDB := rawdb.NewChainDB(db, rawdb.NoopAncient{})
	if got := rawdb.ReadTransactionIndex(chainDB, infos1[1].Id); got != nil {
		t.Fatalf("pre-rebuild tx index = %v, want nil", got)
	}
	if got := rawdb.ReadTransactionInfo(chainDB, infos1[1].Id); got != nil {
		t.Fatalf("pre-rebuild tx info = %+v, want nil", got)
	}
	return db, append(infos1, infos2...)
}

func seedDBBackfillBalanceTraceDatadir(t *testing.T, dataDir string) (string, common.Address, common.Address, *coretypes.Block) {
	t.Helper()
	sender := dbRebuildTraceAddressT(0xd0)
	receiver := dbRebuildTraceAddressT(0xe0)
	genesisPath := filepath.Join(t.TempDir(), "genesis.json")
	genesisJSON := `{
  "chain_id": 1999,
  "p2p_version": 1999,
  "timestamp_ms": 0,
  "parent_hash": "0000000000000000000000000000000000000000000000000000000000000000",
  "accounts": [
    {"address": "` + sender.Hex() + `", "balance": "99000000000000000"},
    {"address": "` + receiver.Hex() + `", "balance": "1"}
  ],
  "dynamic_properties": {
    "maintenance_time_interval": 21600000,
    "next_maintenance_time": 4611686018427387903
  }
}`
	if err := os.WriteFile(genesisPath, []byte(genesisJSON), 0o644); err != nil {
		t.Fatalf("write genesis file: %v", err)
	}
	genesis, err := loadGenesisFile(genesisPath)
	if err != nil {
		t.Fatalf("loadGenesisFile: %v", err)
	}
	db, err := rawdb.NewPebbleDB(chainDataDir(dataDir), 256, 500)
	if err != nil {
		t.Fatalf("open pebble: %v", err)
	}
	defer db.Close()
	_, genesisHash, err := corepkg.SetupGenesisBlock(db, genesis)
	if err != nil {
		t.Fatalf("SetupGenesisBlock: %v", err)
	}
	bc, err := corepkg.NewBlockChain(db, corestate.NewDatabase(rawdb.WrapKeyValueStore(db)), genesis.Config)
	if err != nil {
		t.Fatalf("NewBlockChain: %v", err)
	}
	defer bc.Close()
	block1 := dbBackfillTransferBlock(t, 1, 3000, genesisHash, sender, receiver, 5_000_000)
	if err := bc.InsertBlock(block1); err != nil {
		t.Fatalf("InsertBlock: %v", err)
	}
	if got := rawdb.ReadBlockBalanceTrace(db, 1); got != nil {
		t.Fatalf("pre-backfill BlockBalanceTrace = %+v, want nil", got)
	}
	return genesisPath, sender, receiver, block1
}

func dbBackfillTransferBlock(t *testing.T, number int64, ts int64, parentHash common.Hash, sender, receiver common.Address, amount int64) *coretypes.Block {
	t.Helper()
	tc := &contractpb.TransferContract{
		OwnerAddress: sender.Bytes(),
		ToAddress:    receiver.Bytes(),
		Amount:       amount,
	}
	param, err := anypb.New(tc)
	if err != nil {
		t.Fatalf("Any TransferContract: %v", err)
	}
	txPB := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Expiration: ts + 60_000,
			Contract: []*corepb.Transaction_Contract{{
				Type:      corepb.Transaction_Contract_TransferContract,
				Parameter: param,
			}},
		},
	}
	return coretypes.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:     number,
				Timestamp:  ts,
				ParentHash: parentHash.Bytes(),
			},
		},
		Transactions: []*corepb.Transaction{txPB},
	})
}

func dbRebuildTxIndexBlock(t *testing.T, number uint64, txCount int) (*coretypes.Block, []*corepb.TransactionInfo) {
	t.Helper()
	txs := make([]*corepb.Transaction, 0, txCount)
	infos := make([]*corepb.TransactionInfo, 0, txCount)
	for i := 0; i < txCount; i++ {
		txPB := &corepb.Transaction{
			RawData: &corepb.TransactionRaw{
				Timestamp:  int64(10_000 + number*100 + uint64(i)),
				Expiration: int64(20_000 + number*100 + uint64(i)),
				Data:       []byte{byte(number), byte(i)},
			},
		}
		tx := coretypes.NewTransactionFromPB(txPB)
		txHash := tx.Hash()
		txs = append(txs, txPB)
		infos = append(infos, &corepb.TransactionInfo{
			Id:             append([]byte(nil), txHash[:]...),
			Fee:            int64(1_000 + number*10 + uint64(i)),
			BlockNumber:    int64(number),
			BlockTimeStamp: int64(30_000 + number),
		})
	}
	block := coretypes.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    int64(number),
				Timestamp: int64(30_000 + number),
			},
		},
		Transactions: txs,
	})
	return block, infos
}

func dbRebuildTraceAddress(seed byte) []byte {
	out := make([]byte, common.AddressLength)
	out[0] = common.AddressPrefixMainnet
	for i := 1; i < len(out); i++ {
		out[i] = seed + byte(i)
	}
	return out
}

func dbRebuildTraceAddressT(seed byte) common.Address {
	return common.BytesToAddress(dbRebuildTraceAddress(seed))
}

func dbRebuildBalanceOp(id int64, addr []byte, amount int64) *contractpb.TransactionBalanceTrace_Operation {
	return &contractpb.TransactionBalanceTrace_Operation{
		OperationIdentifier: id,
		Address:             append([]byte(nil), addr...),
		Amount:              amount,
	}
}

func dbRebuildBlockBalanceID(block *coretypes.Block) *contractpb.BlockBalanceTrace_BlockIdentifier {
	return &contractpb.BlockBalanceTrace_BlockIdentifier{
		Hash:   append([]byte(nil), block.Hash().Bytes()...),
		Number: int64(block.Number()),
	}
}

func makeDBTestContext(t *testing.T, argv []string) *cli.Context {
	t.Helper()
	app := cli.NewApp()
	app.Flags = []cli.Flag{
		dataDirFlag,
		dbCacheFlag,
		dbHandlesFlag,
		dbMemtableFlag,
		dbL0CompactionFlag,
		dbL0StopFlag,
		dbFromBlockFlag,
		dbToBlockFlag,
		dbETLTempDirFlag,
		dbETLBufferMiBFlag,
		dbETLBatchMiBFlag,
		dbReplayTempDirFlag,
		dbReplayDirFlag,
		dbBalanceTraceOverwriteFlag,
		dbStageVerifyFlag,
		snapshotDirFlag,
		snapshotTrustedCatalogKeyFlag,
		snapshotTrustedCatalogKeyFileFlag,
		snapshotForkConfigHashFlag,
		testnetFlag,
		genesisFileFlag,
		devFlag,
		devFullFeaturesFlag,
		devMaintenanceIntervalFlag,
		witnessKeyFlag,
	}
	set := flag.NewFlagSet("db-command-test", flag.ContinueOnError)
	for _, f := range app.Flags {
		if err := f.Apply(set); err != nil {
			t.Fatalf("apply flag: %v", err)
		}
	}
	if err := set.Parse(argv); err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	return cli.NewContext(app, set, nil)
}

func captureDBCmdStdout(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe stdout: %v", err)
	}
	os.Stdout = w
	defer func() {
		os.Stdout = old
		_ = r.Close()
	}()
	runErr := fn()
	closeErr := w.Close()
	out, readErr := io.ReadAll(r)
	if runErr != nil {
		return string(out), runErr
	}
	if closeErr != nil {
		return string(out), closeErr
	}
	if readErr != nil {
		return string(out), readErr
	}
	return string(out), nil
}

func dbTestSectionBloomBitSetHas(bitset []byte, bit uint64) bool {
	byteIndex := bit / 8
	if byteIndex >= uint64(len(bitset)) {
		return false
	}
	return bitset[byteIndex]&(1<<(bit%8)) != 0
}
