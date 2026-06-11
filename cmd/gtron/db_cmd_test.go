package main

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	corepkg "github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corestate "github.com/tronprotocol/go-tron/core/state"
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

func dbTestSectionBloomBitSetHas(bitset []byte, bit uint64) bool {
	byteIndex := bit / 8
	if byteIndex >= uint64(len(bitset)) {
		return false
	}
	return bitset[byteIndex]&(1<<(bit%8)) != 0
}
