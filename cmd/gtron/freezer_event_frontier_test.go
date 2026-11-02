package main

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	chainfreezer "github.com/tronprotocol/go-tron/core/freezer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	snapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	coretypes "github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

type independentArchiveSource struct{ db *rawdb.ChainDB }

func (s independentArchiveSource) DB() ethdb.KeyValueStore         { return s.db }
func (s independentArchiveSource) LatestSolidifiedBlockNum() int64 { return 8 }
func (s independentArchiveSource) ReadBlockRawStrict(n uint64) ([]byte, bool, error) {
	return rawdb.ReadBlockRawStrict(s.db, n)
}
func (s independentArchiveSource) ReadTransactionInfosRawStrict(n uint64) ([]byte, bool, error) {
	return rawdb.ReadTransactionInfosRawStrict(s.db, n)
}
func (s independentArchiveSource) ReadBlockHashByNumberStrict(n uint64) (common.Hash, bool, error) {
	return rawdb.ReadBlockHashByNumberStrict(s.db, n)
}
func (s independentArchiveSource) ReadBlockStateRootRaw(common.Hash) ([]byte, error) { return nil, nil }
func (s independentArchiveSource) ReceiptLogRangeCovered(from, to uint64) (bool, error) {
	return s.db.EventLogRangeCovered(from, to)
}

type independentArchiveColdSource struct{ db *rawdb.ChainDB }

func (s independentArchiveColdSource) DB() snapshots.AggregatorDB          { return s.db }
func (s independentArchiveColdSource) LatestSolidifiedBlockNum() int64     { return 8 }
func (s independentArchiveColdSource) SyncRemainingBlocks() (uint64, bool) { return 1000, true }
func (s independentArchiveColdSource) CanonicalBlockHashStrict(n uint64) (common.Hash, bool, error) {
	return rawdb.ReadBlockHashByNumberStrict(s.db, n)
}

// Real direct V2 publication + hot deletion + external receipt hydration. No
// state tx ranges/history/source rows exist, so SnapshotBuild cannot advance.
func TestDirectV2EventFrontierUnblocksWithoutStateHistory(t *testing.T) {
	f, err := rawdbfreezer.NewFreezer(t.TempDir(), "", false, 2049, chainfreezer.FreezerTableSet())
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	db := rawdb.NewChainDB(rawdb.NewMemoryDatabase(), rawdb.NewFreezerReader(f))
	defer db.Close()
	for n := uint64(0); n <= 8; n++ {
		tx := &corepb.Transaction{RawData: &corepb.TransactionRaw{Timestamp: int64(n + 100), Data: []byte{byte(n)}}}
		block := coretypes.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: int64(n), Timestamp: int64(n * 3000)}}, Transactions: []*corepb.Transaction{tx}})
		if err := rawdb.WriteBlock(db, block); err != nil {
			t.Fatal(err)
		}
		info := &corepb.TransactionInfo{Id: coretypes.NewTransactionFromPB(tx).Hash().Bytes(), BlockNumber: int64(n), Log: []*corepb.TransactionInfo_Log{{Address: bytes.Repeat([]byte{0x31}, 20), Data: []byte{byte(n)}}}}
		if err := rawdb.WriteTransactionInfosByBlock(db, n, []*corepb.TransactionInfo{info}); err != nil {
			t.Fatal(err)
		}
		if n == 8 {
			if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageFinish, n, block.Hash()); err != nil {
				t.Fatal(err)
			}
		}
	}
	dir := t.TempDir()
	coldSource := independentArchiveColdSource{db: db}
	cold := snapshots.NewRunner(coldSource, snapshots.Config{Dir: dir, Enabled: true, HistoryWindow: 100,
		BuildEventLogs: true, EventLogVersion: snapshots.EventLogSegmentV4Version,
		BuildEventLogsWhileSyncing: true, DeferDerivedSidecarsWhileSyncing: true, DeferLatestBuildWhileSyncing: true,
		EventLogIndependentFrontier: true, SyncEventLogCatchupBlocks: 4,
		SyncEventLogTargetBlock: makeSyncEventLogTargetBlock(f, 4, nil),
	})
	// Manager and actual freezer source use the same mandatory runtime guard.
	manager, err := snapshots.OpenManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	manager.SetEventLogCanonicalVerifier(snapshots.NewCanonicalEventLogVerifier(coldSource))
	db.SetEventLogReader(manager)
	freezer := chainfreezer.New(independentArchiveSource{db: db}, newFreezerStore(f), chainfreezer.Config{
		Enabled: true, MarginBlocks: 1, V2Enabled: true, DirectV2: true, V2SegmentBlocks: 4, V2FrameBlocks: 2, ExternalizeV2ReceiptLogs: true,
	})
	if n, err := freezer.OnePass(); err != nil || n != 0 {
		t.Fatal("missing event coverage must block direct publication", n, err)
	}
	for pass := uint64(1); pass <= 2; pass++ {
		result, err := cold.OnePass()
		if err != nil || !result.EventLogBuilt || !result.EventLogFreezerHandoff || result.Built {
			t.Fatalf("event pass %d %+v %v", pass, result, err)
		}
		if n, err := freezer.OnePass(); err != nil || n != 4 {
			t.Fatal("direct publication remained coupled to state", n, err)
		}
		if f.V2Coverage() != pass*4 {
			t.Fatal("incomplete segment", f.V2Coverage())
		}
		if _, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotBuild); err != nil || ok {
			t.Fatal("fabricated state history", ok, err)
		}
		for n := pass*4 - 3; n < pass*4; n++ {
			infos, _, err := rawdb.ReadTransactionInfosByBlockStrict(db, n)
			if err != nil || len(infos) != 1 || len(infos[0].Log) != 1 || !bytes.Equal(infos[0].Log[0].Data, []byte{byte(n)}) {
				t.Fatalf("external receipt %d: %+v %v", n, infos, err)
			}
		}
	}
}
