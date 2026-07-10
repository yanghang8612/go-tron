package snapshots

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func TestBuildChainFreezerSnapshotPassBuildsContiguousBatches(t *testing.T) {
	root := t.TempDir()
	store := openChainFreezerTestStore(t, filepath.Join(root, "ancient"))
	defer store.Close()
	appendChainFreezerTestRows(t, store, 0, 4)

	cfg := ChainFreezerSnapshotConfig{
		Dir:         filepath.Join(root, "snapshot"),
		BatchBlocks: 2,
	}
	first, err := BuildChainFreezerSnapshotPass(store, nil, cfg)
	if err != nil {
		t.Fatalf("first chain-freezer snapshot pass: %v", err)
	}
	if !first.Built || first.FromBlock != 0 || first.ToBlock != 1 || first.ColdHead != 2 {
		t.Fatalf("first result = %+v, want build [0,1] with cold head 2", first)
	}
	mgr, err := OpenManager(cfg.Dir)
	if err != nil {
		t.Fatalf("OpenManager after first pass: %v", err)
	}
	if covered, err := mgr.ChainIndexRangeCovered(0, 1); err != nil || !covered {
		t.Fatalf("ChainIndexRangeCovered(0,1) = %v/%v, want true/nil", covered, err)
	}

	second, err := BuildChainFreezerSnapshotPass(store, nil, cfg)
	if err != nil {
		t.Fatalf("second chain-freezer snapshot pass: %v", err)
	}
	if !second.Built || second.FromBlock != 2 || second.ToBlock != 3 || second.ColdHead != 4 {
		t.Fatalf("second result = %+v, want build [2,3] with cold head 4", second)
	}
	third, err := BuildChainFreezerSnapshotPass(store, nil, cfg)
	if err != nil {
		t.Fatalf("third chain-freezer snapshot pass: %v", err)
	}
	if !third.Built || third.FromBlock != 4 || third.ToBlock != 4 || third.ColdHead != 5 {
		t.Fatalf("third result = %+v, want build [4,4] with cold head 5", third)
	}

	idle, err := BuildChainFreezerSnapshotPass(store, nil, cfg)
	if err != nil {
		t.Fatalf("idle chain-freezer snapshot pass: %v", err)
	}
	if idle.Built || idle.ColdHead != 5 {
		t.Fatalf("idle result = %+v, want no build at cold head 5", idle)
	}
	if covered, err := mgr.ChainIndexRangeCovered(0, 4); err != nil || !covered {
		t.Fatalf("ChainIndexRangeCovered(0,4) = %v/%v, want true/nil", covered, err)
	}
}

func TestBuildChainFreezerSnapshotPassRejectsUncoveredLocalTail(t *testing.T) {
	root := t.TempDir()
	store := openChainFreezerTestStore(t, filepath.Join(root, "ancient"))
	defer store.Close()
	appendChainFreezerTestRows(t, store, 0, 2)
	if _, err := store.TruncateTail(1); err != nil {
		t.Fatalf("TruncateTail: %v", err)
	}

	_, err := BuildChainFreezerSnapshotPass(store, nil, ChainFreezerSnapshotConfig{Dir: filepath.Join(root, "snapshot")})
	if err == nil || !strings.Contains(err.Error(), "exceeds verified cold coverage") {
		t.Fatalf("BuildChainFreezerSnapshotPass error = %v, want uncovered local-tail rejection", err)
	}
}

func TestBuildChainFreezerSnapshotPassBuildsIndexedEventLogs(t *testing.T) {
	root := t.TempDir()
	store := openChainFreezerTestStore(t, filepath.Join(root, "ancient"))
	defer store.Close()

	block0 := canonicalBoundaryTestBlock(t, 0)
	block1, _, txInfosRaw := chainFreezerBlockWithTx(t, 1)
	var ret corepb.TransactionRet
	if err := proto.Unmarshal(txInfosRaw, &ret); err != nil {
		t.Fatalf("unmarshal transaction infos: %v", err)
	}
	address := []byte{common.AddressPrefixMainnet, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f, 0x40, 0x41, 0x42, 0x43, 0x44}
	topic := common.Hash{0xaa}
	ret.Transactioninfo[0].Log = []*corepb.TransactionInfo_Log{{
		Address: address,
		Topics:  [][]byte{topic[:]},
		Data:    []byte{0x01, 0x02},
	}}
	txInfosRaw, err := proto.Marshal(&ret)
	if err != nil {
		t.Fatalf("marshal transaction infos: %v", err)
	}
	appendChainFreezerRawRows(t, store, []chainFreezerRawTestRow{
		{block: block0},
		{block: block1, txInfosRaw: txInfosRaw},
	})

	disk := rawdb.NewMemoryDatabase()
	chain := rawdb.NewChainDB(disk, rawdb.NewFreezerReader(store))
	if err := rawdb.WriteBlock(chain, block0); err != nil {
		t.Fatalf("WriteBlock(0): %v", err)
	}
	if err := rawdb.WriteBlock(chain, block1); err != nil {
		t.Fatalf("WriteBlock(1): %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(chain, 1, ret.Transactioninfo); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock(1): %v", err)
	}

	snapshotDir := filepath.Join(root, "snapshot")
	result, err := BuildChainFreezerSnapshotPass(store, chain, ChainFreezerSnapshotConfig{
		Dir:            snapshotDir,
		BatchBlocks:    2,
		BuildEventLogs: true,
	})
	if err != nil {
		t.Fatalf("BuildChainFreezerSnapshotPass: %v", err)
	}
	if !result.Built || !result.EventLogBuilt || result.EventLogFromBlock != 1 || result.EventLogToBlock != 1 {
		t.Fatalf("result = %+v, want chain and indexed event-log build through block 1", result)
	}
	mgr, err := OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	if covered, err := mgr.EventLogIndexedRangeCovered(1, 1); err != nil || !covered {
		t.Fatalf("EventLogIndexedRangeCovered(1,1) = %v/%v, want true/nil", covered, err)
	}
	var rows []rawdb.EventLog
	if err := mgr.IterateEventLogs(1, 1, rawdb.EventLogFilter{}, func(row rawdb.EventLog) (bool, error) {
		rows = append(rows, row)
		return true, nil
	}); err != nil {
		t.Fatalf("IterateEventLogs: %v", err)
	}
	if len(rows) != 1 || rows[0].BlockNum != 1 || string(rows[0].Log.GetData()) != string([]byte{0x01, 0x02}) {
		t.Fatalf("event rows = %+v, want one archived block-1 log", rows)
	}
	if stage, ok, err := rawdb.ReadStageProgressRow(chain, rawdb.StageSnapshotEventLogBuild); err != nil || !ok || !stage.HasBlockHash || stage.BlockNum != 1 || stage.BlockHash != block1.Hash() {
		t.Fatalf("event-log stage = %+v ok=%v err=%v, want hash-bound block 1", stage, ok, err)
	}
}
