package pruning

import (
	"bytes"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
	coretypes "github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestSnapshotLifecycleBuildsVisibleHistoryBeforePruningHotRows(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	writeSnapPruningChange(t, db, 1, 10, 12)

	chain := &fakePruneChain{db: db, solidified: 2}
	lifecycle := NewSnapshotLifecycle(chain, SnapshotLifecycleConfig{
		Snapshot: snapshots.Config{
			Dir:           dir,
			Enabled:       true,
			Interval:      time.Hour,
			HistoryWindow: 1,
		},
		Pruner: PrunerConfig{
			Policy:      SnapPolicy(1, 1),
			Interval:    time.Hour,
			SnapshotDir: dir,
		},
	})

	result, err := lifecycle.OnePass()
	if err != nil {
		t.Fatalf("lifecycle pass: %v", err)
	}
	if !result.Snapshot.Built || result.Snapshot.FromTxNum != 1 || result.Snapshot.ToTxNum != 12 {
		t.Fatalf("snapshot result = %+v, want visible history [1,12]", result.Snapshot)
	}
	if result.Prune.DeletedDomainChangeBlocks != 1 || result.Prune.DeletedTxRanges != 0 {
		t.Fatalf("prune result = %+v, want one covered hot change block pruned and tx range retained", result.Prune)
	}
	if _, ok, err := rawdb.ReadStateDomainChange(db, 1, 1); err != nil || ok {
		t.Fatalf("hot domain change survived ok=%v err=%v", ok, err)
	}
	if _, ok, err := rawdb.ReadStateTxRange(db, 1); err != nil || !ok {
		t.Fatalf("state tx range should remain hot in snap mode ok=%v err=%v", ok, err)
	}
	manifest, err := snapshots.LoadManifest(dir)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if manifest.Progress == nil || manifest.Progress.HistoryBuildTxNum != 12 || manifest.Progress.HotPruneTxNum != 12 {
		t.Fatalf("manifest progress = %+v, want history/hot-prune at 12", manifest.Progress)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotHistory); err != nil || !ok || got != 12 {
		t.Fatalf("snapshot history stage = %d ok=%v err=%v, want 12", got, ok, err)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotHotPrune); err != nil || !ok || got != 12 {
		t.Fatalf("snapshot hot-prune stage = %d ok=%v err=%v, want 12", got, ok, err)
	}
}

func TestSnapshotLifecycleBuildsEventLogsBeforePruningHotRows(t *testing.T) {
	db := rawdb.NewMemoryChainDB()
	dir := t.TempDir()
	writeSnapPruningChange(t, db, 1, 10, 12)
	addr := []byte{0x41, 0x21, 0x22, 0x23, 0x24}
	topic := common.Hash{0xbb}
	block, infos := lifecycleEventLogBlock(t, 1, []*corepb.TransactionInfo_Log{
		{Address: addr, Topics: [][]byte{topic[:]}, Data: []byte{0x01}},
	})
	if err := rawdb.WriteBlock(db, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(db, 1, infos); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}

	chain := &fakePruneChain{db: db, solidified: 2}
	lifecycle := NewSnapshotLifecycle(chain, SnapshotLifecycleConfig{
		Snapshot: snapshots.Config{
			Dir:            dir,
			Enabled:        true,
			Interval:       time.Hour,
			HistoryWindow:  1,
			BuildEventLogs: true,
		},
		Pruner: PrunerConfig{
			Policy:      SnapPolicy(1, 1),
			Interval:    time.Hour,
			SnapshotDir: dir,
		},
	})

	result, err := lifecycle.OnePass()
	if err != nil {
		t.Fatalf("lifecycle pass: %v", err)
	}
	if !result.Snapshot.EventLogBuilt || result.Snapshot.FromBlock != 1 || result.Snapshot.ToBlock != 1 {
		t.Fatalf("snapshot result = %+v, want event-log build over block 1", result.Snapshot)
	}
	if _, ok, err := rawdb.ReadStateDomainChange(db, 1, 1); err != nil || ok {
		t.Fatalf("hot domain change survived ok=%v err=%v", ok, err)
	}
	mgr, err := snapshots.OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	covered, err := mgr.EventLogRangeCovered(1, 1)
	if err != nil || !covered {
		t.Fatalf("EventLogRangeCovered = %v/%v, want true/nil", covered, err)
	}
	var rows []rawdb.EventLog
	if err := mgr.IterateEventLogs(1, 1, rawdb.EventLogFilter{
		Addresses: []common.Address{common.BytesToAddress(addr)},
		Topics:    [][]common.Hash{{topic}},
	}, func(row rawdb.EventLog) (bool, error) {
		rows = append(rows, row)
		return true, nil
	}); err != nil {
		t.Fatalf("IterateEventLogs: %v", err)
	}
	if len(rows) != 1 || rows[0].BlockNum != 1 || !bytes.Equal(rows[0].Log.GetData(), []byte{0x01}) {
		t.Fatalf("event rows = %+v, want one cold event log", rows)
	}
}

func TestSnapshotLifecycleRunsChainLookupPruneAfterHotPrune(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	writeSnapPruningChange(t, db, 1, 10, 12)
	chain := &fakePruneChain{db: db, solidified: 5}
	sawHotPruneStage := false
	chainLookupRan := false
	sawChainLookupBeforeSectionBloom := false
	sectionBloomRan := false
	sawSectionBloomBeforeBalanceTrace := false
	balanceTraceRan := false
	sawBalanceTraceBeforeRetiredPrune := false
	lifecycle := NewSnapshotLifecycle(chain, SnapshotLifecycleConfig{
		Pruner: PrunerConfig{
			Policy:    FullPolicy(2, 1),
			Interval:  time.Hour,
			BatchSize: 10,
		},
		ChainLookupPrune: func() (*snapshots.PruneHotChainLookupResult, error) {
			got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotPrune)
			if err != nil {
				return nil, err
			}
			sawHotPruneStage = ok && got == 5
			chainLookupRan = true
			return &snapshots.PruneHotChainLookupResult{
				HasRange:          true,
				FromBlock:         0,
				ToBlock:           1,
				ColdIndexSegments: 1,
			}, nil
		},
		SectionBloomPrune: func() (*snapshots.PruneHotSectionBloomResult, error) {
			sawChainLookupBeforeSectionBloom = chainLookupRan
			sectionBloomRan = true
			return &snapshots.PruneHotSectionBloomResult{
				HasRange:          true,
				FromSection:       0,
				ToSection:         1,
				ColdBloomSegments: 1,
				RowsDeleted:       2,
			}, nil
		},
		BalanceTracePrune: func() (*snapshots.PruneHotBalanceTraceResult, error) {
			sawSectionBloomBeforeBalanceTrace = sectionBloomRan
			balanceTraceRan = true
			return &snapshots.PruneHotBalanceTraceResult{
				HasRange:             true,
				FromBlock:            10,
				ToBlock:              12,
				ColdTraceSegments:    1,
				BlockTracesDeleted:   2,
				AccountTracesDeleted: 2,
			}, nil
		},
		RetiredPrune: func() (*snapshots.PruneRetiredSegmentFilesResult, error) {
			sawBalanceTraceBeforeRetiredPrune = balanceTraceRan
			return &snapshots.PruneRetiredSegmentFilesResult{
				RetiredSegments: 3,
				FilesDeleted:    2,
				BytesDeleted:    100,
			}, nil
		},
	})

	result, err := lifecycle.OnePass()
	if err != nil {
		t.Fatalf("lifecycle pass: %v", err)
	}
	if !sawHotPruneStage {
		t.Fatal("chain lookup prune hook ran before state hot-prune stage advanced")
	}
	if result.ChainLookupPrune == nil || !result.ChainLookupPrune.HasRange || result.ChainLookupPrune.ToBlock != 1 {
		t.Fatalf("chain lookup prune result = %+v, want hook result", result.ChainLookupPrune)
	}
	if !sawChainLookupBeforeSectionBloom {
		t.Fatal("section bloom prune hook ran before chain lookup prune hook")
	}
	if result.SectionBloomPrune == nil || !result.SectionBloomPrune.HasRange || result.SectionBloomPrune.RowsDeleted != 2 {
		t.Fatalf("section bloom prune result = %+v, want hook result", result.SectionBloomPrune)
	}
	if !sawSectionBloomBeforeBalanceTrace {
		t.Fatal("balance trace prune hook ran before section bloom prune hook")
	}
	if result.BalanceTracePrune == nil || !result.BalanceTracePrune.HasRange || result.BalanceTracePrune.ToBlock != 12 {
		t.Fatalf("balance trace prune result = %+v, want hook result", result.BalanceTracePrune)
	}
	if !sawBalanceTraceBeforeRetiredPrune {
		t.Fatal("retired segment prune hook ran before balance trace prune hook")
	}
	if result.RetiredPrune == nil || result.RetiredPrune.FilesDeleted != 2 || result.RetiredPrune.BytesDeleted != 100 {
		t.Fatalf("retired segment prune result = %+v, want hook result", result.RetiredPrune)
	}
}

func lifecycleEventLogBlock(t *testing.T, number uint64, logs []*corepb.TransactionInfo_Log) (*coretypes.Block, []*corepb.TransactionInfo) {
	t.Helper()
	txPB := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Timestamp:  int64(10_000 + number),
			Expiration: int64(20_000 + number),
			Data:       []byte{byte(number)},
		},
	}
	tx := coretypes.NewTransactionFromPB(txPB)
	info := &corepb.TransactionInfo{
		Id:             append([]byte(nil), tx.Hash().Bytes()...),
		BlockNumber:    int64(number),
		BlockTimeStamp: int64(30_000 + number),
		Log:            logs,
	}
	block := coretypes.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    int64(number),
				Timestamp: int64(30_000 + number),
			},
		},
		Transactions: []*corepb.Transaction{txPB},
	})
	return block, []*corepb.TransactionInfo{info}
}
