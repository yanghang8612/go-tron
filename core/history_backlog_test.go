package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func backlogTestChain(t *testing.T, head uint64) (*BlockChain, []*types.Block) {
	t.Helper()
	db := memorydb.New()
	t.Cleanup(func() { db.Close() })
	cfg := cloneMainnetChainConfig()
	cfg.HistoryEnabled = true
	bc := &BlockChain{db: db, chaindb: rawdb.NewChainDB(db, nil), config: cfg}
	blocks := make([]*types.Block, head+1)
	for i := range blocks {
		blocks[i] = types.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: int64(i), Timestamp: int64(i + 1)}}})
		if err := rawdb.WriteBlock(db, blocks[i]); err != nil {
			t.Fatal(err)
		}
	}
	bc.currentBlock.Store(blocks[head])
	dp := state.NewDynamicProperties()
	dp.SetLatestSolidifiedBlockNum(int64(head))
	bc.storeDynPropsCache(dp)
	return bc, blocks
}

func TestHistoryBacklogVerifiedDurableFrontiers(t *testing.T) {
	bc, blocks := backlogTestChain(t, 10)
	if err := rawdb.WriteStageProgressWithHash(bc.db, rawdb.StageFinish, 7, blocks[7].Hash()); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStageProgressWithHash(bc.db, rawdb.StageSnapshotBuild, 3, blocks[3].Hash()); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	s, err := bc.SampleHistoryBacklog(context.Background(), 1)
	if err != nil || s.Head != 10 || s.Finish != 7 || s.Eligible != 7 || s.Covered != 3 || s.Lag != 4 || s.SampledAt.Before(start) {
		t.Fatalf("sample=%+v err=%v", s, err)
	}
	s, err = bc.SampleHistoryBacklog(context.Background(), 8)
	if err != nil || s.Eligible != 2 || s.Lag != 0 {
		t.Fatalf("covered prefix: %+v %v", s, err)
	}
	s, err = bc.SampleHistoryBacklog(context.Background(), ^uint64(0))
	if err != nil || s.Eligible != 0 || s.Lag != 0 {
		t.Fatalf("large window: %+v %v", s, err)
	}
	// A replaced canonical hash invalidates an unchanged old cold watermark.
	if err := rawdb.WriteStageProgressWithHash(bc.db, rawdb.StageSnapshotBuild, 3, common.Hash{1}); err != nil {
		t.Fatal(err)
	}
	if s, err := bc.SampleHistoryBacklog(context.Background(), 1); err == nil || !s.SampledAt.IsZero() {
		t.Fatalf("accepted stale canonical row: %+v %v", s, err)
	}
}

func TestHistoryBacklogMissingAndMalformedProgress(t *testing.T) {
	bc, blocks := backlogTestChain(t, 10)
	if _, err := bc.SampleHistoryBacklog(context.Background(), 1); err == nil {
		t.Fatal("missing existing-chain Finish admitted")
	}
	if err := rawdb.WriteStageProgressWithHash(bc.db, rawdb.StageFinish, 7, blocks[7].Hash()); err != nil {
		t.Fatal(err)
	}
	s, err := bc.SampleHistoryBacklog(context.Background(), 1)
	if err != nil || s.Covered != 0 || s.Lag != 7 {
		t.Fatalf("missing cold row must count all eligible history: %+v %v", s, err)
	}
	if err := rawdb.WriteStageProgress(bc.db, rawdb.StageSnapshotBuild, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := bc.SampleHistoryBacklog(context.Background(), 1); err == nil {
		t.Fatal("accepted hashless cold row")
	}
	if err := rawdb.WriteStageProgressWithHash(bc.db, rawdb.StageSnapshotBuild, 8, blocks[8].Hash()); err != nil {
		t.Fatal(err)
	}
	if _, err := bc.SampleHistoryBacklog(context.Background(), 1); err == nil {
		t.Fatal("accepted cold ahead of Finish")
	}
}

func TestHistoryBacklogGenesisBusyCancellationAndMissingDP(t *testing.T) {
	bc, _ := backlogTestChain(t, 0)
	bc.dynPropsCache = nil
	if s, err := bc.SampleHistoryBacklog(context.Background(), 65536); err != nil || s.Lag != 0 {
		t.Fatalf("fresh genesis: %+v %v", s, err)
	}
	bc.chainmu.Lock()
	_, err := bc.SampleHistoryBacklog(context.Background(), 65536)
	bc.chainmu.Unlock()
	if !errors.Is(err, ErrHistoryBacklogBusy) {
		t.Fatalf("busy=%v", err)
	}
	bc.insertSessionGate.RLock()
	_, err = bc.SampleHistoryBacklog(context.Background(), 65536)
	bc.insertSessionGate.RUnlock()
	if !errors.Is(err, ErrHistoryBacklogBusy) {
		t.Fatalf("live session=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := bc.SampleHistoryBacklog(ctx, 65536); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled=%v", err)
	}
	bc, blocks := backlogTestChain(t, 1)
	if err := rawdb.WriteStageProgressWithHash(bc.db, rawdb.StageFinish, 1, blocks[1].Hash()); err != nil {
		t.Fatal(err)
	}
	bc.dynPropsCache = nil
	if _, err := bc.SampleHistoryBacklog(context.Background(), 0); err == nil {
		t.Fatal("missing DP cache fabricated zero lag")
	}
	bc.closed.Store(true)
	if _, err := bc.SampleHistoryBacklog(context.Background(), 0); err == nil {
		t.Fatal("closed chain admitted")
	}
}

type backlogAdvanceOnRead struct {
	ethdb.KeyValueStore
	advance func() error
}

func (d *backlogAdvanceOnRead) Get(key []byte) ([]byte, error) {
	v, err := d.KeyValueStore.Get(key)
	if err == nil && d.advance != nil {
		f := d.advance
		d.advance = nil
		err = f()
	}
	return v, err
}

func TestHistoryBacklogConcurrentColdPublication(t *testing.T) {
	bc, blocks := backlogTestChain(t, 10)
	db := bc.db
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageFinish, 7, blocks[7].Hash()); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSnapshotBuild, 3, blocks[3].Hash()); err != nil {
		t.Fatal(err)
	}
	bc.db = &backlogAdvanceOnRead{KeyValueStore: db, advance: func() error {
		if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageFinish, 9, blocks[9].Hash()); err != nil {
			return err
		}
		return rawdb.WriteStageProgressWithHash(db, rawdb.StageSnapshotBuild, 8, blocks[8].Hash())
	}}
	s, err := bc.SampleHistoryBacklog(context.Background(), 0)
	if err != nil || s.Finish != 9 || s.Covered != 3 || s.Lag != 6 {
		t.Fatalf("normal forward writes must give conservative observation: %+v %v", s, err)
	}
	s, err = bc.SampleHistoryBacklog(context.Background(), 0)
	if err != nil || s.Covered != 8 || s.Lag != 1 {
		t.Fatalf("fresh publication: %+v %v", s, err)
	}
}

func TestHistoryBacklogRealConstructorGenesis(t *testing.T) {
	db := memorydb.New()
	defer db.Close()
	cfg := cloneMainnetChainConfig()
	cfg.HistoryEnabled = true
	if _, _, err := SetupGenesisBlock(db, &params.Genesis{Config: cfg}); err != nil {
		t.Fatal(err)
	}
	bc, err := NewBlockChain(db, state.NewDatabase(rawdb.WrapKeyValueStore(db)), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer bc.Close()
	if s, err := bc.SampleHistoryBacklog(context.Background(), 65536); err != nil || s.Head != 0 || s.Lag != 0 {
		t.Fatalf("constructor genesis %+v %v", s, err)
	}
}

type backlogReadErrorStore struct {
	ethdb.KeyValueStore
	failure error
}

func (d backlogReadErrorStore) Get([]byte) ([]byte, error) { return nil, d.failure }

func TestHistoryBacklogStorageFailureIsNotEmpty(t *testing.T) {
	bc, blocks := backlogTestChain(t, 1)
	if err := rawdb.WriteStageProgressWithHash(bc.db, rawdb.StageFinish, 1, blocks[1].Hash()); err != nil {
		t.Fatal(err)
	}
	want := errors.New("test storage failure")
	bc.db = backlogReadErrorStore{bc.db, want}
	if s, err := bc.SampleHistoryBacklog(context.Background(), 0); !errors.Is(err, want) || !s.SampledAt.IsZero() {
		t.Fatalf("sample=%+v err=%v", s, err)
	}
}
