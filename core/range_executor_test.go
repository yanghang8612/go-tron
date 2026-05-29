package core

import (
	"bytes"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func testRangeBlock(number uint64, parent tcommon.Hash) *types.Block {
	return types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:     int64(number),
				Timestamp:  int64(number) * 3000,
				ParentHash: parent.Bytes(),
			},
		},
	})
}

func testRangeState(t *testing.T) (*state.StateDB, *state.CommitScope, *blockbuffer.Buffer, tcommon.Address) {
	t.Helper()
	disk := ethrawdb.NewMemoryDatabase()
	buf := blockbuffer.New(disk)
	sdb, err := state.New(tcommon.Hash{}, state.NewDatabase(disk))
	if err != nil {
		t.Fatal(err)
	}
	sdb.SetAccountKVIndexStore(buf)
	addr := testInsertAddr(0xee)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	return sdb, sdb.NewCommitScope(), buf, addr
}

func advanceToExecution(t *testing.T, pipeline *canonicalStagePipeline) {
	t.Helper()
	if err := pipeline.Advance(rawdb.StageHeaders, rawdb.StageBodies, rawdb.StageExecution); err != nil {
		t.Fatal(err)
	}
}

func TestCanonicalCommitStateDefersScopedLatestFlushUntilClose(t *testing.T) {
	sdb, scope, buf, addr := testRangeState(t)
	block := testRangeBlock(1, tcommon.Hash{})
	buf.BeginBlock(block.Hash(), block.Number())
	pipeline := newCanonicalStagePipeline(buf, block.Number(), block.Hash())
	advanceToExecution(t, pipeline)
	plan := &canonicalBlockExecution{
		state:    sdb,
		commit:   scope,
		pipeline: pipeline,
	}

	if err := sdb.SetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k"), []byte("v1")); err != nil {
		t.Fatalf("set kv: %v", err)
	}
	if _, err := plan.CommitState(buf, block, state.CommitOptions{}, false); err != nil {
		t.Fatalf("commit state: %v", err)
	}
	if _, ok, err := rawdb.ReadStateKVLatest(buf, addr, 0, kvdomains.SystemDynamicProperty, []byte("k")); err != nil || ok {
		t.Fatalf("buffer latest before range close ok=%v err=%v, want not visible", ok, err)
	}
	if _, ok, err := rawdb.ReadStateAccountLatest(buf, addr); err != nil || ok {
		t.Fatalf("buffer account latest before range close ok=%v err=%v, want not visible", ok, err)
	}
	buf.CommitBlock()

	if err := scope.Close(); err != nil {
		t.Fatalf("close scope: %v", err)
	}
	if got, ok, err := rawdb.ReadStateKVLatest(buf, addr, 0, kvdomains.SystemDynamicProperty, []byte("k")); err != nil || !ok || !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("buffer latest after range close = %q ok=%v err=%v, want v1", got, ok, err)
	}
	if _, ok, err := rawdb.ReadStateAccountLatest(buf, addr); err != nil || !ok {
		t.Fatalf("buffer account latest after range close ok=%v err=%v, want visible", ok, err)
	}
}

func TestCanonicalCommitStateFlushesLatestBeforeSolidifiedLayerDrop(t *testing.T) {
	sdb, scope, buf, addr := testRangeState(t)
	parent := tcommon.Hash{}
	blocks := []*types.Block{testRangeBlock(1, parent)}
	blocks = append(blocks, testRangeBlock(2, blocks[0].Hash()))
	var lastPlan *canonicalBlockExecution

	for i, block := range blocks {
		buf.BeginBlock(block.Hash(), block.Number())
		pipeline := newCanonicalStagePipeline(buf, block.Number(), block.Hash())
		advanceToExecution(t, pipeline)
		lastPlan = &canonicalBlockExecution{
			state:    sdb,
			commit:   scope,
			pipeline: pipeline,
		}
		key := []byte{byte('a' + i)}
		value := []byte{byte('1' + i)}
		if err := sdb.SetAccountKV(addr, kvdomains.SystemDynamicProperty, key, value); err != nil {
			t.Fatalf("set kv block %d: %v", block.Number(), err)
		}
		if _, err := lastPlan.CommitState(buf, block, state.CommitOptions{}, false); err != nil {
			t.Fatalf("commit state block %d: %v", block.Number(), err)
		}
		buf.CommitBlock()
	}

	if err := lastPlan.FlushLatestUpTo(1); err != nil {
		t.Fatalf("flush latest up to block 1: %v", err)
	}
	if got, ok, err := rawdb.ReadStateKVLatest(buf, addr, 0, kvdomains.SystemDynamicProperty, []byte("a")); err != nil || !ok || !bytes.Equal(got, []byte("1")) {
		t.Fatalf("block 1 latest after cutoff flush = %q ok=%v err=%v, want 1", got, ok, err)
	}
	if _, ok, err := rawdb.ReadStateAccountLatest(buf, addr); err != nil || !ok {
		t.Fatalf("block 1 account latest after cutoff flush ok=%v err=%v, want visible", ok, err)
	}
	if _, ok, err := rawdb.ReadStateKVLatest(buf, addr, 0, kvdomains.SystemDynamicProperty, []byte("b")); err != nil || ok {
		t.Fatalf("block 2 latest after cutoff flush ok=%v err=%v, want not visible", ok, err)
	}
	if err := scope.Close(); err != nil {
		t.Fatalf("close scope: %v", err)
	}
	if got, ok, err := rawdb.ReadStateKVLatest(buf, addr, 0, kvdomains.SystemDynamicProperty, []byte("b")); err != nil || !ok || !bytes.Equal(got, []byte("2")) {
		t.Fatalf("block 2 latest after close = %q ok=%v err=%v, want 2", got, ok, err)
	}
}

func TestCanonicalRangeAbortFlushesCommittedLatestAndDropsFailedActiveLayer(t *testing.T) {
	sdb, scope, buf, addr := testRangeState(t)
	block1 := testRangeBlock(1, tcommon.Hash{})
	buf.BeginBlock(block1.Hash(), block1.Number())
	pipeline1 := newCanonicalStagePipeline(buf, block1.Number(), block1.Hash())
	advanceToExecution(t, pipeline1)
	plan1 := &canonicalBlockExecution{state: sdb, commit: scope, pipeline: pipeline1}
	if err := sdb.SetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("committed"), []byte("v1")); err != nil {
		t.Fatalf("set committed kv: %v", err)
	}
	if _, err := plan1.CommitState(buf, block1, state.CommitOptions{}, false); err != nil {
		t.Fatalf("commit block 1: %v", err)
	}
	buf.CommitBlock()

	block2 := testRangeBlock(2, block1.Hash())
	buf.BeginBlock(block2.Hash(), block2.Number())
	pipeline2 := newCanonicalStagePipeline(buf, block2.Number(), block2.Hash())
	advanceToExecution(t, pipeline2)
	plan2 := &canonicalBlockExecution{state: sdb, commit: scope, pipeline: pipeline2}
	if err := sdb.SetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("failed"), []byte("v2")); err != nil {
		t.Fatalf("set failed kv: %v", err)
	}
	if _, err := plan2.CommitState(buf, block2, state.CommitOptions{}, false); err != nil {
		t.Fatalf("commit block 2: %v", err)
	}
	buf.DiscardActive()

	if err := scope.Abort(); err != nil {
		t.Fatalf("abort scope: %v", err)
	}
	if got, ok, err := rawdb.ReadStateKVLatest(buf, addr, 0, kvdomains.SystemDynamicProperty, []byte("committed")); err != nil || !ok || !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("committed latest after abort = %q ok=%v err=%v, want v1", got, ok, err)
	}
	if _, ok, err := rawdb.ReadStateKVLatest(buf, addr, 0, kvdomains.SystemDynamicProperty, []byte("failed")); err != nil || ok {
		t.Fatalf("failed active latest after abort ok=%v err=%v, want missing", ok, err)
	}
}

func TestCanonicalRangeAbortFlushesCommittedAccountLatestAndDropsFailedActiveLayer(t *testing.T) {
	disk := ethrawdb.NewMemoryDatabase()
	buf := blockbuffer.New(disk)
	sdb, err := state.New(tcommon.Hash{}, state.NewDatabase(disk))
	if err != nil {
		t.Fatal(err)
	}
	sdb.SetAccountKVIndexStore(buf)
	scope := sdb.NewCommitScope()
	committedAddr := testInsertAddr(0xe1)
	failedAddr := testInsertAddr(0xe2)

	block1 := testRangeBlock(1, tcommon.Hash{})
	buf.BeginBlock(block1.Hash(), block1.Number())
	pipeline1 := newCanonicalStagePipeline(buf, block1.Number(), block1.Hash())
	advanceToExecution(t, pipeline1)
	plan1 := &canonicalBlockExecution{state: sdb, commit: scope, pipeline: pipeline1}
	sdb.CreateAccount(committedAddr, corepb.AccountType_Normal)
	sdb.AddBalance(committedAddr, 11)
	if _, err := plan1.CommitState(buf, block1, state.CommitOptions{}, false); err != nil {
		t.Fatalf("commit block 1: %v", err)
	}
	buf.CommitBlock()

	block2 := testRangeBlock(2, block1.Hash())
	buf.BeginBlock(block2.Hash(), block2.Number())
	pipeline2 := newCanonicalStagePipeline(buf, block2.Number(), block2.Hash())
	advanceToExecution(t, pipeline2)
	plan2 := &canonicalBlockExecution{state: sdb, commit: scope, pipeline: pipeline2}
	sdb.CreateAccount(failedAddr, corepb.AccountType_Normal)
	sdb.AddBalance(failedAddr, 22)
	if _, err := plan2.CommitState(buf, block2, state.CommitOptions{}, false); err != nil {
		t.Fatalf("commit block 2: %v", err)
	}
	buf.DiscardActive()

	if err := scope.Abort(); err != nil {
		t.Fatalf("abort scope: %v", err)
	}
	if _, ok, err := rawdb.ReadStateAccountLatest(buf, committedAddr); err != nil || !ok {
		t.Fatalf("committed account latest after abort ok=%v err=%v, want visible", ok, err)
	}
	if _, ok, err := rawdb.ReadStateAccountLatest(buf, failedAddr); err != nil || ok {
		t.Fatalf("failed active account latest after abort ok=%v err=%v, want missing", ok, err)
	}
}
