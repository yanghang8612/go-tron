package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/internal/jsonrpc"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestTronFilterReorgUsesOrphanReceiptsBeforeReplacement(t *testing.T) {
	witness := testInsertAddr(1)
	bc := newAsyncFlushChainOn(t, ethrawdb.NewMemoryDatabase(), witness)
	t.Cleanup(func() { _ = bc.Close() })
	backend := NewTronBackend(bc, nil)
	events := make(chan []jsonrpc.FilterBlockEvent, 4)
	backend.SubscribeFilterEvents(events, func() bool { return true })
	defer backend.UnsubscribeFilterEvents(events)

	tx := &corepb.Transaction{RawData: &corepb.TransactionRaw{Timestamp: 1}}
	blockAt := func(number int64, timestamp int64) *types.Block {
		return types.NewBlockFromPB(&corepb.Block{
			BlockHeader:  &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: number, Timestamp: timestamp}},
			Transactions: []*corepb.Transaction{tx},
		})
	}
	old := blockAt(1, 3000)
	new1, new2 := blockAt(1, 3001), blockAt(2, 6001)
	putReceipts := func(block *types.Block, data byte) {
		t.Helper()
		id := block.Transactions()[0].Hash()
		if err := rawdb.WriteTransactionInfosByBlock(bc.db, block.Number(), []*corepb.TransactionInfo{{
			Id: id.Bytes(), BlockNumber: int64(block.Number()),
			Log: []*corepb.TransactionInfo_Log{{Address: witness.Bytes(), Data: []byte{data}}},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	putReceipts(old, 1)
	finish := backend.prepareFilterReorg([]*types.Block{old})
	putReceipts(new1, 2)
	putReceipts(new2, 3)
	backend.notifyBlockSubs(new1)
	backend.notifyBlockSubs(new2)
	if len(events) != 0 {
		t.Fatal("reorg prefix escaped before commit")
	}
	finish([]*types.Block{new1, new2})
	batch := <-events
	for i, block := range []*types.Block{old, new1, new2} {
		if len(batch) != 3 || batch[i].Block.Hash() != block.Hash() || batch[i].Removed != (i == 0) ||
			len(batch[i].Logs) != 1 || batch[i].Logs[0].Removed != (i == 0) || batch[i].Logs[0].Data != []string{"0x01", "0x02", "0x03"}[i] {
			t.Fatalf("unexpected reorg batch at index %d: %+v", i, batch)
		}
	}
	finishFailed := backend.prepareFilterReorg([]*types.Block{new1})
	backend.notifyBlockSubs(old)
	finishFailed(nil)
	if len(events) != 0 {
		t.Fatal("failed switch published reorg notifications")
	}
}

func TestTronFilterEventsPublishSuccessfulForkInOrder(t *testing.T) {
	witness := testInsertAddr(1)
	bc := newAsyncFlushChainOn(t, ethrawdb.NewMemoryDatabase(), witness)
	t.Cleanup(func() { _ = bc.Close() })
	bc.SetAsyncCommit(true)
	backend := NewTronBackend(bc, nil)
	events := make(chan []jsonrpc.FilterBlockEvent, 4)
	backend.SubscribeFilterEvents(events, func() bool { return false })
	defer backend.UnsubscribeFilterEvents(events)

	old := chainFrom(bc.genesisBlock, witness, 1, 0)
	replacement := chainFrom(bc.genesisBlock, witness, 2, 1)
	if err := bc.InsertBlocks(old); err != nil {
		t.Fatal(err)
	}
	if err := bc.InsertBlocks(replacement); err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d event batches, want old head and one reorg batch", len(events))
	}
	if batch := <-events; len(batch) != 1 || batch[0].Block.Hash() != old[0].Hash() {
		t.Fatalf("unexpected old head: %+v", batch)
	}
	batch := <-events
	if len(batch) != len(replacement) {
		t.Fatalf("reorg batch length %d, want %d", len(batch), len(replacement))
	}
	for i, event := range batch {
		if event.Block.Hash() != replacement[i].Hash() || event.Removed {
			t.Fatalf("invalid replacement event %d: %+v", i, event)
		}
	}
}
