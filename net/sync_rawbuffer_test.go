package net

import (
	"testing"
	"time"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// rawOf marshals a block to its wire bytes for tests that seed the sync buffer
// directly (the buffer now stores raw bytes, not the decoded block).
func rawOf(t *testing.T, b *types.Block) []byte {
	t.Helper()
	raw, err := proto.Marshal(b.Proto())
	if err != nil {
		t.Fatalf("marshal block #%d: %v", b.Number(), err)
	}
	return raw
}

// blockWithTxs builds a block carrying `ntx` transactions so round-trip tests
// can assert the transaction payload survives raw buffering.
func blockWithTxs(num int64, parent tcommon.Hash, ntx int) *types.Block {
	txs := make([]*corepb.Transaction, ntx)
	for i := range txs {
		txs[i] = &corepb.Transaction{Signature: [][]byte{{byte(num), byte(i)}}}
	}
	return types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData:          &corepb.BlockHeaderRaw{Number: num, Timestamp: num * 3000, ParentHash: parent[:]},
			WitnessSignature: make([]byte, 65),
		},
		Transactions: txs,
	})
}

// TestPopDecodesRawBufferedBlock pins fix #1: the sync buffer holds the raw
// wire bytes (one []byte per block, no inner pointers) instead of the fully
// decoded *types.Block (≈161 M pointer-rich proto objects on the Nile node —
// the GC-storm driver). popBufferedSyncBatchLocked must decode the raw bytes
// back into a block whose hash, number and transactions are faithful to what
// was received.
func TestPopDecodesRawBufferedBlock(t *testing.T) {
	bc := makeTestChain(t) // head = genesis (#0); next = #1
	ss := NewSyncService(bc, nil)

	parent := bc.CurrentBlock().Hash()
	blk := blockWithTxs(1, parent, 3)
	raw, err := proto.Marshal(blk.Proto())
	if err != nil {
		t.Fatal(err)
	}

	ss.mu.Lock()
	ss.ensureSessionMapsLocked()
	ss.blockBuffer[1] = bufferedSyncBlock{raw: raw, num: 1, hash: blk.Hash()}
	ss.bufferedHash[blk.Hash()] = struct{}{}
	batch := ss.popBufferedSyncBatchLocked(time.Now())
	ss.mu.Unlock()

	// pop only moves raw entries (cheap, under lock); decode runs off-lock.
	if len(batch.buffered) != 1 {
		t.Fatalf("expected 1 popped raw entry, got %d", len(batch.buffered))
	}
	ss.decodeBatchBlocks(&batch)
	if len(batch.blocks) != 1 {
		t.Fatalf("expected 1 decoded block, got %d", len(batch.blocks))
	}
	got := batch.blocks[0]
	if got == nil {
		t.Fatal("popped block is nil — raw bytes were not decoded")
	}
	if got.Hash() != blk.Hash() {
		t.Fatalf("hash mismatch after raw round-trip: got %s want %s", got.Hash(), blk.Hash())
	}
	if got.Number() != 1 {
		t.Fatalf("number mismatch: got %d want 1", got.Number())
	}
	if n := len(got.Transactions()); n != 3 {
		t.Fatalf("transactions lost in raw round-trip: got %d want 3", n)
	}
}

// TestHandleBlockBuffersRawBytes pins that the receive path stores the raw
// wire bytes plus light metadata rather than retaining the decoded block, so a
// buffered entry carries no decoded proto tree. Block #2 is delivered while the
// head is at genesis, so it stays buffered (gap at #1) and can be inspected.
func TestHandleBlockBuffersRawBytes(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	peer, closePeer := testPeer(t, "raw-store")
	defer closePeer()

	blk := blockWithTxs(2, tcommon.Hash{0xab}, 2)
	raw, err := proto.Marshal(blk.Proto())
	if err != nil {
		t.Fatal(err)
	}

	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ps, _ := ss.addPeerStateLocked(peer)
	markPendingLocked(ss, ps, blk.ID())
	ss.mu.Unlock()

	if !ss.HandleBlock(peer, blk, raw) {
		t.Fatal("HandleBlock should consume the expected sync block")
	}

	ss.mu.Lock()
	buf, ok := ss.blockBuffer[2]
	ss.mu.Unlock()
	if !ok {
		t.Fatal("block #2 was not buffered")
	}
	if len(buf.raw) == 0 {
		t.Fatal("buffered entry holds no raw bytes")
	}
	if buf.hash != blk.Hash() || buf.num != 2 {
		t.Fatalf("buffered metadata wrong: hash=%s num=%d", buf.hash, buf.num)
	}
}
