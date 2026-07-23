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
// directly. Raw bytes remain authoritative even when the bounded decoded fast
// path also retains the receive object.
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

// TestHandleBlockBuffersRawBytesAndBoundedDecodedFastPath pins that raw bytes
// remain authoritative while a near-tip entry may reuse the receive-path block.
// Block #2 stays buffered behind a gap at #1 and can be inspected.
func TestHandleBlockBuffersRawBytesAndBoundedDecodedFastPath(t *testing.T) {
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
	if &buf.raw[0] != &raw[0] {
		t.Fatal("sync buffer copied an exclusively owned wire payload")
	}
	if buf.decoded != blk {
		t.Fatal("near-tip buffered entry did not retain the receive-path block")
	}
	if buf.hash != blk.Hash() || buf.num != 2 {
		t.Fatalf("buffered metadata wrong: hash=%s num=%d", buf.hash, buf.num)
	}
}

func TestDecodedFastPathIsBoundedReusedAndReleased(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	peer, closePeer := testPeer(t, "decoded-cap")
	defer closePeer()

	count := maxRetainedDecodedBlocks + 5
	originals := make(map[uint64]*types.Block, count)
	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ps, _ := ss.addPeerStateLocked(peer)
	ss.mu.Unlock()

	// Leave #1 missing so every #2.. block remains buffered while admission
	// exercises the decoded-object cap.
	for i := 0; i < count; i++ {
		num := int64(i + 2)
		blk := blockWithTxs(num, tcommon.Hash{0xab}, 1)
		originals[uint64(num)] = blk
		ss.mu.Lock()
		markPendingLocked(ss, ps, blk.ID())
		ss.mu.Unlock()
		if !ss.HandleBlock(peer, blk, rawOf(t, blk)) {
			t.Fatalf("HandleBlock(%d) should consume the expected block", num)
		}
	}
	ss.waitForDrain()

	ss.mu.Lock()
	retained := ss.retainedDecodedBlocks
	retainedBytes := ss.retainedDecodedBytes
	decodedEntries := 0
	for _, buffered := range ss.blockBuffer {
		if buffered.decoded != nil {
			decodedEntries++
		}
	}
	ss.mu.Unlock()
	if retained != maxRetainedDecodedBlocks || decodedEntries != maxRetainedDecodedBlocks {
		t.Fatalf("decoded cap mismatch: charged=%d entries=%d want=%d",
			retained, decodedEntries, maxRetainedDecodedBlocks)
	}
	if retainedBytes <= 0 || retainedBytes > maxRetainedDecodedBytes {
		t.Fatalf("retained decoded bytes=%d outside (0,%d]", retainedBytes, maxRetainedDecodedBytes)
	}

	ss.mu.Lock()
	ss.syncedTipNum = 1
	batch := ss.popBufferedSyncBatchLocked(time.Now())
	chargedWhileActive := ss.retainedDecodedBlocks
	ss.mu.Unlock()
	if chargedWhileActive != maxRetainedDecodedBlocks {
		t.Fatalf("active batch lost decoded charge: got=%d want=%d",
			chargedWhileActive, maxRetainedDecodedBlocks)
	}
	ss.decodeBatchBlocks(&batch)
	if len(batch.blocks) != count {
		t.Fatalf("decoded batch length=%d, want=%d", len(batch.blocks), count)
	}
	for i, block := range batch.blocks {
		num := uint64(i + 2)
		if i < maxRetainedDecodedBlocks && block != originals[num] {
			t.Fatalf("block #%d did not reuse retained receive object", num)
		}
		if i >= maxRetainedDecodedBlocks && block == originals[num] {
			t.Fatalf("block #%d bypassed raw decode beyond retention cap", num)
		}
	}

	ss.releaseDecodedBatch(&batch)
	ss.mu.Lock()
	remainingBlocks := ss.retainedDecodedBlocks
	remainingBytes := ss.retainedDecodedBytes
	ss.mu.Unlock()
	if remainingBlocks != 0 || remainingBytes != 0 {
		t.Fatalf("decoded charge not released: blocks=%d bytes=%d", remainingBlocks, remainingBytes)
	}

	probe := originals[2]
	ss.mu.Lock()
	oversizeRetained := ss.retainDecodedBlockLocked(probe, 2, 0, maxRetainedDecodedBytes+1)
	farRetained := ss.retainDecodedBlockLocked(probe, alwaysFetchRunaheadBlocks+1, 0, 1)
	finalBlocks := ss.retainedDecodedBlocks
	finalBytes := ss.retainedDecodedBytes
	ss.mu.Unlock()
	if oversizeRetained || farRetained || finalBlocks != 0 || finalBytes != 0 {
		t.Fatalf("decoded guards admitted unsafe block: oversize=%v far=%v blocks=%d bytes=%d",
			oversizeRetained, farRetained, finalBlocks, finalBytes)
	}
}

func BenchmarkDecodeBatchBlocks(b *testing.B) {
	const batchSize = maxRetainedDecodedBlocks
	blk := blockWithTxs(1, tcommon.Hash{0xab}, 200)
	raw, err := proto.Marshal(blk.Proto())
	if err != nil {
		b.Fatal(err)
	}
	ss := &SyncService{}

	for _, tc := range []struct {
		name     string
		retained bool
	}{
		{name: "raw"},
		{name: "retained", retained: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			buffered := make([]bufferedSyncBlock, batchSize)
			for i := range buffered {
				buffered[i].raw = raw
				if tc.retained {
					buffered[i].decoded = blk
				}
			}
			b.SetBytes(int64(batchSize * len(raw)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				batch := bufferedSyncBatch{buffered: buffered}
				ss.decodeBatchBlocks(&batch)
				if len(batch.blocks) != batchSize {
					b.Fatalf("decoded %d blocks, want %d", len(batch.blocks), batchSize)
				}
			}
		})
	}
}
