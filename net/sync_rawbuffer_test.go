package net

import (
	"testing"
	"time"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	syncdl "github.com/tronprotocol/go-tron/net/sync/downloader"
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
	ss.blockBuffer[1] = syncdl.BufferedBlock{Raw: raw, Num: 1, Hash: blk.Hash()}
	ss.bufferedHash[blk.Hash()] = struct{}{}
	batch := ss.popBufferedSyncBatchLocked(time.Now())
	ss.mu.Unlock()

	// pop only moves raw entries (cheap, under lock); decode runs off-lock.
	if len(batch.Buffered) != 1 {
		t.Fatalf("expected 1 popped raw entry, got %d", len(batch.Buffered))
	}
	decoded := syncdl.DecodeBufferedBatch(&batch)
	if decoded.Action != syncdl.BufferedBatchDecodeImport {
		t.Fatalf("decode action = %v err=%v, want import", decoded.Action, decoded.Err)
	}
	if len(batch.Blocks) != 1 {
		t.Fatalf("expected 1 decoded block, got %d", len(batch.Blocks))
	}
	got := batch.Blocks[0]
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
	if len(buf.Raw) == 0 {
		t.Fatal("buffered entry holds no raw bytes")
	}
	if buf.Hash != blk.Hash() || buf.Num != 2 {
		t.Fatalf("buffered metadata wrong: hash=%s num=%d", buf.Hash, buf.Num)
	}
	staged, ok, err := rawdb.ReadSyncStagedBlockRaw(bc.DB(), 2)
	if err != nil || !ok {
		t.Fatalf("persistent staged block ok=%v err=%v", ok, err)
	}
	if !bytesEqual(staged.Raw, raw) {
		t.Fatal("persistent staged block did not preserve received raw bytes")
	}
}

func TestHandleRawBlockBuffersFromHeaderOnlyAdmission(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	peer, closePeer := testPeer(t, "raw-header-admission")
	defer closePeer()

	// Keep a gap at #1 so HandleRawBlock only admits and stages #2; the full
	// transaction decode remains deferred until the contiguous drain.
	blk := blockWithTxs(2, tcommon.Hash{0xab}, 64)
	raw, err := proto.Marshal(blk.Proto())
	if err != nil {
		t.Fatal(err)
	}

	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ps, _ := ss.addPeerStateLocked(peer)
	markPendingLocked(ss, ps, blk.ID())
	ss.mu.Unlock()

	if !ss.HandleRawBlock(peer, raw) {
		t.Fatal("HandleRawBlock should consume the expected sync block")
	}

	ss.mu.Lock()
	buf, ok := ss.blockBuffer[2]
	ss.mu.Unlock()
	if !ok || buf.Hash != blk.Hash() || buf.Num != blk.Number() || !bytesEqual(buf.Raw, raw) {
		t.Fatalf("raw-admitted buffer = %+v ok=%v, want original block metadata/body", buf, ok)
	}
	staged, ok, err := rawdb.ReadSyncStagedBlockRaw(bc.DB(), 2)
	if err != nil || !ok || staged.Hash != blk.Hash() || !bytesEqual(staged.Raw, raw) {
		t.Fatalf("raw-admitted staged row = %+v ok=%v err=%v", staged, ok, err)
	}
}

func TestDecodedBufferCommitIsolatesMalformedBodyWithoutSkippingSuffix(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)
	block1 := blockWithTxs(1, bc.CurrentBlock().Hash(), 1)
	block2 := blockWithTxs(2, block1.Hash(), 0)
	block3 := blockWithTxs(3, block2.Hash(), 1)
	raw1 := rawOf(t, block1)
	raw2 := append(rawOf(t, block2), 0x0a, 0x01, 0x80) // malformed nested Transaction
	raw3 := rawOf(t, block3)
	if id, err := types.BlockIDFromRaw(raw2); err != nil || id != block2.ID() {
		t.Fatalf("header-only malformed fixture id=%+v err=%v, want block2 id", id, err)
	}
	if _, err := types.UnmarshalBlock(raw2); err == nil {
		t.Fatal("malformed nested transaction unexpectedly fully decoded")
	}

	ss.mu.Lock()
	ss.ensureSessionMapsLocked()
	ss.syncing = true
	ss.targetHeadNum = 3
	for _, entry := range []syncdl.BufferedBlock{
		{Raw: raw1, Num: 1, Hash: block1.Hash()},
		{Raw: raw2, Num: 2, Hash: block2.Hash()},
		{Raw: raw3, Num: 3, Hash: block3.Hash()},
	} {
		id := types.BlockID{Num: entry.Num, Hash: entry.Hash}
		if result := rawdb.WriteSyncStagedBlockRawIDAndProgress(bc.DB(), id, entry.Raw); result.StageError != nil {
			ss.mu.Unlock()
			t.Fatalf("stage block %d: %v", entry.Num, result.StageError)
		}
		ss.blockBuffer[entry.Num] = entry
		ss.bufferedHash[entry.Hash] = struct{}{}
		ss.blockPath[entry.Num] = entry.Hash
		ss.bufferedBytes += int64(len(entry.Raw))
	}
	if err := rawdb.WriteStageProgressWithHash(bc.DB(), rawdb.StageSyncBodiesReady, block3.Number(), block3.Hash()); err != nil {
		ss.mu.Unlock()
		t.Fatalf("write ready progress: %v", err)
	}
	peek := ss.runStagedBodyDrainLocked(time.Now()).Batch
	ss.mu.Unlock()

	decoded := syncdl.DecodeBufferedBatch(&peek)
	if decoded.Err == nil || decoded.Dropped.Num != 2 || len(peek.Blocks) != 1 {
		t.Fatalf("decode = %+v prefix=%d, want block1 prefix and malformed block2", decoded, len(peek.Blocks))
	}
	if _, err := ss.commitDecodedBufferedBatch(&peek, decoded, time.Now()); err != nil {
		t.Fatalf("commit decoded prefix: %v", err)
	}

	ss.mu.Lock()
	_, has1 := ss.blockBuffer[1]
	_, has2 := ss.blockBuffer[2]
	_, has3 := ss.blockBuffer[3]
	next := ss.nextDrainBlockLocked()
	tip := ss.syncedTipNum
	retries := append([]types.BlockID(nil), ss.retryList...)
	ss.mu.Unlock()
	if has1 || has2 || !has3 || tip != 1 || next != 2 {
		t.Fatalf("buffer has1/2/3=%v/%v/%v tip=%d next=%d, want false/false/true tip1 next2", has1, has2, has3, tip, next)
	}
	if len(peek.Buffered) != 1 || len(peek.Blocks) != 1 || len(retries) != 1 || retries[0] != block2.ID() {
		t.Fatalf("committed prefix/retries = %d/%d/%+v, want block1 and retry block2", len(peek.Buffered), len(peek.Blocks), retries)
	}
	if _, ok, err := rawdb.ReadSyncStagedBlockRaw(bc.DB(), 2); err != nil || ok {
		t.Fatalf("malformed staged block2 ok=%v err=%v, want deleted", ok, err)
	}
	if row, ok, err := rawdb.ReadSyncStagedBlockRaw(bc.DB(), 3); err != nil || !ok || row.Hash != block3.Hash() {
		t.Fatalf("good suffix staged block3=%+v ok=%v err=%v, want retained", row, ok, err)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncBodiesReady); err != nil || !ok || row.BlockNum != 1 || row.BlockHash != block1.Hash() {
		t.Fatalf("ready progress=%+v ok=%v err=%v, want block1 frontier", row, ok, err)
	}
}

func TestRestoreStagedBodyBuffersRawBytes(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	blk := blockWithTxs(1, bc.CurrentBlock().Hash(), 2)
	raw, err := proto.Marshal(blk.Proto())
	if err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteSyncStagedBlockRaw(bc.DB(), blk, raw); err != nil {
		t.Fatalf("write raw staged block: %v", err)
	}

	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	buf, ok := ss.blockBuffer[1]
	bufferedBytes := ss.bufferedBytes
	ss.mu.Unlock()
	if !ok {
		t.Fatal("raw staged block was not restored into sync buffer")
	}
	if !bytesEqual(buf.Raw, raw) {
		t.Fatal("restored sync buffer did not preserve staged raw bytes")
	}
	if buf.Hash != blk.Hash() || buf.Num != 1 {
		t.Fatalf("restored metadata wrong: hash=%s num=%d", buf.Hash, buf.Num)
	}
	if bufferedBytes != int64(len(raw)) {
		t.Fatalf("restored bufferedBytes=%d, want %d", bufferedBytes, len(raw))
	}
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
