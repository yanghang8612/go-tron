package net

import (
	"testing"
	"time"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	syncdl "github.com/tronprotocol/go-tron/net/sync/downloader"
)

// TestPopBufferedBatchPrunesBlockPath pins the fix for the blockPath leak:
// reserveBlockPathLocked adds one entry per reserved block number but nothing
// ever removed them mid-session, so blockPath grew unbounded for the whole
// sync (≈782 MB live observed on the Nile node). Once a block is popped for
// insertion its path reservation is no longer needed — the canonical chain (or
// the sticky pause on failure) becomes the source of truth for that number — so
// popBufferedSyncBatchLocked must drop blockPath[next] alongside blockBuffer
// and bufferedHash.
func TestPopBufferedBatchPrunesBlockPath(t *testing.T) {
	bc := makeTestChain(t) // head = genesis (#0); next = #1
	ss := NewSyncService(bc, nil)

	ss.mu.Lock()
	ss.ensureSessionMapsLocked()
	parent := bc.CurrentBlock().Hash()
	prev := parent
	for n := int64(1); n <= 3; n++ {
		blk := stubBlock(n, prev)
		ss.blockBuffer[uint64(n)] = syncdl.BufferedBlock{Raw: rawOf(t, blk), Num: uint64(n), Hash: blk.Hash()}
		ss.bufferedHash[blk.Hash()] = struct{}{}
		ss.blockPath[uint64(n)] = blk.Hash()
		prev = blk.Hash()
	}
	batch := ss.popBufferedSyncBatchLocked(time.Now())
	pathLen := len(ss.blockPath)
	bufLen := len(ss.blockBuffer)
	hashLen := len(ss.bufferedHash)
	ss.mu.Unlock()

	if len(batch.Buffered) != 3 {
		t.Fatalf("expected 3 popped entries, got %d", len(batch.Buffered))
	}
	if bufLen != 0 {
		t.Fatalf("blockBuffer should be drained, still holds %d", bufLen)
	}
	if hashLen != 0 {
		t.Fatalf("bufferedHash should be drained, still holds %d", hashLen)
	}
	if pathLen != 0 {
		t.Fatalf("blockPath leaked: %d entries retained after the blocks were popped for insertion (want 0)", pathLen)
	}
}

func TestPopBufferedBatchHonorsSyncBodiesReadyFrontier(t *testing.T) {
	bc := makeTestChain(t) // head = genesis (#0); next = #1
	ss := NewSyncService(bc, nil)

	ss.mu.Lock()
	ss.ensureSessionMapsLocked()
	parent := bc.CurrentBlock().Hash()
	prev := parent
	readyHash := parent
	for n := int64(1); n <= 3; n++ {
		blk := stubBlock(n, prev)
		if err := rawdb.WriteSyncStagedBlock(bc.DB(), blk); err != nil {
			t.Fatalf("write staged block %d: %v", n, err)
		}
		ss.blockBuffer[uint64(n)] = syncdl.BufferedBlock{Raw: rawOf(t, blk), Num: uint64(n), Hash: blk.Hash()}
		ss.bufferedHash[blk.Hash()] = struct{}{}
		ss.blockPath[uint64(n)] = blk.Hash()
		if n == 1 {
			readyHash = blk.Hash()
			if err := rawdb.WriteStageProgressWithHash(bc.DB(), rawdb.StageSyncBodiesReady, blk.Number(), blk.Hash()); err != nil {
				t.Fatalf("write SyncBodiesReady: %v", err)
			}
		}
		prev = blk.Hash()
	}
	batch := ss.popBufferedSyncBatchLocked(time.Now())
	pathLen := len(ss.blockPath)
	bufLen := len(ss.blockBuffer)
	hashLen := len(ss.bufferedHash)
	ss.mu.Unlock()

	if len(batch.Buffered) != 1 {
		t.Fatalf("popped entries = %d, want 1 capped by SyncBodiesReady", len(batch.Buffered))
	}
	if batch.Buffered[0].Num != 1 {
		t.Fatalf("popped block number = %d, want 1", batch.Buffered[0].Num)
	}
	if batch.Buffered[0].Hash != readyHash {
		t.Fatalf("popped hash = %x, want %x", batch.Buffered[0].Hash, readyHash)
	}
	if bufLen != 2 {
		t.Fatalf("blockBuffer after capped pop = %d, want 2", bufLen)
	}
	if hashLen != 2 {
		t.Fatalf("bufferedHash after capped pop = %d, want 2", hashLen)
	}
	if pathLen != 2 {
		t.Fatalf("blockPath after capped pop = %d, want 2", pathLen)
	}
}

func TestPopBufferedBatchUsesImportChunkLimit(t *testing.T) {
	bc := makeTestChain(t) // head = genesis (#0); next = #1
	ss := NewSyncService(bc, nil)

	ss.mu.Lock()
	ss.ensureSessionMapsLocked()
	last := seedBufferedSyncRange(t, ss, bc.CurrentBlock().Hash(), 1, maxSyncImportBatch+2)
	batch := ss.popBufferedSyncBatchLocked(time.Now())
	pathLen := len(ss.blockPath)
	bufLen := len(ss.blockBuffer)
	hashLen := len(ss.bufferedHash)
	ss.mu.Unlock()

	if last == nil {
		t.Fatal("seeded range returned nil last block")
	}
	if len(batch.Buffered) != maxSyncImportBatch {
		t.Fatalf("popped entries = %d, want local import chunk %d", len(batch.Buffered), maxSyncImportBatch)
	}
	if got := batch.Buffered[len(batch.Buffered)-1].Num; got != uint64(maxSyncImportBatch) {
		t.Fatalf("last popped block = %d, want %d", got, maxSyncImportBatch)
	}
	if bufLen != 2 || hashLen != 2 || pathLen != 2 {
		t.Fatalf("remaining buffered/hash/path = %d/%d/%d, want 2/2/2", bufLen, hashLen, pathLen)
	}
}

func TestPopBufferedBatchUsesConfiguredImportChunkLimit(t *testing.T) {
	bc := makeTestChain(t) // head = genesis (#0); next = #1
	ss := NewSyncService(bc, nil)
	if err := ss.SetImportBatchSize(2); err != nil {
		t.Fatalf("SetImportBatchSize: %v", err)
	}

	ss.mu.Lock()
	ss.ensureSessionMapsLocked()
	seedBufferedSyncRange(t, ss, bc.CurrentBlock().Hash(), 1, 5)
	batch := ss.popBufferedSyncBatchLocked(time.Now())
	remaining := len(ss.blockBuffer)
	ss.mu.Unlock()

	if len(batch.Buffered) != 2 {
		t.Fatalf("popped entries = %d, want configured chunk 2", len(batch.Buffered))
	}
	if remaining != 3 {
		t.Fatalf("remaining buffered blocks = %d, want 3", remaining)
	}
}

func TestPopBufferedBatchCarriesBufferWait(t *testing.T) {
	bc := makeTestChain(t) // head = genesis (#0); next = #1
	ss := NewSyncService(bc, nil)

	start := time.Unix(1_700_000_000, 0)
	block := stubBlock(1, bc.CurrentBlock().Hash())

	ss.mu.Lock()
	ss.ensureSessionMapsLocked()
	ss.bufferWait.Begin(1, start)
	ss.blockBuffer[1] = syncdl.BufferedBlock{Raw: rawOf(t, block), Num: 1, Hash: block.Hash()}
	ss.bufferedHash[block.Hash()] = struct{}{}
	ss.blockPath[1] = block.Hash()
	batch := ss.popBufferedSyncBatchLocked(start.Add(250 * time.Millisecond))
	ss.mu.Unlock()

	if len(batch.BufferWaits) != 1 {
		t.Fatalf("buffer wait entries = %d, want 1", len(batch.BufferWaits))
	}
	if got := batch.BufferWaits[0]; got != 250*time.Millisecond {
		t.Fatalf("buffer wait = %v, want 250ms", got)
	}
}

func TestSetImportBatchSizeRejectsInvalidLimits(t *testing.T) {
	ss := NewSyncService(makeTestChain(t), nil)
	for _, size := range []int{0, -1, maxFetchBatch + 1} {
		if err := ss.SetImportBatchSize(size); err == nil {
			t.Fatalf("SetImportBatchSize(%d) succeeded, want error", size)
		}
	}
	if err := ss.SetImportBatchSize(maxFetchBatch); err != nil {
		t.Fatalf("SetImportBatchSize(maxFetchBatch): %v", err)
	}
}

func TestDrainBufferedBlocksImportsMultipleChunks(t *testing.T) {
	bc := makeTestChain(t) // head = genesis (#0); next = #1
	ss := NewSyncService(bc, nil)

	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	last := seedBufferedSyncRange(t, ss, bc.CurrentBlock().Hash(), 1, maxSyncImportBatch+3)
	ss.targetHeadNum = last.Number()
	ss.mu.Unlock()

	ss.drainBufferedBlocksOnce()

	if got := bc.CurrentBlock(); got == nil || got.Hash() != last.Hash() {
		t.Fatalf("head after multi-chunk drain = %v, want block %d %x", got, last.Number(), last.Hash())
	}
	if got := ss.stats.CurrentSnapshot().TotalBlocks; got != maxSyncImportBatch+3 {
		t.Fatalf("sync stats total blocks after multi-chunk drain = %d, want %d", got, maxSyncImportBatch+3)
	}
	assertSyncPipelineProgress(t, bc.DB(), last)
	if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncBodiesReady); err != nil || ok {
		t.Fatalf("sync bodies ready after multi-chunk drain = %+v ok=%v err=%v, want deleted", row, ok, err)
	}
}

func seedBufferedSyncRange(t *testing.T, ss *SyncService, parentHash tcommon.Hash, start, count int) *types.Block {
	t.Helper()
	prev := parentHash
	var last *types.Block
	for n := start; n < start+count; n++ {
		blk := stubBlock(int64(n), prev)
		raw := rawOf(t, blk)
		if ss.chain == nil || ss.chain.DB() == nil {
			t.Fatalf("seed buffered sync range requires chain database")
		}
		if err := rawdb.WriteSyncStagedBlockRaw(ss.chain.DB(), blk, raw); err != nil {
			t.Fatalf("write staged block %d: %v", n, err)
		}
		ss.blockBuffer[uint64(n)] = syncdl.BufferedBlock{Raw: raw, Num: uint64(n), Hash: blk.Hash()}
		ss.bufferedHash[blk.Hash()] = struct{}{}
		ss.blockPath[uint64(n)] = blk.Hash()
		prev = blk.Hash()
		last = blk
	}
	return last
}
