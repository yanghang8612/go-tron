package net

import (
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
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
		ss.blockBuffer[uint64(n)] = bufferedSyncBlock{raw: rawOf(t, blk), num: uint64(n), hash: blk.Hash()}
		ss.bufferedHash[blk.Hash()] = struct{}{}
		ss.blockPath[uint64(n)] = blk.Hash()
		prev = blk.Hash()
	}
	batch := ss.popBufferedSyncBatchLocked(time.Now())
	pathLen := len(ss.blockPath)
	bufLen := len(ss.blockBuffer)
	hashLen := len(ss.bufferedHash)
	ss.mu.Unlock()

	if len(batch.buffered) != 3 {
		t.Fatalf("expected 3 popped entries, got %d", len(batch.buffered))
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
		ss.blockBuffer[uint64(n)] = bufferedSyncBlock{raw: rawOf(t, blk), num: uint64(n), hash: blk.Hash()}
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

	if len(batch.buffered) != 1 {
		t.Fatalf("popped entries = %d, want 1 capped by SyncBodiesReady", len(batch.buffered))
	}
	if batch.buffered[0].num != 1 {
		t.Fatalf("popped block number = %d, want 1", batch.buffered[0].num)
	}
	if batch.buffered[0].hash != readyHash {
		t.Fatalf("popped hash = %x, want %x", batch.buffered[0].hash, readyHash)
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
