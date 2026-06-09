package net

import (
	"testing"
	"time"
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
