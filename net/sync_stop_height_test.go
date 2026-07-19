package net

import (
	"errors"
	"testing"
	"time"
)

func TestSyncStopHeightPausesAtExistingHead(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	ss.SetStopAtHeight(0)
	paused, atNum, _, err := ss.PausedStatus()
	if !paused || atNum != 0 || !errors.Is(err, ErrSyncStopHeightReached) {
		t.Fatalf("PausedStatus = (%v, %d, %v), want planned pause at genesis", paused, atNum, err)
	}
}

func TestSyncStopHeightCapsBufferedBatch(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)
	ss.SetStopAtHeight(2)

	parent := bc.CurrentBlock().Hash()
	block1 := blockWithTxs(1, parent, 0)
	block2 := blockWithTxs(2, block1.Hash(), 0)
	block3 := blockWithTxs(3, block2.Hash(), 0)

	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	for _, block := range []bufferedSyncBlock{
		{raw: rawOf(t, block1), hash: block1.Hash(), num: 1},
		{raw: rawOf(t, block2), hash: block2.Hash(), num: 2},
		{raw: rawOf(t, block3), hash: block3.Hash(), num: 3},
	} {
		ss.blockBuffer[block.num] = block
		ss.bufferedHash[block.hash] = struct{}{}
	}
	batch := ss.popBufferedSyncBatchLocked(time.Now())
	_, keptAboveStop := ss.blockBuffer[3]
	ss.mu.Unlock()

	if len(batch.buffered) != 2 || batch.buffered[1].num != 2 {
		t.Fatalf("popped blocks = %+v, want contiguous heights 1..2", batch.buffered)
	}
	if !keptAboveStop {
		t.Fatal("block above stop height must not be popped for import")
	}
}
