package net

import (
	"errors"
	"testing"
	"time"

	syncdl "github.com/tronprotocol/go-tron/net/sync/downloader"
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
	for _, block := range []syncdl.BufferedBlock{
		{Raw: rawOf(t, block1), Hash: block1.Hash(), Num: 1},
		{Raw: rawOf(t, block2), Hash: block2.Hash(), Num: 2},
		{Raw: rawOf(t, block3), Hash: block3.Hash(), Num: 3},
	} {
		ss.blockBuffer[block.Num] = block
		ss.bufferedHash[block.Hash] = struct{}{}
	}
	batch := ss.popBufferedSyncBatchLocked(time.Now())
	_, keptAboveStop := ss.blockBuffer[3]
	ss.mu.Unlock()

	if len(batch.Buffered) != 2 || batch.Buffered[1].Num != 2 {
		t.Fatalf("popped blocks = %+v, want contiguous heights 1..2", batch.Buffered)
	}
	if !keptAboveStop {
		t.Fatal("block above stop height must not be popped for import")
	}
}
