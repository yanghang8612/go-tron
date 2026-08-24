package net

import (
	"testing"
	"time"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	syncdl "github.com/tronprotocol/go-tron/net/sync/downloader"
)

func closedLookaheadDone() chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}

func TestSignatureLookaheadTakeRequiresFreshBatchIdentity(t *testing.T) {
	var firstHash, secondHash tcommon.Hash
	firstHash[0] = 1
	secondHash[0] = 2
	block := new(types.Block)
	aheadBatch := syncdl.BufferedBatch{
		Buffered: []syncdl.BufferedBlock{{Num: 10, Hash: firstHash, Raw: []byte{1, 2, 3}}},
		Blocks:   []*types.Block{block},
	}
	ahead := &syncSignatureLookahead{
		batch:          aheadBatch,
		done:           closedLookaheadDone(),
		selected:       true,
		decode:         syncdl.BufferedBatchDecodeResult{Action: syncdl.BufferedBatchDecodeImport},
		prewarmStarted: time.Now(),
	}

	selected := syncdl.BufferedBatch{Buffered: []syncdl.BufferedBlock{{Num: 10, Hash: firstHash, Raw: []byte{9, 9, 9}}}}
	got, decode, prewarm, started, ok := ahead.take(selected)
	if !ok || len(got.Blocks) != 1 || got.Blocks[0] != block {
		t.Fatalf("matching lookahead not reused: ok=%v blocks=%d", ok, len(got.Blocks))
	}
	if decode.Action != syncdl.BufferedBatchDecodeImport || prewarm != nil || started.IsZero() {
		t.Fatalf("matching lookahead metadata lost: decode=%v prewarm=%v started=%v", decode.Action, prewarm, started)
	}

	mismatch := &syncSignatureLookahead{
		batch:    aheadBatch,
		done:     closedLookaheadDone(),
		selected: true,
	}
	changed := syncdl.BufferedBatch{Buffered: []syncdl.BufferedBlock{{Num: 10, Hash: secondHash, Raw: []byte{1, 2, 3}}}}
	if _, _, _, _, ok := mismatch.take(changed); ok {
		t.Fatal("lookahead reused after buffered ownership changed")
	}
}

func TestSignatureLookaheadUnselectedIsNoop(t *testing.T) {
	ahead := &syncSignatureLookahead{done: closedLookaheadDone()}
	if _, _, _, _, ok := ahead.take(syncdl.BufferedBatch{}); ok {
		t.Fatal("unselected lookahead was reused")
	}
	ahead.discard()
}
