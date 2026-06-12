package downloader

import (
	"bytes"
	"testing"
	"time"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestRawBlockBytesCopiesWirePayload(t *testing.T) {
	block := testBufferedBlock(1)
	raw, err := block.Marshal()
	if err != nil {
		t.Fatalf("marshal block: %v", err)
	}
	got := RawBlockBytes(block, raw)
	if !bytes.Equal(got, raw) {
		t.Fatalf("raw copy differs from source")
	}
	raw[0] ^= 0xff
	if bytes.Equal(got, raw) {
		t.Fatal("raw copy aliases source slice")
	}
}

func TestRawBlockBytesRemarshalsWhenWirePayloadMissing(t *testing.T) {
	block := testBufferedBlock(2)
	got := RawBlockBytes(block, nil)
	decoded, err := types.UnmarshalBlock(got)
	if err != nil {
		t.Fatalf("decode remarshal bytes: %v", err)
	}
	if decoded.Hash() != block.Hash() || decoded.Number() != block.Number() {
		t.Fatalf("decoded block = #%d %x, want #%d %x", decoded.Number(), decoded.Hash(), block.Number(), block.Hash())
	}
}

func TestBufferedBatchDecodeBlocksKeepsPrefixOnError(t *testing.T) {
	block1 := testBufferedBlock(1)
	block3 := testBufferedBlock(3)
	raw1, err := block1.Marshal()
	if err != nil {
		t.Fatalf("marshal block1: %v", err)
	}
	raw3, err := block3.Marshal()
	if err != nil {
		t.Fatalf("marshal block3: %v", err)
	}
	batch := BufferedBatch{Buffered: []BufferedBlock{
		{Raw: raw1, Hash: block1.Hash(), Num: block1.Number()},
		{Raw: []byte{0x01, 0x02}, Hash: tcommon.Hash{0xee}, Num: 2},
		{Raw: raw3, Hash: block3.Hash(), Num: block3.Number()},
	}}

	dropped, err := batch.DecodeBlocks()
	if err == nil {
		t.Fatal("DecodeBlocks succeeded, want decode error")
	}
	if dropped.Num != 2 || dropped.Hash != (tcommon.Hash{0xee}) {
		t.Fatalf("dropped = #%d %x, want #2 ee", dropped.Num, dropped.Hash)
	}
	if len(batch.Blocks) != 1 || batch.Blocks[0].Hash() != block1.Hash() {
		t.Fatalf("decoded prefix = %d blocks, want only block1", len(batch.Blocks))
	}
}

func TestStagedBodyDrainLimit(t *testing.T) {
	tests := []struct {
		name          string
		next          uint64
		max           int
		readyLimit    uint64
		hasReadyLimit bool
		wantLimit     int
		wantOK        bool
	}{
		{name: "no ready frontier uses max", next: 10, max: 32, wantLimit: 32, wantOK: true},
		{name: "ready frontier behind next stops drain", next: 10, max: 32, readyLimit: 9, hasReadyLimit: true},
		{name: "ready frontier clamps partial span", next: 10, max: 32, readyLimit: 12, hasReadyLimit: true, wantLimit: 3, wantOK: true},
		{name: "ready frontier beyond max keeps max", next: 10, max: 32, readyLimit: 99, hasReadyLimit: true, wantLimit: 32, wantOK: true},
		{name: "nonpositive max stops drain", next: 10, max: 0},
	}
	for _, tt := range tests {
		got, ok := StagedBodyDrainLimit(tt.next, tt.max, tt.readyLimit, tt.hasReadyLimit)
		if got != tt.wantLimit || ok != tt.wantOK {
			t.Fatalf("%s: limit=%d ok=%v, want %d %v", tt.name, got, ok, tt.wantLimit, tt.wantOK)
		}
	}
}

func TestPopBufferedBatchReleasesReservationsAndKeepsGap(t *testing.T) {
	now := time.Unix(100, 0)
	waitStart := now.Add(-3 * time.Second)
	var wait BufferWaitTracker
	wait.Begin(4, waitStart)

	h4 := tcommon.Hash{0x04}
	h5 := tcommon.Hash{0x05}
	h7 := tcommon.Hash{0x07}
	buffer := map[uint64]BufferedBlock{
		4: {Num: 4, Hash: h4},
		5: {Num: 5, Hash: h5},
		7: {Num: 7, Hash: h7},
	}
	bufferedHashes := map[tcommon.Hash]struct{}{
		h4: {},
		h5: {},
		h7: {},
	}
	path := BlockPath{
		4: h4,
		5: h5,
		7: h7,
	}

	batch := PopBufferedBatch(buffer, bufferedHashes, path, &wait, 4, 4, now)
	if len(batch.Buffered) != 2 {
		t.Fatalf("popped %d blocks, want 2", len(batch.Buffered))
	}
	if batch.Buffered[0].Num != 4 || batch.Buffered[1].Num != 5 {
		t.Fatalf("popped nums = %d,%d; want 4,5", batch.Buffered[0].Num, batch.Buffered[1].Num)
	}
	if len(batch.BufferWaits) != 2 || batch.BufferWaits[0] != 3*time.Second || batch.BufferWaits[1] != 0 {
		t.Fatalf("waits = %v, want [3s 0s]", batch.BufferWaits)
	}
	if _, ok := buffer[4]; ok {
		t.Fatal("block 4 still buffered")
	}
	if _, ok := buffer[5]; ok {
		t.Fatal("block 5 still buffered")
	}
	if _, ok := buffer[7]; !ok {
		t.Fatal("gap tail block 7 was removed")
	}
	if _, ok := bufferedHashes[h4]; ok {
		t.Fatal("hash 4 still reserved")
	}
	if _, ok := bufferedHashes[h5]; ok {
		t.Fatal("hash 5 still reserved")
	}
	if _, ok := bufferedHashes[h7]; !ok {
		t.Fatal("gap tail hash 7 was removed")
	}
	if _, ok := path[4]; ok {
		t.Fatal("path reservation 4 still present")
	}
	if _, ok := path[5]; ok {
		t.Fatal("path reservation 5 still present")
	}
	if _, ok := path[7]; !ok {
		t.Fatal("gap tail path reservation 7 was removed")
	}
}

func testBufferedBlock(num int64) *types.Block {
	return types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData:          &corepb.BlockHeaderRaw{Number: num, Timestamp: num * 3000},
			WitnessSignature: make([]byte, 65),
		},
		Transactions: []*corepb.Transaction{
			{Signature: [][]byte{{byte(num)}}},
		},
	})
}
