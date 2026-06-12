package downloader

import (
	"bytes"
	"testing"

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
