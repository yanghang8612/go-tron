package types

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func TestNewBlock(t *testing.T) {
	pb := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    100,
				Timestamp: 1000000,
			},
		},
	}
	b := NewBlockFromPB(pb)
	if b.Number() != 100 {
		t.Fatalf("expected number 100, got %d", b.Number())
	}
	if b.Timestamp() != 1000000 {
		t.Fatalf("expected timestamp 1000000, got %d", b.Timestamp())
	}
}

func TestBlockHash(t *testing.T) {
	pb := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    1,
				Timestamp: 3000,
			},
		},
	}
	b := NewBlockFromPB(pb)
	h := b.Hash()
	if h.IsEmpty() {
		t.Fatal("hash should not be empty")
	}
	h2 := b.Hash()
	if h != h2 {
		t.Fatal("hash not deterministic")
	}
}

func TestBlockSerialize(t *testing.T) {
	pb := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    42,
				Timestamp: 9000,
			},
		},
	}
	b := NewBlockFromPB(pb)
	data, err := b.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	b2, err := UnmarshalBlock(data)
	if err != nil {
		t.Fatal(err)
	}
	if b2.Number() != 42 {
		t.Fatalf("expected 42, got %d", b2.Number())
	}
}

func TestBlockID(t *testing.T) {
	pb := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number: 5,
			},
		},
	}
	b := NewBlockFromPB(pb)
	id := b.ID()
	num := id.Number()
	if num != 5 {
		t.Fatalf("expected block number 5 from ID, got %d", num)
	}
}

func TestBlockParentHash(t *testing.T) {
	parent := common.HexToHash("aabbccdd")
	pb := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				ParentHash: parent.Bytes(),
			},
		},
	}
	b := NewBlockFromPB(pb)
	if b.ParentHash() != parent {
		t.Fatal("parent hash mismatch")
	}
}

func TestBlockProtoRoundTrip(t *testing.T) {
	pb := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:         999,
				Timestamp:      123456789,
				WitnessAddress: []byte{0x41, 0x01, 0x02},
				Version:        34,
			},
		},
	}
	b := NewBlockFromPB(pb)
	pb2 := b.Proto()
	if !proto.Equal(pb, pb2) {
		t.Fatal("proto round trip not equal")
	}
}
