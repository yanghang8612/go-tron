package core

// vmKVStore must keep BLOCKHASH resolving after the slice-3 freezer prunes
// hot b-<num> rows: the lookup falls through buffer -> Pebble -> ancient.
// Nile stalled at block 16,745,722 when JustLink VRF asked for
// blockhash(head-211) and the hot row was already pruned (default freezer
// margin 128 < the opcode's 256-block window).

import (
	"bytes"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

type stubAncient struct {
	rawdb.NoopAncient
	blocks map[uint64][]byte
}

func (s stubAncient) Ancient(kind string, number uint64) ([]byte, error) {
	if kind == "bodies" {
		if data, ok := s.blocks[number]; ok {
			return data, nil
		}
	}
	return nil, rawdb.ErrNotInAncient
}

func (s stubAncient) HasAncient(kind string, number uint64) (bool, error) {
	if kind != "bodies" {
		return false, nil
	}
	_, ok := s.blocks[number]
	return ok, nil
}

func testChainBlock(t *testing.T, number uint64) *types.Block {
	t.Helper()
	return types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    int64(number),
				Timestamp: int64(number * 3000),
			},
		},
	})
}

func TestVMKVStoreBlockHashFallsThroughToAncient(t *testing.T) {
	kv := ethrawdb.NewMemoryDatabase()

	hot := testChainBlock(t, 200)
	if err := rawdb.WriteBlock(kv, hot); err != nil {
		t.Fatal(err)
	}

	frozen := testChainBlock(t, 50)
	frozenBytes, err := frozen.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	// Block 50's hot row was pruned by the freezer: present ONLY in ancient.
	anc := stubAncient{blocks: map[uint64][]byte{50: frozenBytes}}

	buffered := testChainBlock(t, 250)
	buf := blockbuffer.New(kv)
	buf.BeginBlock(buffered.Hash(), buffered.Number())
	if err := rawdb.WriteBlock(buf, buffered); err != nil {
		t.Fatal(err)
	}

	store := vmKVStore{BufferedKVStore: buf, chaindb: rawdb.NewChainDB(kv, anc)}

	for _, tc := range []struct {
		name   string
		number uint64
		want   tcommon.Hash
		found  bool
	}{
		{"buffer-only block", 250, buffered.Hash(), true},
		{"hot Pebble block", 200, hot.Hash(), true},
		{"frozen block (hot row pruned)", 50, frozen.Hash(), true},
		{"unknown block", 999, tcommon.Hash{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := store.BlockHashByNumber(tc.number)
			if ok != tc.found || got != tc.want {
				t.Fatalf("BlockHashByNumber(%d) = %x,%v want %x,%v", tc.number, got, ok, tc.want, tc.found)
			}
		})
	}
}

func BenchmarkVMKVStoreBlockHashCompactIndex(b *testing.B) {
	kv := ethrawdb.NewMemoryDatabase()
	pb := &corepb.Block{
		BlockHeader:  &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 250, Timestamp: 750000}},
		Transactions: make([]*corepb.Transaction, 100),
	}
	for i := range pb.Transactions {
		pb.Transactions[i] = &corepb.Transaction{
			RawData: &corepb.TransactionRaw{Data: bytes.Repeat([]byte{byte(i)}, 1024)},
		}
	}
	block := types.NewBlockFromPB(pb)
	buf := blockbuffer.New(kv)
	buf.SetBaseReadCacheSize(1 << 20)
	buf.BeginBlock(block.Hash(), block.Number())
	if err := rawdb.WriteBlock(buf, block); err != nil {
		b.Fatal(err)
	}
	store := vmKVStore{BufferedKVStore: buf, chaindb: rawdb.NewChainDB(kv, rawdb.NoopAncient{})}
	want := block.Hash()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, ok := store.BlockHashByNumber(block.Number())
		if !ok || got != want {
			b.Fatal("block hash mismatch")
		}
	}
}
