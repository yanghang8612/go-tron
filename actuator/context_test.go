package actuator

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestContextEffectiveGenesisHash(t *testing.T) {
	if got := (*Context)(nil).EffectiveGenesisHash(); got != (common.Hash{}) {
		t.Fatalf("nil context EffectiveGenesisHash = %x, want zero", got)
	}
	explicit := common.Hash{0xaa}
	ctx := &Context{GenesisHash: explicit}
	if got := ctx.EffectiveGenesisHash(); got != explicit {
		t.Fatalf("explicit EffectiveGenesisHash = %x, want %x", got, explicit)
	}

	db := ethrawdb.NewMemoryDatabase()
	genesis := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    0,
				Timestamp: 1,
			},
		},
	})
	if err := rawdb.WriteBlock(db, genesis); err != nil {
		t.Fatalf("WriteBlock genesis: %v", err)
	}
	ctx = &Context{DB: db}
	if got := ctx.EffectiveGenesisHash(); got != genesis.Hash() {
		t.Fatalf("db fallback EffectiveGenesisHash = %x, want %x", got, genesis.Hash())
	}
}
