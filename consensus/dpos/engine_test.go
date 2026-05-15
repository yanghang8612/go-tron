package dpos

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

type mockChainReader struct {
	currentBlock *types.Block
	genesisTime  int64
	witnesses    []common.Address
	maintTime    int64
	dp           *state.DynamicProperties
}

func (m *mockChainReader) CurrentBlock() *types.Block           { return m.currentBlock }
func (m *mockChainReader) GetBlockByNumber(uint64) *types.Block { return nil }
func (m *mockChainReader) GenesisTimestamp() int64              { return m.genesisTime }
func (m *mockChainReader) ActiveWitnesses() []common.Address    { return m.witnesses }
func (m *mockChainReader) NextMaintenanceTime() int64           { return m.maintTime }
func (m *mockChainReader) DynProps() *state.DynamicProperties {
	if m.dp == nil {
		m.dp = state.NewDynamicProperties()
	}
	return m.dp
}

func testEngineAddr(b byte) common.Address {
	var addr common.Address
	addr[0] = 0x41
	addr[20] = b
	return addr
}

func TestEngine_GetScheduledWitness(t *testing.T) {
	witnesses := []common.Address{testEngineAddr(1), testEngineAddr(2), testEngineAddr(3)}
	chain := &mockChainReader{
		currentBlock: types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{Number: 0, Timestamp: 0},
			},
		}),
		genesisTime: 0,
		witnesses:   witnesses,
	}

	engine := New(chain)
	addr, err := engine.GetScheduledWitness(1)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, w := range witnesses {
		if w == addr {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("scheduled witness %x not in witness list", addr)
	}
}

func TestEngine_GetScheduledWitness_NoWitnesses(t *testing.T) {
	chain := &mockChainReader{
		currentBlock: types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{Number: 0, Timestamp: 0},
			},
		}),
		genesisTime: 0,
		witnesses:   nil,
	}

	engine := New(chain)
	_, err := engine.GetScheduledWitness(1)
	if err == nil {
		t.Fatal("expected error with no witnesses")
	}
}

func TestEngine_IsInMaintenance(t *testing.T) {
	chain := &mockChainReader{maintTime: 100000}
	engine := New(chain)

	if engine.IsInMaintenance(99999) {
		t.Fatal("should not be in maintenance before maintenance time")
	}
	if !engine.IsInMaintenance(100000) {
		t.Fatal("should be in maintenance at maintenance time")
	}
	if !engine.IsInMaintenance(100001) {
		t.Fatal("should be in maintenance after maintenance time")
	}
}
