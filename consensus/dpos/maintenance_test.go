package dpos

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
)

// mockChainHeaderWriter implements consensus.ChainHeaderWriter for testing.
type mockChainHeaderWriter struct {
	witnesses           map[common.Address]*types.Witness
	allowances          map[common.Address]int64
	witnessPayPerBlock  int64
	witnessStandbyAllow int64
	maintenanceInterval int64
	nextMaintenanceTime int64
}

func newMockChainHeaderWriter() *mockChainHeaderWriter {
	return &mockChainHeaderWriter{
		witnesses:           make(map[common.Address]*types.Witness),
		allowances:          make(map[common.Address]int64),
		witnessPayPerBlock:  16000000,
		witnessStandbyAllow: 115200000000,
		maintenanceInterval: 21600000,
	}
}

func (m *mockChainHeaderWriter) GetWitness(addr common.Address) *types.Witness {
	return m.witnesses[addr]
}
func (m *mockChainHeaderWriter) PutWitness(w *types.Witness) {
	m.witnesses[w.Address()] = w
}
func (m *mockChainHeaderWriter) AddAllowance(addr common.Address, amount int64) {
	m.allowances[addr] += amount
}
func (m *mockChainHeaderWriter) SetNextMaintenanceTime(t int64) {
	m.nextMaintenanceTime = t
}
func (m *mockChainHeaderWriter) WitnessPayPerBlock() int64 {
	return m.witnessPayPerBlock
}
func (m *mockChainHeaderWriter) WitnessStandbyAllowance() int64 {
	return m.witnessStandbyAllow
}
func (m *mockChainHeaderWriter) MaintenanceTimeInterval() int64 {
	return m.maintenanceInterval
}

func TestDoMaintenance_DistributesAllowance(t *testing.T) {
	chain := newMockChainHeaderWriter()
	witnesses := []WitnessVote{
		{Address: common.BytesToAddress([]byte{0x41, 1}), Votes: 300},
		{Address: common.BytesToAddress([]byte{0x41, 2}), Votes: 200},
		{Address: common.BytesToAddress([]byte{0x41, 3}), Votes: 100},
	}

	DoMaintenance(chain, witnesses)

	expectedPerWitness := int64(115200000000) / 3
	for _, w := range witnesses {
		if chain.allowances[w.Address] != expectedPerWitness {
			t.Errorf("witness %v: got allowance %d, want %d", w.Address, chain.allowances[w.Address], expectedPerWitness)
		}
	}

	if chain.nextMaintenanceTime != 21600000 {
		t.Errorf("nextMaintenanceTime: got %d, want 21600000", chain.nextMaintenanceTime)
	}
}

func TestPayBlockReward(t *testing.T) {
	chain := newMockChainHeaderWriter()
	addr := common.BytesToAddress([]byte{0x41, 1})

	PayBlockReward(chain, addr)

	if chain.allowances[addr] != 16000000 {
		t.Errorf("allowance: got %d, want 16000000", chain.allowances[addr])
	}
}
