package dpos

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/consensus"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
)

// mockChainHeaderWriter implements consensus.ChainHeaderWriter for testing.
type mockChainHeaderWriter struct {
	witnesses             map[common.Address]*types.Witness
	allowances            map[common.Address]int64
	witnessPayPerBlock    int64
	witnessStandbyAllow   int64
	maintenanceInterval   int64
	nextMaintenanceTime   int64
	genesisWitnesses      []consensus.GenesisWitnessInfo
	removeThePowerOfTheGr int64
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
func (m *mockChainHeaderWriter) AddWitnessVoteCount(addr common.Address, delta int64) {
	w := m.witnesses[addr]
	if w == nil {
		return
	}
	w.SetVoteCount(w.VoteCount() + delta)
}
func (m *mockChainHeaderWriter) AddAllowance(addr common.Address, amount int64) {
	m.allowances[addr] += amount
}
func (m *mockChainHeaderWriter) NextMaintenanceTime() int64 {
	return m.nextMaintenanceTime
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
func (m *mockChainHeaderWriter) ChangeDelegation() bool {
	return false
}
func (m *mockChainHeaderWriter) GenesisWitnesses() []consensus.GenesisWitnessInfo {
	return m.genesisWitnesses
}
func (m *mockChainHeaderWriter) RemoveThePowerOfTheGr() int64 {
	return m.removeThePowerOfTheGr
}
func (m *mockChainHeaderWriter) SetRemoveThePowerOfTheGr(v int64) {
	m.removeThePowerOfTheGr = v
}

func TestDoMaintenance_DistributesAllowance(t *testing.T) {
	chain := newMockChainHeaderWriter()
	chain.nextMaintenanceTime = 1700000000000 // current maintenance time
	blockTime := int64(1700010000000)          // block during maintenance window
	witnesses := []WitnessVote{
		{Address: common.BytesToAddress([]byte{0x41, 1}), Votes: 300},
		{Address: common.BytesToAddress([]byte{0x41, 2}), Votes: 200},
		{Address: common.BytesToAddress([]byte{0x41, 3}), Votes: 100},
	}

	DoMaintenance(chain, blockTime, witnesses)

	// Pro-rata distribution: eachVotePay = totalPay / voteSum; pay = votes * eachVotePay.
	// voteSum = 600, totalPay = 115_200_000_000 → eachVotePay = 192_000_000.
	expectations := map[int64]int64{
		300: 57_600_000_000,
		200: 38_400_000_000,
		100: 19_200_000_000,
	}
	for _, w := range witnesses {
		want := expectations[w.Votes]
		if chain.allowances[w.Address] != want {
			t.Errorf("witness %v (%d votes): got allowance %d, want %d",
				w.Address, w.Votes, chain.allowances[w.Address], want)
		}
	}

	// nextMaint = currentMaint + (round+1)*interval
	// round = (1700010000000 - 1700000000000) / 21600000 = 462
	// next = 1700000000000 + 463*21600000 = 1700000000000 + 10000800000 = 1700010000800000
	// Wait, let me recalculate...
	// Actually: round = 10000000 / 21600000 = 0  (integer division)
	// next = 1700000000000 + 1*21600000 = 1700021600000
	expectedMaint := int64(1700000000000 + 21600000)
	if chain.nextMaintenanceTime != expectedMaint {
		t.Errorf("nextMaintenanceTime: got %d, want %d", chain.nextMaintenanceTime, expectedMaint)
	}
}

func TestCalcNextMaintenanceTime(t *testing.T) {
	interval := int64(21600000) // 6 hours in ms
	tests := []struct {
		name         string
		blockTime    int64
		currentMaint int64
		want         int64
	}{
		{"first interval", 10000000, 0, 21600000},
		{"exactly at maint", 21600000, 21600000, 43200000},
		{"mid-interval", 30000000, 21600000, 43200000},
		{"multiple intervals elapsed", 70000000, 21600000, 86400000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CalcNextMaintenanceTime(tt.blockTime, tt.currentMaint, interval)
			if got != tt.want {
				t.Errorf("CalcNextMaintenanceTime(%d, %d, %d) = %d, want %d",
					tt.blockTime, tt.currentMaint, interval, got, tt.want)
			}
		})
	}
}

func TestSelectActiveWitnesses(t *testing.T) {
	witnesses := []WitnessVote{
		{Address: common.BytesToAddress([]byte{0x41, 1}), Votes: 100},
		{Address: common.BytesToAddress([]byte{0x41, 2}), Votes: 300},
		{Address: common.BytesToAddress([]byte{0x41, 3}), Votes: 200},
	}
	active := SelectActiveWitnesses(witnesses)
	if len(active) != 3 {
		t.Fatalf("active count: want 3, got %d", len(active))
	}
	if active[0] != (common.BytesToAddress([]byte{0x41, 2})) {
		t.Fatal("first witness should be address 2")
	}
	if active[1] != (common.BytesToAddress([]byte{0x41, 3})) {
		t.Fatal("second witness should be address 3")
	}
}

func TestSelectActiveWitnessesMax(t *testing.T) {
	witnesses := make([]WitnessVote, 50)
	for i := range witnesses {
		witnesses[i] = WitnessVote{
			Address: common.BytesToAddress([]byte{0x41, byte(i)}),
			Votes:   int64(1000 - i),
		}
	}
	active := SelectActiveWitnesses(witnesses)
	if len(active) != params.MaxActiveWitnessNum {
		t.Fatalf("active count: want %d, got %d", params.MaxActiveWitnessNum, len(active))
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

func TestTryRemoveThePowerOfTheGr_StripsGenesisInitialVotes(t *testing.T) {
	chain := newMockChainHeaderWriter()
	const initialGRVote = int64(100_000_000)
	grAddr := common.BytesToAddress([]byte{0x41, 1})
	srAddr := common.BytesToAddress([]byte{0x41, 2})

	gr := types.NewWitness(grAddr, "gr")
	gr.SetVoteCount(150_000_000) // initial 100M + 50M earned
	chain.witnesses[grAddr] = gr

	sr := types.NewWitness(srAddr, "sr")
	sr.SetVoteCount(200_000_000) // pure community SR
	chain.witnesses[srAddr] = sr

	chain.genesisWitnesses = []consensus.GenesisWitnessInfo{
		{Address: grAddr, VoteCount: initialGRVote},
	}
	chain.removeThePowerOfTheGr = 1

	allWitnesses := []WitnessVote{
		{Address: grAddr, Votes: 150_000_000},
		{Address: srAddr, Votes: 200_000_000},
	}
	tryRemoveThePowerOfTheGr(chain, allWitnesses)

	if got := chain.witnesses[grAddr].VoteCount(); got != 50_000_000 {
		t.Errorf("GR voteCount: got %d, want 50_000_000", got)
	}
	if got := chain.witnesses[srAddr].VoteCount(); got != 200_000_000 {
		t.Errorf("SR voteCount: got %d, want 200_000_000 (unchanged)", got)
	}
	if chain.removeThePowerOfTheGr != -1 {
		t.Errorf("flag after strip: got %d, want -1", chain.removeThePowerOfTheGr)
	}
	if allWitnesses[0].Votes != 50_000_000 {
		t.Errorf("in-memory GR votes: got %d, want 50_000_000", allWitnesses[0].Votes)
	}
	if allWitnesses[1].Votes != 200_000_000 {
		t.Errorf("in-memory SR votes: got %d, want 200_000_000", allWitnesses[1].Votes)
	}

	// Second call must be a no-op.
	tryRemoveThePowerOfTheGr(chain, allWitnesses)
	if got := chain.witnesses[grAddr].VoteCount(); got != 50_000_000 {
		t.Errorf("GR voteCount after no-op: got %d, want 50_000_000", got)
	}
	if chain.removeThePowerOfTheGr != -1 {
		t.Errorf("flag after no-op: got %d, want -1", chain.removeThePowerOfTheGr)
	}
}

func TestTryRemoveThePowerOfTheGr_FlagNotOneIsNoOp(t *testing.T) {
	chain := newMockChainHeaderWriter()
	grAddr := common.BytesToAddress([]byte{0x41, 1})
	gr := types.NewWitness(grAddr, "gr")
	gr.SetVoteCount(150_000_000)
	chain.witnesses[grAddr] = gr
	chain.genesisWitnesses = []consensus.GenesisWitnessInfo{
		{Address: grAddr, VoteCount: 100_000_000},
	}

	for _, flag := range []int64{0, -1, 2} {
		chain.removeThePowerOfTheGr = flag
		tryRemoveThePowerOfTheGr(chain, nil)
		if got := chain.witnesses[grAddr].VoteCount(); got != 150_000_000 {
			t.Errorf("flag=%d: GR vote count mutated unexpectedly: got %d", flag, got)
		}
		if chain.removeThePowerOfTheGr != flag {
			t.Errorf("flag=%d: flag mutated to %d", flag, chain.removeThePowerOfTheGr)
		}
	}
}
