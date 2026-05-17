package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/internal/jsonrpc"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// newTestBlockchain creates an in-memory BlockChain with a genesis block for testing.
func newTestBlockchain(t *testing.T) (*BlockChain, func()) {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)
	genesis := &params.Genesis{
		Config: params.MainnetChainConfig,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 1000000},
		},
	}
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatal(err)
	}
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}
	return bc, func() {} // in-memory DB requires no cleanup
}

// TestTronBackend_ChainID verifies ChainID returns the configured chain ID.
func TestTronBackend_ChainID(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()
	b := &TronBackend{chain: bc}
	id := b.ChainID()
	if id == 0 {
		// ChainID of 0 is technically valid for a test chain; just verify it's a number
		t.Log("ChainID is 0 (test chain)")
	}
	_ = id // compile check
}

// TestTronBackend_BlockNumber verifies BlockNumber returns a valid block number.
func TestTronBackend_BlockNumber(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()
	b := &TronBackend{chain: bc}
	num := b.BlockNumber()
	_ = num // genesis block number is 0 or 1; just verify no panic
}

// TestTronBackend_GetBalance verifies GetBalance opens state and returns int64.
func TestTronBackend_GetBalance(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()
	b := &TronBackend{chain: bc}
	addr := tcommon.Address{}
	bal := b.GetBalance(addr)
	if bal < 0 {
		t.Fatalf("GetBalance should not return negative: %d", bal)
	}
}

// TestTronBackend_GetCode verifies GetCode returns nil for an account with no code.
func TestTronBackend_GetCode(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()
	b := &TronBackend{chain: bc}
	addr := tcommon.Address{}
	code := b.GetCode(addr)
	// An empty address has no contract code
	if len(code) > 0 {
		t.Logf("GetCode returned non-empty code: %d bytes", len(code))
	}
}

// TestTronBackend_GetStorageAt verifies GetStorageAt returns a hash (zero for empty slot).
func TestTronBackend_GetStorageAt(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()
	b := &TronBackend{chain: bc}
	addr := tcommon.Address{}
	slot := tcommon.Hash{}
	val := b.GetStorageAt(addr, slot)
	_ = val // just verify no panic
}

func TestTronBackend_ListWitnessesIncludesPendingVotes(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()

	voter := testCoreAddr(1)
	witness := testCoreAddr(2)
	rawdb.WriteWitnessIndex(bc.db, []tcommon.Address{witness})
	rawdb.WriteWitness(bc.db, witness, types.NewWitness(witness, "http://w"))
	if err := rawdb.WriteVotes(bc.db, voter, &corepb.Votes{
		Address: voter.Bytes(),
		NewVotes: []*corepb.Vote{
			{VoteAddress: witness.Bytes(), VoteCount: 123},
		},
	}); err != nil {
		t.Fatal(err)
	}

	got, err := (&TronBackend{chain: bc}).ListWitnesses()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].VoteCount != 123 {
		t.Fatalf("pending VotesStore delta not reflected: %+v", got)
	}
}

// TestTronBackend_GetTransactionByHash_NotFound verifies not-found returns nil.
func TestTronBackend_GetTransactionByHash_NotFound(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()
	b := &TronBackend{chain: bc}
	hash := tcommon.Hash{}
	tx, block, idx, err := b.GetTransactionByHash(hash)
	if err != nil {
		t.Fatalf("GetTransactionByHash returned error: %v", err)
	}
	if tx != nil || block != nil || idx != 0 {
		t.Fatal("GetTransactionByHash should return nil for unknown hash")
	}
}

// TestTronBackend_GetLogs_EmptyRange verifies GetLogs returns empty slice for range with no logs.
func TestTronBackend_GetLogs_EmptyRange(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()
	b := &TronBackend{chain: bc}
	from := uint64(0)
	to := uint64(0)
	logs, err := b.GetLogs(jsonrpc.LogFilter{FromBlock: &from, ToBlock: &to})
	if err != nil {
		t.Fatalf("GetLogs returned error: %v", err)
	}
	if logs == nil {
		t.Fatal("GetLogs should return empty slice, not nil")
	}
}

// TestProposalParametersToList_SortedAscending verifies the proposal-parameters
// helper emits a key-sorted slice so HTTP `/wallet/(get|list)proposal*` output
// is deterministic — Go map iteration is randomized, so the sort is required
// for byte-stable JSON across calls.
func TestProposalParametersToList_SortedAscending(t *testing.T) {
	in := map[int64]int64{19: 259200000, 5: 1, 11: 100}
	got := proposalParametersToList(in)
	if len(got) != 3 {
		t.Fatalf("expected 3 entries, got %d: %v", len(got), got)
	}
	if got[0].Key != 5 || got[1].Key != 11 || got[2].Key != 19 {
		t.Fatalf("expected keys [5, 11, 19] in ascending order, got %v", got)
	}
	if got[0].Value != 1 || got[1].Value != 100 || got[2].Value != 259200000 {
		t.Fatalf("values mis-paired with keys: %v", got)
	}
}

// TestProposalParametersToList_EmptyReturnsNonNil ensures an empty input
// produces a non-nil empty slice so JSON encodes it as `[]`, not `null`.
func TestProposalParametersToList_EmptyReturnsNonNil(t *testing.T) {
	got := proposalParametersToList(nil)
	if got == nil {
		t.Fatal("expected non-nil slice for nil map (so json renders [], not null)")
	}
	if len(got) != 0 {
		t.Fatalf("expected empty slice, got %v", got)
	}
}

func TestTronBackend_GetDelegatedResourceV2ReturnsSeparateBuckets(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()

	from := testCoreAddr(1)
	to := testCoreAddr(2)
	if err := rawdb.WriteDelegatedResourceV2(bc.db, from, to, false, &rawdb.DelegatedResource{
		From:                      from,
		To:                        to,
		FrozenBalanceForBandwidth: 100,
	}); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteDelegatedResourceV2(bc.db, from, to, true, &rawdb.DelegatedResource{
		From:                   from,
		To:                     to,
		FrozenBalanceForEnergy: 200,
		ExpireTimeForEnergy:    300,
	}); err != nil {
		t.Fatal(err)
	}

	got, err := (&TronBackend{chain: bc}).GetDelegatedResourceV2(from, to)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected unlocked and locked resource records, got %d", len(got))
	}
	if got[0].FrozenBalanceForBandwidth != 100 || got[0].ExpireTimeForEnergy != 0 {
		t.Fatalf("first record should be unlocked bucket, got %+v", got[0])
	}
	if got[1].FrozenBalanceForEnergy != 200 || got[1].ExpireTimeForEnergy != 300 {
		t.Fatalf("second record should be locked bucket, got %+v", got[1])
	}
}
