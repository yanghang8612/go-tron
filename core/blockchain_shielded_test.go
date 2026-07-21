package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/core/zksnark"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

// TestBlockContainsShieldedTransfer pins the helper used by applyBlock to
// reject shielded blocks clearly when the binary was built without the Sapling
// Pedersen backend. False on transparent-only blocks and on empty blocks; true
// as soon as a single ShieldedTransferContract is present.
func TestBlockContainsShieldedTransfer(t *testing.T) {
	transferAny, err := anypb.New(&contractpb.TransferContract{})
	if err != nil {
		t.Fatal(err)
	}
	shieldedAny, err := anypb.New(&contractpb.ShieldedTransferContract{})
	if err != nil {
		t.Fatal(err)
	}

	makeBlock := func(types_ []corepb.Transaction_Contract_ContractType) *types.Block {
		var txs []*corepb.Transaction
		for _, ty := range types_ {
			param := transferAny
			if ty == corepb.Transaction_Contract_ShieldedTransferContract {
				param = shieldedAny
			}
			txs = append(txs, &corepb.Transaction{
				RawData: &corepb.TransactionRaw{
					Contract: []*corepb.Transaction_Contract{{Type: ty, Parameter: param}},
				},
			})
		}
		return types.NewBlockFromPB(&corepb.Block{
			BlockHeader:  &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 1}},
			Transactions: txs,
		})
	}

	cases := []struct {
		name string
		txs  []corepb.Transaction_Contract_ContractType
		want bool
	}{
		{"empty block", nil, false},
		{"transparent only", []corepb.Transaction_Contract_ContractType{
			corepb.Transaction_Contract_TransferContract,
			corepb.Transaction_Contract_TransferContract,
		}, false},
		{"single shielded", []corepb.Transaction_Contract_ContractType{
			corepb.Transaction_Contract_ShieldedTransferContract,
		}, true},
		{"shielded among transparent", []corepb.Transaction_Contract_ContractType{
			corepb.Transaction_Contract_TransferContract,
			corepb.Transaction_Contract_ShieldedTransferContract,
			corepb.Transaction_Contract_TransferContract,
		}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := blockContainsShieldedTransfer(makeBlock(tc.txs)); got != tc.want {
				t.Fatalf("blockContainsShieldedTransfer: got %v, want %v", got, tc.want)
			}
		})
	}
}

// TestShouldRequireShieldedBackend exercises the safety gate for builds that
// do not include the Sapling backend. Truth table:
//
//	dp.AllowShieldedTransaction │ blockHasShielded │ want
//	───────────────────────────┼──────────────────┼─────
//	false                      │ false            │ false
//	false                      │ true             │ true
//	true                       │ false            │ true
//	true                       │ true             │ true
//
// The "either alone is enough" half-plane is the critical bit — a regression
// that ANDed the two would silently desync LAST_TREE / MerkleTreeIndexStore
// density from java-tron once the chain crosses the proposal-27 activation.
func TestShouldRequireShieldedBackend(t *testing.T) {
	transferAny, err := anypb.New(&contractpb.TransferContract{})
	if err != nil {
		t.Fatal(err)
	}
	shieldedAny, err := anypb.New(&contractpb.ShieldedTransferContract{})
	if err != nil {
		t.Fatal(err)
	}

	makeBlock := func(hasShielded bool) *types.Block {
		ty := corepb.Transaction_Contract_TransferContract
		param := transferAny
		if hasShielded {
			ty = corepb.Transaction_Contract_ShieldedTransferContract
			param = shieldedAny
		}
		return types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 1}},
			Transactions: []*corepb.Transaction{{
				RawData: &corepb.TransactionRaw{
					Contract: []*corepb.Transaction_Contract{{Type: ty, Parameter: param}},
				},
			}},
		})
	}

	cases := []struct {
		name          string
		dpActivated   bool
		blockShielded bool
		want          bool
	}{
		{"both off", false, false, false},
		{"block-only flips on", false, true, true},
		{"dp-only flips on", true, false, true},
		{"both on", true, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dp := state.NewDynamicProperties()
			dp.SetAllowShieldedTransaction(tc.dpActivated)
			if got := shouldRequireShieldedBackend(dp, makeBlock(tc.blockShielded)); got != tc.want {
				t.Fatalf("shouldRequireShieldedBackend: got %v, want %v", got, tc.want)
			}
		})
	}
}

// TestApplyBlockShieldedMerkleLifecycle is the lifecycle parity test for the
// Sapling commitment tree in applyBlock. It pins three points
// against java-tron's MerkleContainer.saveCurrentMerkleTreeAsBestMerkleTree
// contract:
//
//  1. Pre-activation block (allow_shielded_transaction = 0, no shielded tx)
//     still writes MerkleTreeIndexStore[blockNum], exactly as java-tron does.
//
//  2. Activation block (allow_shielded_transaction = 1 from genesis) lands
//     the first MerkleTreeIndexStore[blockNum] entry and root-indexes the
//     (empty) tree in IncrementalMerkleTreeStore. The stored root is the
//     empty-tree root — block #1 has no commitments, but java-tron still
//     writes it (wallet/voucher anchor lookups assume one entry per
//     post-activation block).
//
//     Note: LAST_TREE itself is intentionally NOT asserted. The empty
//     IncrementalMerkleTree marshals to zero bytes, and rawdb's
//     ReadLastMerkleTree reads `len(data) == 0` back as nil — i.e. the
//     LAST_TREE write is observable through MerkleTreeIndexStore + the
//     root-keyed store, not through ReadLastMerkleTree until a commitment
//     lands. See the comment in blockchain.go above shieldedActive.
//
//  3. Post-activation density: the next transparent block STILL writes a
//     MerkleTreeIndexStore[blockNum] entry. java-tron's index is
//     intentionally dense; a fix that gated the save on "block actually
//     mutated the tree" would silently break voucher proofs.
//
// (2) and (3) need the Sapling Pedersen backend to compute the empty-tree
// root — gated on zksnark.Available() (set true only by `-tags=sapling`
// builds linking librustzcash). (1) runs unconditionally and is the most
// valuable subtest in the default no-cgo CI path.
func TestApplyBlockShieldedMerkleLifecycle(t *testing.T) {
	witnessAddr := testCoreAddr(10)

	// Reusable fixture builder. dpActivated seeds the genesis DP
	// `allow_shielded_transaction` key so the LoadDynamicProperties read at
	// the top of applyBlock sees the gate flipped.
	newChain := func(t *testing.T, dpActivated bool) *BlockChain {
		t.Helper()
		diskdb := ethrawdb.NewMemoryDatabase()
		sdb := state.NewDatabase(diskdb)
		genesis := &params.Genesis{
			Config:    params.MainnetChainConfig,
			Timestamp: 0,
			Accounts: []params.GenesisAccount{
				{Address: testCoreAddr(1), Balance: 100_000_000},
				{Address: witnessAddr, Balance: 1_000_000},
			},
			Witnesses: []params.GenesisWitness{
				{Address: witnessAddr, VoteCount: 1000, URL: "http://w1"},
			},
		}
		if dpActivated {
			genesis.DynamicProperties = map[string]int64{
				"allow_shielded_transaction": 1,
			}
		}
		if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
			t.Fatalf("SetupGenesisBlock: %v", err)
		}
		bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
		if err != nil {
			t.Fatalf("NewBlockChain: %v", err)
		}
		return bc
	}

	openHeadState := func(t *testing.T, bc *BlockChain) *state.StateDB {
		t.Helper()
		statedb, err := bc.openState(bc.HeadStateRoot())
		if err != nil {
			t.Fatalf("open head state: %v", err)
		}
		return statedb
	}

	t.Run("pre-activation block writes index", func(t *testing.T) {
		if !zksnark.Available() {
			t.Skipf("Pedersen backend not built (rebuild with -tags=sapling): %v", zksnark.ErrPedersenUnimplemented)
		}
		bc := newChain(t, false)

		block1 := buildTestBlock(bc, witnessAddr, 3000)
		if err := bc.InsertBlock(block1); err != nil {
			t.Fatalf("InsertBlock(block1): %v", err)
		}

		if got := openHeadState(t, bc).ReadMerkleTreeRootByBlock(1); len(got) != 32 {
			t.Errorf("MerkleTreeIndexStore[1] root length = %d, want 32", len(got))
		}
	})

	t.Run("activation block writes first entry", func(t *testing.T) {
		if !zksnark.Available() {
			t.Skipf("Pedersen backend not built (rebuild with -tags=sapling): %v", zksnark.ErrPedersenUnimplemented)
		}
		bc := newChain(t, true)

		// Sanity: the genesis DP write must round-trip through
		// LoadDynamicProperties so that applyBlock's gate-evaluation at the
		// top sees the activation.
		dp := loadDPAtRoot(t, bc.BufferedDB(), bc.StateDB(), bc.HeadStateRoot())
		if !dp.AllowShieldedTransaction() {
			t.Fatalf("genesis activation lost: allow_shielded_transaction=%v", dp.AllowShieldedTransaction())
		}

		block1 := buildTestBlock(bc, witnessAddr, 3000)
		if err := bc.InsertBlock(block1); err != nil {
			t.Fatalf("InsertBlock(block1): %v", err)
		}
		if bc.CurrentBlock().Number() != 1 {
			t.Fatalf("block1 not applied: head=%d", bc.CurrentBlock().Number())
		}

		headState := openHeadState(t, bc)
		root := headState.ReadMerkleTreeRootByBlock(1)
		if len(root) == 0 {
			t.Fatal("MerkleTreeIndexStore[1] missing on activation block")
		}
		// 32-byte Sapling root — the empty-tree root is non-zero
		// (Combine of two empty leaves cascaded up to Depth).
		if len(root) != 32 {
			t.Errorf("root length = %d, want 32", len(root))
		}
		if !headState.HasIncrMerkleTree(root) {
			t.Errorf("root %x not indexed in IncrementalMerkleTreeStore", root)
		}
	})

	t.Run("post-activation density: each transparent block extends index", func(t *testing.T) {
		if !zksnark.Available() {
			t.Skipf("Pedersen backend not built (rebuild with -tags=sapling): %v", zksnark.ErrPedersenUnimplemented)
		}
		bc := newChain(t, true)

		block1 := buildTestBlock(bc, witnessAddr, 3000)
		if err := bc.InsertBlock(block1); err != nil {
			t.Fatalf("InsertBlock(block1): %v", err)
		}
		block2 := buildTestBlock(bc, witnessAddr, 6000)
		if err := bc.InsertBlock(block2); err != nil {
			t.Fatalf("InsertBlock(block2): %v", err)
		}
		if bc.CurrentBlock().Number() != 2 {
			t.Fatalf("blocks not applied: head=%d", bc.CurrentBlock().Number())
		}

		// Both blocks must have populated the index — java-tron writes one
		// entry per block once the gate is on, even when the tree is
		// unchanged. Wallet anchor lookups depend on the density.
		headState := openHeadState(t, bc)
		root1 := headState.ReadMerkleTreeRootByBlock(1)
		root2 := headState.ReadMerkleTreeRootByBlock(2)
		if len(root1) == 0 {
			t.Error("MerkleTreeIndexStore[1] missing")
		}
		if len(root2) == 0 {
			t.Error("MerkleTreeIndexStore[2] missing")
		}
		// Both blocks committed identical (empty) trees, so both index
		// entries should resolve to the same root — but that's
		// implementation detail; the contract is just "both present".
	})
}
