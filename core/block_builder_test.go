package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/txpool"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/crypto"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestBuildBlock_EmptyPool(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: testProcessorAddr(1), Balance: 10_000_000},
		},
	}
	SetupGenesisBlock(diskdb, genesis)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	pool := txpool.New()
	witnessAddr := testProcessorAddr(0xFF)

	result, err := BuildBlock(bc, pool, witnessAddr, 3000)
	if err != nil {
		t.Fatal(err)
	}
	block := result.Block

	if block.Number() != 1 {
		t.Fatalf("block number: want 1, got %d", block.Number())
	}
	if block.Timestamp() != 3000 {
		t.Fatalf("timestamp: want 3000, got %d", block.Timestamp())
	}
	if block.WitnessAddress() != witnessAddr {
		t.Fatalf("witness: want %x, got %x", witnessAddr, block.WitnessAddress())
	}
	if block.AccountStateRoot() != (tcommon.Hash{}) {
		t.Fatalf("accountStateRoot should be empty before allow_account_state_root, got %x", block.AccountStateRoot())
	}
	if len(block.Transactions()) != 0 {
		t.Fatalf("expected 0 transactions, got %d", len(block.Transactions()))
	}
	if got := block.Version(); got != params.BlockVersion {
		t.Fatalf("block version: want %d, got %d", params.BlockVersion, got)
	}
}

func TestBuildBlock_AccountStateRootEnabledEmptyPool(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: testProcessorAddr(1), Balance: 10_000_000},
		},
		DynamicProperties: map[string]int64{
			"allow_account_state_root": 1,
		},
	}
	SetupGenesisBlock(diskdb, genesis)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	result, err := BuildBlock(bc, txpool.New(), testProcessorAddr(0xFF), 3000)
	if err != nil {
		t.Fatal(err)
	}
	want := tcommon.Hash(ethtypes.EmptyRootHash)
	if got := result.Block.AccountStateRoot(); got != want {
		t.Fatalf("accountStateRoot: got %x, want empty trie root %x", got, want)
	}
}

func TestBuildBlock_WithTransactions(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	sender := testProcessorAddr(1)
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: sender, Balance: 100_000_000},
		},
	}
	SetupGenesisBlock(diskdb, genesis)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	pool := txpool.New()
	tx := makeTestTransferTx(1, 2, 1_000_000)
	pool.Add(tx)

	witnessAddr := testProcessorAddr(0xFF)
	result, err := BuildBlock(bc, pool, witnessAddr, 3000)
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Block.Transactions()) != 1 {
		t.Fatalf("expected 1 transaction, got %d", len(result.Block.Transactions()))
	}
}

func TestBuildBlock_IgnoresPendingTransactionRet(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	owner := testProcessorAddr(1)
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: owner, Balance: 100_000_000},
		},
		DynamicProperties: map[string]int64{
			"allow_creation_of_contracts": 1,
		},
	}
	SetupGenesisBlock(diskdb, genesis)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	csc := &contractpb.CreateSmartContract{
		OwnerAddress: owner.Bytes(),
		NewContract: &contractpb.SmartContract{
			OriginAddress: owner.Bytes(),
			Name:          "RetProbe",
			Bytecode:      []byte{0x60, 0x00, 0x60, 0x00, 0xf3},
		},
	}
	param, err := anypb.New(csc)
	if err != nil {
		t.Fatalf("anypb.New: %v", err)
	}
	tx := types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Expiration: 60_000,
			FeeLimit:   10_000_000,
			Contract: []*corepb.Transaction_Contract{{
				Type:      corepb.Transaction_Contract_CreateSmartContract,
				Parameter: param,
			}},
		},
	})

	pool := txpool.New()
	if err := pool.Add(tx); err != nil {
		t.Fatalf("pool.Add: %v", err)
	}
	tx.Proto().Ret = []*corepb.Transaction_Result{{
		ContractRet: corepb.Transaction_Result_OUT_OF_TIME,
	}}

	result, err := BuildBlock(bc, pool, testProcessorAddr(0xFF), 3000)
	if err != nil {
		t.Fatal(err)
	}
	txs := result.Block.Transactions()
	if len(txs) != 1 {
		t.Fatalf("expected 1 transaction, got %d", len(txs))
	}
	ret := txs[0].Proto().GetRet()
	if len(ret) != 1 {
		t.Fatalf("ret count: got %d, want 1", len(ret))
	}
	if got := ret[0].GetContractRet(); got != corepb.Transaction_Result_SUCCESS {
		t.Fatalf("contractRet: got %s, want SUCCESS", got)
	}
	if got := txs[0].Proto().GetRet()[0].GetRet(); got != corepb.Transaction_Result_SUCESS {
		t.Fatalf("ret code: got %s, want SUCESS", got)
	}
}

func TestBuildBlock_SkipsFailingTx(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: testProcessorAddr(1), Balance: 100_000_000},
		},
	}
	SetupGenesisBlock(diskdb, genesis)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	pool := txpool.New()
	tx1 := makeTestTransferTx(1, 2, 1_000_000)
	pool.Add(tx1)
	tx2 := makeTestTransferTx(3, 4, 1_000_000) // sender 3 doesn't exist
	pool.Add(tx2)

	witnessAddr := testProcessorAddr(0xFF)
	result, err := BuildBlock(bc, pool, witnessAddr, 3000)
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Block.Transactions()) != 1 {
		t.Fatalf("expected 1 transaction (skipped failing), got %d", len(result.Block.Transactions()))
	}
	if len(result.FailedTxIDs) != 1 {
		t.Fatalf("expected 1 failed tx, got %d", len(result.FailedTxIDs))
	}
}

// TestBuildThenInsert_NoDuplicateReward is the regression test for the
// producer-side double-write of payBlockReward / applyRewardMaintenance.
//
// Before the fix, BuildBlock wrote cycleReward to bc.db directly; then
// InsertBlock → applyBlock → ProcessBlock read that value from the buffer
// (which falls through to disk) and added the reward again, doubling
// cycleReward[N][witness] and witness allowance on every locally-produced block.
//
// With change_delegation=1 and the default 20% brokerage, a single block
// reward of 32_000_000 SUN should produce:
//
//	witness allowance: int64(0.20 × 32_000_000) = 6_400_000 SUN
//	cycle reward:      32_000_000 − 6_400_000 = 25_600_000 SUN
//
// Double-write would give 51_200_000 cycle reward and 12_800_000 allowance.
func TestBuildThenInsert_NoDuplicateReward(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	witnessAddr := testProcessorAddr(0x10)
	const brokerage = 20 // 20% default

	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		// Witness must appear in Accounts so statedb.AddAllowance finds the object.
		Accounts: []params.GenesisAccount{
			{Address: witnessAddr, Balance: 1_000_000},
		},
		Witnesses: []params.GenesisWitness{
			{Address: witnessAddr, VoteCount: 1000, URL: "http://sr1"},
		},
		DynamicProperties: map[string]int64{
			"change_delegation":     1,
			"next_maintenance_time": 9_000_000_000, // far in future; no maintenance
		},
	}
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatal(err)
	}

	// Pre-seed cycle brokerage for cycle 0 so payBlockReward sees the correct
	// rate (mirrors what applyRewardMaintenance writes at maintenance boundary).
	dp0 := state.LoadDynamicProperties(diskdb)
	curCycle := dp0.CurrentCycleNumber()
	rawdb.WriteCycleBrokerage(diskdb, curCycle, witnessAddr.Bytes(), brokerage)

	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	pool := txpool.New()
	result, err := BuildBlock(bc, pool, witnessAddr, 3000)
	if err != nil {
		t.Fatalf("BuildBlock: %v", err)
	}
	block := result.Block

	if err := bc.InsertBlock(block); err != nil {
		t.Fatalf("InsertBlock: %v", err)
	}
	// The flush from applyBlock runs asynchronously; wait before reading
	// disk-side counters (cycleReward) below.
	bc.WaitForFlushSettled()

	// Compute expected values accounting for both payBlockReward and
	// payStandbyWitness. With 1 witness holding all 1000 votes:
	//   payBlockReward(32M): voter gets 32M×0.8=25.6M, witness allowance +6.4M
	//   payStandbyWitness(16M): that witness gets 16M; voter gets 16M×0.8=12.8M,
	//                           witness allowance +3.2M
	// Total: cycleReward = 38.4M, allowance = 9.6M.
	// Under the old double-write: cycleReward = 76.8M, allowance = 19.2M.
	dp := state.LoadDynamicProperties(diskdb)
	payPerBlock := dp.WitnessPayPerBlock()   // 32_000_000
	standbyPay := dp.Witness127PayPerBlock() // 16_000_000 (single witness gets all)
	brokerageRate := float64(brokerage) / 100.0
	wantAllowance := int64(brokerageRate*float64(payPerBlock)) +
		int64(brokerageRate*float64(standbyPay)) // 6_400_000 + 3_200_000 = 9_600_000
	// voter portion: (1-brokerage%) of (payPerBlock + standbyPay)
	wantCycleReward := (payPerBlock - int64(brokerageRate*float64(payPerBlock))) +
		(standbyPay - int64(brokerageRate*float64(standbyPay))) // 25_600_000 + 12_800_000 = 38_400_000

	// Read allowance from post-apply state.
	headRoot := bc.HeadStateRoot()
	postState, err := state.New(headRoot, bc.StateDB())
	if err != nil {
		t.Fatalf("open post state: %v", err)
	}
	gotAllowance := postState.GetAllowance(witnessAddr)
	if gotAllowance != wantAllowance {
		t.Errorf("witness allowance: got %d, want %d (double-write would give %d)",
			gotAllowance, wantAllowance, wantAllowance*2)
	}

	// Read cycle reward from disk (flushed by applyBlock via bc.buffer).
	gotCycleReward := rawdb.ReadCycleReward(diskdb, curCycle, witnessAddr.Bytes())
	if gotCycleReward != wantCycleReward {
		t.Errorf("cycleReward[%d][witness]: got %d, want %d (double-write would give %d)",
			curCycle, gotCycleReward, wantCycleReward, wantCycleReward*2)
	}
}

func TestSignBlock(t *testing.T) {
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    1,
				Timestamp: 3000,
			},
		},
	})

	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}

	err = SignBlock(block, key)
	if err != nil {
		t.Fatal(err)
	}

	sig := block.WitnessSignature()
	if len(sig) != 65 {
		t.Fatalf("signature length: want 65, got %d", len(sig))
	}
}
