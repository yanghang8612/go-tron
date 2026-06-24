package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/txpool"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"github.com/tronprotocol/go-tron/vm/tracers"
)

// newTraceBackend builds a minimal chain whose head state holds a contract that
// stores 9 at slot 0, reloads it, then REVERTs — enough to exercise the tracer
// stack/storage capture and the terminal REVERT.
func newTraceBackend(t *testing.T) (*TronBackend, tcommon.Address, tcommon.Address) {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)
	owner := testProcessorAddr(1)
	contractAddr := testProcessorAddr(0x80)
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: owner, Balance: 100_000_000},
		},
		DynamicProperties: map[string]int64{"allow_creation_of_contracts": 1},
	}
	_, genesisHash, err := SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatal(err)
	}

	root := rawdb.ReadGenesisStateRoot(diskdb)
	statedb, err := state.New(root, sdb)
	if err != nil {
		t.Fatalf("open genesis state: %v", err)
	}
	statedb.CreateAccount(contractAddr, corepb.AccountType_Contract)
	statedb.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress:   owner.Bytes(),
		ContractAddress: contractAddr.Bytes(),
	})
	// PUSH1 09 PUSH1 00 SSTORE  PUSH1 00 SLOAD POP  PUSH1 00 PUSH1 00 REVERT
	code := []byte{0x60, 0x09, 0x60, 0x00, 0x55, 0x60, 0x00, 0x54, 0x50, 0x60, 0x00, 0x60, 0x00, 0xfd}
	statedb.SetCode(contractAddr, code)
	newRoot, err := statedb.Commit()
	if err != nil {
		t.Fatalf("commit seeded contract: %v", err)
	}
	rawdb.WriteGenesisStateRoot(diskdb, newRoot)
	rawdb.WriteBlockStateRoot(diskdb, genesisHash, newRoot)

	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { bc.Close() })
	return &TronBackend{chain: bc}, owner, contractAddr
}

func TestTraceCall_StructLoggerEndsAtRevert(t *testing.T) {
	b, owner, contractAddr := newTraceBackend(t)

	res, err := b.TraceCall(&owner, &contractAddr, nil, 0, nil, &tracers.TraceConfig{})
	if err != nil {
		t.Fatalf("TraceCall: %v", err)
	}
	exec, ok := res.(*tracers.ExecutionResult)
	if !ok {
		t.Fatalf("result type: got %T, want *tracers.ExecutionResult", res)
	}
	if !exec.Failed {
		t.Fatal("reverting call must report failed=true")
	}
	if len(exec.StructLogs) == 0 {
		t.Fatal("no struct logs")
	}
	last := exec.StructLogs[len(exec.StructLogs)-1]
	if last.Op != "REVERT" {
		t.Fatalf("trace must end at REVERT, got %q", last.Op)
	}
	// The stored operand must be visible somewhere in the SSTORE step's stack.
	sawSstore := false
	for _, l := range exec.StructLogs {
		if l.Op == "SSTORE" && l.Stack != nil {
			for _, s := range *l.Stack {
				if s == "0x9" {
					sawSstore = true
				}
			}
		}
	}
	if !sawSstore {
		t.Fatal("SSTORE operand 0x9 not visible in the trace")
	}
}

func TestTraceCall_CallTracerReportsRevert(t *testing.T) {
	b, owner, contractAddr := newTraceBackend(t)

	name := "callTracer"
	res, err := b.TraceCall(&owner, &contractAddr, nil, 0, nil, &tracers.TraceConfig{Tracer: &name})
	if err != nil {
		t.Fatalf("TraceCall: %v", err)
	}
	root, ok := res.(*tracers.CallFrame)
	if !ok {
		t.Fatalf("result type: got %T, want *tracers.CallFrame", res)
	}
	if root.Type != "CALL" {
		t.Fatalf("root type: got %q, want CALL", root.Type)
	}
	if root.Error == "" {
		t.Fatal("reverting top-level call must carry an error")
	}
}

func TestTraceCall_UnknownTracerErrors(t *testing.T) {
	b, owner, contractAddr := newTraceBackend(t)
	bad := "noSuchTracer"
	if _, err := b.TraceCall(&owner, &contractAddr, nil, 0, nil, &tracers.TraceConfig{Tracer: &bad}); err == nil {
		t.Fatal("unknown tracer must return an error")
	}
}

// newTraceTxBackend builds a history-enabled chain whose genesis holds a
// contract that stores 9 at slot 0 then STOPs, then includes a TriggerSmartContract
// to it in block 1. Returns the backend and the trigger tx hash for replay tracing.
func newTraceTxBackend(t *testing.T) (*TronBackend, tcommon.Hash) {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)
	cfg := cloneMainnetChainConfig()
	cfg.HistoryEnabled = true
	owner := testProcessorAddr(1)
	contractAddr := testProcessorAddr(0x80)
	witness := testProcessorAddr(0xF0)
	genesis := &params.Genesis{
		Config:    cfg,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: owner, Balance: 100_000_000_000},
		},
		DynamicProperties: map[string]int64{
			"allow_creation_of_contracts": 1,
			"next_maintenance_time":       1<<62 - 1,
		},
	}
	_, genesisHash, err := SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatal(err)
	}

	root := rawdb.ReadGenesisStateRoot(diskdb)
	statedb, err := state.New(root, sdb)
	if err != nil {
		t.Fatalf("open genesis state: %v", err)
	}
	statedb.CreateAccount(contractAddr, corepb.AccountType_Contract)
	statedb.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress:   owner.Bytes(),
		ContractAddress: contractAddr.Bytes(),
	})
	// PUSH1 09 PUSH1 00 SSTORE STOP — a clean successful write.
	statedb.SetCode(contractAddr, []byte{0x60, 0x09, 0x60, 0x00, 0x55, 0x00})
	newRoot, err := statedb.Commit()
	if err != nil {
		t.Fatalf("commit seeded contract: %v", err)
	}
	rawdb.WriteGenesisStateRoot(diskdb, newRoot)
	rawdb.WriteBlockStateRoot(diskdb, genesisHash, newRoot)

	bc, err := NewBlockChain(diskdb, sdb, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { bc.Close() })

	tx := makeTestTriggerTx(1, contractAddr, nil)
	tx.Proto().RawData.Timestamp = 1
	tx.Proto().RawData.FeeLimit = 100_000_000
	pool := txpool.New()
	if err := pool.Add(tx); err != nil {
		t.Fatalf("pool.Add: %v", err)
	}
	result, err := BuildBlock(bc, pool, witness, 3000)
	if err != nil {
		t.Fatalf("BuildBlock: %v", err)
	}
	if got := len(result.Block.Transactions()); got != 1 {
		t.Fatalf("trigger tx not included: %d txs in block", got)
	}
	if err := bc.InsertBlock(result.Block); err != nil {
		t.Fatalf("InsertBlock: %v", err)
	}
	return &TronBackend{chain: bc}, tx.Hash()
}

func TestTraceTransaction_StructLoggerReplaysTx(t *testing.T) {
	b, txHash := newTraceTxBackend(t)

	res, err := b.TraceTransaction(txHash, &tracers.TraceConfig{})
	if err != nil {
		t.Fatalf("TraceTransaction: %v", err)
	}
	exec, ok := res.(*tracers.ExecutionResult)
	if !ok {
		t.Fatalf("result type: got %T, want *tracers.ExecutionResult", res)
	}
	if exec.Failed {
		t.Fatal("successful tx must report failed=false")
	}
	if len(exec.StructLogs) == 0 {
		t.Fatal("replayed tx produced no struct logs")
	}
	sawSstore := false
	for _, l := range exec.StructLogs {
		if l.Op == "SSTORE" {
			sawSstore = true
		}
	}
	if !sawSstore {
		t.Fatalf("replayed trace missing SSTORE: %+v", exec.StructLogs)
	}
	if last := exec.StructLogs[len(exec.StructLogs)-1]; last.Op != "STOP" {
		t.Fatalf("replayed trace must end at STOP, got %q", last.Op)
	}
}

func TestTraceTransaction_NotFound(t *testing.T) {
	b, _ := newTraceTxBackend(t)
	var missing tcommon.Hash
	missing[0] = 0xab
	if _, err := b.TraceTransaction(missing, &tracers.TraceConfig{}); err == nil {
		t.Fatal("tracing an unknown tx must return an error")
	}
}
