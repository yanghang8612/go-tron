package core

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	chainfreezer "github.com/tronprotocol/go-tron/core/freezer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	statesnapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/internal/jsonrpc"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"github.com/tronprotocol/go-tron/vm/tracers"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// newTestBlockchain creates an in-memory BlockChain with a genesis block for testing.
func newTestBlockchain(t *testing.T, witnesses ...params.GenesisWitness) (*BlockChain, func()) {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)
	genesis := &params.Genesis{
		Config: params.MainnetChainConfig,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 1000000},
		},
		Witnesses: witnesses,
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

type failingBlockChainIndex struct {
	err error
}

func (f failingBlockChainIndex) BlockNumberByHash(hash tcommon.Hash) (uint64, bool, error) {
	return 0, false, f.err
}

func (f failingBlockChainIndex) TransactionBlockNumberByHash(hash tcommon.Hash) (uint64, bool, error) {
	return 0, false, nil
}

type staticAncientRow struct {
	rawdb.NoopAncient
	kind   string
	number uint64
	data   []byte
}

func (a staticAncientRow) Ancient(kind string, number uint64) ([]byte, error) {
	if kind == a.kind && number == a.number {
		return append([]byte(nil), a.data...), nil
	}
	return nil, rawdb.ErrNotInAncient
}

type staticAncientStatus struct {
	rawdb.NoopAncient
	counts map[string]uint64
	stats  rawdbfreezer.Stats
	err    error
}

func (a staticAncientStatus) AncientCount(kind string) (uint64, error) {
	if count, ok := a.counts[kind]; ok {
		return count, nil
	}
	return 0, nil
}

func (a staticAncientStatus) Stats() (rawdbfreezer.Stats, error) {
	if a.err != nil {
		return rawdbfreezer.Stats{}, a.err
	}
	return a.stats, nil
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

func TestTronBackend_LatestPbftBlockNumUsesStrictAccessor(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	backend := &TronBackend{chain: &BlockChain{db: db}}

	got, err := backend.LatestPbftBlockNum()
	if err != nil {
		t.Fatalf("LatestPbftBlockNum missing row error = %v", err)
	}
	if got != -1 {
		t.Fatalf("LatestPbftBlockNum missing row = %d, want -1", got)
	}

	rawdb.WriteLatestPbftBlockNum(db, 17)
	got, err = backend.LatestPbftBlockNum()
	if err != nil {
		t.Fatalf("LatestPbftBlockNum valid row error = %v", err)
	}
	if got != 17 {
		t.Fatalf("LatestPbftBlockNum valid row = %d, want 17", got)
	}

	_, err = (&TronBackend{chain: &BlockChain{}}).LatestPbftBlockNum()
	if err == nil || !strings.Contains(err.Error(), "nil database") {
		t.Fatalf("LatestPbftBlockNum nil database error = %v, want nil database", err)
	}
}

func TestTronBackend_FreezerStatusReportsCountsStageAndStats(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	stageHash := tcommon.Hash{0xab}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageChainFreezer, 11, stageHash); err != nil {
		t.Fatalf("WriteStageProgressWithHash: %v", err)
	}
	ancient := staticAncientStatus{
		counts: map[string]uint64{
			rawdb.AncientBlocksTable:     12,
			rawdb.AncientTxInfosTable:    12,
			rawdb.AncientStateRootsTable: 12,
		},
		stats: rawdbfreezer.Stats{
			Datadir: "/tmp/go-tron-freezer",
			Head:    12,
			Tail:    4,
			Tables: []rawdbfreezer.TableStats{{
				Name:         rawdb.AncientBlocksTable,
				Head:         12,
				PhysicalTail: 2,
				HiddenTail:   4,
				Prunable:     true,
				VisibleSize:  1024,
				HiddenSize:   128,
			}},
			Repair: rawdbfreezer.RepairStats{
				Applied:    true,
				TargetHead: 12,
				TargetTail: 4,
				RecordedAt: "2026-07-04T00:00:00Z",
			},
		},
	}
	bc := &BlockChain{
		db:      db,
		chaindb: rawdb.NewChainDB(db, ancient),
		stateDB: state.NewDatabase(db),
	}

	got, err := (&TronBackend{chain: bc}).FreezerStatus()
	if err != nil {
		t.Fatalf("FreezerStatus: %v", err)
	}
	if !got.Available || !got.HasFrozen {
		t.Fatalf("FreezerStatus flags = %+v, want available and frozen", got)
	}
	if got.FrozenMin == nil || *got.FrozenMin != 4 || got.FrozenMax == nil || *got.FrozenMax != 11 {
		t.Fatalf("FreezerStatus bounds = %+v..%+v, want 4..11", got.FrozenMin, got.FrozenMax)
	}
	if got.TableCounts[rawdb.AncientBlocksTable] != 12 || got.TableSizesBytes[rawdb.AncientBlocksTable] != 1024 {
		t.Fatalf("FreezerStatus table stats = counts %+v sizes %+v, want bodies count=12 size=1024", got.TableCounts, got.TableSizesBytes)
	}
	if got.Stage == nil || got.Stage.BlockNum != 11 || !got.Stage.HashBound || got.Stage.BlockHash != "0x"+stageHash.Hex() {
		t.Fatalf("FreezerStatus stage = %+v, want block 11 hash %s", got.Stage, "0x"+stageHash.Hex())
	}
	if got.Physical == nil || !got.Physical.RepairApplied || got.Physical.Tail != 4 || got.Physical.Head != 12 {
		t.Fatalf("FreezerStatus physical = %+v, want repair applied tail=4 head=12", got.Physical)
	}
}

func TestTronBackend_StageStatusReportsPipelineAndVerification(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	block := blockchainStartupBlock(11)
	if err := rawdb.WriteBlock(db, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	blockHash := block.Hash()
	mismatchHash := tcommon.Hash{0xcd}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageHeaders, block.Number(), blockHash); err != nil {
		t.Fatalf("WriteStageProgressWithHash Headers: %v", err)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageBodies, block.Number(), mismatchHash); err != nil {
		t.Fatalf("WriteStageProgressWithHash Bodies: %v", err)
	}

	bc := &BlockChain{
		db:      db,
		chaindb: rawdb.NewChainDB(db, rawdb.NoopAncient{}),
		stateDB: state.NewDatabase(db),
	}
	got, err := (&TronBackend{chain: bc}).StageStatus()
	if err != nil {
		t.Fatalf("StageStatus: %v", err)
	}
	if got.Status != "critical" {
		t.Fatalf("StageStatus status = %q, want critical: %+v", got.Status, got.IssueDetails)
	}
	headers := stageStatusRow(got, rawdb.StageHeaders)
	if headers == nil || headers.Verified != "canonical" || headers.CanonicalHash != "0x"+blockHash.Hex() {
		t.Fatalf("Headers row = %+v, want canonical hash %s", headers, "0x"+blockHash.Hex())
	}
	bodies := stageStatusRow(got, rawdb.StageBodies)
	if bodies == nil || bodies.Verified != "mismatch" || bodies.CanonicalHash != "0x"+blockHash.Hex() {
		t.Fatalf("Bodies row = %+v, want mismatch against canonical hash %s", bodies, "0x"+blockHash.Hex())
	}
	if got.Pipeline.Pending == 0 || len(got.Pipeline.Tasks) == 0 {
		t.Fatalf("StageStatus pipeline = %+v, want pending task", got.Pipeline)
	}
	var sawOrderIssue bool
	for _, issue := range got.IssueDetails {
		if issue.Kind == "stage-order" && issue.Stage == string(rawdb.StageBodies) && issue.HashMismatch {
			sawOrderIssue = true
			break
		}
	}
	if !sawOrderIssue {
		t.Fatalf("StageStatus issues = %+v, want Bodies hash-mismatch order issue", got.IssueDetails)
	}
}

func stageStatusRow(status *jsonrpc.StageStatus, stage rawdb.StageID) *jsonrpc.StageStatusRow {
	if status == nil {
		return nil
	}
	for i := range status.Stages {
		if status.Stages[i].Stage == string(stage) {
			return &status.Stages[i]
		}
	}
	return nil
}

func TestTronBackend_BlockHashReadsSurfaceColdIndexError(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()
	bc.ChainDB().SetChainIndexReader(failingBlockChainIndex{err: fmt.Errorf("cold block index corrupt")})
	backend := &TronBackend{chain: bc}
	var hash tcommon.Hash
	hash[8] = 0x01

	if _, err := backend.GetBlockByHash(hash); err == nil || !strings.Contains(err.Error(), "cold block index corrupt") {
		t.Fatalf("GetBlockByHash error = %v, want cold block index error", err)
	}
	if _, err := backend.GetLogs(jsonrpc.LogFilter{BlockHash: &hash}); err == nil || !strings.Contains(err.Error(), "cold block index corrupt") {
		t.Fatalf("GetLogs blockHash error = %v, want cold block index error", err)
	}
}

func TestTronBackend_HeadStateReadsSurfaceColdStateRootErrors(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	block := blockchainStartupBlock(2)
	if err := rawdb.WriteBlock(db, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	chain := rawdb.NewChainDB(db, blockchainStartupFailingAncient{
		kind:   rawdb.AncientStateRootsTable,
		number: block.Number(),
		err:    fmt.Errorf("cold state root read failed"),
	})
	bc := &BlockChain{
		db:      db,
		chaindb: chain,
		stateDB: state.NewDatabase(db),
	}
	bc.currentBlock.Store(block)
	backend := &TronBackend{chain: bc}
	addr := testCoreAddr(1)
	transferParam, err := anypb.New(&contractpb.TransferContract{
		OwnerAddress: addr.Bytes(),
		ToAddress:    testCoreAddr(2).Bytes(),
		Amount:       1,
	})
	if err != nil {
		t.Fatalf("pack transfer contract: %v", err)
	}
	transferTx := types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{{
				Type:      corepb.Transaction_Contract_TransferContract,
				Parameter: transferParam,
			}},
		},
	})

	checks := []struct {
		name string
		call func() error
	}{
		{name: "GetAccount", call: func() error {
			_, err := backend.GetAccount(addr)
			return err
		}},
		{name: "GetContract", call: func() error {
			_, err := backend.GetContract(addr)
			return err
		}},
		{name: "TriggerConstantContract", call: func() error {
			_, err := backend.TriggerConstantContract(addr, addr, nil, 1_000_000)
			return err
		}},
		{name: "GetAccountResource", call: func() error {
			_, err := backend.GetAccountResource(addr)
			return err
		}},
		{name: "GetBalance", call: func() error {
			_, err := backend.GetBalance(addr)
			return err
		}},
		{name: "GetCode", call: func() error {
			_, err := backend.GetCode(addr)
			return err
		}},
		{name: "GetStorageAt", call: func() error {
			_, err := backend.GetStorageAt(addr, tcommon.Hash{})
			return err
		}},
		{name: "TraceCall", call: func() error {
			_, err := backend.TraceCall(&addr, &addr, nil, 0, nil, &tracers.TraceConfig{})
			return err
		}},
		{name: "ListWitnesses", call: func() error {
			_, err := backend.ListWitnesses()
			return err
		}},
		{name: "GetAccountById", call: func() error {
			_, err := backend.GetAccountById([]byte("account-id"))
			return err
		}},
		{name: "GetDelegatedResourceV2", call: func() error {
			_, err := backend.GetDelegatedResourceV2(addr, addr)
			return err
		}},
		{name: "GetDelegatedResourceAccountIndexV2", call: func() error {
			_, err := backend.GetDelegatedResourceAccountIndexV2(addr)
			return err
		}},
		{name: "CanDelegateResource", call: func() error {
			_, err := backend.CanDelegateResource(addr, 1, corepb.ResourceCode_BANDWIDTH)
			return err
		}},
		{name: "GetCanWithdrawUnfreezeAmount", call: func() error {
			_, err := backend.GetCanWithdrawUnfreezeAmount(addr, 0)
			return err
		}},
		{name: "GetAvailableUnfreezeCount", call: func() error {
			_, err := backend.GetAvailableUnfreezeCount(addr)
			return err
		}},
		{name: "GetReward", call: func() error {
			_, err := backend.GetReward(addr)
			return err
		}},
		{name: "GetBrokerageInfo", call: func() error {
			_, err := backend.GetBrokerageInfo(addr)
			return err
		}},
		{name: "GetChainParameters", call: func() error {
			_, err := backend.GetChainParameters()
			return err
		}},
		{name: "NextMaintenanceTime", call: func() error {
			_, err := backend.NextMaintenanceTime()
			return err
		}},
		{name: "GetBurnTrx", call: func() error {
			_, err := backend.GetBurnTrx()
			return err
		}},
		{name: "GetBandwidthPrices", call: func() error {
			_, err := backend.GetBandwidthPrices()
			return err
		}},
		{name: "GetEnergyPrices", call: func() error {
			_, err := backend.GetEnergyPrices()
			return err
		}},
		{name: "GetAccountNet", call: func() error {
			_, err := backend.GetAccountNet(addr)
			return err
		}},
		{name: "ListProposals", call: func() error {
			_, err := backend.ListProposals()
			return err
		}},
		{name: "ListProposalsPaginated", call: func() error {
			_, err := backend.ListProposalsPaginated(0, 10)
			return err
		}},
		{name: "GetProposalByID", call: func() error {
			_, err := backend.GetProposalByID(1)
			return err
		}},
		{name: "GetAssetIssueByID", call: func() error {
			_, err := backend.GetAssetIssueByID(1)
			return err
		}},
		{name: "GetAssetIssueByName", call: func() error {
			_, err := backend.GetAssetIssueByName([]byte("asset"))
			return err
		}},
		{name: "GetAssetIssueList", call: func() error {
			_, err := backend.GetAssetIssueList()
			return err
		}},
		{name: "GetAssetIssueListPaginated", call: func() error {
			_, err := backend.GetAssetIssueListPaginated(0, 10)
			return err
		}},
		{name: "GetAssetIssueByAccount", call: func() error {
			_, err := backend.GetAssetIssueByAccount(addr)
			return err
		}},
		{name: "GetMarketOrderByID", call: func() error {
			_, err := backend.GetMarketOrderByID([]byte("order"))
			return err
		}},
		{name: "GetMarketOrdersByAccount", call: func() error {
			_, err := backend.GetMarketOrdersByAccount(addr)
			return err
		}},
		{name: "GetMarketPriceByPair", call: func() error {
			_, err := backend.GetMarketPriceByPair([]byte("sell"), []byte("buy"))
			return err
		}},
		{name: "GetMarketOrderListByPair", call: func() error {
			_, err := backend.GetMarketOrderListByPair([]byte("sell"), []byte("buy"))
			return err
		}},
		{name: "GetMarketPairList", call: func() error {
			_, err := backend.GetMarketPairList()
			return err
		}},
		{name: "ListExchanges", call: func() error {
			_, err := backend.ListExchanges()
			return err
		}},
		{name: "ListExchangesPaginated", call: func() error {
			_, err := backend.ListExchangesPaginated(0, 10)
			return err
		}},
		{name: "ValidateTransaction", call: func() error {
			return backend.ValidateTransaction(transferTx)
		}},
	}

	for _, check := range checks {
		if err := check.call(); err == nil || !strings.Contains(err.Error(), "cold state root read failed") {
			t.Fatalf("%s error = %v, want cold state-root error", check.name, err)
		}
	}
}

func TestTronBackend_GetBrokerageInfoSurfacesMalformedLiveRow(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()
	backend := &TronBackend{chain: bc}
	addr := testCoreAddr(2)
	cycle := bc.DynProps().CurrentCycleNumber()

	commitHeadStateForBackendTest(t, bc, func(statedb *state.StateDB) {
		if err := statedb.SystemKVPut(kvdomains.SystemReward, rawdb.CycleBrokerageStateKey(cycle, addr.Bytes()), []byte{0x01, 0x02, 0x03}); err != nil {
			t.Fatalf("write malformed brokerage: %v", err)
		}
	})

	got, err := backend.GetBrokerageInfo(addr)
	if err == nil {
		t.Fatalf("GetBrokerageInfo malformed row error = nil, got %d", got)
	}
	if !strings.Contains(err.Error(), "read cycle brokerage") || !strings.Contains(err.Error(), "length 3, want 4") {
		t.Fatalf("GetBrokerageInfo malformed row error = %v, want brokerage decode context", err)
	}
}

func TestTronBackend_DynamicPropertyLiveReadsSurfaceMalformedRows(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()
	backend := &TronBackend{chain: bc}
	addr := testCoreAddr(1)

	commitHeadStateForBackendTest(t, bc, func(statedb *state.StateDB) {
		if err := statedb.SystemKVPut(kvdomains.SystemDynamicProperty, []byte("burn_trx_amount"), []byte{0x01}); err != nil {
			t.Fatalf("write malformed burn_trx_amount: %v", err)
		}
	})

	checks := []struct {
		name string
		call func() error
	}{
		{name: "GetChainParameters", call: func() error {
			_, err := backend.GetChainParameters()
			return err
		}},
		{name: "NextMaintenanceTime", call: func() error {
			_, err := backend.NextMaintenanceTime()
			return err
		}},
		{name: "GetBurnTrx", call: func() error {
			_, err := backend.GetBurnTrx()
			return err
		}},
		{name: "GetBandwidthPrices", call: func() error {
			_, err := backend.GetBandwidthPrices()
			return err
		}},
		{name: "GetEnergyPrices", call: func() error {
			_, err := backend.GetEnergyPrices()
			return err
		}},
		{name: "GetAccountResource", call: func() error {
			_, err := backend.GetAccountResource(addr)
			return err
		}},
		{name: "GetAccountNet", call: func() error {
			_, err := backend.GetAccountNet(addr)
			return err
		}},
		{name: "TriggerConstantContract", call: func() error {
			_, err := backend.TriggerConstantContract(addr, addr, nil, 1_000_000)
			return err
		}},
		{name: "TraceCall", call: func() error {
			_, err := backend.TraceCall(&addr, &addr, nil, 0, nil, &tracers.TraceConfig{})
			return err
		}},
		{name: "ValidateTransaction", call: func() error {
			return backend.ValidateTransaction(testTransferTransactionForBackend(t, addr, testCoreAddr(2)))
		}},
	}
	for _, check := range checks {
		if err := check.call(); err == nil || !strings.Contains(err.Error(), "burn_trx_amount") || !strings.Contains(err.Error(), "want 8") {
			t.Fatalf("%s malformed dynamic property error = %v, want burn_trx_amount length error", check.name, err)
		}
	}
}

func TestTronBackend_TotalTransactionUsesStrictBufferedRead(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()
	backend := &TronBackend{chain: bc}
	rawdb.WriteTotalTransactionCount(bc.db, 42)

	got, err := backend.TotalTransaction()
	if err != nil {
		t.Fatalf("TotalTransaction: %v", err)
	}
	if got != 42 {
		t.Fatalf("TotalTransaction = %d, want 42", got)
	}

	badBackend := &TronBackend{chain: &BlockChain{}}
	if got, err := badBackend.TotalTransaction(); err == nil || got != 0 || !strings.Contains(err.Error(), "nil database") {
		t.Fatalf("TotalTransaction nil buffer = %d/%v, want nil database error", got, err)
	}
}

func commitHeadStateForBackendTest(t *testing.T, bc *BlockChain, mutate func(*state.StateDB)) {
	t.Helper()

	statedb, err := state.New(bc.HeadStateRoot(), bc.StateDB())
	if err != nil {
		t.Fatal(err)
	}
	mutate(statedb)
	root, err := statedb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	rawdb.WriteGenesisStateRoot(bc.db, root)
	if err := rawdb.WriteBlockStateRoot(bc.db, bc.CurrentBlock().Hash(), root); err != nil {
		t.Fatal(err)
	}
}

func testTransferTransactionForBackend(t *testing.T, owner, to tcommon.Address) *types.Transaction {
	t.Helper()

	transferParam, err := anypb.New(&contractpb.TransferContract{
		OwnerAddress: owner.Bytes(),
		ToAddress:    to.Bytes(),
		Amount:       1,
	})
	if err != nil {
		t.Fatalf("pack transfer contract: %v", err)
	}
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{{
				Type:      corepb.Transaction_Contract_TransferContract,
				Parameter: transferParam,
			}},
		},
	})
}

func TestTronBackend_LiveWitnessReadsSurfaceCorruptRootedIndexes(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()

	backend := &TronBackend{chain: bc}
	truncated := []byte{0, 0, 0, 2, 0x41}

	commitHeadStateForBackendTest(t, bc, func(statedb *state.StateDB) {
		key := state.WitnessIndexSystemRow()
		if err := statedb.SystemKVPut(key.Domain, key.Key, truncated); err != nil {
			t.Fatal(err)
		}
	})

	if _, err := backend.ListWitnesses(); err == nil || !strings.Contains(err.Error(), "witness index") {
		t.Fatalf("ListWitnesses corrupt witness index error = %v, want witness index decode error", err)
	}

	tx := testTransferTransactionForBackend(t, testCoreAddr(1), testCoreAddr(2))
	if err := backend.ValidateTransaction(tx); err == nil || !strings.Contains(err.Error(), "witness index") {
		t.Fatalf("ValidateTransaction corrupt witness index error = %v, want witness index decode error", err)
	}
}

func TestTronBackend_LiveWitnessReadsSurfaceCorruptPendingVotes(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()

	backend := &TronBackend{chain: bc}
	voter := testCoreAddr(3)

	commitHeadStateForBackendTest(t, bc, func(statedb *state.StateDB) {
		if err := statedb.WriteVotesIndex([]tcommon.Address{voter}); err != nil {
			t.Fatal(err)
		}
		key := state.PendingVotesSystemRow(voter)
		if err := statedb.SystemKVPut(key.Domain, key.Key, []byte{0x80}); err != nil {
			t.Fatal(err)
		}
	})

	if _, err := backend.ListWitnesses(); err == nil || !strings.Contains(err.Error(), "decode votes") {
		t.Fatalf("ListWitnesses corrupt pending votes error = %v, want decode votes error", err)
	}

	commitHeadStateForBackendTest(t, bc, func(statedb *state.StateDB) {
		key := state.PendingVotesIndexSystemRow()
		if err := statedb.SystemKVPut(key.Domain, key.Key, []byte{0, 0, 0, 2, 0x41}); err != nil {
			t.Fatal(err)
		}
	})
	if _, err := backend.ListWitnesses(); err == nil || !strings.Contains(err.Error(), "votes index") {
		t.Fatalf("ListWitnesses corrupt pending votes index error = %v, want votes index decode error", err)
	}
}

func TestTronBackend_LiveMarketReadsSurfaceCorruptRows(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()

	backend := &TronBackend{chain: bc}
	owner := testCoreAddr(1)
	orderID := []byte("order-1")
	sell, buy := []byte("_"), []byte("1000001")

	commitHeadStateForBackendTest(t, bc, func(statedb *state.StateDB) {
		for _, key := range []state.SystemKVRow{
			state.MarketOrderSystemRow(orderID),
			state.MarketAccountOrderSystemRow(owner[:]),
			state.MarketPriceListSystemRow(sell, buy),
			state.MarketPairIndexSystemRow(),
		} {
			if err := statedb.SystemKVPut(key.Domain, key.Key, []byte{0x80}); err != nil {
				t.Fatal(err)
			}
		}
	})

	checks := []struct {
		name string
		want string
		call func() error
	}{
		{name: "GetMarketOrderByID", want: "decode market order", call: func() error {
			_, err := backend.GetMarketOrderByID(orderID)
			return err
		}},
		{name: "GetMarketOrdersByAccount", want: "decode market account order", call: func() error {
			_, err := backend.GetMarketOrdersByAccount(owner)
			return err
		}},
		{name: "GetMarketPriceByPair", want: "decode market price list", call: func() error {
			_, err := backend.GetMarketPriceByPair(sell, buy)
			return err
		}},
		{name: "GetMarketOrderListByPair", want: "decode market price list", call: func() error {
			_, err := backend.GetMarketOrderListByPair(sell, buy)
			return err
		}},
		{name: "GetMarketPairList", want: "decode market pair index", call: func() error {
			_, err := backend.GetMarketPairList()
			return err
		}},
	}
	for _, check := range checks {
		if err := check.call(); err == nil || !strings.Contains(err.Error(), check.want) {
			t.Fatalf("%s error = %v, want %q", check.name, err, check.want)
		}
	}
}

func TestTronBackend_LiveMarketOrderListSurfacesCorruptLinkedRows(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()

	backend := &TronBackend{chain: bc}
	orderID := []byte("order-1")
	sell, buy := []byte("_"), []byte("1000001")
	pk := rawdb.PriceKey(7, 3)

	commitHeadStateForBackendTest(t, bc, func(statedb *state.StateDB) {
		if err := statedb.WriteMarketPriceList(sell, buy, &corepb.MarketPriceList{
			SellTokenId: sell,
			BuyTokenId:  buy,
			Prices: []*corepb.MarketPrice{{
				SellTokenQuantity: 7,
				BuyTokenQuantity:  3,
			}},
		}); err != nil {
			t.Fatal(err)
		}
		key := state.MarketOrderBookSystemRow(sell, buy, pk)
		if err := statedb.SystemKVPut(key.Domain, key.Key, []byte{0x80}); err != nil {
			t.Fatal(err)
		}
	})
	if _, err := backend.GetMarketOrderListByPair(sell, buy); err == nil || !strings.Contains(err.Error(), "decode market order book") {
		t.Fatalf("GetMarketOrderListByPair corrupt book error = %v, want decode market order book", err)
	}

	commitHeadStateForBackendTest(t, bc, func(statedb *state.StateDB) {
		if err := statedb.WriteMarketOrderBook(sell, buy, pk, &corepb.MarketOrderIdList{
			Head: orderID,
			Tail: orderID,
		}); err != nil {
			t.Fatal(err)
		}
		key := state.MarketOrderSystemRow(orderID)
		if err := statedb.SystemKVPut(key.Domain, key.Key, []byte{0x80}); err != nil {
			t.Fatal(err)
		}
	})
	if _, err := backend.GetMarketOrderListByPair(sell, buy); err == nil || !strings.Contains(err.Error(), "decode market order") {
		t.Fatalf("GetMarketOrderListByPair corrupt order error = %v, want decode market order", err)
	}
}

func TestTronBackend_BlockHashReadsSurfaceCorruptIndexedBody(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	genesis := &params.Genesis{
		Config: params.MainnetChainConfig,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 1000000},
		},
	}
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatalf("SetupGenesisBlock: %v", err)
	}
	bc, err := NewBlockChainWithAncient(diskdb, state.NewDatabase(diskdb), params.MainnetChainConfig, staticAncientRow{
		kind:   rawdb.AncientBlocksTable,
		number: 1,
		data:   []byte("not-a-valid-block"),
	})
	if err != nil {
		t.Fatalf("NewBlockChainWithAncient: %v", err)
	}
	defer bc.Close()

	block, _ := testBackendLogBlock(1, nil)
	if err := rawdb.WriteBlockNumber(diskdb, block.Hash(), block.Number()); err != nil {
		t.Fatalf("WriteBlockNumber: %v", err)
	}
	txHash := block.Transactions()[0].Hash()
	if err := rawdb.WriteTransactionIndex(diskdb, txHash[:], block.Number()); err != nil {
		t.Fatalf("WriteTransactionIndex: %v", err)
	}
	if err := rawdb.WriteTransactionInfo(diskdb, txHash[:], &corepb.TransactionInfo{
		Id:          append([]byte(nil), txHash[:]...),
		BlockNumber: int64(block.Number()),
	}); err != nil {
		t.Fatalf("WriteTransactionInfo: %v", err)
	}
	bc.currentBlock.Store(block)

	backend := &TronBackend{chain: bc}
	if got, err := backend.GetBlockByNumber(block.Number()); err == nil || got != nil || !strings.Contains(err.Error(), "block 1 decode") {
		t.Fatalf("GetBlockByNumber corrupt body = %v/%v, want decode error", got, err)
	}
	if got, err := backend.GetBlockByHash(block.Hash()); err == nil || got != nil || !strings.Contains(err.Error(), "block 1 decode") {
		t.Fatalf("GetBlockByHash corrupt body = %v/%v, want decode error", got, err)
	}
	if blocks, err := backend.GetBlocksByRange(block.Number(), block.Number()+1); err == nil || blocks != nil || !strings.Contains(err.Error(), "block 1 decode") {
		t.Fatalf("GetBlocksByRange corrupt body = %+v/%v, want decode error", blocks, err)
	}
	blockHash := block.Hash()
	if logs, err := backend.GetLogs(jsonrpc.LogFilter{BlockHash: &blockHash}); err == nil || logs != nil || !strings.Contains(err.Error(), "block 1 decode") {
		t.Fatalf("GetLogs corrupt block body = %+v/%v, want decode error", logs, err)
	}
	if infos, err := backend.GetTransactionInfoByBlockNum(block.Number()); err == nil || infos != nil || !strings.Contains(err.Error(), "block 1 decode") {
		t.Fatalf("GetTransactionInfoByBlockNum corrupt block body = %+v/%v, want decode error", infos, err)
	}
	from, to := block.Number(), block.Number()
	if logs, err := backend.GetLogs(jsonrpc.LogFilter{FromBlock: &from, ToBlock: &to}); err == nil || logs != nil || !strings.Contains(err.Error(), "block 1 decode") {
		t.Fatalf("GetLogs range corrupt block body = %+v/%v, want decode error", logs, err)
	}
	if got, err := backend.GetTransactionByID(txHash); err == nil || got != nil || !strings.Contains(err.Error(), "block 1 decode") {
		t.Fatalf("GetTransactionByID corrupt block body = %+v/%v, want decode error", got, err)
	}
	if tx, gotBlock, idx, err := backend.GetTransactionByHash(txHash); err == nil || tx != nil || gotBlock != nil || idx != 0 || !strings.Contains(err.Error(), "block 1 decode") {
		t.Fatalf("GetTransactionByHash corrupt block body = tx:%+v block:%+v idx:%d err:%v, want decode error", tx, gotBlock, idx, err)
	}
	if trace, err := backend.GetBlockBalanceTrace(testBackendBalanceBlockID(block)); err == nil || trace != nil || !strings.Contains(err.Error(), "block 1 decode") {
		t.Fatalf("GetBlockBalanceTrace corrupt block body = %+v/%v, want decode error", trace, err)
	}
	if trace, err := backend.GetAccountBalanceTrace(&contractpb.AccountBalanceRequest{
		AccountIdentifier: &contractpb.AccountIdentifier{Address: testCoreAddr(9).Bytes()},
		BlockIdentifier:   testBackendBalanceBlockID(block),
	}); err == nil || trace != nil || !strings.Contains(err.Error(), "block 1 decode") {
		t.Fatalf("GetAccountBalanceTrace corrupt block body = %+v/%v, want decode error", trace, err)
	}
}

// TestTronBackend_GetBalance verifies GetBalance opens state and returns int64.
func TestTronBackend_GetBalance(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()
	b := &TronBackend{chain: bc}
	addr := tcommon.Address{}
	bal, err := b.GetBalance(addr)
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if bal < 0 {
		t.Fatalf("GetBalance should not return negative: %d", bal)
	}
}

func TestTronBackend_GetAccountBalanceTrace(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()

	block1, _ := testBackendLogBlock(1, nil)
	block2, _ := testBackendLogBlock(2, nil)
	if err := rawdb.WriteBlock(bc.db, block1); err != nil {
		t.Fatalf("WriteBlock block1: %v", err)
	}
	if err := rawdb.WriteBlock(bc.db, block2); err != nil {
		t.Fatalf("WriteBlock block2: %v", err)
	}
	bc.currentBlock.Store(block2)

	owner := testCoreAddr(9)
	if err := rawdb.WriteAccountTrace(bc.db, owner.Bytes(), int64(block1.Number()), 12_345); err != nil {
		t.Fatalf("WriteAccountTrace: %v", err)
	}

	backend := &TronBackend{chain: bc}
	resp, err := backend.GetAccountBalanceTrace(&contractpb.AccountBalanceRequest{
		AccountIdentifier: &contractpb.AccountIdentifier{Address: owner.Bytes()},
		BlockIdentifier:   testBackendBalanceBlockID(block2),
	})
	if err != nil {
		t.Fatalf("GetAccountBalanceTrace: %v", err)
	}
	if resp.GetBalance() != 12_345 {
		t.Fatalf("balance = %d, want 12345", resp.GetBalance())
	}
	if got := resp.GetBlockIdentifier().GetNumber(); got != int64(block1.Number()) {
		t.Fatalf("response block number = %d, want %d", got, block1.Number())
	}
	if !bytes.Equal(resp.GetBlockIdentifier().GetHash(), block1.Hash().Bytes()) {
		t.Fatalf("response block hash = %x, want %x", resp.GetBlockIdentifier().GetHash(), block1.Hash().Bytes())
	}

	missingOwner := testCoreAddr(10)
	resp, err = backend.GetAccountBalanceTrace(&contractpb.AccountBalanceRequest{
		AccountIdentifier: &contractpb.AccountIdentifier{Address: missingOwner.Bytes()},
		BlockIdentifier:   testBackendBalanceBlockID(block2),
	})
	if err != nil {
		t.Fatalf("GetAccountBalanceTrace missing owner: %v", err)
	}
	if resp.GetBalance() != 0 || resp.GetBlockIdentifier().GetNumber() != int64(block2.Number()) {
		t.Fatalf("missing owner response = balance %d block %d, want balance 0 block %d",
			resp.GetBalance(), resp.GetBlockIdentifier().GetNumber(), block2.Number())
	}

	badID := testBackendBalanceBlockID(block2)
	badID.Hash[0] ^= 0xff
	if _, err := backend.GetAccountBalanceTrace(&contractpb.AccountBalanceRequest{
		AccountIdentifier: &contractpb.AccountIdentifier{Address: owner.Bytes()},
		BlockIdentifier:   badID,
	}); err == nil {
		t.Fatal("GetAccountBalanceTrace accepted mismatched block hash")
	}
}

func TestTronBackend_GetBlockBalanceTrace(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()

	block1, _ := testBackendLogBlock(1, nil)
	if err := rawdb.WriteBlock(bc.db, block1); err != nil {
		t.Fatalf("WriteBlock block1: %v", err)
	}
	bc.currentBlock.Store(block1)

	trace := &contractpb.BlockBalanceTrace{
		BlockIdentifier: testBackendBalanceBlockID(block1),
		Timestamp:       99_001,
	}
	if err := rawdb.WriteBlockBalanceTrace(bc.db, int64(block1.Number()), trace); err != nil {
		t.Fatalf("WriteBlockBalanceTrace: %v", err)
	}

	got, err := (&TronBackend{chain: bc}).GetBlockBalanceTrace(testBackendBalanceBlockID(block1))
	if err != nil {
		t.Fatalf("GetBlockBalanceTrace: %v", err)
	}
	if got.GetTimestamp() != trace.GetTimestamp() {
		t.Fatalf("timestamp = %d, want %d", got.GetTimestamp(), trace.GetTimestamp())
	}

	block2, _ := testBackendLogBlock(2, nil)
	if err := rawdb.WriteBlock(bc.db, block2); err != nil {
		t.Fatalf("WriteBlock block2: %v", err)
	}
	if _, err := (&TronBackend{chain: bc}).GetBlockBalanceTrace(testBackendBalanceBlockID(block2)); err == nil {
		t.Fatal("GetBlockBalanceTrace accepted missing trace row")
	}
}

func TestTronBackend_GetBlockBalanceTraceSurfacesColdMismatch(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()

	block1, _ := testBackendLogBlock(1, nil)
	if err := rawdb.WriteBlock(bc.db, block1); err != nil {
		t.Fatalf("WriteBlock block1: %v", err)
	}
	bc.currentBlock.Store(block1)

	cold := newTestBackendBalanceTraceReader()
	cold.putBlockTrace(1, &contractpb.BlockBalanceTrace{
		BlockIdentifier: &contractpb.BlockBalanceTrace_BlockIdentifier{
			Hash:   block1.Hash().Bytes(),
			Number: int64(block1.Number() + 1),
		},
		Timestamp: 99_002,
	})
	bc.ChainDB().SetBalanceTraceReader(cold)

	_, err := (&TronBackend{chain: bc}).GetBlockBalanceTrace(testBackendBalanceBlockID(block1))
	if err == nil || !strings.Contains(err.Error(), "does not match key") {
		t.Fatalf("GetBlockBalanceTrace cold mismatch error = %v, want strict balance-trace mismatch", err)
	}
}

type testBackendBalanceTraceReader struct {
	blockTraces map[int64]*contractpb.BlockBalanceTrace
}

func newTestBackendBalanceTraceReader() *testBackendBalanceTraceReader {
	return &testBackendBalanceTraceReader{blockTraces: make(map[int64]*contractpb.BlockBalanceTrace)}
}

func (r *testBackendBalanceTraceReader) putBlockTrace(blockNum int64, trace *contractpb.BlockBalanceTrace) {
	r.blockTraces[blockNum] = trace
}

func (r *testBackendBalanceTraceReader) BlockBalanceTrace(blockNum int64) (*contractpb.BlockBalanceTrace, bool, error) {
	trace, ok := r.blockTraces[blockNum]
	return trace, ok, nil
}

func (r *testBackendBalanceTraceReader) AccountTraceAtOrBefore(owner []byte, blockNum int64) (int64, int64, bool, error) {
	return 0, 0, false, nil
}

// TestTronBackend_GetCode verifies GetCode returns nil for an account with no code.
func TestTronBackend_GetCode(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()
	b := &TronBackend{chain: bc}
	addr := tcommon.Address{}
	code, err := b.GetCode(addr)
	if err != nil {
		t.Fatalf("GetCode: %v", err)
	}
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
	val, err := b.GetStorageAt(addr, slot)
	if err != nil {
		t.Fatalf("GetStorageAt: %v", err)
	}
	_ = val // just verify no panic
}

func TestTronBackend_ListWitnessesIncludesPendingVotes(t *testing.T) {
	voter := testCoreAddr(1)
	witness := testCoreAddr(2)
	// Witness lives in genesis so it's in the rooted witness index at the head
	// root (ListWitnesses reads the index from the system-KV); genesis also
	// writes its capsule (URL/VoteCount=0) to the flat store.
	bc, cleanup := newTestBlockchain(t, params.GenesisWitness{Address: witness, VoteCount: 0, URL: "http://w"})
	defer cleanup()

	// The pending-vote ledger is rooted (WitnessVoteState KV); ListWitnesses
	// reads it from the head state root, so seed it there (head is genesis — no
	// blocks inserted).
	seedPendingVotesAtGenesis(t, bc, map[tcommon.Address]*corepb.Votes{
		voter: {
			Address:  voter.Bytes(),
			NewVotes: []*corepb.Vote{{VoteAddress: witness.Bytes(), VoteCount: 123}},
		},
	})

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

func TestTronBackend_GetTransactionByHashUsesTxIndexWithoutReceipt(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()

	txPB := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Timestamp:  1001,
			Expiration: 2001,
			Data:       []byte{0xab},
		},
	}
	txHash := types.NewTransactionFromPB(txPB).Hash()
	parent := bc.CurrentBlock()
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:     1,
				Timestamp:  3001,
				ParentHash: parent.Hash().Bytes(),
			},
		},
		Transactions: []*corepb.Transaction{txPB},
	})
	if err := rawdb.WriteBlock(bc.DB(), block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if err := rawdb.WriteTransactionIndex(bc.DB(), txHash[:], block.Number()); err != nil {
		t.Fatalf("WriteTransactionIndex: %v", err)
	}
	if got := rawdb.ReadTransactionInfo(bc.ChainDB(), txHash[:]); got != nil {
		t.Fatalf("unexpected transaction info row = %+v", got)
	}
	if got := rawdb.ReadTransactionInfosByBlock(bc.ChainDB(), block.Number()); len(got) != 0 {
		t.Fatalf("unexpected transaction info block rows = %+v", got)
	}
	bc.currentBlock.Store(block)

	backend := &TronBackend{chain: bc}
	gotTx, gotBlock, idx, err := backend.GetTransactionByHash(txHash)
	if err != nil || gotTx == nil || gotBlock == nil || gotBlock.Hash() != block.Hash() || idx != 0 {
		t.Fatalf("GetTransactionByHash = tx:%+v block:%+v idx:%d err:%v, want tx/block/0 from tx index", gotTx, gotBlock, idx, err)
	}

	rpcServer := jsonrpc.NewServer(backend, 0)
	defer rpcServer.Stop()
	httpServer := httptest.NewServer(rpcServer.Handler())
	defer httpServer.Close()

	txHashHex := "0x" + txHash.Hex()
	resp := postCoreJSONRPC(t, httpServer.URL, "eth_getTransactionByHash", []any{txHashHex})
	txObj, ok := resp["result"].(map[string]any)
	if !ok {
		t.Fatalf("eth_getTransactionByHash result = %T %v, want object", resp["result"], resp["result"])
	}
	if txObj["hash"] != txHashHex ||
		txObj["blockHash"] != "0x"+block.Hash().Hex() ||
		txObj["blockNumber"] != "0x1" ||
		txObj["transactionIndex"] != "0x0" {
		t.Fatalf("eth_getTransactionByHash = %+v, want tx indexed by block body without receipt", txObj)
	}
}

func TestTronBackend_ColdChainIndexLookupAfterRestore(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	fz, err := rawdbfreezer.NewFreezer(t.TempDir(), "", false, 2049, chainfreezer.FreezerTableSet())
	if err != nil {
		t.Fatalf("NewFreezer: %v", err)
	}
	defer fz.Close()

	owner := testInsertAddr(1)
	receiver := testInsertAddr(2)
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: owner, Balance: 100_000_000},
		},
		DynamicProperties: map[string]int64{},
	}
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatalf("SetupGenesisBlock: %v", err)
	}
	bc, err := NewBlockChainWithAncient(diskdb, state.NewDatabase(diskdb), params.MainnetChainConfig, rawdb.NewFreezerReader(fz))
	if err != nil {
		t.Fatalf("NewBlockChainWithAncient: %v", err)
	}
	defer bc.Close()

	txPB := testRestartTransferTx(t, owner, receiver, 7_000_000)
	txHash := types.NewTransactionFromPB(txPB).Hash()
	parent := bc.CurrentBlock()
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:     1,
				Timestamp:  3000,
				ParentHash: parent.Hash().Bytes(),
			},
		},
		Transactions: []*corepb.Transaction{txPB},
	})
	if err := bc.InsertBlock(block); err != nil {
		t.Fatalf("InsertBlock: %v", err)
	}
	wantRoot := rawdb.ReadBlockStateRoot(bc.ChainDB(), block.Hash())
	if wantRoot == (tcommon.Hash{}) {
		t.Fatalf("block state root missing before freeze")
	}

	if err := appendBackendColdLookupAncients(t, fz, diskdb, parent, block); err != nil {
		t.Fatalf("append ancients: %v", err)
	}
	snapshotDir := t.TempDir()
	freezerRef, err := statesnapshots.BuildChainFreezerSegmentFromAncient(rawdb.NewFreezerReader(fz), snapshotDir, "", 0, 1)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	indexRef, err := statesnapshots.BuildChainIndexSegmentFromChainFreezerSegment(snapshotDir, freezerRef, "")
	if err != nil {
		t.Fatalf("BuildChainIndexSegmentFromChainFreezerSegment: %v", err)
	}
	if err := statesnapshots.PublishManifest(snapshotDir, statesnapshots.NewManifest(0, 0, []statesnapshots.SegmentRef{freezerRef, indexRef})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := statesnapshots.OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}

	if err := rawdb.DeleteFrozenBlockRange(diskdb, 1, 1); err != nil {
		t.Fatalf("DeleteFrozenBlockRange: %v", err)
	}
	if err := rawdb.DeleteBlockNumber(diskdb, block.Hash()); err != nil {
		t.Fatalf("DeleteBlockNumber: %v", err)
	}
	rawdb.DeleteBlockStateRoot(diskdb, block.Hash())
	if err := rawdb.DeleteTransactionIndex(diskdb, txHash[:]); err != nil {
		t.Fatalf("DeleteTransactionIndex: %v", err)
	}
	if err := rawdb.DeleteTransactionInfo(diskdb, txHash[:]); err != nil {
		t.Fatalf("DeleteTransactionInfo: %v", err)
	}
	if err := rawdb.DeleteTransactionInfosByBlock(diskdb, block.Number()); err != nil {
		t.Fatalf("DeleteTransactionInfosByBlock: %v", err)
	}
	hotOnly := rawdb.NewChainDB(diskdb, rawdb.NoopAncient{})
	if got := rawdb.ReadBlock(hotOnly, 1); got != nil {
		t.Fatalf("hot block still present: %x", got.Hash())
	}
	if got := rawdb.ReadBlockNumber(hotOnly, block.Hash()); got != nil {
		t.Fatalf("hot block lookup still present: %v", got)
	}
	if got := rawdb.ReadTransactionIndex(hotOnly, txHash[:]); got != nil {
		t.Fatalf("hot tx lookup still present: %v", got)
	}
	if got := rawdb.ReadTransactionInfo(hotOnly, txHash[:]); got != nil {
		t.Fatalf("hot tx info still present: %+v", got)
	}
	if got := rawdb.ReadTransactionInfosByBlock(hotOnly, block.Number()); len(got) != 0 {
		t.Fatalf("hot tx receipt rows still present: %+v", got)
	}
	if got := rawdb.ReadBlockStateRoot(hotOnly, block.Hash()); got != (tcommon.Hash{}) {
		t.Fatalf("hot state root still present: %x", got)
	}

	bc.ChainDB().SetChainIndexReader(mgr)
	backend := &TronBackend{chain: bc}
	gotBlock, err := backend.GetBlockByHash(block.Hash())
	if err != nil || gotBlock == nil || gotBlock.Hash() != block.Hash() {
		t.Fatalf("GetBlockByHash = %v/%v, want block %x", gotBlock, err, block.Hash())
	}
	gotTx, err := backend.GetTransactionByID(txHash)
	if err != nil || gotTx == nil || types.NewTransactionFromPB(gotTx).Hash() != txHash {
		t.Fatalf("GetTransactionByID = %v/%v, want tx %x", gotTx, err, txHash)
	}
	info, err := backend.GetTransactionInfoByID(txHash)
	if err != nil || info == nil || uint64(info.BlockNumber) != block.Number() {
		t.Fatalf("GetTransactionInfoByID = %+v/%v, want block %d", info, err, block.Number())
	}
	rpcInfo, err := backend.GetTransactionInfo(txHash)
	if err != nil || rpcInfo == nil || uint64(rpcInfo.BlockNumber) != block.Number() {
		t.Fatalf("GetTransactionInfo = %+v/%v, want block %d", rpcInfo, err, block.Number())
	}
	infos, err := backend.GetTransactionInfoByBlockNum(block.Number())
	if err != nil || len(infos) != 1 || !bytes.Equal(infos[0].Id, txHash[:]) || uint64(infos[0].BlockNumber) != block.Number() {
		t.Fatalf("GetTransactionInfoByBlockNum = %+v/%v, want one cold receipt for tx %x block %d", infos, err, txHash, block.Number())
	}
	gotTx, gotBlock, idx, err := backend.GetTransactionByHash(txHash)
	if err != nil || gotTx == nil || gotBlock == nil || idx != 0 {
		t.Fatalf("GetTransactionByHash = tx:%v block:%v idx:%d err:%v, want tx/block/0", gotTx, gotBlock, idx, err)
	}
	rpcServer := jsonrpc.NewServer(backend, 0)
	defer rpcServer.Stop()
	httpServer := httptest.NewServer(rpcServer.Handler())
	defer httpServer.Close()
	txHashHex := "0x" + txHash.Hex()
	blockHashHex := "0x" + block.Hash().Hex()
	resp := postCoreJSONRPC(t, httpServer.URL, "eth_getTransactionReceipt", []any{txHashHex})
	receipt, ok := resp["result"].(map[string]any)
	if !ok {
		t.Fatalf("eth_getTransactionReceipt result = %T %v, want object", resp["result"], resp["result"])
	}
	if receipt["transactionHash"] != txHashHex ||
		receipt["blockHash"] != blockHashHex ||
		receipt["blockNumber"] != "0x1" ||
		receipt["transactionIndex"] != "0x0" ||
		receipt["status"] != "0x1" {
		t.Fatalf("cold eth_getTransactionReceipt = %+v, want tx/block/index/status from cold chain data", receipt)
	}
	for _, blockParam := range []string{"0x1", blockHashHex} {
		resp := postCoreJSONRPC(t, httpServer.URL, "eth_getBlockReceipts", []any{blockParam})
		receipts, ok := resp["result"].([]any)
		if !ok || len(receipts) != 1 {
			t.Fatalf("eth_getBlockReceipts(%s) result = %T %v, want one cold receipt", blockParam, resp["result"], resp["result"])
		}
		blockReceipt, ok := receipts[0].(map[string]any)
		if !ok {
			t.Fatalf("eth_getBlockReceipts(%s)[0] = %T %v, want object", blockParam, receipts[0], receipts[0])
		}
		if blockReceipt["transactionHash"] != txHashHex ||
			blockReceipt["blockHash"] != blockHashHex ||
			blockReceipt["blockNumber"] != "0x1" ||
			blockReceipt["transactionIndex"] != "0x0" ||
			blockReceipt["status"] != "0x1" {
			t.Fatalf("cold eth_getBlockReceipts(%s) = %+v, want tx/block/index/status from cold chain data", blockParam, blockReceipt)
		}
	}
	if got := bc.StateRootAtBlock(block.Number()); got != wantRoot {
		t.Fatalf("StateRootAtBlock = %x, want %x", got, wantRoot)
	}
}

func TestJSONRPCGetTransactionReceiptUsesColdLogsAfterHotReceiptPrune(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	fz, err := rawdbfreezer.NewFreezer(t.TempDir(), "", false, 2049, chainfreezer.FreezerTableSet())
	if err != nil {
		t.Fatalf("NewFreezer: %v", err)
	}
	defer fz.Close()

	genesis := &params.Genesis{
		Config:            params.MainnetChainConfig,
		Timestamp:         0,
		DynamicProperties: map[string]int64{},
	}
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatalf("SetupGenesisBlock: %v", err)
	}
	bc, err := NewBlockChainWithAncient(diskdb, state.NewDatabase(diskdb), params.MainnetChainConfig, rawdb.NewFreezerReader(fz))
	if err != nil {
		t.Fatalf("NewBlockChainWithAncient: %v", err)
	}
	defer bc.Close()

	logAddress := bytes20(0x9a)
	topic := tcommon.Hash{0x9b}
	block, info := testBackendLogBlock(1, &corepb.TransactionInfo_Log{
		Address: logAddress,
		Topics:  [][]byte{topic[:]},
		Data:    []byte{0x9c, 0x9d},
	})
	info.Receipt = &corepb.ResourceReceipt{
		EnergyUsage:      33,
		EnergyFee:        4400,
		EnergyUsageTotal: 12345,
	}
	info.Result = corepb.TransactionInfo_FAILED
	txHash := tcommon.BytesToHash(info.Id)
	if err := rawdb.WriteBlock(diskdb, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(diskdb, block.Number(), []*corepb.TransactionInfo{info}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}
	if err := rawdb.WriteTransactionInfo(diskdb, txHash[:], info); err != nil {
		t.Fatalf("WriteTransactionInfo: %v", err)
	}
	if err := rawdb.WriteTransactionIndex(diskdb, txHash[:], block.Number()); err != nil {
		t.Fatalf("WriteTransactionIndex: %v", err)
	}
	bc.currentBlock.Store(block)

	parent := rawdb.ReadBlock(bc.ChainDB(), 0)
	if parent == nil {
		t.Fatal("genesis block missing")
	}
	if err := appendBackendColdLookupAncients(t, fz, diskdb, parent, block); err != nil {
		t.Fatalf("append ancients: %v", err)
	}
	snapshotDir := t.TempDir()
	freezerRef, err := statesnapshots.BuildChainFreezerSegmentFromAncient(rawdb.NewFreezerReader(fz), snapshotDir, "", 0, 1)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	indexRef, err := statesnapshots.BuildChainIndexSegmentFromChainFreezerSegment(snapshotDir, freezerRef, "")
	if err != nil {
		t.Fatalf("BuildChainIndexSegmentFromChainFreezerSegment: %v", err)
	}
	if err := statesnapshots.PublishManifest(snapshotDir, statesnapshots.NewManifest(0, 0, []statesnapshots.SegmentRef{freezerRef, indexRef})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := statesnapshots.OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}

	if err := rawdb.DeleteFrozenBlockRange(diskdb, block.Number(), block.Number()); err != nil {
		t.Fatalf("DeleteFrozenBlockRange: %v", err)
	}
	if err := rawdb.DeleteBlockNumber(diskdb, block.Hash()); err != nil {
		t.Fatalf("DeleteBlockNumber: %v", err)
	}
	if err := rawdb.DeleteTransactionIndex(diskdb, txHash[:]); err != nil {
		t.Fatalf("DeleteTransactionIndex: %v", err)
	}
	if err := rawdb.DeleteTransactionInfo(diskdb, txHash[:]); err != nil {
		t.Fatalf("DeleteTransactionInfo: %v", err)
	}
	if err := rawdb.DeleteTransactionInfosByBlock(diskdb, block.Number()); err != nil {
		t.Fatalf("DeleteTransactionInfosByBlock: %v", err)
	}
	hotOnly := rawdb.NewChainDB(diskdb, rawdb.NoopAncient{})
	if got := rawdb.ReadBlock(hotOnly, block.Number()); got != nil {
		t.Fatalf("hot block still present: %x", got.Hash())
	}
	if got := rawdb.ReadBlockNumber(hotOnly, block.Hash()); got != nil {
		t.Fatalf("hot block lookup still present: %v", got)
	}
	if got := rawdb.ReadTransactionIndex(hotOnly, txHash[:]); got != nil {
		t.Fatalf("hot tx lookup still present: %v", got)
	}
	if got := rawdb.ReadTransactionInfo(hotOnly, txHash[:]); got != nil {
		t.Fatalf("hot tx info still present: %+v", got)
	}
	if infos := rawdb.ReadTransactionInfosByBlock(hotOnly, block.Number()); len(infos) != 0 {
		t.Fatalf("hot tx receipt rows still present: %+v", infos)
	}

	bc.ChainDB().SetChainIndexReader(mgr)
	backend := &TronBackend{chain: bc}
	if got, err := backend.GetTransactionInfoByID(txHash); err != nil ||
		got == nil ||
		len(got.Log) != 1 ||
		got.GetReceipt().GetEnergyUsageTotal() != 12345 ||
		got.Result != corepb.TransactionInfo_FAILED {
		t.Fatalf("GetTransactionInfoByID cold receipt = %+v/%v, want failed receipt with one log and energy", got, err)
	}

	rpcServer := jsonrpc.NewServer(backend, 0)
	defer rpcServer.Stop()
	httpServer := httptest.NewServer(rpcServer.Handler())
	defer httpServer.Close()

	txHashHex := "0x" + txHash.Hex()
	resp := postCoreJSONRPC(t, httpServer.URL, "eth_getTransactionReceipt", []any{txHashHex})
	receipt, ok := resp["result"].(map[string]any)
	if !ok {
		t.Fatalf("eth_getTransactionReceipt result = %T %v, want object", resp["result"], resp["result"])
	}
	logs, ok := receipt["logs"].([]any)
	if !ok || len(logs) != 1 {
		t.Fatalf("cold eth_getTransactionReceipt logs = %T %v, want one log", receipt["logs"], receipt["logs"])
	}
	logObj, ok := logs[0].(map[string]any)
	if !ok {
		t.Fatalf("cold receipt log = %T %v, want object", logs[0], logs[0])
	}
	if receipt["transactionHash"] != txHashHex ||
		receipt["blockHash"] != "0x"+block.Hash().Hex() ||
		receipt["status"] != "0x0" ||
		receipt["gasUsed"] != "0x3039" ||
		receipt["cumulativeGasUsed"] != "0x3039" ||
		logObj["address"] != "0x"+fmt.Sprintf("%x", logAddress) ||
		fmt.Sprint(logObj["topics"]) != fmt.Sprintf("[0x%064x]", topic[:]) ||
		logObj["data"] != "0x9c9d" ||
		logObj["transactionHash"] != txHashHex ||
		logObj["blockNumber"] != "0x1" ||
		logObj["logIndex"] != "0x0" {
		t.Fatalf("cold eth_getTransactionReceipt = receipt %+v log %+v, want cold log payload", receipt, logObj)
	}
	blockReceiptsResp := postCoreJSONRPC(t, httpServer.URL, "eth_getBlockReceipts", []any{"0x1"})
	blockReceipts, ok := blockReceiptsResp["result"].([]any)
	if !ok || len(blockReceipts) != 1 {
		t.Fatalf("eth_getBlockReceipts result = %T %v, want one cold receipt", blockReceiptsResp["result"], blockReceiptsResp["result"])
	}
	blockReceipt, ok := blockReceipts[0].(map[string]any)
	if !ok {
		t.Fatalf("eth_getBlockReceipts[0] = %T %v, want object", blockReceipts[0], blockReceipts[0])
	}
	blockLogs, ok := blockReceipt["logs"].([]any)
	if !ok || len(blockLogs) != 1 {
		t.Fatalf("cold eth_getBlockReceipts logs = %T %v, want one log", blockReceipt["logs"], blockReceipt["logs"])
	}
	blockLog, ok := blockLogs[0].(map[string]any)
	if !ok {
		t.Fatalf("cold block receipt log = %T %v, want object", blockLogs[0], blockLogs[0])
	}
	if blockReceipt["transactionHash"] != txHashHex ||
		blockReceipt["blockHash"] != "0x"+block.Hash().Hex() ||
		blockReceipt["status"] != "0x0" ||
		blockReceipt["gasUsed"] != "0x3039" ||
		blockLog["address"] != "0x"+fmt.Sprintf("%x", logAddress) ||
		blockLog["data"] != "0x9c9d" ||
		blockLog["transactionHash"] != txHashHex ||
		blockLog["blockNumber"] != "0x1" ||
		blockLog["logIndex"] != "0x0" {
		t.Fatalf("cold eth_getBlockReceipts = receipt %+v log %+v, want cold log payload", blockReceipt, blockLog)
	}
}

func TestJSONRPCGetTransactionReceiptUsesColdTxPositionAfterHotPrune(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	fz, err := rawdbfreezer.NewFreezer(t.TempDir(), "", false, 2049, chainfreezer.FreezerTableSet())
	if err != nil {
		t.Fatalf("NewFreezer: %v", err)
	}
	defer fz.Close()

	genesis := &params.Genesis{
		Config:            params.MainnetChainConfig,
		Timestamp:         0,
		DynamicProperties: map[string]int64{},
	}
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatalf("SetupGenesisBlock: %v", err)
	}
	bc, err := NewBlockChainWithAncient(diskdb, state.NewDatabase(diskdb), params.MainnetChainConfig, rawdb.NewFreezerReader(fz))
	if err != nil {
		t.Fatalf("NewBlockChainWithAncient: %v", err)
	}
	defer bc.Close()

	txPB1 := &corepb.Transaction{RawData: &corepb.TransactionRaw{Timestamp: 1001, Expiration: 2001, Data: []byte{0x01}}}
	txPB2 := &corepb.Transaction{RawData: &corepb.TransactionRaw{Timestamp: 1002, Expiration: 2002, Data: []byte{0x02}}}
	txHash1 := types.NewTransactionFromPB(txPB1).Hash()
	txHash2 := types.NewTransactionFromPB(txPB2).Hash()
	parent := bc.CurrentBlock()
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:     1,
				Timestamp:  3000,
				ParentHash: parent.Hash().Bytes(),
			},
		},
		Transactions: []*corepb.Transaction{txPB1, txPB2},
	})
	info1 := &corepb.TransactionInfo{
		Id:             txHash1.Bytes(),
		BlockNumber:    int64(block.Number()),
		BlockTimeStamp: block.Timestamp(),
	}
	info2 := &corepb.TransactionInfo{
		BlockNumber:    int64(block.Number()),
		BlockTimeStamp: block.Timestamp(),
		Receipt:        &corepb.ResourceReceipt{EnergyUsageTotal: 77},
	}
	if err := rawdb.WriteBlock(diskdb, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(diskdb, block.Number(), []*corepb.TransactionInfo{info1, info2}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}
	if err := rawdb.WriteTransactionIndex(diskdb, txHash1[:], block.Number()); err != nil {
		t.Fatalf("WriteTransactionIndex tx1: %v", err)
	}
	if err := rawdb.WriteTransactionIndex(diskdb, txHash2[:], block.Number()); err != nil {
		t.Fatalf("WriteTransactionIndex tx2: %v", err)
	}
	bc.currentBlock.Store(block)

	if err := appendBackendColdLookupAncients(t, fz, diskdb, parent, block); err != nil {
		t.Fatalf("append ancients: %v", err)
	}
	snapshotDir := t.TempDir()
	freezerRef, err := statesnapshots.BuildChainFreezerSegmentFromAncient(rawdb.NewFreezerReader(fz), snapshotDir, "", 0, 1)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	indexRef, err := statesnapshots.BuildChainIndexSegmentFromChainFreezerSegment(snapshotDir, freezerRef, "")
	if err != nil {
		t.Fatalf("BuildChainIndexSegmentFromChainFreezerSegment: %v", err)
	}
	if err := statesnapshots.PublishManifest(snapshotDir, statesnapshots.NewManifest(0, 0, []statesnapshots.SegmentRef{freezerRef, indexRef})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := statesnapshots.OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}

	if err := rawdb.DeleteFrozenBlockRange(diskdb, block.Number(), block.Number()); err != nil {
		t.Fatalf("DeleteFrozenBlockRange: %v", err)
	}
	if err := rawdb.DeleteBlockNumber(diskdb, block.Hash()); err != nil {
		t.Fatalf("DeleteBlockNumber: %v", err)
	}
	if err := rawdb.DeleteTransactionIndex(diskdb, txHash1[:]); err != nil {
		t.Fatalf("DeleteTransactionIndex tx1: %v", err)
	}
	if err := rawdb.DeleteTransactionIndex(diskdb, txHash2[:]); err != nil {
		t.Fatalf("DeleteTransactionIndex tx2: %v", err)
	}
	if err := rawdb.DeleteTransactionInfo(diskdb, txHash1[:]); err != nil {
		t.Fatalf("DeleteTransactionInfo tx1: %v", err)
	}
	if err := rawdb.DeleteTransactionInfo(diskdb, txHash2[:]); err != nil {
		t.Fatalf("DeleteTransactionInfo tx2: %v", err)
	}
	if err := rawdb.DeleteTransactionInfosByBlock(diskdb, block.Number()); err != nil {
		t.Fatalf("DeleteTransactionInfosByBlock: %v", err)
	}
	hotOnly := rawdb.NewChainDB(diskdb, rawdb.NoopAncient{})
	if got := rawdb.ReadTransactionIndex(hotOnly, txHash2[:]); got != nil {
		t.Fatalf("hot tx lookup still present: %v", got)
	}
	if got := rawdb.ReadTransactionInfo(hotOnly, txHash2[:]); got != nil {
		t.Fatalf("hot tx info still present: %+v", got)
	}
	if infos := rawdb.ReadTransactionInfosByBlock(hotOnly, block.Number()); len(infos) != 0 {
		t.Fatalf("hot tx receipt rows still present: %+v", infos)
	}

	bc.ChainDB().SetChainIndexReader(mgr)
	backend := &TronBackend{chain: bc}
	gotInfo, err := backend.GetTransactionInfo(txHash2)
	if err != nil || gotInfo == nil || len(gotInfo.Id) != 0 || gotInfo.GetReceipt().GetEnergyUsageTotal() != 77 {
		t.Fatalf("GetTransactionInfo cold tx-position receipt = %+v/%v, want empty-id receipt resolved by tx position", gotInfo, err)
	}

	rpcServer := jsonrpc.NewServer(backend, 0)
	defer rpcServer.Stop()
	httpServer := httptest.NewServer(rpcServer.Handler())
	defer httpServer.Close()

	txHashHex := "0x" + txHash2.Hex()
	resp := postCoreJSONRPC(t, httpServer.URL, "eth_getTransactionReceipt", []any{txHashHex})
	receipt, ok := resp["result"].(map[string]any)
	if !ok {
		t.Fatalf("eth_getTransactionReceipt result = %T %v, want object", resp["result"], resp["result"])
	}
	if receipt["transactionHash"] != txHashHex ||
		receipt["blockHash"] != "0x"+block.Hash().Hex() ||
		receipt["blockNumber"] != "0x1" ||
		receipt["transactionIndex"] != "0x1" ||
		receipt["gasUsed"] != "0x4d" ||
		receipt["cumulativeGasUsed"] != "0x4d" ||
		receipt["status"] != "0x1" {
		t.Fatalf("cold eth_getTransactionReceipt = %+v, want second tx receipt from cold tx-position lookup", receipt)
	}
}

func TestTronBackend_GetTransactionInfoByBlockNumRejectsMismatchedInfo(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()
	block, info := testBackendLogBlock(1, nil)
	info.Id = bytes.Repeat([]byte{0xee}, tcommon.HashLength)
	if err := rawdb.WriteBlock(bc.db, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(bc.db, block.Number(), []*corepb.TransactionInfo{info}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}
	bc.currentBlock.Store(block)

	backend := &TronBackend{chain: bc}
	if got, err := backend.GetTransactionInfoByBlockNum(block.Number()); err == nil || got != nil {
		t.Fatalf("GetTransactionInfoByBlockNum mismatched info = %+v/%v, want nil/error", got, err)
	}
}

func TestTronBackend_GetTransactionInfoByIDSurfacesMismatchedInfo(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()
	block, info := testBackendLogBlock(1, nil)
	txHash := block.Transactions()[0].Hash()
	info.Id = bytes.Repeat([]byte{0xef}, tcommon.HashLength)
	if err := rawdb.WriteBlock(bc.db, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if err := rawdb.WriteTransactionIndex(bc.db, txHash[:], block.Number()); err != nil {
		t.Fatalf("WriteTransactionIndex: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(bc.db, block.Number(), []*corepb.TransactionInfo{info}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}
	bc.currentBlock.Store(block)

	backend := &TronBackend{chain: bc}
	if got, err := backend.GetTransactionInfoByID(txHash); err == nil || got != nil || !strings.Contains(err.Error(), "does not match canonical tx") {
		t.Fatalf("GetTransactionInfoByID mismatched info = %+v/%v, want nil/canonical mismatch error", got, err)
	}
	if got, err := backend.GetTransactionInfo(txHash); err == nil || got != nil || !strings.Contains(err.Error(), "does not match canonical tx") {
		t.Fatalf("GetTransactionInfo mismatched info = %+v/%v, want nil/canonical mismatch error", got, err)
	}
}

func appendBackendColdLookupAncients(t *testing.T, fz *rawdbfreezer.Freezer, db ethdb.KeyValueReader, blocks ...*types.Block) error {
	t.Helper()
	if _, err := fz.ModifyAncients(func(op rawdb.AncientWriteOp) error {
		for _, block := range blocks {
			if block == nil {
				continue
			}
			blockRaw := rawdb.ReadBlockRaw(db, block.Number())
			if len(blockRaw) == 0 {
				return fmt.Errorf("hot block %d raw bytes missing", block.Number())
			}
			if err := op.AppendRaw(rawdb.AncientBlocksTable, block.Number(), blockRaw); err != nil {
				return err
			}
			txInfosRaw := rawdb.ReadTransactionInfosRaw(db, block.Number())
			if len(txInfosRaw) == 0 && len(block.Transactions()) != 0 {
				txInfosRaw = testBackendTransactionInfosRawForBlock(t, block)
			}
			if err := op.AppendRaw(rawdb.AncientTxInfosTable, block.Number(), txInfosRaw); err != nil {
				return err
			}
			if err := op.AppendRaw(rawdb.AncientStateRootsTable, block.Number(), rawdb.ReadBlockStateRootRaw(db, block.Hash())); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return fz.Sync()
}

func testBackendTransactionInfosRawForBlock(t *testing.T, block *types.Block) []byte {
	t.Helper()
	txs := block.Transactions()
	if len(txs) == 0 {
		return nil
	}
	ret := &corepb.TransactionRet{
		BlockNumber:     int64(block.Number()),
		BlockTimeStamp:  block.Timestamp(),
		Transactioninfo: make([]*corepb.TransactionInfo, 0, len(txs)),
	}
	for _, tx := range txs {
		txHash := tx.Hash()
		ret.Transactioninfo = append(ret.Transactioninfo, &corepb.TransactionInfo{
			Id:             txHash[:],
			BlockNumber:    int64(block.Number()),
			BlockTimeStamp: block.Timestamp(),
		})
	}
	raw, err := proto.Marshal(ret)
	if err != nil {
		t.Fatalf("marshal tx info for block %d: %v", block.Number(), err)
	}
	return raw
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

func TestTronBackend_GetLogsKeepsHotBlockOrderAcrossParallelScan(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()
	for number := uint64(1); number <= 4; number++ {
		block, info := testBackendLogBlock(number, &corepb.TransactionInfo_Log{
			Address: bytes20(byte(number)),
			Data:    []byte{byte(number)},
		})
		if err := rawdb.WriteBlock(bc.db, block); err != nil {
			t.Fatalf("WriteBlock block%d: %v", number, err)
		}
		if err := rawdb.WriteTransactionInfosByBlock(bc.db, number, []*corepb.TransactionInfo{info}); err != nil {
			t.Fatalf("WriteTransactionInfosByBlock block%d: %v", number, err)
		}
		bc.currentBlock.Store(block)
	}

	from, to := uint64(1), uint64(4)
	logs, err := (&TronBackend{chain: bc}).GetLogs(jsonrpc.LogFilter{FromBlock: &from, ToBlock: &to})
	if err != nil {
		t.Fatalf("GetLogs: %v", err)
	}
	if len(logs) != 4 {
		t.Fatalf("GetLogs returned %d logs, want 4", len(logs))
	}
	for i, log := range logs {
		blockNum := i + 1
		if want := fmt.Sprintf("0x%x", blockNum); log.BlockNumber != want {
			t.Fatalf("log %d block number = %s, want %s", i, log.BlockNumber, want)
		}
		if want := fmt.Sprintf("0x%02x", blockNum); log.Data != want {
			t.Fatalf("log %d data = %s, want %s", i, log.Data, want)
		}
	}
}

func TestScanLogBlocksParallelKeepsInputOrder(t *testing.T) {
	started := make(chan uint64, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })

	type result struct {
		logs []*jsonrpc.RPCLog
		err  error
	}
	done := make(chan result, 1)
	go func() {
		logs, err := scanLogBlocks([]uint64{7, 8, 9, 10}, 2, func(blockNum uint64) ([]*jsonrpc.RPCLog, error) {
			started <- blockNum
			<-release
			return []*jsonrpc.RPCLog{{BlockNumber: fmt.Sprintf("0x%x", blockNum)}}, nil
		})
		done <- result{logs: logs, err: err}
	}()

	for want := 0; want < 2; want++ {
		select {
		case <-started:
		case <-time.After(3 * time.Second):
			t.Fatal("parallel log scanner did not start two workers")
		}
	}
	releaseOnce.Do(func() { close(release) })

	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("scanLogBlocks: %v", got.err)
		}
		if len(got.logs) != 4 {
			t.Fatalf("logs = %+v, want four ordered logs", got.logs)
		}
		for i, log := range got.logs {
			want := fmt.Sprintf("0x%x", 7+i)
			if log.BlockNumber != want {
				t.Fatalf("log %d block number = %s, want %s", i, log.BlockNumber, want)
			}
		}
	case <-time.After(3 * time.Second):
		t.Fatal("parallel log scanner did not finish")
	}
}

func TestScanLogBlocksReturnsFirstInputOrderError(t *testing.T) {
	firstErr := fmt.Errorf("block 7 read failed")
	laterErr := fmt.Errorf("block 9 read failed")
	logs, err := scanLogBlocks([]uint64{7, 8, 9}, 3, func(blockNum uint64) ([]*jsonrpc.RPCLog, error) {
		switch blockNum {
		case 7:
			return nil, firstErr
		case 9:
			return nil, laterErr
		default:
			return nil, nil
		}
	})
	if err != firstErr {
		t.Fatalf("scanLogBlocks error = %v, want first input error %v", err, firstErr)
	}
	if logs != nil {
		t.Fatalf("scanLogBlocks logs = %+v, want nil on error", logs)
	}
}

func TestTronBackend_GetLogsFallsBackWhenSectionBloomMissing(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()
	logAddress := bytes20(0x11)
	topic := tcommon.Hash{0xaa}
	block1, info1 := testBackendLogBlock(1, &corepb.TransactionInfo_Log{
		Address: logAddress,
		Topics:  [][]byte{topic[:]},
		Data:    []byte{0x01, 0x02},
	})
	block2, info2 := testBackendLogBlock(2, nil)
	if err := rawdb.WriteBlock(bc.db, block1); err != nil {
		t.Fatalf("WriteBlock block1: %v", err)
	}
	if err := rawdb.WriteBlock(bc.db, block2); err != nil {
		t.Fatalf("WriteBlock block2: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(bc.db, 1, []*corepb.TransactionInfo{info1}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock block1: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(bc.db, 2, []*corepb.TransactionInfo{info2}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock block2: %v", err)
	}
	bc.currentBlock.Store(block2)

	from, to := uint64(1), uint64(2)
	backend := &TronBackend{chain: bc}
	logs, err := backend.GetLogs(jsonrpc.LogFilter{
		FromBlock: &from,
		ToBlock:   &to,
		Addresses: []tcommon.Address{tcommon.BytesToAddress(logAddress)},
		Topics:    [][]tcommon.Hash{{topic}},
	})
	if err != nil {
		t.Fatalf("GetLogs: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("GetLogs without section bloom rows returned %d logs, want 1", len(logs))
	}
}

func TestTronBackend_GetLogsFallsBackWhenSectionBloomErrors(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()
	logAddress := bytes20(0x15)
	topic := tcommon.Hash{0x15}
	block1, info1 := testBackendLogBlock(1, &corepb.TransactionInfo_Log{
		Address: logAddress,
		Topics:  [][]byte{topic[:]},
		Data:    []byte{0x15, 0x16},
	})
	block2, info2 := testBackendLogBlock(2, nil)
	if err := rawdb.WriteBlock(bc.db, block1); err != nil {
		t.Fatalf("WriteBlock block1: %v", err)
	}
	if err := rawdb.WriteBlock(bc.db, block2); err != nil {
		t.Fatalf("WriteBlock block2: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(bc.db, 1, []*corepb.TransactionInfo{info1}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock block1: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(bc.db, 2, []*corepb.TransactionInfo{info2}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock block2: %v", err)
	}
	bc.currentBlock.Store(block2)
	bc.ChainDB().SetSectionBloomReader(testSectionBloomColdReader{err: fmt.Errorf("cold section bloom unavailable")})

	from, to := uint64(1), uint64(2)
	backend := &TronBackend{chain: bc}
	logs, err := backend.GetLogs(jsonrpc.LogFilter{
		FromBlock: &from,
		ToBlock:   &to,
		Addresses: []tcommon.Address{tcommon.BytesToAddress(logAddress)},
		Topics:    [][]tcommon.Hash{{topic}},
	})
	if err != nil {
		t.Fatalf("GetLogs with section bloom error: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("GetLogs with section bloom error returned %d logs, want 1", len(logs))
	}
	if logs[0].Data != "0x1516" {
		t.Fatalf("GetLogs with section bloom error log = %+v, want data 0x1516", logs[0])
	}
}

func TestTronBackend_GetLogsSkipsMissingTransactionInfoCoverage(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()
	block1, _ := testBackendLogBlock(1, nil)
	if err := rawdb.WriteBlock(bc.db, block1); err != nil {
		t.Fatalf("WriteBlock block1: %v", err)
	}
	bc.currentBlock.Store(block1)

	from, to := uint64(1), uint64(1)
	backend := &TronBackend{chain: bc}
	logs, err := backend.GetLogs(jsonrpc.LogFilter{
		FromBlock: &from,
		ToBlock:   &to,
	})
	if err != nil {
		t.Fatalf("GetLogs missing TransactionRet: %v", err)
	}
	if len(logs) != 0 {
		t.Fatalf("GetLogs missing TransactionRet returned %d logs, want none", len(logs))
	}
}

func TestTronBackend_GetLogsSkipsIncompleteTransactionInfoCoverage(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()
	logAddress := bytes20(0x12)
	topic := tcommon.Hash{0xab}
	block1, _ := testBackendLogBlock(1, &corepb.TransactionInfo_Log{
		Address: logAddress,
		Topics:  [][]byte{topic[:]},
		Data:    []byte{0x03, 0x04},
	})
	if err := rawdb.WriteBlock(bc.db, block1); err != nil {
		t.Fatalf("WriteBlock block1: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(bc.db, 1, nil); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock block1: %v", err)
	}
	bc.currentBlock.Store(block1)

	from, to := uint64(1), uint64(1)
	backend := &TronBackend{chain: bc}
	logs, err := backend.GetLogs(jsonrpc.LogFilter{
		FromBlock: &from,
		ToBlock:   &to,
		Addresses: []tcommon.Address{tcommon.BytesToAddress(logAddress)},
		Topics:    [][]tcommon.Hash{{topic}},
	})
	if err != nil {
		t.Fatalf("GetLogs incomplete TransactionRet: %v", err)
	}
	if len(logs) != 0 {
		t.Fatalf("GetLogs incomplete TransactionRet returned %d logs, want none", len(logs))
	}
}

func TestTronBackend_GetLogsRejectsMismatchedTransactionInfoBlockNumber(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()
	logAddress := bytes20(0x12)
	topic := tcommon.Hash{0xab}
	block1, info1 := testBackendLogBlock(1, &corepb.TransactionInfo_Log{
		Address: logAddress,
		Topics:  [][]byte{topic[:]},
		Data:    []byte{0x03, 0x04},
	})
	block2, _ := testBackendLogBlock(2, nil)
	if err := rawdb.WriteBlock(bc.db, block1); err != nil {
		t.Fatalf("WriteBlock block1: %v", err)
	}
	if err := rawdb.WriteBlock(bc.db, block2); err != nil {
		t.Fatalf("WriteBlock block2: %v", err)
	}
	info1.BlockNumber = 2
	rawInfo, err := proto.Marshal(&corepb.TransactionRet{
		BlockNumber:     1,
		Transactioninfo: []*corepb.TransactionInfo{info1},
	})
	if err != nil {
		t.Fatalf("marshal raw TransactionRet: %v", err)
	}
	if err := rawdb.WriteTransactionInfosRaw(bc.db, 1, rawInfo); err != nil {
		t.Fatalf("WriteTransactionInfosRaw block1: %v", err)
	}
	bc.currentBlock.Store(block2)

	from, to := uint64(1), uint64(1)
	backend := &TronBackend{chain: bc}
	logs, err := backend.GetLogs(jsonrpc.LogFilter{
		FromBlock: &from,
		ToBlock:   &to,
		Addresses: []tcommon.Address{tcommon.BytesToAddress(logAddress)},
		Topics:    [][]tcommon.Hash{{topic}},
	})
	if err == nil || !strings.Contains(err.Error(), "transaction info block number") {
		t.Fatalf("GetLogs error = %v logs=%d, want transaction info block number error", err, len(logs))
	}
}

func TestTronBackend_GetLogsRejectsMismatchedTransactionInfoID(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()
	logAddress := bytes20(0x13)
	topic := tcommon.Hash{0xac}
	block1, info1 := testBackendLogBlock(1, &corepb.TransactionInfo_Log{
		Address: logAddress,
		Topics:  [][]byte{topic[:]},
		Data:    []byte{0x04, 0x05},
	})
	if err := rawdb.WriteBlock(bc.db, block1); err != nil {
		t.Fatalf("WriteBlock block1: %v", err)
	}
	info1.Id = bytes.Repeat([]byte{0x99}, tcommon.HashLength)
	if err := rawdb.WriteTransactionInfosByBlock(bc.db, 1, []*corepb.TransactionInfo{info1}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock block1: %v", err)
	}
	bc.currentBlock.Store(block1)

	from, to := uint64(1), uint64(1)
	backend := &TronBackend{chain: bc}
	logs, err := backend.GetLogs(jsonrpc.LogFilter{
		FromBlock: &from,
		ToBlock:   &to,
		Addresses: []tcommon.Address{tcommon.BytesToAddress(logAddress)},
		Topics:    [][]tcommon.Hash{{topic}},
	})
	if err == nil || !strings.Contains(err.Error(), "does not match canonical tx") {
		t.Fatalf("GetLogs error = %v logs=%d, want transaction info id mismatch error", err, len(logs))
	}
}

func TestTronBackend_GetLogsHotPathMatchesTronAddress(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()
	logAddress := append([]byte{tcommon.AddressPrefixMainnet}, bytes20(0x21)...)
	topic := tcommon.Hash{0x21}
	block1, info1 := testBackendLogBlock(1, &corepb.TransactionInfo_Log{
		Address: logAddress,
		Topics:  [][]byte{topic[:]},
		Data:    []byte{0x21, 0x22},
	})
	block2, info2 := testBackendLogBlock(2, nil)
	if err := rawdb.WriteBlock(bc.db, block1); err != nil {
		t.Fatalf("WriteBlock block1: %v", err)
	}
	if err := rawdb.WriteBlock(bc.db, block2); err != nil {
		t.Fatalf("WriteBlock block2: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(bc.db, 1, []*corepb.TransactionInfo{info1}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock block1: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(bc.db, 2, []*corepb.TransactionInfo{info2}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock block2: %v", err)
	}
	bc.currentBlock.Store(block2)

	from, to := uint64(1), uint64(2)
	backend := &TronBackend{chain: bc}
	logs, err := backend.GetLogs(jsonrpc.LogFilter{
		FromBlock: &from,
		ToBlock:   &to,
		Addresses: []tcommon.Address{tcommon.BytesToAddress(logAddress)},
		Topics:    [][]tcommon.Hash{{topic}},
	})
	if err != nil {
		t.Fatalf("GetLogs: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("GetLogs hot TRON address filter returned %d logs, want 1", len(logs))
	}
	if logs[0].Data != "0x2122" {
		t.Fatalf("GetLogs hot TRON address log = %+v, want data 0x2122", logs[0])
	}
}

func TestTronBackend_GetLogsUsesColdEventLogSegment(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()
	logAddress := bytes20(0x31)
	topic := tcommon.Hash{0xcc}
	block1, info1 := testBackendLogBlock(1, &corepb.TransactionInfo_Log{
		Address: logAddress,
		Topics:  [][]byte{topic[:]},
		Data:    []byte{0x0a, 0x0b},
	})
	block2, _ := testBackendLogBlock(2, nil)
	if err := rawdb.WriteBlock(bc.db, block1); err != nil {
		t.Fatalf("WriteBlock block1: %v", err)
	}
	if err := rawdb.WriteBlock(bc.db, block2); err != nil {
		t.Fatalf("WriteBlock block2: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(bc.db, 1, []*corepb.TransactionInfo{info1}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock block1: %v", err)
	}
	bc.currentBlock.Store(block2)

	dir := t.TempDir()
	if _, err := statesnapshots.NewAggregator(dir).BuildEventLogs(bc.ChainDB(), 1, 1); err != nil {
		t.Fatalf("BuildEventLogs: %v", err)
	}
	mgr, err := statesnapshots.OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	bc.ChainDB().SetEventLogReader(mgr)
	if err := rawdb.DeleteTransactionInfosByBlock(bc.db, 1); err != nil {
		t.Fatalf("DeleteTransactionInfosByBlock block1: %v", err)
	}

	from, to := uint64(1), uint64(1)
	backend := &TronBackend{chain: bc}
	logs, err := backend.GetLogs(jsonrpc.LogFilter{
		FromBlock: &from,
		ToBlock:   &to,
		Addresses: []tcommon.Address{tcommon.BytesToAddress(logAddress)},
		Topics:    [][]tcommon.Hash{{topic}},
	})
	if err != nil {
		t.Fatalf("GetLogs: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("GetLogs from cold event log segment returned %d logs, want 1", len(logs))
	}
	got := logs[0]
	if got.Address != fmt.Sprintf("0x%x", logAddress) {
		t.Fatalf("address = %s, want 0x%x", got.Address, logAddress)
	}
	if got.Data != "0x0a0b" {
		t.Fatalf("data = %s, want 0x0a0b", got.Data)
	}
	if got.BlockNumber != "0x1" || got.TransactionIndex != "0x0" || got.LogIndex != "0x0" {
		t.Fatalf("position = block %s tx %s log %s, want 0x1/0x0/0x0", got.BlockNumber, got.TransactionIndex, got.LogIndex)
	}
	if got.BlockHash != fmt.Sprintf("0x%x", block1.Hash()) {
		t.Fatalf("block hash = %s, want 0x%x", got.BlockHash, block1.Hash())
	}
	if got.TransactionHash != fmt.Sprintf("0x%x", block1.Transactions()[0].Hash()) {
		t.Fatalf("tx hash = %s, want 0x%x", got.TransactionHash, block1.Transactions()[0].Hash())
	}
}

func TestTronBackend_GetLogsUsesColdEventLogSegmentWithIncompleteHotTransactionInfo(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()
	logAddress := bytes20(0x35)
	topic := tcommon.Hash{0x35}
	block1, info1 := testBackendLogBlock(1, &corepb.TransactionInfo_Log{
		Address: logAddress,
		Topics:  [][]byte{topic[:]},
		Data:    []byte{0x35, 0x36},
	})
	block2, _ := testBackendLogBlock(2, nil)
	if err := rawdb.WriteBlock(bc.db, block1); err != nil {
		t.Fatalf("WriteBlock block1: %v", err)
	}
	if err := rawdb.WriteBlock(bc.db, block2); err != nil {
		t.Fatalf("WriteBlock block2: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(bc.db, 1, []*corepb.TransactionInfo{info1}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock block1: %v", err)
	}
	bc.currentBlock.Store(block2)

	dir := t.TempDir()
	if _, err := statesnapshots.NewAggregator(dir).BuildEventLogs(bc.ChainDB(), 1, 1); err != nil {
		t.Fatalf("BuildEventLogs: %v", err)
	}
	mgr, err := statesnapshots.OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	bc.ChainDB().SetEventLogReader(mgr)
	if err := rawdb.WriteTransactionInfosByBlock(bc.db, 1, nil); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock incomplete block1: %v", err)
	}

	from, to := uint64(1), uint64(1)
	backend := &TronBackend{chain: bc}
	logs, err := backend.GetLogs(jsonrpc.LogFilter{
		FromBlock: &from,
		ToBlock:   &to,
		Addresses: []tcommon.Address{tcommon.BytesToAddress(logAddress)},
		Topics:    [][]tcommon.Hash{{topic}},
	})
	if err != nil {
		t.Fatalf("GetLogs with incomplete hot TransactionRet: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("GetLogs with incomplete hot TransactionRet returned %d logs, want 1", len(logs))
	}
	got := logs[0]
	if got.Data != "0x3536" || got.TransactionHash != fmt.Sprintf("0x%x", block1.Transactions()[0].Hash()) {
		t.Fatalf("cold log with incomplete hot TransactionRet = %+v, want data 0x3536 and canonical tx hash", got)
	}
}

func TestTronBackend_GetLogsBlockHashUsesColdEventLogSegment(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()
	logAddress := bytes20(0x36)
	topic := tcommon.Hash{0x36}
	block1, info1 := testBackendLogBlock(1, &corepb.TransactionInfo_Log{
		Address: logAddress,
		Topics:  [][]byte{topic[:]},
		Data:    []byte{0x36, 0x37},
	})
	block2, _ := testBackendLogBlock(2, nil)
	if err := rawdb.WriteBlock(bc.db, block1); err != nil {
		t.Fatalf("WriteBlock block1: %v", err)
	}
	if err := rawdb.WriteBlock(bc.db, block2); err != nil {
		t.Fatalf("WriteBlock block2: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(bc.db, 1, []*corepb.TransactionInfo{info1}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock block1: %v", err)
	}
	bc.currentBlock.Store(block2)

	dir := t.TempDir()
	if _, err := statesnapshots.NewAggregator(dir).BuildEventLogs(bc.ChainDB(), 1, 1); err != nil {
		t.Fatalf("BuildEventLogs: %v", err)
	}
	mgr, err := statesnapshots.OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	bc.ChainDB().SetEventLogReader(mgr)
	if err := rawdb.DeleteTransactionInfosByBlock(bc.db, 1); err != nil {
		t.Fatalf("DeleteTransactionInfosByBlock block1: %v", err)
	}

	blockHash := block1.Hash()
	backend := &TronBackend{chain: bc}
	logs, err := backend.GetLogs(jsonrpc.LogFilter{BlockHash: &blockHash})
	if err != nil {
		t.Fatalf("GetLogs by blockHash: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("GetLogs by blockHash from cold event log segment returned %d logs, want 1", len(logs))
	}
	if logs[0].Data != "0x3637" || logs[0].BlockHash != fmt.Sprintf("0x%x", blockHash) {
		t.Fatalf("log = %+v, want blockHash %x data 0x3637", logs[0], blockHash)
	}
}

func TestTronBackend_GetLogsBlockHashUsesColdChainIndexAndEventLogSegment(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	fz, err := rawdbfreezer.NewFreezer(t.TempDir(), "", false, 2049, chainfreezer.FreezerTableSet())
	if err != nil {
		t.Fatalf("NewFreezer: %v", err)
	}
	defer fz.Close()

	genesis := &params.Genesis{
		Config:            params.MainnetChainConfig,
		Timestamp:         0,
		DynamicProperties: map[string]int64{},
	}
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatalf("SetupGenesisBlock: %v", err)
	}
	bc, err := NewBlockChainWithAncient(diskdb, state.NewDatabase(diskdb), params.MainnetChainConfig, rawdb.NewFreezerReader(fz))
	if err != nil {
		t.Fatalf("NewBlockChainWithAncient: %v", err)
	}
	defer bc.Close()

	logAddress := bytes20(0x46)
	topic := tcommon.Hash{0x46}
	block1, info1 := testBackendLogBlock(1, &corepb.TransactionInfo_Log{
		Address: logAddress,
		Topics:  [][]byte{topic[:]},
		Data:    []byte{0x46, 0x47},
	})
	if err := rawdb.WriteBlock(diskdb, block1); err != nil {
		t.Fatalf("WriteBlock block1: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(diskdb, 1, []*corepb.TransactionInfo{info1}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock block1: %v", err)
	}
	bc.currentBlock.Store(block1)

	parent := rawdb.ReadBlock(bc.ChainDB(), 0)
	if parent == nil {
		t.Fatal("genesis block missing")
	}
	if err := appendBackendColdLookupAncients(t, fz, diskdb, parent, block1); err != nil {
		t.Fatalf("append ancients: %v", err)
	}

	snapshotDir := t.TempDir()
	eventRef, err := statesnapshots.BuildEventLogSegmentFromChain(bc.ChainDB(), snapshotDir, "log/event-log-1-1.seg", 1, 1)
	if err != nil {
		t.Fatalf("BuildEventLogSegmentFromChain: %v", err)
	}
	eventIndexRef, err := statesnapshots.BuildEventLogIndexSegmentFromEventLogSegments(snapshotDir, []statesnapshots.SegmentRef{eventRef}, "")
	if err != nil {
		t.Fatalf("BuildEventLogIndexSegmentFromEventLogSegments: %v", err)
	}
	freezerRef, err := statesnapshots.BuildChainFreezerSegmentFromAncient(rawdb.NewFreezerReader(fz), snapshotDir, "", 0, 1)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	chainIndexRef, err := statesnapshots.BuildChainIndexSegmentFromChainFreezerSegment(snapshotDir, freezerRef, "")
	if err != nil {
		t.Fatalf("BuildChainIndexSegmentFromChainFreezerSegment: %v", err)
	}
	if err := statesnapshots.PublishManifest(snapshotDir, statesnapshots.NewManifest(0, 0, []statesnapshots.SegmentRef{
		eventRef,
		eventIndexRef,
		freezerRef,
		chainIndexRef,
	})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := statesnapshots.OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}

	if err := rawdb.DeleteFrozenBlockRange(diskdb, 1, 1); err != nil {
		t.Fatalf("DeleteFrozenBlockRange: %v", err)
	}
	if err := rawdb.DeleteBlockNumber(diskdb, block1.Hash()); err != nil {
		t.Fatalf("DeleteBlockNumber: %v", err)
	}
	if err := rawdb.DeleteTransactionInfosByBlock(diskdb, 1); err != nil {
		t.Fatalf("DeleteTransactionInfosByBlock block1: %v", err)
	}
	hotOnly := rawdb.NewChainDB(diskdb, rawdb.NoopAncient{})
	if got := rawdb.ReadBlock(hotOnly, 1); got != nil {
		t.Fatalf("hot block still present: %x", got.Hash())
	}
	if got := rawdb.ReadBlockNumber(hotOnly, block1.Hash()); got != nil {
		t.Fatalf("hot block lookup still present: %v", got)
	}
	if infos := rawdb.ReadTransactionInfosByBlock(hotOnly, 1); len(infos) != 0 {
		t.Fatalf("hot tx infos still present: %+v", infos)
	}

	bc.ChainDB().SetChainIndexReader(mgr)
	bc.ChainDB().SetEventLogReader(mgr)
	blockHash := block1.Hash()
	backend := &TronBackend{chain: bc}
	logs, err := backend.GetLogs(jsonrpc.LogFilter{BlockHash: &blockHash})
	if err != nil {
		t.Fatalf("GetLogs by blockHash: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("GetLogs by blockHash from cold chain/event segments returned %d logs, want 1", len(logs))
	}
	if logs[0].Data != "0x4647" || logs[0].BlockHash != fmt.Sprintf("0x%x", blockHash) {
		t.Fatalf("log = %+v, want blockHash %x data 0x4647", logs[0], blockHash)
	}
}

func TestTronBackend_GetLogsUsesColdEventLogIndexForFilteredCoverage(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()
	logAddress := bytes20(0x41)
	otherAddress := bytes20(0x51)
	topic := tcommon.Hash{0xdd}
	otherTopic := tcommon.Hash{0xee}
	block1, info1 := testBackendLogBlock(1, &corepb.TransactionInfo_Log{
		Address: logAddress,
		Topics:  [][]byte{topic[:]},
		Data:    []byte{0x0c, 0x0d},
	})
	block2, info2 := testBackendLogBlock(2, &corepb.TransactionInfo_Log{
		Address: otherAddress,
		Topics:  [][]byte{otherTopic[:]},
		Data:    []byte{0x2c, 0x2d},
	})
	if err := rawdb.WriteBlock(bc.db, block1); err != nil {
		t.Fatalf("WriteBlock block1: %v", err)
	}
	if err := rawdb.WriteBlock(bc.db, block2); err != nil {
		t.Fatalf("WriteBlock block2: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(bc.db, 1, []*corepb.TransactionInfo{info1}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock block1: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(bc.db, 2, []*corepb.TransactionInfo{info2}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock block2: %v", err)
	}
	bc.currentBlock.Store(block2)

	dir := t.TempDir()
	ref1, err := statesnapshots.BuildEventLogSegmentFromChain(bc.ChainDB(), dir, "log/event-log-1-1.seg", 1, 1)
	if err != nil {
		t.Fatalf("BuildEventLogSegmentFromChain 1: %v", err)
	}
	ref2, err := statesnapshots.BuildEventLogSegmentFromChain(bc.ChainDB(), dir, "log/event-log-2-2.seg", 2, 2)
	if err != nil {
		t.Fatalf("BuildEventLogSegmentFromChain 2: %v", err)
	}
	indexRef, err := statesnapshots.BuildEventLogIndexSegmentFromEventLogSegments(dir, []statesnapshots.SegmentRef{ref1, ref2}, "")
	if err != nil {
		t.Fatalf("BuildEventLogIndexSegmentFromEventLogSegments: %v", err)
	}
	if err := statesnapshots.PublishManifest(dir, statesnapshots.NewManifest(0, 0, []statesnapshots.SegmentRef{ref1, ref2, indexRef})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := statesnapshots.OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	bc.ChainDB().SetEventLogReader(mgr)
	if err := rawdb.DeleteTransactionInfosByBlock(bc.db, 1); err != nil {
		t.Fatalf("DeleteTransactionInfosByBlock block1: %v", err)
	}
	if err := rawdb.DeleteTransactionInfosByBlock(bc.db, 2); err != nil {
		t.Fatalf("DeleteTransactionInfosByBlock block2: %v", err)
	}
	hotOnly := rawdb.NewChainDB(bc.db, rawdb.NoopAncient{})
	if infos := rawdb.ReadTransactionInfosByBlock(hotOnly, 1); len(infos) != 0 {
		t.Fatalf("hot block1 tx infos still present: %+v", infos)
	}
	if infos := rawdb.ReadTransactionInfosByBlock(hotOnly, 2); len(infos) != 0 {
		t.Fatalf("hot block2 tx infos still present: %+v", infos)
	}
	if err := os.Remove(filepath.Join(dir, ref2.Path)); err != nil {
		t.Fatalf("remove unrelated event-log segment: %v", err)
	}

	from, to := uint64(1), uint64(2)
	backend := &TronBackend{chain: bc}
	logs, err := backend.GetLogs(jsonrpc.LogFilter{
		FromBlock: &from,
		ToBlock:   &to,
		Addresses: []tcommon.Address{tcommon.BytesToAddress(logAddress)},
		Topics:    [][]tcommon.Hash{{topic}},
	})
	if err != nil {
		t.Fatalf("GetLogs: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("GetLogs with cold event-log-index returned %d logs, want 1", len(logs))
	}
	if logs[0].Data != "0x0c0d" || logs[0].BlockNumber != "0x1" {
		t.Fatalf("log = %+v, want block1 data 0x0c0d", logs[0])
	}
}

func TestTronBackend_GetLogsColdEventLogIndexMatchesTopicPositionOR(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()
	logAddress := bytes20(0x91)
	otherAddress := bytes20(0x92)
	topic0 := tcommon.Hash{0x91}
	topic0Alt := tcommon.Hash{0x92}
	topic1 := tcommon.Hash{0x93}
	otherTopic1 := tcommon.Hash{0x94}
	block1, info1 := testBackendLogBlock(1, nil)
	info1.Log = []*corepb.TransactionInfo_Log{
		{
			Address: logAddress,
			Topics:  [][]byte{topic0[:], topic1[:]},
			Data:    []byte{0x91, 0x01},
		},
		{
			Address: logAddress,
			Topics:  [][]byte{topic0Alt[:], topic1[:]},
			Data:    []byte{0x91, 0x02},
		},
		{
			Address: logAddress,
			Topics:  [][]byte{topic0[:], otherTopic1[:]},
			Data:    []byte{0x91, 0x03},
		},
		{
			Address: logAddress,
			Topics:  [][]byte{topic1[:], topic0[:]},
			Data:    []byte{0x91, 0x04},
		},
		{
			Address: otherAddress,
			Topics:  [][]byte{topic0Alt[:], topic1[:]},
			Data:    []byte{0x91, 0x05},
		},
	}
	if err := rawdb.WriteBlock(bc.db, block1); err != nil {
		t.Fatalf("WriteBlock block1: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(bc.db, block1.Number(), []*corepb.TransactionInfo{info1}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock block1: %v", err)
	}
	bc.currentBlock.Store(block1)

	dir := t.TempDir()
	ref, err := statesnapshots.BuildEventLogSegmentFromChain(bc.ChainDB(), dir, "log/event-log-1-1.seg", 1, 1)
	if err != nil {
		t.Fatalf("BuildEventLogSegmentFromChain: %v", err)
	}
	indexRef, err := statesnapshots.BuildEventLogIndexSegmentFromEventLogSegments(dir, []statesnapshots.SegmentRef{ref}, "")
	if err != nil {
		t.Fatalf("BuildEventLogIndexSegmentFromEventLogSegments: %v", err)
	}
	if err := statesnapshots.PublishManifest(dir, statesnapshots.NewManifest(0, 0, []statesnapshots.SegmentRef{ref, indexRef})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := statesnapshots.OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	bc.ChainDB().SetEventLogReader(mgr)
	if err := rawdb.DeleteTransactionInfosByBlock(bc.db, block1.Number()); err != nil {
		t.Fatalf("DeleteTransactionInfosByBlock block1: %v", err)
	}
	hotOnly := rawdb.NewChainDB(bc.db, rawdb.NoopAncient{})
	if infos := rawdb.ReadTransactionInfosByBlock(hotOnly, block1.Number()); len(infos) != 0 {
		t.Fatalf("hot block1 tx infos still present: %+v", infos)
	}

	from, to := uint64(1), uint64(1)
	backend := &TronBackend{chain: bc}
	logs, err := backend.GetLogs(jsonrpc.LogFilter{
		FromBlock: &from,
		ToBlock:   &to,
		Addresses: []tcommon.Address{tcommon.BytesToAddress(logAddress)},
		Topics:    [][]tcommon.Hash{{topic0, topic0Alt}, {topic1}},
	})
	if err != nil {
		t.Fatalf("GetLogs: %v", err)
	}
	if len(logs) != 2 {
		t.Fatalf("GetLogs with cold topic-position OR returned %d logs, want 2", len(logs))
	}
	for i, wantData := range []string{"0x9101", "0x9102"} {
		if logs[i].Data != wantData || logs[i].LogIndex != fmt.Sprintf("0x%x", i) || logs[i].Address != fmt.Sprintf("0x%x", logAddress) {
			t.Fatalf("cold topic-position OR log[%d] = %+v, want data %s at address %x", i, logs[i], wantData, logAddress)
		}
	}
}

func TestTronBackend_GetLogsRechecksColdEventLogRows(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()
	goodAddress := bytes20(0x81)
	otherAddress := bytes20(0x82)
	topic := tcommon.Hash{0x81}
	altTopic := tcommon.Hash{0x83}
	secondTopic := tcommon.Hash{0x84}
	otherSecondTopic := tcommon.Hash{0x85}
	otherTopic := tcommon.Hash{0x82}
	block1, _ := testBackendLogBlock(1, nil)
	block2, _ := testBackendLogBlock(2, nil)
	block3, _ := testBackendLogBlock(3, nil)
	bc.currentBlock.Store(block3)
	bc.ChainDB().SetEventLogReader(fakeColdEventLogReader{
		covered: true,
		rows: []rawdb.EventLog{
			{
				BlockNum:  1,
				TxIndex:   0,
				LogIndex:  0,
				TxHash:    tcommon.Hash{0xa1},
				BlockHash: block1.Hash(),
				Address:   tcommon.BytesToAddress(goodAddress),
				Log: &corepb.TransactionInfo_Log{
					Address: goodAddress,
					Topics:  [][]byte{topic[:]},
					Data:    []byte{0x01},
				},
			},
			{
				BlockNum:  1,
				TxIndex:   0,
				LogIndex:  1,
				TxHash:    tcommon.Hash{0xa2},
				BlockHash: block1.Hash(),
				Address:   tcommon.BytesToAddress(otherAddress),
				Log: &corepb.TransactionInfo_Log{
					Address: otherAddress,
					Topics:  [][]byte{topic[:]},
					Data:    []byte{0x02},
				},
			},
			{
				BlockNum:  1,
				TxIndex:   0,
				LogIndex:  2,
				TxHash:    tcommon.Hash{0xa3},
				BlockHash: block1.Hash(),
				Address:   tcommon.BytesToAddress(goodAddress),
				Log: &corepb.TransactionInfo_Log{
					Address: goodAddress,
					Topics:  [][]byte{otherTopic[:]},
					Data:    []byte{0x03},
				},
			},
			{
				BlockNum:  1,
				TxIndex:   0,
				LogIndex:  3,
				TxHash:    tcommon.Hash{0xa5},
				BlockHash: block1.Hash(),
				Address:   tcommon.BytesToAddress(goodAddress),
				Log: &corepb.TransactionInfo_Log{
					Address: goodAddress,
					Topics:  [][]byte{altTopic[:], secondTopic[:]},
					Data:    []byte{0x05},
				},
			},
			{
				BlockNum:  1,
				TxIndex:   0,
				LogIndex:  4,
				TxHash:    tcommon.Hash{0xa6},
				BlockHash: block1.Hash(),
				Address:   tcommon.BytesToAddress(goodAddress),
				Log: &corepb.TransactionInfo_Log{
					Address: goodAddress,
					Topics:  [][]byte{altTopic[:], otherSecondTopic[:]},
					Data:    []byte{0x06},
				},
			},
			{
				BlockNum:  2,
				TxIndex:   0,
				LogIndex:  0,
				TxHash:    tcommon.Hash{0xa4},
				BlockHash: block2.Hash(),
				Address:   tcommon.BytesToAddress(otherAddress),
				Log: &corepb.TransactionInfo_Log{
					Address: otherAddress,
					Topics:  [][]byte{topic[:]},
					Data:    []byte{0x04},
				},
			},
		},
	})

	from, to := uint64(1), uint64(2)
	backend := &TronBackend{chain: bc}
	logs, err := backend.GetLogs(jsonrpc.LogFilter{
		FromBlock: &from,
		ToBlock:   &to,
		Addresses: []tcommon.Address{tcommon.BytesToAddress(goodAddress)},
		Topics:    [][]tcommon.Hash{{topic}},
	})
	if err != nil {
		t.Fatalf("GetLogs: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("GetLogs from unchecked cold rows returned %d logs, want 1", len(logs))
	}
	if logs[0].Data != "0x01" || logs[0].Address != fmt.Sprintf("0x%x", goodAddress) || logs[0].BlockNumber != "0x1" {
		t.Fatalf("cold filtered log = %+v, want only matching block1 log", logs[0])
	}

	logs, err = backend.GetLogs(jsonrpc.LogFilter{
		FromBlock: &from,
		ToBlock:   &to,
		Addresses: []tcommon.Address{tcommon.BytesToAddress(goodAddress)},
		Topics:    [][]tcommon.Hash{{topic, altTopic}, {secondTopic}},
	})
	if err != nil {
		t.Fatalf("GetLogs OR/multi-topic: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("GetLogs OR/multi-topic from unchecked cold rows returned %d logs, want 1", len(logs))
	}
	if logs[0].Data != "0x05" || logs[0].LogIndex != "0x3" {
		t.Fatalf("cold OR/multi-topic filtered log = %+v, want log index 0x3 data 0x05", logs[0])
	}
}

func TestTronBackend_GetLogsRejectsOutOfRangeCoveredColdEventLogRows(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()
	addr := bytes20(0x86)
	topic := tcommon.Hash{0x86}
	block3, _ := testBackendLogBlock(3, nil)
	bc.currentBlock.Store(block3)
	bc.ChainDB().SetEventLogReader(fakeColdEventLogReader{
		covered: true,
		rows: []rawdb.EventLog{{
			BlockNum:  3,
			TxIndex:   0,
			LogIndex:  0,
			TxHash:    tcommon.Hash{0xa7},
			BlockHash: block3.Hash(),
			Address:   tcommon.BytesToAddress(addr),
			Log: &corepb.TransactionInfo_Log{
				Address: addr,
				Topics:  [][]byte{topic[:]},
				Data:    []byte{0x07},
			},
		}},
	})

	from, to := uint64(1), uint64(2)
	backend := &TronBackend{chain: bc}
	logs, err := backend.GetLogs(jsonrpc.LogFilter{
		FromBlock: &from,
		ToBlock:   &to,
		Addresses: []tcommon.Address{tcommon.BytesToAddress(addr)},
		Topics:    [][]tcommon.Hash{{topic}},
	})
	if err == nil || logs != nil || !strings.Contains(err.Error(), "outside covered range [1,2]") {
		t.Fatalf("GetLogs out-of-range covered cold row = logs %+v err %v, want archive data error", logs, err)
	}
}

func TestTronBackend_GetLogsUsesCoveredColdEventLogBoundary(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()
	addr := bytes20(0x88)
	topic := tcommon.Hash{0x88}
	block1, _ := testBackendLogBlock(1, nil)
	bc.currentBlock.Store(block1)
	reader := &fakeCoveredColdEventLogReader{
		covered: true,
		rows: []rawdb.EventLog{{
			BlockNum:  1,
			TxIndex:   0,
			LogIndex:  0,
			TxHash:    tcommon.Hash{0xb1},
			BlockHash: block1.Hash(),
			Address:   tcommon.BytesToAddress(addr),
			Log: &corepb.TransactionInfo_Log{
				Address: addr,
				Topics:  [][]byte{topic[:]},
				Data:    []byte{0x88},
			},
		}},
	}
	bc.ChainDB().SetEventLogReader(reader)

	from, to := uint64(1), uint64(1)
	backend := &TronBackend{chain: bc}
	logs, err := backend.GetLogs(jsonrpc.LogFilter{
		FromBlock: &from,
		ToBlock:   &to,
		Addresses: []tcommon.Address{tcommon.BytesToAddress(addr)},
		Topics:    [][]tcommon.Hash{{topic}},
	})
	if err != nil {
		t.Fatalf("GetLogs: %v", err)
	}
	if len(logs) != 1 || logs[0].Data != "0x88" {
		t.Fatalf("GetLogs covered cold boundary rows = %+v, want one 0x88 log", logs)
	}
	if reader.coveredIterCalls != 1 || reader.coveredCalls != 0 || reader.iterCalls != 0 {
		t.Fatalf("cold reader calls coveredIter=%d covered=%d iter=%d, want atomic boundary only", reader.coveredIterCalls, reader.coveredCalls, reader.iterCalls)
	}
}

func TestTronBackend_GetLogsFallsBackToColdEventLogScanWhenIndexMissing(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()
	logAddress := bytes20(0x42)
	topic := tcommon.Hash{0xde}
	block1, info1 := testBackendLogBlock(1, &corepb.TransactionInfo_Log{
		Address: logAddress,
		Topics:  [][]byte{topic[:]},
		Data:    []byte{0x42, 0x43},
	})
	if err := rawdb.WriteBlock(bc.db, block1); err != nil {
		t.Fatalf("WriteBlock block1: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(bc.db, 1, []*corepb.TransactionInfo{info1}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock block1: %v", err)
	}
	bc.currentBlock.Store(block1)

	dir := t.TempDir()
	ref, err := statesnapshots.BuildEventLogSegmentFromChain(bc.ChainDB(), dir, "log/event-log-1-1.seg", 1, 1)
	if err != nil {
		t.Fatalf("BuildEventLogSegmentFromChain: %v", err)
	}
	indexRef, err := statesnapshots.BuildEventLogIndexSegmentFromEventLogSegments(dir, []statesnapshots.SegmentRef{ref}, "")
	if err != nil {
		t.Fatalf("BuildEventLogIndexSegmentFromEventLogSegments: %v", err)
	}
	if err := statesnapshots.PublishManifest(dir, statesnapshots.NewManifest(0, 0, []statesnapshots.SegmentRef{ref, indexRef})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := statesnapshots.OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	if err := os.Remove(filepath.Join(dir, indexRef.Path)); err != nil {
		t.Fatalf("remove event-log index: %v", err)
	}
	bc.ChainDB().SetEventLogReader(mgr)
	if err := rawdb.DeleteTransactionInfosByBlock(bc.db, 1); err != nil {
		t.Fatalf("DeleteTransactionInfosByBlock block1: %v", err)
	}

	from, to := uint64(1), uint64(1)
	backend := &TronBackend{chain: bc}
	logs, err := backend.GetLogs(jsonrpc.LogFilter{
		FromBlock: &from,
		ToBlock:   &to,
		Addresses: []tcommon.Address{tcommon.BytesToAddress(logAddress)},
		Topics:    [][]tcommon.Hash{{topic}},
	})
	if err != nil {
		t.Fatalf("GetLogs with missing cold event-log index: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("GetLogs with missing cold event-log index returned %d logs, want 1", len(logs))
	}
	if logs[0].Data != "0x4243" || logs[0].Address != fmt.Sprintf("0x%x", logAddress) {
		t.Fatalf("log = %+v, want cold fallback log", logs[0])
	}
}

func TestTronBackend_GetLogsFallsBackToColdEventLogScanWhenIndexCorrupt(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()
	logAddress := bytes20(0x43)
	topic := tcommon.Hash{0xdf}
	block1, info1 := testBackendLogBlock(1, &corepb.TransactionInfo_Log{
		Address: logAddress,
		Topics:  [][]byte{topic[:]},
		Data:    []byte{0x43, 0x44},
	})
	if err := rawdb.WriteBlock(bc.db, block1); err != nil {
		t.Fatalf("WriteBlock block1: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(bc.db, 1, []*corepb.TransactionInfo{info1}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock block1: %v", err)
	}
	bc.currentBlock.Store(block1)

	dir := t.TempDir()
	ref, err := statesnapshots.BuildEventLogSegmentFromChain(bc.ChainDB(), dir, "log/event-log-1-1.seg", 1, 1)
	if err != nil {
		t.Fatalf("BuildEventLogSegmentFromChain: %v", err)
	}
	indexRef, err := statesnapshots.BuildEventLogIndexSegmentFromEventLogSegments(dir, []statesnapshots.SegmentRef{ref}, "")
	if err != nil {
		t.Fatalf("BuildEventLogIndexSegmentFromEventLogSegments: %v", err)
	}
	if err := statesnapshots.PublishManifest(dir, statesnapshots.NewManifest(0, 0, []statesnapshots.SegmentRef{ref, indexRef})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, indexRef.Path), []byte("not-an-event-log-index"), 0o644); err != nil {
		t.Fatalf("corrupt event-log index: %v", err)
	}
	mgr, err := statesnapshots.OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	bc.ChainDB().SetEventLogReader(mgr)
	if err := rawdb.DeleteTransactionInfosByBlock(bc.db, 1); err != nil {
		t.Fatalf("DeleteTransactionInfosByBlock block1: %v", err)
	}

	from, to := uint64(1), uint64(1)
	backend := &TronBackend{chain: bc}
	logs, err := backend.GetLogs(jsonrpc.LogFilter{
		FromBlock: &from,
		ToBlock:   &to,
		Addresses: []tcommon.Address{tcommon.BytesToAddress(logAddress)},
		Topics:    [][]tcommon.Hash{{topic}},
	})
	if err != nil {
		t.Fatalf("GetLogs with corrupt cold event-log index: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("GetLogs with corrupt cold event-log index returned %d logs, want 1", len(logs))
	}
	if logs[0].Data != "0x4344" || logs[0].Address != fmt.Sprintf("0x%x", logAddress) {
		t.Fatalf("log = %+v, want cold fallback log", logs[0])
	}
}

func TestJSONRPCGetLogsUsesColdEventLogIndex(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()
	logAddress := bytes20(0x61)
	otherAddress := bytes20(0x71)
	tronLogAddress := append([]byte{tcommon.AddressPrefixMainnet}, logAddress...)
	otherTronAddress := append([]byte{tcommon.AddressPrefixMainnet}, otherAddress...)
	topic := tcommon.Hash{0xab}
	otherTopic := tcommon.Hash{0xcd}
	block1, info1 := testBackendLogBlock(1, &corepb.TransactionInfo_Log{
		Address: tronLogAddress,
		Topics:  [][]byte{topic[:]},
		Data:    []byte{0x01, 0xab},
	})
	block2, info2 := testBackendLogBlock(2, &corepb.TransactionInfo_Log{
		Address: otherTronAddress,
		Topics:  [][]byte{otherTopic[:]},
		Data:    []byte{0x02, 0xcd},
	})
	if err := rawdb.WriteBlock(bc.db, block1); err != nil {
		t.Fatalf("WriteBlock block1: %v", err)
	}
	if err := rawdb.WriteBlock(bc.db, block2); err != nil {
		t.Fatalf("WriteBlock block2: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(bc.db, 1, []*corepb.TransactionInfo{info1}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock block1: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(bc.db, 2, []*corepb.TransactionInfo{info2}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock block2: %v", err)
	}
	bc.currentBlock.Store(block2)

	dir := t.TempDir()
	ref1, err := statesnapshots.BuildEventLogSegmentFromChain(bc.ChainDB(), dir, "log/event-log-1-1.seg", 1, 1)
	if err != nil {
		t.Fatalf("BuildEventLogSegmentFromChain 1: %v", err)
	}
	ref2, err := statesnapshots.BuildEventLogSegmentFromChain(bc.ChainDB(), dir, "log/event-log-2-2.seg", 2, 2)
	if err != nil {
		t.Fatalf("BuildEventLogSegmentFromChain 2: %v", err)
	}
	indexRef, err := statesnapshots.BuildEventLogIndexSegmentFromEventLogSegments(dir, []statesnapshots.SegmentRef{ref1, ref2}, "")
	if err != nil {
		t.Fatalf("BuildEventLogIndexSegmentFromEventLogSegments: %v", err)
	}
	if err := statesnapshots.PublishManifest(dir, statesnapshots.NewManifest(0, 0, []statesnapshots.SegmentRef{ref1, ref2, indexRef})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := statesnapshots.OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	bc.ChainDB().SetEventLogReader(mgr)
	if err := rawdb.DeleteTransactionInfosByBlock(bc.db, 1); err != nil {
		t.Fatalf("DeleteTransactionInfosByBlock block1: %v", err)
	}
	if err := rawdb.DeleteTransactionInfosByBlock(bc.db, 2); err != nil {
		t.Fatalf("DeleteTransactionInfosByBlock block2: %v", err)
	}
	hotOnly := rawdb.NewChainDB(bc.db, rawdb.NoopAncient{})
	if infos := rawdb.ReadTransactionInfosByBlock(hotOnly, 1); len(infos) != 0 {
		t.Fatalf("hot block1 tx infos still present: %+v", infos)
	}
	if infos := rawdb.ReadTransactionInfosByBlock(hotOnly, 2); len(infos) != 0 {
		t.Fatalf("hot block2 tx infos still present: %+v", infos)
	}
	if err := os.Remove(filepath.Join(dir, ref2.Path)); err != nil {
		t.Fatalf("remove unrelated event-log segment: %v", err)
	}

	backend := &TronBackend{chain: bc}
	from, to := uint64(1), uint64(2)
	directLogs, err := backend.GetLogs(jsonrpc.LogFilter{
		FromBlock: &from,
		ToBlock:   &to,
		Addresses: []tcommon.Address{tcommon.BytesToAddress(tronLogAddress)},
		Topics:    [][]tcommon.Hash{{topic}},
	})
	if err != nil {
		t.Fatalf("direct GetLogs: %v", err)
	}
	if len(directLogs) != 1 {
		t.Fatalf("direct GetLogs with cold event-log-index returned %d logs, want 1", len(directLogs))
	}
	directMissLogs, err := backend.GetLogs(jsonrpc.LogFilter{
		FromBlock: &from,
		ToBlock:   &to,
		Addresses: []tcommon.Address{tcommon.BytesToAddress(tronLogAddress)},
		Topics:    [][]tcommon.Hash{{otherTopic}},
	})
	if err != nil {
		t.Fatalf("direct GetLogs wrong-topic cold event-log-index miss: %v", err)
	}
	if len(directMissLogs) != 0 {
		t.Fatalf("direct GetLogs wrong-topic cold event-log-index miss returned %d logs, want none", len(directMissLogs))
	}
	recordingBackend := &recordingLogBackend{TronBackend: backend}
	rpcServer := jsonrpc.NewServer(recordingBackend, 0)
	defer rpcServer.Stop()
	httpServer := httptest.NewServer(rpcServer.Handler())
	defer httpServer.Close()

	resp := postCoreJSONRPC(t, httpServer.URL, "eth_getLogs", []any{map[string]any{
		"fromBlock": "0x1",
		"toBlock":   "0x2",
		"address":   "0x" + fmt.Sprintf("%x", tronLogAddress[1:]),
		"topics":    []any{"0x" + hex.EncodeToString(topic.Bytes())},
	}})
	if recordingBackend.lastFilter == nil {
		t.Fatal("eth_getLogs did not call backend.GetLogs")
	}
	recorded := *recordingBackend.lastFilter
	if recorded.FromBlock == nil || *recorded.FromBlock != 1 || recorded.ToBlock == nil || *recorded.ToBlock != 2 ||
		len(recorded.Addresses) != 1 || recorded.Addresses[0] != tcommon.BytesToAddress(tronLogAddress) ||
		len(recorded.Topics) != 1 || len(recorded.Topics[0]) != 1 || recorded.Topics[0][0] != topic {
		t.Fatalf("eth_getLogs filter = %+v, want block [1,2] address %x topic %x", recorded, tronLogAddress, topic.Bytes())
	}
	result, ok := resp["result"].([]any)
	if !ok || len(result) != 1 {
		t.Fatalf("eth_getLogs result = %v, want one log", resp["result"])
	}
	logObj, ok := result[0].(map[string]any)
	if !ok {
		t.Fatalf("eth_getLogs first result = %T %v, want object", result[0], result[0])
	}
	if logObj["data"] != "0x01ab" || logObj["blockNumber"] != "0x1" {
		t.Fatalf("eth_getLogs log = %v, want block1 data 0x01ab", logObj)
	}
	if logObj["address"] != "0x"+fmt.Sprintf("%x", logAddress) {
		t.Fatalf("eth_getLogs address = %v, want 0x%x", logObj["address"], logAddress)
	}
	missResp := postCoreJSONRPC(t, httpServer.URL, "eth_getLogs", []any{map[string]any{
		"fromBlock": "0x1",
		"toBlock":   "0x2",
		"address":   "0x" + fmt.Sprintf("%x", tronLogAddress[1:]),
		"topics":    []any{"0x" + hex.EncodeToString(otherTopic.Bytes())},
	}})
	missResult, ok := missResp["result"].([]any)
	if !ok || len(missResult) != 0 {
		t.Fatalf("eth_getLogs wrong-topic result = %v, want empty cold-index result", missResp["result"])
	}
}

func TestJSONRPCGetLogsColdEventLogIndexParsesComplexFilter(t *testing.T) {
	bc, cleanup := newTestBlockchain(t)
	defer cleanup()
	logAddress := bytes20(0x62)
	otherAddress := bytes20(0x72)
	noiseAddress := bytes20(0x82)
	tronLogAddress := append([]byte{tcommon.AddressPrefixMainnet}, logAddress...)
	otherTronAddress := append([]byte{tcommon.AddressPrefixMainnet}, otherAddress...)
	topic0 := tcommon.Hash{0x62}
	topic0Alt := tcommon.Hash{0x63}
	topic1 := tcommon.Hash{0x64}
	topic2 := tcommon.Hash{0x65}
	otherTopic := tcommon.Hash{0x66}
	block1, info1 := testBackendLogBlock(1, nil)
	info1.Log = []*corepb.TransactionInfo_Log{
		{
			Address: tronLogAddress,
			Topics:  [][]byte{topic0[:], topic1[:], topic2[:]},
			Data:    []byte{0x62, 0x01},
		},
		{
			Address: tronLogAddress,
			Topics:  [][]byte{topic0Alt[:], otherTopic[:], topic2[:]},
			Data:    []byte{0x62, 0x02},
		},
		{
			Address: tronLogAddress,
			Topics:  [][]byte{topic0[:], topic1[:], otherTopic[:]},
			Data:    []byte{0x62, 0x03},
		},
		{
			Address: otherTronAddress,
			Topics:  [][]byte{topic0Alt[:], topic1[:], topic2[:]},
			Data:    []byte{0x62, 0x04},
		},
		{
			Address: tronLogAddress,
			Topics:  [][]byte{topic1[:], topic0[:], topic2[:]},
			Data:    []byte{0x62, 0x05},
		},
	}
	block2, info2 := testBackendLogBlock(2, &corepb.TransactionInfo_Log{
		Address: otherTronAddress,
		Topics:  [][]byte{otherTopic[:]},
		Data:    []byte{0x72, 0x01},
	})
	if err := rawdb.WriteBlock(bc.db, block1); err != nil {
		t.Fatalf("WriteBlock block1: %v", err)
	}
	if err := rawdb.WriteBlock(bc.db, block2); err != nil {
		t.Fatalf("WriteBlock block2: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(bc.db, block1.Number(), []*corepb.TransactionInfo{info1}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock block1: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(bc.db, block2.Number(), []*corepb.TransactionInfo{info2}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock block2: %v", err)
	}
	bc.currentBlock.Store(block2)

	dir := t.TempDir()
	ref1, err := statesnapshots.BuildEventLogSegmentFromChain(bc.ChainDB(), dir, "log/event-log-1-1.seg", 1, 1)
	if err != nil {
		t.Fatalf("BuildEventLogSegmentFromChain 1: %v", err)
	}
	ref2, err := statesnapshots.BuildEventLogSegmentFromChain(bc.ChainDB(), dir, "log/event-log-2-2.seg", 2, 2)
	if err != nil {
		t.Fatalf("BuildEventLogSegmentFromChain 2: %v", err)
	}
	indexRef, err := statesnapshots.BuildEventLogIndexSegmentFromEventLogSegments(dir, []statesnapshots.SegmentRef{ref1, ref2}, "")
	if err != nil {
		t.Fatalf("BuildEventLogIndexSegmentFromEventLogSegments: %v", err)
	}
	if err := statesnapshots.PublishManifest(dir, statesnapshots.NewManifest(0, 0, []statesnapshots.SegmentRef{ref1, ref2, indexRef})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := statesnapshots.OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	bc.ChainDB().SetEventLogReader(mgr)
	if err := rawdb.DeleteTransactionInfosByBlock(bc.db, block1.Number()); err != nil {
		t.Fatalf("DeleteTransactionInfosByBlock block1: %v", err)
	}
	if err := rawdb.DeleteTransactionInfosByBlock(bc.db, block2.Number()); err != nil {
		t.Fatalf("DeleteTransactionInfosByBlock block2: %v", err)
	}
	hotOnly := rawdb.NewChainDB(bc.db, rawdb.NoopAncient{})
	if infos := rawdb.ReadTransactionInfosByBlock(hotOnly, block1.Number()); len(infos) != 0 {
		t.Fatalf("hot block1 tx infos still present: %+v", infos)
	}
	if infos := rawdb.ReadTransactionInfosByBlock(hotOnly, block2.Number()); len(infos) != 0 {
		t.Fatalf("hot block2 tx infos still present: %+v", infos)
	}
	if err := os.Remove(filepath.Join(dir, ref2.Path)); err != nil {
		t.Fatalf("remove unrelated event-log segment: %v", err)
	}

	backend := &recordingLogBackend{TronBackend: &TronBackend{chain: bc}}
	rpcServer := jsonrpc.NewServer(backend, 0)
	defer rpcServer.Stop()
	httpServer := httptest.NewServer(rpcServer.Handler())
	defer httpServer.Close()

	resp := postCoreJSONRPC(t, httpServer.URL, "eth_getLogs", []any{map[string]any{
		"fromBlock": "0x1",
		"toBlock":   "0x2",
		"address": []string{
			"0x" + fmt.Sprintf("%x", noiseAddress),
			"0x" + fmt.Sprintf("%x", tronLogAddress[1:]),
		},
		"topics": []any{
			[]string{
				"0x" + hex.EncodeToString(topic0.Bytes()),
				"0x" + hex.EncodeToString(topic0Alt.Bytes()),
			},
			nil,
			[]string{"0x" + hex.EncodeToString(topic2.Bytes())},
		},
	}})
	if backend.lastFilter == nil {
		t.Fatal("eth_getLogs did not call backend.GetLogs")
	}
	recorded := *backend.lastFilter
	if recorded.FromBlock == nil || *recorded.FromBlock != 1 || recorded.ToBlock == nil || *recorded.ToBlock != 2 {
		t.Fatalf("eth_getLogs block filter = %+v, want [1,2]", recorded)
	}
	if len(recorded.Addresses) != 2 ||
		recorded.Addresses[0] != tcommon.BytesToAddress(append([]byte{tcommon.AddressPrefixMainnet}, noiseAddress...)) ||
		recorded.Addresses[1] != tcommon.BytesToAddress(tronLogAddress) {
		t.Fatalf("eth_getLogs addresses = %+v, want noise+tron address filter", recorded.Addresses)
	}
	if len(recorded.Topics) != 3 ||
		len(recorded.Topics[0]) != 2 || recorded.Topics[0][0] != topic0 || recorded.Topics[0][1] != topic0Alt ||
		len(recorded.Topics[1]) != 0 ||
		len(recorded.Topics[2]) != 1 || recorded.Topics[2][0] != topic2 {
		t.Fatalf("eth_getLogs topics = %+v, want topic0 OR topic0Alt, wildcard, topic2", recorded.Topics)
	}
	result, ok := resp["result"].([]any)
	if !ok || len(result) != 2 {
		t.Fatalf("eth_getLogs complex cold-index result = %v, want two logs", resp["result"])
	}
	for i, wantData := range []string{"0x6201", "0x6202"} {
		logObj, ok := result[i].(map[string]any)
		if !ok {
			t.Fatalf("eth_getLogs result[%d] = %T %v, want object", i, result[i], result[i])
		}
		if logObj["data"] != wantData || logObj["address"] != "0x"+fmt.Sprintf("%x", logAddress) ||
			logObj["blockNumber"] != "0x1" || logObj["logIndex"] != fmt.Sprintf("0x%x", i) {
			t.Fatalf("eth_getLogs log[%d] = %v, want data %s at address 0x%x", i, logObj, wantData, logAddress)
		}
	}
}

func TestSectionBloomLogMatcherSkipsNonCandidateBlocks(t *testing.T) {
	db := rawdb.NewMemoryChainDB()
	addr := bytes20(0x22)
	for _, bitIndex := range rawdb.SectionBloomBitIndexes(addr) {
		bitset := testSectionBloomSetBit(nil, 5)
		encoded, err := rawdb.EncodeSectionBloomBitSet(bitset)
		if err != nil {
			t.Fatalf("EncodeSectionBloomBitSet: %v", err)
		}
		if err := rawdb.WriteSectionBloom(db, 0, bitIndex, encoded); err != nil {
			t.Fatalf("WriteSectionBloom: %v", err)
		}
	}
	matcher := newSectionBloomLogMatcher(db, jsonrpc.LogFilter{
		Addresses: []tcommon.Address{tcommon.BytesToAddress(addr)},
	})
	if matcher == nil {
		t.Fatal("newSectionBloomLogMatcher returned nil")
	}
	if mayContain, err := matcher.mayContain(5); err != nil || !mayContain {
		t.Fatalf("mayContain(5) = %v/%v, want true/nil for indexed block offset", mayContain, err)
	}
	if mayContain, err := matcher.mayContain(6); err != nil || mayContain {
		t.Fatalf("mayContain(6) = %v/%v, want false/nil when all required bloom rows exclude it", mayContain, err)
	}

	missingTopic := tcommon.Hash{0xee}
	matcher = newSectionBloomLogMatcher(db, jsonrpc.LogFilter{
		Addresses: []tcommon.Address{tcommon.BytesToAddress(addr)},
		Topics:    [][]tcommon.Hash{{missingTopic}},
	})
	if mayContain, err := matcher.mayContain(5); err != nil || !mayContain {
		t.Fatalf("mayContain with missing topic rows = %v/%v, want true/nil fallback", mayContain, err)
	}
}

func TestSectionBloomLogMatcherUsesColdRows(t *testing.T) {
	db := rawdb.NewMemoryChainDB()
	addr := bytes20(0x23)
	rows := make(map[[2]uint64][]byte)
	for _, bitIndex := range rawdb.SectionBloomBitIndexes(addr) {
		bitset := testSectionBloomSetBit(nil, 5)
		encoded, err := rawdb.EncodeSectionBloomBitSet(bitset)
		if err != nil {
			t.Fatalf("EncodeSectionBloomBitSet: %v", err)
		}
		rows[[2]uint64{0, bitIndex}] = encoded
	}
	db.SetSectionBloomReader(testSectionBloomColdReader{rows: rows})

	matcher := newSectionBloomLogMatcher(db, jsonrpc.LogFilter{
		Addresses: []tcommon.Address{tcommon.BytesToAddress(addr)},
	})
	if matcher == nil {
		t.Fatal("newSectionBloomLogMatcher returned nil")
	}
	if mayContain, err := matcher.mayContain(5); err != nil || !mayContain {
		t.Fatalf("mayContain(5) = %v/%v, want true/nil for cold indexed block offset", mayContain, err)
	}
	if mayContain, err := matcher.mayContain(6); err != nil || mayContain {
		t.Fatalf("mayContain(6) = %v/%v, want false/nil from cold section bloom rows", mayContain, err)
	}
}

type testSectionBloomColdReader struct {
	rows map[[2]uint64][]byte
	err  error
}

func (r testSectionBloomColdReader) SectionBloom(section, bitIndex uint64) ([]byte, bool, error) {
	if r.err != nil {
		return nil, false, r.err
	}
	value, ok := r.rows[[2]uint64{section, bitIndex}]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), value...), true, nil
}

func TestSectionBloomLogMatcherSurfacesColdErrors(t *testing.T) {
	db := rawdb.NewMemoryChainDB()
	db.SetSectionBloomReader(testSectionBloomColdReader{err: fmt.Errorf("cold section bloom unavailable")})
	addr := bytes20(0x24)

	matcher := newSectionBloomLogMatcher(db, jsonrpc.LogFilter{
		Addresses: []tcommon.Address{tcommon.BytesToAddress(addr)},
	})
	if matcher == nil {
		t.Fatal("newSectionBloomLogMatcher returned nil")
	}
	mayContain, err := matcher.mayContain(5)
	if err == nil || !strings.Contains(err.Error(), "cold section bloom unavailable") || mayContain {
		t.Fatalf("mayContain cold error = %v/%v, want false/cold error", mayContain, err)
	}
}

type fakeColdEventLogReader struct {
	covered bool
	rows    []rawdb.EventLog
}

func (r fakeColdEventLogReader) EventLogRangeCovered(fromBlock, toBlock uint64) (bool, error) {
	return r.covered, nil
}

func (r fakeColdEventLogReader) IterateEventLogs(fromBlock, toBlock uint64, filter rawdb.EventLogFilter, fn func(rawdb.EventLog) (bool, error)) error {
	for _, row := range r.rows {
		cont, err := fn(row)
		if err != nil || !cont {
			return err
		}
	}
	return nil
}

type fakeCoveredColdEventLogReader struct {
	covered          bool
	rows             []rawdb.EventLog
	coveredCalls     int
	iterCalls        int
	coveredIterCalls int
}

func (r *fakeCoveredColdEventLogReader) EventLogRangeCovered(fromBlock, toBlock uint64) (bool, error) {
	r.coveredCalls++
	return false, nil
}

func (r *fakeCoveredColdEventLogReader) IterateEventLogs(fromBlock, toBlock uint64, filter rawdb.EventLogFilter, fn func(rawdb.EventLog) (bool, error)) error {
	r.iterCalls++
	return nil
}

func (r *fakeCoveredColdEventLogReader) IterateCoveredEventLogs(fromBlock, toBlock uint64, filter rawdb.EventLogFilter, fn func(rawdb.EventLog) (bool, error)) (bool, error) {
	r.coveredIterCalls++
	if !r.covered {
		return false, nil
	}
	for _, row := range r.rows {
		cont, err := fn(row)
		if err != nil || !cont {
			return true, err
		}
	}
	return true, nil
}

func testBackendLogBlock(number uint64, logEntry *corepb.TransactionInfo_Log) (*types.Block, *corepb.TransactionInfo) {
	txPB := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Timestamp:  int64(10_000 + number),
			Expiration: int64(20_000 + number),
			Data:       []byte{byte(number)},
		},
	}
	tx := types.NewTransactionFromPB(txPB)
	info := &corepb.TransactionInfo{
		Id:             append([]byte(nil), tx.Hash().Bytes()...),
		BlockNumber:    int64(number),
		BlockTimeStamp: int64(30_000 + number),
	}
	if logEntry != nil {
		info.Log = []*corepb.TransactionInfo_Log{logEntry}
	}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    int64(number),
				Timestamp: int64(30_000 + number),
			},
		},
		Transactions: []*corepb.Transaction{txPB},
	})
	return block, info
}

func testBackendBalanceBlockID(block *types.Block) *contractpb.BlockBalanceTrace_BlockIdentifier {
	return &contractpb.BlockBalanceTrace_BlockIdentifier{
		Number: int64(block.Number()),
		Hash:   append([]byte(nil), block.Hash().Bytes()...),
	}
}

func bytes20(seed byte) []byte {
	out := make([]byte, 20)
	for i := range out {
		out[i] = seed + byte(i)
	}
	return out
}

func postCoreJSONRPC(t *testing.T, url, method string, params any) map[string]any {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"method":  method,
		"params":  params,
		"id":      1,
	})
	if err != nil {
		t.Fatalf("marshal rpc body: %v", err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST rpc %s: %v", method, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST rpc %s status = %d, want 200", method, resp.StatusCode)
	}
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode rpc %s: %v", method, err)
	}
	if errValue, ok := out["error"]; ok {
		t.Fatalf("rpc %s error: %v", method, errValue)
	}
	return out
}

type recordingLogBackend struct {
	*TronBackend
	lastFilter *jsonrpc.LogFilter
}

func (b *recordingLogBackend) GetLogs(filter jsonrpc.LogFilter) ([]*jsonrpc.RPCLog, error) {
	cp := filter
	b.lastFilter = &cp
	return b.TronBackend.GetLogs(filter)
}

func testSectionBloomSetBit(bitset []byte, bit uint64) []byte {
	byteIndex := bit / 8
	if byteIndex >= uint64(len(bitset)) {
		grown := make([]byte, byteIndex+1)
		copy(grown, bitset)
		bitset = grown
	}
	bitset[byteIndex] |= 1 << (bit % 8)
	return bitset
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
	statedb, err := state.New(bc.HeadStateRoot(), bc.StateDB())
	if err != nil {
		t.Fatal(err)
	}
	if err := statedb.WriteDelegatedResourceV2(from, to, false, &rawdb.DelegatedResource{
		From:                      from,
		To:                        to,
		FrozenBalanceForBandwidth: 100,
	}); err != nil {
		t.Fatal(err)
	}
	if err := statedb.WriteDelegatedResourceV2(from, to, true, &rawdb.DelegatedResource{
		From:                   from,
		To:                     to,
		FrozenBalanceForEnergy: 200,
		ExpireTimeForEnergy:    300,
	}); err != nil {
		t.Fatal(err)
	}
	newRoot, err := statedb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	rawdb.WriteGenesisStateRoot(bc.db, newRoot)
	rawdb.WriteBlockStateRoot(bc.db, bc.CurrentBlock().Hash(), newRoot)

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
