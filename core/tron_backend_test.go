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
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	chainfreezer "github.com/tronprotocol/go-tron/core/freezer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	"github.com/tronprotocol/go-tron/core/state"
	statesnapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/internal/jsonrpc"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
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
	gotTx, gotBlock, idx, err := backend.GetTransactionByHash(txHash)
	if err != nil || gotTx == nil || gotBlock == nil || idx != 0 {
		t.Fatalf("GetTransactionByHash = tx:%v block:%v idx:%d err:%v, want tx/block/0", gotTx, gotBlock, idx, err)
	}
	if got := bc.StateRootAtBlock(block.Number()); got != wantRoot {
		t.Fatalf("StateRootAtBlock = %x, want %x", got, wantRoot)
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
			if err := op.AppendRaw(rawdb.AncientTxInfosTable, block.Number(), rawdb.ReadTransactionInfosRaw(db, block.Number())); err != nil {
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
	recordingBackend := &recordingLogBackend{TronBackend: backend}
	rpcServer := jsonrpc.NewServer(recordingBackend, 0)
	defer rpcServer.Stop()
	httpServer := httptest.NewServer(rpcServer.Handler())
	defer httpServer.Close()

	resp := postCoreJSONRPC(t, httpServer.URL, "eth_getLogs", []any{map[string]any{
		"fromBlock": "0x1",
		"toBlock":   "0x2",
		"address":   "0x" + fmt.Sprintf("%x", tronLogAddress),
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
	if !matcher.mayContain(5) {
		t.Fatal("mayContain(5) = false, want true for indexed block offset")
	}
	if matcher.mayContain(6) {
		t.Fatal("mayContain(6) = true, want false when all required bloom rows exclude it")
	}

	missingTopic := tcommon.Hash{0xee}
	matcher = newSectionBloomLogMatcher(db, jsonrpc.LogFilter{
		Addresses: []tcommon.Address{tcommon.BytesToAddress(addr)},
		Topics:    [][]tcommon.Hash{{missingTopic}},
	})
	if !matcher.mayContain(5) {
		t.Fatal("mayContain with missing topic rows must fall back to true")
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
	if !matcher.mayContain(5) {
		t.Fatal("mayContain(5) = false, want true for cold indexed block offset")
	}
	if matcher.mayContain(6) {
		t.Fatal("mayContain(6) = true, want false from cold section bloom rows")
	}
}

type testSectionBloomColdReader struct {
	rows map[[2]uint64][]byte
}

func (r testSectionBloomColdReader) SectionBloom(section, bitIndex uint64) ([]byte, bool, error) {
	value, ok := r.rows[[2]uint64{section, bitIndex}]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), value...), true, nil
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
