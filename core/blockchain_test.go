package core

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// TestFlipWitnessIsJobs_Rotation locks java-tron MaintenanceManager.applyBlock
// parity (MaintenanceManager.java:135-145): when the active set rotates at a
// maintenance boundary, is_jobs is cleared on every outgoing member and set on
// every incoming member. Members present in both sets end up true.
func TestFlipWitnessIsJobs_Rotation(t *testing.T) {
	statedb := newTestStateDB(t)
	a, b, c := testCoreAddr(1), testCoreAddr(2), testCoreAddr(3)
	for _, addr := range []tcommon.Address{a, b, c} {
		w := types.NewWitness(addr, "")
		w.SetIsJobs(addr == a || addr == b) // a,b are the current active set
		if err := statedb.SetWitnessCapsule(w); err != nil {
			t.Fatal(err)
		}
	}

	flipWitnessIsJobs(statedb, []tcommon.Address{a, b}, []tcommon.Address{b, c})

	want := map[tcommon.Address]bool{a: false, b: true, c: true}
	for addr, exp := range want {
		w := statedb.GetWitness(addr)
		if w == nil {
			t.Fatalf("witness %s missing", addr.Hex())
		}
		if w.IsJobs() != exp {
			t.Errorf("witness %s: IsJobs=%v, want %v", addr.Hex(), w.IsJobs(), exp)
		}
	}
}

// TestFlipWitnessIsJobs_NoChangeSkips verifies the java-tron guard: when the
// active set is unchanged (order-independent), no witness records are touched.
func TestFlipWitnessIsJobs_NoChangeSkips(t *testing.T) {
	statedb := newTestStateDB(t)
	a, b := testCoreAddr(1), testCoreAddr(2)
	for _, addr := range []tcommon.Address{a, b} {
		w := types.NewWitness(addr, "")
		w.SetIsJobs(false) // deliberately stale; guard must leave it untouched
		if err := statedb.SetWitnessCapsule(w); err != nil {
			t.Fatal(err)
		}
	}

	flipWitnessIsJobs(statedb, []tcommon.Address{a, b}, []tcommon.Address{b, a})

	for _, addr := range []tcommon.Address{a, b} {
		if w := statedb.GetWitness(addr); w.IsJobs() {
			t.Errorf("witness %s: IsJobs rewritten despite unchanged set", addr.Hex())
		}
	}
}

func TestNewBlockChain(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	genesis := &params.Genesis{
		Config: params.MainnetChainConfig,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 1000000},
		},
	}

	_, _, err := SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatal(err)
	}

	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	if bc.CurrentBlock() == nil {
		t.Fatal("current block should not be nil")
	}
	if bc.CurrentBlock().Number() != 0 {
		t.Fatalf("current block number: want 0, got %d", bc.CurrentBlock().Number())
	}
}

func TestBlockChainInsertBlock(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 99_000_000_000_000_000},
		},
	}

	_, genesisHash, err := SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatal(err)
	}

	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	block1Header := &corepb.BlockHeaderRaw{
		Number:     1,
		Timestamp:  3000,
		ParentHash: genesisHash[:],
	}

	block1 := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: block1Header,
		},
	})

	err = bc.InsertBlockWithoutVerify(block1)
	if err != nil {
		t.Fatal(err)
	}

	if bc.CurrentBlock().Number() != 1 {
		t.Fatalf("current block number: want 1, got %d", bc.CurrentBlock().Number())
	}

	stored := rawdb.ReadBlock(rawdb.NewChainDB(diskdb, rawdb.NoopAncient{}), 1)
	if stored == nil {
		t.Fatal("block 1 not stored")
	}
}

func TestBlockChainSpeculativeSafetyCircuitRetriesBlockSerially(t *testing.T) {
	for _, asyncCommit := range []bool{false, true} {
		asyncCommit := asyncCommit
		name := "sync-commit"
		if asyncCommit {
			name = "async-commit"
		}
		t.Run(name, func(t *testing.T) {
			diskdb := ethrawdb.NewMemoryDatabase()
			genesis := &params.Genesis{
				Config:    params.MainnetChainConfig,
				Timestamp: 0,
				Accounts: []params.GenesisAccount{
					{Address: testCoreAddr(1), Balance: 1_000_000},
				},
			}
			_, genesisHash, err := SetupGenesisBlock(diskdb, genesis)
			if err != nil {
				t.Fatal(err)
			}
			bc, err := NewBlockChain(diskdb, state.NewDatabase(diskdb), params.MainnetChainConfig)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = bc.Close() })
			bc.SetAsyncCommit(asyncCommit)
			bc.SetParallelTransferExecution(true)
			bc.SetParallelVMExecution(true)
			failedAttemptRawKey := []byte("speculative-safety-failed-attempt")

			var attempts [3]int
			bc.speculativeSafetyTestHook = func(block *types.Block, options processBlockOptions) error {
				attempts[block.Number()]++
				if block.Number() == 1 {
					if !options.parallelTransfers || !options.parallelVM {
						t.Fatalf("block 1 options = %+v, want both speculative publishers", options)
					}
					return nil
				}
				if attempts[2] == 1 {
					if !options.parallelTransfers || !options.parallelVM {
						t.Fatalf("first block 2 attempt options = %+v, want both speculative publishers", options)
					}
					if err := bc.buffer.Put(failedAttemptRawKey, []byte("must-be-discarded")); err != nil {
						t.Fatalf("stage failed-attempt raw write: %v", err)
					}
					return fmt.Errorf("%w: injected", errSpeculativePublicationAudit)
				}
				if options.parallelTransfers || options.parallelVM {
					t.Fatalf("retry options = %+v, want serial execution", options)
				}
				if exists, err := bc.buffer.Has(failedAttemptRawKey); err != nil || exists {
					t.Fatalf("failed-attempt raw write survived into serial retry: exists=%t err=%v", exists, err)
				}
				return nil
			}
			block1 := types.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
				Number: 1, Timestamp: 3_000, ParentHash: genesisHash.Bytes(),
			}}})
			block2 := types.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
				Number: 2, Timestamp: 6_000, ParentHash: block1.Hash().Bytes(),
			}}})
			if err := bc.InsertBlocks([]*types.Block{block1, block2}); err != nil {
				t.Fatalf("insert range with serial safety retry: %v", err)
			}
			if attempts != [3]int{0, 1, 2} {
				t.Fatalf("execution attempts = %v, want block 1 once and block 2 speculative plus serial retry", attempts)
			}
			if !bc.speculativeSafetyDisabled.Load() {
				t.Fatal("speculative safety circuit did not remain open")
			}
			if bc.CurrentBlock().Hash() != block2.Hash() {
				t.Fatalf("current block = %x, want %x", bc.CurrentBlock().Hash(), block2.Hash())
			}
			bc.WaitForCommitSettled()
			if exists, err := diskdb.Has(failedAttemptRawKey); err != nil || exists {
				t.Fatalf("failed-attempt raw write reached durable DB: exists=%t err=%v", exists, err)
			}
			incident, ok, err := rawdb.ReadExecutionSafetyIncident(diskdb)
			if err != nil || !ok || incident.Kind != rawdb.ExecutionSafetyIncidentSpeculativePublication ||
				incident.BlockNum != block2.Number() || incident.BlockHash != block2.Hash() {
				t.Fatalf("persisted safety incident = %+v,%t,%v", incident, ok, err)
			}
			reopened, err := NewBlockChain(diskdb, state.NewDatabase(diskdb), params.MainnetChainConfig)
			if err != nil {
				t.Fatalf("reopen marked datadir: %v", err)
			}
			t.Cleanup(func() { _ = reopened.Close() })
			if !reopened.speculativeSafetyDisabled.Load() {
				t.Fatal("persisted safety incident did not restore circuit after restart")
			}
		})
	}
}

type executionSafetyMarkerFailStore struct {
	ethdb.Database
	putErr  error
	syncErr error
	syncs   int
}

func (s *executionSafetyMarkerFailStore) Put(key, value []byte) error {
	if string(key) == "execution-safety-incident-v1" && s.putErr != nil {
		return s.putErr
	}
	return s.Database.Put(key, value)
}

func (s *executionSafetyMarkerFailStore) SyncKeyValue() error {
	s.syncs++
	return s.syncErr
}

func TestBlockChainSpeculativeSafetyCircuitRequiresDurableMarker(t *testing.T) {
	for _, tc := range []struct {
		name             string
		putErr           error
		syncErr          error
		markerMayBeRead  bool
		wantDurableSyncs int
	}{
		{name: "put-failure", putErr: errors.New("marker write boom")},
		{name: "sync-failure", syncErr: errors.New("marker sync boom"), markerMayBeRead: true, wantDurableSyncs: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := ethrawdb.NewMemoryDatabase()
			diskdb := &executionSafetyMarkerFailStore{Database: base}
			wantErr := tc.putErr
			if wantErr == nil {
				wantErr = tc.syncErr
			}
			genesis := &params.Genesis{
				Config:    params.MainnetChainConfig,
				Timestamp: 0,
				Accounts:  []params.GenesisAccount{{Address: testCoreAddr(1), Balance: 1_000_000}},
			}
			_, genesisHash, err := SetupGenesisBlock(diskdb, genesis)
			if err != nil {
				t.Fatal(err)
			}
			bc, err := NewBlockChain(diskdb, state.NewDatabase(diskdb), params.MainnetChainConfig)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = bc.Close() })
			// Qualification is persisted during clean early-height startup. Arm the
			// injected failure only for the incident write this test exercises.
			diskdb.putErr = tc.putErr
			diskdb.syncErr = tc.syncErr
			diskdb.syncs = 0
			bc.SetParallelTransferExecution(true)
			persistErrorsBefore := parallelExecutionSafetyPersistErrorsCounter.Snapshot().Count()
			attempts := 0
			bc.speculativeSafetyTestHook = func(*types.Block, processBlockOptions) error {
				attempts++
				return fmt.Errorf("%w: injected", errSpeculativePublicationAudit)
			}
			block := types.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
				Number: 1, Timestamp: 3_000, ParentHash: genesisHash.Bytes(),
			}}})
			err = bc.InsertBlock(block)
			if !errors.Is(err, wantErr) {
				t.Fatalf("insert marker failure = %v, want %v", err, wantErr)
			}
			if !bc.speculativeSafetyDisabled.Load() {
				t.Fatal("marker persistence failure did not disable speculative execution in memory")
			}
			latchedErr := bc.speculativeSafetyPersistenceErr.Load()
			if latchedErr == nil || !errors.Is(*latchedErr, wantErr) {
				t.Fatalf("persistence failure latch = %v, want %v", latchedErr, wantErr)
			}
			if bc.CurrentBlock().Number() != 0 {
				t.Fatalf("head advanced to %d after marker failure", bc.CurrentBlock().Number())
			}
			incident, ok, readErr := rawdb.ReadExecutionSafetyIncident(base)
			if readErr != nil || ok != tc.markerMayBeRead {
				t.Fatalf("marker after failed persistence: ok=%t err=%v, want ok=%t", ok, readErr, tc.markerMayBeRead)
			}
			if ok && (incident.Kind != rawdb.ExecutionSafetyIncidentSpeculativePublication || incident.BlockHash != block.Hash()) {
				t.Fatalf("partially durable marker = %+v, want block %s", incident, block.Hash())
			}
			if diskdb.syncs != tc.wantDurableSyncs {
				t.Fatalf("durability syncs = %d, want %d", diskdb.syncs, tc.wantDurableSyncs)
			}
			if attempts != 1 {
				t.Fatalf("first insertion attempts = %d, want 1", attempts)
			}
			err = bc.InsertBlock(block)
			if !errors.Is(err, wantErr) {
				t.Fatalf("second insert after marker failure = %v, want %v", err, wantErr)
			}
			if attempts != 1 {
				t.Fatalf("second insertion re-entered execution: attempts=%d, want 1", attempts)
			}
			if got := parallelExecutionSafetyPersistErrorsCounter.Snapshot().Count() - persistErrorsBefore; got != 1 {
				t.Fatalf("safety persistence error counter delta = %d, want 1", got)
			}
		})
	}
}

func TestBlockChainSpeculativeSafetyIncidentPersistsBeforeAbortFailure(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts:  []params.GenesisAccount{{Address: testCoreAddr(1), Balance: 1_000_000}},
	}
	_, genesisHash, err := SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatal(err)
	}
	bc, err := NewBlockChain(diskdb, state.NewDatabase(diskdb), params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bc.Close() })
	bc.SetParallelTransferExecution(true)
	bc.speculativeSafetyTestHook = func(*types.Block, processBlockOptions) error {
		return fmt.Errorf("%w: injected", errSpeculativePublicationAudit)
	}
	wantAbortErr := errors.New("abort cleanup boom")
	bc.rangeExecutorAbortTestHook = func() error { return wantAbortErr }
	block := types.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
		Number: 1, Timestamp: 3_000, ParentHash: genesisHash.Bytes(),
	}}})
	err = bc.InsertBlocks([]*types.Block{block})
	if !errors.Is(err, wantAbortErr) || !errors.Is(err, errSpeculativePublicationAudit) {
		t.Fatalf("abort failure = %v, want abort and speculative errors", err)
	}
	if !bc.speculativeSafetyDisabled.Load() {
		t.Fatal("abort failure lost in-memory safety circuit")
	}
	incident, ok, readErr := rawdb.ReadExecutionSafetyIncident(diskdb)
	if readErr != nil || !ok || incident.Kind != rawdb.ExecutionSafetyIncidentSpeculativePublication || incident.BlockHash != block.Hash() {
		t.Fatalf("abort-failure safety incident = %+v,%t,%v", incident, ok, readErr)
	}
	if bc.CurrentBlock().Number() != 0 {
		t.Fatalf("head advanced to %d after abort failure", bc.CurrentBlock().Number())
	}
}

func TestNewBlockChainRejectsMalformedExecutionSafetyMarker(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	genesis := &params.Genesis{Config: params.MainnetChainConfig}
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatal(err)
	}
	if err := diskdb.Put([]byte("execution-safety-incident-v1"), []byte{1}); err != nil {
		t.Fatal(err)
	}
	if _, err := NewBlockChain(diskdb, state.NewDatabase(diskdb), params.MainnetChainConfig); err == nil ||
		!strings.Contains(err.Error(), "execution safety incident") {
		t.Fatalf("malformed marker startup error = %v", err)
	}
}

func TestNewBlockChainRejectsMalformedExecutionSafetyQualification(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	if _, _, err := SetupGenesisBlock(diskdb, &params.Genesis{Config: params.MainnetChainConfig}); err != nil {
		t.Fatal(err)
	}
	if err := diskdb.Put([]byte("execution-safety-qualified-v1"), []byte{2}); err != nil {
		t.Fatal(err)
	}
	if _, err := NewBlockChain(diskdb, state.NewDatabase(diskdb), params.MainnetChainConfig); err == nil ||
		!strings.Contains(err.Error(), "execution safety qualification") {
		t.Fatalf("malformed qualification startup error = %v", err)
	}
}

func TestBlockChainHistoricalDatadirRequiresExecutionSafetyQualification(t *testing.T) {
	makeHistoricalDB := func(t *testing.T, qualified bool) ethdb.Database {
		t.Helper()
		diskdb := ethrawdb.NewMemoryDatabase()
		if _, _, err := SetupGenesisBlock(diskdb, &params.Genesis{Config: params.MainnetChainConfig}); err != nil {
			t.Fatal(err)
		}
		head := blockchainStartupBlock(mainnetCreateTransferFailureRepairBlock)
		if err := rawdb.WriteBlock(diskdb, head); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteBlockStateRoot(diskdb, head.Hash(), rawdb.ReadGenesisStateRoot(diskdb)); err != nil {
			t.Fatal(err)
		}
		rawdb.WriteHeadBlockHash(diskdb, head.Hash())
		if qualified {
			if err := rawdb.WriteExecutionSafetyQualification(diskdb); err != nil {
				t.Fatal(err)
			}
		}
		return diskdb
	}

	t.Run("missing qualification refuses and persists", func(t *testing.T) {
		diskdb := makeHistoricalDB(t, false)
		bc, err := NewBlockChain(diskdb, state.NewDatabase(diskdb), params.MainnetChainConfig)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = bc.Close() })
		bc.SetParallelTransferExecution(true)
		bc.SetParallelVMExecution(true)
		if bc.parallelTransfers || bc.parallelVM || !bc.speculativeSafetyDisabled.Load() {
			t.Fatalf("historical datadir rollout state = transfer:%t VM:%t disabled:%t, want false,false,true",
				bc.parallelTransfers, bc.parallelVM, bc.speculativeSafetyDisabled.Load())
		}
		incident, ok, err := rawdb.ReadExecutionSafetyIncident(diskdb)
		if err != nil || !ok || incident.Kind != rawdb.ExecutionSafetyIncidentHistoricalRepairUnknown ||
			incident.BlockNum != mainnetCreateTransferFailureRepairBlock || incident.BlockHash != bc.CurrentBlock().Hash() {
			t.Fatalf("historical qualification incident = %+v,%t,%v", incident, ok, err)
		}
	})

	t.Run("qualification admits", func(t *testing.T) {
		diskdb := makeHistoricalDB(t, true)
		bc, err := NewBlockChain(diskdb, state.NewDatabase(diskdb), params.MainnetChainConfig)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = bc.Close() })
		bc.SetParallelTransferExecution(true)
		bc.SetParallelVMExecution(true)
		if !bc.parallelTransfers || !bc.parallelVM || bc.speculativeSafetyDisabled.Load() {
			t.Fatalf("qualified datadir rollout state = transfer:%t VM:%t disabled:%t, want true,true,false",
				bc.parallelTransfers, bc.parallelVM, bc.speculativeSafetyDisabled.Load())
		}
	})
}

func TestNewBlockChainPersistsExecutionSafetyQualificationBeforeRepairRange(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	if _, _, err := SetupGenesisBlock(diskdb, &params.Genesis{Config: params.MainnetChainConfig}); err != nil {
		t.Fatal(err)
	}
	bc, err := NewBlockChain(diskdb, state.NewDatabase(diskdb), params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bc.Close() })
	qualified, err := rawdb.ReadExecutionSafetyQualification(diskdb)
	if err != nil || !qualified || !bc.speculativeSafetyQualified {
		t.Fatalf("fresh datadir qualification = persisted:%t in-memory:%t err:%v, want true,true,nil",
			qualified, bc.speculativeSafetyQualified, err)
	}
}

func TestBlockChainSpeculativeSafetyCircuitRetriesForkReplaySerially(t *testing.T) {
	for _, asyncCommit := range []bool{false, true} {
		asyncCommit := asyncCommit
		name := "sync-commit"
		if asyncCommit {
			name = "async-commit"
		}
		t.Run(name, func(t *testing.T) {
			diskdb := ethrawdb.NewMemoryDatabase()
			genesis := &params.Genesis{
				Config:    params.MainnetChainConfig,
				Timestamp: 0,
				Accounts: []params.GenesisAccount{{
					Address: testCoreAddr(1), Balance: 1_000_000,
				}},
			}
			_, _, err := SetupGenesisBlock(diskdb, genesis)
			if err != nil {
				t.Fatal(err)
			}
			bc, err := NewBlockChain(diskdb, state.NewDatabase(diskdb), params.MainnetChainConfig)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = bc.Close() })
			bc.SetAsyncCommit(asyncCommit)

			makeBlock := func(number int64, timestamp int64, parent *types.Block) *types.Block {
				return types.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
					Number: number, Timestamp: timestamp, ParentHash: parent.Hash().Bytes(),
				}}})
			}
			genesisBlock := bc.CurrentBlock()
			a1 := makeBlock(1, 3_000, genesisBlock)
			a2 := makeBlock(2, 6_000, a1)
			for _, block := range []*types.Block{a1, a2} {
				if err := bc.InsertBlock(block); err != nil {
					t.Fatalf("insert canonical A%d: %v", block.Number(), err)
				}
			}
			b1 := makeBlock(1, 3_001, genesisBlock)
			b2 := makeBlock(2, 6_001, b1)
			b3 := makeBlock(3, 9_001, b2)
			for _, block := range []*types.Block{b1, b2} {
				if err := bc.InsertBlock(block); err != nil {
					t.Fatalf("insert competing B%d: %v", block.Number(), err)
				}
			}

			bc.SetParallelTransferExecution(true)
			bc.SetParallelVMExecution(true)
			attempts := make(map[tcommon.Hash][]processBlockOptions)
			bc.speculativeSafetyTestHook = func(block *types.Block, options processBlockOptions) error {
				attempts[block.Hash()] = append(attempts[block.Hash()], options)
				if block.Hash() == b2.Hash() && len(attempts[block.Hash()]) == 1 {
					if !options.parallelTransfers || !options.parallelVM {
						t.Fatalf("first fork replay options = %+v, want both speculative publishers", options)
					}
					return fmt.Errorf("%w: injected fork replay", errSpeculativePublicationAudit)
				}
				if block.Hash() == b2.Hash() && (options.parallelTransfers || options.parallelVM) {
					t.Fatalf("fork retry options = %+v, want serial execution", options)
				}
				return nil
			}
			if err := bc.InsertBlock(b3); err != nil {
				t.Fatalf("switch fork with serial safety retry: %v", err)
			}
			if got := len(attempts[b2.Hash()]); got != 2 {
				t.Fatalf("fork block B2 attempts = %d, want speculative plus serial", got)
			}
			if got := attempts[b3.Hash()]; len(got) != 1 || got[0].parallelTransfers || got[0].parallelVM {
				t.Fatalf("fork suffix B3 options = %+v, want one serial attempt", got)
			}
			if !bc.speculativeSafetyDisabled.Load() {
				t.Fatal("fork replay publication failure did not open the sticky circuit")
			}
			if bc.CurrentBlock().Hash() != b3.Hash() {
				t.Fatalf("current block = %x, want fork tip %x", bc.CurrentBlock().Hash(), b3.Hash())
			}
		})
	}
}

func TestBlockChainSpeculativeSafetyCircuitWaitsForInflightAsyncPrefix(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 1_000_000},
		},
	}
	_, genesisHash, err := SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatal(err)
	}
	bc, err := NewBlockChain(diskdb, state.NewDatabase(diskdb), params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}
	bc.SetAsyncCommit(true)
	bc.SetParallelTransferExecution(true)
	bc.SetParallelVMExecution(true)

	foldStarted := make(chan struct{})
	releaseFold := make(chan struct{})
	SetCommitFoldHookForTest(func(blockNum uint64) error {
		if blockNum == 1 {
			close(foldStarted)
			<-releaseFold
		}
		return nil
	})
	t.Cleanup(func() {
		SetCommitFoldHookForTest(nil)
		_ = bc.Close()
	})

	var attempts [3]int
	injected := make(chan struct{})
	bc.speculativeSafetyTestHook = func(block *types.Block, options processBlockOptions) error {
		attempts[block.Number()]++
		if block.Number() == 2 && attempts[2] == 1 {
			select {
			case <-foldStarted:
			case <-time.After(5 * time.Second):
				return errors.New("block 1 async fold did not become in-flight")
			}
			close(injected)
			return fmt.Errorf("%w: injected with prefix in flight", errSpeculativePublicationAudit)
		}
		if block.Number() == 2 && (options.parallelTransfers || options.parallelVM) {
			return fmt.Errorf("block 2 retry options = %+v, want serial execution", options)
		}
		return nil
	}
	block1 := types.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
		Number: 1, Timestamp: 3_000, ParentHash: genesisHash.Bytes(),
	}}})
	block2 := types.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
		Number: 2, Timestamp: 6_000, ParentHash: block1.Hash().Bytes(),
	}}})
	done := make(chan error, 1)
	go func() { done <- bc.InsertBlocks([]*types.Block{block1, block2}) }()

	select {
	case <-injected:
	case <-time.After(5 * time.Second):
		t.Fatal("block 2 speculative failure was not injected")
	}
	close(releaseFold)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("insert range with in-flight prefix: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serial safety retry deadlocked behind in-flight async prefix")
	}
	if attempts != [3]int{0, 1, 2} {
		t.Fatalf("execution attempts = %v, want block 1 once and block 2 twice", attempts)
	}
	if bc.CurrentBlock().Hash() != block2.Hash() {
		t.Fatalf("current block = %x, want %x", bc.CurrentBlock().Hash(), block2.Hash())
	}
}

func TestBlockChainPostApplyMismatchRollsBackAndRetriesRealTransferPath(t *testing.T) {
	for _, asyncCommit := range []bool{false, true} {
		asyncCommit := asyncCommit
		name := "sync-commit"
		if asyncCommit {
			name = "async-commit"
		}
		t.Run(name, func(t *testing.T) {
			diskdb := ethrawdb.NewMemoryDatabase()
			genesis := &params.Genesis{
				Config:    params.MainnetChainConfig,
				Timestamp: 0,
				Accounts: []params.GenesisAccount{
					{Address: testCoreAddr(1), Balance: 1_000_000},
					{Address: testCoreAddr(2), Balance: 10},
				},
			}
			_, genesisHash, err := SetupGenesisBlock(diskdb, genesis)
			if err != nil {
				t.Fatal(err)
			}
			bc, err := NewBlockChain(diskdb, state.NewDatabase(diskdb), params.MainnetChainConfig)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = bc.Close() })
			bc.SetAsyncCommit(asyncCommit)
			bc.SetParallelTransferExecution(true)

			var corruptions int
			bc.speculativePostApplyTestHook = func(family string, txIndex int, writes state.TransactionWriteSet) {
				if family != "Transfer" || txIndex != 0 {
					return
				}
				corruptions++
				delete(writes, state.TransactionAccessKey{
					Kind:         state.TransactionAccessAccountField,
					Address:      testCoreAddr(2),
					AccountField: state.TransactionAccountFieldBalance,
				})
			}
			tx := makeTestTransferTx(1, 2, 100)
			block := types.NewBlockFromPB(&corepb.Block{
				BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
					Number: 1, Timestamp: 3_000, ParentHash: genesisHash.Bytes(),
				}},
				Transactions: []*corepb.Transaction{tx.Proto()},
			})
			if err := bc.InsertBlocks([]*types.Block{block}); err != nil {
				t.Fatalf("insert after real post-apply mismatch: %v", err)
			}
			if corruptions != 1 {
				t.Fatalf("post-apply corruptions = %d, want one speculative attempt", corruptions)
			}
			if !bc.speculativeSafetyDisabled.Load() {
				t.Fatal("real post-apply mismatch did not open the safety circuit")
			}
			statedb, err := bc.openCurrentState()
			if err != nil {
				t.Fatal(err)
			}
			if got := statedb.GetBalance(testCoreAddr(2)); got != 110 {
				t.Fatalf("recipient balance after rollback+serial retry = %d, want 110", got)
			}
		})
	}
}

func TestBlockChainPreOracleMismatchPersistsCircuitAndRetriesSerially(t *testing.T) {
	for _, asyncCommit := range []bool{false, true} {
		asyncCommit := asyncCommit
		name := "sync-commit"
		if asyncCommit {
			name = "async-commit"
		}
		t.Run(name, func(t *testing.T) {
			diskdb := ethrawdb.NewMemoryDatabase()
			genesis := &params.Genesis{
				Config:    params.MainnetChainConfig,
				Timestamp: 0,
				Accounts: []params.GenesisAccount{
					{Address: testCoreAddr(1), Balance: 1_000_000},
					{Address: testCoreAddr(2), Balance: 10},
				},
			}
			_, genesisHash, err := SetupGenesisBlock(diskdb, genesis)
			if err != nil {
				t.Fatal(err)
			}
			bc, err := NewBlockChain(diskdb, state.NewDatabase(diskdb), params.MainnetChainConfig)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = bc.Close() })
			bc.SetAsyncCommit(asyncCommit)
			bc.SetParallelTransferExecution(true)

			var corruptions int
			bc.speculativePreOracleTestHook = func(family string, txIndex int, result *discardShadowTaskResult) {
				if family != "Transfer" || txIndex != 0 {
					return
				}
				corruptions++
				key := state.TransactionAccessKey{
					Kind:         state.TransactionAccessAccountField,
					Address:      testCoreAddr(2),
					AccountField: state.TransactionAccountFieldBalance,
				}
				value := result.writes[key]
				value.Value = binary.BigEndian.AppendUint64(nil, binary.BigEndian.Uint64(value.Value)+1)
				result.writes[key] = value
			}
			tx := makeTestTransferTx(1, 2, 100)
			block := types.NewBlockFromPB(&corepb.Block{
				BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
					Number: 1, Timestamp: 3_000, ParentHash: genesisHash.Bytes(),
				}},
				Transactions: []*corepb.Transaction{tx.Proto()},
			})
			if err := bc.InsertBlocks([]*types.Block{block}); err != nil {
				t.Fatalf("insert after pre-oracle mismatch: %v", err)
			}
			if corruptions != 1 {
				t.Fatalf("pre-oracle corruptions = %d, want one speculative attempt", corruptions)
			}
			if !bc.speculativeSafetyDisabled.Load() {
				t.Fatal("pre-oracle mismatch did not open safety circuit")
			}
			incident, ok, err := rawdb.ReadExecutionSafetyIncident(diskdb)
			if err != nil || !ok || incident.Kind != rawdb.ExecutionSafetyIncidentSpeculativePublication || incident.BlockHash != block.Hash() {
				t.Fatalf("pre-oracle safety incident = %+v,%t,%v", incident, ok, err)
			}
			statedb, err := bc.openCurrentState()
			if err != nil {
				t.Fatal(err)
			}
			if got := statedb.GetBalance(testCoreAddr(2)); got != 110 {
				t.Fatalf("recipient balance after pre-oracle rollback+retry = %d, want 110", got)
			}
		})
	}
}

func TestBlockChainPreApplyMutationRollsBackAndRetriesRealTransferPath(t *testing.T) {
	testBlockChainPreApplyMutationRollsBackAndRetriesRealTransferPath(t, func(writes state.TransactionWriteSet) {
		key := state.TransactionAccessKey{
			Kind:         state.TransactionAccessAccountField,
			Address:      testCoreAddr(2),
			AccountField: state.TransactionAccountFieldBalance,
		}
		value := writes[key]
		value.Value = []byte{0xff}
		writes[key] = value
	})
}

func TestBlockChainValidPreApplyMutationIsRejectedByWriteSeal(t *testing.T) {
	testBlockChainPreApplyMutationRollsBackAndRetriesRealTransferPath(t, func(writes state.TransactionWriteSet) {
		key := state.TransactionAccessKey{
			Kind:         state.TransactionAccessAccountField,
			Address:      testCoreAddr(2),
			AccountField: state.TransactionAccountFieldBalance,
		}
		value := writes[key]
		mutated := binary.BigEndian.Uint64(value.Value) + 1
		value.Value = binary.BigEndian.AppendUint64(nil, mutated)
		writes[key] = value
	})
}

func testBlockChainPreApplyMutationRollsBackAndRetriesRealTransferPath(t *testing.T, mutate func(state.TransactionWriteSet)) {
	t.Helper()
	for _, asyncCommit := range []bool{false, true} {
		asyncCommit := asyncCommit
		name := "sync-commit"
		if asyncCommit {
			name = "async-commit"
		}
		t.Run(name, func(t *testing.T) {
			diskdb := ethrawdb.NewMemoryDatabase()
			genesis := &params.Genesis{
				Config: params.MainnetChainConfig,
				Accounts: []params.GenesisAccount{
					{Address: testCoreAddr(1), Balance: 1_000_000},
					{Address: testCoreAddr(2), Balance: 10},
				},
			}
			_, genesisHash, err := SetupGenesisBlock(diskdb, genesis)
			if err != nil {
				t.Fatal(err)
			}
			bc, err := NewBlockChain(diskdb, state.NewDatabase(diskdb), params.MainnetChainConfig)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = bc.Close() })
			bc.SetAsyncCommit(asyncCommit)
			bc.SetParallelTransferExecution(true)

			var corruptions int
			bc.speculativePreApplyTestHook = func(family string, txIndex int, writes state.TransactionWriteSet) {
				if family != "Transfer" || txIndex != 0 {
					return
				}
				corruptions++
				mutate(writes)
			}
			tx := makeTestTransferTx(1, 2, 100)
			block := types.NewBlockFromPB(&corepb.Block{
				BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
					Number: 1, Timestamp: 3_000, ParentHash: genesisHash.Bytes(),
				}},
				Transactions: []*corepb.Transaction{tx.Proto()},
			})
			if err := bc.InsertBlocks([]*types.Block{block}); err != nil {
				t.Fatalf("insert after pre-apply mutation: %v", err)
			}
			if corruptions != 1 {
				t.Fatalf("pre-apply corruptions = %d, want one speculative attempt", corruptions)
			}
			if !bc.speculativeSafetyDisabled.Load() {
				t.Fatal("pre-apply publication failure did not open the safety circuit")
			}
			statedb, err := bc.openCurrentState()
			if err != nil {
				t.Fatal(err)
			}
			if got := statedb.GetBalance(testCoreAddr(2)); got != 110 {
				t.Fatalf("recipient balance after pre-apply rollback+serial retry = %d, want 110", got)
			}
		})
	}
}

func TestBlockChainGetBlockByNumber(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	genesis := &params.Genesis{
		Config: params.MainnetChainConfig,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 1000},
		},
	}

	SetupGenesisBlock(diskdb, genesis)
	bc, _ := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)

	block := bc.GetBlockByNumber(0)
	if block == nil {
		t.Fatal("genesis block not found")
	}
}

func TestBlockChainBlockIDByNumber(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)
	genesis := &params.Genesis{
		Config: params.MainnetChainConfig,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 1000},
		},
	}
	SetupGenesisBlock(diskdb, genesis)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer bc.Close()
	want := bc.CurrentBlock().ID()
	got, ok := bc.BlockIDByNumber(0)
	if !ok || got != want {
		t.Fatalf("BlockIDByNumber(0) = %+v,%v want %+v,true", got, ok, want)
	}
	if got, ok := bc.BlockIDByNumber(1); ok || got != (types.BlockID{}) {
		t.Fatalf("future BlockIDByNumber = %+v,%v want zero,false", got, ok)
	}
}

func TestBlockChainGetBlockByHash(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	genesis := &params.Genesis{
		Config: params.MainnetChainConfig,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 1000},
		},
	}

	_, genesisHash, err := SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatal(err)
	}

	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	block := bc.GetBlockByHash(genesisHash)
	if block == nil {
		t.Fatal("genesis block not found by hash")
	}
	if block.Number() != 0 {
		t.Fatalf("expected block number 0, got %d", block.Number())
	}
}

func TestBlockChainInsertInvalidNumber(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	genesis := &params.Genesis{
		Config: params.MainnetChainConfig,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 1000},
		},
	}

	SetupGenesisBlock(diskdb, genesis)
	bc, _ := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)

	// Try to insert block with wrong number (2 instead of 1)
	badBlock := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number: 2,
			},
		},
	})

	err := bc.InsertBlockWithoutVerify(badBlock)
	if err != ErrInvalidNumber {
		t.Fatalf("expected ErrInvalidNumber, got %v", err)
	}
}

func TestBlockChainInsertInvalidParent(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	genesis := &params.Genesis{
		Config: params.MainnetChainConfig,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 1000},
		},
	}

	SetupGenesisBlock(diskdb, genesis)
	bc, _ := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)

	// Insert block 1 with wrong parent hash
	wrongParent := tcommon.Hash{0xde, 0xad}
	badBlock := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:     1,
				ParentHash: wrongParent[:],
			},
		},
	})

	err := bc.InsertBlockWithoutVerify(badBlock)
	if err != ErrInvalidParent {
		t.Fatalf("expected ErrInvalidParent, got %v", err)
	}
}

func TestBlockChainActiveWitnesses(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	genesis := &params.Genesis{
		Config: params.MainnetChainConfig,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 1000},
		},
		Witnesses: []params.GenesisWitness{
			{Address: testCoreAddr(10), VoteCount: 100, URL: "http://w1"},
			{Address: testCoreAddr(11), VoteCount: 200, URL: "http://w2"},
		},
	}

	SetupGenesisBlock(diskdb, genesis)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	// Genesis seeds the active set into the state root; startup loaded it into
	// the atomic from the system-KV at the head root.
	witnesses := bc.ActiveWitnesses()
	if len(witnesses) != 2 {
		t.Fatalf("expected 2 genesis-seeded active witnesses, got %d", len(witnesses))
	}

	// SetActiveWitnesses stages the new list into the rooted system-KV on a
	// statedb opened at the head root; the in-memory atomic updates immediately.
	newList := []tcommon.Address{testCoreAddr(20), testCoreAddr(21)}
	genesisRoot := bc.HeadStateRoot()
	statedb, err := state.New(genesisRoot, sdb)
	if err != nil {
		t.Fatal(err)
	}
	if err := bc.SetActiveWitnesses(statedb, newList); err != nil {
		t.Fatal(err)
	}
	got := bc.ActiveWitnesses()
	if len(got) != 2 || got[0] != testCoreAddr(20) || got[1] != testCoreAddr(21) {
		t.Fatalf("unexpected witnesses after set: %v", got)
	}

	// Committing roots the new list. Reloading from the new root keeps it;
	// reloading from the genesis root rewinds the atomic to the seeded set.
	newRoot, err := statedb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	bc.reloadActiveWitnesses(newRoot)
	if g := bc.ActiveWitnesses(); len(g) != 2 || g[0] != testCoreAddr(20) {
		t.Fatalf("reload from new root lost the set: %v", g)
	}
	bc.reloadActiveWitnesses(genesisRoot)
	if g := bc.ActiveWitnesses(); len(g) != 2 {
		t.Fatalf("reload from genesis root should restore the seeded set, got %v", g)
	}
}

func TestBlockChainNextMaintenanceTime(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 1000,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 1000},
		},
		DynamicProperties: map[string]int64{
			"next_maintenance_time": 100000,
		},
	}

	SetupGenesisBlock(diskdb, genesis)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	if bc.NextMaintenanceTime() != 100000 {
		t.Fatalf("expected 100000, got %d", bc.NextMaintenanceTime())
	}
}

func TestBlockChainInsertBlock_Maintenance(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	witnessAddr := testCoreAddr(10)
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
		DynamicProperties: map[string]int64{
			"next_maintenance_time": 6000,
		},
	}

	SetupGenesisBlock(diskdb, genesis)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	// Build block 1 at timestamp 3000 (before maintenance)
	block1 := buildTestBlock(bc, witnessAddr, 3000)
	if err := bc.InsertBlock(block1); err != nil {
		t.Fatal(err)
	}
	bc.WaitForFlushSettled()

	dynProps := loadDPAtRoot(t, diskdb, bc.StateDB(), bc.HeadStateRoot())
	if dynProps.NextMaintenanceTime() != 6000 {
		t.Fatalf("maintenance should not have run yet, got %d", dynProps.NextMaintenanceTime())
	}

	// Build block 2 at timestamp 6000 (at maintenance boundary)
	block2 := buildTestBlock(bc, witnessAddr, 6000)
	if err := bc.InsertBlock(block2); err != nil {
		t.Fatal(err)
	}
	bc.WaitForFlushSettled()

	dynProps = loadDPAtRoot(t, diskdb, bc.StateDB(), bc.HeadStateRoot())
	if dynProps.NextMaintenanceTime() <= 6000 {
		t.Fatalf("next_maintenance_time should have advanced past 6000, got %d", dynProps.NextMaintenanceTime())
	}
}

// TestBlockChainInsertBlock_ProcessProposalsAtMaintenance locks the wiring
// fix that hooks core.ProcessProposals into the per-block maintenance
// boundary in applyBlock. Before this fix the function was defined but
// never called: a Nile soak at h=860k had 4 proposals with 27 SR approvals
// each stuck at `state=PENDING` and `allow_creation_of_contracts=0`,
// keeping every TVM/actuator fork gate disabled forever (2026-05-09).
//
// Setup pre-populates the proposal store with a PENDING proposal that
// would set DP key 9 (allow_creation_of_contracts) to 1, with the sole
// active witness recorded as approver. Crossing the maintenance boundary
// must flip the proposal to APPROVED and apply the DP change.
func TestBlockChainInsertBlock_ProcessProposalsAtMaintenance(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	witnessAddr := testCoreAddr(10)
	const interval = int64(21_600_000)
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
		DynamicProperties: map[string]int64{
			"maintenance_time_interval": interval,
			"next_maintenance_time":     interval,
		},
	}
	_, genesisHash, err := SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatal(err)
	}

	// Seed a PENDING proposal expiring before the maintenance boundary,
	// approved by the sole active witness (= 100% > 70% threshold). Proposals
	// are rooted (Phase 3d), so seed it into the genesis state root and re-point
	// the genesis/head root pointers — mirrors block_builder_test's rooted-seed
	// pattern — so the chain carries it forward to the maintenance block.
	pendingProposal := &rawdb.Proposal{
		ID:             1,
		Proposer:       witnessAddr,
		Parameters:     map[int64]int64{9: 1}, // allow_creation_of_contracts
		CreateTime:     0,
		ExpirationTime: interval - 1,
		Approvals:      []tcommon.Address{witnessAddr},
		State:          rawdb.ProposalStatePending,
	}
	seedRootedProposal(t, diskdb, sdb, genesisHash, []*rawdb.Proposal{pendingProposal})

	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	// Block #1 hits the genesis boundary but java-tron skips doMaintenance
	// on block #1 (MaintenanceManager.applyBlock line 63 `if blockNum != 1`).
	// Push one pre-boundary block first so the boundary crossing happens on
	// block #2 where ProcessProposals actually fires.
	preBoundary := buildTestBlock(bc, witnessAddr, 1)
	if err := bc.InsertBlock(preBoundary); err != nil {
		t.Fatal(err)
	}
	block := buildTestBlock(bc, witnessAddr, interval)
	if err := bc.InsertBlock(block); err != nil {
		t.Fatal(err)
	}
	bc.WaitForFlushSettled()

	got := readRootedProposal(t, sdb, bc.HeadStateRoot(), 1)
	if got == nil {
		t.Fatal("proposal #1 missing after maintenance")
	}
	if got.State != rawdb.ProposalStateApproved {
		t.Fatalf("proposal #1 state: got %d, want APPROVED (%d)", got.State, rawdb.ProposalStateApproved)
	}
	dp := loadDPAtRoot(t, diskdb, bc.StateDB(), bc.HeadStateRoot())
	if !dp.AllowCreationOfContracts() {
		raw, _ := dp.Get("allow_creation_of_contracts")
		t.Fatalf("allow_creation_of_contracts not set after proposal #1 applied (raw value=%d)", raw)
	}
}

// TestBlockChainInsertBlock_MaintenanceFiresOncePerBoundary is the
// regression test for D-2.b — under the original cross-impl fixture
// (CD=OFF) gtron's distributeLegacyStandby fired 37 times in 11 cycles
// (~3.4× over). Even with CD=ON masking the allowance leak, the fix
// must guarantee that crossing a single maintenance boundary triggers
// DoMaintenance exactly once, regardless of how many blocks fall after
// the boundary inside the same maintenance interval.
func TestBlockChainInsertBlock_MaintenanceFiresOncePerBoundary(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	witnessAddr := testCoreAddr(10)
	const interval = int64(21_600_000) // 6h, java-tron default
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
		DynamicProperties: map[string]int64{
			"maintenance_time_interval": interval,
			"next_maintenance_time":     interval, // first boundary at t=interval
		},
	}

	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatal(err)
	}
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	var fires int
	bc.AddMaintenanceHook(func(*types.Block, []tcommon.Address) {
		fires++
	})

	// Push a pre-boundary block #1 first so the boundary crossings below
	// land on block #2+. java-tron skips doMaintenance on block #1
	// regardless of `flag`, so feeding the boundary on block #1 would
	// register zero fires and conflate two distinct behaviors.
	preBoundary := buildTestBlock(bc, witnessAddr, 1)
	if err := bc.InsertBlock(preBoundary); err != nil {
		t.Fatalf("InsertBlock(preBoundary): %v", err)
	}

	// Three blocks all *after* the first boundary but inside the same
	// interval. Only the first should trigger maintenance; the next two
	// must observe the advanced next_maintenance_time and skip.
	timestamps := []int64{interval, interval + 3000, interval + 6000}
	for _, ts := range timestamps {
		block := buildTestBlock(bc, witnessAddr, ts)
		if err := bc.InsertBlock(block); err != nil {
			t.Fatalf("InsertBlock(ts=%d): %v", ts, err)
		}
	}

	if fires != 1 {
		t.Fatalf("DoMaintenance fires across one boundary: got %d, want 1", fires)
	}

	// next_maintenance_time must advance to exactly 2*interval after one
	// fire (round=0 in CalcNextMaintenanceTime, since blockTime − currentMaint
	// < interval). It is a rooted key, so loadDPAtRoot reads it from the head
	// state root through the disk-backed StateDB — but block state reaches disk
	// via the async buffer flush. Wait for that flush to settle first, else the
	// read can observe the pre-maintenance value (= interval) the flusher hasn't
	// overwritten yet. This is intermittent under full-package / -count runs,
	// where sibling tests' CPU/GC pressure delays this chain's flush worker past
	// the read. Mirrors the guard the sibling maintenance/rewind tests already
	// use before their rooted-head reads.
	bc.WaitForFlushSettled()
	dynProps := loadDPAtRoot(t, diskdb, bc.StateDB(), bc.HeadStateRoot())
	if got, want := dynProps.NextMaintenanceTime(), 2*interval; got != want {
		t.Fatalf("next_maintenance_time after fire: got %d, want %d", got, want)
	}

	// Now feed a block that crosses the *second* boundary — exactly one
	// more fire. Confirms multi-boundary cadence.
	block := buildTestBlock(bc, witnessAddr, 2*interval+1000)
	if err := bc.InsertBlock(block); err != nil {
		t.Fatal(err)
	}
	if fires != 2 {
		t.Fatalf("DoMaintenance fires across two boundaries: got %d, want 2", fires)
	}

	// Long stress: feed blocks every 3s for several maintenance intervals.
	// Mirrors the cross-impl scenario where many blocks fall between
	// maintenance boundaries. Trigger must fire exactly once per boundary.
	startBlockNum := bc.CurrentBlock().Number()
	startTs := bc.CurrentBlock().Timestamp()
	const blockTickMs = int64(3000)
	const cycles = int64(5) // five maintenance cycles, ~36k blocks at 3s/block
	want := int(2 + cycles)
	for ts := startTs + blockTickMs; ts <= startTs+cycles*interval+blockTickMs; ts += blockTickMs {
		b := buildTestBlock(bc, witnessAddr, ts)
		if err := bc.InsertBlock(b); err != nil {
			t.Fatalf("InsertBlock at ts=%d: %v", ts, err)
		}
	}
	if fires != want {
		t.Fatalf("DoMaintenance fires across stress run: got %d, want %d (blocks=%d→%d)",
			fires, want, startBlockNum+1, bc.CurrentBlock().Number())
	}
}

// TestBlockChainInsertBlock_RootedDynPropsRewind is the Phase 3b pipeline
// acceptance gate: a maintenance block changes a rooted dynprop
// (next_maintenance_time), which must (a) move the internal full-state root
// (anchor) and (b) remain recoverable by reopening the OLD root after the chain
// advances (rewind). This is the whole point of rooting dynamic properties.
func TestBlockChainInsertBlock_RootedDynPropsRewind(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	witnessAddr := testCoreAddr(10)
	const interval = int64(21_600_000)
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: witnessAddr, Balance: 1_000_000},
		},
		Witnesses: []params.GenesisWitness{
			{Address: witnessAddr, VoteCount: 1000, URL: "http://w1"},
		},
		DynamicProperties: map[string]int64{
			"maintenance_time_interval": interval,
			"next_maintenance_time":     interval,
		},
	}
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatal(err)
	}
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	// Block #1 (ts=1): pre-boundary, next_maintenance_time stays = interval.
	if err := bc.InsertBlock(buildTestBlock(bc, witnessAddr, 1)); err != nil {
		t.Fatalf("InsertBlock(#1): %v", err)
	}
	bc.WaitForFlushSettled()
	rootBefore := bc.HeadStateRoot()
	if got := loadDPAtRoot(t, diskdb, sdb, rootBefore).NextMaintenanceTime(); got != interval {
		t.Fatalf("pre-boundary next_maintenance_time: got %d, want %d", got, interval)
	}

	// Block #2 (ts=interval): crosses the boundary; next_maintenance_time
	// advances to 2*interval and the rooted change moves the state root.
	if err := bc.InsertBlock(buildTestBlock(bc, witnessAddr, interval)); err != nil {
		t.Fatalf("InsertBlock(#2): %v", err)
	}
	bc.WaitForFlushSettled()
	rootAfter := bc.HeadStateRoot()

	if rootBefore == rootAfter {
		t.Fatal("anchor: maintenance changed next_maintenance_time but the state root did not move")
	}
	if got := loadDPAtRoot(t, diskdb, sdb, rootAfter).NextMaintenanceTime(); got != 2*interval {
		t.Fatalf("post-boundary next_maintenance_time at head root: got %d, want %d", got, 2*interval)
	}

	// Flat latest is authoritative: reopening the pre-boundary commitment root
	// reads the current latest-domain value. Historical queries must use domain
	// history rather than treating the root as an MPT snapshot.
	if got := loadDPAtRoot(t, diskdb, sdb, rootBefore).NextMaintenanceTime(); got != 2*interval {
		t.Fatalf("pre-boundary root open reads latest next_maintenance_time = %d, want %d", got, 2*interval)
	}
}

// TestBlockChainInsertBlock_Block1SkipsMaintenance locks the java-tron
// MaintenanceManager.applyBlock contract (lines 62-75): when block #1
// crosses the genesis-seeded boundary, the chain still advances
// next_maintenance_time per `updateNextMaintenanceTime(blockTime)` but
// SKIPS doMaintenance entirely — no legacy standby allowance is paid, no
// active-set rotation, no proposal processing, no cycle 0 VI
// accumulation. This is why Nile's deployed mainnet keeps the GR set
// intact on block #1 and runs its first real maintenance on block #2+.
//
// Without this skip, gtron paid `witness_standby_allowance` to GR
// witnesses on block #1 (and rotated them off the active set), creating
// state-root divergence on the very first block of any Nile bootstrap.
//
// The genesis-seeded boundary fixture uses Nile-like inputs: Timestamp=0,
// MaintenanceTimeInterval=21_600_000, NextMaintenanceTime=21_600_000.
// Block #1 lands at a real Nile-era timestamp (1572408000000 = Oct 30
// 2019 03:20 UTC). java-tron's updateNextMaintenanceTime formula yields
// 1572415200000 (Oct 30 06:00 UTC) — currentMaint + (round+1)*interval
// with round = (blockTime - currentMaint) / interval = 72795.
func TestBlockChainInsertBlock_Block1SkipsMaintenance(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	witnessAddr := testCoreAddr(10)
	const interval = int64(21_600_000)
	const block1Time = int64(1_572_408_000_000) // Oct 30 2019 03:20 UTC
	// java's updateNextMaintenanceTime: currentMaint=21_600_000,
	// blockTime=1_572_408_000_000, interval=21_600_000
	// → round = (1572408000000 - 21600000) / 21600000 = 72795
	// → next = 21600000 + 72796*21600000 = 1572415200000.
	const wantNextMaint = int64(1_572_415_200_000)

	const standbyAllowance = int64(115_200_000_000)
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
		DynamicProperties: map[string]int64{
			"maintenance_time_interval": interval,
			"next_maintenance_time":     interval,
			// CD=OFF so distributeLegacyStandby would pay allowance — if the
			// skip is missing, this witness's allowance will jump by
			// standby_allowance × (votes / total_votes) = standbyAllowance.
			"witness_standby_allowance": standbyAllowance,
			"change_delegation":         0,
		},
	}
	_, genesisHash, err := SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatal(err)
	}

	// Seed a PENDING proposal that would APPROVE at the boundary if
	// ProcessProposals ran. Skip must keep it PENDING. Rooted (Phase 3d), so
	// seed into the genesis state root.
	pendingProposal := &rawdb.Proposal{
		ID:             1,
		Proposer:       witnessAddr,
		Parameters:     map[int64]int64{9: 1}, // allow_creation_of_contracts
		CreateTime:     0,
		ExpirationTime: block1Time - 1,
		Approvals:      []tcommon.Address{witnessAddr},
		State:          rawdb.ProposalStatePending,
	}
	seedRootedProposal(t, diskdb, sdb, genesisHash, []*rawdb.Proposal{pendingProposal})

	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	var maintFires int
	bc.AddMaintenanceHook(func(*types.Block, []tcommon.Address) {
		maintFires++
	})

	block1 := buildTestBlock(bc, witnessAddr, block1Time)
	if err := bc.InsertBlock(block1); err != nil {
		t.Fatalf("InsertBlock(block#1): %v", err)
	}
	bc.WaitForFlushSettled()

	// 1. Grid still advances per java's updateNextMaintenanceTime formula.
	dp := loadDPAtRoot(t, diskdb, bc.StateDB(), bc.HeadStateRoot())
	if got := dp.NextMaintenanceTime(); got != wantNextMaint {
		t.Fatalf("next_maintenance_time after block #1: got %d, want %d (java formula output)", got, wantNextMaint)
	}

	// 2. State flag is still set (java line 76 sets it from `flag` regardless
	//    of blockNum).
	if got := dp.StateFlag(); got != 1 {
		t.Fatalf("state_flag after block #1 boundary: got %d, want 1", got)
	}

	// 3. Maintenance hook MUST NOT fire — java skips srPrePrepare for
	//    blockNum==1 (line 70 guard).
	if maintFires != 0 {
		t.Fatalf("maintenance hook fires on block #1: got %d, want 0", maintFires)
	}

	// 4. Legacy standby allowance did NOT pay out. With CD=OFF, sole-witness
	//    distribution would credit ~standbyAllowance to witnessAddr's
	//    allowance. Block reward also accrues, so the strict invariant is
	//    "allowance < standbyAllowance" (block reward is 16M sun, well under
	//    115.2G).
	stateRoot := rawdb.ReadBlockStateRoot(rawdb.NewChainDB(diskdb, rawdb.NoopAncient{}), bc.CurrentBlock().Hash())
	statedb, err := bc.openState(stateRoot)
	if err != nil {
		t.Fatalf("open post-block#1 state: %v", err)
	}
	if got := statedb.GetAllowance(witnessAddr); got >= standbyAllowance {
		t.Fatalf("witness allowance after block #1: got %d, want < %d (block reward only, no standby payout)", got, standbyAllowance)
	}

	// 5. Pending proposal stays pending (ProcessProposals skipped).
	gotProp := readRootedProposal(t, sdb, bc.HeadStateRoot(), 1)
	if gotProp == nil {
		t.Fatal("proposal #1 missing")
	}
	if gotProp.State != rawdb.ProposalStatePending {
		t.Fatalf("proposal #1 state after block #1: got %d, want PENDING (%d)", gotProp.State, rawdb.ProposalStatePending)
	}
	dpAfter := loadDPAtRoot(t, diskdb, bc.StateDB(), bc.HeadStateRoot())
	if dpAfter.AllowCreationOfContracts() {
		t.Fatal("allow_creation_of_contracts unexpectedly applied — ProcessProposals fired on block #1")
	}
}

func TestSolidifiedBlockSingleSR(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	witnessAddr := testCoreAddr(10)
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 100_000_000},
			{Address: witnessAddr, Balance: 1_000_000},
		},
		Witnesses: []params.GenesisWitness{
			{Address: witnessAddr, VoteCount: 1000, URL: "http://sr1"},
		},
		DynamicProperties: map[string]int64{
			// Push maintenance far out so it doesn't fire during the test.
			"next_maintenance_time": 9_000_000_000,
		},
	}

	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatal(err)
	}

	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	// Single SR: floor(1 * 0.3) = 0, so solidified == that SR's latest block.
	const numBlocks = 5
	for i := 1; i <= numBlocks; i++ {
		block := buildTestBlock(bc, witnessAddr, int64(i*3000))
		if err := bc.InsertBlock(block); err != nil {
			t.Fatalf("block %d: %v", i, err)
		}
	}
	bc.WaitForFlushSettled()

	want := uint64(numBlocks)
	got := uint64(state.LoadDynamicProperties(diskdb, nil).LatestSolidifiedBlockNum()) // derived-only
	if got != want {
		t.Fatalf("LatestSolidifiedBlockNum: got %d, want %d", got, want)
	}

	// Also confirm it matches the current head.
	if bc.CurrentBlock().Number() != want {
		t.Fatalf("CurrentBlock.Number: got %d, want %d", bc.CurrentBlock().Number(), want)
	}
}

// TestBlockChainInsertBlock_TryRemoveThePowerOfTheGr exercises the full path:
// crossing a maintenance boundary with REMOVE_THE_POWER_OF_THE_GR=1 strips
// the GR's initial vote and clears the flag to -1. Mirrors java-tron
// MaintenanceManager.tryRemoveThePowerOfTheGr (consensus/.../dpos/Maintenance
// Manager.java:194-204).
func TestBlockChainInsertBlock_TryRemoveThePowerOfTheGr(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	grAddr := testCoreAddr(10)
	const interval = int64(21_600_000)
	const initialGRVote = int64(100_000_000)
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 100_000_000},
			{Address: grAddr, Balance: 1_000_000},
		},
		Witnesses: []params.GenesisWitness{
			{Address: grAddr, VoteCount: initialGRVote, URL: "http://gr1"},
		},
		DynamicProperties: map[string]int64{
			"maintenance_time_interval":  interval,
			"next_maintenance_time":      interval,
			"remove_the_power_of_the_gr": 1,
		},
	}
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatal(err)
	}
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	// Block #1 pre-boundary (java-tron skips doMaintenance for block #1, so
	// the boundary block must land at block #2+ for tryRemoveThePowerOfTheGr
	// to actually fire).
	if err := bc.InsertBlock(buildTestBlock(bc, grAddr, interval/2)); err != nil {
		t.Fatal(err)
	}

	// Block #2 crosses the maintenance boundary.
	if err := bc.InsertBlock(buildTestBlock(bc, grAddr, interval)); err != nil {
		t.Fatal(err)
	}

	headState, err := bc.openState(bc.HeadStateRoot())
	if err != nil {
		t.Fatal(err)
	}
	w := headState.GetWitness(grAddr)
	if w == nil {
		t.Fatal("GR witness missing after maintenance")
	}
	if got := w.VoteCount(); got != 0 {
		t.Fatalf("GR voteCount after strip: got %d, want 0 (100M − 100M)", got)
	}

	dp := loadDPAtRoot(t, bc.BufferedDB(), bc.StateDB(), bc.HeadStateRoot())
	if got := dp.RemoveThePowerOfTheGr(); got != -1 {
		t.Fatalf("flag after strip: got %d, want -1", got)
	}

	// Second maintenance boundary: flag is -1, GR vote must stay at 0 (no
	// further strip), confirming the one-shot guard.
	if err := bc.InsertBlock(buildTestBlock(bc, grAddr, 2*interval)); err != nil {
		t.Fatal(err)
	}
	headState, err = bc.openState(bc.HeadStateRoot())
	if err != nil {
		t.Fatal(err)
	}
	w2 := headState.GetWitness(grAddr)
	if got := w2.VoteCount(); got != 0 {
		t.Fatalf("GR voteCount after second maintenance: got %d, want 0", got)
	}
	if got := loadDPAtRoot(t, bc.BufferedDB(), bc.StateDB(), bc.HeadStateRoot()).RemoveThePowerOfTheGr(); got != -1 {
		t.Fatalf("flag after second maintenance: got %d, want -1", got)
	}
}

func buildTestBlock(bc *BlockChain, witnessAddr tcommon.Address, timestamp int64) *types.Block {
	parent := bc.CurrentBlock()
	return types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:         int64(parent.Number() + 1),
				Timestamp:      timestamp,
				ParentHash:     parent.Hash().Bytes(),
				WitnessAddress: witnessAddr.Bytes(),
			},
		},
	})
}
