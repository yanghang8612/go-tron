package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

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

	stored := rawdb.ReadBlock(diskdb, 1)
	if stored == nil {
		t.Fatal("block 1 not stored")
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

	witnesses := bc.ActiveWitnesses()
	if len(witnesses) == 0 {
		t.Fatal("expected non-empty active witnesses")
	}

	newList := []tcommon.Address{testCoreAddr(20), testCoreAddr(21)}
	bc.SetActiveWitnesses(newList)

	got := bc.ActiveWitnesses()
	if len(got) != 2 || got[0] != testCoreAddr(20) || got[1] != testCoreAddr(21) {
		t.Fatalf("unexpected witnesses after set: %v", got)
	}

	persisted := rawdb.ReadActiveWitnesses(diskdb)
	if len(persisted) != 2 {
		t.Fatalf("expected 2 persisted witnesses, got %d", len(persisted))
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

	dynProps := state.LoadDynamicProperties(diskdb)
	if dynProps.NextMaintenanceTime() != 6000 {
		t.Fatalf("maintenance should not have run yet, got %d", dynProps.NextMaintenanceTime())
	}

	// Build block 2 at timestamp 6000 (at maintenance boundary)
	block2 := buildTestBlock(bc, witnessAddr, 6000)
	if err := bc.InsertBlock(block2); err != nil {
		t.Fatal(err)
	}

	dynProps = state.LoadDynamicProperties(diskdb)
	if dynProps.NextMaintenanceTime() <= 6000 {
		t.Fatalf("next_maintenance_time should have advanced past 6000, got %d", dynProps.NextMaintenanceTime())
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
	// fire (round=0 in calcNextMaintenanceTime, since blockTime − currentMaint
	// < interval).
	dynProps := state.LoadDynamicProperties(diskdb)
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

	want := uint64(numBlocks)
	got := uint64(state.LoadDynamicProperties(diskdb).LatestSolidifiedBlockNum())
	if got != want {
		t.Fatalf("LatestSolidifiedBlockNum: got %d, want %d", got, want)
	}

	// Also confirm it matches the current head.
	if bc.CurrentBlock().Number() != want {
		t.Fatalf("CurrentBlock.Number: got %d, want %d", bc.CurrentBlock().Number(), want)
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
