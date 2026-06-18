package core

import (
	"strings"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestBackfillBalanceTracesByReplayWritesMissingRows(t *testing.T) {
	sourceDB, sourceChain, genesis, block1 := newBalanceTraceBackfillSource(t)
	if got := rawdb.ReadBlockBalanceTrace(sourceDB, 1); got != nil {
		t.Fatalf("source BlockBalanceTrace before backfill = %+v, want nil", got)
	}

	var progress []uint64
	result, err := BackfillBalanceTracesByReplay(
		rawdb.NewChainDB(sourceDB, rawdb.NoopAncient{}),
		sourceDB,
		ethrawdb.NewMemoryDatabase(),
		genesis,
		BalanceTraceReplayBackfillOptions{
			FromBlock: 1,
			ToBlock:   1,
			ETL:       etl.Options{TempDir: t.TempDir(), BufferLimit: 1},
			Progress: func(p BalanceTraceReplayBackfillProgress) {
				if p.Phase == "replay" {
					progress = append(progress, p.Block)
				}
			},
		},
	)
	if err != nil {
		t.Fatalf("BackfillBalanceTracesByReplay: %v", err)
	}
	if result.BlocksReplayed != 1 || result.BlocksBackfilled != 1 ||
		result.BlockTraceRows != 1 || result.AccountTraceRows != 2 {
		t.Fatalf("result = %+v, want one block trace and two account traces", result)
	}
	if result.ETL.Applied != 3 || result.ETL.SpilledRuns == 0 {
		t.Fatalf("ETL stats = %+v, want 3 applied rows and forced spill", result.ETL)
	}
	if len(progress) != 1 || progress[0] != 1 {
		t.Fatalf("progress = %v, want replay block 1", progress)
	}
	trace := rawdb.ReadBlockBalanceTrace(sourceDB, 1)
	if trace == nil {
		t.Fatal("BlockBalanceTrace missing after backfill")
	}
	if trace.GetBlockIdentifier().GetNumber() != 1 || string(trace.GetBlockIdentifier().GetHash()) != string(block1.Hash().Bytes()) {
		t.Fatalf("trace id = %+v, want block 1 %x", trace.GetBlockIdentifier(), block1.Hash())
	}
	if got := len(trace.GetTransactionBalanceTrace()); got != 1 {
		t.Fatalf("tx traces = %d, want 1", got)
	}
	sender := testInsertAddr(1)
	receiver := testInsertAddr(2)
	if _, ok := rawdb.ReadAccountTrace(sourceDB, sender.Bytes(), 1); !ok {
		t.Fatal("sender AccountTrace missing after backfill")
	}
	if _, ok := rawdb.ReadAccountTrace(sourceDB, receiver.Bytes(), 1); !ok {
		t.Fatal("receiver AccountTrace missing after backfill")
	}
	if sourceChain.CurrentBlock().Hash() != block1.Hash() {
		t.Fatalf("source chain head changed: got %x want %x", sourceChain.CurrentBlock().Hash(), block1.Hash())
	}
}

func TestBackfillBalanceTracesByReplayRejectsExistingMismatch(t *testing.T) {
	sourceDB, _, genesis, _ := newBalanceTraceBackfillSource(t)
	if err := rawdb.WriteBlockBalanceTrace(sourceDB, 1, &contractpb.BlockBalanceTrace{
		BlockIdentifier: &contractpb.BlockBalanceTrace_BlockIdentifier{
			Number: 1,
			Hash:   []byte{0x01},
		},
	}); err != nil {
		t.Fatalf("WriteBlockBalanceTrace mismatch: %v", err)
	}

	_, err := BackfillBalanceTracesByReplay(
		rawdb.NewChainDB(sourceDB, rawdb.NoopAncient{}),
		sourceDB,
		ethrawdb.NewMemoryDatabase(),
		genesis,
		BalanceTraceReplayBackfillOptions{FromBlock: 1, ToBlock: 1},
	)
	if err == nil || !strings.Contains(err.Error(), "differs from replay output") {
		t.Fatalf("BackfillBalanceTracesByReplay error = %v, want mismatch rejection", err)
	}
}

func TestBackfillBalanceTracesByReplayHonorsColdTargetTraceRows(t *testing.T) {
	sourceDB, _, genesis, _ := newBalanceTraceBackfillSource(t)
	replayDB := ethrawdb.NewMemoryDatabase()
	scratchTarget := ethrawdb.NewMemoryDatabase()
	if _, err := BackfillBalanceTracesByReplay(
		rawdb.NewChainDB(sourceDB, rawdb.NoopAncient{}),
		scratchTarget,
		replayDB,
		genesis,
		BalanceTraceReplayBackfillOptions{FromBlock: 1, ToBlock: 1},
	); err != nil {
		t.Fatalf("prime replay DB: %v", err)
	}

	trace := rawdb.ReadBlockBalanceTrace(scratchTarget, 1)
	if trace == nil {
		t.Fatal("scratch BlockBalanceTrace missing")
	}
	cold := newBalanceTraceBackfillColdTraceReader()
	cold.putBlockTrace(1, trace)
	for _, owner := range []tcommon.Address{testInsertAddr(1), testInsertAddr(2)} {
		balance, ok := rawdb.ReadAccountTrace(scratchTarget, owner.Bytes(), 1)
		if !ok {
			t.Fatalf("scratch AccountTrace %x missing", owner)
		}
		cold.putAccountTrace(owner.Bytes(), 1, balance)
	}
	targetReader := rawdb.NewChainDB(sourceDB, rawdb.NoopAncient{})
	targetReader.SetBalanceTraceReader(cold)

	result, err := BackfillBalanceTracesByReplay(
		rawdb.NewChainDB(sourceDB, rawdb.NoopAncient{}),
		sourceDB,
		replayDB,
		genesis,
		BalanceTraceReplayBackfillOptions{
			FromBlock:         1,
			ToBlock:           1,
			TargetTraceReader: targetReader,
		},
	)
	if err != nil {
		t.Fatalf("BackfillBalanceTracesByReplay with cold target rows: %v", err)
	}
	if result.BlockTraceRows != 0 || result.AccountTraceRows != 0 ||
		result.ExistingBlockTraces != 1 || result.ExistingAccountTraces != 2 {
		t.Fatalf("result = %+v, want cold rows counted as existing without hot writes", result)
	}
	if got := rawdb.ReadBlockBalanceTrace(sourceDB, 1); got != nil {
		t.Fatalf("hot target BlockBalanceTrace = %+v, want nil", got)
	}
	for _, owner := range []tcommon.Address{testInsertAddr(1), testInsertAddr(2)} {
		if _, ok := rawdb.ReadAccountTrace(sourceDB, owner.Bytes(), 1); ok {
			t.Fatalf("hot target AccountTrace %x written despite cold existing row", owner)
		}
	}
}

func TestBackfillBalanceTracesByReplayRejectsCorruptColdTargetTrace(t *testing.T) {
	sourceDB, _, genesis, _ := newBalanceTraceBackfillSource(t)
	cold := newBalanceTraceBackfillColdTraceReader()
	cold.putBlockTrace(1, &contractpb.BlockBalanceTrace{
		BlockIdentifier: &contractpb.BlockBalanceTrace_BlockIdentifier{
			Number: 2,
			Hash:   []byte{0x02},
		},
	})
	targetReader := rawdb.NewChainDB(sourceDB, rawdb.NoopAncient{})
	targetReader.SetBalanceTraceReader(cold)

	_, err := BackfillBalanceTracesByReplay(
		rawdb.NewChainDB(sourceDB, rawdb.NoopAncient{}),
		sourceDB,
		ethrawdb.NewMemoryDatabase(),
		genesis,
		BalanceTraceReplayBackfillOptions{
			FromBlock:         1,
			ToBlock:           1,
			TargetTraceReader: targetReader,
		},
	)
	if err == nil || !strings.Contains(err.Error(), "target BlockBalanceTrace at block 1 is corrupt") {
		t.Fatalf("BackfillBalanceTracesByReplay corrupt cold target error = %v, want corrupt target trace error", err)
	}
	if got := rawdb.ReadBlockBalanceTrace(sourceDB, 1); got != nil {
		t.Fatalf("hot target BlockBalanceTrace = %+v, want nil after corrupt cold target rejection", got)
	}
}

func TestBackfillBalanceTracesByReplayResumesFromReplayHead(t *testing.T) {
	sourceDB, sourceChain, genesis, block1 := newBalanceTraceBackfillSource(t)
	block2 := buildTransferBlock(t, 2, 6000, block1.Hash(), tcommon.Address{}, 7_000_000)
	if err := sourceChain.InsertBlock(block2); err != nil {
		t.Fatalf("InsertBlock source block2: %v", err)
	}
	replayDB := ethrawdb.NewMemoryDatabase()
	scratchTarget := ethrawdb.NewMemoryDatabase()
	if _, err := BackfillBalanceTracesByReplay(
		rawdb.NewChainDB(sourceDB, rawdb.NoopAncient{}),
		scratchTarget,
		replayDB,
		genesis,
		BalanceTraceReplayBackfillOptions{FromBlock: 1, ToBlock: 1},
	); err != nil {
		t.Fatalf("prime replay DB: %v", err)
	}
	if got := rawdb.ReadBlockBalanceTrace(sourceDB, 1); got != nil {
		t.Fatalf("source BlockBalanceTrace after replay prime = %+v, want nil", got)
	}

	var phases []string
	result, err := BackfillBalanceTracesByReplay(
		rawdb.NewChainDB(sourceDB, rawdb.NoopAncient{}),
		sourceDB,
		replayDB,
		genesis,
		BalanceTraceReplayBackfillOptions{
			FromBlock: 1,
			ToBlock:   2,
			Progress: func(p BalanceTraceReplayBackfillProgress) {
				phases = append(phases, p.Phase)
			},
		},
	)
	if err != nil {
		t.Fatalf("resume BackfillBalanceTracesByReplay: %v", err)
	}
	if result.ReplayStartBlock != 2 || result.ReplayHeadBlock != 2 || result.BlocksReplayed != 1 || result.BlocksBackfilled != 2 {
		t.Fatalf("result = %+v, want resume from block 2 and backfill two blocks", result)
	}
	if strings.Join(phases, ",") != "copy,replay" {
		t.Fatalf("progress phases = %v, want copy,replay", phases)
	}
	for _, block := range []*types.Block{block1, block2} {
		trace := rawdb.ReadBlockBalanceTrace(sourceDB, int64(block.Number()))
		if trace == nil {
			t.Fatalf("BlockBalanceTrace %d missing after resume", block.Number())
		}
		if string(trace.GetBlockIdentifier().GetHash()) != string(block.Hash().Bytes()) {
			t.Fatalf("trace %d hash = %x, want %x", block.Number(), trace.GetBlockIdentifier().GetHash(), block.Hash())
		}
	}
}

func TestBackfillBalanceTracesByReplaySkipsRestoredPrefixBeforeRange(t *testing.T) {
	sourceDB, sourceChain, genesis, block1 := newBalanceTraceBackfillSource(t)
	block2 := buildTransferBlock(t, 2, 6000, block1.Hash(), tcommon.Address{}, 7_000_000)
	if err := sourceChain.InsertBlock(block2); err != nil {
		t.Fatalf("InsertBlock source block2: %v", err)
	}
	replayDB := ethrawdb.NewMemoryDatabase()
	scratchTarget := ethrawdb.NewMemoryDatabase()
	if _, err := BackfillBalanceTracesByReplay(
		rawdb.NewChainDB(sourceDB, rawdb.NoopAncient{}),
		scratchTarget,
		replayDB,
		genesis,
		BalanceTraceReplayBackfillOptions{FromBlock: 1, ToBlock: 2},
	); err != nil {
		t.Fatalf("prime replay DB: %v", err)
	}

	sourceReads := &balanceTraceBackfillCountingAncient{reads: make(map[uint64]int)}
	var phases []string
	result, err := BackfillBalanceTracesByReplay(
		rawdb.NewChainDB(sourceDB, sourceReads),
		sourceDB,
		replayDB,
		genesis,
		BalanceTraceReplayBackfillOptions{
			FromBlock: 2,
			ToBlock:   2,
			Progress: func(p BalanceTraceReplayBackfillProgress) {
				phases = append(phases, p.Phase)
			},
		},
	)
	if err != nil {
		t.Fatalf("BackfillBalanceTracesByReplay covered range: %v", err)
	}
	if result.ReplayStartBlock != 3 || result.ReplayHeadBlock != 2 || result.BlocksReplayed != 0 || result.BlocksBackfilled != 1 {
		t.Fatalf("result = %+v, want covered range copied without replay", result)
	}
	if strings.Join(phases, ",") != "copy" {
		t.Fatalf("progress phases = %v, want copy only", phases)
	}
	if sourceReads.reads[1] != 0 {
		t.Fatalf("source block 1 reads = %d, want restored prefix before range skipped", sourceReads.reads[1])
	}
	if got := rawdb.ReadBlockBalanceTrace(sourceDB, 1); got != nil {
		t.Fatalf("BlockBalanceTrace 1 = %+v, want skipped source target row", got)
	}
	if got := rawdb.ReadBlockBalanceTrace(sourceDB, 2); got == nil {
		t.Fatal("BlockBalanceTrace 2 missing after covered range copy")
	}
}

type balanceTraceBackfillCountingAncient struct {
	rawdb.NoopAncient
	reads map[uint64]int
}

func (a *balanceTraceBackfillCountingAncient) Ancient(kind string, number uint64) ([]byte, error) {
	if kind == rawdb.AncientBlocksTable {
		a.reads[number]++
	}
	return nil, rawdb.ErrNotInAncient
}

type balanceTraceBackfillColdTraceReader struct {
	blockTraces   map[int64]*contractpb.BlockBalanceTrace
	accountTraces map[string]map[int64]int64
}

func newBalanceTraceBackfillColdTraceReader() *balanceTraceBackfillColdTraceReader {
	return &balanceTraceBackfillColdTraceReader{
		blockTraces:   make(map[int64]*contractpb.BlockBalanceTrace),
		accountTraces: make(map[string]map[int64]int64),
	}
}

func (r *balanceTraceBackfillColdTraceReader) BlockBalanceTrace(blockNum int64) (*contractpb.BlockBalanceTrace, bool, error) {
	trace, ok := r.blockTraces[blockNum]
	return trace, ok, nil
}

func (r *balanceTraceBackfillColdTraceReader) AccountTraceAtOrBefore(owner []byte, blockNum int64) (int64, int64, bool, error) {
	rows := r.accountTraces[string(owner)]
	var bestBlock int64
	var bestBalance int64
	var ok bool
	for n, balance := range rows {
		if n > blockNum {
			continue
		}
		if !ok || n > bestBlock {
			bestBlock = n
			bestBalance = balance
			ok = true
		}
	}
	return bestBlock, bestBalance, ok, nil
}

func (r *balanceTraceBackfillColdTraceReader) putBlockTrace(blockNum int64, trace *contractpb.BlockBalanceTrace) {
	r.blockTraces[blockNum] = trace
}

func (r *balanceTraceBackfillColdTraceReader) putAccountTrace(owner []byte, blockNum int64, balance int64) {
	key := string(owner)
	if r.accountTraces[key] == nil {
		r.accountTraces[key] = make(map[int64]int64)
	}
	r.accountTraces[key][blockNum] = balance
}

func newBalanceTraceBackfillSource(t *testing.T) (ethdb.Database, *BlockChain, *params.Genesis, *types.Block) {
	t.Helper()
	sourceDB := ethrawdb.NewMemoryDatabase()
	cfg := cloneMainnetChainConfig()
	cfg.HistoryEnabled = false
	sender := testInsertAddr(1)
	receiver := testInsertAddr(2)
	genesis := &params.Genesis{
		Config:    cfg,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: sender, Balance: 99_000_000_000_000_000},
			{Address: receiver, Balance: 1},
		},
		DynamicProperties: map[string]int64{
			"next_maintenance_time": 1<<62 - 1,
		},
	}
	_, genesisHash, err := SetupGenesisBlock(sourceDB, genesis)
	if err != nil {
		t.Fatalf("SetupGenesisBlock: %v", err)
	}
	sourceChain, err := NewBlockChain(sourceDB, state.NewDatabase(sourceDB), cfg)
	if err != nil {
		t.Fatalf("NewBlockChain: %v", err)
	}
	t.Cleanup(func() { _ = sourceChain.Close() })
	block1 := buildTransferBlock(t, 1, 3000, genesisHash, tcommon.Address{}, 5_000_000)
	if err := sourceChain.InsertBlock(block1); err != nil {
		t.Fatalf("InsertBlock source: %v", err)
	}
	return sourceDB, sourceChain, genesis, block1
}
