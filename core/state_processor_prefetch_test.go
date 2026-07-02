package core

import (
	"sync/atomic"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func TestProcessBlockStatePrefetchMatchesSerialStateRoot(t *testing.T) {
	run := func(prefetch bool) (tcommon.Hash, []*corepb.TransactionInfo, uint64) {
		statedb, db := newPrefetchProcessBlockState(t)
		dynProps := state.NewDynamicProperties()
		block := types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{
					Number:    1,
					Timestamp: 3000,
				},
			},
			Transactions: prefetchParityTransactions(),
		})
		cfg := processBlockPrefetchConfig{}
		if prefetch {
			cfg = processBlockPrefetchConfig{Enabled: true, Workers: 2, Lookahead: 8}
		}
		txInfos, _, err := processBlock(
			statedb,
			dynProps,
			block,
			db,
			nil,
			0,
			params.DefaultBlockNumForEnergyLimit,
			false,
			tcommon.Hash{},
			nil,
			nil,
			nil,
			cfg,
			nil,
			nil,
			-1,
			nil,
		)
		if err != nil {
			t.Fatalf("processBlock(prefetch=%v): %v", prefetch, err)
		}
		root, err := statedb.Commit()
		if err != nil {
			t.Fatalf("commit(prefetch=%v): %v", prefetch, err)
		}
		return root, txInfos, db.gets.Load()
	}

	serialRoot, serialInfos, serialGets := run(false)
	prefetchRoot, prefetchInfos, prefetchGets := run(true)
	if serialRoot != prefetchRoot {
		t.Fatalf("prefetch root = %x, want serial root %x", prefetchRoot, serialRoot)
	}
	if serialGets != 0 {
		t.Fatalf("serial run unexpectedly read through counting db: %d", serialGets)
	}
	if prefetchGets == 0 {
		t.Fatal("prefetch run did not issue raw db reads")
	}
	if len(prefetchInfos) != len(serialInfos) {
		t.Fatalf("prefetch txInfos len = %d, want %d", len(prefetchInfos), len(serialInfos))
	}
	for i := range serialInfos {
		if !proto.Equal(prefetchInfos[i], serialInfos[i]) {
			t.Fatalf("txInfo %d differs with prefetch\nprefetch=%v\nserial=%v", i, prefetchInfos[i], serialInfos[i])
		}
	}
}

func TestProcessBlockStatePrefetchNormalizesDefaults(t *testing.T) {
	cfg := normalizeProcessBlockPrefetchConfig(processBlockPrefetchConfig{Enabled: true, Workers: -1})
	if !cfg.Enabled {
		t.Fatal("enabled config was disabled")
	}
	if cfg.Workers != 0 {
		t.Fatalf("workers = %d, want 0", cfg.Workers)
	}
	if cfg.Lookahead != params.StatePrefetchDefaultLookahead {
		t.Fatalf("lookahead = %d, want %d", cfg.Lookahead, params.StatePrefetchDefaultLookahead)
	}
}

func TestProcessBlockStatePrefetcherSupportsRuntimeOnlyHints(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	p := newProcessBlockPrefetcher(db, processBlockPrefetchConfig{Enabled: true, Workers: 1, Lookahead: 0}, 1)
	if p == nil {
		t.Fatal("single-tx runtime prefetcher is nil")
	}
	p.Stop()
}

func prefetchParityTransactions() []*corepb.Transaction {
	txs := make([]*corepb.Transaction, 0, 100)
	for i := 0; i < 100; i++ {
		to := byte(2)
		if i%2 == 1 {
			to = 3
		}
		txs = append(txs, makeTestTransferTx(1, to, 1_000).Proto())
	}
	return txs
}

func newPrefetchProcessBlockState(t *testing.T) (*state.StateDB, *countingPrefetchKV) {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)
	statedb, err := state.New(tcommon.Hash(ethtypes.EmptyRootHash), sdb)
	if err != nil {
		t.Fatal(err)
	}
	for _, acct := range []struct {
		addr    tcommon.Address
		balance int64
	}{
		{testProcessorAddr(1), 10_000_000},
		{testProcessorAddr(2), 0},
		{testProcessorAddr(3), 0},
	} {
		statedb.CreateAccount(acct.addr, corepb.AccountType_Normal)
		statedb.AddBalance(acct.addr, acct.balance)
	}
	root, err := statedb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := state.New(root, sdb)
	if err != nil {
		t.Fatal(err)
	}
	return reopened, &countingPrefetchKV{db: diskdb}
}

type countingPrefetchKV struct {
	db   ethdb.KeyValueStore
	gets atomic.Uint64
}

func (db *countingPrefetchKV) Has(key []byte) (bool, error) {
	return db.db.Has(key)
}

func (db *countingPrefetchKV) Get(key []byte) ([]byte, error) {
	db.gets.Add(1)
	return db.db.Get(key)
}

func (db *countingPrefetchKV) Put(key []byte, value []byte) error {
	return db.db.Put(key, value)
}

func (db *countingPrefetchKV) Delete(key []byte) error {
	return db.db.Delete(key)
}
