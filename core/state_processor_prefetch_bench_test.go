package core

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	benchPrefetchAccounts = 2048
	benchPrefetchTxs      = 512
)

func BenchmarkProcessBlock_HeavyTRX_HeavyState(b *testing.B) {
	benchProcessBlockPrefetchVariants(b, 0)
}

func BenchmarkProcessBlock_HeavyTRX_ColdState(b *testing.B) {
	benchProcessBlockPrefetchVariants(b, 25*time.Microsecond)
}

func benchProcessBlockPrefetchVariants(b *testing.B, coldReadLatency time.Duration) {
	variants := []struct {
		name     string
		prefetch processBlockPrefetchConfig
	}{
		{name: "prefetch=off"},
		{name: "prefetch=on_workers=2_lookahead=8", prefetch: processBlockPrefetchConfig{Enabled: true, Workers: 2, Lookahead: 8}},
		{name: "prefetch=on_workers=4_lookahead=8", prefetch: processBlockPrefetchConfig{Enabled: true, Workers: 4, Lookahead: 8}},
		{name: "prefetch=on_workers=8_lookahead=8", prefetch: processBlockPrefetchConfig{Enabled: true, Workers: 8, Lookahead: 8}},
	}
	for _, variant := range variants {
		b.Run(variant.name, func(b *testing.B) {
			benchProcessBlockPrefetchVariant(b, coldReadLatency, variant.prefetch)
		})
	}
}

func benchProcessBlockPrefetchVariant(b *testing.B, coldReadLatency time.Duration, prefetch processBlockPrefetchConfig) {
	fixture := newProcessBlockPrefetchBenchFixture(b, coldReadLatency)
	b.ReportAllocs()
	b.ReportMetric(benchPrefetchTxs, "tx/block")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		fixture.ResetColdReads()
		statedb, err := state.New(fixture.root, fixture.stateDB)
		if err != nil {
			b.Fatal(err)
		}
		dynProps := state.NewDynamicProperties()
		b.StartTimer()
		if _, _, err := processBlock(
			statedb,
			dynProps,
			fixture.block,
			fixture.db,
			nil,
			0,
			params.DefaultBlockNumForEnergyLimit,
			false,
			tcommon.Hash{},
			nil,
			nil,
			nil,
			prefetch,
			nil,
			-1,
			nil,
		); err != nil {
			b.Fatalf("ProcessBlock: %v", err)
		}
	}
}

type processBlockPrefetchBenchFixture struct {
	db      *benchColdReadDB
	stateDB *state.Database
	root    tcommon.Hash
	block   *types.Block
}

func newProcessBlockPrefetchBenchFixture(b *testing.B, coldReadLatency time.Duration) *processBlockPrefetchBenchFixture {
	b.Helper()
	base := ethrawdb.NewMemoryDatabase()
	db := newBenchColdReadDB(base, coldReadLatency)
	stateDB := state.NewDatabase(db)
	statedb, err := state.New(tcommon.Hash{}, stateDB)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < benchPrefetchAccounts; i++ {
		addr := benchPrefetchAddr(i)
		statedb.CreateAccount(addr, corepb.AccountType_Normal)
		statedb.AddBalance(addr, 1_000_000_000_000)
	}
	root, err := statedb.Commit()
	if err != nil {
		b.Fatal(err)
	}
	block := benchPrefetchBlock()
	db.EnableColdReads()
	return &processBlockPrefetchBenchFixture{
		db:      db,
		stateDB: stateDB,
		root:    root,
		block:   block,
	}
}

func benchPrefetchAddr(i int) tcommon.Address {
	var addr tcommon.Address
	addr[0] = tcommon.AddressPrefixMainnet
	binary.BigEndian.PutUint64(addr[1:9], uint64(i+1))
	return addr
}

func benchPrefetchBlock() *types.Block {
	txs := make([]*corepb.Transaction, 0, benchPrefetchTxs)
	for i := 0; i < benchPrefetchTxs; i++ {
		from := benchPrefetchAddr(i)
		to := benchPrefetchAddr(benchPrefetchTxs + i)
		param, _ := anypb.New(&contractpb.TransferContract{
			OwnerAddress: from.Bytes(),
			ToAddress:    to.Bytes(),
			Amount:       1,
		})
		txs = append(txs, &corepb.Transaction{RawData: &corepb.TransactionRaw{
			Expiration: 600_000,
			Contract: []*corepb.Transaction_Contract{{
				Type:      corepb.Transaction_Contract_TransferContract,
				Parameter: param,
			}},
		}})
	}
	return types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number:    1,
			Timestamp: 3000,
		}},
		Transactions: txs,
	})
}

type benchColdReadDB struct {
	ethdb.Database
	latency time.Duration
	enabled atomic.Bool
	mu      sync.Mutex
	warm    map[string]struct{}
}

func newBenchColdReadDB(base ethdb.Database, latency time.Duration) *benchColdReadDB {
	return &benchColdReadDB{
		Database: base,
		latency:  latency,
		warm:     make(map[string]struct{}),
	}
}

func (db *benchColdReadDB) EnableColdReads() {
	db.enabled.Store(true)
}

func (db *benchColdReadDB) ResetColdReads() {
	if db.latency <= 0 {
		return
	}
	db.mu.Lock()
	db.warm = make(map[string]struct{})
	db.mu.Unlock()
}

func (db *benchColdReadDB) Get(key []byte) ([]byte, error) {
	db.delayColdRead(key)
	return db.Database.Get(key)
}

func (db *benchColdReadDB) Has(key []byte) (bool, error) {
	db.delayColdRead(key)
	return db.Database.Has(key)
}

func (db *benchColdReadDB) delayColdRead(key []byte) {
	if db.latency <= 0 || !db.enabled.Load() {
		return
	}
	k := string(key)
	db.mu.Lock()
	_, ok := db.warm[k]
	if !ok {
		db.warm[k] = struct{}{}
	}
	db.mu.Unlock()
	if !ok {
		time.Sleep(db.latency)
	}
}

func (f *processBlockPrefetchBenchFixture) ResetColdReads() {
	if f == nil || f.db == nil {
		return
	}
	f.db.ResetColdReads()
}
