package core

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/statecodec"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/internal/historychunk"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

type historyIndexSnapshotStore struct {
	ethdb.KeyValueStore
	created, closed     int
	createErr, closeErr error
	nilSnapshot         bool
	unpinnedSnapshot    bool
	factoryFailed       bool
	itersAfterFailure   int
}

func TestStateHistoryIndexStageSharedSnapshotAcrossFlushAndNewSequence(t *testing.T) {
	for _, mode := range []string{"success", "interrupted", "close"} {
		t.Run(mode, func(t *testing.T) {
			open := func() *historyIndexSnapshotStore {
				db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
				if err != nil {
					t.Fatal(err)
				}
				return &historyIndexSnapshotStore{KeyValueStore: db}
			}
			hot, history := open(), open()
			disk := rawdb.NewDirectHistoryStore(hot, history)
			t.Cleanup(func() { _ = disk.Close() })
			cfg := cloneMainnetChainConfig()
			cfg.HistoryEnabled = true
			genesis := &params.Genesis{Config: cfg, Accounts: []params.GenesisAccount{{Address: testInsertAddr(1), Balance: 99_000_000_000_000_000}}, DynamicProperties: map[string]int64{"next_maintenance_time": 1<<62 - 1}}
			_, genesisHash, err := SetupGenesisBlock(disk, genesis)
			if err != nil {
				t.Fatal(err)
			}
			bc, err := NewBlockChain(disk, state.NewDatabase(rawdb.WrapKeyValueStore(disk)), cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = bc.Close() })
			one := buildTransferBlock(t, 1, 3000, genesisHash, testInsertAddr(1), 1)
			two := buildTransferBlock(t, 2, 6000, one.Hash(), testInsertAddr(1), 1)
			if err := bc.InsertSyncBlocksWithStageHook([]*types.Block{one, two}, nil); err != nil {
				t.Fatal(err)
			}
			if err := bc.buffer.FlushUpTo(2, disk); err != nil {
				t.Fatal(err)
			}
			dp := bc.cachedDynProps()
			dp.SetLatestSolidifiedBlockNum(2)
			bc.storeDynPropsCache(dp)
			opts := etl.Options{TempDir: t.TempDir(), BufferLimit: 1 << 20, BatchSize: 1 << 20}
			bc.SetStateHistoryIndexETLOptions(opts)

			// A valid legacy delegation record exceeds the shared-chunk gate.
			// The same payload in two blocks makes the second pack reference
			// chunks first published by the first block's atomic Buffer layer.
			owner := common.Address{0x41, 3}
			record := &corepb.DelegatedResourceAccountIndex{Account: owner.Bytes(), ToAccounts: make([][]byte, 20000)}
			rng := rand.New(rand.NewSource(20261016))
			for i := range record.ToAccounts {
				record.ToAccounts[i] = make([]byte, 21)
				_, _ = rng.Read(record.ToAccounts[i])
				record.ToAccounts[i][0] = 0x41
			}
			payload, err := statecodec.MarshalDelegationIndex(record)
			if err != nil || len(payload) < historychunk.MaxSize {
				t.Fatalf("shared fixture size=%d err=%v", len(payload), err)
			}
			key := rawdb.DrAccountIndexLegacyStateKey(owner.Bytes())
			var rows []*rawdb.StateDomainChange
			oracle := rawdb.NewMemoryDatabase()
			defer oracle.Close()
			for _, block := range []*types.Block{one, two} {
				r, ok, err := rawdb.ReadStateTxRange(disk, block.Number())
				if err != nil || !ok {
					t.Fatalf("tx range present=%v err=%v", ok, err)
				}
				row := &rawdb.StateDomainChange{BlockNum: block.Number(), Seq: 1, TxNum: r.BeginTxNum, FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: common.SystemAccountAddress, Domain: kvdomains.SystemDelegation, Key: key, PrevExists: true, Prev: payload}
				rows = append(rows, row)
				if err := rawdb.WriteStateDomainChangeBlockRows(oracle, []*rawdb.StateDomainChange{row}); err != nil {
					t.Fatal(err)
				}
				if err := rawdb.WriteStateTxRange(oracle, block.Number(), block.Hash(), r.BeginTxNum, r.EndTxNum); err != nil {
					t.Fatal(err)
				}
			}
			hashes := map[uint64]common.Hash{1: one.Hash(), 2: two.Hash()}
			lookup := func(n uint64) (common.Hash, bool, error) { hash, ok := hashes[n]; return hash, ok, nil }
			want, err := rawdb.RebuildStateHistoryIndexInterruptible(oracle, oracle, 1, 2, opts, lookup, nil)
			if err != nil {
				t.Fatal(err)
			}
			rawdb.SetStateHistoryCrossBlockDedup(true)
			defer rawdb.SetStateHistoryCrossBlockDedup(false)
			for _, row := range rows {
				bc.buffer.BeginBlock(hashes[row.BlockNum], row.BlockNum)
				if err := rawdb.WriteStateDomainChangeBlockRows(bc.buffer, []*rawdb.StateDomainChange{row}); err != nil {
					t.Fatal(err)
				}
				bc.buffer.CommitBlock()
				if row.BlockNum == 1 {
					if err := bc.buffer.FlushUpTo(1, disk); err != nil {
						t.Fatal(err)
					}
				}
			}
			pin, err := bc.buffer.NewStateHistoryReadSnapshotThrough(2)
			if err != nil {
				t.Fatal(err)
			}
			inspection := rawdb.DefaultHistoryPrevInspectOptions()
			inspection.FromBlock, inspection.ToBlock, inspection.Samples = 1, 2, 2
			proof, err := rawdb.InspectStateHistoryPrev(context.Background(), pin, inspection)
			if err != nil || !proof.Complete || proof.ChunkReads == 0 || proof.PacksComplete != 2 {
				t.Fatalf("shared pack proof=%+v err=%v", proof, err)
			}
			for _, sample := range proof.Samples {
				if sample.Codec != "shared3" {
					t.Fatalf("fixture selected %s instead of shared3", sample.Codec)
				}
			}
			for _, row := range rows {
				got, ok, err := rawdb.ReadStateDomainChange(pin, row.BlockNum, 1)
				if err != nil || !ok || !bytes.Equal(got.Prev, payload) {
					t.Fatalf("overlay/base shared payload present=%v err=%v", ok, err)
				}
			}
			if err := pin.Close(); err != nil {
				t.Fatal(err)
			}
			var chunkKey, chunkValue []byte
			for _, r := range rawdb.DirectHistoryRanges() {
				if r.Name != "state_history_chunk" {
					continue
				}
				it := disk.NewIterator(r.Start, nil)
				if it.Next() {
					chunkKey, chunkValue = bytes.Clone(it.Key()), bytes.Clone(it.Value())
				}
				if err := it.Error(); err != nil {
					t.Fatal(err)
				}
				it.Release()
			}
			if len(chunkKey) == 0 {
				t.Fatal("shared fixture has no durable chunk")
			}
			t.Cleanup(func() { _ = disk.Put(chunkKey, chunkValue) })
			hot.created, hot.closed, history.created, history.closed = 0, 0, 0, 0
			failure := errors.New("shared source close failure")
			if mode == "close" {
				history.closeErr = failure
			}
			calls := 0
			movingCreated, movingClosed := 0, 0
			result, err := bc.AdvanceStateHistoryIndexStageInterruptible(2, func() bool {
				calls++
				if calls == 1 {
					if err := bc.buffer.FlushUpTo(2, disk); err != nil {
						t.Fatal(err)
					}
					if err := disk.Put(chunkKey, []byte("newer corrupt chunk sequence")); err != nil {
						t.Fatal(err)
					}
					// This independent newer view proves the durable corruption
					// is visible. Account for its lifetime apart from the ETL pin.
					beforeCreated, beforeClosed := history.created, history.closed
					moving, closeMoving, err := rawdb.AcquireStateHistoryBuildReadView(disk)
					if err != nil {
						t.Fatal(err)
					}
					_, _, decodeErr := rawdb.ReadStateDomainChange(moving, 1, 1)
					closeErr := closeMoving()
					movingCreated, movingClosed = history.created-beforeCreated, history.closed-beforeClosed
					if movingCreated != 1 || movingClosed != 1 {
						t.Fatalf("newer oracle lifecycle %d/%d", movingCreated, movingClosed)
					}
					if closeErr != nil && !errors.Is(closeErr, failure) {
						t.Fatal(closeErr)
					}
					if decodeErr == nil || errors.Is(decodeErr, failure) {
						t.Fatal("moving source unexpectedly decoded overwritten chunk")
					}
				}
				return mode == "interrupted" && calls >= 3
			})
			if mode == "success" {
				if err != nil || !result.Advanced || !reflect.DeepEqual(result.Rebuilt, want) {
					t.Fatalf("result=%+v oracle=%+v err=%v", result, want, err)
				}
				readFamily := func(source ethdb.Iteratee, prefix []byte) map[string][]byte {
					values := make(map[string][]byte)
					it := source.NewIterator(prefix, nil)
					defer it.Release()
					for it.Next() {
						values[string(it.Key())] = bytes.Clone(it.Value())
					}
					if err := it.Error(); err != nil {
						t.Fatal(err)
					}
					return values
				}
				for _, r := range rawdb.DirectHistoryRanges() {
					if r.Name == "state_change_posting" || r.Name == "state_change_directory" {
						if !reflect.DeepEqual(readFamily(disk, r.Start), readFamily(oracle, r.Start)) {
							t.Fatalf("%s differs from independent serial index bytes", r.Name)
						}
					}
				}
			} else {
				wantErr := failure
				if mode == "interrupted" {
					wantErr = rawdb.ErrStateHistoryIndexRebuildInterrupted
				}
				if !errors.Is(err, wantErr) || result.Advanced {
					t.Fatalf("result=%+v err=%v", result, err)
				}
			}
			if hot.created != 0 || hot.closed != 0 || history.created-movingCreated != 1 || history.closed-movingClosed != 1 {
				t.Fatalf("shared source lifecycle hot=%d/%d history=%d/%d plus newer oracle=%d/%d", hot.created, hot.closed, history.created, history.closed, movingCreated, movingClosed)
			}
			progress, ok, err := rawdb.ReadStageProgressRow(disk, rawdb.StageStateHistoryIndex)
			wantBlock := uint64(0)
			if mode == "success" {
				wantBlock = 2
			}
			if err != nil || !ok || progress.BlockNum != wantBlock {
				t.Fatalf("progress=%+v present=%v err=%v", progress, ok, err)
			}
			t.Logf("shared3 payload=%d chunks_read=%d mode=%s ETL hot=0 history=1 newer oracle history=%d source stayed at original sequence across overlay flush", len(payload), proof.ChunkReads, mode, movingCreated)
		})
	}
}

type historyIndexSnapshot struct {
	pointread.KeyValueSnapshot
	owner *historyIndexSnapshotStore
}

func (s *historyIndexSnapshot) IsPinnedKeyValueView() bool {
	return !s.owner.unpinnedSnapshot && s.KeyValueSnapshot.(pointread.PinnedKeyValueView).IsPinnedKeyValueView()
}

func (s *historyIndexSnapshot) Close() error {
	s.owner.closed++
	return errors.Join(s.KeyValueSnapshot.Close(), s.owner.closeErr)
}
func (s *historyIndexSnapshotStore) NewKeyValueSnapshot() (pointread.KeyValueSnapshot, error) {
	s.created++
	if s.createErr != nil {
		s.factoryFailed = true
		return nil, s.createErr
	}
	if s.nilSnapshot {
		s.factoryFailed = true
		return nil, nil
	}
	snapshot, err := s.KeyValueStore.(pointread.KeyValueSnapshotter).NewKeyValueSnapshot()
	if err != nil {
		return nil, err
	}
	if s.unpinnedSnapshot {
		s.factoryFailed = true
	}
	return &historyIndexSnapshot{KeyValueSnapshot: snapshot, owner: s}, nil
}

func (s *historyIndexSnapshotStore) NewIterator(prefix, start []byte) ethdb.Iterator {
	if s.factoryFailed {
		s.itersAfterFailure++
	}
	return s.KeyValueStore.NewIterator(prefix, start)
}

func TestStateHistoryIndexStageDirectSnapshotScopeAndFailures(t *testing.T) {
	failure := errors.New("history index snapshot failure")
	for _, mode := range []string{"success", "interrupted", "factory", "unsupported", "joined", "nil", "unpinned", "cleanup", "close"} {
		t.Run(mode, func(t *testing.T) {
			open := func(name string) *historyIndexSnapshotStore {
				db, err := rawdb.NewPebbleDB(filepath.Join(t.TempDir(), name), 16, 16)
				if err != nil {
					t.Fatal(err)
				}
				return &historyIndexSnapshotStore{KeyValueStore: db}
			}
			hot, history := open("hot"), open("history")
			disk := rawdb.NewDirectHistoryStore(hot, history)
			t.Cleanup(func() { _ = disk.Close() })
			cfg := cloneMainnetChainConfig()
			cfg.HistoryEnabled = true
			genesis := &params.Genesis{Config: cfg, Accounts: []params.GenesisAccount{{Address: testInsertAddr(1), Balance: 99_000_000_000_000_000}}, DynamicProperties: map[string]int64{"next_maintenance_time": 1<<62 - 1}}
			_, hash, err := SetupGenesisBlock(disk, genesis)
			if err != nil {
				t.Fatal(err)
			}
			bc, err := NewBlockChain(disk, state.NewDatabase(rawdb.WrapKeyValueStore(disk)), cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = bc.Close() })
			block := buildTransferBlock(t, 1, 3000, hash, testInsertAddr(1), 5_000_000)
			if err := bc.InsertSyncBlocksWithStageHook([]*types.Block{block}, nil); err != nil {
				t.Fatal(err)
			}
			dp := bc.cachedDynProps()
			dp.SetLatestSolidifiedBlockNum(1)
			bc.storeDynPropsCache(dp)
			hot.created, hot.closed, history.created, history.closed = 0, 0, 0, 0
			switch mode {
			case "factory":
				history.createErr = failure
			case "unsupported":
				history.createErr = pointread.ErrKeyValueSnapshotUnsupported
			case "joined":
				history.createErr = errors.Join(pointread.ErrKeyValueSnapshotUnsupported, failure)
			case "nil":
				history.nilSnapshot = true
			case "unpinned":
				history.unpinnedSnapshot = true
			case "cleanup":
				history.unpinnedSnapshot = true
				history.closeErr = failure
			case "close":
				history.closeErr = failure
			}
			result, err := bc.AdvanceStateHistoryIndexStageInterruptible(1, func() bool { return mode == "interrupted" })
			if mode == "success" {
				if err != nil || !result.Advanced {
					t.Fatalf("result=%+v err=%v", result, err)
				}
			} else {
				want := failure
				if mode == "interrupted" {
					want = rawdb.ErrStateHistoryIndexRebuildInterrupted
				} else if mode == "unsupported" {
					want = pointread.ErrKeyValueSnapshotUnsupported
				}
				if err == nil || (mode != "nil" && mode != "unpinned" && !errors.Is(err, want)) || result.Advanced {
					t.Fatalf("result=%+v err=%v", result, err)
				}
			}
			if hot.created != 0 || hot.closed != 0 || history.created != 1 {
				t.Fatalf("snapshot lifecycle hot=%d/%d history=%d/%d", hot.created, hot.closed, history.created, history.closed)
			}
			wantClosed := 1
			if mode == "factory" || mode == "unsupported" || mode == "joined" || mode == "nil" {
				wantClosed = 0
			}
			if history.closed != wantClosed {
				t.Fatalf("history closed %d want%d", history.closed, wantClosed)
			}
			if history.itersAfterFailure != 0 {
				t.Fatal("partition snapshot failure fell back to a moving history scan")
			}
			row, ok, e := rawdb.ReadStageProgressRow(disk, rawdb.StageStateHistoryIndex)
			wantBlock := uint64(0)
			if mode == "success" {
				wantBlock = 1
			}
			if e != nil || !ok || row.BlockNum != wantBlock {
				t.Fatalf("progress=%+v present=%v err=%v", row, ok, e)
			}
		})
	}
}
