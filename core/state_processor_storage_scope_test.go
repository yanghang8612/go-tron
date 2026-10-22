package core

import (
	"bytes"
	"errors"
	"math"
	"strings"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/metrics"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

type storageScopeFailStore struct {
	ethdb.KeyValueStore
	failure error
}

func (d *storageScopeFailStore) NewKeyValueSnapshot() (pointread.KeyValueSnapshot, error) {
	s, err := d.KeyValueStore.(pointread.KeyValueSnapshotter).NewKeyValueSnapshot()
	if err != nil {
		return nil, err
	}
	return &storageScopeFailSnapshot{KeyValueSnapshot: s, failure: d.failure}, nil
}

type storageScopeFailSnapshot struct {
	pointread.KeyValueSnapshot
	failure error
}

func (s *storageScopeFailSnapshot) NewPointReadCursor() (pointread.Cursor, error) {
	c, err := s.KeyValueSnapshot.(pointread.KeyValueCursorSnapshot).NewPointReadCursor()
	if err != nil {
		return nil, err
	}
	return &storageScopeFailCloseCursor{Cursor: c, failure: s.failure}, nil
}

type storageScopeFailCloseCursor struct {
	pointread.Cursor
	failure error
}

func (c *storageScopeFailCloseCursor) Close() error { return errors.Join(c.Cursor.Close(), c.failure) }

func TestProcessBlockCanonicalStorageScopeOracleAndCloseRollback(t *testing.T) {
	var wantInfo *corepb.TransactionInfo
	var wantRoot tcommon.Hash
	for _, mode := range []string{"live", "scope", "close-error"} {
		t.Run(mode, func(t *testing.T) {
			disk, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
			if err != nil {
				t.Fatal(err)
			}
			defer disk.Close()
			history, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
			if err != nil {
				t.Fatal(err)
			}
			defer history.Close()
			db := rawdb.NewDirectHistoryStore(disk, history)
			database := state.NewDatabase(rawdb.WrapKeyValueStore(db))
			s, err := state.New(tcommon.Hash{}, database)
			if err != nil {
				t.Fatal(err)
			}
			owner, contract := testProcessorAddr(1), testProcessorAddr(0xed)
			s.DynamicProperties().SetAllowCreationOfContracts(true)
			s.DynamicProperties().SetAllowAdaptiveEnergy(true)
			s.DynamicProperties().SetAllowBlackHoleOptimization(true)
			s.DynamicProperties().SetLatestBlockHeaderTimestamp(30_000)
			s.CreateAccount(owner, corepb.AccountType_Normal)
			s.AddBalance(owner, 100_000_000)
			s.CreateAccount(params.BlackholeAddress, corepb.AccountType_Normal)
			s.CreateAccount(contract, corepb.AccountType_Contract)
			s.SetContract(contract, &contractpb.SmartContract{OriginAddress: owner.Bytes(), ContractAddress: contract.Bytes()})
			var code []byte
			for i := byte(0); i < 8; i++ {
				code = append(code, 0x60, i, 0x54, 0x60, i*32, 0x52) // SLOAD -> memory
				if i%2 == 0 {
					s.SetState(contract, tcommon.Hash{31: i}, tcommon.Hash{31: i + 1})
				}
			}
			code = append(code,
				0x61, 0x01, 0x00, 0x60, 0x00, 0xa0, // LOG0 of all loaded values
				0x60, 0x00, 0x54, 0x60, 0x01, 0x01, 0x60, 0x00, 0x55, // SSTORE(slot0+1)
				0x61, 0x01, 0x00, 0x60, 0x00, 0xf3) // RETURN all loaded values
			s.SetCode(contract, code)
			root, err := s.Commit()
			if err != nil {
				t.Fatal(err)
			}
			if err := disk.Compact(nil, nil); err != nil {
				t.Fatal(err)
			}
			dynProps := s.DynamicProperties().Copy()
			s, err = state.New(root, database)
			if err != nil {
				t.Fatal(err)
			}
			s.SetDynamicProperties(dynProps)
			var source ethdb.KeyValueStore = db
			failure := errors.New("injected storage cursor close")
			if mode == "close-error" {
				source = &storageScopeFailStore{KeyValueStore: db, failure: failure}
			}
			buffer := blockbuffer.New(source)
			s.SetAccountKVIndexStore(buffer)
			commits := s.NewCommitScope()
			defer commits.Discard()
			buffer.BeginBlock(tcommon.Hash{1}, 1)
			tx := makeTestTriggerTx(1, contract, nil)
			tx.Proto().RawData.FeeLimit = 10_000_000
			tx.Proto().Ret = []*corepb.Transaction_Result{{ContractRet: corepb.Transaction_Result_SUCCESS}}
			block := types.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 1, Timestamp: 33_000}}, Transactions: []*corepb.Transaction{tx.Proto()}})
			counter := metrics.GetOrRegisterCounter("blockbuffer/storage_read/durable_calls", nil)
			opens := metrics.GetOrRegisterCounter("blockbuffer/storage_read/pebble/get_cursor/table_iterator_opens", nil)
			beforeOpens := opens.Snapshot().Count()
			scopes := metrics.GetOrRegisterCounter("blockbuffer/storage_read/scopes", nil)
			beforeScopes := scopes.Snapshot().Count()
			beforeCalls, beforeBalance := counter.Snapshot().Count(), s.GetBalance(owner)
			infos, _, err := processBlockWithOptions(s, s.DynamicProperties(), block, buffer, nil, 0, params.DefaultBlockNumForEnergyLimit, false, tcommon.Hash{}, nil, nil, nil, forks.NewVersionPassCache(), new(transactionInfoBatch), true, -1, nil, processBlockOptions{canonicalStorageReadScope: mode != "live"})
			wantScopes := int64(1)
			if mode == "live" {
				wantScopes = 0
			}
			if got := scopes.Snapshot().Count() - beforeScopes; got != wantScopes {
				t.Fatalf("nonempty scope activation: got %d want %d", got, wantScopes)
			}
			if mode == "close-error" {
				if !errors.Is(err, failure) {
					t.Fatalf("close error %v", err)
				}
				if balance := s.GetBalance(owner); balance != beforeBalance {
					t.Fatalf("close did not rollback balance: %d/%d", balance, beforeBalance)
				}
				if got := s.GetState(contract, tcommon.Hash{}); got != (tcommon.Hash{31: 1}) {
					t.Fatalf("close did not rollback storage %x", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(infos) != 1 || infos[0].GetResult() != corepb.TransactionInfo_SUCESS {
				t.Fatalf("receipt %v", infos)
			}
			if mode == "scope" && counter.Snapshot().Count()-beforeCalls != 8 {
				t.Fatalf("scope not exercised: calls=%d", counter.Snapshot().Count()-beforeCalls)
			}
			if mode == "scope" && opens.Snapshot().Count() <= beforeOpens {
				t.Fatal("canonical cursor never opened SST iterator")
			}
			root, _, err = s.CommitWithStatsOptionsInScope(commits, state.CommitOptions{BlockNumber: 1})
			if err != nil {
				t.Fatal(err)
			}
			if mode == "live" {
				wantInfo, wantRoot = infos[0], root
			} else if !proto.Equal(wantInfo, infos[0]) || root != wantRoot {
				t.Fatalf("full block mismatch roots %x/%x\n%v\n%v", root, wantRoot, infos[0], wantInfo)
			}
		})
	}
}

// Empty blocks still read and write ContractStorage outside the TVM. Compare
// the original no-scope reader with the enabled canonical option, with a real
// durable prior-cycle ring value, an empty decoded-storage cache, and a failure
// after the ring write. The fixed prior/new values are independent of either
// reader; the resulting commitment must also match the original path.
func TestProcessBlockCanonicalStorageScopeEmptyRingWrapAndRollback(t *testing.T) {
	priorHash := tcommon.HexToHash("cafebabecafebabecafebabecafebabecafebabecafebabecafebabecafebabe")
	parentHash := tcommon.HexToHash("deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
	slot := uint64ToDataWord(0)
	blockNum := historyServeWindow + 1
	for _, fail := range []bool{false, true} {
		for _, explicitEmpty := range []bool{false, true} {
			name := "nil-transactions"
			if explicitEmpty {
				name = "empty-transactions"
			}
			if fail {
				name += "/adaptive-failure"
			}
			t.Run(name, func(t *testing.T) {
				var referenceRoot tcommon.Hash
				for _, enabled := range []bool{false, true} {
					mode := "original-live"
					if enabled {
						mode = "canonical-enabled"
					}
					t.Run(mode, func(t *testing.T) {
						disk, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
						if err != nil {
							t.Fatal(err)
						}
						defer disk.Close()
						history, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
						if err != nil {
							t.Fatal(err)
						}
						defer history.Close()
						db := rawdb.NewDirectHistoryStore(disk, history)
						database := state.NewDatabase(rawdb.WrapKeyValueStore(db))
						seed, err := state.New(tcommon.Hash{}, database)
						if err != nil {
							t.Fatal(err)
						}
						deployHistoryBlockHash(seed, seed.DynamicProperties())
						seed.SetState(historyStorageAddress, slot, priorHash)
						root, err := seed.Commit()
						if err != nil {
							t.Fatal(err)
						}
						dp := seed.DynamicProperties().Copy()
						dp.SetAllowAdaptiveEnergy(fail)
						dp.SetLatestBlockHeaderTimestamp(30_000)
						dp.SetBlockEnergyUsage(123)
						if fail {
							dp.SetAllowHardenResourceCalculation(true)
							dp.SetTotalEnergyAverageUsage(0)
							dp.SetTotalEnergyTargetLimit(math.MaxInt64)
							dp.SetTotalEnergyCurrentLimit(10_000_000_000_000_000)
							dp.SetTotalEnergyLimit(10_000_000_000_000_000)
							dp.SetAdaptiveResourceLimitMultiplier(1000)
						}
						// Reopen after committing the nonzero slot. Do not preload it:
						// writeHistoryBlockHash itself must hydrate the old pre-image.
						s, err := state.New(root, database)
						if err != nil {
							t.Fatal(err)
						}
						s.SetDynamicProperties(dp)
						buffer := blockbuffer.New(db)
						s.SetAccountKVIndexStore(buffer)
						buffer.BeginBlock(tcommon.Hash{1}, blockNum)
						commits := s.NewCommitScope()
						defer commits.Discard()
						changes := ethrawdb.NewMemoryDatabase()
						defer changes.Close()
						begin, end, err := rawdb.NextStateTxRange(blockNum-1, 0)
						if err != nil {
							t.Fatal(err)
						}
						s.BeginDomainChangeJournalCapture(changes, blockNum, tcommon.Hash{0xab}, begin, end)
						mark := s.DomainChangeJournalMark()
						pb := &corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: int64(blockNum), Timestamp: 33_000, ParentHash: parentHash.Bytes()}}}
						if explicitEmpty {
							pb.Transactions = []*corepb.Transaction{}
						}
						scopes := metrics.GetOrRegisterCounter("blockbuffer/storage_read/scopes", nil)
						durable := metrics.GetOrRegisterCounter("blockbuffer/storage_read/durable_calls", nil)
						beforeScopes, beforeDurable := scopes.Snapshot().Count(), durable.Snapshot().Count()
						infos, _, err := processBlockWithOptions(s, dp, types.NewBlockFromPB(pb), buffer, nil, 0, params.DefaultBlockNumForEnergyLimit, false, tcommon.Hash{}, nil, nil, nil, forks.NewVersionPassCache(), new(transactionInfoBatch), true, -1, nil, processBlockOptions{canonicalStorageReadScope: enabled})
						if got := scopes.Snapshot().Count() - beforeScopes; got != 0 {
							t.Fatalf("empty block opened %d scopes", got)
						}
						if got := durable.Snapshot().Count() - beforeDurable; got != 0 {
							t.Fatalf("empty block used cursor %d times", got)
						}
						wantSlot := parentHash
						if fail {
							if err == nil || !strings.Contains(err.Error(), "adaptive total energy") {
								t.Fatalf("expected post-ring adaptive failure: %v", err)
							}
							wantSlot = priorHash
							if dp.BlockEnergyUsage() != 123 {
								t.Fatalf("dynamic snapshot not reverted: %d", dp.BlockEnergyUsage())
							}
						} else if err != nil || len(infos) != 0 {
							t.Fatalf("empty block result %v/%v", infos, err)
						}
						if got := s.GetState(historyStorageAddress, slot); got != wantSlot {
							t.Fatalf("ring slot got %x want %x", got, wantSlot)
						}
						if err := s.FlushDomainChangesSince(mark, end); err != nil {
							t.Fatal(err)
						}
						var slotChanges int
						if err := rawdb.IterateStateDomainChanges(changes, blockNum, func(change *rawdb.StateDomainChange) (bool, error) {
							if change.Owner == historyStorageAddress && change.Domain == kvdomains.ContractStorage {
								slotChanges++
								if !change.PrevExists || !bytes.Equal(change.Prev, priorHash.Bytes()) {
									t.Errorf("ring prior-cycle pre-image %x/%v, want %x", change.Prev, change.PrevExists, priorHash)
								}
							}
							return true, nil
						}); err != nil {
							t.Fatal(err)
						}
						wantChanges := 1
						if fail {
							wantChanges = 0
						}
						if slotChanges != wantChanges {
							t.Fatalf("ring changes %d want %d", slotChanges, wantChanges)
						}
						gotRoot, _, err := s.CommitWithStatsOptionsInScope(commits, state.CommitOptions{BlockNumber: blockNum})
						if err != nil {
							t.Fatal(err)
						}
						if !enabled {
							referenceRoot = gotRoot
						} else if gotRoot != referenceRoot {
							t.Fatalf("empty block root %x differs from original live %x", gotRoot, referenceRoot)
						}
					})
				}
			})
		}
	}
}
