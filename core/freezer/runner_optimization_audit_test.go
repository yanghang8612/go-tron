package freezer

import (
	"context"
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	coretypes "github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// auditAncientBodyStore changes only the body presented to the maintenance
// reader. This lets malformed historical bytes reach the exact production
// iterator without first passing through a canonicalizing test builder.
type auditAncientBodyStore struct {
	FreezerStore
	body []byte
}

func (s *auditAncientBodyStore) Ancient(kind string, number uint64) ([]byte, error) {
	if kind == rawdbAncientBlocks {
		return s.body, nil
	}
	return s.FreezerStore.Ancient(kind, number)
}

func auditBytesField(number protowire.Number, value []byte) []byte {
	return protowire.AppendBytes(protowire.AppendTag(nil, number, protowire.BytesType), value)
}

func auditVarintField(number protowire.Number, value uint64) []byte {
	return protowire.AppendVarint(protowire.AppendTag(nil, number, protowire.VarintType), value)
}

func TestTransactionIndexOptimizationMatchesAuthoritativeBlockDecode(t *testing.T) {
	canonicalRaw := append(auditVarintField(8, 1), auditVarintField(14, 2)...)
	canonicalTx := auditBytesField(1, canonicalRaw)
	canonicalBlock := auditBytesField(1, canonicalTx)
	prePQHeader := auditBytesField(2, auditBytesField(1, auditVarintField(10, 36)))
	postPQHeader := auditBytesField(2, auditBytesField(1, auditVarintField(10, 37)))
	legacyTx := append(append([]byte(nil), canonicalTx...), auditBytesField(6, []byte{0xff})...)
	cases := map[string][]byte{
		"canonical": canonicalBlock,
		"raw_fields_out_of_order": auditBytesField(1, auditBytesField(1,
			append(auditVarintField(14, 2), auditVarintField(8, 1)...))),
		"duplicate_raw_data_merge": auditBytesField(1, append(
			auditBytesField(1, auditVarintField(8, 1)), auditBytesField(1, auditVarintField(14, 2))...)),
		"duplicate_raw_scalar_last_wins": auditBytesField(1, auditBytesField(1,
			append(auditVarintField(14, 2), auditVarintField(14, 3)...))),
		"non_minimal_raw_varint": auditBytesField(1, auditBytesField(1, []byte{0x70, 0x82, 0x00})),
		"non_minimal_raw_tag":    auditBytesField(1, auditBytesField(1, []byte{0xf0, 0x00, 0x02})),
		"explicit_default_scalar": auditBytesField(1, auditBytesField(1,
			append(auditVarintField(8, 0), auditVarintField(14, 2)...))),
		"unknown_raw_field": auditBytesField(1, auditBytesField(1,
			append(auditVarintField(63, 3), canonicalRaw...))),
		"known_field_wrong_wire_type": auditBytesField(1, auditBytesField(1,
			append(auditBytesField(14, []byte{2}), canonicalRaw...))),
		"missing_raw_data_zero_hash":   auditBytesField(1, nil),
		"present_empty_raw_data_hash":  auditBytesField(1, auditBytesField(1, nil)),
		"legacy_pre_pq":                append(append([]byte(nil), prePQHeader...), auditBytesField(1, legacyTx)...),
		"post_pq_malformed_nested":     append(append([]byte(nil), postPQHeader...), auditBytesField(1, legacyTx)...),
		"malformed_transaction_suffix": auditBytesField(1, append(append([]byte(nil), canonicalTx...), 0xff)),
		"malformed_raw_suffix": auditBytesField(1, auditBytesField(1,
			append(append([]byte(nil), canonicalRaw...), 0xff))),
		"malformed_result_after_raw": auditBytesField(1, append(
			append([]byte(nil), canonicalTx...), auditBytesField(5, []byte{0xff})...)),
		"malformed_header_after_transaction": append(append([]byte(nil), canonicalBlock...), auditBytesField(2, []byte{0xff})...),
		"malformed_second_transaction":       append(append([]byte(nil), canonicalBlock...), auditBytesField(1, []byte{0xff})...),
		"malformed_block_suffix":             append(append([]byte(nil), canonicalBlock...), 0xff),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			store := &auditAncientBodyStore{FreezerStore: wrapFreezer(newFreezer(t)), body: body}
			r := New(newFakeChain(), store, Config{Enabled: true})
			want, decodeErr := coretypes.UnmarshalBlockBorrowed(body)
			var entries []rawdbfreezer.TransactionIndexEntry
			rows, err := r.iterateTransactionIndexEntriesContext(context.Background(), 7, 8, func(entry rawdbfreezer.TransactionIndexEntry) error {
				entries = append(entries, entry)
				return nil
			})
			if decodeErr != nil {
				if err == nil || rows != 0 || len(entries) != 0 {
					t.Fatalf("malformed full block yielded rows=%d entries=%d err=%v; authoritative error=%v", rows, len(entries), err, decodeErr)
				}
				return
			}
			transactions := want.Transactions()
			if err != nil || rows != uint64(len(transactions)) || len(entries) != len(transactions) {
				t.Fatalf("rows=%d entries=%d err=%v, want %d", rows, len(entries), err, len(transactions))
			}
			for ordinal, tx := range transactions {
				location, err := rawdb.EncodeTransactionLocation(7, ordinal)
				if err != nil {
					t.Fatal(err)
				}
				if entries[ordinal].Hash != tx.Hash() || entries[ordinal].Location != location {
					t.Fatalf("entry %d = %+v, want hash=%x location=%d", ordinal, entries[ordinal], tx.Hash(), location)
				}
			}
		})
	}
}

func TestTransactionIndexOptimizationPropagatesYieldFailure(t *testing.T) {
	body := append(auditBytesField(1, auditBytesField(1, auditVarintField(14, 1))),
		auditBytesField(1, auditBytesField(1, auditVarintField(14, 2)))...)
	store := &auditAncientBodyStore{FreezerStore: wrapFreezer(newFreezer(t)), body: body}
	r := New(newFakeChain(), store, Config{Enabled: true})
	injected := errors.New("audit collector failure")
	calls := 0
	rows, err := r.iterateTransactionIndexEntriesContext(context.Background(), 0, 1, func(rawdbfreezer.TransactionIndexEntry) error {
		calls++
		return injected
	})
	if !errors.Is(err, injected) || rows != 0 || calls != 1 {
		t.Fatalf("yield failure rows=%d calls=%d err=%v", rows, calls, err)
	}
}

// auditPruneBatchDB forces one deletion per batch, exercising resumability
// without creating hundreds of thousands of fixture transactions just to fill
// the production 16 MiB batch budget.
type auditPruneBatchDB struct {
	ethdb.KeyValueStore
	writes  int
	failAt  int
	failErr error
	after   func(int)
}

type auditPruneBatch struct {
	ethdb.Batch
	db *auditPruneBatchDB
}

func (db *auditPruneBatchDB) NewBatchWithSize(size int) ethdb.Batch {
	return &auditPruneBatch{Batch: db.KeyValueStore.NewBatchWithSize(size), db: db}
}

func (b *auditPruneBatch) ValueSize() int {
	if b.Batch.ValueSize() > 0 {
		return txIndexDeleteBatchBytes
	}
	return 0
}

func (b *auditPruneBatch) Write() error {
	b.db.writes++
	if b.db.writes == b.db.failAt {
		return b.db.failErr
	}
	if err := b.Batch.Write(); err != nil {
		return err
	}
	if b.db.after != nil {
		b.db.after(b.db.writes)
	}
	return nil
}

func newAuditPublishedTransactionIndex(t *testing.T) (*Runner, *fakeChain, *rawdbfreezer.Freezer, []common.Hash) {
	t.Helper()
	chain := newFakeChain()
	t.Cleanup(func() { _ = chain.db.Close() })
	store := newFreezer(t)
	hashes := make([]common.Hash, 4)
	for number := uint64(0); number < 4; number++ {
		block := coretypes.NewBlockFromPB(&corepb.Block{
			BlockHeader:  &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: int64(number), Timestamp: int64(number) * 3000}},
			Transactions: []*corepb.Transaction{{RawData: &corepb.TransactionRaw{Timestamp: int64(number + 1)}}},
		})
		body, err := block.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		hashes[number] = block.Transactions()[0].Hash()
		ret, err := proto.Marshal(&corepb.TransactionRet{Transactioninfo: []*corepb.TransactionInfo{{Id: hashes[number][:]}}})
		if err != nil {
			t.Fatal(err)
		}
		chain.blockRaw[number], chain.txInfosRaw[number] = body, ret
		chain.stateRootRaw[number] = stateRootBytes(number)
		chain.blockHashByNo[number] = block.Hash()
		if err := rawdb.WriteBlock(chain.db, block); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteTransactionLocation(chain.db, hashes[number][:], number, 0); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteBlockStateRoot(chain.db, block.Hash(), common.BytesToHash(stateRootBytes(number))); err != nil {
			t.Fatal(err)
		}
		if err := writeTxInfosKV(chain.db, number, ret); err != nil {
			t.Fatal(err)
		}
	}
	chain.setSolidified(3)
	r := New(chain, wrapFreezer(store), Config{
		Enabled: true, V2Enabled: true, DirectV2: true,
		V2FrameBlocks: 2, V2SegmentBlocks: 4, TransactionIndexPrefixBits: 8,
	})
	if frozen, err := r.OnePass(); err != nil || frozen != 4 {
		t.Fatalf("freeze fixture = %d/%v", frozen, err)
	}
	r.cfg.TransactionIndexEnabled = true
	if changed, err := r.ensureTransactionIndexCoverageContext(context.Background(), 4); err != nil || !changed {
		t.Fatalf("publish fixture index = %t/%v", changed, err)
	}
	return r, chain, store, hashes
}

func TestTransactionIndexPruneOptimizationResumesAfterPartialBatchFailure(t *testing.T) {
	for _, mode := range []string{"batch_write_error", "context_canceled"} {
		t.Run(mode, func(t *testing.T) {
			r, chain, store, hashes := newAuditPublishedTransactionIndex(t)
			injected := errors.New("audit prune batch write failure")
			db := &auditPruneBatchDB{KeyValueStore: chain.db, failErr: injected}
			chain.db = db
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			wantErr := injected
			if mode == "batch_write_error" {
				db.failAt = 2
			} else {
				wantErr = context.Canceled
				db.after = func(int) { cancel() }
			}
			if changed, err := r.pruneTransactionIndexDebtContext(ctx, 4); changed || !errors.Is(err, wantErr) {
				t.Fatalf("interrupted prune = %t/%v, want false/%v", changed, err, wantErr)
			}
			if progress, ok, err := rawdb.ReadStageProgress(db, rawdb.StageFreezerTxIndexPrune); err != nil || (ok && progress != 0) {
				t.Fatalf("interrupted prune advanced cursor = %d/%t/%v", progress, ok, err)
			}
			// At least one real delete reached the KV store. Both that cold-only
			// lookup and the remaining hot lookups must work before retry.
			hotDB := rawdb.NewChainDB(db, nil)
			if got := rawdb.ReadTransactionIndex(hotDB, hashes[0][:]); got != nil {
				t.Fatalf("first hot row was not deleted before interruption: %v", got)
			}
			assertQueries := func(phase string) {
				t.Helper()
				chainDB := rawdb.NewChainDB(db, rawdb.NewFreezerReader(store))
				for number, hash := range hashes {
					got, ok, err := rawdb.ReadTransactionIndexStrict(chainDB, hash[:])
					if err != nil || !ok || got != uint64(number) {
						t.Fatalf("%s query %d = %d/%t/%v", phase, number, got, ok, err)
					}
				}
			}
			assertQueries("interrupted")
			db.failAt, db.after = 0, nil
			if changed, err := r.pruneTransactionIndexDebtContext(context.Background(), 4); err != nil || !changed {
				t.Fatalf("retry prune = %t/%v", changed, err)
			}
			if progress, ok, err := rawdb.ReadStageProgress(db, rawdb.StageFreezerTxIndexPrune); err != nil || !ok || progress != 4 {
				t.Fatalf("completed prune cursor = %d/%t/%v", progress, ok, err)
			}
			for number, hash := range hashes {
				if got := rawdb.ReadTransactionIndex(hotDB, hash[:]); got != nil {
					t.Fatalf("retry left hot row %d: %v", number, got)
				}
			}
			assertQueries("resumed")
		})
	}
}

func TestTransactionIndexPruneOptimizationHonorsEligibleCoverage(t *testing.T) {
	r, chain, store, hashes := newAuditPublishedTransactionIndex(t)
	if changed, err := r.pruneTransactionIndexDebtContext(context.Background(), 2); err != nil || !changed {
		t.Fatalf("bounded prune = %t/%v", changed, err)
	}
	if progress, ok, err := rawdb.ReadStageProgress(chain.db, rawdb.StageFreezerTxIndexPrune); err != nil || !ok || progress != 2 {
		t.Fatalf("bounded cursor = %d/%t/%v, want 2", progress, ok, err)
	}
	hotDB := rawdb.NewChainDB(chain.db, nil)
	chainDB := rawdb.NewChainDB(chain.db, rawdb.NewFreezerReader(store))
	for number, hash := range hashes {
		got := rawdb.ReadTransactionIndex(hotDB, hash[:])
		if (number < 2 && got != nil) || (number >= 2 && (got == nil || *got != uint64(number))) {
			t.Fatalf("hot row %d = %v after pruning only [0,2)", number, got)
		}
		resolved, ok, err := rawdb.ReadTransactionIndexStrict(chainDB, hash[:])
		if err != nil || !ok || resolved != uint64(number) {
			t.Fatalf("bounded query %d = %d/%t/%v", number, resolved, ok, err)
		}
	}
	if changed, err := r.pruneTransactionIndexDebtContext(context.Background(), 1); err == nil || changed {
		t.Fatalf("regressed eligible coverage accepted: %t/%v", changed, err)
	}
	if changed, err := r.pruneTransactionIndexDebtContext(context.Background(), 4); err != nil || !changed {
		t.Fatalf("remaining range prune = %t/%v", changed, err)
	}
}
