package freezer

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

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

// auditTransactionIndexMaintenanceStore exposes a large immutable coverage
// while serving one canonical empty body for every block. It keeps the test
// focused on scheduler admission and durable quantum boundaries instead of
// spending time constructing thousands of unrelated V2 fixtures.
type auditTransactionIndexMaintenanceStore struct {
	FreezerStore
	body       []byte
	bodyFor    func(uint64) []byte
	coverage   uint64
	v2Coverage uint64
	dir        string
	published  atomic.Bool
	merge      bool
	mergeCalls atomic.Uint64
	mergeBlock bool
	mergeStart chan struct{}
	mergeOnce  atomic.Bool
}

func (s *auditTransactionIndexMaintenanceStore) Ancient(kind string, number uint64) ([]byte, error) {
	if kind == rawdbAncientBlocks && number < s.v2Coverage {
		if s.bodyFor != nil {
			return s.bodyFor(number), nil
		}
		return s.body, nil
	}
	return s.FreezerStore.Ancient(kind, number)
}

func (s *auditTransactionIndexMaintenanceStore) AncientDatadir() (string, error) {
	return s.dir, nil
}

func (s *auditTransactionIndexMaintenanceStore) TransactionIndexCoverage() uint64 {
	return s.coverage
}

func (s *auditTransactionIndexMaintenanceStore) PublishTransactionIndexRun(result rawdbfreezer.TransactionIndexBuildResult) error {
	s.coverage = result.EndBlock
	s.published.Store(true)
	return nil
}

func (s *auditTransactionIndexMaintenanceStore) CompactTransactionIndexTail() (bool, error) {
	s.mergeCalls.Add(1)
	return s.merge, nil
}

func (s *auditTransactionIndexMaintenanceStore) CompactTransactionIndexTailContext(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if s.mergeBlock {
		s.mergeCalls.Add(1)
		if s.mergeOnce.CompareAndSwap(false, true) {
			close(s.mergeStart)
		}
		<-ctx.Done()
		return false, ctx.Err()
	}
	return s.CompactTransactionIndexTail()
}

func (s *auditTransactionIndexMaintenanceStore) V2Coverage() uint64 {
	return s.v2Coverage
}

func (s *auditTransactionIndexMaintenanceStore) MigrateV2(rawdbfreezer.V2MigrationOptions) (rawdbfreezer.V2MigrationResult, error) {
	return rawdbfreezer.V2MigrationResult{}, nil
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
	before  func(int)
	after   func(int)
	sync    func() error
}

type auditPruneBatch struct {
	ethdb.Batch
	db *auditPruneBatchDB
}

type auditSyncDelayDB struct {
	ethdb.KeyValueStore
	delay time.Duration
}

func (db *auditSyncDelayDB) SyncKeyValue() error {
	time.Sleep(db.delay)
	return nil
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
	if b.db.before != nil {
		b.db.before(b.db.writes)
	}
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

func (db *auditPruneBatchDB) SyncKeyValue() error {
	if db.sync != nil {
		return db.sync()
	}
	if syncer, ok := db.KeyValueStore.(interface{ SyncKeyValue() error }); ok {
		return syncer.SyncKeyValue()
	}
	return nil
}

func auditTransactionBlockBytes(number uint64) []byte {
	block := coretypes.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number: int64(number),
		}},
		Transactions: []*corepb.Transaction{{RawData: &corepb.TransactionRaw{
			Timestamp: int64(number + 1),
		}}},
	})
	body, err := block.Marshal()
	if err != nil {
		panic(err)
	}
	return body
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

func TestTransactionIndexPruneDurationIncludesDurableStageSync(t *testing.T) {
	r, chain, _, _ := newAuditPublishedTransactionIndex(t)
	const syncDelay = 20 * time.Millisecond
	chain.db = &auditSyncDelayDB{KeyValueStore: chain.db, delay: syncDelay}
	if changed, err := r.pruneTransactionIndexDebtContext(context.Background(), 4); err != nil || !changed {
		t.Fatalf("prune changed=%v err=%v", changed, err)
	}
	if elapsed := r.Snapshot().TransactionIndexPruneDuration; elapsed < syncDelay {
		t.Fatalf("prune duration=%s, want at least durable sync delay %s", elapsed, syncDelay)
	}
}

func TestTransactionIndexActiveMaintenanceBuildsAndPrunesUnderOneLease(t *testing.T) {
	chain := newFakeChain()
	store := &auditTransactionIndexMaintenanceStore{
		FreezerStore: wrapFreezer(newFreezer(t)),
		bodyFor:      auditTransactionBlockBytes,
		v2Coverage:   4,
		dir:          t.TempDir(),
	}
	db := &auditPruneBatchDB{KeyValueStore: chain.db}
	chain.db = db
	db.before = func(write int) {
		if !store.published.Load() || store.coverage != 4 {
			t.Fatalf("delete batch %d ran before immutable publication: published=%t coverage=%d", write, store.published.Load(), store.coverage)
		}
	}
	db.sync = func() error {
		progress, ok, err := rawdb.ReadStageProgress(db, rawdb.StageFreezerTxIndexPrune)
		if err != nil || !ok || progress != 4 {
			t.Fatalf("durability sync preceded prune stage: progress=%d ok=%t err=%v", progress, ok, err)
		}
		return nil
	}
	var syncing atomic.Bool
	syncing.Store(true)
	namespace := "test/tx-index-active-build-prune/"
	unregisterRunnerMetricNamespace(namespace)
	t.Cleanup(func() { unregisterRunnerMetricNamespace(namespace) })
	r := New(chain, store, Config{
		Enabled:                    true,
		V2Enabled:                  true,
		V2SegmentBlocks:            4,
		TransactionIndexEnabled:    true,
		TransactionIndexPrefixBits: 8,
		SyncActive:                 syncing.Load,
		MetricsNamespace:           namespace,
	})
	if changed, err := r.MaintainTransactionIndexOnce(); err != nil || !changed {
		t.Fatalf("active build+prune changed=%t err=%v", changed, err)
	}
	progress, ok, err := rawdb.ReadStageProgress(db, rawdb.StageFreezerTxIndexPrune)
	if err != nil || !ok || progress != 4 {
		t.Fatalf("active build+prune progress=%d ok=%t err=%v", progress, ok, err)
	}
	stats := r.Snapshot()
	if stats.TransactionIndexCoverage != 4 || stats.TransactionIndexPruned != 4 || stats.TransactionIndexDebtBlocks != 0 ||
		stats.TransactionIndexRowsArchived != 4 || stats.TransactionIndexRowsPruned != 4 || stats.TransactionIndexBlocksPruned != 4 ||
		stats.TransactionIndexMaintenanceAdmitted != 1 {
		t.Fatalf("active build+prune stats=%+v", stats)
	}
}

func TestTransactionIndexBuildPruneFailureResumesFromPublishedCoverage(t *testing.T) {
	chain := newFakeChain()
	store := &auditTransactionIndexMaintenanceStore{
		FreezerStore: wrapFreezer(newFreezer(t)),
		bodyFor:      auditTransactionBlockBytes,
		v2Coverage:   4,
		dir:          t.TempDir(),
	}
	injected := errors.New("injected hot-index delete failure")
	db := &auditPruneBatchDB{KeyValueStore: chain.db, failAt: 1, failErr: injected}
	chain.db = db
	db.before = func(int) {
		if !store.published.Load() {
			t.Fatal("hot-index delete ran before immutable publication")
		}
	}
	var syncing atomic.Bool
	syncing.Store(true)
	config := Config{
		Enabled:                    true,
		V2Enabled:                  true,
		V2SegmentBlocks:            4,
		TransactionIndexEnabled:    true,
		TransactionIndexPrefixBits: 8,
		SyncActive:                 syncing.Load,
		MetricsNamespace:           "test/tx-index-build-prune-failure/",
	}
	unregisterRunnerMetricNamespace(config.MetricsNamespace)
	t.Cleanup(func() { unregisterRunnerMetricNamespace(config.MetricsNamespace) })
	r := New(chain, store, config)
	if changed, err := r.MaintainTransactionIndexOnce(); !changed || !errors.Is(err, injected) {
		t.Fatalf("failed build+prune changed=%t err=%v", changed, err)
	}
	if store.coverage != 4 {
		t.Fatalf("immutable coverage=%d after prune failure, want 4", store.coverage)
	}
	if progress, ok, err := rawdb.ReadStageProgress(db, rawdb.StageFreezerTxIndexPrune); err != nil || (ok && progress != 0) {
		t.Fatalf("failed prune advanced stage=%d ok=%t err=%v", progress, ok, err)
	}
	if stats := r.Snapshot(); stats.TransactionIndexErrors != 1 || stats.TransactionIndexDebtBlocks != 4 {
		t.Fatalf("failed build+prune stats=%+v", stats)
	}

	// Restarting observes the published coverage and performs only the
	// idempotent prune; it must not try to republish the immutable run.
	db.failAt = 0
	db.before = nil
	store.published.Store(false)
	r = New(chain, store, config)
	if changed, err := r.MaintainTransactionIndexOnce(); err != nil || !changed {
		t.Fatalf("resumed prune changed=%t err=%v", changed, err)
	}
	if store.published.Load() {
		t.Fatal("resume unexpectedly republished immutable transaction index")
	}
	if progress, ok, err := rawdb.ReadStageProgress(db, rawdb.StageFreezerTxIndexPrune); err != nil || !ok || progress != 4 {
		t.Fatalf("resumed prune stage=%d ok=%t err=%v", progress, ok, err)
	}
}

func TestTransactionIndexBuildPruneCancellationLeavesRetryableDebt(t *testing.T) {
	chain := newFakeChain()
	store := &auditTransactionIndexMaintenanceStore{
		FreezerStore: wrapFreezer(newFreezer(t)),
		bodyFor:      auditTransactionBlockBytes,
		v2Coverage:   4,
		dir:          t.TempDir(),
	}
	db := &auditPruneBatchDB{KeyValueStore: chain.db}
	chain.db = db
	ctx, cancel := context.WithCancel(context.Background())
	db.after = func(int) { cancel() }
	r := New(chain, store, Config{
		Enabled:                    true,
		V2Enabled:                  true,
		V2SegmentBlocks:            4,
		TransactionIndexEnabled:    true,
		TransactionIndexPrefixBits: 8,
	})
	if changed, err := r.maintainTransactionIndexOnceContext(ctx); !changed || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled build+prune changed=%t err=%v", changed, err)
	}
	if store.coverage != 4 {
		t.Fatalf("immutable coverage=%d after cancellation, want 4", store.coverage)
	}
	if progress, ok, err := rawdb.ReadStageProgress(db, rawdb.StageFreezerTxIndexPrune); err != nil || (ok && progress != 0) {
		t.Fatalf("canceled prune advanced stage=%d ok=%t err=%v", progress, ok, err)
	}
	db.after = nil
	if changed, err := r.maintainTransactionIndexOnceContext(context.Background()); err != nil || !changed {
		t.Fatalf("retry canceled prune changed=%t err=%v", changed, err)
	}
	if progress, ok, err := rawdb.ReadStageProgress(db, rawdb.StageFreezerTxIndexPrune); err != nil || !ok || progress != 4 {
		t.Fatalf("retry canceled prune stage=%d ok=%t err=%v", progress, ok, err)
	}
}

func TestCompactV2ScheduledSkipsDirectLayoutWithoutPollutingMetrics(t *testing.T) {
	fz := wrapFreezer(newFreezer(t))
	namespace := "test/direct-v2-scheduled-skip/"
	unregisterRunnerMetricNamespace(namespace)
	t.Cleanup(func() { unregisterRunnerMetricNamespace(namespace) })
	r := New(nil, fz, Config{
		Enabled:                    true,
		V2Enabled:                  true,
		DirectV2:                   true,
		V2FrameBlocks:              2,
		V2SegmentBlocks:            4,
		SyncActive:                 func() bool { return true },
		CatchupMaintenanceInterval: time.Hour,
		MetricsNamespace:           namespace,
	})
	r.v2LastBatchSegments.Store(1)
	r.v2LastBatchDuration.Store(int64(time.Second))
	r.updateMetrics()
	if compacted, err := r.compactV2Scheduled(); err != nil || compacted != 0 {
		t.Fatalf("scheduled direct V2 compaction=%d err=%v", compacted, err)
	}
	stats := r.Snapshot()
	if stats.V2CatchupDeferred != 0 || stats.V2ResourceDeferred != 0 || stats.V2LastBatchSegments != 1 || stats.V2LastBatchDuration != time.Second {
		t.Fatalf("scheduled direct V2 stats=%+v", stats)
	}
	if got := runnerGaugeValue(t, namespace+"v2/batch/segments"); got != 1 {
		t.Fatalf("scheduled direct V2 batch segments metric=%d, want 1", got)
	}
	if got := runnerGaugeValue(t, namespace+"v2/batch/duration"); got != int64(time.Second) {
		t.Fatalf("scheduled direct V2 batch duration metric=%d, want %d", got, time.Second)
	}

	// DirectV2 is also the publication kill switch. Once the data layout is
	// immutable-only, disabling future direct publication must not send the
	// legacy promoter through an otherwise useless active-sync admission.
	r.cfg.DirectV2 = false
	if compacted, err := r.compactV2Scheduled(); err != nil || compacted != 0 {
		t.Fatalf("scheduled direct V2 compaction with kill switch=%d err=%v", compacted, err)
	}
	if admittedAt := r.lastV2CatchupMaintenance.Load(); admittedAt != 0 {
		t.Fatalf("direct-layout kill switch consumed legacy catch-up admission at %d", admittedAt)
	}
}

func TestCompactV2NoopPreservesLastSuccessfulBatchMetrics(t *testing.T) {
	fz := wrapFreezer(newFreezer(t))
	namespace := "test/legacy-v2-noop-metrics/"
	unregisterRunnerMetricNamespace(namespace)
	t.Cleanup(func() { unregisterRunnerMetricNamespace(namespace) })
	r := New(nil, fz, Config{Enabled: true, V2Enabled: true, V2FrameBlocks: 2, V2SegmentBlocks: 4, MetricsNamespace: namespace})
	r.v2LastBatchSegments.Store(3)
	r.v2LastBatchDuration.Store(int64(2 * time.Second))
	r.updateMetrics()
	if compacted, err := r.compactV2OnceContext(context.Background()); err != nil || compacted != 0 {
		t.Fatalf("legacy V2 no-op=%d err=%v", compacted, err)
	}
	stats := r.Snapshot()
	if stats.V2LastBatchSegments != 3 || stats.V2LastBatchDuration != 2*time.Second {
		t.Fatalf("legacy V2 no-op reset successful batch stats=%+v", stats)
	}
	if got := runnerGaugeValue(t, namespace+"v2/batch/segments"); got != 3 {
		t.Fatalf("legacy V2 no-op batch segments metric=%d, want 3", got)
	}
	if got := runnerGaugeValue(t, namespace+"v2/batch/duration"); got != int64(2*time.Second) {
		t.Fatalf("legacy V2 no-op batch duration metric=%d, want %d", got, 2*time.Second)
	}
}

func TestTransactionIndexBusyMaintenanceUsesCrashSafeQuantum(t *testing.T) {
	chain := newFakeChain()
	store := &auditTransactionIndexMaintenanceStore{
		FreezerStore: wrapFreezer(newFreezer(t)),
		body:         blockBytes(0),
		coverage:     2 * transactionIndexMaintenanceBlocks,
		v2Coverage:   2 * transactionIndexMaintenanceBlocks,
		dir:          t.TempDir(),
	}
	var syncing atomic.Bool
	syncing.Store(true)
	namespace := "test/tx-index-busy-quantum/"
	unregisterRunnerMetricNamespace(namespace)
	t.Cleanup(func() { unregisterRunnerMetricNamespace(namespace) })
	config := Config{
		Enabled:                    true,
		V2Enabled:                  true,
		TransactionIndexEnabled:    true,
		SyncActive:                 syncing.Load,
		CatchupMaintenanceInterval: time.Hour,
		MetricsNamespace:           namespace,
	}
	r := New(chain, store, config)
	if changed, err := r.MaintainTransactionIndexOnce(); err != nil || !changed {
		t.Fatalf("busy maintenance changed=%v err=%v", changed, err)
	}
	if progress, ok, err := rawdb.ReadStageProgress(chain.db, rawdb.StageFreezerTxIndexPrune); err != nil || !ok || progress != transactionIndexMaintenanceBlocks {
		t.Fatalf("first quantum progress=%d ok=%v err=%v, want %d", progress, ok, err, transactionIndexMaintenanceBlocks)
	}
	stats := r.Snapshot()
	if stats.TransactionIndexBlocksPruned != transactionIndexMaintenanceBlocks || stats.TransactionIndexDebtBlocks != transactionIndexMaintenanceBlocks || stats.TransactionIndexMaintenanceAdmitted != 1 || stats.TransactionIndexPruneDuration <= 0 {
		t.Fatalf("first quantum stats=%+v", stats)
	}
	if changed, err := r.MaintainTransactionIndexOnce(); err != nil || changed {
		t.Fatalf("busy duty-cycle retry changed=%v err=%v, want deferred", changed, err)
	}
	stats = r.Snapshot()
	if stats.TransactionIndexCatchupDeferred != 1 || stats.TransactionIndexMaintenanceDeferred != 1 {
		t.Fatalf("busy duty-cycle stats=%+v", stats)
	}

	// A new runner must resume from the durable quantum boundary, not repeat
	// the full original range or skip the remaining debt.
	syncing.Store(false)
	r = New(chain, store, config)
	if changed, err := r.MaintainTransactionIndexOnce(); err != nil || !changed {
		t.Fatalf("restart maintenance changed=%v err=%v", changed, err)
	}
	if progress, ok, err := rawdb.ReadStageProgress(chain.db, rawdb.StageFreezerTxIndexPrune); err != nil || !ok || progress != 2*transactionIndexMaintenanceBlocks {
		t.Fatalf("restart progress=%d ok=%v err=%v, want %d", progress, ok, err, 2*transactionIndexMaintenanceBlocks)
	}
}

func TestTransactionIndexMaintenanceQuantumPreservesBalancedRuns(t *testing.T) {
	for _, segment := range []uint64{4_096, 65_536, 10_000, 12_289, 10_007, 1_000_003} {
		r := &Runner{cfg: Config{V2SegmentBlocks: segment}}
		needed := uint64(1) + (segment-1)/transactionIndexMaintenanceBlocks
		chunks := uint64(1)
		for chunks < needed {
			chunks <<= 1
		}
		start := uint64(0)
		minSize, maxSize := ^uint64(0), uint64(0)
		for count := uint64(0); start < segment; count++ {
			if count >= chunks {
				t.Fatalf("segment %d produced more than %d chunks", segment, chunks)
			}
			end := r.transactionIndexMaintenanceEnd(start, segment)
			if end <= start || end > segment || end-start > transactionIndexMaintenanceBlocks {
				t.Fatalf("segment %d invalid chunk [%d,%d)", segment, start, end)
			}
			size := end - start
			minSize = min(minSize, size)
			maxSize = max(maxSize, size)
			start = end
			if start == segment && count+1 != chunks {
				t.Fatalf("segment %d chunks=%d, want %d", segment, count+1, chunks)
			}
		}
		if maxSize-minSize > 1 {
			t.Fatalf("segment %d chunk spread=%d..%d, want <=1", segment, minSize, maxSize)
		}
	}
}

func TestTransactionIndexActiveMaintenanceSkipsUnboundedTailMerge(t *testing.T) {
	chain := newFakeChain()
	if err := rawdb.WriteStageProgress(chain.db, rawdb.StageFreezerTxIndexPrune, transactionIndexMaintenanceBlocks); err != nil {
		t.Fatal(err)
	}
	store := &auditTransactionIndexMaintenanceStore{
		FreezerStore: wrapFreezer(newFreezer(t)),
		body:         blockBytes(0),
		coverage:     transactionIndexMaintenanceBlocks,
		v2Coverage:   transactionIndexMaintenanceBlocks,
		dir:          t.TempDir(),
		merge:        true,
	}
	var syncing atomic.Bool
	syncing.Store(true)
	namespace := "test/tx-index-active-no-merge/"
	unregisterRunnerMetricNamespace(namespace)
	t.Cleanup(func() { unregisterRunnerMetricNamespace(namespace) })
	r := New(chain, store, Config{
		Enabled:                 true,
		V2Enabled:               true,
		TransactionIndexEnabled: true,
		SyncActive:              syncing.Load,
		MetricsNamespace:        namespace,
	})
	if changed, err := r.MaintainTransactionIndexOnce(); err != nil || changed {
		t.Fatalf("active maintenance changed=%v err=%v, want bounded no-op", changed, err)
	}
	if calls := store.mergeCalls.Load(); calls != 0 {
		t.Fatalf("active maintenance entered tail merge %d times", calls)
	}
	syncing.Store(false)
	if changed, err := r.MaintainTransactionIndexOnce(); err != nil || !changed {
		t.Fatalf("idle maintenance changed=%v err=%v, want merge", changed, err)
	}
	if calls := store.mergeCalls.Load(); calls != 1 {
		t.Fatalf("idle maintenance tail merge calls=%d, want 1", calls)
	}
}

func TestTransactionIndexIdleTailMergeCancelsWhenSyncStarts(t *testing.T) {
	chain := newFakeChain()
	if err := rawdb.WriteStageProgress(chain.db, rawdb.StageFreezerTxIndexPrune, transactionIndexMaintenanceBlocks); err != nil {
		t.Fatal(err)
	}
	store := &auditTransactionIndexMaintenanceStore{
		FreezerStore: wrapFreezer(newFreezer(t)),
		coverage:     transactionIndexMaintenanceBlocks,
		v2Coverage:   transactionIndexMaintenanceBlocks,
		dir:          t.TempDir(),
		mergeBlock:   true,
		mergeStart:   make(chan struct{}),
	}
	var syncing atomic.Bool
	namespace := "test/tx-index-merge-sync-transition/"
	unregisterRunnerMetricNamespace(namespace)
	t.Cleanup(func() { unregisterRunnerMetricNamespace(namespace) })
	r := New(chain, store, Config{
		Enabled:                 true,
		V2Enabled:               true,
		TransactionIndexEnabled: true,
		SyncActive:              syncing.Load,
		MetricsNamespace:        namespace,
	})
	done := make(chan struct {
		changed bool
		err     error
	}, 1)
	go func() {
		changed, err := r.MaintainTransactionIndexOnce()
		done <- struct {
			changed bool
			err     error
		}{changed: changed, err: err}
	}()
	select {
	case <-store.mergeStart:
	case <-time.After(2 * time.Second):
		t.Fatal("idle merge did not start")
	}
	syncing.Store(true)
	select {
	case result := <-done:
		if result.err != nil || result.changed {
			t.Fatalf("sync-canceled merge changed=%v err=%v", result.changed, result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sync transition did not cancel merge")
	}
	if calls := store.mergeCalls.Load(); calls != 1 {
		t.Fatalf("context merge calls=%d, want 1", calls)
	}
	stats := r.Snapshot()
	if stats.TransactionIndexSyncDeferred != 1 || stats.TransactionIndexMaintenanceDeferred != 0 || stats.TransactionIndexErrors != 0 {
		t.Fatalf("sync-canceled merge stats=%+v", stats)
	}
}
