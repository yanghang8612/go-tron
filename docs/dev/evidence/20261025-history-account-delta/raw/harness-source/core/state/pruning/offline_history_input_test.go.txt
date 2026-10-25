package pruning

import (
	"bytes"
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

func offlineInputFixture(t *testing.T, db ethdb.KeyValueWriter, block uint64, changes []*rawdb.StateDomainChange) rawdb.StateTxRange {
	t.Helper()
	row := rawdb.StateTxRange{BlockNum: block, BlockHash: common.Hash{byte(block)}, BeginTxNum: block * 10, EndTxNum: block*10 + 9}
	if err := rawdb.WriteStateTxRange(db, row.BlockNum, row.BlockHash, row.BeginTxNum, row.EndTxNum); err != nil {
		t.Fatal(err)
	}
	for i, c := range changes {
		c.BlockNum, c.BlockHash, c.TxNum, c.Seq = row.BlockNum, row.BlockHash, row.BeginTxNum+uint64(i), uint64(i+1)
		c.Owner = common.Address{0: common.AddressPrefixMainnet, common.AddressLength - 1: byte(i + 1)}
	}
	if len(changes) > 0 {
		if err := rawdb.WriteStateDomainChangeBlockRows(db, changes); err != nil {
			t.Fatal(err)
		}
	}
	return row
}

func TestOfflineHistoryInputScanCountsPersistedPayload(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	row := offlineInputFixture(t, db, 3, []*rawdb.StateDomainChange{
		{FlatDomain: rawdb.StateFlatDomainAccountLatest, PrevExists: true, Prev: []byte("account"), NextExists: true, Next: bytes.Repeat([]byte{9}, 1000)},
		{FlatDomain: rawdb.StateFlatDomainKVLatest, Domain: kvdomains.SystemDynamicProperty, Key: []byte("key"), PrevExists: true, Prev: []byte("value")},
	})
	before := offlineInputLogicalDump(t, db)
	got, err := scanOfflineHistoryBlock(context.Background(), db, row)
	if err != nil {
		t.Fatal(err)
	}
	// New hot packs omit Next. Account lookup identity is 21 bytes, KV is
	// 31+len(key). The independently specified V6 payload is 21+len(prev).
	if got.Records != 2 || got.Blocks != 1 || got.NextValueBytes != 0 || got.PreviousValueBytes != 12 || got.KeyBytes != 3 || got.LogicalKeyBytes != 55 || got.EncodedRecordBytes != 54 || got.InputBytes != 219 || !got.Ordered {
		t.Fatalf("unexpected decoded input accounting: %+v", got)
	}
	if got.ColdScratchUpperBytes <= got.InputBytes || got.SegmentUpperBytes <= got.EncodedRecordBytes {
		t.Fatalf("missing working-space allowance: %+v", got)
	}
	if !reflect.DeepEqual(before, offlineInputLogicalDump(t, db)) {
		t.Fatal("read-only scan mutated database")
	}
	if _, err := scanOfflineHistoryBlock(context.Background(), db, row, got.InputBytes); err != nil {
		t.Fatalf("exact input budget rejected: %v", err)
	}
	if _, err := scanOfflineHistoryBlock(context.Background(), db, row, got.InputBytes-1); !errors.Is(err, ErrOfflineHistoryInputLimit) {
		t.Fatalf("undersized budget error = %v", err)
	}
}

func TestOfflineHistoryInputScanRejectsWrongRangeAndLegacy(t *testing.T) {
	t.Run("range-would-hide-record", func(t *testing.T) {
		db := rawdb.NewMemoryDatabase()
		defer db.Close()
		row := offlineInputFixture(t, db, 4, []*rawdb.StateDomainChange{{FlatDomain: rawdb.StateFlatDomainAccountLatest, PrevExists: true, Prev: []byte("a")}})
		row.BeginTxNum++
		if _, err := scanOfflineHistoryBlock(context.Background(), db, row); err == nil {
			t.Fatal("accepted record outside claimed tx range")
		}
		row.BeginTxNum--
		row.BlockHash[1]++
		if _, err := scanOfflineHistoryBlock(context.Background(), db, row); err == nil {
			t.Fatal("accepted mismatching block identity")
		}
	})
	t.Run("legacy-positive-sequence", func(t *testing.T) {
		db := rawdb.NewMemoryDatabase()
		defer db.Close()
		row := offlineInputFixture(t, db, 5, nil)
		if err := rawdb.WriteStateDomainChangeRow(db, &rawdb.StateDomainChange{BlockNum: row.BlockNum, TxNum: row.BeginTxNum, Seq: 3, FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: common.Address{0: common.AddressPrefixMainnet}, PrevExists: true, Prev: []byte("a")}); err != nil {
			t.Fatal(err)
		}
		if _, err := scanOfflineHistoryBlock(context.Background(), db, row); !errors.Is(err, rawdb.ErrStateDomainChangeBorrowedLegacyRows) {
			t.Fatalf("legacy source error = %v", err)
		}
	})
}

type offlineInputCountingDB struct {
	ethdb.KeyValueStore
	reads  int
	cancel context.CancelFunc
}

func (d *offlineInputCountingDB) NewIterator(prefix, start []byte) ethdb.Iterator {
	d.reads++
	if d.cancel != nil {
		d.cancel()
	}
	return d.KeyValueStore.NewIterator(prefix, start)
}

func TestOfflineHistoryInputCancellationAndSingleRecordLimit(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	row := offlineInputFixture(t, db, 6, []*rawdb.StateDomainChange{{FlatDomain: rawdb.StateFlatDomainAccountLatest, PrevExists: true, Prev: bytes.Repeat([]byte{7}, 1<<20)}})
	if _, err := scanOfflineHistoryBlock(context.Background(), db, row, 1024); !errors.Is(err, ErrOfflineHistoryInputLimit) {
		t.Fatalf("large single record error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	counted := &offlineInputCountingDB{KeyValueStore: db}
	if _, err := scanOfflineHistoryBlock(ctx, counted, row); !errors.Is(err, context.Canceled) || counted.reads != 0 {
		t.Fatalf("cancel before iteration err=%v reads=%d", err, counted.reads)
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	counted.cancel = cancel
	if _, err := scanOfflineHistoryBlock(ctx, counted, row); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel inside iteration = %v", err)
	}
}

func TestOfflineHistoryInputAggregationRejectsOverflowAndOrder(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	var scans []OfflineHistoryInputStats
	for _, block := range []uint64{7, 8} {
		row := offlineInputFixture(t, db, block, []*rawdb.StateDomainChange{{FlatDomain: rawdb.StateFlatDomainKVLatest, Domain: kvdomains.SystemDynamicProperty, Key: []byte("x"), PrevExists: true, Prev: []byte("v")}})
		s, err := scanOfflineHistoryBlock(context.Background(), db, row)
		if err != nil {
			t.Fatal(err)
		}
		scans = append(scans, s)
	}
	var combined OfflineHistoryInputStats
	if err := combined.Add(scans[0]); err != nil {
		t.Fatal(err)
	}
	if err := combined.Add(scans[1]); err != nil {
		t.Fatal(err)
	}
	if combined.Records != 2 || combined.Blocks != 2 || combined.InputBytes != scans[0].InputBytes+scans[1].InputBytes || combined.FirstBlock != 7 || combined.LastBlock != 8 {
		t.Fatalf("combined = %+v", combined)
	}
	before := combined
	if err := combined.Add(scans[0]); err == nil || combined != before {
		t.Fatal("out-of-order Add did not fail atomically")
	}
	huge := scans[1]
	huge.FirstBlock, huge.LastBlock, huge.FirstTxNum, huge.LastTxNum = 9, 9, 90, 90
	huge.InputBytes = math.MaxUint64
	if err := combined.Add(huge); err == nil || combined != before {
		t.Fatal("overflow Add did not fail atomically")
	}
	huge = scans[1]
	huge.LogicalKeyBytes = math.MaxUint64
	if err := huge.deriveBounds(); err == nil {
		t.Fatal("scratch estimate silently overflowed")
	}
}

func TestOfflineHistoryInputBoundCoversProducedHistoryFormats(t *testing.T) {
	for _, format := range []string{"1", "2"} {
		t.Run(format, func(t *testing.T) {
			t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", format)
			db := rawdb.NewMemoryDatabase()
			defer db.Close()
			previous := make([]byte, 256<<10)
			// Deterministic, poorly compressible values exercise physical output,
			// while the one-byte ETL budget forces on-disk sort runs.
			x := uint64(0x123456789abcdef)
			for i := range previous {
				x ^= x << 13
				x ^= x >> 7
				x ^= x << 17
				previous[i] = byte(x)
			}
			row := offlineInputFixture(t, db, 9, []*rawdb.StateDomainChange{
				{FlatDomain: rawdb.StateFlatDomainKVLatest, Domain: kvdomains.SystemDynamicProperty, Key: []byte("one"), PrevExists: true, Prev: previous},
				{FlatDomain: rawdb.StateFlatDomainKVLatest, Domain: kvdomains.SystemDynamicProperty, Key: []byte("two"), PrevExists: true, Prev: previous},
			})
			s, err := scanOfflineHistoryBlock(context.Background(), db, row)
			if err != nil {
				t.Fatal(err)
			}
			dir := t.TempDir()
			cfg, _ := snapshots.DefaultDomainRegistry().Dataset(snapshots.SegmentDatasetStateDomainChange)
			refs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDBByBlockRangeContext(context.Background(), db, dir, row.BeginTxNum, row.EndTxNum, row.BlockNum, row.BlockNum, cfg.HistoryPath(row.BeginTxNum, row.EndTxNum), etl.Options{BufferLimit: 1})
			if err != nil {
				t.Fatal(err)
			}
			var total uint64
			for _, ref := range refs {
				st, err := os.Stat(filepath.Join(dir, ref.Path))
				if err != nil {
					t.Fatal(err)
				}
				n := uint64(st.Size())
				total += n
				bound := s.SegmentUpperBytes
				switch ref.Kind {
				case snapshots.SegmentAccessor:
					bound = s.AccessorUpperBytes
				case snapshots.SegmentInverted:
					bound = s.IndexUpperBytes
				}
				if n > bound {
					t.Fatalf("format %s %s length %d exceeds %d", format, ref.Kind, n, bound)
				}
			}
			if total > s.ColdScratchUpperBytes {
				t.Fatalf("output %d exceeds aggregate allowance %d", total, s.ColdScratchUpperBytes)
			}
		})
	}
}

func offlineInputLogicalDump(t *testing.T, db ethdb.Iteratee) map[string]string {
	t.Helper()
	it := db.NewIterator(nil, nil)
	defer it.Release()
	out := make(map[string]string)
	for it.Next() {
		out[string(it.Key())] = string(it.Value())
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	return out
}
