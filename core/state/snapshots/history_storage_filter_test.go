package snapshots

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestStoragePrefixFilterAdaptiveNoFalseNegatives(t *testing.T) {
	for _, count := range []int{0, 1, 32, 512, 1000, 2000, 5000, storageRoutingSourceHashes} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			hashes := make([]storagePrefixHash, count)
			keys := make([][]byte, count)
			for i := range keys {
				keys[i] = stateDomainChangeBinaryAccessorLookupKey(rawdb.StateFlatDomainKVLatest, common.Address{0x41, byte(i), byte(i >> 8), byte(i >> 16)}, uint64(i), kvdomains.ContractStorage, []byte("slot"))
				hashes[i] = hashStoragePrefix(keys[i])
			}
			f := storageFilterFromHashes(hashes)
			if len(f.bits) != storageFilterBytes(uint32(count)) || len(f.bits) > 128<<10 || count > 0 && len(f.bits)*8 < 16*count {
				t.Fatal("unbounded/undersized adaptive filter", count, len(f.bits))
			}
			for _, key := range keys {
				if !f.mayContain(key) || !f.mayContain(append(bytes.Clone(key[:31]), []byte("another slot")...)) {
					t.Fatal("lost inserted prefix or narrowed it to one slot")
				}
			}
			if !f.mayContain([]byte("different family")) {
				t.Fatal("filter excluded an unrelated family")
			}
		})
	}
}

func addStorageFilterFixture(t *testing.T, dir string, m *Manifest, maxNew int) *Manifest {
	t.Helper()
	ref, built, err := BuildStorageHistoryRoutingContext(context.Background(), dir, m, maxNew)
	if err != nil || !built {
		t.Fatal(ref, built, err)
	}
	if err := checkStorageHistoryRoutingContext(context.Background(), dir, m, ref); err != nil {
		t.Fatal(err)
	}
	next, err := publishPagedHistoryRoutingContext(context.Background(), dir, m, ref, SegmentStorageHistoryRouting)
	if err != nil {
		t.Fatal(err)
	}
	return next
}

func TestStoragePrefixRoutingPartialCorruptAndGenerationOracle(t *testing.T) {
	dir, original, queries := storageRoutingFixture(t)
	oracle, err := OpenPinnedManager(dir, original)
	if err != nil {
		t.Fatal(err)
	}
	check := func(m *Manifest) {
		t.Helper()
		reader, err := OpenPinnedManager(dir, m)
		if err != nil {
			t.Fatal(err)
		}
		for from := uint64(1); from <= 4; from++ {
			for _, query := range queries {
				got, err := reader.FirstStateDomainChangesContext(context.Background(), from, 4, []StateHistoryQuery{query})
				if err != nil {
					t.Fatal(err)
				}
				var want *rawdb.StateDomainChange
				err = oracle.IterateStateDomainChangesByKey(from, 4, query.FlatDomain, query.Owner, query.Generation, query.Domain, query.Key, func(row *rawdb.StateDomainChange) (bool, error) { want = row; return false, nil })
				if err != nil || !reflect.DeepEqual(got[0], want) {
					t.Fatalf("from=%d generation=%d key=%x got=%+v want=%+v err=%v", from, query.Generation, query.Key, got[0], want, err)
				}
			}
		}
	}
	m := original
	for i := 0; i < 3; i++ {
		m = addStorageFilterFixture(t, dir, m, 1)
		check(m)
	}
	if !reflect.DeepEqual(m.Progress, original.Progress) {
		t.Fatal("auxiliary filters changed canonical progress")
	}
	for _, ref := range m.Segments {
		if ref.Kind != SegmentStorageHistoryRouting {
			continue
		}
		if err := os.WriteFile(filepath.Join(dir, ref.Path), []byte("broken optional page"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := checkStorageHistoryRoutingContext(context.Background(), dir, m, ref); err == nil {
			t.Fatal("strict verifier accepted damaged optional page")
		}
	}
	check(m) // Query authority comes from the unaffected canonical pairs.
}

func TestStoragePrefixRoutingReaderEmptyUnknownAndCancellation(t *testing.T) {
	dir, m := smallManyRoutingFixtureSources(t, 2)
	m = addStorageFilterFixture(t, dir, m, 1)
	r := newStorageHistoryRoutingReader(m)
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	for _, h := range m.Segments {
		if h.Kind != SegmentHistory {
			continue
		}
		a, _ := cfg.HistoryAccessorRef(m, h)
		f, covered, err := r.source(context.Background(), dir, h, systemHistoryRoutingPairID(h, a))
		if err != nil || covered != (h.FromTxNum == 1) || covered && (f.count != 0 || len(f.bits) != 0) {
			t.Fatal(f, covered, err)
		}
		wrong := systemHistoryRoutingPairID(h, a)
		wrong[0] ^= 1
		if _, covered, _ := r.source(context.Background(), dir, h, wrong); covered {
			t.Fatal("inherited another pair's proof")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := r.source(ctx, dir, SegmentRef{}, [32]byte{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestStoragePrefixRoutingRepairPreservesProofsBeforeCursor(t *testing.T) {
	dir, m := smallManyRoutingFixtureSources(t, 256)
	m = addStorageFilterFixture(t, dir, m, 256)
	m = replaceMetadataRepairFixtureSources(t, dir, 128, 128)
	next := uint64(128)
	ref, built, err := buildStorageHistoryRoutingContext(context.Background(), dir, m, 1, 128, &next, smallSystemRoutingWorkLimit{})
	if err != nil || !built || ref.Rows != 256 || ref.FromTxNum != 1 || ref.ToTxNum != 256 || next != 129 {
		t.Fatal(ref, built, next, err)
	}
	m, err = publishPagedHistoryRoutingContext(context.Background(), dir, m, ref, SegmentStorageHistoryRouting)
	if err != nil {
		t.Fatal(err)
	}
	r := newStorageHistoryRoutingReader(m)
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	for _, h := range m.Segments {
		if h.Kind != SegmentHistory {
			continue
		}
		a, _ := cfg.HistoryAccessorRef(m, h)
		if _, covered, err := r.source(context.Background(), dir, h, systemHistoryRoutingPairID(h, a)); err != nil || !covered {
			t.Fatal("repair discarded proof", h.Path, covered, err)
		}
	}
}

func TestStoragePrefixRoutingRepairCannotShrinkToByteCap(t *testing.T) {
	dir, m := smallManyRoutingFixtureSources(t, 8)
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	rows := make(map[[32]byte]storageSourceFilter)
	var last SegmentRef
	var lastID [32]byte
	for _, h := range m.Segments {
		if h.Kind != SegmentHistory {
			continue
		}
		a, _ := cfg.HistoryAccessorRef(m, h)
		id := systemHistoryRoutingPairID(h, a)
		if h.FromTxNum == 8 {
			last, lastID = h, id
		} else {
			rows[id] = storageSourceFilter{storageRoutingSourceHashes, bytes.Repeat([]byte{255}, 128<<10)}
		}
	}
	ref, built, err := writeStorageHistoryRoutingPage(context.Background(), dir, 1, 8, rows)
	if err != nil || !built {
		t.Fatal(ref, built, err)
	}
	before, _ := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if _, built, err = repairStorageHistoryRoutingPage(context.Background(), dir, manifestLookupView(m), []SegmentRef{ref}, last, lastID, storageSourceFilter{storageRoutingSourceHashes, bytes.Repeat([]byte{255}, 128<<10)}); err != nil || built {
		t.Fatal("over-capacity repair must defer", built, err)
	}
	after, _ := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if !bytes.Equal(before, after) {
		t.Fatal("deferred repair changed manifest")
	}
}

func TestStoragePrefixRoutingEarlyHitDoesNotReadTailPages(t *testing.T) {
	dir, m := smallManyRoutingFixtureSources(t, 300)
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	owner := common.Address{0x41, 77}
	if err := rawdb.WriteStateTxRange(db, 1, common.Hash{1}, 1, 1); err != nil {
		t.Fatal(err)
	}
	row := &rawdb.StateDomainChange{BlockNum: 1, BlockHash: common.Hash{1}, TxNum: 1, FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: owner, Domain: kvdomains.ContractStorage, Key: []byte{7}, PrevExists: true, Prev: []byte{33}}
	if err := rawdb.WriteStateDomainChange(db, row); err != nil {
		t.Fatal(err)
	}
	refs, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 1, 1, "history/state-domain-change-first-storage.seg")
	if err != nil {
		t.Fatal(err)
	}
	m, err = NewAggregator(dir).Integrate(1, 300, refs)
	if err != nil {
		t.Fatal(err)
	}
	m = addStorageFilterFixture(t, dir, m, 256)
	m = addStorageFilterFixture(t, dir, m, 256)
	r := newStorageHistoryRoutingReader(m)
	loaded := 0
	r.load = func(ctx context.Context, dir string, ref SegmentRef) (map[[32]byte]storageSourceFilter, error) {
		loaded++
		return readStorageHistoryRouting(ctx, dir, ref)
	}
	manager, err := OpenPinnedManager(dir, m)
	if err != nil {
		t.Fatal(err)
	}
	got, err := manager.firstStateDomainChangesContext(context.Background(), 1, 300, []StateHistoryQuery{{FlatDomain: row.FlatDomain, Owner: owner, Domain: row.Domain, Key: row.Key}}, r)
	if err != nil || len(got) != 1 || got[0] == nil || got[0].TxNum != 1 || !bytes.Equal(got[0].Prev, row.Prev) {
		t.Fatal(got, err)
	}
	if loaded != 1 {
		t.Fatalf("first hit unnecessarily opened tail routing pages: loaded=%d, want 1", loaded)
	}
}

func TestStoragePrefixRoutingPublicationRejectsChangedCanonicalPair(t *testing.T) {
	dir, m := smallManyRoutingFixtureSources(t, 3)
	ref, built, err := BuildStorageHistoryRoutingContext(context.Background(), dir, m, 3)
	if err != nil || !built {
		t.Fatal(ref, built, err)
	}
	_ = replaceMetadataRepairFixtureSources(t, dir, 1, 1)
	before, _ := os.ReadFile(filepath.Join(dir, ManifestFile))
	if _, err := publishPagedHistoryRoutingContext(context.Background(), dir, m, ref, SegmentStorageHistoryRouting); !errors.Is(err, errMetadataRoutingRefreshConflict) {
		t.Fatal("stale page published after pair replacement", err)
	}
	after, _ := os.ReadFile(filepath.Join(dir, ManifestFile))
	if !bytes.Equal(before, after) {
		t.Fatal("conflicting page changed manifest")
	}
}

func TestStoragePrefixRoutingRetirementRespectsReadAndPublishedLeases(t *testing.T) {
	for _, published := range []bool{false, true} {
		t.Run(fmt.Sprint(published), func(t *testing.T) {
			dir, m := smallManyRoutingFixtureSources(t, 3)
			m = addStorageFilterFixture(t, dir, m, 3)
			oldManifest := cloneManifest(m)
			var old SegmentRef
			for _, ref := range m.Segments {
				if ref.Kind == SegmentStorageHistoryRouting {
					old = ref
				}
			}
			manager, err := OpenManager(dir)
			if err != nil {
				t.Fatal(err)
			}
			_, release, err := manager.PinHistoryReadView()
			if err != nil {
				t.Fatal(err)
			}
			m = replaceMetadataRepairFixtureSources(t, dir, 1, 1)
			m = addStorageFilterFixture(t, dir, m, 1)
			result, err := pruneRetiredHistoryRoutingContext(context.Background(), dir, retiredMetadataSweepCursor{})
			release()
			if err != nil || !result.Deferred || result.FilesDeleted != 0 {
				t.Fatal("read lease did not protect storage page", result, err)
			}
			if published {
				data, err := json.Marshal(oldManifest)
				if err != nil {
					t.Fatal(err)
				}
				name, err := publishedSnapshotManifestPath(oldManifest.Generation, checksumBytes(data))
				if err != nil {
					t.Fatal(err)
				}
				if err := publishImmutableSnapshotManifest(dir, name, data); err != nil {
					t.Fatal(err)
				}
			}
			result, err = pruneRetiredHistoryRoutingContext(context.Background(), dir, retiredMetadataSweepCursor{})
			if err != nil || result.FilesDeleted != map[bool]int{false: 1, true: 0}[published] {
				t.Fatal("retired storage page lease result", result, err)
			}
			_, err = os.Stat(filepath.Join(dir, old.Path))
			if published && err != nil || !published && !os.IsNotExist(err) {
				t.Fatal("wrong retired storage page existence", err)
			}
		})
	}
}
