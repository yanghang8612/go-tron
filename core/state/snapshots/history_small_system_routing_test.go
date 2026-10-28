package snapshots

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func addSmallRoutingFixture(t *testing.T, dir string, m *Manifest, maxNew int) *Manifest {
	t.Helper()
	ref, built, err := BuildSmallSystemHistoryRoutingContext(context.Background(), dir, m, maxNew)
	if err != nil || !built {
		t.Fatalf("build routing: built=%v err=%v", built, err)
	}
	next, err := NewAggregator(dir).Integrate(m.VisibleTxStart, m.VisibleTxEnd, []SegmentRef{ref})
	if err != nil {
		t.Fatal(err)
	}
	return next
}

func TestSmallSystemHistoryRoutingPartialAndCompleteMatchFullHistory(t *testing.T) {
	dir, original, queries := routingFixture(t)
	oracle, err := OpenPinnedManager(dir, original)
	if err != nil {
		t.Fatal(err)
	}
	m := original
	for pass := 0; pass < 3; pass++ {
		m = addSmallRoutingFixture(t, dir, m, 1)
		route, err := loadSmallSystemHistoryRouting(context.Background(), dir, m)
		if err != nil {
			t.Fatal(err)
		}
		if len(route.sources) != pass+1 {
			t.Fatalf("coverage=%d", len(route.sources))
		}
		if err := checkSmallSystemHistoryRoutingContext(context.Background(), dir, m, mustSmallRoutingRef(t, m)); err != nil {
			t.Fatal(err)
		}
		reader, err := OpenPinnedManager(dir, m)
		if err != nil {
			t.Fatal(err)
		}
		for from := uint64(1); from <= 4; from++ {
			got, err := reader.FirstStateDomainChangesContext(context.Background(), from, 4, queries)
			if err != nil {
				t.Fatal(err)
			}
			for i, q := range queries {
				var want *rawdb.StateDomainChange
				err := oracle.IterateStateDomainChangesByKey(from, 4, q.FlatDomain, q.Owner, q.Generation, q.Domain, q.Key, func(row *rawdb.StateDomainChange) (bool, error) { want = row; return false, nil })
				if err != nil || !reflect.DeepEqual(got[i], want) {
					t.Fatalf("pass=%d from=%d query=%d got=%+v want=%+v err=%v", pass, from, i, got[i], want, err)
				}
			}
		}
	}
	if _, built, err := BuildSmallSystemHistoryRoutingContext(context.Background(), dir, m, 64); err != nil || built {
		t.Fatalf("covered index rebuilt: %v %v", built, err)
	}
	// The auxiliary object must not alter restoration or any history frontier.
	if !reflect.DeepEqual(original.Progress, m.Progress) {
		t.Fatal("routing changed frontiers")
	}
	restored := rawdb.NewMemoryDatabase()
	defer restored.Close()
	reader, _ := OpenPinnedManager(dir, m)
	result, err := reader.RestoreStateDomainHistory(restored, 1, 3)
	if err != nil || result.ChangesRestored == 0 {
		t.Fatalf("restore: %+v %v", result, err)
	}
	count := uint64(0)
	for block := uint64(1); block <= 3; block++ {
		if err := rawdb.IterateStateDomainChanges(restored, block, func(*rawdb.StateDomainChange) (bool, error) { count++; return true, nil }); err != nil {
			t.Fatal(err)
		}
	}
	if count != result.ChangesRestored {
		t.Fatalf("restored %d rows, counted %d", result.ChangesRestored, count)
	}
}

func mustSmallRoutingRef(t *testing.T, m *Manifest) SegmentRef {
	t.Helper()
	for _, ref := range m.Segments {
		if ref.Kind == SegmentSmallSystemHistoryRouting {
			return ref
		}
	}
	t.Fatal("missing routing ref")
	return SegmentRef{}
}

func TestSmallSystemHistoryRoutingStaleIdentityFallsBackAndCorruptionFails(t *testing.T) {
	dir, original, queries := routingFixture(t)
	m := addSmallRoutingFixture(t, dir, original, 64)
	// Identity mismatches must fall back even when bounds and paths coincide.
	stale := cloneManifest(m)
	for i := range stale.Segments {
		ref := &stale.Segments[i]
		if ref.Kind == SegmentAccessor {
			ref.Checksum = "sha256:" + fmt.Sprintf("%064x", 1)
		}
	}
	r, err := OpenPinnedManager(dir, stale)
	if err != nil {
		t.Fatal(err)
	}
	got, err := r.FirstStateDomainChangesContext(context.Background(), 1, 3, queries)
	if err != nil || got[0] == nil {
		t.Fatalf("stale route hid history: %+v %v", got, err)
	}
	ref := mustSmallRoutingRef(t, m)
	p := filepath.Join(dir, ref.Path)
	stat, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 1
	if err := os.WriteFile(p, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(p, stat.ModTime(), stat.ModTime()); err != nil {
		t.Fatal(err)
	}
	r, _ = OpenPinnedManager(dir, m)
	if _, err := r.FirstStateDomainChangesContext(context.Background(), 1, 3, queries); err == nil {
		t.Fatal("same-inode/size/mtime corruption accepted")
	}
}

func TestSmallSystemHistoryRoutingStrictCheckDetectsOmittedKey(t *testing.T) {
	dir, m, queries := routingFixture(t)
	m = addSmallRoutingFixture(t, dir, m, 64)
	ref := mustSmallRoutingRef(t, m)
	data, err := os.ReadFile(filepath.Join(dir, ref.Path))
	if err != nil {
		t.Fatal(err)
	}
	route, err := decodeHistoryRoutingFamily(context.Background(), data, ref, true)
	if err != nil {
		t.Fatal(err)
	}
	lookup := stateDomainChangeBinaryAccessorLookupKey(queries[0].FlatDomain, queries[0].Owner, queries[0].Generation, queries[0].Domain, queries[0].Key)
	if route.keys[string(lookup)][0] != 0 {
		t.Fatal("expected dense fixture")
	}
	offset := systemHistoryRoutingHeaderSize + len(route.sources)*32
	for {
		size := int(binary.BigEndian.Uint16(data[offset : offset+2]))
		offset += 2
		key := data[offset : offset+size]
		offset += size
		length := int(binary.BigEndian.Uint32(data[offset+1 : offset+5]))
		offset += 5
		if bytes.Equal(key, lookup) {
			data[offset] &^= 1
			break
		}
		offset += length
	}
	ref.Checksum = fmt.Sprintf("sha256:%x", sha256.Sum256(data))
	if err := os.WriteFile(filepath.Join(dir, ref.Path), data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readSmallSystemHistoryRouting(context.Background(), dir, ref); err != nil {
		t.Fatal(err)
	}
	if err := checkSmallSystemHistoryRoutingContext(context.Background(), dir, m, ref); err == nil {
		t.Fatal("semantic omission accepted by snapshot checker")
	}
}

func TestSmallSystemHistoryRoutingBoundsCancelAndPairIdentity(t *testing.T) {
	dir, m, _ := routingFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, built, err := BuildSmallSystemHistoryRoutingContext(ctx, dir, m, 64); built || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled build: %v %v", built, err)
	}
	m = addSmallRoutingFixture(t, dir, m, 64)
	ref := mustSmallRoutingRef(t, m)
	data, err := os.ReadFile(filepath.Join(dir, ref.Path))
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []int{12, 16} {
		bad := append([]byte(nil), data...)
		binary.BigEndian.PutUint32(bad[field:field+4], math.MaxUint32)
		if _, err := decodeHistoryRoutingFamily(context.Background(), bad, ref, true); err == nil {
			t.Fatal("oversized count accepted")
		}
	}
	for _, n := range []int{0, 8, 39, len(data) - 1} {
		if _, err := decodeHistoryRoutingFamily(context.Background(), data[:n], ref, true); err == nil {
			t.Fatalf("truncation %d accepted", n)
		}
	}
	var history, accessor SegmentRef
	for _, r := range m.Segments {
		if r.Kind == SegmentHistory {
			history = r
		}
		if r.Kind == SegmentAccessor {
			accessor = r
		}
	}
	original := systemHistoryRoutingPairID(history, accessor)
	for _, mutate := range []func(*SegmentRef){func(r *SegmentRef) { r.Checksum += "x" }, func(r *SegmentRef) { r.Path += "x" }, func(r *SegmentRef) { r.AggregationSteps++ }, func(r *SegmentRef) { r.Size++ }, func(r *SegmentRef) { r.FromTxNum++ }, func(r *SegmentRef) { r.Rows++ }} {
		changed := accessor
		mutate(&changed)
		if systemHistoryRoutingPairID(history, changed) == original {
			t.Fatal("source identity field omitted")
		}
	}
}

func TestSmallSystemHistoryRoutingReplacementAndSparseMembership(t *testing.T) {
	dir, m, queries := routingFixture(t)
	m = addSmallRoutingFixture(t, dir, m, 64)
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	q := queries[0]
	q.Key = []byte("new-key")
	if err := rawdb.WriteStateTxRange(db, 1, common.Hash{1}, 1, 1); err != nil {
		t.Fatal(err)
	}
	row := &rawdb.StateDomainChange{BlockNum: 1, BlockHash: common.Hash{1}, TxNum: 1, Seq: 0, FlatDomain: q.FlatDomain, Owner: q.Owner, Generation: q.Generation, Domain: q.Domain, Key: q.Key, PrevExists: true, Prev: []byte("replacement")}
	if err := rawdb.WriteStateDomainChange(db, row); err != nil {
		t.Fatal(err)
	}
	refs, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 1, 1, "history/state-domain-change-replaced-route.seg")
	if err != nil {
		t.Fatal(err)
	}
	m, err = NewAggregator(dir).Integrate(1, 1, refs)
	if err != nil {
		t.Fatal(err)
	}
	for pass := 0; pass < 2; pass++ {
		r, err := OpenPinnedManager(dir, m)
		if err != nil {
			t.Fatal(err)
		}
		got, err := r.FirstStateDomainChangesContext(context.Background(), 1, 3, []StateHistoryQuery{q})
		if err != nil || got[0] == nil || string(got[0].Prev) != "replacement" {
			t.Fatalf("replacement hidden: %+v %v", got, err)
		}
		if pass == 0 {
			m = addSmallRoutingFixture(t, dir, m, 64)
		}
	}
	// A sparse list stores one uint32 column rather than a 9-byte bitmap.
	const n = 65
	key := stateDomainChangeBinaryAccessorLookupKey(rawdb.StateFlatDomainKVLatest, common.SystemAccountAddress, 0, kvdomains.SystemDynamicProperty, []byte("meta"))
	data := make([]byte, systemHistoryRoutingHeaderSize)
	copy(data, smallSystemHistoryRoutingMagic[:])
	binary.BigEndian.PutUint32(data[8:], 1)
	binary.BigEndian.PutUint32(data[12:], n)
	binary.BigEndian.PutUint32(data[16:], 1)
	binary.BigEndian.PutUint64(data[24:], 1)
	binary.BigEndian.PutUint64(data[32:], 65)
	for i := 0; i < n; i++ {
		sum := sha256.Sum256([]byte{byte(i)})
		data = append(data, sum[:]...)
	}
	data = binary.BigEndian.AppendUint16(data, uint16(len(key)))
	data = append(data, key...)
	data = append(data, 1)
	data = binary.BigEndian.AppendUint32(data, 4)
	data = binary.BigEndian.AppendUint32(data, 64)
	ref := SegmentRef{FromTxNum: 1, ToTxNum: 65, Rows: 1}
	route, err := decodeHistoryRoutingFamily(context.Background(), data, ref, true)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if route.mayContain(i, key) != (i == 64) {
			t.Fatalf("sparse membership %d", i)
		}
	}
	var seen []int
	systemHistoryMembersIterate(route.keys[string(key)], func(column int) { seen = append(seen, column) })
	if !reflect.DeepEqual(seen, []int{64}) {
		t.Fatal(seen)
	}
	bad := append([]byte(nil), data...)
	binary.BigEndian.PutUint32(bad[len(bad)-4:], 65)
	if _, err := decodeHistoryRoutingFamily(context.Background(), bad, ref, true); err == nil {
		t.Fatal("sparse out-of-bounds column accepted")
	}
}

func TestSmallSystemHistoryRoutingRefreshHonorsBusyBatch(t *testing.T) {
	dir, m, _ := routingFixture(t)
	if built, err := refreshSmallSystemHistoryRoutingContext(context.Background(), dir, true); err != nil || built {
		t.Fatalf("small catalog should use normal readers: %v %v", built, err)
	}
	m = addSmallRoutingFixture(t, dir, m, 1)
	if built, err := refreshSmallSystemHistoryRoutingContext(context.Background(), dir, true); err != nil || built {
		t.Fatalf("busy tail admitted: %v %v", built, err)
	}
	if built, err := refreshSmallSystemHistoryRoutingContext(context.Background(), dir, false); err != nil || !built {
		t.Fatalf("idle tail deferred: %v %v", built, err)
	}
	current, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	route, err := loadSmallSystemHistoryRouting(context.Background(), dir, current)
	if err != nil || len(route.sources) != 3 {
		t.Fatalf("idle coverage: %+v %v", route, err)
	}
	if err := CheckSmallSystemHistoryRouting(dir, mustSmallRoutingRef(t, current)); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyLoadedManifestFiles(dir, current, VerifyManifestOptions{RequireRegistered: true, RequireChecksums: true}); err != nil {
		t.Fatal(err)
	}
}

func TestSmallSystemHistoryRoutingDoesNotReadAnUnrelatedIndex(t *testing.T) {
	dir, m, queries := routingFixture(t)
	m = addSmallRoutingFixture(t, dir, m, 64)
	ref := mustSmallRoutingRef(t, m)
	// Make the unused auxiliary object unavailable. A pure hot/future range
	// must not read it; an overlapping read must still fail closed.
	if err := os.Rename(filepath.Join(dir, ref.Path), filepath.Join(dir, ref.Path+".unavailable")); err != nil {
		t.Fatal(err)
	}
	r, err := OpenPinnedManager(dir, m)
	if err != nil {
		t.Fatal(err)
	}
	got, err := r.FirstStateDomainChangesContext(context.Background(), 4, 5, queries)
	if err != nil {
		t.Fatalf("unrelated index opened: %v", err)
	}
	for _, row := range got {
		if row != nil {
			t.Fatal("unexpected future history")
		}
	}
	if _, err := r.FirstStateDomainChangesContext(context.Background(), 1, 3, queries); err == nil {
		t.Fatal("missing used index ignored")
	}
}

func TestSmallSystemHistoryRoutingCompleteDoesNotLoadBroadRoute(t *testing.T) {
	dir, m, queries := routingFixture(t)
	m = addRoutingFixture(t, dir, m, 64)
	broad := mustRoutingRef(t, m)
	if err := os.Rename(filepath.Join(dir, broad.Path), filepath.Join(dir, broad.Path+".missing")); err != nil {
		t.Fatal(err)
	}
	for _, install := range []bool{false, true} {
		if install {
			// Build the independent small proof without consulting the
			// intentionally unavailable broad object used by this read test.
			input := *m
			input.Segments = nil
			for _, ref := range m.Segments {
				if ref.Kind != SegmentSystemHistoryRouting {
					input.Segments = append(input.Segments, ref)
				}
			}
			m = addSmallRoutingFixture(t, dir, &input, 64)
		}
		reader, err := OpenPinnedManager(dir, m)
		if err != nil {
			t.Fatal(err)
		}
		var pure []StateHistoryQuery
		for _, q := range queries {
			if smallSystemHistoryRoutingQuery(q) {
				pure = append(pure, q)
			}
		}
		got, err := reader.FirstStateDomainChangesContext(context.Background(), 1, 3, pure)
		if !install {
			if err == nil {
				t.Fatal("bootstrap fallback ignored missing broad route")
			}
		} else if err != nil || got[0] == nil {
			t.Fatalf("complete small route opened broad route or result lost: %v", err)
		}
		if _, err := reader.FirstStateDomainChangesContext(context.Background(), 1, 3, queries); err == nil {
			t.Fatal("mixed metadata batch failed to authenticate broad route")
		}
	}
}

func TestSmallSystemHistoryRoutingBootstrapNegativeAndMixedSelection(t *testing.T) {
	for _, version := range []uint32{1, 2} {
		for _, smallSources := range []int{0, 1, 64} {
			t.Run(fmt.Sprintf("broad-v%d/small-%d", version, smallSources), func(t *testing.T) {
				dir, m, queries := routingFixture(t)
				m = addRoutingFixture(t, dir, m, 64)
				if version == 1 {
					ref := mustRoutingRef(t, m)
					data, err := os.ReadFile(filepath.Join(dir, ref.Path))
					if err != nil {
						t.Fatal(err)
					}
					// This fixture has no storage-owner-only V2 rows.
					binary.BigEndian.PutUint32(data[8:12], 1)
					sum := sha256.Sum256(data)
					ref.Checksum = fmt.Sprintf("sha256:%x", sum)
					ref.Path = fmt.Sprintf("history/bootstrap-legacy-%x.route", sum)
					if err := os.WriteFile(filepath.Join(dir, ref.Path), data, 0600); err != nil {
						t.Fatal(err)
					}
					m, err = NewAggregator(dir).Integrate(1, 3, []SegmentRef{ref})
					if err != nil {
						t.Fatal(err)
					}
				}
				if smallSources != 0 {
					m = addSmallRoutingFixture(t, dir, m, smallSources)
				}
				missingSystem := queries[0]
				missingSystem.Key = []byte("bootstrap-missing-system")
				missingMetadata := queries[7]
				missingMetadata.Key = []byte("bootstrap-missing-metadata")
				// Every query is absent from the authenticated routes. Removing
				// accessors proves that uncovered small keys used the broad proof,
				// including when metadata loaded the selected view first.
				for _, ref := range m.Segments {
					if ref.Kind == SegmentAccessor {
						if err := os.Rename(filepath.Join(dir, ref.Path), filepath.Join(dir, ref.Path+".missing")); err != nil {
							t.Fatal(err)
						}
					}
				}
				for _, batch := range [][]StateHistoryQuery{{missingSystem}, {missingMetadata, missingSystem}, {missingSystem, missingMetadata}} {
					reader, err := OpenPinnedManager(dir, m)
					if err != nil {
						t.Fatal(err)
					}
					got, err := reader.FirstStateDomainChangesContext(context.Background(), 1, 3, batch)
					if err != nil {
						t.Fatal(err)
					}
					for _, row := range got {
						if row != nil {
							t.Fatal("negative proof returned a row")
						}
					}
				}
			})
		}
	}
}

func TestSmallSystemHistoryRoutingBootstrapDoesNotHideUncoveredTail(t *testing.T) {
	dir, original, queries := routingFixture(t)
	oracle, err := OpenPinnedManager(dir, original)
	if err != nil {
		t.Fatal(err)
	}
	m := addRoutingFixture(t, dir, original, 1)
	m = addSmallRoutingFixture(t, dir, m, 1)
	reader, err := OpenPinnedManager(dir, m)
	if err != nil {
		t.Fatal(err)
	}
	for from := uint64(1); from <= 3; from++ {
		got, err := reader.FirstStateDomainChangesContext(context.Background(), from, 3, queries)
		if err != nil {
			t.Fatal(err)
		}
		for i, q := range queries {
			var want *rawdb.StateDomainChange
			err := oracle.IterateStateDomainChangesByKey(from, 3, q.FlatDomain, q.Owner, q.Generation, q.Domain, q.Key, func(row *rawdb.StateDomainChange) (bool, error) { want = row; return false, nil })
			if err != nil || !reflect.DeepEqual(got[i], want) {
				t.Fatalf("from=%d query=%d got=%+v want=%+v err=%v", from, i, got[i], want, err)
			}
		}
	}
}

func smallManyRoutingFixture(t *testing.T) (string, *Manifest) {
	t.Helper()
	return smallManyRoutingFixtureSources(t, 70)
}

func smallManyRoutingFixtureSources(t *testing.T, count uint64) (string, *Manifest) {
	t.Helper()
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	var refs []SegmentRef
	// More than the maintenance cap, including an empty-family source and a
	// new system key beyond the observed 59-key set.
	for tx := uint64(1); tx <= count; tx++ {
		if err := rawdb.WriteStateTxRange(db, tx, common.Hash{byte(tx)}, tx, tx); err != nil {
			t.Fatal(err)
		}
		owner, domain, key := common.SystemAccountAddress, kvdomains.SystemDynamicProperty, []byte(fmt.Sprintf("future-%d", tx))
		if tx == count {
			owner = common.Address{0x41, 9}
			domain = kvdomains.ContractMetadata
		}
		row := &rawdb.StateDomainChange{BlockNum: tx, BlockHash: common.Hash{byte(tx)}, TxNum: tx, FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: owner, Domain: domain, Key: key, PrevExists: true, Prev: []byte{1}}
		if err := rawdb.WriteStateDomainChange(db, row); err != nil {
			t.Fatal(err)
		}
		built, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, tx, tx, fmt.Sprintf("history/state-domain-change-small-%d.seg", tx))
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, built...)
	}
	m := NewManifest(1, count, refs)
	if err := PublishManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	return dir, m
}

func TestSmallSystemHistoryRoutingOnePassAllSourcesAndNewKeys(t *testing.T) {
	dir, m := smallManyRoutingFixture(t)
	m = addSmallRoutingFixture(t, dir, m, 1000)
	route, err := loadSmallSystemHistoryRouting(context.Background(), dir, m)
	if err != nil {
		t.Fatal(err)
	}
	if len(route.sources) != 70 || len(route.keys) != 69 {
		t.Fatalf("sources=%d keys=%d", len(route.sources), len(route.keys))
	}
	if err := CheckSmallSystemHistoryRouting(dir, mustSmallRoutingRef(t, m)); err != nil {
		t.Fatal(err)
	}
	for key, members := range route.keys {
		if !smallSystemHistoryRoutingKey([]byte(key)) {
			t.Fatal("foreign family admitted")
		}
		if systemHistoryMembersContain(members, 69) {
			t.Fatal("empty source has membership")
		}
	}
	// Pure queries use the new route in mixed catalogs; ordinary owner aliases
	// still undergo the authoritative full-owner checks.
	if _, err := VerifyLoadedManifestFiles(dir, m, VerifyManifestOptions{RequireChecksums: true, RequireRegistered: true}); err != nil {
		t.Fatal(err)
	}
}

func retiredSmallRoutingFixture(t *testing.T) (string, *Manifest, SegmentRef) {
	t.Helper()
	dir, m, _ := routingFixture(t)
	m = addSmallRoutingFixture(t, dir, m, 1)
	old := mustSmallRoutingRef(t, m)
	m = addSmallRoutingFixture(t, dir, m, 64)
	return dir, m, old
}

func TestRetiredSmallHistoryRoutingOnlyDeletesUnleasedAuxiliaryFiles(t *testing.T) {
	dir, m, old := retiredSmallRoutingFixture(t)
	before, err := os.ReadFile(filepath.Join(dir, ManifestFile))
	if err != nil {
		t.Fatal(err)
	}
	canonical := make(map[string][32]byte)
	for _, ref := range m.Segments {
		if ref.Kind != SegmentSmallSystemHistoryRouting {
			data, err := os.ReadFile(filepath.Join(dir, ref.Path))
			if err != nil {
				t.Fatal(err)
			}
			canonical[ref.Path] = sha256.Sum256(data)
		}
	}
	result, err := pruneRetiredHistoryRoutingContext(context.Background(), dir, retiredMetadataSweepCursor{})
	if err != nil || result.FilesDeleted != 1 || result.BytesDeleted != old.Size {
		t.Fatalf("prune=%+v err=%v", result, err)
	}
	if _, err := os.Stat(filepath.Join(dir, old.Path)); !os.IsNotExist(err) {
		t.Fatal("old auxiliary file retained", err)
	}
	if _, err := os.Stat(filepath.Join(dir, mustSmallRoutingRef(t, m).Path)); err != nil {
		t.Fatal("active index lost", err)
	}
	after, err := os.ReadFile(filepath.Join(dir, ManifestFile))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("cleanup changed signed metadata", err)
	}
	for path, want := range canonical {
		data, err := os.ReadFile(filepath.Join(dir, path))
		if err != nil || sha256.Sum256(data) != want {
			t.Fatal("canonical file changed", path, err)
		}
	}
	if err := checkSmallSystemHistoryRoutingContext(context.Background(), dir, m, mustSmallRoutingRef(t, m)); err != nil {
		t.Fatal(err)
	}
}

func TestRetiredSmallHistoryRoutingPinsReadersAndPublishedGenerations(t *testing.T) {
	dir, m, old := retiredSmallRoutingFixture(t)
	r, err := OpenManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	_, release, err := r.PinHistoryReadView()
	if err != nil {
		t.Fatal(err)
	}
	result, err := pruneRetiredHistoryRoutingContext(context.Background(), dir, retiredMetadataSweepCursor{})
	release()
	if err != nil || !result.Deferred || result.FilesDeleted != 0 {
		t.Fatalf("reader lease: %+v %v", result, err)
	}
	published := cloneManifest(m)
	for i, ref := range published.Segments {
		if ref.Kind == SegmentSmallSystemHistoryRouting {
			published.Segments[i] = old
		}
	}
	data, err := json.Marshal(published)
	if err != nil {
		t.Fatal(err)
	}
	name, err := publishedSnapshotManifestPath(published.Generation, checksumBytes(data))
	if err != nil {
		t.Fatal(err)
	}
	if err := publishImmutableSnapshotManifest(dir, name, data); err != nil {
		t.Fatal(err)
	}
	result, err = pruneRetiredHistoryRoutingContext(context.Background(), dir, retiredMetadataSweepCursor{})
	if err != nil || result.FilesDeleted != 0 {
		t.Fatalf("published lease: %+v %v", result, err)
	}
	if _, err := os.Stat(filepath.Join(dir, old.Path)); err != nil {
		t.Fatal(err)
	}
}

func TestSmallSystemHistoryRoutingUnsupportedSourcesDoNotStarveTail(t *testing.T) {
	dir, m := smallManyRoutingFixture(t)
	count := 0
	for i := range m.Segments {
		ref := &m.Segments[i]
		if ref.Kind != SegmentAccessor {
			continue
		}
		if count < 64 {
			data, err := os.ReadFile(filepath.Join(dir, ref.Path))
			if err != nil {
				t.Fatal(err)
			}
			binary.BigEndian.PutUint32(data[8:12], 0) // unsupported accessor version
			if err := os.WriteFile(filepath.Join(dir, ref.Path), data, 0600); err != nil {
				t.Fatal(err)
			}
			ref.Checksum = fmt.Sprintf("sha256:%x", sha256.Sum256(data))
		}
		count++
	}
	if err := PublishManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	// The larger bounded batch reaches the supported tail on its first pass.
	if built, err := refreshSmallSystemHistoryRoutingContext(context.Background(), dir, false); err != nil || !built {
		t.Fatalf("supported tail starved: %v %v", built, err)
	}
	next, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	route, err := loadSmallSystemHistoryRouting(context.Background(), dir, next)
	if err != nil || len(route.sources) != 6 {
		t.Fatalf("coverage=%v err=%v", route, err)
	}
}

func TestSmallSystemHistoryRoutingEmptyCatalogFamily(t *testing.T) {
	dir, m, queries := storageRoutingFixture(t)
	m = addSmallRoutingFixture(t, dir, m, 1000)
	route, err := loadSmallSystemHistoryRouting(context.Background(), dir, m)
	if err != nil || len(route.keys) != 0 || len(route.sources) != 3 {
		t.Fatalf("zero-key route: %+v %v", route, err)
	}
	if err := CheckSmallSystemHistoryRouting(dir, mustSmallRoutingRef(t, m)); err != nil {
		t.Fatal(err)
	}
	m = addRoutingFixture(t, dir, m, 64)
	reader, err := OpenPinnedManager(dir, m)
	if err != nil {
		t.Fatal(err)
	}
	queries = append(queries[:len(queries)-1], StateHistoryQuery{FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: common.SystemAccountAddress, Domain: kvdomains.SystemDynamicProperty, Key: []byte("missing")})
	got, err := reader.FirstStateDomainChangesContext(context.Background(), 1, 3, queries)
	if err != nil || got[0] == nil || got[len(got)-1] != nil {
		t.Fatalf("mixed storage/system: %+v %v", got, err)
	}
}

// Deterministically cancel after I/O-independent decode has already begun.
type smallRouteCancelDuringDecode struct {
	context.Context
	remaining int
}

func (c *smallRouteCancelDuringDecode) Err() error {
	c.remaining--
	if c.remaining <= 0 {
		return context.Canceled
	}
	return nil
}
func TestSmallSystemHistoryRoutingCancellationDuringDecode(t *testing.T) {
	dir, m, _ := routingFixture(t)
	m = addSmallRoutingFixture(t, dir, m, 64)
	ref := mustSmallRoutingRef(t, m)
	data, err := os.ReadFile(filepath.Join(dir, ref.Path))
	if err != nil {
		t.Fatal(err)
	}
	ctx := &smallRouteCancelDuringDecode{Context: context.Background(), remaining: 4}
	if _, err := decodeHistoryRoutingFamily(ctx, data, ref, true); !errors.Is(err, context.Canceled) {
		t.Fatalf("mid-decode cancellation ignored: %v", err)
	}
}

func TestSmallSystemHistoryRoutingRotatedReplacementAndNewTail(t *testing.T) {
	dir, m, queries := routingFixture(t)
	m = addSmallRoutingFixture(t, dir, m, 1)
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	var refs []SegmentRef
	for _, tx := range []uint64{1, 4} {
		q := queries[0]
		q.Key = []byte(fmt.Sprintf("rotated-%d", tx))
		queries = append(queries, q)
		if err := rawdb.WriteStateTxRange(db, tx, common.Hash{byte(tx)}, tx, tx); err != nil {
			t.Fatal(err)
		}
		row := &rawdb.StateDomainChange{BlockNum: tx, BlockHash: common.Hash{byte(tx)}, TxNum: tx, FlatDomain: q.FlatDomain, Owner: q.Owner, Domain: q.Domain, Key: q.Key, PrevExists: true, Prev: []byte{byte(tx)}}
		if err := rawdb.WriteStateDomainChange(db, row); err != nil {
			t.Fatal(err)
		}
		built, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, tx, tx, fmt.Sprintf("history/state-domain-change-rotated-%d.seg", tx))
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, built...)
	}
	var err error
	m, err = NewAggregator(dir).Integrate(1, 4, refs)
	if err != nil {
		t.Fatal(err)
	}
	without := cloneManifest(m)
	without.Segments = nil
	for _, r := range m.Segments {
		if r.Kind != SegmentSmallSystemHistoryRouting {
			without.Segments = append(without.Segments, r)
		}
	}
	oracle, err := OpenPinnedManager(dir, without)
	if err != nil {
		t.Fatal(err)
	}
	next := len(m.Segments) - 2
	for pass := 0; pass < 3; pass++ {
		ref, built, err := buildSmallSystemHistoryRoutingContext(context.Background(), dir, m, 2, next, &next)
		if err != nil {
			t.Fatal(err)
		}
		if built {
			m, err = NewAggregator(dir).Integrate(1, 4, []SegmentRef{ref})
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := checkSmallSystemHistoryRoutingContext(context.Background(), dir, m, mustSmallRoutingRef(t, m)); err != nil {
			t.Fatal(err)
		}
		reader, err := OpenPinnedManager(dir, m)
		if err != nil {
			t.Fatal(err)
		}
		for from := uint64(1); from <= 5; from++ {
			got, err := reader.FirstStateDomainChangesContext(context.Background(), from, 5, queries)
			if err != nil {
				t.Fatal(err)
			}
			want, err := oracle.FirstStateDomainChangesContext(context.Background(), from, 5, queries)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("pass=%d from=%d mismatch: %v", pass, from, err)
			}
		}
	}
	route, err := loadSmallSystemHistoryRouting(context.Background(), dir, m)
	if err != nil || len(route.sources) != 4 {
		t.Fatalf("coverage=%+v err=%v", route, err)
	}
}

func TestSmallSystemHistoryRoutingVerifiedSnapshotAndSignedCatalogRestore(t *testing.T) {
	dir, identity, owner := writeVerifiableHistoryManifest(t)
	m, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	m = addSmallRoutingFixture(t, dir, m, 1000)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := PublishSignedSnapshotCatalog(dir, priv); err != nil {
		t.Fatal(err)
	}
	for _, signed := range []bool{false, true} {
		db := rawdb.NewMemoryDatabase()
		if signed {
			result, err := RestoreSnapshotFromVerifiedCatalog(db, dir, identity, []ed25519.PublicKey{pub})
			if err != nil || result.ChangesRestored != 2 || result.TxRangesRestored != 2 {
				db.Close()
				t.Fatalf("signed restore: %+v %v", result, err)
			}
		} else {
			result, err := RestoreSnapshotFromVerifiedManifestWithOptions(db, dir, identity, strictBoundaryOptions(t, db))
			if err != nil || result.ChangesRestored != 2 || result.TxRangesRestored != 2 {
				db.Close()
				t.Fatalf("strict restore: %+v %v", result, err)
			}
		}
		row, ok, err := rawdb.ReadStateDomainChange(db, 1, 1)
		if err != nil || !ok || row.Owner != owner || row.TxNum != 10 || string(row.Prev) != "old-a" {
			db.Close()
			t.Fatalf("restored history: %+v %v", row, err)
		}
		census, err := rawdb.InspectStateKVGenerations(context.Background(), db, true, nil, nil)
		db.Close()
		if err != nil || !census.Complete || !census.MuHashVerified {
			t.Fatalf("restored MuHash: %+v %v", census, err)
		}
	}
	if err := checkSmallSystemHistoryRoutingContext(context.Background(), dir, m, mustSmallRoutingRef(t, m)); err != nil {
		t.Fatal(err)
	}
}

func TestSmallSystemHistoryRoutingMixedFamiliesWithLegacyBroadRoute(t *testing.T) {
	dir, original, queries := routingFixture(t)
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	q := StateHistoryQuery{FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: common.Address{0x41, 11}, Generation: 9, Domain: kvdomains.ContractStorage, Key: make([]byte, 32)}
	q.Key[31] = 9
	prev := make([]byte, 32)
	prev[31] = 7
	if err := rawdb.WriteStateTxRange(db, 4, common.Hash{4}, 4, 4); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{BlockNum: 4, BlockHash: common.Hash{4}, TxNum: 4, FlatDomain: q.FlatDomain, Owner: q.Owner, Generation: q.Generation, Domain: q.Domain, Key: q.Key, PrevExists: true, Prev: prev}); err != nil {
		t.Fatal(err)
	}
	refs, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 4, 4, "history/state-domain-change-mixed-fourth.seg")
	if err != nil {
		t.Fatal(err)
	}
	original, err = NewAggregator(dir).Integrate(1, 4, refs)
	if err != nil {
		t.Fatal(err)
	}
	oracle, err := OpenPinnedManager(dir, original)
	if err != nil {
		t.Fatal(err)
	}
	queries = append(queries, q)
	m := addRoutingFixture(t, dir, original, 64)
	m = addSmallRoutingFixture(t, dir, m, 64)
	for _, legacy := range []bool{false, true} {
		if legacy {
			ref := mustRoutingRef(t, m)
			data, err := os.ReadFile(filepath.Join(dir, ref.Path))
			if err != nil {
				t.Fatal(err)
			}
			n := int(binary.BigEndian.Uint32(data[12:16]))
			off := systemHistoryRoutingHeaderSize + n*32
			encoded := append([]byte(nil), data[:off]...)
			keys := uint32(0)
			for off < len(data) {
				begin := off
				size := int(binary.BigEndian.Uint16(data[off : off+2]))
				off += 2
				key := data[off : off+size]
				off += size
				length := int(binary.BigEndian.Uint32(data[off+1 : off+5]))
				off += 5 + length
				if !contractStorageHistoryRoutingKey(key) {
					encoded = append(encoded, data[begin:off]...)
					keys++
				}
			}
			binary.BigEndian.PutUint32(encoded[8:12], 1)
			binary.BigEndian.PutUint32(encoded[16:20], keys)
			sum := sha256.Sum256(encoded)
			ref.Rows = uint64(keys)
			ref.Size = uint64(len(encoded))
			ref.Checksum = fmt.Sprintf("sha256:%x", sum)
			ref.Path = fmt.Sprintf("history/system-routing-%d-%d-%x.route", ref.FromTxNum, ref.ToTxNum, sum)
			if err := os.WriteFile(filepath.Join(dir, ref.Path), encoded, 0600); err != nil {
				t.Fatal(err)
			}
			m, err = NewAggregator(dir).Integrate(1, 4, []SegmentRef{ref})
			if err != nil {
				t.Fatal(err)
			}
		}
		reader, err := OpenPinnedManager(dir, m)
		if err != nil {
			t.Fatal(err)
		}
		for from := uint64(1); from <= 5; from++ {
			got, err := reader.FirstStateDomainChangesContext(context.Background(), from, 5, queries)
			if err != nil {
				t.Fatal(err)
			}
			want, err := oracle.FirstStateDomainChangesContext(context.Background(), from, 5, queries)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("legacy=%v from=%d mixed family mismatch: %v", legacy, from, err)
			}
			if from <= 4 && (got[len(got)-1] == nil || !bytes.Equal(got[len(got)-1].Prev, prev)) {
				t.Fatal("storage before-image lost")
			}
		}
		if _, err := VerifyLoadedManifestFiles(dir, m, VerifyManifestOptions{RequireRegistered: true, RequireChecksums: true}); err != nil {
			t.Fatal(err)
		}
	}
}
