package snapshots

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
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

func routingFixture(t *testing.T) (string, *Manifest, []StateHistoryQuery) {
	t.Helper()
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	zero := (common.AccountID{}).Address(common.AddressPrefixMainnet)
	queries := []StateHistoryQuery{
		{FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: common.SystemAccountAddress, Generation: 0, Domain: kvdomains.SystemDynamicProperty, Key: []byte("alpha")},
		{FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: common.SystemAccountAddress, Generation: 7, Domain: kvdomains.SystemForkVote, Key: []byte("vote")},
		{FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: common.SystemAccountAddress, Generation: math.MaxUint64, Domain: kvdomains.SystemDynamicProperty, Key: []byte("last")},
		{FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: common.SystemAccountAddress},
		{FlatDomain: rawdb.StateFlatDomainKVGeneration, Owner: zero},
		{FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: common.Address{0x41, 3}, Domain: kvdomains.SystemDynamicProperty, Key: []byte("alpha")},
		{FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: common.SystemAccountAddress, Domain: kvdomains.SystemAsset, Key: []byte("other")},
		{FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: common.Address{0x41, 3}, Generation: 3, Domain: kvdomains.ContractMetadata, Key: []byte("meta")},
	}
	var refs []SegmentRef
	for tx := uint64(1); tx <= 3; tx++ {
		if err := rawdb.WriteStateTxRange(db, tx, common.Hash{byte(tx)}, tx, tx); err != nil {
			t.Fatal(err)
		}
		for i, q := range queries {
			if tx != 2 && (i == 1 || i == 4) {
				continue
			}
			row := &rawdb.StateDomainChange{BlockNum: tx, BlockHash: common.Hash{byte(tx)}, TxNum: tx, Seq: uint64(i), FlatDomain: q.FlatDomain, Owner: q.Owner, Generation: q.Generation, Domain: q.Domain, Key: q.Key, PrevExists: tx != 1, Prev: []byte{byte(tx), byte(i)}}
			if !row.PrevExists {
				row.Prev = nil
			}
			if q.FlatDomain == rawdb.StateFlatDomainKVGeneration && row.PrevExists {
				row.Prev = rawdb.EncodeStateKVGenerationValue(tx)
			}
			if err := rawdb.WriteStateDomainChange(db, row); err != nil {
				t.Fatal(err)
			}
		}
		built, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, tx, tx, fmt.Sprintf("history/state-domain-change-route-%d.seg", tx))
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, built...)
	}
	m := NewManifest(1, 3, refs)
	if err := PublishManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	missing := queries[0]
	missing.Key = []byte("missing")
	otherGeneration := queries[0]
	otherGeneration.Generation = 8
	queries = append(queries, missing, otherGeneration, StateHistoryQuery{FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: zero})
	return dir, m, queries
}

func addRoutingFixture(t *testing.T, dir string, m *Manifest, maxNew int) *Manifest {
	t.Helper()
	ref, built, err := BuildSystemHistoryRoutingContext(context.Background(), dir, m, maxNew)
	if err != nil || !built {
		t.Fatalf("build routing: built=%v err=%v", built, err)
	}
	next, err := NewAggregator(dir).Integrate(m.VisibleTxStart, m.VisibleTxEnd, []SegmentRef{ref})
	if err != nil {
		t.Fatal(err)
	}
	return next
}

func TestSystemHistoryRoutingPartialAndCompleteMatchFullHistory(t *testing.T) {
	dir, original, queries := routingFixture(t)
	oracle, err := OpenPinnedManager(dir, original)
	if err != nil {
		t.Fatal(err)
	}
	m := original
	for pass := 0; pass < 3; pass++ {
		m = addRoutingFixture(t, dir, m, 1)
		route, err := loadSystemHistoryRouting(context.Background(), dir, m)
		if err != nil {
			t.Fatal(err)
		}
		if len(route.sources) != pass+1 {
			t.Fatalf("coverage=%d", len(route.sources))
		}
		if err := checkSystemHistoryRoutingContext(context.Background(), dir, m, mustRoutingRef(t, m)); err != nil {
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
	if _, built, err := BuildSystemHistoryRoutingContext(context.Background(), dir, m, 64); err != nil || built {
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

func mustRoutingRef(t *testing.T, m *Manifest) SegmentRef {
	t.Helper()
	for _, ref := range m.Segments {
		if ref.Kind == SegmentSystemHistoryRouting {
			return ref
		}
	}
	t.Fatal("missing routing ref")
	return SegmentRef{}
}

func TestSystemHistoryRoutingStaleIdentityFallsBackAndCorruptionFails(t *testing.T) {
	dir, original, queries := routingFixture(t)
	m := addRoutingFixture(t, dir, original, 64)
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
	ref := mustRoutingRef(t, m)
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

func TestSystemHistoryRoutingStrictCheckDetectsOmittedKey(t *testing.T) {
	dir, m, queries := routingFixture(t)
	m = addRoutingFixture(t, dir, m, 64)
	ref := mustRoutingRef(t, m)
	data, err := os.ReadFile(filepath.Join(dir, ref.Path))
	if err != nil {
		t.Fatal(err)
	}
	route, err := decodeSystemHistoryRouting(data, ref)
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
	if _, err := readSystemHistoryRouting(context.Background(), dir, ref); err != nil {
		t.Fatal(err)
	}
	if err := checkSystemHistoryRoutingContext(context.Background(), dir, m, ref); err == nil {
		t.Fatal("semantic omission accepted by snapshot checker")
	}
}

func TestSystemHistoryRoutingBoundsCancelAndPairIdentity(t *testing.T) {
	dir, m, _ := routingFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, built, err := BuildSystemHistoryRoutingContext(ctx, dir, m, 64); built || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled build: %v %v", built, err)
	}
	m = addRoutingFixture(t, dir, m, 64)
	ref := mustRoutingRef(t, m)
	data, err := os.ReadFile(filepath.Join(dir, ref.Path))
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []int{12, 16} {
		bad := append([]byte(nil), data...)
		binary.BigEndian.PutUint32(bad[field:field+4], math.MaxUint32)
		if _, err := decodeSystemHistoryRouting(bad, ref); err == nil {
			t.Fatal("oversized count accepted")
		}
	}
	for _, n := range []int{0, 8, 39, len(data) - 1} {
		if _, err := decodeSystemHistoryRouting(data[:n], ref); err == nil {
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

func TestSystemHistoryRoutingReplacementAndSparseMembership(t *testing.T) {
	dir, m, queries := routingFixture(t)
	m = addRoutingFixture(t, dir, m, 64)
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
			m = addRoutingFixture(t, dir, m, 64)
		}
	}
	// A sparse list stores one uint32 column rather than a 9-byte bitmap.
	const n = 65
	key := stateDomainChangeBinaryAccessorLookupKey(rawdb.StateFlatDomainKVLatest, common.Address{0x41, 7}, 0, kvdomains.ContractMetadata, []byte("meta"))
	data := make([]byte, systemHistoryRoutingHeaderSize)
	copy(data, systemHistoryRoutingMagic[:])
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
	route, err := decodeSystemHistoryRouting(data, ref)
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
	if _, err := decodeSystemHistoryRouting(bad, ref); err == nil {
		t.Fatal("sparse out-of-bounds column accepted")
	}
}

func TestSystemHistoryRoutingRefreshHonorsBusyBatch(t *testing.T) {
	dir, m, _ := routingFixture(t)
	if built, err := refreshSystemHistoryRoutingContext(context.Background(), dir, true); err != nil || built {
		t.Fatalf("small catalog should use normal readers: %v %v", built, err)
	}
	m = addRoutingFixture(t, dir, m, 1)
	if built, err := refreshSystemHistoryRoutingContext(context.Background(), dir, true); err != nil || built {
		t.Fatalf("busy tail admitted: %v %v", built, err)
	}
	if built, err := refreshSystemHistoryRoutingContext(context.Background(), dir, false); err != nil || !built {
		t.Fatalf("idle tail deferred: %v %v", built, err)
	}
	current, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	route, err := loadSystemHistoryRouting(context.Background(), dir, current)
	if err != nil || len(route.sources) != 3 {
		t.Fatalf("idle coverage: %+v %v", route, err)
	}
	if err := CheckSystemHistoryRouting(dir, mustRoutingRef(t, current)); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyLoadedManifestFiles(dir, current, VerifyManifestOptions{RequireRegistered: true, RequireChecksums: true}); err != nil {
		t.Fatal(err)
	}
}
