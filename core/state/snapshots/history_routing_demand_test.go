package snapshots

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestStorageLightRoutingCompleteDoesNotLoadBroad(t *testing.T) {
	dir, m, queries := storageRoutingFixture(t)
	oracle, _ := OpenPinnedManager(dir, m)
	want, err := oracle.FirstStateDomainChangesContext(context.Background(), 1, 3, queries[:12])
	if err != nil {
		t.Fatal(err)
	}
	m = addRoutingFixture(t, dir, m, 3)
	for range 3 {
		m = addStorageFilterFixture(t, dir, m, 1)
	}
	broad := mustRoutingRef(t, m)
	if err = os.Rename(filepath.Join(dir, broad.Path), filepath.Join(dir, broad.Path)+".held"); err != nil {
		t.Fatal(err)
	}
	reader, _ := OpenPinnedManager(dir, m)
	got, err := reader.FirstStateDomainChangesContext(context.Background(), 1, 3, queries[:12])
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("complete light storage route depended on broad", err, got, want)
	}
	// Coverage belongs to an exact pair. Altering one source must invalidate its
	// light absence and demand the missing broad object rather than hide a row.
	stale := cloneManifest(m)
	for i := range stale.Segments {
		if stale.Segments[i].Kind == SegmentAccessor && stale.Segments[i].FromTxNum == 1 {
			stale.Segments[i].Checksum = "sha256:" + string(bytes.Repeat([]byte{'1'}, 64))
		}
	}
	reader, _ = OpenPinnedManager(dir, stale)
	if _, err = reader.FirstStateDomainChangesContext(context.Background(), 1, 3, queries[:1]); err == nil {
		t.Fatal("replacement pair inherited a light negative")
	}
}
func TestStorageRoutingBootstrapMatchesCanonicalFiltersAndRestoration(t *testing.T) {
	dir, original, queries := storageRoutingFixture(t)
	m := addRoutingFixture(t, dir, original, 3)
	ref := mustRoutingRef(t, m)
	data, err := os.ReadFile(filepath.Join(dir, ref.Path))
	if err != nil {
		t.Fatal(err)
	}
	b, err := projectStorageHistoryRouting(context.Background(), data, ref)
	if err != nil || b == nil {
		t.Fatal(err)
	}
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	for _, h := range m.Segments {
		if h.Kind != SegmentHistory {
			continue
		}
		a, _ := cfg.HistoryAccessorRef(m, h)
		id := systemHistoryRoutingPairID(h, a)
		canonical, err := storageHistorySourceFilter(context.Background(), dir, h, a)
		if err != nil {
			t.Fatal(err)
		}
		col, ok := b.route.columns[id]
		if !ok || !reflect.DeepEqual(b.filters[col], canonical) {
			t.Fatal("projection differs from canonical dictionary", h, col)
		}
	}
	next := m
	for range 3 {
		ref, built, err := buildStorageHistoryRoutingWithPolicy(context.Background(), dir, manifestLookupView(next), 1, 0, nil, storageRoutingBuildPolicy{bootstrap: b})
		if err != nil || !built {
			t.Fatal(built, err)
		}
		next, err = publishPagedHistoryRoutingContext(context.Background(), dir, next, ref, SegmentStorageHistoryRouting)
		if err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(original.Progress, next.Progress) {
		t.Fatal("changed frontiers")
	}
	oracle, _ := OpenPinnedManager(dir, original)
	reader, _ := OpenPinnedManager(dir, next)
	for from := uint64(1); from <= 3; from++ {
		want, err := oracle.FirstStateDomainChangesContext(context.Background(), from, 3, queries[:12])
		if err != nil {
			t.Fatal(err)
		}
		got, err := reader.FirstStateDomainChangesContext(context.Background(), from, 3, queries[:12])
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatal(from, err, got, want)
		}
	}
	bad := bytes.Clone(data)
	bad[len(bad)-1] = 0
	if _, err = projectStorageHistoryRouting(context.Background(), bad, ref); err == nil {
		t.Fatal("invalid projection accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = projectStorageHistoryRouting(ctx, data, ref); err != context.Canceled {
		t.Fatal(err)
	}
}
func TestRoutingImageCacheReuseAndSameMtimeCorruption(t *testing.T) {
	dir, m, queries := storageRoutingFixture(t)
	m = addRoutingFixture(t, dir, m, 3)
	ref := mustRoutingRef(t, m)
	path := filepath.Join(dir, ref.Path)
	first, err := readSystemHistoryRoutingImage(context.Background(), dir, ref)
	if err != nil {
		t.Fatal(err)
	}
	again, err := readSystemHistoryRoutingImage(context.Background(), dir, ref)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, known := historyStagingChangeTime(info); known && &first.data[0] != &again.data[0] {
		t.Fatal("owned authenticated image not reused")
	}
	for _, q := range queries[:12] {
		key := stateDomainChangeBinaryAccessorLookupKey(q.FlatDomain, q.Owner, q.Generation, q.Domain, q.Key)
		wanted := map[string]bool{string(normalizedHistoryRoutingKey(key)): true}
		selected, err := first.index.selected(context.Background(), first.data, ref, wanted)
		if err != nil {
			t.Fatal(err)
		}
		full, err := decodeHistoryRoutingFamilySelected(context.Background(), first.data, ref, false, wanted)
		if err != nil || !reflect.DeepEqual(selected, full) {
			t.Fatal(err, selected, full)
		}
	}
	changed := bytes.Clone(first.data)
	changed[len(changed)-1] ^= 1
	if err = os.WriteFile(path, changed, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	if _, err = readSystemHistoryRoutingImage(context.Background(), dir, ref); err == nil {
		t.Fatal("restored mtime bypassed corruption detection")
	}
}

func TestStorageRoutingBootstrapRejectsChangedPhysicalProof(t *testing.T) {
	dir, m, _ := storageRoutingFixture(t)
	m = addRoutingFixture(t, dir, m, 3)
	// Runtime admission requires existing storage indexing for a tiny fixture.
	m = addStorageFilterFixture(t, dir, m, 1)
	state := storageRoutingRefreshState{}
	now := time.Now()
	if built, err := refreshStorageHistoryRoutingContext(context.Background(), dir, &state, now); err != nil || !built {
		t.Fatal(built, err)
	}
	// Depending on tail batching the first pass fills modern holes. The cached
	// legacy projection must recheck its physical parent before publishing more.
	broad := mustRoutingRef(t, m)
	path := filepath.Join(dir, broad.Path)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 1
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	if built, err := refreshStorageHistoryRoutingContext(context.Background(), dir, &state, now.Add(time.Second)); err == nil || built {
		t.Fatal("changed physical broad published a new proof", built, err)
	}
}
