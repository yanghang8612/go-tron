package snapshots

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

func TestManifestLookupPreservesCompanionIdentity(t *testing.T) {
	base := manifestValidationFixture(32)
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	for _, mutation := range []string{"none", "range", "steps", "dataset", "kind", "path", "duplicate"} {
		t.Run(mutation, func(t *testing.T) {
			m := cloneManifest(base)
			for i := range m.Segments {
				ref := &m.Segments[i]
				if ref.Kind != SegmentAccessor || ref.FromTxNum != 150 {
					continue
				}
				switch mutation {
				case "range":
					ref.ToTxNum++
				case "steps":
					ref.AggregationSteps = 2
				case "dataset":
					ref.Dataset = SegmentDatasetCode
				case "kind":
					ref.Kind = SegmentInverted
				case "path":
					ref.Path = "other.kv"
				case "duplicate":
					// An invalid same-path row before the correct row must not hide
					// the later match in generic, unvalidated function inputs.
					correct := *ref
					ref.ToTxNum++
					m.Segments = append(m.Segments, correct)
				}
				break
			}
			view := manifestLookupView(m)
			if m.lookup != nil || view.lookup == nil {
				t.Fatal("index attached to externally mutable input")
			}
			for _, history := range m.Segments {
				if history.Kind != SegmentHistory {
					continue
				}
				want, wantOK := cfg.HistoryAccessorRef(m, history)
				got, gotOK := cfg.HistoryAccessorRef(view, history)
				if got != want || gotOK != wantOK {
					t.Fatalf("lookup differs: got %+v/%v want %+v/%v", got, gotOK, want, wantOK)
				}
			}
		})
	}
}

func TestManifestLookupMutableCopiesAndCompactor(t *testing.T) {
	base := cachedManifestFixture(8)
	manager, err := OpenPinnedManager(t.TempDir(), base)
	if err != nil {
		t.Fatal(err)
	}
	view, err := manager.currentManifest()
	if err != nil || view.lookup == nil {
		t.Fatalf("private index missing: %v", err)
	}
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	for _, copied := range []*Manifest{cloneManifest(view), manager.Manifest(), base} {
		if copied.lookup != nil {
			t.Fatal("mutable copy retained an immutable index")
		}
		if got := len(historyCompactionCandidates(copied, cfg)); got != 8 {
			t.Fatalf("candidates before mutation = %d", got)
		}
		for i := range copied.Segments {
			if copied.Segments[i].Kind == SegmentAccessor {
				copied.Segments[i].Path = "changed.kv"
				break
			}
		}
		if got := len(historyCompactionCandidates(copied, cfg)); got != 7 {
			t.Fatalf("candidates ignored caller mutation: %d", got)
		}
		if copied.lookup != nil {
			t.Fatal("bulk operation indexed caller-owned object in place")
		}
	}
	if got := len(historyCompactionCandidates(view, cfg)); got != 8 {
		t.Fatalf("public mutation changed pinned view: %d", got)
	}
}

func TestManifestLookupManagerReloadAndPublicationBytes(t *testing.T) {
	dir := t.TempDir()
	m := cachedManifestFixture(8)
	if err := PublishManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, ManifestFile))
	if err != nil {
		t.Fatal(err)
	}
	wantJSON, err := json.Marshal(m)
	if err != nil || !bytes.Equal(raw, wantJSON) {
		t.Fatalf("publication is not compact schema-equivalent JSON: %v", err)
	}
	manager, err := OpenManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	old, err := manager.currentManifest()
	if err != nil || old.lookup == nil {
		t.Fatalf("initial index missing: %v", err)
	}
	changed := manager.Manifest()
	for i := range changed.Segments {
		changed.Segments[i].Size++
	}
	// Atomic replacement must install fresh row identities even when generation
	// and PublishedUnix do not change. The original immutable view stays valid.
	if err := PublishManifest(dir, changed); err != nil {
		t.Fatal(err)
	}
	next, err := manager.currentManifest()
	if err != nil || next.lookup == nil || next.lookup == old.lookup {
		t.Fatalf("replacement did not replace index: %v", err)
	}
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	for _, history := range changed.Segments {
		if history.Kind != SegmentHistory {
			continue
		}
		before, ok := cfg.HistoryAccessorRef(old, history)
		after, found := cfg.HistoryAccessorRef(next, history)
		if !ok || !found || after.Size != before.Size+1 {
			t.Fatal("generation replacement reused stale companion metadata")
		}
	}
	loaded, err := LoadProductionManifest(dir)
	if err != nil || loaded.lookup != nil || !reflect.DeepEqual(loaded, changed) {
		t.Fatalf("public loader changed mutable/JSON contract: %v", err)
	}
}

func TestManifestLookupFreezerOrderingAndConcurrentCopies(t *testing.T) {
	refs := []SegmentRef{
		{Kind: SegmentChainFreezer, FromTxNum: 10, ToTxNum: 19, Path: "b"},
		{Dataset: SegmentDatasetChainFreezer, Kind: SegmentChainFreezer, FromTxNum: 0, ToTxNum: 9, Path: "a"},
		{Dataset: SegmentDatasetChainFreezer, Kind: SegmentChainFreezer, FromTxNum: 0, ToTxNum: 19, Path: "c"},
		{Dataset: SegmentDatasetCode, Kind: SegmentChainFreezer, FromTxNum: 100, ToTxNum: 200, Path: "excluded"},
	}
	m := &Manifest{Segments: refs}
	view := manifestLookupView(m)
	want := chainFreezerRefs(m)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				got := chainFreezerRefs(view)
				if !reflect.DeepEqual(got, want) {
					t.Error("indexed freezer order differs from scan")
					return
				}
				got[0].Path = "mutated"
			}
		}()
	}
	wg.Wait()
}

func BenchmarkManifestDerivedLookups(b *testing.B) {
	for _, groups := range []int{2048, 8192} {
		b.Run(fmt.Sprintf("groups=%d", groups), func(b *testing.B) {
			m := cachedManifestFixture(groups)
			view := manifestLookupView(m)
			cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
			var histories []SegmentRef
			for _, ref := range m.Segments {
				if ref.Kind == SegmentHistory {
					histories = append(histories, ref)
				}
			}
			for _, mode := range []string{"scan", "indexed", "build-index-and-lookup"} {
				b.Run("companions/"+mode, func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						current := m
						if mode == "indexed" {
							current = view
						} else if mode == "build-index-and-lookup" {
							current = manifestLookupView(m)
						}
						for _, history := range histories {
							if _, ok := cfg.HistoryAccessorRef(current, history); !ok {
								b.Fatal("missing accessor")
							}
							if _, ok := cfg.HistoryIndexRef(current, history); !ok {
								b.Fatal("missing index")
							}
						}
					}
				})
			}
			for _, mode := range []string{"scan", "indexed"} {
				b.Run("freezer-miss/"+mode, func(b *testing.B) {
					current := m
					if mode == "indexed" {
						current = view
					}
					b.ReportAllocs()
					for b.Loop() {
						if len(chainFreezerRefs(current)) != 0 {
							b.Fatal("unexpected freezer")
						}
					}
				})
			}
		})
	}
}

func BenchmarkManifestPublicationEncoding(b *testing.B) {
	m := cachedManifestFixture(6000)
	for _, pretty := range []bool{true, false} {
		b.Run(fmt.Sprintf("pretty=%v", pretty), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var raw []byte
				var err error
				if pretty {
					raw, err = json.MarshalIndent(m, "", "  ")
				} else {
					raw, err = json.Marshal(m)
				}
				if err != nil {
					b.Fatal(err)
				}
				b.ReportMetric(float64(len(raw)), "encoded-B")
			}
		})
	}
}
