package snapshots

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/maintenance"
)

func TestAccountRoutingCompactionExpansionRejection(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	var refs []SegmentRef
	for tx := uint64(1); tx <= 65; tx++ {
		refs = append(refs, buildRoutingBudgetBenchmarkHistory(t, dir, tx, tx, 1)...)
	}
	original := NewManifest(1, 65, refs)
	if err := PublishManifest(dir, original); err != nil {
		t.Fatal(err)
	}
	original = addAccountRoutingFixture(t, dir, original, 1)
	for _, refresh := range []func() (bool, error){
		func() (bool, error) { return refreshSmallSystemHistoryRoutingContext(ctx, dir, false) },
		func() (bool, error) { return refreshMetadataHistoryRoutingContext(ctx, dir, false) },
		func() (bool, error) {
			return refreshSystemHistoryRoutingWithStateContext(ctx, dir, false, nil, time.Now())
		},
	} {
		for i := 0; ; i++ {
			built, err := refresh()
			if err != nil || i >= 4 {
				t.Fatalf("prime other routing families: built=%t err=%v", built, err)
			}
			if !built {
				break
			}
		}
	}
	original, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	realRoute := mustAccountRoutingRef(t, original)

	for _, tc := range []struct {
		name             string
		rows             uint64
		end              uint64
		baseKeys         uint64
		redLight         string
		wantProbe, built bool
	}{
		{"hard no build", accountRoutingExpansionMinRows, 65, 0, "hard", true, false},
		{"CPU memory no build", accountRoutingExpansionMinRows, 65, 0, "capacity", true, false},
		{"hard after safe base work", realRoute.Rows, 65, 1, "hard", true, true},
		{"CPU memory after safe base work", realRoute.Rows, 65, 1, "capacity", true, true},
		{"below row threshold", accountRoutingExpansionMinRows - 1, 65, 0, "hard", false, false},
		{"above max-row headroom", accountRoutingMaxRows - accountRoutingExpandedDictionaryKeys + 1, 65, 0, "hard", false, false},
		{"63 uncovered", accountRoutingExpansionMinRows, 64, 0, "hard", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := cloneDecodedManifest(original)
			m.Segments = slices.DeleteFunc(m.Segments, func(ref SegmentRef) bool {
				if ref.Kind == SegmentAccountHistoryRouting {
					return true
				}
				// Keep the primed routing sidecars in the 63-uncovered case.
				return ref.FromTxNum > tc.end &&
					(ref.Kind == SegmentHistory || ref.Kind == SegmentAccessor || ref.Kind == SegmentInverted)
			})
			route := realRoute
			if tc.rows != realRoute.Rows {
				route = sparseAccountRoutingMetadataFixture(t, dir, m, tc.rows)
			}
			m.Segments = append(m.Segments, route)
			if err := PublishManifest(dir, m); err != nil {
				t.Fatal(err)
			}

			now := time.Now()
			var ordinary, probes, capacity int
			probe := func() maintenance.StoragePressure { return healthyHistoryLoad(time.Now()) }
			r := NewRunner(nil, Config{
				Enabled: true, Dir: dir, HistoryDataset: SegmentDatasetStateDomainChange,
				HistoryCatchupMode: HistoryCatchupThroughput, RetainObsoleteSegments: true,
				HeavyWorkGate:    maintenance.NewHeavyWorkGate(),
				HistoryLoadProbe: func() maintenance.StoragePressure { ordinary++; return probe() },
				HistoryRecoveryLoadProbe: func() maintenance.StoragePressure {
					probes++
					p := probe()
					if tc.redLight == "hard" {
						p.MemTableCount = int64(p.MemTableStopWritesThreshold)
					}
					return p
				},
				BusyHistoryBuildReady: func() bool { capacity++; return false },
			})
			r.historyLoad.level, r.historyLoad.good = 3, 3
			r.historyLoad.sample, r.historyLoad.lastAccepted = healthyHistoryLoad(now), now
			limits := defaultAccountRoutingRefreshLimits()
			// Scale only the key charge. The fake high-row cases still use
			// unmodified production row/64-source admission thresholds.
			limits.baseKeys, limits.extraKeys = tc.baseKeys, 1
			if tc.built {
				limits.expansionMinRows = realRoute.Rows
			}
			result, err := r.compactHistoryWithRoutingLimits(ctx, true, limits)
			wantProbes, wantCapacity := 0, 0
			if tc.wantProbe {
				wantProbes = 1
				if tc.redLight == "capacity" {
					wantCapacity = 1
				}
			}
			if err != nil || result.RoutingBuilt != tc.built || ordinary != 1 || probes != wantProbes {
				t.Fatalf("result=%+v ordinary=%d probes=%d err=%v", result, ordinary, probes, err)
			}
			if capacity != wantCapacity {
				t.Fatalf("independent capacity calls %d, want %d", capacity, wantCapacity)
			}
			if tc.wantProbe {
				if result.Merged || r.compactionBudget.outerMetrics != nil || result.Recovery <= 0 ||
					(!tc.built && (!result.Deferred || result.DeferReason != "import-load")) {
					t.Fatalf("red light admitted new leaf work or lost recovery: %+v", result)
				}
			} else if r.compactionBudget.outerMetrics == nil {
				t.Fatalf("baseline leaf path not reached: %+v", result)
			}
			after, err := LoadProductionManifest(dir)
			if err != nil {
				t.Fatal(err)
			}
			got := mustAccountRoutingRef(t, after)
			if tc.built {
				if got.ToTxNum != 2 {
					t.Fatalf("published extra work or lost safe work: %+v", got)
				}
				if err := CheckAccountHistoryRouting(dir, got); err != nil {
					t.Fatal(err)
				}
			} else if tc.wantProbe && got != route {
				t.Fatalf("unexpected route replacement: %+v", got)
			}
		})
	}
}

// Only metadata can be consumed in the admission-only cases. A sparse extent
// gives the reader the real size and authenticated roots at the row boundary.
// Its unread zero pages deliberately fail normal page authentication.
func sparseAccountRoutingMetadataFixture(t *testing.T, dir string, manifest *Manifest, rows uint64) SegmentRef {
	t.Helper()
	m := manifestLookupView(manifest)
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	h := accountRoutingHeader{sources: 1, rows: rows, pages: ceilDiv(rows, accountRoutingPageRecords), from: 1, to: 1}
	h.directories = ceilDiv(h.pages, accountRoutingDirectoryEntries)
	data := make([]byte, h.metadataBytes())
	copy(data, accountRoutingMagic[:])
	binary.BigEndian.PutUint32(data[8:], 1)
	binary.BigEndian.PutUint32(data[12:], 1)
	for i, field := range []uint64{h.rows, h.pages, h.directories, h.from, h.to} {
		binary.BigEndian.PutUint64(data[16+i*8:], field)
	}
	for _, ref := range m.Segments {
		if ref.Kind == SegmentHistory && ref.FromTxNum == 1 {
			a, ok := cfg.HistoryAccessorRef(m, ref)
			if !ok {
				t.Fatal("missing first accessor")
			}
			id := systemHistoryRoutingPairID(ref, a)
			copy(data[64:], id[:])
			break
		}
	}
	for i := uint64(0); i < h.directories; i++ {
		key, _ := benchmarkRouteKey(i + 1)
		copy(data[96+i*accountRoutingDirectoryEntryBytes:], key[:])
	}
	hash := sha256.Sum256(data)
	ref := SegmentRef{
		Dataset: SegmentDatasetStateDomainChange, Kind: SegmentAccountHistoryRouting,
		Rows: rows, FromTxNum: 1, ToTxNum: 1, Size: h.fileBytes(),
		Path:     fmt.Sprintf("history/account-routing-1-1-%x.route", hash),
		Checksum: fmt.Sprintf("sha256:%x", hash), LookupRoot: fmt.Sprintf("sha256:%x", hash),
	}
	f, err := os.Create(filepath.Join(dir, ref.Path))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(int64(h.fileBytes())); err != nil {
		t.Fatal(err)
	}
	return ref
}
