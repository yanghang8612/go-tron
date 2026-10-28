package snapshots

import (
	"context"
	"github.com/tronprotocol/go-tron/core/maintenance"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestHistoryRoutingBootstrapReusesExactProofsWithoutSourceRescan(t *testing.T) {
	dir, m, _ := routingFixture(t)
	m = addRoutingFixture(t, dir, m, 64)
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	bootstrap, err := loadHistoryRoutingBootstrap(context.Background(), dir, m)
	if err != nil || len(bootstrap.sources) != 3 {
		t.Fatal(bootstrap, err)
	}
	// Independently derive small memberships and metadata nonemptiness from each
	// authenticated canonical dictionary before making the covered sources absent.
	for _, h := range m.Segments {
		if h.Kind != SegmentHistory {
			continue
		}
		a, _ := cfg.HistoryAccessorRef(m, h)
		col := bootstrap.columns[systemHistoryRoutingPairID(h, a)]
		keys, err := smallSystemHistorySourceKeys(context.Background(), dir, h, a)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for key, members := range bootstrap.small {
			if systemHistoryMembersContain(members, col) {
				got = append(got, key)
			}
		}
		if len(got) != len(keys) {
			t.Fatal("small projection differs", col)
		}
		for _, key := range keys {
			if !systemHistoryMembersContain(bootstrap.small[key], col) {
				t.Fatal("missing small key")
			}
		}
		f, err := metadataHistorySourceFilter(context.Background(), dir, h, a)
		if err != nil || bootstrap.metadataNonempty[col] != (f.count > 0) {
			t.Fatal("metadata projection differs", col, err)
		}
	}
	var moved []string
	for _, r := range m.Segments {
		if r.Kind == SegmentAccessor {
			p := filepath.Join(dir, r.Path)
			if err := os.Rename(p, p+".held"); err != nil {
				t.Fatal(err)
			}
			moved = append(moved, p)
		}
	}
	ref, built, err := BuildSmallSystemHistoryRoutingContext(context.Background(), dir, m, 1)
	if err != nil || !built {
		t.Fatal("reusable broad proof reread canonical source", built, err)
	}
	route, err := readSmallSystemHistoryRouting(context.Background(), dir, ref)
	if err != nil || len(route.sources) != 3 {
		t.Fatal(route, err)
	}
	for _, p := range moved {
		if err := os.Rename(p+".held", p); err != nil {
			t.Fatal(err)
		}
	}
	if err := checkSmallSystemHistoryRoutingContext(context.Background(), dir, m, ref); err != nil {
		t.Fatal(err)
	}
	next, err := NewAggregator(dir).Integrate(m.VisibleTxStart, m.VisibleTxEnd, []SegmentRef{ref})
	if err != nil || !reflect.DeepEqual(m.Progress, next.Progress) {
		t.Fatal("frontier changed", err)
	}
	// The projected shared cache cannot bypass current physical authentication.
	broad := mustRoutingRef(t, m)
	p := filepath.Join(dir, broad.Path)
	raw, _ := os.ReadFile(p)
	raw[len(raw)-1] ^= 1
	if err := os.WriteFile(p, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadHistoryRoutingBootstrap(context.Background(), dir, m); err == nil {
		t.Fatal("changed broad bytes bypassed authentication")
	}
}

func TestHistoryRoutingBootstrapWorkBudgetsPublishCompletedPrefix(t *testing.T) {
	for _, family := range []string{"small", "metadata"} {
		t.Run(family, func(t *testing.T) {
			dir, m, _ := routingFixture(t)
			var ref SegmentRef
			var built bool
			var err error
			if family == "small" {
				ref, built, err = buildSmallSystemHistoryRoutingWithBudget(context.Background(), dir, m, 256, 0, nil, smallSystemRoutingWorkLimit{bytes: 1})
			} else {
				ref, built, err = buildMetadataHistoryRoutingWithBudget(context.Background(), dir, m, 256, 0, nil, smallSystemRoutingWorkLimit{bytes: 1})
			}
			if err != nil || !built {
				t.Fatal("completed first source was discarded", built, err)
			}
			if family == "small" {
				route, err := readSmallSystemHistoryRouting(context.Background(), dir, ref)
				if err != nil || len(route.sources) != 1 {
					t.Fatal("source budget not bounded", route, err)
				}
			} else if ref.Rows != 1 {
				t.Fatal("metadata heavy reads exceeded one source", ref.Rows)
			}
		})
	}
}

func TestHistoryRoutingBootstrapMaintenanceDoesNotStarveMetadata(t *testing.T) {
	dir, m := smallManyRoutingFixture(t)
	r := NewRunner(nil, Config{Enabled: true, Dir: dir, HistoryDataset: SegmentDatasetStateDomainChange, HeavyWorkGate: maintenance.NewHeavyWorkGate()})
	first, err := r.compactHistory(context.Background(), false)
	if err != nil || !first.RoutingBuilt {
		t.Fatal(first, err)
	}
	second, err := r.compactHistory(context.Background(), false)
	if err != nil || !second.RoutingBuilt {
		t.Fatal(second, err)
	}
	next, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	var metadata uint64
	for _, ref := range next.Segments {
		if ref.Kind == SegmentMetadataHistoryRouting {
			metadata += ref.Rows
		}
	}
	if metadata == 0 || !reflect.DeepEqual(m.Progress, next.Progress) {
		t.Fatal("metadata starved or progress changed", metadata)
	}
}
