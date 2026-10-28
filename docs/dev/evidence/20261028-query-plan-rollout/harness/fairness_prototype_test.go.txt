package snapshots

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/tronprotocol/go-tron/core/maintenance"
)

func TestHistoryRoutingMaintenanceAccountsBeforeLightweightDebtCompletes(t *testing.T) {
	dir, before := smallManyRoutingFixtureSources(t, 350)
	r := NewRunner(nil, Config{Enabled: true, Dir: dir, HistoryDataset: SegmentDatasetStateDomainChange, HeavyWorkGate: maintenance.NewHeavyWorkGate()})
	for pass := 0; pass < 3; pass++ {
		result, err := r.compactHistory(context.Background(), false)
		if err != nil || !result.RoutingBuilt {
			t.Fatalf("pass %d: %+v, %v", pass, result, err)
		}
	}
	after, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	ref, ok := accountRoutingRef(after, 0, ^uint64(0))
	if !ok {
		t.Fatal("account family starved behind a second small-system batch")
	}
	accounts, err := openAccountHistoryRouting(context.Background(), dir, ref)
	if err != nil {
		t.Fatal(err)
	}
	defer accounts.Close()
	small, err := loadSmallSystemHistoryRouting(context.Background(), dir, after)
	if err != nil {
		t.Fatal(err)
	}
	if len(accounts.metadata.covered) == 0 || len(small.sources) >= 350 || !reflect.DeepEqual(before.Progress, after.Progress) {
		t.Fatal("account progress must coexist with unfinished lightweight debt and unchanged canonical progress")
	}
}

func TestHistoryRoutingMaintenanceContinuousFamiliesAndUnavailableWork(t *testing.T) {
	var s historyCompactionBudgetState
	var sequence []int
	var families [4]func() (bool, error)
	for i := range families {
		families[i] = func() (bool, error) { sequence = append(sequence, i); return true, nil }
	}
	for range 12 {
		if built, err := s.refreshRoutingFamilies(families); !built || err != nil {
			t.Fatal(built, err)
		}
	}
	if !reflect.DeepEqual(sequence, []int{0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3}) {
		t.Fatal("a productive family starved another", sequence)
	}
	// No-work and a budget-deferred broad family do not deny account work.
	sequence = nil
	families[0] = func() (bool, error) { sequence = append(sequence, 0); return false, nil }
	families[1] = func() (bool, error) { sequence = append(sequence, 1); return false, nil }
	families[3] = func() (bool, error) { sequence = append(sequence, 3); return false, nil }
	for range 3 {
		if built, err := s.refreshRoutingFamilies(families); !built || err != nil {
			t.Fatal(built, err)
		}
	}
	if !reflect.DeepEqual(sequence, []int{0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2}) {
		t.Fatal("cursor did not advance past the actual productive family", sequence)
	}
}

func TestHistoryRoutingMaintenancePropagatesErrorWithoutStarvingNeighbors(t *testing.T) {
	var s historyCompactionBudgetState
	want := errors.New("routing file corrupt")
	var calls [4]int
	var families [4]func() (bool, error)
	for i := range families {
		families[i] = func() (bool, error) { calls[i]++; return true, nil }
	}
	families[0] = func() (bool, error) { calls[0]++; return false, want }
	if built, err := s.refreshRoutingFamilies(families); built || !errors.Is(err, want) || calls != [4]int{1, 0, 0, 0} {
		t.Fatal("error was hidden or work continued after it", built, err, calls)
	}
	if built, err := s.refreshRoutingFamilies(families); !built || err != nil || calls != [4]int{1, 1, 0, 0} {
		t.Fatal("failed family denied the next family a turn", built, err, calls)
	}
}
