package state

import (
	"testing"

	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// A staking-opcode weight delta applied inside a snapshot must roll back on
// RevertToSnapshot. java drops the discardable Repository's total_*_weight delta
// on revert; gtron mutates the shared DynamicProperties directly (Set is not
// journaled), so the delta has to be journaled explicitly. Regression for the
// Nile 27,405,576 total_energy_weight over-count, whose source was a
// freeze-opcode-then-revert leaking the FREEZE weight delta.
func TestAddResourceWeightJournaled_RevertRollsBackDelta(t *testing.T) {
	cases := []struct {
		name     string
		resource corepb.ResourceCode
		get      func(*DynamicProperties) int64
		set      func(*DynamicProperties, int64)
	}{
		{"energy", corepb.ResourceCode_ENERGY, (*DynamicProperties).TotalEnergyWeight, (*DynamicProperties).SetTotalEnergyWeight},
		{"bandwidth", corepb.ResourceCode_BANDWIDTH, (*DynamicProperties).TotalNetWeight, (*DynamicProperties).SetTotalNetWeight},
		{"tron_power", corepb.ResourceCode_TRON_POWER, (*DynamicProperties).TotalTronPowerWeight, (*DynamicProperties).SetTotalTronPowerWeight},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestStateDB(t)
			dp := s.DynamicProperties()
			tc.set(dp, 1000)

			// FREEZE opcode then frame revert: the +delta must vanish.
			snap := s.Snapshot()
			s.AddResourceWeightJournaled(dp, tc.resource, 5)
			if got := tc.get(dp); got != 1005 {
				t.Fatalf("after journaled add: weight=%d, want 1005", got)
			}
			s.RevertToSnapshot(snap)
			if got := tc.get(dp); got != 1000 {
				t.Fatalf("after revert: weight=%d, want 1000 (delta leaked)", got)
			}

			// SELFDESTRUCT release (negative) then frame revert: must roll back.
			snap = s.Snapshot()
			s.AddResourceWeightJournaled(dp, tc.resource, -300)
			if got := tc.get(dp); got != 700 {
				t.Fatalf("after journaled release: weight=%d, want 700", got)
			}
			s.RevertToSnapshot(snap)
			if got := tc.get(dp); got != 1000 {
				t.Fatalf("after revert of release: weight=%d, want 1000", got)
			}

			// A committed (never reverted) delta persists.
			s.AddResourceWeightJournaled(dp, tc.resource, 7)
			if got := tc.get(dp); got != 1007 {
				t.Fatalf("committed weight=%d, want 1007", got)
			}
		})
	}
}

// Reverting to an inner snapshot rolls back only the inner delta; reverting to
// the outer rolls back both — bounded exactly by journal length like every
// other journaled change.
func TestAddResourceWeightJournaled_NestedSnapshots(t *testing.T) {
	s := newTestStateDB(t)
	dp := s.DynamicProperties()
	dp.SetTotalEnergyWeight(1000)

	outer := s.Snapshot()
	s.AddResourceWeightJournaled(dp, corepb.ResourceCode_ENERGY, 5)
	inner := s.Snapshot()
	s.AddResourceWeightJournaled(dp, corepb.ResourceCode_ENERGY, 3)
	if got := dp.TotalEnergyWeight(); got != 1008 {
		t.Fatalf("weight=%d, want 1008", got)
	}
	s.RevertToSnapshot(inner)
	if got := dp.TotalEnergyWeight(); got != 1005 {
		t.Fatalf("after inner revert: weight=%d, want 1005", got)
	}
	s.RevertToSnapshot(outer)
	if got := dp.TotalEnergyWeight(); got != 1000 {
		t.Fatalf("after outer revert: weight=%d, want 1000", got)
	}
}

// The journaled write and its revert target the exact dp passed in, even when it
// is NOT the StateDB's own — the never-committed simulation path runs the VM
// against a freshly loaded DynamicProperties distinct from s.dynProps.
func TestAddResourceWeightJournaled_DistinctDp(t *testing.T) {
	s := newTestStateDB(t)
	own := s.DynamicProperties()
	own.SetTotalEnergyWeight(1000)
	other := NewDynamicProperties()
	other.SetTotalEnergyWeight(2000)

	snap := s.Snapshot()
	s.AddResourceWeightJournaled(other, corepb.ResourceCode_ENERGY, 5)
	if got := other.TotalEnergyWeight(); got != 2005 {
		t.Fatalf("other weight=%d, want 2005", got)
	}
	if got := own.TotalEnergyWeight(); got != 1000 {
		t.Fatalf("own weight=%d, want 1000 untouched", got)
	}
	s.RevertToSnapshot(snap)
	if got := other.TotalEnergyWeight(); got != 2000 {
		t.Fatalf("after revert: other weight=%d, want 2000 (rolled back wrong dp)", got)
	}
}
