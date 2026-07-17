package forks_test

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/state"
)

// activeProposalTypes is the exhaustive list of active (not commented-out)
// entries in java-tron's org.tron.core.utils.ProposalUtil.ProposalType
// enum. Keep in sync with the java-tron source; any change there must be
// mirrored here and in core/forks/forks.go.
var activeProposalTypes = []int64{
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
	10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
	20, 21, 22, 23, 24, 25, 26,
	29, 30, 31, 32, 33,
	35, 39,
	40, 41, 44, 45, 46, 47, 48, 49,
	51, 52, 53, 59, 60, 61, 62, 63,
	65, 66, 67, 68, 69,
	70, 71, 72, 73, 74, 75, 76, 77, 78, 79,
	81, 82, 83, 87, 88, 89,
	92, 94, 95, 96, 97, 98,
	1000, 1001,
}

func TestProposalParamKey_AllActiveTypesMapped(t *testing.T) {
	dp := state.NewDynamicProperties()
	for _, id := range activeProposalTypes {
		key := forks.ProposalParamKey(id)
		if key == "" {
			t.Errorf("ProposalType id=%d: no go-tron key mapped", id)
			continue
		}
		if _, ok := dp.Get(key); !ok {
			t.Errorf("ProposalType id=%d → %q: key missing from DynamicProperties defaults",
				id, key)
		}
	}
}

func TestProposalParamKey_UnknownReturnsEmpty(t *testing.T) {
	cases := []int64{
		// Historical IDs that were commented out in ProposalUtil.java and
		// must not map to anything in go-tron.
		28, 34, 42, 43, 58,
		// Far outside the defined range.
		1002, -1,
	}
	for _, id := range cases {
		if key := forks.ProposalParamKey(id); key != "" {
			t.Errorf("ProposalParamKey(%d): got %q, want empty", id, key)
		}
	}
}

func TestProposalParamKey_HistoricalNileShieldedActivationMapped(t *testing.T) {
	const id int64 = 27
	const want = "allow_shielded_transaction"
	if got := forks.ProposalParamKey(id); got != want {
		t.Fatalf("ProposalParamKey(%d): got %q, want %q", id, got, want)
	}
	if _, ok := state.NewDynamicProperties().Get(want); !ok {
		t.Fatalf("ProposalParamKey(%d): key %q missing from DynamicProperties defaults", id, want)
	}
}

// TestProposalParamKey_NewerProposalsMapped guards the recently-added
// java-tron ProposalType entries (v4.8.x and Nile PQ1).
func TestProposalParamKey_NewerProposalsMapped(t *testing.T) {
	want := map[int64]string{
		95:   "allow_tvm_prague",
		97:   "allow_harden_resource_calculation",
		98:   "allow_harden_exchange_calculation",
		1000: "allow_fn_dsa_512",
		1001: "allow_ml_dsa_44",
	}
	for id, expect := range want {
		if got := forks.ProposalParamKey(id); got != expect {
			t.Errorf("ProposalParamKey(%d): got %q, want %q", id, got, expect)
		}
	}
}
