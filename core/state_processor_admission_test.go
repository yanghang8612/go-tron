package core

import "testing"

func TestShouldRunParallelTransferBlockAdmission(t *testing.T) {
	tests := []struct {
		name       string
		enabled    bool
		sampled    bool
		minimum    int
		candidates int
		want       bool
	}{
		{name: "disabled", minimum: 8, candidates: 20},
		{name: "sparse sync block", enabled: true, minimum: 8, candidates: 7},
		{name: "profitable sync block", enabled: true, minimum: 8, candidates: 8, want: true},
		{name: "sampled canary bypasses threshold", enabled: true, sampled: true, minimum: 8, want: true},
		{name: "ordinary import keeps configured behavior", enabled: true, candidates: 1, want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldRunParallelTransferBlock(tc.enabled, tc.sampled, tc.minimum, tc.candidates); got != tc.want {
				t.Fatalf("admission = %v, want %v", got, tc.want)
			}
		})
	}
}
