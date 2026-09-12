package main

import "testing"

func TestHistoryRangePruneEnabled(t *testing.T) {
	for _, tc := range []struct {
		value         string
		want, invalid bool
	}{{"", false, false}, {"0", false, false}, {"1", true, false}, {"true", false, true}, {" 1", false, true}, {"2", false, true}} {
		t.Run(tc.value, func(t *testing.T) {
			got, err := historyRangePruneEnabled(tc.value)
			if got != tc.want || (err != nil) != tc.invalid {
				t.Fatalf("enabled(%q) = %v, %v", tc.value, got, err)
			}
		})
	}
}
