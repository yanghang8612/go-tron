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

func TestHistoryRangeQueueRequiresExplicitRangeOptIn(t *testing.T) {
	for _, tc := range []struct {
		value                 string
		ranges, want, invalid bool
	}{{"", false, false, false}, {"0", false, false, false}, {"1", false, false, true},
		{"1", true, true, false}, {"0", true, false, false}, {"true", true, false, true}, {" 1", true, false, true}} {
		got, err := historyRangeQueueEnabled(tc.value, tc.ranges)
		if got != tc.want || (err != nil) != tc.invalid {
			t.Fatalf("queue(%q, ranges=%v) = %v, %v", tc.value, tc.ranges, got, err)
		}
	}
}
