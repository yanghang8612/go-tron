package main

import "testing"

func TestPostingPruneEnabled(t *testing.T) {
	for _, value := range []string{"", "0", "1", "true", " 1", "2"} {
		got, err := postingPruneEnabled(value)
		valid := value == "" || value == "0" || value == "1"
		if (err == nil) != valid || got != (value == "1") {
			t.Fatalf("%q: enabled=%v err=%v", value, got, err)
		}
	}
}
