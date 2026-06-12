package downloader

import "testing"

func TestFetchWindowZeroTipContainsNothing(t *testing.T) {
	window := NewFetchWindow(0, 100)
	for _, id := range []uint64{0, 1, 100} {
		if window.Contains(queueID(id)) {
			t.Fatalf("zero-tip window contains block %d", id)
		}
	}
}

func TestFetchWindowWithinInventorySpanStartsAtZero(t *testing.T) {
	window := NewFetchWindow(150, 100)
	if window.Min != 0 || window.Max != 150 {
		t.Fatalf("window = [%d,%d], want [0,150]", window.Min, window.Max)
	}
	for _, id := range []uint64{0, 1, 150} {
		if !window.Contains(queueID(id)) {
			t.Fatalf("window should contain block %d", id)
		}
	}
	if window.Contains(queueID(151)) {
		t.Fatal("window contains block 151 above max")
	}
}

func TestFetchWindowTrimsToDoubleInventorySpan(t *testing.T) {
	window := NewFetchWindow(250, 100)
	if window.Min != 50 || window.Max != 250 {
		t.Fatalf("window = [%d,%d], want [50,250]", window.Min, window.Max)
	}
	if window.Contains(queueID(49)) {
		t.Fatal("window contains block below min")
	}
	for _, id := range []uint64{50, 100, 250} {
		if !window.Contains(queueID(id)) {
			t.Fatalf("window should contain block %d", id)
		}
	}
}
