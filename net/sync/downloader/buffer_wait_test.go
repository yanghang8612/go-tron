package downloader

import (
	"testing"
	"time"
)

func TestBufferWaitTrackerMeasuresMatchingTarget(t *testing.T) {
	var tracker BufferWaitTracker
	start := time.Unix(100, 0)
	tracker.Begin(12, start)
	if got := tracker.End(12, start.Add(150*time.Millisecond)); got != 150*time.Millisecond {
		t.Fatalf("End duration = %v, want 150ms", got)
	}
	if got := tracker.End(12, start.Add(time.Second)); got != 0 {
		t.Fatalf("End after reset = %v, want 0", got)
	}
}

func TestBufferWaitTrackerRetargetsWhenNextChanges(t *testing.T) {
	var tracker BufferWaitTracker
	start := time.Unix(200, 0)
	tracker.Begin(10, start)
	tracker.Begin(10, start.Add(time.Second))
	if got := tracker.End(10, start.Add(2*time.Second)); got != 2*time.Second {
		t.Fatalf("same-target Begin reset duration = %v, want 2s from original start", got)
	}

	tracker.Begin(10, start)
	tracker.Begin(11, start.Add(500*time.Millisecond))
	if got := tracker.End(11, start.Add(2*time.Second)); got != 1500*time.Millisecond {
		t.Fatalf("retargeted duration = %v, want 1.5s", got)
	}
}

func TestBufferWaitTrackerMismatchClears(t *testing.T) {
	var tracker BufferWaitTracker
	start := time.Unix(300, 0)
	tracker.Begin(5, start)
	if got := tracker.End(6, start.Add(time.Second)); got != 0 {
		t.Fatalf("mismatched End duration = %v, want 0", got)
	}
	if got := tracker.End(5, start.Add(2*time.Second)); got != 0 {
		t.Fatalf("End after mismatch reset = %v, want 0", got)
	}
}

func TestBufferWaitTrackerClampsNegativeDuration(t *testing.T) {
	var tracker BufferWaitTracker
	start := time.Unix(400, 0)
	tracker.Begin(1, start)
	if got := tracker.End(1, start.Add(-time.Second)); got != 0 {
		t.Fatalf("negative duration = %v, want 0", got)
	}
}
