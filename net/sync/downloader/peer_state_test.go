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

func TestObserveInventoryTarget(t *testing.T) {
	tests := []struct {
		name        string
		current     uint64
		tip         uint64
		remain      int64
		limit       int
		target      uint64
		stageTarget uint64
		observed    uint64
		advanced    bool
		windowMin   uint64
		windowMax   uint64
	}{
		{name: "empty tip", current: 10, target: 10},
		{name: "tip advances target", current: 10, tip: 25, limit: 5, target: 25, stageTarget: 25, observed: 25, advanced: true, windowMin: 15, windowMax: 25},
		{name: "remain extends target", current: 10, tip: 25, remain: 7, limit: 5, target: 32, stageTarget: 32, observed: 32, advanced: true, windowMin: 15, windowMax: 25},
		{name: "negative remain ignored", current: 10, tip: 25, remain: -7, limit: 5, target: 25, stageTarget: 25, observed: 25, advanced: true, windowMin: 15, windowMax: 25},
		{name: "stale observed keeps current", current: 50, tip: 25, remain: 7, limit: 5, target: 50, stageTarget: 50, observed: 32, windowMin: 15, windowMax: 25},
	}
	for _, tt := range tests {
		got := ObserveInventoryTarget(tt.current, tt.tip, tt.remain, tt.limit)
		if got.Target != tt.target || got.StageTarget != tt.stageTarget || got.Observed != tt.observed || got.Advanced != tt.advanced || got.Window.Min != tt.windowMin || got.Window.Max != tt.windowMax {
			t.Fatalf("%s: update = %+v, want target %d stage %d observed %d advanced %v window %d-%d", tt.name, got, tt.target, tt.stageTarget, tt.observed, tt.advanced, tt.windowMin, tt.windowMax)
		}
	}
}

func TestShouldMarkInventoryDone(t *testing.T) {
	tests := []struct {
		name         string
		inventoryIDs int
		queued       int
		remain       int64
		done         bool
	}{
		{name: "empty inventory", done: true},
		{name: "single already known head", inventoryIDs: 1, queued: 0, remain: 0, done: true},
		{name: "single with queued block", inventoryIDs: 1, queued: 1, remain: 0},
		{name: "single with remain", inventoryIDs: 1, queued: 0, remain: 2},
		{name: "multi without queued", inventoryIDs: 2, queued: 0, remain: 0},
	}
	for _, tt := range tests {
		if got := ShouldMarkInventoryDone(tt.inventoryIDs, tt.queued, tt.remain); got != tt.done {
			t.Fatalf("%s: done = %v, want %v", tt.name, got, tt.done)
		}
	}
}

func TestPlanDrainedPeerAction(t *testing.T) {
	tests := []struct {
		name         string
		done         bool
		inventoryTip uint64
		head         uint64
		action       DrainedPeerAction
	}{
		{name: "done", done: true, inventoryTip: 10, head: 10, action: DrainedPeerIdle},
		{name: "wait local head", inventoryTip: 10, head: 9, action: DrainedPeerWaitLocalHead},
		{name: "request at tip", inventoryTip: 10, head: 10, action: DrainedPeerRequestInventory},
		{name: "request past tip", inventoryTip: 10, head: 11, action: DrainedPeerRequestInventory},
		{name: "request without tip", action: DrainedPeerRequestInventory},
	}
	for _, tt := range tests {
		if got := PlanDrainedPeerAction(tt.done, tt.inventoryTip, tt.head); got != tt.action {
			t.Fatalf("%s: action = %v, want %v", tt.name, got, tt.action)
		}
	}
}
