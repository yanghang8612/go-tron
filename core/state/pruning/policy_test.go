package pruning

import "testing"

func TestPolicyValidate(t *testing.T) {
	if err := ArchivePolicy().Validate(); err != nil {
		t.Fatalf("archive validate: %v", err)
	}
	if err := FullPolicy(100, 20).Validate(); err != nil {
		t.Fatalf("full validate: %v", err)
	}
	if err := SnapPolicy(100, 20).Validate(); err != nil {
		t.Fatalf("snap validate: %v", err)
	}
	if err := FullPolicy(0, 20).Validate(); err == nil {
		t.Fatal("zero history window accepted")
	}
	if err := FullPolicy(10, 20).Validate(); err == nil {
		t.Fatal("history window below reorg window accepted")
	}
	if err := (Policy{Mode: Mode("bad")}).Validate(); err == nil {
		t.Fatal("unknown mode accepted")
	}
}

func TestPolicyHistoryAndReorgRetention(t *testing.T) {
	archive := ArchivePolicy()
	for _, block := range []uint64{0, 1, 99} {
		if !archive.RetainHistory(block, 100) || !archive.RetainReorgData(block, 100) {
			t.Fatalf("archive did not retain block %d", block)
		}
	}

	full := FullPolicy(10, 3)
	if !full.RetainHistory(91, 100) || full.RetainHistory(90, 100) {
		t.Fatal("full history retention boundary wrong")
	}
	if !full.RetainReorgData(98, 100) || full.RetainReorgData(97, 100) {
		t.Fatal("full reorg retention boundary wrong")
	}
}

func TestPolicySnapshotRetention(t *testing.T) {
	if !ArchivePolicy().RetainSnapshot(1, 10, 20) {
		t.Fatal("archive should retain snapshots regardless of visible range")
	}
	if FullPolicy(10, 3).RetainSnapshot(15, 10, 20) {
		t.Fatal("full mode should not depend on immutable snapshots")
	}
	snap := SnapPolicy(10, 3)
	if !snap.RetainSnapshot(15, 10, 20) {
		t.Fatal("snap mode should retain visible snapshots")
	}
	if snap.RetainSnapshot(9, 10, 20) || snap.RetainSnapshot(21, 10, 20) {
		t.Fatal("snap mode retained invisible snapshot")
	}
}
