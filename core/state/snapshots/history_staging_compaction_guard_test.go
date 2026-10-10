package snapshots

import (
	"context"
	"testing"
)

func TestHistoryStagingProofGuardAllowsOtherMergesAndReleases(t *testing.T) {
	for _, busy := range []bool{false, true} {
		t.Run(map[bool]string{false: "aligned-reference", true: "busy-leaf"}[busy], func(t *testing.T) {
			dir := t.TempDir()
			var refs []SegmentRef
			for n := uint64(1); n <= 4; n++ {
				refs = append(refs, writeCompactionStateDomainChangeSegment(t, dir, n, n, binaryStateDomainChange(n, n, 1, "a"))...)
			}
			if err := PublishManifest(dir, NewManifest(1, 4, refs)); err != nil {
				t.Fatal(err)
			}
			release, ok, err := TryProtectHistoryStagingProofRange(dir, 1, 1)
			if err != nil || !ok {
				t.Fatal(ok, err)
			}
			nested, ok, err := TryProtectHistoryStagingProofRange(dir, 1, 2)
			if err != nil || !ok {
				t.Fatal(ok, err)
			}
			cfg := CompactionConfig{MaxSteps: 2, BusyLeafOnly: busy, MaxSources: 2}
			merged, err := CompactHistoryDomainContext(context.Background(), dir, SegmentDatasetStateDomainChange, cfg)
			if err != nil || !merged.Merged || merged.FromTxNum != 3 || merged.ToTxNum != 4 {
				t.Fatalf("unrelated merge=%+v err=%v", merged, err)
			}
			release()
			release()
			merged, err = CompactHistoryDomainContext(context.Background(), dir, SegmentDatasetStateDomainChange, cfg)
			if err != nil || merged.Merged {
				t.Fatal("nested proof lost protection", merged, err)
			}
			nested()
			merged, err = CompactHistoryDomainContext(context.Background(), dir, SegmentDatasetStateDomainChange, cfg)
			if err != nil || !merged.Merged || merged.FromTxNum != 1 || merged.ToTxNum != 2 {
				t.Fatal("released proof still blocked", merged, err)
			}
		})
	}
}

func TestHistoryStagingProofGuardAtomicWithRunningMergeAndDirectoryIsolation(t *testing.T) {
	dir := t.TempDir()
	merging, ok, err := claimHistoryProofRange(dir, 10, 20, true)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	release, ok, err := TryProtectHistoryStagingProofRange(dir, 20, 21)
	if err != nil || ok || release != nil {
		t.Fatal("overlapping active merge ignored")
	}
	other, ok, err := TryProtectHistoryStagingProofRange(t.TempDir(), 10, 20)
	if err != nil || !ok {
		t.Fatal("different directory blocked")
	}
	other()
	disjoint, ok, err := TryProtectHistoryStagingProofRange(dir, 21, 30)
	if err != nil || !ok {
		t.Fatal("disjoint proof blocked")
	}
	disjoint()
	merging()
	merging()
	proof, ok, err := TryProtectHistoryStagingProofRange(dir, 10, 20)
	if err != nil || !ok {
		t.Fatal("merge release failed")
	}
	defer proof()
	release, ok, err = claimHistoryProofRange(dir, 1, 11, true)
	if err != nil || ok || release != nil {
		t.Fatal("merge claimed protected input")
	}
	if _, _, err := TryProtectHistoryStagingProofRange(dir, 2, 1); err == nil {
		t.Fatal("invalid bounds accepted")
	}
}
