package main

import (
	"context"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestSelectRepairTargetSlicesRequiresExactCanonicalTargetBlocks(t *testing.T) {
	f := newRetireFixture(t, false)
	ctx := context.Background()
	p, manager, err := inspectRetirePlan(ctx, f.hot, f.stage, 1, 1, 64)
	if err != nil {
		t.Fatal(err)
	}
	chain := rawdb.NewChainDB(f.hot, rawdb.NoopAncient{})
	got, err := selectRepairTargetSlices(ctx, p, manager, chain, 1029, 1030)
	if err != nil || len(got) != 1 || got[0] != (repairTargetSlice{Bucket: 1, FromBlock: 1029, ToBlock: 1030, FromTxNum: 1029, ToTxNum: 1030}) {
		t.Fatalf("exact TARGET interval: slices=%+v err=%v", got, err)
	}
	if _, err := selectRepairTargetSlices(ctx, p, manager, chain, 1028, 2048); err == nil {
		t.Fatal("TARGET interval beyond the complete bucket was accepted")
	}
	if err := manager.ReleaseTargetToCold(ctx, 1, func(rawdb.HistoryStagingColdBinding) error { return nil }); err == nil {
		t.Fatal("unbound target was released")
	}
	p.Buckets[0].Route.TargetCleared = true
	if _, err := selectRepairTargetSlices(ctx, p, manager, chain, 1029, 1030); err == nil {
		t.Fatal("already-cleared TARGET route was accepted")
	}
}
