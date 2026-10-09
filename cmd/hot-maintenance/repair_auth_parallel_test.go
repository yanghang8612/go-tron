package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

func TestRepairVerifyNewRefsAuthenticatesCompleteTriosInParallel(t *testing.T) {
	for _, workers := range []int{1, 2, 4, 8} {
		t.Run(strconv.Itoa(workers), func(t *testing.T) {
			// A fresh directory and second trio keep each configuration on its
			// first physical authentication, not a prior worker's cache hit.
			f := newRetireFixture(t, false)
			second, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDBByBlockRange(
				f.hot, f.cold, 1024, 1535, 1024, 1535, "history/state-domain-change-auth-1024-1535.seg")
			if err != nil {
				t.Fatal(err)
			}
			refs := append(append([]snapshots.SegmentRef(nil), f.manifest.Segments...), second...)
			proofCtx, facts, err := snapshots.WithHistoryStagingPhysicalFacts(context.Background(), f.cold)
			if err != nil {
				t.Fatal(err)
			}
			if err := repairVerifyNewRefs(proofCtx, f.cold, f.manifest, refs, workers); err != nil {
				t.Fatalf("authenticate valid trios: %v", err)
			}
			if err := facts.RecheckAll(context.Background()); err != nil {
				t.Fatalf("parallel authentication did not retain strong file fingerprints: %v", err)
			}
			if err := repairVerifyNewRefs(context.Background(), f.cold, f.manifest, refs[:len(refs)-1], workers); err == nil {
				t.Fatal("accepted incomplete replacement trio")
			}
			if err := repairVerifyNewRefs(context.Background(), f.cold, f.manifest, nil, workers); err == nil {
				t.Fatal("accepted empty replacement trios")
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := repairVerifyNewRefs(ctx, f.cold, f.manifest, refs, workers); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled authentication: %v", err)
			}

			var historyPath string
			for _, ref := range second {
				if ref.Kind == snapshots.SegmentHistory {
					historyPath = filepath.Join(f.cold, ref.Path)
				}
			}
			if historyPath == "" {
				t.Fatal("second trio has no history segment")
			}
			file, err := os.OpenFile(historyPath, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := file.WriteAt([]byte{0xff}, 0); err != nil {
				file.Close()
				t.Fatal(err)
			}
			if err := file.Sync(); err != nil {
				file.Close()
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			if err := facts.RecheckAll(context.Background()); err == nil {
				t.Fatal("final fingerprint check accepted tampered replacement trio")
			}
			if err := repairVerifyNewRefs(context.Background(), f.cold, f.manifest, refs, workers); err == nil {
				t.Fatal("accepted tampered replacement trio")
			}
		})
	}
}
