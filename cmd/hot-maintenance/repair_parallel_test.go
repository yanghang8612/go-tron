package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestRepairTargetColdRejectsUnsupportedWorkersBeforeOpeningDB(t *testing.T) {
	for _, workers := range []int{0, 3, 5} {
		root := t.TempDir()
		out, err := os.CreateTemp(t.TempDir(), "rejected-report")
		if err != nil {
			t.Fatal(err)
		}
		original := os.Stdout
		os.Stdout = out
		args := []string{"--hot-dir", filepath.Join(root, "hot"), "--stage-dir", filepath.Join(root, "stage"), "--cold-dir", filepath.Join(root, "cold"),
			"--manifest-sha256", strings.Repeat("01", 32),
			"--from-bucket", "1", "--through-bucket", "1", "--from-tx", "1", "--to-tx", "1", "--history-window", "1", "--workers", strconv.Itoa(workers)}
		got := runRepairTargetCold(args)
		os.Stdout = original
		if err := out.Close(); err != nil {
			t.Fatal(err)
		}
		if got == nil {
			t.Fatalf("accepted unsupported workers=%d", workers)
		}
		var report repairReport
		content, err := os.ReadFile(out.Name())
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(content, &report); err != nil {
			t.Fatal(err)
		}
		if report.Phase != "preflight" || report.Workers != workers || report.Error != got.Error() || !strings.Contains(report.Error, "bounded budgets are required") {
			t.Fatalf("workers=%d rejected for the wrong reason: %+v / %v", workers, report, got)
		}
		entries, err := os.ReadDir(root)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Fatalf("unsupported workers=%d touched DB paths: %v", workers, entries)
		}
	}
}

func TestRepairCandidateBindingsParallelAcrossTargetBuckets(t *testing.T) {
	f := newRetireFixture(t, false)
	ctx := context.Background()
	var second []rawdb.HistoryStagingBlockProof
	for number := uint64(2048); number <= 3071; number++ {
		block := types.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: int64(number), Timestamp: int64(number)}}})
		if err := rawdb.WriteBlock(f.hot, block); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteStateTxRange(f.hot, number, block.Hash(), number, number); err != nil {
			t.Fatal(err)
		}
		second = append(second, rawdb.HistoryStagingBlockProof{Number: number, Hash: block.Hash(), BeginTxNum: number, EndTxNum: number})
	}
	change := &rawdb.StateDomainChange{BlockNum: 2053, BlockHash: second[5].Hash, TxNum: 2053, Seq: 1,
		FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: common.Address{0x41, 11}, PrevExists: true, Prev: []byte("second-target-prev")}
	if err := rawdb.WriteStateDomainChangeBlockRows(f.hot, []*rawdb.StateDomainChange{change}); err != nil {
		t.Fatal(err)
	}
	refs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDBByBlockRange(f.hot, f.cold, 2048, 3071, 2048, 3071, "history/state-domain-change-2048-3071.seg")
	if err != nil {
		t.Fatal(err)
	}
	manifest := snapshots.NewManifest(1024, 3071, append(append([]snapshots.SegmentRef(nil), f.manifest.Segments...), refs...))
	proof := rawdb.HistoryStagingProof{Bucket: 2, Epoch: 1, EligibleThrough: 3071, FinishBlock: 3072, FinishHash: rawdb.ReadHeadBlockHash(f.hot), IndexBlock: 3072, IndexHash: rawdb.ReadHeadBlockHash(f.hot), Blocks: second}
	claim, err := f.m.BeginClaim(ctx, proof, [32]byte{8})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.m.CopyClaim(ctx, claim, f.limits); err != nil {
		t.Fatal(err)
	}
	if _, err := f.m.AdoptClaim(ctx, claim, proof); err != nil {
		t.Fatal(err)
	}
	if err := f.m.ClearSource(ctx, claim, f.limits); err != nil {
		t.Fatal(err)
	}
	lockPath := filepath.Join(t.TempDir(), "start.lock")
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	holdPath := filepath.Join(t.TempDir(), "offline.hold")
	if err := os.WriteFile(holdPath, nil, 0600); err != nil {
		t.Fatal(err)
	}
	hold, err := readCleanupHold(holdPath)
	if err != nil {
		t.Fatal(err)
	}
	e := repairExecution{hotPath: f.hotPath, stagePath: f.stagePath, coldPath: f.cold, minFree: 1,
		lockPath: lockPath, lock: lock, hold: hold, manager: f.m,
		routes: retirePlan{Epoch: 1, Buckets: []retireBucket{{Bucket: 1}, {Bucket: 2}}}}
	slices := []repairTargetSlice{{Bucket: 1, FromBlock: 1029, ToBlock: 1029, FromTxNum: 1029, ToTxNum: 1029},
		{Bucket: 2, FromBlock: 2053, ToBlock: 2053, FromTxNum: 2053, ToTxNum: 2053}}
	chain := rawdb.NewChainDB(f.hot, rawdb.NoopAncient{})
	var serial []rawdb.HistoryStagingColdBinding
	for _, workers := range []int{1, 2, 4} {
		e.workers = workers
		got, err := repairCandidateBindings(ctx, e, chain, manifest, slices, nil, nil)
		if err != nil {
			t.Fatalf("workers=%d: %v", workers, err)
		}
		if len(got) != 2 || got[0].Bucket != 1 || got[1].Bucket != 2 || len(got[0].Spans) == 0 || len(got[1].Spans) == 0 {
			t.Fatalf("workers=%d unordered or empty proofs: %+v", workers, got)
		}
		if workers == 1 {
			serial = got
		} else if !reflect.DeepEqual(got, serial) {
			t.Fatalf("workers=%d changed semantic bindings", workers)
		}
	}
}
