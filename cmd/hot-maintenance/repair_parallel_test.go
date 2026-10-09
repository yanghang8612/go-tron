package main

import (
	"context"
	"encoding/binary"
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
	for _, workers := range []int{0, 3, 5, 16} {
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
	for _, workers := range []int{1, 2, 4, 8} {
		t.Run(strconv.Itoa(workers), func(t *testing.T) {
			f, manifest, e, slices := newEightBucketRepairProofFixture(t)
			chain := rawdb.NewChainDB(f.hot, rawdb.NoopAncient{})
			e.workers = workers
			got, err := repairCandidateBindings(context.Background(), e, chain, manifest, slices, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 8 {
				t.Fatalf("workers=%d proved %d of 8 buckets", workers, len(got))
			}
			for i, binding := range got {
				if binding.Bucket != uint64(i+1) || len(binding.Spans) == 0 {
					t.Fatalf("workers=%d unordered or empty bucket %d proof: %+v", workers, i+1, binding)
				}
			}
			if workers > 1 {
				e.workers = 1
				serial, err := repairCandidateBindings(context.Background(), e, chain, manifest, slices, nil, nil)
				if err != nil || !reflect.DeepEqual(got, serial) {
					t.Fatalf("workers=%d changed semantic bindings: %v", workers, err)
				}
			}
		})
	}
}

func TestRepairCandidateBindingsResumeEightWorkersAuditsDurableBuckets(t *testing.T) {
	f, manifest, e, slices := newEightBucketRepairProofFixture(t)
	ctx := context.Background()
	chain := rawdb.NewChainDB(f.hot, rawdb.NoopAncient{})
	e.workers = 8
	fresh, err := repairCandidateBindings(ctx, e, chain, manifest, slices, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(fresh) != 8 {
		t.Fatalf("proved %d of 8 buckets", len(fresh))
	}
	// A crash after publication and partial certification leaves real durable
	// candidate-generation bindings. Resumption must authenticate their receipts
	// through the single-goroutine audit, even when --workers remains eight.
	for i, binding := range fresh {
		if err := f.m.CertifyColdRange(ctx, binding, func() error { return nil }); err != nil {
			t.Fatal(err)
		}
		durable, present, err := f.m.ReadColdBindingAt(1, binding.Bucket)
		if err != nil || !present || !reflect.DeepEqual(durable, binding) {
			t.Fatalf("bucket %d did not persist binding: %v", binding.Bucket, err)
		}
		e.routes.Buckets[i].OldBinding = &durable
	}
	pinned, err := snapshots.OpenPinnedManager(f.cold, manifest)
	if err != nil {
		t.Fatal(err)
	}
	audit, err := snapshots.NewHistoryStagingReceiptAudit(pinned)
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := repairCandidateBindings(ctx, e, chain, manifest, slices, &repairJournal{Bindings: fresh}, audit)
	if err != nil || !reflect.DeepEqual(resumed, fresh) {
		t.Fatalf("resumed durable proofs changed: %v", err)
	}
	if got := audit.AuthenticatedTrios(); got != 8 {
		t.Fatalf("resumed audit authenticated %d distinct trios, want 8", got)
	}
	if err := audit.RecheckAll(ctx); err != nil {
		t.Fatal(err)
	}
	var lastHistory string
	for _, ref := range manifest.Segments {
		if ref.FromTxNum == 8192 && ref.Kind == snapshots.SegmentHistory {
			lastHistory = filepath.Join(f.cold, ref.Path)
		}
	}
	if lastHistory == "" {
		t.Fatal("missing eighth bucket history file")
	}
	file, err := os.OpenFile(lastHistory, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{0xff}, 0); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := repairCandidateBindings(ctx, e, chain, manifest, slices, &repairJournal{Bindings: fresh}, audit); err == nil {
		t.Fatal("resumed receipt audit accepted mutated cold trio")
	}
}

// Every worker mode gets a fresh physical trio and TARGET payload per bucket.
// In particular, the eight-worker proof is not clamped to two tasks or warmed
// by a preceding serial proof in the same cold directory.
func newEightBucketRepairProofFixture(t *testing.T) (*retireFixture, *snapshots.Manifest, repairExecution, []repairTargetSlice) {
	t.Helper()
	f := newRetireFixture(t, false)
	ctx := context.Background()
	if err := f.m.InitializeOfflineSourceRoutes(ctx, 1, 4, 8); err != nil {
		t.Fatal(err)
	}
	var buckets [9][]rawdb.HistoryStagingBlockProof
	var head common.Hash
	for number := uint64(2048); number <= 9216; number++ {
		block := types.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: int64(number), Timestamp: int64(number)}}})
		if err := rawdb.WriteBlock(f.hot, block); err != nil {
			t.Fatal(err)
		}
		if number == 9216 {
			head = block.Hash()
			continue
		}
		if err := rawdb.WriteStateTxRange(f.hot, number, block.Hash(), number, number); err != nil {
			t.Fatal(err)
		}
		bucket := number / 1024
		buckets[bucket] = append(buckets[bucket], rawdb.HistoryStagingBlockProof{Number: number, Hash: block.Hash(), BeginTxNum: number, EndTxNum: number})
	}
	rawdb.WriteHeadBlockHash(f.hot, head)
	var number [8]byte
	binary.BigEndian.PutUint64(number[:], 9216)
	rawdb.WriteDynamicProperty(f.hot, "latest_block_header_number", number[:])
	rawdb.WriteDynamicProperty(f.hot, "latest_solidified_block_num", number[:])
	rawdb.WriteDynamicProperty(f.hot, "latest_block_header_hash", head[:])
	for _, stage := range []rawdb.StageID{rawdb.StageExecution, rawdb.StageFinish, rawdb.StageStateHistoryIndex, rawdb.StageCommitment} {
		if err := rawdb.WriteStageProgressWithHash(f.hot, stage, 9216, head); err != nil {
			t.Fatal(err)
		}
	}
	if err := rawdb.WriteLatestDomainCommitmentRoot(f.hot, head); err != nil {
		t.Fatal(err)
	}
	refs := append([]snapshots.SegmentRef(nil), f.manifest.Segments...)
	slices := []repairTargetSlice{{Bucket: 1, FromBlock: 1029, ToBlock: 1029, FromTxNum: 1029, ToTxNum: 1029}}
	routes := []retireBucket{{Bucket: 1}}
	for bucket := uint64(2); bucket <= 8; bucket++ {
		first, last := bucket*1024, bucket*1024+1023
		selected := first + 5
		change := &rawdb.StateDomainChange{BlockNum: selected, BlockHash: buckets[bucket][5].Hash, TxNum: selected, Seq: 1,
			FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: common.Address{0x41, byte(bucket)}, PrevExists: true, Prev: []byte("bucket-target-prev")}
		if err := rawdb.WriteStateDomainChangeBlockRows(f.hot, []*rawdb.StateDomainChange{change}); err != nil {
			t.Fatal(err)
		}
		one, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDBByBlockRange(f.hot, f.cold, first, last, first, last,
			"history/state-domain-change-"+strconv.FormatUint(first, 10)+"-"+strconv.FormatUint(last, 10)+".seg")
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, one...)
		proof := rawdb.HistoryStagingProof{Bucket: bucket, Epoch: 1, EligibleThrough: 9215, FinishBlock: 9216, FinishHash: head, IndexBlock: 9216, IndexHash: head, Blocks: buckets[bucket]}
		claim, err := f.m.BeginClaim(ctx, proof, [32]byte{byte(bucket + 6)})
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
		routes = append(routes, retireBucket{Bucket: bucket})
		slices = append(slices, repairTargetSlice{Bucket: bucket, FromBlock: selected, ToBlock: selected, FromTxNum: selected, ToTxNum: selected})
	}
	manifest := snapshots.NewManifest(1024, 9215, refs)
	manifest.Generation = f.manifest.Generation + 1
	if err := snapshots.PublishManifest(f.cold, manifest); err != nil {
		t.Fatal(err)
	}
	lockPath := filepath.Join(t.TempDir(), "start.lock")
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lock.Close() })
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
		routes: retirePlan{Epoch: 1, Buckets: routes}}
	return f, manifest, e, slices
}
