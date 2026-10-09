package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

type retireFixture struct {
	hot, stage               ethdb.KeyValueStore
	m                        *rawdb.HistoryStagingManager
	hotPath, stagePath, cold string
	manifest                 *snapshots.Manifest
	blocks                   []rawdb.HistoryStagingBlockProof
	limits                   rawdb.HistoryStagingLimits
}

func newRetireFixture(t *testing.T, wrongPrev bool) *retireFixture {
	t.Helper()
	root := t.TempDir()
	f := &retireFixture{hotPath: filepath.Join(root, "hot"), stagePath: filepath.Join(root, "stage"), cold: filepath.Join(root, "cold")}
	if err := os.Mkdir(f.cold, 0700); err != nil {
		t.Fatal(err)
	}
	var err error
	f.hot, err = rawdb.NewPebbleDB(f.hotPath, 16, 32)
	if err != nil {
		t.Fatal(err)
	}
	f.stage, err = rawdb.NewHistoryStagingPebbleDB(f.stagePath, 16, 32, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.hot.Close(); f.stage.Close() })
	id := rawdb.HistoryStagingIdentity{Version: 1, GenesisHash: common.Hash{1}, NetworkID: 1, SourceID: [32]byte{2}, TargetID: [32]byte{3}}
	f.m, err = rawdb.NewHistoryStagingManager(f.hot, f.stage, id)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := f.m.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.m.InitializeOfflineSourceRoutes(ctx, 1, 1, 3); err != nil {
		t.Fatal(err)
	}
	var head common.Hash
	for n := uint64(1024); n <= 3072; n++ {
		if n > 2047 && n != 3072 {
			continue
		}
		b := types.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: int64(n), Timestamp: int64(n)}}})
		if err := rawdb.WriteBlock(f.hot, b); err != nil {
			t.Fatal(err)
		}
		if n == 3072 {
			head = b.Hash()
			continue
		}
		if err := rawdb.WriteStateTxRange(f.hot, n, b.Hash(), n, n); err != nil {
			t.Fatal(err)
		}
		f.blocks = append(f.blocks, rawdb.HistoryStagingBlockProof{Number: n, Hash: b.Hash(), BeginTxNum: n, EndTxNum: n})
	}
	rawdb.WriteHeadBlockHash(f.hot, head)
	var number [8]byte
	binary.BigEndian.PutUint64(number[:], 3072)
	rawdb.WriteDynamicProperty(f.hot, "latest_block_header_number", number[:])
	rawdb.WriteDynamicProperty(f.hot, "latest_solidified_block_num", number[:])
	rawdb.WriteDynamicProperty(f.hot, "latest_block_header_hash", head[:])
	for _, stage := range []rawdb.StageID{rawdb.StageExecution, rawdb.StageFinish, rawdb.StageStateHistoryIndex, rawdb.StageCommitment} {
		if err := rawdb.WriteStageProgressWithHash(f.hot, stage, 3072, head); err != nil {
			t.Fatal(err)
		}
	}
	if err := rawdb.WriteLatestDomainCommitmentRoot(f.hot, head); err != nil {
		t.Fatal(err)
	}
	change := &rawdb.StateDomainChange{BlockNum: 1029, BlockHash: f.blocks[5].Hash, TxNum: 1029, Seq: 1, FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: common.Address{0x41, 9}, PrevExists: true, Prev: []byte("authentic-prev"), NextExists: true, Next: []byte("next")}
	if err := rawdb.WriteStateDomainChangeBlockRows(f.hot, []*rawdb.StateDomainChange{change}); err != nil {
		t.Fatal(err)
	}
	refs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDBByBlockRange(f.hot, f.cold, 1024, 2047, 1024, 2047, "history/state-domain-change-1024-2047.seg")
	if err != nil {
		t.Fatal(err)
	}
	f.manifest = snapshots.NewManifest(1024, 2047, refs)
	if err := snapshots.PublishManifest(f.cold, f.manifest); err != nil {
		t.Fatal(err)
	}
	if wrongPrev {
		change.Prev = []byte("corrupt-target-prev")
		if err := rawdb.WriteStateDomainChangeBlockRows(f.hot, []*rawdb.StateDomainChange{change}); err != nil {
			t.Fatal(err)
		}
	}
	proof := rawdb.HistoryStagingProof{Bucket: 1, Epoch: 1, EligibleThrough: 2047, FinishBlock: 3072, FinishHash: head, IndexBlock: 3072, IndexHash: head, Blocks: f.blocks}
	f.limits = rawdb.HistoryStagingLimits{MaxRowBytes: 4 << 20, MaxBatchBytes: 4 << 20, MaxBucketBytes: 16 << 20, MaxWorkBytes: 128 << 20, MaxDecodedBytes: 8 << 20, MinFreeBytes: 1, FreeBytes: func() (uint64, error) { return 1 << 30, nil }}
	claim, err := f.m.BeginClaim(ctx, proof, [32]byte{7})
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
	barrier := rawdb.HistoryStagingRouteBarrier{Version: 1, Epoch: 1, ThroughBucket: 3, EligibleThrough: 1, PlanDigest: [32]byte{1}, CandidateSHA: [32]byte{2}}
	if err := f.m.PublishHistoryStagingRouteBarrier(ctx, barrier, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	return f
}

func fixtureRetireAudit(t *testing.T, f *retireFixture) *snapshots.HistoryStagingReceiptAudit {
	t.Helper()
	p, err := snapshots.OpenPinnedManager(f.cold, f.manifest)
	if err != nil {
		t.Fatal(err)
	}
	a, err := snapshots.NewHistoryStagingReceiptAudit(p)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestRetireMissingBindingRequiresExplicitSemanticProofAndPreservesProtectedState(t *testing.T) {
	f := newRetireFixture(t, false)
	ctx := context.Background()
	p, m, err := inspectRetirePlan(ctx, f.hot, f.stage, 1, 1, 64)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Buckets) != 1 || !p.Buckets[0].NeedsCertification || p.Buckets[0].OldBinding != nil {
		t.Fatalf("wrong metadata %+v", p)
	}
	if err := authenticateRetirePlan(ctx, &p, m, f.hot, f.cold, f.manifest, fixtureRetireAudit(t, f), false, nil); err == nil {
		t.Fatal("missing binding accepted without explicit certification")
	}
	if err := authenticateRetirePlan(ctx, &p, m, f.hot, f.cold, f.manifest, fixtureRetireAudit(t, f), true, nil); err != nil {
		t.Fatal(err)
	}
	if !cleanupBindingCovers(1, *p.Buckets[0].Prepared) {
		t.Fatal("semantic proof did not cover zero-change blocks")
	}
	current, _, err := inspectRetirePlan(ctx, f.hot, f.stage, 1, 1, 64)
	if err != nil || !retireMetadataEqual(p, current) {
		t.Fatal("read-only authentication changed metadata", err)
	}
	if err := recheckRetireCanonical(ctx, p, m, f.hot); err != nil {
		t.Fatal(err)
	}
	if err := m.CertifyColdRange(ctx, *p.Buckets[0].Prepared, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	afterCert, _, err := inspectRetirePlan(ctx, f.hot, f.stage, 1, 1, 64)
	if err != nil || verifyRetireTransitions(p, afterCert, false) != nil {
		t.Fatal("certification partial restart rejected", err)
	}
	if err := m.ReleaseTargetToCold(ctx, 1, func(b rawdb.HistoryStagingColdBinding) error {
		if !reflect.DeepEqual(b, *p.Buckets[0].Prepared) {
			return errors.New("binding changed")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	afterRelease, _, err := inspectRetirePlan(ctx, f.hot, f.stage, 1, 1, 64)
	if err != nil || verifyRetireTransitions(p, afterRelease, false) != nil {
		t.Fatal("COLD intermediate restart rejected", err)
	}
	// A bounded failed clear is a legitimate resumable state. The route must
	// already be COLD and no failure may restore the target owner.
	blocked := f.limits
	blocked.FreeBytes = func() (uint64, error) { return 0, nil }
	if err := m.ClearColdTarget(ctx, 1, blocked); err == nil {
		t.Fatal("low space allowed target deletion")
	}
	if err := m.ClearColdTarget(ctx, 1, f.limits); err != nil {
		t.Fatal(err)
	}
	after, _, err := inspectRetirePlan(ctx, f.hot, f.stage, 1, 1, 64)
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyRetireTransitions(p, after, true); err != nil {
		t.Fatal(err)
	}
	if err := m.ClearColdTarget(ctx, 1, f.limits); err != nil {
		t.Fatal("completed retry failed", err)
	}
	if err := authenticateRetirePlan(ctx, &after, m, f.hot, f.cold, f.manifest, fixtureRetireAudit(t, f), false, nil); err != nil {
		t.Fatal("COLD retry unnecessarily required target payload", err)
	}
}

func TestRetireSemanticMismatchNeverPublishesBinding(t *testing.T) {
	f := newRetireFixture(t, true)
	ctx := context.Background()
	p, m, err := inspectRetirePlan(ctx, f.hot, f.stage, 1, 1, 64)
	if err != nil {
		t.Fatal(err)
	}
	if err := authenticateRetirePlan(ctx, &p, m, f.hot, f.cold, f.manifest, fixtureRetireAudit(t, f), true, nil); !errors.Is(err, snapshots.ErrHistoryStagingColdSemanticMismatch) {
		t.Fatalf("wrong Prev accepted: %v", err)
	}
	route, _, _ := m.ReadRoute(1)
	if route.Owner != rawdb.HistoryStagingOwnerTarget || route.ColdBindingEpoch != 0 {
		t.Fatal("failed proof changed route")
	}
}

func TestRetireBoundsAndTransitionsFailClosed(t *testing.T) {
	p := cleanupProtected{Boundary: chainBoundary{HeadNumber: 4096, SolidNumber: 4096}, Guard: guard{Finish: field[rawdb.StageProgress]{true, rawdb.StageProgress{BlockNum: 4096}}, HistoryIndex: field[rawdb.StageProgress]{true, rawdb.StageProgress{BlockNum: 4096}}}}
	b := rawdb.HistoryStagingRouteBarrier{EligibleThrough: 2, ThroughBucket: 3}
	for _, c := range []struct{ from, to, window uint64 }{{0, 1, 64}, {1, 3, 64}, {1, 2, 2048}, {2, 1, 64}, {1, 1, 0}} {
		if err := validateRetireBounds(p, b, c.from, c.to, c.window); err == nil {
			t.Fatalf("invalid bounds accepted %+v", c)
		}
	}
	if err := validateRetireBounds(p, b, 1, 2, 64); err != nil {
		t.Fatal(err)
	}
	f := newRetireFixture(t, false)
	before, m, err := inspectRetirePlan(context.Background(), f.hot, f.stage, 1, 1, 64)
	if err != nil {
		t.Fatal(err)
	}
	if err := authenticateRetirePlan(context.Background(), &before, m, f.hot, f.cold, f.manifest, fixtureRetireAudit(t, f), true, nil); err != nil {
		t.Fatal(err)
	}
	after := before
	after.Buckets = append([]retireBucket(nil), before.Buckets...)
	after.Buckets[0].Route.WriteVersion++
	if err := verifyRetireTransitions(before, after, false); err == nil {
		t.Fatal("unexplained route mutation accepted")
	}
	if err := verifyRetireTransitions(before, before, true); err == nil {
		t.Fatal("unprocessed TARGET reported complete")
	}
}

func TestRetireMetadataDryRunDoesNotWriteAndYesRequiresInheritedLock(t *testing.T) {
	f := newRetireFixture(t, false)
	if err := f.hot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.stage.Close(); err != nil {
		t.Fatal(err)
	}
	sha, err := cleanupFileSHA256(filepath.Join(f.cold, snapshots.ManifestFile))
	if err != nil {
		t.Fatal(err)
	}
	args := []string{"--hot-dir", f.hotPath, "--stage-dir", f.stagePath, "--cold-dir", f.cold, "--manifest-sha256", fmtHash(sha), "--from-bucket", "1", "--through-bucket", "1", "--history-window", "64"}
	hotBefore, stageBefore, coldBefore := databaseFileHashes(t, f.hotPath), databaseFileHashes(t, f.stagePath), databaseFileHashes(t, f.cold)
	if err := runRetireTarget(args); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(hotBefore, databaseFileHashes(t, f.hotPath)) || !reflect.DeepEqual(stageBefore, databaseFileHashes(t, f.stagePath)) || !reflect.DeepEqual(coldBefore, databaseFileHashes(t, f.cold)) {
		t.Fatal("metadata dry-run wrote files")
	}
	if err := runRetireTarget(append(args, "--yes", "--certify-missing")); err == nil {
		t.Fatal("writer accepted missing inherited offline lock/hold")
	}
}

type retireHotFinishFault struct {
	ethdb.KeyValueStore
	fail bool
}

func (s *retireHotFinishFault) SyncKeyValue() error {
	return s.KeyValueStore.(interface{ SyncKeyValue() error }).SyncKeyValue()
}
func (s *retireHotFinishFault) NewKeyValueSnapshot() (pointread.KeyValueSnapshot, error) {
	return s.KeyValueStore.(pointread.KeyValueSnapshotter).NewKeyValueSnapshot()
}
func (s *retireHotFinishFault) NewBatch() ethdb.Batch {
	b := s.KeyValueStore.NewBatch()
	if s.fail {
		return cleanupFaultBatch{Batch: b, err: errors.New("crash before hot TargetCleared write")}
	}
	return b
}

func TestRetireResumesReceiptGoneBeforeTargetClearedFlag(t *testing.T) {
	f := newRetireFixture(t, false)
	ctx := context.Background()
	before, m, err := inspectRetirePlan(ctx, f.hot, f.stage, 1, 1, 64)
	if err != nil {
		t.Fatal(err)
	}
	if err := authenticateRetirePlan(ctx, &before, m, f.hot, f.cold, f.manifest, fixtureRetireAudit(t, f), true, nil); err != nil {
		t.Fatal(err)
	}
	if err := m.CertifyColdRange(ctx, *before.Buckets[0].Prepared, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := m.ReleaseTargetToCold(ctx, 1, func(rawdb.HistoryStagingColdBinding) error { return nil }); err != nil {
		t.Fatal(err)
	}
	id, _, err := rawdb.ReadHistoryStagingIdentity(f.hot)
	if err != nil {
		t.Fatal(err)
	}
	fault := &retireHotFinishFault{KeyValueStore: f.hot, fail: true}
	m, err = rawdb.NewHistoryStagingManager(fault, f.stage, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.ClearColdTarget(ctx, 1, f.limits); err == nil {
		t.Fatal("injected hot finish write succeeded")
	}
	if _, found, err := m.ReadReceiptAt(1, 1); err != nil || found {
		t.Fatal("receipt did not reach durable missing intermediate state", err)
	}
	route, _, err := m.ReadRoute(1)
	if err != nil || route.Owner != rawdb.HistoryStagingOwnerCold || route.TargetCleared {
		t.Fatal("unexpected interrupted route", route, err)
	}
	partial, _, err := inspectRetirePlan(ctx, f.hot, f.stage, 1, 1, 64)
	if err != nil {
		t.Fatal("valid missing-receipt COLD intermediate rejected", err)
	}
	if err := verifyRetireTransitions(before, partial, false); err != nil {
		t.Fatal(err)
	}
	fault.fail = false
	if err := m.ClearColdTarget(ctx, 1, f.limits); err != nil {
		t.Fatal("retry after receipt deletion failed", err)
	}
	after, _, err := inspectRetirePlan(ctx, f.hot, f.stage, 1, 1, 64)
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyRetireTransitions(before, after, true); err != nil {
		t.Fatal(err)
	}
}

func TestRetirePartialBindingExtendsWithMixedProof(t *testing.T) {
	f := newRetireFixture(t, false)
	ctx := context.Background()
	mask := make([]bool, len(f.blocks))
	for i := 0; i < 512; i++ {
		mask[i] = true
	}
	spans, err := snapshots.BuildHistoryStagingColdSpans(ctx, f.cold, f.manifest, f.blocks, mask)
	if err != nil {
		t.Fatal(err)
	}
	b := rawdb.HistoryStagingColdBinding{Version: 1, Epoch: 1, Bucket: 1, BindingEpoch: 1, ManifestEpoch: f.manifest.Generation, Spans: spans}
	if err := f.m.CertifyColdRange(ctx, b, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	p, m, err := inspectRetirePlan(ctx, f.hot, f.stage, 1, 1, 64)
	if err != nil {
		t.Fatal(err)
	}
	if p.Buckets[0].OldBinding == nil || !p.Buckets[0].NeedsCertification {
		t.Fatal("partial binding misclassified")
	}
	if err := authenticateRetirePlan(ctx, &p, m, f.hot, f.cold, f.manifest, fixtureRetireAudit(t, f), true, nil); err != nil {
		t.Fatal(err)
	}
	if p.Buckets[0].Prepared.BindingEpoch != 2 || !cleanupBindingCovers(1, *p.Buckets[0].Prepared) {
		t.Fatal("mixed proof did not fully extend binding")
	}
}

func TestRetireFullCLIWithInheritedLock(t *testing.T) {
	const env = "GTRON_RETIRE_CLI_TEST_CHILD"
	if os.Getenv(env) == "1" {
		var args []string
		if err := json.Unmarshal([]byte(os.Getenv("GTRON_RETIRE_CLI_TEST_ARGS")), &args); err != nil {
			t.Fatal(err)
		}
		if err := runRetireTarget(args); err != nil {
			t.Fatal(err)
		}
		return
	}
	f := newRetireFixture(t, false)
	f.hot.Close()
	f.stage.Close()
	sha, err := cleanupFileSHA256(filepath.Join(f.cold, snapshots.ManifestFile))
	if err != nil {
		t.Fatal(err)
	}
	lockPath := filepath.Join(t.TempDir(), "start.lock")
	parent, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	if err := syscall.Flock(int(parent.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	holdPath := filepath.Join(t.TempDir(), "hold")
	if err := os.WriteFile(holdPath, nil, 0600); err != nil {
		t.Fatal(err)
	}
	args := []string{"--hot-dir", f.hotPath, "--stage-dir", f.stagePath, "--cold-dir", f.cold, "--manifest-sha256", fmtHash(sha), "--from-bucket", "1", "--through-bucket", "1", "--history-window", "64", "--min-free-gib", "1", "--max-delete-gib", "1", "--start-lock", lockPath, "--hold-file", holdPath, "--yes", "--certify-missing"}
	data, _ := json.Marshal(args)
	var extras []*os.File
	for i := 3; i < 9; i++ {
		null, err := os.Open(os.DevNull)
		if err != nil {
			t.Fatal(err)
		}
		defer null.Close()
		extras = append(extras, null)
	}
	extras = append(extras, parent)
	cmd := exec.Command(os.Args[0], "-test.run=^TestRetireFullCLIWithInheritedLock$")
	cmd.ExtraFiles = extras
	cmd.Env = append(os.Environ(), env+"=1", "GTRON_RETIRE_CLI_TEST_ARGS="+string(data))
	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("CLI failed: %v\n%s\n%s", err, out.String(), stderr.String())
	}
	var report retireReport
	if err := json.NewDecoder(&out).Decode(&report); err != nil {
		t.Fatal(err)
	}
	if report.Error != "" || report.Phase != "complete" || !report.ProtectedStateVerified || report.Certified != 1 || report.Completed != 1 || report.AdmittedLogicalBytes == 0 {
		t.Fatalf("bad CLI report %+v", report)
	}
	probe, err := os.OpenFile(lockPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Close()
	if err := syscall.Flock(int(probe.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err == nil {
		t.Fatal("CLI released parent lock")
	}
}

func fmtHash(hash [32]byte) string {
	const digits = "0123456789abcdef"
	b := make([]byte, 64)
	for i, x := range hash {
		b[2*i], b[2*i+1] = digits[x>>4], digits[x&15]
	}
	return string(b)
}
