package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	corestate "github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	statesnapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"github.com/urfave/cli/v2"
)

type historyStagingAtomicTestBatch struct {
	ethdb.KeyValueStore
	batch   ethdb.Batch
	pending map[string][]byte
}

func (b *historyStagingAtomicTestBatch) StateHistoryChunkWritesAtomic() bool { return true }
func (b *historyStagingAtomicTestBatch) Put(key, value []byte) error {
	if err := b.batch.Put(key, value); err != nil {
		return err
	}
	b.pending[string(key)] = bytes.Clone(value)
	return nil
}
func (b *historyStagingAtomicTestBatch) Get(key []byte) ([]byte, error) {
	if value, ok := b.pending[string(key)]; ok {
		return bytes.Clone(value), nil
	}
	return b.KeyValueStore.Get(key)
}
func (b *historyStagingAtomicTestBatch) Has(key []byte) (bool, error) {
	if _, ok := b.pending[string(key)]; ok {
		return true, nil
	}
	return b.KeyValueStore.Has(key)
}

type historyStagingE2EFixture struct {
	datadir, cold, config, job, candidate string
	args                                  []string
}

func newHistoryStagingE2EFixture(t *testing.T) historyStagingE2EFixture {
	t.Helper()
	root := t.TempDir()
	datadir := filepath.Join(root, "datadir")
	cold := stateSnapshotsDir(datadir)
	config := filepath.Join(root, "history.toml")
	if err := os.WriteFile(config, []byte("[history]\nmode = \"archive\"\nprune_window = 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(chainDataDir(datadir), 0o700); err != nil {
		t.Fatal(err)
	}
	hot, err := rawdb.NewPebbleDB(chainDataDir(datadir), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer hot.Close()
	genesis := params.DefaultMainnetGenesis()
	if _, _, err := core.SetupGenesisBlock(hot, genesis); err != nil {
		t.Fatal(err)
	}
	previous, _, err := rawdb.ReadBlockHashByNumberStrict(hot, 0)
	if err != nil {
		t.Fatal(err)
	}
	const head = uint64(3073)
	var headHash, solidHash common.Hash
	for number := uint64(1); number <= head; number++ {
		block := types.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{Number: int64(number), Timestamp: int64(number * 3000), ParentHash: previous[:]},
		}})
		if err := rawdb.WriteBlock(hot, block); err != nil {
			t.Fatal(err)
		}
		hash := block.Hash()
		if err := rawdb.WriteStateTxRange(hot, number, hash, number, number); err != nil {
			t.Fatal(err)
		}
		previous = hash
		if number == head-1 {
			solidHash = hash
		}
		headHash = hash
	}
	writeNumber := func(name string, value uint64) {
		var bytes [8]byte
		binary.BigEndian.PutUint64(bytes[:], value)
		rawdb.WriteDynamicProperty(hot, name, bytes[:])
	}
	rawdb.WriteHeadBlockHash(hot, headHash)
	writeNumber("latest_block_header_number", head)
	rawdb.WriteDynamicProperty(hot, "latest_block_header_hash", headHash[:])
	writeNumber("latest_solidified_block_num", head-1)
	rawdb.WriteHeadSolidBlockHash(hot, solidHash)
	for _, stage := range []rawdb.StageID{rawdb.StageExecution, rawdb.StageFinish, rawdb.StageStateHistoryIndex} {
		if err := rawdb.WriteStageProgressWithHash(hot, stage, head, headHash); err != nil {
			t.Fatal(err)
		}
	}
	if err := rawdb.WriteStageProgress(hot, rawdb.StageSnapshotHotPrune, 1535); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteHistoryPruneMode(hot, "archive"); err != nil {
		t.Fatal(err)
	}
	writeChange := func(number uint64, prev []byte, seq uint64) {
		hash, present, err := rawdb.ReadBlockHashByNumberStrict(hot, number)
		if err != nil || !present {
			t.Fatalf("canonical %d: %v", number, err)
		}
		row := &rawdb.StateDomainChange{BlockNum: number, BlockHash: hash, TxNum: number, Seq: seq,
			FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: common.Address{0x41, 3},
			Generation: 7, Domain: kvdomains.SystemDelegation, Key: []byte("history-staging-e2e"),
			PrevExists: true, Prev: prev}
		if err := rawdb.WriteStateDomainChangeBlockRows(hot, []*rawdb.StateDomainChange{row}); err != nil {
			t.Fatal(err)
		}
	}
	writeChange(1030, []byte("cold-one"), 1)
	writeChange(1300, []byte("cold-two"), 1)
	rawdb.SetStateHistoryCrossBlockDedup(true)
	defer rawdb.SetStateHistoryCrossBlockDedup(false)
	rng := rand.New(rand.NewSource(20261001))
	large := make([]byte, 512<<10)
	if _, err := rng.Read(large); err != nil {
		t.Fatal(err)
	}
	for _, number := range []uint64{1600, 1601} {
		hash, _, err := rawdb.ReadBlockHashByNumberStrict(hot, number)
		if err != nil {
			t.Fatal(err)
		}
		row := &rawdb.StateDomainChange{BlockNum: number, BlockHash: hash, TxNum: number, Seq: 1,
			FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: common.Address{0x41, 3},
			Generation: 7, Domain: kvdomains.SystemDelegation, Key: []byte("shared-history"),
			PrevExists: true, Prev: append([]byte{byte(number)}, large...)}
		atomic := &historyStagingAtomicTestBatch{KeyValueStore: hot, batch: hot.NewBatch(), pending: make(map[string][]byte)}
		if err := rawdb.WriteStateDomainChangeBlockRows(atomic, []*rawdb.StateDomainChange{row}); err != nil {
			t.Fatal(err)
		}
		if err := atomic.batch.Write(); err != nil {
			t.Fatal(err)
		}
	}
	writeChange(1800, []byte("recent-history"), 1)
	legacy := &rawdb.StateDomainChange{BlockNum: 2500, TxNum: 2500, Seq: 1,
		FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: common.Address{0x41, 3},
		Generation: 7, Domain: kvdomains.SystemDelegation, Key: []byte("legacy-repair"),
		PrevExists: true, Prev: []byte("recent-repair")}
	if err := rawdb.WriteStateDomainChangeRow(hot, legacy); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(cold, 0o700); err != nil {
		t.Fatal(err)
	}
	var refs []statesnapshots.SegmentRef
	for _, interval := range [][2]uint64{{1024, 1279}, {1280, 2047}} {
		path := fmt.Sprintf("history/state-domain-change-%d-%d.seg", interval[0], interval[1])
		part, err := statesnapshots.BuildStateDomainChangeHistorySegmentsFromDBByBlockRangeContext(
			context.Background(), hot, cold, interval[0], interval[1], interval[0], interval[1], path, etl.Options{})
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, part...)
	}
	identity, err := snapshotExpectedChainIdentityFromGenesis(genesis, "")
	if err != nil {
		t.Fatal(err)
	}
	manifest := statesnapshots.NewManifestForChain(1024, 2047, refs, identity)
	if err := statesnapshots.PublishManifest(cold, manifest); err != nil {
		t.Fatal(err)
	}
	for _, number := range []uint64{1030, 1300} {
		if err := rawdb.DeleteStateDomainChanges(hot, number); err != nil {
			t.Fatal(err)
		}
	}
	if syncer, ok := hot.(interface{ SyncKeyValue() error }); ok {
		if err := syncer.SyncKeyValue(); err != nil {
			t.Fatal(err)
		}
	}
	candidate, err := runningHistoryStagingExecutableSHA256()
	if err != nil {
		t.Fatal(err)
	}
	job := strings.Repeat("a", 32)
	return historyStagingE2EFixture{datadir: datadir, cold: cold, config: config,
		job: job, candidate: candidate, args: []string{
			"--datadir", datadir, "--snapshot.dir", cold, "--config", config,
			"--job-id", job, "--candidate-sha256", candidate,
			"--max-row-mib", "4", "--max-batch-mib", "4", "--max-bucket-mib", "16",
			"--max-work-mib", "64", "--max-decoded-mib", "16", "--min-free-gib", "1",
		}}
}

func (f historyStagingE2EFixture) command(t *testing.T, name string, extra ...string) (historyStagingCLIEvent, error) {
	t.Helper()
	output, err := f.commandRaw(t, name, extra...)
	if err != nil {
		return historyStagingCLIEvent{}, err
	}
	var event historyStagingCLIEvent
	if err := json.Unmarshal(output, &event); err != nil {
		return event, fmt.Errorf("decode %s output %q: %w", name, string(output), err)
	}
	return event, nil
}

func (f historyStagingE2EFixture) commandRaw(t *testing.T, name string, extra ...string) ([]byte, error) {
	t.Helper()
	var out, errOut bytes.Buffer
	app := &cli.App{Writer: &out, ErrWriter: &errOut, Commands: []*cli.Command{dbHistoryStagingCommand()}}
	args := append([]string{"gtron", "history-staging", name}, f.args...)
	args = append(args, extra...)
	err := app.Run(args)
	if err != nil {
		return nil, err
	}
	if (name == "migrate" || name == "apply" || name == "resume" ||
		(name == "inspect" && slices.Contains(extra, "--verify-complete"))) &&
		!strings.Contains(errOut.String(), "history-staging progress phase="+name) {
		t.Fatalf("%s produced no stderr progress: %q", name, errOut.String())
	}
	return append([]byte(nil), out.Bytes()...), nil
}

func TestHistoryStagingCLIRealDualPebbleMigration(t *testing.T) {
	f := newHistoryStagingE2EFixture(t)
	partial, err := f.command(t, "migrate", "--max-buckets", "1")
	if err != nil || partial.PlanID == "" || partial.Phase != "migrate" {
		t.Fatalf("partial plan: %+v %v", partial, err)
	}
	if _, err := f.command(t, "inspect", "--verify-complete", "--plan-id", partial.PlanID); err == nil {
		t.Fatal("partial plan activated reader")
	}
	// A second durable job identifies the full inventory independently.
	f.job = strings.Repeat("b", 32)
	f.args[7] = f.job
	plan, err := f.command(t, "migrate", "--max-buckets", "0")
	if err != nil || plan.PlanID == "" || plan.Phase != "migrate" {
		t.Fatalf("full plan: %+v %v", plan, err)
	}
	if event, err := f.command(t, "apply", "--plan-id", plan.PlanID); err != nil || event.Phase != "apply" || !event.Durable {
		t.Fatalf("apply event %+v: %v", event, err)
	}
	if event, err := f.command(t, "resume", "--plan-id", plan.PlanID); err != nil || event.Phase != "resume" || !event.Durable {
		t.Fatalf("idempotent resume event %+v: %v", event, err)
	}
	verified, err := f.command(t, "inspect", "--verify-complete", "--plan-id", plan.PlanID)
	if err != nil || verified.Phase != "inspect" || !verified.VerifiedComplete || !verified.Durable ||
		verified.HistoryWindow != 1 || verified.PruneMode != "archive" {
		t.Fatalf("verify complete: %+v %v", verified, err)
	}
	raw, err := f.commandRaw(t, "inspect", "--verify-complete", "--plan-id", plan.PlanID)
	if err != nil {
		t.Fatal(err)
	}
	scriptDir, err := filepath.Abs("../../scripts")
	if err != nil {
		t.Fatal(err)
	}
	python := `import json,sys;sys.path.insert(0,sys.argv[1]);import history_staging_migrate as m;record=json.load(sys.stdin);latch={'job_id':sys.argv[2],'candidate_sha256':sys.argv[3],'source':sys.argv[4],'target':sys.argv[5]};m.validate_cli_record(latch,record);assert record['phase']=='inspect' and record['verified_complete'] is True and record['durable'] is True and record['prune_mode']=='archive' and record['history_window']==1`
	source, err := filepath.EvalSymlinks(chainDataDir(f.datadir))
	if err != nil {
		t.Fatal(err)
	}
	target, err := filepath.EvalSymlinks(defaultHistoryStagingDir(f.datadir))
	if err != nil {
		t.Fatal(err)
	}
	check := exec.Command("python3", "-c", python, scriptDir, f.job, f.candidate,
		source, target)
	check.Stdin = bytes.NewReader(raw)
	if output, err := check.CombinedOutput(); err != nil {
		t.Fatalf("actual Go JSON rejected by Python orchestrator: raw=%s expected job=%s source=%s target=%s python=%s: %v",
			raw, f.job, source, target, output, err)
	}
}

func TestHistoryStagingCLILegacyManifestScopedAdmission(t *testing.T) {
	f := newHistoryStagingE2EFixture(t)
	manifest, err := statesnapshots.LoadProductionManifest(f.cold)
	if err != nil {
		t.Fatal(err)
	}
	manifest.Chain = nil
	if err := statesnapshots.PublishManifest(f.cold, manifest); err != nil {
		t.Fatal(err)
	}
	if _, err := f.command(t, "inspect"); err == nil || !strings.Contains(err.Error(), "legacy-manifest-sha256") {
		t.Fatalf("unbound manifest admitted without explicit pin: %v", err)
	}
	f.args = append(f.args, "--legacy-manifest-sha256", strings.Repeat("0", 64))
	if _, err := f.command(t, "inspect"); err == nil || !strings.Contains(err.Error(), "legacy-manifest-sha256") {
		t.Fatalf("wrong manifest pin admitted: %v", err)
	}
	pin, err := historyStagingFileSHA256(filepath.Join(f.cold, statesnapshots.ManifestFile))
	if err != nil {
		t.Fatal(err)
	}
	f.args[len(f.args)-1] = pin
	pristine, err := f.command(t, "inspect", "--verify-pristine")
	if err != nil || !pristine.Pristine || pristine.Phase != "inspect" {
		t.Fatalf("pristine preflight: %+v %v", pristine, err)
	}
	// The first cold trio endpoint must match the stopped canonical source.
	hot, err := rawdb.NewPebbleDB(chainDataDir(f.datadir), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	endpoint, present, err := rawdb.ReadStateTxRange(hot, 1024)
	if err != nil || !present {
		t.Fatalf("read endpoint: %v", err)
	}
	if err := rawdb.WriteStateTxRange(hot, 1024, common.Hash{1}, endpoint.BeginTxNum, endpoint.EndTxNum); err != nil {
		t.Fatal(err)
	}
	if err := hot.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.command(t, "inspect"); err == nil || !strings.Contains(err.Error(), "boundary proof") {
		t.Fatalf("wrong cold/canonical endpoint admitted: %v", err)
	}
	hot, err = rawdb.NewPebbleDB(chainDataDir(f.datadir), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateTxRange(hot, 1024, endpoint.BlockHash, endpoint.BeginTxNum, endpoint.EndTxNum); err != nil {
		t.Fatal(err)
	}
	if err := hot.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.command(t, "inspect"); err != nil {
		t.Fatalf("valid scoped cold admission: %v", err)
	}
	var coldHistoryPath string
	for _, ref := range manifest.Segments {
		if ref.Kind == statesnapshots.SegmentHistory &&
			ref.NormalizedDataset() == statesnapshots.SegmentDatasetStateDomainChange {
			coldHistoryPath = filepath.Join(f.cold, ref.Path)
			break
		}
	}
	if coldHistoryPath == "" {
		t.Fatal("fixture has no cold state-history segment")
	}
	coldBytes, err := os.ReadFile(coldHistoryPath)
	if err != nil || len(coldBytes) == 0 {
		t.Fatalf("read cold history: %v", err)
	}
	coldBytes[len(coldBytes)-1] ^= 1
	if err := os.WriteFile(coldHistoryPath, coldBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := f.command(t, "migrate", "--max-buckets", "0"); err == nil {
		t.Fatal("corrupt cold history admitted by full migration proof")
	}
	coldBytes[len(coldBytes)-1] ^= 1
	if err := os.WriteFile(coldHistoryPath, coldBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	plan, err := f.command(t, "migrate", "--max-buckets", "0")
	if err != nil || plan.PlanID == "" {
		t.Fatalf("unbound plan: %+v %v", plan, err)
	}
	if _, err := f.command(t, "inspect", "--verify-pristine"); err == nil {
		t.Fatal("planned migration still reported pristine")
	}
	if _, err := f.command(t, "apply", "--plan-id", plan.PlanID); err != nil {
		t.Fatal(err)
	}
	if verified, err := f.command(t, "inspect", "--verify-complete", "--plan-id", plan.PlanID); err != nil || !verified.VerifiedComplete {
		t.Fatalf("unbound verify complete: %+v %v", verified, err)
	}
	finalManifest, err := statesnapshots.LoadProductionManifest(f.cold)
	if err != nil || finalManifest.Chain != nil {
		t.Fatalf("scoped admission relabeled global manifest: %+v %v", finalManifest, err)
	}
	hot, err = rawdb.NewPebbleDB(chainDataDir(f.datadir), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer hot.Close()
	stage, err := rawdb.NewHistoryStagingPebbleDB(defaultHistoryStagingDir(f.datadir), 16, 16, false)
	if err != nil {
		t.Fatal(err)
	}
	defer stage.Close()
	identity, present, err := rawdb.ReadHistoryStagingIdentity(hot)
	if err != nil || !present {
		t.Fatalf("staging identity: %v", err)
	}
	manager, err := rawdb.NewHistoryStagingManager(hot, stage, identity)
	if err != nil || manager.VerifyIdentity() != nil {
		t.Fatalf("staging manager: %v", err)
	}
	genesis := params.DefaultMainnetGenesis()
	bc, err := core.NewBlockChainWithAncient(hot, corestate.NewDatabase(rawdb.WrapKeyValueStore(hot)), genesis.Config, rawdb.NoopAncient{})
	if err != nil {
		t.Fatal(err)
	}
	defer bc.Close()
	coldManager, err := statesnapshots.OpenManager(f.cold)
	if err != nil {
		t.Fatal(err)
	}
	bc.SetStateCodeColdHistory(coldManager)
	if err := bc.SetHistoryStagingManager(manager); err != nil {
		t.Fatal(err)
	}
	if err := statesnapshots.BindHistoryStagingColdRetention(f.cold, manager); err != nil {
		t.Fatal(err)
	}
	if err := bc.VerifyHistoryStagingRuntimeReady(context.Background()); err != nil {
		t.Fatalf("unbound manifest runtime startup: %v", err)
	}
}

func TestHistoryStagingCLIDefaultLimitsReachPhysicalInventory(t *testing.T) {
	f := newHistoryStagingE2EFixture(t)
	for i := len(f.args) - 2; i >= 0; i-- {
		switch f.args[i] {
		case "--max-row-mib", "--max-batch-mib", "--max-bucket-mib",
			"--max-work-mib", "--max-decoded-mib", "--min-free-gib":
			f.args = slices.Delete(f.args, i, i+2)
		}
	}
	invalid := f
	invalid.args = append(slices.Clone(f.args), "--max-decoded-mib", "129")
	if _, err := invalid.command(t, "migrate", "--max-buckets", "1"); err == nil ||
		!strings.Contains(err.Error(), "invalid --max-decoded-mib") {
		t.Fatalf("129 MiB was not rejected before inventory: %v", err)
	}
	if _, err := os.Stat(historyStagingPlanDirectory(f.datadir)); !os.IsNotExist(err) {
		t.Fatalf("invalid decoded budget created a plan: %v", err)
	}
	plan, err := f.command(t, "migrate", "--max-buckets", "1")
	if err != nil || plan.PlanID == "" || plan.Bucket != 1 {
		t.Fatalf("default limits failed real first-bucket inventory: %+v %v", plan, err)
	}
	data, err := os.ReadFile(filepath.Join(historyStagingPlanDirectory(f.datadir), plan.PlanID+".jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var header historyStagingPlanHeader
	if err := json.Unmarshal(bytes.SplitN(data, []byte{'\n'}, 2)[0], &header); err != nil {
		t.Fatal(err)
	}
	if header.Limits.MaxDecodedBytes != rawdb.HistoryStagingMaxDecodedBytes ||
		header.Limits.MaxRowBytes != 16<<20 || header.Limits.MaxBatchBytes != 32<<20 ||
		header.Limits.MaxBucketBytes != 4096<<20 || header.Limits.MaxWorkBytes != 8192<<20 ||
		header.Limits.MinFreeBytes != 16<<30 {
		t.Fatalf("frozen plan did not use actual CLI defaults: %+v", header.Limits)
	}
}

func historyStagingE2EPlanBucket(t *testing.T, f historyStagingE2EFixture, planID string) historyStagingPlanBucket {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(historyStagingPlanDirectory(f.datadir), planID+".jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(data, []byte{'\n'})
	if len(lines) < 3 {
		t.Fatal("frozen plan missing first bucket")
	}
	var bucket historyStagingPlanBucket
	if err := json.Unmarshal(lines[1], &bucket); err != nil {
		t.Fatal(err)
	}
	return bucket
}

func TestHistoryStagingCLIResumeAfterDurablePhaseCrashes(t *testing.T) {
	for _, phase := range []string{"copy", "adopt", "partial-clear"} {
		t.Run(phase, func(t *testing.T) {
			f := newHistoryStagingE2EFixture(t)
			plan, err := f.command(t, "migrate")
			if err != nil {
				t.Fatal(err)
			}
			bucket := historyStagingE2EPlanBucket(t, f, plan.PlanID)
			planData, err := os.ReadFile(filepath.Join(historyStagingPlanDirectory(f.datadir), plan.PlanID+".jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			var header historyStagingPlanHeader
			if err := json.Unmarshal(bytes.SplitN(planData, []byte{'\n'}, 2)[0], &header); err != nil {
				t.Fatal(err)
			}
			hot, err := rawdb.NewPebbleDB(chainDataDir(f.datadir), 16, 16)
			if err != nil {
				t.Fatal(err)
			}
			stage, err := rawdb.NewHistoryStagingPebbleDB(defaultHistoryStagingDir(f.datadir), 16, 16, false)
			if err != nil {
				hot.Close()
				t.Fatal(err)
			}
			identity := historyStagingIdentity(header.Paths, header.GenesisHash, header.NetworkID)
			manager, err := rawdb.NewHistoryStagingManager(hot, stage, identity)
			if err != nil {
				stage.Close()
				hot.Close()
				t.Fatal(err)
			}
			background := context.Background()
			if err := manager.Initialize(background); err != nil {
				stage.Close()
				hot.Close()
				t.Fatal(err)
			}
			if err := manager.InitializeOfflineSourceRoutes(background, 1, 1, 3); err != nil {
				stage.Close()
				hot.Close()
				t.Fatal(err)
			}
			manifest, err := statesnapshots.LoadProductionManifest(f.cold)
			if err != nil {
				stage.Close()
				hot.Close()
				t.Fatal(err)
			}
			binding := rawdb.HistoryStagingColdBinding{Version: rawdb.HistoryStagingFormatVersion,
				Bucket: 1, Epoch: 1, BindingEpoch: 1, ManifestEpoch: manifest.Generation,
				Spans: bucket.Proof.ColdSpans}
			if err := manager.CertifyColdRange(background, binding, func() error {
				return statesnapshots.VerifyHistoryStagingColdBinding(background, f.cold, manifest, binding, bucket.Proof.Blocks)
			}); err != nil {
				stage.Close()
				hot.Close()
				t.Fatal(err)
			}
			claimID, err := historyStagingClaimID(plan.PlanID, 1)
			if err != nil {
				stage.Close()
				hot.Close()
				t.Fatal(err)
			}
			claim, err := manager.BeginClaim(background, bucket.Proof, claimID)
			if err != nil {
				stage.Close()
				hot.Close()
				t.Fatal(err)
			}
			limits := rawdb.HistoryStagingLimits{MaxRowBytes: 4 << 20, MaxBucketBytes: 16 << 20,
				MaxBatchBytes: 4 << 20, MaxWorkBytes: 64 << 20, MaxDecodedBytes: 16 << 20,
				MinFreeBytes: 1, FreeBytes: func() (uint64, error) { return 1 << 30, nil }}
			if _, err := manager.CopyClaim(background, claim, limits); err != nil {
				stage.Close()
				hot.Close()
				t.Fatal(err)
			}
			if phase != "copy" {
				if _, err := manager.AdoptClaim(background, claim, bucket.Proof); err != nil {
					stage.Close()
					hot.Close()
					t.Fatal(err)
				}
			}
			if phase == "partial-clear" {
				calls := 0
				limits.MaxRowBytes = 64
				limits.MaxBatchBytes = 128
				limits.FreeBytes = func() (uint64, error) {
					calls++
					if calls > 1 {
						return 0, nil
					}
					return 1 << 30, nil
				}
				if err := manager.ClearSource(background, claim, limits); err == nil {
					stage.Close()
					hot.Close()
					t.Fatal("expected interrupted clear")
				}
			}
			if err := stage.Close(); err != nil {
				t.Fatal(err)
			}
			if err := hot.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := f.command(t, "resume", "--plan-id", plan.PlanID); err != nil {
				t.Fatalf("resume after %s: %v", phase, err)
			}
			verified, err := f.command(t, "inspect", "--verify-complete", "--plan-id", plan.PlanID)
			if err != nil || !verified.VerifiedComplete {
				t.Fatalf("verify after %s: %+v %v", phase, verified, err)
			}
		})
	}
}

func TestHistoryStagingCLIRejectsChangedHeadOrPlanBeforeSourceDelete(t *testing.T) {
	for _, mutation := range []string{"finish", "plan"} {
		t.Run(mutation, func(t *testing.T) {
			f := newHistoryStagingE2EFixture(t)
			plan, err := f.command(t, "migrate")
			if err != nil {
				t.Fatal(err)
			}
			if mutation == "finish" {
				hot, err := rawdb.NewPebbleDB(chainDataDir(f.datadir), 16, 16)
				if err != nil {
					t.Fatal(err)
				}
				hash, present, err := rawdb.ReadBlockHashByNumberStrict(hot, 3072)
				if err != nil || !present {
					hot.Close()
					t.Fatal("missing solid block", err)
				}
				if err := rawdb.WriteStageProgressWithHash(hot, rawdb.StageFinish, 3072, hash); err != nil {
					hot.Close()
					t.Fatal(err)
				}
				if err := hot.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				path := filepath.Join(historyStagingPlanDirectory(f.datadir), plan.PlanID+".jsonl")
				file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := file.Write([]byte("\n")); err != nil {
					file.Close()
					t.Fatal(err)
				}
				if err := file.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := f.command(t, "apply", "--plan-id", plan.PlanID); err == nil {
				t.Fatal("changed source or plan accepted")
			}
			if _, err := os.Stat(defaultHistoryStagingDir(f.datadir)); !os.IsNotExist(err) {
				t.Fatalf("target created before input validation: %v", err)
			}
			hot, err := rawdb.NewPebbleDBReadOnly(chainDataDir(f.datadir), 16, 16)
			if err != nil {
				t.Fatal(err)
			}
			defer hot.Close()
			if row, present, err := rawdb.ReadStateDomainChange(hot, 1800, 1); err != nil || !present || row == nil {
				t.Fatalf("source history changed: present=%v err=%v", present, err)
			}
		})
	}
}
