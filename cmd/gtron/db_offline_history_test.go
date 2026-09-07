package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	statepruning "github.com/tronprotocol/go-tron/core/state/pruning"
	statesnapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/params"
	"github.com/urfave/cli/v2"
)

func TestOfflineHistoryWorkBudgetIncludesFullCoverageVerification(t *testing.T) {
	plan := statepruning.OfflineHistoryPlan{Blocks: 1, BuildNeeded: true, VerificationBytes: 7 << 30, Input: statepruning.OfflineHistoryInputStats{ColdScratchUpperBytes: 2 << 30}}
	if n, err := offlineHistoryWorkBudget(plan); err != nil || n != 10<<30 {
		t.Fatalf("budget=%d error=%v", n, err)
	}
	plan.BuildNeeded = false
	if n, err := offlineHistoryWorkBudget(plan); err != nil || n != 8<<30 {
		t.Fatalf("reuse budget=%d error=%v", n, err)
	}
	plan.BuildNeeded = true
	plan.Input.ColdScratchUpperBytes = math.MaxUint64
	if _, err := offlineHistoryWorkBudget(plan); err == nil {
		t.Fatal("overflow accepted")
	}
	plan.BuildNeeded = false
	plan.VerificationBytes = math.MaxUint64
	if _, err := offlineHistoryWorkBudget(plan); err == nil {
		t.Fatal("reuse verification overflow accepted")
	}
	plan.VerificationBytes = math.MaxUint64 - (1 << 30)
	if n, err := offlineHistoryWorkBudget(plan); err != nil || n != math.MaxUint64 {
		t.Fatalf("maximum exact budget=%d error=%v", n, err)
	}
	plan.Blocks = 0
	if n, err := offlineHistoryWorkBudget(plan); err != nil || n != 0 {
		t.Fatal("empty plan has a work budget")
	}
}

func TestOfflineHistoryWALRejectsSymlinkUndercount(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(t.TempDir(), "large-wal")
	file, err := os.Create(target)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(257 << 20); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(dir, "000001.log")); err != nil {
		t.Fatal(err)
	}
	if err := offlineHistoryCheckWAL(dir); err == nil || !strings.Contains(err.Error(), "regular WAL") {
		t.Fatalf("symlink was counted by link length rather than rejected: %v", err)
	}
}

func TestOfflineHistoryWritableOpenFailureHasNoTypedNilHandle(t *testing.T) {
	current := rawdb.NewMemoryDatabase()
	defer current.Close()
	parent := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(parent, []byte("sentinel"), 0600); err != nil {
		t.Fatal(err)
	}
	next, err := offlineHistoryReopenWritable(context.Background(), current, filepath.Join(parent, "chaindata"))
	if err == nil || next != nil {
		t.Fatalf("failed writable Open returned an owned/typed-nil handle: db=%T error=%v", next, err)
	}
	// This is the caller's deferred-close path after Open failed. The old
	// handle remains a valid object; its repeated Close must not panic.
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(parent); err != nil || string(data) != "sentinel" {
		t.Fatalf("failed Open altered unrelated file: %q %v", data, err)
	}
}

func TestOfflineHistoryCancellationPreventsWritableTransition(t *testing.T) {
	current := rawdb.NewMemoryDatabase()
	defer current.Close()
	if err := current.Put([]byte("sentinel"), []byte("unchanged")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	path := filepath.Join(t.TempDir(), "must-not-be-created")
	next, err := offlineHistoryReopenWritable(ctx, current, path)
	if !errors.Is(err, context.Canceled) || next != nil {
		t.Fatalf("cancelled transition: db=%T error=%v", next, err)
	}
	if value, err := current.Get([]byte("sentinel")); err != nil || string(value) != "unchanged" {
		t.Fatalf("cancelled transition closed or changed source: %q %v", value, err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("cancelled transition opened destination: %v", err)
	}
}

func TestOfflineHistoryErrorsAlwaysProduceJSON(t *testing.T) {
	for _, tc := range []struct {
		name   string
		args   []string
		cancel bool
		want   string
	}{
		{name: "required target", want: "--datadir is required"},
		{name: "retention floor", args: []string{"--datadir", t.TempDir(), "--hot-window", "65535"}, want: "at least 65536"},
		{name: "cancel before any open", cancel: true, args: []string{"--datadir", t.TempDir(), "--yes"}, want: "context canceled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			app := &cli.App{Writer: &stdout, ErrWriter: &stderr, Commands: []*cli.Command{dbOfflineHistoryCommand()}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancel {
				cancel()
			}
			err := app.RunContext(ctx, append([]string{"gtron", "offline-history"}, tc.args...))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v want=%q", err, tc.want)
			}
			var report offlineHistoryReport
			if decodeErr := json.Unmarshal(stdout.Bytes(), &report); decodeErr != nil {
				t.Fatalf("invalid failure JSON: %v: %s", decodeErr, stdout.String())
			}
			if report.Error != err.Error() || report.VerifiedAfterReopen || report.Result != nil {
				t.Fatalf("incorrect failure report: %+v returned=%v", report, err)
			}
		})
	}
}

func TestOfflineHistoryAdmissionFailureReportsPlanAndPreservesFiles(t *testing.T) {
	dir := t.TempDir()
	path := chainDataDir(dir)
	db, err := rawdb.NewPebbleDB(path, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const solid, head = uint64(65538), uint64(65539)
	for _, n := range []uint64{1, 2, solid, head} {
		block, _ := dbRebuildTxIndexBlock(t, n, 0)
		if err := rawdb.WriteBlock(db, block); err != nil {
			t.Fatal(err)
		}
		if n <= 2 {
			if err := rawdb.WriteStateTxRange(db, n, block.Hash(), n, n); err != nil {
				t.Fatal(err)
			}
			change := &rawdb.StateDomainChange{BlockNum: n, BlockHash: block.Hash(), TxNum: n, Seq: 1,
				FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: common.Address{0: common.AddressPrefixMainnet}, PrevExists: true, Prev: []byte("previous")}
			if err := rawdb.WriteStateDomainChangeBlockRows(db, []*rawdb.StateDomainChange{change}); err != nil {
				t.Fatal(err)
			}
		}
		if n == head {
			rawdb.WriteHeadBlockHash(db, block.Hash())
			rawdb.WriteDynamicProperty(db, "latest_block_header_hash", block.Hash().Bytes())
			for name, value := range map[string]uint64{"latest_block_header_number": head, "latest_solidified_block_num": solid} {
				buf := make([]byte, 8)
				binary.BigEndian.PutUint64(buf, value)
				rawdb.WriteDynamicProperty(db, name, buf)
			}
			for _, stage := range []rawdb.StageID{rawdb.StageExecution, rawdb.StageFinish} {
				if err := rawdb.WriteStageProgressWithHash(db, stage, head, block.Hash()); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if err := rawdb.WriteHistoryPruneMode(db, "snap"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	identity, err := snapshotExpectedChainIdentityFromGenesis(params.DefaultMainnetGenesis(), "")
	if err != nil {
		t.Fatal(err)
	}
	if err := statesnapshots.PublishManifest(stateSnapshotsDir(dir), statesnapshots.NewManifestForChain(0, 0, nil, identity)); err != nil {
		t.Fatal(err)
	}
	beforeDB := offlineFixtureFiles(t, path)
	beforeSnapshots := offlineFixtureFiles(t, stateSnapshotsDir(dir))
	var stdout, stderr bytes.Buffer
	app := &cli.App{Writer: &stdout, ErrWriter: &stderr, Commands: []*cli.Command{dbOfflineHistoryCommand()}}
	err = app.Run([]string{"gtron", "offline-history", "--datadir", dir, "--yes", "--max-work-gib", "1"})
	if err == nil || !strings.Contains(err.Error(), "above configured maximum") {
		t.Fatalf("want complete-budget admission failure, got %v", err)
	}
	var report offlineHistoryReport
	if decodeErr := json.Unmarshal(stdout.Bytes(), &report); decodeErr != nil {
		t.Fatalf("invalid admission JSON: %v: %s", decodeErr, stdout.String())
	}
	if report.Error != err.Error() || report.DryRun || report.Result != nil || report.Plan.Blocks != 2 || report.Plan.HistoryWindow != 65536 || report.WorkBudgetBytes <= 1<<30 {
		t.Fatalf("incorrect admission report: %+v", report)
	}
	for directory, before := range map[string]map[string][]byte{path: beforeDB, stateSnapshotsDir(dir): beforeSnapshots} {
		after := offlineFixtureFiles(t, directory)
		if len(before) != len(after) {
			t.Fatalf("failed admission changed file count in %s", directory)
		}
		for name, data := range before {
			if !bytes.Equal(data, after[name]) {
				t.Fatalf("failed admission changed %s", filepath.Join(directory, name))
			}
		}
	}
}

func TestOfflineHistoryRefusesUnboundedWALRecovery(t *testing.T) {
	dir := t.TempDir()
	file, err := os.Create(filepath.Join(dir, "000001.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := file.Truncate(256 << 20); err != nil {
		t.Fatal(err)
	}
	if err := offlineHistoryCheckWAL(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "000002.log"), []byte{1}, 0600); err != nil {
		t.Fatal(err)
	}
	if err := offlineHistoryCheckWAL(dir); err == nil {
		t.Fatal("aggregate recovery limit ignored")
	}
}
