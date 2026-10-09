package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	chainfreezer "github.com/tronprotocol/go-tron/core/freezer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	rawfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

func TestRepairTargetColdMetadataOnlyDoesNotCreateJournalOrCatalog(t *testing.T) {
	f := newRetireFixture(t, false)
	if err := f.hot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.stage.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := cleanupFileSHA256(filepath.Join(f.cold, snapshots.ManifestFile))
	if err != nil {
		t.Fatal(err)
	}
	out, err := os.CreateTemp(t.TempDir(), "report")
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	original := os.Stdout
	os.Stdout = out
	defer func() { os.Stdout = original }()
	err = runRepairTargetCold([]string{"--hot-dir", f.hotPath, "--stage-dir", f.stagePath, "--cold-dir", f.cold, "--manifest-sha256", fmtHash(before), "--from-bucket", "1", "--through-bucket", "1", "--from-tx", "1024", "--to-tx", "2047", "--history-window", "64"})
	os.Stdout = original
	if err != nil {
		t.Fatal(err)
	}
	if _, err := out.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	var report repairReport
	if err := json.NewDecoder(out).Decode(&report); err != nil {
		t.Fatal(err)
	}
	if !report.DryRun || report.Phase != "metadata_only" || report.Plan == nil || len(report.Plan.SourceRefs) != 3 {
		t.Fatalf("unexpected report %+v", report)
	}
	after, err := cleanupFileSHA256(filepath.Join(f.cold, snapshots.ManifestFile))
	if err != nil || before != after {
		t.Fatal("metadata-only changed manifest", err)
	}
	if _, err := os.Lstat(repairJournalPath(f.cold)); !os.IsNotExist(err) {
		t.Fatal("metadata-only created operation journal", err)
	}
	entries, err := os.ReadDir(f.cold)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if bytes.HasPrefix([]byte(entry.Name()), []byte("state-domain-change-repair-")) {
			t.Fatal("metadata-only built a replacement")
		}
	}
}

func TestRepairTargetColdFullCLIWithInheritedLock(t *testing.T) {
	testRepairTargetColdCLI(t, false, "")
}

func TestRepairTargetColdPartialBatchLeavesBadTailUncertified(t *testing.T) {
	testRepairTargetColdCLI(t, true, "")
}

func TestRepairTargetColdDurablePhaseRecovery(t *testing.T) {
	for _, tc := range []struct{ name, phase string }{{"prepared", journalPrepared}, {"manifest_handoff", "manifest-published"}, {"published_journal", journalPublished}, {"partial_certified", "certified-bucket-1"}, {"rebound", journalRebound}} {
		t.Run(tc.name, func(t *testing.T) { testRepairTargetColdCLI(t, true, tc.phase) })
	}
}

func TestRepairTargetColdFinalFileMutationCannotArchive(t *testing.T) {
	testRepairTargetColdCLI(t, true, "mutate-after-rebound")
}

func TestRepairTargetColdSplitLeftBoundaryResume(t *testing.T) {
	testRepairTargetColdCLIAt(t, false, journalPublished, 1500)
}

func testRepairTargetColdCLI(t *testing.T, partial bool, failPhase string) {
	testRepairTargetColdCLIAt(t, partial, failPhase, 1024)
}

func testRepairTargetColdCLIAt(t *testing.T, partial bool, failPhase string, fromTx uint64) {
	t.Helper()
	const childEnv = "GTRON_REPAIR_CLI_TEST_CHILD"
	if os.Getenv(childEnv) == "1" {
		var args []string
		if err := json.Unmarshal([]byte(os.Getenv("GTRON_REPAIR_CLI_TEST_ARGS")), &args); err != nil {
			t.Fatal(err)
		}
		hook := func(phase string) error {
			if phase == journalRebound && os.Getenv("GTRON_REPAIR_CLI_TEST_FAIL_PHASE") == "mutate-after-rebound" {
				var cold string
				for i := 0; i+1 < len(args); i++ {
					if args[i] == "--cold-dir" {
						cold = args[i+1]
					}
				}
				journal, found, err := readRepairJournal(cold)
				if err != nil || !found || len(journal.NewRefs) == 0 {
					return errors.New("fault fixture could not read rebound journal")
				}
				path := filepath.Join(cold, journal.NewRefs[0].Path)
				file, err := os.OpenFile(path, os.O_RDWR, 0)
				if err != nil {
					return err
				}
				defer file.Close()
				if _, err := file.WriteAt([]byte{0xff}, 0); err != nil {
					return err
				}
				return file.Sync()
			}
			if phase == os.Getenv("GTRON_REPAIR_CLI_TEST_FAIL_PHASE") {
				return errors.New("injected process interruption after " + phase)
			}
			return nil
		}
		if err := runRepairTargetColdInternal(args, hook); err != nil {
			fmt.Fprintln(os.Stderr, err)
			t.Fatal(err)
		}
		return
	}
	f := newRetireFixtureWithTail(t, false, partial)
	// The source rows were cleared after staging adoption. Rebuilding a cold
	// trio from that hot-only view reproduces the production zero-row bug;
	// TARGET still owns the actual nonzero history record.
	bad, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDBByBlockRange(f.hot, f.cold, 1024, 2047, 1024, 2047, "history/state-domain-change-zero-1024-2047.seg")
	if err != nil {
		t.Fatal(err)
	}
	f.manifest = snapshots.NewManifest(1024, 2047, bad)
	if err := snapshots.PublishManifest(f.cold, f.manifest); err != nil {
		t.Fatal(err)
	}
	ancientPath := filepath.Join(t.TempDir(), "ancient")
	writer, err := rawfreezer.NewFreezer(ancientPath, "", false, 1<<20, chainfreezer.FreezerTableSet())
	if err != nil {
		t.Fatal(err)
	}
	_, err = writer.ModifyAncients(func(op rawdb.AncientWriteOp) error {
		for n := uint64(0); n <= 2047; n++ {
			body := []byte{0}
			if n >= 1024 {
				body = rawdb.ReadBlockRaw(f.hot, n)
			}
			for _, table := range []string{rawdb.AncientBlocksTable, rawdb.AncientTxInfosTable, rawdb.AncientStateRootsTable} {
				payload := []byte{0}
				if table == rawdb.AncientBlocksTable {
					payload = body
				}
				if err := op.AppendRaw(table, n, payload); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err := errors.Join(err, writer.Sync(), writer.Close()); err != nil {
		t.Fatal(err)
	}
	if err := f.hot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.stage.Close(); err != nil {
		t.Fatal(err)
	}
	oldSHA, err := cleanupFileSHA256(filepath.Join(f.cold, snapshots.ManifestFile))
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
	toTx := "2047"
	if partial {
		toTx = "1034"
	}
	args := []string{"--hot-dir", f.hotPath, "--stage-dir", f.stagePath, "--cold-dir", f.cold, "--ancient-dir", ancientPath, "--manifest-sha256", fmtHash(oldSHA), "--from-bucket", "1", "--through-bucket", "1", "--from-tx", strconv.FormatUint(fromTx, 10), "--to-tx", toTx, "--history-window", "64", "--min-free-gib", "1", "--start-lock", lockPath, "--hold-file", holdPath, "--yes"}
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
	run := func(arguments []string, phase string) (repairReport, error) {
		t.Helper()
		data, _ := json.Marshal(arguments)
		cmd := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$")
		cmd.ExtraFiles = extras
		cmd.Env = append(os.Environ(), childEnv+"=1", "GTRON_REPAIR_CLI_TEST_ARGS="+string(data), "GTRON_REPAIR_CLI_TEST_FAIL_PHASE="+phase)
		var out, stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &out, &stderr
		cmdErr := cmd.Run()
		var report repairReport
		if err := json.NewDecoder(&out).Decode(&report); err != nil {
			t.Fatalf("repair report missing: %v\nstdout: %s\nstderr: %s", err, out.String(), stderr.String())
		}
		if cmdErr != nil && phase == "" {
			t.Fatalf("repair CLI failed: %v\nstdout: %s\nstderr: %s", cmdErr, out.String(), stderr.String())
		}
		return report, cmdErr
	}
	if failPhase != "" {
		failed, err := run(args, failPhase)
		if err == nil || failed.Error == "" {
			t.Fatal("fault injection did not stop this phase", failed)
		}
		journal, present, err := readRepairJournal(f.cold)
		if err != nil || !present {
			t.Fatal("durable journal missing after interruption", err)
		}
		if fromTx > 1024 && failPhase == journalPublished && len(journal.NewRefs) < 6*3 {
			t.Fatalf("split left boundary was not durably published: %d refs", len(journal.NewRefs))
		}
		wantPhase := failPhase
		if failPhase == "certified-bucket-1" {
			wantPhase = journalPublished
		} else if failPhase == "manifest-published" {
			wantPhase = journalPrepared
		} else if failPhase == "mutate-after-rebound" {
			wantPhase = journalRebound
		}
		if journal.Phase != wantPhase {
			t.Fatalf("journal phase=%s want=%s", journal.Phase, wantPhase)
		}
		current, err := cleanupFileSHA256(filepath.Join(f.cold, snapshots.ManifestFile))
		if err != nil {
			t.Fatal(err)
		}
		if current != journal.OldSHA && current != journal.CandidateSHA {
			t.Fatal("interrupted manifest differs from old and candidate")
		}
		if failPhase == "mutate-after-rebound" {
			if failed.ProtectedStateVerified == false || !strings.Contains(failed.Error, "cold files changed") {
				t.Fatalf("final physical change was not rejected after protected reopen: %+v", failed)
			}
			if _, err := os.Lstat(repairJournalPath(f.cold)); err != nil {
				t.Fatal("file mutation incorrectly archived durable journal", err)
			}
			return
		}
		for i := 0; i+1 < len(args); i++ {
			if args[i] == "--manifest-sha256" {
				args[i+1] = fmtHash(current)
				break
			}
		}
		args = append(args, "--resume")
	}
	report, err := run(args, "")
	if err != nil {
		t.Fatal(err)
	}
	if report.Phase != "complete" || !report.ProtectedStateVerified || report.Error != "" || report.CandidateSHA256 == "" {
		t.Fatalf("repair did not complete: %+v", report)
	}
	probe, err := os.OpenFile(lockPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Close()
	if err := syscall.Flock(int(probe.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err == nil {
		t.Fatal("child released inherited parent operation lock")
	}
	stage, err := rawdb.NewHistoryStagingPebbleDB(f.stagePath, 16, 16, true)
	if err != nil {
		t.Fatal(err)
	}
	defer stage.Close()
	hot, err := rawdb.NewPebbleDBReadOnly(f.hotPath, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer hot.Close()
	manager, err := rawdb.NewHistoryStagingManager(hot, stage, rawdb.HistoryStagingIdentity{Version: 1, GenesisHash: common.Hash{1}, NetworkID: 1, SourceID: [32]byte{2}, TargetID: [32]byte{3}})
	if err != nil {
		t.Fatal(err)
	}
	binding, present, err := manager.ReadColdBindingAt(1, 1)
	if err != nil || !present {
		t.Fatal("repair did not leave certified cold binding", err)
	}
	if partial {
		if len(binding.Spans) != 1 || binding.Spans[0].From != 1024 || binding.Spans[0].To != 1034 {
			t.Fatalf("repair certified unrepaired TARGET tail: %+v", binding.Spans)
		}
	} else if fromTx > 1024 {
		if len(binding.Spans) == 0 || binding.Spans[0].From != fromTx || binding.Spans[len(binding.Spans)-1].To != 2047 {
			t.Fatalf("repair certified outside selected TARGET suffix: %+v", binding.Spans)
		}
		for i := 1; i < len(binding.Spans); i++ {
			if binding.Spans[i].From != binding.Spans[i-1].To+1 {
				t.Fatalf("repair binding has a gap: %+v", binding.Spans)
			}
		}
	} else if !cleanupBindingCovers(1, binding) {
		t.Fatal("full repair did not cover the whole bucket")
	}
	view, err := manager.AcquireTargetBucketView(1, 1)
	if err != nil {
		t.Fatal("repair removed TARGET data", err)
	}
	defer view.Close()
	var rows int
	if err := rawdb.IterateStateHistorySpanBlocks(context.Background(), view, 1029, 1029, 1029, 1029, func(block *rawdb.StateHistorySpanBlock) (bool, error) {
		_, err := block.IterateRows(func(*rawdb.StateHistorySpanRow) (bool, error) { rows++; return true, nil })
		return true, err
	}); err != nil || rows != 1 {
		t.Fatal("TARGET not readable after repair", err)
	}
	if partial {
		rows = 0
		if err := rawdb.IterateStateHistorySpanBlocks(context.Background(), view, 1500, 1500, 1500, 1500, func(block *rawdb.StateHistorySpanBlock) (bool, error) {
			_, err := block.IterateRows(func(*rawdb.StateHistorySpanRow) (bool, error) { rows++; return true, nil })
			return true, err
		}); err != nil || rows != 1 {
			t.Fatal("unrepaired TARGET tail was removed", err)
		}
		if failPhase == "" {
			if err := errors.Join(view.Close(), hot.Close(), stage.Close()); err != nil {
				t.Fatal(err)
			}
			current, err := cleanupFileSHA256(filepath.Join(f.cold, snapshots.ManifestFile))
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i+1 < len(args); i++ {
				switch args[i] {
				case "--manifest-sha256":
					args[i+1] = fmtHash(current)
				case "--from-tx":
					args[i+1] = "1035"
				case "--to-tx":
					args[i+1] = "2047"
				}
			}
			// The first publication leaves several 128-block replacement trios.
			// The next batch intentionally replaces their complete overlap.
			args = append(args, "--max-source-trios=16")
			second, err := run(args, "")
			if err != nil || second.Phase != "complete" || !second.ProtectedStateVerified {
				t.Fatalf("next batch after done journal failed: %+v: %v", second, err)
			}
			stage2, err := rawdb.NewHistoryStagingPebbleDB(f.stagePath, 16, 16, true)
			if err != nil {
				t.Fatal(err)
			}
			defer stage2.Close()
			hot2, err := rawdb.NewPebbleDBReadOnly(f.hotPath, 16, 16)
			if err != nil {
				t.Fatal(err)
			}
			defer hot2.Close()
			manager2, err := rawdb.NewHistoryStagingManager(hot2, stage2, rawdb.HistoryStagingIdentity{Version: 1, GenesisHash: common.Hash{1}, NetworkID: 1, SourceID: [32]byte{2}, TargetID: [32]byte{3}})
			if err != nil {
				t.Fatal(err)
			}
			full, present, err := manager2.ReadColdBindingAt(1, 1)
			if err != nil || !present || !cleanupBindingCovers(1, full) {
				t.Fatal("second batch did not certify the whole bucket", err)
			}
		}
	}
}
