package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestStageDigestIncludesEmptyAndFFKeysAndFraming(t *testing.T) {
	f := newRetireFixture(t, false)
	ctx := context.Background()
	before, err := digestStageLive(ctx, f.stage)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range [][]byte{nil, {0xff, 0xff}} {
		if err := f.stage.Put(key, []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	after, err := digestStageLive(ctx, f.stage)
	if err != nil || after.Rows != before.Rows+2 || after.SHA256 == before.SHA256 {
		t.Fatal(after, err)
	}
	if err := f.stage.Put([]byte{0xff, 0xff}, []byte("other")); err != nil {
		t.Fatal(err)
	}
	changed, err := digestStageLive(ctx, f.stage)
	if err != nil || changed.Rows != after.Rows || changed.LogicalBytes != after.LogicalBytes || changed.SHA256 == after.SHA256 {
		t.Fatal(changed, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := digestStageLive(canceled, f.stage); err == nil {
		t.Fatal("canceled digest succeeded")
	}
}

func TestCompactStageDryRunIsReadOnlyAndWriterRequiresFence(t *testing.T) {
	f := newRetireFixture(t, false)
	f.hot.Close()
	f.stage.Close()
	args := []string{"--hot-dir", f.hotPath, "--stage-dir", f.stagePath}
	hotBefore, stageBefore := databaseFileHashes(t, f.hotPath), databaseFileHashes(t, f.stagePath)
	if err := runCompactStage(args); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(hotBefore, databaseFileHashes(t, f.hotPath)) || !reflect.DeepEqual(stageBefore, databaseFileHashes(t, f.stagePath)) {
		t.Fatal("dryrun changed files")
	}
	if err := runCompactStage(append(args, "--yes")); err == nil {
		t.Fatal("writer accepted absent fences")
	}
	if err := runCompactStage([]string{"--hot-dir", f.hotPath, "--stage-dir", f.hotPath}); err == nil {
		t.Fatal("overlapping stores accepted")
	}
}

// Exercise the real CLI in a subprocess with the exact inherited FD9 contract;
// never replace a descriptor in the concurrent Go test runner itself.
func TestCompactStageCLIWithInheritedLock(t *testing.T) {
	const childEnv = "GTRON_COMPACT_STAGE_TEST_CHILD"
	if os.Getenv(childEnv) == "1" {
		var args []string
		if err := json.Unmarshal([]byte(os.Getenv("GTRON_COMPACT_STAGE_TEST_ARGS")), &args); err != nil {
			t.Fatal(err)
		}
		err := runCompactStage(args)
		wantFail := os.Getenv("GTRON_COMPACT_STAGE_EXPECT_FAIL") == "1"
		if (err != nil) != wantFail {
			t.Fatalf("unexpected compact error: %v", err)
		}
		return
	}
	for _, failFloor := range []bool{false, true} {
		t.Run(fmt.Sprintf("floor_failure_%v", failFloor), func(t *testing.T) {
			f := newRetireFixture(t, false)
			// Keep boundary keys and mixed live/deleted stage payload. All live
			// stage content, including the TARGET receipt, must survive compaction.
			for _, key := range [][]byte{nil, {0xff}} {
				if err := f.stage.Put(key, bytes.Repeat([]byte("live"), 1024)); err != nil {
					t.Fatal(err)
				}
			}
			for i := 0; i < 32; i++ {
				key := []byte(fmt.Sprintf("stage-test-obsolete-%04d", i))
				if err := f.stage.Put(key, bytes.Repeat([]byte("deleted"), 2048)); err != nil {
					t.Fatal(err)
				}
				if err := f.stage.Delete(key); err != nil {
					t.Fatal(err)
				}
			}
			expected, err := digestStageLive(context.Background(), f.stage)
			if err != nil {
				t.Fatal(err)
			}
			f.hot.Close()
			f.stage.Close()
			hotFiles := databaseFileHashes(t, f.hotPath)
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
			floor := "1"
			if failFloor {
				floor = "1048576"
			}
			args := []string{"--hot-dir", f.hotPath, "--stage-dir", f.stagePath, "--start-lock", lockPath, "--hold-file", holdPath, "--min-free-gib", floor, "--max-sst-write-gib", "1", "--target-sst-mib", "1", "--allow-wal-replay", "--yes"}
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
			cmd := exec.Command(os.Args[0], "-test.run=^TestCompactStageCLIWithInheritedLock$")
			cmd.ExtraFiles = extras
			expectFail := "0"
			if failFloor {
				expectFail = "1"
			}
			cmd.Env = append(os.Environ(), childEnv+"=1", "GTRON_COMPACT_STAGE_TEST_ARGS="+string(data), "GTRON_COMPACT_STAGE_EXPECT_FAIL="+expectFail)
			var out, stderr bytes.Buffer
			cmd.Stdout = &out
			cmd.Stderr = &stderr
			if err := cmd.Run(); err != nil {
				t.Fatalf("CLI failed: %v\n%s\n%s", err, out.String(), stderr.String())
			}
			var report stageCompactReport
			if err := json.NewDecoder(&out).Decode(&report); err != nil {
				t.Fatal(err, out.String())
			}
			if !report.ProtectedStateVerified || report.LiveBefore == nil || report.LiveAfter == nil || *report.LiveBefore != expected || *report.LiveAfter != expected {
				t.Fatalf("bad preservation report %+v", report)
			}
			if failFloor {
				if report.Error == "" || report.Phase == "complete" || !strings.Contains(report.Error, "free") {
					t.Fatalf("bad failure report %+v", report)
				}
			} else if report.Error != "" || report.Phase != "complete" || report.Result == nil {
				t.Fatalf("bad completion report %+v", report)
			}
			if !reflect.DeepEqual(hotFiles, databaseFileHashes(t, f.hotPath)) {
				t.Fatal("hot physical files changed")
			}
			probe, err := os.OpenFile(lockPath, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer probe.Close()
			if err := syscall.Flock(int(probe.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err == nil {
				t.Fatal("parent lock was released")
			}
			s, err := rawdb.NewHistoryStagingPebbleDB(f.stagePath, 16, 32, true)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			actual, err := digestStageLive(context.Background(), s)
			if err != nil || actual != expected {
				t.Fatal(actual, err)
			}
		})
	}
}
