package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/urfave/cli/v2"
	"golang.org/x/sys/unix"
)

// Run only inside scripts/dev/history_staging_upgrade_root_test.py's private
// mount namespace. This test must never replace production migration fences.
func TestHistoryStagingUpgradeNativeRoot(t *testing.T) {
	if os.Getenv("GO_TRON_UPGRADE_ROOT_NAMESPACE") != "1" {
		t.Skip("requires isolated root mount namespace")
	}
	if runtime.GOOS != "linux" || os.Geteuid() != 0 {
		t.Fatal("Linux root required")
	}
	marker, err := os.ReadFile(historyStagingRootState + "NATIVE_UPGRADE_TEST_ONLY")
	if err != nil || string(marker) != "isolated mount namespace\n" {
		t.Fatal("missing isolation marker")
	}
	f := newHistoryStagingE2EFixture(t)
	plan, err := f.command(t, "migrate")
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(historyStagingPlanDirectory(f.datadir), plan.PlanID+".jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.SplitN(data, []byte{'\n'}, 2)
	var header historyStagingPlanHeader
	if err = json.Unmarshal(lines[0], &header); err != nil {
		t.Fatal(err)
	}
	// This fixture seals a synthetic older producer before any claims exist.
	// Only fixture construction changes bytes; the published digest then remains
	// immutable through preflight, executor handoff, resume and verification.
	oldSHA := strings.Repeat("d", 64)
	header.CandidateSHA256 = oldSHA
	line, _ := json.Marshal(header)
	data = append(append(line, '\n'), lines[1]...)
	digest := sha256.Sum256(data)
	planID := hex.EncodeToString(digest[:])
	planPath := filepath.Join(historyStagingPlanDirectory(f.datadir), planID+".jsonl")
	if err = os.WriteFile(planPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	hot, err := rawdb.NewPebbleDB(header.Paths.Source, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	stage, err := rawdb.NewHistoryStagingPebbleDB(header.Paths.Target, 16, 16, false)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := rawdb.NewHistoryStagingManager(hot, stage, historyStagingIdentity(header.Paths, header.GenesisHash, header.NetworkID))
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = manager.InitializeOfflineSourceRoutes(context.Background(), 1, 1, header.Head.HeadBlock/rawdb.StateHistoryChunkBucketBlocks); err != nil {
		t.Fatal(err)
	}
	// Leave a real durable claim under the sealed original digest.
	row := historyStagingE2EPlanBucket(t, f, planID)
	claimID, err := historyStagingClaimID(planID, row.Proof.Bucket)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = manager.BeginClaim(context.Background(), row.Proof, claimID); err != nil {
		t.Fatal(err)
	}
	hot.Close()
	stage.Close()
	var locks []*os.File
	for _, path := range []string{"/data/gtron/start.lock", filepath.Join(header.Paths.Source, ".history-staging-migration.lock"), filepath.Join(header.Paths.Target, ".history-staging-migration.lock"), filepath.Join(header.Paths.Cold, ".history-staging-migration.lock")} {
		lock, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			t.Fatal(err)
		}
		if err = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
			t.Fatal(err)
		}
		locks = append(locks, lock)
		defer lock.Close()
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	old := historyStagingUpgradeIdentity{JobID: f.job, SourceCommit: strings.Repeat("b", 40), CandidateSHA: oldSHA, Candidate: "/old-fixture-executor", Source: header.Paths.Source, Target: header.Paths.Target, Cold: header.Paths.Cold}
	newID := old
	newID.Candidate = exe
	newID.CandidateSHA = f.candidate
	newID.SourceCommit = strings.Repeat("e", 40)
	oldL := map[string]any{"version": 1, "state": "MIGRATION_IN_PROGRESS", "job_id": f.job, "plan_id": planID, "candidate": old.Candidate, "candidate_sha256": oldSHA, "source_commit": old.SourceCommit, "source": old.Source, "target": old.Target, "cold": old.Cold}
	oldP := map[string]any{"version": 1, "candidate_sha256": oldSHA, "source_commit": old.SourceCommit, "source": old.Source, "target": old.Target, "cold": old.Cold}
	newL, newP := map[string]any{}, map[string]any{}
	for k, v := range oldL {
		newL[k] = v
	}
	for k, v := range oldP {
		newP[k] = v
	}
	newL["candidate"] = exe
	newL["candidate_sha256"] = f.candidate
	newL["source_commit"] = newID.SourceCommit
	newL["plan_producer_sha256"] = oldSHA
	newL["upgrade_binding"] = historyStagingUpgradeBinding
	newP["candidate_sha256"] = f.candidate
	newP["source_commit"] = newID.SourceCommit
	enc := func(v any) json.RawMessage { b, _ := json.Marshal(v); return b }
	journal := historyStagingUpgradeJournal{Version: 1, State: "PRECHECK", JobID: f.job, PlanID: planID, ProducerSHA: oldSHA, Old: old, New: newID, OldLatch: enc(oldL), NewLatch: enc(newL), OldPrepared: enc(oldP), NewPrepared: enc(newP)}
	write := func(path string, v any) {
		t.Helper()
		if err := os.WriteFile(path, enc(v), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write(historyStagingUpgradeBinding, journal)
	write(historyStagingRootState+"MIGRATION_IN_PROGRESS.json", oldL)
	write(historyStagingRootState+"HISTORY_STAGING_PREPARED.json", oldP)
	write(historyStagingRepinIntent, map[string]any{"version": 1, "state": "REPIN_APPLY"})
	run := func(action string, binding, want bool) {
		t.Helper()
		args := append([]string{"gtron", "history-staging", action}, f.args...)
		args = append(args, "--plan-id", planID)
		if action == "inspect" {
			args = append(args, "--verify-complete")
		}
		if binding {
			args = append(args, "--upgrade-binding", historyStagingUpgradeBinding)
		}
		cmd := exec.Command(exe, "-test.run=^TestHistoryStagingUpgradeNativeChild$", "-test.v")
		cmd.Env = append(os.Environ(), "GO_TRON_UPGRADE_CHILD_ARGS="+string(enc(args)))
		output, err := cmd.CombinedOutput()
		if (err == nil) != want {
			t.Fatalf("native %s binding=%v want=%v: %v\n%s", action, binding, want, err, output)
		}
		if want && !bytes.Contains(output, []byte(`"candidate_sha256":"`+f.candidate+`"`)) {
			t.Fatalf("native event did not identify real executor: %s", output)
		}
	}
	run("upgrade-check", true, true)
	run("resume", false, false)
	// Only the private namespace fixture inode is changed. Preserve the held FD.
	startPath := "/data/gtron/start.lock"
	heldInfo, err := locks[0].Stat()
	if err != nil {
		t.Fatal(err)
	}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	uid, gid, accountErr := historyStagingSharedLockAccount()
	if accountErr == nil {
		must(locks[0].Chown(int(uid), int(gid)))
		must(locks[0].Chmod(0644))
		run("upgrade-check", true, true)
		same, err := os.Lstat(startPath)
		if err != nil || !os.SameFile(heldInfo, same) {
			t.Fatal("shared lock changed inode")
		}
		must(locks[0].Chown(int(uid)+1, int(gid)))
		run("upgrade-check", true, false)
		must(locks[0].Chown(int(uid), int(gid)))
		must(locks[0].Chmod(0666))
		run("upgrade-check", true, false)
		must(locks[0].Chmod(0644))
		// The identical service ownership is never allowed for a storage lock.
		must(locks[3].Chown(int(uid), int(gid)))
		must(locks[3].Chmod(0644))
		run("upgrade-check", true, false)
		must(locks[3].Chown(0, 0))
		must(locks[3].Chmod(0600))
	} else {
		t.Logf("java-tron shared lock account unavailable; root-private branch remains valid: %v", accountErr)
	}
	must(locks[0].Chown(0, 0))
	must(locks[0].Chmod(0666))
	run("upgrade-check", true, false)
	must(locks[0].Chmod(0600))
	must(os.Rename(startPath, startPath+".held"))
	must(os.Symlink(startPath+".held", startPath))
	run("upgrade-check", true, false)
	must(os.Remove(startPath))
	must(os.Rename(startPath+".held", startPath))
	must(os.Rename(startPath, startPath+".held"))
	must(os.WriteFile(startPath, nil, 0600))
	run("upgrade-check", true, false)
	must(os.Remove(startPath))
	must(os.Rename(startPath+".held", startPath))
	run("upgrade-check", true, true)
	unix.Flock(int(locks[3].Fd()), unix.LOCK_UN)
	run("upgrade-check", true, false)
	unix.Flock(int(locks[3].Fd()), unix.LOCK_EX)
	beforeConfig, _ := os.ReadFile(f.config)
	os.WriteFile(f.config, append(beforeConfig, []byte("\n# changed input\n")...), 0600)
	run("upgrade-check", true, false)
	os.WriteFile(f.config, beforeConfig, 0600)
	journal.State = "AUTHORIZED"
	write(historyStagingUpgradeBinding, journal)
	write(historyStagingRootState+"HISTORY_STAGING_PREPARED.json", newP)
	run("upgrade-check", true, true)
	run("resume", true, false)
	write(historyStagingRootState+"MIGRATION_IN_PROGRESS.json", newL)
	journal.State = "DONE"
	write(historyStagingUpgradeBinding, journal)
	os.Remove(historyStagingRepinIntent)
	run("resume", false, false)
	run("resume", true, true)
	run("inspect", true, true)
	verifiedHot, err := rawdb.NewPebbleDBReadOnly(header.Paths.Source, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer verifiedHot.Close()
	verifiedStage, err := rawdb.NewHistoryStagingPebbleDB(header.Paths.Target, 16, 16, true)
	if err != nil {
		t.Fatal(err)
	}
	defer verifiedStage.Close()
	verifiedManager, err := rawdb.NewHistoryStagingManager(verifiedHot, verifiedStage, historyStagingIdentity(header.Paths, header.GenesisHash, header.NetworkID))
	if err != nil {
		t.Fatal(err)
	}
	barrier, present, err := verifiedManager.ReadHistoryStagingRouteBarrier()
	actualSHA, shaErr := historyStagingCandidateBytes(f.candidate)
	if err != nil || shaErr != nil || !present || barrier.CandidateSHA != actualSHA {
		t.Fatal("native activation barrier did not pin actual replacement executor")
	}
	after, err := os.ReadFile(planPath)
	if err != nil || !bytes.Equal(after, data) {
		t.Fatal("executor replacement modified immutable original plan")
	}
	t.Log("real Linux root parent exact four FLOCKs, original sealed producer, real executor SHA, partial claim resume, and negative gates passed")
}
func TestHistoryStagingUpgradeNativeChild(t *testing.T) {
	raw := os.Getenv("GO_TRON_UPGRADE_CHILD_ARGS")
	if raw == "" {
		t.Skip("native CLI subprocess helper")
	}
	var args []string
	if err := json.Unmarshal([]byte(raw), &args); err != nil {
		t.Fatal(err)
	}
	app := &cli.App{Writer: os.Stdout, ErrWriter: os.Stderr, Commands: []*cli.Command{dbHistoryStagingCommand()}}
	if err := app.Run(args); err != nil {
		t.Fatal(err)
	}
}
