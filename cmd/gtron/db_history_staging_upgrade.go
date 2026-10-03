package main

// A sealed plan identifies its producer. An upgrade journal authorizes a
// different executor; it never changes the plan, claim IDs, or executable SHA.
import (
	"bytes"

	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"syscall"

	"github.com/tronprotocol/go-tron/core/rawdb"
	statesnapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/urfave/cli/v2"
	"golang.org/x/sys/unix"
)

const historyStagingUpgradeBinding = "/var/lib/gtron-history-staging/apply-upgrade.json"
const historyStagingRootState = "/data/gtron/main/"
const historyStagingRepinIntent = historyStagingRootState + "HISTORY_STAGING_REPIN_INTENT.json"

type historyStagingUpgradeIdentity struct {
	JobID        string `json:"job_id"`
	SourceCommit string `json:"source_commit"`
	CandidateSHA string `json:"candidate_sha256"`
	Candidate    string `json:"candidate"`
	Source       string `json:"source"`
	Target       string `json:"target"`
	Cold         string `json:"cold"`
}
type historyStagingUpgradeJournal struct {
	Version     int                           `json:"version"`
	State       string                        `json:"state"`
	JobID       string                        `json:"job_id"`
	PlanID      string                        `json:"plan_id"`
	ProducerSHA string                        `json:"plan_producer_sha256"`
	Old         historyStagingUpgradeIdentity `json:"old"`
	New         historyStagingUpgradeIdentity `json:"new"`
	OldLatch    json.RawMessage               `json:"old_latch"`
	NewLatch    json.RawMessage               `json:"new_latch"`
	OldPrepared json.RawMessage               `json:"old_prepared"`
	NewPrepared json.RawMessage               `json:"new_prepared"`
}

func historyStagingPlanProducer(c *historyStagingCLIContext) string {
	if c.planProducerSHA != "" {
		return c.planProducerSHA
	}
	return c.event.CandidateSHA256
}
func historyStagingRootJSON(path string, private bool) ([]byte, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || st.Uid != 0 || st.Nlink != 1 || info.Mode().Perm()&0022 != 0 || info.Size() <= 0 || info.Size() > 65536 || private && info.Mode().Perm() != 0600 {
		return nil, errors.New("unsafe root-owned upgrade state")
	}
	data, err := io.ReadAll(io.LimitReader(f, 65537))
	if err != nil {
		return nil, err
	}
	after, err := f.Stat()
	if err != nil || !historyStagingStableFileInfo(info, after) || int64(len(data)) != info.Size() {
		return nil, errors.New("upgrade state changed while reading")
	}
	return data, nil
}
func historyStagingStableFileInfo(before, after os.FileInfo) bool {
	a, b := reflect.ValueOf(before.Sys()).Elem(), reflect.ValueOf(after.Sys()).Elem()
	for i := 0; i < a.NumField(); i++ {
		name := a.Type().Field(i).Name
		if strings.HasPrefix(name, "Atim") {
			continue
		}
		if !reflect.DeepEqual(a.Field(i).Interface(), b.Field(i).Interface()) {
			return false
		}
	}
	return true
}
func historyStagingTrustedBindingParents(path string) error {
	for dir := filepath.Dir(path); ; dir = filepath.Dir(dir) {
		info, err := os.Lstat(dir)
		if err != nil {
			return err
		}
		st, ok := info.Sys().(*syscall.Stat_t)
		if dir == filepath.Dir(path) && info.Mode().Perm() != 0700 {
			return errors.New("upgrade binding directory must be root private")
		}
		if !ok || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || st.Uid != 0 || info.Mode().Perm()&0022 != 0 {
			return errors.New("unsafe upgrade binding ancestor")
		}
		if dir == "/" {
			return nil
		}
	}
}
func sameHistoryStagingJSON(a, b []byte) bool {
	var av, bv any
	return json.Unmarshal(a, &av) == nil && json.Unmarshal(b, &bv) == nil && reflect.DeepEqual(av, bv)
}
func historyStagingUpgradeActiveLatch(current, sealed []byte) bool {
	var cur, old map[string]any
	if json.Unmarshal(current, &cur) != nil || json.Unmarshal(sealed, &old) != nil {
		return false
	}
	if cur["state"] != "MIGRATION_IN_PROGRESS" && cur["state"] != "VERIFIED_PENDING_ACTIVATION" {
		return false
	}
	for k, v := range old {
		if k != "state" && !reflect.DeepEqual(cur[k], v) {
			return false
		}
	}
	for k := range cur {
		if _, ok := old[k]; !ok {
			switch k {
			case "prune_mode", "history_window", "activation_intent_sha256", "activation_complete":
			default:
				return false
			}
		}
	}
	return true
}

var historyStagingCommitPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)

func historyStagingUpgradeSnapshots(j historyStagingUpgradeJournal) error {
	var oldL, newL, oldP, newP map[string]any
	for _, item := range []struct {
		raw []byte
		out *map[string]any
	}{{j.OldLatch, &oldL}, {j.NewLatch, &newL}, {j.OldPrepared, &oldP}, {j.NewPrepared, &newP}} {
		if json.Unmarshal(item.raw, item.out) != nil {
			return errors.New("invalid upgrade identity snapshot")
		}
	}
	for _, item := range []struct {
		l, p map[string]any
		i    historyStagingUpgradeIdentity
	}{{oldL, oldP, j.Old}, {newL, newP, j.New}} {
		if item.l["version"] != float64(1) || item.p["version"] != float64(1) || item.l["state"] != "MIGRATION_IN_PROGRESS" || item.l["job_id"] != j.JobID || item.l["plan_id"] != j.PlanID || item.l["candidate"] != item.i.Candidate {
			return errors.New("upgrade snapshot job or plan differs")
		}
		for k, v := range map[string]string{"source": item.i.Source, "target": item.i.Target, "cold": item.i.Cold, "candidate_sha256": item.i.CandidateSHA, "source_commit": item.i.SourceCommit} {
			if item.l[k] != v || item.p[k] != v {
				return errors.New("upgrade snapshot executor identity differs")
			}
		}
	}
	if newL["plan_producer_sha256"] != j.ProducerSHA || newL["upgrade_binding"] != historyStagingUpgradeBinding {
		return errors.New("upgrade snapshot producer authority differs")
	}
	for k, v := range oldL {
		switch k {
		case "candidate", "candidate_sha256", "source_commit":
			continue
		}
		if !reflect.DeepEqual(newL[k], v) {
			return errors.New("upgrade changed frozen migration intent")
		}
	}
	for k, v := range oldP {
		switch k {
		case "candidate_sha256", "source_commit":
			continue
		}
		if !reflect.DeepEqual(newP[k], v) {
			return errors.New("upgrade changed prepared intent")
		}
	}
	for k := range newL {
		if _, ok := oldL[k]; !ok && k != "plan_producer_sha256" && k != "upgrade_binding" {
			return errors.New("unexpected upgrade latch field")
		}
	}
	if len(newP) != len(oldP) {
		return errors.New("unexpected prepared upgrade field")
	}
	return nil
}
func validateHistoryStagingUpgradeJournal(j historyStagingUpgradeJournal, c *historyStagingCLIContext, planID, action string, latch, prepared []byte, pending bool) error {
	if j.Version != 1 || j.JobID != c.event.JobID || j.PlanID != planID || !historyStagingSHAPattern.MatchString(j.ProducerSHA) || !historyStagingSHAPattern.MatchString(j.PlanID) ||
		j.ProducerSHA != j.Old.CandidateSHA || j.Old.JobID != j.JobID || j.New.JobID != j.JobID || j.New.CandidateSHA != c.event.CandidateSHA256 || !historyStagingSHAPattern.MatchString(j.Old.CandidateSHA) ||
		j.New.CandidateSHA == j.Old.CandidateSHA || !historyStagingSHAPattern.MatchString(j.New.CandidateSHA) ||
		j.Old.Source != c.paths.Source || j.New.Source != c.paths.Source || j.Old.Target != c.paths.Target || j.New.Target != c.paths.Target || j.Old.Cold != c.paths.Cold || j.New.Cold != c.paths.Cold ||
		!historyStagingCommitPattern.MatchString(j.Old.SourceCommit) || !historyStagingCommitPattern.MatchString(j.New.SourceCommit) || !filepath.IsAbs(j.New.Candidate) || !filepath.IsAbs(j.Old.Candidate) {
		return errors.New("history staging upgrade journal identity differs")
	}
	if err := historyStagingUpgradeSnapshots(j); err != nil {
		return err
	}
	// Snapshots are the sole authority for an interrupted prepared/latch switch.
	if action == "upgrade-check" {
		if j.State != "PRECHECK" && j.State != "AUTHORIZED" && j.State != "DONE" {
			return errors.New("unknown upgrade journal phase")
		}
		if !pending {
			return errors.New("upgrade preflight requires pending repin fence")
		}
		oldL, newL := sameHistoryStagingJSON(latch, j.OldLatch), sameHistoryStagingJSON(latch, j.NewLatch)
		oldP, newP := sameHistoryStagingJSON(prepared, j.OldPrepared), sameHistoryStagingJSON(prepared, j.NewPrepared)
		if !(oldL && oldP || j.State != "PRECHECK" && (oldL && newP || newL && newP)) {
			return errors.New("upgrade prepared/latch switch is inconsistent")
		}
	} else {
		if j.State != "DONE" || pending || !historyStagingUpgradeActiveLatch(latch, j.NewLatch) || !sameHistoryStagingJSON(prepared, j.NewPrepared) {
			return errors.New("upgrade is pending or durable executor binding differs")
		}
		if action != "apply" && action != "resume" && action != "inspect" {
			return errors.New("upgrade binding is restricted to the sealed plan")
		}
	}
	return nil
}
func historyStagingSharedLockAccount() (uint32, uint32, error) {
	account, err := user.Lookup("java-tron")
	if err != nil {
		return 0, 0, err
	}
	group, err := user.LookupGroup("java-tron")
	if err != nil {
		return 0, 0, err
	}
	uid, err := strconv.ParseUint(account.Uid, 10, 32)
	if err != nil || uid == 0 {
		return 0, 0, errors.New("invalid java-tron deployment UID")
	}
	gid, err := strconv.ParseUint(group.Gid, 10, 32)
	if err != nil || gid == 0 || account.Gid != group.Gid {
		return 0, 0, errors.New("invalid java-tron deployment GID")
	}
	return uint32(uid), uint32(gid), nil
}
func historyStagingLockOwnerAllowed(info os.FileInfo, shared bool) bool {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || info.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 || st.Nlink != 1 {
		return false
	}
	if st.Uid == 0 && st.Gid == 0 && info.Mode().Perm() == 0600 {
		return true
	}
	if !shared || info.Mode().Perm() != 0644 {
		return false
	}
	uid, gid, err := historyStagingSharedLockAccount()
	return err == nil && st.Uid == uid && st.Gid == gid
}
func historyStagingParentLockProof(paths historyStagingPaths) error {
	if runtime.GOOS != "linux" || os.Geteuid() != 0 {
		return errors.New("apply upgrade requires root Linux orchestrator")
	}
	pid := os.Getppid()
	if pid <= 1 {
		return errors.New("missing upgrade orchestrator")
	}
	status, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return err
	}
	root := false
	for _, line := range strings.Split(string(status), "\n") {
		if strings.HasPrefix(line, "Uid:") {
			fields := strings.Fields(line)
			root = len(fields) == 5 && fields[1] == "0" && fields[2] == "0"
		}
	}
	if !root {
		return errors.New("upgrade parent is not root")
	}
	before, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return err
	}
	locks, err := os.ReadFile("/proc/locks")
	if err != nil {
		return err
	}
	for _, path := range []string{"/data/gtron/start.lock", filepath.Join(paths.Source, ".history-staging-migration.lock"), filepath.Join(paths.Target, ".history-staging-migration.lock"), filepath.Join(paths.Cold, ".history-staging-migration.lock")} {
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok || !historyStagingLockOwnerAllowed(info, path == "/data/gtron/start.lock") {
			return errors.New("unsafe upgrade storage lock")
		}
		key := fmt.Sprintf("%x:%x:%d", unix.Major(uint64(st.Dev)), unix.Minor(uint64(st.Dev)), st.Ino)
		held := false
		for _, line := range strings.Split(string(locks), "\n") {
			f := strings.Fields(line)
			if len(f) >= 8 && f[1] == "FLOCK" && f[2] == "ADVISORY" && f[3] == "WRITE" && f[4] == strconv.Itoa(pid) {
				parts := strings.Split(f[5], ":")
				if len(parts) == 3 {
					ma, _ := strconv.ParseUint(parts[0], 16, 64)
					mi, _ := strconv.ParseUint(parts[1], 16, 64)
					ino, _ := strconv.ParseUint(parts[2], 10, 64)
					held = held || fmt.Sprintf("%x:%x:%d", ma, mi, ino) == key
				}
			}
		}
		if held {
			entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid))
			if err != nil {
				return err
			}
			fdMatch := false
			for _, entry := range entries {
				fdInfo, err := os.Stat(fmt.Sprintf("/proc/%d/fd/%s", pid, entry.Name()))
				if err == nil && os.SameFile(info, fdInfo) {
					fdMatch = true
					break
				}
			}
			named, err := os.Lstat(path)
			if err != nil || !os.SameFile(info, named) || !historyStagingLockOwnerAllowed(named, path == "/data/gtron/start.lock") {
				return errors.New("upgrade lock path changed")
			}
			held = fdMatch
		}
		if !held {
			return errors.New("upgrade parent lacks all four exact exclusive locks")
		}
	}
	after, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return err
	}
	// /proc stat CPU counters change; process start time must remain the same.
	start := func(v []byte) string {
		at := bytes.LastIndexByte(v, ')')
		if at < 0 {
			return ""
		}
		f := strings.Fields(string(v[at+1:]))
		if len(f) <= 19 {
			return ""
		}
		return f[19]
	}
	if start(before) == "" || start(before) != start(after) || os.Getppid() != pid {
		return errors.New("upgrade parent changed")
	}
	return nil
}
func historyStagingUpgradeAuthorization(ctx *cli.Context, c *historyStagingCLIContext) (string, error) {
	path := ctx.String("upgrade-binding")
	action := ctx.Command.Name
	if path == "" {
		if _, err := os.Lstat(historyStagingRepinIntent); err == nil {
			return "", errors.New("pending repin blocks ordinary CLI")
		} else if !os.IsNotExist(err) {
			return "", err
		}
		if action == "upgrade-check" {
			return "", errors.New("upgrade-check requires root binding")
		}
		return c.event.CandidateSHA256, nil
	}
	if path != historyStagingUpgradeBinding {
		return "", errors.New("upgrade binding must use the fixed trusted path")
	}
	if err := historyStagingTrustedBindingParents(path); err != nil {
		return "", err
	}
	data, err := historyStagingRootJSON(path, true)
	if err != nil {
		return "", err
	}
	var j historyStagingUpgradeJournal
	if err = json.Unmarshal(data, &j); err != nil {
		return "", err
	}
	latch, err := historyStagingRootJSON(historyStagingRootState+"MIGRATION_IN_PROGRESS.json", false)
	if err != nil {
		return "", err
	}
	prepared, err := historyStagingRootJSON(historyStagingRootState+"HISTORY_STAGING_PREPARED.json", false)
	if err != nil {
		return "", err
	}
	_, e := os.Lstat(historyStagingRepinIntent)
	pending := e == nil
	if e != nil && !os.IsNotExist(e) {
		return "", e
	}
	if err = validateHistoryStagingUpgradeJournal(j, c, ctx.String("plan-id"), action, latch, prepared, pending); err != nil {
		return "", err
	}
	if err = historyStagingParentLockProof(c.paths); err != nil {
		return "", err
	}
	actual, err := os.Executable()
	if err != nil {
		return "", err
	}
	if actual != j.New.Candidate {
		return "", errors.New("upgrade executable path differs from pinned executor")
	}
	return j.ProducerSHA, nil
}

func verifyHistoryStagingUpgradeBucket(state rawdb.HistoryStagingBucketState, row historyStagingPlanBucket, planID string) error {
	p := row.Proof
	if !state.HasRoute || state.Route.Epoch != p.Epoch || state.Route.TargetCleared {
		return rawdb.ErrHistoryStagingConflict
	}
	claimID, err := historyStagingClaimID(planID, p.Bucket)
	if err != nil {
		return err
	}
	proofDigest, err := rawdb.HistoryStagingProofDigest(p)
	if err != nil {
		return err
	}
	if state.HasClaim && (state.Claim.ClaimID != claimID || state.Claim.Epoch != p.Epoch || state.Claim.Cancelled || !reflect.DeepEqual(state.Claim.Proof, p) || state.Claim.ProofDigest != proofDigest) {
		return rawdb.ErrHistoryStagingConflict
	}
	if state.HasReceipt && (state.Receipt.ClaimID != claimID || state.Receipt.Epoch != p.Epoch || state.Receipt.ProofDigest != proofDigest || state.Receipt.DataDigest != row.Physical.Digest || state.Receipt.PayloadRows != row.Physical.ChangeRows || state.Receipt.TxRangeRows != row.Physical.TxRangeRows || state.Receipt.ChunkRows != row.Physical.ChunkRows || state.Receipt.PayloadBytes != row.Physical.Bytes) {
		return rawdb.ErrHistoryStagingConflict
	}
	if state.HasClaim {
		if state.Claim.TargetReady {
			if !state.HasReceipt || state.Claim.SourceDigest != row.Physical.Digest || state.Claim.SourceRows != row.Physical.Rows || state.Claim.SourceBytes != row.Physical.Bytes {
				return rawdb.ErrHistoryStagingConflict
			}
		} else if state.Claim.SourceDigest != ([32]byte{}) || state.Claim.SourceRows != 0 || state.Claim.SourceBytes != 0 {
			return rawdb.ErrHistoryStagingConflict
		}
	}
	switch state.Route.Owner {
	case rawdb.HistoryStagingOwnerSource:
		if state.HasClaim && (state.Route.WriteVersion == ^uint64(0) || state.Claim.WriteVersion != state.Route.WriteVersion+1) {
			return rawdb.ErrHistoryStagingConflict
		}
		if state.Route.SourceCleared || state.HasReceipt && !state.HasClaim {
			return rawdb.ErrHistoryStagingConflict
		}
	case rawdb.HistoryStagingOwnerTarget:
		if state.HasClaim && state.Claim.WriteVersion != state.Route.WriteVersion {
			return rawdb.ErrHistoryStagingConflict
		}
		if !state.HasReceipt || state.Route.SourceCleared && state.HasClaim || !state.Route.SourceCleared && !state.HasClaim {
			return rawdb.ErrHistoryStagingIncomplete
		}
		digest, err := rawdb.HistoryStagingReceiptDigest(state.Receipt)
		if err != nil || digest != state.Route.ReceiptDigest {
			return rawdb.ErrHistoryStagingConflict
		}
	case rawdb.HistoryStagingOwnerCold:
		if !historyStagingColdCoversWholeBucket(p) || state.HasClaim || state.HasReceipt {
			return rawdb.ErrHistoryStagingConflict
		}
	default:
		return rawdb.ErrHistoryStagingConflict
	}
	return nil
}
func dbHistoryStagingUpgradeCheck(ctx *cli.Context) error {
	c, err := newHistoryStagingCLIContext(ctx)
	if err != nil {
		return err
	}
	reader, err := openHistoryStagingPlan(ctx, c)
	if err != nil {
		return err
	}
	defer reader.Close()
	if !reader.CompleteEligibleCoverage() {
		return errors.New("upgrade requires complete original sealed plan")
	}
	source, err := c.openSource()
	if err != nil {
		return err
	}
	defer source.Close()
	if err = verifyHistoryStagingPlanInputs(ctx, c, source, reader.header); err != nil {
		return err
	}
	limits, _, err := historyStagingCLIWorkLimits(ctx, c.paths.Source, c.paths.Target)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(historyStagingFrozenLimits(limits), reader.header.Limits) {
		return errors.New("upgrade work limits differ from frozen plan")
	}
	manifest, err := statesnapshots.LoadProductionManifest(c.paths.Cold)
	if err != nil {
		return err
	}
	// Only metadata and frozen input proofs. No cold/payload semantic full scan.
	stage, err := rawdb.NewHistoryStagingPebbleDB(c.paths.Target, 64, 128, true)
	if err != nil {
		return err
	}
	defer stage.Close()
	manager, err := rawdb.NewHistoryStagingManager(source, stage, historyStagingIdentity(c.paths, reader.header.GenesisHash, reader.header.NetworkID))
	if err != nil {
		return err
	}
	if err = manager.VerifyIdentity(); err != nil {
		return err
	}
	epoch, err := manager.CurrentEpoch()
	if err != nil || epoch != 1 {
		return errors.New("upgrade route epoch differs")
	}
	reset, present, err := manager.ReadResetIntent()
	if err != nil || present && !reset.Complete {
		return errors.New("upgrade refuses pending reset")
	}

	if _, present, err := manager.ReadHistoryStagingRouteBarrier(); err != nil || present {
		return errors.New("upgrade refuses an activation barrier")
	}
	for {
		row, ok, err := reader.Next(c.ctx)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		state, err := manager.InspectBucket(row.Proof.Bucket)
		if err != nil {
			return err
		}
		if err = verifyHistoryStagingUpgradeBucket(state, row, c.event.PlanID); err != nil {
			return fmt.Errorf("upgrade bucket %d: %w", row.Proof.Bucket, err)
		}
		if (state.Route.Owner == rawdb.HistoryStagingOwnerCold || state.Route.Owner == rawdb.HistoryStagingOwnerTarget && len(row.Proof.ColdSpans) > 0) && state.Route.ColdBindingEpoch == 0 {
			return fmt.Errorf("upgrade bucket %d missing required cold binding", row.Proof.Bucket)
		}
		if state.Route.ColdBindingEpoch != 0 {
			binding, present, err := manager.ReadColdBindingAt(row.Proof.Epoch, row.Proof.Bucket)
			if err != nil || !present || binding.BindingEpoch != state.Route.ColdBindingEpoch || binding.ManifestEpoch != manifest.Generation || !reflect.DeepEqual(binding.Spans, row.Proof.ColdSpans) {
				return fmt.Errorf("upgrade bucket %d cold binding differs", row.Proof.Bucket)
			}
		}
	}
	// Classify retained genesis/tail routes and reject claims from another inventory.
	through := reader.header.Head.HeadBlock / rawdb.StateHistoryChunkBucketBlocks
	for bucket := uint64(0); bucket <= through; bucket++ {
		if bucket > 0 && bucket <= reader.header.LastBucket {
			continue
		}
		state, err := manager.InspectBucket(bucket)
		if err != nil {
			return err
		}
		if !state.HasRoute || state.Route.Epoch != 1 || state.Route.Owner != rawdb.HistoryStagingOwnerSource || state.Route.SourceCleared || state.HasClaim || state.HasReceipt {
			return fmt.Errorf("upgrade retained route %d conflicts", bucket)
		}
	}
	var cursor []byte
	for {
		rows, next, done, err := manager.ScanBuckets(c.ctx, cursor, 256)
		if err != nil {
			return err
		}
		for _, state := range rows {
			if state.Bucket > through {
				return errors.New("upgrade route or claim outside frozen inventory")
			}
		}
		if done {
			break
		}
		cursor = next
	}
	if err = historyStagingParentLockProof(c.paths); err != nil {
		return err
	}
	if _, err = historyStagingUpgradeAuthorization(ctx, c); err != nil {
		return err
	}
	return c.emit("upgrade-check")
}
