package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

type repairReport struct {
	Version                int                                 `json:"version"`
	ObservedUTC            string                              `json:"observed_utc"`
	DryRun                 bool                                `json:"dry_run"`
	Phase                  string                              `json:"phase"`
	HotPath                string                              `json:"hot_path"`
	StagePath              string                              `json:"stage_path"`
	ColdPath               string                              `json:"cold_path"`
	ManifestSHA256         string                              `json:"manifest_sha256"`
	CandidateSHA256        string                              `json:"candidate_sha256,omitempty"`
	JournalPhase           string                              `json:"journal_phase,omitempty"`
	FromBucket             uint64                              `json:"from_bucket"`
	ThroughBucket          uint64                              `json:"through_bucket"`
	HistoryWindow          uint64                              `json:"history_window"`
	Plan                   *snapshots.OfflineHistoryRepairPlan `json:"plan,omitempty"`
	TargetSlices           []repairTargetSlice                 `json:"target_slices,omitempty"`
	ProtectedStateVerified bool                                `json:"protected_state_verified"`
	Error                  string                              `json:"error,omitempty"`
	Note                   string                              `json:"note"`
}

func runRepairTargetCold(args []string) error { return runRepairTargetColdInternal(args, nil) }

// phaseHook is used only by same-process fault fixtures. The CLI does not
// expose a failpoint flag or environment variable.
func runRepairTargetColdInternal(args []string, phaseHook func(string) error) (retErr error) {
	fs := flag.NewFlagSet("repair-target-cold", flag.ContinueOnError)
	hotInput := fs.String("hot-dir", "", "existing hot chaindata")
	stageInput := fs.String("stage-dir", "", "existing history-staging store")
	coldInput := fs.String("cold-dir", "", "existing immutable cold directory")
	ancientInput := fs.String("ancient-dir", "", "existing read-only canonical freezer; required for --yes")
	manifestText := fs.String("manifest-sha256", "", "exact pinned current manifest SHA-256")
	chainText := fs.String("expected-chain-id", "", "configured chain ID if manifest has chain identity")
	forkHash := fs.String("expected-fork-config-hash", "", "configured fork identity if manifest has chain identity")
	fromBucket := fs.Uint64("from-bucket", 0, "first affected nonzero bucket")
	throughBucket := fs.Uint64("through-bucket", 0, "last affected bucket")
	fromTx := fs.Uint64("from-tx", 0, "first TARGET tx number at a complete block boundary")
	toTx := fs.Uint64("to-tx", 0, "last TARGET tx number at a complete block boundary")
	maxSourceTrios := fs.Uint64("max-source-trios", 1, "maximum old cold trios replaced in one durable publication (1..16)")
	window := fs.Uint64("history-window", 0, "configured retained block window")
	yes := fs.Bool("yes", false, "authenticate, build exact replacement, and publish; default reads metadata only")
	resume := fs.Bool("resume", false, "resume only this exact durable repair journal after an interrupted --yes")
	lockPath := fs.String("start-lock", "", "start.lock matching inherited locked fd9; required for --yes")
	holdPath := fs.String("hold-file", "", "existing offline hold; required for --yes")
	minGiB := fs.Uint64("min-free-gib", 128, "free-space floor for hot, stage, and cold")
	if err := fs.Parse(args); err != nil {
		return err
	}
	report := repairReport{Version: 1, ObservedUTC: time.Now().UTC().Format(time.RFC3339), DryRun: !*yes, Phase: "preflight", FromBucket: *fromBucket, ThroughBucket: *throughBucket, HistoryWindow: *window,
		Note: "Metadata-only by default. --yes proves the current TARGET interval against a new immutable cold trio, publishes a complete replacement manifest, and durably rebinds/certifies. It does not release or delete TARGET, hot, or old cold files."}
	defer func() {
		if retErr != nil {
			report.Error = retErr.Error()
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		retErr = errors.Join(retErr, enc.Encode(report))
	}()
	if fs.NArg() != 0 || len(*manifestText) != 64 || *fromBucket == 0 || *throughBucket < *fromBucket || *throughBucket-*fromBucket >= 16 ||
		*fromTx == 0 || *toTx < *fromTx || *window == 0 || *maxSourceTrios == 0 || *maxSourceTrios > 16 || *minGiB == 0 || *minGiB > 4096 {
		return errors.New("repair: exact manifest SHA, explicit contiguous buckets/TARGET tx interval, history window, and bounded budgets are required")
	}
	decoded, err := hex.DecodeString(*manifestText)
	if err != nil {
		return err
	}
	var expected [32]byte
	copy(expected[:], decoded)
	if expected == ([32]byte{}) {
		return errors.New("repair: zero manifest SHA is invalid")
	}
	if *resume && !*yes {
		return errors.New("repair: --resume requires --yes")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	hot, err := checkedDir(*hotInput)
	if err != nil {
		return err
	}
	stage, err := checkedDir(*stageInput)
	if err != nil {
		return err
	}
	cold, err := filepath.EvalSymlinks(*coldInput)
	if err != nil {
		return err
	}
	cold, err = filepath.Abs(cold)
	if err != nil {
		return err
	}
	if info, err := os.Stat(cold); err != nil || !info.IsDir() {
		return errors.Join(errors.New("repair: missing cold directory"), err)
	}
	if err := rawdb.ValidateHistoryStagingPaths(hot, stage, cold); err != nil {
		return err
	}
	report.HotPath, report.StagePath, report.ColdPath, report.ManifestSHA256 = hot, stage, cold, *manifestText
	var lock *os.File
	var hold cleanupHoldState
	if *yes {
		hold, err = readCleanupHold(*holdPath)
		if err != nil {
			return err
		}
		lock, err = acquireCleanupOperationLock(*lockPath)
		if err != nil {
			return err
		}
		defer lock.Close() // same OFD as parent fd9: never LOCK_UN
	}
	checkFence := func(checkCtx context.Context) error {
		if err := checkCtx.Err(); err != nil {
			return err
		}
		if !*yes {
			return nil
		}
		if err := errors.Join(hold.Recheck(), checkCleanupOperationLock(lock, *lockPath)); err != nil {
			return err
		}
		for _, path := range []string{hot, stage, cold} {
			free, err := cleanupFreeBytes(path)
			if err != nil || free < *minGiB<<30 {
				return fmt.Errorf("repair: free-space floor on %s: have %d: %w", path, free, errors.Join(err, rawdb.ErrHistoryStagingIncomplete))
			}
		}
		return nil
	}
	if err := checkFence(ctx); err != nil {
		return err
	}
	var hotDB, stageDB ethdb.KeyValueStore
	defer func() {
		if hotDB != nil {
			retErr = errors.Join(retErr, hotDB.Close())
		}
		if stageDB != nil {
			retErr = errors.Join(retErr, stageDB.Close())
		}
	}()
	hotDB, err = rawdb.NewPebbleDBReadOnly(hot, 64, 128)
	if err != nil {
		return err
	}
	stageDB, err = rawdb.NewHistoryStagingPebbleDB(stage, 64, 128, true)
	if err != nil {
		return err
	}
	manifest, err := loadCleanupManifest(cold, expected)
	if err != nil {
		return err
	}
	p, manager, err := inspectRetirePlan(ctx, hotDB, stageDB, *fromBucket, *throughBucket, *window)
	if err != nil {
		return err
	}
	if manifest.Chain != nil {
		chainID, err := strconv.ParseInt(*chainText, 10, 64)
		if err != nil {
			return fmt.Errorf("repair: expected chain ID required: %w", err)
		}
		id := p.Protected.Guard.StagingIdentity.Value
		if id.NetworkID > uint64(^uint32(0)>>1) {
			return errors.New("repair: network ID overflows chain identity")
		}
		if err := manifest.ValidateChainIdentity(snapshots.ChainIdentity{ChainID: chainID, NetworkID: int32(id.NetworkID), GenesisHash: strings.TrimPrefix(id.GenesisHash.Hex(), "0x"), ForkConfigHash: *forkHash}); err != nil {
			return err
		}
	}
	journal, journalPresent, err := readRepairJournal(cold)
	if err != nil {
		return err
	}
	var plan *snapshots.OfflineHistoryRepairPlan
	if journalPresent {
		report.JournalPhase = journal.Phase
		if expected != journal.OldSHA && expected != journal.CandidateSHA || journal.Plan.FromTxNum != *fromTx || journal.Plan.ToTxNum != *toTx || journal.Epoch != p.Epoch || len(journal.Bindings) != len(p.Buckets) ||
			!reflect.DeepEqual(journal.Protected, p.Protected) || journal.Barrier != p.Barrier {
			return errors.New("repair: operation journal identity, guard, or range differs")
		}
		if journal.Phase != journalPrepared && expected == journal.OldSHA {
			return errors.New("repair: published journal cannot return to original manifest")
		}
		for i, row := range p.Buckets {
			if journal.Bindings[i].Bucket != row.Bucket || journal.Bindings[i].Epoch != p.Epoch || journal.Bindings[i].Version != rawdb.HistoryStagingFormatVersion || len(journal.Bindings[i].Spans) == 0 {
				return fmt.Errorf("repair: operation journal binding %d differs from requested bucket", row.Bucket)
			}
		}
		if *yes && !*resume {
			return errors.New("repair: existing operation journal requires explicit --resume")
		}
		plan = &journal.Plan
		if expected == journal.OldSHA {
			actual, err := snapshots.PlanOfflineHistoryRepair(manifest, *fromTx, *toTx)
			if err != nil || !reflect.DeepEqual(actual, plan) {
				return errors.Join(errors.New("repair: old manifest no longer matches frozen repair plan"), err)
			}
		} else if err := verifyRepairCandidateManifest(manifest, journal); err != nil {
			return err
		}
	} else {
		if *resume {
			return errors.New("repair: no durable journal to resume")
		}
		plan, err = snapshots.PlanOfflineHistoryRepair(manifest, *fromTx, *toTx)
		if err != nil {
			return err
		}
	}
	if len(plan.SourceRefs)%3 != 0 || len(plan.SourceRefs)/3 > int(*maxSourceTrios) {
		return fmt.Errorf("repair: %d old trios exceed --max-source-trios=%d", len(plan.SourceRefs)/3, *maxSourceTrios)
	}
	report.Plan = plan
	if !*yes {
		report.Phase = "metadata_only"
		return nil
	}
	// The remaining write path is entered only after metadata and route checks.
	return executeRepairTargetCold(ctx, &report, repairExecution{hotPath: hot, stagePath: stage, coldPath: cold, ancientPath: *ancientInput, currentSHA: expected, fromTx: *fromTx, toTx: *toTx, fromBucket: *fromBucket, throughBucket: *throughBucket, window: *window, minFree: *minGiB << 30, lockPath: *lockPath, lock: lock, hold: hold, manifest: manifest, plan: plan, routes: p, manager: manager, hotDB: &hotDB, stageDB: &stageDB, journal: journal, journalPresent: journalPresent, phaseHook: phaseHook})
}

type repairExecution struct {
	hotPath, stagePath, coldPath, ancientPath                string
	currentSHA                                               [32]byte
	fromTx, toTx, fromBucket, throughBucket, window, minFree uint64
	lockPath                                                 string
	lock                                                     *os.File
	hold                                                     cleanupHoldState
	manifest                                                 *snapshots.Manifest
	plan                                                     *snapshots.OfflineHistoryRepairPlan
	routes                                                   retirePlan
	manager                                                  *rawdb.HistoryStagingManager
	hotDB, stageDB                                           *ethdb.KeyValueStore
	journal                                                  repairJournal
	journalPresent                                           bool
	phaseHook                                                func(string) error
}

func repairPlanSHA(plan *snapshots.OfflineHistoryRepairPlan, routes retirePlan) ([32]byte, error) {
	data, err := json.Marshal(struct {
		Plan    *snapshots.OfflineHistoryRepairPlan `json:"plan"`
		Epoch   uint64                              `json:"epoch"`
		Barrier rawdb.HistoryStagingRouteBarrier    `json:"barrier"`
		Routes  []retireBucket                      `json:"routes"`
	}{plan, routes.Epoch, routes.Barrier, routes.Buckets})
	return sha256.Sum256(data), err
}

func repairOtherRefsSHA(manifest *snapshots.Manifest, oldRefs []snapshots.SegmentRef, newRefs []snapshots.SegmentRef) ([32]byte, error) {
	removed := make(map[string]struct{}, len(oldRefs)+len(newRefs))
	for _, ref := range append(append([]snapshots.SegmentRef(nil), oldRefs...), newRefs...) {
		removed[ref.Path] = struct{}{}
	}
	var unrelated []snapshots.SegmentRef
	for _, ref := range manifest.Segments {
		if _, ok := removed[ref.Path]; !ok {
			unrelated = append(unrelated, ref)
		}
	}
	data, err := json.Marshal(unrelated)
	return sha256.Sum256(data), err
}

func repairUnchangedMetadata(old, now retirePlan) error {
	if old.Epoch != now.Epoch || old.Barrier != now.Barrier || !reflect.DeepEqual(old.Protected, now.Protected) || !retireMetadataEqual(old, now) {
		return errors.New("repair: protected chain or staging metadata changed")
	}
	return nil
}
