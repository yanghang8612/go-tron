package main

import (
	"context"
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
	chainfreezer "github.com/tronprotocol/go-tron/core/freezer"
	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
	rawfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

type retireReport struct {
	Version                int         `json:"version"`
	ObservedUTC            string      `json:"observed_utc"`
	HotPath                string      `json:"hot_path"`
	StagePath              string      `json:"stage_path"`
	ColdPath               string      `json:"cold_path"`
	AncientPath            string      `json:"ancient_path,omitempty"`
	ManifestSHA256         string      `json:"manifest_sha256"`
	DryRun                 bool        `json:"dry_run"`
	CertifyMissing         bool        `json:"certify_missing"`
	HistoryWindow          uint64      `json:"history_window"`
	FromBucket             uint64      `json:"from_bucket"`
	ThroughBucket          uint64      `json:"through_bucket"`
	Before                 *retirePlan `json:"before,omitempty"`
	After                  *retirePlan `json:"after,omitempty"`
	PlanSHA256             string      `json:"plan_sha256,omitempty"`
	NeedsCertification     uint64      `json:"needs_certification"`
	MissingBinding         uint64      `json:"missing_binding"`
	PartialBinding         uint64      `json:"partial_binding"`
	FullBinding            uint64      `json:"full_binding"`
	Certified              uint64      `json:"certified"`
	Completed              uint64      `json:"completed"`
	AdmittedLogicalBytes   uint64      `json:"admitted_logical_bytes"`
	ProtectedStateVerified bool        `json:"protected_state_verified"`
	Phase                  string      `json:"phase"`
	Error                  string      `json:"error,omitempty"`
	Note                   string      `json:"note"`
}

func retireMetadataEqual(proved, current retirePlan) bool {
	copyPlan := proved
	copyPlan.Buckets = append([]retireBucket(nil), proved.Buckets...)
	for i := range copyPlan.Buckets {
		copyPlan.Buckets[i].Prepared = nil
		copyPlan.Buckets[i].CanonicalDigest = [32]byte{}
	}
	return reflect.DeepEqual(copyPlan, current)
}

func openRetireAncient(path string) (rawdb.AncientReader, func() error, string, error) {
	if path == "" {
		return rawdb.NoopAncient{}, func() error { return nil }, "", nil
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, nil, "", err
	}
	resolved, err = filepath.Abs(resolved)
	if err != nil {
		return nil, nil, "", err
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.IsDir() {
		return nil, nil, "", errors.Join(errors.New("retire ancient directory does not exist"), err)
	}
	f, err := rawfreezer.NewFreezer(resolved, "", true, 2*1024*1024*1024, chainfreezer.FreezerTableSet())
	if err != nil {
		return nil, nil, "", err
	}
	return rawdb.NewFreezerReader(f), f.Close, resolved, nil
}

func runRetireTarget(args []string) (retErr error) {
	fs := flag.NewFlagSet("retire-target", flag.ContinueOnError)
	hotInput := fs.String("hot-dir", "", "existing hot chaindata")
	stageInput := fs.String("stage-dir", "", "existing staging store")
	coldInput := fs.String("cold-dir", "", "existing immutable snapshots directory")
	ancientInput := fs.String("ancient-dir", "", "existing read-only chain freezer for canonical bucket proofs")
	manifestText := fs.String("manifest-sha256", "", "exact pinned manifest SHA-256")
	chainText := fs.String("expected-chain-id", "", "configured chain ID for identity-bearing manifest")
	forkHash := fs.String("expected-fork-config-hash", "", "configured fork identity for identity-bearing manifest")
	from := fs.Uint64("from-bucket", 0, "explicit first nonzero full bucket")
	through := fs.Uint64("through-bucket", 0, "explicit last full bucket, at most immutable barrier eligibility")
	window := fs.Uint64("history-window", 0, "actual configured retained history blocks; required")
	certify := fs.Bool("certify-missing", false, "allow one complete pinned TARGET/cold semantic comparison before publishing missing or partial binding")
	yes := fs.Bool("yes", false, "authenticate then perform protocol retirement; default metadata-only dry-run")
	lockPath := fs.String("start-lock", "", "production start.lock matching inherited fd9; required with --yes")
	holdPath := fs.String("hold-file", "", "existing offline hold; required with --yes")
	maxGiB := fs.Uint64("max-delete-gib", 256, "total admitted logical target-delete bytes, including attempted tail")
	minGiB := fs.Uint64("min-free-gib", 128, "free-space floor for both stores before every delete batch")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || len(*manifestText) != 64 || *from == 0 || *through < *from || *through-*from >= 4096 || *window == 0 || *maxGiB == 0 || *maxGiB > 4096 || *minGiB == 0 || *minGiB > 4096 {
		return errors.New("retire-target requires exact manifest SHA, explicit 1..4096 buckets, positive history window and bounded space budgets")
	}
	decoded, err := hex.DecodeString(*manifestText)
	if err != nil {
		return err
	}
	var expected [32]byte
	copy(expected[:], decoded)
	if expected == ([32]byte{}) {
		return errors.New("retire-target rejects zero manifest SHA")
	}
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
	if err := rawdb.ValidateHistoryStagingPaths(hot, stage, cold); err != nil {
		return err
	}
	report := retireReport{Version: 1, ObservedUTC: time.Now().UTC().Format(time.RFC3339), HotPath: hot, StagePath: stage, ColdPath: cold, ManifestSHA256: hex.EncodeToString(expected[:]), DryRun: !*yes, CertifyMissing: *certify, HistoryWindow: *window, FromBucket: *from, ThroughBucket: *through, Phase: "metadata", Note: "Dry-run reads metadata only and grants no delete authority. --yes proves every candidate before writer admission. Only protocol-certified TARGET payload is reclaimed; no SOURCE, posting, shared hot chunks or cold files are deleted. Logical admitted bytes are not physical reclaimed bytes."}
	defer func() {
		if retErr != nil {
			report.Error = retErr.Error()
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		retErr = errors.Join(retErr, enc.Encode(report))
	}()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var operationLock *os.File
	var hold cleanupHoldState
	if *yes {
		hold, err = readCleanupHold(*holdPath)
		if err != nil {
			return err
		}
		operationLock, err = acquireCleanupOperationLock(*lockPath)
		if err != nil {
			return err
		}
		defer operationLock.Close()
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
	before, manager, err := inspectRetirePlan(ctx, hotDB, stageDB, *from, *through, *window)
	if err != nil {
		return err
	}
	if manifest.Chain != nil {
		chainID, err := strconv.ParseInt(*chainText, 10, 64)
		if err != nil {
			return fmt.Errorf("retire identity-bearing manifest requires configured --expected-chain-id: %w", err)
		}
		id := before.Protected.Guard.StagingIdentity.Value
		if id.NetworkID > uint64(^uint32(0)>>1) {
			return errors.New("retire network ID overflows chain identity")
		}
		if err := manifest.ValidateChainIdentity(snapshots.ChainIdentity{ChainID: chainID, NetworkID: int32(id.NetworkID), GenesisHash: strings.TrimPrefix(id.GenesisHash.Hex(), "0x"), ForkConfigHash: *forkHash}); err != nil {
			return err
		}
	}
	report.Before = &before
	for _, row := range before.Buckets {
		if row.NeedsCertification {
			report.NeedsCertification++
			if row.OldBinding == nil {
				report.MissingBinding++
			} else {
				report.PartialBinding++
			}
		} else {
			report.FullBinding++
		}
	}
	if !*yes {
		return nil
	}
	ancient, closeAncient, ancientPath, err := openRetireAncient(*ancientInput)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, closeAncient()) }()
	report.AncientPath = ancientPath
	if ancientPath != "" {
		if err := rawdb.ValidateHistoryStagingPaths(stage, cold, ancientPath); err != nil {
			return err
		}
		if err := rawdb.ValidateHistoryStagingPaths(hot, stage, ancientPath); err != nil {
			return err
		}
	}
	pinned, err := snapshots.OpenPinnedManager(cold, manifest)
	if err != nil {
		return err
	}
	audit, err := snapshots.NewHistoryStagingReceiptAudit(pinned)
	if err != nil {
		return err
	}
	proofCtx, physicalFacts, err := snapshots.WithHistoryStagingPhysicalFacts(ctx, cold)
	if err != nil {
		return err
	}
	lastProgress := time.Now()
	currentBucket := *from
	proofCtx = maintenance.WithWorkCheckpoint(proofCtx, func(uint64) error {
		cleanupProgress("retire-authenticate", currentBucket, *through, &lastProgress)
		return ctx.Err()
	})
	report.Phase = "authenticate"
	if err := authenticateRetirePlan(proofCtx, &before, manager, rawdb.NewChainDB(hotDB, ancient), cold, manifest, audit, *certify, func(bucket uint64) {
		currentBucket = bucket + 1
		cleanupProgress("retire-authenticate", bucket, *through, &lastProgress)
	}); err != nil {
		return err
	}
	digest, err := retirePlanDigest(before)
	if err != nil {
		return err
	}
	report.PlanSHA256 = hex.EncodeToString(digest[:])
	checkFence := func(checkCtx context.Context) error {
		if err := checkCtx.Err(); err != nil {
			return err
		}
		if err := hold.Recheck(); err != nil {
			return err
		}
		if err := checkCleanupOperationLock(operationLock, *lockPath); err != nil {
			return err
		}
		for _, path := range []string{hot, stage} {
			free, err := cleanupFreeBytes(path)
			if err != nil || free < *minGiB<<30 {
				return fmt.Errorf("retire free-space floor on %s: have %d: %w", path, free, errors.Join(err, rawdb.ErrHistoryStagingIncomplete))
			}
		}
		return nil
	}
	checkCold := func(checkCtx context.Context) error {
		if err := checkFence(checkCtx); err != nil {
			return err
		}
		actual, err := cleanupFileSHA256(filepath.Join(cold, snapshots.ManifestFile))
		if err != nil || actual != expected {
			return errors.Join(errors.New("retire pinned manifest changed"), err)
		}
		return errors.Join(audit.RecheckAll(checkCtx), physicalFacts.RecheckAll(checkCtx))
	}
	if err := checkCold(ctx); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "history_retire event=auth_complete epoch=%d buckets=%d needs_certification=%d plan_sha256=%x at=%s\n", before.Epoch, len(before.Buckets), report.NeedsCertification, digest, time.Now().UTC().Format(time.RFC3339))
	// Parent FD9 fences every writer over both DB lock handoffs. The frozen
	// target receipt/route/canonical state is checked again before first write.
	if err := hotDB.Close(); err != nil {
		return err
	}
	hotDB = nil
	if err := stageDB.Close(); err != nil {
		return err
	}
	stageDB = nil
	writerAttempted := false
	defer func() {
		if !writerAttempted {
			return
		}
		if hotDB != nil {
			retErr = errors.Join(retErr, hotDB.Close())
			hotDB = nil
		}
		if stageDB != nil {
			retErr = errors.Join(retErr, stageDB.Close())
			stageDB = nil
		}
		verifyCtx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
		defer cancel()
		h, err := rawdb.NewPebbleDBReadOnly(hot, 64, 128)
		if err != nil {
			retErr = errors.Join(retErr, err)
			return
		}
		defer h.Close()
		s, err := rawdb.NewHistoryStagingPebbleDB(stage, 64, 128, true)
		if err != nil {
			retErr = errors.Join(retErr, err)
			return
		}
		defer s.Close()
		after, _, err := inspectRetirePlan(verifyCtx, h, s, *from, *through, *window)
		if err == nil {
			err = verifyRetireTransitions(before, after, retErr == nil)
		}
		if err == nil {
			err = checkCold(verifyCtx)
		}
		if err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("retire post-writer protected verification: %w", err))
			return
		}
		report.After = &after
		report.ProtectedStateVerified = true
	}()
	writerAttempted = true
	tune := rawdb.DefaultPebbleOptions()
	tune.MemTableSizeBytes = 64 << 20
	tune.MaxConcurrentCompactions = 1
	tune.DisableAutomaticCompactions = false
	hotDB, err = rawdb.NewPebbleDBWithOptions(hot, 128, 128, tune)
	if err != nil {
		return err
	}
	stageDB, err = rawdb.NewHistoryStagingPebbleDB(stage, 128, 128, false)
	if err != nil {
		return err
	}
	current, manager, err := inspectRetirePlan(ctx, hotDB, stageDB, *from, *through, *window)
	if err != nil {
		return err
	}
	if !retireMetadataEqual(before, current) {
		return errors.New("retire chain/routes/receipts changed across writer handoff")
	}
	if err := recheckRetireCanonical(ctx, before, manager, rawdb.NewChainDB(hotDB, ancient)); err != nil {
		return err
	}
	if err := checkCold(ctx); err != nil {
		return err
	}
	report.Phase = "write-admitted"
	fmt.Fprintf(os.Stderr, "history_retire event=write_admitted epoch=%d buckets=%d at=%s\n", before.Epoch, len(before.Buckets), time.Now().UTC().Format(time.RFC3339))
	limits := rawdb.HistoryStagingLimits{MaxRowBytes: 64 << 20, MaxBatchBytes: 64 << 20, MaxBucketBytes: 8 << 30, MaxWorkBytes: 8 << 30, MaxDecodedBytes: rawdb.HistoryStagingMaxDecodedBytes, MinFreeBytes: *minGiB << 30, FreeBytes: func() (uint64, error) { return cleanupFreeBytes(stage) }}
	limits.Checkpoint = func(cost uint64) error {
		if cost > 0 {
			if cost > (*maxGiB<<30)-report.AdmittedLogicalBytes {
				return errors.New("retire total target delete budget exhausted; rerun scans remaining keys")
			}
			report.AdmittedLogicalBytes += cost
			return ctx.Err()
		}
		return checkFence(ctx)
	}
	for _, row := range before.Buckets {
		if err := checkFence(ctx); err != nil {
			return err
		}
		if row.NeedsCertification {
			report.Phase = "certify"
			if err := manager.CertifyColdRange(ctx, *row.Prepared, func() error { return checkFence(ctx) }); err != nil {
				return err
			}
			report.Certified++
		}
		report.Phase = "release"
		if err := manager.ReleaseTargetToCold(ctx, row.Bucket, func(actual rawdb.HistoryStagingColdBinding) error {
			if !reflect.DeepEqual(actual, *row.Prepared) {
				return rawdb.ErrHistoryStagingConflict
			}
			return checkFence(ctx)
		}); err != nil {
			return err
		}
		report.Phase = "clear-target"
		if err := manager.ClearColdTarget(ctx, row.Bucket, limits); err != nil {
			return err
		}
		report.Completed++
		cleanupProgress("retire-clear-target", row.Bucket, *through, &lastProgress)
	}
	if err := checkCold(ctx); err != nil {
		return err
	}
	// Both APIs persist physical SHA facts only, after durable semantic
	// binding/route publication and target-cleared synchronization.
	for _, commit := range []func(context.Context) error{physicalFacts.CommitPhysicalCertificates, audit.CommitPhysicalCertificates} {
		if err := commit(ctx); err != nil {
			if !errors.Is(err, snapshots.ErrHistoryStagingReceiptSidecarWrite) {
				return err
			}
			fmt.Fprintf(os.Stderr, "history_retire event=receipt_sidecar_warning err=%v\n", err)
		}
	}
	report.Phase = "complete"
	return nil
}
