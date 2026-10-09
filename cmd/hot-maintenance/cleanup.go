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
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/pebbledb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

type cleanupReport struct {
	Version                int                 `json:"version"`
	ObservedUTC            string              `json:"observed_utc"`
	HotPath                string              `json:"hot_path"`
	StagePath              string              `json:"stage_path"`
	ColdPath               string              `json:"cold_path"`
	DryRun                 bool                `json:"dry_run"`
	FromBlock              uint64              `json:"from_block"`
	ThroughBlock           uint64              `json:"through_block"`
	ManifestSHA256         string              `json:"manifest_sha256"`
	HoldFile               string              `json:"hold_file,omitempty"`
	HoldSHA256             string              `json:"hold_sha256,omitempty"`
	Epoch                  uint64              `json:"epoch,omitempty"`
	RouteBindingSHA256     string              `json:"route_binding_sha256,omitempty"`
	ColdBindings           uint64              `json:"cold_bindings"`
	AuthenticatedTrios     int                 `json:"authenticated_trios"`
	Before                 *cleanupProtected   `json:"before,omitempty"`
	After                  *cleanupProtected   `json:"after,omitempty"`
	Stats                  *cleanupDeleteStats `json:"delete_stats,omitempty"`
	SSTOverlapEstimates    map[string]uint64   `json:"sst_overlap_estimates,omitempty"`
	ProtectedStateVerified bool                `json:"protected_state_verified"`
	Error                  string              `json:"error,omitempty"`
	Note                   string              `json:"note"`
}

// acquireCleanupOperationLock duplicates inherited fd 9, checks that it is
// the same regular file as the declared start.lock path, and holds a Linux
// flock over the entire RO→RW→RO transition. Never LOCK_UN this duplicate:
// the parent shell may share its open-file description and still hold fd 9.
func acquireCleanupOperationLock(path string) (*os.File, error) {
	if path == "" || !filepath.IsAbs(path) {
		return nil, errors.New("cleanup requires absolute start-lock path")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, err
	}
	pathInfo, err := os.Stat(resolved)
	if err != nil || !pathInfo.Mode().IsRegular() {
		return nil, errors.New("cleanup start-lock path is not a regular file")
	}
	dup, err := syscall.Dup(9)
	if err != nil {
		return nil, fmt.Errorf("cleanup must inherit locked fd 9: %w", err)
	}
	f := os.NewFile(uintptr(dup), "cleanup-start-lock-duplicate")
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || !os.SameFile(info, pathInfo) {
		_ = f.Close()
		return nil, errors.New("cleanup fd 9 is not the declared regular start-lock file")
	}
	if err := syscall.Flock(dup, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("cleanup cannot hold exclusive start-lock: %w", err)
	}
	return f, nil
}

func checkCleanupOperationLock(f *os.File, path string) error {
	if f == nil {
		return errors.New("cleanup start-lock descriptor missing")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return err
	}
	pathInfo, err := os.Stat(resolved)
	if err != nil {
		return err
	}
	fdInfo, err := f.Stat()
	if err != nil || !pathInfo.Mode().IsRegular() || !fdInfo.Mode().IsRegular() || !os.SameFile(pathInfo, fdInfo) {
		return errors.New("cleanup start-lock path no longer matches held fd 9")
	}
	return nil
}

func cleanupValidateState(ctx context.Context, hot, stage ethdb.KeyValueStore, audit *snapshots.HistoryStagingReceiptAudit, cold string, expected [32]byte, through uint64, progress bool) (cleanupAuthorization, error) {
	var out cleanupAuthorization
	if err := ctx.Err(); err != nil {
		return out, err
	}
	manifestSHA, err := cleanupFileSHA256(filepath.Join(cold, snapshots.ManifestFile))
	if err != nil || manifestSHA != expected {
		return out, errors.New("cleanup pinned manifest changed")
	}
	protected, err := captureCleanupProtected(hot)
	if err != nil {
		return out, err
	}
	if err := validateCleanupBounds(protected, through); err != nil {
		return out, err
	}
	manager, err := rawdb.NewHistoryStagingManager(hot, stage, protected.Guard.StagingIdentity.Value)
	if err != nil {
		return out, err
	}
	if err := manager.VerifyIdentity(); err != nil {
		return out, err
	}
	lastProgress := time.Now()
	var callback func(uint64)
	if progress {
		callback = func(bucket uint64) {
			cleanupProgress("authenticate-cold-buckets", bucket, through/rawdb.StateHistoryChunkBucketBlocks, &lastProgress)
		}
	}
	out, err = proveCleanupPrefix(ctx, manager, audit, through, callback)
	if err != nil {
		return out, err
	}
	if after, err := cleanupFileSHA256(filepath.Join(cold, snapshots.ManifestFile)); err != nil || after != expected {
		return out, errors.New("cleanup pinned manifest changed during route authentication")
	}
	out.ManifestSHA256 = expected
	out.Protected = protected
	return out, nil
}

func runCleanupIndex(args []string) (retErr error) {
	fs := flag.NewFlagSet("cleanup-index", flag.ContinueOnError)
	hotInput := fs.String("hot-dir", "", "existing hot chaindata")
	stageInput := fs.String("stage-dir", "", "existing staging store")
	coldInput := fs.String("cold-dir", "", "existing immutable cold snapshot directory")
	manifestText := fs.String("manifest-sha256", "", "exact SHA-256 of pinned manifest.json")
	chainIDText := fs.String("expected-chain-id", "", "required for an identity-bearing manifest; sourced from configured chain")
	forkHash := fs.String("expected-fork-config-hash", "", "configured sha256:<hex> when the manifest has a fork identity")
	from := fs.Uint64("from-block", 0, "must equal complete bucket one start, 1024")
	through := fs.Uint64("through-block", 0, "inclusive end of an explicitly authenticated COLD bucket prefix")
	yes := fs.Bool("yes", false, "perform bounded logical deletion; default authenticates only")
	lockPath := fs.String("start-lock", "", "production start.lock matching inherited fd 9; required with --yes")
	holdPath := fs.String("hold-file", "", "existing maintenance hold that fences automatic node startup; required with --yes")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || *from != cleanupFirstBlock || *through < *from || len(*manifestText) != 64 {
		return errors.New("cleanup-index requires exact manifest SHA, --from-block=1024 and complete --through-block")
	}
	var expected [32]byte
	decoded, err := hex.DecodeString(*manifestText)
	if err != nil {
		return err
	}
	copy(expected[:], decoded)
	if expected == ([32]byte{}) {
		return errors.New("cleanup-index rejects zero manifest SHA")
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
	if err := rawdb.ValidateHistoryStagingPaths(hot, stage, cold); err != nil {
		return err
	}
	report := cleanupReport{Version: 1, ObservedUTC: time.Now().UTC().Format(time.RFC3339), HotPath: hot, StagePath: stage, ColdPath: cold,
		DryRun: !*yes, FromBlock: *from, ThroughBlock: *through, ManifestSHA256: hex.EncodeToString(expected[:]),
		Note: "Cold proof authorizes only whole posting frames inside the explicit prefix. Dry-run does not scan posting rows. Logical delete bytes are not physical reclaimed bytes."}
	defer func() {
		if retErr != nil {
			report.Error = retErr.Error()
		}
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		retErr = errors.Join(retErr, encoder.Encode(report))
	}()
	var operationLock *os.File
	var hold cleanupHoldState
	if *yes {
		hold, err = readCleanupHold(*holdPath)
		if err != nil {
			return err
		}
		report.HoldFile, report.HoldSHA256 = hold.path, hex.EncodeToString(hold.hash[:])
		operationLock, err = acquireCleanupOperationLock(*lockPath)
		if err != nil {
			return err
		}
		defer operationLock.Close() // Deliberately never flock LOCK_UN.
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	stageDB, err := rawdb.NewHistoryStagingPebbleDB(stage, 64, 128, true)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, stageDB.Close()) }()
	hotRO, err := rawdb.NewPebbleDBReadOnly(hot, 64, 128)
	if err != nil {
		return err
	}
	defer func() {
		if hotRO != nil {
			retErr = errors.Join(retErr, hotRO.Close())
		}
	}()
	manifest, err := loadCleanupManifest(cold, expected)
	if err != nil {
		return err
	}
	if manifest.Chain != nil {
		if *chainIDText == "" {
			return errors.New("cleanup identity-bearing manifest requires --expected-chain-id")
		}
		chainID, err := strconv.ParseInt(*chainIDText, 10, 64)
		if err != nil {
			return err
		}
		identity, present, err := rawdb.ReadHistoryStagingIdentity(hotRO)
		if err != nil || !present || identity.NetworkID > uint64(^uint32(0)>>1) {
			return errors.New("cleanup cannot bind manifest to hot chain identity")
		}
		expectedChain := snapshots.ChainIdentity{ChainID: chainID, NetworkID: int32(identity.NetworkID),
			GenesisHash: strings.TrimPrefix(identity.GenesisHash.Hex(), "0x"), ForkConfigHash: *forkHash}
		if err := manifest.ValidateChainIdentity(expectedChain); err != nil {
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
	before, err := cleanupValidateState(ctx, hotRO, stageDB, audit, cold, expected, *through, true)
	if err != nil {
		return err
	}
	report.Epoch, report.ColdBindings, report.AuthenticatedTrios = before.Epoch, before.ColdBindings, before.AuthenticatedTrio
	report.RouteBindingSHA256 = hex.EncodeToString(before.RouteBindingHash[:])
	report.Before = &before.Protected
	fmt.Fprintf(os.Stderr, "history_cleanup event=auth_complete epoch=%d from=%d through=%d bindings=%d trios=%d manifest_sha256=%x route_binding_sha256=%x\n",
		before.Epoch, *from, *through, before.ColdBindings, before.AuthenticatedTrio, expected, before.RouteBindingHash)
	if !*yes {
		// SST overlap is an estimate, never a reclaim estimate. Open the
		// separate read-only maintenance inspector after releasing hot RO;
		// the staging RO lock remains held throughout.
		if err := hotRO.Close(); err != nil {
			return err
		}
		hotRO = nil
		m, err := pebbledb.OpenMaintenance(hot, pebbledb.MaintenanceOptions{MinFreeBytes: 1 << 30, MaxSSTWriteBytes: 1 << 30})
		if err != nil {
			return err
		}
		report.SSTOverlapEstimates = make(map[string]uint64)
		for _, r := range rawdb.HotMaintenanceRanges() {
			if r.Name != "state_change_posting" && r.Name != "state_change_directory" {
				continue
			}
			inspection, inspectErr := m.Inspect(r.Start, r.End)
			if inspectErr != nil {
				return errors.Join(inspectErr, m.Close())
			}
			report.SSTOverlapEstimates[r.Name] = inspection.EstimatedBytes
		}
		if err := m.Close(); err != nil {
			return err
		}
		if err := audit.RecheckAll(ctx); err != nil {
			return err
		}
		if after, err := cleanupFileSHA256(filepath.Join(cold, snapshots.ManifestFile)); err != nil || after != expected {
			return errors.New("cleanup pinned manifest changed during dry-run estimate")
		}
		return nil
	}
	if err := hold.Recheck(); err != nil {
		return err
	}
	if err := checkCleanupOperationLock(operationLock, *lockPath); err != nil {
		return err
	}
	if free, err := cleanupFreeBytes(hot); err != nil || free < cleanupMinFreeBytes {
		return fmt.Errorf("cleanup requires %d free bytes before opening writable Pebble (have %d): %w", cleanupMinFreeBytes, free, err)
	}
	// The inherited flock and stage RO lock remain held over this hot Pebble
	// LOCK handoff. Revalidate everything before the first Delete.
	if err := hotRO.Close(); err != nil {
		return err
	}
	hotRO = nil
	var writer ethdb.KeyValueStore
	defer func() {
		if writer != nil {
			retErr = errors.Join(retErr, writer.Close())
		}
		verifyCtx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
		defer cancel()
		reopened, reopenErr := rawdb.NewPebbleDBReadOnly(hot, 64, 128)
		if reopenErr != nil {
			retErr = errors.Join(retErr, fmt.Errorf("cleanup reopen after writer: %w", reopenErr))
			return
		}
		defer func() { retErr = errors.Join(retErr, reopened.Close()) }()
		after, verifyErr := cleanupValidateState(verifyCtx, reopened, stageDB, audit, cold, expected, *through, false)
		if verifyErr == nil {
			verifyErr = compareCleanupAuthorization(before, after)
		}
		if holdErr := hold.Recheck(); holdErr != nil {
			verifyErr = errors.Join(verifyErr, holdErr)
		}
		if lockErr := checkCleanupOperationLock(operationLock, *lockPath); lockErr != nil {
			verifyErr = errors.Join(verifyErr, lockErr)
		}
		if verifyErr != nil {
			retErr = errors.Join(retErr, fmt.Errorf("cleanup post-write protected verification: %w", verifyErr))
		} else {
			report.After = &after.Protected
			report.ProtectedStateVerified = true
		}
	}()
	tune := rawdb.DefaultPebbleOptions()
	tune.MemTableSizeBytes = 64 << 20
	tune.MaxConcurrentCompactions = 1
	tune.DisableAutomaticCompactions = false
	writer, err = rawdb.NewPebbleDBWithOptions(hot, 256, 128, tune)
	if err != nil {
		return err
	}
	afterOpen, err := cleanupValidateState(ctx, writer, stageDB, audit, cold, expected, *through, false)
	if err != nil {
		return err
	}
	if err := compareCleanupAuthorization(before, afterOpen); err != nil {
		return err
	}
	if err := hold.Recheck(); err != nil {
		return err
	}
	if err := checkCleanupOperationLock(operationLock, *lockPath); err != nil {
		return err
	}
	if free, err := cleanupFreeBytes(hot); err != nil || free < cleanupMinFreeBytes {
		return fmt.Errorf("cleanup requires %d free bytes before first Delete (have %d): %w", cleanupMinFreeBytes, free, err)
	}
	fmt.Fprintf(os.Stderr, "history_cleanup event=write_admitted epoch=%d from=%d through=%d bindings=%d trios=%d manifest_sha256=%x route_binding_sha256=%x\n",
		afterOpen.Epoch, *from, *through, afterOpen.ColdBindings, afterOpen.AuthenticatedTrio, expected, afterOpen.RouteBindingHash)
	stats := &cleanupDeleteStats{}
	report.Stats = stats
	if err := runCleanupDeletes(ctx, writer, hot, *through, &hold, operationLock, *lockPath, stats); err != nil {
		return err
	}
	return nil
}
