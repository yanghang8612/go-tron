package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/pebbledb"
)

type stageLiveDigest struct {
	Rows         uint64 `json:"rows"`
	LogicalBytes uint64 `json:"logical_bytes"`
	SHA256       string `json:"sha256"`
}

// Hash every live key and value, with lengths to prevent ambiguous framing.
// This includes staging metadata and retained SOURCE/TARGET payload alike.
func digestStageLive(ctx context.Context, db ethdb.KeyValueStore) (stageLiveDigest, error) {
	var out stageLiveDigest
	h := sha256.New()
	var size [8]byte
	it := db.NewIterator(nil, nil)
	defer it.Release()
	last := time.Now()
	for it.Next() {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		k, v := it.Key(), it.Value()
		n := uint64(len(k)) + uint64(len(v))
		if out.Rows == math.MaxUint64 || n > math.MaxUint64-out.LogicalBytes {
			return out, errors.New("stage live digest count overflow")
		}
		binary.BigEndian.PutUint64(size[:], uint64(len(k)))
		h.Write(size[:])
		h.Write(k)
		binary.BigEndian.PutUint64(size[:], uint64(len(v)))
		h.Write(size[:])
		h.Write(v)
		out.Rows++
		out.LogicalBytes += n
		if time.Since(last) >= 30*time.Second {
			fmt.Fprintf(os.Stderr, "stage_digest rows=%d logical_bytes=%d utc=%s\n", out.Rows, out.LogicalBytes, time.Now().UTC().Format(time.RFC3339))
			last = time.Now()
		}
	}
	if err := errors.Join(it.Error(), ctx.Err()); err != nil {
		return out, err
	}
	out.SHA256 = hex.EncodeToString(h.Sum(nil))
	return out, nil
}

type stageCompactGuard struct {
	Protected cleanupProtected             `json:"protected"`
	Epoch     uint64                       `json:"epoch"`
	Identity  rawdb.HistoryStagingIdentity `json:"identity"`
}

func captureStageCompactGuard(hot, stage ethdb.KeyValueStore) (stageCompactGuard, error) {
	var out stageCompactGuard
	var err error
	out.Protected, err = captureCleanupProtected(hot)
	if err != nil {
		return out, err
	}
	out.Identity = out.Protected.Guard.StagingIdentity.Value
	m, err := rawdb.NewHistoryStagingManager(hot, stage, out.Identity)
	if err != nil {
		return out, err
	}
	if err := m.VerifyIdentity(); err != nil {
		return out, err
	}
	out.Epoch, err = m.CurrentEpoch()
	if err != nil || out.Epoch == 0 {
		return out, errors.Join(errors.New("compact-stage requires initialized epoch"), err)
	}
	if _, present, err := m.ReadResetIntent(); err != nil || present {
		return out, errors.Join(errors.New("compact-stage refuses any reset intent"), err)
	}
	return out, nil
}

type stageCompactReport struct {
	Version                int                             `json:"version"`
	ObservedUTC            string                          `json:"observed_utc"`
	HotPath                string                          `json:"hot_path"`
	StagePath              string                          `json:"stage_path"`
	DryRun                 bool                            `json:"dry_run"`
	AllowWALReplay         bool                            `json:"allow_wal_replay"`
	MinFreeBytes           uint64                          `json:"min_free_bytes"`
	MaxSSTWriteBytes       uint64                          `json:"max_sst_write_bytes"`
	TargetSSTBytes         uint64                          `json:"target_sst_bytes"`
	Before                 *stageCompactGuard              `json:"before,omitempty"`
	After                  *stageCompactGuard              `json:"after,omitempty"`
	LiveBefore             *stageLiveDigest                `json:"live_before,omitempty"`
	LiveAfter              *stageLiveDigest                `json:"live_after,omitempty"`
	Inspection             *pebbledb.MaintenanceInspection `json:"inspection,omitempty"`
	Result                 *pebbledb.MaintenanceResult     `json:"result,omitempty"`
	ProtectedStateVerified bool                            `json:"protected_state_verified"`
	Phase                  string                          `json:"phase"`
	Error                  string                          `json:"error,omitempty"`
	Note                   string                          `json:"note"`
}

func runCompactStage(args []string) (retErr error) {
	fs := flag.NewFlagSet("compact-stage", flag.ContinueOnError)
	hotInput := fs.String("hot-dir", "", "existing hot chaindata, locked read-only throughout")
	stageInput := fs.String("stage-dir", "", "existing separate staging store")
	yes := fs.Bool("yes", false, "physically compact all stage SSTs; default hashes live KV read-only")
	lockPath := fs.String("start-lock", "", "start.lock matching inherited fd9; required with --yes")
	holdPath := fs.String("hold-file", "", "existing offline hold; required with --yes")
	minGiB := fs.Uint64("min-free-gib", 64, "minimum free space to retain")
	maxGiB := fs.Uint64("max-sst-write-gib", 256, "hard cap on SST allocation attempts")
	targetMiB := fs.Uint64("target-sst-mib", 32, "target SST file size, 1..128 MiB")
	wal := fs.Bool("allow-wal-replay", false, "explicitly permit bounded WAL recovery during write-open")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || *minGiB == 0 || *maxGiB == 0 || *targetMiB < 1 || *targetMiB > 128 || *minGiB > math.MaxUint64>>30 || *maxGiB > math.MaxUint64>>30 {
		return errors.New("compact-stage requires positive bounded space budgets and no positional arguments")
	}
	floor, budget := *minGiB<<30, *maxGiB<<30
	if floor > math.MaxUint64-pebbledb.MaintenanceControlReserveBytes || budget > math.MaxUint64-floor-pebbledb.MaintenanceControlReserveBytes {
		return errors.New("compact-stage space limits overflow")
	}
	hot, err := checkedDir(*hotInput)
	if err != nil {
		return err
	}
	stage, err := checkedDir(*stageInput)
	if err != nil {
		return err
	}
	if hot == stage || strings.HasPrefix(hot, stage+string(filepath.Separator)) || strings.HasPrefix(stage, hot+string(filepath.Separator)) {
		return errors.New("compact-stage hot/stage paths overlap")
	}
	report := stageCompactReport{Version: 1, ObservedUTC: time.Now().UTC().Format(time.RFC3339), HotPath: hot, StagePath: stage, DryRun: !*yes, AllowWALReplay: *wal, MinFreeBytes: floor, MaxSSTWriteBytes: budget, TargetSSTBytes: *targetMiB << 20, Phase: "read_only", Note: "All live staging KV must retain the same framed SHA-256 and row/byte counts. Hot state is locked read-only throughout. SST admission is a hard attempted-allocation budget, not an estimate of reclaimed space. Failure can leave earlier physical rewrites complete."}
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
		defer lock.Close()
	}
	checkFence := func(checkCtx context.Context) error {
		if err := checkCtx.Err(); err != nil {
			return err
		}
		if !*yes {
			return nil
		}
		return errors.Join(hold.Recheck(), checkCleanupOperationLock(lock, *lockPath))
	}
	h, err := rawdb.NewPebbleDBReadOnly(hot, 64, 128)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, h.Close()) }()
	var s ethdb.KeyValueStore
	s, err = rawdb.NewHistoryStagingPebbleDB(stage, 64, 128, true)
	if err != nil {
		return err
	}
	defer func() {
		if s != nil {
			retErr = errors.Join(retErr, s.Close())
		}
	}()
	before, err := captureStageCompactGuard(h, s)
	if err != nil {
		return err
	}
	report.Before = &before
	live, err := digestStageLive(ctx, s)
	if err != nil {
		return err
	}
	report.LiveBefore = &live
	if err := checkFence(ctx); err != nil {
		return err
	}
	if !*yes {
		report.Phase = "dry_run_complete"
		return nil
	}
	// Register the independent post-verification before any handoff/write attempt.
	// Hot's RO handle and inherited operation lock remain held until it completes.
	var m *pebbledb.MaintenanceDB
	defer func() {
		verifyCtx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
		defer cancel()
		if m != nil {
			reopenErr := m.ReopenReadOnly()
			closeErr := m.Close()
			m = nil
			retErr = errors.Join(retErr, reopenErr, closeErr)
		}
		if s != nil {
			retErr = errors.Join(retErr, s.Close())
			s = nil
		}
		s, err = rawdb.NewHistoryStagingPebbleDB(stage, 64, 128, true)
		if err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("compact-stage reopen verification: %w", err))
			return
		}
		after, captureErr := captureStageCompactGuard(h, s)
		report.After = &after
		afterLive, digestErr := digestStageLive(verifyCtx, s)
		report.LiveAfter = &afterLive
		fenceErr := checkFence(verifyCtx)
		verifyErr := errors.Join(captureErr, digestErr, fenceErr)
		if !reflect.DeepEqual(before, after) || live != afterLive {
			verifyErr = errors.Join(verifyErr, errors.New("compact-stage protected state or live staging KV changed"))
		}
		report.ProtectedStateVerified = verifyErr == nil
		retErr = errors.Join(retErr, verifyErr)
		if retErr == nil {
			report.Phase = "complete"
		}
	}()
	closeErr := s.Close()
	s = nil
	if closeErr != nil {
		return closeErr
	}
	if err := checkFence(ctx); err != nil {
		return err
	}
	m, err = pebbledb.OpenMaintenance(stage, pebbledb.MaintenanceOptions{MinFreeBytes: floor, MaxSSTWriteBytes: budget, TargetFileSizeBytes: int64(report.TargetSSTBytes), AllowWALReplay: *wal})
	if err != nil {
		return err
	}
	identity, present, err := rawdb.ReadHistoryStagingIdentity(m)
	if err != nil || !present || !reflect.DeepEqual(identity, before.Identity) {
		return errors.Join(errors.New("compact-stage identity changed during handoff"), err)
	}
	inspection, err := m.InspectAll()
	report.Inspection = &inspection
	if err != nil {
		return err
	}
	if err := checkFence(ctx); err != nil {
		return err
	}
	report.Phase = "write_admitted"
	fmt.Fprintf(os.Stderr, "stage_compact write_admitted utc=%s\n", time.Now().UTC().Format(time.RFC3339))
	result, compactErr := m.CompactPreservingAll()
	report.Result = &result
	return compactErr
}
