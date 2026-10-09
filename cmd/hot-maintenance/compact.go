package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"reflect"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/pebbledb"
)

type chainBoundary struct {
	HeadNumber  uint64 `json:"head_number"`
	HeadHash    string `json:"head_hash"`
	SolidNumber uint64 `json:"solid_number"`
	SolidHash   string `json:"solid_hash"`
}

type protectedState struct {
	Boundary chainBoundary                           `json:"boundary"`
	Guard    guard                                   `json:"guard"`
	Canaries map[string][]pebbledb.MaintenanceSample `json:"canaries"`
}

type compactReport struct {
	Version                int                            `json:"version"`
	ObservedUTC            string                         `json:"observed_utc"`
	HotPath                string                         `json:"hot_path"`
	StagePath              string                         `json:"stage_path,omitempty"`
	Range                  string                         `json:"range"`
	DryRun                 bool                           `json:"dry_run"`
	AllowWALReplay         bool                           `json:"allow_wal_replay"`
	MinFreeBytes           uint64                         `json:"min_free_bytes"`
	MaxSSTWriteBytes       uint64                         `json:"max_sst_write_bytes"`
	TargetSSTBytes         uint64                         `json:"target_sst_bytes"`
	Before                 protectedState                 `json:"before"`
	After                  *protectedState                `json:"after,omitempty"`
	Inspection             pebbledb.MaintenanceInspection `json:"inspection"`
	Result                 *pebbledb.MaintenanceResult    `json:"result,omitempty"`
	ProtectedStateVerified bool                           `json:"protected_state_verified"`
	Error                  string                         `json:"error,omitempty"`
	Note                   string                         `json:"note"`
}

func captureProtected(m *pebbledb.MaintenanceDB) (protectedState, error) {
	var out protectedState
	var err error
	out.Boundary, err = strictBoundary(m)
	if err != nil {
		return out, err
	}
	out.Guard, err = readGuard(m)
	if err != nil {
		return out, err
	}
	if !out.Guard.LatestCommitmentRoot.Present {
		return out, errors.New("compact requires latest commitment root")
	}
	out.Canaries = make(map[string][]pebbledb.MaintenanceSample)
	for _, r := range rawdb.HotMaintenanceCanaryRanges() {
		samples, err := m.SamplePrefixHashes(r.Start, r.End, 4)
		if err != nil {
			return out, err
		}
		out.Canaries[r.Name] = samples
	}
	return out, nil
}

func runCompact(args []string) (retErr error) {
	fs := flag.NewFlagSet("compact", flag.ContinueOnError)
	hotInput := fs.String("hot-dir", "", "existing hot chaindata Pebble directory")
	stageInput := fs.String("stage-dir", "", "existing separate staging directory if identity is present")
	rangeName := fs.String("range", "", "schema-owned range name, or explicit all")
	yes := fs.Bool("yes", false, "perform physical-only compaction; default is read-only dry-run")
	minFreeGiB := fs.Uint64("min-free-gib", 64, "minimum free space to retain")
	maxWriteGiB := fs.Uint64("max-sst-write-gib", 8, "hard cap on this handle's SST allocation attempts")
	targetSSTMiB := fs.Uint64("target-sst-mib", 32, "target SST file size, 1..128 MiB")
	wal := fs.Bool("allow-wal-replay", false, "explicitly permit bounded Pebble WAL replay on write-open")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || *rangeName == "" || *minFreeGiB == 0 || *maxWriteGiB == 0 || *targetSSTMiB < 1 || *targetSSTMiB > 128 || *minFreeGiB > math.MaxUint64>>30 || *maxWriteGiB > math.MaxUint64>>30 {
		return errors.New("compact requires --range, positive bounded GiB budgets, and no positional arguments")
	}
	hot, err := checkedDir(*hotInput)
	if err != nil {
		return err
	}
	var stage string
	if *stageInput != "" {
		stage, err = checkedDir(*stageInput)
		if err != nil {
			return err
		}
		if stage == hot {
			return errors.New("hot and stage stores must be distinct")
		}
	}
	var selected *rawdb.PhysicalSpaceRange
	if *rangeName != "all" {
		for _, r := range rawdb.HotMaintenanceRanges() {
			if r.Name == *rangeName {
				selected = &r
				break
			}
		}
		if selected == nil {
			return fmt.Errorf("unknown physical range %q", *rangeName)
		}
	}
	report := compactReport{Version: 1, ObservedUTC: time.Now().UTC().Format(time.RFC3339), HotPath: hot, StagePath: stage, Range: *rangeName,
		DryRun: !*yes, AllowWALReplay: *wal, MinFreeBytes: *minFreeGiB << 30, MaxSSTWriteBytes: *maxWriteGiB << 30, TargetSSTBytes: *targetSSTMiB << 20,
		Note: "Physical compaction preserves all live KV; overlapping SST estimates are not a reclaim or peak-space guarantee. A failed compaction may have completed earlier SST rewrites."}
	defer func() {
		if retErr != nil {
			report.Error = retErr.Error()
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		retErr = errors.Join(retErr, enc.Encode(report))
	}()
	m, err := pebbledb.OpenMaintenance(hot, pebbledb.MaintenanceOptions{MinFreeBytes: report.MinFreeBytes, MaxSSTWriteBytes: report.MaxSSTWriteBytes, TargetFileSizeBytes: int64(report.TargetSSTBytes), AllowWALReplay: *wal})
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, m.Close()) }()
	report.Before, err = captureProtected(m)
	if err != nil {
		return err
	}
	if report.Before.Guard.StagingIdentity.Present {
		if stage == "" {
			return errors.New("staging identity present: --stage-dir required")
		}
		s, err := rawdb.NewHistoryStagingPebbleDB(stage, 64, 128, true)
		if err != nil {
			return err
		}
		identity, present, readErr := rawdb.ReadHistoryStagingIdentity(s)
		closeErr := s.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			return err
		}
		if !present || !reflect.DeepEqual(identity, report.Before.Guard.StagingIdentity.Value) {
			return errors.New("hot and stage identity mismatch")
		}
	} else if stage != "" {
		return errors.New("stage directory supplied but hot staging identity absent")
	}
	if selected == nil {
		report.Inspection, err = m.InspectAll()
	} else {
		report.Inspection, err = m.Inspect(selected.Start, selected.End)
	}
	if err != nil || !*yes {
		return err
	}
	var result pebbledb.MaintenanceResult
	var compactErr error
	if selected == nil {
		result, compactErr = m.CompactPreservingAll()
	} else {
		result, compactErr = m.CompactPreservingRange(selected.Start, selected.End)
	}
	report.Result = &result
	reopenErr := m.ReopenReadOnly()
	if reopenErr != nil {
		return errors.Join(compactErr, fmt.Errorf("reopen under lock: %w", reopenErr))
	}
	after, captureErr := captureProtected(m)
	report.After = &after
	if captureErr != nil {
		return errors.Join(compactErr, fmt.Errorf("reopen verification: %w", captureErr))
	}
	if err := verifyProtectedUnchanged(report.Before, after); err != nil {
		return errors.Join(compactErr, err)
	}
	report.ProtectedStateVerified = true
	return compactErr
}

func verifyProtectedUnchanged(before, after protectedState) error {
	if !reflect.DeepEqual(before, after) {
		return errors.New("protected metadata or current-state samples changed after compaction")
	}
	return nil
}
