// hot-maintenance is an offline, storage-only diagnostic and physical
// compaction utility. It is never the gtron node executable.
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
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/pebbledb"
)

type field[T any] struct {
	Present bool `json:"present"`
	Value   T    `json:"value,omitempty"`
}

type guard struct {
	HeadHash             field[string]                         `json:"head_hash"`
	SolidHash            field[string]                         `json:"solid_hash"`
	Execution            field[rawdb.StageProgress]            `json:"execution"`
	Finish               field[rawdb.StageProgress]            `json:"finish"`
	HistoryIndex         field[rawdb.StageProgress]            `json:"history_index"`
	Commitment           field[rawdb.StageProgress]            `json:"commitment_stage"`
	LatestCommitmentRoot field[string]                         `json:"latest_commitment_root"`
	CommitmentBase       field[rawdb.CommitmentBranchBase]     `json:"commitment_base"`
	CommitmentRotation   field[rawdb.CommitmentBranchRotation] `json:"commitment_rotation"`
	EngineStateSHA256    field[string]                         `json:"engine_state_sha256"`
	StagingIdentity      field[rawdb.HistoryStagingIdentity]   `json:"staging_identity"`
}

type routeCensus struct {
	Epoch                     uint64               `json:"epoch"`
	HeadBucket                uint64               `json:"head_bucket"`
	Source                    uint64               `json:"source"`
	Target                    uint64               `json:"target"`
	Cold                      uint64               `json:"cold"`
	SourceCleared             uint64               `json:"source_cleared"`
	TargetCleared             uint64               `json:"target_cleared"`
	Claim                     uint64               `json:"claim"`
	Receipt                   uint64               `json:"receipt"`
	Buckets                   uint64               `json:"buckets"`
	StaleEpochRoutes          uint64               `json:"stale_epoch_routes"`
	ColdIntervals             []coldBucketInterval `json:"cold_intervals"`
	FirstNonColdFromBucketOne *uint64              `json:"first_non_cold_from_bucket_one"`
	ResetIntent               routeResetSummary    `json:"reset_intent"`
	Barrier                   routeBarrierSummary  `json:"barrier"`
	Warning                   string               `json:"warning"`
	nextNonColdCandidate      uint64
}

type coldBucketInterval struct {
	StartBucket uint64 `json:"start_bucket"`
	EndBucket   uint64 `json:"end_bucket"`
	StartBlock  uint64 `json:"start_block"`
	EndBlock    uint64 `json:"end_block"`
}

type routeResetSummary struct {
	Present      bool   `json:"present"`
	Complete     bool   `json:"complete,omitempty"`
	OldEpoch     uint64 `json:"old_epoch,omitempty"`
	NewEpoch     uint64 `json:"new_epoch,omitempty"`
	ReadyThrough uint64 `json:"ready_through,omitempty"`
	TargetHeight uint64 `json:"target_height,omitempty"`
}

type routeBarrierSummary struct {
	Present         bool   `json:"present"`
	Epoch           uint64 `json:"epoch,omitempty"`
	ThroughBucket   uint64 `json:"through_bucket,omitempty"`
	EligibleThrough uint64 `json:"eligible_through_bucket,omitempty"`
	PlanDigest      string `json:"plan_digest,omitempty"`
	RouteDigest     string `json:"route_digest,omitempty"`
}

type statusReport struct {
	Version         int                             `json:"version"`
	ObservedUTC     string                          `json:"observed_utc"`
	HotPath         string                          `json:"hot_path"`
	StagePath       string                          `json:"stage_path,omitempty"`
	Guard           guard                           `json:"guard"`
	HotPebble       pebbledb.MaintenanceSpaceStats  `json:"hot_pebble"`
	StagePebble     *pebbledb.MaintenanceSpaceStats `json:"stage_pebble,omitempty"`
	Routes          *routeCensus                    `json:"routes,omitempty"`
	HotInspection   *rawdb.DatabaseInspection       `json:"hot_inspection,omitempty"`
	HotInspectRange string                          `json:"hot_inspect_range,omitempty"`
	StageInspection *rawdb.DatabaseInspection       `json:"stage_inspection,omitempty"`
	Note            string                          `json:"note"`
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: hot-maintenance status|inspect|compact --hot-dir EXISTING_CHAINDATA [--stage-dir EXISTING_STAGING]")
	}
	switch args[0] {
	case "status", "inspect":
		fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
		hot := fs.String("hot-dir", "", "existing hot chaindata Pebble directory")
		stage := fs.String("stage-dir", "", "existing separate history-staging Pebble directory")
		routes := fs.Bool("routes", false, "scan and count routed buckets; not a completeness proof")
		full := fs.Bool("full", false, "scan every live KV in hot and target stores")
		inspectRange := fs.String("inspect-range", "", "schema-owned hot read-only history range")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if fs.NArg() != 0 {
			return errors.New("unexpected positional arguments")
		}
		if args[0] == "inspect" {
			*full = true
		}
		if *inspectRange != "" && !*full {
			return errors.New("--inspect-range requires a live-key inspection")
		}
		report, err := readStatusRange(*hot, *stage, *routes, *full, *inspectRange)
		if err != nil {
			return err
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	case "compact":
		return runCompact(args[1:])
	case "cleanup-index":
		return runCleanupIndex(args[1:])
	case "retire-target":
		return runRetireTarget(args[1:])
	default:
		return fmt.Errorf("unknown operation %q", args[0])
	}
}

func checkedDir(path string) (string, error) {
	if path == "" {
		return "", errors.New("database directory is required")
	}
	path, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("database path %q is not a directory", path)
	}
	if _, err := os.Stat(filepath.Join(path, "CURRENT")); err != nil {
		return "", fmt.Errorf("database %q has no CURRENT: %w", path, err)
	}
	return path, nil
}

func readGuard(db ethdb.KeyValueReader) (g guard, err error) {
	h, present, err := rawdb.ReadHeadBlockHashStrict(db)
	if err != nil {
		return g, err
	}
	g.HeadHash = field[string]{present, h.Hex()}
	h, present, err = rawdb.ReadHeadSolidBlockHashStrict(db)
	if err != nil {
		return g, err
	}
	g.SolidHash = field[string]{present, h.Hex()}
	for _, item := range []struct {
		stage rawdb.StageID
		out   *field[rawdb.StageProgress]
	}{
		{rawdb.StageExecution, &g.Execution}, {rawdb.StageFinish, &g.Finish}, {rawdb.StageStateHistoryIndex, &g.HistoryIndex}, {rawdb.StageCommitment, &g.Commitment},
	} {
		row, ok, readErr := rawdb.ReadStageProgressRow(db, item.stage)
		if readErr != nil {
			return g, readErr
		}
		*item.out = field[rawdb.StageProgress]{ok, row}
	}
	root, present, err := rawdb.ReadLatestDomainCommitmentRoot(db)
	if err != nil {
		return g, err
	}
	g.LatestCommitmentRoot = field[string]{present, root.Hex()}
	base, present, err := rawdb.ReadCommitmentBranchBase(db)
	if err != nil {
		return g, err
	}
	g.CommitmentBase = field[rawdb.CommitmentBranchBase]{present, base}
	rotation, present, err := rawdb.ReadCommitmentBranchRotation(db)
	if err != nil {
		return g, err
	}
	g.CommitmentRotation = field[rawdb.CommitmentBranchRotation]{present, rotation}
	engine, present, err := rawdb.ReadCommitmentEngineState(db)
	if err != nil {
		return g, err
	}
	if present {
		sum := sha256.Sum256(engine)
		g.EngineStateSHA256 = field[string]{true, hex.EncodeToString(sum[:])}
	}
	identity, present, err := rawdb.ReadHistoryStagingIdentity(db)
	if err != nil {
		return g, err
	}
	g.StagingIdentity = field[rawdb.HistoryStagingIdentity]{present, identity}
	return g, nil
}

func spaceStats(path string) (pebbledb.MaintenanceSpaceStats, error) {
	m, err := pebbledb.OpenMaintenance(path, pebbledb.MaintenanceOptions{MinFreeBytes: 1 << 30, MaxSSTWriteBytes: 1 << 30})
	if err != nil {
		return pebbledb.MaintenanceSpaceStats{}, err
	}
	stats, readErr := m.SpaceStats()
	return stats, errors.Join(readErr, m.Close())
}

func readStatus(hotInput, stageInput string, routes, full bool) (statusReport, error) {
	return readStatusRange(hotInput, stageInput, routes, full, "")
}

func readStatusRange(hotInput, stageInput string, routes, full bool, inspectRange string) (statusReport, error) {
	var out statusReport
	var selected *rawdb.PhysicalSpaceRange
	if inspectRange != "" {
		if !full {
			return out, errors.New("selected inspect range requires live-key inspection")
		}
		for _, r := range rawdb.HotMaintenanceDiagnosticRanges() {
			if r.Name == inspectRange {
				selected = &r
				break
			}
		}
		if selected == nil {
			return out, fmt.Errorf("unknown read-only diagnostic range %q", inspectRange)
		}
	}
	hot, err := checkedDir(hotInput)
	if err != nil {
		return out, err
	}
	var stage string
	if stageInput != "" {
		stage, err = checkedDir(stageInput)
		if err != nil {
			return out, err
		}
		if stage == hot {
			return out, errors.New("hot and stage stores must be distinct")
		}
	}
	out.Version, out.ObservedUTC, out.HotPath, out.StagePath = 1, time.Now().UTC().Format(time.RFC3339), hot, stage
	out.HotPebble, err = spaceStats(hot)
	if err != nil {
		return out, err
	}
	if stage != "" {
		stats, err := spaceStats(stage)
		if err != nil {
			return out, err
		}
		out.StagePebble = &stats
	}
	hotDB, err := rawdb.NewPebbleDBReadOnly(hot, 64, 128)
	if err != nil {
		return out, err
	}
	defer hotDB.Close()
	out.Guard, err = readGuard(hotDB)
	if err != nil {
		return out, err
	}
	if out.Guard.StagingIdentity.Present && stage == "" {
		return out, errors.New("hot store has staging identity: --stage-dir is required")
	}
	var stageDB ethdb.KeyValueStore
	if stage != "" {
		stageDB, err = rawdb.NewHistoryStagingPebbleDB(stage, 64, 128, true)
		if err != nil {
			return out, err
		}
		defer stageDB.Close()
		identity, present, err := rawdb.ReadHistoryStagingIdentity(stageDB)
		if err != nil {
			return out, err
		}
		if !out.Guard.StagingIdentity.Present || !present || !reflect.DeepEqual(identity, out.Guard.StagingIdentity.Value) {
			return out, errors.New("hot and stage identity mismatch")
		}
	}
	if routes {
		if stageDB == nil {
			return out, errors.New("route census requires --stage-dir")
		}
		manager, err := rawdb.NewHistoryStagingManager(hotDB, stageDB, out.Guard.StagingIdentity.Value)
		if err != nil {
			return out, err
		}
		head, present, err := rawdb.ReadHeadBlockHashStrict(hotDB)
		if err != nil || !present || head == ([32]byte{}) || !out.Guard.Finish.Present || !out.Guard.Finish.Value.HasBlockHash || out.Guard.Finish.Value.BlockHash != head || out.Guard.Finish.Value.BlockNum != binary.BigEndian.Uint64(head[:8]) {
			return out, errors.Join(err, errors.New("route census requires coherent head and Finish pointer"))
		}
		census, err := scanRoutes(context.Background(), manager, out.Guard.Finish.Value.BlockNum/rawdb.StateHistoryChunkBucketBlocks)
		if err != nil {
			return out, err
		}
		out.Routes = &census
	}
	if full {
		opts := rawdb.InspectOptions{ProgressInterval: 30 * time.Second, Progress: func(p rawdb.InspectProgress) {
			fmt.Fprintf(os.Stderr, "hot rows=%d logical_bytes=%d elapsed=%s\n", p.Rows, p.LogicalBytes, p.Elapsed.Round(time.Second))
		}}
		if selected != nil {
			opts.Start, opts.End = selected.Start, selected.End
			out.HotInspectRange = selected.Name
		}
		inspection, err := rawdb.InspectDatabase(hotDB, opts)
		if err != nil {
			return out, err
		}
		out.HotInspection = &inspection
		if stageDB != nil && selected == nil {
			inspection, err = rawdb.InspectDatabase(stageDB, rawdb.InspectOptions{ProgressInterval: 30 * time.Second, Progress: func(p rawdb.InspectProgress) {
				fmt.Fprintf(os.Stderr, "stage rows=%d logical_bytes=%d elapsed=%s\n", p.Rows, p.LogicalBytes, p.Elapsed.Round(time.Second))
			}})
			if err != nil {
				return out, err
			}
			out.StageInspection = &inspection
		}
	}
	out.Note = "Pebble SST/metric bytes include obsolete versions; live key/value bytes are uncompressed; neither is immediately reclaimable space. Route census is not a canonical/cold proof."
	return out, nil
}

func scanRoutes(ctx context.Context, manager *rawdb.HistoryStagingManager, headBucket uint64) (routeCensus, error) {
	out := routeCensus{HeadBucket: headBucket}
	var err error
	out.Epoch, err = manager.CurrentEpoch()
	if err != nil {
		return routeCensus{}, err
	}
	reset, present, err := manager.ReadResetIntent()
	if err != nil {
		return routeCensus{}, err
	}
	out.ResetIntent = routeResetSummary{Present: present}
	if present {
		out.ResetIntent.Complete, out.ResetIntent.OldEpoch, out.ResetIntent.NewEpoch = reset.Complete, reset.OldEpoch, reset.NewEpoch
		out.ResetIntent.ReadyThrough, out.ResetIntent.TargetHeight = reset.ReadyThrough, reset.TargetHeight
	}
	barrier, present, err := manager.ReadHistoryStagingRouteBarrier()
	if err != nil {
		return routeCensus{}, err
	}
	out.Barrier = routeBarrierSummary{Present: present}
	if present {
		out.Barrier.Epoch, out.Barrier.ThroughBucket, out.Barrier.EligibleThrough = barrier.Epoch, barrier.ThroughBucket, barrier.EligibleThrough
		out.Barrier.PlanDigest, out.Barrier.RouteDigest = hex.EncodeToString(barrier.PlanDigest[:]), hex.EncodeToString(barrier.RouteDigest[:])
	}
	var after []byte
	out.nextNonColdCandidate = 1
	for {
		states, next, complete, err := manager.ScanBuckets(ctx, after, 128)
		if err != nil {
			return routeCensus{}, err
		}
		for _, state := range states {
			out.Buckets++
			if state.HasClaim {
				out.Claim++
			}
			if state.HasReceipt {
				out.Receipt++
			}
			currentRoute := state.HasRoute && state.Route.Epoch == out.Epoch
			if state.HasRoute && !currentRoute {
				out.StaleEpochRoutes++
			}
			isCold := currentRoute && state.Route.Owner == rawdb.HistoryStagingOwnerCold
			out.observeColdRoute(state.Bucket, isCold)
			if currentRoute {
				if state.Route.SourceCleared {
					out.SourceCleared++
				}
				if state.Route.TargetCleared {
					out.TargetCleared++
				}
				switch state.Route.Owner {
				case rawdb.HistoryStagingOwnerSource:
					out.Source++
				case rawdb.HistoryStagingOwnerTarget:
					out.Target++
				case rawdb.HistoryStagingOwnerCold:
					out.Cold++
				default:
					return routeCensus{}, errors.New("invalid route owner")
				}
			}
		}
		if complete {
			break
		}
		after = next
	}
	if out.FirstNonColdFromBucketOne == nil && out.nextNonColdCandidate <= headBucket {
		v := out.nextNonColdCandidate
		out.FirstNonColdFromBucketOne = &v
	}
	out.Warning = "Metadata census only; bucket 0 is excluded from intervals; missing routes count as non-COLD; intervals do not authenticate canonical history, receipts or cold files"
	return out, nil
}

func (out *routeCensus) observeColdRoute(bucket uint64, isCold bool) {
	if bucket >= 1 && bucket <= out.HeadBucket && out.FirstNonColdFromBucketOne == nil {
		if bucket > out.nextNonColdCandidate || (bucket == out.nextNonColdCandidate && !isCold) {
			v := out.nextNonColdCandidate
			out.FirstNonColdFromBucketOne = &v
		} else if bucket == out.nextNonColdCandidate {
			out.nextNonColdCandidate++
		}
	}
	if isCold && bucket >= 1 && bucket <= out.HeadBucket {
		out.addColdBucket(bucket)
	}
}

func (out *routeCensus) addColdBucket(bucket uint64) {
	const span = rawdb.StateHistoryChunkBucketBlocks
	n := len(out.ColdIntervals)
	if n > 0 && out.ColdIntervals[n-1].EndBucket+1 == bucket {
		out.ColdIntervals[n-1].EndBucket = bucket
		out.ColdIntervals[n-1].EndBlock = (bucket+1)*span - 1
		return
	}
	out.ColdIntervals = append(out.ColdIntervals, coldBucketInterval{StartBucket: bucket, EndBucket: bucket, StartBlock: bucket * span, EndBlock: (bucket+1)*span - 1})
}
