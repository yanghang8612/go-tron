package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	statepruning "github.com/tronprotocol/go-tron/core/state/pruning"
	"github.com/tronprotocol/go-tron/params"
	"github.com/urfave/cli/v2"
)

func dbOfflineHistoryCommand() *cli.Command {
	return &cli.Command{
		Name: "offline-history", Usage: "Plan or execute one bounded cold-history batch with the node stopped",
		Description: "Default is read-only. --yes builds and verifies one cold history batch, then durably removes only covered hot changesets. Does not start a node, merge cold segments or compact SSTs. Keep automatic deployment stopped.",
		Flags: []cli.Flag{dataDirFlag, snapshotDirFlag, testnetFlag, genesisFileFlag, snapshotForkConfigHashFlag,
			&cli.StringFlag{Name: "legacy-manifest-sha256", Usage: "Exact digest permitting a legacy unbound manifest only after genesis and every history segment boundary match the canonical database; does not relabel the manifest"},
			&cli.Uint64Flag{Name: "hot-window", Value: params.HistoryColdDefaultPruneWindow, Usage: "Recent solidified blocks to retain in hot history"},
			&cli.Uint64Flag{Name: "max-blocks", Value: 256, Usage: "Maximum blocks in this single batch (at most 5000)"},
			&cli.Uint64Flag{Name: "max-txnums", Value: 20000, Usage: "Maximum transaction numbers in this single batch"},
			&cli.Uint64Flag{Name: "max-input-mib", Value: 256, Usage: "Hard bound on decoded source bytes admitted to a batch"},
			&cli.Uint64Flag{Name: "min-free-gib", Value: 64, Usage: "Free-space reserve beyond the complete work budget"},
			&cli.Uint64Flag{Name: "max-work-gib", Value: 8, Usage: "Maximum total admitted work budget, including 1 GiB for database/control writes"},
			&cli.StringFlag{Name: "etl-tempdir", Usage: "Existing directory for ETL scratch (defaults to snapshot directory)"},
			&cli.BoolFlag{Name: "yes", Usage: "Execute one verified offline history batch"},
		}, Action: dbOfflineHistoryCmd,
	}
}

type offlineHistoryReport struct {
	DryRun              bool                               `json:"dry_run"`
	Boundary            offlineChainBoundary               `json:"boundary"`
	Plan                statepruning.OfflineHistoryPlan    `json:"plan"`
	WorkBudgetBytes     uint64                             `json:"work_budget_bytes"`
	FreeBytesBefore     uint64                             `json:"free_bytes_before"`
	FreeBytesAfter      uint64                             `json:"free_bytes_after"`
	Result              *statepruning.OfflineHistoryResult `json:"result,omitempty"`
	VerifiedAfterReopen bool                               `json:"verified_after_reopen"`
	Error               string                             `json:"error,omitempty"`
}

func offlineHistoryWorkBudget(plan statepruning.OfflineHistoryPlan) (uint64, error) {
	if plan.Blocks == 0 {
		return 0, nil
	}
	// VerificationBytes describes scratch for the entire covering trio, which
	// can be much larger than this batch's compressed/source byte counts.
	n := plan.Input.ColdScratchUpperBytes
	if !plan.BuildNeeded {
		n = 0
	}
	if n > math.MaxUint64-(1<<30) || plan.VerificationBytes > math.MaxUint64-n-(1<<30) {
		return 0, fmt.Errorf("offline history work budget overflows")
	}
	return n + plan.VerificationBytes + (1 << 30), nil
}

func dbOfflineHistoryCmd(ctx *cli.Context) (retErr error) {
	report := offlineHistoryReport{DryRun: !ctx.Bool("yes")}
	var spacePath string
	// Keep failures before or during writable Open reviewable too. Register
	// this before the database close defer so close errors enter the JSON.
	defer func() {
		if spacePath != "" {
			var spaceErr error
			report.FreeBytesAfter, spaceErr = offlineSpaceAvailable(spacePath, 0, 0)
			retErr = errors.Join(retErr, spaceErr)
		}
		if retErr != nil {
			report.Error = retErr.Error()
		}
		encoder := json.NewEncoder(ctx.App.Writer)
		encoder.SetIndent("", "  ")
		retErr = errors.Join(retErr, encoder.Encode(report))
	}()
	if err := contextOrBackground(ctx).Err(); err != nil {
		return err
	}
	if !ctx.IsSet("datadir") {
		return fmt.Errorf("--datadir is required")
	}
	inputMiB := ctx.Uint64("max-input-mib")
	if inputMiB == 0 || inputMiB > math.MaxUint64>>20 {
		return fmt.Errorf("invalid --max-input-mib")
	}
	window := ctx.Uint64("hot-window")
	if window < params.HistoryColdDefaultPruneWindow {
		return fmt.Errorf("--hot-window must retain at least %d blocks", params.HistoryColdDefaultPruneWindow)
	}
	floor, err := maintenanceGiB(ctx.Uint64("min-free-gib"))
	if err != nil {
		return err
	}
	maxWork, err := maintenanceGiB(ctx.Uint64("max-work-gib"))
	if err != nil {
		return err
	}
	path := chainDataDir(ctx.String("datadir"))
	dir := snapshotDir(ctx, ctx.String("datadir"))
	tmp := ctx.String("etl-tempdir")
	if tmp == "" {
		tmp = dir
	}
	paths := []string{path, dir, tmp}
	for _, p := range paths {
		info, err := os.Stat(p)
		if err != nil {
			return err
		}
		if !info.IsDir() {
			return fmt.Errorf("%s must be an existing directory", p)
		}
	}
	spacePath = path
	forkHash, err := normaliseSnapshotForkConfigHash(ctx.String("snapshot.fork-config-hash"))
	if err != nil {
		return err
	}
	identity, err := snapshotExpectedChainIdentityFromContext(ctx, forkHash)
	if err != nil {
		return err
	}
	db, err := rawdb.NewPebbleDBReadOnly(path, 64, 128)
	if err != nil {
		return err
	}
	defer func() {
		if db != nil {
			retErr = errors.Join(retErr, db.Close())
		}
	}()
	boundary, err := readOfflineChainBoundary(db)
	if err != nil {
		return err
	}
	report.Boundary = boundary
	mode, ok, err := rawdb.ReadHistoryPruneMode(db)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("offline history requires persisted prune mode")
	}
	opts := statepruning.OfflineHistoryOptions{SnapshotDir: dir, ExpectedChain: identity, LegacyManifestSHA256: ctx.String("legacy-manifest-sha256"), Policy: statepruning.Policy{Mode: statepruning.Mode(mode), HistoryWindow: window, ReorgWindow: window}, SolidifiedBlock: boundary.SolidifiedBlock, MaxBlocks: ctx.Uint64("max-blocks"), MaxTxNums: ctx.Uint64("max-txnums"), MaxInputBytes: inputMiB << 20, ETL: etl.Options{TempDir: tmp, BufferLimit: 32 << 20, BatchSize: 4 << 20}}
	ancient, closeAncient, err := openSnapshotPruneAncientReader(ctx.String("datadir"))
	if err != nil {
		return err
	}
	defer closeAncient()
	// Keep the freezer read-only and reuse it across the RO/RW hot transition.
	canonical := rawdb.NewChainDB(db, ancient)
	opts.CanonicalHash = func(n uint64) (common.Hash, bool, error) { return rawdb.ReadBlockHashByNumberStrict(canonical, n) }
	plan, err := statepruning.PlanOfflineHistoryContext(contextOrBackground(ctx), db, opts)
	if err != nil {
		return err
	}
	report.Plan = plan
	work, err := offlineHistoryWorkBudget(plan)
	if err != nil {
		return err
	}
	free, err := offlineSpaceAvailable(path, 0, 0)
	if err != nil {
		return err
	}
	report.WorkBudgetBytes, report.FreeBytesBefore = work, free
	admit := func(p statepruning.OfflineHistoryPlan) error {
		if err := contextOrBackground(ctx).Err(); err != nil {
			return err
		}
		needed, err := offlineHistoryWorkBudget(p)
		if err != nil {
			return err
		}
		if needed > maxWork {
			return fmt.Errorf("offline history requires work budget %d, above configured maximum %d", needed, maxWork)
		}
		for _, path := range paths {
			if _, err := offlineSpaceAvailable(path, floor, needed); err != nil {
				return err
			}
		}
		return nil
	}
	if ctx.Bool("yes") && plan.Blocks > 0 {
		if err := admit(plan); err != nil {
			return err
		}
		// Bound ordinary RW Open's synchronous WAL recovery before using the
		// business-write wrapper. Larger recovery belongs to the guarded SST
		// maintenance command, not this cold-history transaction.
		if err := offlineHistoryCheckWAL(path); err != nil {
			return err
		}
		writable, writableErr := offlineHistoryReopenWritable(contextOrBackground(ctx), db, path)
		if writableErr != nil {
			return writableErr
		}
		db = writable
		canonical = rawdb.NewChainDB(db, ancient)
		after, checkErr := readOfflineChainBoundary(db)
		if checkErr != nil {
			return checkErr
		}
		if after != boundary {
			return fmt.Errorf("chain boundary changed before offline write")
		}
		syncer, ok := db.(interface{ SyncKeyValue() error })
		if !ok {
			return fmt.Errorf("database lacks durable WAL sync")
		}
		opts.Sync = syncer.SyncKeyValue
		opts.BeforeWrite = func(p statepruning.OfflineHistoryPlan) error {
			if p.ManifestSHA256 != plan.ManifestSHA256 || p.FromBlock != plan.FromBlock || p.ToBlock != plan.ToBlock {
				return fmt.Errorf("offline history plan changed before write")
			}
			if err := admit(p); err != nil {
				return err
			}
			fmt.Fprintf(ctx.App.ErrWriter, "Offline history batch blocks=[%d,%d] txnums=%d input=%d; build=%t; no node is started.\n", p.FromBlock, p.ToBlock, p.TxNums, p.Input.InputBytes, p.BuildNeeded)
			return nil
		}
		passCtx, stop := watchOfflineSpace(contextOrBackground(ctx), paths, floor)
		result, passErr := statepruning.OfflineHistoryPassContext(passCtx, db, opts)
		cause := context.Cause(passCtx)
		stop()
		if passErr != nil && cause != nil {
			passErr = errors.Join(passErr, cause)
		}
		report.Result = &result
		err = errors.Join(passErr, db.Close())
		check, openErr := rawdb.NewPebbleDBReadOnly(path, 64, 128)
		err = errors.Join(err, openErr)
		if openErr == nil {
			after, checkErr := readOfflineChainBoundary(check)
			if checkErr == nil && after != boundary {
				checkErr = fmt.Errorf("chain boundary changed across offline history")
			}
			closeErr := check.Close()
			report.VerifiedAfterReopen = checkErr == nil && closeErr == nil
			err = errors.Join(err, checkErr, closeErr)
		}
	}
	return err
}

// Preserve the caller's owned handle on failure. rawdb wraps Pebble's nil
// pointer in a KeyValueStore interface on Open errors, so assigning that value
// to the caller's deferred-close variable would panic despite db != nil.
func offlineHistoryReopenWritable(ctx context.Context, current ethdb.KeyValueStore, path string) (ethdb.KeyValueStore, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := current.Close(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	tune := rawdb.DefaultPebbleOptions()
	tune.DisableAutomaticCompactions = true
	tune.MaxConcurrentCompactions = 1
	tune.MemTableSizeBytes = 64 << 20
	// Existing L0 can exceed the normal stop threshold. With automatic
	// compaction disabled, waiting for it to shrink would deadlock writes.
	tune.L0CompactionThreshold = math.MaxInt32 - 1
	tune.L0StopWritesThreshold = math.MaxInt32
	next, err := rawdb.NewPebbleDBWithOptions(path, 64, 128, tune)
	if err != nil {
		return nil, err
	}
	return next, nil
}

func offlineHistoryCheckWAL(path string) error {
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	var total uint64
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".log") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("offline history requires regular WAL files: %s", e.Name())
		}
		if info.Size() < 0 || uint64(info.Size()) > 256<<20 || total > 256<<20-uint64(info.Size()) {
			return fmt.Errorf("offline history WAL recovery exceeds 256 MiB; use guarded physical maintenance first")
		}
		total += uint64(info.Size())
	}
	return nil
}
