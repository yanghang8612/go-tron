package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	statesnapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/urfave/cli/v2"
)

var historyStagingJobIDPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)
var historyStagingSHAPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

func dbHistoryStagingCommand() *cli.Command {
	common := []cli.Flag{dataDirFlag, snapshotDirFlag, snapshotForkConfigHashFlag,
		testnetFlag, genesisFileFlag,
		configFileFlag, pruneModeFlag, historyEnabledFlag,
		&cli.StringFlag{Name: "staging-dir", Usage: "Separate history-staging Pebble directory"},
		&cli.StringFlag{Name: "job-id", Usage: "Durable 32-hex offline migration job identity"},
		&cli.StringFlag{Name: "candidate-sha256", Usage: "Required SHA256 of this running gtron executable"},
		&cli.StringFlag{Name: "legacy-manifest-sha256", Usage: "Exact SHA256 of an unbound local cold manifest for scoped history admission"},
		&cli.Uint64Flag{Name: "max-row-mib", Value: 16, Usage: "Maximum physical row size in MiB"},
		&cli.Uint64Flag{Name: "max-bucket-mib", Value: 4096, Usage: "Maximum physical source bucket size in MiB"},
		&cli.Uint64Flag{Name: "max-batch-mib", Value: 32, Usage: "Maximum copy/delete batch size in MiB"},
		&cli.Uint64Flag{Name: "max-work-mib", Value: 8192, Usage: "Maximum per-bucket copy work in MiB"},
		&cli.Uint64Flag{Name: "max-decoded-mib", Value: rawdb.HistoryStagingMaxDecodedBytes >> 20, Usage: "Maximum decoded row work in MiB"},
		&cli.Uint64Flag{Name: "min-free-gib", Value: 16, Usage: "Free-space reserve above per-bucket work"},
	}
	flags := func(extra ...cli.Flag) []cli.Flag { return append(append([]cli.Flag{}, common...), extra...) }
	return &cli.Command{Name: "history-staging", Usage: "Inspect and move history into an independent Pebble store offline",
		Description: "The node and automatic deployment must be stopped. Every command verifies its own executable SHA and opens only offline storage.",
		Subcommands: []*cli.Command{
			{Name: "capability", Usage: "Report the staged reader and migration protocol versions", Action: func(ctx *cli.Context) error {
				return json.NewEncoder(ctx.App.Writer).Encode(struct {
					FormatVersion        uint8 `json:"format_version"`
					HistoryStagingReader bool  `json:"history_staging_reader"`
					ProtocolVersion      uint8 `json:"protocol_version"`
				}{rawdb.HistoryStagingFormatVersion, true, 1})
			}},
			{Name: "inspect", Usage: "Read-only source/cold inventory or verify a completed plan",
				Flags:  flags(&cli.BoolFlag{Name: "verify-complete"}, &cli.BoolFlag{Name: "verify-pristine"}, &cli.StringFlag{Name: "plan-id"}),
				Action: dbHistoryStagingInspect},
			{Name: "migrate", Usage: "Freeze a bounded, authenticated JSONL plan without changing either Pebble store",
				Flags:  flags(&cli.Uint64Flag{Name: "max-buckets", Usage: "Maximum complete buckets; 0 means all"}),
				Action: dbHistoryStagingMigrate},
			{Name: "apply", Usage: "Apply a frozen plan using durable claim/copy/adopt/clear phases",
				Flags: flags(&cli.StringFlag{Name: "plan-id", Required: true}), Action: dbHistoryStagingApply},
			{Name: "resume", Usage: "Reconcile and finish a previously started frozen plan",
				Flags: flags(&cli.StringFlag{Name: "plan-id", Required: true}), Action: dbHistoryStagingResume},
		},
	}
}

type historyStagingCLIEvent struct {
	Version          int    `json:"version"`
	Phase            string `json:"phase"`
	JobID            string `json:"job_id"`
	CandidateSHA256  string `json:"candidate_sha256"`
	Source           string `json:"source"`
	Target           string `json:"target"`
	Cold             string `json:"cold"`
	PlanID           string `json:"plan_id,omitempty"`
	Head             uint64 `json:"head,omitempty"`
	Solid            uint64 `json:"solid,omitempty"`
	EligibleThrough  uint64 `json:"eligible_through,omitempty"`
	Bucket           uint64 `json:"bucket,omitempty"`
	Durable          bool   `json:"durable,omitempty"`
	VerifiedComplete bool   `json:"verified_complete,omitempty"`
	Pristine         bool   `json:"pristine,omitempty"`
	HistoryWindow    uint64 `json:"history_window,omitempty"`
	PruneMode        string `json:"prune_mode,omitempty"`
}

type historyStagingCLIContext struct {
	ctx    context.Context
	paths  historyStagingPaths
	event  historyStagingCLIEvent
	output io.Writer
}

type historyStagingPlanHeader struct {
	Version                int                      `json:"version"`
	JobID                  string                   `json:"job_id"`
	CandidateSHA256        string                   `json:"candidate_sha256"`
	Paths                  historyStagingPaths      `json:"paths"`
	GenesisHash            common.Hash              `json:"genesis_hash"`
	NetworkID              uint64                   `json:"network_id"`
	Head                   offlineChainBoundary     `json:"head"`
	IndexBlock             uint64                   `json:"index_block"`
	IndexHash              common.Hash              `json:"index_hash"`
	EligibleThrough        uint64                   `json:"eligible_through"`
	FullEligibleLastBucket uint64                   `json:"full_eligible_last_bucket"`
	LastBucket             uint64                   `json:"last_bucket"`
	PruneTxNum             uint64                   `json:"prune_tx_num"`
	HistoryWindow          uint64                   `json:"history_window"`
	PruneMode              string                   `json:"prune_mode"`
	ConfigSHA256           string                   `json:"config_sha256"`
	ManifestSHA256         string                   `json:"manifest_sha256"`
	Limits                 historyStagingPlanLimits `json:"limits"`
	CreatedUnix            int64                    `json:"created_unix"`
}

type historyStagingPlanLimits struct {
	MaxRowBytes     uint64 `json:"max_row_bytes"`
	MaxBucketBytes  uint64 `json:"max_bucket_bytes"`
	MaxBatchBytes   uint64 `json:"max_batch_bytes"`
	MaxWorkBytes    uint64 `json:"max_work_bytes"`
	MaxDecodedBytes uint64 `json:"max_decoded_bytes"`
	MinFreeBytes    uint64 `json:"min_free_bytes"`
}

type historyStagingPlanBucket struct {
	Proof    rawdb.HistoryStagingProof         `json:"proof"`
	Physical rawdb.HistoryStagingPhysicalStats `json:"physical"`
}

func historyStagingCLIWorkLimits(ctx *cli.Context, source, target string) (rawdb.HistoryStagingLimits, historyStagingPlanLimits, error) {
	readMiB := func(name string) (uint64, error) {
		value := ctx.Uint64(name)
		if value == 0 || value > ^uint64(0)>>20 {
			return 0, fmt.Errorf("invalid --%s", name)
		}
		return value << 20, nil
	}
	row, err := readMiB("max-row-mib")
	if err != nil {
		return rawdb.HistoryStagingLimits{}, historyStagingPlanLimits{}, err
	}
	bucket, err := readMiB("max-bucket-mib")
	if err != nil {
		return rawdb.HistoryStagingLimits{}, historyStagingPlanLimits{}, err
	}
	batch, err := readMiB("max-batch-mib")
	if err != nil {
		return rawdb.HistoryStagingLimits{}, historyStagingPlanLimits{}, err
	}
	work, err := readMiB("max-work-mib")
	if err != nil {
		return rawdb.HistoryStagingLimits{}, historyStagingPlanLimits{}, err
	}
	decoded, err := readMiB("max-decoded-mib")
	if err != nil {
		return rawdb.HistoryStagingLimits{}, historyStagingPlanLimits{}, err
	}
	if decoded > rawdb.HistoryStagingMaxDecodedBytes {
		return rawdb.HistoryStagingLimits{}, historyStagingPlanLimits{}, fmt.Errorf("invalid --max-decoded-mib: exceeds codec limit of %d MiB", rawdb.HistoryStagingMaxDecodedBytes>>20)
	}
	freeGiB := ctx.Uint64("min-free-gib")
	if freeGiB == 0 || freeGiB > ^uint64(0)>>30 {
		return rawdb.HistoryStagingLimits{}, historyStagingPlanLimits{}, errors.New("invalid --min-free-gib")
	}
	minimum := freeGiB << 30
	plan := historyStagingPlanLimits{row, bucket, batch, work, decoded, minimum}
	if row > batch || batch > bucket || bucket > work || decoded > work {
		return rawdb.HistoryStagingLimits{}, historyStagingPlanLimits{}, errors.New("history staging work limits are not monotonic")
	}
	limits := rawdb.HistoryStagingLimits{
		MaxRowBytes: row, MaxBucketBytes: bucket, MaxBatchBytes: batch,
		MaxWorkBytes: work, MaxDecodedBytes: decoded, MinFreeBytes: minimum,
		FreeBytes: func() (uint64, error) { return historyStagingMinimumFreeBytes(source, target) },
	}
	return limits, plan, nil
}

func historyStagingFileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	h := sha256.New()
	if _, err := io.Copy(h, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func runningHistoryStagingExecutableSHA256() (string, error) {
	path, err := os.Executable()
	if err != nil {
		return "", err
	}
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	before, err := file.Stat()
	if err != nil || !before.Mode().IsRegular() || before.Size() <= 0 || before.Size() > 512<<20 {
		return "", errors.New("history staging executable is not a bounded regular file")
	}
	h := sha256.New()
	if _, err := io.Copy(h, io.LimitReader(file, 512<<20+1)); err != nil {
		return "", err
	}
	after, err := file.Stat()
	if err != nil || !os.SameFile(before, after) || before.Size() != after.Size() ||
		before.ModTime() != after.ModTime() {
		return "", errors.New("history staging executable changed during hashing")
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func newHistoryStagingCLIContext(ctx *cli.Context) (*historyStagingCLIContext, error) {
	if !ctx.IsSet("datadir") {
		return nil, errors.New("history staging requires explicit --datadir")
	}
	job := ctx.String("job-id")
	claimed := ctx.String("candidate-sha256")
	if !historyStagingJobIDPattern.MatchString(job) || !historyStagingSHAPattern.MatchString(claimed) {
		return nil, errors.New("history staging requires valid --job-id and --candidate-sha256")
	}
	if pin := ctx.String("legacy-manifest-sha256"); pin != "" && !historyStagingSHAPattern.MatchString(pin) {
		return nil, errors.New("history staging legacy manifest pin must be a 64-hex SHA256")
	}
	if decoded := ctx.Uint64("max-decoded-mib"); decoded == 0 || decoded > rawdb.HistoryStagingMaxDecodedBytes>>20 {
		return nil, fmt.Errorf("invalid --max-decoded-mib: codec limit is %d MiB", rawdb.HistoryStagingMaxDecodedBytes>>20)
	}
	actual, err := runningHistoryStagingExecutableSHA256()
	if err != nil {
		return nil, err
	}
	if claimed != actual {
		return nil, fmt.Errorf("history staging executable SHA256 %s differs from pinned %s", actual, claimed)
	}
	dataDir := ctx.String("datadir")
	paths, err := resolveHistoryStagingPaths(dataDir, ctx.String("staging-dir"), snapshotDir(ctx, dataDir), false)
	if err != nil {
		return nil, err
	}
	return &historyStagingCLIContext{ctx: contextOrBackground(ctx), paths: paths,
		output: ctx.App.Writer, event: historyStagingCLIEvent{
			Version: 1, JobID: job, CandidateSHA256: actual,
			Source: paths.Source, Target: paths.Target, Cold: paths.Cold}}, nil
}

func (c *historyStagingCLIContext) emit(phase string) error {
	c.event.Phase = phase
	return json.NewEncoder(c.output).Encode(c.event)
}

func (c *historyStagingCLIContext) openSource() (ethdb.KeyValueStore, error) {
	return rawdb.NewPebbleDBReadOnly(c.paths.Source, 64, 128)
}

// inspectBoundary verifies the persisted head, solid, Finish and derived index
// against canonical bodies; a digest-free or ahead stage never grants work.
func (c *historyStagingCLIContext) inspectBoundary(ctx *cli.Context, source ethdb.KeyValueStore, datadir string) (offlineChainBoundary, uint64, error) {
	ancient, closeAncient, err := openSnapshotPruneAncientReader(datadir)
	if err != nil {
		return offlineChainBoundary{}, 0, err
	}
	defer closeAncient()
	canonical := rawdb.NewChainDB(source, ancient)
	forkHash, err := normaliseSnapshotForkConfigHash(ctx.String("snapshot.fork-config-hash"))
	if err != nil {
		return offlineChainBoundary{}, 0, err
	}
	expectedChain, err := snapshotExpectedChainIdentityFromContext(ctx, forkHash)
	if err != nil {
		return offlineChainBoundary{}, 0, err
	}
	actualGenesis, present, err := rawdb.ReadBlockHashByNumberStrict(canonical, 0)
	if err != nil || !present || actualGenesis != common.HexToHash(expectedChain.GenesisHash) {
		return offlineChainBoundary{}, 0, errors.New("history staging source genesis differs from configured chain")
	}
	boundary, err := readOfflineChainBoundary(canonical)
	if err != nil {
		return boundary, 0, err
	}
	readCanonical := func(n uint64) (common.Hash, bool, error) {
		return rawdb.ReadBlockHashByNumberStrict(canonical, n)
	}
	index, ok, err := rawdb.ReadVerifiedStageProgressBlockWithHashLookup(source,
		rawdb.StageStateHistoryIndex, readCanonical)
	if err != nil || !ok {
		return boundary, 0, fmt.Errorf("history staging needs hash-bound StateHistoryIndex: %w", err)
	}
	finish, ok, err := rawdb.ReadVerifiedStageProgressBlockWithHashLookup(source,
		rawdb.StageFinish, readCanonical)
	if err != nil || !ok || finish != boundary.HeadBlock {
		return boundary, 0, fmt.Errorf("history staging Finish differs from canonical head: %w", err)
	}
	genesis, err := makeGenesis(ctx)
	if err != nil {
		return boundary, 0, err
	}
	if err := applyHistoryConfig(ctx, genesis.Config); err != nil {
		return boundary, 0, err
	}
	mode, ok, err := rawdb.ReadHistoryPruneMode(source)
	if err != nil || !ok || mode != string(genesis.Config.EffectiveHistoryMode()) {
		return boundary, 0, fmt.Errorf("history staging configured prune mode differs from persisted mode: %w", err)
	}
	window := genesis.Config.EffectiveHistoryPruneWindow()
	eligible := min(boundary.SolidifiedBlock, index)
	if boundary.HeadBlock <= window || eligible > boundary.HeadBlock-window {
		if boundary.HeadBlock <= window {
			eligible = 0
		} else {
			eligible = boundary.HeadBlock - window
		}
	}
	return boundary, eligible, nil
}

func (c *historyStagingCLIContext) inspectCold(ctx *cli.Context, source ethdb.KeyValueStore) (*statesnapshots.Manifest, error) {
	manifestPath := filepath.Join(c.paths.Cold, statesnapshots.ManifestFile)
	beforeSHA, err := historyStagingFileSHA256(manifestPath)
	if err != nil {
		return nil, err
	}
	manifest, err := statesnapshots.LoadProductionManifest(c.paths.Cold)
	if err != nil {
		return nil, err
	}
	forkHash, err := normaliseSnapshotForkConfigHash(ctx.String("snapshot.fork-config-hash"))
	if err != nil {
		return nil, err
	}
	expected, err := snapshotExpectedChainIdentityFromContext(ctx, forkHash)
	if err != nil {
		return nil, err
	}
	if manifest.Chain != nil {
		if err := manifest.ValidateChainIdentity(expected); err != nil {
			return nil, err
		}
	} else {
		if pin := ctx.String("legacy-manifest-sha256"); pin == "" || pin != beforeSHA {
			return nil, errors.New("history staging unbound cold manifest requires its exact --legacy-manifest-sha256")
		}
		ancient, closeAncient, err := openSnapshotPruneAncientReader(ctx.String("datadir"))
		if err != nil {
			return nil, err
		}
		defer closeAncient()
		canonical := rawdb.NewChainDB(source, ancient)
		if err := statesnapshots.VerifyLegacyStateDomainHistoryBoundariesContext(c.ctx,
			source, c.paths.Cold, manifest, expected, func(number uint64) (common.Hash, bool, error) {
				return rawdb.ReadBlockHashByNumberStrict(canonical, number)
			}); err != nil {
			return nil, fmt.Errorf("history staging legacy cold boundary proof: %w", err)
		}
	}
	afterSHA, err := historyStagingFileSHA256(manifestPath)
	if err != nil || beforeSHA != afterSHA {
		return nil, errors.New("history staging cold manifest changed during admission")
	}
	return manifest, nil
}

func dbHistoryStagingInspect(ctx *cli.Context) error {
	c, err := newHistoryStagingCLIContext(ctx)
	if err != nil {
		return err
	}
	if ctx.Bool("verify-complete") && ctx.Bool("verify-pristine") {
		return errors.New("history staging inspect cannot combine complete and pristine checks")
	}
	if ctx.Bool("verify-complete") {
		return verifyHistoryStagingComplete(ctx)
	}
	progress := startHistoryStagingCLIProgress(ctx.App.ErrWriter, "inspect", "inspect-boundary", 30*time.Second)
	defer progress.close()
	source, err := c.openSource()
	if err != nil {
		return err
	}
	defer source.Close()
	boundary, eligible, err := c.inspectBoundary(ctx, source, ctx.String("datadir"))
	if err != nil {
		return err
	}
	if ctx.Bool("verify-pristine") {
		progress.stage.Store("verify-pristine")
		if err := c.verifyPristine(source, ctx.String("datadir")); err != nil {
			return err
		}
		c.event.Pristine = true
		c.event.Head, c.event.Solid, c.event.EligibleThrough =
			boundary.HeadBlock, boundary.SolidifiedBlock, eligible
		return c.emit("inspect")
	}
	progress.stage.Store("cold-admission")
	if _, err := c.inspectCold(ctx, source); err != nil {
		return err
	}
	c.event.Head, c.event.Solid, c.event.EligibleThrough =
		boundary.HeadBlock, boundary.SolidifiedBlock, eligible
	return c.emit("inspect")
}

func dbHistoryStagingMigrate(ctx *cli.Context) error {
	c, err := newHistoryStagingCLIContext(ctx)
	if err != nil {
		return err
	}
	progress := startHistoryStagingCLIProgress(ctx.App.ErrWriter, "migrate", "inspect-boundary", 30*time.Second)
	defer progress.close()
	source, err := c.openSource()
	if err != nil {
		return err
	}
	defer source.Close()
	boundary, eligible, err := c.inspectBoundary(ctx, source, ctx.String("datadir"))
	if err != nil {
		return err
	}
	manifest, err := c.inspectCold(ctx, source)
	if err != nil {
		return err
	}
	prover, err := statesnapshots.NewHistoryStagingColdProver(c.paths.Cold, manifest)
	if err != nil {
		return err
	}
	limits, frozenLimits, err := historyStagingCLIWorkLimits(ctx, c.paths.Source, c.paths.Target)
	if err != nil {
		return err
	}
	pruneTx := uint64(0)
	if progress, present, err := rawdb.ReadStageProgressRow(source, rawdb.StageSnapshotHotPrune); err != nil {
		return err
	} else if present {
		pruneTx = progress.BlockNum // this stage records txNum, not block height
	}
	index, present, err := rawdb.ReadStageProgressRow(source, rawdb.StageStateHistoryIndex)
	if err != nil || !present || !index.HasBlockHash {
		return errors.New("history staging index stage lost hash binding")
	}
	fullEligibleLastBucket := historyStagingFullEligibleBucket(eligible)
	lastBucket := fullEligibleLastBucket
	if maxBuckets := ctx.Uint64("max-buckets"); maxBuckets > 0 && lastBucket > maxBuckets {
		lastBucket = maxBuckets
	}
	progress.total.Store(lastBucket)
	manifestSHA, err := historyStagingFileSHA256(filepath.Join(c.paths.Cold, statesnapshots.ManifestFile))
	if err != nil {
		return err
	}
	forkHash, err := normaliseSnapshotForkConfigHash(ctx.String("snapshot.fork-config-hash"))
	if err != nil {
		return err
	}
	chain, err := snapshotExpectedChainIdentityFromContext(ctx, forkHash)
	if err != nil {
		return err
	}
	if chain.NetworkID < 0 {
		return errors.New("history staging manifest network ID is negative")
	}
	genesis, err := makeGenesis(ctx)
	if err != nil {
		return err
	}
	if err := applyHistoryConfig(ctx, genesis.Config); err != nil {
		return err
	}
	configSHA := fmt.Sprintf("%064x", 0)
	if configPath := ctx.String("config"); configPath != "" {
		configSHA, err = historyStagingFileSHA256(configPath)
		if err != nil {
			return err
		}
	}
	header := historyStagingPlanHeader{Version: 1, JobID: c.event.JobID,
		CandidateSHA256: c.event.CandidateSHA256, Paths: c.paths,
		GenesisHash: common.HexToHash(chain.GenesisHash), NetworkID: uint64(chain.NetworkID),
		Head: boundary, IndexBlock: index.BlockNum, IndexHash: index.BlockHash,
		EligibleThrough: eligible, FullEligibleLastBucket: fullEligibleLastBucket,
		LastBucket: lastBucket, HistoryWindow: genesis.Config.EffectiveHistoryPruneWindow(),
		PruneMode: string(genesis.Config.EffectiveHistoryMode()), ConfigSHA256: configSHA,
		PruneTxNum: pruneTx, ManifestSHA256: manifestSHA, Limits: frozenLimits,
		CreatedUnix: 0}
	planDir := historyStagingPlanDirectory(ctx.String("datadir"))
	if err := os.MkdirAll(planDir, 0o700); err != nil {
		return err
	}
	if err := os.Chmod(planDir, 0o700); err != nil {
		return err
	}
	if recoveredID, found, err := recoverHistoryStagingJobPlan(planDir, header); err != nil {
		return err
	} else if found {
		c.event.Head, c.event.Solid, c.event.EligibleThrough = boundary.HeadBlock, boundary.SolidifiedBlock, eligible
		c.event.PlanID = recoveredID
		return c.emit("migrate")
	}
	file, err := os.CreateTemp(planDir, "."+c.event.JobID+".plan.*.tmp")
	if err != nil {
		return err
	}
	tmp := file.Name()
	if err := file.Chmod(0o600); err != nil {
		file.Close()
		os.Remove(tmp)
		return err
	}
	defer os.Remove(tmp)
	defer file.Close()
	hash := sha256.New()
	buffer := bufio.NewWriterSize(io.MultiWriter(file, hash), 256<<10)
	if err := encodeHistoryStagingPlanRow(buffer, header); err != nil {
		return err
	}
	ancient, closeAncient, err := openSnapshotPruneAncientReader(ctx.String("datadir"))
	if err != nil {
		return err
	}
	defer closeAncient()
	canonical := rawdb.NewChainDB(source, ancient)
	hotView, releaseHot, err := rawdb.AcquireStateHistoryReadView(source)
	if err != nil {
		return err
	}
	defer releaseHot()
	progress.stage.Store("plan-buckets")
	for bucket := uint64(1); bucket <= lastBucket; bucket++ {
		progress.bucket.Store(bucket)
		if err := c.ctx.Err(); err != nil {
			return err
		}
		first, last, err := rawdb.StateHistoryChunkBucketBounds(bucket)
		if err != nil {
			return err
		}
		proof := rawdb.HistoryStagingProof{Bucket: bucket, Epoch: 1,
			EligibleThrough: eligible, FinishBlock: boundary.HeadBlock,
			FinishHash: boundary.HeadHash, IndexBlock: index.BlockNum,
			IndexHash: index.BlockHash,
			Blocks:    make([]rawdb.HistoryStagingBlockProof, 0, rawdb.StateHistoryChunkBucketBlocks)}
		missing := make([]bool, rawdb.StateHistoryChunkBucketBlocks)
		for number := first; number <= last; number++ {
			canonicalHash, present, err := rawdb.ReadBlockHashByNumberStrict(canonical, number)
			if err != nil || !present || canonicalHash == (common.Hash{}) {
				return fmt.Errorf("history staging canonical block %d unavailable: %w", number, err)
			}
			rangeRow, present, err := rawdb.ReadStateTxRange(hotView, number)
			if err != nil || !present || rangeRow == nil || rangeRow.BlockHash != canonicalHash {
				return fmt.Errorf("history staging tx range %d unavailable or noncanonical: %w", number, err)
			}
			proof.Blocks = append(proof.Blocks, rawdb.HistoryStagingBlockProof{
				Number: number, Hash: canonicalHash,
				BeginTxNum: rangeRow.BeginTxNum, EndTxNum: rangeRow.EndTxNum})
			missing[number-first] = rangeRow.EndTxNum <= pruneTx
		}
		proof.ColdSpans, err = prover.Build(c.ctx, proof.Blocks, missing)
		if err != nil {
			return fmt.Errorf("history staging bucket %d cold proof: %w", bucket, err)
		}
		if err := rawdb.VerifyHistoryStagingProof(proof); err != nil {
			return err
		}
		physical, err := rawdb.InspectHistoryStagingPhysicalBucket(c.ctx, hotView, proof, limits)
		if err != nil {
			return fmt.Errorf("history staging bucket %d physical inventory: %w", bucket, err)
		}
		if physical.Bytes > rawdb.HistoryStagingMaxCopyPhysicalBytes(limits.MaxWorkBytes) {
			return fmt.Errorf("history staging bucket %d exceeds bounded scan/copy/verify work budget", bucket)
		}
		if err := encodeHistoryStagingPlanRow(buffer, historyStagingPlanBucket{Proof: proof, Physical: physical}); err != nil {
			return err
		}
		c.event.Bucket = bucket
		progress.completed.Store(bucket)
	}
	progress.stage.Store("publish-plan")
	if err := buffer.Flush(); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := verifyHistoryStagingPlanInputs(ctx, c, source, header); err != nil {
		return err
	}
	planID := hex.EncodeToString(hash.Sum(nil))
	jobPlan := filepath.Join(planDir, c.event.JobID+".jsonl")
	if _, err := os.Lstat(jobPlan); err == nil {
		return errors.New("history staging job plan appeared during build")
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.Rename(tmp, jobPlan); err != nil {
		return err
	}
	if err := linkHistoryStagingDigestPlan(planDir, jobPlan, planID); err != nil {
		return err
	}
	directory, err := os.Open(planDir)
	if err != nil {
		return err
	}
	if err := directory.Sync(); err != nil {
		directory.Close()
		return err
	}
	if err := directory.Close(); err != nil {
		return err
	}
	c.event.Head, c.event.Solid, c.event.EligibleThrough = boundary.HeadBlock, boundary.SolidifiedBlock, eligible
	c.event.PlanID = planID
	return c.emit("migrate")
}

func dbHistoryStagingApply(ctx *cli.Context) error {
	return runHistoryStagingApply(ctx, "apply")
}

func dbHistoryStagingResume(ctx *cli.Context) error {
	return runHistoryStagingApply(ctx, "resume")
}

func historyStagingPlanDirectory(datadir string) string {
	return filepath.Join(datadir, "gtron", "history-staging-plans")
}
