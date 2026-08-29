package snapshots

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

type AggregatorDB interface {
	ethdb.KeyValueReader
	ethdb.Iteratee
}

type Aggregator struct {
	dir string
}

type AggregatorBuildOptions struct {
	FromTxNum uint64
	ToTxNum   uint64
	KVDomains []kvdomains.KVDomain
}

type AggregatorBuildChainFreezerOptions struct {
	ETL RestoreETLOptions
}

type AggregatorBuildDerivedOptions struct {
	BalanceTraces   bool
	SectionBlooms   bool
	EventLogs       bool
	EventLogVersion uint32
	ETL             RestoreETLOptions
}

type EventLogBuildOptions struct {
	Version uint32
	ETL     RestoreETLOptions
}

type AggregatorBuildResult struct {
	Manifest *Manifest
	Segments []SegmentRef
}

func NewAggregator(dir string) *Aggregator {
	return &Aggregator{dir: dir}
}

func (a *Aggregator) Build(db AggregatorDB, opts AggregatorBuildOptions) (*AggregatorBuildResult, error) {
	refs, err := a.BuildSegments(db, opts)
	if err != nil {
		return nil, err
	}
	manifest, err := a.Integrate(opts.FromTxNum, opts.ToTxNum, refs)
	if err != nil {
		return nil, err
	}
	if writer, ok := db.(ethdb.KeyValueWriter); ok {
		if err := WriteManifestProgressStages(writer, manifest.Progress); err != nil {
			return nil, err
		}
	}
	return &AggregatorBuildResult{
		Manifest: manifest,
		Segments: append([]SegmentRef(nil), refs...),
	}, nil
}

func (a *Aggregator) buildLatestSegments(db AggregatorDB, opts AggregatorBuildOptions) ([]SegmentRef, error) {
	return a.buildLatestSegmentsContext(context.Background(), db, opts)
}

func (a *Aggregator) buildLatestSegmentsContext(ctx context.Context, db AggregatorDB, opts AggregatorBuildOptions) ([]SegmentRef, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if a == nil || a.dir == "" {
		return nil, errors.New("snapshots: nil aggregator or empty directory")
	}
	if db == nil {
		return nil, errors.New("snapshots: nil database")
	}
	if opts.ToTxNum < opts.FromTxNum {
		return nil, fmt.Errorf("snapshots: aggregate range [%d,%d] is inverted", opts.FromTxNum, opts.ToTxNum)
	}
	var domains []kvdomains.KVDomain
	if len(opts.KVDomains) != 0 {
		var err error
		domains, err = normalizeAggregationKVDomains(opts.KVDomains)
		if err != nil {
			return nil, err
		}
	}
	kvLatestDomains := make(map[SegmentDataset][]kvdomains.KVDomain)
	kvLatestDomains[SegmentDatasetKVLatest] = domains

	var refs []SegmentRef
	registry := DefaultDomainRegistry()
	for _, cfg := range registry.LatestConfigs() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if cfg.BuildLatest == nil && cfg.BuildLatestContext == nil && cfg.BuildLatestDomainsContext == nil {
			return nil, fmt.Errorf("snapshots: latest domain %s has no builder", cfg.Dataset)
		}
		if cfg.DomainSpecific {
			if cfg.BuildLatestDomainsContext != nil {
				built, err := cfg.BuildLatestDomainsContext(ctx, db, a.dir, kvLatestDomains[cfg.Dataset], opts.FromTxNum, opts.ToTxNum, func(domain kvdomains.KVDomain) string {
					return aggregateLatestPath(cfg.LatestPathBase(domain), opts, cfg.latestPathExt())
				})
				if err != nil {
					return nil, err
				}
				refs = append(refs, built...)
				continue
			}
			datasetDomains := kvLatestDomains[cfg.Dataset]
			if len(datasetDomains) == 0 {
				var discoverErr error
				datasetDomains, discoverErr = aggregationKVDomainsContext(ctx, db, nil)
				if discoverErr != nil {
					return nil, discoverErr
				}
			}
			for _, domain := range datasetDomains {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				built, err := cfg.buildLatestContext(ctx, db, a.dir, domain, opts.FromTxNum, opts.ToTxNum, aggregateLatestPath(cfg.LatestPathBase(domain), opts, cfg.latestPathExt()))
				if err != nil {
					return nil, err
				}
				refs = append(refs, built...)
			}
			continue
		}
		built, err := cfg.buildLatestContext(ctx, db, a.dir, 0, opts.FromTxNum, opts.ToTxNum, aggregateLatestPath(cfg.LatestPathBase(0), opts, cfg.latestPathExt()))
		if err != nil {
			return nil, err
		}
		refs = append(refs, built...)
	}
	return refs, nil
}

func (a *Aggregator) BuildSegments(db AggregatorDB, opts AggregatorBuildOptions) ([]SegmentRef, error) {
	refs, err := a.buildLatestSegments(db, opts)
	if err != nil {
		return nil, err
	}
	registry := DefaultDomainRegistry()
	for _, cfg := range registry.HistoryConfigs() {
		if cfg.BuildHistory == nil {
			return nil, fmt.Errorf("snapshots: history domain %s has no builder", cfg.Dataset)
		}
		historyRefs, err := cfg.BuildHistory(db, a.dir, opts.FromTxNum, opts.ToTxNum, cfg.HistoryPath(opts.FromTxNum, opts.ToTxNum))
		if err != nil {
			return nil, err
		}
		refs = append(refs, historyRefs...)
	}
	sortSegments(refs)
	return refs, nil
}

// BuildLatest builds only the registered latest-domain segments for [FromTxNum,
// ToTxNum] and integrates them into the manifest. History segments are owned by
// the cold history Runner pass and are not touched here.
func (a *Aggregator) BuildLatest(db AggregatorDB, opts AggregatorBuildOptions) (*AggregatorBuildResult, error) {
	return a.BuildLatestContext(context.Background(), db, opts)
}

func (a *Aggregator) BuildLatestContext(ctx context.Context, db AggregatorDB, opts AggregatorBuildOptions) (*AggregatorBuildResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	refs, err := a.buildLatestSegmentsContext(ctx, db, opts)
	if err != nil {
		return nil, err
	}
	if len(refs) == 0 {
		return &AggregatorBuildResult{}, nil
	}
	sortSegments(refs)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	manifest, err := a.Integrate(opts.FromTxNum, opts.ToTxNum, refs)
	if err != nil {
		return nil, err
	}
	if writer, ok := db.(ethdb.KeyValueWriter); ok {
		if err := WriteManifestProgressStages(writer, manifest.Progress); err != nil {
			return nil, err
		}
	}
	return &AggregatorBuildResult{Manifest: manifest, Segments: append([]SegmentRef(nil), refs...)}, nil
}

// BuildCommitmentBranchBaseContext builds only the immutable commitment root
// and branch families. It is intentionally separate from BuildLatestContext:
// deep sync can bound the mutable branch delta without scanning Accounts, KV,
// Code, or checkpoints and without falsely advancing their shared latest or
// accessor progress.
func (a *Aggregator) BuildCommitmentBranchBaseContext(ctx context.Context, db AggregatorDB, opts AggregatorBuildOptions) (*AggregatorBuildResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if a == nil || a.dir == "" {
		return nil, errors.New("snapshots: nil aggregator or empty directory")
	}
	if db == nil {
		return nil, errors.New("snapshots: nil database")
	}
	if opts.ToTxNum < opts.FromTxNum {
		return nil, fmt.Errorf("snapshots: aggregate range [%d,%d] is inverted", opts.FromTxNum, opts.ToTxNum)
	}

	registry := DefaultDomainRegistry()
	datasets := [...]SegmentDataset{SegmentDatasetCommitmentRoot, SegmentDatasetCommitmentBranch}
	var refs []SegmentRef
	for _, dataset := range datasets {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cfg, ok := registry.Dataset(dataset)
		if !ok || !cfg.HasLatest || cfg.DomainSpecific {
			return nil, fmt.Errorf("snapshots: commitment base dataset %s is not a scalar latest domain", dataset)
		}
		if cfg.BuildLatest == nil && cfg.BuildLatestContext == nil {
			return nil, fmt.Errorf("snapshots: latest domain %s has no builder", dataset)
		}
		built, err := cfg.buildLatestContext(ctx, db, a.dir, 0, opts.FromTxNum, opts.ToTxNum,
			aggregateLatestPath(cfg.LatestPathBase(0), opts, cfg.latestPathExt()))
		if err != nil {
			return nil, err
		}
		refs = append(refs, built...)
	}
	if len(refs) == 0 {
		return &AggregatorBuildResult{}, nil
	}
	sortSegments(refs)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	manifest, err := a.integrateCommitmentBranchBase(opts.FromTxNum, opts.ToTxNum, refs)
	if err != nil {
		return nil, err
	}
	if writer, ok := db.(ethdb.KeyValueWriter); ok {
		if err := WriteManifestProgressStages(writer, manifest.Progress); err != nil {
			return nil, err
		}
	}
	return &AggregatorBuildResult{Manifest: manifest, Segments: append([]SegmentRef(nil), refs...)}, nil
}

func (a *Aggregator) BuildChainFreezer(reader rawdb.AncientReader, fromBlock, toBlock uint64) (*AggregatorBuildResult, error) {
	return a.BuildChainFreezerWithOptions(reader, fromBlock, toBlock, AggregatorBuildChainFreezerOptions{})
}

func (a *Aggregator) BuildChainFreezerWithOptions(reader rawdb.AncientReader, fromBlock, toBlock uint64, opts AggregatorBuildChainFreezerOptions) (*AggregatorBuildResult, error) {
	if a == nil || a.dir == "" {
		return nil, errors.New("snapshots: nil aggregator or empty directory")
	}
	refs, err := buildChainFreezerCompanionSegmentsFromAncient(reader, a.dir,
		ChainFreezerSegmentPath(fromBlock, toBlock),
		ChainFreezerAccessorSegmentPath(fromBlock, toBlock),
		ChainIndexSegmentPath(fromBlock, toBlock),
		fromBlock, toBlock, opts.ETL)
	if err != nil {
		return nil, err
	}
	visibleStart, visibleEnd := uint64(0), uint64(0)
	if old, err := LoadProductionManifest(a.dir); err == nil {
		visibleStart = old.VisibleTxStart
		visibleEnd = old.VisibleTxEnd
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	manifest, err := a.Integrate(visibleStart, visibleEnd, refs)
	if err != nil {
		return nil, err
	}
	return &AggregatorBuildResult{Manifest: manifest, Segments: refs}, nil
}

func (a *Aggregator) BuildBalanceTraces(db AggregatorDB, fromBlock, toBlock uint64) (*AggregatorBuildResult, error) {
	return a.BuildBalanceTracesWithOptions(db, fromBlock, toBlock, RestoreETLOptions{})
}

func (a *Aggregator) BuildBalanceTracesWithOptions(db AggregatorDB, fromBlock, toBlock uint64, opts RestoreETLOptions) (*AggregatorBuildResult, error) {
	return a.BuildDerivedIndexes(db, fromBlock, toBlock, AggregatorBuildDerivedOptions{BalanceTraces: true, ETL: opts})
}

func (a *Aggregator) BuildBalanceTracesFromReader(reader rawdb.BalanceTraceReader, fromBlock, toBlock uint64) (*AggregatorBuildResult, error) {
	return a.BuildBalanceTracesFromReaderWithOptions(reader, fromBlock, toBlock, RestoreETLOptions{})
}

func (a *Aggregator) BuildBalanceTracesFromReaderWithOptions(reader rawdb.BalanceTraceReader, fromBlock, toBlock uint64, opts RestoreETLOptions) (*AggregatorBuildResult, error) {
	if a == nil || a.dir == "" {
		return nil, errors.New("snapshots: nil aggregator or empty directory")
	}
	ref, err := BuildBalanceTraceSegmentFromReaderWithOptions(reader, a.dir, BalanceTraceSegmentPath(fromBlock, toBlock), fromBlock, toBlock, opts)
	if err != nil {
		return nil, err
	}
	visibleStart, visibleEnd := uint64(0), uint64(0)
	if old, err := LoadProductionManifest(a.dir); err == nil {
		visibleStart = old.VisibleTxStart
		visibleEnd = old.VisibleTxEnd
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	manifest, err := a.Integrate(visibleStart, visibleEnd, []SegmentRef{ref})
	if err != nil {
		return nil, err
	}
	return &AggregatorBuildResult{Manifest: manifest, Segments: []SegmentRef{ref}}, nil
}

func (a *Aggregator) BuildSectionBlooms(db AggregatorDB, fromBlock, toBlock uint64) (*AggregatorBuildResult, error) {
	return a.BuildSectionBloomsWithOptions(db, fromBlock, toBlock, RestoreETLOptions{})
}

func (a *Aggregator) BuildSectionBloomsWithOptions(db AggregatorDB, fromBlock, toBlock uint64, opts RestoreETLOptions) (*AggregatorBuildResult, error) {
	return a.BuildDerivedIndexes(db, fromBlock, toBlock, AggregatorBuildDerivedOptions{SectionBlooms: true, ETL: opts})
}

func (a *Aggregator) BuildSectionBloomsFromReader(reader rawdb.SectionBloomReader, fromBlock, toBlock uint64) (*AggregatorBuildResult, error) {
	return a.BuildSectionBloomsFromReaderWithOptions(reader, fromBlock, toBlock, RestoreETLOptions{})
}

func (a *Aggregator) BuildSectionBloomsFromReaderWithOptions(reader rawdb.SectionBloomReader, fromBlock, toBlock uint64, opts RestoreETLOptions) (*AggregatorBuildResult, error) {
	if a == nil || a.dir == "" {
		return nil, errors.New("snapshots: nil aggregator or empty directory")
	}
	ref, err := BuildSectionBloomSegmentFromReaderWithOptions(reader, a.dir, SectionBloomSegmentPath(fromBlock, toBlock), fromBlock, toBlock, opts)
	if err != nil {
		return nil, err
	}
	visibleStart, visibleEnd := uint64(0), uint64(0)
	if old, err := LoadProductionManifest(a.dir); err == nil {
		visibleStart = old.VisibleTxStart
		visibleEnd = old.VisibleTxEnd
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	manifest, err := a.Integrate(visibleStart, visibleEnd, []SegmentRef{ref})
	if err != nil {
		return nil, err
	}
	return &AggregatorBuildResult{Manifest: manifest, Segments: []SegmentRef{ref}}, nil
}

func (a *Aggregator) BuildEventLogs(chain *rawdb.ChainDB, fromBlock, toBlock uint64) (*AggregatorBuildResult, error) {
	return a.BuildEventLogsWithOptions(chain, fromBlock, toBlock, RestoreETLOptions{})
}

func (a *Aggregator) BuildEventLogsWithOptions(chain *rawdb.ChainDB, fromBlock, toBlock uint64, opts RestoreETLOptions) (*AggregatorBuildResult, error) {
	return a.BuildEventLogsWithBuildOptions(chain, fromBlock, toBlock, EventLogBuildOptions{Version: EventLogSegmentVersion, ETL: opts})
}

func (a *Aggregator) BuildEventLogsWithBuildOptions(chain *rawdb.ChainDB, fromBlock, toBlock uint64, opts EventLogBuildOptions) (*AggregatorBuildResult, error) {
	if a == nil || a.dir == "" {
		return nil, errors.New("snapshots: nil aggregator or empty directory")
	}
	if chain == nil {
		return nil, errors.New("snapshots: nil chain database")
	}
	if toBlock < fromBlock {
		return nil, fmt.Errorf("snapshots: event log block range [%d,%d] is inverted", fromBlock, toBlock)
	}
	refs, err := buildEventLogPairFromChain(chain, a.dir, fromBlock, toBlock, opts)
	if err != nil {
		return nil, err
	}
	visibleStart, visibleEnd := uint64(0), uint64(0)
	if old, err := LoadProductionManifest(a.dir); err == nil {
		visibleStart = old.VisibleTxStart
		visibleEnd = old.VisibleTxEnd
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	manifest, err := a.Integrate(visibleStart, visibleEnd, refs)
	if err != nil {
		return nil, err
	}
	if err := writeEventLogBuildStage(chain, manifest); err != nil {
		return nil, err
	}
	return &AggregatorBuildResult{Manifest: manifest, Segments: refs}, nil
}

func (a *Aggregator) BuildEventLogsFromReader(reader rawdb.EventLogReader, fromBlock, toBlock uint64) (*AggregatorBuildResult, error) {
	return a.BuildEventLogsFromReaderWithOptions(reader, fromBlock, toBlock, RestoreETLOptions{})
}

func (a *Aggregator) BuildEventLogsFromReaderWithOptions(reader rawdb.EventLogReader, fromBlock, toBlock uint64, opts RestoreETLOptions) (*AggregatorBuildResult, error) {
	return a.BuildEventLogsFromReaderWithBuildOptions(reader, fromBlock, toBlock, EventLogBuildOptions{Version: EventLogSegmentVersion, ETL: opts})
}

func (a *Aggregator) BuildEventLogsFromReaderWithBuildOptions(reader rawdb.EventLogReader, fromBlock, toBlock uint64, opts EventLogBuildOptions) (*AggregatorBuildResult, error) {
	if a == nil || a.dir == "" {
		return nil, errors.New("snapshots: nil aggregator or empty directory")
	}
	refs, err := buildEventLogPairFromReader(reader, a.dir, fromBlock, toBlock, opts)
	if err != nil {
		return nil, err
	}
	visibleStart, visibleEnd := uint64(0), uint64(0)
	if old, err := LoadProductionManifest(a.dir); err == nil {
		visibleStart = old.VisibleTxStart
		visibleEnd = old.VisibleTxEnd
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	manifest, err := a.Integrate(visibleStart, visibleEnd, refs)
	if err != nil {
		return nil, err
	}
	return &AggregatorBuildResult{Manifest: manifest, Segments: refs}, nil
}

func normalizedEventLogBuildVersion(version uint32) (uint32, error) {
	if version == 0 {
		version = EventLogSegmentVersion
	}
	if version != EventLogSegmentVersion && version != EventLogSegmentV4Version {
		return 0, fmt.Errorf("snapshots: event-log build version %d is unsupported; use 2 or 4", version)
	}
	return version, nil
}

func buildEventLogPairFromChain(chain *rawdb.ChainDB, dir string, fromBlock, toBlock uint64, opts EventLogBuildOptions) ([]SegmentRef, error) {
	version, err := normalizedEventLogBuildVersion(opts.Version)
	if err != nil {
		return nil, err
	}
	if version == EventLogSegmentV4Version {
		ref, err := BuildEventLogV4SegmentFromChain(chain, dir, EventLogSegmentPath(fromBlock, toBlock), fromBlock, toBlock)
		if err != nil {
			return nil, err
		}
		indexRef, err := writeFreshEventLogV4Index(dir, ref, EventLogIndexSegmentPath(fromBlock, toBlock))
		if err != nil {
			return nil, err
		}
		return []SegmentRef{ref, indexRef}, nil
	}
	build, err := buildEventLogSegmentFromChainWithOptions(chain, dir, EventLogSegmentPath(fromBlock, toBlock), fromBlock, toBlock, opts.ETL)
	if err != nil {
		return nil, err
	}
	indexRef, err := writeFreshEventLogIndexSegment(dir, build, EventLogIndexSegmentPath(fromBlock, toBlock))
	if err != nil {
		return nil, err
	}
	return []SegmentRef{build.Ref, indexRef}, nil
}

func buildEventLogPairFromReader(reader rawdb.EventLogReader, dir string, fromBlock, toBlock uint64, opts EventLogBuildOptions) ([]SegmentRef, error) {
	version, err := normalizedEventLogBuildVersion(opts.Version)
	if err != nil {
		return nil, err
	}
	if version == EventLogSegmentV4Version {
		ref, err := BuildEventLogV4SegmentFromReader(reader, dir, EventLogSegmentPath(fromBlock, toBlock), fromBlock, toBlock)
		if err != nil {
			return nil, err
		}
		indexRef, err := writeFreshEventLogV4Index(dir, ref, EventLogIndexSegmentPath(fromBlock, toBlock))
		if err != nil {
			return nil, err
		}
		return []SegmentRef{ref, indexRef}, nil
	}
	build, err := buildEventLogSegmentFromReaderWithOptions(reader, dir, EventLogSegmentPath(fromBlock, toBlock), fromBlock, toBlock, opts.ETL)
	if err != nil {
		return nil, err
	}
	indexRef, err := writeFreshEventLogIndexSegment(dir, build, EventLogIndexSegmentPath(fromBlock, toBlock))
	if err != nil {
		return nil, err
	}
	return []SegmentRef{build.Ref, indexRef}, nil
}

func (a *Aggregator) BuildDerivedIndexes(db AggregatorDB, fromBlock, toBlock uint64, opts AggregatorBuildDerivedOptions) (*AggregatorBuildResult, error) {
	if a == nil || a.dir == "" {
		return nil, errors.New("snapshots: nil aggregator or empty directory")
	}
	if db == nil {
		return nil, errors.New("snapshots: nil database")
	}
	if toBlock < fromBlock {
		return nil, fmt.Errorf("snapshots: derived index block range [%d,%d] is inverted", fromBlock, toBlock)
	}
	if !opts.BalanceTraces && !opts.SectionBlooms && !opts.EventLogs {
		return nil, errors.New("snapshots: no derived index datasets selected")
	}
	var chain *rawdb.ChainDB
	if opts.EventLogs {
		var ok bool
		chain, ok = db.(*rawdb.ChainDB)
		if !ok {
			return nil, errors.New("snapshots: event log derived index build requires rawdb.ChainDB")
		}
	}

	refs := make([]SegmentRef, 0, 3)
	if opts.BalanceTraces {
		ref, err := BuildBalanceTraceSegmentFromDBWithOptions(db, a.dir, BalanceTraceSegmentPath(fromBlock, toBlock), fromBlock, toBlock, opts.ETL)
		if err != nil {
			return nil, err
		}
		refs = append(refs, ref)
	}
	if opts.SectionBlooms {
		ref, err := BuildSectionBloomSegmentFromDBWithOptions(db, a.dir, SectionBloomSegmentPath(fromBlock, toBlock), fromBlock, toBlock, opts.ETL)
		if err != nil {
			return nil, err
		}
		refs = append(refs, ref)
	}
	if opts.EventLogs {
		eventRefs, err := buildEventLogPairFromChain(chain, a.dir, fromBlock, toBlock, EventLogBuildOptions{Version: opts.EventLogVersion, ETL: opts.ETL})
		if err != nil {
			return nil, err
		}
		refs = append(refs, eventRefs...)
	}
	sortSegments(refs)

	visibleStart, visibleEnd := uint64(0), uint64(0)
	if old, err := LoadProductionManifest(a.dir); err == nil {
		visibleStart = old.VisibleTxStart
		visibleEnd = old.VisibleTxEnd
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	manifest, err := a.Integrate(visibleStart, visibleEnd, refs)
	if err != nil {
		return nil, err
	}
	if opts.EventLogs {
		if err := writeEventLogBuildStage(db, manifest); err != nil {
			return nil, err
		}
	}
	return &AggregatorBuildResult{Manifest: manifest, Segments: append([]SegmentRef(nil), refs...)}, nil
}

func writeEventLogBuildStage(db any, manifest *Manifest) error {
	writer, ok := db.(ethdb.KeyValueWriter)
	if !ok {
		return nil
	}
	row, ok, err := eventLogBuildStageProgress(db, manifest)
	if err != nil || !ok {
		return err
	}
	return rawdb.WriteStageProgressRows(writer, []rawdb.StageProgress{row})
}

func eventLogBuildStageProgress(db any, manifest *Manifest) (rawdb.StageProgress, bool, error) {
	if _, ok := db.(ethdb.KeyValueWriter); !ok {
		return rawdb.StageProgress{}, false, nil
	}
	block, ok := eventLogBuildBlockFromManifest(manifest)
	if !ok {
		return rawdb.StageProgress{}, false, nil
	}
	reader, ok := db.(ethdb.KeyValueReader)
	if !ok {
		return rawdb.StageProgress{}, false, fmt.Errorf("snapshots: %s stage block %d requires readable database", rawdb.StageSnapshotEventLogBuild, block)
	}
	hash, err := requireSnapshotStageBoundaryHash(reader, rawdb.StageSnapshotEventLogBuild, block)
	if err != nil {
		return rawdb.StageProgress{}, false, err
	}
	return rawdb.StageProgress{
		Stage:        rawdb.StageSnapshotEventLogBuild,
		BlockNum:     block,
		BlockHash:    hash,
		HasBlockHash: true,
	}, true, nil
}

func (a *Aggregator) Integrate(visibleStart, visibleEnd uint64, refs []SegmentRef) (*Manifest, error) {
	if a == nil || a.dir == "" {
		return nil, errors.New("snapshots: nil aggregator or empty directory")
	}
	var old *Manifest
	if manifest, err := LoadProductionManifest(a.dir); err == nil {
		old = manifest
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	return a.integrateWithManifest(visibleStart, visibleEnd, refs, old)
}

func (a *Aggregator) integrateCommitmentBranchBase(visibleStart, visibleEnd uint64, refs []SegmentRef) (*Manifest, error) {
	var old *Manifest
	if manifest, err := LoadProductionManifest(a.dir); err == nil {
		old = manifest
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	return a.integrateWithManifestMode(visibleStart, visibleEnd, refs, old, true)
}

// integrateWithManifest publishes refs on top of an already authenticated
// production manifest. The cold lifecycle loads the manifest once per pass and
// reuses it for visible-history, section-bloom, and integration decisions;
// generic callers should continue to use Integrate so they observe the latest
// on-disk generation themselves.
func (a *Aggregator) integrateWithManifest(visibleStart, visibleEnd uint64, refs []SegmentRef, old *Manifest) (*Manifest, error) {
	return a.integrateWithManifestMode(visibleStart, visibleEnd, refs, old, false)
}

func (a *Aggregator) integrateWithManifestMode(visibleStart, visibleEnd uint64, refs []SegmentRef, old *Manifest, preserveGeneralLatestProgress bool) (*Manifest, error) {
	if a == nil || a.dir == "" {
		return nil, errors.New("snapshots: nil aggregator or empty directory")
	}
	if visibleEnd < visibleStart {
		return nil, fmt.Errorf("snapshots: aggregate range [%d,%d] is inverted", visibleStart, visibleEnd)
	}
	for _, ref := range refs {
		if err := validateSegment(ref, visibleStart, visibleEnd); err != nil {
			return nil, err
		}
	}
	segments := append([]SegmentRef(nil), refs...)
	generation := uint64(1)
	var retired []SegmentRef
	var progress *Progress
	var chain *ChainIdentity
	if old != nil {
		visibleStart = min(visibleStart, old.VisibleTxStart)
		visibleEnd = max(visibleEnd, old.VisibleTxEnd)
		generation = old.Generation + 1
		progress = cloneProgress(old.Progress)
		chain = cloneChainIdentity(old.Chain)
		retired = append(retired, old.Retired...)
		for _, ref := range old.Segments {
			if segmentOverlapsAnyFamily(ref, refs) {
				retired = append(retired, ref)
			} else {
				segments = append(segments, ref)
			}
		}
	}
	manifest := NewManifest(visibleStart, visibleEnd, segments)
	manifest.Generation = generation
	manifest.Chain = chain
	manifest.Progress = mergeProgress(progress, progressFromRefs(refs, visibleEnd))
	if preserveGeneralLatestProgress {
		// Commitment root/branch refs are current at visibleEnd, but the account,
		// KV, generation and code baselines may still be older. Keep the two
		// cross-family progress cursors at their prior values. CommitmentFlush is
		// also a shared root/checkpoint/branch restore boundary, so the branch-only
		// build must not advance it past the older checkpoint snapshot.
		var latestBuildTxNum, accessorBuildTxNum, commitmentFlushTxNum uint64
		if old != nil && old.Progress != nil {
			latestBuildTxNum = old.Progress.LatestBuildTxNum
			accessorBuildTxNum = old.Progress.AccessorBuildTxNum
			commitmentFlushTxNum = old.Progress.CommitmentFlushTxNum
		}
		if manifest.Progress == nil {
			manifest.Progress = new(Progress)
		}
		manifest.Progress.LatestBuildTxNum = latestBuildTxNum
		manifest.Progress.AccessorBuildTxNum = accessorBuildTxNum
		manifest.Progress.CommitmentFlushTxNum = commitmentFlushTxNum
	}
	manifest.Retired = dedupeSegmentRefs(retired)
	if err := PublishManifest(a.dir, manifest); err != nil {
		return nil, err
	}
	return manifest, nil
}

func UpdateHotPruneProgress(dir string, blockNum, txNum uint64) error {
	if dir == "" {
		return nil
	}
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	progress := cloneProgress(manifest.Progress)
	if progress == nil {
		progress = new(Progress)
	}
	progress.HotPruneTxNum = max(progress.HotPruneTxNum, txNum)
	progress.HotPruneBlockNum = max(progress.HotPruneBlockNum, blockNum)
	manifest.Progress = progress
	return PublishManifest(dir, manifest)
}

// UpdateStateChangeIndexPruneProgress records the inclusive authoritative hot
// history boundary covered by the latest full stale-posting sweep. Keeping the
// cursor in the snapshot manifest makes the maintenance restart-safe without
// turning a rebuildable accelerator into a canonical execution stage.
func UpdateStateChangeIndexPruneProgress(dir string, blockNum uint64) error {
	if dir == "" {
		return nil
	}
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	progress := cloneProgress(manifest.Progress)
	if progress == nil {
		progress = new(Progress)
	}
	progress.StateChangeIndexPruneBlockNum = max(progress.StateChangeIndexPruneBlockNum, blockNum)
	manifest.Progress = progress
	return PublishManifest(dir, manifest)
}

func WriteManifestProgressStages(db ethdb.KeyValueWriter, progress *Progress) error {
	if db == nil {
		return nil
	}
	return writeManifestProgressStages(newRawDBStageProgressStore(db), progress)
}

func WriteSnapshotInstallProgress(db ethdb.KeyValueWriter, manifest *Manifest) (*Progress, error) {
	if db == nil || manifest == nil {
		return nil, nil
	}
	progress := progressForInstalledManifest(manifest)
	rows, err := snapshotInstallProgressRows(db, progress, manifest)
	if err != nil {
		return nil, err
	}
	if err := rawdb.WriteStageProgressRows(db, rows); err != nil {
		return nil, err
	}
	return progress, nil
}

func snapshotInstallProgressRows(db ethdb.KeyValueWriter, progress *Progress, manifest *Manifest) ([]rawdb.StageProgress, error) {
	rows := manifestProgressStageRows(progress)
	rows = append(rows, rawdb.StageProgress{
		Stage:    rawdb.StageSnapshotInstall,
		BlockNum: manifest.VisibleTxEnd,
	})
	eventRow, ok, err := eventLogBuildStageProgress(db, manifest)
	if err != nil {
		return nil, err
	}
	if ok {
		rows = append(rows, eventRow)
	}
	return rows, nil
}

func manifestProgressStageRows(progress *Progress) []rawdb.StageProgress {
	if progress == nil {
		return nil
	}
	stages := []rawdb.StageProgress{
		{Stage: rawdb.StageSnapshotLatest, BlockNum: progress.LatestBuildTxNum},
		{Stage: rawdb.StageSnapshotHistory, BlockNum: progress.HistoryBuildTxNum},
		{Stage: rawdb.StageSnapshotAccessor, BlockNum: progress.AccessorBuildTxNum},
		{Stage: rawdb.StageSnapshotCommitmentFlush, BlockNum: progress.CommitmentFlushTxNum},
		{Stage: rawdb.StageSnapshotHotPrune, BlockNum: progress.HotPruneTxNum},
	}
	rows := make([]rawdb.StageProgress, 0, len(stages))
	for _, row := range stages {
		if row.BlockNum == 0 {
			continue
		}
		rows = append(rows, row)
	}
	return rows
}

func writeManifestProgressStages(store stageProgressStore, progress *Progress) error {
	if store == nil {
		return nil
	}
	for _, row := range manifestProgressStageRows(progress) {
		if err := store.Write(row.Stage, row.BlockNum); err != nil {
			return err
		}
	}
	return nil
}

func eventLogBuildBlockFromManifest(manifest *Manifest) (uint64, bool) {
	refs := eventLogRefs(manifest)
	if len(refs) == 0 {
		return 0, false
	}
	block, ok := eventLogCoverageBlockFromRefs(refs, 1)
	if !ok {
		return 0, false
	}
	indexedBlock, indexed := eventLogIndexedCoverageBlockFromRefs(eventLogIndexRefs(manifest), 1, block)
	return indexedBlock, indexed
}

func eventLogCoverageBlockFromRefs(refs []SegmentRef, fromBlock uint64) (uint64, bool) {
	sortSegmentRefsAscending(refs)
	next := fromBlock
	for _, ref := range refs {
		if ref.ToTxNum < next {
			continue
		}
		if ref.FromTxNum > next {
			break
		}
		if ref.ToTxNum == ^uint64(0) {
			return ref.ToTxNum, true
		}
		next = ref.ToTxNum + 1
	}
	if next == fromBlock {
		return 0, false
	}
	return next - 1, true
}

func eventLogIndexedCoverageBlockFromRefs(indexRefs []SegmentRef, fromBlock, maxBlock uint64) (uint64, bool) {
	if maxBlock < fromBlock {
		return 0, false
	}
	sortSegmentRefsAscending(indexRefs)
	next := fromBlock
	for _, ref := range indexRefs {
		if ref.ToTxNum < next {
			continue
		}
		if ref.FromTxNum > next {
			break
		}
		if ref.ToTxNum >= maxBlock {
			return maxBlock, true
		}
		if ref.ToTxNum == ^uint64(0) {
			return 0, false
		}
		next = ref.ToTxNum + 1
	}
	if next == fromBlock {
		return 0, false
	}
	return next - 1, true
}

func progressForInstalledManifest(manifest *Manifest) *Progress {
	if manifest == nil {
		return nil
	}
	return mergeProgress(cloneProgress(manifest.Progress), progressFromRefs(manifest.Segments, manifest.VisibleTxEnd))
}

func progressFromRefs(refs []SegmentRef, _ uint64) *Progress {
	if len(refs) == 0 {
		return nil
	}
	var p Progress
	registry := DefaultDomainRegistry()
	for _, ref := range refs {
		cfg, ok := registry.ConfigForRef(ref)
		switch ref.Kind {
		case SegmentLatest:
			p.LatestBuildTxNum = max(p.LatestBuildTxNum, ref.ToTxNum)
			if ok && cfg.TracksCommitmentFlush {
				p.CommitmentFlushTxNum = max(p.CommitmentFlushTxNum, ref.ToTxNum)
			}
		case SegmentAccessor, SegmentBTree:
			p.AccessorBuildTxNum = max(p.AccessorBuildTxNum, ref.ToTxNum)
		case SegmentHistory:
			if ok && cfg.HasHistory {
				p.HistoryBuildTxNum = max(p.HistoryBuildTxNum, ref.ToTxNum)
			}
		}
	}
	if p == (Progress{}) {
		return nil
	}
	return &p
}

func mergeProgress(base, update *Progress) *Progress {
	if base == nil {
		return cloneProgress(update)
	}
	out := *base
	if update == nil {
		return &out
	}
	out.LatestBuildTxNum = max(out.LatestBuildTxNum, update.LatestBuildTxNum)
	out.HistoryBuildTxNum = max(out.HistoryBuildTxNum, update.HistoryBuildTxNum)
	out.AccessorBuildTxNum = max(out.AccessorBuildTxNum, update.AccessorBuildTxNum)
	out.CommitmentFlushTxNum = max(out.CommitmentFlushTxNum, update.CommitmentFlushTxNum)
	out.HotPruneTxNum = max(out.HotPruneTxNum, update.HotPruneTxNum)
	out.HotPruneBlockNum = max(out.HotPruneBlockNum, update.HotPruneBlockNum)
	out.StateChangeIndexPruneBlockNum = max(out.StateChangeIndexPruneBlockNum, update.StateChangeIndexPruneBlockNum)
	return &out
}

func cloneProgress(progress *Progress) *Progress {
	if progress == nil {
		return nil
	}
	out := *progress
	return &out
}

func aggregationKVDomains(db ethdb.Iteratee, configured []kvdomains.KVDomain) ([]kvdomains.KVDomain, error) {
	return aggregationKVDomainsContext(context.Background(), db, configured)
}

func aggregationKVDomainsContext(ctx context.Context, db ethdb.Iteratee, configured []kvdomains.KVDomain) ([]kvdomains.KVDomain, error) {
	if len(configured) != 0 {
		return normalizeAggregationKVDomains(configured)
	}
	seen := make(map[kvdomains.KVDomain]struct{})
	if err := rawdb.IterateStateKVLatestRowsContext(ctx, db, func(row rawdb.StateKVLatestRow) (bool, error) {
		seen[row.Domain] = struct{}{}
		return true, nil
	}); err != nil {
		return nil, err
	}
	out := make([]kvdomains.KVDomain, 0, len(seen))
	for domain := range seen {
		out = append(out, domain)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

func normalizeAggregationKVDomains(domains []kvdomains.KVDomain) ([]kvdomains.KVDomain, error) {
	seen := make(map[kvdomains.KVDomain]struct{}, len(domains))
	out := make([]kvdomains.KVDomain, 0, len(domains))
	for _, domain := range domains {
		if !kvdomains.IsRegistered(domain) {
			return nil, fmt.Errorf("snapshots: unregistered kv latest domain %#04x", uint16(domain))
		}
		if _, ok := seen[domain]; ok {
			continue
		}
		seen[domain] = struct{}{}
		out = append(out, domain)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

func aggregatePath(base string, opts AggregatorBuildOptions) string {
	return fmt.Sprintf("%s-%d-%d.json", base, opts.FromTxNum, opts.ToTxNum)
}

func aggregateLatestPath(base string, opts AggregatorBuildOptions, ext string) string {
	if ext == "" {
		ext = ".seg"
	}
	return fmt.Sprintf("%s-%d-%d%s", base, opts.FromTxNum, opts.ToTxNum, ext)
}

func segmentOverlapsAnyFamily(ref SegmentRef, refs []SegmentRef) bool {
	for _, candidate := range refs {
		if ref.normalizedDataset() != candidate.normalizedDataset() || ref.Domain != candidate.Domain {
			continue
		}
		if ref.Kind != candidate.Kind &&
			!(IsLatestAccessorRef(ref) && (candidate.Kind == SegmentLatest || candidate.Kind == SegmentBTree)) &&
			!(IsLatestAccessorRef(candidate) && (ref.Kind == SegmentLatest || ref.Kind == SegmentBTree)) {
			continue
		}
		if ref.FromTxNum <= candidate.ToTxNum && candidate.FromTxNum <= ref.ToTxNum {
			return true
		}
	}
	return false
}

func dedupeSegmentRefs(refs []SegmentRef) []SegmentRef {
	if len(refs) == 0 {
		return nil
	}
	seen := make(map[segmentRefKey]struct{}, len(refs))
	out := make([]SegmentRef, 0, len(refs))
	for _, ref := range refs {
		key := segmentRefKey{
			dataset:  ref.normalizedDataset(),
			domain:   ref.Domain,
			kind:     ref.Kind,
			from:     ref.FromTxNum,
			to:       ref.ToTxNum,
			path:     ref.Path,
			size:     ref.Size,
			checksum: ref.Checksum,
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, ref)
	}
	sortSegments(out)
	return out
}

type segmentRefKey struct {
	dataset  SegmentDataset
	domain   kvdomains.KVDomain
	kind     SegmentKind
	from     uint64
	to       uint64
	path     string
	size     uint64
	checksum string
}
