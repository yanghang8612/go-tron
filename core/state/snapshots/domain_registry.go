package snapshots

import (
	"fmt"
	"sort"
	"sync"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// DomainCfg describes one registered physical snapshot domain. It is the
// central place for latest/history file-family constraints so adding a domain
// does not require updating every manifest/checker switch.
type DomainCfg struct {
	Name                              string
	Dataset                           SegmentDataset
	DomainSpecific                    bool
	LatestPathStem                    string
	LatestPathExt                     string // default ".seg"; CommitmentBranch will use ".json"
	HistoryPathStem                   string
	HasLatest                         bool
	HasLatestAccessor                 bool
	HasLatestBTree                    bool
	HasHistory                        bool
	HasHistoryInvertedIndex           bool
	HasHistoryAccessor                bool
	TracksCommitmentFlush             bool
	BuildLatest                       LatestSnapshotBuilder
	ReadHotAccountLatest              HotAccountLatestReader
	IterateHotAccountLatest           HotAccountLatestIterator
	ReadHotKVLatest                   HotKVLatestReader
	IterateHotKVLatestRows            HotKVLatestRowsIterator
	ReadHotKVGeneration               HotKVGenerationReader
	IterateHotKVGeneration            HotKVGenerationIterator
	ReadHotCode                       HotCodeReader
	IterateHotCode                    HotCodeIterator
	DeleteHotCode                     HotCodeDeleter
	IterateHotCommitmentDomain        HotCommitmentDomainIterator
	WriteHotCommitmentCheckpoint      HotCommitmentCheckpointWriter
	ReadHotLatestCommitmentCheckpoint HotLatestCommitmentCheckpointReader
	IterateHotCommitmentCheckpoints   HotCommitmentCheckpointIterator
	DeleteHotCommitmentCheckpoint     HotCommitmentCheckpointDeleter
	BuildHistory                      HistorySnapshotBuilder
	OpenHistory                       HistorySnapshotOpener
	WriteHistory                      HistorySnapshotWriter
	CompactHistory                    HistoryCompactor
	ReadHistoryRange                  HistoryRangeReader
	ReadHistoryByKey                  HistoryKeyReader
	IterateHistoryRange               HistoryRangeIterator
	IterateHistoryByKey               HistoryKeyIterator
	WriteHotHistoryRow                HotHistoryWriter
	WriteHotHistoryIndex              HotHistoryWriter
	WriteHotHistoryTxRange            HotHistoryTxRangeWriter
	ReadHotHistoryTxRange             HotHistoryTxRangeReader
	IterateHotHistoryTxRanges         HotHistoryTxRangeIterator
	DeleteHotHistoryTxRange           HotHistoryTxRangeDeleter
	DeleteHotHistoryBlock             HotHistoryBlockDeleter
	IterateHotHistoryTxRangeChanges   HotHistoryTxRangeChangeIterator
	IterateHotHistoryBlocks           HotHistoryBlockIterator
	IterateHotHistoryChanges          HotHistoryChangeIterator
	IterateHotHistoryPrefix           HotHistoryPrefixIterator
	ReadHotAccountLatestAsOf          HotAccountLatestAsOfReader
	ReadHotKVLatestAsOf               HotKVLatestAsOfReader
	ReadHotKVGenerationAsOf           HotKVGenerationAsOfReader
	ReadHotAccountKVAsOf              HotAccountKVAsOfReader
	IterateHotAccountKVPrefixAsOf     HotAccountKVPrefixAsOfIterator
	CheckLatest                       SnapshotRefChecker
	CheckLatestAccessor               SnapshotRefChecker
	CheckLatestBTree                  SnapshotRefChecker
	CheckHistory                      SnapshotRefChecker
	CheckHistoryIndex                 SnapshotRefChecker
	CheckHistoryAccessor              SnapshotRefChecker
	IsHistoryBinaryPath               func(path string) bool
	IsHistoryCompanionPath            func(path string) bool
	HistoryIndexPath                  func(segmentPath string) string
	HistoryAccessorPath               func(segmentPath string) string
}

type DomainRegistry struct {
	byDataset map[SegmentDataset]DomainCfg
	ordered   []DomainCfg
}

type LatestSnapshotBuilder func(db AggregatorDB, dir string, domain kvdomains.KVDomain, fromTxNum, toTxNum uint64, relPath string) ([]SegmentRef, error)

type HotAccountLatestReader func(db ethdb.KeyValueReader, owner common.Address) ([]byte, bool, error)

type HotAccountLatestIterator func(db ethdb.Iteratee, ownerPrefix []byte, fn func(rawdb.StateAccountLatestRow) (bool, error)) error

type HotKVLatestReader func(db ethdb.KeyValueReader, owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error)

type HotKVLatestRowsIterator func(db ethdb.Iteratee, fn func(rawdb.StateKVLatestRow) (bool, error)) error

type HotKVGenerationReader func(db ethdb.KeyValueReader, owner common.Address) (uint64, bool, error)

type HotKVGenerationIterator func(db ethdb.Iteratee, ownerPrefix []byte, fn func(rawdb.StateKVGenerationRow) (bool, error)) error

type HotCodeReader func(db ethdb.KeyValueReader, hash common.Hash) ([]byte, bool, error)

type HotCodeIterator func(db ethdb.Iteratee, fn func(rawdb.StateCodeRow) (bool, error)) error

type HotCodeDeleter func(db ethdb.KeyValueWriter, hash common.Hash) error

type HotCommitmentDomainIterator func(db ethdb.Iteratee, logicalPrefix []byte, fn func(logicalKey, value []byte) (bool, error)) error

type HotCommitmentCheckpointWriter func(db ethdb.KeyValueWriter, checkpoint *rawdb.StateCommitmentCheckpoint) error

type HotLatestCommitmentCheckpointReader func(db ethdb.KeyValueReader) (*rawdb.StateCommitmentCheckpoint, bool, error)

type HotCommitmentCheckpointIterator func(db ethdb.Iteratee, fn func(*rawdb.StateCommitmentCheckpoint) (bool, error)) error

type HotCommitmentCheckpointDeleter func(db ethdb.KeyValueWriter, blockNum uint64) error

type HistorySnapshotBuilder func(db AggregatorDB, dir string, fromTxNum, toTxNum uint64, relPath string) ([]SegmentRef, error)

type HistorySnapshotOpener func(dir string, ref SegmentRef) ([]*rawdb.StateDomainChange, error)

type HistorySnapshotWriter func(dir string, ref SegmentRef, changes []*rawdb.StateDomainChange) (SegmentRef, SegmentRef, SegmentRef, error)

type HistoryCompactor func(dir string, cfg DomainCfg, selection historyCompactionSelection) ([]SegmentRef, error)

type HistoryRangeReader func(dir string, manifest *Manifest, ref SegmentRef, fromTxNum, toTxNum uint64) ([]*rawdb.StateDomainChange, error)

type HistoryKeyReader func(dir string, manifest *Manifest, ref SegmentRef, lookupKey []byte, fromTxNum, toTxNum uint64) ([]*rawdb.StateDomainChange, error)

type HistoryRangeIterator func(dir string, manifest *Manifest, ref SegmentRef, fromTxNum, toTxNum uint64, fn func(*rawdb.StateDomainChange) (bool, error)) error

type HistoryKeyIterator func(dir string, manifest *Manifest, ref SegmentRef, lookupKey []byte, fromTxNum, toTxNum uint64, fn func(*rawdb.StateDomainChange) (bool, error)) error

type HotHistoryWriter func(db ethdb.KeyValueWriter, change *rawdb.StateDomainChange) error

type HotHistoryTxRangeWriter func(db ethdb.KeyValueWriter, blockNum uint64, blockHash common.Hash, beginTxNum, endTxNum uint64) error

type HotHistoryTxRangeIterator func(db ethdb.Iteratee, fn func(*rawdb.StateTxRange) (bool, error)) error

type HotHistoryTxRangeDeleter func(db ethdb.KeyValueWriter, blockNum uint64) error

type HotHistoryBlockDeleter func(db rawdb.StateKVLatestStore, blockNum uint64) error

type HotHistoryBlockIterator func(db ethdb.Iteratee, flatDomain rawdb.StateFlatDomain, owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte, fn func(blockNum uint64) (bool, error)) error

type HotHistoryChangeIterator func(db rawdb.StateKVHistoryReader, targetTxNum, headTxNum uint64, flatDomain rawdb.StateFlatDomain, owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte, fn func(*rawdb.StateDomainChange) (bool, error)) error

type HotHistoryPrefixIterator func(db rawdb.StateKVHistoryReader, targetTxNum, headTxNum uint64, owner common.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte, fn func(*rawdb.StateDomainChange) (bool, error)) error

type HotAccountLatestAsOfReader func(db rawdb.StateKVHistoryReader, owner common.Address, targetTxNum, headTxNum uint64) ([]byte, bool, error)

type HotKVLatestAsOfReader func(db rawdb.StateKVHistoryReader, owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte, targetTxNum, headTxNum uint64) ([]byte, bool, error)

type HotKVGenerationAsOfReader func(db rawdb.StateKVHistoryReader, owner common.Address, targetTxNum, headTxNum uint64) (uint64, bool, error)

type HotAccountKVAsOfReader func(db rawdb.StateKVHistoryReader, owner common.Address, domain kvdomains.KVDomain, key []byte, targetTxNum, headTxNum uint64) ([]byte, bool, error)

type HotAccountKVPrefixAsOfIterator func(db rawdb.StateKVHistoryReader, owner common.Address, domain kvdomains.KVDomain, prefix []byte, targetTxNum, headTxNum uint64, fn func(key, value []byte) (bool, error)) error

type SnapshotRefChecker func(dir string, ref SegmentRef) error

// DefaultDomainRegistry returns the process-wide snapshot-domain registry. The
// registry is immutable static configuration — read-only lookups, value-copied
// DomainCfg, freshly-built result slices — so one shared instance is safe to
// read concurrently. It is built once instead of being reconstructed (closures,
// map and slice) on every call; the old per-call rebuild showed up as a hot
// allocation site on the block-apply path.
func DefaultDomainRegistry() DomainRegistry {
	defaultDomainRegistryOnce.Do(func() {
		defaultDomainRegistry = buildDefaultDomainRegistry()
	})
	return defaultDomainRegistry
}

// Lazy package-level singleton. The reference to buildDefaultDomainRegistry
// lives in the function body above (not in a var initializer) to avoid a static
// initialization cycle: the builder transitively references DefaultDomainRegistry
// via the manifest validators.
var (
	defaultDomainRegistryOnce sync.Once
	defaultDomainRegistry     DomainRegistry
)

func buildDefaultDomainRegistry() DomainRegistry {
	cfgs := []DomainCfg{
		{
			Name:              "AccountsDomain",
			Dataset:           SegmentDatasetAccountLatest,
			LatestPathStem:    "latest/account-latest",
			HasLatest:         true,
			HasLatestAccessor: true,
			HasLatestBTree:    true,
			BuildLatest: func(db AggregatorDB, dir string, _ kvdomains.KVDomain, fromTxNum, toTxNum uint64, relPath string) ([]SegmentRef, error) {
				latest, accessor, btree, err := BuildAccountLatestSegmentFilesFromDB(db, dir, fromTxNum, toTxNum, relPath)
				if err != nil {
					return nil, err
				}
				return []SegmentRef{latest, accessor, btree}, nil
			},
			ReadHotAccountLatest:    rawdb.ReadStateAccountLatest,
			IterateHotAccountLatest: rawdb.IterateStateAccountLatest,
		},
		{
			Name:              "AccountKVDomain",
			Dataset:           SegmentDatasetKVLatest,
			DomainSpecific:    true,
			LatestPathStem:    "latest/kv-latest",
			HasLatest:         true,
			HasLatestAccessor: true,
			HasLatestBTree:    true,
			BuildLatest: func(db AggregatorDB, dir string, domain kvdomains.KVDomain, fromTxNum, toTxNum uint64, relPath string) ([]SegmentRef, error) {
				latest, accessor, btree, err := BuildLatestDomainSegmentFilesFromDB(db, dir, domain, fromTxNum, toTxNum, relPath)
				if err != nil {
					return nil, err
				}
				return []SegmentRef{latest, accessor, btree}, nil
			},
			ReadHotKVLatest:        rawdb.ReadStateKVLatest,
			IterateHotKVLatestRows: rawdb.IterateStateKVLatestRows,
		},
		{
			Name:              "KVGenerationDomain",
			Dataset:           SegmentDatasetKVGeneration,
			LatestPathStem:    "latest/kv-generation",
			HasLatest:         true,
			HasLatestAccessor: true,
			HasLatestBTree:    true,
			BuildLatest: func(db AggregatorDB, dir string, _ kvdomains.KVDomain, fromTxNum, toTxNum uint64, relPath string) ([]SegmentRef, error) {
				latest, accessor, btree, err := BuildKVGenerationSegmentFilesFromDB(db, dir, fromTxNum, toTxNum, relPath)
				if err != nil {
					return nil, err
				}
				return []SegmentRef{latest, accessor, btree}, nil
			},
			ReadHotKVGeneration:    rawdb.ReadStateKVGeneration,
			IterateHotKVGeneration: rawdb.IterateStateKVGeneration,
		},
		// CodeDomain is content-addressed: contract bytecode is a latest-only
		// snapshot family keyed by code hash. Historical CodeAt is served by
		// account-envelope history selecting the right hash plus content-addressed
		// retention of every referenced hash — there is deliberately no separate
		// temporal code changeset. This is the intended final retention policy
		// (erigon-gap #5 / #8 in
		// docs/superpowers/specs/2026-05-25-erigon-state-architecture-gap.md), not
		// a transitional stage. Snap-mode pruning may delete a hot state-code row
		// ONLY once it is backed by a CodeDomain snapshot (the
		// codeHashAvailableInSnapshot gate in core/state/pruning) — coverage is the
		// sole deletion path, locked by TestWorkerSnapPreservesHotCodeWithout-
		// CodeDomainCoverage. Add a distinct temporal CodeDomain only if a
		// java-tron parity fixture proves hash-bound retention is insufficient.
		{
			Name:              "CodeDomain",
			Dataset:           SegmentDatasetCode,
			LatestPathStem:    "latest/code",
			HasLatest:         true,
			HasLatestAccessor: true,
			HasLatestBTree:    true,
			BuildLatest: func(db AggregatorDB, dir string, _ kvdomains.KVDomain, fromTxNum, toTxNum uint64, relPath string) ([]SegmentRef, error) {
				latest, accessor, btree, err := BuildCodeSegmentFilesFromDB(db, dir, fromTxNum, toTxNum, relPath)
				if err != nil {
					return nil, err
				}
				return []SegmentRef{latest, accessor, btree}, nil
			},
			ReadHotCode:    readHotStateCode,
			IterateHotCode: rawdb.IterateStateCode,
			DeleteHotCode:  rawdb.DeleteStateCode,
		},
		{
			Name:                  "CommitmentRoot",
			Dataset:               SegmentDatasetCommitmentRoot,
			LatestPathStem:        "commitment/root",
			HasLatest:             true,
			HasLatestAccessor:     true,
			HasLatestBTree:        true,
			TracksCommitmentFlush: true,
			BuildLatest: func(db AggregatorDB, dir string, _ kvdomains.KVDomain, fromTxNum, toTxNum uint64, relPath string) ([]SegmentRef, error) {
				latest, accessor, btree, err := BuildCommitmentRootSegmentFilesFromDB(db, dir, fromTxNum, toTxNum, relPath)
				if err != nil {
					return nil, err
				}
				return []SegmentRef{latest, accessor, btree}, nil
			},
		},
		{
			Name:                  "CommitmentCheckpoint",
			Dataset:               SegmentDatasetCommitmentCheckpoint,
			LatestPathStem:        "commitment/checkpoints",
			HasLatest:             true,
			HasLatestAccessor:     true,
			HasLatestBTree:        true,
			TracksCommitmentFlush: true,
			BuildLatest: func(db AggregatorDB, dir string, _ kvdomains.KVDomain, fromTxNum, toTxNum uint64, relPath string) ([]SegmentRef, error) {
				latest, accessor, btree, err := BuildCommitmentCheckpointSegmentFilesFromDB(db, dir, fromTxNum, toTxNum, relPath)
				if err != nil {
					return nil, err
				}
				return []SegmentRef{latest, accessor, btree}, nil
			},
			WriteHotCommitmentCheckpoint:      rawdb.WriteStateCommitmentCheckpoint,
			ReadHotLatestCommitmentCheckpoint: rawdb.ReadLatestStateCommitmentCheckpoint,
			IterateHotCommitmentCheckpoints:   rawdb.IterateStateCommitmentCheckpoints,
			DeleteHotCommitmentCheckpoint:     rawdb.DeleteStateCommitmentCheckpoint,
		},
		{
			Name:                  "CommitmentBranch",
			Dataset:               SegmentDatasetCommitmentBranch,
			LatestPathStem:        "commitment/branch",
			LatestPathExt:         ".json",
			HasLatest:             true,
			HasLatestAccessor:     false,
			HasLatestBTree:        false,
			TracksCommitmentFlush: true,
			BuildLatest:           buildCommitmentBranchLatest,
			CheckLatest:           checkCommitmentBranchSegment,
		},
		{
			Name:                    "HistoryDomain",
			Dataset:                 SegmentDatasetStateDomainChange,
			HistoryPathStem:         "history/state-domain-change",
			HasHistory:              true,
			HasHistoryInvertedIndex: true,
			HasHistoryAccessor:      true,
			BuildHistory: func(db AggregatorDB, dir string, fromTxNum, toTxNum uint64, relPath string) ([]SegmentRef, error) {
				return BuildStateDomainChangeHistorySegmentsFromDB(db, dir, fromTxNum, toTxNum, relPath)
			},
			IsHistoryBinaryPath:             isStateDomainChangeBinarySegmentPath,
			IsHistoryCompanionPath:          isStateDomainChangeBinaryCompanionPath,
			HistoryIndexPath:                stateDomainChangeBinaryIndexPath,
			HistoryAccessorPath:             stateDomainChangeBinaryAccessorPath,
			OpenHistory:                     openStateDomainChangeHistoryChanges,
			WriteHistory:                    writeHistorySegmentFiles,
			CompactHistory:                  compactStateDomainChangeBinaryHistoryRun,
			ReadHistoryRange:                readStateDomainChangeHistoryRange,
			ReadHistoryByKey:                readStateDomainChangeHistoryByKey,
			IterateHistoryRange:             iterateStateDomainChangeHistoryRange,
			IterateHistoryByKey:             iterateStateDomainChangeHistoryByKey,
			WriteHotHistoryRow:              rawdb.WriteStateDomainChangeRow,
			WriteHotHistoryIndex:            rawdb.WriteStateDomainChangeInverseIndex,
			WriteHotHistoryTxRange:          rawdb.WriteStateTxRange,
			ReadHotHistoryTxRange:           rawdb.ReadStateTxRange,
			IterateHotHistoryTxRanges:       rawdb.IterateStateTxRanges,
			DeleteHotHistoryTxRange:         rawdb.DeleteStateTxRange,
			DeleteHotHistoryBlock:           rawdb.DeleteStateDomainChanges,
			IterateHotHistoryTxRangeChanges: rawdb.IterateStateDomainChangesByTxRange,
			IterateHotHistoryBlocks:         rawdb.IterateStateDomainChangeBlocksByKey,
			IterateHotHistoryChanges:        rawdb.IterateStateDomainChangesByKey,
			IterateHotHistoryPrefix:         rawdb.IterateStateDomainChangesByPrefix,
			ReadHotAccountLatestAsOf:        rawdb.ReadStateAccountLatestAsOfTxNum,
			ReadHotKVLatestAsOf:             rawdb.ReadStateKVAsOfTxNum,
			ReadHotKVGenerationAsOf:         rawdb.ReadStateKVGenerationAsOfTxNum,
			ReadHotAccountKVAsOf:            rawdb.ReadStateAccountKVAsOfTxNum,
			IterateHotAccountKVPrefixAsOf:   rawdb.IterateStateAccountKVAsOfPrefixTxNum,
			CheckHistory:                    CheckStateDomainChangeSegment,
			CheckHistoryIndex:               CheckStateDomainChangeIndexSegment,
			CheckHistoryAccessor:            CheckStateDomainChangeAccessorSegment,
		},
	}
	reg := DomainRegistry{byDataset: make(map[SegmentDataset]DomainCfg, len(cfgs))}
	for _, cfg := range cfgs {
		if cfg.HasLatest && cfg.CheckLatest == nil {
			cfg.CheckLatest = checkLatestSegmentRef
		}
		if cfg.HasLatestAccessor && cfg.CheckLatestAccessor == nil {
			cfg.CheckLatestAccessor = CheckLatestAccessorSegment
		}
		if cfg.HasLatestBTree && cfg.CheckLatestBTree == nil {
			cfg.CheckLatestBTree = CheckLatestBTreeSegment
		}
		reg.byDataset[cfg.Dataset] = cfg
		reg.ordered = append(reg.ordered, cfg)
	}
	return reg
}

func readHotStateCode(db ethdb.KeyValueReader, hash common.Hash) ([]byte, bool, error) {
	if hash == (common.Hash{}) {
		return nil, false, nil
	}
	code := rawdb.ReadStateCode(db, hash)
	if len(code) == 0 {
		return nil, false, nil
	}
	return code, true, nil
}

func (r DomainRegistry) Dataset(dataset SegmentDataset) (DomainCfg, bool) {
	if r.byDataset == nil {
		r = DefaultDomainRegistry()
	}
	cfg, ok := r.byDataset[dataset]
	return cfg, ok
}

func (r DomainRegistry) LatestConfigs() []DomainCfg {
	if r.byDataset == nil {
		r = DefaultDomainRegistry()
	}
	var out []DomainCfg
	for _, cfg := range r.ordered {
		if cfg.HasLatest {
			out = append(out, cfg)
		}
	}
	return out
}

func (r DomainRegistry) HistoryConfigs() []DomainCfg {
	if r.byDataset == nil {
		r = DefaultDomainRegistry()
	}
	var out []DomainCfg
	for _, cfg := range r.ordered {
		if cfg.HasHistory {
			out = append(out, cfg)
		}
	}
	return out
}

func (r DomainRegistry) ConfigForRef(ref SegmentRef) (DomainCfg, bool) {
	return r.Dataset(ref.NormalizedDataset())
}

func (cfg DomainCfg) AllowsKind(kind SegmentKind) bool {
	switch kind {
	case SegmentLatest:
		return cfg.HasLatest
	case SegmentAccessor:
		return cfg.HasLatestAccessor || cfg.HasHistoryAccessor
	case SegmentBTree:
		return cfg.HasLatestBTree
	case SegmentHistory:
		return cfg.HasHistory
	case SegmentInverted:
		return cfg.HasHistoryInvertedIndex
	default:
		return false
	}
}

func (cfg DomainCfg) ValidateRef(seg SegmentRef) error {
	if cfg.DomainSpecific {
		if !kvdomains.IsRegistered(seg.Domain) {
			return fmt.Errorf("snapshots: unregistered %s domain %#04x", cfg.Dataset, uint16(seg.Domain))
		}
		return nil
	}
	if seg.Domain != 0 {
		return fmt.Errorf("snapshots: %s segment %q must not set kv domain %#04x", cfg.Dataset, seg.Path, uint16(seg.Domain))
	}
	return nil
}

func (cfg DomainCfg) LatestPathBase(domain kvdomains.KVDomain) string {
	if cfg.DomainSpecific {
		return fmt.Sprintf("%s-%04x", cfg.LatestPathStem, uint16(domain))
	}
	return cfg.LatestPathStem
}

func (cfg DomainCfg) latestPathExt() string {
	if cfg.LatestPathExt == "" {
		return ".seg"
	}
	return cfg.LatestPathExt
}

func (cfg DomainCfg) HistoryPath(fromTxNum, toTxNum uint64) string {
	return fmt.Sprintf("%s-%d-%d.seg", cfg.HistoryPathStem, fromTxNum, toTxNum)
}

func (cfg DomainCfg) IsHistoryBinarySegmentPath(path string) bool {
	return cfg.IsHistoryBinaryPath != nil && cfg.IsHistoryBinaryPath(path)
}

func (cfg DomainCfg) IsHistoryBinaryCompanionPath(path string) bool {
	return cfg.IsHistoryCompanionPath != nil && cfg.IsHistoryCompanionPath(path)
}

func (cfg DomainCfg) HistoryIndexPathFor(segmentPath string) string {
	if cfg.HistoryIndexPath == nil {
		return ""
	}
	return cfg.HistoryIndexPath(segmentPath)
}

func (cfg DomainCfg) HistoryAccessorPathFor(segmentPath string) string {
	if cfg.HistoryAccessorPath == nil {
		return ""
	}
	return cfg.HistoryAccessorPath(segmentPath)
}

func (cfg DomainCfg) HistoryIndexRef(manifest *Manifest, historyRef SegmentRef) (SegmentRef, bool) {
	return cfg.historyCompanionRef(manifest, historyRef, SegmentInverted, cfg.HistoryIndexPathFor(historyRef.Path))
}

func (cfg DomainCfg) HistoryAccessorRef(manifest *Manifest, historyRef SegmentRef) (SegmentRef, bool) {
	return cfg.historyCompanionRef(manifest, historyRef, SegmentAccessor, cfg.HistoryAccessorPathFor(historyRef.Path))
}

func (cfg DomainCfg) historyCompanionRef(manifest *Manifest, historyRef SegmentRef, kind SegmentKind, wantPath string) (SegmentRef, bool) {
	if manifest == nil || wantPath == "" {
		return SegmentRef{}, false
	}
	for _, ref := range manifest.Segments {
		if ref.normalizedDataset() == cfg.Dataset &&
			ref.Kind == kind &&
			ref.FromTxNum == historyRef.FromTxNum &&
			ref.ToTxNum == historyRef.ToTxNum &&
			ref.Path == wantPath {
			return ref, true
		}
	}
	return SegmentRef{}, false
}

type TxRange struct {
	From uint64
	To   uint64
}

func HistoryTxRanges(manifest *Manifest, dataset SegmentDataset) []TxRange {
	if manifest == nil {
		return nil
	}
	cfg, ok := DefaultDomainRegistry().Dataset(dataset)
	if !ok || !cfg.HasHistory {
		return nil
	}
	out := make([]TxRange, 0)
	for _, ref := range manifest.Segments {
		if ref.normalizedDataset() != cfg.Dataset || ref.Kind != SegmentHistory {
			continue
		}
		out = append(out, TxRange{From: ref.FromTxNum, To: ref.ToTxNum})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].From == out[j].From {
			return out[i].To < out[j].To
		}
		return out[i].From < out[j].From
	})
	return out
}

func ContiguousHistoryVisibleTxEnd(manifest *Manifest, dataset SegmentDataset, startTxNum uint64) uint64 {
	next := startTxNum
	visibleEnd := uint64(0)
	if startTxNum == 0 {
		visibleEnd = 0
	}
	for _, r := range HistoryTxRanges(manifest, dataset) {
		if r.To < next {
			continue
		}
		if r.From > next {
			break
		}
		visibleEnd = r.To
		if r.To == ^uint64(0) {
			break
		}
		next = r.To + 1
	}
	return visibleEnd
}

func IsLatestAccessorRef(ref SegmentRef) bool {
	cfg, ok := DefaultDomainRegistry().ConfigForRef(ref)
	return ok && ref.Kind == SegmentAccessor && cfg.HasLatestAccessor
}

func IsLatestBTreeRef(ref SegmentRef) bool {
	cfg, ok := DefaultDomainRegistry().ConfigForRef(ref)
	return ok && ref.Kind == SegmentBTree && cfg.HasLatestBTree
}

func CheckRegisteredSegment(dir string, ref SegmentRef) (bool, error) {
	cfg, ok := DefaultDomainRegistry().ConfigForRef(ref)
	if !ok {
		return false, nil
	}
	switch ref.Kind {
	case SegmentLatest:
		if cfg.CheckLatest != nil {
			return true, cfg.CheckLatest(dir, ref)
		}
	case SegmentAccessor:
		if cfg.HasHistoryAccessor && cfg.CheckHistoryAccessor != nil {
			return true, cfg.CheckHistoryAccessor(dir, ref)
		}
		if cfg.HasLatestAccessor && cfg.CheckLatestAccessor != nil {
			return true, cfg.CheckLatestAccessor(dir, ref)
		}
	case SegmentBTree:
		if cfg.CheckLatestBTree != nil {
			return true, cfg.CheckLatestBTree(dir, ref)
		}
	case SegmentHistory:
		if cfg.CheckHistory != nil {
			return true, cfg.CheckHistory(dir, ref)
		}
	case SegmentInverted:
		if cfg.CheckHistoryIndex != nil {
			return true, cfg.CheckHistoryIndex(dir, ref)
		}
	}
	return false, nil
}

func checkLatestSegmentRef(dir string, ref SegmentRef) error {
	return CheckLatestSegment(dir, ref)
}
