package snapshots

import (
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
	if a == nil || a.dir == "" {
		return nil, errors.New("snapshots: nil aggregator or empty directory")
	}
	if db == nil {
		return nil, errors.New("snapshots: nil database")
	}
	if opts.ToTxNum < opts.FromTxNum {
		return nil, fmt.Errorf("snapshots: aggregate range [%d,%d] is inverted", opts.FromTxNum, opts.ToTxNum)
	}
	domains, err := aggregationKVDomains(db, opts.KVDomains)
	if err != nil {
		return nil, err
	}
	kvLatestDomains := make(map[SegmentDataset][]kvdomains.KVDomain)
	kvLatestDomains[SegmentDatasetKVLatest] = domains

	var refs []SegmentRef
	registry := DefaultDomainRegistry()
	for _, cfg := range registry.LatestConfigs() {
		if cfg.BuildLatest == nil {
			return nil, fmt.Errorf("snapshots: latest domain %s has no builder", cfg.Dataset)
		}
		if cfg.DomainSpecific {
			for _, domain := range kvLatestDomains[cfg.Dataset] {
				built, err := cfg.BuildLatest(db, a.dir, domain, opts.FromTxNum, opts.ToTxNum, aggregateLatestPath(cfg.LatestPathBase(domain), opts, cfg.latestPathExt()))
				if err != nil {
					return nil, err
				}
				refs = append(refs, built...)
			}
			continue
		}
		built, err := cfg.BuildLatest(db, a.dir, 0, opts.FromTxNum, opts.ToTxNum, aggregateLatestPath(cfg.LatestPathBase(0), opts, cfg.latestPathExt()))
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
	refs, err := a.buildLatestSegments(db, opts)
	if err != nil {
		return nil, err
	}
	if len(refs) == 0 {
		return &AggregatorBuildResult{}, nil
	}
	sortSegments(refs)
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

func (a *Aggregator) Integrate(visibleStart, visibleEnd uint64, refs []SegmentRef) (*Manifest, error) {
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
	if old, err := LoadProductionManifest(a.dir); err == nil {
		visibleStart = min(visibleStart, old.VisibleTxStart)
		visibleEnd = max(visibleEnd, old.VisibleTxEnd)
		generation = old.Generation + 1
		progress = cloneProgress(old.Progress)
		retired = append(retired, old.Retired...)
		for _, ref := range old.Segments {
			if segmentOverlapsAnyFamily(ref, refs) {
				retired = append(retired, ref)
			} else {
				segments = append(segments, ref)
			}
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	manifest := NewManifest(visibleStart, visibleEnd, segments)
	manifest.Generation = generation
	manifest.Progress = mergeProgress(progress, progressFromRefs(refs, visibleEnd))
	manifest.Retired = dedupeSegmentRefs(retired)
	if err := PublishManifest(a.dir, manifest); err != nil {
		return nil, err
	}
	return manifest, nil
}

func UpdateHotPruneProgress(dir string, txNum uint64) error {
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
	manifest.Progress = progress
	return PublishManifest(dir, manifest)
}

func WriteManifestProgressStages(db ethdb.KeyValueWriter, progress *Progress) error {
	if db == nil {
		return nil
	}
	return writeManifestProgressStages(newRawDBStageProgressStore(db), progress)
}

func writeManifestProgressStages(store stageProgressStore, progress *Progress) error {
	if store == nil || progress == nil {
		return nil
	}
	stages := []struct {
		stage rawdb.StageID
		txNum uint64
	}{
		{stage: rawdb.StageSnapshotLatest, txNum: progress.LatestBuildTxNum},
		{stage: rawdb.StageSnapshotHistory, txNum: progress.HistoryBuildTxNum},
		{stage: rawdb.StageSnapshotAccessor, txNum: progress.AccessorBuildTxNum},
		{stage: rawdb.StageSnapshotCommitmentFlush, txNum: progress.CommitmentFlushTxNum},
		{stage: rawdb.StageSnapshotHotPrune, txNum: progress.HotPruneTxNum},
	}
	for _, item := range stages {
		if item.txNum == 0 {
			continue
		}
		if err := store.Write(item.stage, item.txNum); err != nil {
			return err
		}
	}
	return nil
}

func progressFromRefs(refs []SegmentRef, txNum uint64) *Progress {
	if len(refs) == 0 {
		return nil
	}
	var p Progress
	registry := DefaultDomainRegistry()
	for _, ref := range refs {
		cfg, ok := registry.ConfigForRef(ref)
		switch ref.Kind {
		case SegmentLatest:
			p.LatestBuildTxNum = max(p.LatestBuildTxNum, txNum)
			if ok && cfg.TracksCommitmentFlush {
				p.CommitmentFlushTxNum = max(p.CommitmentFlushTxNum, txNum)
			}
		case SegmentAccessor, SegmentBTree:
			p.AccessorBuildTxNum = max(p.AccessorBuildTxNum, txNum)
		case SegmentHistory:
			if ok && cfg.HasHistory {
				p.HistoryBuildTxNum = max(p.HistoryBuildTxNum, txNum)
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
	if len(configured) != 0 {
		return normalizeAggregationKVDomains(configured)
	}
	cfg, ok := DefaultDomainRegistry().Dataset(SegmentDatasetKVLatest)
	if !ok || cfg.IterateHotKVLatestRows == nil {
		return nil, errors.New("snapshots: missing account-KV latest iterator")
	}
	seen := make(map[kvdomains.KVDomain]struct{})
	if err := cfg.IterateHotKVLatestRows(db, func(row rawdb.StateKVLatestRow) (bool, error) {
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
		if ref.normalizedDataset() != candidate.normalizedDataset() || ref.Domain != candidate.Domain || ref.Kind != candidate.Kind {
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
