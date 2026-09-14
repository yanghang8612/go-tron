package snapshots

// Frozen master 44a97531 metadata validation loops. This oracle intentionally
// keeps full SegmentRef family copies and pre-kind-filter registry calls so
// candidate changes are compared against the previous public semantics.
import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
)

func frozenManifestValidate(m *Manifest) error {
	if m == nil {
		return errors.New("snapshots: nil manifest")
	}
	if m.Version != CurrentManifestVersion {
		return fmt.Errorf("snapshots: unsupported manifest version %d", m.Version)
	}
	if m.VisibleTxEnd < m.VisibleTxStart {
		return fmt.Errorf("snapshots: visible range [%d,%d] is inverted", m.VisibleTxStart, m.VisibleTxEnd)
	}
	if err := validateChainIdentity(m.Chain); err != nil {
		return err
	}
	// Reuse the duplicate-path index for companion validation. Scanning the
	// entire catalog for each history/index/accessor triple is quadratic in
	// the number of cold segments and competes with import on every load.
	seenPath := make(map[string]*SegmentRef, len(m.Segments))
	byFamily := make(map[segmentFamily][]SegmentRef)
	for i := range m.Segments {
		seg := &m.Segments[i]
		if err := validateActiveSegment(*seg, m.VisibleTxStart, m.VisibleTxEnd); err != nil {
			return err
		}
		if _, dup := seenPath[seg.Path]; dup {
			return fmt.Errorf("snapshots: duplicate segment path %q", seg.Path)
		}
		seenPath[seg.Path] = seg
		fam := segmentFamily{dataset: seg.normalizedDataset(), domain: seg.Domain, kind: seg.Kind}
		byFamily[fam] = append(byFamily[fam], *seg)
	}
	for family, segments := range byFamily {
		sort.Slice(segments, func(i, j int) bool {
			if segments[i].FromTxNum == segments[j].FromTxNum {
				return segments[i].ToTxNum < segments[j].ToTxNum
			}
			return segments[i].FromTxNum < segments[j].FromTxNum
		})
		for i := 1; i < len(segments); i++ {
			if segments[i].FromTxNum <= segments[i-1].ToTxNum {
				return fmt.Errorf("snapshots: overlapping %s segments for domain %#04x: [%d,%d] and [%d,%d]",
					family.kind, uint16(family.domain),
					segments[i-1].FromTxNum, segments[i-1].ToTxNum,
					segments[i].FromTxNum, segments[i].ToTxNum)
			}
		}
	}
	for _, seg := range m.Retired {
		if err := validateRetiredSegment(seg); err != nil {
			return err
		}
	}
	if err := frozenValidateHistoryBinaryCompanionTriples(m, seenPath); err != nil {
		return err
	}
	if err := frozenValidateLatestBinaryCompanionTriples(m); err != nil {
		return err
	}
	if err := validateChainIndexCompanions(m); err != nil {
		return err
	}
	if err := validateEventLogIndexCompanions(m); err != nil {
		return err
	}
	return nil
}

func frozenValidateHistoryBinaryCompanionTriples(manifest *Manifest, byPath map[string]*SegmentRef) error {
	if manifest == nil {
		return nil
	}
	historyByCompanion := make(map[string]struct{})
	registry := DefaultDomainRegistry()
	// Duplicate paths have already failed Validate. Retain every identity
	// predicate of DomainCfg.historyCompanionRef, including the legacy zero
	// aggregation-step normalization; a matching filename alone is not proof.
	companion := func(cfg DomainCfg, history SegmentRef, kind SegmentKind, path string) (*SegmentRef, bool) {
		ref := byPath[path]
		return ref, path != "" && ref != nil && ref.normalizedDataset() == cfg.Dataset &&
			ref.Kind == kind && ref.FromTxNum == history.FromTxNum && ref.ToTxNum == history.ToTxNum &&
			ref.effectiveAggregationSteps() == history.effectiveAggregationSteps()
	}
	for _, ref := range manifest.Segments {
		cfg, ok := registry.ConfigForRef(ref)
		if !ok || ref.Kind != SegmentHistory || !cfg.IsHistoryBinarySegmentPath(ref.Path) {
			continue
		}
		if cfg.HasHistoryInvertedIndex {
			idxRef, ok := companion(cfg, ref, SegmentInverted, cfg.HistoryIndexPathFor(ref.Path))
			if !ok {
				return fmt.Errorf("snapshots: binary %s history %q missing required index %q", cfg.Dataset, ref.Path, cfg.HistoryIndexPathFor(ref.Path))
			}
			historyByCompanion[idxRef.Path] = struct{}{}
		}
		if cfg.HasHistoryAccessor {
			accessorRef, ok := companion(cfg, ref, SegmentAccessor, cfg.HistoryAccessorPathFor(ref.Path))
			if !ok {
				return fmt.Errorf("snapshots: binary %s history %q missing required accessor %q", cfg.Dataset, ref.Path, cfg.HistoryAccessorPathFor(ref.Path))
			}
			historyByCompanion[accessorRef.Path] = struct{}{}
		}
	}
	for _, ref := range manifest.Segments {
		cfg, ok := registry.ConfigForRef(ref)
		if !ok || !cfg.HasHistory || (ref.Kind != SegmentInverted && ref.Kind != SegmentAccessor) {
			continue
		}
		if _, ok := historyByCompanion[ref.Path]; !ok && cfg.IsHistoryBinaryCompanionPath(ref.Path) {
			return fmt.Errorf("snapshots: binary %s %s %q has no matching history segment", cfg.Dataset, ref.Kind, ref.Path)
		}
	}
	return nil
}

func frozenValidateLatestBinaryCompanionTriples(manifest *Manifest) error {
	if manifest == nil {
		return nil
	}
	registry := DefaultDomainRegistry()
	companionByLatest := make(map[string]struct{})
	for _, ref := range manifest.Segments {
		if ref.Kind != SegmentLatest || !isLatestBinarySegmentPath(ref.Path) {
			continue
		}
		cfg, ok := registry.ConfigForRef(ref)
		if !ok || !cfg.HasLatest {
			continue
		}
		if cfg.HasLatestAccessor {
			accessorRef, ok := latestBinaryAccessorRef(manifest, ref)
			if ok {
				companionByLatest[accessorRef.Path] = struct{}{}
			}
		}
		if cfg.HasLatestBTree {
			btreeRef, ok := latestBinaryBTreeRef(manifest, ref)
			if !ok {
				return fmt.Errorf("snapshots: binary latest %q missing required btree %q", ref.Path, latestBinaryBTreePath(ref.Path))
			}
			companionByLatest[btreeRef.Path] = struct{}{}
		}
	}
	for _, ref := range manifest.Segments {
		cfg, ok := registry.ConfigForRef(ref)
		if !ok || !cfg.HasLatest {
			continue
		}
		switch {
		case ref.Kind == SegmentAccessor && strings.EqualFold(filepath.Ext(ref.Path), ".lidx"):
			if _, ok := companionByLatest[ref.Path]; !ok {
				return fmt.Errorf("snapshots: binary latest accessor %q has no matching latest segment", ref.Path)
			}
		case ref.Kind == SegmentBTree && strings.EqualFold(filepath.Ext(ref.Path), ".bt"):
			if _, ok := companionByLatest[ref.Path]; !ok {
				return fmt.Errorf("snapshots: binary latest btree %q has no matching latest segment", ref.Path)
			}
		}
	}
	return nil
}

func frozenValidateProductionHistorySegments(manifest *Manifest) error {
	if manifest == nil {
		return nil
	}
	registry := DefaultDomainRegistry()
	for _, ref := range manifest.Segments {
		cfg, ok := registry.ConfigForRef(ref)
		if !ok || !cfg.HasHistory || ref.Kind != SegmentHistory {
			continue
		}
		if !cfg.IsHistoryBinarySegmentPath(ref.Path) {
			return fmt.Errorf("snapshots: production %s history segment %q must use binary .seg history with registered companions", cfg.Dataset, ref.Path)
		}
	}
	return nil
}
