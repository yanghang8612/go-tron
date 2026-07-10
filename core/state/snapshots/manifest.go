package snapshots

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

const (
	CurrentManifestVersion = 1
	ManifestVersion        = CurrentManifestVersion
	ManifestFile           = "manifest.json"
)

type SegmentKind string

const (
	SegmentLatest   SegmentKind = "latest"
	SegmentAccessor SegmentKind = "accessor"
	SegmentBTree    SegmentKind = "btree"
	SegmentHistory  SegmentKind = "history"
	SegmentInverted SegmentKind = "inverted"
	// SegmentChainFreezer is a block-numbered chain-freezer snapshot family.
	// SegmentRef.FromTxNum/ToTxNum carry freezer block numbers for this kind.
	SegmentChainFreezer SegmentKind = "chain-freezer"
	// SegmentChainIndex is a block-numbered read-only lookup sidecar for
	// chain-freezer segments. SegmentRef.FromTxNum/ToTxNum carry freezer
	// block numbers for this kind.
	SegmentChainIndex SegmentKind = "chain-index"
	// SegmentChainFreezerAccessor is a block-numbered offset sidecar for
	// chain-freezer segments. SegmentRef.FromTxNum/ToTxNum carry freezer
	// block numbers for this kind.
	SegmentChainFreezerAccessor SegmentKind = "chain-freezer-accessor"
	// SegmentBalanceTrace is a block-numbered cold account/balance trace
	// segment. SegmentRef.FromTxNum/ToTxNum carry block numbers for this kind.
	SegmentBalanceTrace SegmentKind = "balance-trace"
	// SegmentSectionBloom is a block-numbered cold section-bloom segment.
	// SegmentRef.FromTxNum/ToTxNum carry the source block range.
	SegmentSectionBloom SegmentKind = "section-bloom"
	// SegmentEventLog is a block-numbered cold TVM event log segment.
	// SegmentRef.FromTxNum/ToTxNum carry the source block range.
	SegmentEventLog SegmentKind = "event-log"
	// SegmentEventLogIndex is a block-numbered global lookup sidecar for
	// event-log segments. It maps address/topic keys to candidate event-log
	// segment start blocks, avoiding unrelated segment opens on filtered reads.
	SegmentEventLogIndex SegmentKind = "event-log-index"
)

type SegmentDataset string

const (
	SegmentDatasetAccountLatest        SegmentDataset = "account-latest"
	SegmentDatasetKVLatest             SegmentDataset = "kv-latest"
	SegmentDatasetKVGeneration         SegmentDataset = "kv-generation"
	SegmentDatasetCode                 SegmentDataset = "code"
	SegmentDatasetCommitmentRoot       SegmentDataset = "commitment-root"
	SegmentDatasetCommitmentCheckpoint SegmentDataset = "commitment-checkpoint"
	SegmentDatasetStateDomainChange    SegmentDataset = "state-domain-change"
	SegmentDatasetChainFreezer         SegmentDataset = "chain-freezer"
	SegmentDatasetBalanceTrace         SegmentDataset = "balance-trace"
	SegmentDatasetSectionBloom         SegmentDataset = "section-bloom"
	SegmentDatasetEventLog             SegmentDataset = "event-log"
)

type Manifest struct {
	Version        uint32         `json:"version"`
	Generation     uint64         `json:"generation,omitempty"`
	PublishedUnix  int64          `json:"publishedUnix"`
	VisibleTxStart uint64         `json:"visibleTxStart"`
	VisibleTxEnd   uint64         `json:"visibleTxEnd"`
	Chain          *ChainIdentity `json:"chain,omitempty"`
	Progress       *Progress      `json:"progress,omitempty"`
	Segments       []SegmentRef   `json:"segments"`
	Retired        []SegmentRef   `json:"retired,omitempty"`
}

type ChainIdentity struct {
	ChainID        int64  `json:"chainId"`
	NetworkID      int32  `json:"networkId"`
	GenesisHash    string `json:"genesisHash"`
	ForkConfigHash string `json:"forkConfigHash,omitempty"`
}

type Progress struct {
	LatestBuildTxNum     uint64 `json:"latestBuildTxNum,omitempty"`
	HistoryBuildTxNum    uint64 `json:"historyBuildTxNum,omitempty"`
	AccessorBuildTxNum   uint64 `json:"accessorBuildTxNum,omitempty"`
	CommitmentFlushTxNum uint64 `json:"commitmentFlushTxNum,omitempty"`
	HotPruneTxNum        uint64 `json:"hotPruneTxNum,omitempty"`
}

type SegmentRef struct {
	Dataset   SegmentDataset     `json:"dataset,omitempty"`
	Domain    kvdomains.KVDomain `json:"domain,omitempty"`
	Kind      SegmentKind        `json:"kind"`
	FromTxNum uint64             `json:"fromTxNum"`
	ToTxNum   uint64             `json:"toTxNum"`
	Path      string             `json:"path"`
	Size      uint64             `json:"size"`
	Checksum  string             `json:"checksum,omitempty"`
}

func NewManifest(visibleTxStart, visibleTxEnd uint64, segments []SegmentRef) *Manifest {
	out := &Manifest{
		Version:        CurrentManifestVersion,
		Generation:     1,
		PublishedUnix:  time.Now().Unix(),
		VisibleTxStart: visibleTxStart,
		VisibleTxEnd:   visibleTxEnd,
		Segments:       append([]SegmentRef(nil), segments...),
	}
	sortSegments(out.Segments)
	return out
}

func NewManifestForChain(visibleTxStart, visibleTxEnd uint64, segments []SegmentRef, identity ChainIdentity) *Manifest {
	out := NewManifest(visibleTxStart, visibleTxEnd, segments)
	out.Chain = &identity
	normalizeManifest(out)
	return out
}

func LoadManifest(dir string) (*Manifest, error) {
	data, err := os.ReadFile(filepath.Join(dir, ManifestFile))
	if err != nil {
		return nil, err
	}
	return decodeManifest(data)
}

func decodeManifest(data []byte) (*Manifest, error) {
	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, err
	}
	normalizeManifest(&manifest)
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	sortSegments(manifest.Segments)
	sortSegments(manifest.Retired)
	return &manifest, nil
}

func decodeProductionManifest(data []byte) (*Manifest, error) {
	manifest, err := decodeManifest(data)
	if err != nil {
		return nil, err
	}
	if err := manifest.ValidateProduction(); err != nil {
		return nil, err
	}
	return manifest, nil
}

func LoadProductionManifest(dir string) (*Manifest, error) {
	manifest, err := LoadManifest(dir)
	if err != nil {
		return nil, err
	}
	if err := manifest.ValidateProduction(); err != nil {
		return nil, err
	}
	return manifest, nil
}

func PublishManifest(dir string, manifest *Manifest) error {
	if manifest == nil {
		return errors.New("snapshots: nil manifest")
	}
	if manifest.Version == 0 {
		manifest.Version = CurrentManifestVersion
	}
	if manifest.Generation == 0 {
		manifest.Generation = 1
	}
	normalizeChainIdentity(manifest.Chain)
	if manifest.PublishedUnix == 0 {
		manifest.PublishedUnix = time.Now().Unix()
	}
	sortSegments(manifest.Segments)
	sortSegments(manifest.Retired)
	if err := manifest.Validate(); err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".manifest-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, filepath.Join(dir, ManifestFile))
}

func (m *Manifest) Validate() error {
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
	seenPath := make(map[string]struct{}, len(m.Segments))
	byFamily := make(map[segmentFamily][]SegmentRef)
	for _, seg := range m.Segments {
		if err := validateActiveSegment(seg, m.VisibleTxStart, m.VisibleTxEnd); err != nil {
			return err
		}
		if _, dup := seenPath[seg.Path]; dup {
			return fmt.Errorf("snapshots: duplicate segment path %q", seg.Path)
		}
		seenPath[seg.Path] = struct{}{}
		fam := segmentFamily{dataset: seg.normalizedDataset(), domain: seg.Domain, kind: seg.Kind}
		byFamily[fam] = append(byFamily[fam], seg)
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
	if err := validateHistoryBinaryCompanionTriples(m); err != nil {
		return err
	}
	if err := validateLatestBinaryCompanionTriples(m); err != nil {
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

func (m *Manifest) ValidateProduction() error {
	if m == nil {
		return errors.New("snapshots: nil manifest")
	}
	return validateProductionHistorySegments(m)
}

func (m *Manifest) ValidateChainIdentity(expected ChainIdentity) error {
	if m == nil {
		return errors.New("snapshots: nil manifest")
	}
	normalizeChainIdentity(&expected)
	if err := validateChainIdentity(&expected); err != nil {
		return fmt.Errorf("snapshots: invalid expected chain identity: %w", err)
	}
	if m.Chain == nil {
		return errors.New("snapshots: manifest has no chain identity")
	}
	got := *m.Chain
	normalizeChainIdentity(&got)
	if got != expected {
		return fmt.Errorf("snapshots: manifest chain identity mismatch: got chainId=%d networkId=%d genesisHash=%s forkConfigHash=%s, want chainId=%d networkId=%d genesisHash=%s forkConfigHash=%s",
			got.ChainID, got.NetworkID, got.GenesisHash, got.ForkConfigHash,
			expected.ChainID, expected.NetworkID, expected.GenesisHash, expected.ForkConfigHash)
	}
	return nil
}

// EnsureProductionManifestChainIdentity attaches identity to the active
// production manifest when it is still legacy/unbound. An existing identity
// must match exactly; snapshot publishers must never relabel a manifest from a
// different chain.
func EnsureProductionManifestChainIdentity(dir string, identity ChainIdentity) (*Manifest, error) {
	normalizeChainIdentity(&identity)
	if err := validateChainIdentity(&identity); err != nil {
		return nil, fmt.Errorf("snapshots: invalid production manifest chain identity: %w", err)
	}
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		return nil, err
	}
	if manifest.Chain != nil {
		if err := manifest.ValidateChainIdentity(identity); err != nil {
			return nil, err
		}
		return manifest, nil
	}
	manifest.Chain = &identity
	if err := PublishManifest(dir, manifest); err != nil {
		return nil, err
	}
	return manifest, nil
}

type segmentFamily struct {
	dataset SegmentDataset
	domain  kvdomains.KVDomain
	kind    SegmentKind
}

func cloneManifest(in *Manifest) *Manifest {
	if in == nil {
		return nil
	}
	out := *in
	if in.Chain != nil {
		chain := *in.Chain
		out.Chain = &chain
	}
	if in.Progress != nil {
		progress := *in.Progress
		out.Progress = &progress
	}
	out.Segments = append([]SegmentRef(nil), in.Segments...)
	out.Retired = append([]SegmentRef(nil), in.Retired...)
	return &out
}

func normalizeManifest(manifest *Manifest) {
	if manifest.Generation == 0 {
		manifest.Generation = 1
	}
	normalizeChainIdentity(manifest.Chain)
}

func normalizeChainIdentity(identity *ChainIdentity) {
	if identity == nil {
		return
	}
	identity.GenesisHash = normalizeHashLiteral(identity.GenesisHash)
	identity.ForkConfigHash = strings.ToLower(strings.TrimSpace(identity.ForkConfigHash))
}

func validateChainIdentity(identity *ChainIdentity) error {
	if identity == nil {
		return nil
	}
	if identity.GenesisHash == "" {
		return errors.New("snapshots: chain identity missing genesisHash")
	}
	if err := validateHexHash("genesisHash", identity.GenesisHash); err != nil {
		return err
	}
	if identity.ForkConfigHash != "" {
		if !strings.HasPrefix(identity.ForkConfigHash, "sha256:") {
			return fmt.Errorf("snapshots: chain identity forkConfigHash %q must use sha256:<hex>", identity.ForkConfigHash)
		}
		if err := validateHexHash("forkConfigHash", strings.TrimPrefix(identity.ForkConfigHash, "sha256:")); err != nil {
			return err
		}
	}
	return nil
}

func normalizeHashLiteral(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	return strings.TrimPrefix(s, "0x")
}

func validateHexHash(field, value string) error {
	if len(value) != 64 {
		return fmt.Errorf("snapshots: chain identity %s has %d hex chars, want 64", field, len(value))
	}
	if _, err := hex.DecodeString(value); err != nil {
		return fmt.Errorf("snapshots: chain identity %s is not hex: %w", field, err)
	}
	return nil
}

func validateHistoryBinaryCompanionTriples(manifest *Manifest) error {
	if manifest == nil {
		return nil
	}
	historyByCompanion := make(map[string]struct{})
	registry := DefaultDomainRegistry()
	for _, ref := range manifest.Segments {
		cfg, ok := registry.ConfigForRef(ref)
		if !ok || ref.Kind != SegmentHistory || !cfg.IsHistoryBinarySegmentPath(ref.Path) {
			continue
		}
		if cfg.HasHistoryInvertedIndex {
			idxRef, ok := cfg.HistoryIndexRef(manifest, ref)
			if !ok {
				return fmt.Errorf("snapshots: binary %s history %q missing required index %q", cfg.Dataset, ref.Path, cfg.HistoryIndexPathFor(ref.Path))
			}
			historyByCompanion[idxRef.Path] = struct{}{}
		}
		if cfg.HasHistoryAccessor {
			accessorRef, ok := cfg.HistoryAccessorRef(manifest, ref)
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

func validateLatestBinaryCompanionTriples(manifest *Manifest) error {
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
			if !ok {
				return fmt.Errorf("snapshots: binary latest %q missing required accessor %q", ref.Path, latestBinaryAccessorPath(ref.Path))
			}
			companionByLatest[accessorRef.Path] = struct{}{}
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

func validateProductionHistorySegments(manifest *Manifest) error {
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

func validateActiveSegment(seg SegmentRef, visibleStart, visibleEnd uint64) error {
	if err := validateSegmentRef(seg); err != nil {
		return err
	}
	if isBlockNumberedSegmentKind(seg.Kind) {
		return nil
	}
	if seg.FromTxNum < visibleStart || seg.ToTxNum > visibleEnd {
		return fmt.Errorf("snapshots: segment %q range [%d,%d] outside visible range [%d,%d]",
			seg.Path, seg.FromTxNum, seg.ToTxNum, visibleStart, visibleEnd)
	}
	return nil
}

func validateRetiredSegment(seg SegmentRef) error {
	return validateSegmentRef(seg)
}

func validateSegment(seg SegmentRef, visibleStart, visibleEnd uint64) error {
	return validateActiveSegment(seg, visibleStart, visibleEnd)
}

func validateSegmentRef(seg SegmentRef) error {
	switch seg.Kind {
	case SegmentLatest, SegmentAccessor, SegmentBTree, SegmentHistory, SegmentInverted, SegmentChainFreezer, SegmentChainIndex, SegmentChainFreezerAccessor, SegmentBalanceTrace, SegmentSectionBloom, SegmentEventLog, SegmentEventLogIndex:
	default:
		return fmt.Errorf("snapshots: unknown segment kind %q", seg.Kind)
	}
	dataset := seg.normalizedDataset()
	if seg.Kind == SegmentBalanceTrace {
		if err := validateBalanceTraceSegmentRefDataset(seg, dataset); err != nil {
			return err
		}
	} else if seg.Kind == SegmentSectionBloom {
		if err := validateSectionBloomSegmentRefDataset(seg, dataset); err != nil {
			return err
		}
	} else if seg.Kind == SegmentEventLog || seg.Kind == SegmentEventLogIndex {
		if err := validateEventLogSegmentRefDataset(seg, dataset); err != nil {
			return err
		}
	} else if isChainBlockSegmentKind(seg.Kind) {
		if err := validateChainFreezerSegmentRefDataset(seg, dataset); err != nil {
			return err
		}
	} else if seg.Kind == SegmentLatest {
		if err := validateLatestSegmentRefDataset(seg, dataset); err != nil {
			return err
		}
	} else {
		if err := validateIndexedSegmentRefDataset(seg, dataset); err != nil {
			return err
		}
	}
	if seg.ToTxNum < seg.FromTxNum {
		return fmt.Errorf("snapshots: segment %q range [%d,%d] is inverted", seg.Path, seg.FromTxNum, seg.ToTxNum)
	}
	if seg.Path == "" || filepath.IsAbs(seg.Path) || filepath.Clean(seg.Path) != seg.Path || seg.Path == "." || hasParentDir(seg.Path) {
		return fmt.Errorf("snapshots: invalid relative segment path %q", seg.Path)
	}
	return nil
}

func isChainBlockSegmentKind(kind SegmentKind) bool {
	return kind == SegmentChainFreezer || kind == SegmentChainIndex || kind == SegmentChainFreezerAccessor
}

func isBlockNumberedSegmentKind(kind SegmentKind) bool {
	return isChainBlockSegmentKind(kind) || kind == SegmentBalanceTrace || kind == SegmentSectionBloom || kind == SegmentEventLog || kind == SegmentEventLogIndex
}

func validateChainFreezerSegmentRefDataset(seg SegmentRef, dataset SegmentDataset) error {
	if dataset != SegmentDatasetChainFreezer {
		return fmt.Errorf("snapshots: %s segment %q must use dataset %q, got %q", seg.Kind, seg.Path, SegmentDatasetChainFreezer, seg.Dataset)
	}
	if seg.Domain != 0 {
		return fmt.Errorf("snapshots: %s segment %q must not set kv domain %#04x", dataset, seg.Path, uint16(seg.Domain))
	}
	return nil
}

func validateBalanceTraceSegmentRefDataset(seg SegmentRef, dataset SegmentDataset) error {
	if dataset != SegmentDatasetBalanceTrace {
		return fmt.Errorf("snapshots: %s segment %q must use dataset %q, got %q", seg.Kind, seg.Path, SegmentDatasetBalanceTrace, seg.Dataset)
	}
	if seg.Domain != 0 {
		return fmt.Errorf("snapshots: %s segment %q must not set kv domain %#04x", dataset, seg.Path, uint16(seg.Domain))
	}
	return nil
}

func validateSectionBloomSegmentRefDataset(seg SegmentRef, dataset SegmentDataset) error {
	if dataset != SegmentDatasetSectionBloom {
		return fmt.Errorf("snapshots: %s segment %q must use dataset %q, got %q", seg.Kind, seg.Path, SegmentDatasetSectionBloom, seg.Dataset)
	}
	if seg.Domain != 0 {
		return fmt.Errorf("snapshots: %s segment %q must not set kv domain %#04x", dataset, seg.Path, uint16(seg.Domain))
	}
	return nil
}

func validateEventLogSegmentRefDataset(seg SegmentRef, dataset SegmentDataset) error {
	if dataset != SegmentDatasetEventLog {
		return fmt.Errorf("snapshots: %s segment %q must use dataset %q, got %q", seg.Kind, seg.Path, SegmentDatasetEventLog, seg.Dataset)
	}
	if seg.Domain != 0 {
		return fmt.Errorf("snapshots: %s segment %q must not set kv domain %#04x", dataset, seg.Path, uint16(seg.Domain))
	}
	return nil
}

func validateLatestSegmentRefDataset(seg SegmentRef, dataset SegmentDataset) error {
	cfg, ok := DefaultDomainRegistry().Dataset(dataset)
	if !ok || !cfg.HasLatest {
		return fmt.Errorf("snapshots: unknown latest dataset %q", seg.Dataset)
	}
	if err := cfg.ValidateRef(seg); err != nil {
		return err
	}
	return nil
}

func validateIndexedSegmentRefDataset(seg SegmentRef, dataset SegmentDataset) error {
	cfg, ok := DefaultDomainRegistry().Dataset(dataset)
	if !ok {
		if dataset != "" {
			return fmt.Errorf("snapshots: unknown %s dataset %q", seg.Kind, seg.Dataset)
		}
		if !kvdomains.IsRegistered(seg.Domain) {
			return fmt.Errorf("snapshots: unregistered domain %#04x", uint16(seg.Domain))
		}
		return nil
	}
	if !cfg.AllowsKind(seg.Kind) {
		return fmt.Errorf("snapshots: %s is not valid for %s segment %q", dataset, seg.Kind, seg.Path)
	}
	if err := cfg.ValidateRef(seg); err != nil {
		return err
	}
	return nil
}

func (seg SegmentRef) normalizedDataset() SegmentDataset {
	return seg.NormalizedDataset()
}

func (seg SegmentRef) NormalizedDataset() SegmentDataset {
	if seg.Dataset == "" && seg.Kind == SegmentLatest {
		return SegmentDatasetKVLatest
	}
	return seg.Dataset
}

func hasParentDir(path string) bool {
	for _, part := range strings.Split(filepath.ToSlash(path), "/") {
		if part == ".." {
			return true
		}
	}
	return false
}

func sortSegments(segments []SegmentRef) {
	sort.Slice(segments, func(i, j int) bool {
		if segments[i].normalizedDataset() != segments[j].normalizedDataset() {
			return segments[i].normalizedDataset() < segments[j].normalizedDataset()
		}
		if segments[i].Domain != segments[j].Domain {
			return segments[i].Domain < segments[j].Domain
		}
		if segments[i].Kind != segments[j].Kind {
			return segments[i].Kind < segments[j].Kind
		}
		if segments[i].FromTxNum != segments[j].FromTxNum {
			return segments[i].FromTxNum < segments[j].FromTxNum
		}
		if segments[i].ToTxNum != segments[j].ToTxNum {
			return segments[i].ToTxNum < segments[j].ToTxNum
		}
		return segments[i].Path < segments[j].Path
	})
}
