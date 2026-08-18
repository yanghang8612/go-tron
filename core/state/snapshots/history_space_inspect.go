package snapshots

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

const (
	historySpaceTxIndexFrameEntries    = uint64(256)
	historySpacePostingFrameEntries    = uint64(128)
	historySpaceRecordDirectoryEntries = uint64(256)
	historySpaceTxIndexFrameBytes      = uint64(32)
	historySpacePostingFrameBytes      = uint64(24)
	historySpacePostingFramePointer    = uint64(8)
	historySpaceRecordDirectoryEntry   = uint64(16)
	historySpaceCandidateHeaderBytes   = uint64(64)
	historySpaceDefaultIndexEntries    = uint64(32 * 1024)
	historySpaceDefaultAccessorBlocks  = uint64(64)
	historySpaceDefaultHistoryBytes    = uint64(8 << 20)
	historySpaceMaxPostingPairsPerKey  = uint64(128)
	historySpaceMaxValueTxSamples      = uint64(256)
	historySpaceMaxValueRecords        = uint64(65_536)
)

var historySpaceBlockSizes = [...]uint64{16 << 10, 32 << 10, 64 << 10, 128 << 10}

// HistorySpaceInspectOptions bounds the read-only state-history profiler. The
// profiler pins one production manifest, reads headers for every active history
// trio, and samples immutable files without opening chaindata.
type HistorySpaceInspectOptions struct {
	SampleSegments       uint64
	SampleIndexEntries   uint64
	SampleAccessorBlocks uint64
	SampleHistoryBytes   uint64
	ProgressInterval     time.Duration
	Progress             func(HistorySpaceInspectProgress)
	Context              context.Context
}

type HistorySpaceInspectProgress struct {
	Phase          string
	CompletedTrios uint64
	TotalTrios     uint64
	HistoryPath    string
	Elapsed        time.Duration
}

type HistorySpacePhysicalBytes struct {
	History  uint64 `json:"history"`
	Accessor uint64 `json:"accessor"`
	Inverted uint64 `json:"inverted"`
	Total    uint64 `json:"total"`
}

type HistorySpaceSampleConfig struct {
	SelectionMode       string `json:"selectionMode"`
	Segments            uint64 `json:"segments"`
	IndexEntries        uint64 `json:"indexEntriesPerSegment"`
	AccessorBlocks      uint64 `json:"accessorBlocksPerSegment"`
	HistoryBytes        uint64 `json:"historyBytesPerSegment"`
	TxIndexFrameEntries uint64 `json:"txIndexFrameEntries"`
	PostingFrameEntries uint64 `json:"postingFrameEntries"`
	RecordDirectoryStep uint64 `json:"recordDirectoryEntries"`
}

type HistorySpaceSegmentStats struct {
	HistoryPath       string `json:"historyPath"`
	FromTxNum         uint64 `json:"fromTxNum"`
	ToTxNum           uint64 `json:"toTxNum"`
	AggregationSteps  uint64 `json:"aggregationSteps"`
	Records           uint64 `json:"records"`
	Keys              uint64 `json:"keys"`
	IndexEntries      uint64 `json:"indexEntries"`
	HistoryBytes      uint64 `json:"historyBytes"`
	AccessorBytes     uint64 `json:"accessorBytes"`
	InvertedBytes     uint64 `json:"invertedBytes"`
	HistoryCompressed bool   `json:"historyCompressed"`
	HistoryLogical    uint64 `json:"historyLogicalBytes"`
	HistoryBlocks     uint64 `json:"historyBlocks"`
	SampledRawBytes   uint64 `json:"sampledHistoryRawBytes"`
	SampledStored     uint64 `json:"sampledHistoryStoredBytes"`
	SampledIndex      uint64 `json:"sampledIndexEntries"`
	SampledKeys       uint64 `json:"sampledAccessorKeys"`
	SampledPostings   uint64 `json:"sampledAccessorPostings"`
}

type HistorySpaceIndexStats struct {
	Entries                uint64 `json:"entries"`
	CurrentBytes           uint64 `json:"currentBytes"`
	SampledEntries         uint64 `json:"sampledEntries"`
	SampledVariableBytes   uint64 `json:"sampledDeltaVarintBytes"`
	ProjectedFrames        uint64 `json:"projectedFrames"`
	ProjectedVariableBytes uint64 `json:"projectedDeltaVarintBytes"`
	ProjectedBytes         uint64 `json:"projectedBytes"`
}

type HistorySpaceAccessorStats struct {
	Records                uint64 `json:"records"`
	Keys                   uint64 `json:"keys"`
	CurrentBytes           uint64 `json:"currentBytes"`
	CurrentPostingBytes    uint64 `json:"currentPostingBytes"`
	CurrentMetadataBytes   uint64 `json:"currentMetadataBytes"`
	SampledKeys            uint64 `json:"sampledKeys"`
	SampledPostings        uint64 `json:"sampledPostings"`
	SampledCandidateBytes  uint64 `json:"sampledCandidatePostingBytes"`
	ProjectedPostingBytes  uint64 `json:"projectedPostingBytes"`
	ProjectedMetadataBytes uint64 `json:"projectedMetadataBytes"`
	ProjectedBytes         uint64 `json:"projectedBytes"`
}

type HistorySpaceCompressionStats struct {
	BlockBytes            uint64 `json:"blockBytes"`
	SampledRawBytes       uint64 `json:"sampledRawBytes"`
	SampledStoredBytes    uint64 `json:"sampledStoredBytes"`
	ProjectedHistoryBytes uint64 `json:"projectedHistoryBytes"`
}

type HistorySpaceValueDomainStats struct {
	Records        uint64 `json:"records"`
	PresentRecords uint64 `json:"presentRecords"`
	ValueBytes     uint64 `json:"valueBytes"`
	DuplicateBytes uint64 `json:"duplicateBytes"`
}

// HistorySpaceValueStats measures exact previous-value reuse in decoded V6
// records. CurrentBytes includes the existing marker and fixed 4-byte length
// on every row. ContentAddressedBytes models a per-segment dense value table
// plus one tagged uvarint value ID per present record; it deliberately excludes
// speculative cross-segment/global table metadata.
type HistorySpaceValueStats struct {
	SampledTxEntries       uint64                                  `json:"sampledTxEntries"`
	SampledRecords         uint64                                  `json:"sampledRecords"`
	PresentRecords         uint64                                  `json:"presentRecords"`
	ValueBytes             uint64                                  `json:"valueBytes"`
	SegmentUniqueValues    uint64                                  `json:"segmentUniqueValues"`
	SegmentUniqueBytes     uint64                                  `json:"segmentUniqueBytes"`
	SegmentDuplicateBytes  uint64                                  `json:"segmentDuplicateBytes"`
	CrossSegmentDuplicates uint64                                  `json:"crossSegmentDuplicateBytes"`
	CurrentBytes           uint64                                  `json:"currentBytes"`
	ContentAddressedBytes  uint64                                  `json:"contentAddressedBytes"`
	SavingsBytes           uint64                                  `json:"savingsBytes"`
	SavingsMilli           int64                                   `json:"savingsMilli"`
	Domains                map[string]HistorySpaceValueDomainStats `json:"domains"`
}

type HistorySpaceDictionaryStats struct {
	SampledSegments       uint64 `json:"sampledSegments"`
	TrainingFailures      uint64 `json:"trainingFailures"`
	TrainingRawBytes      uint64 `json:"trainingRawBytes"`
	EvaluationRawBytes    uint64 `json:"evaluationRawBytes"`
	PlainStoredBytes      uint64 `json:"plainStoredBytes"`
	DictionaryStoredBytes uint64 `json:"dictionaryStoredBytes"`
	DictionaryBytes       uint64 `json:"dictionaryBytes"`
	ComparedHistoryBytes  uint64 `json:"comparedHistoryBytes"`
	ProjectedHistoryBytes uint64 `json:"projectedHistoryBytes"`
	SavingsBytes          uint64 `json:"savingsBytes"`
	SavingsMilli          int64  `json:"savingsMilli"`
}

type HistorySpaceCandidate struct {
	Name                   string `json:"name"`
	HistoryBlockBytes      uint64 `json:"historyBlockBytes"`
	TxIndexFrameEntries    uint64 `json:"txIndexFrameEntries"`
	PostingFrameEntries    uint64 `json:"postingFrameEntries"`
	RecordDirectoryEntries uint64 `json:"recordDirectoryEntries"`
	HistoryBytes           uint64 `json:"historyBytes"`
	AccessorBytes          uint64 `json:"accessorBytes"`
	InvertedBytes          uint64 `json:"invertedBytes"`
	RecordDirectoryBytes   uint64 `json:"recordDirectoryBytes"`
	EstimatedPhysicalBytes uint64 `json:"estimatedPhysicalBytes"`
	ComparedPhysicalBytes  uint64 `json:"comparedPhysicalBytes"`
	SavingsBytes           uint64 `json:"savingsBytes"`
	SavingsMilli           int64  `json:"savingsMilli"`
	MaxHistoryDecompress   uint64 `json:"maxHistoryDecompressBytes"`
	MaxTxIndexScanEntries  uint64 `json:"maxTxIndexScanEntries"`
	MaxPostingScanEntries  uint64 `json:"maxPostingScanEntries"`
}

type HistorySpaceInspection struct {
	ManifestGeneration  uint64                         `json:"manifestGeneration"`
	ManifestPath        string                         `json:"manifestPath"`
	ManifestLoadMode    string                         `json:"manifestLoadMode"`
	ActiveHistoryTrios  uint64                         `json:"activeHistoryTrios"`
	SampledHistoryTrios uint64                         `json:"sampledHistoryTrios"`
	ManifestPhysical    HistorySpacePhysicalBytes      `json:"manifestPhysical"`
	SampleConfig        HistorySpaceSampleConfig       `json:"sampleConfig"`
	Index               HistorySpaceIndexStats         `json:"index"`
	Accessor            HistorySpaceAccessorStats      `json:"accessor"`
	Values              HistorySpaceValueStats         `json:"values"`
	TrainedDictionary   HistorySpaceDictionaryStats    `json:"trainedZstdDictionary"`
	Compression         []HistorySpaceCompressionStats `json:"compression"`
	Candidates          []HistorySpaceCandidate        `json:"candidates"`
	Segments            []HistorySpaceSegmentStats     `json:"segments"`
	Limitations         []string                       `json:"limitations,omitempty"`
	ElapsedSeconds      float64                        `json:"elapsedSeconds"`
}

type historySpaceTrio struct {
	history  SegmentRef
	accessor SegmentRef
	inverted SegmentRef
}

type historySpaceHeaders struct {
	records           uint64
	keys              uint64
	indexEntries      uint64
	accessorPosting   uint64
	accessorMetadata  uint64
	historyLogical    uint64
	historyBlocks     uint64
	historyCompressed bool
	accessorV6        bool
	accessorVersion   uint32
}

type historySpaceSampleTotals struct {
	indexEntries        uint64
	indexVariableBytes  uint64
	accessorKeys        uint64
	accessorPostings    uint64
	accessorBytes       uint64
	historyRaw          uint64
	historyCandidates   map[uint64]uint64
	selectedPhysical    uint64
	selectedProjected   map[uint64]uint64
	dictionary          HistorySpaceDictionaryStats
	dictionaryProjected uint64
}

type historySpaceDictionarySample struct {
	trainingRaw      uint64
	evaluationRaw    uint64
	plainStored      uint64
	dictionaryStored uint64
	dictionaryBytes  uint64
	failed           bool
}

type historySpaceValueKey struct {
	digest [sha256.Size]byte
	length uint64
}

type historySpaceValueAccumulator struct {
	global map[historySpaceValueKey]struct{}
	stats  HistorySpaceValueStats
}

// InspectHistorySpace profiles active immutable state-history files from one
// pinned production manifest. It never opens chaindata and never mutates files.
func InspectHistorySpace(dir string, opts HistorySpaceInspectOptions) (*HistorySpaceInspection, error) {
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		return nil, err
	}
	return InspectHistorySpaceFromManifest(dir, manifest, opts)
}

func InspectHistorySpaceFromManifest(dir string, manifest *Manifest, opts HistorySpaceInspectOptions) (*HistorySpaceInspection, error) {
	started := time.Now()
	if manifest == nil {
		return nil, errors.New("snapshots: nil history-space inspect manifest")
	}
	manifest = cloneManifest(manifest)
	if err := manifest.ValidateProduction(); err != nil {
		return nil, err
	}
	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	opts = normalizedHistorySpaceOptions(opts)
	trios, err := historySpaceTrios(manifest)
	if err != nil {
		return nil, err
	}
	out := &HistorySpaceInspection{
		ManifestGeneration:  manifest.Generation,
		ManifestPath:        ManifestFile,
		ManifestLoadMode:    "single-production-manifest-no-follow",
		ActiveHistoryTrios:  uint64(len(trios)),
		SampledHistoryTrios: min(opts.SampleSegments, uint64(len(trios))),
		SampleConfig: HistorySpaceSampleConfig{
			SelectionMode:       "largest-half-plus-even-range",
			Segments:            min(opts.SampleSegments, uint64(len(trios))),
			IndexEntries:        opts.SampleIndexEntries,
			AccessorBlocks:      opts.SampleAccessorBlocks,
			HistoryBytes:        opts.SampleHistoryBytes,
			TxIndexFrameEntries: historySpaceTxIndexFrameEntries,
			PostingFrameEntries: historySpacePostingFrameEntries,
			RecordDirectoryStep: historySpaceRecordDirectoryEntries,
		},
	}
	if len(trios) == 0 {
		out.Limitations = append(out.Limitations, "production manifest has no active state-domain-change history trios")
		out.ElapsedSeconds = time.Since(started).Seconds()
		return out, nil
	}
	progress := newHistorySpaceInspectProgress(started, opts.ProgressInterval, opts.Progress)

	selected := selectHistorySpaceTrios(trios, opts.SampleSegments)
	selectedSet := make(map[string]struct{}, len(selected))
	for _, trio := range selected {
		selectedSet[trio.history.Path] = struct{}{}
	}
	headers := make(map[string]historySpaceHeaders, len(trios))
	var totalRecords, totalKeys, totalIndexEntries, totalIndexFrames uint64
	var totalRecordDirectoryEntries uint64
	var totalAccessorPosting, totalAccessorMetadata uint64
	allV7 := true
	for i, trio := range trios {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		out.ManifestPhysical.History += trio.history.Size
		out.ManifestPhysical.Accessor += trio.accessor.Size
		out.ManifestPhysical.Inverted += trio.inverted.Size
		h, err := inspectHistorySpaceHeaders(dir, trio)
		if err != nil {
			return nil, err
		}
		headers[trio.history.Path] = h
		totalRecords += h.records
		totalKeys += h.keys
		totalIndexEntries += h.indexEntries
		totalIndexFrames += ceilDiv(h.indexEntries, historySpaceTxIndexFrameEntries)
		totalRecordDirectoryEntries += ceilDiv(h.records, historySpaceRecordDirectoryEntries)
		totalAccessorPosting += h.accessorPosting
		totalAccessorMetadata += h.accessorMetadata
		allV7 = allV7 && h.accessorVersion == stateDomainChangeBinaryVersionV7
		if !h.accessorV6 {
			return nil, fmt.Errorf("snapshots: history-space benchmark requires a key-oriented V6/V7 accessor %s", trio.accessor.Path)
		}
		if !h.historyCompressed {
			return nil, fmt.Errorf("snapshots: history-space benchmark requires compressed history %s", trio.history.Path)
		}
		progress.report("headers", uint64(i+1), uint64(len(trios)), trio.history.Path, false)
	}
	out.ManifestPhysical.Total = out.ManifestPhysical.History + out.ManifestPhysical.Accessor + out.ManifestPhysical.Inverted

	samples := historySpaceSampleTotals{
		historyCandidates: make(map[uint64]uint64),
		selectedProjected: make(map[uint64]uint64),
	}
	values := historySpaceValueAccumulator{
		global: make(map[historySpaceValueKey]struct{}),
		stats:  HistorySpaceValueStats{Domains: make(map[string]HistorySpaceValueDomainStats)},
	}
	var sampled uint64
	for _, trio := range trios {
		if _, ok := selectedSet[trio.history.Path]; !ok {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		stats, err := inspectHistorySpaceTrio(ctx, dir, trio, headers[trio.history.Path], opts, &samples)
		if err != nil {
			return nil, err
		}
		out.Segments = append(out.Segments, stats)
		if err := sampleHistorySpaceValues(ctx, dir, trio, headers[trio.history.Path], opts, &values); err != nil {
			return nil, err
		}
		sampled++
		progress.report("samples", sampled, uint64(len(selected)), trio.history.Path, false)
	}

	indexProjectedVariable := scaleHistorySpaceSample(samples.indexVariableBytes, samples.indexEntries, totalIndexEntries)
	indexProjected := uint64(len(trios))*historySpaceCandidateHeaderBytes + totalIndexFrames*historySpaceTxIndexFrameBytes + indexProjectedVariable
	out.Index = HistorySpaceIndexStats{
		Entries:                totalIndexEntries,
		CurrentBytes:           out.ManifestPhysical.Inverted,
		SampledEntries:         samples.indexEntries,
		SampledVariableBytes:   samples.indexVariableBytes,
		ProjectedFrames:        totalIndexFrames,
		ProjectedVariableBytes: indexProjectedVariable,
		ProjectedBytes:         indexProjected,
	}
	accessorProjectedPosting := scaleHistorySpaceSample(samples.accessorBytes, samples.accessorPostings, totalRecords)
	accessorProjected := totalAccessorMetadata + accessorProjectedPosting
	out.Accessor = HistorySpaceAccessorStats{
		Records:                totalRecords,
		Keys:                   totalKeys,
		CurrentBytes:           out.ManifestPhysical.Accessor,
		CurrentPostingBytes:    totalAccessorPosting,
		CurrentMetadataBytes:   totalAccessorMetadata,
		SampledKeys:            samples.accessorKeys,
		SampledPostings:        samples.accessorPostings,
		SampledCandidateBytes:  samples.accessorBytes,
		ProjectedPostingBytes:  accessorProjectedPosting,
		ProjectedMetadataBytes: totalAccessorMetadata,
		ProjectedBytes:         accessorProjected,
	}
	out.Values = values.stats
	if out.Values.CurrentBytes >= out.Values.ContentAddressedBytes {
		out.Values.SavingsBytes = out.Values.CurrentBytes - out.Values.ContentAddressedBytes
	}
	out.Values.SavingsMilli = checkedCandidateSavings(out.Values.CurrentBytes, out.Values.ContentAddressedBytes)
	out.TrainedDictionary = samples.dictionary
	out.TrainedDictionary.ComparedHistoryBytes = out.ManifestPhysical.History
	if samples.selectedPhysical > 0 {
		out.TrainedDictionary.ProjectedHistoryBytes = scaleHistorySpaceSample(samples.dictionaryProjected, samples.selectedPhysical, out.ManifestPhysical.History)
	}
	if out.TrainedDictionary.ComparedHistoryBytes >= out.TrainedDictionary.ProjectedHistoryBytes {
		out.TrainedDictionary.SavingsBytes = out.TrainedDictionary.ComparedHistoryBytes - out.TrainedDictionary.ProjectedHistoryBytes
	}
	out.TrainedDictionary.SavingsMilli = checkedCandidateSavings(out.TrainedDictionary.ComparedHistoryBytes, out.TrainedDictionary.ProjectedHistoryBytes)

	projectedHistory := make(map[uint64]uint64, len(historySpaceBlockSizes))
	for _, blockBytes := range historySpaceBlockSizes {
		projected := out.ManifestPhysical.History
		if blockBytes != historyCompressChunkSize && samples.selectedPhysical > 0 {
			projected = scaleHistorySpaceSample(samples.selectedProjected[blockBytes], samples.selectedPhysical, out.ManifestPhysical.History)
		}
		projectedHistory[blockBytes] = projected
		out.Compression = append(out.Compression, HistorySpaceCompressionStats{
			BlockBytes:            blockBytes,
			SampledRawBytes:       samples.historyRaw,
			SampledStoredBytes:    samples.historyCandidates[blockBytes],
			ProjectedHistoryBytes: projected,
		})
	}

	recordDirectoryBytes := uint64(len(trios))*historySpaceCandidateHeaderBytes + totalRecordDirectoryEntries*historySpaceRecordDirectoryEntry
	currentName := "current-v6"
	if allV7 {
		currentName = "current-v7"
	}
	current := HistorySpaceCandidate{
		Name:                   currentName,
		HistoryBlockBytes:      historyCompressChunkSize,
		HistoryBytes:           out.ManifestPhysical.History,
		AccessorBytes:          out.ManifestPhysical.Accessor,
		InvertedBytes:          out.ManifestPhysical.Inverted,
		EstimatedPhysicalBytes: out.ManifestPhysical.Total,
		ComparedPhysicalBytes:  out.ManifestPhysical.Total,
		MaxHistoryDecompress:   historyCompressChunkSize,
	}
	out.Candidates = append(out.Candidates, current)
	for _, blockBytes := range historySpaceBlockSizes {
		candidate := HistorySpaceCandidate{
			Name:                   fmt.Sprintf("delta-index-%d-delta-posting-%d-zstd-%dk", historySpaceTxIndexFrameEntries, historySpacePostingFrameEntries, blockBytes>>10),
			HistoryBlockBytes:      blockBytes,
			TxIndexFrameEntries:    historySpaceTxIndexFrameEntries,
			PostingFrameEntries:    historySpacePostingFrameEntries,
			RecordDirectoryEntries: historySpaceRecordDirectoryEntries,
			HistoryBytes:           projectedHistory[blockBytes],
			AccessorBytes:          accessorProjected,
			InvertedBytes:          indexProjected,
			RecordDirectoryBytes:   recordDirectoryBytes,
			ComparedPhysicalBytes:  out.ManifestPhysical.Total,
			MaxHistoryDecompress:   blockBytes,
			MaxTxIndexScanEntries:  historySpaceTxIndexFrameEntries,
			MaxPostingScanEntries:  historySpacePostingFrameEntries,
		}
		candidate.EstimatedPhysicalBytes = saturatingHistorySpaceSum(
			candidate.HistoryBytes,
			candidate.AccessorBytes,
			candidate.InvertedBytes,
			candidate.RecordDirectoryBytes,
		)
		if candidate.ComparedPhysicalBytes >= candidate.EstimatedPhysicalBytes {
			candidate.SavingsBytes = candidate.ComparedPhysicalBytes - candidate.EstimatedPhysicalBytes
		}
		candidate.SavingsMilli = checkedCandidateSavings(candidate.ComparedPhysicalBytes, candidate.EstimatedPhysicalBytes)
		out.Candidates = append(out.Candidates, candidate)
	}
	out.Limitations = append(out.Limitations,
		"candidate sizes are sampled projections, not bytes produced by an implemented on-disk format",
		"delta index/posting candidates remove per-record offsets and assume a sparse record directory; reported scan bounds are the resulting worst-case local scans",
		"history compression candidates recompress sampled uncompressed blocks with the production zstd codec",
		"value reuse samples at most 256 evenly spaced transaction-index entries per selected segment; SHA-256 plus length identifies exact-value candidates without retaining payloads",
		"content-addressed value bytes model per-segment dense IDs only; cross-segment duplicate bytes are reported separately and are not counted as savings",
		"trained-dictionary history projection trains on alternating sampled windows and evaluates only disjoint windows; each projected segment includes its dictionary bytes",
	)
	out.ElapsedSeconds = time.Since(started).Seconds()
	progress.report("complete", uint64(len(selected)), uint64(len(selected)), "", true)
	return out, nil
}

type historySpaceInspectProgressReporter struct {
	started  time.Time
	interval time.Duration
	next     time.Time
	callback func(HistorySpaceInspectProgress)
}

func newHistorySpaceInspectProgress(started time.Time, interval time.Duration, callback func(HistorySpaceInspectProgress)) *historySpaceInspectProgressReporter {
	return &historySpaceInspectProgressReporter{
		started:  started,
		interval: interval,
		next:     started.Add(interval),
		callback: callback,
	}
}

func (p *historySpaceInspectProgressReporter) report(phase string, completed, total uint64, path string, force bool) {
	if p == nil || p.callback == nil || p.interval <= 0 {
		return
	}
	now := time.Now()
	if !force && now.Before(p.next) {
		return
	}
	p.callback(HistorySpaceInspectProgress{
		Phase:          phase,
		CompletedTrios: completed,
		TotalTrios:     total,
		HistoryPath:    path,
		Elapsed:        now.Sub(p.started),
	})
	p.next = now.Add(p.interval)
}

func normalizedHistorySpaceOptions(opts HistorySpaceInspectOptions) HistorySpaceInspectOptions {
	if opts.SampleSegments == 0 {
		opts.SampleSegments = 8
	}
	if opts.SampleIndexEntries == 0 {
		opts.SampleIndexEntries = historySpaceDefaultIndexEntries
	}
	if opts.SampleAccessorBlocks == 0 {
		opts.SampleAccessorBlocks = historySpaceDefaultAccessorBlocks
	}
	if opts.SampleHistoryBytes == 0 {
		opts.SampleHistoryBytes = historySpaceDefaultHistoryBytes
	}
	return opts
}

func historySpaceTrios(manifest *Manifest) ([]historySpaceTrio, error) {
	cfg, ok := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	if !ok {
		return nil, errors.New("snapshots: state-domain-change registry is unavailable")
	}
	candidates := historyCompactionCandidates(manifest, cfg)
	out := make([]historySpaceTrio, 0, len(candidates))
	for _, candidate := range candidates {
		trio := historySpaceTrio{history: candidate.history}
		for _, companion := range candidate.companions {
			switch companion.Kind {
			case SegmentAccessor:
				trio.accessor = companion
			case SegmentInverted:
				trio.inverted = companion
			}
		}
		if trio.accessor.Path == "" || trio.inverted.Path == "" {
			return nil, fmt.Errorf("snapshots: history %s has incomplete accessor/index companions", trio.history.Path)
		}
		out = append(out, trio)
	}
	return out, nil
}

func selectHistorySpaceTrios(trios []historySpaceTrio, limit uint64) []historySpaceTrio {
	if limit == 0 || limit >= uint64(len(trios)) {
		return append([]historySpaceTrio(nil), trios...)
	}
	want := int(limit)
	selected := make(map[string]historySpaceTrio, want)
	bySize := append([]historySpaceTrio(nil), trios...)
	sort.Slice(bySize, func(i, j int) bool {
		left := bySize[i].history.Size + bySize[i].accessor.Size + bySize[i].inverted.Size
		right := bySize[j].history.Size + bySize[j].accessor.Size + bySize[j].inverted.Size
		if left == right {
			return bySize[i].history.FromTxNum < bySize[j].history.FromTxNum
		}
		return left > right
	})
	largest := max(1, want/2)
	for i := 0; i < largest && i < len(bySize); i++ {
		selected[bySize[i].history.Path] = bySize[i]
	}
	for i := 0; len(selected) < want && i < want*2; i++ {
		index := int((uint64(2*i+1) * uint64(len(trios))) / uint64(2*want))
		if index >= len(trios) {
			index = len(trios) - 1
		}
		selected[trios[index].history.Path] = trios[index]
	}
	for _, trio := range trios {
		if len(selected) >= want {
			break
		}
		selected[trio.history.Path] = trio
	}
	out := make([]historySpaceTrio, 0, len(selected))
	for _, trio := range selected {
		out = append(out, trio)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].history.FromTxNum < out[j].history.FromTxNum })
	return out
}

func inspectHistorySpaceHeaders(dir string, trio historySpaceTrio) (historySpaceHeaders, error) {
	var out historySpaceHeaders
	index, indexHeader, err := openStateDomainChangeBinaryIndexReader(dir, trio.inverted)
	if err != nil {
		return out, err
	}
	out.indexEntries = indexHeader.count
	_ = index.Close()

	accessorPath := filepath.Join(dir, trio.accessor.Path)
	accessor, err := os.Open(accessorPath)
	if err != nil {
		return out, err
	}
	defer accessor.Close()
	stat, err := accessor.Stat()
	if err != nil {
		return out, err
	}
	var fixed [12]byte
	if _, err := accessor.ReadAt(fixed[:], 0); err != nil {
		return out, err
	}
	accessorVersion := binary.BigEndian.Uint32(fixed[8:12])
	if string(fixed[:8]) == string(stateDomainChangeBinaryAccessorMagic[:]) && (accessorVersion == stateDomainChangeBinaryVersionV6 || accessorVersion == stateDomainChangeBinaryVersionV7) {
		h, err := decodeStateDomainChangeBinaryAccessorKeyHeader(accessor, uint64(stat.Size()))
		if err != nil {
			return out, err
		}
		out.records, out.keys = h.recordCount, h.keyCount
		out.accessorPosting = h.postingLen
		if uint64(stat.Size()) < h.postingLen {
			return out, errors.New("snapshots: key-oriented accessor posting section exceeds file size")
		}
		out.accessorMetadata = uint64(stat.Size()) - h.postingLen
		out.accessorV6 = true
		out.accessorVersion = accessorVersion
	} else {
		out.accessorMetadata = uint64(stat.Size())
	}

	historyPath := filepath.Join(dir, trio.history.Path)
	history, err := os.Open(historyPath)
	if err != nil {
		return out, err
	}
	defer history.Close()
	var compressedHeader [compressedBlockHeaderSize]byte
	if _, err := io.ReadFull(history, compressedHeader[:]); err != nil {
		return out, err
	}
	if string(compressedHeader[:8]) == compressedBlockMagic {
		out.historyCompressed = true
		out.historyBlocks = binary.BigEndian.Uint64(compressedHeader[24:32])
		out.historyLogical = binary.BigEndian.Uint64(compressedHeader[32:40])
	} else {
		stat, err := history.Stat()
		if err != nil {
			return out, err
		}
		out.historyLogical = uint64(stat.Size())
	}
	return out, nil
}

func inspectHistorySpaceTrio(ctx context.Context, dir string, trio historySpaceTrio, headers historySpaceHeaders, opts HistorySpaceInspectOptions, totals *historySpaceSampleTotals) (HistorySpaceSegmentStats, error) {
	stats := HistorySpaceSegmentStats{
		HistoryPath:       trio.history.Path,
		FromTxNum:         trio.history.FromTxNum,
		ToTxNum:           trio.history.ToTxNum,
		AggregationSteps:  trio.history.effectiveAggregationSteps(),
		Records:           headers.records,
		Keys:              headers.keys,
		IndexEntries:      headers.indexEntries,
		HistoryBytes:      trio.history.Size,
		AccessorBytes:     trio.accessor.Size,
		InvertedBytes:     trio.inverted.Size,
		HistoryCompressed: headers.historyCompressed,
		HistoryLogical:    headers.historyLogical,
		HistoryBlocks:     headers.historyBlocks,
	}
	indexEntries, indexBytes, err := sampleHistorySpaceIndex(ctx, dir, trio.inverted, headers.indexEntries, opts.SampleIndexEntries)
	if err != nil {
		return stats, err
	}
	stats.SampledIndex = indexEntries
	totals.indexEntries += indexEntries
	totals.indexVariableBytes += indexBytes

	keys, postings, accessorBytes, err := sampleHistorySpaceAccessor(ctx, dir, trio.accessor, opts.SampleAccessorBlocks)
	if err != nil {
		return stats, err
	}
	stats.SampledKeys, stats.SampledPostings = keys, postings
	totals.accessorKeys += keys
	totals.accessorPostings += postings
	totals.accessorBytes += accessorBytes

	current, raw, candidates, dictionary, err := sampleHistorySpaceCompression(ctx, dir, trio.history, opts.SampleHistoryBytes)
	if err != nil {
		return stats, err
	}
	stats.SampledStored, stats.SampledRawBytes = current, raw
	totals.historyRaw += raw
	totals.selectedPhysical += trio.history.Size
	for blockBytes, candidateBytes := range candidates {
		totals.historyCandidates[blockBytes] += candidateBytes
		projected := trio.history.Size
		if current > 0 {
			projected = scaleHistorySpaceSample(candidateBytes, current, trio.history.Size)
		}
		totals.selectedProjected[blockBytes] += projected
	}
	totals.dictionary.SampledSegments++
	totals.dictionary.TrainingRawBytes += dictionary.trainingRaw
	totals.dictionary.EvaluationRawBytes += dictionary.evaluationRaw
	totals.dictionary.PlainStoredBytes += dictionary.plainStored
	totals.dictionary.DictionaryStoredBytes += dictionary.dictionaryStored
	totals.dictionary.DictionaryBytes += dictionary.dictionaryBytes
	if dictionary.failed {
		totals.dictionary.TrainingFailures++
		totals.dictionaryProjected += trio.history.Size
	} else if dictionary.plainStored > 0 {
		compressedProjection := scaleHistorySpaceSample(dictionary.dictionaryStored, dictionary.plainStored, trio.history.Size)
		totals.dictionaryProjected += saturatingHistorySpaceSum(compressedProjection, dictionary.dictionaryBytes)
	} else {
		totals.dictionaryProjected += trio.history.Size
	}
	return stats, nil
}

func sampleHistorySpaceValues(ctx context.Context, dir string, trio historySpaceTrio, headers historySpaceHeaders, opts HistorySpaceInspectOptions, accumulator *historySpaceValueAccumulator) error {
	if accumulator == nil || headers.records == 0 || headers.indexEntries == 0 {
		return nil
	}
	reader, logicalSize, header, err := openHistorySegmentForReadWithCacheLimit(dir, trio.history, 2)
	if err != nil {
		return err
	}
	defer reader.Close()
	if header.version != stateDomainChangeBinaryVersionV6 {
		return fmt.Errorf("snapshots: value reuse benchmark requires V6 history records in %s", trio.history.Path)
	}
	contextual, ok := reader.(*stateDomainChangeHistoryReader)
	if !ok {
		return fmt.Errorf("snapshots: value reuse benchmark lacks contextual history reader for %s", trio.history.Path)
	}
	index, indexHeader, err := openStateDomainChangeBinaryIndexReader(dir, trio.inverted)
	if err != nil {
		return err
	}
	defer index.Close()
	entryCount := min(headers.indexEntries, indexHeader.count)
	selected := evenlySpacedHistoryIndexes(entryCount, min(historySpaceMaxValueTxSamples, entryCount))
	local := make(map[historySpaceValueKey]uint64)
	var (
		payload             []byte
		change              rawdb.StateDomainChange
		sampledRecords      uint64
		presentRecords      uint64
		valueBytes          uint64
		currentBytes        uint64
		contentAddressBytes uint64
	)
	for _, entryIndex := range selected {
		if err := ctx.Err(); err != nil {
			return err
		}
		entry, err := readStateDomainChangeBinaryIndexEntryAt(index, entryIndex)
		if err != nil {
			return err
		}
		offset := entry.offset
		processed := false
		for row := uint64(0); row < entry.count && sampledRecords < historySpaceMaxValueRecords; row++ {
			var keyID uint32
			payload, keyID, offset, err = readStateDomainChangeBinaryRecordV6FrameRawInto(reader, offset, logicalSize, payload, &change)
			if err != nil {
				return err
			}
			key, err := contextual.v6Key(keyID)
			if err != nil {
				return err
			}
			if err := decodeStateDomainChangeBinaryAccessorKey(key, &change); err != nil {
				return err
			}
			processed = true
			sampledRecords++
			currentBytes += 5 // previous-value marker + fixed uint32 length
			contentAddressBytes++
			domainName := change.FlatDomain.String()
			domain := accumulator.stats.Domains[domainName]
			domain.Records++
			if change.PrevExists {
				presentRecords++
				length := uint64(len(change.Prev))
				valueBytes += length
				currentBytes += length
				domain.PresentRecords++
				domain.ValueBytes += length
				key := historySpaceValueKey{digest: sha256.Sum256(change.Prev), length: length}
				id, exists := local[key]
				if exists {
					accumulator.stats.SegmentDuplicateBytes += length
					domain.DuplicateBytes += length
				} else {
					id = uint64(len(local))
					local[key] = id
					accumulator.stats.SegmentUniqueValues++
					accumulator.stats.SegmentUniqueBytes += length
					contentAddressBytes += uint64(uvarintLen(length)) + length
					if _, globalExists := accumulator.global[key]; globalExists {
						accumulator.stats.CrossSegmentDuplicates += length
					} else {
						accumulator.global[key] = struct{}{}
					}
				}
				contentAddressBytes += uint64(uvarintLen(id))
			}
			accumulator.stats.Domains[domainName] = domain
			if valueBytes >= opts.SampleHistoryBytes {
				break
			}
		}
		if processed {
			accumulator.stats.SampledTxEntries++
		}
		if sampledRecords >= historySpaceMaxValueRecords || valueBytes >= opts.SampleHistoryBytes {
			break
		}
	}
	accumulator.stats.SampledRecords += sampledRecords
	accumulator.stats.PresentRecords += presentRecords
	accumulator.stats.ValueBytes += valueBytes
	accumulator.stats.CurrentBytes += currentBytes
	accumulator.stats.ContentAddressedBytes += contentAddressBytes
	return nil
}

func sampleHistorySpaceIndex(ctx context.Context, dir string, ref SegmentRef, count, limit uint64) (uint64, uint64, error) {
	file, header, err := openStateDomainChangeBinaryIndexReader(dir, ref)
	if err != nil {
		return 0, 0, err
	}
	defer file.Close()
	count = min(count, header.count)
	if count == 0 {
		return 0, 0, nil
	}
	windows := max(uint64(1), ceilDiv(min(count, limit), historySpaceTxIndexFrameEntries))
	var sampled, encoded uint64
	starts := evenlySpacedHistoryWindowStarts(count, windows, historySpaceTxIndexFrameEntries)
	for _, start := range starts {
		if err := ctx.Err(); err != nil {
			return sampled, encoded, err
		}
		end := min(count, start+historySpaceTxIndexFrameEntries)
		var previous stateDomainChangeBinaryTxOffset
		for i := start; i < end; i++ {
			entry, err := readStateDomainChangeBinaryIndexEntryAt(file, i)
			if err != nil {
				return sampled, encoded, err
			}
			encoded += uint64(uvarintLen(entry.count))
			if i != start {
				encoded += uint64(uvarintLen(entry.txNum - previous.txNum))
			}
			previous = entry
			sampled++
		}
	}
	return sampled, encoded, nil
}

func sampleHistorySpaceAccessor(ctx context.Context, dir string, ref SegmentRef, blockLimit uint64) (uint64, uint64, uint64, error) {
	file, err := os.Open(filepath.Join(dir, ref.Path))
	if err != nil {
		return 0, 0, 0, err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return 0, 0, 0, err
	}
	h, err := decodeStateDomainChangeBinaryAccessorKeyHeader(file, uint64(stat.Size()))
	if err != nil {
		return 0, 0, 0, err
	}
	if h.blockCount == 0 {
		return 0, 0, 0, nil
	}
	selected := evenlySpacedHistoryIndexes(h.blockCount, min(blockLimit, h.blockCount))
	var keys, postings, candidateBytes uint64
	for _, blockIndex := range selected {
		if err := ctx.Err(); err != nil {
			return keys, postings, candidateBytes, err
		}
		block, err := stateDomainChangeBinaryAccessorV6ReadBlockDirectoryEntry(file, h, blockIndex)
		if err != nil {
			return keys, postings, candidateBytes, err
		}
		records, err := stateDomainChangeBinaryAccessorV6ReadBlock(file, uint64(stat.Size()), h, block, uint32(blockIndex*stateDomainChangeBinaryAccessorV6BlockKeys))
		if err != nil {
			return keys, postings, candidateBytes, err
		}
		for _, record := range records {
			bytes, err := estimateHistorySpacePostingKey(file, h, record)
			if err != nil {
				return keys, postings, candidateBytes, err
			}
			keys++
			postings += uint64(record.postings)
			candidateBytes += bytes
		}
	}
	return keys, postings, candidateBytes, nil
}

func estimateHistorySpacePostingKey(file io.ReaderAt, h stateDomainChangeBinaryAccessorV6Header, record stateDomainChangeBinaryAccessorV6Record) (uint64, error) {
	count := uint64(record.postings)
	if count == 0 {
		return 0, errors.New("snapshots: key-oriented accessor key has zero postings")
	}
	if h.version == stateDomainChangeBinaryVersionV7 {
		return stateDomainChangeBinaryAccessorV7PostingListLength(file, h, record)
	}
	first, err := stateDomainChangeBinaryAccessorV6PostingAt(file, h, record, 0)
	if err != nil {
		return 0, err
	}
	bytes := uint64(uvarintLen(first.txNum-h.fromTxNum) + uvarintLen(uint64(first.recordIndex)))
	if count > 1 {
		pairs := min(count-1, historySpaceMaxPostingPairsPerKey)
		var pairBytes uint64
		var pairCount uint64
		for sample := uint64(0); sample < pairs; sample++ {
			index := uint64(1)
			if count > 2 && pairs > 1 {
				index += sample * (count - 2) / (pairs - 1)
			}
			previous, err := stateDomainChangeBinaryAccessorV6PostingAt(file, h, record, uint32(index-1))
			if err != nil {
				return 0, err
			}
			current, err := stateDomainChangeBinaryAccessorV6PostingAt(file, h, record, uint32(index))
			if err != nil {
				return 0, err
			}
			pairBytes += uint64(uvarintLen(current.txNum-previous.txNum) + uvarintLen(uint64(current.recordIndex-previous.recordIndex)))
			pairCount++
		}
		bytes += scaleHistorySpaceSample(pairBytes, pairCount, count-1)
	}
	frames := ceilDiv(count, historySpacePostingFrameEntries)
	if frames > 1 {
		bytes += frames*historySpacePostingFrameBytes + historySpacePostingFramePointer
	}
	return bytes, nil
}

func sampleHistorySpaceCompression(ctx context.Context, dir string, ref SegmentRef, byteLimit uint64) (uint64, uint64, map[uint64]uint64, historySpaceDictionarySample, error) {
	var dictionary historySpaceDictionarySample
	path := filepath.Join(dir, ref.Path)
	r, err := openCompressedBlockReaderWithCacheLimit(path, 1)
	if err != nil {
		// New fresh-sync history is compressed. Keep an explicit error here
		// instead of silently projecting an unrelated ratio over a raw file.
		return 0, 0, nil, dictionary, fmt.Errorf("inspect compressed history %s: %w", ref.Path, err)
	}
	defer r.Close()
	result := make(map[uint64]uint64, len(historySpaceBlockSizes))
	if len(r.table) == 0 {
		return 0, 0, result, dictionary, nil
	}
	maxBlock := historySpaceBlockSizes[len(historySpaceBlockSizes)-1]
	windows := max(uint64(1), ceilDiv(byteLimit, maxBlock))
	blockSpan := max(uint64(1), ceilDiv(maxBlock, historyCompressChunkSize))
	starts := evenlySpacedHistoryWindowStarts(uint64(len(r.table)), windows, blockSpan)
	enc, _, err := cbCodec()
	if err != nil {
		return 0, 0, nil, dictionary, err
	}
	var current, rawTotal uint64
	rawWindows := make([][]byte, 0, len(starts))
	for _, start := range starts {
		if err := ctx.Err(); err != nil {
			return current, rawTotal, result, dictionary, err
		}
		end := min(uint64(len(r.table)), start+blockSpan)
		var raw []byte
		for i := start; i < end; i++ {
			r.mu.Lock()
			block, readErr := r.blockBytes(int(i))
			if readErr == nil {
				raw = append(raw, block...)
			}
			r.mu.Unlock()
			if readErr != nil {
				return current, rawTotal, result, dictionary, readErr
			}
			current += r.table[i].compressedLen + compressedBlockTableEntry
		}
		rawTotal += uint64(len(raw))
		rawWindows = append(rawWindows, raw)
		for _, blockBytes := range historySpaceBlockSizes {
			for offset := 0; offset < len(raw); offset += int(blockBytes) {
				chunk := raw[offset:min(len(raw), offset+int(blockBytes))]
				result[blockBytes] += uint64(len(enc.EncodeAll(chunk, nil))) + compressedBlockTableEntry
			}
		}
	}
	dictionary = sampleHistorySpaceTrainedDictionary(rawWindows)
	return current, rawTotal, result, dictionary, nil
}

func sampleHistorySpaceTrainedDictionary(rawWindows [][]byte) historySpaceDictionarySample {
	var out historySpaceDictionarySample
	if len(rawWindows) < 2 {
		out.failed = true
		return out
	}
	training := make([][]byte, 0, (len(rawWindows)+1)/2)
	evaluation := make([][]byte, 0, len(rawWindows)/2)
	for i, raw := range rawWindows {
		if i&1 == 0 {
			training = append(training, raw)
			out.trainingRaw += uint64(len(raw))
		} else {
			evaluation = append(evaluation, raw)
			out.evaluationRaw += uint64(len(raw))
		}
	}
	if len(training) == 0 || len(evaluation) == 0 {
		out.failed = true
		return out
	}
	history := make([]byte, 0, historySpaceDictionaryBytes)
	for i, sample := range training {
		remaining := len(training) - i
		quota := (historySpaceDictionaryBytes - len(history) + remaining - 1) / remaining
		quota = min(quota, len(sample))
		start := 0
		if len(sample) > quota {
			start = i * (len(sample) - quota) / max(1, len(training)-1)
		}
		history = append(history, sample[start:start+quota]...)
	}
	dictionary, err := buildHistorySpaceDictionary(zstd.BuildDictOptions{
		ID:       1,
		Contents: training,
		History:  history,
		Offsets:  [3]int{1, 4, 8},
		Level:    zstd.SpeedDefault,
	})
	if err != nil {
		out.failed = true
		return out
	}
	encoder, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderDict(dictionary),
	)
	if err != nil {
		out.failed = true
		return out
	}
	defer encoder.Close()
	plain, _, err := cbCodec()
	if err != nil {
		out.failed = true
		return out
	}
	for _, raw := range evaluation {
		for offset := 0; offset < len(raw); offset += historyCompressChunkSize {
			chunk := raw[offset:min(len(raw), offset+historyCompressChunkSize)]
			out.plainStored += uint64(len(plain.EncodeAll(chunk, nil))) + compressedBlockTableEntry
			out.dictionaryStored += uint64(len(encoder.EncodeAll(chunk, nil))) + compressedBlockTableEntry
		}
	}
	out.dictionaryBytes = uint64(len(dictionary))
	return out
}

const historySpaceDictionaryBytes = 64 << 10

func buildHistorySpaceDictionary(options zstd.BuildDictOptions) (dictionary []byte, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			dictionary = nil
			err = fmt.Errorf("zstd dictionary builder panic: %v", recovered)
		}
	}()
	return zstd.BuildDict(options)
}

func evenlySpacedHistoryIndexes(total, count uint64) []uint64 {
	if total == 0 || count == 0 {
		return nil
	}
	if count >= total {
		out := make([]uint64, total)
		for i := range out {
			out[i] = uint64(i)
		}
		return out
	}
	out := make([]uint64, 0, count)
	var previous = uint64(math.MaxUint64)
	for i := uint64(0); i < count; i++ {
		index := (2*i + 1) * total / (2 * count)
		if index != previous {
			out = append(out, index)
			previous = index
		}
	}
	return out
}

func evenlySpacedHistoryWindowStarts(total, windows, span uint64) []uint64 {
	if total == 0 || windows == 0 {
		return nil
	}
	span = min(max(span, 1), total)
	maxStart := total - span
	if windows == 1 || maxStart == 0 {
		return []uint64{0}
	}
	starts := make([]uint64, 0, windows)
	var previous = uint64(math.MaxUint64)
	for i := uint64(0); i < windows; i++ {
		start := i * maxStart / (windows - 1)
		start -= start % span
		if start > maxStart {
			start = maxStart
		}
		if start != previous {
			starts = append(starts, start)
			previous = start
		}
	}
	return starts
}

func scaleHistorySpaceSample(value, sampled, total uint64) uint64 {
	if value == 0 || sampled == 0 || total == 0 {
		return 0
	}
	// Compute value*total/sampled without overflowing the intermediate
	// product. bits.Div64 requires the high word to be below the divisor;
	// otherwise the exact quotient does not fit in uint64 and is saturated.
	hi, lo := bits.Mul64(value, total)
	if hi >= sampled {
		return math.MaxUint64
	}
	quotient, _ := bits.Div64(hi, lo, sampled)
	return quotient
}

func saturatingHistorySpaceSum(values ...uint64) uint64 {
	var total uint64
	for _, value := range values {
		var carry uint64
		total, carry = bits.Add64(total, value, 0)
		if carry != 0 {
			return math.MaxUint64
		}
	}
	return total
}

func uvarintLen(value uint64) int {
	var scratch [binary.MaxVarintLen64]byte
	return binary.PutUvarint(scratch[:], value)
}
