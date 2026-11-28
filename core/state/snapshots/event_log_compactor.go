package snapshots

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/common"
)

const (
	defaultEventLogCompactionTargetBlocks       = uint64(65_536)
	defaultEventLogCompactionMaxSourceSegments  = 128
	defaultEventLogCompactionMaxSourceMainBytes = uint64(256 << 20)
	defaultEventLogCompactionMinV5MergeSegments = 16
	defaultEventLogCompactionMaxSpan            = uint64(320_000)
	eventLogCompactionMetricsPrefix             = defaultColdSnapshotMetrics + "event_log_compaction/"
)

var (
	eventLogCompactionMergesCounter        = metrics.NewRegisteredCounter(eventLogCompactionMetricsPrefix+"merges", nil)
	eventLogCompactionSourceSegsCounter    = metrics.NewRegisteredCounter(eventLogCompactionMetricsPrefix+"source_segments", nil)
	eventLogCompactionBytesBeforeCounter   = metrics.NewRegisteredCounter(eventLogCompactionMetricsPrefix+"bytes_before", nil)
	eventLogCompactionBytesAfterCounter    = metrics.NewRegisteredCounter(eventLogCompactionMetricsPrefix+"bytes_after", nil)
	eventLogCompactionBytesSavedCounter    = metrics.NewRegisteredCounter(eventLogCompactionMetricsPrefix+"bytes_saved", nil)
	eventLogCompactionDeletedFilesCounter  = metrics.NewRegisteredCounter(eventLogCompactionMetricsPrefix+"deleted_files", nil)
	eventLogCompactionLastDurationGauge    = metrics.NewRegisteredGauge(eventLogCompactionMetricsPrefix+"last_duration_ns", nil)
)

// EventLogCompactionConfig controls bounded compaction of cold event-log pairs
// into 65k-block V5 main segments + V3 split-block bloom filter sidecars.
type EventLogCompactionConfig struct {
	TargetBlocks           uint64
	MaxSourceSegments      int
	MaxSourceMainBytes     uint64
	MinV5MergeSegments     int
	RetainObsoleteSegments bool
	DryRun                 bool
	VerificationCache      *ChainFreezerVerificationCache
	// MinFromBlockHint lets a long-lived runner skip already-settled V5+V3 prefixes
	// without reopening their file headers on every pass. When empty or nothing
	// is found after the hint, callers only probe the prefix if needed.
	MinFromBlockHint uint64
}

func (c EventLogCompactionConfig) withDefaults() EventLogCompactionConfig {
	if c.TargetBlocks == 0 {
		c.TargetBlocks = defaultEventLogCompactionTargetBlocks
	}
	if c.TargetBlocks > defaultEventLogCompactionMaxSpan {
		c.TargetBlocks = defaultEventLogCompactionMaxSpan
	}
	if c.MaxSourceSegments <= 0 {
		c.MaxSourceSegments = defaultEventLogCompactionMaxSourceSegments
	}
	if c.MaxSourceMainBytes == 0 {
		c.MaxSourceMainBytes = defaultEventLogCompactionMaxSourceMainBytes
	}
	if c.MinV5MergeSegments <= 0 {
		c.MinV5MergeSegments = defaultEventLogCompactionMinV5MergeSegments
	}
	return c
}

// EventLogCompactionResult summarizes one bounded event-log compaction pass.
type EventLogCompactionResult struct {
	Compacted           bool         `json:"compacted"`
	DryRun              bool         `json:"dryRun"`
	FromBlock           uint64       `json:"fromBlock"`
	ToBlock             uint64       `json:"toBlock"`
	SourceSegments      int          `json:"sourceSegments"`
	SourceIndexSegments int          `json:"sourceIndexSegments"`
	SourceMainBytes     uint64       `json:"sourceMainBytes"`
	SourceIndexBytes    uint64       `json:"sourceIndexBytes"`
	NewMainBytes        uint64       `json:"newMainBytes"`
	NewIndexBytes       uint64       `json:"newIndexBytes"`
	BytesSaved          int64        `json:"bytesSaved"`
	DeletedFiles        int          `json:"deletedFiles"`
	RetiredTrimmed      int          `json:"retiredTrimmed"`
	Generation          uint64       `json:"generation"`
	Segments            []SegmentRef `json:"segments,omitempty"`
}

type eventLogCompactionSelection struct {
	sources         []SegmentRef
	replacedIndexes []SegmentRef
	fromBlock       uint64
	toBlock         uint64
	sourceMainBytes uint64
	sourceIdxBytes  uint64
}

// HasPotentialEventLogCompaction performs a cheap manifest + 8-byte magic probe
// to report whether at least one event-log compaction batch is ready.
func HasPotentialEventLogCompaction(dir string, cfg EventLogCompactionConfig) (bool, error) {
	manifest, err := loadProductionManifestReadOnly(dir)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	_, ok, err := selectEventLogCompactionRun(dir, manifest, cfg.withDefaults())
	return ok, err
}

// CompactEventLogs compacts one bounded contiguous run of legacy/small event-log
// segments into a verified V5 main segment + V3 bloom filter sidecar.
func CompactEventLogs(dir string, cfg EventLogCompactionConfig) (EventLogCompactionResult, error) {
	return CompactEventLogsContext(context.Background(), dir, cfg)
}

// CompactEventLogsContext is the cancellable form of CompactEventLogs.
func CompactEventLogsContext(ctx context.Context, dir string, cfg EventLogCompactionConfig) (EventLogCompactionResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return EventLogCompactionResult{}, err
	}
	if dir == "" {
		return EventLogCompactionResult{}, errors.New("snapshots: event-log compaction directory is empty")
	}
	cfg = cfg.withDefaults()
	started := time.Now()

	manifest, err := loadProductionManifestReadOnly(dir)
	if err != nil {
		return EventLogCompactionResult{}, err
	}
	sel, ok, err := selectEventLogCompactionRun(dir, manifest, cfg)
	if err != nil {
		return EventLogCompactionResult{}, err
	}
	if !ok {
		return EventLogCompactionResult{Generation: manifest.Generation}, nil
	}
	if cfg.DryRun {
		return EventLogCompactionResult{
			Compacted:           false,
			DryRun:              true,
			FromBlock:           sel.fromBlock,
			ToBlock:             sel.toBlock,
			SourceSegments:      len(sel.sources),
			SourceIndexSegments: len(sel.replacedIndexes),
			SourceMainBytes:     sel.sourceMainBytes,
			SourceIndexBytes:    sel.sourceIdxBytes,
			Generation:          manifest.Generation,
		}, nil
	}
	if err := ctx.Err(); err != nil {
		return EventLogCompactionResult{}, err
	}

	pinned := newPinnedManager(dir, manifestLookupView(manifest), cfg.VerificationCache)
	reader, release, err := pinEventLogRewriteReader(pinned)
	if err != nil {
		return EventLogCompactionResult{}, err
	}
	released := false
	releaseReader := func() {
		if !released {
			released = true
			release()
		}
	}
	defer releaseReader()

	v5Ref, idxRef, err := buildAnchoredEventLogV5Pair(reader, dir, sel.fromBlock, sel.toBlock, sel.sources)
	releaseReader()
	if err != nil {
		return EventLogCompactionResult{}, err
	}
	var maxSourceSteps uint64
	for _, s := range sel.sources {
		if s.AggregationSteps > maxSourceSteps {
			maxSourceSteps = s.AggregationSteps
		}
	}
	nextSteps := maxSourceSteps + 1
	v5Ref.AggregationSteps = nextSteps
	idxRef.AggregationSteps = nextSteps
	if err := ctx.Err(); err != nil {
		return EventLogCompactionResult{}, err
	}
	if err := verifyEventLogIndexSegmentAgainstEventLogs(dir, idxRef, []SegmentRef{v5Ref}); err != nil {
		return EventLogCompactionResult{}, fmt.Errorf("snapshots: verify compacted V5+V3 event-log pair: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return EventLogCompactionResult{}, err
	}

	publishRefs := []SegmentRef{v5Ref, idxRef}
	published, err := publishEventLogCompactionManifest(dir, sel, publishRefs)
	if err != nil {
		return EventLogCompactionResult{}, err
	}
	if cfg.VerificationCache != nil {
		if err := cfg.VerificationCache.recordTrustedEventLogs(dir, idxRef, []SegmentRef{v5Ref}); err != nil {
			return EventLogCompactionResult{}, fmt.Errorf("snapshots: record compacted event-log verification proof: %w", err)
		}
		cfg.VerificationCache.pruneEventLogIndexRecords(sel.replacedIndexes)
	}

	var deletedFiles, retiredTrimmed int
	if !cfg.RetainObsoleteSegments {
		deletedPaths, derr := deleteObsoleteEventLogCompactionFiles(dir, sel.sources, sel.replacedIndexes, publishRefs)
		if derr != nil {
			return EventLogCompactionResult{}, derr
		}
		deletedFiles = len(deletedPaths)
		if deletedFiles > 0 {
			retiredTrimmed, err = trimUnlinkedEventLogRetiredManifest(dir, deletedPaths)
			if err != nil {
				return EventLogCompactionResult{}, err
			}
		}
	}

	beforeTotal := sel.sourceMainBytes + sel.sourceIdxBytes
	afterTotal := v5Ref.Size + idxRef.Size
	saved := signedByteSavings(beforeTotal, afterTotal)
	res := EventLogCompactionResult{
		Compacted:           true,
		FromBlock:           sel.fromBlock,
		ToBlock:             sel.toBlock,
		SourceSegments:      len(sel.sources),
		SourceIndexSegments: len(sel.replacedIndexes),
		SourceMainBytes:     sel.sourceMainBytes,
		SourceIndexBytes:    sel.sourceIdxBytes,
		NewMainBytes:        v5Ref.Size,
		NewIndexBytes:       idxRef.Size,
		BytesSaved:          saved,
		DeletedFiles:        deletedFiles,
		RetiredTrimmed:      retiredTrimmed,
		Generation:          published.Generation,
		Segments:            publishRefs,
	}
	eventLogCompactionMergesCounter.Inc(1)
	eventLogCompactionSourceSegsCounter.Inc(int64(res.SourceSegments))
	eventLogCompactionBytesBeforeCounter.Inc(coldSnapshotUintGauge(beforeTotal))
	eventLogCompactionBytesAfterCounter.Inc(coldSnapshotUintGauge(afterTotal))
	if saved > 0 {
		eventLogCompactionBytesSavedCounter.Inc(saved)
	}
	if deletedFiles > 0 {
		eventLogCompactionDeletedFilesCounter.Inc(int64(deletedFiles))
	}
	eventLogCompactionLastDurationGauge.Update(coldSnapshotPhaseDuration(started).Nanoseconds())
	return res, nil
}

func buildAnchoredEventLogV5Pair(reader rawdbEventLogReaderWithAnchor, dir string, fromBlock, toBlock uint64, sources []SegmentRef) (SegmentRef, SegmentRef, error) {
	requireAnchor := false
	for _, ref := range sources {
		if ref.SourceBlockHash != "" {
			requireAnchor = true
			break
		}
	}
	var anchor common.Hash
	if requireAnchor {
		var err error
		anchor, err = requireEventLogReaderAnchor(reader, fromBlock, toBlock)
		if err != nil {
			return SegmentRef{}, SegmentRef{}, err
		}
	}
	v5Ref, idxRef, err := buildEventLogV5PairFromReaderWorkers(reader, dir, fromBlock, toBlock, eventLogPayloadWorkers())
	if err != nil {
		return SegmentRef{}, SegmentRef{}, err
	}
	if requireAnchor {
		current, err := requireEventLogReaderAnchor(reader, fromBlock, toBlock)
		if err != nil {
			return SegmentRef{}, SegmentRef{}, err
		}
		if current != anchor {
			return SegmentRef{}, SegmentRef{}, fmt.Errorf("snapshots: event-log compaction terminal source changed at block %d", toBlock)
		}
		v5Ref.SourceBlockHash = anchor.Hex()
	}
	return v5Ref, idxRef, nil
}

type rawdbEventLogReaderWithAnchor interface {
	EventLogRangeCovered(fromBlock, toBlock uint64) (bool, error)
	IterateEventLogs(fromBlock, toBlock uint64, filter EventLogFilter, fn func(EventLog) (bool, error)) error
}

func selectEventLogCompactionRun(dir string, manifest *Manifest, cfg EventLogCompactionConfig) (eventLogCompactionSelection, bool, error) {
	if manifest == nil {
		return eventLogCompactionSelection{}, false, nil
	}
	refs := eventLogRefs(manifest)
	if len(refs) == 0 {
		return eventLogCompactionSelection{}, false, nil
	}
	idxRefs := eventLogIndexRefs(manifest)

	mainVersionCache := make(map[int]uint32)
	mainVersion := func(i int) (uint32, error) {
		if v, ok := mainVersionCache[i]; ok {
			return v, nil
		}
		v, err := eventLogSegmentVersionFast(dir, refs[i])
		if err != nil {
			return 0, err
		}
		mainVersionCache[i] = v
		return v, nil
	}
	idxVersionCache := make(map[string]uint32)
	idxVersion := func(ref SegmentRef) (uint32, error) {
		if v, ok := idxVersionCache[ref.Path]; ok {
			return v, nil
		}
		v, err := eventLogIndexVersionFast(dir, ref)
		if err != nil {
			return 0, err
		}
		idxVersionCache[ref.Path] = v
		return v, nil
	}

	isSettled := func(i int) (bool, error) {
		ver, err := mainVersion(i)
		if err != nil || ver != EventLogSegmentV5Version {
			return false, err
		}
		overlapping := eventLogRefsForIndex(idxRefs, refs[i])
		if len(overlapping) != 1 || overlapping[0].FromTxNum != refs[i].FromTxNum || overlapping[0].ToTxNum != refs[i].ToTxNum {
			return false, nil
		}
		iVer, err := idxVersion(overlapping[0])
		if err != nil || iVer != EventLogIndexV3Version {
			return false, err
		}
		span := refs[i].ToTxNum - refs[i].FromTxNum + 1
		if refs[i].ToTxNum-refs[i].FromTxNum == math.MaxUint64 {
			span = math.MaxUint64
		}
		return span >= cfg.TargetBlocks || refs[i].Size >= cfg.MaxSourceMainBytes/2, nil
	}

	startIdx := 0
	if cfg.MinFromBlockHint > 0 {
		startIdx = sort.Search(len(refs), func(i int) bool {
			return refs[i].FromTxNum >= cfg.MinFromBlockHint
		})
	}

	for start := startIdx; start < len(refs); start++ {
		settled, err := isSettled(start)
		if err != nil {
			return eventLogCompactionSelection{}, false, err
		}
		if settled {
			continue
		}
		// Never start inside the middle of a multi-segment legacy index boundary.
		if crossesStartingIndexBoundary(idxRefs, refs[start].FromTxNum) {
			continue
		}
		end := start
		sumBytes := refs[start].Size
		startVer, err := mainVersion(start)
		if err != nil {
			return eventLogCompactionSelection{}, false, err
		}
		hasLegacy := startVer != EventLogSegmentV5Version

		for next := start + 1; next < len(refs); next++ {
			if next-start+1 > cfg.MaxSourceSegments {
				break
			}
			if refs[next-1].ToTxNum == math.MaxUint64 || refs[next].FromTxNum != refs[next-1].ToTxNum+1 {
				break
			}
			nextSettled, err := isSettled(next)
			if err != nil {
				return eventLogCompactionSelection{}, false, err
			}
			if nextSettled {
				break
			}
			if refs[next].ToTxNum-refs[start].FromTxNum >= cfg.TargetBlocks {
				break
			}
			if refs[next].Size > cfg.MaxSourceMainBytes-sumBytes {
				break
			}
			end = next
			sumBytes += refs[next].Size
			nextVer, err := mainVersion(next)
			if err != nil {
				return eventLogCompactionSelection{}, false, err
			}
			if nextVer != EventLogSegmentV5Version {
				hasLegacy = true
			}
		}

		// Trim end if any overlapping index extends past refs[end].ToTxNum.
		for end >= start {
			toBlock := refs[end].ToTxNum
			crossed := false
			for _, idx := range eventLogRefsForIndex(idxRefs, SegmentRef{FromTxNum: refs[start].FromTxNum, ToTxNum: toBlock}) {
				if idx.FromTxNum < refs[start].FromTxNum || idx.ToTxNum > toBlock {
					crossed = true
					break
				}
			}
			if !crossed {
				break
			}
			end--
		}
		if end < start {
			continue
		}

		fromBlock, toBlock := refs[start].FromTxNum, refs[end].ToTxNum
		overlapping := eventLogRefsForIndex(idxRefs, SegmentRef{FromTxNum: fromBlock, ToTxNum: toBlock})
		for _, idx := range overlapping {
			iVer, err := idxVersion(idx)
			if err != nil {
				return eventLogCompactionSelection{}, false, err
			}
			if iVer != EventLogIndexV3Version {
				hasLegacy = true
				break
			}
		}
		if len(overlapping) == 0 || !eventLogRangeCoveredByRefs(overlapping, fromBlock, toBlock) {
			hasLegacy = true
		}

		count := end - start + 1
		if !hasLegacy {
			span := toBlock - fromBlock + 1
			if toBlock-fromBlock == math.MaxUint64 {
				span = math.MaxUint64
			}
			if count < 2 {
				continue
			}
			if count < cfg.MinV5MergeSegments && span < cfg.TargetBlocks/2 && sumBytes < cfg.MaxSourceMainBytes/2 {
				continue
			}
		}

		sel := eventLogCompactionSelection{
			sources:         append([]SegmentRef(nil), refs[start:end+1]...),
			replacedIndexes: append([]SegmentRef(nil), overlapping...),
			fromBlock:       fromBlock,
			toBlock:         toBlock,
			sourceMainBytes: sumBytes,
		}
		for _, idx := range overlapping {
			sel.sourceIdxBytes += idx.Size
		}
		return sel, true, nil
	}
	return eventLogCompactionSelection{}, false, nil
}

func crossesStartingIndexBoundary(idxRefs []SegmentRef, fromBlock uint64) bool {
	pos := sort.Search(len(idxRefs), func(i int) bool {
		return idxRefs[i].ToTxNum >= fromBlock
	})
	for i := pos; i < len(idxRefs) && idxRefs[i].FromTxNum < fromBlock; i++ {
		if idxRefs[i].ToTxNum >= fromBlock {
			return true
		}
	}
	return false
}

func eventLogSegmentVersionFast(dir string, ref SegmentRef) (uint32, error) {
	f, err := os.Open(filepath.Join(dir, ref.Path))
	if err != nil {
		return 0, err
	}
	defer f.Close()
	var magic [8]byte
	if _, err := io.ReadFull(f, magic[:]); err != nil {
		return 0, err
	}
	switch magic {
	case eventLogMagicV1:
		return 1, nil
	case eventLogMagicV2:
		return EventLogSegmentVersion, nil
	case eventLogMagicV3:
		return EventLogSegmentV3Version, nil
	case eventLogMagicV4:
		return EventLogSegmentV4Version, nil
	case eventLogMagicV5:
		return EventLogSegmentV5Version, nil
	default:
		return 0, fmt.Errorf("snapshots: event log %q has invalid magic", ref.Path)
	}
}

func eventLogIndexVersionFast(dir string, ref SegmentRef) (uint32, error) {
	f, err := os.Open(filepath.Join(dir, ref.Path))
	if err != nil {
		return 0, err
	}
	defer f.Close()
	var magic [8]byte
	if _, err := io.ReadFull(f, magic[:]); err != nil {
		return 0, err
	}
	switch magic {
	case eventLogIndexMagic:
		return EventLogIndexVersion, nil
	case eventLogIndexMagicV3:
		return EventLogIndexV3Version, nil
	default:
		return 0, fmt.Errorf("snapshots: event log index %q has invalid magic", ref.Path)
	}
}

func publishEventLogCompactionManifest(dir string, sel eventLogCompactionSelection, publishRefs []SegmentRef) (*Manifest, error) {
	manifestPublicationMu.Lock()
	defer manifestPublicationMu.Unlock()

	current, err := loadProductionManifestReadOnly(dir)
	if err != nil {
		return nil, err
	}
	currentEvents := eventLogRefsInRange(current, sel.fromBlock, sel.toBlock)
	currentIndexes := eventLogIndexRefsInRange(current, sel.fromBlock, sel.toBlock)
	if !reflect.DeepEqual(currentEvents, sel.sources) || !reflect.DeepEqual(currentIndexes, sel.replacedIndexes) {
		return nil, fmt.Errorf("snapshots: event-log refs in [%d,%d] changed concurrently at manifest generation %d", sel.fromBlock, sel.toBlock, current.Generation)
	}

	replacedSet := make(map[string]struct{}, len(sel.sources)+len(sel.replacedIndexes))
	for _, ref := range sel.sources {
		replacedSet[ref.Path] = struct{}{}
	}
	for _, ref := range sel.replacedIndexes {
		replacedSet[ref.Path] = struct{}{}
	}

	segments := make([]SegmentRef, 0, len(current.Segments)+len(publishRefs))
	retired := append([]SegmentRef(nil), current.Retired...)
	for _, ref := range current.Segments {
		if _, remove := replacedSet[ref.Path]; remove {
			retired = append(retired, ref)
			continue
		}
		segments = append(segments, ref)
	}
	segments = append(segments, publishRefs...)

	next := &Manifest{
		Version:                  CurrentManifestVersion,
		CommitmentScheme:         current.CommitmentScheme,
		ElementVersion:           current.ElementVersion,
		Generation:               current.Generation + 1,
		PublishedUnix:            time.Now().Unix(),
		VisibleTxStart:           current.VisibleTxStart,
		VisibleTxEnd:             current.VisibleTxEnd,
		LatestStateTxNum:         current.LatestStateTxNum,
		HistoryStagingResetEpoch: current.HistoryStagingResetEpoch,
		SourceRows:               current.SourceRows,
		Chain:                    cloneChainIdentity(current.Chain),
		Progress:                 cloneProgress(current.Progress),
		Segments:                 segments,
		Retired:                  dedupeSegmentRefsInPlace(retired),
	}
	if err := publishManifestLocked(dir, next); err != nil {
		return nil, err
	}
	return next, nil
}

func eventLogRefsInRange(manifest *Manifest, fromBlock, toBlock uint64) []SegmentRef {
	var out []SegmentRef
	for _, ref := range eventLogRefs(manifest) {
		if ref.ToTxNum < fromBlock || ref.FromTxNum > toBlock {
			continue
		}
		out = append(out, ref)
	}
	return out
}

func eventLogIndexRefsInRange(manifest *Manifest, fromBlock, toBlock uint64) []SegmentRef {
	var out []SegmentRef
	for _, ref := range eventLogIndexRefs(manifest) {
		if ref.ToTxNum < fromBlock || ref.FromTxNum > toBlock {
			continue
		}
		out = append(out, ref)
	}
	return out
}

func deleteObsoleteEventLogCompactionFiles(dir string, sources, replacedIndexes, newRefs []SegmentRef) (map[string]struct{}, error) {
	release, ok := tryHistoryRetirement()
	if !ok {
		return nil, nil
	}
	defer release()

	manifest, err := loadProductionManifestReadOnly(dir)
	if err != nil {
		return nil, err
	}
	keep := make(map[string]struct{}, len(manifest.Segments)+len(newRefs))
	for _, ref := range manifest.Segments {
		keep[ref.Path] = struct{}{}
	}
	for _, ref := range newRefs {
		if ref.Path != "" {
			keep[ref.Path] = struct{}{}
		}
	}
	published, err := LoadPublishedSnapshotManifests(dir)
	if err != nil {
		return nil, fmt.Errorf("snapshots: load published manifest leases before event-log cleanup: %w", err)
	}
	for _, generation := range published {
		for _, ref := range generation.Manifest.Segments {
			keep[ref.Path] = struct{}{}
		}
	}

	deleted := make(map[string]struct{}, len(sources)+len(replacedIndexes))
	for _, ref := range append(append([]SegmentRef(nil), sources...), replacedIndexes...) {
		if ref.Path == "" {
			continue
		}
		if _, protected := keep[ref.Path]; protected {
			continue
		}
		abs := filepath.Join(dir, ref.Path)
		if err := os.Remove(abs); err != nil && !os.IsNotExist(err) {
			return deleted, fmt.Errorf("snapshots: remove obsolete event-log file %q: %w", ref.Path, err)
		}
		deleted[ref.Path] = struct{}{}
	}
	return deleted, nil
}

func trimUnlinkedEventLogRetiredManifest(dir string, deletedPaths map[string]struct{}) (int, error) {
	if len(deletedPaths) == 0 {
		return 0, nil
	}
	manifestPublicationMu.Lock()
	defer manifestPublicationMu.Unlock()

	current, err := loadProductionManifestReadOnly(dir)
	if err != nil {
		return 0, err
	}
	active := make(map[string]struct{}, len(current.Segments))
	for _, ref := range current.Segments {
		active[ref.Path] = struct{}{}
	}
	keptRetired := make([]SegmentRef, 0, len(current.Retired))
	trimmed := 0
	for _, ref := range current.Retired {
		if _, wasDeleted := deletedPaths[ref.Path]; wasDeleted {
			if _, isActive := active[ref.Path]; !isActive {
				trimmed++
				continue
			}
		}
		keptRetired = append(keptRetired, ref)
	}
	if trimmed == 0 {
		return 0, nil
	}
	sort.Slice(keptRetired, func(i, j int) bool {
		return keptRetired[i].Path < keptRetired[j].Path
	})
	next := cloneManifest(current)
	next.Generation = current.Generation + 1
	next.PublishedUnix = time.Now().Unix()
	next.Retired = dedupeSegmentRefsInPlace(keptRetired)
	if err := publishManifestLocked(dir, next); err != nil {
		return 0, err
	}
	return trimmed, nil
}
