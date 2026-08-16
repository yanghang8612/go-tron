package snapshots

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// HistoryV7MigrationOptions controls the offline conversion of active state
// history trios. MaxTrios bounds newly rewritten trios; zero converts all
// remaining trios. The migration commits one verified trio at a time, so a
// later invocation resumes by skipping already-current active trios.
type HistoryV7MigrationOptions struct {
	Context    context.Context
	MaxTrios   uint64
	OnProgress func(HistoryV7MigrationProgress)
}

type HistoryV7MigrationProgress struct {
	TotalTrios        int
	CompletedTrios    int
	MigratedTrios     int
	CurrentHistory    string
	FromTxNum         uint64
	ToTxNum           uint64
	ActiveBytesBefore uint64
	ActiveBytesAfter  uint64
	Elapsed           time.Duration
}

type HistoryV7MigrationResult struct {
	SnapshotDir       string  `json:"snapshotDir"`
	TotalTrios        int     `json:"totalTrios"`
	AlreadyCurrent    int     `json:"alreadyCurrentTrios"`
	MigratedTrios     int     `json:"migratedTrios"`
	RemainingTrios    int     `json:"remainingTrios"`
	ActiveBytesBefore uint64  `json:"activeBytesBefore"`
	ActiveBytesAfter  uint64  `json:"activeBytesAfter"`
	RetiredBytesAdded uint64  `json:"retiredBytesAdded"`
	ElapsedSeconds    float64 `json:"elapsedSeconds"`
}

// MigrateHistoryV7 rewrites every non-current active state-domain-change trio
// using the current 128 KiB compressed history, delta-framed transaction index,
// and delta-framed key accessor. The node must be stopped: this function owns
// manifest publication for the duration of the conversion.
func MigrateHistoryV7(dir string, opts HistoryV7MigrationOptions) (*HistoryV7MigrationResult, error) {
	if dir == "" {
		return nil, errors.New("snapshots: history V7 migration directory is empty")
	}
	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		return nil, err
	}
	cfg, ok := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	if !ok || cfg.CompactHistory == nil {
		return nil, errors.New("snapshots: state-domain-change history compactor is unavailable")
	}
	candidates := historyCompactionCandidates(manifest, cfg)
	historyCount := activeBinaryHistoryCount(manifest, cfg)
	if len(candidates) != historyCount {
		return nil, fmt.Errorf("snapshots: found %d active binary history segments but only %d complete history trios", historyCount, len(candidates))
	}
	started := time.Now()
	result := &HistoryV7MigrationResult{
		SnapshotDir: dir,
		TotalTrios:  len(candidates),
	}
	for _, candidate := range candidates {
		bytesBefore := historyCandidateBytes(candidate)
		result.ActiveBytesBefore += bytesBefore
		current, err := historyCandidateUsesCurrentV7(dir, candidate)
		if err != nil {
			return nil, fmt.Errorf("snapshots: inspect history trio %q: %w", candidate.history.Path, err)
		}
		if current {
			result.AlreadyCurrent++
			continue
		}
		result.RemainingTrios++
	}

	completed := result.AlreadyCurrent
	migratedThisRun := uint64(0)
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		current, err := historyCandidateUsesCurrentV7(dir, candidate)
		if err != nil {
			return result, fmt.Errorf("snapshots: inspect history trio %q: %w", candidate.history.Path, err)
		}
		if current {
			continue
		}
		if opts.MaxTrios != 0 && migratedThisRun >= opts.MaxTrios {
			break
		}
		selection := historyCompactionSelection{
			candidates:       []historyCompactionCandidate{candidate},
			fromTxNum:        candidate.history.FromTxNum,
			toTxNum:          candidate.history.ToTxNum,
			aggregationSteps: candidate.history.effectiveAggregationSteps(),
		}
		refs, err := cfg.CompactHistory(dir, cfg, selection)
		if err != nil {
			return result, fmt.Errorf("snapshots: rewrite history trio %q: %w", candidate.history.Path, err)
		}
		if _, err := NewAggregator(dir).Integrate(selection.fromTxNum, selection.toTxNum, refs); err != nil {
			for _, ref := range refs {
				_ = os.Remove(filepath.Join(dir, ref.Path))
			}
			return result, fmt.Errorf("snapshots: publish rewritten history trio %q: %w", candidate.history.Path, err)
		}
		oldBytes := historyCandidateBytes(candidate)
		newBytes := segmentRefsBytes(refs)
		result.MigratedTrios++
		result.RemainingTrios--
		result.RetiredBytesAdded += oldBytes
		migratedThisRun++
		completed++
		if opts.OnProgress != nil {
			opts.OnProgress(HistoryV7MigrationProgress{
				TotalTrios:        result.TotalTrios,
				CompletedTrios:    completed,
				MigratedTrios:     result.MigratedTrios,
				CurrentHistory:    candidate.history.Path,
				FromTxNum:         candidate.history.FromTxNum,
				ToTxNum:           candidate.history.ToTxNum,
				ActiveBytesBefore: oldBytes,
				ActiveBytesAfter:  newBytes,
				Elapsed:           time.Since(started),
			})
		}
	}
	finalManifest, err := LoadProductionManifest(dir)
	if err != nil {
		return result, err
	}
	finalCandidates := historyCompactionCandidates(finalManifest, cfg)
	result.ActiveBytesAfter = 0
	result.RemainingTrios = 0
	for _, candidate := range finalCandidates {
		result.ActiveBytesAfter += historyCandidateBytes(candidate)
		current, err := historyCandidateUsesCurrentV7(dir, candidate)
		if err != nil {
			return result, err
		}
		if !current {
			result.RemainingTrios++
		}
	}
	result.ElapsedSeconds = time.Since(started).Seconds()
	return result, nil
}

func activeBinaryHistoryCount(manifest *Manifest, cfg DomainCfg) int {
	count := 0
	if manifest == nil {
		return count
	}
	for _, ref := range manifest.Segments {
		if ref.normalizedDataset() == cfg.Dataset && ref.Kind == SegmentHistory && cfg.IsHistoryBinarySegmentPath(ref.Path) {
			count++
		}
	}
	return count
}

func historyCandidateUsesCurrentV7(dir string, candidate historyCompactionCandidate) (bool, error) {
	indexRef, ok := historyCompactionCompanion(candidate, SegmentInverted)
	if !ok {
		return false, errors.New("missing transaction index companion")
	}
	accessorRef, ok := historyCompactionCompanion(candidate, SegmentAccessor)
	if !ok {
		return false, errors.New("missing key accessor companion")
	}
	history, historyHeader, _, err := openStateDomainChangeBinarySegmentReader(dir, candidate.history)
	if err != nil {
		return false, err
	}
	if err := history.Close(); err != nil {
		return false, err
	}
	currentCompression, err := historyUsesCurrentCompression(filepath.Join(dir, candidate.history.Path))
	if err != nil {
		return false, err
	}
	index, indexHeader, err := openStateDomainChangeBinaryIndexReader(dir, indexRef)
	if err != nil {
		return false, err
	}
	if err := index.Close(); err != nil {
		return false, err
	}
	accessor, accessorHeader, _, err := openStateDomainChangeBinaryAccessorReader(dir, accessorRef)
	if err != nil {
		return false, err
	}
	if err := accessor.Close(); err != nil {
		return false, err
	}
	return currentCompression &&
		historyHeader.version == stateDomainChangeBinaryVersionV6 &&
		indexHeader.version == stateDomainChangeBinaryIndexCurrentVersion &&
		accessorHeader.version == stateDomainChangeBinaryVersionV7, nil
}

func historyUsesCurrentCompression(path string) (bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return false, err
	}
	var magic [8]byte
	_, readErr := io.ReadFull(file, magic[:])
	closeErr := file.Close()
	if readErr != nil {
		return false, readErr
	}
	if closeErr != nil {
		return false, closeErr
	}
	if string(magic[:]) != compressedBlockMagic {
		return false, nil
	}
	reader, err := openCompressedBlockReaderWithCacheLimit(path, 1)
	if err != nil {
		return false, err
	}
	defer reader.Close()
	if len(reader.table) <= 1 {
		return reader.uncSize <= uint64(historyCompressChunkSize), nil
	}
	for i, block := range reader.table {
		end := reader.uncSize
		if i+1 < len(reader.table) {
			end = reader.table[i+1].uncompressedStart
		}
		if end < block.uncompressedStart {
			return false, errors.New("compressed history block bounds regress")
		}
		span := end - block.uncompressedStart
		if i+1 < len(reader.table) && span != uint64(historyCompressChunkSize) || i+1 == len(reader.table) && span > uint64(historyCompressChunkSize) {
			return false, nil
		}
	}
	return true, nil
}

func historyCandidateBytes(candidate historyCompactionCandidate) uint64 {
	return candidate.history.Size + segmentRefsBytes(candidate.companions)
}

func segmentRefsBytes(refs []SegmentRef) uint64 {
	var total uint64
	for _, ref := range refs {
		total += ref.Size
	}
	return total
}
