package snapshots

import (
	"errors"
	"fmt"
	"reflect"
)

type EventLogV3MigrationOptions struct {
	FromBlock  uint64
	ToBlock    uint64
	ToBlockSet bool
	Merge      uint64
	Publish    bool
}

type EventLogV3MigrationResult struct {
	SourceGeneration uint64                  `json:"sourceGeneration"`
	Published        bool                    `json:"published"`
	Generation       uint64                  `json:"generation"`
	FromBlock        uint64                  `json:"fromBlock"`
	ToBlock          uint64                  `json:"toBlock"`
	SourceSegments   int                     `json:"sourceSegments"`
	SourceMainBytes  uint64                  `json:"sourceMainBytes"`
	SourceIndexBytes uint64                  `json:"sourceIndexBytes"`
	V3MainBytes      uint64                  `json:"v3MainBytes"`
	V3IndexBytes     uint64                  `json:"v3IndexBytes"`
	MainSavingsBytes int64                   `json:"mainSavingsBytes"`
	V3Physical       EventLogV3PhysicalStats `json:"v3Physical"`
	Segments         []SegmentRef            `json:"segments"`
}

// MigrateEventLogsV3 rewrites exact active segment boundaries from one pinned
// production manifest. Publication is opt-in and is refused when the active
// manifest changes while the new immutable files are being built.
func MigrateEventLogsV3(dir string, opts EventLogV3MigrationOptions) (*EventLogV3MigrationResult, error) {
	if dir == "" {
		return nil, errors.New("snapshots: V3 migration directory is empty")
	}
	if opts.Merge == 0 {
		opts.Merge = 8
	}
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		return nil, err
	}
	sources, err := selectEventLogV3MigrationSources(manifest, opts)
	if err != nil {
		return nil, err
	}
	fromBlock, toBlock := sources[0].FromTxNum, sources[len(sources)-1].ToTxNum
	for _, indexRef := range eventLogIndexRefs(manifest) {
		if indexRef.ToTxNum < fromBlock || indexRef.FromTxNum > toBlock {
			continue
		}
		if indexRef.FromTxNum < fromBlock || indexRef.ToTxNum > toBlock {
			return nil, fmt.Errorf("snapshots: event-log-index %q crosses migration range [%d,%d]; migrate its full [%d,%d] range", indexRef.Path, fromBlock, toBlock, indexRef.FromTxNum, indexRef.ToTxNum)
		}
	}
	pinned, err := OpenPinnedManager(dir, manifest)
	if err != nil {
		return nil, err
	}
	v3Ref, err := BuildEventLogV3SegmentFromReader(pinned, dir, EventLogSegmentPath(fromBlock, toBlock), fromBlock, toBlock)
	if err != nil {
		return nil, err
	}
	indexRef, err := writeFreshEventLogV3Index(dir, v3Ref, EventLogIndexSegmentPath(fromBlock, toBlock))
	if err != nil {
		return nil, err
	}
	if err := verifyEventLogIndexSegmentAgainstEventLogs(dir, indexRef, []SegmentRef{v3Ref}); err != nil {
		return nil, fmt.Errorf("snapshots: verify V3 migration companion: %w", err)
	}
	result := &EventLogV3MigrationResult{
		SourceGeneration: manifest.Generation,
		Generation:       manifest.Generation,
		FromBlock:        fromBlock,
		ToBlock:          toBlock,
		SourceSegments:   len(sources),
		V3MainBytes:      v3Ref.Size,
		V3IndexBytes:     indexRef.Size,
		Segments:         []SegmentRef{v3Ref, indexRef},
	}
	for _, ref := range sources {
		result.SourceMainBytes += ref.Size
	}
	for _, ref := range eventLogIndexRefs(manifest) {
		if ref.FromTxNum >= fromBlock && ref.ToTxNum <= toBlock {
			result.SourceIndexBytes += ref.Size
		}
	}
	result.MainSavingsBytes = signedByteSavings(result.SourceMainBytes, result.V3MainBytes)
	result.V3Physical, err = InspectEventLogV3Physical(dir, v3Ref)
	if err != nil {
		return nil, err
	}
	if !opts.Publish {
		return result, nil
	}
	current, err := LoadProductionManifest(dir)
	if err != nil {
		return nil, err
	}
	if current.Generation != manifest.Generation || !reflect.DeepEqual(current.Segments, manifest.Segments) {
		return nil, fmt.Errorf("snapshots: production manifest changed during V3 build (started generation %d, now %d); immutable outputs were left unreferenced and the migration is safe to rerun", manifest.Generation, current.Generation)
	}
	published, err := NewAggregator(dir).integrateWithManifest(fromBlock, toBlock, []SegmentRef{v3Ref, indexRef}, current)
	if err != nil {
		return nil, err
	}
	result.Published = true
	result.Generation = published.Generation
	return result, nil
}

func selectEventLogV3MigrationSources(manifest *Manifest, opts EventLogV3MigrationOptions) ([]SegmentRef, error) {
	refs := eventLogRefs(manifest)
	if len(refs) == 0 {
		return nil, errors.New("snapshots: production manifest has no active event-log segments")
	}
	start := -1
	for i, ref := range refs {
		if ref.FromTxNum == opts.FromBlock {
			start = i
			break
		}
	}
	if start < 0 {
		return nil, fmt.Errorf("snapshots: --snapshot.from-block=%d is not an active event-log segment boundary", opts.FromBlock)
	}
	end := start
	if opts.ToBlockSet {
		if opts.ToBlock < opts.FromBlock {
			return nil, fmt.Errorf("snapshots: migration range [%d,%d] is inverted", opts.FromBlock, opts.ToBlock)
		}
		for end < len(refs) && refs[end].ToTxNum < opts.ToBlock {
			end++
		}
		if end >= len(refs) || refs[end].ToTxNum != opts.ToBlock {
			return nil, fmt.Errorf("snapshots: --snapshot.to-block=%d is not an active event-log segment boundary", opts.ToBlock)
		}
	} else {
		limit := start + int(opts.Merge) - 1
		if limit >= len(refs) {
			limit = len(refs) - 1
		}
		end = limit
	}
	selected := append([]SegmentRef(nil), refs[start:end+1]...)
	for i := 1; i < len(selected); i++ {
		if selected[i-1].ToTxNum == ^uint64(0) || selected[i].FromTxNum != selected[i-1].ToTxNum+1 {
			return nil, fmt.Errorf("snapshots: active event-log segments %q and %q are not contiguous", selected[i-1].Path, selected[i].Path)
		}
	}
	return selected, nil
}

func signedByteSavings(current, candidate uint64) int64 {
	if current >= candidate {
		delta := current - candidate
		if delta > uint64(^uint64(0)>>1) {
			return int64(^uint64(0) >> 1)
		}
		return int64(delta)
	}
	delta := candidate - current
	if delta > uint64(^uint64(0)>>1) {
		return -int64(^uint64(0)>>1) - 1
	}
	return -int64(delta)
}
