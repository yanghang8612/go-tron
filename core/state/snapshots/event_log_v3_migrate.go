package snapshots

import (
	"errors"
	"fmt"
	"reflect"
)

const (
	eventLogV4MigrationMaxSourceSegments = 64
	eventLogV4MigrationMaxBlockSpan      = 320_000
)

type EventLogV4MigrationOptions struct {
	FromBlock  uint64
	ToBlock    uint64
	ToBlockSet bool
	Merge      uint64
	Publish    bool
}

type EventLogV4MigrationResult struct {
	SourceGeneration    uint64                  `json:"sourceGeneration"`
	Published           bool                    `json:"published"`
	Generation          uint64                  `json:"generation"`
	FromBlock           uint64                  `json:"fromBlock"`
	ToBlock             uint64                  `json:"toBlock"`
	SourceSegments      int                     `json:"sourceSegments"`
	SourceMainBytes     uint64                  `json:"sourceMainBytes"`
	SourceIndexBytes    uint64                  `json:"sourceIndexBytes"`
	V4MainBytes         uint64                  `json:"v4MainBytes"`
	V4IndexBytes        uint64                  `json:"v4IndexBytes"`
	PreservedIndexes    int                     `json:"preservedIndexSegments"`
	PreservedIndexBytes uint64                  `json:"preservedIndexBytes"`
	MainSavingsBytes    int64                   `json:"mainSavingsBytes"`
	V4Physical          EventLogV4PhysicalStats `json:"v4Physical"`
	Segments            []SegmentRef            `json:"segments"`
}

// MigrateEventLogsV4 rewrites exact active segment boundaries from one pinned
// production manifest. Publication is opt-in and is refused when the active
// manifest changes while the new immutable files are being built.
func MigrateEventLogsV4(dir string, opts EventLogV4MigrationOptions) (*EventLogV4MigrationResult, error) {
	if dir == "" {
		return nil, errors.New("snapshots: V4 migration directory is empty")
	}
	if opts.Merge == 0 {
		opts.Merge = 8
	}
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		return nil, err
	}
	sources, err := selectEventLogV4MigrationSources(manifest, opts)
	if err != nil {
		return nil, err
	}
	fromBlock, toBlock := sources[0].FromTxNum, sources[len(sources)-1].ToTxNum
	var overlappingIndexes []SegmentRef
	for _, indexRef := range eventLogIndexRefs(manifest) {
		if indexRef.ToTxNum < fromBlock || indexRef.FromTxNum > toBlock {
			continue
		}
		overlappingIndexes = append(overlappingIndexes, indexRef)
	}
	preserveIndexes := len(sources) == 1 && len(overlappingIndexes) > 0
	if !preserveIndexes {
		for _, indexRef := range overlappingIndexes {
			if indexRef.FromTxNum >= fromBlock && indexRef.ToTxNum <= toBlock {
				continue
			}
			return nil, fmt.Errorf("snapshots: event-log-index %q crosses migration range [%d,%d]; migrate its full [%d,%d] range", indexRef.Path, fromBlock, toBlock, indexRef.FromTxNum, indexRef.ToTxNum)
		}
	}
	pinned, err := OpenPinnedManager(dir, manifest)
	if err != nil {
		return nil, err
	}
	v4Ref, err := BuildEventLogV4SegmentFromReader(pinned, dir, EventLogSegmentPath(fromBlock, toBlock), fromBlock, toBlock)
	if err != nil {
		return nil, err
	}
	publishRefs := []SegmentRef{v4Ref}
	var indexRef SegmentRef
	if preserveIndexes {
		if err := verifyEventLogSegmentCandidateKeysEqual(dir, sources[0], v4Ref); err != nil {
			return nil, fmt.Errorf("snapshots: cannot preserve crossing event-log-index: %w", err)
		}
	} else {
		indexRef, err = writeFreshEventLogV4Index(dir, v4Ref, EventLogIndexSegmentPath(fromBlock, toBlock))
		if err != nil {
			return nil, err
		}
		if err := verifyEventLogIndexSegmentAgainstEventLogs(dir, indexRef, []SegmentRef{v4Ref}); err != nil {
			return nil, fmt.Errorf("snapshots: verify V4 migration companion: %w", err)
		}
		publishRefs = append(publishRefs, indexRef)
	}
	result := &EventLogV4MigrationResult{
		SourceGeneration: manifest.Generation,
		Generation:       manifest.Generation,
		FromBlock:        fromBlock,
		ToBlock:          toBlock,
		SourceSegments:   len(sources),
		V4MainBytes:      v4Ref.Size,
		V4IndexBytes:     indexRef.Size,
		Segments:         append([]SegmentRef(nil), publishRefs...),
	}
	for _, ref := range sources {
		result.SourceMainBytes += ref.Size
	}
	for _, ref := range overlappingIndexes {
		if preserveIndexes {
			result.PreservedIndexes++
			result.PreservedIndexBytes += ref.Size
			continue
		}
		if ref.FromTxNum >= fromBlock && ref.ToTxNum <= toBlock {
			result.SourceIndexBytes += ref.Size
		}
	}
	result.MainSavingsBytes = signedByteSavings(result.SourceMainBytes, result.V4MainBytes)
	result.V4Physical, err = InspectEventLogV4Physical(dir, v4Ref)
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
		return nil, fmt.Errorf("snapshots: production manifest changed during V4 build (started generation %d, now %d); immutable outputs were left unreferenced and the migration is safe to rerun", manifest.Generation, current.Generation)
	}
	published, err := NewAggregator(dir).integrateWithManifest(fromBlock, toBlock, publishRefs, current)
	if err != nil {
		return nil, err
	}
	result.Published = true
	result.Generation = published.Generation
	return result, nil
}

func verifyEventLogSegmentCandidateKeysEqual(dir string, source, candidate SegmentRef) error {
	sourceAddress, sourceTopic, err := verifiedEventLogSegmentCandidateKeys(dir, source)
	if err != nil {
		return err
	}
	candidateAddress, candidateTopic, err := verifiedEventLogSegmentCandidateKeys(dir, candidate)
	if err != nil {
		return err
	}
	if !sameEventLogCandidateKeys(sourceAddress, candidateAddress) {
		return errors.New("address candidate keys changed")
	}
	if !sameEventLogCandidateKeys(sourceTopic, candidateTopic) {
		return errors.New("topic candidate keys changed")
	}
	return nil
}

func verifiedEventLogSegmentCandidateKeys(dir string, ref SegmentRef) (map[string][]uint64, map[string][]uint64, error) {
	address := make(map[string][]uint64)
	topic := make(map[string][]uint64)
	if err := collectVerifiedEventLogSegmentPostings(dir, ref, address, topic); err != nil {
		return nil, nil, err
	}
	return address, topic, nil
}

func sameEventLogCandidateKeys(a, b map[string][]uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for key := range a {
		if _, ok := b[key]; !ok {
			return false
		}
	}
	return true
}

func selectEventLogV4MigrationSources(manifest *Manifest, opts EventLogV4MigrationOptions) ([]SegmentRef, error) {
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
		remaining := uint64(len(refs) - start)
		merge := opts.Merge
		if merge == 0 {
			merge = 8
		}
		if merge > remaining {
			merge = remaining
		}
		end = start + int(merge) - 1
	}
	selected := append([]SegmentRef(nil), refs[start:end+1]...)
	for i := 1; i < len(selected); i++ {
		if selected[i-1].ToTxNum == ^uint64(0) || selected[i].FromTxNum != selected[i-1].ToTxNum+1 {
			return nil, fmt.Errorf("snapshots: active event-log segments %q and %q are not contiguous", selected[i-1].Path, selected[i].Path)
		}
	}
	if len(selected) > eventLogV4MigrationMaxSourceSegments {
		return nil, fmt.Errorf("snapshots: event-log V4 migration selects %d source segments, exceeds safety limit %d", len(selected), eventLogV4MigrationMaxSourceSegments)
	}
	fromBlock, toBlock := selected[0].FromTxNum, selected[len(selected)-1].ToTxNum
	if toBlock < fromBlock {
		return nil, fmt.Errorf("snapshots: event-log V4 migration range [%d,%d] is inverted", fromBlock, toBlock)
	}
	// Compare the inclusive span without adding one first, so a MaxUint64 end
	// block cannot overflow the check.
	if toBlock-fromBlock >= eventLogV4MigrationMaxBlockSpan {
		return nil, fmt.Errorf("snapshots: event-log V4 migration range [%d,%d] exceeds safety limit of %d blocks", fromBlock, toBlock, eventLogV4MigrationMaxBlockSpan)
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
