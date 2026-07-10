package snapshots

import (
	"errors"
	"fmt"
	"os"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

// DefaultChainFreezerSnapshotBatchBlocks bounds one automatic cold-chain
// snapshot pass. It deliberately matches the state-history builder's default
// so a node can make steady progress without monopolising disk IO.
const DefaultChainFreezerSnapshotBatchBlocks = defaultColdSnapshotBatchBlocks

// ChainFreezerSnapshotSource is the local immutable store used to publish
// verified chain-freezer snapshot segments. Tail is required because source
// rows below it cannot be rebuilt after minimal-mode reclamation.
type ChainFreezerSnapshotSource interface {
	rawdb.AncientReader
	Tail() (uint64, error)
}

// ChainFreezerSnapshotConfig controls one incremental chain-freezer snapshot
// build pass.
type ChainFreezerSnapshotConfig struct {
	Dir string

	// BatchBlocks caps newly published chain-freezer rows per pass. Zero uses
	// DefaultChainFreezerSnapshotBatchBlocks.
	BatchBlocks uint64

	// BuildEventLogs keeps the indexed event-log archive continuously aligned
	// with chain-freezer coverage. This is required before minimal-mode tail
	// reclamation may hide historical local freezer rows.
	BuildEventLogs bool
	ETL            RestoreETLOptions
}

// ChainFreezerSnapshotPassResult describes one automatic cold-chain build
// pass. AncientHead and ColdHead are exclusive block numbers.
type ChainFreezerSnapshotPassResult struct {
	AncientHead uint64
	LocalTail   uint64
	ColdHead    uint64

	Built     bool
	FromBlock uint64
	ToBlock   uint64

	EventLogBuilt     bool
	EventLogFromBlock uint64
	EventLogToBlock   uint64
	Manifest          *Manifest
}

// BuildChainFreezerSnapshotPass publishes at most one contiguous batch of
// local ancient rows into chain-freezer, accessor, and chain-index segments.
// It never writes across an existing coverage gap and refuses to proceed when
// the local freezer tail has already hidden rows not covered by verified cold
// segments. When requested, it also advances the indexed event-log archive to
// the same cold-chain boundary.
func BuildChainFreezerSnapshotPass(source ChainFreezerSnapshotSource, chain *rawdb.ChainDB, cfg ChainFreezerSnapshotConfig) (ChainFreezerSnapshotPassResult, error) {
	if source == nil {
		return ChainFreezerSnapshotPassResult{}, errors.New("snapshots: nil chain-freezer snapshot source")
	}
	if cfg.Dir == "" {
		return ChainFreezerSnapshotPassResult{}, errors.New("snapshots: empty chain-freezer snapshot directory")
	}
	if cfg.BatchBlocks == 0 {
		cfg.BatchBlocks = DefaultChainFreezerSnapshotBatchBlocks
	}

	ancientHead, err := chainFreezerSnapshotAncientHead(source)
	if err != nil {
		return ChainFreezerSnapshotPassResult{}, err
	}
	tail, err := source.Tail()
	if err != nil {
		return ChainFreezerSnapshotPassResult{}, fmt.Errorf("snapshots: read chain-freezer local tail: %w", err)
	}
	if tail > ancientHead {
		return ChainFreezerSnapshotPassResult{}, fmt.Errorf("snapshots: local chain-freezer tail %d exceeds ancient head %d", tail, ancientHead)
	}

	manifest, err := loadChainFreezerSnapshotManifest(cfg.Dir)
	if err != nil {
		return ChainFreezerSnapshotPassResult{}, err
	}
	coldHead, err := verifiedChainFreezerSnapshotHead(cfg.Dir, manifest)
	if err != nil {
		return ChainFreezerSnapshotPassResult{}, err
	}
	if tail > coldHead {
		return ChainFreezerSnapshotPassResult{}, fmt.Errorf("snapshots: local chain-freezer tail %d exceeds verified cold coverage head %d", tail, coldHead)
	}

	result := ChainFreezerSnapshotPassResult{
		AncientHead: ancientHead,
		LocalTail:   tail,
		ColdHead:    coldHead,
		Manifest:    manifest,
	}
	if coldHead < ancientHead {
		toBlock := coldHead + cfg.BatchBlocks - 1
		if toBlock < coldHead || toBlock >= ancientHead {
			toBlock = ancientHead - 1
		}
		built, err := NewAggregator(cfg.Dir).BuildChainFreezerWithOptions(source, coldHead, toBlock, AggregatorBuildChainFreezerOptions{ETL: cfg.ETL})
		if err != nil {
			return result, fmt.Errorf("snapshots: build chain-freezer range [%d,%d]: %w", coldHead, toBlock, err)
		}
		result.Built = true
		result.FromBlock = coldHead
		result.ToBlock = toBlock
		result.ColdHead = toBlock + 1
		result.Manifest = built.Manifest
		manifest = built.Manifest
	}

	if !cfg.BuildEventLogs || result.ColdHead <= 1 {
		return result, nil
	}
	if chain == nil {
		return result, errors.New("snapshots: event-log chain database is required for chain-freezer snapshot build")
	}
	eventHead, err := verifiedIndexedEventLogHead(cfg.Dir, manifest)
	if err != nil {
		return result, err
	}
	if eventHead >= result.ColdHead {
		return result, nil
	}
	eventFrom := eventHead
	if eventFrom < 1 {
		eventFrom = 1
	}
	eventTo := result.ColdHead - 1
	built, err := NewAggregator(cfg.Dir).BuildEventLogsWithOptions(chain, eventFrom, eventTo, cfg.ETL)
	if err != nil {
		return result, fmt.Errorf("snapshots: build event-log range [%d,%d]: %w", eventFrom, eventTo, err)
	}
	result.EventLogBuilt = true
	result.EventLogFromBlock = eventFrom
	result.EventLogToBlock = eventTo
	result.Manifest = built.Manifest
	return result, nil
}

func chainFreezerSnapshotAncientHead(source rawdb.AncientReader) (uint64, error) {
	var head uint64
	for i, table := range []string{
		rawdb.AncientBlocksTable,
		rawdb.AncientTxInfosTable,
		rawdb.AncientStateRootsTable,
	} {
		count, err := source.AncientCount(table)
		if err != nil {
			return 0, fmt.Errorf("snapshots: read local ancient %s count: %w", table, err)
		}
		if i == 0 {
			head = count
			continue
		}
		if count != head {
			return 0, fmt.Errorf("snapshots: local ancient tables are not aligned: %s has %d rows, want %d", table, count, head)
		}
	}
	return head, nil
}

func loadChainFreezerSnapshotManifest(dir string) (*Manifest, error) {
	manifest, err := LoadProductionManifest(dir)
	if err == nil {
		return manifest, nil
	}
	if os.IsNotExist(err) {
		return nil, nil
	}
	return nil, err
}

// verifiedChainFreezerSnapshotHead returns the exclusive end of the complete
// cold chain prefix. Existing ranges must be contiguous and have a matching,
// verified chain-index sidecar before a new batch may be appended.
func verifiedChainFreezerSnapshotHead(dir string, manifest *Manifest) (uint64, error) {
	refs := chainFreezerRefs(manifest)
	sortSegmentRefsAscending(refs)
	next := uint64(0)
	for _, freezerRef := range refs {
		if freezerRef.FromTxNum != next {
			return 0, fmt.Errorf("snapshots: chain-freezer cold coverage is not contiguous: want block %d, found range [%d,%d]", next, freezerRef.FromTxNum, freezerRef.ToTxNum)
		}
		if err := CheckChainFreezerSegment(dir, freezerRef); err != nil {
			return 0, fmt.Errorf("snapshots: verify chain-freezer segment %q: %w", freezerRef.Path, err)
		}
		if accessorRef, ok := chainFreezerAccessorRefForFreezer(manifest, freezerRef); ok {
			if err := VerifyChainFreezerAccessorSegmentAgainstChainFreezer(dir, accessorRef, freezerRef); err != nil {
				return 0, err
			}
		}
		indexRef, ok := chainIndexRefForFreezer(manifest, freezerRef)
		if !ok {
			return 0, fmt.Errorf("snapshots: chain-freezer segment %q is missing matching chain-index coverage", freezerRef.Path)
		}
		if err := VerifyChainIndexSegmentAgainstChainFreezer(dir, indexRef, freezerRef); err != nil {
			return 0, err
		}
		if freezerRef.ToTxNum == ^uint64(0) {
			return 0, errors.New("snapshots: chain-freezer cold coverage reaches uint64 maximum")
		}
		next = freezerRef.ToTxNum + 1
	}
	return next, nil
}

// verifiedIndexedEventLogHead returns the exclusive end of the continuous
// indexed event-log prefix beginning at block 1. Event and index boundaries
// must agree: rebuilding an overlapping event segment would otherwise retire
// valid event rows while trying to repair a damaged index.
func verifiedIndexedEventLogHead(dir string, manifest *Manifest) (uint64, error) {
	eventRefs := eventLogRefs(manifest)
	eventHead, hasEvents := eventLogCoverageBlockFromRefs(eventRefs, 1)
	indexRefs := eventLogIndexRefs(manifest)
	indexHead, hasIndexes := eventLogIndexedCoverageBlockFromRefs(indexRefs, 1, eventHead)
	if hasEvents != hasIndexes || (hasEvents && eventHead != indexHead) {
		return 0, errors.New("snapshots: event-log and event-log-index cold coverage are not an identical continuous prefix")
	}
	if !hasEvents {
		return 1, nil
	}

	if err := verifyEventLogSnapshotPrefix(dir, eventRefs, 1, eventHead); err != nil {
		return 0, err
	}
	if err := verifyEventLogIndexSnapshotPrefix(dir, indexRefs, eventRefs, 1, eventHead); err != nil {
		return 0, err
	}
	if eventHead == ^uint64(0) {
		return 0, errors.New("snapshots: event-log cold coverage reaches uint64 maximum")
	}
	return eventHead + 1, nil
}

func verifyEventLogSnapshotPrefix(dir string, refs []SegmentRef, fromBlock, toBlock uint64) error {
	sortSegmentRefsAscending(refs)
	next := fromBlock
	for _, ref := range refs {
		if ref.ToTxNum < next {
			continue
		}
		if ref.FromTxNum > next {
			break
		}
		if err := CheckEventLogSegment(dir, ref); err != nil {
			return err
		}
		if ref.ToTxNum >= toBlock {
			return nil
		}
		if ref.ToTxNum == ^uint64(0) {
			break
		}
		next = ref.ToTxNum + 1
	}
	return fmt.Errorf("snapshots: event-log cold coverage [%d,%d] is incomplete", fromBlock, toBlock)
}

func verifyEventLogIndexSnapshotPrefix(dir string, indexRefs, eventRefs []SegmentRef, fromBlock, toBlock uint64) error {
	sortSegmentRefsAscending(indexRefs)
	next := fromBlock
	for _, ref := range indexRefs {
		if ref.ToTxNum < next {
			continue
		}
		if ref.FromTxNum > next {
			break
		}
		if err := verifyEventLogIndexSegmentAgainstEventLogs(dir, ref, eventRefs); err != nil {
			return err
		}
		if ref.ToTxNum >= toBlock {
			return nil
		}
		if ref.ToTxNum == ^uint64(0) {
			break
		}
		next = ref.ToTxNum + 1
	}
	return fmt.Errorf("snapshots: indexed event-log cold coverage [%d,%d] is incomplete", fromBlock, toBlock)
}
