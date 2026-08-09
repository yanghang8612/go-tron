package snapshots

import (
	"errors"
	"fmt"
	"os"

	"github.com/tronprotocol/go-tron/core/maintenance"
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
	// HeavyWorkGate serializes chain/event cold-segment verification and
	// construction with state-history snapshots and optional freezer
	// maintenance. Short no-op passes release before the production gate's
	// minimum-heavy-duration threshold and therefore do not renew its cooldown.
	HeavyWorkGate *maintenance.HeavyWorkGate
	// VerificationCache reuses exact immutable freezer/index/accessor semantic
	// proofs across lifecycle passes. Persistent hits still re-hash every file;
	// destructive V1 tail reclamation retains its independent strict gate.
	VerificationCache *ChainFreezerVerificationCache
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
	ResourceDeferred  bool
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
	result := ChainFreezerSnapshotPassResult{
		AncientHead: ancientHead,
		LocalTail:   tail,
		Manifest:    manifest,
	}
	releaseHeavyWork, admitted := cfg.HeavyWorkGate.TryAcquire()
	if !admitted {
		result.ResourceDeferred = true
		return result, nil
	}
	defer releaseHeavyWork()
	coldHead, err := verifiedChainFreezerSnapshotHeadWithCache(cfg.Dir, manifest, cfg.VerificationCache)
	if err != nil {
		return ChainFreezerSnapshotPassResult{}, err
	}
	if tail > coldHead {
		return ChainFreezerSnapshotPassResult{}, fmt.Errorf("snapshots: local chain-freezer tail %d exceeds verified cold coverage head %d", tail, coldHead)
	}

	result.ColdHead = coldHead
	if coldHead < ancientHead {
		toBlock := coldHead + cfg.BatchBlocks - 1
		if toBlock < coldHead || toBlock >= ancientHead {
			toBlock = ancientHead - 1
		}
		built, err := NewAggregator(cfg.Dir).BuildChainFreezerWithOptions(source, coldHead, toBlock, AggregatorBuildChainFreezerOptions{ETL: cfg.ETL})
		if err != nil {
			return result, fmt.Errorf("snapshots: build chain-freezer range [%d,%d]: %w", coldHead, toBlock, err)
		}
		freezerRef, indexRef, accessorRef, err := chainFreezerBuildCompanions(built.Segments)
		if err != nil {
			return result, fmt.Errorf("snapshots: inspect built chain-freezer range [%d,%d]: %w", coldHead, toBlock, err)
		}
		if err := requireBuiltSegmentsActive(built.Manifest, built.Segments); err != nil {
			return result, fmt.Errorf("snapshots: authenticate built chain-freezer range [%d,%d]: %w", coldHead, toBlock, err)
		}
		if err := cfg.VerificationCache.recordTrustedChain(cfg.Dir, freezerRef, indexRef, accessorRef, true); err != nil {
			return result, fmt.Errorf("snapshots: record trusted chain-freezer range [%d,%d]: %w", coldHead, toBlock, err)
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
	eventHead, err := verifiedIndexedEventLogHeadWithCache(cfg.Dir, manifest, cfg.VerificationCache)
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
	eventRef, indexRef, err := eventLogBuildCompanions(built.Segments)
	if err != nil {
		return result, fmt.Errorf("snapshots: inspect built event-log range [%d,%d]: %w", eventFrom, eventTo, err)
	}
	if err := requireBuiltSegmentsActive(built.Manifest, built.Segments); err != nil {
		return result, fmt.Errorf("snapshots: authenticate built event-log range [%d,%d]: %w", eventFrom, eventTo, err)
	}
	if err := cfg.VerificationCache.recordTrustedEventLogs(cfg.Dir, indexRef, []SegmentRef{eventRef}); err != nil {
		return result, fmt.Errorf("snapshots: record trusted event-log range [%d,%d]: %w", eventFrom, eventTo, err)
	}
	result.EventLogBuilt = true
	result.EventLogFromBlock = eventFrom
	result.EventLogToBlock = eventTo
	result.Manifest = built.Manifest
	return result, nil
}

func chainFreezerBuildCompanions(refs []SegmentRef) (freezerRef, indexRef, accessorRef SegmentRef, err error) {
	for _, ref := range refs {
		switch ref.Kind {
		case SegmentChainFreezer:
			if freezerRef != (SegmentRef{}) {
				return SegmentRef{}, SegmentRef{}, SegmentRef{}, errors.New("duplicate chain-freezer segment")
			}
			freezerRef = ref
		case SegmentChainIndex:
			if indexRef != (SegmentRef{}) {
				return SegmentRef{}, SegmentRef{}, SegmentRef{}, errors.New("duplicate chain-index segment")
			}
			indexRef = ref
		case SegmentChainFreezerAccessor:
			if accessorRef != (SegmentRef{}) {
				return SegmentRef{}, SegmentRef{}, SegmentRef{}, errors.New("duplicate chain-freezer accessor segment")
			}
			accessorRef = ref
		default:
			return SegmentRef{}, SegmentRef{}, SegmentRef{}, fmt.Errorf("unexpected %s segment %q", ref.Kind, ref.Path)
		}
	}
	if freezerRef == (SegmentRef{}) || indexRef == (SegmentRef{}) || accessorRef == (SegmentRef{}) {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, fmt.Errorf("incomplete companion set: freezer=%t index=%t accessor=%t",
			freezerRef != (SegmentRef{}), indexRef != (SegmentRef{}), accessorRef != (SegmentRef{}))
	}
	return freezerRef, indexRef, accessorRef, nil
}

func eventLogBuildCompanions(refs []SegmentRef) (eventRef, indexRef SegmentRef, err error) {
	for _, ref := range refs {
		switch ref.Kind {
		case SegmentEventLog:
			if eventRef != (SegmentRef{}) {
				return SegmentRef{}, SegmentRef{}, errors.New("duplicate event-log segment")
			}
			eventRef = ref
		case SegmentEventLogIndex:
			if indexRef != (SegmentRef{}) {
				return SegmentRef{}, SegmentRef{}, errors.New("duplicate event-log-index segment")
			}
			indexRef = ref
		default:
			return SegmentRef{}, SegmentRef{}, fmt.Errorf("unexpected %s segment %q", ref.Kind, ref.Path)
		}
	}
	if eventRef == (SegmentRef{}) || indexRef == (SegmentRef{}) {
		return SegmentRef{}, SegmentRef{}, fmt.Errorf("incomplete companion set: event-log=%t index=%t",
			eventRef != (SegmentRef{}), indexRef != (SegmentRef{}))
	}
	return eventRef, indexRef, nil
}

func requireBuiltSegmentsActive(manifest *Manifest, refs []SegmentRef) error {
	if manifest == nil {
		return errors.New("nil production manifest")
	}
	active := make(map[SegmentRef]struct{}, len(manifest.Segments))
	for _, ref := range manifest.Segments {
		active[ref] = struct{}{}
	}
	for _, ref := range refs {
		if _, ok := active[ref]; !ok {
			return fmt.Errorf("segment %q is not active in production manifest generation %d", ref.Path, manifest.Generation)
		}
	}
	return nil
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
	return verifiedChainFreezerSnapshotHeadWithCache(dir, manifest, nil)
}

func verifiedChainFreezerSnapshotHeadWithCache(dir string, manifest *Manifest, cache *ChainFreezerVerificationCache) (uint64, error) {
	refs := chainFreezerRefs(manifest)
	sortSegmentRefsAscending(refs)
	next := uint64(0)
	active := make(map[chainFreezerVerificationKey]struct{}, len(refs))
	for _, freezerRef := range refs {
		if freezerRef.FromTxNum != next {
			return 0, fmt.Errorf("snapshots: chain-freezer cold coverage is not contiguous: want block %d, found range [%d,%d]", next, freezerRef.FromTxNum, freezerRef.ToTxNum)
		}
		indexRef, ok := chainIndexRefForFreezer(manifest, freezerRef)
		if !ok {
			return 0, fmt.Errorf("snapshots: chain-freezer segment %q is missing matching chain-index coverage", freezerRef.Path)
		}
		accessorRef, hasAccessor := chainFreezerAccessorRefForFreezer(manifest, freezerRef)
		key, _, err := cache.verify(dir, freezerRef, indexRef, accessorRef, hasAccessor)
		if err != nil {
			return 0, err
		}
		if key != (chainFreezerVerificationKey{}) {
			active[key] = struct{}{}
		}
		if freezerRef.ToTxNum == ^uint64(0) {
			return 0, errors.New("snapshots: chain-freezer cold coverage reaches uint64 maximum")
		}
		next = freezerRef.ToTxNum + 1
	}
	if err := cache.commit(active); err != nil {
		return 0, fmt.Errorf("snapshots: persist chain-freezer verification cache: %w", err)
	}
	return next, nil
}

// verifyChainFreezerSnapshotCompanionsSinglePass authenticates the three
// immutable files independently, then decodes the freezer payload exactly once
// while proving both derived sidecars. The former lifecycle path rechecked and
// decoded the freezer for its own checker and again inside each sidecar gate.
func verifyChainFreezerSnapshotCompanionsSinglePass(dir string, freezerRef, indexRef, accessorRef SegmentRef, hasAccessor bool) error {
	if freezerRef.Kind != SegmentChainFreezer || indexRef.Kind != SegmentChainIndex {
		return fmt.Errorf("snapshots: chain-freezer snapshot verification requires %s with %s, got %s with %s",
			SegmentChainFreezer, SegmentChainIndex, freezerRef.Kind, indexRef.Kind)
	}
	if indexRef.FromTxNum != freezerRef.FromTxNum || indexRef.ToTxNum != freezerRef.ToTxNum {
		return fmt.Errorf("snapshots: chain-index range [%d,%d] does not match chain-freezer range [%d,%d]",
			indexRef.FromTxNum, indexRef.ToTxNum, freezerRef.FromTxNum, freezerRef.ToTxNum)
	}
	if err := checkSegmentFileMetadata(dir, freezerRef, false); err != nil {
		return fmt.Errorf("snapshots: verify chain-freezer segment %q: %w", freezerRef.Path, err)
	}
	if err := CheckChainIndexSegment(dir, indexRef); err != nil {
		return err
	}
	index, err := OpenChainIndexSegment(dir, indexRef)
	if err != nil {
		return err
	}
	defer index.Close()

	var accessor *ChainFreezerAccessorSegment
	if hasAccessor {
		if accessorRef.Kind != SegmentChainFreezerAccessor || accessorRef.FromTxNum != freezerRef.FromTxNum || accessorRef.ToTxNum != freezerRef.ToTxNum {
			return fmt.Errorf("snapshots: chain-freezer accessor range/kind does not match chain-freezer segment %q", freezerRef.Path)
		}
		if err := CheckChainFreezerAccessorSegment(dir, accessorRef); err != nil {
			return err
		}
		accessor, err = OpenChainFreezerAccessorSegment(dir, accessorRef)
		if err != nil {
			return err
		}
		defer accessor.Close()
	}

	var blocksSeen, txsSeen uint64
	err = iterateChainFreezerSegmentRowsWithOffset(dir, freezerRef, func(rowOffset uint64, row chainFreezerRow) error {
		verified, err := validateChainFreezerRowPayload(row, "chain-freezer snapshot verification")
		if err != nil {
			return err
		}
		if accessor != nil {
			offset, ok, err := accessor.RowOffset(row.blockNum)
			if err != nil {
				return err
			}
			if !ok || offset != rowOffset {
				return fmt.Errorf("snapshots: chain-freezer accessor segment %q offset for block %d is %d, want %d", accessorRef.Path, row.blockNum, offset, rowOffset)
			}
		}
		block := verified.block
		blockHash := block.Hash()
		blockNum, ok, err := index.BlockNumberByHash(blockHash)
		if err != nil {
			return err
		}
		if !ok || blockNum != row.blockNum {
			return fmt.Errorf("snapshots: chain-index segment %q missing block hash %x at block %d", indexRef.Path, blockHash, row.blockNum)
		}
		blocksSeen++
		for txIndex, tx := range block.Transactions() {
			txHash := tx.Hash()
			lookup, ok, err := index.TransactionIndexByHash(txHash)
			if err != nil {
				return err
			}
			if !ok || lookup.BlockNum != row.blockNum || lookup.TxIndex != uint32(txIndex) {
				return fmt.Errorf("snapshots: chain-index segment %q missing tx hash %x at block %d index %d", indexRef.Path, txHash, row.blockNum, txIndex)
			}
			txsSeen++
		}
		return nil
	})
	if err != nil {
		return err
	}
	if blocksSeen != index.header.blockCount {
		return fmt.Errorf("snapshots: chain-index segment %q block entries %d, freezer has %d", indexRef.Path, index.header.blockCount, blocksSeen)
	}
	if txsSeen != index.header.txCount {
		return fmt.Errorf("snapshots: chain-index segment %q tx entries %d, freezer has %d", indexRef.Path, index.header.txCount, txsSeen)
	}
	return nil
}

// verifiedIndexedEventLogHead returns the exclusive end of the continuous
// indexed event-log prefix beginning at block 1. Event and index boundaries
// must agree: rebuilding an overlapping event segment would otherwise retire
// valid event rows while trying to repair a damaged index.
func verifiedIndexedEventLogHead(dir string, manifest *Manifest) (uint64, error) {
	return verifiedIndexedEventLogHeadWithCache(dir, manifest, nil)
}

func verifiedIndexedEventLogHeadWithCache(dir string, manifest *Manifest, cache *ChainFreezerVerificationCache) (uint64, error) {
	eventRefs := eventLogRefs(manifest)
	eventHead, hasEvents := eventLogCoverageBlockFromRefs(eventRefs, 1)
	indexRefs := eventLogIndexRefs(manifest)
	indexHead, hasIndexes := eventLogIndexedCoverageBlockFromRefs(indexRefs, 1, eventHead)
	if hasEvents != hasIndexes || (hasEvents && eventHead != indexHead) {
		return 0, errors.New("snapshots: event-log and event-log-index cold coverage are not an identical continuous prefix")
	}
	if !hasEvents {
		if err := cache.commitEventLogs(nil); err != nil {
			return 0, fmt.Errorf("snapshots: persist event-log verification cache: %w", err)
		}
		return 1, nil
	}

	if cache == nil {
		if err := verifyEventLogSnapshotPrefix(dir, eventRefs, 1, eventHead); err != nil {
			return 0, err
		}
		if err := verifyEventLogIndexSnapshotPrefix(dir, indexRefs, eventRefs, 1, eventHead); err != nil {
			return 0, err
		}
	} else if err := verifyEventLogIndexSnapshotPrefixWithCache(dir, indexRefs, eventRefs, 1, eventHead, cache); err != nil {
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
	return verifyEventLogIndexSnapshotPrefixWithCache(dir, indexRefs, eventRefs, fromBlock, toBlock, nil)
}

func verifyEventLogIndexSnapshotPrefixWithCache(dir string, indexRefs, eventRefs []SegmentRef, fromBlock, toBlock uint64, cache *ChainFreezerVerificationCache) error {
	sortSegmentRefsAscending(indexRefs)
	next := fromBlock
	active := make(map[eventLogVerificationKey]struct{}, len(indexRefs))
	for _, ref := range indexRefs {
		if ref.ToTxNum < next {
			continue
		}
		if ref.FromTxNum > next {
			break
		}
		key, _, err := cache.verifyEventLogIndex(dir, ref, eventRefs)
		if err != nil {
			return err
		}
		if key != (eventLogVerificationKey{}) {
			active[key] = struct{}{}
		}
		if ref.ToTxNum >= toBlock {
			if err := cache.commitEventLogs(active); err != nil {
				return fmt.Errorf("snapshots: persist event-log verification cache: %w", err)
			}
			return nil
		}
		if ref.ToTxNum == ^uint64(0) {
			break
		}
		next = ref.ToTxNum + 1
	}
	return fmt.Errorf("snapshots: indexed event-log cold coverage [%d,%d] is incomplete", fromBlock, toBlock)
}
