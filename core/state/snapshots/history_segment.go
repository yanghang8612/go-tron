package snapshots

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

const StateDomainChangeSegmentVersion = 1

type StateDomainChangeSegment struct {
	Version   uint32                     `json:"version"`
	Dataset   SegmentDataset             `json:"dataset"`
	Kind      SegmentKind                `json:"kind"`
	FromTxNum uint64                     `json:"fromTxNum"`
	ToTxNum   uint64                     `json:"toTxNum"`
	TxRanges  []*rawdb.StateTxRange      `json:"txRanges,omitempty"`
	Changes   []*rawdb.StateDomainChange `json:"changes"`
}

type RestoreStateDomainHistoryResult struct {
	FromTxNum        uint64
	ToTxNum          uint64
	ChangesRestored  uint64
	TxRangesRestored uint64
}

func BuildStateDomainChangeHistorySegmentFromDB(db ethdb.Iteratee, dir string, fromTxNum, toTxNum uint64, relPath string) (SegmentRef, error) {
	refs, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, fromTxNum, toTxNum, relPath)
	if err != nil {
		return SegmentRef{}, err
	}
	if len(refs) == 0 {
		return SegmentRef{}, errors.New("snapshots: state-domain-change builder produced no segment")
	}
	return refs[0], nil
}

func BuildStateDomainChangeHistorySegmentsFromDB(db ethdb.Iteratee, dir string, fromTxNum, toTxNum uint64, relPath string) ([]SegmentRef, error) {
	return buildStateDomainChangeHistorySegmentsFromDB(db, dir, fromTxNum, toTxNum, relPath, nil)
}

// BuildStateDomainChangeHistorySegmentsFromDBByBlockRange is the production
// cold-builder entry point. The tx range remains the authoritative segment
// identity, while the corresponding block range lets physical hot-history
// iterators seek directly to their first input row.
func BuildStateDomainChangeHistorySegmentsFromDBByBlockRange(db ethdb.Iteratee, dir string, fromTxNum, toTxNum, fromBlock, toBlock uint64, relPath string) ([]SegmentRef, error) {
	if toBlock < fromBlock {
		return nil, fmt.Errorf("snapshots: state-domain-change block range [%d,%d] is inverted", fromBlock, toBlock)
	}
	return buildStateDomainChangeHistorySegmentsFromDB(db, dir, fromTxNum, toTxNum, relPath, &stateDomainChangeHistoryBlockRange{
		from: fromBlock,
		to:   toBlock,
	})
}

// BuildStateDomainChangeHistorySegmentsFromDBByBlockRangeReadContext is the
// production context-aware single-trio entry. Reader options are local to this
// build. In particular they do not change the existing effective 64MiB ETL
// defaults by importing the unrelated runtime restore/sidecar options.
func BuildStateDomainChangeHistorySegmentsFromDBByBlockRangeReadContext(ctx context.Context, db ethdb.Iteratee, dir string, fromTxNum, toTxNum, fromBlock, toBlock uint64, relPath string, reads HistoryReadOptions) ([]SegmentRef, error) {
	cfg, ok := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	if !ok {
		return nil, errors.New("snapshots: missing state-domain history configuration")
	}
	return buildStateHistoryReadContext(ctx, db, dir, SegmentRef{
		Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory, FromTxNum: fromTxNum, ToTxNum: toTxNum, Path: relPath,
	}, &stateDomainChangeHistoryBlockRange{from: fromBlock, to: toBlock}, cfg, etl.Options{}, os.Getenv("GTRON_HISTORY_COMPRESSION_FORMAT"), reads, reads.compressionWorkers())
}

func buildStateDomainChangeHistorySegmentsFromDB(db ethdb.Iteratee, dir string, fromTxNum, toTxNum uint64, relPath string, blockRange *stateDomainChangeHistoryBlockRange) ([]SegmentRef, error) {
	historyView, releaseHistoryView, viewErr := rawdb.AcquireStateHistoryReadView(db)
	if viewErr != nil {
		return nil, viewErr
	}
	defer func() { _ = releaseHistoryView() }()
	db = historyView
	if db == nil {
		return nil, errors.New("snapshots: nil database")
	}
	if toTxNum < fromTxNum {
		return nil, fmt.Errorf("snapshots: state-domain-change range [%d,%d] is inverted", fromTxNum, toTxNum)
	}
	cfg, ok := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	if !ok || cfg.IterateHotHistoryTxRangeChanges == nil || cfg.IterateHotHistoryTxRanges == nil {
		return nil, errors.New("snapshots: missing state-domain history iterators")
	}
	ref := SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentHistory,
		FromTxNum: fromTxNum,
		ToTxNum:   toTxNum,
		Path:      relPath,
	}
	if isStateDomainChangeBinarySegmentPath(relPath) {
		result, err := buildStateDomainChangeHistoryBinarySegmentsFromDBRange(db, dir, ref, cfg, etl.Options{}, blockRange)
		if err != nil {
			return nil, err
		}
		return result.refs, nil
	}

	var changes []*rawdb.StateDomainChange
	if err := iterateStateDomainChangeHistoryChanges(db, cfg, fromTxNum, toTxNum, blockRange, func(change *rawdb.StateDomainChange) (bool, error) {
		changes = append(changes, cloneStateDomainChangeForSegment(change))
		return true, nil
	}); err != nil {
		return nil, err
	}
	var txRanges []*rawdb.StateTxRange
	if err := iterateStateDomainChangeHistorySourceTxRanges(db, cfg, blockRange, func(row *rawdb.StateTxRange) (bool, error) {
		if row.EndTxNum < fromTxNum || row.BeginTxNum > toTxNum {
			return true, nil
		}
		txRanges = append(txRanges, cloneStateTxRangeForSegment(row))
		return true, nil
	}); err != nil {
		return nil, err
	}
	segRef, err := WriteStateDomainChangeSegment(dir, ref, changes, txRanges)
	if err != nil {
		return nil, err
	}
	return []SegmentRef{segRef}, nil
}

func WriteStateDomainChangeSegment(dir string, ref SegmentRef, changes []*rawdb.StateDomainChange, txRanges ...[]*rawdb.StateTxRange) (SegmentRef, error) {
	if ref.Kind == "" {
		ref.Kind = SegmentHistory
	}
	if ref.Dataset == "" {
		ref.Dataset = SegmentDatasetStateDomainChange
	}
	if err := validateSegment(ref, ref.FromTxNum, ref.ToTxNum); err != nil {
		return SegmentRef{}, err
	}
	seg := &StateDomainChangeSegment{
		Version:   StateDomainChangeSegmentVersion,
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentHistory,
		FromTxNum: ref.FromTxNum,
		ToTxNum:   ref.ToTxNum,
		TxRanges:  normalizeStateTxRangesForSegment(firstStateTxRangeSet(txRanges)),
		Changes:   normalizeStateDomainChanges(changes),
	}
	if err := seg.Validate(); err != nil {
		return SegmentRef{}, err
	}
	data, err := json.Marshal(seg)
	if err != nil {
		return SegmentRef{}, err
	}
	sum := sha256.Sum256(data)
	ref.Size = uint64(len(data))
	ref.Checksum = "sha256:" + hex.EncodeToString(sum[:])

	abs := filepath.Join(dir, ref.Path)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return SegmentRef{}, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(abs), "."+filepath.Base(abs)+".*.tmp")
	if err != nil {
		return SegmentRef{}, err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	if err := tmp.Close(); err != nil {
		return SegmentRef{}, err
	}
	if err := os.Rename(tmpName, abs); err != nil {
		return SegmentRef{}, err
	}
	return ref, nil
}

func OpenStateDomainChangeSegment(dir string, ref SegmentRef) (*StateDomainChangeSegment, error) {
	if ref.Dataset != SegmentDatasetStateDomainChange || ref.Kind != SegmentHistory {
		return nil, fmt.Errorf("snapshots: segment %q is %s/%s, want state-domain-change/history", ref.Path, ref.Dataset, ref.Kind)
	}
	if err := validateSegment(ref, ref.FromTxNum, ref.ToTxNum); err != nil {
		return nil, err
	}
	if isStateDomainChangeBinarySegmentPath(ref.Path) {
		changes, err := readStateDomainChangeBinarySegment(dir, ref)
		if err != nil {
			return nil, err
		}
		txRanges, err := readStateDomainChangeBinaryTxRanges(dir, ref)
		if err != nil {
			return nil, err
		}
		return &StateDomainChangeSegment{
			Version:   StateDomainChangeSegmentVersion,
			Dataset:   SegmentDatasetStateDomainChange,
			Kind:      SegmentHistory,
			FromTxNum: ref.FromTxNum,
			ToTxNum:   ref.ToTxNum,
			TxRanges:  txRanges,
			Changes:   changes,
		}, nil
	}
	data, err := os.ReadFile(filepath.Join(dir, ref.Path))
	if err != nil {
		return nil, err
	}
	if ref.Size != 0 && uint64(len(data)) != ref.Size {
		return nil, fmt.Errorf("snapshots: segment %q size %d, want %d", ref.Path, len(data), ref.Size)
	}
	if ref.Checksum != "" {
		sum := sha256.Sum256(data)
		if got := "sha256:" + hex.EncodeToString(sum[:]); got != ref.Checksum {
			return nil, fmt.Errorf("snapshots: segment %q checksum %s, want %s", ref.Path, got, ref.Checksum)
		}
	}
	var seg StateDomainChangeSegment
	if err := json.Unmarshal(data, &seg); err != nil {
		return nil, err
	}
	if err := seg.Validate(); err != nil {
		return nil, err
	}
	if seg.FromTxNum != ref.FromTxNum || seg.ToTxNum != ref.ToTxNum {
		return nil, fmt.Errorf("snapshots: segment %q range [%d,%d], want [%d,%d]", ref.Path, seg.FromTxNum, seg.ToTxNum, ref.FromTxNum, ref.ToTxNum)
	}
	return &seg, nil
}

func CheckStateDomainChangeIndexSegment(dir string, ref SegmentRef) error {
	return checkStateDomainChangeBinaryIndex(dir, ref)
}

func CheckStateDomainChangeAccessorSegment(dir string, ref SegmentRef) error {
	return checkStateDomainChangeBinaryAccessor(dir, ref)
}

func CheckStateDomainChangeAccessorSegmentContext(ctx context.Context, dir string, ref SegmentRef) error {
	return checkStateDomainChangeBinaryAccessorContext(ctx, dir, ref)
}

func CheckStateDomainChangeSegment(dir string, ref SegmentRef) error {
	if ref.Dataset != SegmentDatasetStateDomainChange || ref.Kind != SegmentHistory {
		return fmt.Errorf("snapshots: segment %q is %s/%s, want state-domain-change/history", ref.Path, ref.Dataset, ref.Kind)
	}
	if isStateDomainChangeBinarySegmentPath(ref.Path) {
		return checkStateDomainChangeBinarySegment(dir, ref)
	}
	_, err := OpenStateDomainChangeSegment(dir, ref)
	return err
}

func openStateDomainChangeHistoryChanges(dir string, ref SegmentRef) ([]*rawdb.StateDomainChange, error) {
	seg, err := OpenStateDomainChangeSegment(dir, ref)
	if err != nil {
		return nil, err
	}
	out := make([]*rawdb.StateDomainChange, 0, len(seg.Changes))
	for _, change := range seg.Changes {
		out = append(out, cloneStateDomainChangeForSegment(change))
	}
	return out, nil
}

func (m *Manager) IterateStateDomainChanges(fromTxNum, toTxNum uint64, fn func(*rawdb.StateDomainChange) (bool, error)) error {
	if m == nil {
		return nil
	}
	if toTxNum < fromTxNum {
		return fmt.Errorf("snapshots: state-domain-change range [%d,%d] is inverted", fromTxNum, toTxNum)
	}
	manifest, err := m.currentManifest()
	if err != nil {
		return err
	}
	if manifest == nil {
		return nil
	}
	for _, ref := range manifest.Segments {
		if ref.Dataset != SegmentDatasetStateDomainChange || ref.Kind != SegmentHistory || ref.ToTxNum < fromTxNum || ref.FromTxNum > toTxNum {
			continue
		}
		cfg, ok := DefaultDomainRegistry().ConfigForRef(ref)
		if !ok || (cfg.IterateHistoryRange == nil && cfg.ReadHistoryRange == nil) {
			return fmt.Errorf("snapshots: no history range reader registered for %s", ref.normalizedDataset())
		}
		if cfg.IterateHistoryRange != nil {
			stop := false
			if err := cfg.IterateHistoryRange(m.dir, manifest, ref, fromTxNum, toTxNum, func(change *rawdb.StateDomainChange) (bool, error) {
				cont, err := fn(cloneStateDomainChangeForSegment(change))
				if err != nil {
					return false, err
				}
				if !cont {
					stop = true
					return false, nil
				}
				return true, nil
			}); err != nil {
				return err
			}
			if stop {
				return nil
			}
			continue
		}
		changes, err := cfg.ReadHistoryRange(m.dir, manifest, ref, fromTxNum, toTxNum)
		if err != nil {
			return err
		}
		for _, change := range changes {
			cont, err := fn(cloneStateDomainChangeForSegment(change))
			if err != nil {
				return err
			}
			if !cont {
				return nil
			}
		}
	}
	return nil
}

func (m *Manager) IterateStateDomainChangesByKey(fromTxNum, toTxNum uint64, flatDomain rawdb.StateFlatDomain, owner common.Address, generation uint64, domain kvdomains.KVDomain, logicalKey []byte, fn func(*rawdb.StateDomainChange) (bool, error)) error {
	if m == nil {
		return nil
	}
	if toTxNum < fromTxNum {
		return fmt.Errorf("snapshots: state-domain-change range [%d,%d] is inverted", fromTxNum, toTxNum)
	}
	manifest, err := m.currentManifest()
	if err != nil {
		return err
	}
	if manifest == nil {
		return nil
	}
	lookupKey := stateDomainChangeBinaryAccessorLookupKey(flatDomain, owner, generation, domain, logicalKey)
	match := func(change *rawdb.StateDomainChange) bool {
		return stateDomainChangeMatchesAccessorLookup(change, flatDomain, owner, generation, domain, logicalKey)
	}
	for _, ref := range manifest.Segments {
		if ref.Dataset != SegmentDatasetStateDomainChange || ref.Kind != SegmentHistory || ref.ToTxNum < fromTxNum || ref.FromTxNum > toTxNum {
			continue
		}
		cfg, ok := DefaultDomainRegistry().ConfigForRef(ref)
		if !ok || (cfg.IterateHistoryByKey == nil && cfg.ReadHistoryByKey == nil) {
			return fmt.Errorf("snapshots: no history key reader registered for %s", ref.normalizedDataset())
		}
		if cfg.IterateHistoryByKey != nil {
			stop := false
			if err := cfg.IterateHistoryByKey(m.dir, manifest, ref, lookupKey, fromTxNum, toTxNum, func(change *rawdb.StateDomainChange) (bool, error) {
				if !match(change) {
					return true, nil
				}
				cont, err := fn(cloneStateDomainChangeForSegment(change))
				if err != nil {
					return false, err
				}
				if !cont {
					stop = true
					return false, nil
				}
				return true, nil
			}); err != nil {
				return err
			}
			if stop {
				return nil
			}
			continue
		}
		changes, err := cfg.ReadHistoryByKey(m.dir, manifest, ref, lookupKey, fromTxNum, toTxNum)
		if err != nil {
			return err
		}
		for _, change := range changes {
			if !match(change) {
				continue
			}
			cont, err := fn(cloneStateDomainChangeForSegment(change))
			if err != nil {
				return err
			}
			if !cont {
				return nil
			}
		}
	}
	return nil
}

// FirstStateDomainChangesByKeysContext returns the earliest matching change in
// the requested transaction range for every logical key. Binary history files
// are opened once per segment for the entire batch; this is the primary archive
// read path used when loading the many dynamic properties required by eth_call.
func (m *Manager) FirstStateDomainChangesByKeysContext(ctx context.Context, fromTxNum, toTxNum uint64, flatDomain rawdb.StateFlatDomain, owner common.Address, generation uint64, domain kvdomains.KVDomain, logicalKeys [][]byte) (map[string]*rawdb.StateDomainChange, error) {
	queries := make([]StateHistoryQuery, len(logicalKeys))
	for i, key := range logicalKeys {
		queries[i] = StateHistoryQuery{flatDomain, owner, generation, domain, key}
	}
	changes, err := m.FirstStateDomainChangesContext(ctx, fromTxNum, toTxNum, queries)
	if err != nil {
		return nil, err
	}
	out := make(map[string]*rawdb.StateDomainChange, len(logicalKeys))
	for i, change := range changes {
		if change != nil {
			out[string(logicalKeys[i])] = change
		}
	}
	return out, nil
}

// StateHistoryQuery identifies one physical logical state key. A mixed batch
// shares each authenticated segment/accessor open across owners and domains.
// Results are positional; nil proves no change in the requested closed range.
type StateHistoryQuery struct {
	FlatDomain rawdb.StateFlatDomain
	Owner      common.Address
	Generation uint64
	Domain     kvdomains.KVDomain
	Key        []byte
}

func (m *Manager) FirstStateDomainChangesContext(ctx context.Context, fromTxNum, toTxNum uint64, queries []StateHistoryQuery) ([]*rawdb.StateDomainChange, error) {
	return m.firstStateDomainChangesContext(ctx, fromTxNum, toTxNum, queries, nil)
}

func (m *Manager) firstStateDomainChangesContext(ctx context.Context, fromTxNum, toTxNum uint64, queries []StateHistoryQuery, storageRouting *storageHistoryRoutingReader) ([]*rawdb.StateDomainChange, error) {
	out := make([]*rawdb.StateDomainChange, len(queries))
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if toTxNum < fromTxNum {
		return nil, fmt.Errorf("snapshots: state-domain-change range [%d,%d] is inverted", fromTxNum, toTxNum)
	}
	if m == nil || len(queries) == 0 {
		return out, nil
	}
	manifest, err := m.currentManifest()
	if err != nil || manifest == nil {
		return out, err
	}
	type queryEntry struct {
		query             StateHistoryQuery
		lookup            []byte
		indices           []int
		first             *rawdb.StateDomainChange
		accountBound      uint64
		accountBoundFound bool
	}
	entries := make([]*queryEntry, 0, len(queries))
	byLookup := make(map[string]*queryEntry, len(queries))
	for i, query := range queries {
		switch query.FlatDomain {
		case rawdb.StateFlatDomainAccountLatest, rawdb.StateFlatDomainKVGeneration:
		case rawdb.StateFlatDomainKVLatest:
			if !kvdomains.IsRegistered(query.Domain) {
				return nil, fmt.Errorf("snapshots: unknown history KV domain %d", query.Domain)
			}
		default:
			return nil, fmt.Errorf("snapshots: unknown history flat domain %d", query.FlatDomain)
		}
		lookup := stateDomainChangeBinaryAccessorLookupKey(query.FlatDomain, query.Owner, query.Generation, query.Domain, query.Key)
		entry := byLookup[string(lookup)]
		// Accessors omit the address prefix, while the authoritative match
		// predicate retains it. Do not merge two different full owners merely
		// because they encode the same AccountID. Advisory callers can retry
		// those unusual identities through independent demand reads.
		if entry != nil && entry.query.Owner != query.Owner {
			return nil, errors.New("snapshots: mixed history batch contains aliased owners")
		}
		if entry == nil {
			query.Key = append([]byte(nil), query.Key...)
			entry = &queryEntry{query: query, lookup: lookup}
			entries = append(entries, entry)
			byLookup[string(lookup)] = entry
		}
		entry.indices = append(entry.indices, i)
	}
	var routing *systemHistoryRouting
	broadLoaded := false
	loadBroad := func() error {
		if !broadLoaded {
			broadLoaded = true
			lookups := make([][]byte, 0, len(entries))
			for _, candidate := range entries {
				// Select the whole batch on first load. Metadata/storage can
				// trigger this before an uncovered small-system source does.
				if systemHistoryRoutingQuery(candidate.query) {
					lookups = append(lookups, candidate.lookup)
				}
			}
			routing, err = loadSystemHistoryRoutingForKeys(ctx, m.dir, manifest, lookups, TxRange{From: fromTxNum, To: toTxNum})
		}
		return err
	}
	metadataRouting := newMetadataHistoryRoutingReader(manifest)
	if storageRouting == nil {
		storageRouting = newStorageHistoryRoutingReader(manifest)
	}
	var smallRouting *systemHistoryRouting
	for _, entry := range entries {
		if smallSystemHistoryRoutingQuery(entry.query) {
			smallRouting, err = loadSmallSystemHistoryRouting(ctx, m.dir, manifest, TxRange{From: fromTxNum, To: toTxNum})
			if err != nil {
				return nil, err
			}
			break
		}
	}
	var accountRouting *accountHistoryRoutingReader
	for _, entry := range entries {
		if !accountRoutingQuery(entry.query) {
			continue
		}
		if ref, ok := accountRoutingRef(manifest, fromTxNum, toTxNum); ok {
			accountRouting, err = openAccountHistoryRouting(ctx, m.dir, ref)
			if err != nil {
				return nil, err
			}
			defer accountRouting.Close()
		}
		break
	}
	if accountRouting != nil {
		for _, entry := range entries {
			if !accountRoutingQuery(entry.query) {
				continue
			}
			entry.accountBound, entry.accountBoundFound, err = accountRouting.lookup(ctx, entry.lookup)
			if err != nil {
				return nil, err
			}
		}
	}
	deltaKeys := make([][]byte, 0, len(entries))
	for _, entry := range entries {
		if accountRoutingQuery(entry.query) {
			deltaKeys = append(deltaKeys, entry.lookup)
		}
	}
	accountDeltas := newAccountDeltaHistoryReader(manifest, fromTxNum, toTxNum, deltaKeys)
	planning := make([]historyCandidateQuery, len(entries))
	for i, entry := range entries {
		planning[i] = historyCandidateQuery{entry.query, entry.lookup, entry.accountBound, entry.accountBoundFound}
	}
	candidatePlan, err := newHistoryCandidatePlan(ctx, m.dir, manifest, fromTxNum, toTxNum, planning, routing, smallRouting, accountRouting, metadataRouting)
	if err != nil {
		return nil, err
	}
	consider := func(lookup []byte, change *rawdb.StateDomainChange) (bool, error) {
		entry := byLookup[string(lookup)]
		if entry == nil {
			return false, errors.New("snapshots: unexpected batch history lookup key")
		}
		q := entry.query
		if change == nil || change.TxNum < fromTxNum || change.TxNum > toTxNum || !stateDomainChangeMatchesAccessorLookup(change, q.FlatDomain, q.Owner, q.Generation, q.Domain, q.Key) {
			return true, nil
		}
		if entry.first == nil || change.TxNum < entry.first.TxNum || change.TxNum == entry.first.TxNum && change.Seq < entry.first.Seq {
			entry.first = cloneStateDomainChangeForSegment(change)
		}
		return false, nil
	}
	reads := newHistoryOrderedReads(ctx, func(lookup []byte, change *rawdb.StateDomainChange) bool {
		entry := byLookup[string(lookup)]
		if entry == nil || change == nil || change.TxNum < fromTxNum || change.TxNum > toTxNum {
			return false
		}
		q := entry.query
		return stateDomainChangeMatchesAccessorLookup(change, q.FlatDomain, q.Owner, q.Generation, q.Domain, q.Key)
	})
	defer reads.close()
	active := func(key []byte) bool { return byLookup[string(key)].first == nil }
	flush := func() error {
		for len(reads.queue) > 0 {
			if err := reads.consume(active, consider); err != nil {
				return err
			}
		}
		return nil
	}
	for source, more := candidatePlan.next(active); more; source, more = candidatePlan.next(active) {
		ref := manifest.Segments[source.row]
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if ref.Dataset != SegmentDatasetStateDomainChange || ref.Kind != SegmentHistory || ref.ToTxNum < fromTxNum || ref.FromTxNum > toTxNum {
			continue
		}
		var lookupKeys [][]byte
		if routing == nil && smallRouting == nil && accountRouting == nil {
			lookupKeys = make([][]byte, 0, len(entries))
		}
		column, covered := 0, false
		smallColumn, smallCovered := 0, false
		accountCovered := false
		var metadataFilter metadataSourceFilter
		metadataCovered := false
		var pairID [32]byte
		pairFound := false
		if routing != nil || smallRouting != nil || accountRouting != nil || len(metadataRouting.refs) > 0 || len(storageRouting.refs) > 0 || accountDeltas.ranges != nil && len(accountDeltas.ranges.rows) > 0 || !broadLoaded {
			if accessor, ok := DefaultDomainRegistry().byDataset[SegmentDatasetStateDomainChange].HistoryAccessorRef(manifest, ref); ok {
				id := source.id
				if !source.paired {
					id = systemHistoryRoutingPairID(ref, accessor)
				}
				pairID = id
				pairFound = true
				for _, entry := range entries {
					if entry.first == nil && entry.query.FlatDomain == rawdb.StateFlatDomainKVLatest && entry.query.Domain == kvdomains.ContractMetadata {
						metadataFilter, metadataCovered, err = metadataRouting.source(ctx, m.dir, ref, id)
						if err != nil {
							return nil, err
						}
						break
					}
				}
				if smallRouting != nil {
					smallColumn, smallCovered = smallRouting.columns[id]
				}
				if routing != nil {
					column, covered = routing.columns[id]
				}
				if accountRouting != nil {
					_, accountCovered = accountRouting.metadata.covered[id]
				}
			}
		}
		for _, entry := range entries {
			if entry.first != nil {
				continue
			}
			if entry.query.FlatDomain == rawdb.StateFlatDomainKVLatest && entry.query.Domain == kvdomains.ContractMetadata {
				if metadataCovered && !metadataFilter.mayContain(entry.lookup) {
					continue
				}
				if !metadataCovered {
					if err := loadBroad(); err != nil {
						return nil, err
					}
					if routing != nil && pairFound {
						column, covered = routing.columns[pairID]
					}
				}
			}
			if accountCovered && accountRoutingQuery(entry.query) && (!entry.accountBoundFound || fromTxNum > entry.accountBound || ref.FromTxNum > entry.accountBound) {
				continue
			}
			if smallSystemHistoryRoutingQuery(entry.query) {
				if smallCovered {
					if !smallRouting.mayContain(smallColumn, entry.lookup) {
						continue
					}
					// A complete lightweight proof needs no broad object.
					lookupKeys = append(lookupKeys, entry.lookup)
					continue
				}
				// Bootstrap must retain the old exact-pair negative proofs
				// until the new lightweight route has caught up. Otherwise
				// deployment turns formerly indexed sources into raw scans.
				if pairFound {
					if err := loadBroad(); err != nil {
						return nil, err
					}
					if routing != nil {
						column, covered = routing.columns[pairID]
					}
				}
			}
			if pairFound && contractStorageHistoryRoutingKey(entry.lookup) {
				filter, storageCovered, err := storageRouting.source(ctx, m.dir, ref, pairID)
				if err != nil {
					return nil, err
				}
				if storageCovered {
					if filter.mayContain(entry.lookup) {
						lookupKeys = append(lookupKeys, entry.lookup)
					}
					continue
				}
				// A missing optional page is unknown. Authenticate the broad
				// legacy proof only when this exact pair actually needs it.
				if err := loadBroad(); err != nil {
					return nil, err
				}
				if routing != nil {
					column, covered = routing.columns[pairID]
				}
			}
			if covered && systemHistoryRoutingQuery(entry.query) && !routing.mayContain(column, entry.lookup) {
				continue
			}
			if pairFound && accountRoutingQuery(entry.query) {
				skip, deltaErr := accountDeltas.maySkip(ctx, m.dir, ref, pairID, entry.lookup, fromTxNum)
				if deltaErr != nil {
					return nil, deltaErr
				}
				if skip {
					continue
				}
			}
			lookupKeys = append(lookupKeys, entry.lookup)
		}
		if len(lookupKeys) == 0 {
			// A queued earlier hit must become visible even if every later
			// source is filtered out. Otherwise the queue never reaches its
			// window and a first hit can still open every tail routing page.
			if len(reads.queue) > 0 {
				if err := reads.consume(active, consider); err != nil {
					return nil, err
				}
			}
			// A negative route covers this pair only. Later uncovered segments
			// still require authoritative reads; never terminate the whole range.
			allFound := true
			for _, entry := range entries {
				allFound = allFound && entry.first != nil
			}
			if allFound {
				break
			}
			continue
		}
		cfg, ok := DefaultDomainRegistry().ConfigForRef(ref)
		if !ok {
			return nil, fmt.Errorf("snapshots: no history key reader registered for %s", ref.normalizedDataset())
		}
		if isStateDomainChangeBinarySegmentPath(ref.Path) {
			accessorRef, ok := cfg.HistoryAccessorRef(manifest, ref)
			if !ok {
				return nil, fmt.Errorf("snapshots: binary state-domain-change history %q missing required accessor %q", ref.Path, cfg.HistoryAccessorPathFor(ref.Path))
			}
			// Immutable pairs can issue independent point reads concurrently, but
			// callbacks and query resolution retain the original manifest order.
			reads.submit(&historyReadAheadTask{keys: lookupKeys, read: func(readCtx context.Context, keys [][]byte, emit func([]byte, *rawdb.StateDomainChange) (bool, error)) error {
				return iterateStateDomainChangeBinarySegmentByAccessorKeysFile(readCtx, m.dir, ref, accessorRef, keys, fromTxNum, toTxNum, emit)
			}})
			if len(reads.queue) >= historyReadAheadWindow {
				if err := reads.consume(active, consider); err != nil {
					return nil, err
				}
			}
			continue
		}
		// JSON/legacy readers retain their original sequential path.
		if err := flush(); err != nil {
			return nil, err
		}
		if err := iterateStateDomainChangeHistoryRange(m.dir, manifest, ref, fromTxNum, toTxNum, func(change *rawdb.StateDomainChange) (bool, error) {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			if change != nil {
				lookup := stateDomainChangeBinaryAccessorKey(change)
				if byLookup[string(lookup)] != nil {
					_, err := consider(lookup, change)
					return true, err
				}
			}
			return true, nil
		}); err != nil {
			return nil, err
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for _, entry := range entries {
		for _, i := range entry.indices {
			out[i] = cloneStateDomainChangeForSegment(entry.first)
		}
	}
	return out, nil
}

func (m *Manager) IterateStateDomainChangesByPrefix(fromTxNum, toTxNum uint64, owner common.Address, generation uint64, domain kvdomains.KVDomain, logicalPrefix []byte, fn func(*rawdb.StateDomainChange) (bool, error)) error {
	if m == nil {
		return nil
	}
	if toTxNum < fromTxNum {
		return fmt.Errorf("snapshots: state-domain-change range [%d,%d] is inverted", fromTxNum, toTxNum)
	}
	manifest, err := m.currentManifest()
	if err != nil {
		return err
	}
	if manifest == nil {
		return nil
	}
	lookupPrefix := stateDomainChangeBinaryAccessorLookupPrefix(owner, generation, domain, logicalPrefix)
	match := func(change *rawdb.StateDomainChange) bool {
		return stateDomainChangeMatchesAccessorPrefix(change, owner, generation, domain, logicalPrefix)
	}
	for _, ref := range manifest.Segments {
		if ref.Dataset != SegmentDatasetStateDomainChange || ref.Kind != SegmentHistory || ref.ToTxNum < fromTxNum || ref.FromTxNum > toTxNum {
			continue
		}
		stop := false
		if err := iterateStateDomainChangeHistoryByPrefix(m.dir, manifest, ref, lookupPrefix, fromTxNum, toTxNum, func(change *rawdb.StateDomainChange) (bool, error) {
			if !match(change) {
				return true, nil
			}
			cont, err := fn(cloneStateDomainChangeForSegment(change))
			if err != nil {
				return false, err
			}
			if !cont {
				stop = true
				return false, nil
			}
			return true, nil
		}); err != nil {
			return err
		}
		if stop {
			return nil
		}
	}
	return nil
}

func (m *Manager) RestoreStateDomainHistory(db ethdb.KeyValueWriter, fromTxNum, toTxNum uint64) (*RestoreStateDomainHistoryResult, error) {
	return m.RestoreStateDomainHistoryWithOptions(db, fromTxNum, toTxNum, RestoreETLOptions{})
}

func (m *Manager) RestoreStateDomainHistoryWithOptions(db ethdb.KeyValueWriter, fromTxNum, toTxNum uint64, opts RestoreETLOptions) (*RestoreStateDomainHistoryResult, error) {
	if m == nil {
		return nil, errors.New("snapshots: nil manager")
	}
	if db == nil {
		return nil, errors.New("snapshots: nil database")
	}
	if toTxNum < fromTxNum {
		return nil, fmt.Errorf("snapshots: state-domain history restore range [%d,%d] is inverted", fromTxNum, toTxNum)
	}
	manifest, err := m.currentManifest()
	if err != nil {
		return nil, err
	}
	if manifest == nil {
		return nil, errors.New("snapshots: manifest missing")
	}
	if fromTxNum < manifest.VisibleTxStart || toTxNum > manifest.VisibleTxEnd {
		return nil, fmt.Errorf("snapshots: state-domain history restore range [%d,%d] outside manifest visible range [%d,%d]",
			fromTxNum, toTxNum, manifest.VisibleTxStart, manifest.VisibleTxEnd)
	}
	cfg, ok := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	if !ok || cfg.WriteHotHistoryRow == nil || cfg.WriteHotHistoryIndex == nil || cfg.WriteHotHistoryTxRange == nil {
		return nil, errors.New("snapshots: missing state-domain hot history writers")
	}

	result := &RestoreStateDomainHistoryResult{
		FromTxNum: fromTxNum,
		ToTxNum:   toTxNum,
	}
	collector, err := etl.NewCollector(opts.collectorOptions())
	if err != nil {
		return nil, fmt.Errorf("snapshots: create state-domain history restore ETL collector: %w", err)
	}
	defer collector.Close()
	restoreWriter := ethdb.KeyValueWriter(collector)

	txRanges := make(map[uint64]*rawdb.StateTxRange)
	if err := m.IterateStateDomainChanges(fromTxNum, toTxNum, func(change *rawdb.StateDomainChange) (bool, error) {
		if err := cfg.WriteHotHistoryRow(restoreWriter, change); err != nil {
			return false, err
		}
		if err := cfg.WriteHotHistoryIndex(restoreWriter, change); err != nil {
			return false, err
		}
		if err := mergeStateTxRangeFromChange(txRanges, change); err != nil {
			return false, err
		}
		result.ChangesRestored++
		return true, nil
	}); err != nil {
		return nil, err
	}
	if err := m.restoreExplicitStateTxRanges(txRanges, manifest, fromTxNum, toTxNum); err != nil {
		return nil, err
	}

	blockNums := make([]uint64, 0, len(txRanges))
	for blockNum := range txRanges {
		blockNums = append(blockNums, blockNum)
	}
	sort.Slice(blockNums, func(i, j int) bool { return blockNums[i] < blockNums[j] })
	for _, blockNum := range blockNums {
		row := txRanges[blockNum]
		if err := cfg.WriteHotHistoryTxRange(restoreWriter, row.BlockNum, row.BlockHash, row.BeginTxNum, row.EndTxNum); err != nil {
			return nil, err
		}
		result.TxRangesRestored++
	}
	if _, err := collector.Load(db); err != nil {
		return nil, fmt.Errorf("snapshots: load state-domain history restore ETL collector: %w", err)
	}
	return result, nil
}

func (m *Manager) restoreExplicitStateTxRanges(txRanges map[uint64]*rawdb.StateTxRange, manifest *Manifest, fromTxNum, toTxNum uint64) error {
	for _, ref := range manifest.Segments {
		if ref.Dataset != SegmentDatasetStateDomainChange || ref.Kind != SegmentHistory || ref.ToTxNum < fromTxNum || ref.FromTxNum > toTxNum {
			continue
		}
		if isStateDomainChangeBinarySegmentPath(ref.Path) {
			if err := iterateStateDomainChangeBinaryTxRanges(m.dir, ref, func(row *rawdb.StateTxRange) (bool, error) {
				if row.BeginTxNum < fromTxNum || row.EndTxNum > toTxNum {
					return true, nil
				}
				return true, mergeStateTxRange(txRanges, row)
			}); err != nil {
				return err
			}
			continue
		}
		seg, err := OpenStateDomainChangeSegment(m.dir, ref)
		if err != nil {
			return err
		}
		for _, row := range seg.TxRanges {
			if row == nil || row.BeginTxNum < fromTxNum || row.EndTxNum > toTxNum {
				continue
			}
			if err := mergeStateTxRange(txRanges, cloneStateTxRangeForSegment(row)); err != nil {
				return err
			}
		}
	}
	return nil
}

func mergeStateTxRangeFromChange(txRanges map[uint64]*rawdb.StateTxRange, change *rawdb.StateDomainChange) error {
	if change == nil {
		return nil
	}
	return mergeStateTxRange(txRanges, &rawdb.StateTxRange{
		BlockNum:   change.BlockNum,
		BlockHash:  change.BlockHash,
		BeginTxNum: change.TxNum,
		EndTxNum:   change.TxNum,
	})
}

func mergeStateTxRange(txRanges map[uint64]*rawdb.StateTxRange, row *rawdb.StateTxRange) error {
	if row == nil {
		return nil
	}
	if row.EndTxNum < row.BeginTxNum {
		return fmt.Errorf("snapshots: state tx range for block %d is inverted", row.BlockNum)
	}
	if existing := txRanges[row.BlockNum]; existing != nil {
		if existing.BlockHash != row.BlockHash {
			return fmt.Errorf("snapshots: state-domain history has multiple hashes for block %d", row.BlockNum)
		}
		if row.BeginTxNum < existing.BeginTxNum {
			existing.BeginTxNum = row.BeginTxNum
		}
		if row.EndTxNum > existing.EndTxNum {
			existing.EndTxNum = row.EndTxNum
		}
		return nil
	}
	txRanges[row.BlockNum] = cloneStateTxRangeForSegment(row)
	return nil
}

func (m *Manager) StateTxRangeForBlock(blockNum uint64) (*rawdb.StateTxRange, bool, error) {
	return m.StateTxRangeForBlockContext(context.Background(), blockNum)
}

func (m *Manager) StateTxRangeForBlockContext(ctx context.Context, blockNum uint64) (*rawdb.StateTxRange, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	manifest, err := m.currentManifest()
	if err != nil || manifest == nil {
		return nil, false, err
	}
	var derived *rawdb.StateTxRange
	registry := DefaultDomainRegistry()
	for _, ref := range manifest.Segments {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		if ref.Dataset != SegmentDatasetStateDomainChange || ref.Kind != SegmentHistory {
			continue
		}
		cfg, ok := registry.ConfigForRef(ref)
		if ok && cfg.IsHistoryBinarySegmentPath(ref.Path) {
			idxRef, ok := cfg.HistoryIndexRef(manifest, ref)
			if !ok {
				return nil, false, fmt.Errorf("snapshots: binary state-domain-change history %q missing required index %q", ref.Path, cfg.HistoryIndexPathFor(ref.Path))
			}
			row, ok, err := readStateDomainChangeBinaryTxRangeForBlockByIndexFileContext(ctx, m.dir, ref, idxRef, blockNum)
			if err != nil {
				return nil, false, err
			}
			if !ok {
				continue
			}
			if row.BeginTxNum < manifest.VisibleTxStart || row.EndTxNum > manifest.VisibleTxEnd {
				continue
			}
			if row.BeginTxNum < ref.FromTxNum || row.EndTxNum > ref.ToTxNum {
				continue
			}
			return row, true, nil
		}
		seg, err := OpenStateDomainChangeSegment(m.dir, ref)
		if err != nil {
			return nil, false, err
		}
		for _, row := range seg.TxRanges {
			if row == nil || row.BlockNum != blockNum {
				continue
			}
			if row.BeginTxNum < manifest.VisibleTxStart || row.EndTxNum > manifest.VisibleTxEnd {
				continue
			}
			if row.BeginTxNum < ref.FromTxNum || row.EndTxNum > ref.ToTxNum {
				continue
			}
			return cloneStateTxRangeForSegment(row), true, nil
		}
		if derived == nil {
			derived = stateTxRangeForBlockFromChanges(blockNum, ref, seg.Changes)
		}
	}
	if derived != nil && derived.BeginTxNum >= manifest.VisibleTxStart && derived.EndTxNum <= manifest.VisibleTxEnd {
		return derived, true, nil
	}
	return nil, false, nil
}

func readStateDomainChangeHistoryRange(dir string, manifest *Manifest, ref SegmentRef, fromTxNum, toTxNum uint64) ([]*rawdb.StateDomainChange, error) {
	var changes []*rawdb.StateDomainChange
	err := iterateStateDomainChangeHistoryRange(dir, manifest, ref, fromTxNum, toTxNum, func(change *rawdb.StateDomainChange) (bool, error) {
		changes = append(changes, change)
		return true, nil
	})
	return changes, err
}

func iterateStateDomainChangeHistoryRange(dir string, manifest *Manifest, ref SegmentRef, fromTxNum, toTxNum uint64, fn func(*rawdb.StateDomainChange) (bool, error)) error {
	cfg, ok := DefaultDomainRegistry().ConfigForRef(ref)
	if !ok {
		return fmt.Errorf("snapshots: no history config registered for %s", ref.normalizedDataset())
	}
	if isStateDomainChangeBinarySegmentPath(ref.Path) {
		if idxRef, ok := cfg.HistoryIndexRef(manifest, ref); ok {
			return iterateStateDomainChangeBinarySegmentTxRangeByIndexFile(dir, ref, idxRef, fromTxNum, toTxNum, fn)
		}
		return fmt.Errorf("snapshots: binary state-domain-change history %q missing required index %q", ref.Path, cfg.HistoryIndexPathFor(ref.Path))
	}
	seg, err := OpenStateDomainChangeSegment(dir, ref)
	if err != nil {
		return err
	}
	for _, change := range seg.Changes {
		if change.TxNum >= fromTxNum && change.TxNum <= toTxNum {
			cont, err := fn(change)
			if err != nil || !cont {
				return err
			}
		}
	}
	return nil
}

func readStateDomainChangeHistoryByKey(dir string, manifest *Manifest, ref SegmentRef, lookupKey []byte, fromTxNum, toTxNum uint64) ([]*rawdb.StateDomainChange, error) {
	var changes []*rawdb.StateDomainChange
	err := iterateStateDomainChangeHistoryByKey(dir, manifest, ref, lookupKey, fromTxNum, toTxNum, func(change *rawdb.StateDomainChange) (bool, error) {
		changes = append(changes, change)
		return true, nil
	})
	return changes, err
}

func iterateStateDomainChangeHistoryByKey(dir string, manifest *Manifest, ref SegmentRef, lookupKey []byte, fromTxNum, toTxNum uint64, fn func(*rawdb.StateDomainChange) (bool, error)) error {
	cfg, ok := DefaultDomainRegistry().ConfigForRef(ref)
	if !ok {
		return fmt.Errorf("snapshots: no history config registered for %s", ref.normalizedDataset())
	}
	if isStateDomainChangeBinarySegmentPath(ref.Path) {
		if accessorRef, ok := cfg.HistoryAccessorRef(manifest, ref); ok {
			return iterateStateDomainChangeBinarySegmentByAccessorFile(dir, ref, accessorRef, lookupKey, fromTxNum, toTxNum, fn)
		}
		return fmt.Errorf("snapshots: binary state-domain-change history %q missing required accessor %q", ref.Path, cfg.HistoryAccessorPathFor(ref.Path))
	}
	return iterateStateDomainChangeHistoryRange(dir, manifest, ref, fromTxNum, toTxNum, fn)
}

func iterateStateDomainChangeHistoryByPrefix(dir string, manifest *Manifest, ref SegmentRef, lookupPrefix []byte, fromTxNum, toTxNum uint64, fn func(*rawdb.StateDomainChange) (bool, error)) error {
	cfg, ok := DefaultDomainRegistry().ConfigForRef(ref)
	if !ok {
		return fmt.Errorf("snapshots: no history config registered for %s", ref.normalizedDataset())
	}
	if isStateDomainChangeBinarySegmentPath(ref.Path) {
		if accessorRef, ok := cfg.HistoryAccessorRef(manifest, ref); ok {
			return iterateStateDomainChangeBinarySegmentByAccessorPrefixFile(dir, ref, accessorRef, lookupPrefix, fromTxNum, toTxNum, fn)
		}
		return fmt.Errorf("snapshots: binary state-domain-change history %q missing required accessor %q", ref.Path, cfg.HistoryAccessorPathFor(ref.Path))
	}
	return iterateStateDomainChangeHistoryRange(dir, manifest, ref, fromTxNum, toTxNum, fn)
}

func stateDomainChangeMatchesAccessorLookup(change *rawdb.StateDomainChange, flatDomain rawdb.StateFlatDomain, owner common.Address, generation uint64, domain kvdomains.KVDomain, logicalKey []byte) bool {
	if change == nil || change.FlatDomain != flatDomain || change.Owner != owner {
		return false
	}
	if flatDomain != rawdb.StateFlatDomainKVLatest {
		return true
	}
	return change.Generation == generation && change.Domain == domain && bytes.Equal(change.Key, logicalKey)
}

func stateDomainChangeMatchesAccessorPrefix(change *rawdb.StateDomainChange, owner common.Address, generation uint64, domain kvdomains.KVDomain, logicalPrefix []byte) bool {
	return change != nil &&
		change.FlatDomain == rawdb.StateFlatDomainKVLatest &&
		change.Owner == owner &&
		change.Generation == generation &&
		change.Domain == domain &&
		bytes.HasPrefix(change.Key, logicalPrefix)
}

func (s *StateDomainChangeSegment) Validate() error {
	if s == nil {
		return errors.New("snapshots: nil state-domain-change segment")
	}
	if s.Version != StateDomainChangeSegmentVersion {
		return fmt.Errorf("snapshots: unsupported state-domain-change segment version %d", s.Version)
	}
	if s.Dataset != SegmentDatasetStateDomainChange || s.Kind != SegmentHistory {
		return fmt.Errorf("snapshots: state-domain-change segment has dataset/kind %s/%s", s.Dataset, s.Kind)
	}
	if s.ToTxNum < s.FromTxNum {
		return fmt.Errorf("snapshots: state-domain-change segment range [%d,%d] is inverted", s.FromTxNum, s.ToTxNum)
	}
	for i, change := range s.Changes {
		if change == nil {
			return fmt.Errorf("snapshots: nil state-domain-change entry %d", i)
		}
		if change.TxNum < s.FromTxNum || change.TxNum > s.ToTxNum {
			return fmt.Errorf("snapshots: state-domain-change tx %d outside segment range [%d,%d]", change.TxNum, s.FromTxNum, s.ToTxNum)
		}
		if i > 0 {
			prev := s.Changes[i-1]
			if change.TxNum < prev.TxNum || (change.TxNum == prev.TxNum && change.Seq < prev.Seq) {
				return errors.New("snapshots: state-domain-change entries are not sorted")
			}
		}
	}
	for i, row := range s.TxRanges {
		if row == nil {
			return fmt.Errorf("snapshots: nil state tx range entry %d", i)
		}
		if row.EndTxNum < row.BeginTxNum {
			return fmt.Errorf("snapshots: state tx range for block %d is inverted", row.BlockNum)
		}
		if row.EndTxNum < s.FromTxNum || row.BeginTxNum > s.ToTxNum {
			return fmt.Errorf("snapshots: state tx range for block %d outside segment range [%d,%d]", row.BlockNum, s.FromTxNum, s.ToTxNum)
		}
		if i > 0 && row.BlockNum <= s.TxRanges[i-1].BlockNum {
			return errors.New("snapshots: state tx ranges are not sorted")
		}
	}
	return nil
}

func normalizeStateDomainChanges(changes []*rawdb.StateDomainChange) []*rawdb.StateDomainChange {
	out := make([]*rawdb.StateDomainChange, 0, len(changes))
	for _, change := range changes {
		if change != nil {
			out = append(out, cloneStateDomainChangeForSegment(change))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].TxNum != out[j].TxNum {
			return out[i].TxNum < out[j].TxNum
		}
		return out[i].Seq < out[j].Seq
	})
	return out
}

func cloneStateDomainChangeForSegment(in *rawdb.StateDomainChange) *rawdb.StateDomainChange {
	if in == nil {
		return nil
	}
	out := *in
	out.Key = append([]byte(nil), in.Key...)
	out.Prev = append([]byte(nil), in.Prev...)
	out.Next = append([]byte(nil), in.Next...)
	return &out
}

func firstStateTxRangeSet(sets [][]*rawdb.StateTxRange) []*rawdb.StateTxRange {
	if len(sets) == 0 {
		return nil
	}
	return sets[0]
}

func normalizeStateTxRangesForSegment(ranges []*rawdb.StateTxRange) []*rawdb.StateTxRange {
	out := make([]*rawdb.StateTxRange, 0, len(ranges))
	for _, row := range ranges {
		if row != nil {
			out = append(out, cloneStateTxRangeForSegment(row))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].BlockNum < out[j].BlockNum })
	dedup := out[:0]
	var last uint64
	for i, row := range out {
		if i > 0 && row.BlockNum == last {
			dedup[len(dedup)-1] = row
			continue
		}
		dedup = append(dedup, row)
		last = row.BlockNum
	}
	return dedup
}

func cloneStateTxRangeForSegment(in *rawdb.StateTxRange) *rawdb.StateTxRange {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func stateTxRangeForBlockFromChanges(blockNum uint64, ref SegmentRef, changes []*rawdb.StateDomainChange) *rawdb.StateTxRange {
	var out *rawdb.StateTxRange
	for _, change := range changes {
		if change == nil || change.BlockNum != blockNum {
			continue
		}
		if change.TxNum < ref.FromTxNum || change.TxNum > ref.ToTxNum {
			continue
		}
		if out == nil {
			out = &rawdb.StateTxRange{
				BlockNum:   change.BlockNum,
				BlockHash:  change.BlockHash,
				BeginTxNum: change.TxNum,
				EndTxNum:   change.TxNum,
			}
			continue
		}
		if change.TxNum < out.BeginTxNum {
			out.BeginTxNum = change.TxNum
		}
		if change.TxNum > out.EndTxNum {
			out.EndTxNum = change.TxNum
		}
	}
	return out
}

func isStateDomainChangeBinarySegmentPath(path string) bool {
	return strings.EqualFold(filepath.Ext(path), ".seg") && strings.HasPrefix(filepath.Base(path), "state-domain-change-")
}

func isStateDomainChangeBinaryCompanionPath(path string) bool {
	base := filepath.Base(path)
	ext := filepath.Ext(path)
	return strings.HasPrefix(base, "state-domain-change-") && (strings.EqualFold(ext, ".idx") || strings.EqualFold(ext, ".kv"))
}
