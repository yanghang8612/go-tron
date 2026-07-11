package snapshots

import (
	"bytes"
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
		result, err := buildStateDomainChangeHistoryBinarySegmentsFromDB(db, dir, ref, cfg, etl.Options{})
		if err != nil {
			return nil, err
		}
		return result.refs, nil
	}

	var changes []*rawdb.StateDomainChange
	if err := cfg.IterateHotHistoryTxRangeChanges(db, fromTxNum, toTxNum, func(change *rawdb.StateDomainChange) (bool, error) {
		changes = append(changes, cloneStateDomainChangeForSegment(change))
		return true, nil
	}); err != nil {
		return nil, err
	}
	var txRanges []*rawdb.StateTxRange
	if err := cfg.IterateHotHistoryTxRanges(db, func(row *rawdb.StateTxRange) (bool, error) {
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
	manifest, err := m.currentManifest()
	if err != nil || manifest == nil {
		return nil, false, err
	}
	var derived *rawdb.StateTxRange
	registry := DefaultDomainRegistry()
	for _, ref := range manifest.Segments {
		if ref.Dataset != SegmentDatasetStateDomainChange || ref.Kind != SegmentHistory {
			continue
		}
		cfg, ok := registry.ConfigForRef(ref)
		if ok && cfg.IsHistoryBinarySegmentPath(ref.Path) {
			idxRef, ok := cfg.HistoryIndexRef(manifest, ref)
			if !ok {
				return nil, false, fmt.Errorf("snapshots: binary state-domain-change history %q missing required index %q", ref.Path, cfg.HistoryIndexPathFor(ref.Path))
			}
			row, ok, err := readStateDomainChangeBinaryTxRangeForBlockByIndexFile(m.dir, ref, idxRef, blockNum)
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
