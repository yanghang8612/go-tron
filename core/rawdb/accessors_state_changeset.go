package rawdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// NextStateTxRange returns the compact global txNum range for the next block.
// The range covers every transaction ordinal plus one block-final ordinal for
// maintenance and derived stores flushed after transaction execution.
func NextStateTxRange(parentEndTxNum, txCount uint64) (uint64, uint64, error) {
	if parentEndTxNum == ^uint64(0) {
		return 0, 0, fmt.Errorf("rawdb: state tx range overflows after parent end %d", parentEndTxNum)
	}
	begin := parentEndTxNum + 1
	end, err := StateTxNumAt(begin, txCount)
	if err != nil {
		return 0, 0, err
	}
	return begin, end, nil
}

// StateTxNumAt returns the txNum for an ordinal inside a compact block txNum
// range returned by NextStateTxRange.
func StateTxNumAt(beginTxNum, ordinal uint64) (uint64, error) {
	if ordinal > ^uint64(0)-beginTxNum {
		return 0, fmt.Errorf("rawdb: state tx ordinal %d overflows block begin txNum %d", ordinal, beginTxNum)
	}
	return beginTxNum + ordinal, nil
}

type StateTxRange struct {
	BlockNum   uint64
	BlockHash  common.Hash
	BeginTxNum uint64
	EndTxNum   uint64
}

type StateFlatDomain uint8

const (
	StateFlatDomainUnknown StateFlatDomain = iota
	StateFlatDomainAccountLatest
	StateFlatDomainKVLatest
	StateFlatDomainKVGeneration
)

func (d StateFlatDomain) String() string {
	switch d {
	case StateFlatDomainAccountLatest:
		return "account-latest"
	case StateFlatDomainKVLatest:
		return "kv-latest"
	case StateFlatDomainKVGeneration:
		return "kv-generation"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(d))
	}
}

// StateDomainChange records one mutation in a flat latest domain. Account
// latest and KV generation Prev/Next values are the physical row payloads; KV
// latest values are the decoded account-KV payloads, with the latest-row
// presence wrapper applied only when rows or commitment leaves are restored.
type StateDomainChange struct {
	BlockNum   uint64
	BlockHash  common.Hash
	TxNum      uint64
	Seq        uint64
	FlatDomain StateFlatDomain
	Owner      common.Address
	Generation uint64
	Domain     kvdomains.KVDomain
	Key        []byte
	PrevExists bool
	Prev       []byte
	NextExists bool
	Next       []byte
}

type StateKVHistoryReader interface {
	ethdb.KeyValueReader
	ethdb.Iteratee
}

type stateKVHistoryReader = StateKVHistoryReader

func WriteStateTxRange(db ethdb.KeyValueWriter, blockNum uint64, blockHash common.Hash, beginTxNum, endTxNum uint64) error {
	if endTxNum < beginTxNum {
		return fmt.Errorf("rawdb: invalid state tx range for block %d: [%d,%d]", blockNum, beginTxNum, endTxNum)
	}
	row := &StateTxRange{
		BlockNum:   blockNum,
		BlockHash:  blockHash,
		BeginTxNum: beginTxNum,
		EndTxNum:   endTxNum,
	}
	data, err := rlp.EncodeToBytes(row)
	if err != nil {
		return err
	}
	return db.Put(stateTxRangeKey(blockNum), data)
}

func ReadStateTxRange(db ethdb.KeyValueReader, blockNum uint64) (*StateTxRange, bool, error) {
	data, err := db.Get(stateTxRangeKey(blockNum))
	if err != nil {
		return nil, false, nil
	}
	var row StateTxRange
	if err := rlp.DecodeBytes(data, &row); err != nil {
		return nil, false, err
	}
	return &row, true, nil
}

// StateTxNumAtBlockEnd returns the txNum that represents end-of-block state.
// Blocks written before txNum ranges existed fall back to the legacy blockNum
// value so old block-scoped change rows keep their original ordering.
func StateTxNumAtBlockEnd(db ethdb.KeyValueReader, blockNum uint64) (uint64, error) {
	_, endTxNum, err := stateBlockTxRange(db, blockNum)
	return endTxNum, err
}

func DeleteStateTxRange(db ethdb.KeyValueWriter, blockNum uint64) error {
	return db.Delete(stateTxRangeKey(blockNum))
}

func IterateStateTxRanges(db ethdb.Iteratee, fn func(*StateTxRange) (bool, error)) error {
	it := db.NewIterator(stateTxRangePrefix, nil)
	defer it.Release()
	for it.Next() {
		var row StateTxRange
		if err := rlp.DecodeBytes(it.Value(), &row); err != nil {
			return err
		}
		cont, err := fn(&row)
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return it.Error()
}

func WriteStateDomainChange(db ethdb.KeyValueWriter, change *StateDomainChange) error {
	if err := WriteStateDomainChangeRow(db, change); err != nil {
		return err
	}
	return WriteStateDomainChangeInverseIndex(db, change)
}

// WriteStateDomainChangeRow writes the block-scoped temporal mutation row
// without publishing its latest-key inverse index. Staged publishers use this
// with WriteStateDomainChangeInverseIndex so hot history row and accessor
// publication are explicit domain-stage steps.
func WriteStateDomainChangeRow(db ethdb.KeyValueWriter, change *StateDomainChange) error {
	if change == nil {
		return errors.New("rawdb: nil StateDomainChange")
	}
	if err := validateStateDomainChange(change); err != nil {
		return err
	}
	c := cloneStateDomainChange(change)
	data, err := rlp.EncodeToBytes(c)
	if err != nil {
		return err
	}
	return db.Put(stateChangeSetKey(c.BlockNum, c.Seq), data)
}

// WriteStateDomainChangeInverseIndex writes the latest-key -> block index for
// an already materialized StateDomainChange row.
func WriteStateDomainChangeInverseIndex(db ethdb.KeyValueWriter, change *StateDomainChange) error {
	if change == nil {
		return errors.New("rawdb: nil StateDomainChange")
	}
	if err := validateStateDomainChange(change); err != nil {
		return err
	}
	latestKey, err := stateDomainChangeLatestKey(change)
	if err != nil {
		return err
	}
	return db.Put(stateChangeInverseKey(latestKey, change.BlockNum), nil)
}

func validateStateDomainChange(change *StateDomainChange) error {
	if change == nil {
		return errors.New("rawdb: nil StateDomainChange")
	}
	if _, err := stateDomainChangeLatestKey(change); err != nil {
		return err
	}
	if change.PrevExists {
		if _, err := stateDomainChangeCommitmentValue(change, change.Prev); err != nil {
			return err
		}
	}
	if change.NextExists {
		if _, err := stateDomainChangeCommitmentValue(change, change.Next); err != nil {
			return err
		}
	}
	return nil
}

func ReadStateDomainChange(db ethdb.KeyValueReader, blockNum, seq uint64) (*StateDomainChange, bool, error) {
	data, err := db.Get(stateChangeSetKey(blockNum, seq))
	if err != nil {
		return nil, false, nil
	}
	var row StateDomainChange
	if err := rlp.DecodeBytes(data, &row); err != nil {
		return nil, false, err
	}
	return cloneStateDomainChange(&row), true, nil
}

func IterateStateDomainChanges(db ethdb.Iteratee, blockNum uint64, fn func(*StateDomainChange) (bool, error)) error {
	prefix := stateChangeSetBlockPrefix(blockNum)
	it := db.NewIterator(prefix, nil)
	defer it.Release()
	for it.Next() {
		key := it.Key()
		if !bytes.HasPrefix(key, prefix) {
			continue
		}
		var row StateDomainChange
		if err := rlp.DecodeBytes(it.Value(), &row); err != nil {
			return err
		}
		cont, err := fn(cloneStateDomainChange(&row))
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return it.Error()
}

// IterateStateDomainChangesByTxRange walks StateDomainChange rows whose
// TxNum is inside [fromTxNum, toTxNum]. StateTxRange rows provide the block to
// txNum mapping, so callers can build txNum-native history files without
// scanning unrelated blocks.
func IterateStateDomainChangesByTxRange(db ethdb.Iteratee, fromTxNum, toTxNum uint64, fn func(*StateDomainChange) (bool, error)) error {
	if toTxNum < fromTxNum {
		return fmt.Errorf("rawdb: inverted state domain change tx range [%d,%d]", fromTxNum, toTxNum)
	}
	return IterateStateTxRanges(db, func(row *StateTxRange) (bool, error) {
		if row.EndTxNum < fromTxNum || row.BeginTxNum > toTxNum {
			return true, nil
		}
		if err := IterateStateDomainChanges(db, row.BlockNum, func(change *StateDomainChange) (bool, error) {
			if change.TxNum < fromTxNum || change.TxNum > toTxNum {
				return true, nil
			}
			return fn(change)
		}); err != nil {
			return false, err
		}
		return true, nil
	})
}

func DeleteStateDomainChanges(db stateKVLatestStore, blockNum uint64) error {
	inverseKeys := make([][]byte, 0, resetScanBatch)
	flushInverse := func() error {
		if err := deleteStateKVKeys(db, inverseKeys); err != nil {
			return err
		}
		inverseKeys = inverseKeys[:0]
		return nil
	}
	if err := IterateStateDomainChanges(db, blockNum, func(change *StateDomainChange) (bool, error) {
		latestKey, err := stateDomainChangeLatestKey(change)
		if err != nil {
			return false, err
		}
		key := stateChangeInverseKey(latestKey, change.BlockNum)
		inverseKeys = append(inverseKeys, key)
		if len(inverseKeys) >= resetScanBatch {
			if err := flushInverse(); err != nil {
				return false, err
			}
		}
		return true, nil
	}); err != nil {
		return err
	}
	if err := flushInverse(); err != nil {
		return err
	}
	// Domain pruning deletes a small per-block prefix repeatedly while a node
	// is syncing. Use point deletes here: Pebble range tombstones are excellent
	// for one-shot resets, but high-frequency per-block DeleteRange calls make
	// every later iterator pay keyspan-fragment costs.
	return deleteStateKVPrefixByPointScan(db, stateChangeSetBlockPrefix(blockNum))
}

func IterateStateDomainChangeBlocks(db ethdb.Iteratee, owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte, fn func(blockNum uint64) (bool, error)) error {
	prefix := stateChangeInverseKeyPrefix(StateKVLatestCommitmentKey(owner, generation, domain, key))
	return iterateStateDomainChangeBlocksByInversePrefix(db, prefix, fn)
}

func IterateStateKVGenerationChangeBlocks(db ethdb.Iteratee, owner common.Address, fn func(blockNum uint64) (bool, error)) error {
	prefix := stateChangeInverseKeyPrefix(StateKVGenerationCommitmentKey(owner))
	return iterateStateDomainChangeBlocksByInversePrefix(db, prefix, fn)
}

func IterateStateAccountLatestChangeBlocks(db ethdb.Iteratee, owner common.Address, fn func(blockNum uint64) (bool, error)) error {
	prefix := stateChangeInverseKeyPrefix(StateAccountLatestCommitmentKey(owner))
	return iterateStateDomainChangeBlocksByInversePrefix(db, prefix, fn)
}

func IterateStateDomainChangeBlocksByKey(db ethdb.Iteratee, flatDomain StateFlatDomain, owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte, fn func(blockNum uint64) (bool, error)) error {
	switch flatDomain {
	case StateFlatDomainAccountLatest:
		return IterateStateAccountLatestChangeBlocks(db, owner, fn)
	case StateFlatDomainKVLatest:
		return IterateStateDomainChangeBlocks(db, owner, generation, domain, key, fn)
	case StateFlatDomainKVGeneration:
		return IterateStateKVGenerationChangeBlocks(db, owner, fn)
	default:
		return nil
	}
}

// IterateStateDomainChangesByKey walks hot StateDomainChange rows matching one
// latest-domain logical key inside the tx window (targetTxNum, headTxNum].
func IterateStateDomainChangesByKey(db StateKVHistoryReader, targetTxNum, headTxNum uint64, flatDomain StateFlatDomain, owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte, fn func(*StateDomainChange) (bool, error)) error {
	if targetTxNum >= headTxNum {
		return nil
	}
	blocks := make(map[uint64]struct{})
	if err := IterateStateDomainChangeBlocksByKey(db, flatDomain, owner, generation, domain, key, func(blockNum uint64) (bool, error) {
		ok, err := stateBlockIntersectsTxWindow(db, blockNum, targetTxNum, headTxNum)
		if err != nil {
			return false, err
		}
		if ok {
			blocks[blockNum] = struct{}{}
		}
		return true, nil
	}); err != nil {
		return err
	}
	ordered := make([]uint64, 0, len(blocks))
	for blockNum := range blocks {
		ordered = append(ordered, blockNum)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	for _, blockNum := range ordered {
		if err := IterateStateDomainChanges(db, blockNum, func(change *StateDomainChange) (bool, error) {
			if !stateDomainChangeInTxWindow(change, targetTxNum, headTxNum) {
				return true, nil
			}
			if !stateDomainChangeMatchesKey(change, flatDomain, owner, generation, domain, key) {
				return true, nil
			}
			return fn(change)
		}); err != nil {
			return err
		}
	}
	return nil
}

// IterateStateDomainChangesByPrefix walks hot KV-latest StateDomainChange rows
// matching one logical key prefix inside the tx window (targetTxNum, headTxNum].
func IterateStateDomainChangesByPrefix(db StateKVHistoryReader, targetTxNum, headTxNum uint64, owner common.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte, fn func(*StateDomainChange) (bool, error)) error {
	if targetTxNum >= headTxNum {
		return nil
	}
	blocks := make(map[uint64]struct{})
	if err := IterateStateDomainChangeBlocksByPrefix(db, owner, generation, domain, prefix, func(blockNum uint64) (bool, error) {
		ok, err := stateBlockIntersectsTxWindow(db, blockNum, targetTxNum, headTxNum)
		if err != nil {
			return false, err
		}
		if ok {
			blocks[blockNum] = struct{}{}
		}
		return true, nil
	}); err != nil {
		return err
	}
	ordered := make([]uint64, 0, len(blocks))
	for blockNum := range blocks {
		ordered = append(ordered, blockNum)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	for _, blockNum := range ordered {
		if err := IterateStateDomainChanges(db, blockNum, func(change *StateDomainChange) (bool, error) {
			if !stateDomainChangeInTxWindow(change, targetTxNum, headTxNum) {
				return true, nil
			}
			if !stateDomainChangeMatchesKVLatestPrefix(change, owner, generation, domain, prefix) {
				return true, nil
			}
			return fn(change)
		}); err != nil {
			return err
		}
	}
	return nil
}

func stateDomainChangeMatchesKey(change *StateDomainChange, flatDomain StateFlatDomain, owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte) bool {
	if change == nil || change.FlatDomain != flatDomain || change.Owner != owner {
		return false
	}
	switch flatDomain {
	case StateFlatDomainAccountLatest:
		return true
	case StateFlatDomainKVLatest:
		return change.Generation == generation && change.Domain == domain && bytes.Equal(change.Key, key)
	case StateFlatDomainKVGeneration:
		return true
	default:
		return false
	}
}

func iterateStateDomainChangeBlocksByInversePrefix(db ethdb.Iteratee, prefix []byte, fn func(blockNum uint64) (bool, error)) error {
	it := db.NewIterator(prefix, nil)
	defer it.Release()
	for it.Next() {
		blockNum, ok := StateDomainChangeInverseBlockNum(it.Key(), len(prefix))
		if !ok {
			continue
		}
		cont, err := fn(blockNum)
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return it.Error()
}

func IterateStateDomainChangeBlocksByPrefix(db ethdb.Iteratee, owner common.Address, generation uint64, domain kvdomains.KVDomain, keyPrefix []byte, fn func(blockNum uint64) (bool, error)) error {
	prefix := stateChangeInverseKeyPrefix(StateKVLatestCommitmentKey(owner, generation, domain, keyPrefix))
	it := db.NewIterator(prefix, nil)
	defer it.Release()
	seen := make(map[uint64]struct{})
	for it.Next() {
		if !bytes.HasPrefix(it.Key(), prefix) {
			continue
		}
		if len(it.Key()) < len(prefix)+8 {
			continue
		}
		blockNum := binary.BigEndian.Uint64(it.Key()[len(it.Key())-8:])
		if _, ok := seen[blockNum]; ok {
			continue
		}
		seen[blockNum] = struct{}{}
		cont, err := fn(blockNum)
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return it.Error()
}

func StateDomainChangeInverseBlockNum(key []byte, prefixLen int) (uint64, bool) {
	if len(key) != prefixLen+8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(key[prefixLen:]), true
}

func stateDomainChangeLatestKey(change *StateDomainChange) ([]byte, error) {
	if change == nil {
		return nil, errors.New("rawdb: nil StateDomainChange")
	}
	switch change.FlatDomain {
	case StateFlatDomainAccountLatest:
		return StateAccountLatestCommitmentKey(change.Owner), nil
	case StateFlatDomainKVLatest:
		if !kvdomains.IsRegistered(change.Domain) {
			return nil, fmt.Errorf("rawdb: unregistered change KV domain %#04x", uint16(change.Domain))
		}
		return StateKVLatestCommitmentKey(change.Owner, change.Generation, change.Domain, change.Key), nil
	case StateFlatDomainKVGeneration:
		return StateKVGenerationCommitmentKey(change.Owner), nil
	default:
		return nil, fmt.Errorf("rawdb: unknown state flat domain %d", uint8(change.FlatDomain))
	}
}

func stateDomainChangeCommitmentValue(change *StateDomainChange, value []byte) ([]byte, error) {
	switch change.FlatDomain {
	case StateFlatDomainAccountLatest:
		return append([]byte(nil), value...), nil
	case StateFlatDomainKVLatest:
		return EncodeStateKVLatestValue(value), nil
	case StateFlatDomainKVGeneration:
		if len(value) != 8 {
			return nil, fmt.Errorf("rawdb: bad KV generation change value length %d", len(value))
		}
		return append([]byte(nil), value...), nil
	default:
		return nil, fmt.Errorf("rawdb: unknown state flat domain %d", uint8(change.FlatDomain))
	}
}

func writeStateDomainLatestRow(db stateKVLatestStore, change *StateDomainChange) error {
	switch change.FlatDomain {
	case StateFlatDomainAccountLatest:
		return WriteStateAccountLatest(db, change.Owner, change.Prev)
	case StateFlatDomainKVLatest:
		return WriteStateKVLatest(db, change.Owner, change.Generation, change.Domain, change.Key, change.Prev)
	case StateFlatDomainKVGeneration:
		if len(change.Prev) != 8 {
			return fmt.Errorf("rawdb: bad KV generation change value length %d", len(change.Prev))
		}
		return WriteStateKVGeneration(db, change.Owner, binary.BigEndian.Uint64(change.Prev))
	default:
		return fmt.Errorf("rawdb: unknown state flat domain %d", uint8(change.FlatDomain))
	}
}

func deleteStateDomainLatestRow(db stateKVLatestStore, change *StateDomainChange) error {
	switch change.FlatDomain {
	case StateFlatDomainAccountLatest:
		return DeleteStateAccountLatest(db, change.Owner)
	case StateFlatDomainKVLatest:
		return DeleteStateKVLatest(db, change.Owner, change.Generation, change.Domain, change.Key)
	case StateFlatDomainKVGeneration:
		return DeleteStateKVGeneration(db, change.Owner)
	default:
		return fmt.Errorf("rawdb: unknown state flat domain %d", uint8(change.FlatDomain))
	}
}

func stateDomainChangeMatchesKVLatest(change *StateDomainChange, owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte) bool {
	return change.FlatDomain == StateFlatDomainKVLatest &&
		change.Owner == owner &&
		change.Generation == generation &&
		change.Domain == domain &&
		bytes.Equal(change.Key, key)
}

func stateDomainChangeMatchesKVLatestPrefix(change *StateDomainChange, owner common.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte) bool {
	return change.FlatDomain == StateFlatDomainKVLatest &&
		change.Owner == owner &&
		change.Generation == generation &&
		change.Domain == domain &&
		bytes.HasPrefix(change.Key, prefix)
}

func stateDomainChangeMatchesKVGeneration(change *StateDomainChange, owner common.Address) bool {
	return change.FlatDomain == StateFlatDomainKVGeneration && change.Owner == owner
}

func stateDomainChangeMatchesAccountLatest(change *StateDomainChange, owner common.Address) bool {
	return change.FlatDomain == StateFlatDomainAccountLatest && change.Owner == owner
}

func stateDomainChangeInTxWindow(change *StateDomainChange, targetTxNum, headTxNum uint64) bool {
	return change.TxNum > targetTxNum && change.TxNum <= headTxNum
}

func stateBlockTxRange(db ethdb.KeyValueReader, blockNum uint64) (uint64, uint64, error) {
	row, ok, err := ReadStateTxRange(db, blockNum)
	if err != nil {
		return 0, 0, err
	}
	if !ok {
		return blockNum, blockNum, nil
	}
	if row.EndTxNum < row.BeginTxNum {
		return 0, 0, fmt.Errorf("rawdb: invalid stored state tx range for block %d: [%d,%d]", blockNum, row.BeginTxNum, row.EndTxNum)
	}
	return row.BeginTxNum, row.EndTxNum, nil
}

func stateBlockIntersectsTxWindow(db ethdb.KeyValueReader, blockNum, targetTxNum, headTxNum uint64) (bool, error) {
	beginTxNum, endTxNum, err := stateBlockTxRange(db, blockNum)
	if err != nil {
		return false, err
	}
	return endTxNum > targetTxNum && beginTxNum <= headTxNum, nil
}

// ReadStateKVAsOf reconstructs one account-KV latest value at the end of
// targetBlock by starting from the current latest row and rolling back captured
// block change sets in (targetBlock, headBlock].
func ReadStateKVAsOf(db stateKVHistoryReader, owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte, targetBlock, headBlock uint64) ([]byte, bool, error) {
	targetTxNum, err := StateTxNumAtBlockEnd(db, targetBlock)
	if err != nil {
		return nil, false, err
	}
	headTxNum, err := StateTxNumAtBlockEnd(db, headBlock)
	if err != nil {
		return nil, false, err
	}
	return ReadStateKVAsOfTxNum(db, owner, generation, domain, key, targetTxNum, headTxNum)
}

func ReadStateAccountLatestAsOf(db stateKVHistoryReader, owner common.Address, targetBlock, headBlock uint64) ([]byte, bool, error) {
	targetTxNum, err := StateTxNumAtBlockEnd(db, targetBlock)
	if err != nil {
		return nil, false, err
	}
	headTxNum, err := StateTxNumAtBlockEnd(db, headBlock)
	if err != nil {
		return nil, false, err
	}
	return ReadStateAccountLatestAsOfTxNum(db, owner, targetTxNum, headTxNum)
}

func ReadStateAccountLatestAsOfTxNum(db stateKVHistoryReader, owner common.Address, targetTxNum, headTxNum uint64) ([]byte, bool, error) {
	value, exists, err := ReadStateAccountLatest(db, owner)
	if err != nil {
		return nil, false, err
	}
	if targetTxNum >= headTxNum {
		return value, exists, nil
	}
	changes, err := collectStateDomainChangesByKey(db, targetTxNum, headTxNum, StateFlatDomainAccountLatest, owner, 0, 0, nil)
	if err != nil {
		return nil, false, err
	}
	for i := len(changes) - 1; i >= 0; i-- {
		change := changes[i]
		if change.PrevExists {
			value = append([]byte(nil), change.Prev...)
			exists = true
		} else {
			value = nil
			exists = false
		}
	}
	return append([]byte(nil), value...), exists, nil
}

// ReadStateKVAsOfTxNum reconstructs one account-KV latest value at the end of
// targetTxNum by starting from the current latest row and rolling back captured
// domain changes in (targetTxNum, headTxNum].
func ReadStateKVAsOfTxNum(db stateKVHistoryReader, owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte, targetTxNum, headTxNum uint64) ([]byte, bool, error) {
	value, exists, err := ReadStateKVLatest(db, owner, generation, domain, key)
	if err != nil {
		return nil, false, err
	}
	if targetTxNum >= headTxNum {
		return value, exists, nil
	}
	changes, err := collectStateDomainChangesByKey(db, targetTxNum, headTxNum, StateFlatDomainKVLatest, owner, generation, domain, key)
	if err != nil {
		return nil, false, err
	}
	for i := len(changes) - 1; i >= 0; i-- {
		change := changes[i]
		if change.PrevExists {
			value = append([]byte(nil), change.Prev...)
			exists = true
		} else {
			value = nil
			exists = false
		}
	}
	return append([]byte(nil), value...), exists, nil
}

func ReadStateKVGenerationAsOf(db stateKVHistoryReader, owner common.Address, targetBlock, headBlock uint64) (uint64, bool, error) {
	targetTxNum, err := StateTxNumAtBlockEnd(db, targetBlock)
	if err != nil {
		return 0, false, err
	}
	headTxNum, err := StateTxNumAtBlockEnd(db, headBlock)
	if err != nil {
		return 0, false, err
	}
	return ReadStateKVGenerationAsOfTxNum(db, owner, targetTxNum, headTxNum)
}

func ReadStateKVGenerationAsOfTxNum(db stateKVHistoryReader, owner common.Address, targetTxNum, headTxNum uint64) (uint64, bool, error) {
	generation, exists, err := ReadStateKVGeneration(db, owner)
	if err != nil {
		return 0, false, err
	}
	if targetTxNum >= headTxNum {
		return generation, exists, nil
	}
	changes, err := collectStateDomainChangesByKey(db, targetTxNum, headTxNum, StateFlatDomainKVGeneration, owner, 0, 0, nil)
	if err != nil {
		return 0, false, err
	}
	for i := len(changes) - 1; i >= 0; i-- {
		change := changes[i]
		if !change.PrevExists {
			generation = 0
			exists = false
			continue
		}
		prev, err := DecodeStateKVGenerationValue(change.Prev)
		if err != nil {
			return 0, false, err
		}
		generation = prev
		exists = true
	}
	return generation, exists, nil
}

func collectStateDomainChangesByKey(db stateKVHistoryReader, targetTxNum, headTxNum uint64, flatDomain StateFlatDomain, owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte) ([]*StateDomainChange, error) {
	var changes []*StateDomainChange
	if err := IterateStateDomainChangesByKey(db, targetTxNum, headTxNum, flatDomain, owner, generation, domain, key, func(change *StateDomainChange) (bool, error) {
		changes = append(changes, change)
		return true, nil
	}); err != nil {
		return nil, err
	}
	sortStateDomainChangesForReplay(changes)
	return changes, nil
}

func collectStateDomainChangesByPrefix(db stateKVHistoryReader, targetTxNum, headTxNum uint64, owner common.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte) ([]*StateDomainChange, error) {
	var changes []*StateDomainChange
	if err := IterateStateDomainChangesByPrefix(db, targetTxNum, headTxNum, owner, generation, domain, prefix, func(change *StateDomainChange) (bool, error) {
		changes = append(changes, change)
		return true, nil
	}); err != nil {
		return nil, err
	}
	sortStateDomainChangesForReplay(changes)
	return changes, nil
}

func collectStateAccountKVChangesByTxNum(db stateKVHistoryReader, owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte, targetTxNum, headTxNum uint64) ([]*StateDomainChange, error) {
	changes, err := collectStateDomainChangesByKey(db, targetTxNum, headTxNum, StateFlatDomainKVLatest, owner, generation, domain, key)
	if err != nil {
		return nil, err
	}
	generationChanges, err := collectStateDomainChangesByKey(db, targetTxNum, headTxNum, StateFlatDomainKVGeneration, owner, 0, 0, nil)
	if err != nil {
		return nil, err
	}
	changes = append(changes, generationChanges...)
	sortStateDomainChangesForReplay(changes)
	return changes, nil
}

func collectStateAccountKVPrefixChangesByTxNum(db stateKVHistoryReader, owner common.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte, targetTxNum, headTxNum uint64) ([]*StateDomainChange, error) {
	changes, err := collectStateDomainChangesByPrefix(db, targetTxNum, headTxNum, owner, generation, domain, prefix)
	if err != nil {
		return nil, err
	}
	generationChanges, err := collectStateDomainChangesByKey(db, targetTxNum, headTxNum, StateFlatDomainKVGeneration, owner, 0, 0, nil)
	if err != nil {
		return nil, err
	}
	changes = append(changes, generationChanges...)
	sortStateDomainChangesForReplay(changes)
	return changes, nil
}

func sortStateDomainChangesForReplay(changes []*StateDomainChange) {
	sort.Slice(changes, func(i, j int) bool {
		if changes[i].BlockNum != changes[j].BlockNum {
			return changes[i].BlockNum < changes[j].BlockNum
		}
		if changes[i].Seq != changes[j].Seq {
			return changes[i].Seq < changes[j].Seq
		}
		return changes[i].TxNum < changes[j].TxNum
	})
}

func ReadStateAccountKVAsOf(db stateKVHistoryReader, owner common.Address, domain kvdomains.KVDomain, key []byte, targetBlock, headBlock uint64) ([]byte, bool, error) {
	targetTxNum, err := StateTxNumAtBlockEnd(db, targetBlock)
	if err != nil {
		return nil, false, err
	}
	headTxNum, err := StateTxNumAtBlockEnd(db, headBlock)
	if err != nil {
		return nil, false, err
	}
	return ReadStateAccountKVAsOfTxNum(db, owner, domain, key, targetTxNum, headTxNum)
}

func ReadStateAccountKVAsOfTxNum(db stateKVHistoryReader, owner common.Address, domain kvdomains.KVDomain, key []byte, targetTxNum, headTxNum uint64) ([]byte, bool, error) {
	generation, _, err := ReadStateKVGeneration(db, owner)
	if err != nil {
		return nil, false, err
	}
	value, exists, err := ReadStateKVLatest(db, owner, generation, domain, key)
	if err != nil {
		return nil, false, err
	}
	if targetTxNum >= headTxNum {
		return value, exists, nil
	}
	upperTxNum := headTxNum
	for targetTxNum < upperTxNum {
		changes, err := collectStateAccountKVChangesByTxNum(db, owner, generation, domain, key, targetTxNum, upperTxNum)
		if err != nil {
			return nil, false, err
		}
		if len(changes) == 0 {
			break
		}
		generationChanged := false
		for i := len(changes) - 1; i >= 0; i-- {
			change := changes[i]
			switch {
			case stateDomainChangeMatchesKVLatest(change, owner, generation, domain, key):
				if change.PrevExists {
					value = append([]byte(nil), change.Prev...)
					exists = true
				} else {
					value = nil
					exists = false
				}
			case stateDomainChangeMatchesKVGeneration(change, owner):
				generation, err = prevStateKVGeneration(change)
				if err != nil {
					return nil, false, err
				}
				value, exists, err = ReadStateKVLatest(db, owner, generation, domain, key)
				if err != nil {
					return nil, false, err
				}
				if change.TxNum == 0 {
					upperTxNum = 0
				} else {
					upperTxNum = change.TxNum - 1
				}
				generationChanged = true
			}
			if generationChanged {
				break
			}
		}
		if !generationChanged {
			break
		}
	}
	return append([]byte(nil), value...), exists, nil
}

func prevStateKVGeneration(change *StateDomainChange) (uint64, error) {
	if change == nil || !change.PrevExists {
		return 0, nil
	}
	return DecodeStateKVGenerationValue(change.Prev)
}

func IterateStateKVAsOfPrefix(db stateKVHistoryReader, owner common.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte, targetBlock, headBlock uint64, fn func(key, value []byte) (bool, error)) error {
	targetTxNum, err := StateTxNumAtBlockEnd(db, targetBlock)
	if err != nil {
		return err
	}
	headTxNum, err := StateTxNumAtBlockEnd(db, headBlock)
	if err != nil {
		return err
	}
	return IterateStateKVAsOfPrefixTxNum(db, owner, generation, domain, prefix, targetTxNum, headTxNum, fn)
}

func IterateStateKVAsOfPrefixTxNum(db stateKVHistoryReader, owner common.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte, targetTxNum, headTxNum uint64, fn func(key, value []byte) (bool, error)) error {
	entries := make(map[string][]byte)
	if err := IterateStateKVLatest(db, owner, generation, domain, prefix, func(key, value []byte) (bool, error) {
		entries[string(key)] = append([]byte(nil), value...)
		return true, nil
	}); err != nil {
		return err
	}
	if targetTxNum < headTxNum {
		changes, err := collectStateDomainChangesByPrefix(db, targetTxNum, headTxNum, owner, generation, domain, prefix)
		if err != nil {
			return err
		}
		for i := len(changes) - 1; i >= 0; i-- {
			change := changes[i]
			if change.PrevExists {
				entries[string(change.Key)] = append([]byte(nil), change.Prev...)
			} else {
				delete(entries, string(change.Key))
			}
		}
	}
	return iterateStateKVEntries(entries, fn)
}

func iterateStateKVEntries(entries map[string][]byte, fn func(key, value []byte) (bool, error)) error {
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		cont, err := fn([]byte(key), append([]byte(nil), entries[key]...))
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return nil
}

func IterateStateAccountKVAsOfPrefix(db stateKVHistoryReader, owner common.Address, domain kvdomains.KVDomain, prefix []byte, targetBlock, headBlock uint64, fn func(key, value []byte) (bool, error)) error {
	targetTxNum, err := StateTxNumAtBlockEnd(db, targetBlock)
	if err != nil {
		return err
	}
	headTxNum, err := StateTxNumAtBlockEnd(db, headBlock)
	if err != nil {
		return err
	}
	return IterateStateAccountKVAsOfPrefixTxNum(db, owner, domain, prefix, targetTxNum, headTxNum, fn)
}

func IterateStateAccountKVAsOfPrefixTxNum(db stateKVHistoryReader, owner common.Address, domain kvdomains.KVDomain, prefix []byte, targetTxNum, headTxNum uint64, fn func(key, value []byte) (bool, error)) error {
	generation, _, err := ReadStateKVGeneration(db, owner)
	if err != nil {
		return err
	}
	entries := make(map[string][]byte)
	if err := readStateKVLatestPrefixInto(db, owner, generation, domain, prefix, entries); err != nil {
		return err
	}
	upperTxNum := headTxNum
	for targetTxNum < upperTxNum {
		changes, err := collectStateAccountKVPrefixChangesByTxNum(db, owner, generation, domain, prefix, targetTxNum, upperTxNum)
		if err != nil {
			return err
		}
		if len(changes) == 0 {
			break
		}
		generationChanged := false
		for i := len(changes) - 1; i >= 0; i-- {
			change := changes[i]
			switch {
			case stateDomainChangeMatchesKVLatestPrefix(change, owner, generation, domain, prefix):
				if change.PrevExists {
					entries[string(change.Key)] = append([]byte(nil), change.Prev...)
				} else {
					delete(entries, string(change.Key))
				}
			case stateDomainChangeMatchesKVGeneration(change, owner):
				generation, err = prevStateKVGeneration(change)
				if err != nil {
					return err
				}
				entries = make(map[string][]byte)
				if err := readStateKVLatestPrefixInto(db, owner, generation, domain, prefix, entries); err != nil {
					return err
				}
				if change.TxNum == 0 {
					upperTxNum = 0
				} else {
					upperTxNum = change.TxNum - 1
				}
				generationChanged = true
			}
			if generationChanged {
				break
			}
		}
		if !generationChanged {
			break
		}
	}
	return iterateStateKVEntries(entries, fn)
}

func readStateKVLatestPrefixInto(db stateKVHistoryReader, owner common.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte, entries map[string][]byte) error {
	return IterateStateKVLatest(db, owner, generation, domain, prefix, func(key, value []byte) (bool, error) {
		entries[string(key)] = append([]byte(nil), value...)
		return true, nil
	})
}

func cloneStateDomainChange(in *StateDomainChange) *StateDomainChange {
	if in == nil {
		return nil
	}
	out := *in
	out.Key = append([]byte(nil), in.Key...)
	out.Prev = append([]byte(nil), in.Prev...)
	out.Next = append([]byte(nil), in.Next...)
	return &out
}
