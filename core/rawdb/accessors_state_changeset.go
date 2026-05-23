package rawdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// BlockStateTxNum returns the first phase's monotonic state transaction number.
// This phase assigns one txNum per block; later phases can expand the
// corresponding StateTxRange without changing the block-level call sites.
func BlockStateTxNum(blockNum uint64) uint64 {
	return blockNum
}

type StateTxRange struct {
	BlockNum   uint64
	BlockHash  common.Hash
	BeginTxNum uint64
	EndTxNum   uint64
}

type StateDomainChange struct {
	BlockNum   uint64
	BlockHash  common.Hash
	TxNum      uint64
	Seq        uint64
	Owner      common.Address
	Generation uint64
	Domain     kvdomains.KVDomain
	Key        []byte
	PrevExists bool
	Prev       []byte
	NextExists bool
	Next       []byte
}

func WriteStateTxRange(db ethdb.KeyValueWriter, blockNum uint64, blockHash common.Hash, beginTxNum, endTxNum uint64) error {
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
	if change == nil {
		return errors.New("rawdb: nil StateDomainChange")
	}
	if !kvdomains.IsRegistered(change.Domain) {
		return fmt.Errorf("rawdb: unregistered change domain %#04x", uint16(change.Domain))
	}
	c := cloneStateDomainChange(change)
	data, err := rlp.EncodeToBytes(c)
	if err != nil {
		return err
	}
	if err := db.Put(stateChangeSetKey(c.BlockNum, c.Seq), data); err != nil {
		return err
	}
	return db.Put(stateChangeInverseKey(c.Owner, c.Generation, c.Domain, c.Key, c.BlockNum), nil)
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

func DeleteStateDomainChanges(db stateKVLatestStore, blockNum uint64) error {
	if err := IterateStateDomainChanges(db, blockNum, func(change *StateDomainChange) (bool, error) {
		key := stateChangeInverseKey(change.Owner, change.Generation, change.Domain, change.Key, change.BlockNum)
		return true, db.Delete(key)
	}); err != nil {
		return err
	}
	return deleteStateKVPrefixByScan(db, stateChangeSetBlockPrefix(blockNum))
}

func IterateStateDomainChangeBlocks(db ethdb.Iteratee, owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte, fn func(blockNum uint64) (bool, error)) error {
	prefix := stateChangeInverseKeyPrefix(owner, generation, domain, key)
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

func StateDomainChangeInverseBlockNum(key []byte, prefixLen int) (uint64, bool) {
	if len(key) != prefixLen+8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(key[prefixLen:]), true
}

// UnwindStateDomainChanges restores the physical latest-state index to the
// state before blockNum for every generic-domain row captured in that block.
// It intentionally leaves the change-set rows themselves intact; callers decide
// whether they are pruning history or merely checking an unwind candidate.
func UnwindStateDomainChanges(db stateKVLatestStore, blockNum uint64) error {
	var changes []*StateDomainChange
	if err := IterateStateDomainChanges(db, blockNum, func(change *StateDomainChange) (bool, error) {
		changes = append(changes, change)
		return true, nil
	}); err != nil {
		return err
	}
	for i := len(changes) - 1; i >= 0; i-- {
		change := changes[i]
		if change.PrevExists {
			if err := WriteStateKVLatest(db, change.Owner, change.Generation, change.Domain, change.Key, change.Prev); err != nil {
				return err
			}
			continue
		}
		if err := DeleteStateKVLatest(db, change.Owner, change.Generation, change.Domain, change.Key); err != nil {
			return err
		}
	}
	return nil
}

// ReadStateKVAsOf reconstructs one generic latest-domain value at the end of
// targetBlock by starting from the current latest row and rolling back captured
// block change sets in (targetBlock, headBlock]. This first archive-domain
// reader is intentionally simple and block-scanning; later phases add inverted
// indexes so callers can jump directly to blocks that touched the requested key.
func ReadStateKVAsOf(db stateKVLatestStore, owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte, targetBlock, headBlock uint64) ([]byte, bool, error) {
	value, exists, err := ReadStateKVLatest(db, owner, generation, domain, key)
	if err != nil {
		return nil, false, err
	}
	if targetBlock >= headBlock {
		return value, exists, nil
	}
	var touched []uint64
	if err := IterateStateDomainChangeBlocks(db, owner, generation, domain, key, func(blockNum uint64) (bool, error) {
		if blockNum > targetBlock && blockNum <= headBlock {
			touched = append(touched, blockNum)
		}
		return true, nil
	}); err != nil {
		return nil, false, err
	}
	for i := len(touched) - 1; i >= 0; i-- {
		blockNum := touched[i]
		var changes []*StateDomainChange
		if err := IterateStateDomainChanges(db, blockNum, func(change *StateDomainChange) (bool, error) {
			changes = append(changes, change)
			return true, nil
		}); err != nil {
			return nil, false, err
		}
		for i := len(changes) - 1; i >= 0; i-- {
			change := changes[i]
			if change.Owner != owner ||
				change.Generation != generation ||
				change.Domain != domain ||
				!bytes.Equal(change.Key, key) {
				continue
			}
			if change.PrevExists {
				value = append([]byte(nil), change.Prev...)
				exists = true
			} else {
				value = nil
				exists = false
			}
		}
	}
	return append([]byte(nil), value...), exists, nil
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
