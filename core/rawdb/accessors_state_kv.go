package rawdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

const stateKVLatestPresencePrefix = 0x01

// StateKVLatestStore is the mutable flat-latest database surface needed by
// state-domain history unwind and pruning helpers.
type StateKVLatestStore interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
	ethdb.Iteratee
}

type stateKVLatestStore = StateKVLatestStore

type StateKVLatestRow struct {
	Owner      common.Address
	Generation uint64
	Domain     kvdomains.KVDomain
	Key        []byte
	Value      []byte
}

type StateKVGenerationRow struct {
	Owner      common.Address
	Generation uint64
}

func WriteStateKVLatest(db ethdb.KeyValueWriter, owner common.Address, generation uint64, domain kvdomains.KVDomain, logicalKey, value []byte) error {
	return WriteStateKVLatestEncoded(db, owner, generation, domain, logicalKey, EncodeStateKVLatestValue(value))
}

func EncodeStateKVLatestValue(value []byte) []byte {
	wrapped := make([]byte, 1+len(value))
	wrapped[0] = stateKVLatestPresencePrefix
	copy(wrapped[1:], value)
	return wrapped
}

// WriteStateKVLatestEncoded writes a value that is already encoded with the
// latest-state presence prefix. State commit uses it to share the same encoded
// bytes with the account KV trie and avoid wrapping each update twice.
func WriteStateKVLatestEncoded(db ethdb.KeyValueWriter, owner common.Address, generation uint64, domain kvdomains.KVDomain, logicalKey, wrapped []byte) error {
	return db.Put(stateKVLatestKey(owner, generation, domain, logicalKey), wrapped)
}

func ReadStateKVLatest(db ethdb.KeyValueReader, owner common.Address, generation uint64, domain kvdomains.KVDomain, logicalKey []byte) ([]byte, bool, error) {
	raw, err := db.Get(stateKVLatestKey(owner, generation, domain, logicalKey))
	if err != nil {
		return nil, false, nil
	}
	value, err := DecodeStateKVLatestValue(raw)
	if err != nil {
		return nil, false, fmt.Errorf("%w for %s domain %#04x", err, owner.Hex(), uint16(domain))
	}
	return value, true, nil
}

func DeleteStateKVLatest(db ethdb.KeyValueWriter, owner common.Address, generation uint64, domain kvdomains.KVDomain, logicalKey []byte) error {
	return db.Delete(stateKVLatestKey(owner, generation, domain, logicalKey))
}

func IterateStateKVLatest(db ethdb.Iteratee, owner common.Address, generation uint64, domain kvdomains.KVDomain, logicalPrefix []byte, fn func(logicalKey, value []byte) (bool, error)) error {
	prefix := stateKVLatestLogicalPrefix(owner, generation, domain, logicalPrefix)
	headerLen := len(stateKVLatestDomainPrefix(owner, generation, domain))
	it := db.NewIterator(prefix, nil)
	defer it.Release()
	for it.Next() {
		key := it.Key()
		if len(key) < headerLen || !bytes.HasPrefix(key, prefix) {
			continue
		}
		value, err := DecodeStateKVLatestValue(it.Value())
		if err != nil {
			return fmt.Errorf("%w for key %x", err, key)
		}
		cont, err := fn(append([]byte(nil), key[headerLen:]...), value)
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return it.Error()
}

func IterateStateKVLatestRows(db ethdb.Iteratee, fn func(StateKVLatestRow) (bool, error)) error {
	it := db.NewIterator(stateKVLatestPrefix, nil)
	defer it.Release()
	for it.Next() {
		owner, generation, domain, logicalKey, ok := DecodeStateKVLatestKey(it.Key())
		if !ok {
			continue
		}
		value, err := DecodeStateKVLatestValue(it.Value())
		if err != nil {
			return fmt.Errorf("%w for key %x", err, it.Key())
		}
		cont, err := fn(StateKVLatestRow{
			Owner:      owner,
			Generation: generation,
			Domain:     domain,
			Key:        logicalKey,
			Value:      value,
		})
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return it.Error()
}

func IterateStateKVLatestDomainRows(db ethdb.Iteratee, domain kvdomains.KVDomain, fn func(StateKVLatestRow) (bool, error)) error {
	if !kvdomains.IsRegistered(domain) {
		return fmt.Errorf("state kv latest: unregistered domain %#04x", uint16(domain))
	}
	return IterateStateKVLatestRows(db, func(row StateKVLatestRow) (bool, error) {
		if row.Domain != domain {
			return true, nil
		}
		return fn(row)
	})
}

func DecodeStateKVLatestKey(key []byte) (common.Address, uint64, kvdomains.KVDomain, []byte, bool) {
	headerLen := len(stateKVLatestPrefix) + common.AccountIDLength + 8 + 2
	if len(key) < headerLen || !bytes.HasPrefix(key, stateKVLatestPrefix) {
		return common.Address{}, 0, 0, nil, false
	}
	off := len(stateKVLatestPrefix)
	var id common.AccountID
	copy(id[:], key[off:off+common.AccountIDLength])
	off += common.AccountIDLength
	generation := binary.BigEndian.Uint64(key[off : off+8])
	off += 8
	domain := kvdomains.KVDomain(binary.BigEndian.Uint16(key[off : off+2]))
	if !kvdomains.IsRegistered(domain) {
		return common.Address{}, 0, 0, nil, false
	}
	off += 2
	return id.Address(common.AddressPrefixMainnet), generation, domain, append([]byte(nil), key[off:]...), true
}

func DecodeStateKVLatestValue(raw []byte) ([]byte, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("state kv latest: empty encoded value")
	}
	if raw[0] != stateKVLatestPresencePrefix {
		return nil, fmt.Errorf("state kv latest: bad presence prefix %#x", raw[0])
	}
	return append([]byte(nil), raw[1:]...), nil
}

func DeleteStateKVLatestPrefix(db stateKVLatestStore, owner common.Address, generation uint64, domain kvdomains.KVDomain, logicalPrefix []byte) error {
	prefix := stateKVLatestLogicalPrefix(owner, generation, domain, logicalPrefix)
	return deleteStateKVPrefixByScan(db, prefix)
}

func DeleteStateKVLatestOwner(db stateKVLatestStore, owner common.Address) error {
	return deleteStateKVPrefixByScan(db, stateKVLatestOwnerPrefix(owner))
}

func WriteStateKVGeneration(db ethdb.KeyValueWriter, owner common.Address, generation uint64) error {
	return db.Put(stateKVGenerationKey(owner), EncodeStateKVGenerationValue(generation))
}

func EncodeStateKVGenerationValue(generation uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], generation)
	return append([]byte(nil), buf[:]...)
}

func ReadStateKVGeneration(db ethdb.KeyValueReader, owner common.Address) (uint64, bool, error) {
	data, err := db.Get(stateKVGenerationKey(owner))
	if err != nil {
		return 0, false, nil
	}
	generation, err := DecodeStateKVGenerationValue(data)
	if err != nil {
		return 0, false, fmt.Errorf("%w for %s", err, owner.Hex())
	}
	return generation, true, nil
}

func DecodeStateKVGenerationValue(data []byte) (uint64, error) {
	if len(data) != 8 {
		return 0, fmt.Errorf("state kv generation: bad length %d", len(data))
	}
	return binary.BigEndian.Uint64(data), nil
}

func DeleteStateKVGeneration(db ethdb.KeyValueWriter, owner common.Address) error {
	return db.Delete(stateKVGenerationKey(owner))
}

func DecodeStateKVGenerationKey(key []byte) (common.Address, bool) {
	headerLen := len(stateKVGenerationPrefix)
	if len(key) != headerLen+common.AccountIDLength || !bytes.HasPrefix(key, stateKVGenerationPrefix) {
		return common.Address{}, false
	}
	var id common.AccountID
	copy(id[:], key[headerLen:])
	return id.Address(common.AddressPrefixMainnet), true
}

func IterateStateKVGeneration(db ethdb.Iteratee, ownerPrefix []byte, fn func(StateKVGenerationRow) (bool, error)) error {
	prefix := append(append([]byte{}, stateKVGenerationPrefix...), ownerPrefix...)
	it := db.NewIterator(prefix, nil)
	defer it.Release()
	for it.Next() {
		owner, ok := DecodeStateKVGenerationKey(it.Key())
		if !ok {
			continue
		}
		generation, err := DecodeStateKVGenerationValue(it.Value())
		if err != nil {
			return fmt.Errorf("%w for key %x", err, it.Key())
		}
		cont, err := fn(StateKVGenerationRow{Owner: owner, Generation: generation})
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return it.Error()
}

func deleteStateKVPrefixByScan(db stateKVLatestStore, prefix []byte) error {
	if deleter, ok := db.(ethdb.KeyValueRangeDeleter); ok {
		if err := deleter.DeleteRange(prefix, prefixUpperBound(prefix)); err == nil {
			return nil
		} else if !errors.Is(err, ethdb.ErrTooManyKeys) {
			return err
		}
	}
	return deleteStateKVPrefixByPointScan(db, prefix)
}

func deleteStateKVPrefixByPointScan(db stateKVLatestStore, prefix []byte) error {
	for {
		it := db.NewIterator(prefix, nil)
		keys := make([][]byte, 0, resetScanBatch)
		for it.Next() {
			keys = append(keys, append([]byte(nil), it.Key()...))
			if len(keys) >= resetScanBatch {
				break
			}
		}
		err := it.Error()
		it.Release()
		if err != nil {
			return err
		}
		if len(keys) == 0 {
			return nil
		}
		if err := deleteStateKVKeys(db, keys); err != nil {
			return err
		}
		if len(keys) < resetScanBatch {
			return nil
		}
	}
}

func deleteStateKVKeys(db stateKVLatestStore, keys [][]byte) error {
	if len(keys) == 0 {
		return nil
	}
	if batcher, ok := db.(ethdb.Batcher); ok {
		batch := batcher.NewBatch()
		for _, key := range keys {
			if err := batch.Delete(key); err != nil {
				return err
			}
		}
		return batch.Write()
	}
	for _, key := range keys {
		if err := db.Delete(key); err != nil {
			return err
		}
	}
	return nil
}
