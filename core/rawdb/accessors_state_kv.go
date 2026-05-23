package rawdb

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

const stateKVLatestPresencePrefix = 0x01

type stateKVLatestStore interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
	ethdb.Iteratee
}

func WriteStateKVLatest(db ethdb.KeyValueWriter, owner common.Address, generation uint64, domain kvdomains.KVDomain, logicalKey, value []byte) error {
	wrapped := make([]byte, 1+len(value))
	wrapped[0] = stateKVLatestPresencePrefix
	copy(wrapped[1:], value)
	return db.Put(stateKVLatestKey(owner, generation, domain, logicalKey), wrapped)
}

func ReadStateKVLatest(db ethdb.KeyValueReader, owner common.Address, generation uint64, domain kvdomains.KVDomain, logicalKey []byte) ([]byte, bool, error) {
	raw, err := db.Get(stateKVLatestKey(owner, generation, domain, logicalKey))
	if err != nil {
		return nil, false, nil
	}
	if len(raw) == 0 {
		return nil, false, fmt.Errorf("state kv latest: empty encoded value for %s domain %#04x", owner.Hex(), uint16(domain))
	}
	if raw[0] != stateKVLatestPresencePrefix {
		return nil, false, fmt.Errorf("state kv latest: bad presence prefix %#x for %s domain %#04x", raw[0], owner.Hex(), uint16(domain))
	}
	return append([]byte(nil), raw[1:]...), true, nil
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
		raw := it.Value()
		if len(raw) == 0 {
			return fmt.Errorf("state kv latest: empty encoded value for key %x", key)
		}
		if raw[0] != stateKVLatestPresencePrefix {
			return fmt.Errorf("state kv latest: bad presence prefix %#x for key %x", raw[0], key)
		}
		cont, err := fn(append([]byte(nil), key[headerLen:]...), append([]byte(nil), raw[1:]...))
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return it.Error()
}

func DeleteStateKVLatestPrefix(db stateKVLatestStore, owner common.Address, generation uint64, domain kvdomains.KVDomain, logicalPrefix []byte) error {
	prefix := stateKVLatestLogicalPrefix(owner, generation, domain, logicalPrefix)
	return deleteStateKVPrefixByScan(db, prefix)
}

func DeleteStateKVLatestOwner(db stateKVLatestStore, owner common.Address) error {
	return deleteStateKVPrefixByScan(db, stateKVLatestOwnerPrefix(owner))
}

func WriteStateKVGeneration(db ethdb.KeyValueWriter, owner common.Address, generation uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], generation)
	return db.Put(stateKVGenerationKey(owner), buf[:])
}

func ReadStateKVGeneration(db ethdb.KeyValueReader, owner common.Address) (uint64, bool, error) {
	data, err := db.Get(stateKVGenerationKey(owner))
	if err != nil {
		return 0, false, nil
	}
	if len(data) != 8 {
		return 0, false, fmt.Errorf("state kv generation: bad length %d for %s", len(data), owner.Hex())
	}
	return binary.BigEndian.Uint64(data), true, nil
}

func deleteStateKVPrefixByScan(db stateKVLatestStore, prefix []byte) error {
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
		for _, key := range keys {
			if err := db.Delete(key); err != nil {
				return err
			}
		}
		if len(keys) < resetScanBatch {
			return nil
		}
	}
}
