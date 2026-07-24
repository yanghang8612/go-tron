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

// cachedNoCopyStateKVLatestReader is the optional layered-store fast path for
// the hot flat-latest lookup. Keeping the structured fields separate lets the
// store assemble the physical key in stack storage for overlay/cache hits;
// passing a prebuilt []byte here would make stateKVLatestKey allocate before
// the store has a chance to avoid it.
type cachedNoCopyStateKVLatestReader interface {
	GetNoCopyCachedStateKVLatest(prefix []byte, accountID common.AccountID, generation uint64, domain uint16, logicalKey []byte) ([]byte, error)
}

// stateKVLatestStructuredWriter is the write/delete counterpart. Layered
// stores whose native map key is a string can join the schema fields directly
// into that owned string instead of allocating a temporary []byte and then
// copying it again during Put/Delete.
type stateKVLatestStructuredWriter interface {
	PutStateKVLatest(prefix []byte, accountID common.AccountID, generation uint64, domain uint16, logicalKey, value []byte) error
	DeleteStateKVLatest(prefix []byte, accountID common.AccountID, generation uint64, domain uint16, logicalKey []byte) error
}

// stateKVLatestStructuredOwnedWriter retains a freshly encoded immutable value
// while still assembling the structured physical key without an intermediate
// byte slice. It is intentionally optional; ordinary writes preserve the
// defensive-copy contract of stateKVLatestStructuredWriter.
type stateKVLatestStructuredOwnedWriter interface {
	PutStateKVLatestOwnedValue(prefix []byte, accountID common.AccountID, generation uint64, domain uint16, logicalKey, value []byte) error
}

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
	return AppendStateKVLatestValue(nil, value)
}

// AppendStateKVLatestValue appends the latest-state presence envelope to dst.
// Callers that repeatedly replace a captured value can pass dst[:0] to reuse
// its backing allocation; EncodeStateKVLatestValue remains the owning helper.
func AppendStateKVLatestValue(dst, value []byte) []byte {
	dst = append(dst, stateKVLatestPresencePrefix)
	return append(dst, value...)
}

// WriteStateKVLatestEncoded writes a value that is already encoded with the
// latest-state presence prefix. State commit uses it to share the same encoded
// bytes with the account KV trie and avoid wrapping each update twice.
func WriteStateKVLatestEncoded(db ethdb.KeyValueWriter, owner common.Address, generation uint64, domain kvdomains.KVDomain, logicalKey, wrapped []byte) error {
	if writer, ok := db.(stateKVLatestStructuredWriter); ok {
		return writer.PutStateKVLatest(stateKVLatestPrefix, owner.AccountID(), generation, uint16(domain), logicalKey, wrapped)
	}
	return db.Put(stateKVLatestKey(owner, generation, domain, logicalKey), wrapped)
}

// WriteStateKVLatestEncodedOwned transfers an already encoded immutable value
// to writers that advertise an ownership-taking path. Fallback writers retain
// the normal copying Put semantics.
func WriteStateKVLatestEncodedOwned(db ethdb.KeyValueWriter, owner common.Address, generation uint64, domain kvdomains.KVDomain, logicalKey, wrapped []byte) error {
	if writer, ok := db.(stateKVLatestStructuredOwnedWriter); ok {
		return writer.PutStateKVLatestOwnedValue(stateKVLatestPrefix, owner.AccountID(), generation, uint16(domain), logicalKey, wrapped)
	}
	if writer, ok := db.(ownedValueWriter); ok {
		return writer.PutOwnedValue(stateKVLatestKey(owner, generation, domain, logicalKey), wrapped)
	}
	return WriteStateKVLatestEncoded(db, owner, generation, domain, logicalKey, wrapped)
}

func ReadStateKVLatest(db ethdb.KeyValueReader, owner common.Address, generation uint64, domain kvdomains.KVDomain, logicalKey []byte) ([]byte, bool, error) {
	value, ok, err := ReadStateKVLatestNoCopy(db, owner, generation, domain, logicalKey)
	if err != nil || !ok {
		return nil, ok, err
	}
	return append([]byte(nil), value...), true, nil
}

// ReadStateKVLatestNoCopy returns the unwrapped state value without a trailing
// defensive copy. The returned bytes may alias the reader's cache or overlay
// and must be consumed before the next database operation. Internal StateDB
// decode paths use it only for immediate protobuf or scalar decoding.
func ReadStateKVLatestNoCopy(db ethdb.KeyValueReader, owner common.Address, generation uint64, domain kvdomains.KVDomain, logicalKey []byte) ([]byte, bool, error) {
	var (
		raw []byte
		err error
	)
	if reader, ok := db.(cachedNoCopyStateKVLatestReader); ok {
		raw, err = reader.GetNoCopyCachedStateKVLatest(stateKVLatestPrefix, owner.AccountID(), generation, uint16(domain), logicalKey)
	} else {
		raw, err = readStateNoCopyCached(db, stateKVLatestKey(owner, generation, domain, logicalKey))
	}
	if err != nil {
		return nil, false, nil
	}
	value, err := decodeStateKVLatestValueNoCopy(raw)
	if err != nil {
		return nil, false, fmt.Errorf("%w for %s domain %#04x", err, owner.Hex(), uint16(domain))
	}
	return value, true, nil
}

func DeleteStateKVLatest(db ethdb.KeyValueWriter, owner common.Address, generation uint64, domain kvdomains.KVDomain, logicalKey []byte) error {
	if writer, ok := db.(stateKVLatestStructuredWriter); ok {
		return writer.DeleteStateKVLatest(stateKVLatestPrefix, owner.AccountID(), generation, uint16(domain), logicalKey)
	}
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
	value, err := decodeStateKVLatestValueNoCopy(raw)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), value...), nil
}

func decodeStateKVLatestValueNoCopy(raw []byte) ([]byte, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("state kv latest: empty encoded value")
	}
	if raw[0] != stateKVLatestPresencePrefix {
		return nil, fmt.Errorf("state kv latest: bad presence prefix %#x", raw[0])
	}
	return raw[1:], nil
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
	data, err := readStateNoCopyCached(db, stateKVGenerationKey(owner))
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
