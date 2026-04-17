package rawdb

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/ethereum/go-ethereum/ethdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// WriteDrAccountIndexDelegate writes both sides of a delegation edge
// for the chosen resource version (V1 or V2). Mirrors java-tron's
// DelegatedResourceAccountIndexStore.delegate (V1) / delegateV2 (V2):
// for the pair (from → to) it writes a from-anchored record pointing
// at `to` and a to-anchored record pointing at `from`, both with the
// supplied timestamp.
func WriteDrAccountIndexDelegate(
	db ethdb.KeyValueWriter,
	v2 bool,
	from, to []byte,
	timestamp int64,
) error {
	if len(from) == 0 || len(to) == 0 {
		return fmt.Errorf("dr account index: empty address (from=%d to=%d)", len(from), len(to))
	}
	fromDir, toDir := DrAccIdxV1From, DrAccIdxV1To
	if v2 {
		fromDir, toDir = DrAccIdxV2From, DrAccIdxV2To
	}

	// from-anchored: account = to
	fromPayload, err := proto.Marshal(&corepb.DelegatedResourceAccountIndex{
		Account:   append([]byte(nil), to...),
		Timestamp: timestamp,
	})
	if err != nil {
		return fmt.Errorf("dr account index: marshal from: %w", err)
	}
	if err := db.Put(drAccIdxKey(fromDir, from, to), fromPayload); err != nil {
		return err
	}

	// to-anchored: account = from
	toPayload, err := proto.Marshal(&corepb.DelegatedResourceAccountIndex{
		Account:   append([]byte(nil), from...),
		Timestamp: timestamp,
	})
	if err != nil {
		return fmt.Errorf("dr account index: marshal to: %w", err)
	}
	return db.Put(drAccIdxKey(toDir, to, from), toPayload)
}

// WriteDrAccountIndexUnDelegate deletes both sides. Mirrors unDelegate /
// unDelegateV2.
func WriteDrAccountIndexUnDelegate(db ethdb.KeyValueWriter, v2 bool, from, to []byte) error {
	if len(from) == 0 || len(to) == 0 {
		return fmt.Errorf("dr account index: empty address")
	}
	fromDir, toDir := DrAccIdxV1From, DrAccIdxV1To
	if v2 {
		fromDir, toDir = DrAccIdxV2From, DrAccIdxV2To
	}
	if err := db.Delete(drAccIdxKey(fromDir, from, to)); err != nil {
		return err
	}
	return db.Delete(drAccIdxKey(toDir, to, from))
}

// ReadDrAccountIndexEntry returns the proto record stored at
// (direction, anchor, counterparty) or nil if absent.
func ReadDrAccountIndexEntry(db ethdb.KeyValueReader, dir drAccIdxDirection, anchor, counterparty []byte) *corepb.DelegatedResourceAccountIndex {
	data, err := db.Get(drAccIdxKey(dir, anchor, counterparty))
	if err != nil || len(data) == 0 {
		return nil
	}
	var m corepb.DelegatedResourceAccountIndex
	if err := proto.Unmarshal(data, &m); err != nil {
		return nil
	}
	return &m
}

// IterateDrAccountIndex scans every counterparty for (direction, anchor)
// and invokes fn. Aborts on fn returning an error.
func IterateDrAccountIndex(
	db ethdb.Iteratee,
	dir drAccIdxDirection,
	anchor []byte,
	fn func(counterparty []byte, rec *corepb.DelegatedResourceAccountIndex) error,
) error {
	prefix := drAccIdxAnchorPrefix(dir, anchor)
	it := db.NewIterator(prefix, nil)
	defer it.Release()

	for it.Next() {
		key := it.Key()
		if len(key) < len(prefix) {
			continue
		}
		counterparty := append([]byte(nil), key[len(prefix):]...)
		var m corepb.DelegatedResourceAccountIndex
		if err := proto.Unmarshal(it.Value(), &m); err != nil {
			return fmt.Errorf("dr account index: decode %x: %w", key, err)
		}
		if err := fn(counterparty, &m); err != nil {
			return err
		}
	}
	return it.Error()
}
