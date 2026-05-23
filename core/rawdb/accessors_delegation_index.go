package rawdb

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/ethereum/go-ethereum/ethdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

type delegationIndexReadWriter interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
}

// ReadDrAccountIndexLegacy returns the aggregate pre-proposal-69 V1 index
// stored under the account address.
func ReadDrAccountIndexLegacy(db ethdb.KeyValueReader, account []byte) *corepb.DelegatedResourceAccountIndex {
	data, err := db.Get(drAccIdxLegacyKey(account))
	if err != nil || len(data) == 0 {
		return nil
	}
	var m corepb.DelegatedResourceAccountIndex
	if err := proto.Unmarshal(data, &m); err != nil {
		return nil
	}
	return &m
}

func writeDrAccountIndexLegacy(db ethdb.KeyValueWriter, account []byte, rec *corepb.DelegatedResourceAccountIndex) error {
	data, err := proto.Marshal(rec)
	if err != nil {
		return fmt.Errorf("dr account index: marshal legacy: %w", err)
	}
	return db.Put(drAccIdxLegacyKey(account), data)
}

func deleteDrAccountIndexLegacy(db ethdb.KeyValueWriter, account []byte) error {
	return db.Delete(drAccIdxLegacyKey(account))
}

func appendUniqueAccount(list [][]byte, account []byte) [][]byte {
	for _, existing := range list {
		if string(existing) == string(account) {
			return list
		}
	}
	return append(list, append([]byte(nil), account...))
}

func removeAccount(list [][]byte, account []byte) [][]byte {
	out := list[:0]
	for _, existing := range list {
		if string(existing) != string(account) {
			out = append(out, existing)
		}
	}
	return out
}

// WriteDrAccountIndexLegacyDelegate updates java-tron's aggregate V1 index
// used before allow_delegate_optimization.
func WriteDrAccountIndexLegacyDelegate(db delegationIndexReadWriter, from, to []byte) error {
	if len(from) == 0 || len(to) == 0 {
		return fmt.Errorf("dr account index: empty address (from=%d to=%d)", len(from), len(to))
	}
	fromRec := ReadDrAccountIndexLegacy(db, from)
	if fromRec == nil {
		fromRec = &corepb.DelegatedResourceAccountIndex{Account: append([]byte(nil), from...)}
	}
	fromRec.ToAccounts = appendUniqueAccount(fromRec.ToAccounts, to)
	if err := writeDrAccountIndexLegacy(db, from, fromRec); err != nil {
		return err
	}

	toRec := ReadDrAccountIndexLegacy(db, to)
	if toRec == nil {
		toRec = &corepb.DelegatedResourceAccountIndex{Account: append([]byte(nil), to...)}
	}
	toRec.FromAccounts = appendUniqueAccount(toRec.FromAccounts, from)
	return writeDrAccountIndexLegacy(db, to, toRec)
}

// WriteDrAccountIndexLegacyUnDelegate removes a V1 edge from the aggregate
// pre-proposal-69 index.
func WriteDrAccountIndexLegacyUnDelegate(db delegationIndexReadWriter, from, to []byte) error {
	if len(from) == 0 || len(to) == 0 {
		return fmt.Errorf("dr account index: empty address")
	}
	if fromRec := ReadDrAccountIndexLegacy(db, from); fromRec != nil {
		fromRec.ToAccounts = removeAccount(fromRec.ToAccounts, to)
		if err := writeDrAccountIndexLegacy(db, from, fromRec); err != nil {
			return err
		}
	}
	if toRec := ReadDrAccountIndexLegacy(db, to); toRec != nil {
		toRec.FromAccounts = removeAccount(toRec.FromAccounts, from)
		return writeDrAccountIndexLegacy(db, to, toRec)
	}
	return nil
}

// ConvertDrAccountIndexLegacy mirrors java-tron's
// DelegatedResourceAccountIndexStore.convert: aggregate V1 records are expanded
// into directional V1 entries using list position as timestamp, then removed.
func ConvertDrAccountIndexLegacy(db delegationIndexReadWriter, account []byte) error {
	rec := ReadDrAccountIndexLegacy(db, account)
	if rec == nil {
		return nil
	}
	for i, to := range rec.ToAccounts {
		if err := WriteDrAccountIndexDelegate(db, false, account, to, int64(i+1)); err != nil {
			return err
		}
	}
	for i, from := range rec.FromAccounts {
		if err := WriteDrAccountIndexDelegate(db, false, from, account, int64(i+1)); err != nil {
			return err
		}
	}
	return deleteDrAccountIndexLegacy(db, account)
}

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
func ReadDrAccountIndexEntry(db ethdb.KeyValueReader, dir DrAccIdxDirection, anchor, counterparty []byte) *corepb.DelegatedResourceAccountIndex {
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
	dir DrAccIdxDirection,
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

func DrAccountIndexLegacyStateKey(account []byte) []byte {
	return drAccIdxLegacyKey(account)
}

func DrAccountIndexStateKey(dir DrAccIdxDirection, anchor, counterparty []byte) []byte {
	return drAccIdxKey(dir, anchor, counterparty)
}

func DrAccountIndexAnchorStatePrefix(dir DrAccIdxDirection, anchor []byte) []byte {
	return drAccIdxAnchorPrefix(dir, anchor)
}
