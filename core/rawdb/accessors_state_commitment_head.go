package rawdb

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/common/muhash3072"
)

const StateCommitmentHeadVersion uint16 = 3
const StateCommitmentElementVersion uint16 = 3

var stateCommitmentHeadMagic = []byte{'G', 'M', 'H', 'H', 'E', 'A', 'D', 3}
var ErrStateCommitmentHead = errors.New("invalid MuHash latest-domain HEAD; fresh-genesis resync required")

// StateCommitmentHead is a complete current source-cut commitment. Only HEAD
// and actual cold snapshot cuts store State; historical block roots stay32B.
type StateCommitmentHead struct {
	Scheme                  string
	Version, ElementVersion uint16
	BlockNum                uint64
	BlockHash, ParentHash   common.Hash
	TxNum                   uint64
	DeltaID, Root           common.Hash
	State                   muhash3072.State
}

func NewStateCommitmentHead() StateCommitmentHead {
	return StateCommitmentHead{Scheme: LatestDomainCommitmentScheme, Version: StateCommitmentHeadVersion, ElementVersion: StateCommitmentElementVersion, State: muhash3072.EmptyState(), Root: common.Hash(muhash3072.EmptyDigest())}
}
func StateCommitmentHeadKey() []byte           { return bytes.Clone(stateMuHashCommitmentHeadKey) }
func IsStateCommitmentHeadKey(key []byte) bool { return bytes.Equal(key, stateMuHashCommitmentHeadKey) }
func ValidateStateCommitmentHead(h StateCommitmentHead) error {
	if h.Scheme != LatestDomainCommitmentScheme || h.Version != StateCommitmentHeadVersion || h.ElementVersion != StateCommitmentElementVersion {
		return ErrStateCommitmentHead
	}
	root, err := muhash3072.StateDigest(h.State)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrStateCommitmentHead, err)
	}
	if common.Hash(root) != h.Root {
		return fmt.Errorf("%w: state/digest mismatch", ErrStateCommitmentHead)
	}
	return nil
}
func EncodeStateCommitmentHead(h StateCommitmentHead) ([]byte, error) {
	if err := ValidateStateCommitmentHead(h); err != nil {
		return nil, err
	}
	out := append([]byte(nil), stateCommitmentHeadMagic...)
	out = binary.BigEndian.AppendUint16(out, h.Version)
	out = binary.BigEndian.AppendUint16(out, h.ElementVersion)
	out = binary.BigEndian.AppendUint16(out, uint16(len(h.Scheme)))
	out = append(out, h.Scheme...)
	out = binary.BigEndian.AppendUint64(out, h.BlockNum)
	out = append(out, h.BlockHash[:]...)
	out = append(out, h.ParentHash[:]...)
	out = binary.BigEndian.AppendUint64(out, h.TxNum)
	out = append(out, h.DeltaID[:]...)
	out = append(out, h.Root[:]...)
	out = append(out, h.State[:]...)
	return out, nil
}
func DecodeStateCommitmentHead(raw []byte) (h StateCommitmentHead, err error) {
	const fixed = 8 + 2 + 2 + 2 + 8 + 32 + 32 + 8 + 32 + 32 + 384
	want := fixed + len(LatestDomainCommitmentScheme)
	if len(raw) != want || !bytes.Equal(raw[:8], stateCommitmentHeadMagic) {
		return h, ErrStateCommitmentHead
	}
	off := 8
	h.Version = binary.BigEndian.Uint16(raw[off:])
	off += 2
	h.ElementVersion = binary.BigEndian.Uint16(raw[off:])
	off += 2
	n := int(binary.BigEndian.Uint16(raw[off:]))
	off += 2
	if n != len(LatestDomainCommitmentScheme) {
		return h, ErrStateCommitmentHead
	}
	h.Scheme = string(raw[off : off+n])
	off += n
	h.BlockNum = binary.BigEndian.Uint64(raw[off:])
	off += 8
	copy(h.BlockHash[:], raw[off:off+32])
	off += 32
	copy(h.ParentHash[:], raw[off:off+32])
	off += 32
	h.TxNum = binary.BigEndian.Uint64(raw[off:])
	off += 8
	copy(h.DeltaID[:], raw[off:off+32])
	off += 32
	copy(h.Root[:], raw[off:off+32])
	off += 32
	copy(h.State[:], raw[off:])
	if err = ValidateStateCommitmentHead(h); err != nil {
		return StateCommitmentHead{}, err
	}
	return h, nil
}
func ReadStateCommitmentHead(db ethdb.KeyValueReader) (StateCommitmentHead, bool, error) {
	data, present, err := readPresentValue(db, stateMuHashCommitmentHeadKey, "MuHash latest-domain HEAD")
	if err != nil || !present {
		return StateCommitmentHead{}, present, err
	}
	h, err := DecodeStateCommitmentHead(data)
	return h, err == nil, err
}
func WriteStateCommitmentHead(db ethdb.KeyValueWriter, h StateCommitmentHead) error {
	data, err := EncodeStateCommitmentHead(h)
	if err != nil {
		return err
	}
	return db.Put(stateMuHashCommitmentHeadKey, data)
}

// StateCommitmentDelta owns both real physical images, including presence.
type StateCommitmentDelta struct {
	Key          []byte
	BeforeExists bool
	Before       []byte
	AfterExists  bool
	After        []byte
}

func CloneStateCommitmentDelta(d StateCommitmentDelta) StateCommitmentDelta {
	d.Key = bytes.Clone(d.Key)
	d.Before = bytes.Clone(d.Before)
	d.After = bytes.Clone(d.After)
	if !d.BeforeExists {
		d.Before = nil
	}
	if !d.AfterExists {
		d.After = nil
	}
	return d
}
func StateCommitmentElementFamily(key []byte) (byte, error) {
	if bytes.HasPrefix(key, stateAccountLatestPrefix) && len(key) == len(stateAccountLatestPrefix)+common.AccountIDLength {
		return 1, nil
	}
	if _, ok := DecodeStateKVGenerationKey(key); ok {
		return 2, nil
	}
	if _, _, _, _, ok := DecodeStateKVLatestKey(key); ok {
		return 3, nil
	}
	return 0, fmt.Errorf("invalid latest-domain commitment source key %x", key)
}
func EncodeStateCommitmentElement(key, value []byte) ([]byte, error) {
	family, err := StateCommitmentElementFamily(key)
	if err != nil {
		return nil, err
	}
	if family == 3 {
		if err := ValidateStateKVLatestValue(value); err != nil {
			return nil, err
		}
	}
	const domain = "gtron/latest-domain/muhash3072/compact-v3\x00"
	out := make([]byte, 0, len(domain)+1+16+len(key)+len(value))
	out = append(out, domain...)
	out = append(out, family)
	out = binary.BigEndian.AppendUint64(out, uint64(len(key)))
	out = append(out, key...)
	out = binary.BigEndian.AppendUint64(out, uint64(len(value)))
	return append(out, value...), nil
}
func StateCommitmentDeltaID(deltas []StateCommitmentDelta) common.Hash {
	h := sha256.New()
	h.Write([]byte("gtron/latest-domain/muhash-delta/compact-v3\x00"))
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(len(deltas)))
	h.Write(b[:])
	for _, d := range deltas {
		binary.BigEndian.PutUint64(b[:], uint64(len(d.Key)))
		h.Write(b[:])
		h.Write(d.Key)
		for _, v := range []struct {
			exists bool
			data   []byte
		}{{d.BeforeExists, d.Before}, {d.AfterExists, d.After}} {
			var flag byte
			if v.exists {
				flag = 1
			}
			h.Write([]byte{flag})
			binary.BigEndian.PutUint64(b[:], uint64(len(v.data)))
			h.Write(b[:])
			h.Write(v.data)
		}
	}
	var out common.Hash
	copy(out[:], h.Sum(nil))
	return out
}

// CoalesceStateCommitmentDeltas retains first-before/last-after and validates
// intermediate captures. Caller data is copied before retention.
func CoalesceStateCommitmentDeltas(input []StateCommitmentDelta) ([]StateCommitmentDelta, error) {
	byKey := make(map[string]*StateCommitmentDelta, len(input))
	for _, d := range input {
		if _, err := StateCommitmentElementFamily(d.Key); err != nil {
			return nil, err
		}
		d = CloneStateCommitmentDelta(d)
		if old := byKey[string(d.Key)]; old != nil {
			if old.AfterExists != d.BeforeExists || (d.BeforeExists && !bytes.Equal(old.After, d.Before)) {
				return nil, errors.New("inconsistent commitment delta intermediate image")
			}
			old.AfterExists = d.AfterExists
			old.After = d.After
		} else {
			byKey[string(d.Key)] = &d
		}
	}
	out := make([]StateCommitmentDelta, 0, len(byKey))
	for _, d := range byKey {
		if d.BeforeExists == d.AfterExists && (!d.BeforeExists || bytes.Equal(d.Before, d.After)) {
			continue
		}
		out = append(out, *d)
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].Key, out[j].Key) < 0 })
	return out, nil
}
func StateCommitmentDeltasFromUpdates(updates []StateCommitmentUpdate) ([]StateCommitmentDelta, error) {
	deltas := make([]StateCommitmentDelta, 0, len(updates))
	for _, u := range updates {
		if !u.BeforeKnown {
			return nil, fmt.Errorf("commitment update %x lacks pre-write before image", u.Key)
		}
		deltas = append(deltas, StateCommitmentDelta{Key: u.Key, BeforeExists: u.BeforeExists, Before: u.Before, AfterExists: !u.Delete, After: u.Value})
	}
	return CoalesceStateCommitmentDeltas(deltas)
}
func ReadStateCommitmentSource(db ethdb.KeyValueReader, key []byte) ([]byte, bool, error) {
	if _, err := StateCommitmentElementFamily(key); err != nil {
		return nil, false, err
	}
	return readPresentValue(db, key, "latest-domain commitment source")
}

func InitializeStateCommitment(db interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
	ethdb.Iteratee
}) error {
	if pending, err := StateCommitmentUnwindCleanupPending(db); err != nil {
		return err
	} else if pending {
		return fmt.Errorf("%w: unwind cleanup incomplete", ErrStateCommitmentHead)
	}
	if pending, err := StateCommitmentRestoreInProgress(db); err != nil {
		return err
	} else if pending {
		return fmt.Errorf("%w: snapshot restore incomplete", ErrStateCommitmentHead)
	}
	if _, present, err := ReadStateCommitmentHead(db); err != nil || present {
		return err
	}
	if _, present, err := ReadLatestDomainCommitmentRoot(db); err != nil {
		return err
	} else if present {
		return ErrStateCommitmentHead
	}
	if err := CheckEmptyStateCommitmentSources(db); err != nil {
		return err
	}
	head := NewStateCommitmentHead()
	batcher, ok := db.(ethdb.Batcher)
	if !ok {
		return errors.New("empty commitment initialization requires atomic batch")
	}
	batch := batcher.NewBatch()
	defer batch.Close()
	if err := WriteStateCommitmentHead(batch, head); err != nil {
		return err
	}
	if err := WriteLatestDomainCommitmentRoot(batch, head.Root); err != nil {
		return err
	}
	return batch.Write()
}

func StateCommitmentRestoreInProgress(db ethdb.KeyValueReader) (bool, error) {
	_, present, err := readPresentValue(db, stateCommitmentRestoreMarkerKey, "MuHash restore marker")
	return present, err
}
func WriteStateCommitmentRestoreMarker(db ethdb.KeyValueWriter) error {
	return db.Put(stateCommitmentRestoreMarkerKey, []byte{1})
}
func DeleteStateCommitmentRestoreMarker(db ethdb.KeyValueWriter) error {
	return db.Delete(stateCommitmentRestoreMarkerKey)
}

func StateCommitmentUnwindCleanupPending(db ethdb.KeyValueReader) (bool, error) {
	data, ok, err := readPresentValue(db, stateCommitmentUnwindCleanupKey, "MuHash unwind cleanup")
	if err != nil || !ok {
		return ok, err
	}
	if len(data) != 16 || binary.BigEndian.Uint64(data[:8]) <= binary.BigEndian.Uint64(data[8:]) {
		return false, ErrStateCommitmentHead
	}
	return true, nil
}
func WriteStateCommitmentUnwindCleanup(db ethdb.KeyValueWriter, from, to uint64) error {
	if from <= to {
		return errors.New("invalid unwind cleanup bounds")
	}
	var b [16]byte
	binary.BigEndian.PutUint64(b[:8], from)
	binary.BigEndian.PutUint64(b[8:], to)
	return db.Put(stateCommitmentUnwindCleanupKey, b[:])
}
func DeleteStateCommitmentUnwindCleanup(db ethdb.KeyValueWriter) error {
	return db.Delete(stateCommitmentUnwindCleanupKey)
}

// CheckEmptyStateCommitmentSources rejects residual fresh or legacy state
// before genesis/restore publication. Raw 32-byte trie hashes are not KV rows.
func CheckEmptyStateCommitmentSources(db ethdb.Iteratee) error {
	legacy := db.NewIterator(legacyStateKVLatestPrefix, nil)
	has := false
	for legacy.Next() {
		if len(legacy.Key()) != 32 {
			has = true
			break
		}
	}
	err := legacy.Error()
	legacy.Release()
	err = errors.Join(err, legacy.Error())
	if err != nil {
		return err
	}
	if has {
		return ErrStateCommitmentHead
	}
	return IterateLatestDomainCommitmentSources(db, func([]byte, []byte) (bool, error) { return false, ErrStateCommitmentHead })
}
