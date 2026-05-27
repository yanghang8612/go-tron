package rawdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"golang.org/x/crypto/sha3"
)

const commitmentPathBits = common.HashLength * 8

// commitmentNodePrefix is the logical key prefix the latest-domain commitment
// branch rows are stored under inside the CommitmentDomain keyspace. The
// snapshot and pruning layers reference it via
// LatestDomainCommitmentNodeLogicalPrefix / IsLatestDomainCommitmentNodeLogicalKey.
var commitmentNodePrefix = []byte("tree/node/")

type StateCommitmentUpdate struct {
	Key    []byte
	Value  []byte
	Delete bool
}

func NewStateCommitmentPut(key, value []byte) StateCommitmentUpdate {
	return StateCommitmentUpdate{
		Key:   append([]byte(nil), key...),
		Value: append([]byte(nil), value...),
	}
}

func NewStateCommitmentDelete(key []byte) StateCommitmentUpdate {
	return StateCommitmentUpdate{
		Key:    append([]byte(nil), key...),
		Delete: true,
	}
}

func StateAccountLatestCommitmentKey(owner common.Address) []byte {
	return stateAccountLatestKey(owner)
}

func StateKVLatestCommitmentKey(owner common.Address, generation uint64, domain kvdomains.KVDomain, logicalKey []byte) []byte {
	return stateKVLatestKey(owner, generation, domain, logicalKey)
}

func StateKVGenerationCommitmentKey(owner common.Address) []byte {
	return stateKVGenerationKey(owner)
}

// IterateLatestDomainCommitmentSources iterates every row in the three
// latest-domain source tables (account-latest, KV-generation, KV-latest) in a
// deterministic prefix order and calls fn with the physical (key, value) of
// each row. The physical key is exactly what NewStateCommitmentPut expects as a
// commitment key. Iteration stops when fn returns (false, nil) or an error. It
// exists so callers outside rawdb can bootstrap a commitment engine from the
// latest-domain rows without duplicating the unexported prefix literals.
func IterateLatestDomainCommitmentSources(db ethdb.Iteratee, fn func(key, value []byte) (bool, error)) error {
	for _, prefix := range [][]byte{stateAccountLatestPrefix, stateKVGenerationPrefix, stateKVLatestPrefix} {
		it := db.NewIterator(prefix, nil)
		for it.Next() {
			cont, err := fn(it.Key(), it.Value())
			if err != nil {
				it.Release()
				return err
			}
			if !cont {
				it.Release()
				return nil
			}
		}
		err := it.Error()
		it.Release()
		if err != nil {
			return err
		}
	}
	return nil
}

func CoalesceStateCommitmentUpdates(updates []StateCommitmentUpdate) []StateCommitmentUpdate {
	if len(updates) == 0 {
		return nil
	}
	byKey := make(map[string]StateCommitmentUpdate, len(updates))
	for _, update := range updates {
		if len(update.Key) == 0 {
			continue
		}
		byKey[string(update.Key)] = cloneStateCommitmentUpdate(update)
	}
	if len(byKey) == 0 {
		return nil
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare([]byte(keys[i]), []byte(keys[j])) < 0
	})
	out := make([]StateCommitmentUpdate, 0, len(keys))
	for _, key := range keys {
		out = append(out, byKey[key])
	}
	return out
}

func cloneStateCommitmentUpdate(update StateCommitmentUpdate) StateCommitmentUpdate {
	return StateCommitmentUpdate{
		Key:    append([]byte(nil), update.Key...),
		Value:  append([]byte(nil), update.Value...),
		Delete: update.Delete,
	}
}

// RebuildLatestDomainCommitment recomputes the binary-radix commitment over the
// three latest-domain source tables from scratch and writes the resulting root.
func RebuildLatestDomainCommitment(db latestDomainCommitmentStore) (common.Hash, error) {
	if err := clearLatestDomainCommitmentNodes(db); err != nil {
		return common.Hash{}, err
	}
	for _, prefix := range [][]byte{stateAccountLatestPrefix, stateKVGenerationPrefix, stateKVLatestPrefix} {
		if err := applyCommitmentUpdatesFromPrefix(db, prefix); err != nil {
			return common.Hash{}, err
		}
	}
	root, _, err := readCommitmentNode(db, 0, nil)
	if err != nil {
		return common.Hash{}, err
	}
	if err := WriteLatestDomainCommitmentRoot(db, root); err != nil {
		return common.Hash{}, err
	}
	return root, nil
}

// UpdateLatestDomainCommitment applies an incremental batch of latest-domain
// updates to the persisted binary-radix branch state and rewrites the root.
func UpdateLatestDomainCommitment(db latestDomainCommitmentStore, updates []StateCommitmentUpdate) (common.Hash, error) {
	updates = CoalesceStateCommitmentUpdates(updates)
	if err := applyCommitmentUpdates(db, updates); err != nil {
		return common.Hash{}, err
	}
	root, _, err := readCommitmentNode(db, 0, nil)
	if err != nil {
		return common.Hash{}, err
	}
	if err := WriteLatestDomainCommitmentRoot(db, root); err != nil {
		return common.Hash{}, err
	}
	return root, nil
}

func clearLatestDomainCommitmentNodes(db latestDomainCommitmentStore) error {
	var keys [][]byte
	if err := IterateStateCommitmentDomain(db, commitmentNodePrefix, func(logicalKey, _ []byte) (bool, error) {
		keys = append(keys, append([]byte(nil), logicalKey...))
		return true, nil
	}); err != nil {
		return err
	}
	for _, key := range keys {
		if err := DeleteStateCommitmentDomain(db, key); err != nil {
			return err
		}
	}
	return nil
}

func applyCommitmentUpdatesFromPrefix(db latestDomainCommitmentStore, prefix []byte) error {
	it := db.NewIterator(prefix, nil)
	defer it.Release()
	var updates []StateCommitmentUpdate
	for it.Next() {
		updates = append(updates, NewStateCommitmentPut(it.Key(), it.Value()))
	}
	if err := it.Error(); err != nil {
		return err
	}
	return applyCommitmentUpdates(db, updates)
}

func applyCommitmentUpdates(db latestDomainCommitmentStore, updates []StateCommitmentUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	current := make(map[string][]byte, len(updates))
	for _, update := range updates {
		path := commitmentPath(update.Key)
		leaf := common.Hash{}
		if !update.Delete {
			leaf = commitmentLeafHash(update.Key, update.Value)
		}
		if err := writeCommitmentNode(db, commitmentPathBits, path[:], leaf); err != nil {
			return err
		}
		current[string(commitmentNodeKey(commitmentPathBits, path[:]))] = append([]byte(nil), path[:]...)
	}
	for depth := commitmentPathBits - 1; depth >= 0; depth-- {
		parents := make(map[string][]byte, len(current))
		for _, path := range current {
			parents[string(commitmentNodeKey(depth, path))] = append([]byte(nil), path...)
		}
		keys := make([]string, 0, len(parents))
		for key := range parents {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool {
			return bytes.Compare([]byte(keys[i]), []byte(keys[j])) < 0
		})
		for _, key := range keys {
			path := parents[key]
			leftPath := commitmentChildPath(path, depth, 0)
			rightPath := commitmentChildPath(path, depth, 1)
			left, _, err := readCommitmentNode(db, depth+1, leftPath)
			if err != nil {
				return err
			}
			right, _, err := readCommitmentNode(db, depth+1, rightPath)
			if err != nil {
				return err
			}
			if err := writeCommitmentNode(db, depth, path, commitmentBranchHash(left, right)); err != nil {
				return err
			}
		}
		current = parents
	}
	return nil
}

func readCommitmentNode(db ethdb.KeyValueReader, depth int, path []byte) (common.Hash, bool, error) {
	value, ok, err := ReadStateCommitmentDomain(db, commitmentNodeKey(depth, path))
	if err != nil || !ok {
		return common.Hash{}, ok, err
	}
	if len(value) != common.HashLength {
		return common.Hash{}, false, fmt.Errorf("commitment node depth %d: bad hash length %d", depth, len(value))
	}
	return common.BytesToHash(value), true, nil
}

func writeCommitmentNode(db ethdb.KeyValueWriter, depth int, path []byte, hash common.Hash) error {
	key := commitmentNodeKey(depth, path)
	if hash == (common.Hash{}) {
		return DeleteStateCommitmentDomain(db, key)
	}
	return WriteStateCommitmentDomain(db, key, hash.Bytes())
}

func commitmentNodeKey(depth int, path []byte) []byte {
	out := make([]byte, 0, len(commitmentNodePrefix)+2+(depth+7)/8)
	out = append(out, commitmentNodePrefix...)
	var depthBuf [2]byte
	binary.BigEndian.PutUint16(depthBuf[:], uint16(depth))
	out = append(out, depthBuf[:]...)
	prefixLen := (depth + 7) / 8
	if prefixLen == 0 {
		return out
	}
	prefix := make([]byte, prefixLen)
	copy(prefix, path)
	if rem := depth % 8; rem != 0 {
		prefix[prefixLen-1] &= byte(0xff << (8 - rem))
	}
	return append(out, prefix...)
}

func commitmentChildPath(path []byte, parentDepth, childBit int) []byte {
	child := append([]byte(nil), path...)
	setCommitmentPathBit(child, parentDepth, childBit)
	return child
}

func commitmentPath(key []byte) common.Hash {
	h := sha3.NewLegacyKeccak256()
	writeCommitmentLenPrefixed(h, key)
	var out common.Hash
	h.Sum(out[:0])
	return out
}

func commitmentLeafHash(key, value []byte) common.Hash {
	h := sha3.NewLegacyKeccak256()
	_, _ = h.Write([]byte{0x00})
	writeCommitmentLenPrefixed(h, key)
	writeCommitmentLenPrefixed(h, value)
	var out common.Hash
	h.Sum(out[:0])
	return out
}

func commitmentBranchHash(left, right common.Hash) common.Hash {
	if left == (common.Hash{}) && right == (common.Hash{}) {
		return common.Hash{}
	}
	h := sha3.NewLegacyKeccak256()
	_, _ = h.Write([]byte{0x01})
	_, _ = h.Write(left.Bytes())
	_, _ = h.Write(right.Bytes())
	var out common.Hash
	h.Sum(out[:0])
	return out
}

func writeCommitmentLenPrefixed(h interface{ Write([]byte) (int, error) }, data []byte) {
	var lenBuf [8]byte
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(data)))
	_, _ = h.Write(lenBuf[:])
	_, _ = h.Write(data)
}

func setCommitmentPathBit(path []byte, bitIndex, bit int) {
	if bitIndex < 0 || bitIndex >= len(path)*8 {
		return
	}
	mask := byte(0x80 >> (bitIndex % 8))
	if bit == 0 {
		path[bitIndex/8] &^= mask
		return
	}
	path[bitIndex/8] |= mask
}
