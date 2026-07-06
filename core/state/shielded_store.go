package state

import (
	"encoding/binary"
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	shieldpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

func (s *StateDB) readSystemShielded(key []byte) ([]byte, bool) {
	raw, ok, err := s.readSystemShieldedWithError(key)
	if err != nil || !ok {
		return nil, false
	}
	return raw, true
}

func (s *StateDB) readSystemShieldedWithError(key []byte) ([]byte, bool, error) {
	return s.GetAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemShielded, key)
}

func (s *StateDB) writeSystemShielded(key, value []byte) error {
	return s.SetAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemShielded, key, value)
}

func (s *StateDB) deleteSystemShielded(key []byte) error {
	return s.DeleteAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemShielded, key)
}

func (s *StateDB) HasNullifier(nullifier []byte) bool {
	_, ok := s.readSystemShielded(rawdb.NullifierStateKey(nullifier))
	return ok
}

func (s *StateDB) HasNullifierStrict(nullifier []byte) (bool, error) {
	_, ok, err := s.readSystemShieldedWithError(rawdb.NullifierStateKey(nullifier))
	return ok, err
}

func (s *StateDB) WriteNullifier(nullifier []byte) error {
	return s.writeSystemShielded(rawdb.NullifierStateKey(nullifier), []byte{1})
}

// ShieldedNullifierPrefetchKey returns the latest nullifier spent-marker row.
func ShieldedNullifierPrefetchKey(nullifier []byte) PrefetchKey {
	return AccountKVPrefetchKey(tcommon.SystemAccountAddress, kvdomains.SystemShielded, rawdb.NullifierStateKey(nullifier))
}

func (s *StateDB) NoteCommitmentCount() int64 {
	data, ok := s.readSystemShielded(rawdb.NoteCommitmentCountStateKey())
	if !ok || len(data) != 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(data))
}

func (s *StateDB) NoteCommitmentCountStrict() (int64, bool, error) {
	data, ok, err := s.readSystemShieldedWithError(rawdb.NoteCommitmentCountStateKey())
	if err != nil || !ok {
		return 0, ok, err
	}
	if len(data) != 8 {
		return 0, false, fmt.Errorf("decode note commitment count: length %d, want 8", len(data))
	}
	return int64(binary.BigEndian.Uint64(data)), true, nil
}

// ShieldedNoteCommitmentCountPrefetchKey returns the latest note-commitment counter row.
func ShieldedNoteCommitmentCountPrefetchKey() PrefetchKey {
	return AccountKVPrefetchKey(tcommon.SystemAccountAddress, kvdomains.SystemShielded, rawdb.NoteCommitmentCountStateKey())
}

func (s *StateDB) AppendNoteCommitment(commitment []byte) error {
	idx := s.NoteCommitmentCount()
	if err := s.writeSystemShielded(rawdb.NoteCommitmentStateKey(idx), commitment); err != nil {
		return err
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(idx+1))
	return s.writeSystemShielded(rawdb.NoteCommitmentCountStateKey(), buf[:])
}

func (s *StateDB) ReadNoteCommitment(index int64) []byte {
	data, ok := s.readSystemShielded(rawdb.NoteCommitmentStateKey(index))
	if !ok {
		return nil
	}
	return data
}

func (s *StateDB) ReadNoteCommitmentStrict(index int64) ([]byte, bool, error) {
	return s.readSystemShieldedWithError(rawdb.NoteCommitmentStateKey(index))
}

func (s *StateDB) ReadZKProofResult(txID []byte) (bool, bool) {
	data, ok := s.readSystemShielded(rawdb.ZKProofStateKey(txID))
	if !ok || len(data) == 0 {
		return false, false
	}
	return data[0] == 0x01, true
}

func (s *StateDB) ReadZKProofResultStrict(txID []byte) (bool, bool, error) {
	data, ok, err := s.readSystemShieldedWithError(rawdb.ZKProofStateKey(txID))
	if err != nil || !ok {
		return false, ok, err
	}
	if len(data) != 1 {
		return false, false, fmt.Errorf("decode zk proof result %x: length %d, want 1", txID, len(data))
	}
	return data[0] == 0x01, true, nil
}

func (s *StateDB) WriteZKProofResult(txID []byte, ok bool) error {
	value := byte(0x00)
	if ok {
		value = 0x01
	}
	return s.writeSystemShielded(rawdb.ZKProofStateKey(txID), []byte{value})
}

// ShieldedZKProofResultPrefetchKey returns the latest cached proof-result row.
func ShieldedZKProofResultPrefetchKey(txID []byte) PrefetchKey {
	return AccountKVPrefetchKey(tcommon.SystemAccountAddress, kvdomains.SystemShielded, rawdb.ZKProofStateKey(txID))
}

func (s *StateDB) WriteIncrMerkleTree(root []byte, tree *shieldpb.IncrementalMerkleTree) error {
	data, err := proto.Marshal(tree)
	if err != nil {
		return err
	}
	return s.writeSystemShielded(rawdb.IncrMerkleTreeStateKey(root), data)
}

func (s *StateDB) ReadIncrMerkleTree(root []byte) *shieldpb.IncrementalMerkleTree {
	return decodeShieldedMerkleTree(s.readSystemShielded(rawdb.IncrMerkleTreeStateKey(root)))
}

func (s *StateDB) ReadIncrMerkleTreeStrict(root []byte) (*shieldpb.IncrementalMerkleTree, bool, error) {
	data, ok, err := s.readSystemShieldedWithError(rawdb.IncrMerkleTreeStateKey(root))
	if err != nil {
		return nil, false, err
	}
	return decodeShieldedMerkleTreeStrict(fmt.Sprintf("decode incremental merkle tree %x", root), data, ok)
}

func (s *StateDB) HasIncrMerkleTree(root []byte) bool {
	_, ok := s.readSystemShielded(rawdb.IncrMerkleTreeStateKey(root))
	return ok
}

func (s *StateDB) HasIncrMerkleTreeStrict(root []byte) (bool, error) {
	_, ok, err := s.readSystemShieldedWithError(rawdb.IncrMerkleTreeStateKey(root))
	return ok, err
}

// ShieldedMerkleAnchorPrefetchKey returns the latest anchor-root merkle tree row.
func ShieldedMerkleAnchorPrefetchKey(root []byte) PrefetchKey {
	return AccountKVPrefetchKey(tcommon.SystemAccountAddress, kvdomains.SystemShielded, rawdb.IncrMerkleTreeStateKey(root))
}

func (s *StateDB) ReadLastMerkleTree() *shieldpb.IncrementalMerkleTree {
	return decodeShieldedMerkleTree(s.readSystemShielded(rawdb.IncrMerkleLastTreeStateKey()))
}

func (s *StateDB) ReadLastMerkleTreeStrict() (*shieldpb.IncrementalMerkleTree, bool, error) {
	data, ok, err := s.readSystemShieldedWithError(rawdb.IncrMerkleLastTreeStateKey())
	if err != nil {
		return nil, false, err
	}
	return decodeShieldedMerkleTreeStrict("decode last incremental merkle tree", data, ok)
}

func (s *StateDB) WriteLastMerkleTree(tree *shieldpb.IncrementalMerkleTree) error {
	data, err := proto.Marshal(tree)
	if err != nil {
		return err
	}
	return s.writeSystemShielded(rawdb.IncrMerkleLastTreeStateKey(), data)
}

func (s *StateDB) ReadCurrentMerkleTree() *shieldpb.IncrementalMerkleTree {
	return decodeShieldedMerkleTree(s.readSystemShielded(rawdb.IncrMerkleCurrentTreeStateKey()))
}

func (s *StateDB) ReadCurrentMerkleTreeStrict() (*shieldpb.IncrementalMerkleTree, bool, error) {
	data, ok, err := s.readSystemShieldedWithError(rawdb.IncrMerkleCurrentTreeStateKey())
	if err != nil {
		return nil, false, err
	}
	return decodeShieldedMerkleTreeStrict("decode current incremental merkle tree", data, ok)
}

func (s *StateDB) WriteCurrentMerkleTree(tree *shieldpb.IncrementalMerkleTree) error {
	data, err := proto.Marshal(tree)
	if err != nil {
		return err
	}
	return s.writeSystemShielded(rawdb.IncrMerkleCurrentTreeStateKey(), data)
}

func (s *StateDB) DeleteCurrentMerkleTree() error {
	return s.deleteSystemShielded(rawdb.IncrMerkleCurrentTreeStateKey())
}

func (s *StateDB) ReadMerkleTreeRootByBlock(blockNum int64) []byte {
	data, ok := s.readSystemShielded(rawdb.MerkleTreeIndexStateKey(blockNum))
	if !ok || len(data) == 0 {
		return nil
	}
	return data
}

func (s *StateDB) ReadMerkleTreeRootByBlockStrict(blockNum int64) ([]byte, bool, error) {
	data, ok, err := s.readSystemShieldedWithError(rawdb.MerkleTreeIndexStateKey(blockNum))
	if err != nil || !ok {
		return nil, ok, err
	}
	if len(data) != 32 {
		return nil, true, fmt.Errorf("decode merkle tree root by block %d: length %d, want 32", blockNum, len(data))
	}
	return data, true, nil
}

func (s *StateDB) WriteMerkleTreeRootByBlock(blockNum int64, root []byte) error {
	return s.writeSystemShielded(rawdb.MerkleTreeIndexStateKey(blockNum), root)
}

func decodeShieldedMerkleTree(data []byte, ok bool) *shieldpb.IncrementalMerkleTree {
	tree, exists, err := decodeShieldedMerkleTreeStrict("", data, ok)
	if err != nil || !exists {
		return nil
	}
	if len(data) == 0 {
		return nil
	}
	return tree
}

func decodeShieldedMerkleTreeStrict(context string, data []byte, ok bool) (*shieldpb.IncrementalMerkleTree, bool, error) {
	if !ok || len(data) == 0 {
		if !ok {
			return nil, false, nil
		}
		return &shieldpb.IncrementalMerkleTree{}, true, nil
	}
	var tree shieldpb.IncrementalMerkleTree
	if err := proto.Unmarshal(data, &tree); err != nil {
		if context == "" {
			context = "decode shielded merkle tree"
		}
		return nil, true, fmt.Errorf("%s: %w", context, err)
	}
	return &tree, true, nil
}
