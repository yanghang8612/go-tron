package state

import (
	"bytes"
	"encoding/binary"
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/statecodec"
	shieldpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func (s *StateDB) readSystemShielded(key []byte) ([]byte, bool) {
	raw, ok, err := s.readSystemShieldedWithError(key)
	if err != nil {
		s.recordStateError(fmt.Sprintf("read system shielded key=%x", key), err)
		return nil, false
	}
	if !ok {
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

func (s *StateDB) NoteCommitmentCount() int64 {
	count, ok, err := s.NoteCommitmentCountStrict()
	if err != nil {
		s.recordStateError("read note commitment count", err)
		return 0
	}
	if !ok {
		return 0
	}
	return count
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

func (s *StateDB) AppendNoteCommitment(commitment []byte) error {
	idx, _, err := s.NoteCommitmentCountStrict()
	if err != nil {
		return err
	}
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
	result, ok, err := s.ReadZKProofResultStrict(txID)
	if err != nil {
		s.recordStateError(fmt.Sprintf("read zk proof result %x", txID), err)
		return false, false
	}
	if !ok {
		return false, false
	}
	return result, true
}

func (s *StateDB) ReadZKProofResultStrict(txID []byte) (bool, bool, error) {
	data, ok, err := s.readSystemShieldedWithError(rawdb.ZKProofStateKey(txID))
	if err != nil || !ok {
		return false, ok, err
	}
	if len(data) != 1 {
		return false, false, fmt.Errorf("decode zk proof result %x: length %d, want 1", txID, len(data))
	}
	if data[0] > 1 {
		return false, true, fmt.Errorf("decode zk proof result %x: non-canonical value %d", txID, data[0])
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

func (s *StateDB) WriteIncrMerkleTree(root []byte, tree *shieldpb.IncrementalMerkleTree) error {
	data, err := statecodec.Marshal(tree)
	if err != nil {
		return err
	}
	return s.writeSystemShielded(rawdb.IncrMerkleTreeStateKey(root), data)
}

func (s *StateDB) ReadIncrMerkleTree(root []byte) *shieldpb.IncrementalMerkleTree {
	tree, ok, err := s.ReadIncrMerkleTreeStrict(root)
	if err != nil {
		s.recordStateError(fmt.Sprintf("read incremental merkle tree %x", root), err)
		return nil
	}
	if !ok {
		return nil
	}
	return tree
}

func (s *StateDB) ReadIncrMerkleTreeStrict(root []byte) (*shieldpb.IncrementalMerkleTree, bool, error) {
	data, ok, err := s.readSystemShieldedWithError(rawdb.IncrMerkleTreeStateKey(root))
	if err != nil {
		return nil, false, err
	}
	return decodeShieldedMerkleTreeStrict(fmt.Sprintf("decode incremental merkle tree %x", root), data, ok)
}

func (s *StateDB) HasIncrMerkleTree(root []byte) bool {
	_, ok, err := s.ReadIncrMerkleTreeStrict(root)
	if err != nil {
		s.recordStateError(fmt.Sprintf("check incremental merkle tree %x", root), err)
		return false
	}
	return ok
}

func (s *StateDB) HasIncrMerkleTreeStrict(root []byte) (bool, error) {
	_, ok, err := s.readSystemShieldedWithError(rawdb.IncrMerkleTreeStateKey(root))
	return ok, err
}

func (s *StateDB) ReadLastMerkleTree() *shieldpb.IncrementalMerkleTree {
	tree, ok, err := s.ReadLastMerkleTreeStrict()
	if err != nil {
		s.recordStateError("read last incremental merkle tree", err)
		return nil
	}
	if !ok {
		return nil
	}
	return tree
}

func (s *StateDB) ReadLastMerkleTreeStrict() (*shieldpb.IncrementalMerkleTree, bool, error) {
	data, ok, err := s.readSystemShieldedWithError(rawdb.IncrMerkleLastTreeStateKey())
	if err != nil {
		return nil, false, err
	}
	return decodeShieldedMerkleTreeStrict("decode last incremental merkle tree", data, ok)
}

func (s *StateDB) WriteLastMerkleTree(tree *shieldpb.IncrementalMerkleTree) error {
	data, err := statecodec.Marshal(tree)
	if err != nil {
		return err
	}
	return s.writeSystemShielded(rawdb.IncrMerkleLastTreeStateKey(), data)
}

func (s *StateDB) ReadCurrentMerkleTree() *shieldpb.IncrementalMerkleTree {
	tree, ok, err := s.ReadCurrentMerkleTreeStrict()
	if err != nil {
		s.recordStateError("read current incremental merkle tree", err)
		return nil
	}
	if !ok {
		return nil
	}
	return tree
}

func (s *StateDB) ReadCurrentMerkleTreeStrict() (*shieldpb.IncrementalMerkleTree, bool, error) {
	data, ok, err := s.readSystemShieldedWithError(rawdb.IncrMerkleCurrentTreeStateKey())
	if err != nil {
		return nil, false, err
	}
	return decodeShieldedMerkleTreeStrict("decode current incremental merkle tree", data, ok)
}

func (s *StateDB) WriteCurrentMerkleTree(tree *shieldpb.IncrementalMerkleTree) error {
	data, err := statecodec.Marshal(tree)
	if err != nil {
		return err
	}
	return s.writeSystemShielded(rawdb.IncrMerkleCurrentTreeStateKey(), data)
}

func (s *StateDB) DeleteCurrentMerkleTree() error {
	return s.deleteSystemShielded(rawdb.IncrMerkleCurrentTreeStateKey())
}

func (s *StateDB) ReadMerkleTreeRootByBlock(blockNum int64) []byte {
	data, ok, err := s.ReadMerkleTreeRootByBlockStrict(blockNum)
	if err != nil {
		s.recordStateError(fmt.Sprintf("read merkle tree root by block %d", blockNum), err)
		return nil
	}
	if !ok {
		return nil
	}
	return data
}

// shieldedRootReadHeight bounds sparse runs by this view's canonical cut.
// A predecessor alone must never invent a root for an unsynchronized height.
func (s *StateDB) shieldedRootReadHeight() (uint64, error) {
	if view, ok := s.accountKVLatestReader.(historicalLatestView); ok {
		return view.blockNum, nil
	}
	var height uint64
	if s.dynProps != nil && s.dynProps.LatestBlockHeaderNumber() > 0 {
		height = uint64(s.dynProps.LatestBlockHeaderNumber())
	}
	if s.changeSet.enabled && s.changeSet.blockNum > height {
		height = s.changeSet.blockNum
	}
	if height > 0 {
		return height, nil
	}
	head, ok, err := rawdb.ReadStateCommitmentHead(s.accountKVIndex())
	if err != nil {
		return 0, err
	}
	if ok {
		return head.BlockNum, nil
	}
	return 0, nil
}
func (s *StateDB) ReadMerkleTreeRootByBlockStrict(blockNum int64) ([]byte, bool, error) {
	if blockNum <= 0 {
		return nil, false, nil
	}
	height, err := s.shieldedRootReadHeight()
	if err != nil {
		return nil, false, err
	}
	if uint64(blockNum) > height {
		return nil, false, nil
	}
	_, data, ok, err := s.shieldedRootBefore(uint64(blockNum) + 1)
	return data, ok, err
}
func (s *StateDB) shieldedRootBefore(upper uint64) (uint64, []byte, bool, error) {
	key, data, ok, err := s.AccountKVBefore(tcommon.SystemAccountAddress, kvdomains.SystemShielded, rawdb.ShieldedRootChangePrefix(), rawdb.ShieldedRootChangeStateKey(upper))
	if err != nil || !ok {
		return 0, nil, ok, err
	}
	height, err := rawdb.DecodeShieldedRootChangeStateKey(key)
	if err != nil {
		return 0, nil, true, err
	}
	if len(data) != 32 {
		return 0, nil, true, fmt.Errorf("decode merkle tree root by block %d: length %d, want 32", height, len(data))
	}
	return height, data, true, nil
}

// WriteMerkleTreeRootByBlock starts a new run only when the root changes.
// An equal root at a later block performs no KV write or history update.
func (s *StateDB) WriteMerkleTreeRootByBlock(blockNum int64, root []byte) error {
	if blockNum <= 0 || len(root) != 32 {
		return fmt.Errorf("invalid shielded root change: block=%d root length=%d", blockNum, len(root))
	}
	cut, err := s.shieldedRootReadHeight()
	if err != nil {
		return err
	}
	if uint64(blockNum) < cut {
		return fmt.Errorf("shielded root change at %d precedes execution cut %d", blockNum, cut)
	}
	height, previous, ok, err := s.shieldedRootBefore(uint64(blockNum) + 1)
	if err != nil {
		return err
	}
	if ok && bytes.Equal(previous, root) {
		return nil
	}
	key := rawdb.ShieldedRootChangeStateKey(uint64(blockNum))
	if ok && height == uint64(blockNum) {
		_, before, exists, err := s.shieldedRootBefore(uint64(blockNum))
		if err != nil {
			return err
		}
		if exists && bytes.Equal(before, root) {
			return s.deleteSystemShielded(key)
		}
	}
	return s.writeSystemShielded(key, root)
}

func decodeShieldedMerkleTreeStrict(context string, data []byte, ok bool) (*shieldpb.IncrementalMerkleTree, bool, error) {
	if !ok {
		return nil, false, nil
	}
	if len(data) == 0 {
		if context == "" {
			context = "decode shielded merkle tree"
		}
		return nil, true, fmt.Errorf("%s: empty value", context)
	}
	var tree shieldpb.IncrementalMerkleTree
	if err := statecodec.Unmarshal(data, &tree); err != nil {
		if context == "" {
			context = "decode shielded merkle tree"
		}
		return nil, true, fmt.Errorf("%s: %w", context, err)
	}
	return &tree, true, nil
}
