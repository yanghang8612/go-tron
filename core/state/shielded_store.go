package state

import (
	"encoding/binary"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	shieldpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

func (s *StateDB) readSystemShielded(key []byte) ([]byte, bool) {
	raw, ok, err := s.GetAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemShielded, key)
	if err != nil || !ok {
		return nil, false
	}
	return raw, true
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

func (s *StateDB) WriteNullifier(nullifier []byte) error {
	return s.writeSystemShielded(rawdb.NullifierStateKey(nullifier), []byte{1})
}

func (s *StateDB) NoteCommitmentCount() int64 {
	data, ok := s.readSystemShielded(rawdb.NoteCommitmentCountStateKey())
	if !ok || len(data) != 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(data))
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

func (s *StateDB) ReadZKProofResult(txID []byte) (bool, bool) {
	data, ok := s.readSystemShielded(rawdb.ZKProofStateKey(txID))
	if !ok || len(data) == 0 {
		return false, false
	}
	return data[0] == 0x01, true
}

func (s *StateDB) WriteZKProofResult(txID []byte, ok bool) error {
	value := byte(0x00)
	if ok {
		value = 0x01
	}
	return s.writeSystemShielded(rawdb.ZKProofStateKey(txID), []byte{value})
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

func (s *StateDB) HasIncrMerkleTree(root []byte) bool {
	_, ok := s.readSystemShielded(rawdb.IncrMerkleTreeStateKey(root))
	return ok
}

func (s *StateDB) ReadLastMerkleTree() *shieldpb.IncrementalMerkleTree {
	return decodeShieldedMerkleTree(s.readSystemShielded(rawdb.IncrMerkleLastTreeStateKey()))
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

func (s *StateDB) WriteMerkleTreeRootByBlock(blockNum int64, root []byte) error {
	return s.writeSystemShielded(rawdb.MerkleTreeIndexStateKey(blockNum), root)
}

func decodeShieldedMerkleTree(data []byte, ok bool) *shieldpb.IncrementalMerkleTree {
	if !ok || len(data) == 0 {
		return nil
	}
	var tree shieldpb.IncrementalMerkleTree
	if err := proto.Unmarshal(data, &tree); err != nil {
		return nil
	}
	return &tree
}
