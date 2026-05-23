package rawdb

import (
	"encoding/binary"

	"github.com/ethereum/go-ethereum/ethdb"
	shieldpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

// HasNullifier returns true if the nullifier has already been spent.
func HasNullifier(db ethdb.KeyValueReader, nullifier []byte) bool {
	has, err := db.Has(nullifierKey(nullifier))
	return err == nil && has
}

// WriteNullifier marks a nullifier as spent (double-spend prevention).
func WriteNullifier(db ethdb.KeyValueWriter, nullifier []byte) error {
	return db.Put(nullifierKey(nullifier), []byte{1})
}

// NoteCommitmentCount returns the total number of note commitments stored.
func NoteCommitmentCount(db ethdb.KeyValueReader) int64 {
	data, err := db.Get(noteCommitmentCountKey)
	if err != nil || len(data) != 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(data))
}

// AppendNoteCommitment stores a note commitment at the next sequential index.
// The index is used for Merkle tree position tracking. The db parameter is
// the read+write composite so both the on-disk store and a buffer-aware
// view (`core/blockbuffer.Buffer`) work — slice 3 of the fork-rewind fix
// routes shielded-transfer writes through the buffer for switchFork
// rewindability.
func AppendNoteCommitment(db interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
}, commitment []byte) error {
	idx := NoteCommitmentCount(db)
	if err := db.Put(noteCommitmentKey(idx), commitment); err != nil {
		return err
	}
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(idx+1))
	return db.Put(noteCommitmentCountKey, buf)
}

// ReadNoteCommitment returns the note commitment at the given index.
func ReadNoteCommitment(db ethdb.KeyValueReader, index int64) []byte {
	data, err := db.Get(noteCommitmentKey(index))
	if err != nil {
		return nil
	}
	return data
}

// HasZKProof returns true if this shielded transaction has a cached proof
// result. Mirrors java-tron ZKProofStore.has.
func HasZKProof(db ethdb.KeyValueReader, txID []byte) bool {
	ok, _ := db.Has(zkProofKey(txID))
	return ok
}

// ReadZKProofResult returns the cached proof-verification result for a
// shielded transaction. The second return value is false when no record exists.
func ReadZKProofResult(db ethdb.KeyValueReader, txID []byte) (bool, bool) {
	data, err := db.Get(zkProofKey(txID))
	if err != nil || len(data) == 0 {
		return false, false
	}
	return data[0] == 0x01, true
}

// WriteZKProofResult stores the proof-verification result for a shielded
// transaction. Mirrors java-tron ZKProofStore.put.
func WriteZKProofResult(db ethdb.KeyValueWriter, txID []byte, ok bool) error {
	value := byte(0x00)
	if ok {
		value = 0x01
	}
	return db.Put(zkProofKey(txID), []byte{value})
}

// WriteZKProof marks a shielded transaction proof as accepted. Kept as a
// convenience wrapper for callers that only need the accepted state.
func WriteZKProof(db ethdb.KeyValueWriter, txID []byte) error {
	return WriteZKProofResult(db, txID, true)
}

// DeleteZKProof removes a cached proof entry (used during state rollback).
func DeleteZKProof(db ethdb.KeyValueWriter, txID []byte) error {
	return db.Delete(zkProofKey(txID))
}

// WriteIncrMerkleTree stores the IncrementalMerkleTree state keyed by the
// 32-byte commitment-tree root (anchor). Mirrors java-tron
// IncrementalMerkleTreeStore.put.
func WriteIncrMerkleTree(db ethdb.KeyValueWriter, root []byte, tree *shieldpb.IncrementalMerkleTree) error {
	data, err := proto.Marshal(tree)
	if err != nil {
		return err
	}
	return db.Put(incrMerkleTreeKey(root), data)
}

// ReadIncrMerkleTree returns the IncrementalMerkleTree for the given
// commitment-tree root (anchor), or nil if absent. Mirrors java-tron
// IncrementalMerkleTreeStore.get.
func ReadIncrMerkleTree(db ethdb.KeyValueReader, root []byte) *shieldpb.IncrementalMerkleTree {
	data, err := db.Get(incrMerkleTreeKey(root))
	if err != nil || len(data) == 0 {
		return nil
	}
	var tree shieldpb.IncrementalMerkleTree
	if err := proto.Unmarshal(data, &tree); err != nil {
		return nil
	}
	return &tree
}

// HasIncrMerkleTree reports whether a tree state is stored for root.
func HasIncrMerkleTree(db ethdb.KeyValueReader, root []byte) bool {
	ok, _ := db.Has(incrMerkleTreeKey(root))
	return ok
}

// DeleteIncrMerkleTree removes the tree state for root (state rollback).
func DeleteIncrMerkleTree(db ethdb.KeyValueWriter, root []byte) error {
	return db.Delete(incrMerkleTreeKey(root))
}

// ReadLastMerkleTree returns the best (most-recently-saved) IncrementalMerkleTree.
// Mirrors java-tron `MerkleContainer.getBestMerkle()` reading the "LAST_TREE"
// sentinel from the IncrementalMerkleTreeStore. Returns nil when absent —
// callers should treat that as the empty-tree case.
func ReadLastMerkleTree(db ethdb.KeyValueReader) *shieldpb.IncrementalMerkleTree {
	data, err := db.Get(incrMerkleLastTreeKey)
	if err != nil || len(data) == 0 {
		return nil
	}
	var tree shieldpb.IncrementalMerkleTree
	if err := proto.Unmarshal(data, &tree); err != nil {
		return nil
	}
	return &tree
}

// WriteLastMerkleTree persists the best tree under the "LAST_TREE" sentinel.
// Called by MerkleContainer.SaveCurrentAsBest after a block's shielded txs
// have been applied.
func WriteLastMerkleTree(db ethdb.KeyValueWriter, tree *shieldpb.IncrementalMerkleTree) error {
	data, err := proto.Marshal(tree)
	if err != nil {
		return err
	}
	return db.Put(incrMerkleLastTreeKey, data)
}

// ReadCurrentMerkleTree returns the working tree being mutated by the active
// block's shielded txs. Returns nil if no current tree is set; callers should
// fall back to ReadLastMerkleTree (and then the empty tree).
func ReadCurrentMerkleTree(db ethdb.KeyValueReader) *shieldpb.IncrementalMerkleTree {
	data, err := db.Get(incrMerkleCurrentTreeKey)
	if err != nil || len(data) == 0 {
		return nil
	}
	var tree shieldpb.IncrementalMerkleTree
	if err := proto.Unmarshal(data, &tree); err != nil {
		return nil
	}
	return &tree
}

// WriteCurrentMerkleTree persists the working tree under the "CURRENT_TREE"
// sentinel. Reset at block start (copy of best) and advanced per shielded
// receive.
func WriteCurrentMerkleTree(db ethdb.KeyValueWriter, tree *shieldpb.IncrementalMerkleTree) error {
	data, err := proto.Marshal(tree)
	if err != nil {
		return err
	}
	return db.Put(incrMerkleCurrentTreeKey, data)
}

// DeleteCurrentMerkleTree clears the working-tree sentinel — used when
// resetting from a fresh empty tree (i.e. best is absent and we want
// current to be absent too).
func DeleteCurrentMerkleTree(db ethdb.KeyValueWriter) error {
	return db.Delete(incrMerkleCurrentTreeKey)
}

// ReadMerkleTreeRootByBlock returns the 32-byte tree root indexed at
// blockNum, or nil if absent. Mirrors java-tron's MerkleTreeIndexStore.
func ReadMerkleTreeRootByBlock(db ethdb.KeyValueReader, blockNum int64) []byte {
	data, err := db.Get(merkleTreeIndexKey(blockNum))
	if err != nil || len(data) == 0 {
		return nil
	}
	return data
}

// WriteMerkleTreeRootByBlock indexes blockNum → tree root. Called after a
// block successfully promotes its current tree to best.
func WriteMerkleTreeRootByBlock(db ethdb.KeyValueWriter, blockNum int64, root []byte) error {
	return db.Put(merkleTreeIndexKey(blockNum), root)
}

// DeleteMerkleTreeRootByBlock drops the block-number → root mapping (used
// during reorg-driven rollback).
func DeleteMerkleTreeRootByBlock(db ethdb.KeyValueWriter, blockNum int64) error {
	return db.Delete(merkleTreeIndexKey(blockNum))
}

func NullifierStateKey(nullifier []byte) []byte {
	return nullifierKey(nullifier)
}

func NoteCommitmentCountStateKey() []byte {
	return append([]byte(nil), noteCommitmentCountKey...)
}

func NoteCommitmentStateKey(index int64) []byte {
	return noteCommitmentKey(index)
}

func ZKProofStateKey(txID []byte) []byte {
	return zkProofKey(txID)
}

func IncrMerkleTreeStateKey(root []byte) []byte {
	return incrMerkleTreeKey(root)
}

func IncrMerkleLastTreeStateKey() []byte {
	return append([]byte(nil), incrMerkleLastTreeKey...)
}

func IncrMerkleCurrentTreeStateKey() []byte {
	return append([]byte(nil), incrMerkleCurrentTreeKey...)
}

func MerkleTreeIndexStateKey(blockNum int64) []byte {
	return merkleTreeIndexKey(blockNum)
}
