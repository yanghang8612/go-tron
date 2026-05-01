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

// HasZKProof returns true if this ZK proof has already been accepted.
// Used to prevent replay attacks on shielded transactions. Mirrors
// java-tron ZKProofStore.has.
func HasZKProof(db ethdb.KeyValueReader, proof []byte) bool {
	ok, _ := db.Has(zkProofKey(proof))
	return ok
}

// WriteZKProof marks a ZK proof as accepted (replay prevention).
// Mirrors java-tron ZKProofStore.put.
func WriteZKProof(db ethdb.KeyValueWriter, proof []byte) error {
	return db.Put(zkProofKey(proof), []byte{0x01})
}

// DeleteZKProof removes a ZK proof entry (used during state rollback).
func DeleteZKProof(db ethdb.KeyValueWriter, proof []byte) error {
	return db.Delete(zkProofKey(proof))
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
