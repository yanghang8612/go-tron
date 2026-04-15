package rawdb

import (
	"encoding/binary"

	"github.com/ethereum/go-ethereum/ethdb"
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
// The index is used for Merkle tree position tracking.
func AppendNoteCommitment(db ethdb.KeyValueStore, commitment []byte) error {
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
