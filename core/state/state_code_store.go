package state

import (
	"errors"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type stateCodeReader interface {
	// ReadStateCode returns caller-owned immutable bytecode. The caller may
	// retain the slice; changing it must not mutate the backing store.
	ReadStateCode(hash tcommon.Hash) []byte
}

type stateCodeWriter interface {
	WriteStateCode(hash tcommon.Hash, code []byte) error
}

type stateCodeStore interface {
	stateCodeReader
	stateCodeWriter
}

type rawDBStateCodeStore struct {
	reader ethdb.KeyValueReader
	writer ethdb.KeyValueWriter
}

func newRawDBStateCodeStore(db ethdb.Database) stateCodeStore {
	return rawDBStateCodeStore{reader: db, writer: db}
}

func newDefaultStateCodeStore(db *Database) stateCodeStore {
	if db == nil {
		return nil
	}
	return newRawDBStateCodeStore(db.DiskDB())
}

func (s rawDBStateCodeStore) ReadStateCode(hash tcommon.Hash) []byte {
	if s.reader == nil {
		return nil
	}
	return rawdb.ReadStateCode(s.reader, hash)
}

func (s rawDBStateCodeStore) WriteStateCode(hash tcommon.Hash, code []byte) error {
	if s.writer == nil {
		return errors.New("state code store: nil writer")
	}
	return rawdb.WriteStateCode(s.writer, hash, code)
}

func (s *StateDB) getStateCodeStore() stateCodeStore {
	if s == nil {
		return nil
	}
	if s.codeStore != nil {
		return s.codeStore
	}
	if s.db == nil {
		return nil
	}
	return newRawDBStateCodeStore(s.db.DiskDB())
}

// SetStateCodeReader routes immutable content-addressed bytecode reads through
// reader while preserving the durable code writer. Block application supplies
// its generation-safe blockbuffer here so repeated contract calls can use the
// same bounded two-hit base-read cache as flat latest state. Code bytes remain
// caller-owned at the stateCodeReader boundary.
func (s *StateDB) SetStateCodeReader(reader ethdb.KeyValueReader) {
	if s == nil || reader == nil {
		return
	}
	current, ok := s.getStateCodeStore().(rawDBStateCodeStore)
	if !ok {
		// Explicit typed-store overrides own both sides of their contract.
		return
	}
	current.reader = reader
	s.codeStore = current
}

func (s *StateDB) readStateCode(hash tcommon.Hash) []byte {
	store := s.getStateCodeStore()
	if store == nil {
		return nil
	}
	return store.ReadStateCode(hash)
}

func (s *StateDB) writeStateCode(hash tcommon.Hash, code []byte) error {
	store := s.getStateCodeStore()
	if store == nil {
		return errors.New("state code store: nil store")
	}
	return store.WriteStateCode(hash, code)
}
