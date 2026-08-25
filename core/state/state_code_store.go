package state

import (
	"errors"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type stateCodeReader interface {
	// ReadStateCode returns immutable bytecode that remains valid after return.
	// The caller may retain the slice but must not mutate it.
	ReadStateCode(hash tcommon.Hash) []byte
}

type stateCodeStrictReader interface {
	ReadStateCodeStrict(hash tcommon.Hash) ([]byte, bool, error)
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
	return rawdb.ReadStateCodeImmutable(s.reader, hash)
}

func (s rawDBStateCodeStore) ReadStateCodeStrict(hash tcommon.Hash) ([]byte, bool, error) {
	if s.reader == nil {
		return nil, false, nil
	}
	return rawdb.ReadStateCodeStrict(s.reader, hash)
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
// immutable and lifetime-stable at the stateCodeReader boundary.
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
	if code, ok := s.cachedStateCode(hash, store); ok {
		return code
	}
	code := store.ReadStateCode(hash)
	s.admitStateCode(hash, code, store)
	return code
}

func (s *StateDB) readStateCodeStrict(hash tcommon.Hash) ([]byte, bool, error) {
	store := s.getStateCodeStore()
	if store == nil {
		return nil, false, nil
	}
	if code, ok := s.cachedStateCode(hash, store); ok {
		return code, true, nil
	}
	if strict, ok := store.(stateCodeStrictReader); ok {
		code, found, err := strict.ReadStateCodeStrict(hash)
		if err == nil && found {
			s.admitStateCode(hash, code, store)
		}
		return code, found, err
	}
	code := store.ReadStateCode(hash)
	if len(code) == 0 {
		return nil, false, nil
	}
	s.admitStateCode(hash, code, store)
	return append([]byte(nil), code...), true, nil
}

func (s *StateDB) writeStateCode(hash tcommon.Hash, code []byte) error {
	store := s.getStateCodeStore()
	if store == nil {
		return errors.New("state code store: nil store")
	}
	return store.WriteStateCode(hash, code)
}

func (s *StateDB) cachedStateCode(hash tcommon.Hash, store stateCodeStore) ([]byte, bool) {
	if s == nil || s.db == nil || s.db.codeCache == nil {
		return nil, false
	}
	// Typed store overrides may represent a different logical database. Only
	// raw stores wired to this Database (including blockbuffer reader overrides)
	// participate in its cache.
	if _, ok := store.(rawDBStateCodeStore); !ok {
		return nil, false
	}
	return s.db.codeCache.get(hash)
}

func (s *StateDB) admitStateCode(hash tcommon.Hash, code []byte, store stateCodeStore) {
	if s == nil || s.db == nil || s.db.codeCache == nil {
		return
	}
	if _, ok := store.(rawDBStateCodeStore); !ok {
		return
	}
	s.db.codeCache.admit(hash, code)
}
