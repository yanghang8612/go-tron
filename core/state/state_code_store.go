package state

import (
	"errors"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type stateCodeReader interface {
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
