package state

import (
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type derivedDynamicPropertyReader interface {
	ReadDerivedDynamicProperty(name string) []byte
	IterateDerivedDynamicProperties(fn func(name string, value []byte)) bool
}

type derivedDynamicPropertyWriter interface {
	WriteDerivedDynamicProperty(name string, value []byte)
}

type rawDBDerivedDynamicPropertyStore struct {
	reader ethdb.KeyValueReader
	writer ethdb.KeyValueWriter
	iter   ethdb.Iteratee
}

func newRawDBDerivedDynamicPropertyReader(db ethdb.KeyValueReader) rawDBDerivedDynamicPropertyStore {
	store := rawDBDerivedDynamicPropertyStore{reader: db}
	if iter, ok := db.(ethdb.Iteratee); ok {
		store.iter = iter
	}
	return store
}

func newRawDBDerivedDynamicPropertyWriter(db ethdb.KeyValueWriter) rawDBDerivedDynamicPropertyStore {
	return rawDBDerivedDynamicPropertyStore{writer: db}
}

func (s rawDBDerivedDynamicPropertyStore) ReadDerivedDynamicProperty(name string) []byte {
	if s.reader == nil {
		return nil
	}
	return rawdb.ReadDynamicProperty(s.reader, name)
}

func (s rawDBDerivedDynamicPropertyStore) IterateDerivedDynamicProperties(fn func(name string, value []byte)) bool {
	if s.iter == nil || fn == nil {
		return false
	}
	rawdb.IterateDynamicProperties(s.iter, fn)
	return true
}

func (s rawDBDerivedDynamicPropertyStore) WriteDerivedDynamicProperty(name string, value []byte) {
	if s.writer == nil {
		return
	}
	rawdb.WriteDynamicProperty(s.writer, name, value)
}
