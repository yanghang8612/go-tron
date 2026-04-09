package trondb

import (
	"errors"
	"io"
)

// ErrNotFound is returned when a requested key is not found in the database.
var ErrNotFound = errors.New("not found")

// KeyValueReader provides read access to a key-value store.
type KeyValueReader interface {
	Has(key []byte) (bool, error)
	Get(key []byte) ([]byte, error)
}

// KeyValueWriter provides write access to a key-value store.
type KeyValueWriter interface {
	Put(key []byte, value []byte) error
	Delete(key []byte) error
}

// KeyValueStore combines read and write access with iteration and batch support.
type KeyValueStore interface {
	KeyValueReader
	KeyValueWriter
	NewBatch() Batch
	NewIterator(prefix []byte, start []byte) Iterator
	Stat() (string, error)
	Compact(start []byte, limit []byte) error
	io.Closer
}

// Database is the main database interface.
type Database interface {
	KeyValueStore
}

// Batch is a write-only batch that commits atomically.
type Batch interface {
	KeyValueWriter
	ValueSize() int
	Write() error
	Reset()
}

// Iterator iterates over key-value pairs in key order.
type Iterator interface {
	Next() bool
	Key() []byte
	Value() []byte
	Release()
	Error() error
}
