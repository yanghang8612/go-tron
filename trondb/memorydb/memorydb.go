package memorydb

import (
	"bytes"
	"errors"
	"sort"
	"sync"

	"github.com/tronprotocol/go-tron/trondb"
)

var errClosed = errors.New("database closed")

// Database is an in-memory key-value database for testing.
type Database struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func New() *Database {
	return &Database{data: make(map[string][]byte)}
}

func (db *Database) Has(key []byte) (bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.data == nil {
		return false, errClosed
	}
	_, ok := db.data[string(key)]
	return ok, nil
}

func (db *Database) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.data == nil {
		return nil, errClosed
	}
	val, ok := db.data[string(key)]
	if !ok {
		return nil, trondb.ErrNotFound
	}
	return append([]byte{}, val...), nil
}

func (db *Database) Put(key []byte, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.data == nil {
		return errClosed
	}
	db.data[string(key)] = append([]byte{}, value...)
	return nil
}

func (db *Database) Delete(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.data == nil {
		return errClosed
	}
	delete(db.data, string(key))
	return nil
}

func (db *Database) NewBatch() trondb.Batch {
	return &batch{db: db}
}

func (db *Database) NewIterator(prefix []byte, start []byte) trondb.Iterator {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var startKey []byte
	if start != nil {
		startKey = append(append([]byte{}, prefix...), start...)
	}

	var keys []string
	for k := range db.data {
		if prefix != nil && !bytes.HasPrefix([]byte(k), prefix) {
			continue
		}
		if startKey != nil && bytes.Compare([]byte(k), startKey) < 0 {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)

	items := make([]kv, len(keys))
	for i, k := range keys {
		items[i] = kv{key: []byte(k), value: append([]byte{}, db.data[k]...)}
	}
	return &iterator{items: items, pos: -1}
}

func (db *Database) Stat() (string, error)                    { return "memorydb", nil }
func (db *Database) Compact(start []byte, limit []byte) error { return nil }

func (db *Database) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.data = nil
	return nil
}

// Verify interface compliance.
var _ trondb.Database = (*Database)(nil)

// batch

type batchOp struct {
	key    []byte
	value  []byte
	delete bool
}

type batch struct {
	db   *Database
	ops  []batchOp
	size int
}

func (b *batch) Put(key, value []byte) error {
	b.ops = append(b.ops, batchOp{key: append([]byte{}, key...), value: append([]byte{}, value...)})
	b.size += len(key) + len(value)
	return nil
}

func (b *batch) Delete(key []byte) error {
	b.ops = append(b.ops, batchOp{key: append([]byte{}, key...), delete: true})
	b.size += len(key)
	return nil
}

func (b *batch) ValueSize() int { return b.size }

func (b *batch) Write() error {
	b.db.mu.Lock()
	defer b.db.mu.Unlock()
	for _, op := range b.ops {
		if op.delete {
			delete(b.db.data, string(op.key))
		} else {
			b.db.data[string(op.key)] = op.value
		}
	}
	return nil
}

func (b *batch) Reset() {
	b.ops = b.ops[:0]
	b.size = 0
}

// iterator

type kv struct {
	key   []byte
	value []byte
}

type iterator struct {
	items []kv
	pos   int
}

func (it *iterator) Next() bool {
	it.pos++
	return it.pos < len(it.items)
}

func (it *iterator) Key() []byte {
	if it.pos < 0 || it.pos >= len(it.items) {
		return nil
	}
	return it.items[it.pos].key
}

func (it *iterator) Value() []byte {
	if it.pos < 0 || it.pos >= len(it.items) {
		return nil
	}
	return it.items[it.pos].value
}

func (it *iterator) Release()        {}
func (it *iterator) Error() error    { return nil }
