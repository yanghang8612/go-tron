package state

import (
	"github.com/VictoriaMetrics/fastcache"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
)

type trieNodeCacheDB struct {
	ethdb.Database
	cache *fastcache.Cache
}

func (db *trieNodeCacheDB) Get(key []byte) ([]byte, error) {
	if db.cache != nil && len(key) == common.HashLength {
		if value, ok := db.cache.HasGet(nil, key); ok {
			return value, nil
		}
	}
	value, err := db.Database.Get(key)
	if err != nil {
		return nil, err
	}
	if db.cache != nil && len(key) == common.HashLength && len(value) > 0 {
		db.cache.Set(key, value)
	}
	return value, nil
}

func (db *trieNodeCacheDB) Has(key []byte) (bool, error) {
	if db.cache != nil && len(key) == common.HashLength && db.cache.Has(key) {
		return true, nil
	}
	return db.Database.Has(key)
}

func (db *trieNodeCacheDB) Put(key, value []byte) error {
	if err := db.Database.Put(key, value); err != nil {
		return err
	}
	if db.cache != nil && len(key) == common.HashLength && len(value) > 0 {
		db.cache.Set(key, value)
	}
	return nil
}

func (db *trieNodeCacheDB) Delete(key []byte) error {
	if err := db.Database.Delete(key); err != nil {
		return err
	}
	if db.cache != nil && len(key) == common.HashLength {
		db.cache.Del(key)
	}
	return nil
}

func (db *trieNodeCacheDB) DeleteRange(start, end []byte) error {
	if err := db.Database.DeleteRange(start, end); err != nil {
		return err
	}
	if db.cache != nil {
		db.cache.Reset()
	}
	return nil
}

func (db *trieNodeCacheDB) NewBatch() ethdb.Batch {
	return &trieNodeCacheBatch{
		Batch: db.Database.NewBatch(),
		cache: db.cache,
	}
}

func (db *trieNodeCacheDB) NewBatchWithSize(size int) ethdb.Batch {
	return &trieNodeCacheBatch{
		Batch: db.Database.NewBatchWithSize(size),
		cache: db.cache,
	}
}

type trieNodeCacheBatch struct {
	ethdb.Batch
	cache      *fastcache.Cache
	pending    []trieNodeCacheBatchPut
	resetCache bool
}

type trieNodeCacheBatchPut struct {
	key   []byte
	value []byte
}

func (b *trieNodeCacheBatch) Put(key, value []byte) error {
	if err := b.Batch.Put(key, value); err != nil {
		return err
	}
	if b.cache != nil && len(key) == common.HashLength && len(value) > 0 {
		b.pending = append(b.pending, trieNodeCacheBatchPut{
			key:   append([]byte(nil), key...),
			value: append([]byte(nil), value...),
		})
	}
	return nil
}

func (b *trieNodeCacheBatch) Delete(key []byte) error {
	if err := b.Batch.Delete(key); err != nil {
		return err
	}
	if b.cache != nil && len(key) == common.HashLength {
		b.cache.Del(key)
	}
	return nil
}

func (b *trieNodeCacheBatch) DeleteRange(start, end []byte) error {
	if err := b.Batch.DeleteRange(start, end); err != nil {
		return err
	}
	b.resetCache = b.cache != nil
	return nil
}

func (b *trieNodeCacheBatch) Write() error {
	if err := b.Batch.Write(); err != nil {
		return err
	}
	if b.cache != nil {
		if b.resetCache {
			b.cache.Reset()
		}
		for _, put := range b.pending {
			b.cache.Set(put.key, put.value)
		}
	}
	b.pending = b.pending[:0]
	b.resetCache = false
	return nil
}

func (b *trieNodeCacheBatch) Reset() {
	b.Batch.Reset()
	b.pending = b.pending[:0]
	b.resetCache = false
}
