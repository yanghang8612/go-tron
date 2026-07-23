package rawdb

import "github.com/ethereum/go-ethereum/ethdb"

// noCopyKeyValueReader is an optional fast path for state accessors that
// consume or defensively copy a value before returning it. Buffer-backed
// readers can expose immutable overlay storage without first copying it.
type noCopyKeyValueReader interface {
	GetNoCopy(key []byte) ([]byte, error)
}

// cachedNoCopyKeyValueReader additionally caches reads that fall through every
// rewindable overlay to the durable base. The block buffer invalidates cached
// keys when their writes become durable, so this is safe for all flat-latest
// rows, not only commitment branches.
type cachedNoCopyKeyValueReader interface {
	GetNoCopyCached(key []byte) ([]byte, error)
}

func readStateNoCopyCached(db ethdb.KeyValueReader, key []byte) ([]byte, error) {
	if cached, ok := db.(cachedNoCopyKeyValueReader); ok {
		return cached.GetNoCopyCached(key)
	}
	if noCopy, ok := db.(noCopyKeyValueReader); ok {
		return noCopy.GetNoCopy(key)
	}
	return db.Get(key)
}
