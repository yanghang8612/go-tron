package rawdb

import (
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
)

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

type statePrefetchReader interface {
	Prefetch(key []byte) ([]byte, error)
}

// keyNotFoundClassifier lets a reader identify its native missing-key error.
// Layered readers already have to classify durable backend misses before they
// can safely negative-cache them, so asking the same reader here avoids a
// second Has point lookup solely to rediscover that classification.
type keyNotFoundClassifier interface {
	IsKeyNotFound(error) bool
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

func readStatePresentNoCopy(db ethdb.KeyValueReader, key []byte, context string) ([]byte, bool, error) {
	// The bounded blockbuffer cache provides a one-lookup no-copy fast path.
	// Missing keys still run through Has so backend failures are not mistaken
	// for absence.
	if _, ok := db.(cachedNoCopyKeyValueReader); ok {
		value, err := readStateNoCopyCached(db, key)
		if err != nil {
			return verifyStateReadMiss(db, key, context, err)
		}
		return value, true, nil
	}
	exists, err := readKeyPresence(db, key, context)
	if err != nil || !exists {
		return nil, exists, err
	}
	value, err := readStateNoCopyCached(db, key)
	if err != nil {
		return nil, false, fmt.Errorf("rawdb: read %s: %w", context, err)
	}
	return value, true, nil
}

func prefetchStatePresentNoCopy(db ethdb.KeyValueReader, key []byte, context string) ([]byte, bool, error) {
	if reader, ok := db.(statePrefetchReader); ok {
		value, err := reader.Prefetch(key)
		if err != nil {
			return verifyStateReadMiss(db, key, context, err)
		}
		return value, true, nil
	}
	return readStatePresentNoCopy(db, key, context)
}

// verifyStateReadMiss distinguishes the not-found error returned by Pebble and
// memorydb from an actual storage failure. State hot paths optimistically issue
// one no-copy Get; only misses and failures pay for the follow-up Has call.
func verifyStateReadMiss(db ethdb.KeyValueReader, key []byte, context string, readErr error) ([]byte, bool, error) {
	if db == nil {
		return nil, false, fmt.Errorf("rawdb: nil database while reading %s", context)
	}
	if classifier, ok := db.(keyNotFoundClassifier); ok && classifier.IsKeyNotFound(readErr) {
		return nil, false, nil
	}
	exists, err := db.Has(key)
	if err != nil {
		return nil, false, fmt.Errorf("rawdb: read %s presence after get error: %w", context, err)
	}
	if !exists {
		return nil, false, nil
	}
	return nil, false, fmt.Errorf("rawdb: read %s: %w", context, readErr)
}
