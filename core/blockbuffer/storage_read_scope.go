package blockbuffer

import (
	"bytes"
	"errors"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/pointread"
)

var (
	storageReadScopes      = metrics.NewRegisteredCounter("blockbuffer/storage_read/scopes", nil)
	storageReadBusy        = metrics.NewRegisteredCounter("blockbuffer/storage_read/fallback_busy", nil)
	storageReadUnsupported = metrics.NewRegisteredCounter("blockbuffer/storage_read/fallback_unsupported", nil)
	storageReadDurable     = metrics.NewRegisteredCounter("blockbuffer/storage_read/durable_calls", nil)
)

var errStorageReadScope = errors.New("blockbuffer: storage read scope no longer owns its active layer")

// StorageReadScope is one canonical block's fixed durable cut and captured
// layer topology. The active layer remains mutable during that block. Its
// caller owns the canonical writer lock, does not replace/discard the active
// layer, and closes before publishing that layer or starting the next block.
// It is execution-confined, including its cursor and callback scratch state.
type StorageReadScope struct {
	buffer   *Buffer
	snapshot *ReadSnapshot
	cursor   pointread.Cursor
	active   *layer
	callback func([]byte) error
	result   []byte
}

// TryStorageReadScope never queues behind a base flush. Absence of the optional
// snapshot/cursor capability leaves the existing live read path in place;
// supported storage failures are errors, not permission to read a moving cut.
func (b *Buffer) TryStorageReadScope(number uint64) (*StorageReadScope, error) {
	snapshot, err := b.TryNewReadSnapshot()
	if err == ErrReadSnapshotBusy {
		storageReadBusy.Inc(1)
		return nil, nil
	}
	if storageSnapshotUnsupportedOnly(err) {
		storageReadUnsupported.Inc(1)
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if snapshot.base == nil {
		return nil, errors.Join(errors.New("blockbuffer: storage snapshot has no base"), snapshot.Close())
	}
	layers := snapshot.view.inflight
	if len(layers) == 0 || layers[len(layers)-1].number != number {
		return nil, errors.Join(errStorageReadScope, snapshot.Close())
	}
	factory, ok := snapshot.base.(pointread.KeyValueCursorSnapshot)
	if !ok {
		storageReadUnsupported.Inc(1)
		return nil, snapshot.Close()
	}
	cursor, err := factory.NewPointReadCursor()
	if err == pointread.ErrPointReadCursorUnsupported {
		storageReadUnsupported.Inc(1)
		return nil, snapshot.Close()
	}
	if err != nil {
		return nil, errors.Join(err, snapshot.Close())
	}
	if cursor == nil {
		return nil, errors.Join(errors.New("blockbuffer: storage snapshot returned nil cursor"), snapshot.Close())
	}
	s := &StorageReadScope{buffer: b, snapshot: snapshot, cursor: cursor, active: layers[len(layers)-1]}
	s.callback = s.consume
	storageReadScopes.Inc(1)
	return s, nil
}

// A compound factory error can contain an unsupported leaf AND a failed
// cleanup of another already-open snapshot. Only pure capability absence may
// fall back; errors.Is alone would silently drop the latter real failure.
func storageSnapshotUnsupportedOnly(err error) bool {
	if err == ErrReadSnapshotUnsupported || err == pointread.ErrKeyValueSnapshotUnsupported {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !storageSnapshotUnsupportedOnly(child) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return storageSnapshotUnsupportedOnly(wrapped.Unwrap())
	}
	return false
}

func (s *StorageReadScope) consume(value []byte) error {
	// ReleaseValue can itself fail after the callback. Own the bytes here and
	// publish to the shared cache only after View has completely succeeded.
	s.result = bytes.Clone(value)
	return nil
}

func (s *StorageReadScope) get(key []byte) ([]byte, error) {
	if s == nil || s.snapshot == nil || s.cursor == nil {
		return nil, errStorageReadScope
	}
	current := s.buffer.loadReadView().inflight
	if len(current) == 0 || current[len(current)-1] != s.active {
		return nil, errStorageReadScope
	}
	view := s.snapshot.view
	hash := layerBloomHashBytes(key)
	if value, found, deleted := lookupLayersNewest(view.inflight, key, hash); deleted {
		return nil, ErrNotFound
	} else if found {
		return value, nil
	}
	if value, found, deleted := lookupLayersNewest(view.layers, key, hash); deleted {
		return nil, ErrNotFound
	} else if found {
		return value, nil
	}
	cache := view.baseReadCache
	value, found, epoch, cacheable := cache.getAtVersion(key, s.snapshot.cacheVersion)
	if found {
		if value == nil {
			return nil, ErrNotFound
		}
		return value, nil
	}
	return s.readDurablePooledKey(key, cache, epoch, cacheable)
}

// Keep the caller's stack key out of the cursor/cache interface calls. Overlay
// and resident-cache hits never borrow a scratch key; only a durable miss pays
// for the copy. Neither the cursor nor cache may retain the pooled key after
// readDurable returns (cache admission owns its copy).
func (s *StorageReadScope) readDurablePooledKey(key []byte, cache *baseReadCache, epoch baseReadCacheEpoch, cacheable bool) ([]byte, error) {
	if len(key) > splitReadKeyStackSize {
		return s.readDurable(bytes.Clone(key), cache, epoch, cacheable)
	}
	scratch := borrowSplitReadKey()
	defer returnSplitReadKey(scratch)
	pooledKey := scratch[:len(key)]
	copy(pooledKey, key)
	return s.readDurable(pooledKey, cache, epoch, cacheable)
}

func (s *StorageReadScope) readDurable(key []byte, cache *baseReadCache, epoch baseReadCacheEpoch, cacheable bool) ([]byte, error) {
	s.result = nil
	storageReadDurable.Inc(1)
	readMetrics := pooledForegroundBaseMetricsForKey(key)
	found, err := s.cursor.View(key, s.callback)
	value := s.result
	s.result = nil
	if err == nil && found && cacheable {
		if stored, ok := cache.setSnapshotEntry(key, value, false, epoch, s.snapshot.cacheVersion); ok {
			value = stored
		}
	}
	if err == nil && !found {
		if cacheable {
			cache.setSnapshotEntry(key, nil, true, epoch, s.snapshot.cacheVersion)
		}
		err = ErrNotFound
	}
	readMetrics.recordCompleted(err)
	return value, err
}

func (s *StorageReadScope) Get(key []byte) ([]byte, error) {
	value, err := s.get(key)
	return bytes.Clone(value), err
}

func (s *StorageReadScope) Has(key []byte) (bool, error) {
	_, err := s.get(key)
	if err == ErrNotFound {
		return false, nil
	}
	return err == nil, err
}

// IsKeyNotFound does not classify a joined storage/cleanup failure as absence.
func (*StorageReadScope) IsKeyNotFound(err error) bool { return err == ErrNotFound }

func (s *StorageReadScope) GetNoCopyCachedStateKVLatest(prefix []byte, accountID common.AccountID, generation uint64, domain uint16, logicalKey []byte) ([]byte, error) {
	size := len(prefix) + common.AccountIDLength + 10 + len(logicalKey)
	if size > splitReadKeyStackSize {
		return s.get(appendStateKVLatestKey(make([]byte, 0, size), prefix, accountID, generation, domain, logicalKey))
	}
	var stack [splitReadKeyStackSize]byte
	key := appendStateKVLatestKey(stack[:0], prefix, accountID, generation, domain, logicalKey)
	return s.get(key)
}

func (s *StorageReadScope) Close() error {
	if s == nil || s.snapshot == nil {
		return nil
	}
	cursor, snapshot := s.cursor, s.snapshot
	s.cursor, s.snapshot, s.buffer, s.active = nil, nil, nil, nil
	s.result = nil
	return errors.Join(cursor.Close(), snapshot.Close())
}
