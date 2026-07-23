package blockbuffer

import (
	"strings"

	"github.com/ethereum/go-ethereum/ethdb"
)

// LayerView is a read/write view bound to ONE in-flight layer. Reads resolve
// that layer's own writes/tombstones first, then the committed stack
// (newest-first), then the base reader — it deliberately IGNORES every other
// in-flight layer so there is no forward dependency on a layer the worker has
// not produced yet. Writes target the bound layer only.
//
// The async commit worker uses a LayerView (obtained via Buffer.ViewLayer) as
// the commitment store / account-KV index for block N's fold + publish tail,
// while the foreground writes the newer layer N+1 through the Buffer directly.
// Because both go through Buffer.mu and target disjoint layers, the sharded
// layer maps stay race-free.
//
// A LayerView satisfies ethdb.KeyValueReader + ethdb.KeyValueWriter +
// ethdb.Iteratee, so it drops in anywhere those interfaces (CommitmentDB,
// accountKVIndexStore) are expected.
type LayerView struct {
	b *Buffer
	l *layer
}

// ViewLayer returns a read/write view bound to the in-flight layer referenced
// by h. The handle must still be in flight; a view over a no-longer-in-flight
// layer reads/writes a detached layer (its writes never reach the committed
// stack), which the caller avoids by draining the worker before discarding.
func (b *Buffer) ViewLayer(h InflightHandle) *LayerView {
	return &LayerView{b: b, l: h.l}
}

// LayerWriter returns just the write half of a LayerView (an
// ethdb.KeyValueWriter) bound to h's layer. Convenience for tail writers that
// only Put/Delete (dynProps.Flush, WriteHeadBlockHash, …).
func (b *Buffer) LayerWriter(h InflightHandle) ethdb.KeyValueWriter {
	return &LayerView{b: b, l: h.l}
}

// putInto writes (k,v) into a specific layer under the key's shard lock. Used
// by the layer-bound writer so the worker can target an older in-flight layer
// (concurrently with the foreground writing the newest one — disjoint layers,
// disjoint locks).
func (b *Buffer) putInto(l *layer, key, value []byte) {
	k := string(key)
	b.putIntoString(l, k, value)
}

// joinKeyParts constructs the immutable map key in one allocation. Building an
// intermediate []byte and then converting it to string would allocate twice.
func joinKeyParts(first, second []byte) string {
	var key strings.Builder
	key.Grow(len(first) + len(second))
	_, _ = key.Write(first)
	_, _ = key.Write(second)
	return key.String()
}

func (b *Buffer) putIntoKeyParts(l *layer, first, second, value []byte) {
	b.putIntoString(l, joinKeyParts(first, second), value)
}

func (b *Buffer) putIntoString(l *layer, key string, value []byte) {
	v := append([]byte(nil), value...)
	s := l.shardForString(key)
	s.mu.Lock()
	delete(s.deletes, key)
	if s.writes == nil {
		s.writes = make(map[string][]byte)
	}
	s.writes[key] = v
	s.mu.Unlock()
}

// deleteInto tombstones key in a specific layer under the key's shard lock.
func (b *Buffer) deleteInto(l *layer, key []byte) {
	b.deleteIntoString(l, string(key))
}

func (b *Buffer) deleteIntoKeyParts(l *layer, first, second []byte) {
	b.deleteIntoString(l, joinKeyParts(first, second))
}

func (b *Buffer) deleteIntoString(l *layer, key string) {
	s := l.shardForString(key)
	s.mu.Lock()
	delete(s.writes, key)
	if s.deletes == nil {
		s.deletes = make(map[string]struct{})
	}
	s.deletes[key] = struct{}{}
	s.mu.Unlock()
}

func (v *LayerView) Put(key, value []byte) error {
	v.b.putInto(v.l, key, value)
	return nil
}

func (v *LayerView) Delete(key []byte) error {
	v.b.deleteInto(v.l, key)
	return nil
}

// PutKeyParts implements rawdb's optional split-key writer path. It is public
// only so a structural interface in rawdb can discover it without introducing
// a package dependency; callers should otherwise use Put.
func (v *LayerView) PutKeyParts(first, second, value []byte) error {
	v.b.putIntoKeyParts(v.l, first, second, value)
	return nil
}

// DeleteKeyParts is the delete counterpart of PutKeyParts.
func (v *LayerView) DeleteKeyParts(first, second []byte) error {
	v.b.deleteIntoKeyParts(v.l, first, second)
	return nil
}

// Get resolves key over [bound layer, committed stack newest-first, base].
// b.mu.RLock keeps the committed slice stable; each layer's matching map shard
// (including the bound in-flight layer, which the worker writes via putInto) is
// read under its own shard lock via lookup.
func (v *LayerView) Get(key []byte) ([]byte, error) {
	b := v.b
	b.mu.RLock()
	val, found, tomb := v.l.lookup(key)
	if tomb {
		b.mu.RUnlock()
		return nil, ErrNotFound
	}
	if found {
		out := append([]byte(nil), val...)
		b.mu.RUnlock()
		return out, nil
	}
	for i := len(b.layers) - 1; i >= 0; i-- {
		val, found, tomb := b.layers[i].lookup(key)
		if tomb {
			b.mu.RUnlock()
			return nil, ErrNotFound
		}
		if found {
			out := append([]byte(nil), val...)
			b.mu.RUnlock()
			return out, nil
		}
	}
	b.mu.RUnlock()
	if b.base == nil {
		return nil, ErrNotFound
	}
	return b.base.Get(key)
}

// GetNoCopy is Get without the defensive value copy for hits in the bound or
// committed layers. It deliberately has the same visibility as Get — the bound
// layer first, then committed layers newest-first, never another in-flight
// layer — and falls back to the base reader unchanged.
//
// The returned slice aliases immutable-by-replacement layer storage and must
// not be mutated. Replacement never changes the old backing bytes, so the
// commitment fold can borrow decoded leaf-key fields until its synchronous
// descent finishes. Implementing this optional rawdb fast-path on LayerView
// matters for async commit, where every fold is bound to a specific in-flight
// layer rather than reading through Buffer.GetNoCopy directly.
func (v *LayerView) GetNoCopy(key []byte) ([]byte, error) {
	return v.getNoCopy(key, false)
}

// GetNoCopyCached is GetNoCopy plus the Buffer's bounded durable-base cache.
// It is consumed by rawdb flat-latest and commitment branch reads; the bound
// and committed overlays still take precedence and are never inserted into the
// base cache.
func (v *LayerView) GetNoCopyCached(key []byte) ([]byte, error) {
	return v.getNoCopy(key, true)
}

func (v *LayerView) getNoCopy(key []byte, cacheBase bool) ([]byte, error) {
	b := v.b
	b.mu.RLock()
	val, found, tomb := v.l.lookup(key)
	if tomb {
		b.mu.RUnlock()
		return nil, ErrNotFound
	}
	if found {
		b.mu.RUnlock()
		return val, nil
	}
	for i := len(b.layers) - 1; i >= 0; i-- {
		val, found, tomb := b.layers[i].lookup(key)
		if tomb {
			b.mu.RUnlock()
			return nil, ErrNotFound
		}
		if found {
			b.mu.RUnlock()
			return val, nil
		}
	}
	cache := b.baseReadCache
	b.mu.RUnlock()
	if b.base == nil {
		return nil, ErrNotFound
	}
	var cacheEpoch uint64
	if cacheBase && cache != nil {
		if value, ok, epoch := cache.getWithEpoch(key); ok {
			return value, nil
		} else {
			cacheEpoch = epoch
		}
	}
	if !cacheBase || cache == nil {
		return b.base.Get(key)
	}
	return readBaseIntoCache(b.base, cache, key, cacheEpoch)
}

// Has reports existence over [bound layer, committed stack, base].
func (v *LayerView) Has(key []byte) (bool, error) {
	b := v.b
	b.mu.RLock()
	if _, found, tomb := v.l.lookup(key); tomb {
		b.mu.RUnlock()
		return false, nil
	} else if found {
		b.mu.RUnlock()
		return true, nil
	}
	for i := len(b.layers) - 1; i >= 0; i-- {
		_, found, tomb := b.layers[i].lookup(key)
		if tomb {
			b.mu.RUnlock()
			return false, nil
		}
		if found {
			b.mu.RUnlock()
			return true, nil
		}
	}
	b.mu.RUnlock()
	if b.base == nil {
		return false, nil
	}
	return b.base.Has(key)
}

// NewIterator iterates [bound layer, committed stack newest-first, base],
// skipping all other in-flight layers. Reuses the Buffer's overlay+base merge.
func (v *LayerView) NewIterator(prefix, start []byte) ethdb.Iterator {
	b := v.b
	b.mu.RLock()
	overlay := newOverlayState()
	overlay.walk(v.l, prefix, start)
	for i := len(b.layers) - 1; i >= 0; i-- {
		overlay.walk(b.layers[i], prefix, start)
	}
	b.mu.RUnlock()
	return b.finishIterator(overlay, prefix, start)
}
