package blockbuffer

import (
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
// Because both go through Buffer.mu and target disjoint layers, the per-layer
// maps stay race-free.
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

// putInto writes (k,v) into a specific layer under that layer's lock. Used by
// the layer-bound writer so the worker can target an older in-flight layer
// (concurrently with the foreground writing the newest one — disjoint layers,
// disjoint locks).
func (b *Buffer) putInto(l *layer, key, value []byte) {
	k := string(key)
	v := append([]byte(nil), value...)
	l.mu.Lock()
	delete(l.deletes, k)
	l.writes[k] = v
	l.mu.Unlock()
}

// deleteInto tombstones key in a specific layer under that layer's lock.
func (b *Buffer) deleteInto(l *layer, key []byte) {
	k := string(key)
	l.mu.Lock()
	delete(l.writes, k)
	l.deletes[k] = struct{}{}
	l.mu.Unlock()
}

func (v *LayerView) Put(key, value []byte) error {
	v.b.putInto(v.l, key, value)
	return nil
}

func (v *LayerView) Delete(key []byte) error {
	v.b.deleteInto(v.l, key)
	return nil
}

// Get resolves key over [bound layer, committed stack newest-first, base].
// b.mu.RLock keeps the committed slice stable; each layer's map (including the
// bound in-flight layer, which the worker writes via putInto) is read under its
// own lock via lookup.
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
