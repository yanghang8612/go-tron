package blockbuffer

import (
	"bytes"
	"errors"
	"sort"
	"strings"
	"unsafe"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func seekBeforeWithBase(base ethdb.KeyValueReader, o *overlayState, prefix, upper []byte) (key, value []byte, ok bool, err error) {
	var best string
	for k, op := range o.m {
		if op.deleted || upper != nil && bytes.Compare([]byte(k), upper) >= 0 {
			continue
		}
		if !ok || k > best {
			best = k
			value = op.value
			ok = true
		}
	}
	if ok {
		key = []byte(best)
		value = bytes.Clone(value)
	}
	iter, yes := base.(ethdb.Iteratee)
	if !yes {
		return
	}
	limit := bytes.Clone(upper)
	for {
		k, v, found, e := pointread.SeekBefore(iter, prefix, limit)
		if e != nil {
			return nil, nil, false, e
		}
		if !found {
			return
		}
		if ok && bytes.Compare(key, k) >= 0 {
			return
		}
		if op, masked := o.m[string(k)]; masked {
			if !op.deleted {
				return bytes.Clone(k), bytes.Clone(op.value), true, nil
			}
			limit = k
			continue
		}
		if o.rangeDeletesString(string(k)) {
			limit = k
			continue
		}
		return k, v, true, nil
	}
}

// predecessorState retains only the best visible write and the tombstones that
// can still hide a larger older key. Layers are visited newest first; the best
// write can therefore only increase, so smaller keys never need materializing.
// Values in layer maps are immutable after publication and are copied on return.
type predecessorState struct {
	prefix, upper string
	schema        string
	bucket        byte
	bounded       bool
	best          string
	value         []byte
	found         bool
	deleted       map[string]struct{}
	ranges        []layerRangeDelete
}

func newPredecessorState(prefix, upper []byte) predecessorState {
	// The fixed header is sufficient for rawdb's namespace/domain classifier;
	// decoding it avoids copying the caller's logical prefix. Partial headers
	// and generic callers keep the original path without candidate allocations.
	headerLen := rawdb.StateKVLatestCommitmentKeySize(0)
	if len(prefix) < headerLen {
		return predecessorState{}
	}
	owner, _, _, _, typed := rawdb.DecodeStateKVLatestKey(prefix[:headerLen])
	if !typed {
		return predecessorState{}
	}
	// These strings are used only during this synchronous read. The schema is
	// explicitly cloned below before it can escape into a persistent index.
	p := predecessorState{
		prefix:  unsafe.String(unsafe.SliceData(prefix), len(prefix)),
		upper:   unsafe.String(unsafe.SliceData(upper), len(upper)),
		bounded: upper != nil,
	}
	// Common callers already supply an upper inside the prefix. Only absent
	// or out-of-prefix upper bounds need a newly constructed prefix end.
	if upper == nil || p.upper > p.prefix && !strings.HasPrefix(p.upper, p.prefix) {
		end := bytes.Clone(prefix)
		for len(end) > 0 && end[len(end)-1] == 0xff {
			end = end[:len(end)-1]
		}
		if len(end) > 0 {
			end[len(end)-1]++
			if upper == nil || bytes.Compare(end, upper) < 0 {
				p.upper, p.bounded = string(end), true
			}
		}
	}
	schemaLen := headerLen - common.AccountIDLength - 8 - 2
	p.schema = p.prefix[:schemaLen]
	p.bucket = owner.AccountID()[0]
	return p
}

func (p *predecessorState) matches(key string) bool {
	return strings.HasPrefix(key, p.prefix) && (!p.found || key > p.best) && (!p.bounded || key < p.upper)
}

func (p *predecessorState) masked(key string) bool {
	if _, deleted := p.deleted[key]; deleted {
		return true
	}
	for _, r := range p.ranges {
		if r.containsString(key) {
			return true
		}
	}
	return false
}

func (p *predecessorState) addDelete(key string) {
	if p.deleted == nil {
		p.deleted = make(map[string]struct{})
	}
	p.deleted[key] = struct{}{}
}

func (p *predecessorState) walk(l *layer) {
	if l == nil {
		return
	}
	for i := range l.shards {
		s := &l.shards[i]
		s.mu.RLock()
		index := s.prefixBucketIndex
		if index != nil && index.iteratorKeysBuilt {
			p.walkSortedLocked(s, index.iteratorKeys)
		} else if p.schema != "" && (index == nil || index.prefix == "" || index.prefix == p.schema) {
			if index != nil && index.prefix == p.schema && index.bucketSorted(p.bucket) {
				p.walkSortedLocked(s, index.buckets[p.bucket])
			} else if len(s.writes)+len(s.deletes) != 0 {
				s.mu.RUnlock()
				// Do not queue for an index build behind a flush retaining this
				// shard's read lock across I/O. The map fallback needs only RLock.
				if s.mu.TryLock() {
					index = s.prefixBucketIndex
					if index == nil {
						index = newLayerPrefixBucketIndex(strings.Clone(p.schema))
						s.prefixBucketIndex = index
					}
					if index.prefix == "" {
						index.prefix = strings.Clone(p.schema)
					}
					if index.prefix == p.schema {
						index.ensureBucketSorted(p.bucket, s.writes, s.deletes)
						p.walkSortedLocked(s, index.buckets[p.bucket])
					} else {
						p.walkMapsLocked(s)
					}
					s.mu.Unlock()
					continue
				}
				s.mu.RLock()
				p.walkMapsLocked(s)
			}
		} else {
			p.walkMapsLocked(s)
		}
		s.mu.RUnlock()
	}
	// This layer's point operations remain authoritative over its own ranges.
	// The ranges mask only candidates in older layers and the durable base.
	p.ranges = append(p.ranges, l.loadRangeDeletes()...)
}

func (p *predecessorState) walkMapsLocked(s *layerShard) {
	for key, value := range s.writes {
		if p.matches(key) && !p.masked(key) {
			p.best, p.value, p.found = key, value, true
		}
	}
	for key := range s.deletes {
		if p.matches(key) {
			if _, written := s.writes[key]; !written {
				p.addDelete(key)
			}
		}
	}
}

func (p *predecessorState) walkSortedLocked(s *layerShard, keys []string) {
	at := len(keys)
	if p.bounded {
		at = sort.Search(len(keys), func(i int) bool { return keys[i] >= p.upper })
	}
	for at--; at >= 0; at-- {
		key := keys[at]
		if key < p.prefix || p.found && key <= p.best {
			break
		}
		if !strings.HasPrefix(key, p.prefix) || p.masked(key) {
			continue
		}
		if value, written := s.writes[key]; written {
			p.best, p.value, p.found = key, value, true
			break
		}
		if _, deleted := s.deletes[key]; deleted {
			p.addDelete(key)
		}
	}
}

func (p *predecessorState) seekBase(base ethdb.KeyValueReader, prefix, upper []byte) (key, value []byte, ok bool, err error) {
	if p.found {
		key, value, ok = []byte(p.best), bytes.Clone(p.value), true
	}
	iter, yes := base.(ethdb.Iteratee)
	if !yes {
		return
	}
	limit := bytes.Clone(upper)
	for {
		k, v, found, e := pointread.SeekBefore(iter, prefix, limit)
		if e != nil {
			return nil, nil, false, e
		}
		if !found || ok && bytes.Compare(key, k) >= 0 {
			return
		}
		if p.masked(string(k)) {
			limit = k
			continue
		}
		return k, v, true, nil
	}
}

func (b *Buffer) SeekPrefixBefore(prefix, upper []byte) ([]byte, []byte, bool, error) {
	v := b.loadReadView()
	p := newPredecessorState(prefix, upper)
	if p.schema == "" {
		o := newOverlayState()
		for i := len(v.inflight) - 1; i >= 0; i-- {
			o.walk(v.inflight[i], prefix, nil)
		}
		for i := len(v.layers) - 1; i >= 0; i-- {
			o.walk(v.layers[i], prefix, nil)
		}
		return seekBeforeWithBase(b.base, o, prefix, upper)
	}
	for i := len(v.inflight) - 1; i >= 0; i-- {
		p.walk(v.inflight[i])
	}
	for i := len(v.layers) - 1; i >= 0; i-- {
		p.walk(v.layers[i])
	}
	return p.seekBase(b.base, prefix, upper)
}
func (v *LayerView) SeekPrefixBefore(prefix, upper []byte) ([]byte, []byte, bool, error) {
	view := v.b.loadReadView()
	p := newPredecessorState(prefix, upper)
	if p.schema == "" {
		o := newOverlayState()
		o.walk(v.l, prefix, nil)
		for i := len(view.layers) - 1; i >= 0; i-- {
			o.walk(view.layers[i], prefix, nil)
		}
		return seekBeforeWithBase(v.b.base, o, prefix, upper)
	}
	p.walk(v.l)
	for i := len(view.layers) - 1; i >= 0; i-- {
		p.walk(view.layers[i])
	}
	return p.seekBase(v.b.base, prefix, upper)
}
func (s *ReadSnapshot) SeekPrefixBefore(prefix, upper []byte) ([]byte, []byte, bool, error) {
	if s != nil && s.historyOnly && !rawdb.IsDirectHistoryPrefix(prefix) {
		return nil, nil, false, rawdb.ErrStateHistorySnapshotScope
	}
	if s == nil || s.closed.Load() || s.view == nil || s.base == nil {
		return nil, nil, false, errors.New("blockbuffer: closed predecessor snapshot")
	}
	p := newPredecessorState(prefix, upper)
	if p.schema == "" {
		o := newOverlayState()
		for i := len(s.view.inflight) - 1; i >= 0; i-- {
			o.walk(s.view.inflight[i], prefix, nil)
		}
		for i := len(s.view.layers) - 1; i >= 0; i-- {
			o.walk(s.view.layers[i], prefix, nil)
		}
		return seekBeforeWithBase(s.base, o, prefix, upper)
	}
	for i := len(s.view.inflight) - 1; i >= 0; i-- {
		p.walk(s.view.inflight[i])
	}
	for i := len(s.view.layers) - 1; i >= 0; i-- {
		p.walk(s.view.layers[i])
	}
	return p.seekBase(s.base, prefix, upper)
}
