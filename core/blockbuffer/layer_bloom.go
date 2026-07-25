package blockbuffer

import (
	"hash/maphash"
	"sync/atomic"
)

const (
	layerBloomBitsPerKey = 12
	layerBloomMinBits    = 64
	// Segments turn a committed-stack miss from one atomic Bloom probe per layer
	// into one probe per complete group. Mainnet's solidification line normally
	// trails the head by about 19 blocks, so eight yields two useful groups plus
	// a short tail; a 32-layer group never formed in production.
	layerBloomSegmentSize = 8
)

// layerBloom is an immutable-size, atomically additive filter for one
// committed layer. False positives fall through to the authoritative sharded
// maps; false negatives are forbidden.
type layerBloom struct {
	words    []atomic.Uint64
	wordMask uint64
}

// layerBloomSegment is shared by one consecutive, complete group of committed
// layers. first/last validate that every member is still present in a read
// view before lookupLayersNewest skips the group. A reorg or prefix flush that
// removes any member therefore degrades to ordinary per-layer probes instead
// of risking a false negative.
type layerBloomSegment struct {
	first *layer
	last  *layer
	size  int
	bloom *layerBloom
	ready atomic.Bool
}

func newLayerBloom(keys int) *layerBloom {
	if keys < 0 {
		return nil
	}
	target := keys * layerBloomBitsPerKey
	bits := layerBloomMinBits
	for bits < target {
		bits <<= 1
	}
	return &layerBloom{
		words:    make([]atomic.Uint64, bits/64),
		wordMask: uint64(bits/64 - 1),
	}
}

// The Bloom is process-local and rebuilt from authoritative layer maps, so its
// hash does not need a stable cross-process encoding. maphash uses the runtime's
// hardware-accelerated string/byte hash while one shared seed preserves exact
// byte/string parity for build, late-write and lookup paths.
var layerBloomHashSeed = maphash.MakeSeed()

func layerBloomHashBytes(key []byte) uint64 { return maphash.Bytes(layerBloomHashSeed, key) }

func layerBloomHashString(key string) uint64 { return maphash.String(layerBloomHashSeed, key) }

func layerBloomLocation(hash, wordMask uint64) (uint64, uint64) {
	// A blocked Bloom keeps both probes in one 64-bit word. One atomic load can
	// therefore test both bits, while 12 bits/key retains the same approximate
	// false-positive rate as two independent probes across the full bitset.
	// maphash already returns uniformly mixed 64-bit output: use disjoint high
	// bit fields for the two probes and low bits for the word, avoiding a second
	// Murmur finalizer on every lookup and insertion.
	word := hash & wordMask
	firstBit := (hash >> 32) & 63
	secondBit := (hash >> 38) & 63
	if secondBit == firstBit {
		secondBit = (secondBit + 1) & 63
	}
	return word, uint64(1)<<firstBit | uint64(1)<<secondBit
}

func (b *layerBloom) addHash(hash uint64) {
	if b == nil {
		return
	}
	word, mask := layerBloomLocation(hash, b.wordMask)
	b.words[word].Or(mask)
}

func (b *layerBloom) mayContainHash(hash uint64) bool {
	if b == nil {
		return true
	}
	word, mask := layerBloomLocation(hash, b.wordMask)
	return b.words[word].Load()&mask == mask
}

// buildBloom freezes the filter size at promotion time. It holds every shard
// lock while publishing the pointer so a late batch write either contributes
// to this initial scan or observes the published filter and atomically adds its
// key before making the map mutation visible.
func (l *layer) buildBloom() {
	if l == nil || l.bloom.Load() != nil {
		return
	}
	for i := range l.shards {
		l.shards[i].mu.Lock()
	}
	defer func() {
		for i := len(l.shards) - 1; i >= 0; i-- {
			l.shards[i].mu.Unlock()
		}
	}()

	keys := 0
	for i := range l.shards {
		keys += len(l.shards[i].writes) + len(l.shards[i].deletes)
	}
	bloom := newLayerBloom(keys)
	for i := range l.shards {
		for key := range l.shards[i].writes {
			bloom.addHash(layerBloomHashString(key))
		}
		for key := range l.shards[i].deletes {
			bloom.addHash(layerBloomHashString(key))
		}
	}
	l.bloom.Store(bloom)
}

// addBloomString must run while the key's layer shard is write-locked and
// before its map mutation. In-flight layers have no filter yet; committed
// layers can receive delayed batch writes after promotion.
func (l *layer) addBloomString(key string) {
	if l == nil {
		return
	}
	bloom := l.bloom.Load()
	segment := l.segment.Load()
	if bloom == nil && segment == nil {
		return
	}
	hash := layerBloomHashString(key)
	if bloom != nil {
		bloom.addHash(hash)
	}
	if segment != nil {
		segment.bloom.addHash(hash)
	}
}

// buildNewestLayerBloomSegmentLocked seals the newest full run of unsegmented
// committed layers. Caller holds Buffer.mu, so the topology cannot change while
// the group is selected; buildLayerBloomSegment separately locks every member's
// maps against delayed batch writes.
func (b *Buffer) buildNewestLayerBloomSegmentLocked() {
	end := len(b.layers)
	start := end
	for start > 0 && end-start < layerBloomSegmentSize && b.layers[start-1].segment.Load() == nil {
		start--
	}
	if end-start == layerBloomSegmentSize {
		buildLayerBloomSegment(b.layers[start:end])
	}
}

func buildLayerBloomSegment(layers []*layer) {
	if len(layers) != layerBloomSegmentSize {
		return
	}
	// Count under one shard read lock at a time. The estimate may be slightly
	// stale by allocation time, but late additions only increase saturation;
	// they cannot create a false negative.
	keys := 0
	for _, l := range layers {
		for shard := range l.shards {
			s := &l.shards[shard]
			s.mu.RLock()
			keys += len(s.writes) + len(s.deletes)
			s.mu.RUnlock()
		}
	}
	bloom := newLayerBloom(keys)
	segment := &layerBloomSegment{
		first: layers[0],
		last:  layers[len(layers)-1],
		size:  len(layers),
		bloom: bloom,
	}
	// Publish membership before scanning, but leave ready=false. A delayed
	// writer mutates its layer under the same shard lock used below: if it wins
	// before publication/scan, the scan sees the key; if it wins afterwards,
	// addBloomString updates this segment. Readers cannot skip until ready=true.
	for _, l := range layers {
		l.segment.Store(segment)
	}
	for _, l := range layers {
		for shard := range l.shards {
			s := &l.shards[shard]
			s.mu.RLock()
			for key := range s.writes {
				bloom.addHash(layerBloomHashString(key))
			}
			for key := range s.deletes {
				bloom.addHash(layerBloomHashString(key))
			}
			s.mu.RUnlock()
		}
	}
	segment.ready.Store(true)
}
