package blockbuffer

import "sync/atomic"

const (
	layerBloomBitsPerKey = 12
	layerBloomMinBits    = 64
)

// layerBloom is an immutable-size, atomically additive filter for one
// committed layer. False positives fall through to the authoritative sharded
// maps; false negatives are forbidden.
type layerBloom struct {
	words    []atomic.Uint64
	wordMask uint64
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

func layerBloomHashBytes(key []byte) uint64 {
	const (
		offset = uint64(14695981039346656037)
		prime  = uint64(1099511628211)
	)
	h := offset
	for _, c := range key {
		h ^= uint64(c)
		h *= prime
	}
	return h
}

func layerBloomHashString(key string) uint64 {
	const (
		offset = uint64(14695981039346656037)
		prime  = uint64(1099511628211)
	)
	h := offset
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= prime
	}
	return h
}

func layerBloomLocation(hash, wordMask uint64) (uint64, uint64) {
	// A blocked Bloom keeps both probes in one 64-bit word. One atomic load can
	// therefore test both bits, while 12 bits/key retains the same approximate
	// false-positive rate as two independent probes across the full bitset.
	word := hash & wordMask
	mixed := hash
	mixed ^= mixed >> 33
	mixed *= 0xff51afd7ed558ccd
	mixed ^= mixed >> 33
	mixed *= 0xc4ceb9fe1a85ec53
	mixed ^= mixed >> 33
	firstBit := mixed & 63
	secondBit := (mixed >> 6) & 63
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
	if bloom := l.bloom.Load(); bloom != nil {
		bloom.addHash(layerBloomHashString(key))
	}
}
