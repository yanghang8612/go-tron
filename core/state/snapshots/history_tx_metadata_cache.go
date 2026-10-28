package snapshots

import (
	"bytes"
	"sync"
)

const historyTxMetadataBytes = 8 << 20
const historyTxScratchBytes = 2 << 20
const historyTxScratchCount = 4

// Keys select a candidate parse; exact equality with the CURRENT complete
// bytes authenticates reuse. CRC collisions only cause misses. Include every
// external bound used by the corresponding successful parser.
type historyTxDirectoryKey struct {
	footer            [64]byte
	records, from, to uint64
}
type historyTxRunKey struct {
	entry                      [32]byte
	index, records, to, nextTx uint64
}

// Values are immutable after publication. No reader, context or mutable table
// state survives through this cache. FIFO and map metadata have a separate
// entry bound; bytes accounts for raw bytes plus parsed backing arrays.
type historyTxMetadataValue[V any] struct {
	raw  []byte
	rows []V
}

type historyTxMetadataCache[K comparable, V any] struct {
	mu                             sync.Mutex
	values                         map[K]historyTxMetadataValue[V]
	fifo                           []K
	next, bytes                    int
	maxBytes, maxEntries, rowBytes int
}

var sharedHistoryTxDirectories = &historyTxMetadataCache[historyTxDirectoryKey, historyTxPage]{maxBytes: historyTxMetadataBytes, maxEntries: 256, rowBytes: 32}
var sharedHistoryTxRuns = &historyTxMetadataCache[historyTxRunKey, historyTxRun]{maxBytes: historyTxMetadataBytes, maxEntries: 2048, rowBytes: 16}

func (c *historyTxMetadataCache[K, V]) get(key K, current []byte) ([]V, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	v, ok := c.values[key]
	c.mu.Unlock()
	if !ok || !bytes.Equal(v.raw, current) {
		return nil, false
	}
	return v.rows, true
}

// put copies the checked raw bytes and takes ownership of a freshly validated
// exact-capacity array. Neither parser nor callers modify published entries.
func (c *historyTxMetadataCache[K, V]) put(key K, raw []byte, value []V) {
	if c == nil || c.maxBytes <= 0 || c.maxEntries <= 0 || c.rowBytes <= 0 {
		return
	}
	cost := len(raw) + cap(value)*c.rowBytes
	if cost > c.maxBytes {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.values[key]; ok {
		return
	}
	if c.values == nil {
		c.values = make(map[K]historyTxMetadataValue[V])
		c.fifo = make([]K, c.maxEntries)
	}
	// Remove the oldest live entries. The compact ring never grows beyond the
	// entry limit and never retains evicted keys in a growing prefix.
	for len(c.values) > 0 && (c.bytes+cost > c.maxBytes || len(c.values) >= c.maxEntries) {
		old := c.fifo[c.next]
		oldValue := c.values[old]
		c.bytes -= len(oldValue.raw) + cap(oldValue.rows)*c.rowBytes
		delete(c.values, old)
		var zero K
		c.fifo[c.next] = zero
		c.next = (c.next + 1) % len(c.fifo)
	}
	at := (c.next + len(c.values)) % len(c.fifo)
	c.fifo[at] = key
	owned := make([]byte, len(raw))
	copy(owned, raw)
	c.values[key] = historyTxMetadataValue[V]{raw: owned, rows: value}
	c.bytes += cost
}

type historyTxScratchPool struct {
	mu    sync.Mutex
	idle  [][]byte
	bytes int
}

var sharedHistoryTxScratch historyTxScratchPool

func (p *historyTxScratchPool) take(n int) []byte {
	if n == 0 {
		return nil
	}
	p.mu.Lock()
	for i, b := range p.idle {
		if cap(b) >= n {
			last := len(p.idle) - 1
			p.idle[i] = p.idle[last]
			p.idle[last] = nil
			p.idle = p.idle[:last]
			p.bytes -= cap(b)
			p.mu.Unlock()
			return b[:n]
		}
	}
	p.mu.Unlock()
	return make([]byte, n)
}
func (p *historyTxScratchPool) release(b []byte) {
	if cap(b) == 0 || cap(b) > historyTxDirectoryLimit {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.idle) < historyTxScratchCount && p.bytes+cap(b) <= historyTxScratchBytes {
		p.idle = append(p.idle, b)
		p.bytes += cap(b)
	}
}
