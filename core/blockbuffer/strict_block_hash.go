package blockbuffer

import (
	"bytes"
	"sync"

	"github.com/tronprotocol/go-tron/common"
)

// Every strict lookup still reads the authoritative current raw row. This
// adapter only removes Buffer.Get's defensive overlay copy; GetNoCopy values
// are immutable, and the memo makes its own bounded copy before retaining one.
// Keeping Has unchanged preserves ReadBlockRawStrict's presence/error behavior.
type strictBlockRawReader struct{ *Buffer }

func (r strictBlockRawReader) Get(key []byte) ([]byte, error) { return r.GetNoCopy(key) }

const (
	strictBlockHashSlots       = 256
	strictBlockHashMaxRowBytes = 1 << 20
	strictBlockHashMaxBytes    = 8 << 20
)

type strictBlockHashEntry struct {
	number uint64
	raw    []byte
	hash   common.Hash
	valid  bool
}

// A successful full decode authenticates all bytes, including transaction
// bodies. Header-only comparisons could hide a malformed replacement body.
// Entries are bounded by both count and owned raw bytes; no protobuf graphs or
// input backing allocations survive a miss. Out-of-order concurrent fills are
// harmless because every hit compares the entire freshly read row again.
type strictBlockHashCache struct {
	mu      sync.Mutex
	entries [strictBlockHashSlots]strictBlockHashEntry
	bytes   int
	evict   int
}

func (c *strictBlockHashCache) lookup(number uint64, raw []byte) (common.Hash, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := &c.entries[number%strictBlockHashSlots]
	if e.valid && e.number == number && bytes.Equal(e.raw, raw) {
		return e.hash, true
	}
	return common.Hash{}, false
}

func (c *strictBlockHashCache) remember(number uint64, raw []byte, hash common.Hash) {
	if len(raw) > strictBlockHashMaxRowBytes {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e := &c.entries[number%strictBlockHashSlots]
	if e.valid && e.number == number && bytes.Equal(e.raw, raw) {
		return
	}
	c.bytes -= len(e.raw)
	*e = strictBlockHashEntry{}
	for c.bytes+len(raw) > strictBlockHashMaxBytes {
		old := &c.entries[c.evict]
		c.bytes -= len(old.raw)
		*old = strictBlockHashEntry{}
		c.evict = (c.evict + 1) % strictBlockHashSlots
	}
	owned := make([]byte, len(raw))
	copy(owned, raw)
	*e = strictBlockHashEntry{number: number, raw: owned, hash: hash, valid: true}
	c.bytes += len(owned)
}
