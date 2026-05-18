package dpos

import (
	"container/list"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tronprotocol/go-tron/common"
	gtronlog "github.com/tronprotocol/go-tron/common/log"
	"github.com/tronprotocol/go-tron/core/types"
)

var log = gtronlog.NewModule("consensus/dpos")

// witnessHexKey formats a witness address as lowercase hex without "0x"
// prefix. Mirrors java-tron `ByteArray.toHexString(witnessAddress)` used as
// the map key in `WitnessProductBlockService.validWitnessProductTwoBlock`.
func witnessHexKey(addr common.Address) string {
	return hex.EncodeToString(addr.Bytes())
}

// cheatInfoString renders CheatWitnessInfo in a layout close to java-tron's
// CheatWitnessInfo.toString output (see lines 109-117 of the Java source).
// We don't need byte-perfect parity here — the field is exposed via NodeInfo
// as a free-form string for operator viewing.
func cheatInfoString(c *CheatWitnessInfo) string {
	var hashes strings.Builder
	hashes.WriteByte('[')
	for i, h := range c.BlockSet {
		if i > 0 {
			hashes.WriteString(", ")
		}
		fmt.Fprintf(&hashes, "%x", h)
	}
	hashes.WriteByte(']')
	return fmt.Sprintf("{times=%d, time=%d, latestBlockNum=%d, blockCapsuleSet=%s}",
		c.Times, c.Time, c.LatestBlockNum, hashes.String())
}

// HistoryBlockCacheSize bounds the recent-blocks cache used by the cheat
// detector. Mirrors java-tron's `WitnessProductBlockService.historyBlockCapsuleCache`
// which is built with Guava `CacheBuilder.newBuilder().initialCapacity(200).maximumSize(200)`
// (see java-tron `framework/src/main/java/org/tron/core/services/WitnessProductBlockService.java:20-21`).
const HistoryBlockCacheSize = 200

// cachedBlock is the minimal subset of a Block we need to detect duplicates:
// the witness address that signed it and its block hash. Storing only these
// two fields keeps the cache cheap regardless of block payload size.
type cachedBlock struct {
	witness common.Address
	hash    common.Hash
}

// CheatWitnessInfo accumulates evidence of a single witness producing two or
// more distinct blocks at the same height. Mirrors java-tron's
// `WitnessProductBlockService.CheatWitnessInfo` (same file lines 51-118):
//   - Times    — number of cheat events recorded for this witness.
//   - LatestBlockNum — block number of the most recent cheat event.
//   - Time     — wall-clock millis when the most recent event was observed.
//   - BlockSet — distinct cheat blocks (by hash) seen in the most recent event.
//
// The whole struct is purely informational: it is exposed via NodeInfo for
// operator monitoring and never feeds back into consensus or witness state.
type CheatWitnessInfo struct {
	Times          int32
	LatestBlockNum uint64
	Time           int64
	BlockSet       []common.Hash
}

// String mirrors java-tron's CheatWitnessInfo.toString format closely enough
// to be useful in logs.
func (c *CheatWitnessInfo) String() string {
	return cheatInfoString(c)
}

// CheatDetector watches advertised blocks for the "same height, same witness,
// different hash" pattern that indicates a witness double-signed a slot. This
// is a port of java-tron's `WitnessProductBlockService` and follows it
// faithfully:
//
//   - Detection is in-process only. No witness state is mutated, no DP key is
//     bumped, no `Witness.is_jobs` flag is flipped, nothing is persisted.
//     Java-tron's `setIsJobs` is touched only at genesis (DposService) and in
//     the maintenance-cycle rotation (MaintenanceManager); cheat detection
//     does not touch it.
//   - The history cache is bounded at HistoryBlockCacheSize (200) entries; on
//     overflow the oldest insert is evicted. This matches Guava's
//     `maximumSize` for a cache with no read-side promotion in normal use.
//   - The cheat info map is unbounded across the process lifetime, just like
//     the java-tron `HashMap`. It is reset only on process restart.
//
// CheatDetector is safe for concurrent use.
type CheatDetector struct {
	mu sync.Mutex

	// history holds at most HistoryBlockCacheSize cached blocks keyed by
	// block number. The list is FIFO over insertion order so eviction
	// drops the oldest insert first, matching Guava's behavior under no
	// read promotion.
	history map[uint64]*list.Element // blockNum -> list element
	order   *list.List               // *historyEntry, oldest at front

	// cheatInfo is the accumulated CheatWitnessInfo per witness, keyed by
	// the witness address (lowercase hex without 0x, mirroring java-tron's
	// `ByteArray.toHexString`).
	cheatInfo map[string]*CheatWitnessInfo

	// nowMillis is overridable for tests; defaults to time.Now in UnixMilli.
	nowMillis func() int64

	// cheatEvents counts total cheat events ever recorded; useful for
	// metrics and tests that don't want to grub around in the map.
	cheatEvents atomic.Int64
}

type historyEntry struct {
	num   uint64
	block cachedBlock
}

// NewCheatDetector returns an empty detector ready for use.
func NewCheatDetector() *CheatDetector {
	return &CheatDetector{
		history:   make(map[uint64]*list.Element),
		order:     list.New(),
		cheatInfo: make(map[string]*CheatWitnessInfo),
		nowMillis: func() int64 { return time.Now().UnixMilli() },
	}
}

// CheckBlock inspects a newly accepted advertised block for the double-sign
// pattern. If a previous block at the same height has already been cached and
// its witness matches but its hash differs, the event is recorded. Otherwise
// the block is added to (or refreshes) the history cache.
//
// This is the go-tron equivalent of
// `WitnessProductBlockService.validWitnessProductTwoBlock` (java-tron
// `framework/src/main/java/org/tron/core/services/WitnessProductBlockService.java:25-45`).
// Errors / nil blocks are silently ignored, matching the java-tron try/catch.
func (d *CheatDetector) CheckBlock(block *types.Block) {
	if block == nil {
		return
	}
	num := block.Number()
	witness := block.WitnessAddress()
	hash := block.Hash()

	d.mu.Lock()
	defer d.mu.Unlock()

	if elem, ok := d.history[num]; ok {
		prev := elem.Value.(*historyEntry).block
		if prev.witness == witness && prev.hash != hash {
			d.recordCheatLocked(witness, num, prev.hash, hash)
			return
		}
		// Same num + same witness + same hash: nothing to do (it's the
		// block we already cached). Same num + different witness: also
		// nothing to do; that case is impossible on the same chain
		// because a height has exactly one scheduled witness, but if
		// two peers gossip a fork from different branches we just
		// leave the older entry alone — java-tron does the same.
		return
	}

	d.putHistoryLocked(num, cachedBlock{witness: witness, hash: hash})
}

// recordCheatLocked mutates cheat state for a confirmed double-sign. mu must
// be held. Mirrors lines 31-37 of java-tron WitnessProductBlockService:
// ensure entry exists, then `clear().setTime(...).setLatestBlockNum(num).add(b1).add(b2).increment()`.
func (d *CheatDetector) recordCheatLocked(witness common.Address, num uint64, prevHash, newHash common.Hash) {
	key := witnessHexKey(witness)
	info, ok := d.cheatInfo[key]
	if !ok {
		info = &CheatWitnessInfo{}
		d.cheatInfo[key] = info
	}
	// Mirror java-tron `clear()`: rebuild BlockSet from scratch on each
	// new event (it only ever holds the two blocks of the most recent
	// event). Times accumulates across events.
	info.BlockSet = info.BlockSet[:0]
	info.BlockSet = append(info.BlockSet, prevHash, newHash)
	info.LatestBlockNum = num
	info.Time = d.nowMillis()
	info.Times++
	d.cheatEvents.Add(1)

	// Java-tron does not log on cheat detection (the only logger.error in
	// validWitnessProductTwoBlock is on exception). We log a warn so go-tron
	// operators get a signal in stdout; this is purely informational and
	// does not affect consensus.
	//
	// Use witness[:] / hash[:] to format the underlying bytes; common.Address
	// and common.Hash both implement Stringer that returns hex, which would
	// then be hex-encoded again by %x.
	log.Warn("Witness cheat detected",
		"witness", fmt.Sprintf("%x", witness[:]),
		"number", num,
		"prevHash", fmt.Sprintf("%x", prevHash[:]),
		"newHash", fmt.Sprintf("%x", newHash[:]),
		"times", info.Times)
}

// putHistoryLocked inserts a new history entry, evicting the oldest insertion
// if we are at capacity. mu must be held.
func (d *CheatDetector) putHistoryLocked(num uint64, blk cachedBlock) {
	entry := &historyEntry{num: num, block: blk}
	elem := d.order.PushBack(entry)
	d.history[num] = elem
	for d.order.Len() > HistoryBlockCacheSize {
		front := d.order.Front()
		if front == nil {
			break
		}
		oldest := front.Value.(*historyEntry)
		d.order.Remove(front)
		delete(d.history, oldest.num)
	}
}

// QueryCheatWitnessInfo returns a snapshot of the current cheat-witness map.
// Keys are lowercase hex of the 21-byte TRON witness address (no 0x), matching
// java-tron's `ByteArray.toHexString(witnessAddress)`. Values are deep copies
// so callers may inspect them without locking.
//
// This is the go-tron equivalent of
// `WitnessProductBlockService.queryCheatWitnessInfo` (java-tron
// `framework/src/main/java/org/tron/core/services/WitnessProductBlockService.java:47-49`).
func (d *CheatDetector) QueryCheatWitnessInfo() map[string]*CheatWitnessInfo {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make(map[string]*CheatWitnessInfo, len(d.cheatInfo))
	for k, v := range d.cheatInfo {
		cp := *v
		cp.BlockSet = append([]common.Hash(nil), v.BlockSet...)
		out[k] = &cp
	}
	return out
}

// CheatEventCount returns the total number of cheat events recorded since the
// detector was created. Convenient for tests and metrics.
func (d *CheatDetector) CheatEventCount() int64 {
	return d.cheatEvents.Load()
}

// HistoryLen returns the current number of cached history blocks. Exported for
// tests; not part of the public API.
func (d *CheatDetector) HistoryLen() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.order.Len()
}

// HasHistoryAt reports whether a history entry exists for the given block
// number. Exported for tests.
func (d *CheatDetector) HasHistoryAt(num uint64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.history[num]
	return ok
}
