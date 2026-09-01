package blockbuffer

import (
	"bytes"
	"errors"
	"sync"
)

// errCommitmentParentReadAborted is delivered only to followers when the
// leader leaves the durable cursor path without publishing a storage result
// (for example because its cursor or callback panicked). The leader preserves
// the original panic; followers must merely be released rather than hang.
var errCommitmentParentReadAborted = errors.New("blockbuffer: commitment parent durable read aborted")

// commitmentParentReadFlights coalesces exact durable reads only inside one
// commitmentParentReadSession. That scope is important: two sessions may have
// different Pebble sequence numbers and overlay cuts even when their cache
// version and physical key happen to match.
//
// Sharding keeps unrelated commitment lanes out of one coordination mutex.
// Calls retain an owned physical-key string only while the cursor read is in
// flight. The cursor value remains zero-copy for an uncontended leader; the
// callback copies it once only when a follower has actually joined.
type commitmentParentReadFlights struct {
	shards [baseReadCacheShardCount]commitmentParentReadFlightShard
}

type commitmentParentReadFlightShard struct {
	mu    sync.Mutex
	calls map[uint64]*commitmentParentReadFlight
}

type commitmentParentReadFlight struct {
	shard *commitmentParentReadFlightShard
	next  *commitmentParentReadFlight
	hash  uint64
	key   [splitReadKeyStackSize]byte
	// oversizedKey is only used by the generic >128-byte fallback. Production
	// commitment branch keys fit in key and therefore allocate no owned string.
	oversizedKey string
	keyLen       int
	done         chan struct{}

	refs           int
	prefetchLeader bool
	callbackPassed bool
	shareable      bool
	followers      int
	found          bool
	value          []byte
	err            error
}

var commitmentParentReadFlightPool = sync.Pool{
	New: func() any { return new(commitmentParentReadFlight) },
}

func (c *commitmentParentReadFlight) matches(key []byte, hash uint64) bool {
	if c.hash != hash || c.keyLen != len(key) {
		return false
	}
	if c.oversizedKey != "" {
		return c.oversizedKey == string(key)
	}
	return bytes.Equal(c.key[:c.keyLen], key)
}

func (c *commitmentParentReadFlight) setKey(key []byte, hash uint64) {
	c.hash = hash
	c.keyLen = len(key)
	if len(key) <= len(c.key) {
		copy(c.key[:], key)
		return
	}
	c.oversizedKey = string(key)
}

// acquire returns leader=true when the caller installed the physical read.
// A follower with share=true joined before the cursor callback and can consume
// the copied result. Once such a copy exists, later followers may share it too;
// an uncontended leader remains zero-copy and late followers merely wait for
// cache publication before retrying.
func (g *commitmentParentReadFlights) acquire(key []byte, hash uint64, prefetch bool) (call *commitmentParentReadFlight, leader, share, prefetchLeader bool) {
	shard := &g.shards[hash&(baseReadCacheShardCount-1)]
	shard.mu.Lock()
	for existing := shard.calls[hash]; existing != nil; existing = existing.next {
		if existing.matches(key, hash) {
			existing.refs++
			existing.followers++
			if existing.done == nil {
				existing.done = make(chan struct{})
			}
			share = !existing.callbackPassed || existing.shareable
			prefetchLeader = existing.prefetchLeader
			shard.mu.Unlock()
			return existing, false, share, prefetchLeader
		}
	}
	if shard.calls == nil {
		shard.calls = make(map[uint64]*commitmentParentReadFlight)
	}
	call = commitmentParentReadFlightPool.Get().(*commitmentParentReadFlight)
	call.shard = shard
	call.refs = 1
	call.prefetchLeader = prefetch
	call.setKey(key, hash)
	call.next = shard.calls[hash]
	shard.calls[hash] = call
	shard.mu.Unlock()
	return call, true, false, prefetch
}

// capture seals the callback phase. Existing followers require one owned copy
// because pointread.Cursor keeps value valid only until its callback returns.
// Publishing shareable with that copy lets followers arriving before complete
// reuse it; an uncontended leader retains the zero-copy path.
func (g *commitmentParentReadFlights) capture(call *commitmentParentReadFlight, value []byte) bool {
	shard := call.shard
	shard.mu.Lock()
	call.callbackPassed = true
	shared := call.followers > 0
	if shared {
		call.value = make([]byte, len(value))
		copy(call.value, value)
		call.shareable = true
	}
	shard.mu.Unlock()
	return shared
}

// complete removes the active key before waking followers. The call storage is
// kept alive by refs until the leader and every follower finish their own cache
// admission and callback.
func (g *commitmentParentReadFlights) complete(call *commitmentParentReadFlight, found bool, err error) {
	shard := call.shard
	shard.mu.Lock()
	var previous *commitmentParentReadFlight
	for current := shard.calls[call.hash]; current != nil; current = current.next {
		if current != call {
			previous = current
			continue
		}
		if previous == nil {
			if current.next == nil {
				delete(shard.calls, call.hash)
			} else {
				shard.calls[call.hash] = current.next
			}
		} else {
			previous.next = current.next
		}
		break
	}
	call.found = found
	call.err = err
	if call.done != nil {
		close(call.done)
	}
	shard.mu.Unlock()
}

func (g *commitmentParentReadFlights) wait(call *commitmentParentReadFlight) (found bool, value []byte, err error) {
	<-call.done
	return call.found, call.value, call.err
}

func (g *commitmentParentReadFlights) release(call *commitmentParentReadFlight) {
	shard := call.shard
	shard.mu.Lock()
	call.refs--
	recycle := call.refs == 0
	if recycle {
		call.shard = nil
		call.next = nil
		call.hash = 0
		call.oversizedKey = ""
		call.keyLen = 0
		call.done = nil
		call.prefetchLeader = false
		call.callbackPassed = false
		call.shareable = false
		call.followers = 0
		call.found = false
		call.value = nil
		call.err = nil
	}
	shard.mu.Unlock()
	if recycle {
		commitmentParentReadFlightPool.Put(call)
	}
}
