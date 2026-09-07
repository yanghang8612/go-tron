package state

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/ethereum/go-ethereum/metrics"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/statecodec"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

const (
	legacyDelegationCacheBytes = 32 << 20
	legacyDelegationCacheRows  = 1024
	legacyDelegationSetMinimum = 16
)

var (
	legacyDelegationHits     = metrics.NewRegisteredCounter("state/delegation_legacy/cache_hits", nil)
	legacyDelegationMisses   = metrics.NewRegisteredCounter("state/delegation_legacy/cache_misses", nil)
	legacyDelegationBypasses = metrics.NewRegisteredCounter("state/delegation_legacy/cache_bypasses", nil)
	legacyDelegationNoops    = metrics.NewRegisteredCounter("state/delegation_legacy/unchanged_sides", nil)
	legacyDelegationChanges  = metrics.NewRegisteredCounter("state/delegation_legacy/changed_sides", nil)
	legacyDelegationDecoded  = metrics.NewRegisteredCounter("state/delegation_legacy/decoded_addresses", nil)
)

// legacyDelegationCache is private to the system stateObject. Encoded KV rows
// and the existing journal remain authoritative; this cache never owns a dirty
// state version. Every generic mutation invalidates the corresponding entry.
// Public readers and StateDB copies never borrow these mutable protobufs.
type legacyDelegationCache struct {
	entries    map[string]*legacyDelegationEntry
	newest     *legacyDelegationEntry
	oldest     *legacyDelegationEntry
	bytes      int
	generation uint64
}

type legacyDelegationEntry struct {
	key            string
	record         *corepb.DelegatedResourceAccountIndex
	fromSet, toSet map[string]struct{}
	charge         int
	newer, older   *legacyDelegationEntry
}

func (c *legacyDelegationCache) remove(e *legacyDelegationEntry) {
	if e == nil || c.entries[e.key] != e {
		return
	}
	if e.newer != nil {
		e.newer.older = e.older
	} else {
		c.newest = e.older
	}
	if e.older != nil {
		e.older.newer = e.newer
	} else {
		c.oldest = e.newer
	}
	delete(c.entries, e.key)
	c.bytes -= e.charge
	e.newer, e.older = nil, nil
}

func (c *legacyDelegationCache) get(key string) *legacyDelegationEntry {
	e := c.entries[key]
	if e != nil && e != c.newest {
		c.remove(e)
		c.insert(e)
	}
	return e
}

func (c *legacyDelegationCache) insert(e *legacyDelegationEntry) {
	e.newer, e.older = nil, c.newest
	if c.newest != nil {
		c.newest.newer = e
	} else {
		c.oldest = e
	}
	c.newest = e
	c.entries[e.key] = e
	c.bytes += e.charge
}

func (c *legacyDelegationCache) admit(e *legacyDelegationEntry) {
	c.remove(c.entries[e.key])
	if e.charge > legacyDelegationCacheBytes {
		legacyDelegationBypasses.Inc(1)
		return
	}
	for c.oldest != nil && (c.bytes+e.charge > legacyDelegationCacheBytes || len(c.entries) >= legacyDelegationCacheRows) {
		c.remove(c.oldest)
	}
	c.insert(e)
}

func (s *stateObject) invalidateLegacyDelegation(mapKey string) {
	if s.legacyDelegation == nil || len(mapKey) < 2 || kvdomains.KVDomain(binary.BigEndian.Uint16([]byte(mapKey[:2]))) != kvdomains.SystemDelegation {
		return
	}
	s.legacyDelegation.remove(s.legacyDelegation.entries[mapKey[2:]])
}

func (s *StateDB) clearLegacyDelegationCache() {
	if obj := s.stateObjects[tcommon.SystemAccountAddress]; obj != nil {
		obj.legacyDelegation = nil
	}
}

func (s *StateDB) legacyDelegationCacheFor(obj *stateObject) *legacyDelegationCache {
	// A transaction-versioned reader can change the logical base independently
	// of canonical KV writes. Its consumers use the uncached path exclusively.
	if obj == nil || obj.deleted || s.transactionVersionedReader != nil {
		return nil
	}
	if obj.legacyDelegation == nil || obj.legacyDelegation.generation != obj.accountKVGeneration {
		obj.legacyDelegation = &legacyDelegationCache{
			entries: make(map[string]*legacyDelegationEntry), generation: obj.accountKVGeneration,
		}
	}
	return obj.legacyDelegation
}

func newLegacyDelegationEntry(key []byte, rec *corepb.DelegatedResourceAccountIndex) *legacyDelegationEntry {
	e := &legacyDelegationEntry{key: string(key), record: rec}
	// Conservative accounting includes message/entry/cache-map overhead, the
	// decoder's complete byte/header arenas, and separately owned membership
	// keys plus map buckets. Oversized rows do not allocate membership tables.
	e.charge = 512 + len(key) + len(rec.Account) + len(rec.ProtoReflect().GetUnknown())
	for _, list := range [][][]byte{rec.FromAccounts, rec.ToAccounts} {
		e.charge += cap(list) * 24
		for _, addr := range list {
			e.charge += len(addr)
		}
		if len(list) >= legacyDelegationSetMinimum {
			for _, addr := range list {
				e.charge += 96 + len(addr)
			}
		}
	}
	if e.charge <= legacyDelegationCacheBytes {
		e.fromSet = legacyDelegationMembership(rec.FromAccounts)
		e.toSet = legacyDelegationMembership(rec.ToAccounts)
	}
	return e
}

func legacyDelegationMembership(list [][]byte) map[string]struct{} {
	if len(list) < legacyDelegationSetMinimum {
		return nil
	}
	set := make(map[string]struct{}, len(list))
	for _, addr := range list {
		set[string(addr)] = struct{}{}
	}
	return set
}

func legacyDelegationContains(list [][]byte, set map[string]struct{}, peer []byte) bool {
	if set != nil {
		_, ok := set[string(peer)]
		return ok
	}
	for _, addr := range list {
		if bytes.Equal(addr, peer) {
			return true
		}
	}
	return false
}

// loadLegacyDelegation returns a private decoded row, never borrowed raw state
// bytes. A hit still records precisely the generic point/existence dependency.
func (s *StateDB) loadLegacyDelegation(account []byte) (*legacyDelegationEntry, bool, error) {
	key := rawdb.DrAccountIndexLegacyStateKey(account)
	s.recordAccountKVRead(tcommon.SystemAccountAddress, kvdomains.SystemDelegation, key)
	obj := s.getStateObjectForField(tcommon.SystemAccountAddress, TransactionAccountFieldExistence)
	cache := s.legacyDelegationCacheFor(obj)
	if cache != nil {
		if e := cache.get(string(key)); e != nil {
			legacyDelegationHits.Inc(1)
			return e, true, nil
		}
	}
	legacyDelegationMisses.Inc(1)
	data, exists, err := s.readSystemDelegationWithError(key)
	if err != nil {
		return nil, exists, err
	}
	if !exists {
		return newLegacyDelegationEntry(key, &corepb.DelegatedResourceAccountIndex{Account: bytes.Clone(account)}), false, nil
	}
	rec, err := statecodec.UnmarshalDelegationIndex(data)
	if err != nil {
		return nil, true, fmt.Errorf("decode dr account index legacy: %w", err)
	}
	legacyDelegationDecoded.Inc(int64(len(rec.FromAccounts) + len(rec.ToAccounts)))
	e := newLegacyDelegationEntry(key, rec)
	if cache != nil {
		cache.admit(e)
	}
	return e, true, nil
}

func (s *StateDB) detachLegacyDelegation(e *legacyDelegationEntry) {
	if obj := s.stateObjects[tcommon.SystemAccountAddress]; obj != nil && obj.legacyDelegation != nil {
		obj.legacyDelegation.remove(e)
	}
}

// changeLegacyDelegation detaches before modifying a private decoded row. Undo
// and temporal history retain only immutable encoded KV, so no cache undo or
// shared mutable snapshot is necessary. A failed write leaves no stale entry.
func (s *StateDB) changeLegacyDelegation(e *legacyDelegationEntry, peer []byte, incoming, remove bool) error {
	s.detachLegacyDelegation(e)
	list, set := &e.record.ToAccounts, e.toSet
	if incoming {
		list, set = &e.record.FromAccounts, e.fromSet
	}
	if remove {
		old := *list
		*list = removeDelegationAccount(old, peer)
		clear(old[len(*list):])
		delete(set, string(peer))
	} else {
		oldCap := cap(*list)
		ownedPeer := bytes.Clone(peer)
		*list = append(*list, ownedPeer)
		e.charge += cap(ownedPeer)
		if cap(*list) != oldCap {
			// The other list may still pin the old shared header arena. Retain
			// its charge and add the whole new allocation, not just the delta.
			e.charge += cap(*list) * 24
		}
		if set != nil {
			set[string(peer)] = struct{}{}
			e.charge += 96 + len(peer)
		} else if len(*list) >= legacyDelegationSetMinimum && e.charge <= legacyDelegationCacheBytes {
			for _, addr := range *list {
				e.charge += 96 + len(addr)
			}
			if e.charge <= legacyDelegationCacheBytes {
				if incoming {
					e.fromSet = legacyDelegationMembership(*list)
				} else {
					e.toSet = legacyDelegationMembership(*list)
				}
			}
		}
	}
	data, err := statecodec.MarshalDelegationIndex(e.record)
	if err != nil {
		return fmt.Errorf("dr account index: marshal legacy: %w", err)
	}
	if err := s.writeSystemDelegation([]byte(e.key), data); err != nil {
		return err
	}
	legacyDelegationChanges.Inc(1)
	obj := s.getStateObjectWithoutAccess(tcommon.SystemAccountAddress)
	if cache := s.legacyDelegationCacheFor(obj); cache != nil {
		cache.admit(e)
	}
	return nil
}
