package state

import (
	"bytes"
	"encoding/binary"
	"strings"

	"github.com/ethereum/go-ethereum/metrics"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

const (
	legacyDelegationCacheBytes = 32 << 20
	legacyDelegationCacheRows  = 1024
	legacyDelegationSetMinimum = 16
)

var (
	legacyDelegationHits         = metrics.NewRegisteredCounter("state/delegation_legacy/cache_hits", nil)
	legacyDelegationMisses       = metrics.NewRegisteredCounter("state/delegation_legacy/cache_misses", nil)
	legacyDelegationBypasses     = metrics.NewRegisteredCounter("state/delegation_legacy/cache_bypasses", nil)
	legacyDelegationNoops        = metrics.NewRegisteredCounter("state/delegation_legacy/unchanged_sides", nil)
	legacyDelegationChanges      = metrics.NewRegisteredCounter("state/delegation_legacy/changed_sides", nil)
	legacyDelegationBaseOversize = metrics.NewRegisteredCounter("state/delegation_legacy/cache_base_oversize_loads", nil)
	legacyDelegationMapSkipped   = metrics.NewRegisteredCounter("state/delegation_legacy/optional_map_skipped", nil)
	legacyDelegationWithoutSet   = metrics.NewRegisteredCounter("state/delegation_legacy/cache_admitted_without_set", nil)
	legacyDelegationDecoded      = metrics.NewRegisteredCounter("state/delegation_legacy/decoded_addresses", nil)
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
	pages          legacyDelegationPages
	exists         bool
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
	if e.record != nil && ((len(e.record.FromAccounts) >= legacyDelegationSetMinimum && e.fromSet == nil) || (len(e.record.ToAccounts) >= legacyDelegationSetMinimum && e.toSet == nil)) {
		legacyDelegationWithoutSet.Inc(1)
	}
	c.insert(e)
}

func (s *stateObject) invalidateLegacyDelegation(mapKey string) {
	if s.legacyDelegation == nil || len(mapKey) < 2 || kvdomains.KVDomain(binary.BigEndian.Uint16([]byte(mapKey[:2]))) != kvdomains.SystemDelegation {
		return
	}
	key := []byte(mapKey[2:])
	if account, ok := rawdb.DrAccountIndexLegacyPageAccount(key); ok {
		key = rawdb.DrAccountIndexLegacyStateKey(account)
	}
	s.legacyDelegation.remove(s.legacyDelegation.entries[string(key)])
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
	return newLegacyDelegationEntryWithPages(key, rec, splitLegacyDelegationPages(rec))
}

func newLegacyDelegationEntryWithPages(key []byte, rec *corepb.DelegatedResourceAccountIndex, pages legacyDelegationPages) *legacyDelegationEntry {
	e := &legacyDelegationEntry{key: string(key), record: rec, pages: pages}
	// Include the materialized list and page header arrays, owned page byte
	// arenas (at most ten framing bytes per entry), and membership map keys.
	e.charge = 512 + len(key) + len(rec.Account) + len(rec.ProtoReflect().GetUnknown())
	for _, direction := range [][][][]byte{pages.from, pages.to} {
		e.charge += cap(direction) * 24
		for _, page := range direction {
			e.charge += cap(page)*24 + 15
		}
	}
	for _, list := range [][][]byte{rec.FromAccounts, rec.ToAccounts} {
		e.charge += cap(list) * 24
		for _, addr := range list {
			e.charge += len(addr) + 10
		}
	}
	// Membership maps are optional accelerators. A list whose owned pages fit
	// must not bypass the cache because of maps which were never allocated.
	fromCharge, fromFits := legacyDelegationMembershipCharge(rec.FromAccounts, legacyDelegationCacheBytes-e.charge)
	toCharge, toFits := legacyDelegationMembershipCharge(rec.ToAccounts, legacyDelegationCacheBytes-e.charge-fromCharge)
	if e.charge > legacyDelegationCacheBytes {
		legacyDelegationBaseOversize.Inc(1)
	} else if !fromFits || !toFits {
		legacyDelegationMapSkipped.Inc(1)
	}
	if fromFits && toFits {
		e.charge += fromCharge + toCharge
		e.fromSet = legacyDelegationMembership(rec.FromAccounts)
		e.toSet = legacyDelegationMembership(rec.ToAccounts)
	}
	return e
}

// Return the conservative map/owned-key-arena charge only if the complete
// optional index fits. Do not charge failed construction attempts; membership
// without a map uses the original bytewise scan over the owned list.
func legacyDelegationMembershipCharge(list [][]byte, budget int) (charge int, fits bool) {
	if budget < 0 {
		return 0, false
	}
	if len(list) < legacyDelegationSetMinimum {
		return 0, true
	}
	if len(list) > budget/96 {
		return 0, false
	}
	for _, addr := range list {
		if budget-charge < 96 || len(addr) > budget-charge-96 {
			return 0, false
		}
		charge += 96 + len(addr)
	}
	return charge, true
}

func legacyDelegationMembership(list [][]byte) map[string]struct{} {
	if len(list) < legacyDelegationSetMinimum {
		return nil
	}
	// Own the immutable keys in one arena instead of allocating a string for
	// every address. The builder is never written after String, so no map key
	// borrows mutable record bytes. Existing per-address accounting covers the
	// whole arena even after deletions, including keys retained by duplicates.
	var keys strings.Builder
	var size int
	for _, addr := range list {
		if len(addr) > legacyDelegationCacheBytes-size {
			// Resident entries have already passed the smaller complete-row
			// budget. Preserve the original allocation path for any other
			// caller, and avoid overflowing the arena length before Grow.
			set := make(map[string]struct{}, len(list))
			for _, addr := range list {
				set[string(addr)] = struct{}{}
			}
			return set
		}
		size += len(addr)
	}
	keys.Grow(size)
	for _, addr := range list {
		keys.Write(addr)
	}
	arena := keys.String()
	set := make(map[string]struct{}, len(list))
	offset := 0
	for _, addr := range list {
		end := offset + len(addr)
		set[arena[offset:end]] = struct{}{}
		offset = end
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
			s.recordLegacyDelegationPageReads(e)
			return e, true, nil
		}
	}
	legacyDelegationMisses.Inc(1)
	rec, pages, exists, err := readPagedDelegationIndex(account, s.readSystemDelegationWithError)
	if err != nil {
		return nil, exists, err
	}
	if !exists {
		return newLegacyDelegationEntry(key, &corepb.DelegatedResourceAccountIndex{Account: bytes.Clone(account)}), false, nil
	}
	legacyDelegationDecoded.Inc(int64(len(rec.FromAccounts) + len(rec.ToAccounts)))
	e := newLegacyDelegationEntryWithPages(key, rec, pages)
	e.exists = true
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
	oldFromPages, oldToPages := len(e.pages.from), len(e.pages.to)
	if err := s.changeLegacyDelegationPages(e, peer, incoming, remove); err != nil {
		return err
	}
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
		e.charge += 2*cap(ownedPeer) + 10
		if cap(*list) != oldCap {
			// The other list may still pin the old shared header arena. Retain
			// its charge and add the whole new allocation, not just the delta.
			e.charge += cap(*list) * 24
		}
		if set != nil {
			set[string(peer)] = struct{}{}
			e.charge += 96 + len(peer)
		} else {
			charge, fits := legacyDelegationMembershipCharge(*list, legacyDelegationCacheBytes-e.charge)
			if fits {
				e.charge += charge
				if incoming {
					e.fromSet = legacyDelegationMembership(*list)
				} else {
					e.toSet = legacyDelegationMembership(*list)
				}
			} else if e.charge <= legacyDelegationCacheBytes {
				legacyDelegationMapSkipped.Inc(1)
			}
		}
	}
	if !e.exists || oldFromPages != len(e.pages.from) || oldToPages != len(e.pages.to) {
		if err := s.writeLegacyDelegationDescriptor(e); err != nil {
			return err
		}
		e.exists = true
	}
	legacyDelegationChanges.Inc(1)
	obj := s.getStateObjectWithoutAccess(tcommon.SystemAccountAddress)
	if cache := s.legacyDelegationCacheFor(obj); cache != nil {
		cache.admit(e)
	}
	return nil
}
