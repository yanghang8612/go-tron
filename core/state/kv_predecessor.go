package state

import (
	"bytes"
	"fmt"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

type kvBeforeFunc func(prefix, upper []byte) (key, value []byte, ok bool, err error)
type kvBeforeEntry struct {
	value   []byte
	deleted bool
}
type kvLatestPredecessor interface {
	KVLatestBefore(tcommon.Address, uint64, kvdomains.KVDomain, []byte, []byte) ([]byte, []byte, bool, error)
}
type kvPhysicalPredecessor interface {
	ReadKVLatestBefore(tcommon.Address, uint64, kvdomains.KVDomain, []byte, []byte) ([]byte, []byte, bool, error)
}

// mergeKVBefore resolves the same overlay as point reads, including tombstones.
// Repeated reverse seeks skip deleted base keys without scanning the whole prefix.
func mergeKVBefore(base kvBeforeFunc, overlay map[string]kvBeforeEntry, prefix, upper []byte) (key, value []byte, ok bool, err error) {
	for k, e := range overlay {
		if e.deleted || !bytes.HasPrefix([]byte(k), prefix) || upper != nil && bytes.Compare([]byte(k), upper) >= 0 {
			continue
		}
		if !ok || k > string(key) {
			key = []byte(k)
			value = bytes.Clone(e.value)
			ok = true
		}
	}
	if base == nil {
		return
	}
	limit := bytes.Clone(upper)
	for {
		bk, bv, found, e := base(prefix, limit)
		if e != nil {
			return nil, nil, false, e
		}
		if !found {
			return
		}
		if ok && bytes.Compare(key, bk) >= 0 {
			return
		}
		if e, masked := overlay[string(bk)]; masked {
			if !e.deleted {
				return bytes.Clone(bk), bytes.Clone(e.value), true, nil
			}
			limit = bk
			continue
		}
		return bk, bv, true, nil
	}
}
func iterateKVBefore(iterate func([]byte, func([]byte, []byte) (bool, error)) error, prefix, upper []byte) (key, value []byte, ok bool, err error) {
	err = iterate(prefix, func(k, v []byte) (bool, error) {
		if upper != nil && bytes.Compare(k, upper) >= 0 {
			return false, nil
		}
		key = bytes.Clone(k)
		value = bytes.Clone(v)
		ok = true
		return true, nil
	})
	if err != nil {
		return nil, nil, false, err
	}
	return
}
func physicalKVBefore(store accountKVPhysicalLatestStore, owner tcommon.Address, generation uint64, domain kvdomains.KVDomain) kvBeforeFunc {
	if native, ok := store.(kvPhysicalPredecessor); ok {
		return func(p, u []byte) ([]byte, []byte, bool, error) {
			return native.ReadKVLatestBefore(owner, generation, domain, p, u)
		}
	}
	return func(p, u []byte) ([]byte, []byte, bool, error) {
		return iterateKVBefore(func(p []byte, fn func([]byte, []byte) (bool, error)) error {
			return store.IterateKVLatest(owner, generation, domain, p, fn)
		}, p, u)
	}
}
func (w *accountKVLatestBatch) readLatestBefore(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, prefix, upper []byte) ([]byte, []byte, bool, error) {
	if w == nil || w.latestStore == nil {
		return nil, nil, false, nil
	}
	overlay := make(map[string]kvBeforeEntry)
	id := owner.AccountID()
	for k, e := range w.pending {
		if k.owner == id && k.generation == generation && k.domain == domain && bytes.HasPrefix([]byte(k.logicalKey), prefix) {
			overlay[k.logicalKey] = kvBeforeEntry{e.value, e.deleted}
		}
	}
	return mergeKVBefore(physicalKVBefore(w.latestStore, owner, generation, domain), overlay, prefix, upper)
}
func (r *commitScopeLatestReader) KVLatestBefore(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, prefix, upper []byte) ([]byte, []byte, bool, error) {
	if r == nil || r.writer == nil {
		return nil, nil, false, nil
	}
	return r.writer.readLatestBefore(owner, generation, domain, prefix, upper)
}
func (v historicalLatestView) KVLatestBefore(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, prefix, upper []byte) ([]byte, []byte, bool, error) {
	return iterateKVBefore(func(p []byte, fn func([]byte, []byte) (bool, error)) error {
		return v.KVLatestPrefix(owner, generation, domain, p, fn)
	}, prefix, upper)
}

// AccountKVBefore returns the last logical row strictly below upper in this
// StateDB view. It includes dirty writes, commit-scope writes and pinned layers.
func (s *StateDB) AccountKVBefore(owner tcommon.Address, domain kvdomains.KVDomain, prefix, upper []byte) ([]byte, []byte, bool, error) {
	s.recordAccountKVPrefixRead(owner)
	if !kvdomains.IsRegistered(domain) {
		return nil, nil, false, fmt.Errorf("account kv: unregistered domain %#04x", uint16(domain))
	}
	obj := s.getStateObject(owner)
	if obj == nil || obj.deleted {
		return nil, nil, false, s.Error()
	}
	overlay := make(map[string]kvBeforeEntry)
	for k, e := range obj.kvDirty {
		d, logical, ok := splitKVCompositeKey(ownedStringBytes(k))
		if ok && d == domain && bytes.HasPrefix(logical, prefix) {
			overlay[string(logical)] = kvBeforeEntry{e.val, e.deleted}
		}
	}
	var base kvBeforeFunc
	if !hasFreshAccountKVGeneration(obj) {
		if native, ok := s.accountKVLatestReader.(kvLatestPredecessor); ok {
			base = func(p, u []byte) ([]byte, []byte, bool, error) {
				return native.KVLatestBefore(owner, obj.accountKVGeneration, domain, p, u)
			}
		} else if s.accountKVLatestIterator != nil {
			base = func(p, u []byte) ([]byte, []byte, bool, error) {
				return iterateKVBefore(func(p []byte, fn func([]byte, []byte) (bool, error)) error {
					return s.iterateAccountKVLatest(owner, obj.accountKVGeneration, domain, p, fn)
				}, p, u)
			}
		} else {
			base = physicalKVBefore(s.accountKVPhysicalLatestStore(), owner, obj.accountKVGeneration, domain)
		}
	}
	return mergeKVBefore(base, overlay, prefix, upper)
}
