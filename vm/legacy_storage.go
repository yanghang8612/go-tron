package vm

import (
	"bytes"
	"maps"

	tcommon "github.com/tronprotocol/go-tron/common"
)

// Java Storage caches by DataWord and commits the physical row captured at
// insertion. Cached reads can affect the HashMap traversal of dirty rows.
type legacyStorageRow struct {
	key          tcommon.Hash
	value        tcommon.Hash
	initialValue tcommon.Hash
	beforeNon32  []byte
	hasBefore    bool
	before32     bool
	dirty        bool
}

type legacyStorageCache struct {
	order  *javaHashMap[tcommon.Hash]
	rows   map[tcommon.Hash]legacyStorageRow
	owners map[tcommon.Hash]tcommon.Hash
}

func newLegacyStorageCache() *legacyStorageCache {
	return &legacyStorageCache{
		order: newJavaHashMap(func(key tcommon.Hash) int32 {
			h := int32(1) // java.util.Arrays.hashCode(byte[])
			for _, b := range key {
				h = h*31 + int32(int8(b))
			}
			return h
		}, func(a, b tcommon.Hash) int { return bytes.Compare(a[:], b[:]) }, nil),
		rows: make(map[tcommon.Hash]legacyStorageRow),
	}
}

func (c *legacyStorageCache) put(slot tcommon.Hash, row legacyStorageRow) {
	if _, exists := c.rows[slot]; !exists {
		c.order.put(slot)
	}
	c.rows[slot] = row
}

func (c *legacyStorageCache) copyForChild() *legacyStorageCache {
	child := newLegacyStorageCache()
	child.owners = maps.Clone(c.owners)
	for _, slot := range c.order.iterationOrder() {
		child.put(slot, c.rows[slot])
	}
	return child
}

// claim guards even a storage read that finds no backing row.
func (c *legacyStorageCache) claim(row, slot tcommon.Hash) bool {
	if owner, exists := c.owners[row]; exists {
		return owner == slot
	}
	if c.owners == nil {
		c.owners = make(map[tcommon.Hash]tcommon.Hash)
	}
	c.owners[row] = slot
	return true
}

func (c *legacyStorageCache) commit(publish func(legacyStorageRow) error) error {
	for _, slot := range c.order.iterationOrder() {
		row := c.rows[slot]
		if row.dirty {
			if err := publish(row); err != nil {
				return err
			}
		}
	}
	return nil
}

type legacyStorageFrame struct {
	parent *legacyStorageFrame
	caches map[tcommon.Address]*legacyStorageCache
}

func (tvm *TVM) enterLegacyStorageFrame(snapshot int, outErr *error) func() {
	parent := tvm.legacyStorage
	frame := &legacyStorageFrame{parent: parent}
	tvm.legacyStorage = frame
	return func() {
		tvm.legacyStorage = parent
		if *outErr != nil || len(frame.caches) == 0 {
			return
		}
		if parent != nil {
			if parent.caches == nil {
				parent.caches = make(map[tcommon.Address]*legacyStorageCache, len(frame.caches))
			}
			for addr, cache := range frame.caches {
				parent.caches[addr] = cache
			}
			return
		}
		for addr, cache := range frame.caches {
			err := cache.commit(func(row legacyStorageRow) error {
				var before []byte
				if row.hasBefore {
					if row.before32 {
						initial := row.initialValue
						before = initial[:]
					} else {
						before = row.beforeNon32
					}
				}
				return tvm.StateDB.WriteLegacyTVMStorageRow(addr, row.key, row.value, before)
			})
			if err != nil {
				tvm.StateDB.RevertToSnapshot(snapshot)
				*outErr = err
				return
			}
		}
	}
}

func (tvm *TVM) legacyStorageCache(addr tcommon.Address) *legacyStorageCache {
	frame := tvm.legacyStorage
	if cache := frame.caches[addr]; cache != nil {
		return cache
	}

	cache := frame.storageForChild(addr, tvm.cfg.ShareLegacyStorage)
	if frame.caches == nil {
		frame.caches = make(map[tcommon.Address]*legacyStorageCache)
	}
	frame.caches[addr] = cache
	return cache
}

// Repository.getStorage does not cache an inherited lookup by itself. Every
// uncached repository on the way back to a child makes its own fresh copy.
func (f *legacyStorageFrame) storageForChild(addr tcommon.Address, share bool) *legacyStorageCache {
	if cache := f.caches[addr]; cache != nil {
		return cache
	}
	if f.parent == nil {
		return newLegacyStorageCache()
	}
	inherited := f.parent.storageForChild(addr, share)
	if share {
		return inherited
	}
	return inherited.copyForChild()
}

func (tvm *TVM) readLegacyStorage(addr tcommon.Address, slot tcommon.Hash) (tcommon.Hash, bool, error) {
	cache := tvm.legacyStorageCache(addr)
	row, cached := cache.rows[slot]
	if cached && !tvm.cfg.StorageAliasCheck {
		return row.value, true, nil
	}
	key := tvm.StateDB.LegacyTVMStorageRowKey(addr, slot)
	if tvm.cfg.StorageAliasCheck && !cache.claim(key, slot) {
		return tcommon.Hash{}, false, storageAliasTimeout
	}
	if cached {
		return row.value, true, nil
	}
	value, beforeNon32, hasBefore, before32, exists, err := tvm.StateDB.ReadLegacyTVMStorageRow(addr, slot, key)
	if exists {
		cache.put(slot, legacyStorageRow{
			key:          key,
			value:        value,
			initialValue: value,
			beforeNon32:  beforeNon32,
			hasBefore:    hasBefore,
			before32:     before32,
		})
	}
	return value, exists, err
}

func (tvm *TVM) writeLegacyStorage(addr tcommon.Address, slot, value tcommon.Hash) {
	cache := tvm.legacyStorageCache(addr)
	row, exists := cache.rows[slot]
	if !exists {
		row.key = tvm.StateDB.LegacyTVMStorageRowKey(addr, slot)
	}
	row.dirty = true
	row.value = value
	cache.put(slot, row)
}
