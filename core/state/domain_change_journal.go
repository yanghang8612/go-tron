package state

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"fmt"
	"slices"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// domainChangeBlockArena slab-allocates rawdb.StateDomainChange values and
// small copied payloads for the duration of a single block capture. Each
// block starts with a zero arena, so external callers retaining returned
// slices keep ownership without cross-block mutation or pool contention.
type domainChangeBlockArena struct {
	changeSlab []rawdb.StateDomainChange
	changeIdx  int
	byteSlab   []byte
}

func (a *domainChangeBlockArena) allocChange() *rawdb.StateDomainChange {
	if a.changeIdx >= len(a.changeSlab) {
		capHint := len(a.changeSlab) * 2
		if capHint < 64 {
			capHint = 64
		} else if capHint > 512 {
			capHint = 512
		}
		a.changeSlab = make([]rawdb.StateDomainChange, capHint)
		a.changeIdx = 0
	}
	c := &a.changeSlab[a.changeIdx]
	a.changeIdx++
	return c
}

func (a *domainChangeBlockArena) allocBytes(n int) []byte {
	if n == 0 {
		return nil
	}
	if n > 2048 {
		return make([]byte, n)
	}
	a.ensureByteSpace(n)
	start := len(a.byteSlab)
	end := start + n
	a.byteSlab = a.byteSlab[:end]
	return a.byteSlab[start:end:end]
}

func (a *domainChangeBlockArena) ensureByteSpace(n int) {
	if cap(a.byteSlab)-len(a.byteSlab) >= n {
		return
	}
	slabCap := 8192
	if cap(a.byteSlab) > 0 {
		slabCap = cap(a.byteSlab) * 2
		if slabCap > 32768 {
			slabCap = 32768
		}
	}
	if slabCap < n {
		slabCap = n
	}
	a.byteSlab = make([]byte, 0, slabCap)
}

func (a *domainChangeBlockArena) copyBytes(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	dst := a.allocBytes(len(src))
	copy(dst, src)
	return dst
}

func (a *domainChangeBlockArena) copyStringBytes(src string) []byte {
	if len(src) == 0 {
		return nil
	}
	dst := a.allocBytes(len(src))
	copy(dst, src)
	return dst
}

func (a *domainChangeBlockArena) encodeUint64(v uint64) []byte {
	dst := a.allocBytes(8)
	binary.BigEndian.PutUint64(dst, v)
	return dst
}

func (a *domainChangeBlockArena) appendAccountLatest(obj *stateObject, flatRoot bool) (next []byte, nextExist bool, rewindStart int, rewindable bool, err error) {
	if obj == nil || obj.deleted || obj.selfDestructed || obj.account == nil {
		return nil, false, 0, false, nil
	}
	accBytes, err := obj.deterministicAccountProto()
	if err != nil {
		return nil, false, 0, false, err
	}
	accountKVRoot := obj.accountKVRoot
	if flatRoot {
		accountKVRoot = EmptyKVRoot
	}
	size := stateAccountV5EncodedSize(accBytes, accountKVRoot, obj.accountKVGeneration, obj.codeHash)
	if size > 2048 {
		buf := make([]byte, 0, size)
		buf = appendStateAccountV2Fields(buf, StateAccountVersion, accBytes, accountKVRoot, obj.accountKVGeneration, obj.codeHash)
		return buf[:len(buf):len(buf)], true, 0, false, nil
	}
	a.ensureByteSpace(size)
	start := len(a.byteSlab)
	a.byteSlab = appendStateAccountV2Fields(a.byteSlab, StateAccountVersion, accBytes, accountKVRoot, obj.accountKVGeneration, obj.codeHash)
	end := len(a.byteSlab)
	return a.byteSlab[start:end:end], true, start, true, nil
}

type kvDomainTouchKey struct {
	addr   tcommon.Address
	mapKey string
}

type storageDomainTouchKey struct {
	addr tcommon.Address
	slot tcommon.Hash
}

type domainChangeCollectScratch struct {
	accounts       map[tcommon.Address]accountDomainJournalTouch
	kvs            map[kvDomainTouchKey]kvDomainJournalTouch
	storages       map[storageDomainTouchKey]storageDomainJournalTouch
	generations    map[tcommon.Address]generationDomainJournalTouch
	accountKeys    []tcommon.Address
	generationKeys []tcommon.Address
	kvKeys         []kvDomainTouchKey
	storageKeys    []storageDomainTouchKey
}

type accountDomainJournalTouch struct {
	addr      tcommon.Address
	prev      []byte
	prevExist bool
}

type kvDomainJournalTouch struct {
	addr       tcommon.Address
	mapKey     string
	domain     kvdomains.KVDomain
	keyStr     string
	prev       []byte
	prevExist  bool
	prevLoaded bool
}

type storageDomainJournalTouch struct {
	addr      tcommon.Address
	slot      tcommon.Hash
	prev      tcommon.Hash
	prevExist bool
}

type generationDomainJournalTouch struct {
	addr      tcommon.Address
	prev      uint64
	prevExist bool
}

func splitKVCompositeKeyString(mapKey string) (kvdomains.KVDomain, string, bool) {
	if len(mapKey) < 2 {
		return 0, "", false
	}
	domain := kvdomains.KVDomain(uint16(mapKey[0])<<8 | uint16(mapKey[1]))
	return domain, mapKey[2:], true
}

// Deletion always persists a generation tombstone, including generation zero
// whose physical counter may not exist yet. Account creation followed by
// deletion can have no net account/KV journal change, so the account history
// cannot stand in for this separate physical source row.
func (s *StateDB) journalDeletedAccountGeneration(obj *stateObject) error {
	if obj == nil || !s.changeSet.enabled || s.changeSet.captureAtCommit {
		return nil
	}
	previous, present := obj.accountKVGeneration, obj.accountKVGenerationDirty
	if !present {
		var err error
		previous, present, err = s.readStateKVGeneration(obj.address)
		if err != nil {
			return err
		}
	}
	s.journal.append(kvGenerationHistoryChange{address: obj.address, previous: previous, prevExists: present})
	return nil
}

func (s *StateDB) collectKVJournalTouch(scratch *domainChangeCollectScratch, e *kvChange) {
	if e == nil {
		return
	}
	id := kvDomainTouchKey{addr: e.address, mapKey: e.mapKey}
	if scratch.kvs != nil {
		if _, ok := scratch.kvs[id]; ok {
			return
		}
	}
	obj := s.stateObjects[e.address]
	if obj == nil {
		return
	}
	domain, keyStr, ok := splitKVCompositeKeyString(e.mapKey)
	if !ok {
		return
	}
	if scratch.kvs == nil {
		scratch.kvs = make(map[kvDomainTouchKey]kvDomainJournalTouch)
	}
	touch := kvDomainJournalTouch{
		addr:   e.address,
		mapKey: e.mapKey,
		domain: domain,
		keyStr: keyStr,
	}
	if e.hadEntry {
		touch.prevLoaded = true
		if !e.prevEntry.deleted {
			touch.prevExist = true
			touch.prev = e.prevEntry.val
		}
	} else if cur, ok := obj.kvDirty[e.mapKey]; ok && cur.prevLoaded {
		touch.prevLoaded = true
		touch.prevExist = cur.prevExists
		touch.prev = cur.prev
	}
	scratch.kvs[id] = touch
	scratch.kvKeys = append(scratch.kvKeys, id)
}

func (s *StateDB) FlushDomainChangesSince(mark int, txNum uint64) error {
	if s == nil || !s.changeSet.enabled || s.changeSet.captureAtCommit {
		return nil
	}
	return s.publishDomainChangesSince(defaultStateDomainChangeRunner(s.changeSet.writer), mark, txNum)
}

func (s *StateDB) publishDomainChangesSince(publisher StateDomainChangePublisher, mark int, txNum uint64) error {
	changes, err := s.collectDomainChangesSince(mark, txNum)
	if err != nil {
		return err
	}
	if len(changes) == 0 {
		s.finishDomainChangeFlush()
		return nil
	}
	if publisher == nil {
		return fmt.Errorf("state domain change stage: nil publisher")
	}
	if err := publisher.PublishStateDomainChanges(changes); err != nil {
		return err
	}
	s.finishDomainChangeFlush()
	return nil
}

func (s *StateDB) collectDomainChangesSince(mark int, txNum uint64) ([]*rawdb.StateDomainChange, error) {
	return s.appendDomainChangesSince(nil, mark, txNum)
}

func (s *StateDB) appendDomainChangesSince(dst []*rawdb.StateDomainChange, mark int, txNum uint64) ([]*rawdb.StateDomainChange, error) {
	if s == nil || !s.changeSet.enabled || s.changeSet.captureAtCommit {
		return dst, nil
	}
	if txNum == s.changeSet.endTxNum {
		if err := s.stageFinalContractABIHistory(); err != nil {
			return nil, err
		}
	}
	if mark < 0 {
		mark = 0
	}
	if mark > s.journal.length() {
		mark = s.journal.length()
	}
	s.SetDomainChangeTxNum(txNum)
	var err error
	if mark < s.journal.length() {
		dst, err = s.appendJournalDomainChanges(dst, s.journal.entries[mark:])
		if err != nil {
			return nil, err
		}
	}
	if len(s.domainChangeNoJournal) > 0 {
		dst, err = s.appendJournalDomainChanges(dst, s.domainChangeNoJournal)
		if err != nil {
			return nil, err
		}
	}
	return dst, nil
}

func (s *StateDB) finishDomainChangeFlush() {
	if s == nil || !s.changeSet.enabled || s.changeSet.captureAtCommit {
		return
	}
	s.domainChangeNoJournal = s.domainChangeNoJournal[:0]
	s.changeSet.journalMark = s.journal.length()
}

func (s *StateDB) appendJournalDomainChanges(dst []*rawdb.StateDomainChange, entries []journalChange) ([]*rawdb.StateDomainChange, error) {
	if len(entries) == 0 {
		return dst, nil
	}
	const maxRetainedTouchScratch = 256
	scratch := s.changeSet.scratch
	if scratch == nil {
		scratch = &domainChangeCollectScratch{}
		s.changeSet.scratch = scratch
	}
	defer func() {
		if n := len(scratch.accountKeys); n > 0 {
			if n > maxRetainedTouchScratch {
				scratch.accounts = nil
				scratch.accountKeys = nil
			} else {
				if n <= 4 {
					for _, k := range scratch.accountKeys {
						delete(scratch.accounts, k)
					}
				} else {
					clear(scratch.accounts)
				}
				scratch.accountKeys = scratch.accountKeys[:0]
			}
		}
		if n := len(scratch.generationKeys); n > 0 {
			if n > maxRetainedTouchScratch {
				scratch.generations = nil
				scratch.generationKeys = nil
			} else {
				if n <= 4 {
					for _, k := range scratch.generationKeys {
						delete(scratch.generations, k)
					}
				} else {
					clear(scratch.generations)
				}
				scratch.generationKeys = scratch.generationKeys[:0]
			}
		}
		if n := len(scratch.kvKeys); n > 0 {
			if n > maxRetainedTouchScratch {
				scratch.kvs = nil
				scratch.kvKeys = nil
			} else {
				if n <= 4 {
					for _, k := range scratch.kvKeys {
						delete(scratch.kvs, k)
					}
				} else {
					clear(scratch.kvs)
				}
				clear(scratch.kvKeys)
				scratch.kvKeys = scratch.kvKeys[:0]
			}
		}
		if n := len(scratch.storageKeys); n > 0 {
			if n > maxRetainedTouchScratch {
				scratch.storages = nil
				scratch.storageKeys = nil
			} else {
				if n <= 4 {
					for _, k := range scratch.storageKeys {
						delete(scratch.storages, k)
					}
				} else {
					clear(scratch.storages)
				}
				scratch.storageKeys = scratch.storageKeys[:0]
			}
		}
	}()

	// Copy journal-owned slices into block-stage ownership only when a domain
	// change actually emits. The values must remain stable across later
	// transactions until the complete block changeset is encoded by FlushFinal.
	for _, entry := range entries {
		switch e := entry.(type) {
		case accountChange:
			if scratch.accounts == nil {
				scratch.accounts = make(map[tcommon.Address]accountDomainJournalTouch)
			} else if _, ok := scratch.accounts[e.address]; ok {
				continue
			}
			scratch.accounts[e.address] = accountDomainJournalTouch{
				addr:      e.address,
				prev:      e.prevLatest,
				prevExist: len(e.prevLatest) > 0 && !e.prevDeleted,
			}
			scratch.accountKeys = append(scratch.accountKeys, e.address)
		case codeChange:
			if scratch.accounts == nil {
				scratch.accounts = make(map[tcommon.Address]accountDomainJournalTouch)
			} else if _, ok := scratch.accounts[e.address]; ok {
				continue
			}
			scratch.accounts[e.address] = accountDomainJournalTouch{
				addr:      e.address,
				prev:      e.prevLatest,
				prevExist: len(e.prevLatest) > 0,
			}
			scratch.accountKeys = append(scratch.accountKeys, e.address)
		case kvChange:
			s.collectKVJournalTouch(scratch, &e)
		case *kvChange:
			s.collectKVJournalTouch(scratch, e)
		case contractMetadataKVChange:
			s.collectKVJournalTouch(scratch, &e.kvChange)
		case *storageChange:
			id := storageDomainTouchKey{addr: e.address, slot: e.key}
			if scratch.storages == nil {
				scratch.storages = make(map[storageDomainTouchKey]storageDomainJournalTouch)
			} else if _, ok := scratch.storages[id]; ok {
				continue
			}
			prevExist := e.prevExists && e.prev != (tcommon.Hash{})
			scratch.storages[id] = storageDomainJournalTouch{
				addr:      e.address,
				slot:      e.key,
				prev:      e.prev,
				prevExist: prevExist,
			}
			scratch.storageKeys = append(scratch.storageKeys, id)
		case kvResetChange:
			if scratch.generations == nil {
				scratch.generations = make(map[tcommon.Address]generationDomainJournalTouch)
			} else if _, ok := scratch.generations[e.address]; ok {
				continue
			}
			scratch.generations[e.address] = generationDomainJournalTouch{
				addr:      e.address,
				prev:      e.prevGeneration,
				prevExist: e.prevGenerationExists,
			}
			scratch.generationKeys = append(scratch.generationKeys, e.address)
		case kvGenerationHistoryChange:
			if scratch.generations == nil {
				scratch.generations = make(map[tcommon.Address]generationDomainJournalTouch)
			} else if _, ok := scratch.generations[e.address]; ok {
				continue
			}
			scratch.generations[e.address] = generationDomainJournalTouch{
				addr:      e.address,
				prev:      e.previous,
				prevExist: e.prevExists,
			}
			scratch.generationKeys = append(scratch.generationKeys, e.address)
		}
	}

	if len(scratch.accountKeys) > 1 {
		slices.SortFunc(scratch.accountKeys, func(a, b tcommon.Address) int { return bytes.Compare(a[:], b[:]) })
	}
	for _, addr := range scratch.accountKeys {
		change, err := s.accountJournalDomainChange(scratch.accounts[addr])
		if err != nil {
			return nil, err
		}
		if change != nil {
			dst = append(dst, s.prepareJournalDomainChange(change))
		}
	}
	if len(scratch.generationKeys) > 1 {
		slices.SortFunc(scratch.generationKeys, func(a, b tcommon.Address) int { return bytes.Compare(a[:], b[:]) })
	}
	for _, addr := range scratch.generationKeys {
		change, err := s.generationJournalDomainChange(scratch.generations[addr])
		if err != nil {
			return nil, err
		}
		if change != nil {
			dst = append(dst, s.prepareJournalDomainChange(change))
		}
	}
	if len(scratch.kvKeys) > 1 {
		slices.SortFunc(scratch.kvKeys, func(a, b kvDomainTouchKey) int {
			if c := bytes.Compare(a.addr[:], b.addr[:]); c != 0 {
				return c
			}
			return cmp.Compare(a.mapKey, b.mapKey)
		})
	}
	for _, id := range scratch.kvKeys {
		change, err := s.kvJournalDomainChange(scratch.kvs[id])
		if err != nil {
			return nil, err
		}
		if change != nil {
			dst = append(dst, s.prepareJournalDomainChange(change))
		}
	}
	if len(scratch.storageKeys) > 1 {
		slices.SortFunc(scratch.storageKeys, func(a, b storageDomainTouchKey) int {
			if c := bytes.Compare(a.addr[:], b.addr[:]); c != 0 {
				return c
			}
			return bytes.Compare(a.slot[:], b.slot[:])
		})
	}
	for _, id := range scratch.storageKeys {
		change, err := s.storageJournalDomainChange(scratch.storages[id])
		if err != nil {
			return nil, err
		}
		if change != nil {
			dst = append(dst, s.prepareJournalDomainChange(change))
		}
	}
	return dst, nil
}

func (s *StateDB) accountJournalDomainChange(touch accountDomainJournalTouch) (*rawdb.StateDomainChange, error) {
	obj := s.stateObjects[touch.addr]
	arena := &s.changeSet.arena
	next, nextExist, rewindStart, rewindable, err := arena.appendAccountLatest(obj, true)
	if err != nil {
		return nil, err
	}
	if touch.prevExist == nextExist && (!nextExist || bytes.Equal(touch.prev, next)) {
		if rewindable && rewindStart <= len(arena.byteSlab) {
			arena.byteSlab = arena.byteSlab[:rewindStart]
		}
		return nil, nil
	}
	prev := arena.copyBytes(touch.prev)
	change := arena.allocChange()
	*change = rawdb.StateDomainChange{
		FlatDomain: rawdb.StateFlatDomainAccountLatest,
		Owner:      touch.addr,
		PrevExists: touch.prevExist,
		Prev:       prev,
		NextExists: nextExist,
		Next:       next,
	}
	return change, nil
}

func (s *StateDB) generationJournalDomainChange(touch generationDomainJournalTouch) (*rawdb.StateDomainChange, error) {
	obj := s.stateObjects[touch.addr]
	if obj == nil {
		return nil, nil
	}
	if touch.prevExist && touch.prev == obj.accountKVGeneration {
		return nil, nil
	}
	arena := &s.changeSet.arena
	next := arena.encodeUint64(obj.accountKVGeneration)
	var prev []byte
	if touch.prevExist {
		prev = arena.encodeUint64(touch.prev)
	}
	change := arena.allocChange()
	*change = rawdb.StateDomainChange{
		FlatDomain: rawdb.StateFlatDomainKVGeneration,
		Owner:      touch.addr,
		PrevExists: touch.prevExist,
		Prev:       prev,
		NextExists: true,
		Next:       next,
	}
	return change, nil
}

func (s *StateDB) kvJournalDomainChange(touch kvDomainJournalTouch) (*rawdb.StateDomainChange, error) {
	obj := s.stateObjects[touch.addr]
	if obj == nil || obj.deleted || obj.selfDestructed {
		return nil, nil
	}
	entry, ok := obj.kvDirty[touch.mapKey]
	if !ok {
		return nil, nil
	}
	prev := touch.prev
	prevExist := touch.prevExist
	prevOwned := false
	if !touch.prevLoaded {
		if hasFreshAccountKVGeneration(obj) {
			recordFreshAccountKVPreimageReadAvoided()
			prev = nil
			prevExist = false
		} else {
			var err error
			prev, prevExist, err = s.readAccountKVLatest(touch.addr, obj.accountKVGeneration, touch.domain, ownedStringBytes(touch.keyStr))
			if err != nil {
				return nil, err
			}
			prevOwned = true
		}
	}
	nextExist := !entry.deleted
	next := entry.val
	if prevExist == nextExist && (!nextExist || bytes.Equal(prev, next)) {
		return nil, nil
	}
	arena := &s.changeSet.arena
	key := arena.copyStringBytes(touch.keyStr)
	if !prevOwned && len(prev) > 0 {
		prev = arena.copyBytes(prev)
	}
	var nextCopy []byte
	if nextExist {
		nextCopy = arena.copyBytes(next)
	}
	change := arena.allocChange()
	*change = rawdb.StateDomainChange{
		FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner:      touch.addr,
		Generation: obj.accountKVGeneration,
		Domain:     touch.domain,
		Key:        key,
		PrevExists: prevExist,
		Prev:       prev,
		NextExists: nextExist,
		Next:       nextCopy,
	}
	return change, nil
}

func (s *StateDB) storageJournalDomainChange(touch storageDomainJournalTouch) (*rawdb.StateDomainChange, error) {
	obj := s.stateObjects[touch.addr]
	if obj == nil || obj.deleted || obj.selfDestructed {
		return nil, nil
	}
	value := obj.storage[touch.slot].value
	nextExist := value != (tcommon.Hash{})
	if touch.prevExist == nextExist && (!nextExist || touch.prev == value) {
		return nil, nil
	}
	arena := &s.changeSet.arena
	var prev []byte
	if touch.prevExist {
		prev = arena.copyBytes(touch.prev[:])
	}
	var next []byte
	if nextExist {
		next = arena.copyBytes(value[:])
	}
	rowHash := s.storageRowKey(touch.addr, touch.slot)
	rowKey := arena.copyBytes(rowHash[:])
	change := arena.allocChange()
	*change = rawdb.StateDomainChange{
		FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner:      touch.addr,
		Generation: obj.accountKVGeneration,
		Domain:     kvdomains.ContractStorage,
		Key:        rowKey,
		PrevExists: touch.prevExist,
		Prev:       prev,
		NextExists: nextExist,
		Next:       next,
	}
	return change, nil
}

func (s *StateDB) prepareJournalDomainChange(change *rawdb.StateDomainChange) *rawdb.StateDomainChange {
	return s.changeSet.prepareDomainChange(change)
}

func (c *domainChangeSetCapture) publishCommitDomainChange(change *rawdb.StateDomainChange) error {
	if c == nil || !c.enabled || !c.captureAtCommit || change == nil {
		return nil
	}
	prepared := c.prepareDomainChange(change)
	return defaultStateDomainChangeRunner(c.writer).PublishStateDomainChanges([]*rawdb.StateDomainChange{prepared})
}

func (c *domainChangeSetCapture) prepareDomainChange(change *rawdb.StateDomainChange) *rawdb.StateDomainChange {
	if c == nil || change == nil {
		return nil
	}
	c.seq++
	// All callers construct a fresh, publication-owned change. Stamp it in
	// place; cloning Key/Prev/Next here would duplicate large account envelopes
	// retained by the block stage until its final packed write.
	change.BlockNum = c.blockNum
	change.BlockHash = c.blockHash
	change.TxNum = c.txNum
	change.Seq = c.seq
	return change
}
