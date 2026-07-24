package state

import (
	"bytes"
	"fmt"
	"sort"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

type accountDomainJournalTouch struct {
	addr      tcommon.Address
	prev      []byte
	prevExist bool
}

type kvDomainJournalTouch struct {
	addr       tcommon.Address
	mapKey     string
	domain     kvdomains.KVDomain
	key        []byte
	prev       []byte
	prevExist  bool
	prevLoaded bool
}

type storageDomainJournalTouch struct {
	addr      tcommon.Address
	slot      tcommon.Hash
	prev      []byte
	prevExist bool
}

type generationDomainJournalTouch struct {
	addr      tcommon.Address
	prev      uint64
	prevExist bool
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
	if s == nil || !s.changeSet.enabled || s.changeSet.captureAtCommit {
		return nil, nil
	}
	if mark < 0 {
		mark = 0
	}
	if mark > s.journal.length() {
		mark = s.journal.length()
	}
	s.SetDomainChangeTxNum(txNum)
	changes, err := s.collectJournalDomainChanges(s.journal.entries[mark:])
	if err != nil {
		return nil, err
	}
	noJournalChanges, err := s.collectJournalDomainChanges(s.domainChangeNoJournal)
	if err != nil {
		return nil, err
	}
	changes = append(changes, noJournalChanges...)
	return changes, nil
}

func (s *StateDB) finishDomainChangeFlush() {
	if s == nil || !s.changeSet.enabled || s.changeSet.captureAtCommit {
		return
	}
	s.domainChangeNoJournal = s.domainChangeNoJournal[:0]
	s.changeSet.journalMark = s.journal.length()
}

func (s *StateDB) collectJournalDomainChanges(entries []journalChange) ([]*rawdb.StateDomainChange, error) {
	accounts := make(map[tcommon.Address]accountDomainJournalTouch)
	kvs := make(map[string]kvDomainJournalTouch)
	storages := make(map[string]storageDomainJournalTouch)
	generations := make(map[tcommon.Address]generationDomainJournalTouch)

	for _, entry := range entries {
		switch e := entry.(type) {
		case accountChange:
			if _, ok := accounts[e.address]; ok {
				continue
			}
			accounts[e.address] = accountDomainJournalTouch{
				addr:      e.address,
				prev:      append([]byte(nil), e.prevLatest...),
				prevExist: len(e.prevLatest) > 0 && !e.prevDeleted,
			}
		case codeChange:
			if _, ok := accounts[e.address]; ok {
				continue
			}
			accounts[e.address] = accountDomainJournalTouch{
				addr:      e.address,
				prev:      append([]byte(nil), e.prevLatest...),
				prevExist: len(e.prevLatest) > 0,
			}
		case kvChange:
			obj := s.stateObjects[e.address]
			if obj == nil {
				continue
			}
			domain, key, ok := splitKVCompositeKeyView([]byte(e.mapKey))
			if !ok {
				continue
			}
			id := string(e.address.Bytes()) + e.mapKey
			if _, ok := kvs[id]; ok {
				continue
			}
			touch := kvDomainJournalTouch{
				addr:   e.address,
				mapKey: e.mapKey,
				domain: domain,
				key:    append([]byte(nil), key...),
			}
			if e.hadEntry {
				touch.prevLoaded = true
				if !e.prevEntry.deleted {
					touch.prevExist = true
					touch.prev = append([]byte(nil), e.prevEntry.val...)
				}
			} else if cur, ok := obj.kvDirty[e.mapKey]; ok && cur.prevLoaded {
				touch.prevLoaded = true
				touch.prevExist = cur.prevExists
				touch.prev = append([]byte(nil), cur.prev...)
			}
			kvs[id] = touch
		case *storageChange:
			id := string(e.address.Bytes()) + string(e.key.Bytes())
			if _, ok := storages[id]; ok {
				continue
			}
			prevExist := e.prevExists && e.prev != (tcommon.Hash{})
			var prev []byte
			if prevExist {
				prev = append([]byte(nil), e.prev.Bytes()...)
			}
			storages[id] = storageDomainJournalTouch{
				addr:      e.address,
				slot:      e.key,
				prev:      prev,
				prevExist: prevExist,
			}
		case kvResetChange:
			if _, ok := generations[e.address]; ok {
				continue
			}
			generations[e.address] = generationDomainJournalTouch{
				addr:      e.address,
				prev:      e.prevGeneration,
				prevExist: e.prevGenerationExists,
			}
		}
	}

	changes := make([]*rawdb.StateDomainChange, 0, len(accounts)+len(generations)+len(kvs)+len(storages))
	for _, addr := range sortedAccountTouches(accounts) {
		change, err := s.accountJournalDomainChange(accounts[addr])
		if err != nil {
			return nil, err
		}
		if change != nil {
			changes = append(changes, s.prepareJournalDomainChange(change))
		}
	}
	for _, addr := range sortedGenerationTouches(generations) {
		change, err := s.generationJournalDomainChange(generations[addr])
		if err != nil {
			return nil, err
		}
		if change != nil {
			changes = append(changes, s.prepareJournalDomainChange(change))
		}
	}
	for _, id := range sortedKVTouches(kvs) {
		change, err := s.kvJournalDomainChange(kvs[id])
		if err != nil {
			return nil, err
		}
		if change != nil {
			changes = append(changes, s.prepareJournalDomainChange(change))
		}
	}
	for _, id := range sortedStorageTouches(storages) {
		change, err := s.storageJournalDomainChange(storages[id])
		if err != nil {
			return nil, err
		}
		if change != nil {
			changes = append(changes, s.prepareJournalDomainChange(change))
		}
	}
	return changes, nil
}

func (s *StateDB) accountJournalDomainChange(touch accountDomainJournalTouch) (*rawdb.StateDomainChange, error) {
	obj := s.stateObjects[touch.addr]
	next, nextExist, err := encodeAccountLatestObject(obj, true)
	if err != nil {
		return nil, err
	}
	if touch.prevExist == nextExist && (!nextExist || bytes.Equal(touch.prev, next)) {
		return nil, nil
	}
	return &rawdb.StateDomainChange{
		FlatDomain: rawdb.StateFlatDomainAccountLatest,
		Owner:      touch.addr,
		PrevExists: touch.prevExist,
		Prev:       touch.prev,
		NextExists: nextExist,
		Next:       next,
	}, nil
}

func (s *StateDB) generationJournalDomainChange(touch generationDomainJournalTouch) (*rawdb.StateDomainChange, error) {
	obj := s.stateObjects[touch.addr]
	if obj == nil {
		return nil, nil
	}
	next := rawdb.EncodeStateKVGenerationValue(obj.accountKVGeneration)
	var prev []byte
	if touch.prevExist {
		prev = rawdb.EncodeStateKVGenerationValue(touch.prev)
	}
	if touch.prevExist && touch.prev == obj.accountKVGeneration {
		return nil, nil
	}
	return &rawdb.StateDomainChange{
		FlatDomain: rawdb.StateFlatDomainKVGeneration,
		Owner:      touch.addr,
		PrevExists: touch.prevExist,
		Prev:       prev,
		NextExists: true,
		Next:       next,
	}, nil
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
	if !touch.prevLoaded {
		var err error
		prev, prevExist, err = s.readAccountKVLatest(touch.addr, obj.accountKVGeneration, touch.domain, touch.key)
		if err != nil {
			return nil, err
		}
	}
	nextExist := !entry.deleted
	next := entry.val
	if prevExist == nextExist && (!nextExist || bytes.Equal(prev, next)) {
		return nil, nil
	}
	var nextCopy []byte
	if nextExist {
		nextCopy = append([]byte(nil), next...)
	}
	return &rawdb.StateDomainChange{
		FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner:      touch.addr,
		Generation: obj.accountKVGeneration,
		Domain:     touch.domain,
		Key:        touch.key,
		PrevExists: prevExist,
		Prev:       prev,
		NextExists: nextExist,
		Next:       nextCopy,
	}, nil
}

func (s *StateDB) storageJournalDomainChange(touch storageDomainJournalTouch) (*rawdb.StateDomainChange, error) {
	obj := s.stateObjects[touch.addr]
	if obj == nil || obj.deleted || obj.selfDestructed {
		return nil, nil
	}
	value := obj.storage[touch.slot].value
	nextExist := value != (tcommon.Hash{})
	var next []byte
	if nextExist {
		next = append([]byte(nil), value.Bytes()...)
	}
	if touch.prevExist == nextExist && (!nextExist || bytes.Equal(touch.prev, next)) {
		return nil, nil
	}
	rowKey := s.storageRowKey(touch.addr, touch.slot).Bytes()
	return &rawdb.StateDomainChange{
		FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner:      touch.addr,
		Generation: obj.accountKVGeneration,
		Domain:     kvdomains.ContractStorage,
		Key:        rowKey,
		PrevExists: touch.prevExist,
		Prev:       touch.prev,
		NextExists: nextExist,
		Next:       next,
	}, nil
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
	prepared := cloneStateDomainChange(change)
	prepared.BlockNum = c.blockNum
	prepared.BlockHash = c.blockHash
	prepared.TxNum = c.txNum
	prepared.Seq = c.seq
	return prepared
}

func cloneStateDomainChange(change *rawdb.StateDomainChange) *rawdb.StateDomainChange {
	if change == nil {
		return nil
	}
	clone := *change
	clone.Key = append([]byte(nil), change.Key...)
	clone.Prev = append([]byte(nil), change.Prev...)
	clone.Next = append([]byte(nil), change.Next...)
	return &clone
}

func sortedAccountTouches(touches map[tcommon.Address]accountDomainJournalTouch) []tcommon.Address {
	out := make([]tcommon.Address, 0, len(touches))
	for addr := range touches {
		out = append(out, addr)
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].Bytes(), out[j].Bytes()) < 0 })
	return out
}

func sortedGenerationTouches(touches map[tcommon.Address]generationDomainJournalTouch) []tcommon.Address {
	out := make([]tcommon.Address, 0, len(touches))
	for addr := range touches {
		out = append(out, addr)
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].Bytes(), out[j].Bytes()) < 0 })
	return out
}

func sortedKVTouches(touches map[string]kvDomainJournalTouch) []string {
	out := make([]string, 0, len(touches))
	for id := range touches {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func sortedStorageTouches(touches map[string]storageDomainJournalTouch) []string {
	out := make([]string, 0, len(touches))
	for id := range touches {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}
