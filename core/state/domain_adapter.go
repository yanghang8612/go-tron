package state

import (
	"bytes"
	"context"
	"slices"
	"strings"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	statedomains "github.com/tronprotocol/go-tron/core/state/domains"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

var (
	_ statedomains.LatestReader               = (*DomainState)(nil)
	_ statedomains.Writer                     = (*DomainState)(nil)
	_ statedomains.AsOfReader                 = (*DomainHistoryState)(nil)
	_ statedomains.HeadTxNumSetter            = (*DomainHistoryState)(nil)
	_ statedomains.CommitmentProcessor        = (*DomainCommitmentState)(nil)
	_ statedomains.CommitmentMutationRecorder = (*DomainCommitmentState)(nil)
)

// DomainState adapts the current StateDB flat account-KV latest view to the
// domain engine interfaces.
type DomainState struct {
	state *StateDB
}

func NewDomainState(s *StateDB) *DomainState {
	return &DomainState{state: s}
}

func (s *StateDB) Domains() *DomainState {
	return NewDomainState(s)
}

func (s *StateDB) TemporalDomains(headTxNum uint64) statedomains.TemporalTx {
	tx := statedomains.NewSharedDomainTx(statedomains.SharedDomainTxConfig{
		Latest:     s.Domains(),
		Writer:     s.Domains(),
		History:    NewDomainHistoryState(s, headTxNum),
		Commitment: NewDomainCommitmentState(s),
	})
	tx.SetTxNum(headTxNum)
	return tx
}

func (d *DomainState) GetLatest(owner tcommon.Address, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error) {
	if d == nil || d.state == nil {
		return nil, false, nil
	}
	return d.state.GetAccountKV(owner, domain, key)
}

func (d *DomainState) DomainPut(owner tcommon.Address, domain kvdomains.KVDomain, key, value []byte) error {
	if d == nil || d.state == nil {
		return nil
	}
	return d.state.SetAccountKV(owner, domain, key, value)
}

func (d *DomainState) DomainDel(owner tcommon.Address, domain kvdomains.KVDomain, key []byte) error {
	if d == nil || d.state == nil {
		return nil
	}
	return d.state.DeleteAccountKV(owner, domain, key)
}

func (d *DomainState) DomainDelPrefix(owner tcommon.Address, domain kvdomains.KVDomain, prefix []byte) error {
	if d == nil || d.state == nil {
		return nil
	}
	return d.state.DeleteAccountKVPrefix(owner, domain, prefix)
}

type DomainHistoryState struct {
	state     *StateDB
	headTxNum uint64
}

func NewDomainHistoryState(s *StateDB, headTxNum uint64) *DomainHistoryState {
	return &DomainHistoryState{
		state:     s,
		headTxNum: headTxNum,
	}
}

func (d *DomainHistoryState) SetHeadTxNum(txNum uint64) {
	if d == nil {
		return
	}
	d.headTxNum = txNum
}

func (d *DomainHistoryState) GetAsOf(owner tcommon.Address, domain kvdomains.KVDomain, key []byte, txNum uint64) ([]byte, bool, error) {
	if d == nil || d.state == nil {
		return nil, false, nil
	}
	return d.state.GetAccountKVAsOfTxNum(owner, domain, key, txNum, d.headTxNum)
}

type DomainCommitmentState struct {
	state        *StateDB
	generation   statedomains.GenerationResolver
	latestReader domainCommitmentLatestReader
	touches      map[domainCommitmentTouch]int
	touchValues  []domainCommitmentCapturedValue
}

type domainCommitmentTouch struct {
	flatDomain rawdb.StateFlatDomain
	owner      tcommon.AccountID
	generation uint64
	domain     kvdomains.KVDomain
	key        string
}

// domainCommitmentCapturedValue is the final value carried by a KV-latest
// mutation. The temporal overlay already owns this value before it is flushed,
// so retaining one immutable copy avoids reading the just-written row back from
// the latest store when commitment updates are assembled. Account-latest and
// generation touches still use the reader and leave loaded false.
type domainCommitmentCapturedValue struct {
	value  []byte
	exists bool
	loaded bool
}

type domainCommitmentLatestReader interface {
	AccountLatest(owner tcommon.Address) ([]byte, bool, error)
	KVGeneration(owner tcommon.Address) (uint64, bool, error)
	KVLatest(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error)
}

type domainCommitmentLatestPrefixIterator interface {
	KVLatestPrefix(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte, fn func(key, value []byte) (bool, error)) error
}

type stateDomainCommitmentLatestReader struct {
	state *StateDB
}

func (r stateDomainCommitmentLatestReader) AccountLatest(owner tcommon.Address) ([]byte, bool, error) {
	if r.state == nil {
		return nil, false, nil
	}
	return r.state.readStateAccountLatest(owner)
}

func (r stateDomainCommitmentLatestReader) KVGeneration(owner tcommon.Address) (uint64, bool, error) {
	if r.state == nil {
		return 0, false, nil
	}
	return r.state.readStateKVGeneration(owner)
}

func (r stateDomainCommitmentLatestReader) KVLatest(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error) {
	if r.state == nil {
		return nil, false, nil
	}
	return r.state.readAccountKVLatest(owner, generation, domain, key)
}

func (r stateDomainCommitmentLatestReader) KVLatestPrefix(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte, fn func(key, value []byte) (bool, error)) error {
	if r.state == nil {
		return nil
	}
	return r.state.iterateAccountKVLatest(owner, generation, domain, prefix, fn)
}

func NewDomainCommitmentState(s *StateDB) *DomainCommitmentState {
	return NewDomainCommitmentStateWithGenerationResolver(s, nil)
}

func NewDomainCommitmentStateWithGenerationResolver(s *StateDB, generation statedomains.GenerationResolver) *DomainCommitmentState {
	return &DomainCommitmentState{
		state:      s,
		generation: generation,
		touches:    make(map[domainCommitmentTouch]int),
	}
}

func (d *DomainCommitmentState) RecordCommitmentMutations(ctx context.Context, mutations []statedomains.Mutation) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if d == nil || d.state == nil || len(mutations) == 0 {
		return nil
	}
	for _, mutation := range mutations {
		if err := contextError(ctx); err != nil {
			return err
		}
		generation, err := d.resolveGeneration(mutation.Owner)
		if err != nil {
			return err
		}
		switch mutation.Kind {
		case statedomains.MutationPut:
			d.recordKVLatestValue(mutation.Owner, generation, mutation.Domain, mutation.Key, mutation.Value, true)
		case statedomains.MutationDel:
			d.recordKVLatestValue(mutation.Owner, generation, mutation.Domain, mutation.Key, nil, false)
		case statedomains.MutationDelPrefix:
			if err := d.iterateKVLatestPrefix(mutation.Owner, generation, mutation.Domain, mutation.Key, func(key, _ []byte) (bool, error) {
				d.recordKVLatestValue(mutation.Owner, generation, mutation.Domain, key, nil, false)
				return true, nil
			}); err != nil {
				return err
			}
			// The latest reader does not include mutations still in this temporal
			// overlay. Apply the prefix tombstone to earlier captured puts too;
			// later mutations in the loop can overwrite it normally.
			ownerID := mutation.Owner.AccountID()
			prefix := string(mutation.Key)
			for touch, index := range d.touches {
				if touch.flatDomain == rawdb.StateFlatDomainKVLatest && touch.owner == ownerID && touch.generation == generation && touch.domain == mutation.Domain && strings.HasPrefix(touch.key, prefix) {
					d.touchValues[index] = domainCommitmentCapturedValue{loaded: true}
				}
			}
		}
	}
	return nil
}

func (d *DomainCommitmentState) latestReaderOrDefault() domainCommitmentLatestReader {
	if d == nil || d.state == nil {
		return nil
	}
	if d.latestReader != nil {
		return d.latestReader
	}
	return stateDomainCommitmentLatestReader{state: d.state}
}

func (d *DomainCommitmentState) iterateKVLatestPrefix(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte, fn func(key, value []byte) (bool, error)) error {
	reader := d.latestReaderOrDefault()
	if iterator, ok := reader.(domainCommitmentLatestPrefixIterator); ok {
		return iterator.KVLatestPrefix(owner, generation, domain, prefix, fn)
	}
	if d == nil || d.state == nil {
		return nil
	}
	return d.state.iterateAccountKVLatest(owner, generation, domain, prefix, fn)
}

func (d *DomainCommitmentState) recordAccountLatestTouch(owner tcommon.Address) {
	d.recordTouch(domainCommitmentTouch{
		flatDomain: rawdb.StateFlatDomainAccountLatest,
		owner:      owner.AccountID(),
	})
}

func (d *DomainCommitmentState) recordKVGenerationTouch(owner tcommon.Address) {
	d.recordTouch(domainCommitmentTouch{
		flatDomain: rawdb.StateFlatDomainKVGeneration,
		owner:      owner.AccountID(),
	})
}

func (d *DomainCommitmentState) latestUpdatesFromTouches() ([]rawdb.StateCommitmentUpdate, error) {
	if d == nil || d.state == nil || len(d.touches) == 0 {
		return nil, nil
	}
	reader := d.latestReaderOrDefault()
	updates := make([]rawdb.StateCommitmentUpdate, 0, len(d.touches))
	accountTouchCount := 0
	for touch := range d.touches {
		if touch.flatDomain == rawdb.StateFlatDomainAccountLatest {
			accountTouchCount++
		}
	}
	accountKeyArena := make([]byte, 0, accountTouchCount*rawdb.StateAccountLatestCommitmentKeySize())
	for touch, index := range d.touches {
		var accountCommitmentKey []byte
		if touch.flatDomain == rawdb.StateFlatDomainAccountLatest {
			start := len(accountKeyArena)
			owner := touch.owner.Address(tcommon.AddressPrefixMainnet)
			accountKeyArena = rawdb.AppendStateAccountLatestCommitmentKey(accountKeyArena, owner)
			accountCommitmentKey = accountKeyArena[start:len(accountKeyArena):len(accountKeyArena)]
		}
		update, err := d.latestUpdateFromTouch(reader, touch, d.touchValues[index], accountCommitmentKey)
		if err != nil {
			return nil, err
		}
		updates = append(updates, update)
	}
	slices.SortFunc(updates, func(a, b rawdb.StateCommitmentUpdate) int {
		return bytes.Compare(a.Key, b.Key)
	})
	return updates, nil
}

func (d *DomainCommitmentState) latestUpdateFromTouch(reader domainCommitmentLatestReader, touch domainCommitmentTouch, captured domainCommitmentCapturedValue, accountCommitmentKey []byte) (rawdb.StateCommitmentUpdate, error) {
	owner := touch.owner.Address(tcommon.AddressPrefixMainnet)
	switch touch.flatDomain {
	case rawdb.StateFlatDomainAccountLatest:
		commitmentKey := accountCommitmentKey
		if len(commitmentKey) == 0 {
			commitmentKey = rawdb.StateAccountLatestCommitmentKey(owner)
		}
		value, ok, err := reader.AccountLatest(owner)
		if err != nil {
			return rawdb.StateCommitmentUpdate{}, err
		}
		if ok {
			return rawdb.NewStateCommitmentPutOwned(commitmentKey, value), nil
		}
		return rawdb.NewStateCommitmentDeleteOwned(commitmentKey), nil
	case rawdb.StateFlatDomainKVGeneration:
		commitmentKey := rawdb.StateKVGenerationCommitmentKey(owner)
		generation, ok, err := reader.KVGeneration(owner)
		if err != nil {
			return rawdb.StateCommitmentUpdate{}, err
		}
		if ok {
			return rawdb.NewStateCommitmentPutOwned(commitmentKey, rawdb.EncodeStateKVGenerationValue(generation)), nil
		}
		return rawdb.NewStateCommitmentDeleteOwned(commitmentKey), nil
	case rawdb.StateFlatDomainKVLatest:
		logicalKey := []byte(touch.key)
		commitmentKey := rawdb.StateKVLatestCommitmentKey(owner, touch.generation, touch.domain, logicalKey)
		if captured.loaded {
			if captured.exists {
				return rawdb.NewStateCommitmentPutOwned(commitmentKey, rawdb.EncodeStateKVLatestValue(captured.value)), nil
			}
			return rawdb.NewStateCommitmentDeleteOwned(commitmentKey), nil
		}
		value, ok, err := reader.KVLatest(owner, touch.generation, touch.domain, logicalKey)
		if err != nil {
			return rawdb.StateCommitmentUpdate{}, err
		}
		if ok {
			return rawdb.NewStateCommitmentPutOwned(commitmentKey, rawdb.EncodeStateKVLatestValue(value)), nil
		}
		return rawdb.NewStateCommitmentDeleteOwned(commitmentKey), nil
	default:
		return rawdb.StateCommitmentUpdate{}, nil
	}
}

func (d *DomainCommitmentState) recordKVLatestTouch(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, key []byte) {
	ownerID := owner.AccountID()
	// A transaction can update the same storage slot many times. Probe with a
	// temporary string conversion first; the compiler keeps this map lookup
	// allocation-free. Only a genuinely new touch needs an immutable key copy.
	if d.hasKVLatestTouch(ownerID, generation, domain, key) {
		return
	}
	d.recordTouch(domainCommitmentTouch{
		flatDomain: rawdb.StateFlatDomainKVLatest,
		owner:      ownerID,
		generation: generation,
		domain:     domain,
		key:        string(key),
	})
}

func (d *DomainCommitmentState) recordKVLatestValue(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, key, value []byte, exists bool) {
	if d == nil {
		return
	}
	ownerID := owner.AccountID()
	if index, ok := d.kvLatestTouchIndex(ownerID, generation, domain, key); ok {
		captured := &d.touchValues[index]
		captured.exists = exists
		captured.loaded = true
		if exists {
			captured.value = append(captured.value[:0], value...)
		} else {
			captured.value = nil
		}
		return
	}
	captured := domainCommitmentCapturedValue{exists: exists, loaded: true}
	if exists {
		captured.value = append([]byte(nil), value...)
	}
	d.recordTouchWithValue(domainCommitmentTouch{
		flatDomain: rawdb.StateFlatDomainKVLatest,
		owner:      ownerID,
		generation: generation,
		domain:     domain,
		key:        string(key),
	}, captured)
}

func (d *DomainCommitmentState) hasKVLatestTouch(owner tcommon.AccountID, generation uint64, domain kvdomains.KVDomain, key []byte) bool {
	_, ok := d.kvLatestTouchIndex(owner, generation, domain, key)
	return ok
}

func (d *DomainCommitmentState) kvLatestTouchIndex(owner tcommon.AccountID, generation uint64, domain kvdomains.KVDomain, key []byte) (int, bool) {
	if d == nil {
		return 0, false
	}
	index, ok := d.touches[domainCommitmentTouch{
		flatDomain: rawdb.StateFlatDomainKVLatest,
		owner:      owner,
		generation: generation,
		domain:     domain,
		key:        string(key),
	}]
	return index, ok
}

func (d *DomainCommitmentState) recordTouch(touch domainCommitmentTouch) {
	d.recordTouchWithValue(touch, domainCommitmentCapturedValue{})
}

func (d *DomainCommitmentState) recordTouchWithValue(touch domainCommitmentTouch, captured domainCommitmentCapturedValue) {
	if d == nil {
		return
	}
	switch touch.flatDomain {
	case rawdb.StateFlatDomainAccountLatest, rawdb.StateFlatDomainKVGeneration, rawdb.StateFlatDomainKVLatest:
	default:
		return
	}
	if d.touches == nil {
		d.touches = make(map[domainCommitmentTouch]int)
	}
	if _, exists := d.touches[touch]; exists {
		return
	}
	d.touches[touch] = len(d.touchValues)
	d.touchValues = append(d.touchValues, captured)
}

func (d *DomainCommitmentState) resolveGeneration(owner tcommon.Address) (uint64, error) {
	if d == nil || d.state == nil {
		return 0, nil
	}
	if d.generation != nil {
		return d.generation(owner)
	}
	if obj := d.state.getStateObject(owner); obj != nil {
		return obj.accountKVGeneration, nil
	}
	generation, ok, err := d.state.readStateKVGeneration(owner)
	if err != nil || ok {
		return generation, err
	}
	return 0, nil
}

func (d *DomainCommitmentState) SeekCommitment(ctx context.Context) (uint64, uint64, error) {
	if err := contextError(ctx); err != nil {
		return 0, 0, err
	}
	if d == nil || d.state == nil {
		return 0, 0, nil
	}
	index := d.state.accountKVIndex()
	return statedomains.SeekLatestCommitmentWithStore(statedomains.NewStagedCommitmentStore(index), func(blockNum uint64) (uint64, error) {
		return snapshots.StateDomainHistoryTxNumAtBlockEnd(index, blockNum)
	})
}

func (d *DomainCommitmentState) ComputeCommitment(ctx context.Context, blockNum, txNum uint64) (tcommon.Hash, error) {
	_ = blockNum
	_ = txNum
	if err := contextError(ctx); err != nil {
		return tcommon.Hash{}, err
	}
	if d == nil || d.state == nil {
		return tcommon.Hash{}, nil
	}
	index := d.state.accountKVIndex()
	updates, err := d.latestUpdatesFromTouches()
	if err != nil {
		return tcommon.Hash{}, err
	}
	store := d.state.latestCommitmentStore(index)
	root, err := statedomains.ApplyLatestCommitmentWithStore(store, updates)
	return tcommon.Hash(root), err
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}
