package state

import (
	"context"
	"sort"

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
	touches      map[string]domainCommitmentTouch
}

type domainCommitmentTouch struct {
	flatDomain rawdb.StateFlatDomain
	owner      tcommon.Address
	generation uint64
	domain     kvdomains.KVDomain
	key        []byte
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
		touches:    make(map[string]domainCommitmentTouch),
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
		case statedomains.MutationPut, statedomains.MutationDel:
			d.recordKVLatestTouch(mutation.Owner, generation, mutation.Domain, mutation.Key)
		case statedomains.MutationDelPrefix:
			if err := d.iterateKVLatestPrefix(mutation.Owner, generation, mutation.Domain, mutation.Key, func(key, _ []byte) (bool, error) {
				d.recordKVLatestTouch(mutation.Owner, generation, mutation.Domain, key)
				return true, nil
			}); err != nil {
				return err
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
		owner:      owner,
	})
}

func (d *DomainCommitmentState) recordKVGenerationTouch(owner tcommon.Address) {
	d.recordTouch(domainCommitmentTouch{
		flatDomain: rawdb.StateFlatDomainKVGeneration,
		owner:      owner,
	})
}

func (d *DomainCommitmentState) latestUpdatesFromTouches() ([]rawdb.StateCommitmentUpdate, error) {
	if d == nil || d.state == nil || len(d.touches) == 0 {
		return nil, nil
	}
	reader := d.latestReaderOrDefault()
	keys := make([]string, 0, len(d.touches))
	for key := range d.touches {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	updates := make([]rawdb.StateCommitmentUpdate, 0, len(keys))
	for _, key := range keys {
		touch := d.touches[key]
		update, err := d.latestUpdateFromTouch(reader, touch)
		if err != nil {
			return nil, err
		}
		updates = append(updates, update)
	}
	return updates, nil
}

func (d *DomainCommitmentState) latestUpdateFromTouch(reader domainCommitmentLatestReader, touch domainCommitmentTouch) (rawdb.StateCommitmentUpdate, error) {
	switch touch.flatDomain {
	case rawdb.StateFlatDomainAccountLatest:
		commitmentKey := rawdb.StateAccountLatestCommitmentKey(touch.owner)
		value, ok, err := reader.AccountLatest(touch.owner)
		if err != nil {
			return rawdb.StateCommitmentUpdate{}, err
		}
		if ok {
			return rawdb.NewStateCommitmentPut(commitmentKey, value), nil
		}
		return rawdb.NewStateCommitmentDelete(commitmentKey), nil
	case rawdb.StateFlatDomainKVGeneration:
		commitmentKey := rawdb.StateKVGenerationCommitmentKey(touch.owner)
		generation, ok, err := reader.KVGeneration(touch.owner)
		if err != nil {
			return rawdb.StateCommitmentUpdate{}, err
		}
		if ok {
			return rawdb.NewStateCommitmentPut(commitmentKey, rawdb.EncodeStateKVGenerationValue(generation)), nil
		}
		return rawdb.NewStateCommitmentDelete(commitmentKey), nil
	case rawdb.StateFlatDomainKVLatest:
		commitmentKey := rawdb.StateKVLatestCommitmentKey(touch.owner, touch.generation, touch.domain, touch.key)
		value, ok, err := reader.KVLatest(touch.owner, touch.generation, touch.domain, touch.key)
		if err != nil {
			return rawdb.StateCommitmentUpdate{}, err
		}
		if ok {
			return rawdb.NewStateCommitmentPut(commitmentKey, rawdb.EncodeStateKVLatestValue(value)), nil
		}
		return rawdb.NewStateCommitmentDelete(commitmentKey), nil
	default:
		return rawdb.StateCommitmentUpdate{}, nil
	}
}

func (d *DomainCommitmentState) recordKVLatestTouch(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, key []byte) {
	d.recordTouch(domainCommitmentTouch{
		flatDomain: rawdb.StateFlatDomainKVLatest,
		owner:      owner,
		generation: generation,
		domain:     domain,
		key:        append([]byte(nil), key...),
	})
}

func (d *DomainCommitmentState) recordTouch(touch domainCommitmentTouch) {
	if d == nil {
		return
	}
	commitmentKey := domainCommitmentKey(touch)
	if len(commitmentKey) == 0 {
		return
	}
	d.touches[string(commitmentKey)] = touch
}

func domainCommitmentKey(touch domainCommitmentTouch) []byte {
	switch touch.flatDomain {
	case rawdb.StateFlatDomainAccountLatest:
		return rawdb.StateAccountLatestCommitmentKey(touch.owner)
	case rawdb.StateFlatDomainKVGeneration:
		return rawdb.StateKVGenerationCommitmentKey(touch.owner)
	case rawdb.StateFlatDomainKVLatest:
		return rawdb.StateKVLatestCommitmentKey(touch.owner, touch.generation, touch.domain, touch.key)
	default:
		return nil
	}
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
