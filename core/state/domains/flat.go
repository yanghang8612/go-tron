package domains

import (
	"errors"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

var ErrNilFlatStore = errors.New("domains: nil flat store backing db")

type latestStateDB interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
	ethdb.Iteratee
}

type latestKVStore interface {
	GetLatest(owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error)
	PutLatest(owner common.Address, generation uint64, domain kvdomains.KVDomain, key, value []byte) error
	DeleteLatest(owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte) error
	DeleteLatestPrefix(owner common.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte) error
	IterateLatest(owner common.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte, fn IterateFunc) error
}

type GenerationResolver func(owner common.Address) (uint64, error)

type FlatStore struct {
	db                latestStateDB
	latest            latestKVStore
	defaultGeneration uint64
	resolveGeneration GenerationResolver
	commitment        bool
}

var (
	_ Store         = (*FlatStore)(nil)
	_ Iterator      = (*FlatStore)(nil)
	_ IterableStore = (*FlatStore)(nil)
)

func NewFlatStore(db latestStateDB, defaultGeneration uint64) *FlatStore {
	return newFlatStore(db, defaultGeneration, nil, false)
}

func NewFlatStoreWithGenerationResolver(db latestStateDB, defaultGeneration uint64, resolver GenerationResolver) *FlatStore {
	return newFlatStore(db, defaultGeneration, resolver, false)
}

func NewFlatStoreWithCommitment(db latestStateDB, defaultGeneration uint64) *FlatStore {
	return newFlatStore(db, defaultGeneration, nil, true)
}

func NewFlatStoreWithCommitmentResolver(db latestStateDB, defaultGeneration uint64, resolver GenerationResolver) *FlatStore {
	return newFlatStore(db, defaultGeneration, resolver, true)
}

func newFlatStore(db latestStateDB, defaultGeneration uint64, resolver GenerationResolver, commitment bool) *FlatStore {
	return &FlatStore{
		db:                db,
		latest:            newRawDBLatestKVStore(db),
		defaultGeneration: defaultGeneration,
		resolveGeneration: resolver,
		commitment:        commitment,
	}
}

func FixedGeneration(generation uint64) GenerationResolver {
	return func(common.Address) (uint64, error) {
		return generation, nil
	}
}

func (s *FlatStore) GetLatest(owner common.Address, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error) {
	if err := validateDomain(domain); err != nil {
		return nil, false, err
	}
	store, _, generation, err := s.context(owner)
	if err != nil {
		return nil, false, err
	}
	return store.GetLatest(owner, generation, domain, key)
}

func (s *FlatStore) DomainPut(owner common.Address, domain kvdomains.KVDomain, key, value []byte) error {
	if err := validateDomain(domain); err != nil {
		return err
	}
	store, db, generation, err := s.context(owner)
	if err != nil {
		return err
	}
	if err := store.PutLatest(owner, generation, domain, key, value); err != nil {
		return err
	}
	return s.updateCommitment(db, []rawdb.StateCommitmentUpdate{
		rawdb.NewStateCommitmentPut(rawdb.StateKVLatestCommitmentKey(owner, generation, domain, key), rawdb.EncodeStateKVLatestValue(value)),
	})
}

func (s *FlatStore) DomainDel(owner common.Address, domain kvdomains.KVDomain, key []byte) error {
	if err := validateDomain(domain); err != nil {
		return err
	}
	store, db, generation, err := s.context(owner)
	if err != nil {
		return err
	}
	if err := store.DeleteLatest(owner, generation, domain, key); err != nil {
		return err
	}
	return s.updateCommitment(db, []rawdb.StateCommitmentUpdate{
		rawdb.NewStateCommitmentDelete(rawdb.StateKVLatestCommitmentKey(owner, generation, domain, key)),
	})
}

func (s *FlatStore) DomainDelPrefix(owner common.Address, domain kvdomains.KVDomain, prefix []byte) error {
	if err := validateDomain(domain); err != nil {
		return err
	}
	store, db, generation, err := s.context(owner)
	if err != nil {
		return err
	}
	var updates []rawdb.StateCommitmentUpdate
	if s.commitment {
		if err := store.IterateLatest(owner, generation, domain, prefix, func(key, _ []byte) (bool, error) {
			updates = append(updates, rawdb.NewStateCommitmentDelete(rawdb.StateKVLatestCommitmentKey(owner, generation, domain, key)))
			return true, nil
		}); err != nil {
			return err
		}
	}
	if err := store.DeleteLatestPrefix(owner, generation, domain, prefix); err != nil {
		return err
	}
	return s.updateCommitment(db, updates)
}

func (s *FlatStore) DomainIterate(owner common.Address, domain kvdomains.KVDomain, prefix []byte, fn IterateFunc) error {
	if err := validateDomain(domain); err != nil {
		return err
	}
	if fn == nil {
		return nil
	}
	store, _, generation, err := s.context(owner)
	if err != nil {
		return err
	}
	return store.IterateLatest(owner, generation, domain, prefix, fn)
}

func (s *FlatStore) context(owner common.Address) (latestKVStore, latestStateDB, uint64, error) {
	if s == nil || s.latest == nil {
		return nil, nil, 0, ErrNilFlatStore
	}
	if s.resolveGeneration == nil {
		return s.latest, s.db, s.defaultGeneration, nil
	}
	generation, err := s.resolveGeneration(owner)
	if err != nil {
		return nil, nil, 0, err
	}
	return s.latest, s.db, generation, nil
}

func (s *FlatStore) updateCommitment(db latestStateDB, updates []rawdb.StateCommitmentUpdate) error {
	if s == nil || !s.commitment {
		return nil
	}
	_, err := ApplyLatestCommitment(db, updates)
	return err
}
