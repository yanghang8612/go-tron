package state

import (
	"errors"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

type hotStateLatestReader interface {
	AccountLatest(owner tcommon.Address) ([]byte, bool, error)
	KVLatest(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error)
	KVGeneration(owner tcommon.Address) (uint64, bool, error)
	Code(hash tcommon.Hash) ([]byte, bool, error)
}

type coldStateLatestReader interface {
	GetAccountLatest(owner tcommon.Address, txNum uint64) ([]byte, bool, error)
	GetKVLatest(domain kvdomains.KVDomain, owner tcommon.Address, generation uint64, key []byte, txNum uint64) ([]byte, bool, error)
	IterateKVLatestPrefix(domain kvdomains.KVDomain, owner tcommon.Address, generation uint64, prefix []byte, txNum uint64, fn func(key, value []byte) (bool, error)) error
	GetKVGeneration(owner tcommon.Address, txNum uint64) (uint64, bool, error)
}

type registryHotStateLatestReader struct {
	db       ethdb.KeyValueReader
	registry snapshots.DomainRegistry
}

func newRegistryHotStateLatestReader(db ethdb.KeyValueReader, registry snapshots.DomainRegistry) hotStateLatestReader {
	return registryHotStateLatestReader{db: db, registry: registry}
}

func (r registryHotStateLatestReader) AccountLatest(owner tcommon.Address) ([]byte, bool, error) {
	cfg, ok := r.registry.Dataset(snapshots.SegmentDatasetAccountLatest)
	if !ok || cfg.ReadHotAccountLatest == nil {
		return nil, false, ErrStateDomainHistoryUnavailable
	}
	return cfg.ReadHotAccountLatest(r.db, owner)
}

func (r registryHotStateLatestReader) KVLatest(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error) {
	cfg, ok := r.registry.Dataset(snapshots.SegmentDatasetKVLatest)
	if !ok || cfg.ReadHotKVLatest == nil {
		return nil, false, ErrStateDomainHistoryUnavailable
	}
	return cfg.ReadHotKVLatest(r.db, owner, generation, domain, key)
}

func (r registryHotStateLatestReader) KVLatestPrefix(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte, fn func(key, value []byte) (bool, error)) error {
	iteratee, ok := r.db.(ethdb.Iteratee)
	if !ok {
		return ErrStateDomainHistoryUnavailable
	}
	return rawdb.IterateStateKVLatest(iteratee, owner, generation, domain, prefix, fn)
}

func (r registryHotStateLatestReader) KVGeneration(owner tcommon.Address) (uint64, bool, error) {
	cfg, ok := r.registry.Dataset(snapshots.SegmentDatasetKVGeneration)
	if !ok || cfg.ReadHotKVGeneration == nil {
		return 0, false, ErrStateDomainHistoryUnavailable
	}
	return cfg.ReadHotKVGeneration(r.db, owner)
}

func (r registryHotStateLatestReader) Code(hash tcommon.Hash) ([]byte, bool, error) {
	cfg, ok := r.registry.Dataset(snapshots.SegmentDatasetCode)
	if !ok || cfg.ReadHotCode == nil {
		return nil, false, ErrStateDomainHistoryUnavailable
	}
	return cfg.ReadHotCode(r.db, hash)
}

func (r *PersistentHistoryReader) hotLatest() hotStateLatestReader {
	if r == nil {
		return nil
	}
	if r.latest != nil {
		return r.latest
	}
	return newRegistryHotStateLatestReader(r.db, snapshots.DefaultDomainRegistry())
}

func (r *PersistentHistoryReader) latestAtTxNum(txNum uint64) hotStateLatestReader {
	hot := r.hotLatest()
	if r == nil || r.coldHistory == nil {
		return hot
	}
	cold, ok := r.coldHistory.(coldStateLatestReader)
	if !ok {
		return hot
	}
	return coldFallbackStateLatestReader{
		hot:   hot,
		cold:  cold,
		txNum: txNum,
	}
}

func defaultHotLatest(db ethdb.KeyValueReader) hotStateLatestReader {
	return newRegistryHotStateLatestReader(db, snapshots.DefaultDomainRegistry())
}

type coldFallbackStateLatestReader struct {
	hot   hotStateLatestReader
	cold  coldStateLatestReader
	txNum uint64
}

func (r coldFallbackStateLatestReader) AccountLatest(owner tcommon.Address) ([]byte, bool, error) {
	if r.hot != nil {
		value, ok, err := r.hot.AccountLatest(owner)
		if err != nil || ok {
			return value, ok, err
		}
	}
	if r.cold == nil {
		return nil, false, nil
	}
	return r.cold.GetAccountLatest(owner, r.txNum)
}

func (r coldFallbackStateLatestReader) KVLatest(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error) {
	if r.hot != nil {
		value, ok, err := r.hot.KVLatest(owner, generation, domain, key)
		if err != nil || ok {
			return value, ok, err
		}
	}
	if r.cold == nil {
		return nil, false, nil
	}
	return r.cold.GetKVLatest(domain, owner, generation, key, r.txNum)
}

func (r coldFallbackStateLatestReader) KVLatestPrefix(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte, fn func(key, value []byte) (bool, error)) error {
	seen := make(map[string]struct{})
	stopped := false
	if r.hot != nil {
		if latest, ok := r.hot.(hotStateLatestPrefixReader); ok {
			if err := latest.KVLatestPrefix(owner, generation, domain, prefix, func(key, value []byte) (bool, error) {
				seen[string(key)] = struct{}{}
				cont, err := fn(key, value)
				if err != nil || !cont {
					stopped = true
				}
				return cont, err
			}); err != nil {
				return err
			}
		}
	}
	if stopped || r.cold == nil {
		return nil
	}
	return r.cold.IterateKVLatestPrefix(domain, owner, generation, prefix, r.txNum, func(key, value []byte) (bool, error) {
		if _, ok := seen[string(key)]; ok {
			return true, nil
		}
		return fn(key, value)
	})
}

func (r coldFallbackStateLatestReader) KVGeneration(owner tcommon.Address) (uint64, bool, error) {
	if r.hot != nil {
		generation, ok, err := r.hot.KVGeneration(owner)
		if err != nil || ok {
			return generation, ok, err
		}
	}
	if r.cold == nil {
		return 0, false, nil
	}
	return r.cold.GetKVGeneration(owner, r.txNum)
}

func (r coldFallbackStateLatestReader) Code(hash tcommon.Hash) ([]byte, bool, error) {
	if r.hot == nil {
		return nil, false, nil
	}
	return r.hot.Code(hash)
}

func decodeHotAccountEnvelope(latest hotStateLatestReader, addr tcommon.Address) (*StateAccountV2, bool, error) {
	if latest == nil {
		return nil, false, errors.New("history latest reader: nil latest reader")
	}
	data, ok, err := latest.AccountLatest(addr)
	if err != nil || !ok {
		return nil, false, err
	}
	envelope, err := DecodeStateAccountV2(data)
	if err != nil {
		return nil, false, err
	}
	return envelope, true, nil
}
