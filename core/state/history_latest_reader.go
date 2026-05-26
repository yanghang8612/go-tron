package state

import (
	"errors"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

type hotStateLatestReader interface {
	AccountLatest(owner tcommon.Address) ([]byte, bool, error)
	KVLatest(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error)
	KVGeneration(owner tcommon.Address) (uint64, bool, error)
	Code(hash tcommon.Hash) ([]byte, bool, error)
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

func defaultHotLatest(db ethdb.KeyValueReader) hotStateLatestReader {
	return newRegistryHotStateLatestReader(db, snapshots.DefaultDomainRegistry())
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
