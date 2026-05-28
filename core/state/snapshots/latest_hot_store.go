package snapshots

import (
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

type latestHotStore interface {
	IterateAccountLatest(fn func(owner common.Address, value []byte) (bool, error)) error
	WriteAccountLatest(owner common.Address, value []byte) error
	IterateKVLatestDomain(domain kvdomains.KVDomain, fn func(owner common.Address, generation uint64, key, value []byte) (bool, error)) error
	WriteKVLatest(owner common.Address, generation uint64, domain kvdomains.KVDomain, key, value []byte) error
	ReadKVGeneration(owner common.Address) (uint64, bool, error)
	IterateKVGeneration(fn func(owner common.Address, generation uint64) (bool, error)) error
	WriteKVGeneration(owner common.Address, generation uint64) error
	IterateCode(fn func(hash common.Hash, code []byte) (bool, error)) error
	WriteCode(hash common.Hash, code []byte) error
	ReadCommitmentRoot() (common.Hash, bool, error)
	IterateCommitmentDomain(logicalPrefix []byte, fn func(logicalKey, value []byte) (bool, error)) error
	WriteCommitmentDomain(logicalKey, value []byte) error
}

// rawDBLatestHotStore is the compatibility adapter between latest snapshot
// build/restore and the current rawdb latest keyspace.
type rawDBLatestHotStore struct {
	reader   ethdb.KeyValueReader
	writer   ethdb.KeyValueWriter
	iterator ethdb.Iteratee
}

func newRawDBLatestHotBuildStore(db ethdb.Iteratee) latestHotStore {
	store := rawDBLatestHotStore{iterator: db}
	if reader, ok := db.(ethdb.KeyValueReader); ok {
		store.reader = reader
	}
	return store
}

func newRawDBLatestHotReadStore(db ethdb.KeyValueReader) latestHotStore {
	return rawDBLatestHotStore{reader: db}
}

func newRawDBLatestHotRestoreStore(db ethdb.KeyValueWriter) latestHotStore {
	return rawDBLatestHotStore{writer: db}
}

func (s rawDBLatestHotStore) IterateAccountLatest(fn func(owner common.Address, value []byte) (bool, error)) error {
	if s.iterator == nil {
		return fmt.Errorf("snapshots latest hot store: nil iterator")
	}
	return rawdb.IterateStateAccountLatest(s.iterator, nil, func(row rawdb.StateAccountLatestRow) (bool, error) {
		return fn(row.Owner, row.Value)
	})
}

func (s rawDBLatestHotStore) WriteAccountLatest(owner common.Address, value []byte) error {
	if s.writer == nil {
		return fmt.Errorf("snapshots latest hot store: nil writer")
	}
	return rawdb.WriteStateAccountLatest(s.writer, owner, value)
}

func (s rawDBLatestHotStore) IterateKVLatestDomain(domain kvdomains.KVDomain, fn func(owner common.Address, generation uint64, key, value []byte) (bool, error)) error {
	if s.iterator == nil {
		return fmt.Errorf("snapshots latest hot store: nil iterator")
	}
	return rawdb.IterateStateKVLatestDomainRows(s.iterator, domain, func(row rawdb.StateKVLatestRow) (bool, error) {
		return fn(row.Owner, row.Generation, row.Key, row.Value)
	})
}

func (s rawDBLatestHotStore) WriteKVLatest(owner common.Address, generation uint64, domain kvdomains.KVDomain, key, value []byte) error {
	if s.writer == nil {
		return fmt.Errorf("snapshots latest hot store: nil writer")
	}
	return rawdb.WriteStateKVLatest(s.writer, owner, generation, domain, key, value)
}

func (s rawDBLatestHotStore) ReadKVGeneration(owner common.Address) (uint64, bool, error) {
	if s.reader == nil {
		return 0, false, nil
	}
	return rawdb.ReadStateKVGeneration(s.reader, owner)
}

func (s rawDBLatestHotStore) IterateKVGeneration(fn func(owner common.Address, generation uint64) (bool, error)) error {
	if s.iterator == nil {
		return fmt.Errorf("snapshots latest hot store: nil iterator")
	}
	return rawdb.IterateStateKVGeneration(s.iterator, nil, func(row rawdb.StateKVGenerationRow) (bool, error) {
		return fn(row.Owner, row.Generation)
	})
}

func (s rawDBLatestHotStore) WriteKVGeneration(owner common.Address, generation uint64) error {
	if s.writer == nil {
		return fmt.Errorf("snapshots latest hot store: nil writer")
	}
	return rawdb.WriteStateKVGeneration(s.writer, owner, generation)
}

func (s rawDBLatestHotStore) IterateCode(fn func(hash common.Hash, code []byte) (bool, error)) error {
	if s.iterator == nil {
		return fmt.Errorf("snapshots latest hot store: nil iterator")
	}
	return rawdb.IterateStateCode(s.iterator, func(row rawdb.StateCodeRow) (bool, error) {
		return fn(row.Hash, row.Code)
	})
}

func (s rawDBLatestHotStore) WriteCode(hash common.Hash, code []byte) error {
	if s.writer == nil {
		return fmt.Errorf("snapshots latest hot store: nil writer")
	}
	return rawdb.WriteStateCode(s.writer, hash, code)
}

func (s rawDBLatestHotStore) ReadCommitmentRoot() (common.Hash, bool, error) {
	if s.reader == nil {
		return common.Hash{}, false, fmt.Errorf("snapshots latest hot store: nil reader")
	}
	return rawdb.ReadLatestDomainCommitmentRoot(s.reader)
}

func (s rawDBLatestHotStore) IterateCommitmentDomain(logicalPrefix []byte, fn func(logicalKey, value []byte) (bool, error)) error {
	if s.iterator == nil {
		return fmt.Errorf("snapshots latest hot store: nil iterator")
	}
	return rawdb.IterateStateCommitmentDomain(s.iterator, logicalPrefix, fn)
}

func (s rawDBLatestHotStore) WriteCommitmentDomain(logicalKey, value []byte) error {
	if s.writer == nil {
		return fmt.Errorf("snapshots latest hot store: nil writer")
	}
	return rawdb.WriteStateCommitmentDomain(s.writer, logicalKey, value)
}
