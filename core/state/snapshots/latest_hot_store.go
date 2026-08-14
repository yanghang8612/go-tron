package snapshots

import (
	"context"
	"errors"
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

type latestHotStoreContext interface {
	IterateKVLatestDomainContext(ctx context.Context, domain kvdomains.KVDomain, fn func(owner common.Address, generation uint64, key, value []byte) (bool, error)) error
}

type latestHotStoreRowsContext interface {
	IterateKVLatestRowsContext(ctx context.Context, fn func(rawdb.StateKVLatestRow) (bool, error)) error
}

func iterateAccountLatestContext(ctx context.Context, store latestHotStore, fn func(owner common.Address, value []byte) (bool, error)) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	return store.IterateAccountLatest(func(owner common.Address, value []byte) (bool, error) {
		if err := contextError(ctx); err != nil {
			return false, err
		}
		return fn(owner, value)
	})
}

func iterateKVLatestDomainContext(ctx context.Context, store latestHotStore, domain kvdomains.KVDomain, fn func(owner common.Address, generation uint64, key, value []byte) (bool, error)) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if contextual, ok := store.(latestHotStoreContext); ok {
		return contextual.IterateKVLatestDomainContext(ctx, domain, fn)
	}
	return store.IterateKVLatestDomain(domain, func(owner common.Address, generation uint64, key, value []byte) (bool, error) {
		if err := contextError(ctx); err != nil {
			return false, err
		}
		return fn(owner, generation, key, value)
	})
}

func iterateKVLatestRowsContext(ctx context.Context, store latestHotStore, fn func(rawdb.StateKVLatestRow) (bool, error)) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	contextual, ok := store.(latestHotStoreRowsContext)
	if !ok {
		return errors.New("snapshots latest hot store: all-domain iterator unavailable")
	}
	return contextual.IterateKVLatestRowsContext(ctx, fn)
}

func iterateKVGenerationContext(ctx context.Context, store latestHotStore, fn func(owner common.Address, generation uint64) (bool, error)) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	return store.IterateKVGeneration(func(owner common.Address, generation uint64) (bool, error) {
		if err := contextError(ctx); err != nil {
			return false, err
		}
		return fn(owner, generation)
	})
}

func iterateCodeContext(ctx context.Context, store latestHotStore, fn func(hash common.Hash, code []byte) (bool, error)) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	return store.IterateCode(func(hash common.Hash, code []byte) (bool, error) {
		if err := contextError(ctx); err != nil {
			return false, err
		}
		return fn(hash, code)
	})
}

func iterateCommitmentDomainContext(ctx context.Context, store latestHotStore, logicalPrefix []byte, fn func(logicalKey, value []byte) (bool, error)) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	return store.IterateCommitmentDomain(logicalPrefix, func(logicalKey, value []byte) (bool, error) {
		if err := contextError(ctx); err != nil {
			return false, err
		}
		return fn(logicalKey, value)
	})
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
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
	return s.IterateKVLatestDomainContext(context.Background(), domain, fn)
}

func (s rawDBLatestHotStore) IterateKVLatestDomainContext(ctx context.Context, domain kvdomains.KVDomain, fn func(owner common.Address, generation uint64, key, value []byte) (bool, error)) error {
	if s.iterator == nil {
		return fmt.Errorf("snapshots latest hot store: nil iterator")
	}
	return rawdb.IterateStateKVLatestDomainRowsContext(ctx, s.iterator, domain, func(row rawdb.StateKVLatestRow) (bool, error) {
		return fn(row.Owner, row.Generation, row.Key, row.Value)
	})
}

func (s rawDBLatestHotStore) IterateKVLatestRowsContext(ctx context.Context, fn func(rawdb.StateKVLatestRow) (bool, error)) error {
	if s.iterator == nil {
		return fmt.Errorf("snapshots latest hot store: nil iterator")
	}
	return rawdb.IterateStateKVLatestRowsContext(ctx, s.iterator, fn)
}

func (s rawDBLatestHotStore) WriteKVLatest(owner common.Address, generation uint64, domain kvdomains.KVDomain, key, value []byte) error {
	if s.writer == nil {
		return fmt.Errorf("snapshots latest hot store: nil writer")
	}
	return rawdb.WriteStateKVLatest(s.writer, owner, generation, domain, key, value)
}

func (s rawDBLatestHotStore) ReadKVGeneration(owner common.Address) (uint64, bool, error) {
	if s.reader == nil {
		return 0, false, fmt.Errorf("snapshots latest hot store: nil reader while reading KV generation for %s", owner.Hex())
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
	// During a branch rotation the live root advances with the delta while the
	// frozen legacy table is streamed. Bind the root segment to the persisted
	// rotation boundary instead, so the immutable root and branch families
	// always describe the same trie even though block import continues.
	rotation, rotating, err := rawdb.ReadCommitmentBranchRotation(s.reader)
	if err != nil {
		return common.Hash{}, false, err
	}
	if rotating {
		return rotation.Root, true, nil
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
