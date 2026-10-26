package state

import (
	"errors"
	"fmt"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"

	"github.com/tronprotocol/go-tron/core/blockbuffer"
)

// BeginCanonicalStorageReadScope is explicitly enabled by the serial canonical
// processor, after BeginBlock and its rollback snapshots. It must not be called
// by a worker, historical reader, or while the caller can replace the active
// buffer topology. The returned cleanup unbinds before closing and must run
// while processor rollback is still possible.
func (s *StateDB) BeginCanonicalStorageReadScope(number uint64) (func() error, error) {
	noop := func() error { return nil }
	if s == nil || s.transactionVersionedReader != nil {
		return noop, nil
	}
	if s.storageReadScope != nil {
		return nil, errors.New("state: canonical storage scope already open")
	}
	reader, ok := s.accountKVLatestReader.(*commitScopeLatestReader)
	if !ok || reader == nil || reader.state != s || reader.writer == nil || reader.atomicScope != nil {
		return noop, nil
	}
	buffer, ok := s.accountKVIndex().(*blockbuffer.Buffer)
	if !ok || reader.writer.index != buffer {
		return noop, nil
	}
	scope, err := buffer.TryStorageReadScope(number)
	if err != nil {
		return nil, err
	}
	if scope == nil {
		return noop, nil
	}
	s.storageReadScope = scope
	s.storageReadScopeReader = reader
	return func() error {
		if s.storageReadScope == scope {
			s.storageReadScope = nil
			s.storageReadScopeReader = nil
		}
		return scope.Close()
	}, nil
}

// Keep all scope-only pending resolution and source validation out of the
// ordinary decoding function. Owning reads and preimages retain their original
// live route; only immediate decoding borrows the captured cut. Dirty/fresh
// resolution remains in callers. Pending commits precede the physical cut.
func (s *StateDB) readCanonicalAccountKVLatest(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error) {
	reader, ok := s.accountKVLatestReader.(*commitScopeLatestReader)
	if !ok || reader != s.storageReadScopeReader || reader.state != s || reader.writer == nil || reader.writer.index != s.accountKVIndex() {
		return nil, false, fmt.Errorf("state: canonical storage reader changed within block")
	}
	w := reader.writer
	if pending, ok := w.pending[accountKVLatestPendingLookupKey(owner, generation, domain, key)]; ok {
		if domain == kvdomains.ContractStorage && w.storagePendingResolved != nil {
			(*w.storagePendingResolved)++
		}
		if pending.deleted {
			return nil, false, nil
		}
		return pending.value, true, nil
	}
	return rawdb.ReadStateKVLatestNoCopy(s.storageReadScope, owner, generation, domain, key)
}
