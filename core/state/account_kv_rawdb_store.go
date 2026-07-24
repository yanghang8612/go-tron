package state

import (
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// rawdbAccountKVPhysicalLatestStore is the compatibility adapter between the
// typed account-KV latest-store boundary and the current rawdb keyspace.
type rawdbAccountKVPhysicalLatestStore struct {
	reader   ethdb.KeyValueReader
	writer   ethdb.KeyValueWriter
	iterator ethdb.Iteratee
}

func newRawdbAccountKVPhysicalLatestStore(index accountKVIndexStore, writer ethdb.KeyValueWriter) rawdbAccountKVPhysicalLatestStore {
	return rawdbAccountKVPhysicalLatestStore{
		reader:   index,
		writer:   writer,
		iterator: index,
	}
}

func (s rawdbAccountKVPhysicalLatestStore) ReadAccountLatest(owner tcommon.Address) ([]byte, bool, error) {
	if s.reader == nil {
		return nil, false, nil
	}
	return rawdb.ReadStateAccountLatest(s.reader, owner)
}

func (s rawdbAccountKVPhysicalLatestStore) ReadAccountLatestNoCopy(owner tcommon.Address) ([]byte, bool, error) {
	if s.reader == nil {
		return nil, false, nil
	}
	return rawdb.ReadStateAccountLatestNoCopy(s.reader, owner)
}

func (s rawdbAccountKVPhysicalLatestStore) WriteAccountLatest(owner tcommon.Address, value []byte) error {
	if s.writer == nil {
		return fmt.Errorf("account kv latest store: nil writer")
	}
	return rawdb.WriteStateAccountLatest(s.writer, owner, value)
}

func (s rawdbAccountKVPhysicalLatestStore) DeleteAccountLatest(owner tcommon.Address) error {
	if s.writer == nil {
		return fmt.Errorf("account kv latest store: nil writer")
	}
	return rawdb.DeleteStateAccountLatest(s.writer, owner)
}

func (s rawdbAccountKVPhysicalLatestStore) ReadKVGeneration(owner tcommon.Address) (uint64, bool, error) {
	if s.reader == nil {
		return 0, false, nil
	}
	return rawdb.ReadStateKVGeneration(s.reader, owner)
}

func (s rawdbAccountKVPhysicalLatestStore) WriteKVGeneration(owner tcommon.Address, generation uint64) error {
	if s.writer == nil {
		return fmt.Errorf("account kv latest store: nil writer")
	}
	return rawdb.WriteStateKVGeneration(s.writer, owner, generation)
}

func (s rawdbAccountKVPhysicalLatestStore) DeleteKVGeneration(owner tcommon.Address) error {
	if s.writer == nil {
		return fmt.Errorf("account kv latest store: nil writer")
	}
	return rawdb.DeleteStateKVGeneration(s.writer, owner)
}

func (s rawdbAccountKVPhysicalLatestStore) ReadKVLatest(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, logicalKey []byte) ([]byte, bool, error) {
	if s.reader == nil {
		return nil, false, nil
	}
	return rawdb.ReadStateKVLatest(s.reader, owner, generation, domain, logicalKey)
}

func (s rawdbAccountKVPhysicalLatestStore) WriteKVLatestEncoded(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, logicalKey, encodedValue []byte) error {
	if s.writer == nil {
		return fmt.Errorf("account kv latest store: nil writer")
	}
	return rawdb.WriteStateKVLatestEncoded(s.writer, owner, generation, domain, logicalKey, encodedValue)
}

func (s rawdbAccountKVPhysicalLatestStore) WriteKVLatestEncodedOwned(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, logicalKey, encodedValue []byte) error {
	if s.writer == nil {
		return fmt.Errorf("account kv latest store: nil writer")
	}
	return rawdb.WriteStateKVLatestEncodedOwned(s.writer, owner, generation, domain, logicalKey, encodedValue)
}

func (s rawdbAccountKVPhysicalLatestStore) DeleteKVLatest(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, logicalKey []byte) error {
	if s.writer == nil {
		return fmt.Errorf("account kv latest store: nil writer")
	}
	return rawdb.DeleteStateKVLatest(s.writer, owner, generation, domain, logicalKey)
}

func (s rawdbAccountKVPhysicalLatestStore) IterateKVLatest(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte, fn func(key, value []byte) (bool, error)) error {
	if s.iterator == nil {
		return nil
	}
	return rawdb.IterateStateKVLatest(s.iterator, owner, generation, domain, prefix, fn)
}
