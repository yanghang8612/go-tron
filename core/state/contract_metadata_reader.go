package state

import (
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// ReadCommittedContractMetadataBytes reads serialized contract metadata from
// flat latest state without hydrating a StateDB object. It is safe for
// concurrent offline readers while the underlying database is read-only.
func ReadCommittedContractMetadataBytes(db ethdb.KeyValueReader, addr tcommon.Address) ([]byte, bool, error) {
	generation, ok, err := rawdb.ReadStateKVGeneration(db, addr)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		generation = 0
	}
	return rawdb.ReadStateKVLatest(db, addr, generation, kvdomains.ContractMetadata, contractMetaKVKey)
}
