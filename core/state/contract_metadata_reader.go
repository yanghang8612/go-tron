package state

import (
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/statecodec"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
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
	data, ok, err := rawdb.ReadStateKVLatest(db, addr, generation, kvdomains.ContractMetadata, contractMetaKVKey)
	if err != nil || !ok {
		return nil, ok, err
	}
	meta := new(contractpb.SmartContract)
	if err := statecodec.Unmarshal(data, meta); err != nil {
		return nil, true, err
	}
	data, err = proto.Marshal(meta)
	return data, true, err
}

// ReadCommittedAccountCodeHash reads the code-hash edge stored in the flat
// account envelope without hydrating a StateDB. Contract metadata comparison
// uses this because java-tron keeps code_hash in SmartContract while go-tron
// keeps it in StateAccountV2 and stores bytecode by hash.
func ReadCommittedAccountCodeHash(db ethdb.KeyValueReader, addr tcommon.Address) (tcommon.Hash, bool, error) {
	raw, ok, err := rawdb.ReadStateAccountLatest(db, addr)
	if err != nil || !ok {
		return tcommon.Hash{}, ok, err
	}
	envelope, err := DecodeStateAccountV2(raw)
	if err != nil {
		return tcommon.Hash{}, true, err
	}
	return envelope.CodeHash, true, nil
}
