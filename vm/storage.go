package vm

import (
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

func (tvm *TVM) readStorage(addr tcommon.Address, key tcommon.Hash) (tcommon.Hash, bool, error) {
	if !tvm.cfg.StorageAliasCheck && !tvm.cfg.OptimizeStorage {
		value, exists := tvm.StateDB.GetStateWithExist(addr, key)
		return value, exists, nil
	}
	if tvm.storage == nil {
		tvm.storage = &state.TVMStorage{
			Optimize:           tvm.cfg.OptimizeStorage,
			ProtectLegacyReads: tvm.cfg.ProtectLegacyStorageReads,
		}
	}
	value, exists, alias, err := tvm.storage.Read(tvm.StateDB, addr, key)
	if alias {
		return tcommon.Hash{}, false, storageAliasTimeout
	}
	return value, exists, err
}

func (tvm *TVM) writeStorage(addr tcommon.Address, key, value tcommon.Hash) error {
	if tvm.storage == nil {
		tvm.StateDB.SetState(addr, key, value)
		return nil
	}
	return tvm.storage.Write(tvm.StateDB, addr, key, value)
}

// extCodeHash returns the persisted contract hash or computes it from code.
// Proposal 102 skips caching the result for an empty code byte array.
func (tvm *TVM) extCodeHash(addr tcommon.Address) tcommon.Hash {
	meta := tvm.StateDB.GetContract(addr)
	if meta == nil {
		return tcommon.Keccak256(nil)
	}
	if cached := meta.GetCodeHash(); len(cached) > 0 {
		return tcommon.BytesToHash(cached)
	}
	code := tvm.StateDB.GetCode(addr)
	hash := tcommon.Keccak256(code)
	if tvm.cfg.ProtectLegacyStorageReads && len(code) == 0 {
		return hash
	}
	tvm.cacheContractCodeHash(addr, meta, hash)
	return hash
}

func (tvm *TVM) cacheContractCodeHash(addr tcommon.Address, meta *contractpb.SmartContract, hash tcommon.Hash) {
	updated := proto.Clone(meta).(*contractpb.SmartContract)
	updated.CodeHash = hash.Bytes()
	tvm.StateDB.SetContract(addr, updated)
}
