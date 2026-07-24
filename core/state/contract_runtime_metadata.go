package state

import (
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/encoding/protowire"
)

// ContractRuntimeMetadata is the immutable subset of SmartContract needed by
// transaction validation, energy accounting and TVM call setup. The full
// protobuf (including ABI entries) remains available through GetContract.
type ContractRuntimeMetadata struct {
	OriginAddress              tcommon.Address
	ConsumeUserResourcePercent int64
	OriginEnergyLimit          int64
	Version                    int32

	storageKeyPrefix   [storageKeyPrefixBytes]byte
	storageKeyHashSlot bool
}

func contractRuntimeMetadataFromProto(addr tcommon.Address, meta *contractpb.SmartContract) (ContractRuntimeMetadata, bool) {
	if meta == nil {
		return ContractRuntimeMetadata{}, false
	}
	prefix, hashSlot := javaStorageKeyLayout(addr, meta)
	return ContractRuntimeMetadata{
		OriginAddress:              tcommon.BytesToAddress(meta.GetOriginAddress()),
		ConsumeUserResourcePercent: meta.GetConsumeUserResourcePercent(),
		OriginEnergyLimit:          meta.GetOriginEnergyLimit(),
		Version:                    meta.GetVersion(),
		storageKeyPrefix:           prefix,
		storageKeyHashSlot:         hashSlot,
	}, true
}

// decodeContractRuntimeMetadata scans only the scalar runtime fields and the
// transaction hash used by java-tron's storage-key layout. ABI, bytecode and
// other length-delimited fields are skipped without allocating.
func decodeContractRuntimeMetadata(addr tcommon.Address, data []byte) (ContractRuntimeMetadata, error) {
	var (
		meta    ContractRuntimeMetadata
		trxHash []byte
	)
	for len(data) > 0 {
		number, wireType, tagSize := protowire.ConsumeTag(data)
		if tagSize < 0 {
			return ContractRuntimeMetadata{}, protowire.ParseError(tagSize)
		}
		data = data[tagSize:]
		switch number {
		case 1: // origin_address
			if wireType != protowire.BytesType {
				return ContractRuntimeMetadata{}, fmt.Errorf("contract metadata field %d has wire type %d, want bytes", number, wireType)
			}
			value, size := protowire.ConsumeBytes(data)
			if size < 0 {
				return ContractRuntimeMetadata{}, protowire.ParseError(size)
			}
			meta.OriginAddress = tcommon.BytesToAddress(value)
			data = data[size:]
		case 6, 8, 11: // resource percent, origin limit, version
			if wireType != protowire.VarintType {
				return ContractRuntimeMetadata{}, fmt.Errorf("contract metadata field %d has wire type %d, want varint", number, wireType)
			}
			value, size := protowire.ConsumeVarint(data)
			if size < 0 {
				return ContractRuntimeMetadata{}, protowire.ParseError(size)
			}
			switch number {
			case 6:
				meta.ConsumeUserResourcePercent = int64(value)
			case 8:
				meta.OriginEnergyLimit = int64(value)
			case 11:
				meta.Version = int32(value)
			}
			data = data[size:]
		case 10: // trx_hash
			if wireType != protowire.BytesType {
				return ContractRuntimeMetadata{}, fmt.Errorf("contract metadata field %d has wire type %d, want bytes", number, wireType)
			}
			value, size := protowire.ConsumeBytes(data)
			if size < 0 {
				return ContractRuntimeMetadata{}, protowire.ParseError(size)
			}
			trxHash = value
			data = data[size:]
		default:
			size := protowire.ConsumeFieldValue(number, wireType, data)
			if size < 0 {
				return ContractRuntimeMetadata{}, protowire.ParseError(size)
			}
			data = data[size:]
		}
	}
	meta.storageKeyPrefix, meta.storageKeyHashSlot = javaStorageKeyLayoutFields(addr, trxHash, meta.Version)
	return meta, nil
}

// ContractRuntime returns the lightweight metadata used by hot execution
// paths. It never materializes SmartContract's ABI graph for clean state.
func (s *StateDB) ContractRuntime(addr tcommon.Address) (ContractRuntimeMetadata, bool) {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted {
		return ContractRuntimeMetadata{}, false
	}
	return s.contractRuntime(obj)
}

func (s *StateDB) contractRuntime(obj *stateObject) (ContractRuntimeMetadata, bool) {
	if obj == nil || obj.deleted {
		return ContractRuntimeMetadata{}, false
	}
	// A materialized or dirty protobuf may be mutated in place before
	// SetContract is called. Derive from the live object instead of caching it.
	if obj.contractMeta != nil || obj.contractMetaDirty {
		return contractRuntimeMetadataFromProto(obj.address, obj.contractMeta)
	}
	if obj.contractRuntimeLoaded {
		return obj.contractRuntime, obj.contractRuntimeExists
	}
	data, ok, err := s.getAccountKVForDecoding(obj.address, kvdomains.ContractMetadata, contractMetaKVKey)
	if err != nil {
		return ContractRuntimeMetadata{}, false
	}
	if !ok || len(data) == 0 {
		obj.contractRuntime = ContractRuntimeMetadata{}
		obj.contractRuntimeLoaded = true
		obj.contractRuntimeExists = false
		return ContractRuntimeMetadata{}, false
	}
	meta, err := decodeContractRuntimeMetadata(obj.address, data)
	if err != nil {
		obj.contractRuntime = ContractRuntimeMetadata{}
		obj.contractRuntimeLoaded = true
		obj.contractRuntimeExists = false
		return ContractRuntimeMetadata{}, false
	}
	obj.contractRuntime = meta
	obj.contractRuntimeLoaded = true
	obj.contractRuntimeExists = true
	return meta, true
}
