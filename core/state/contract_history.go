package state

import (
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

// ContractAt reconstructs contract metadata at addr at the end of blockNum.
func (r *PersistentHistoryReader) ContractAt(addr tcommon.Address, blockNum uint64) (*contractpb.SmartContract, error) {
	raw, ok, err := r.contractMetadataAt(addr, blockNum)
	if err != nil || !ok || len(raw) == 0 {
		return nil, err
	}
	sc := &contractpb.SmartContract{}
	if err := proto.Unmarshal(raw, sc); err != nil {
		return nil, fmt.Errorf("decode contract metadata at block %d: %w", blockNum, err)
	}
	return sc, nil
}

func (r *PersistentHistoryReader) contractMetadataAt(addr tcommon.Address, blockNum uint64) ([]byte, bool, error) {
	if r == nil {
		return nil, false, nil
	}
	var (
		accountData   []byte
		accountExists bool
		err           error
		headLatest    hotStateLatestReader
	)
	if blockNum >= r.headNum {
		headLatest, err = r.latestAtHead()
		if err != nil {
			return nil, false, err
		}
		accountData, accountExists, err = headLatest.AccountLatest(addr)
		if err != nil {
			return nil, false, err
		}
	} else {
		ok, err := r.stateDomainHistoryAvailable()
		if err != nil || !ok {
			return nil, false, err
		}
		accountData, accountExists, err = r.readStateAccountLatestAsOf(addr, blockNum, r.headNum)
		if err != nil {
			return nil, false, err
		}
	}
	if !accountExists {
		return r.AccountKVAt(addr, kvdomains.ContractMetadata, contractMetaKVKey, blockNum)
	}
	envelope, err := DecodeStateAccountV2(accountData)
	if err != nil {
		return nil, false, fmt.Errorf("decode contract account at block %d: %w", blockNum, err)
	}
	if blockNum >= r.headNum {
		return headLatest.KVLatest(addr, envelope.AccountKVGeneration, kvdomains.ContractMetadata, contractMetaKVKey)
	}
	return r.readStateKVAsOf(addr, envelope.AccountKVGeneration, kvdomains.ContractMetadata, contractMetaKVKey, blockNum, r.headNum)
}
