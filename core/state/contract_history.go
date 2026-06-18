package state

import (
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

// ContractAt reconstructs contract metadata at addr at the end of blockNum.
func (r *PersistentHistoryReader) ContractAt(addr tcommon.Address, blockNum uint64) (*contractpb.SmartContract, error) {
	raw, ok, err := r.AccountKVAt(addr, kvdomains.ContractMetadata, contractMetaKVKey, blockNum)
	if err != nil || !ok || len(raw) == 0 {
		return nil, err
	}
	sc := &contractpb.SmartContract{}
	if err := proto.Unmarshal(raw, sc); err != nil {
		return nil, nil
	}
	return sc, nil
}
