package state

import (
	"encoding/binary"
	"fmt"
	"strconv"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

var accountAuxDomains = [...]kvdomains.KVDomain{
	kvdomains.AccountAsset,
	kvdomains.AccountAssetV2,
	kvdomains.AccountFreeAssetNetUsage,
	kvdomains.AccountFreeAssetNetUsageV2,
	kvdomains.AccountAssetOperationTime,
	kvdomains.AccountAssetOperationTimeV2,
}

var accountSplitDomains = [...]kvdomains.KVDomain{
	kvdomains.AccountAsset,
	kvdomains.AccountAssetV2,
	kvdomains.AccountFreeAssetNetUsage,
	kvdomains.AccountFreeAssetNetUsageV2,
	kvdomains.AccountAssetOperationTime,
	kvdomains.AccountAssetOperationTimeV2,
	kvdomains.AccountPermissionAux,
	kvdomains.AccountVotesAux,
	kvdomains.AccountFrozenV2Aux,
	kvdomains.AccountUnfrozenV2Aux,
	kvdomains.AccountFrozenSupplyAux,
	kvdomains.AccountResourceAux,
	kvdomains.AccountFrozenBandwidthAux,
	kvdomains.AccountTronPowerAux,
}

func isAccountSplitDomain(domain kvdomains.KVDomain) bool {
	for _, candidate := range accountSplitDomains {
		if candidate == domain {
			return true
		}
	}
	return false
}

func invalidateAccountSplitMaterialization(obj *stateObject, domain kvdomains.KVDomain) {
	// All split Account domains are deliberately allocated as one contiguous
	// range. Keep the generic-KV write path cheap for contract/system domains,
	// which are substantially hotter than direct writes to these auxiliary rows.
	if obj == nil || domain < kvdomains.AccountPermissionAux || domain > kvdomains.AccountTronPowerAux {
		return
	}
	if domain == kvdomains.AccountPermissionAux {
		obj.accountPermissionsLoaded = false
		return
	}
	if domain == kvdomains.AccountVotesAux {
		obj.accountVotesLoaded = false
		return
	}
	if domain == kvdomains.AccountFrozenV2Aux {
		obj.accountStakeV2Loaded = false
		clearAccountFrozenV2PointCache(obj)
		return
	}
	if domain == kvdomains.AccountUnfrozenV2Aux {
		obj.accountStakeV2Loaded = false
		return
	}
	if domain == kvdomains.AccountFrozenSupplyAux {
		obj.accountFrozenSupplyLoaded = false
		return
	}
	if domain == kvdomains.AccountResourceAux {
		obj.accountResourceLoaded = false
		return
	}
	if domain == kvdomains.AccountFrozenBandwidthAux {
		if obj.account != nil {
			obj.account.Proto().Frozen = nil
		}
		obj.accountFrozenBandwidthLoaded = false
		return
	}
	if domain == kvdomains.AccountTronPowerAux {
		if obj.account != nil {
			obj.account.Proto().TronPower = nil
		}
		obj.accountTronPowerLoaded = false
		return
	}
	for _, candidate := range accountAuxDomains {
		if domain == candidate {
			obj.accountMapsLoaded = false
			return
		}
	}
}

func encodeAccountAuxInt64(value int64) []byte {
	var out [8]byte
	binary.BigEndian.PutUint64(out[:], uint64(value))
	return out[:]
}

func decodeAccountAuxInt64(value []byte) (int64, error) {
	if len(value) != 8 {
		return 0, fmt.Errorf("account auxiliary int64 length %d, want 8", len(value))
	}
	return int64(binary.BigEndian.Uint64(value)), nil
}

func clearAccountAuxProto(pb *corepb.Account) {
	if pb == nil {
		return
	}
	pb.Asset = nil
	pb.AssetV2 = nil
	pb.FreeAssetNetUsage = nil
	pb.FreeAssetNetUsageV2 = nil
	pb.LatestAssetOperationTime = nil
	pb.LatestAssetOperationTimeV2 = nil
}

func accountAuxMap(pb *corepb.Account, domain kvdomains.KVDomain, create bool) map[string]int64 {
	if pb == nil {
		return nil
	}
	var values *map[string]int64
	switch domain {
	case kvdomains.AccountAsset:
		values = &pb.Asset
	case kvdomains.AccountAssetV2:
		values = &pb.AssetV2
	case kvdomains.AccountFreeAssetNetUsage:
		values = &pb.FreeAssetNetUsage
	case kvdomains.AccountFreeAssetNetUsageV2:
		values = &pb.FreeAssetNetUsageV2
	case kvdomains.AccountAssetOperationTime:
		values = &pb.LatestAssetOperationTime
	case kvdomains.AccountAssetOperationTimeV2:
		values = &pb.LatestAssetOperationTimeV2
	default:
		return nil
	}
	if *values == nil && create {
		*values = make(map[string]int64)
	}
	return *values
}

func (s *StateDB) accountAuxValue(addr tcommon.Address, domain kvdomains.KVDomain, key []byte) (int64, bool, error) {
	value, ok, err := s.getAccountKVForDecoding(addr, domain, key)
	if err != nil || !ok {
		return 0, ok, err
	}
	decoded, err := decodeAccountAuxInt64(value)
	return decoded, true, err
}

func (s *StateDB) setAccountAuxValue(addr tcommon.Address, domain kvdomains.KVDomain, key []byte, value int64) error {
	if err := s.SetAccountKV(addr, domain, key, encodeAccountAuxInt64(value)); err != nil {
		return err
	}
	if obj := s.stateObjects[addr]; obj != nil && obj.account != nil {
		clearAccountAuxProto(obj.account.Proto())
		obj.accountMapsLoaded = false
	}
	return nil
}

func (s *StateDB) materializeAccountAux(obj *stateObject) error {
	if obj == nil || obj.account == nil || obj.accountMapsLoaded {
		return nil
	}
	pb := obj.account.Proto()
	clearAccountAuxProto(pb)
	for _, domain := range accountAuxDomains {
		values := accountAuxMap(pb, domain, true)
		if err := s.IterateAccountKV(obj.address, domain, nil, func(key, value []byte) (bool, error) {
			decoded, err := decodeAccountAuxInt64(value)
			if err != nil {
				return false, err
			}
			values[string(key)] = decoded
			return true, nil
		}); err != nil {
			clearAccountAuxProto(pb)
			return err
		}
	}
	obj.accountMapsLoaded = true
	return nil
}

func (s *StateDB) trc10Balance(addr tcommon.Address, domain kvdomains.KVDomain, key string) int64 {
	value, _, err := s.accountAuxValue(addr, domain, []byte(key))
	if err != nil {
		return 0
	}
	return value
}

func (s *StateDB) setTRC10BalanceKey(addr tcommon.Address, domain kvdomains.KVDomain, key string, amount int64) {
	_ = s.setAccountAuxValue(addr, domain, []byte(key), amount)
}

func trc10TokenKey(tokenID int64) string { return strconv.FormatInt(tokenID, 10) }
