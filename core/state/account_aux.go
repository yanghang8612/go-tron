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

func invalidateAccountKVMaterialization(obj *stateObject, domain kvdomains.KVDomain) {
	if obj == nil {
		return
	}
	if domain == kvdomains.ContractMetadata {
		// Generic domain writers are also allowed to replace the canonical
		// metadata row. Drop both full and lightweight materializations so the
		// next reader observes the dirty overlay and recomputes storage layout.
		obj.contractMeta = nil
		obj.contractMetaDirty = false
		obj.contractRuntime = ContractRuntimeMetadata{}
		obj.contractRuntimeLoaded = false
		obj.contractRuntimeExists = false
		obj.invalidateStorageKeyLayout()
		return
	}
	// All split Account domains are deliberately allocated as one contiguous
	// range. Keep the generic-KV write path cheap for contract/system domains,
	// which are substantially hotter than direct writes to these auxiliary rows.
	if domain < kvdomains.AccountPermissionAux || domain > kvdomains.AccountTronPowerAux {
		return
	}
	if domain == kvdomains.AccountPermissionAux {
		obj.accountPermissionsLoaded = false
		clearAccountPermissionCaches(obj)
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
		clearAccountFrozenBandwidthCache(obj)
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

func isTRC10BalanceDomain(domain kvdomains.KVDomain) bool {
	return domain == kvdomains.AccountAsset || domain == kvdomains.AccountAssetV2
}

func clearTRC10PointCache(obj *stateObject) {
	if obj == nil {
		return
	}
	obj.trc10PointDomain = 0
	obj.trc10PointKey = ""
	obj.trc10PointValue = 0
	obj.trc10PointExists = false
	obj.trc10PointLoaded = false
}

func trc10PointValue(obj *stateObject, domain kvdomains.KVDomain, key []byte) (int64, bool, bool) {
	if obj == nil || !obj.trc10PointLoaded || obj.trc10PointDomain != domain || obj.trc10PointKey != string(key) {
		return 0, false, false
	}
	return obj.trc10PointValue, obj.trc10PointExists, true
}

func cacheTRC10PointValue(obj *stateObject, domain kvdomains.KVDomain, key []byte, value int64, exists bool) {
	if obj == nil {
		return
	}
	obj.trc10PointDomain = domain
	obj.trc10PointKey = string(key)
	obj.trc10PointValue = value
	obj.trc10PointExists = exists
	obj.trc10PointLoaded = true
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
	if isTRC10BalanceDomain(domain) {
		obj := s.getStateObject(addr)
		if obj == nil || obj.deleted {
			return 0, false, nil
		}
		if entry, dirty := lookupKVEntry(obj.kvDirty, domain, key); dirty {
			if entry.deleted {
				return 0, false, nil
			}
			decoded, err := decodeAccountAuxInt64(entry.val)
			return decoded, true, err
		}
		if value, exists, cached := trc10PointValue(obj, domain, key); cached {
			return value, exists, nil
		}
		value, exists, err := s.readAccountKVLatestForDecoding(addr, obj.accountKVGeneration, domain, key)
		if err != nil {
			return 0, false, err
		}
		if !exists {
			cacheTRC10PointValue(obj, domain, key, 0, false)
			return 0, false, nil
		}
		decoded, err := decodeAccountAuxInt64(value)
		if err != nil {
			return 0, true, err
		}
		cacheTRC10PointValue(obj, domain, key, decoded, true)
		return decoded, true, nil
	}
	value, ok, err := s.getAccountKVForDecoding(addr, domain, key)
	if err != nil || !ok {
		return 0, ok, err
	}
	decoded, err := decodeAccountAuxInt64(value)
	return decoded, true, err
}

func (s *StateDB) setAccountAuxValue(addr tcommon.Address, domain kvdomains.KVDomain, key []byte, value int64) error {
	encoded := encodeAccountAuxInt64(value)
	var err error
	if obj := s.stateObjects[addr]; isTRC10BalanceDomain(domain) && obj != nil {
		if previous, exists, cached := trc10PointValue(obj, domain, key); cached {
			var previousEncoded []byte
			if exists {
				previousEncoded = encodeAccountAuxInt64(previous)
			}
			err = s.setAccountKVWithPrev(addr, domain, key, encoded, true, previousEncoded, exists, true)
		} else {
			err = s.SetAccountKV(addr, domain, key, encoded)
		}
	} else {
		err = s.SetAccountKV(addr, domain, key, encoded)
	}
	if err != nil {
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

func (s *StateDB) trc10Balance(addr tcommon.Address, domain kvdomains.KVDomain, key []byte) int64 {
	value, _, err := s.accountAuxValue(addr, domain, key)
	if err != nil {
		return 0
	}
	return value
}

func (s *StateDB) setTRC10BalanceKey(addr tcommon.Address, domain kvdomains.KVDomain, key []byte, amount int64) {
	_ = s.setAccountAuxValue(addr, domain, key, amount)
}

func (s *StateDB) trc10TokenKey(tokenID int64) []byte {
	return strconv.AppendInt(s.trc10TokenKeyScratch[:0], tokenID, 10)
}
