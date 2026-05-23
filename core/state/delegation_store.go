package state

import (
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/proto"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func (s *StateDB) readSystemDelegation(key []byte) ([]byte, bool) {
	raw, ok, err := s.GetAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemDelegation, key)
	if err != nil || !ok {
		return nil, false
	}
	return raw, true
}

func (s *StateDB) writeSystemDelegation(key, value []byte) error {
	return s.SetAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemDelegation, key, value)
}

func (s *StateDB) deleteSystemDelegation(key []byte) error {
	return s.DeleteAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemDelegation, key)
}

func (s *StateDB) ReadDelegatedResource(from, to tcommon.Address) *rawdb.DelegatedResource {
	var out *rawdb.DelegatedResource
	merge := func(dr *rawdb.DelegatedResource) {
		if dr == nil {
			return
		}
		if out == nil {
			out = &rawdb.DelegatedResource{From: from, To: to}
		}
		out.FrozenBalanceForBandwidth += dr.FrozenBalanceForBandwidth
		out.FrozenBalanceForEnergy += dr.FrozenBalanceForEnergy
		if dr.ExpireTimeForBandwidth > out.ExpireTimeForBandwidth {
			out.ExpireTimeForBandwidth = dr.ExpireTimeForBandwidth
		}
		if dr.ExpireTimeForEnergy > out.ExpireTimeForEnergy {
			out.ExpireTimeForEnergy = dr.ExpireTimeForEnergy
		}
	}
	merge(s.ReadDelegatedResourceLegacy(from, to))
	merge(s.ReadDelegatedResourceV2(from, to, false))
	merge(s.ReadDelegatedResourceV2(from, to, true))
	return out
}

func (s *StateDB) ReadDelegatedResourceLegacy(from, to tcommon.Address) *rawdb.DelegatedResource {
	return s.readDelegatedResourceByKey(rawdb.DelegatedResourceStateKey(from, to))
}

func (s *StateDB) ReadDelegatedResourceV2(from, to tcommon.Address, locked bool) *rawdb.DelegatedResource {
	return s.readDelegatedResourceByKey(rawdb.DelegatedResourceV2StateKey(from, to, locked))
}

func (s *StateDB) readDelegatedResourceByKey(key []byte) *rawdb.DelegatedResource {
	data, ok := s.readSystemDelegation(key)
	if !ok || len(data) == 0 {
		return nil
	}
	dr := &rawdb.DelegatedResource{}
	if err := json.Unmarshal(data, dr); err != nil {
		return nil
	}
	return dr
}

func (s *StateDB) WriteDelegatedResourceLegacy(from, to tcommon.Address, dr *rawdb.DelegatedResource) error {
	return s.writeDelegatedResourceByKey(rawdb.DelegatedResourceStateKey(from, to), dr)
}

func (s *StateDB) WriteDelegatedResourceV2(from, to tcommon.Address, locked bool, dr *rawdb.DelegatedResource) error {
	return s.writeDelegatedResourceByKey(rawdb.DelegatedResourceV2StateKey(from, to, locked), dr)
}

func (s *StateDB) writeDelegatedResourceByKey(key []byte, dr *rawdb.DelegatedResource) error {
	data, err := json.Marshal(dr)
	if err != nil {
		return err
	}
	return s.writeSystemDelegation(key, data)
}

func (s *StateDB) DeleteDelegatedResourceLegacy(from, to tcommon.Address) error {
	return s.deleteSystemDelegation(rawdb.DelegatedResourceStateKey(from, to))
}

func (s *StateDB) DeleteDelegatedResourceV2(from, to tcommon.Address, locked bool) error {
	return s.deleteSystemDelegation(rawdb.DelegatedResourceV2StateKey(from, to, locked))
}

func (s *StateDB) UnlockExpiredDelegatedResource(from, to tcommon.Address, now int64) error {
	lockResource := s.ReadDelegatedResourceV2(from, to, true)
	if lockResource == nil {
		return nil
	}
	if lockResource.ExpireTimeForEnergy >= now && lockResource.ExpireTimeForBandwidth >= now {
		return nil
	}

	unlockResource := s.ReadDelegatedResourceV2(from, to, false)
	if unlockResource == nil {
		unlockResource = &rawdb.DelegatedResource{From: from, To: to}
	}
	if lockResource.ExpireTimeForEnergy < now {
		unlockResource.FrozenBalanceForEnergy += lockResource.FrozenBalanceForEnergy
		lockResource.FrozenBalanceForEnergy = 0
		lockResource.ExpireTimeForEnergy = 0
	}
	if lockResource.ExpireTimeForBandwidth < now {
		unlockResource.FrozenBalanceForBandwidth += lockResource.FrozenBalanceForBandwidth
		lockResource.FrozenBalanceForBandwidth = 0
		lockResource.ExpireTimeForBandwidth = 0
	}
	if lockResource.FrozenBalanceForBandwidth == 0 && lockResource.FrozenBalanceForEnergy == 0 {
		if err := s.DeleteDelegatedResourceV2(from, to, true); err != nil {
			return err
		}
	} else if err := s.WriteDelegatedResourceV2(from, to, true, lockResource); err != nil {
		return err
	}
	return s.WriteDelegatedResourceV2(from, to, false, unlockResource)
}

func (s *StateDB) WriteDelegationIndex(from tcommon.Address, receivers []tcommon.Address) error {
	buf := make([]byte, tcommon.AddressLength*len(receivers))
	for i, r := range receivers {
		copy(buf[i*tcommon.AddressLength:], r[:])
	}
	return s.writeSystemDelegation(rawdb.DelegationIndexStateKey(from), buf)
}

func (s *StateDB) ReadDelegationIndex(from tcommon.Address) []tcommon.Address {
	data, ok := s.readSystemDelegation(rawdb.DelegationIndexStateKey(from))
	if !ok || len(data) == 0 {
		return nil
	}
	count := len(data) / tcommon.AddressLength
	addrs := make([]tcommon.Address, count)
	for i := range addrs {
		copy(addrs[i][:], data[i*tcommon.AddressLength:])
	}
	return addrs
}

func (s *StateDB) ReadDrAccountIndexLegacy(account []byte) *corepb.DelegatedResourceAccountIndex {
	data, ok := s.readSystemDelegation(rawdb.DrAccountIndexLegacyStateKey(account))
	if !ok || len(data) == 0 {
		return nil
	}
	var rec corepb.DelegatedResourceAccountIndex
	if err := proto.Unmarshal(data, &rec); err != nil {
		return nil
	}
	return &rec
}

func (s *StateDB) writeDrAccountIndexLegacy(account []byte, rec *corepb.DelegatedResourceAccountIndex) error {
	data, err := proto.Marshal(rec)
	if err != nil {
		return fmt.Errorf("dr account index: marshal legacy: %w", err)
	}
	return s.writeSystemDelegation(rawdb.DrAccountIndexLegacyStateKey(account), data)
}

func (s *StateDB) deleteDrAccountIndexLegacy(account []byte) error {
	return s.deleteSystemDelegation(rawdb.DrAccountIndexLegacyStateKey(account))
}

func (s *StateDB) WriteDrAccountIndexLegacyDelegate(from, to []byte) error {
	if len(from) == 0 || len(to) == 0 {
		return fmt.Errorf("dr account index: empty address (from=%d to=%d)", len(from), len(to))
	}
	fromRec := s.ReadDrAccountIndexLegacy(from)
	if fromRec == nil {
		fromRec = &corepb.DelegatedResourceAccountIndex{Account: append([]byte(nil), from...)}
	}
	fromRec.ToAccounts = appendUniqueDelegationAccount(fromRec.ToAccounts, to)
	if err := s.writeDrAccountIndexLegacy(from, fromRec); err != nil {
		return err
	}

	toRec := s.ReadDrAccountIndexLegacy(to)
	if toRec == nil {
		toRec = &corepb.DelegatedResourceAccountIndex{Account: append([]byte(nil), to...)}
	}
	toRec.FromAccounts = appendUniqueDelegationAccount(toRec.FromAccounts, from)
	return s.writeDrAccountIndexLegacy(to, toRec)
}

func (s *StateDB) WriteDrAccountIndexLegacyUnDelegate(from, to []byte) error {
	if len(from) == 0 || len(to) == 0 {
		return fmt.Errorf("dr account index: empty address")
	}
	if fromRec := s.ReadDrAccountIndexLegacy(from); fromRec != nil {
		fromRec.ToAccounts = removeDelegationAccount(fromRec.ToAccounts, to)
		if err := s.writeDrAccountIndexLegacy(from, fromRec); err != nil {
			return err
		}
	}
	if toRec := s.ReadDrAccountIndexLegacy(to); toRec != nil {
		toRec.FromAccounts = removeDelegationAccount(toRec.FromAccounts, from)
		return s.writeDrAccountIndexLegacy(to, toRec)
	}
	return nil
}

func (s *StateDB) ConvertDrAccountIndexLegacy(account []byte) error {
	rec := s.ReadDrAccountIndexLegacy(account)
	if rec == nil {
		return nil
	}
	for i, to := range rec.ToAccounts {
		if err := s.WriteDrAccountIndexDelegate(false, account, to, int64(i+1)); err != nil {
			return err
		}
	}
	for i, from := range rec.FromAccounts {
		if err := s.WriteDrAccountIndexDelegate(false, from, account, int64(i+1)); err != nil {
			return err
		}
	}
	return s.deleteDrAccountIndexLegacy(account)
}

func (s *StateDB) WriteDrAccountIndexDelegate(v2 bool, from, to []byte, timestamp int64) error {
	if len(from) == 0 || len(to) == 0 {
		return fmt.Errorf("dr account index: empty address (from=%d to=%d)", len(from), len(to))
	}
	fromDir, toDir := rawdb.DrAccIdxV1From, rawdb.DrAccIdxV1To
	if v2 {
		fromDir, toDir = rawdb.DrAccIdxV2From, rawdb.DrAccIdxV2To
	}

	fromPayload, err := proto.Marshal(&corepb.DelegatedResourceAccountIndex{
		Account:   append([]byte(nil), to...),
		Timestamp: timestamp,
	})
	if err != nil {
		return fmt.Errorf("dr account index: marshal from: %w", err)
	}
	if err := s.writeSystemDelegation(rawdb.DrAccountIndexStateKey(fromDir, from, to), fromPayload); err != nil {
		return err
	}

	toPayload, err := proto.Marshal(&corepb.DelegatedResourceAccountIndex{
		Account:   append([]byte(nil), from...),
		Timestamp: timestamp,
	})
	if err != nil {
		return fmt.Errorf("dr account index: marshal to: %w", err)
	}
	return s.writeSystemDelegation(rawdb.DrAccountIndexStateKey(toDir, to, from), toPayload)
}

func (s *StateDB) WriteDrAccountIndexUnDelegate(v2 bool, from, to []byte) error {
	if len(from) == 0 || len(to) == 0 {
		return fmt.Errorf("dr account index: empty address")
	}
	fromDir, toDir := rawdb.DrAccIdxV1From, rawdb.DrAccIdxV1To
	if v2 {
		fromDir, toDir = rawdb.DrAccIdxV2From, rawdb.DrAccIdxV2To
	}
	if err := s.deleteSystemDelegation(rawdb.DrAccountIndexStateKey(fromDir, from, to)); err != nil {
		return err
	}
	return s.deleteSystemDelegation(rawdb.DrAccountIndexStateKey(toDir, to, from))
}

func (s *StateDB) ReadDrAccountIndexEntry(dir rawdb.DrAccIdxDirection, anchor, counterparty []byte) *corepb.DelegatedResourceAccountIndex {
	data, ok := s.readSystemDelegation(rawdb.DrAccountIndexStateKey(dir, anchor, counterparty))
	if !ok || len(data) == 0 {
		return nil
	}
	var rec corepb.DelegatedResourceAccountIndex
	if err := proto.Unmarshal(data, &rec); err != nil {
		return nil
	}
	return &rec
}

func (s *StateDB) IterateDrAccountIndex(dir rawdb.DrAccIdxDirection, anchor []byte, fn func(counterparty []byte, rec *corepb.DelegatedResourceAccountIndex) error) error {
	prefix := rawdb.DrAccountIndexAnchorStatePrefix(dir, anchor)
	return s.IterateAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemDelegation, prefix, func(key, value []byte) (bool, error) {
		counterparty := append([]byte(nil), key[len(prefix):]...)
		var rec corepb.DelegatedResourceAccountIndex
		if err := proto.Unmarshal(value, &rec); err != nil {
			return false, fmt.Errorf("dr account index: decode %x: %w", key, err)
		}
		if err := fn(counterparty, &rec); err != nil {
			return false, err
		}
		return true, nil
	})
}

func appendUniqueDelegationAccount(list [][]byte, account []byte) [][]byte {
	for _, existing := range list {
		if string(existing) == string(account) {
			return list
		}
	}
	return append(list, append([]byte(nil), account...))
}

func removeDelegationAccount(list [][]byte, account []byte) [][]byte {
	out := list[:0]
	for _, existing := range list {
		if string(existing) != string(account) {
			out = append(out, existing)
		}
	}
	return out
}
