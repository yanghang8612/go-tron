package state

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

var accountTronPowerKey = []byte{0x00}

type accountFrozenBandwidthRow struct {
	key   []byte
	index uint32
	entry *corepb.Account_Frozen
}

func accountFrozenBandwidthKey(index uint32) []byte {
	var key [4]byte
	binary.BigEndian.PutUint32(key[:], index)
	return key[:]
}

func decodeAccountFrozenBandwidthRow(key, value []byte) (accountFrozenBandwidthRow, error) {
	if len(key) != 4 {
		return accountFrozenBandwidthRow{}, fmt.Errorf("account frozen-bandwidth key length %d, want 4", len(key))
	}
	var entry corepb.Account_Frozen
	if err := proto.Unmarshal(value, &entry); err != nil {
		return accountFrozenBandwidthRow{}, fmt.Errorf("decode account frozen-bandwidth %x: %w", key, err)
	}
	return accountFrozenBandwidthRow{
		key:   append([]byte(nil), key...),
		index: binary.BigEndian.Uint32(key),
		entry: &entry,
	}, nil
}

func decodeAccountTronPower(key, value []byte) (*corepb.Account_Frozen, error) {
	if !bytes.Equal(key, accountTronPowerKey) {
		return nil, fmt.Errorf("account tron-power key %x, want %x", key, accountTronPowerKey)
	}
	var entry corepb.Account_Frozen
	if err := proto.Unmarshal(value, &entry); err != nil {
		return nil, fmt.Errorf("decode account tron-power: %w", err)
	}
	return &entry, nil
}

func clearAccountStakeV1Proto(pb *corepb.Account) {
	if pb == nil {
		return
	}
	pb.Frozen = nil
	pb.TronPower = nil
}

func cacheAccountFrozenBandwidth(obj *stateObject, entries []*corepb.Account_Frozen) {
	if obj == nil || obj.account == nil {
		return
	}
	pb := obj.account.Proto()
	pb.Frozen = nil
	for _, entry := range entries {
		if entry != nil {
			pb.Frozen = append(pb.Frozen, proto.Clone(entry).(*corepb.Account_Frozen))
		}
	}
	obj.accountFrozenBandwidthLoaded = true
}

func cacheAccountTronPower(obj *stateObject, entry *corepb.Account_Frozen) {
	if obj == nil || obj.account == nil {
		return
	}
	obj.account.Proto().TronPower = nil
	if entry != nil {
		obj.account.Proto().TronPower = proto.Clone(entry).(*corepb.Account_Frozen)
	}
	obj.accountTronPowerLoaded = true
}

func (s *StateDB) accountFrozenBandwidthRows(obj *stateObject) ([]accountFrozenBandwidthRow, error) {
	if obj == nil || obj.account == nil {
		return nil, nil
	}
	rows := make([]accountFrozenBandwidthRow, 0, 2)
	// Historical partial unfreezes delete only the expired physical row so
	// temporal history records exactly that mutation. The remaining indexes may
	// therefore be sparse; a stop-at-first-miss point loop would silently lose a
	// live row after reopen. Keep the prefix read for this legacy repeated field.
	if err := s.IterateAccountKV(obj.address, kvdomains.AccountFrozenBandwidthAux, nil, func(key, value []byte) (bool, error) {
		row, err := decodeAccountFrozenBandwidthRow(key, value)
		if err != nil {
			return false, err
		}
		rows = append(rows, row)
		return true, nil
	}); err != nil {
		return nil, err
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].index < rows[j].index })
	return rows, nil
}

func (s *StateDB) accountFrozenBandwidthRowAt(obj *stateObject, index uint32) (accountFrozenBandwidthRow, bool, error) {
	if obj == nil || obj.account == nil {
		return accountFrozenBandwidthRow{}, false, nil
	}
	key := accountFrozenBandwidthKey(index)
	value, exists, err := s.GetAccountKV(obj.address, kvdomains.AccountFrozenBandwidthAux, key)
	if err != nil || !exists {
		return accountFrozenBandwidthRow{}, exists, err
	}
	row, err := decodeAccountFrozenBandwidthRow(key, value)
	if err != nil {
		return accountFrozenBandwidthRow{}, false, err
	}
	return row, true, nil
}

// accountFrozenBandwidthFastRows point-reads java-tron's valid V1 shape
// (MAX_FROZEN_NUMBER == 1). Index 1 is probed as a compatibility guard: when a
// migrated/synthetic account contains multiple or sparse rows, fall back to the
// generic iterator so no stored state is lost.
func (s *StateDB) accountFrozenBandwidthFastRows(obj *stateObject) ([]accountFrozenBandwidthRow, error) {
	first, firstExists, err := s.accountFrozenBandwidthRowAt(obj, 0)
	if err != nil {
		return nil, err
	}
	_, secondExists, err := s.accountFrozenBandwidthRowAt(obj, 1)
	if err != nil {
		return nil, err
	}
	if secondExists {
		return s.accountFrozenBandwidthRows(obj)
	}
	if !firstExists {
		return nil, nil
	}
	return []accountFrozenBandwidthRow{first}, nil
}

func (s *StateDB) accountTronPower(obj *stateObject) (*corepb.Account_Frozen, bool, error) {
	if obj == nil || obj.account == nil {
		return nil, false, nil
	}
	value, exists, err := s.GetAccountKV(obj.address, kvdomains.AccountTronPowerAux, accountTronPowerKey)
	if err != nil || !exists {
		return nil, exists, err
	}
	entry, err := decodeAccountTronPower(accountTronPowerKey, value)
	if err != nil {
		return nil, false, err
	}
	return entry, true, nil
}

func (s *StateDB) materializeAccountFrozenBandwidth(obj *stateObject) error {
	if obj == nil || obj.account == nil || obj.accountFrozenBandwidthLoaded {
		return nil
	}
	rows, err := s.accountFrozenBandwidthRows(obj)
	if err != nil {
		obj.account.Proto().Frozen = nil
		return err
	}
	entries := make([]*corepb.Account_Frozen, 0, len(rows))
	for _, row := range rows {
		entries = append(entries, row.entry)
	}
	cacheAccountFrozenBandwidth(obj, entries)
	return nil
}

func (s *StateDB) materializeAccountTronPower(obj *stateObject) error {
	if obj == nil || obj.account == nil || obj.accountTronPowerLoaded {
		return nil
	}
	tronPower, exists, err := s.accountTronPower(obj)
	if err != nil {
		obj.account.Proto().TronPower = nil
		return err
	}
	if !exists {
		tronPower = nil
	}
	cacheAccountTronPower(obj, tronPower)
	return nil
}

func (s *StateDB) materializeAccountStakeV1(obj *stateObject) error {
	if err := s.materializeAccountFrozenBandwidth(obj); err != nil {
		return err
	}
	return s.materializeAccountTronPower(obj)
}

func (s *StateDB) writeAccountFrozenBandwidth(obj *stateObject, entries []*corepb.Account_Frozen) error {
	if obj == nil || obj.account == nil {
		return nil
	}
	existing, err := s.accountFrozenBandwidthRows(obj)
	if err != nil {
		return err
	}
	return s.writeAccountFrozenBandwidthReplacing(obj, existing, entries)
}

// writeAccountFrozenBandwidthReplacing replaces existing with entries. Callers
// that already read the rows to calculate a total can reuse them and avoid a
// second prefix iterator over the blockbuffer overlay.
func (s *StateDB) writeAccountFrozenBandwidthReplacing(obj *stateObject, existing []accountFrozenBandwidthRow, entries []*corepb.Account_Frozen) error {
	if obj == nil || obj.account == nil {
		return nil
	}
	for _, row := range existing {
		if err := s.DeleteAccountKV(obj.address, kvdomains.AccountFrozenBandwidthAux, row.key); err != nil {
			return err
		}
	}
	nonNil := uint64(0)
	for _, entry := range entries {
		if entry != nil {
			nonNil++
		}
	}
	if nonNil > uint64(math.MaxUint32)+1 {
		return fmt.Errorf("account frozen-bandwidth index overflow")
	}
	var index uint32
	for _, entry := range entries {
		if entry == nil {
			continue
		}
		value, err := proto.MarshalOptions{Deterministic: true}.Marshal(entry)
		if err != nil {
			return err
		}
		if err := s.SetAccountKV(obj.address, kvdomains.AccountFrozenBandwidthAux, accountFrozenBandwidthKey(index), value); err != nil {
			return err
		}
		index++
	}
	cacheAccountFrozenBandwidth(obj, entries)
	return nil
}

func (s *StateDB) setAccountFrozenBandwidth(obj *stateObject, amount, expireTimeMs int64) error {
	return s.writeAccountFrozenBandwidth(obj, []*corepb.Account_Frozen{{
		FrozenBalance: amount,
		ExpireTime:    expireTimeMs,
	}})
}

func (s *StateDB) accountFrozenBandwidthTotal(obj *stateObject) (int64, error) {
	if obj == nil || obj.account == nil {
		return 0, nil
	}
	rows, err := s.accountFrozenBandwidthRows(obj)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, row := range rows {
		total += row.entry.FrozenBalance
	}
	return total, nil
}

// FrozenV1BandwidthCount returns the number of legacy bandwidth-freeze rows
// without materializing any unrelated split account domain.
func (s *StateDB) FrozenV1BandwidthCount(addr tcommon.Address) int {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted {
		return 0
	}
	rows, err := s.accountFrozenBandwidthFastRows(obj)
	if err != nil {
		return 0
	}
	return len(rows)
}

// HasExpiredFrozenV1Bandwidth reports whether any legacy bandwidth freeze may
// be released at now. It examines only the bandwidth split domain.
func (s *StateDB) HasExpiredFrozenV1Bandwidth(addr tcommon.Address, now int64) bool {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted {
		return false
	}
	rows, err := s.accountFrozenBandwidthFastRows(obj)
	if err != nil {
		return false
	}
	for _, row := range rows {
		if row.entry.ExpireTime <= now {
			return true
		}
	}
	return false
}

// FrozenV1ResourceInfo returns the amount and expiry for a single-slot legacy
// ENERGY or TRON_POWER freeze without materializing other account domains.
func (s *StateDB) FrozenV1ResourceInfo(addr tcommon.Address, resource corepb.ResourceCode) (amount, expireTime int64) {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted {
		return 0, 0
	}
	switch resource {
	case corepb.ResourceCode_ENERGY:
		if err := s.materializeAccountResource(obj); err != nil {
			return 0, 0
		}
		return obj.account.FrozenEnergyAmount(), obj.account.FrozenEnergyExpireTime()
	case corepb.ResourceCode_TRON_POWER:
		entry, exists, err := s.accountTronPower(obj)
		if err != nil || !exists || entry == nil {
			return 0, 0
		}
		return entry.FrozenBalance, entry.ExpireTime
	default:
		return 0, 0
	}
}

// FrozenV1ResourceAmount returns the owner-side legacy frozen amount for one
// resource. It loads only the corresponding V1 split domain.
func (s *StateDB) FrozenV1ResourceAmount(addr tcommon.Address, resource corepb.ResourceCode) int64 {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted {
		return 0
	}
	switch resource {
	case corepb.ResourceCode_BANDWIDTH:
		rows, err := s.accountFrozenBandwidthFastRows(obj)
		if err != nil {
			return 0
		}
		var amount int64
		for _, row := range rows {
			amount += row.entry.FrozenBalance
		}
		return amount
	case corepb.ResourceCode_ENERGY:
		if err := s.materializeAccountResource(obj); err != nil {
			return 0
		}
		return obj.account.FrozenEnergyAmount()
	case corepb.ResourceCode_TRON_POWER:
		entry, exists, err := s.accountTronPower(obj)
		if err != nil || !exists || entry == nil {
			return 0
		}
		return entry.FrozenBalance
	default:
		return 0
	}
}

// AcquiredDelegatedFrozenV1Amount returns the receiver-side legacy delegated
// amount while loading only the account envelope (bandwidth) or resource row
// (energy).
func (s *StateDB) AcquiredDelegatedFrozenV1Amount(addr tcommon.Address, resource corepb.ResourceCode) int64 {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted {
		return 0
	}
	switch resource {
	case corepb.ResourceCode_BANDWIDTH:
		return obj.account.AcquiredDelegatedFrozenBandwidth()
	case corepb.ResourceCode_ENERGY:
		if err := s.materializeAccountResource(obj); err != nil {
			return 0
		}
		return obj.account.AcquiredDelegatedFrozenEnergy()
	default:
		return 0
	}
}

func (s *StateDB) accountFrozenBandwidthMaxExpire(obj *stateObject) (int64, error) {
	rows, err := s.accountFrozenBandwidthRows(obj)
	if err != nil {
		return 0, err
	}
	var maxExpire int64
	for _, row := range rows {
		if row.entry.ExpireTime > maxExpire {
			maxExpire = row.entry.ExpireTime
		}
	}
	return maxExpire, nil
}

func (s *StateDB) removeExpiredAccountFrozenBandwidth(obj *stateObject, now int64) (int64, error) {
	rows, err := s.accountFrozenBandwidthRows(obj)
	if err != nil {
		return 0, err
	}
	var refunded int64
	removed := false
	for _, row := range rows {
		if row.entry.ExpireTime > now {
			continue
		}
		if err := s.DeleteAccountKV(obj.address, kvdomains.AccountFrozenBandwidthAux, row.key); err != nil {
			return 0, err
		}
		refunded += row.entry.FrozenBalance
		removed = true
	}
	if removed {
		remaining := make([]*corepb.Account_Frozen, 0, len(rows))
		for _, row := range rows {
			if row.entry.ExpireTime > now {
				remaining = append(remaining, row.entry)
			}
		}
		cacheAccountFrozenBandwidth(obj, remaining)
	}
	return refunded, nil
}

func (s *StateDB) writeAccountTronPower(obj *stateObject, entry *corepb.Account_Frozen) error {
	if obj == nil || obj.account == nil {
		return nil
	}
	if entry == nil {
		if err := s.DeleteAccountKV(obj.address, kvdomains.AccountTronPowerAux, accountTronPowerKey); err != nil {
			return err
		}
		cacheAccountTronPower(obj, nil)
		return nil
	}
	value, err := proto.MarshalOptions{Deterministic: true}.Marshal(entry)
	if err != nil {
		return err
	}
	if err := s.SetAccountKV(obj.address, kvdomains.AccountTronPowerAux, accountTronPowerKey, value); err != nil {
		return err
	}
	cacheAccountTronPower(obj, entry)
	return nil
}

func (s *StateDB) addAccountTronPower(obj *stateObject, amount, expireTimeMs int64) error {
	entry, exists, err := s.accountTronPower(obj)
	if err != nil {
		return err
	}
	if !exists {
		entry = &corepb.Account_Frozen{}
	}
	entry.FrozenBalance += amount
	if expireTimeMs > entry.ExpireTime {
		entry.ExpireTime = expireTimeMs
	}
	return s.writeAccountTronPower(obj, entry)
}

func (s *StateDB) removeExpiredAccountTronPower(obj *stateObject, now int64) (int64, error) {
	entry, exists, err := s.accountTronPower(obj)
	if err != nil || !exists || entry.ExpireTime > now || entry.FrozenBalance == 0 {
		return 0, err
	}
	if err := s.writeAccountTronPower(obj, nil); err != nil {
		return 0, err
	}
	return entry.FrozenBalance, nil
}
