package state

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

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
	// Frozen-bandwidth rows are written as a dense zero-based sequence by
	// writeAccountFrozenBandwidth and by the account migration path. Reading the
	// handful of expected indexes directly avoids opening a prefix iterator,
	// whose blockbuffer overlay setup otherwise scans every write in every live
	// layer just to find the usual zero-or-one matching row.
	for index := uint32(0); ; index++ {
		key := accountFrozenBandwidthKey(index)
		value, exists, err := s.GetAccountKV(obj.address, kvdomains.AccountFrozenBandwidthAux, key)
		if err != nil {
			return nil, err
		}
		if !exists {
			return rows, nil
		}
		row, err := decodeAccountFrozenBandwidthRow(key, value)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
		if index == math.MaxUint32 {
			return nil, fmt.Errorf("account frozen-bandwidth index overflow")
		}
	}
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
	if err := s.DeleteAccountKVPrefix(obj.address, kvdomains.AccountFrozenBandwidthAux, nil); err != nil {
		return err
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
