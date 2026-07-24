package state

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"

	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

const accountFrozenV2ValueLength = 12

const accountFrozenV2PointCacheMask uint8 = 0b111

type accountFrozenV2Row struct {
	resource corepb.ResourceCode
	ordinal  uint32
	amount   int64
}

type accountUnfrozenV2Row struct {
	key   []byte
	seq   uint64
	entry *corepb.Account_UnFreezeV2
}

func accountFrozenV2Key(resource corepb.ResourceCode) []byte {
	var key [4]byte
	binary.BigEndian.PutUint32(key[:], uint32(resource))
	return key[:]
}

func encodeAccountFrozenV2Value(ordinal uint32, amount int64) []byte {
	var value [accountFrozenV2ValueLength]byte
	binary.BigEndian.PutUint32(value[:4], ordinal)
	binary.BigEndian.PutUint64(value[4:], uint64(amount))
	return value[:]
}

func decodeAccountFrozenV2Row(key, value []byte) (accountFrozenV2Row, error) {
	if len(key) != 4 {
		return accountFrozenV2Row{}, fmt.Errorf("account frozen-v2 key length %d, want 4", len(key))
	}
	if len(value) != accountFrozenV2ValueLength {
		return accountFrozenV2Row{}, fmt.Errorf("account frozen-v2 value length %d, want %d", len(value), accountFrozenV2ValueLength)
	}
	return accountFrozenV2Row{
		resource: corepb.ResourceCode(int32(binary.BigEndian.Uint32(key))),
		ordinal:  binary.BigEndian.Uint32(value[:4]),
		amount:   int64(binary.BigEndian.Uint64(value[4:])),
	}, nil
}

func accountUnfrozenV2Key(seq uint64) []byte {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], seq)
	return key[:]
}

func decodeAccountUnfrozenV2Row(key, value []byte) (accountUnfrozenV2Row, error) {
	if len(key) != 8 {
		return accountUnfrozenV2Row{}, fmt.Errorf("account unfrozen-v2 key length %d, want 8", len(key))
	}
	var entry corepb.Account_UnFreezeV2
	if err := proto.Unmarshal(value, &entry); err != nil {
		return accountUnfrozenV2Row{}, fmt.Errorf("decode account unfrozen-v2 %x: %w", key, err)
	}
	return accountUnfrozenV2Row{
		key:   append([]byte(nil), key...),
		seq:   binary.BigEndian.Uint64(key),
		entry: &entry,
	}, nil
}

func clearAccountStakeV2Proto(pb *corepb.Account) {
	if pb == nil {
		return
	}
	pb.FrozenV2 = nil
	pb.UnfrozenV2 = nil
}

func accountFrozenV2PointSlot(resource corepb.ResourceCode) (int, uint8, bool) {
	if resource < corepb.ResourceCode_BANDWIDTH || resource > corepb.ResourceCode_TRON_POWER {
		return 0, 0, false
	}
	slot := int(resource)
	return slot, uint8(1) << slot, true
}

func clearAccountFrozenV2PointCache(obj *stateObject) {
	if obj == nil {
		return
	}
	obj.accountFrozenV2PointLoaded = 0
	obj.accountFrozenV2PointExists = 0
	obj.accountFrozenV2PointAmounts = [3]int64{}
}

func cacheAccountFrozenV2Point(obj *stateObject, resource corepb.ResourceCode, amount int64, exists bool) {
	slot, bit, ok := accountFrozenV2PointSlot(resource)
	if obj == nil || !ok {
		return
	}
	obj.accountFrozenV2PointLoaded |= bit
	if exists {
		obj.accountFrozenV2PointExists |= bit
		obj.accountFrozenV2PointAmounts[slot] = amount
		return
	}
	obj.accountFrozenV2PointExists &^= bit
	obj.accountFrozenV2PointAmounts[slot] = 0
}

func cachedAccountFrozenV2Point(obj *stateObject, resource corepb.ResourceCode) (amount int64, exists, loaded bool) {
	slot, bit, ok := accountFrozenV2PointSlot(resource)
	if obj == nil || !ok || obj.accountFrozenV2PointLoaded&bit == 0 {
		return 0, false, false
	}
	return obj.accountFrozenV2PointAmounts[slot], obj.accountFrozenV2PointExists&bit != 0, true
}

func (s *StateDB) accountFrozenV2Rows(obj *stateObject) ([]accountFrozenV2Row, error) {
	if obj == nil || obj.account == nil {
		return nil, nil
	}
	rows := make([]accountFrozenV2Row, 0, 3)
	if err := s.IterateAccountKV(obj.address, kvdomains.AccountFrozenV2Aux, nil, func(key, value []byte) (bool, error) {
		row, err := decodeAccountFrozenV2Row(key, value)
		if err != nil {
			return false, err
		}
		rows = append(rows, row)
		return true, nil
	}); err != nil {
		return nil, err
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ordinal < rows[j].ordinal })
	return rows, nil
}

func (s *StateDB) accountUnfrozenV2Rows(obj *stateObject) ([]accountUnfrozenV2Row, error) {
	if obj == nil || obj.account == nil {
		return nil, nil
	}
	rows := make([]accountUnfrozenV2Row, 0, 32)
	if err := s.IterateAccountKV(obj.address, kvdomains.AccountUnfrozenV2Aux, nil, func(key, value []byte) (bool, error) {
		row, err := decodeAccountUnfrozenV2Row(key, value)
		if err != nil {
			return false, err
		}
		rows = append(rows, row)
		return true, nil
	}); err != nil {
		return nil, err
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].seq < rows[j].seq })
	return rows, nil
}

func (s *StateDB) materializeAccountStakeV2(obj *stateObject) error {
	if obj == nil || obj.account == nil || obj.accountStakeV2Loaded {
		return nil
	}
	frozen, err := s.accountFrozenV2Rows(obj)
	if err != nil {
		clearAccountStakeV2Proto(obj.account.Proto())
		return err
	}
	unfrozen, err := s.accountUnfrozenV2Rows(obj)
	if err != nil {
		clearAccountStakeV2Proto(obj.account.Proto())
		return err
	}
	pb := obj.account.Proto()
	clearAccountStakeV2Proto(pb)
	for _, row := range frozen {
		pb.FrozenV2 = append(pb.FrozenV2, &corepb.Account_FreezeV2{Type: row.resource, Amount: row.amount})
	}
	for _, row := range unfrozen {
		pb.UnfrozenV2 = append(pb.UnfrozenV2, row.entry)
	}
	clearAccountFrozenV2PointCache(obj)
	obj.accountFrozenV2PointLoaded = accountFrozenV2PointCacheMask
	for _, row := range frozen {
		cacheAccountFrozenV2Point(obj, row.resource, row.amount, true)
	}
	obj.accountStakeV2Loaded = true
	return nil
}

func (s *StateDB) invalidateAccountStakeV2(obj *stateObject) {
	if obj == nil || obj.account == nil {
		return
	}
	clearAccountStakeV2Proto(obj.account.Proto())
	obj.accountStakeV2Loaded = false
	clearAccountFrozenV2PointCache(obj)
}

func (s *StateDB) accountFrozenV2Amount(obj *stateObject, resource corepb.ResourceCode) (int64, bool, error) {
	if obj == nil || obj.account == nil {
		return 0, false, nil
	}
	if amount, exists, loaded := cachedAccountFrozenV2Point(obj, resource); loaded {
		return amount, exists, nil
	}
	key := accountFrozenV2Key(resource)
	value, exists, err := s.GetAccountKV(obj.address, kvdomains.AccountFrozenV2Aux, key)
	if err != nil || !exists {
		if err == nil {
			cacheAccountFrozenV2Point(obj, resource, 0, false)
		}
		return 0, exists, err
	}
	row, err := decodeAccountFrozenV2Row(key, value)
	if err != nil {
		return 0, false, err
	}
	cacheAccountFrozenV2Point(obj, resource, row.amount, true)
	return row.amount, true, nil
}

func (s *StateDB) setAccountFrozenV2Amount(obj *stateObject, resource corepb.ResourceCode, amount int64) error {
	if obj == nil || obj.account == nil {
		return nil
	}
	key := accountFrozenV2Key(resource)
	value, exists, err := s.GetAccountKV(obj.address, kvdomains.AccountFrozenV2Aux, key)
	if err != nil {
		return err
	}
	var ordinal uint32
	if exists {
		row, err := decodeAccountFrozenV2Row(key, value)
		if err != nil {
			return err
		}
		ordinal = row.ordinal
	} else {
		rows, err := s.accountFrozenV2Rows(obj)
		if err != nil {
			return err
		}
		if len(rows) > 0 {
			last := rows[len(rows)-1].ordinal
			if last == math.MaxUint32 {
				return fmt.Errorf("account frozen-v2 ordinal overflow")
			}
			ordinal = last + 1
		}
	}
	if err := s.SetAccountKV(obj.address, kvdomains.AccountFrozenV2Aux, key, encodeAccountFrozenV2Value(ordinal, amount)); err != nil {
		return err
	}
	s.invalidateAccountStakeV2(obj)
	cacheAccountFrozenV2Point(obj, resource, amount, true)
	return nil
}

func (s *StateDB) addAccountFrozenV2(obj *stateObject, resource corepb.ResourceCode, delta int64) error {
	amount, _, err := s.accountFrozenV2Amount(obj, resource)
	if err != nil {
		return err
	}
	return s.setAccountFrozenV2Amount(obj, resource, amount+delta)
}

func (s *StateDB) reduceAccountFrozenV2(obj *stateObject, resource corepb.ResourceCode, delta int64) error {
	amount, exists, err := s.accountFrozenV2Amount(obj, resource)
	if err != nil || !exists {
		return err
	}
	amount -= delta
	if amount < 0 {
		amount = 0
	}
	return s.setAccountFrozenV2Amount(obj, resource, amount)
}

func (s *StateDB) clearAccountFrozenV2(obj *stateObject) error {
	if obj == nil || obj.account == nil {
		return nil
	}
	if err := s.DeleteAccountKVPrefix(obj.address, kvdomains.AccountFrozenV2Aux, nil); err != nil {
		return err
	}
	s.invalidateAccountStakeV2(obj)
	return nil
}

func (s *StateDB) addAccountUnfrozenV2(obj *stateObject, resource corepb.ResourceCode, amount, expireTime int64) error {
	if obj == nil || obj.account == nil {
		return nil
	}
	rows, err := s.accountUnfrozenV2Rows(obj)
	if err != nil {
		return err
	}
	var seq uint64
	if len(rows) > 0 {
		last := rows[len(rows)-1].seq
		if last == math.MaxUint64 {
			return fmt.Errorf("account unfrozen-v2 sequence overflow")
		}
		seq = last + 1
	}
	entry := &corepb.Account_UnFreezeV2{Type: resource, UnfreezeAmount: amount, UnfreezeExpireTime: expireTime}
	value, err := proto.MarshalOptions{Deterministic: true}.Marshal(entry)
	if err != nil {
		return err
	}
	if err := s.SetAccountKV(obj.address, kvdomains.AccountUnfrozenV2Aux, accountUnfrozenV2Key(seq), value); err != nil {
		return err
	}
	s.invalidateAccountStakeV2(obj)
	return nil
}

func (s *StateDB) clearAccountUnfrozenV2(obj *stateObject) error {
	if obj == nil || obj.account == nil {
		return nil
	}
	if err := s.DeleteAccountKVPrefix(obj.address, kvdomains.AccountUnfrozenV2Aux, nil); err != nil {
		return err
	}
	s.invalidateAccountStakeV2(obj)
	return nil
}
