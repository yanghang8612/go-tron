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

type accountFrozenSupplyRow struct {
	key   []byte
	index uint32
	entry *corepb.Account_Frozen
}

func accountFrozenSupplyKey(index uint32) []byte {
	var key [4]byte
	binary.BigEndian.PutUint32(key[:], index)
	return key[:]
}

func decodeAccountFrozenSupplyRow(key, value []byte) (accountFrozenSupplyRow, error) {
	if len(key) != 4 {
		return accountFrozenSupplyRow{}, fmt.Errorf("account frozen-supply key length %d, want 4", len(key))
	}
	var entry corepb.Account_Frozen
	if err := proto.Unmarshal(value, &entry); err != nil {
		return accountFrozenSupplyRow{}, fmt.Errorf("decode account frozen-supply %x: %w", key, err)
	}
	return accountFrozenSupplyRow{
		key:   append([]byte(nil), key...),
		index: binary.BigEndian.Uint32(key),
		entry: &entry,
	}, nil
}

func clearAccountFrozenSupplyProto(pb *corepb.Account) {
	if pb != nil {
		pb.FrozenSupply = nil
	}
}

func (s *StateDB) accountFrozenSupplyRows(obj *stateObject) ([]accountFrozenSupplyRow, error) {
	if obj == nil || obj.account == nil {
		return nil, nil
	}
	rows := make([]accountFrozenSupplyRow, 0, 10)
	if err := s.IterateAccountKV(obj.address, kvdomains.AccountFrozenSupplyAux, nil, func(key, value []byte) (bool, error) {
		row, err := decodeAccountFrozenSupplyRow(key, value)
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

func (s *StateDB) materializeAccountFrozenSupply(obj *stateObject) error {
	if obj == nil || obj.account == nil || obj.accountFrozenSupplyLoaded {
		return nil
	}
	rows, err := s.accountFrozenSupplyRows(obj)
	if err != nil {
		clearAccountFrozenSupplyProto(obj.account.Proto())
		return err
	}
	pb := obj.account.Proto()
	clearAccountFrozenSupplyProto(pb)
	for _, row := range rows {
		pb.FrozenSupply = append(pb.FrozenSupply, row.entry)
	}
	obj.accountFrozenSupplyLoaded = true
	return nil
}

func (s *StateDB) invalidateAccountFrozenSupply(obj *stateObject) {
	if obj == nil || obj.account == nil {
		return
	}
	clearAccountFrozenSupplyProto(obj.account.Proto())
	obj.accountFrozenSupplyLoaded = false
}

func (s *StateDB) addAccountFrozenSupply(obj *stateObject, entries []*corepb.Account_Frozen) error {
	if obj == nil || obj.account == nil || len(entries) == 0 {
		return nil
	}
	rows, err := s.accountFrozenSupplyRows(obj)
	if err != nil {
		return err
	}
	var next uint32
	if len(rows) > 0 {
		last := rows[len(rows)-1].index
		if last == math.MaxUint32 {
			return fmt.Errorf("account frozen-supply index overflow")
		}
		next = last + 1
	}
	nonNil := uint64(0)
	for _, entry := range entries {
		if entry != nil {
			nonNil++
		}
	}
	if nonNil > 0 && uint64(next)+nonNil-1 > math.MaxUint32 {
		return fmt.Errorf("account frozen-supply index overflow")
	}
	for _, entry := range entries {
		if entry == nil {
			continue
		}
		value, err := proto.MarshalOptions{Deterministic: true}.Marshal(entry)
		if err != nil {
			return err
		}
		if err := s.SetAccountKV(obj.address, kvdomains.AccountFrozenSupplyAux, accountFrozenSupplyKey(next), value); err != nil {
			return err
		}
		next++
	}
	s.invalidateAccountFrozenSupply(obj)
	return nil
}

func (s *StateDB) removeExpiredAccountFrozenSupply(obj *stateObject, now int64) (int64, error) {
	rows, err := s.accountFrozenSupplyRows(obj)
	if err != nil {
		return 0, err
	}
	var amount int64
	removed := false
	for _, row := range rows {
		if row.entry.ExpireTime > now {
			continue
		}
		if err := s.DeleteAccountKV(obj.address, kvdomains.AccountFrozenSupplyAux, row.key); err != nil {
			return 0, err
		}
		amount += row.entry.FrozenBalance
		removed = true
	}
	if removed {
		s.invalidateAccountFrozenSupply(obj)
	}
	return amount, nil
}
