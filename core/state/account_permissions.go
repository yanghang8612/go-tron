package state

import (
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

var (
	accountOwnerPermissionKey   = []byte{0x00}
	accountWitnessPermissionKey = []byte{0x01}
	accountActivePermissionRoot = []byte{0x02}
)

func accountActivePermissionKey(id int32) []byte {
	var key [5]byte
	key[0] = accountActivePermissionRoot[0]
	binary.BigEndian.PutUint32(key[1:], uint32(id))
	return key[:]
}

func decodeAccountPermissionRow(key, value []byte) (*corepb.Permission, byte, error) {
	if len(key) != 1 && !(len(key) == 5 && key[0] == accountActivePermissionRoot[0]) {
		return nil, 0, fmt.Errorf("account permission key %x has invalid length/type", key)
	}
	var permission corepb.Permission
	if err := proto.Unmarshal(value, &permission); err != nil {
		return nil, 0, fmt.Errorf("decode account permission %x: %w", key, err)
	}
	return &permission, key[0], nil
}

func clearAccountPermissionProto(pb *corepb.Account) {
	if pb == nil {
		return
	}
	pb.OwnerPermission = nil
	pb.WitnessPermission = nil
	pb.ActivePermission = nil
}

func (s *StateDB) materializeAccountPermissions(obj *stateObject) error {
	if obj == nil || obj.account == nil || obj.accountPermissionsLoaded {
		return nil
	}
	pb := obj.account.Proto()
	clearAccountPermissionProto(pb)
	if err := s.IterateAccountKV(obj.address, kvdomains.AccountPermissionAux, nil, func(key, value []byte) (bool, error) {
		permission, kind, err := decodeAccountPermissionRow(key, value)
		if err != nil {
			return false, err
		}
		switch kind {
		case accountOwnerPermissionKey[0]:
			pb.OwnerPermission = permission
		case accountWitnessPermissionKey[0]:
			pb.WitnessPermission = permission
		case accountActivePermissionRoot[0]:
			pb.ActivePermission = append(pb.ActivePermission, permission)
		default:
			return false, fmt.Errorf("unknown account permission key %x", key)
		}
		return true, nil
	}); err != nil {
		clearAccountPermissionProto(pb)
		return err
	}
	sort.Slice(pb.ActivePermission, func(i, j int) bool {
		return pb.ActivePermission[i].GetId() < pb.ActivePermission[j].GetId()
	})
	obj.accountPermissionsLoaded = true
	return nil
}

func (s *StateDB) writeAccountPermissionRow(obj *stateObject, key []byte, permission *corepb.Permission) error {
	if permission == nil {
		return s.DeleteAccountKV(obj.address, kvdomains.AccountPermissionAux, key)
	}
	value, err := proto.MarshalOptions{Deterministic: true}.Marshal(permission)
	if err != nil {
		return err
	}
	return s.SetAccountKV(obj.address, kvdomains.AccountPermissionAux, key, value)
}

func (s *StateDB) writeAccountPermissions(obj *stateObject, owner, witness *corepb.Permission, actives []*corepb.Permission) error {
	if obj == nil || obj.account == nil {
		return nil
	}
	if err := s.writeAccountPermissionRow(obj, accountOwnerPermissionKey, owner); err != nil {
		return err
	}
	if err := s.writeAccountPermissionRow(obj, accountWitnessPermissionKey, witness); err != nil {
		return err
	}
	if err := s.DeleteAccountKVPrefix(obj.address, kvdomains.AccountPermissionAux, accountActivePermissionRoot); err != nil {
		return err
	}
	for _, permission := range actives {
		if permission == nil {
			continue
		}
		if err := s.writeAccountPermissionRow(obj, accountActivePermissionKey(permission.GetId()), permission); err != nil {
			return err
		}
	}
	clearAccountPermissionProto(obj.account.Proto())
	obj.accountPermissionsLoaded = false
	return nil
}
