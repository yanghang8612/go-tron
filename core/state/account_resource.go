package state

import (
	"bytes"
	"fmt"

	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

var accountResourceKey = []byte{0x00}

func clearAccountResourceProto(pb *corepb.Account) {
	if pb != nil {
		pb.AccountResource = nil
	}
}

func (s *StateDB) materializeAccountResource(obj *stateObject) error {
	if obj == nil || obj.account == nil || obj.accountResourceLoaded {
		return nil
	}
	pb := obj.account.Proto()
	clearAccountResourceProto(pb)
	value, exists, err := s.GetAccountKV(obj.address, kvdomains.AccountResourceAux, accountResourceKey)
	if err != nil {
		return err
	}
	if exists {
		var resource corepb.Account_AccountResource
		if err := proto.Unmarshal(value, &resource); err != nil {
			return fmt.Errorf("decode account resource: %w", err)
		}
		pb.AccountResource = &resource
	}
	obj.accountResourceLoaded = true
	return nil
}

func (s *StateDB) writeAccountResource(obj *stateObject) error {
	if obj == nil || obj.account == nil {
		return nil
	}
	resource := obj.account.Proto().AccountResource
	if resource == nil {
		if err := s.DeleteAccountKV(obj.address, kvdomains.AccountResourceAux, accountResourceKey); err != nil {
			return err
		}
		obj.accountResourceLoaded = true
		return nil
	}
	value, err := proto.MarshalOptions{Deterministic: true}.Marshal(resource)
	if err != nil {
		return err
	}
	if err := s.SetAccountKV(obj.address, kvdomains.AccountResourceAux, accountResourceKey, value); err != nil {
		return err
	}
	obj.accountResourceLoaded = true
	return nil
}

func (s *StateDB) mutateAccountResource(obj *stateObject, mutate func(*corepb.Account_AccountResource)) error {
	if obj == nil || obj.account == nil {
		return nil
	}
	if err := s.materializeAccountResource(obj); err != nil {
		return err
	}
	mutate(obj.account.Proto().AccountResource)
	return s.writeAccountResource(obj)
}

func decodeHistoricalAccountResource(key, value []byte) (*corepb.Account_AccountResource, error) {
	if !bytes.Equal(key, accountResourceKey) {
		return nil, fmt.Errorf("account resource key %x, want %x", key, accountResourceKey)
	}
	var resource corepb.Account_AccountResource
	if err := proto.Unmarshal(value, &resource); err != nil {
		return nil, fmt.Errorf("decode historical account resource: %w", err)
	}
	return &resource, nil
}
