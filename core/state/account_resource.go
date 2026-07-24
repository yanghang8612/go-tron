package state

import (
	"bytes"
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
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

// GetAccountFrozenResourceTotals returns the frozen balances that contribute
// to the account's bandwidth and energy limits. It intentionally reads only
// the resource-bearing account data needed by java-tron's
// getAllFrozenBalanceForBandwidth/getAllFrozenBalanceForEnergy calculations:
// the compact account envelope, V1 bandwidth rows, the single AccountResource
// row, and the two point-addressable V2 frozen rows.
//
// In particular, it does not materialize the account's asset maps,
// permissions, votes, frozen supply, tron power, or V2 unfreeze queue. This is
// suitable for per-transaction resource accounting and diagnostics where a
// full GetAccount would make unrelated account state part of the hot path.
func (s *StateDB) GetAccountFrozenResourceTotals(addr tcommon.Address) (bandwidth, energy int64, err error) {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted || obj.account == nil {
		return 0, 0, nil
	}
	acct := obj.account

	var frozenV1Bandwidth int64
	if obj.accountFrozenBandwidthLoaded {
		frozenV1Bandwidth = acct.TotalFrozenBandwidth()
	} else {
		frozenV1Bandwidth, err = s.accountFrozenBandwidthTotal(obj)
		if err != nil {
			return 0, 0, err
		}
	}

	if err := s.materializeAccountResource(obj); err != nil {
		return 0, 0, err
	}

	var frozenV2Bandwidth, frozenV2Energy int64
	if obj.accountStakeV2Loaded {
		frozenV2Bandwidth = acct.GetFrozenV2Amount(corepb.ResourceCode_BANDWIDTH)
		frozenV2Energy = acct.GetFrozenV2Amount(corepb.ResourceCode_ENERGY)
	} else {
		frozenV2Bandwidth, _, err = s.accountFrozenV2Amount(obj, corepb.ResourceCode_BANDWIDTH)
		if err != nil {
			return 0, 0, err
		}
		frozenV2Energy, _, err = s.accountFrozenV2Amount(obj, corepb.ResourceCode_ENERGY)
		if err != nil {
			return 0, 0, err
		}
	}

	bandwidth = frozenV1Bandwidth +
		acct.AcquiredDelegatedFrozenBandwidth() +
		frozenV2Bandwidth +
		acct.AcquiredDelegatedFrozenV2BalanceForBandwidth()
	energy = acct.FrozenEnergyAmount() +
		acct.AcquiredDelegatedFrozenEnergy() +
		frozenV2Energy +
		acct.AcquiredDelegatedFrozenV2BalanceForEnergy()
	return bandwidth, energy, nil
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
