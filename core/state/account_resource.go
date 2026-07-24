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

// GetAccountFrozenBandwidth returns the frozen balance that contributes to an
// account's bandwidth limit. It reads the compact account envelope, the V1
// bandwidth rows, and the point-addressable V2 bandwidth row without loading
// any other split account domain.
func (s *StateDB) GetAccountFrozenBandwidth(addr tcommon.Address) (int64, error) {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted || obj.account == nil {
		return 0, nil
	}
	return s.accountFrozenBandwidthForLimit(obj, true)
}

// GetAccountFrozenBandwidthV1 is the pre-Stake-2.0 counterpart of
// GetAccountFrozenBandwidth. Before proposal #70 activates, valid java-tron
// state cannot contain V2 freezes, so resource accounting can avoid probing
// that split domain entirely.
func (s *StateDB) GetAccountFrozenBandwidthV1(addr tcommon.Address) (int64, error) {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted || obj.account == nil {
		return 0, nil
	}
	return s.accountFrozenBandwidthForLimit(obj, false)
}

func (s *StateDB) accountFrozenBandwidthForLimit(obj *stateObject, includeV2 bool) (int64, error) {
	acct := obj.account
	// The owner resource snapshot and the consensus bandwidth charge both read
	// this value for the same transaction. Valid java-tron state has at most one
	// V1 bandwidth row, so point-read and cache it instead of opening a prefix
	// iterator over the block overlay. The fast materializer keeps a generic
	// fallback for non-standard multi-row state.
	if err := s.materializeAccountFrozenBandwidthFast(obj); err != nil {
		return 0, err
	}
	frozenV1 := acct.TotalFrozenBandwidth()

	var frozenV2, acquiredV2 int64
	if includeV2 {
		acquiredV2 = acct.AcquiredDelegatedFrozenV2BalanceForBandwidth()
		if obj.accountStakeV2Loaded {
			frozenV2 = acct.GetFrozenV2Amount(corepb.ResourceCode_BANDWIDTH)
		} else {
			var err error
			frozenV2, _, err = s.accountFrozenV2Amount(obj, corepb.ResourceCode_BANDWIDTH)
			if err != nil {
				return 0, err
			}
		}
	}

	return frozenV1 +
		acct.AcquiredDelegatedFrozenBandwidth() +
		frozenV2 +
		acquiredV2, nil
}

// GetAccountFrozenEnergy returns the frozen balance that contributes to an
// account's VM energy limit. It loads only the compact envelope, the single
// AccountResource row, and the point-addressable V2 ENERGY row. In particular
// it avoids the full account prefix scans performed by GetAccount.
func (s *StateDB) GetAccountFrozenEnergy(addr tcommon.Address) (int64, error) {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted || obj.account == nil {
		return 0, nil
	}
	return s.accountFrozenEnergyForLimit(obj, true)
}

// GetAccountFrozenEnergyV1 avoids the impossible V2 resource row lookup before
// Stake 2.0 activation while preserving the full accessor for later blocks and
// general callers.
func (s *StateDB) GetAccountFrozenEnergyV1(addr tcommon.Address) (int64, error) {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted || obj.account == nil {
		return 0, nil
	}
	return s.accountFrozenEnergyForLimit(obj, false)
}

func (s *StateDB) accountFrozenEnergyForLimit(obj *stateObject, includeV2 bool) (int64, error) {
	acct := obj.account
	if err := s.materializeAccountResource(obj); err != nil {
		return 0, err
	}

	var frozenV2Energy, acquiredV2Energy int64
	if includeV2 {
		acquiredV2Energy = acct.AcquiredDelegatedFrozenV2BalanceForEnergy()
		if obj.accountStakeV2Loaded {
			frozenV2Energy = acct.GetFrozenV2Amount(corepb.ResourceCode_ENERGY)
		} else {
			var err error
			frozenV2Energy, _, err = s.accountFrozenV2Amount(obj, corepb.ResourceCode_ENERGY)
			if err != nil {
				return 0, err
			}
		}
	}

	return acct.FrozenEnergyAmount() +
		acct.AcquiredDelegatedFrozenEnergy() +
		frozenV2Energy +
		acquiredV2Energy, nil
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
	return s.getAccountFrozenResourceTotals(addr, true)
}

// GetAccountFrozenResourceTotalsV1 returns the two pre-Stake-2.0 resource
// totals without touching AccountFrozenV2Aux.
func (s *StateDB) GetAccountFrozenResourceTotalsV1(addr tcommon.Address) (bandwidth, energy int64, err error) {
	return s.getAccountFrozenResourceTotals(addr, false)
}

func (s *StateDB) getAccountFrozenResourceTotals(addr tcommon.Address, includeV2 bool) (bandwidth, energy int64, err error) {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted || obj.account == nil {
		return 0, 0, nil
	}
	bandwidth, err = s.accountFrozenBandwidthForLimit(obj, includeV2)
	if err != nil {
		return 0, 0, err
	}

	energy, err = s.accountFrozenEnergyForLimit(obj, includeV2)
	if err != nil {
		return 0, 0, err
	}
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
