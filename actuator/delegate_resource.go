package actuator

import (
	"errors"
	"fmt"

	"github.com/tronprotocol/go-tron/core/delegation"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type DelegateResourceActuator struct{}

// getLockPeriod mirrors java-tron `DelegateResourceActuator.getLockPeriod`:
// before proposal #78 raises `max_delegate_lock_period` AND proposal #70
// activates Stake-2.0 unfreeze delay, the contract's `lock_period` field is
// ignored and lockPeriod forced to the bootstrap default
// (`DelegatePeriod/BlockProducedInterval` = 86400 blocks ≈ 3 days). After
// both proposals activate (`SupportMaxDelegateLockPeriod`), the contract
// value is honored (0 still means default).
//
// Unit: returns blocks. Multiply by `BlockProducedInterval` (ms) to get
// the duration the receiver's delegation is locked for.
func getLockPeriod(supportMax bool, contractLockPeriod int64) int64 {
	defaultBlocks := int64(params.DelegatePeriod / params.BlockProducedInterval)
	if !supportMax {
		return defaultBlocks
	}
	if contractLockPeriod == 0 {
		return defaultBlocks
	}
	return contractLockPeriod
}

func (a *DelegateResourceActuator) getContract(ctx *Context) (*contractpb.DelegateResourceContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.DelegateResourceContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal DelegateResourceContract")
	}
	return c, nil
}

func (a *DelegateResourceActuator) Validate(ctx *Context) error {
	if !forks.IsActive(forks.AllowDelegateResource, ctx.BlockNumber, ctx.DynProps) {
		return errors.New("resource delegation not yet enabled")
	}
	if !ctx.DynProps.SupportUnfreezeDelay() {
		return errors.New("staking v2 not yet enabled")
	}
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr, err := checkedAddress(c.OwnerAddress, "address")
	if err != nil {
		return err
	}
	receiverAddr, err := checkedAddress(c.ReceiverAddress, "receiverAddress")
	if err != nil {
		return err
	}
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if c.Balance < int64(params.TRXPrecision) {
		return errors.New("delegateBalance must be greater than or equal to 1 TRX")
	}
	if c.Resource != corepb.ResourceCode_BANDWIDTH && c.Resource != corepb.ResourceCode_ENERGY {
		return errors.New("invalid resource type")
	}
	if ownerAddr == receiverAddr {
		return errors.New("cannot delegate to self")
	}
	if !ctx.State.AccountExists(receiverAddr) {
		return errors.New("receiver account does not exist")
	}
	receiver := ctx.State.GetAccount(receiverAddr)
	if receiver != nil && receiver.Type() == corepb.AccountType_Contract {
		return errors.New("Do not allow delegate resources to contract addresses")
	}
	available := delegation.AvailableFrozenV2ForDelegation(ctx.State, ctx.DynProps, ownerAddr, c.Resource, ctx.ResourceTime())
	if available < c.Balance {
		switch c.Resource {
		case corepb.ResourceCode_BANDWIDTH:
			return errors.New("delegateBalance must be less than or equal to available FreezeBandwidthV2 balance")
		case corepb.ResourceCode_ENERGY:
			return errors.New("delegateBalance must be less than or equal to available FreezeEnergyV2 balance")
		}
		return errors.New("insufficient frozen balance to delegate")
	}

	// Lock-period range check + validRemainTime — mirror java-tron
	// `DelegateResourceActuator.validate` lines 211-243. Only runs when
	// the chain has reached the post-#70+#78 fork state; before that the
	// contract's lock_period is forced to the default (see getLockPeriod).
	if c.Lock && ctx.DynProps.SupportMaxDelegateLockPeriod() {
		lockPeriod := getLockPeriod(true, c.LockPeriod)
		maxLock := ctx.DynProps.MaxDelegateLockPeriod()
		if lockPeriod < 0 || lockPeriod > maxLock {
			return fmt.Errorf("the lock period of delegate resource cannot be less than 0 and cannot exceed %d!", maxLock)
		}
		if dr := ctx.State.ReadDelegatedResourceV2(ownerAddr, receiverAddr, true); dr != nil {
			var existingExpire int64
			switch c.Resource {
			case corepb.ResourceCode_BANDWIDTH:
				existingExpire = dr.ExpireTimeForBandwidth
			case corepb.ResourceCode_ENERGY:
				existingExpire = dr.ExpireTimeForEnergy
			}
			remain := existingExpire - ctx.PrevBlockTime
			if lockPeriod*params.BlockProducedInterval < remain {
				return fmt.Errorf("the lock period for %s this time cannot be less than the remaining time[%dms] of the last lock period for %s!", c.Resource, remain, c.Resource)
			}
		}
	}
	return nil
}

func (a *DelegateResourceActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr, err := checkedAddress(c.OwnerAddress, "address")
	if err != nil {
		return nil, err
	}
	receiverAddr, err := checkedAddress(c.ReceiverAddress, "receiverAddress")
	if err != nil {
		return nil, err
	}

	// NOTE: java DelegateResourceActuator.execute does NOT recover/persist the
	// owner's net/energy usage — updateUsageForDelegated runs only in validate()
	// on a throwaway capsule that is never put back (and the VM native-contract
	// path's getAccount returns a byte-snapshot copy, so its validate mutation is
	// likewise discarded). Recovering+writing the owner's usage here (the old
	// FoldUsageIntoOwner(…,0) call) wrote a recovered usage + latest_consume_time
	// where java leaves them untouched — a Stake-2.0 state divergence. Leave the
	// owner's usage alone; only the frozen-balance bookkeeping changes on delegate.

	// Subtract from owner's frozen balance
	ctx.State.ReduceFreezeV2(ownerAddr, c.Resource, c.Balance)
	// Track outgoing delegation on owner
	ctx.State.AddDelegatedFrozenV2(ownerAddr, c.Resource, c.Balance)
	// Track incoming delegation on receiver
	ctx.State.AddAcquiredDelegatedFrozenV2(receiverAddr, c.Resource, c.Balance)

	if err := ctx.State.UnlockExpiredDelegatedResource(ownerAddr, receiverAddr, ctx.PrevBlockTime); err != nil {
		return nil, err
	}

	locked := c.Lock
	dr := ctx.State.ReadDelegatedResourceV2(ownerAddr, receiverAddr, locked)
	if dr == nil {
		dr = &rawdb.DelegatedResource{From: ownerAddr, To: receiverAddr}
	}
	// java-tron `DelegateResourceActuator.execute` line 297:
	// expireTime = now + getLockPeriod(...) * BLOCK_PRODUCED_INTERVAL.
	// `c.LockPeriod` is denominated in *blocks*; multiplying by
	// `BlockProducedInterval` (ms) yields the duration the receiver's
	// stake is locked for. Before the chain reaches the
	// SupportMaxDelegateLockPeriod fork state, contract.LockPeriod is
	// ignored and forced to the default (86400 blocks).
	lockPeriodBlocks := getLockPeriod(ctx.DynProps.SupportMaxDelegateLockPeriod(), c.LockPeriod)
	expireTime := ctx.PrevBlockTime + lockPeriodBlocks*params.BlockProducedInterval
	if c.Resource == corepb.ResourceCode_BANDWIDTH {
		dr.FrozenBalanceForBandwidth += c.Balance
		if locked {
			dr.ExpireTimeForBandwidth = expireTime
		}
	} else {
		dr.FrozenBalanceForEnergy += c.Balance
		if locked {
			dr.ExpireTimeForEnergy = expireTime
		}
	}
	if err := ctx.State.WriteDelegatedResourceV2(ownerAddr, receiverAddr, locked, dr); err != nil {
		return nil, err
	}

	// Stake 2.0 uses java-tron's directional V2 index: 0x03 owner→receiver
	// and 0x04 receiver→owner. Re-delegation refreshes both timestamps.
	if err := ctx.State.WriteDrAccountIndexDelegate(true, ownerAddr[:], receiverAddr[:], ctx.PrevBlockTime); err != nil {
		return nil, err
	}

	return &Result{Fee: 0, ContractRet: 1}, nil
}
