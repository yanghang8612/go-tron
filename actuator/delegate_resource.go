package actuator

import (
	"errors"
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
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
	ownerAddr := tcommon.BytesToAddress(c.OwnerAddress)
	receiverAddr := tcommon.BytesToAddress(c.ReceiverAddress)
	if ownerAddr == receiverAddr {
		return errors.New("cannot delegate to self")
	}
	if c.Balance <= 0 {
		return errors.New("delegation balance must be positive")
	}
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if !ctx.State.AccountExists(receiverAddr) {
		return errors.New("receiver account does not exist")
	}
	if c.Resource != corepb.ResourceCode_BANDWIDTH && c.Resource != corepb.ResourceCode_ENERGY {
		return errors.New("invalid resource type")
	}
	frozen := ctx.State.GetFrozenV2Amount(ownerAddr, c.Resource)
	alreadyDelegated := ctx.State.GetDelegatedFrozenV2(ownerAddr, c.Resource)
	available := frozen - alreadyDelegated
	if available < c.Balance {
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
		if ctx.DB != nil {
			// NOTE: gtron does not yet split locked/unlocked delegation
			// entries (java-tron `DelegatedResourceCapsule.createDbKeyV2`).
			// Until that lands we check the single entry, which over-rejects
			// only when a prior unlocked delegate happens to have a later
			// expire time — a window java-tron treats as a fresh start.
			if dr := rawdb.ReadDelegatedResource(ctx.DB, ownerAddr, receiverAddr); dr != nil {
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
	}
	return nil
}

func (a *DelegateResourceActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := tcommon.BytesToAddress(c.OwnerAddress)
	receiverAddr := tcommon.BytesToAddress(c.ReceiverAddress)

	// Mirrors java-tron DelegateResourceActuator.execute line 155:
	// refresh owner's usage counter before their frozen pool shifts, so
	// the sliding-window decay keeps tracking from the correct anchor.
	// Passing transferUsage=0 just writes back the recovered value.
	delegation.FoldUsageIntoOwner(ctx.State, ownerAddr, c.Resource, 0, ctx.PrevBlockTime)

	// Subtract from owner's frozen balance
	ctx.State.ReduceFreezeV2(ownerAddr, c.Resource, c.Balance)
	// Track outgoing delegation on owner
	ctx.State.AddDelegatedFrozenV2(ownerAddr, c.Resource, c.Balance)
	// Track incoming delegation on receiver
	ctx.State.AddAcquiredDelegatedFrozenV2(receiverAddr, c.Resource, c.Balance)

	// Update delegation record in rawdb
	if ctx.DB != nil {
		dr := rawdb.ReadDelegatedResource(ctx.DB, ownerAddr, receiverAddr)
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
			if c.Lock {
				dr.ExpireTimeForBandwidth = expireTime
			}
		} else {
			dr.FrozenBalanceForEnergy += c.Balance
			if c.Lock {
				dr.ExpireTimeForEnergy = expireTime
			}
		}
		rawdb.WriteDelegatedResource(ctx.DB, ownerAddr, receiverAddr, dr)

		// Update delegation index
		receivers := rawdb.ReadDelegationIndex(ctx.DB, ownerAddr)
		if !containsAddress(receivers, receiverAddr) {
			receivers = append(receivers, receiverAddr)
			rawdb.WriteDelegationIndex(ctx.DB, ownerAddr, receivers)
		}
	}

	return &Result{Fee: 0, ContractRet: 1}, nil
}
