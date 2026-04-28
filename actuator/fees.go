package actuator

import (
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/params"
)

// burnFee subtracts fee from owner's balance. When AllowBlackholeOptimization
// is active (proposal #49), the fee is burned (removed from circulation).
// Before activation, the fee is credited to the Blackhole genesis account,
// matching java-tron's pre-fork behaviour.
func burnFee(ctx *Context, owner common.Address, fee int64) error {
	if fee <= 0 {
		return nil
	}
	if err := ctx.State.SubBalance(owner, fee); err != nil {
		return err
	}
	if !forks.IsActive(forks.AllowBlackholeOptimization, ctx.BlockNumber, ctx.DynProps) {
		ctx.State.AddBalance(params.BlackholeAddress, fee)
	}
	return nil
}
