package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type WithdrawExpireUnfreezeActuator struct{}

func (a *WithdrawExpireUnfreezeActuator) getContract(ctx *Context) (*contractpb.WithdrawExpireUnfreezeContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	wc := &contractpb.WithdrawExpireUnfreezeContract{}
	if err := contract.Parameter.UnmarshalTo(wc); err != nil {
		return nil, errors.New("failed to unmarshal WithdrawExpireUnfreezeContract")
	}
	return wc, nil
}

func (a *WithdrawExpireUnfreezeActuator) Validate(ctx *Context) error {
	wc, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := common.BytesToAddress(wc.OwnerAddress)
	ownerAcc := rawdb.ReadAccount(ctx.DB, ownerAddr)
	if ownerAcc == nil {
		return errors.New("owner account does not exist")
	}
	hasExpired := false
	for _, u := range ownerAcc.UnfrozenV2() {
		if u.UnfreezeExpireTime <= ctx.BlockTime {
			hasExpired = true
			break
		}
	}
	if !hasExpired {
		return errors.New("no expired unfreeze entries")
	}
	return nil
}

func (a *WithdrawExpireUnfreezeActuator) Execute(ctx *Context) (*Result, error) {
	wc, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := common.BytesToAddress(wc.OwnerAddress)
	ownerAcc := rawdb.ReadAccount(ctx.DB, ownerAddr)
	withdrawn := ownerAcc.RemoveExpiredUnfreezeV2(ctx.BlockTime)
	ownerAcc.SetBalance(ownerAcc.Balance() + withdrawn)
	rawdb.WriteAccount(ctx.DB, ownerAddr, ownerAcc)
	return &Result{Fee: 0}, nil
}
