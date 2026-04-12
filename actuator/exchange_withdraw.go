package actuator

import (
	"bytes"
	"errors"
	"math/big"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// ExchangeWithdrawActuator handles ExchangeWithdrawContract (type 43).
type ExchangeWithdrawActuator struct{}

func (a *ExchangeWithdrawActuator) getContract(ctx *Context) (*contractpb.ExchangeWithdrawContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.ExchangeWithdrawContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal ExchangeWithdrawContract")
	}
	return c, nil
}

// Validate checks all preconditions for an ExchangeWithdraw transaction.
func (a *ExchangeWithdrawActuator) Validate(ctx *Context) error {
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	if c.Quant <= 0 {
		return errors.New("quant must be positive")
	}
	ownerAddr := tcommon.BytesToAddress(c.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	ex := rawdb.ReadExchange(ctx.DB, c.ExchangeId)
	if ex == nil {
		return errors.New("exchange not found")
	}
	// Check exchange has enough of the specified token to withdraw
	var tokenBalance int64
	if bytes.Equal(ex.FirstTokenId, c.TokenId) {
		tokenBalance = ex.FirstTokenBalance
	} else if bytes.Equal(ex.SecondTokenId, c.TokenId) {
		tokenBalance = ex.SecondTokenBalance
	} else {
		return errors.New("token not in exchange")
	}
	if c.Quant > tokenBalance {
		return errors.New("insufficient exchange balance")
	}
	return nil
}

// Execute removes liquidity from an exchange and credits tokens to owner.
func (a *ExchangeWithdrawActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := tcommon.BytesToAddress(c.OwnerAddress)
	ex := rawdb.ReadExchange(ctx.DB, c.ExchangeId)
	if ex == nil {
		return nil, errors.New("exchange not found")
	}

	var thisBalance, otherBalance int64
	firstIsThis := bytes.Equal(ex.FirstTokenId, c.TokenId)
	if firstIsThis {
		thisBalance = ex.FirstTokenBalance
		otherBalance = ex.SecondTokenBalance
	} else {
		thisBalance = ex.SecondTokenBalance
		otherBalance = ex.FirstTokenBalance
	}

	// Calculate proportional other token amount
	anotherQuant := new(big.Int).Mul(big.NewInt(otherBalance), big.NewInt(c.Quant))
	anotherQuant.Div(anotherQuant, big.NewInt(thisBalance))
	otherQuant := anotherQuant.Int64()

	var otherTokenId []byte
	if firstIsThis {
		otherTokenId = ex.SecondTokenId
	} else {
		otherTokenId = ex.FirstTokenId
	}

	// Credit withdrawn tokens to owner
	if err := transferToken(ctx, ownerAddr, c.TokenId, c.Quant); err != nil {
		return nil, err
	}
	if err := transferToken(ctx, ownerAddr, otherTokenId, otherQuant); err != nil {
		return nil, err
	}

	// Deduct from exchange
	if firstIsThis {
		ex.FirstTokenBalance -= c.Quant
		ex.SecondTokenBalance -= otherQuant
	} else {
		ex.SecondTokenBalance -= c.Quant
		ex.FirstTokenBalance -= otherQuant
	}
	if err := rawdb.WriteExchange(ctx.DB, ex); err != nil {
		return nil, err
	}
	return &Result{ContractRet: 1}, nil
}
