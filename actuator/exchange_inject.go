package actuator

import (
	"bytes"
	"errors"
	"math/big"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// ExchangeInjectActuator handles ExchangeInjectContract (type 42).
type ExchangeInjectActuator struct{}

func (a *ExchangeInjectActuator) getContract(ctx *Context) (*contractpb.ExchangeInjectContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.ExchangeInjectContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal ExchangeInjectContract")
	}
	return c, nil
}

// Validate checks all preconditions for an ExchangeInject transaction.
func (a *ExchangeInjectActuator) Validate(ctx *Context) error {
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
	// Validate owner has enough of the injected token
	if err := checkTokenBalance(ctx, ownerAddr, c.TokenId, c.Quant); err != nil {
		return err
	}
	return nil
}

// Execute injects liquidity into an existing exchange.
func (a *ExchangeInjectActuator) Execute(ctx *Context) (*Result, error) {
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

	// Calculate how much of the other token must be added proportionally
	anotherQuant := new(big.Int).Mul(big.NewInt(otherBalance), big.NewInt(c.Quant))
	anotherQuant.Div(anotherQuant, big.NewInt(thisBalance))
	otherQuant := anotherQuant.Int64()

	// Deduct injected token from owner
	if err := deductToken(ctx, ownerAddr, c.TokenId, c.Quant); err != nil {
		return nil, err
	}
	// Deduct other token from owner
	var otherTokenId []byte
	if firstIsThis {
		otherTokenId = ex.SecondTokenId
	} else {
		otherTokenId = ex.FirstTokenId
	}
	if err := deductToken(ctx, ownerAddr, otherTokenId, otherQuant); err != nil {
		return nil, err
	}

	// Update exchange balances
	if firstIsThis {
		ex.FirstTokenBalance += c.Quant
		ex.SecondTokenBalance += otherQuant
	} else {
		ex.SecondTokenBalance += c.Quant
		ex.FirstTokenBalance += otherQuant
	}
	if err := rawdb.WriteExchange(ctx.DB, ex); err != nil {
		return nil, err
	}
	return &Result{ContractRet: 1}, nil
}
