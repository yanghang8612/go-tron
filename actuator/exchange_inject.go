package actuator

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"

	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// ExchangeInjectActuator handles ExchangeInjectContract (type 42).
// Mirrors java-tron's ExchangeInjectActuator.
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
// Mirrors ExchangeInjectActuator.validate() in java-tron.
func (a *ExchangeInjectActuator) Validate(ctx *Context) error {
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr, err := checkedAddress(c.OwnerAddress, "ownerAddress")
	if err != nil {
		return err
	}
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	ex := readExchangeForCurrentFork(ctx, c.ExchangeId)
	if ex == nil {
		return errors.New("exchange not found")
	}
	// Only the exchange creator may inject (java line 167-169).
	if !bytes.Equal(c.OwnerAddress, ex.CreatorAddress) {
		return errors.New("account is not creator")
	}
	// Token must be one of the two in the pool (java line 188-190).
	if !bytes.Equal(ex.FirstTokenId, c.TokenId) && !bytes.Equal(ex.SecondTokenId, c.TokenId) {
		return errors.New("token id is not in exchange")
	}
	// Exchange must be open (java line 192-195).
	if ex.FirstTokenBalance == 0 || ex.SecondTokenBalance == 0 {
		return errors.New("Token balance in exchange is equal with 0,the exchange has been closed")
	}
	// Injected quant > 0 (java line 197-199).
	if c.Quant <= 0 {
		return errors.New("injected token quant must greater than zero")
	}

	// Compute the proportional other-side deposit using BigInt (java 201-219).
	if err := validateExchangeTokenID(ctx, c.TokenId, "token id"); err != nil {
		return err
	}
	var thisBalance, otherBalance int64
	var otherTokenId []byte
	firstIsThis := bytes.Equal(ex.FirstTokenId, c.TokenId)
	if firstIsThis {
		thisBalance = ex.FirstTokenBalance
		otherBalance = ex.SecondTokenBalance
		otherTokenId = ex.SecondTokenId
	} else {
		thisBalance = ex.SecondTokenBalance
		otherBalance = ex.FirstTokenBalance
		otherTokenId = ex.FirstTokenId
	}

	anotherBig := new(big.Int).Mul(big.NewInt(otherBalance), big.NewInt(c.Quant))
	anotherBig.Div(anotherBig, big.NewInt(thisBalance))
	if !anotherBig.IsInt64() {
		return errors.New("the calculated token quant overflows int64")
	}
	anotherTokenQuant := anotherBig.Int64()
	if anotherTokenQuant <= 0 {
		return errors.New("the calculated token quant  must be greater than 0")
	}

	// Balance-cap check for the post-injection pool (java 225-228).
	harden := ctx.DynProps.AllowHardenExchangeCalculation()
	balanceLimit := ctx.DynProps.ExchangeBalanceLimit()
	newThisBalance, err := exchangeAdd(thisBalance, c.Quant, harden)
	if err != nil {
		return err
	}
	newOtherBalance, err := exchangeAdd(otherBalance, anotherTokenQuant, harden)
	if err != nil {
		return err
	}
	if newThisBalance > balanceLimit || newOtherBalance > balanceLimit {
		return fmt.Errorf("token balance must less than %d", balanceLimit)
	}

	// Owner must have enough of the injected token (java 230-238).
	if err := checkTokenBalance(ctx, ownerAddr, c.TokenId, c.Quant); err != nil {
		return err
	}
	// Owner must also have enough of the other token (java 240-248).
	if err := checkTokenBalance(ctx, ownerAddr, otherTokenId, anotherTokenQuant); err != nil {
		return err
	}
	return nil
}

// Execute injects liquidity into an existing exchange. Mirrors java-tron's
// ExchangeInjectActuator.execute() — the proportional amount is computed with
// floor-divide (BigInt.Div for positive operands).
func (a *ExchangeInjectActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr, err := checkedAddress(c.OwnerAddress, "ownerAddress")
	if err != nil {
		return nil, err
	}
	ex := readExchangeForCurrentFork(ctx, c.ExchangeId)
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

	anotherBig := new(big.Int).Mul(big.NewInt(otherBalance), big.NewInt(c.Quant))
	anotherBig.Div(anotherBig, big.NewInt(thisBalance))
	if !anotherBig.IsInt64() {
		return nil, errors.New("the calculated token quant overflows int64")
	}
	otherQuant := anotherBig.Int64()

	if err := deductToken(ctx, ownerAddr, c.TokenId, c.Quant); err != nil {
		return nil, err
	}
	var otherTokenId []byte
	if firstIsThis {
		otherTokenId = ex.SecondTokenId
	} else {
		otherTokenId = ex.FirstTokenId
	}
	if err := deductToken(ctx, ownerAddr, otherTokenId, otherQuant); err != nil {
		return nil, err
	}

	harden := ctx.DynProps.AllowHardenExchangeCalculation()
	if firstIsThis {
		if ex.FirstTokenBalance, err = addExchangeBalance(ex.FirstTokenBalance, c.Quant, harden); err != nil {
			return nil, err
		}
		if ex.SecondTokenBalance, err = addExchangeBalance(ex.SecondTokenBalance, otherQuant, harden); err != nil {
			return nil, err
		}
	} else {
		if ex.SecondTokenBalance, err = addExchangeBalance(ex.SecondTokenBalance, c.Quant, harden); err != nil {
			return nil, err
		}
		if ex.FirstTokenBalance, err = addExchangeBalance(ex.FirstTokenBalance, otherQuant, harden); err != nil {
			return nil, err
		}
	}
	if err := writeExchangeForCurrentFork(ctx, ex); err != nil {
		return nil, err
	}
	return &Result{ExchangeInjectAnotherAmount: otherQuant, ContractRet: 1}, nil
}

func addExchangeBalance(balance, delta int64, harden bool) (int64, error) {
	return exchangeAdd(balance, delta, harden)
}
