package actuator

import (
	"bytes"
	"errors"
	"fmt"

	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// ExchangeTransactionActuator handles ExchangeTransactionContract (type 44).
// It executes a swap using the Bancor connector-weight formula implemented in
// exchangeProcessor, mirroring java-tron's ExchangeTransactionActuator +
// ExchangeProcessor.
type ExchangeTransactionActuator struct{}

func (a *ExchangeTransactionActuator) getContract(ctx *Context) (*contractpb.ExchangeTransactionContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.ExchangeTransactionContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal ExchangeTransactionContract")
	}
	return c, nil
}

// Validate checks all preconditions for an ExchangeTransaction swap.
// Mirrors ExchangeTransactionActuator.validate() in java-tron.
func (a *ExchangeTransactionActuator) Validate(ctx *Context) error {
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
	// Token must belong to the exchange (java line 174-176).
	if err := validateExchangeTokenID(ctx, c.TokenId, "token id"); err != nil {
		return err
	}
	if !bytes.Equal(ex.FirstTokenId, c.TokenId) && !bytes.Equal(ex.SecondTokenId, c.TokenId) {
		return errors.New("token is not in exchange")
	}
	// tokenQuant > 0 (java line 178-180).
	if c.Quant <= 0 {
		return errors.New("token quant must greater than zero")
	}
	// tokenExpected > 0 (java line 182-184).
	if c.Expected <= 0 {
		return errors.New("token expected must greater than zero")
	}
	// Exchange must be open (java line 186-189).
	if ex.FirstTokenBalance == 0 || ex.SecondTokenBalance == 0 {
		return errors.New("Token balance in exchange is equal with 0,the exchange has been closed")
	}

	// Work out which side is the sell side and determine balances.
	var sellBalance, buyBalance int64
	if bytes.Equal(ex.FirstTokenId, c.TokenId) {
		sellBalance = ex.FirstTokenBalance
		buyBalance = ex.SecondTokenBalance
	} else {
		sellBalance = ex.SecondTokenBalance
		buyBalance = ex.FirstTokenBalance
	}

	// Pool balance cap (java line 191-197).
	balanceLimit := ctx.DynProps.ExchangeBalanceLimit()
	harden := ctx.DynProps.AllowHardenExchangeCalculation()
	newSellBalance, err := exchangeAdd(sellBalance, c.Quant, harden)
	if err != nil {
		return err
	}
	if newSellBalance > balanceLimit {
		return fmt.Errorf("token balance must less than %d", balanceLimit)
	}

	// Owner has the funds to sell (java line 199-207).
	if err := checkTokenBalance(ctx, ownerAddr, c.TokenId, c.Quant); err != nil {
		return err
	}

	// Bancor quote — note this uses a throwaway processor since Validate must
	// not mutate any persisted state (java calls exchangeCapsule.transaction(..)
	// which mutates a local copy that is discarded on return).
	anotherTokenQuant, err := exchangeQuote(sellBalance, buyBalance, c.Quant, ctx.DynProps.AllowStrictMath(), harden)
	if err != nil {
		return err
	}
	if harden {
		if _, err := exchangeSub(buyBalance, anotherTokenQuant, true); err != nil {
			return err
		}
	}
	if anotherTokenQuant < c.Expected {
		return errors.New("token required must greater than expected")
	}
	return nil
}

// Execute performs the Bancor swap: sells Quant of TokenId, receives the other token.
func (a *ExchangeTransactionActuator) Execute(ctx *Context) (*Result, error) {
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

	var sellBalance, buyBalance int64
	var buyTokenId []byte
	if bytes.Equal(ex.FirstTokenId, c.TokenId) {
		sellBalance = ex.FirstTokenBalance
		buyBalance = ex.SecondTokenBalance
		buyTokenId = ex.SecondTokenId
	} else {
		sellBalance = ex.SecondTokenBalance
		buyBalance = ex.FirstTokenBalance
		buyTokenId = ex.FirstTokenId
	}

	// Fresh processor per execution — supply state is never shared.
	harden := ctx.DynProps.AllowHardenExchangeCalculation()
	receive, err := exchangeQuote(sellBalance, buyBalance, c.Quant, ctx.DynProps.AllowStrictMath(), harden)
	if err != nil {
		return nil, err
	}
	if receive < c.Expected {
		return nil, errors.New("exchange transaction: receive amount below expected")
	}

	if err := deductToken(ctx, ownerAddr, c.TokenId, c.Quant); err != nil {
		return nil, err
	}
	if err := transferToken(ctx, ownerAddr, buyTokenId, receive); err != nil {
		return nil, err
	}

	if bytes.Equal(ex.FirstTokenId, c.TokenId) {
		if ex.FirstTokenBalance, err = exchangeAdd(ex.FirstTokenBalance, c.Quant, harden); err != nil {
			return nil, err
		}
		if ex.SecondTokenBalance, err = exchangeSub(ex.SecondTokenBalance, receive, harden); err != nil {
			return nil, err
		}
	} else {
		if ex.SecondTokenBalance, err = exchangeAdd(ex.SecondTokenBalance, c.Quant, harden); err != nil {
			return nil, err
		}
		if ex.FirstTokenBalance, err = exchangeSub(ex.FirstTokenBalance, receive, harden); err != nil {
			return nil, err
		}
	}
	if err := writeExchangeForCurrentFork(ctx, ex); err != nil {
		return nil, err
	}
	return &Result{ExchangeReceivedAmount: receive, ContractRet: 1}, nil
}
