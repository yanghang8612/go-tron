package actuator

import (
	"bytes"
	"errors"
	"fmt"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// ExchangeCreateActuator handles ExchangeCreateContract (type 41).
// Mirrors java-tron's ExchangeCreateActuator.
type ExchangeCreateActuator struct{}

func (a *ExchangeCreateActuator) getContract(ctx *Context) (*contractpb.ExchangeCreateContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.ExchangeCreateContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal ExchangeCreateContract")
	}
	return c, nil
}

// Validate checks all preconditions for an ExchangeCreate transaction.
func (a *ExchangeCreateActuator) Validate(ctx *Context) error {
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
	// Cannot pair a token with itself (java line 188-190).
	if bytes.Equal(c.FirstTokenId, c.SecondTokenId) {
		return errors.New("cannot exchange same tokens")
	}
	if err := validateExchangeTokenID(ctx, c.FirstTokenId, "first token id"); err != nil {
		return err
	}
	if err := validateExchangeTokenID(ctx, c.SecondTokenId, "second token id"); err != nil {
		return err
	}
	if c.FirstTokenBalance <= 0 || c.SecondTokenBalance <= 0 {
		return errors.New("token balance must greater than zero")
	}
	// Per-token balance cap (java 196-199).
	balanceLimit := ctx.DynProps.ExchangeBalanceLimit()
	if c.FirstTokenBalance > balanceLimit || c.SecondTokenBalance > balanceLimit {
		return fmt.Errorf("token balance must less than %d", balanceLimit)
	}

	if err := checkTokenBalance(ctx, ownerAddr, c.FirstTokenId, c.FirstTokenBalance); err != nil {
		return err
	}
	if err := checkTokenBalance(ctx, ownerAddr, c.SecondTokenId, c.SecondTokenBalance); err != nil {
		return err
	}
	// Owner must have enough TRX for fee, plus any TRX they are committing to the pool.
	fee := ctx.DynProps.ExchangeCreateFee()
	harden := ctx.DynProps.AllowHardenExchangeCalculation()
	trxNeeded := fee
	if bytes.Equal(c.FirstTokenId, []byte("_")) {
		var err error
		trxNeeded, err = exchangeAdd(trxNeeded, c.FirstTokenBalance, harden)
		if err != nil {
			return err
		}
	}
	if bytes.Equal(c.SecondTokenId, []byte("_")) {
		var err error
		trxNeeded, err = exchangeAdd(trxNeeded, c.SecondTokenBalance, harden)
		if err != nil {
			return err
		}
	}
	if ctx.State.GetBalance(ownerAddr) < trxNeeded {
		return errors.New("insufficient TRX for fee and token deposit")
	}
	return nil
}

// Execute creates the exchange and deducts tokens + fee from owner.
func (a *ExchangeCreateActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr, err := checkedAddress(c.OwnerAddress, "ownerAddress")
	if err != nil {
		return nil, err
	}
	fee := ctx.DynProps.ExchangeCreateFee()

	if err := burnFee(ctx, ownerAddr, fee); err != nil {
		return nil, err
	}
	if err := deductToken(ctx, ownerAddr, c.FirstTokenId, c.FirstTokenBalance); err != nil {
		return nil, err
	}
	if err := deductToken(ctx, ownerAddr, c.SecondTokenId, c.SecondTokenBalance); err != nil {
		return nil, err
	}

	exchangeID := ctx.DynProps.LatestExchangeNum() + 1
	ctx.DynProps.SetLatestExchangeNum(exchangeID)

	ex := &corepb.Exchange{
		ExchangeId:         exchangeID,
		CreatorAddress:     c.OwnerAddress,
		CreateTime:         ctx.DynProps.LatestBlockHeaderTimestamp(),
		FirstTokenId:       c.FirstTokenId,
		FirstTokenBalance:  c.FirstTokenBalance,
		SecondTokenId:      c.SecondTokenId,
		SecondTokenBalance: c.SecondTokenBalance,
	}
	if err := writeExchangeForCurrentFork(ctx, ex); err != nil {
		return nil, err
	}
	return &Result{Fee: fee, ExchangeID: exchangeID, ContractRet: 1}, nil
}

func validateExchangeTokenID(ctx *Context, tokenID []byte, field string) error {
	if !ctx.DynProps.AllowSameTokenName() || bytes.Equal(tokenID, []byte("_")) {
		return nil
	}
	if !isNumericBytes(tokenID) {
		return fmt.Errorf("%s is not a valid number", field)
	}
	return nil
}
