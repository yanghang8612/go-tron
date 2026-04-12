package actuator

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
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
	ownerAddr := tcommon.BytesToAddress(c.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	// Cannot pair a token with itself (java line 188-190).
	if bytes.Equal(c.FirstTokenId, c.SecondTokenId) {
		return errors.New("cannot exchange same tokens")
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
	trxNeeded := fee
	if bytes.Equal(c.FirstTokenId, []byte("_")) {
		trxNeeded = safeAdd(trxNeeded, c.FirstTokenBalance)
	}
	if bytes.Equal(c.SecondTokenId, []byte("_")) {
		trxNeeded = safeAdd(trxNeeded, c.SecondTokenBalance)
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
	ownerAddr := tcommon.BytesToAddress(c.OwnerAddress)
	fee := ctx.DynProps.ExchangeCreateFee()

	if err := ctx.State.SubBalance(ownerAddr, fee); err != nil {
		return nil, err
	}
	if err := deductToken(ctx, ownerAddr, c.FirstTokenId, c.FirstTokenBalance); err != nil {
		return nil, err
	}
	if err := deductToken(ctx, ownerAddr, c.SecondTokenId, c.SecondTokenBalance); err != nil {
		return nil, err
	}

	exchangeID := ctx.DynProps.NextExchangeID()
	ctx.DynProps.SetNextExchangeID(exchangeID + 1)

	ex := &corepb.Exchange{
		ExchangeId:         exchangeID,
		CreatorAddress:     c.OwnerAddress,
		CreateTime:         ctx.BlockTime,
		FirstTokenId:       c.FirstTokenId,
		FirstTokenBalance:  c.FirstTokenBalance,
		SecondTokenId:      c.SecondTokenId,
		SecondTokenBalance: c.SecondTokenBalance,
	}
	if err := rawdb.WriteExchange(ctx.DB, ex); err != nil {
		return nil, err
	}
	return &Result{Fee: fee, ContractRet: 1}, nil
}

// safeAdd adds two int64 values, saturating at MaxInt64 on overflow.
func safeAdd(a, b int64) int64 {
	r := new(big.Int).Add(big.NewInt(a), big.NewInt(b))
	if !r.IsInt64() {
		return int64(^uint64(0) >> 1) // max int64
	}
	return r.Int64()
}
