package actuator

import (
	"bytes"
	"errors"
	"math/big"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

const exchangeCreateFee = int64(1_024_000_000) // 1024 TRX in sun

// ExchangeCreateActuator handles ExchangeCreateContract (type 41).
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
	if c.FirstTokenBalance <= 0 || c.SecondTokenBalance <= 0 {
		return errors.New("token balances must be positive")
	}

	// Check first token balance
	if err := checkTokenBalance(ctx, ownerAddr, c.FirstTokenId, c.FirstTokenBalance); err != nil {
		return err
	}
	// Check second token balance
	if err := checkTokenBalance(ctx, ownerAddr, c.SecondTokenId, c.SecondTokenBalance); err != nil {
		return err
	}
	// Check fee payment: owner must have exchangeCreateFee TRX on top of any TRX already committed
	trxNeeded := exchangeCreateFee
	// If first token is TRX, add it to the TRX needed
	if bytes.Equal(c.FirstTokenId, []byte("_")) {
		trxNeeded = safeAdd(trxNeeded, c.FirstTokenBalance)
	}
	// If second token is TRX, add it too
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

	// Deduct creation fee
	if err := ctx.State.SubBalance(ownerAddr, exchangeCreateFee); err != nil {
		return nil, err
	}
	// Deduct first token
	if err := deductToken(ctx, ownerAddr, c.FirstTokenId, c.FirstTokenBalance); err != nil {
		return nil, err
	}
	// Deduct second token
	if err := deductToken(ctx, ownerAddr, c.SecondTokenId, c.SecondTokenBalance); err != nil {
		return nil, err
	}

	// Assign exchange ID and increment counter
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
	return &Result{Fee: exchangeCreateFee, ContractRet: 1}, nil
}

func safeAdd(a, b int64) int64 {
	r := new(big.Int).Add(big.NewInt(a), big.NewInt(b))
	if !r.IsInt64() {
		return int64(^uint64(0) >> 1) // max int64
	}
	return r.Int64()
}
