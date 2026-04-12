package actuator

import (
	"bytes"
	"errors"
	"math/big"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// ExchangeTransactionActuator handles ExchangeTransactionContract (type 44).
// It executes an AMM swap using the constant product formula:
// receive = otherBalance * quant / (thisBalance + quant).
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
func (a *ExchangeTransactionActuator) Validate(ctx *Context) error {
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
	// Verify the token is in the exchange
	if !bytes.Equal(ex.FirstTokenId, c.TokenId) && !bytes.Equal(ex.SecondTokenId, c.TokenId) {
		return errors.New("token not in exchange")
	}
	// Verify owner has enough to sell
	if err := checkTokenBalance(ctx, ownerAddr, c.TokenId, c.Quant); err != nil {
		return err
	}
	return nil
}

// Execute performs the AMM swap: sells quant of tokenId, receives the other token.
func (a *ExchangeTransactionActuator) Execute(ctx *Context) (*Result, error) {
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

	// AMM constant-product: receive = otherBalance * quant / (thisBalance + quant)
	receiveBI := new(big.Int).Mul(big.NewInt(otherBalance), big.NewInt(c.Quant))
	receiveBI.Div(receiveBI, big.NewInt(thisBalance+c.Quant))
	receive := receiveBI.Int64()

	// Slippage guard
	if receive < c.Expected {
		return nil, errors.New("exchange transaction: receive amount below expected")
	}

	// Transfer sold token from owner to exchange (deduct from owner)
	if err := deductToken(ctx, ownerAddr, c.TokenId, c.Quant); err != nil {
		return nil, err
	}

	// Transfer received token from exchange to owner
	var otherTokenId []byte
	if firstIsThis {
		otherTokenId = ex.SecondTokenId
	} else {
		otherTokenId = ex.FirstTokenId
	}
	if err := transferToken(ctx, ownerAddr, otherTokenId, receive); err != nil {
		return nil, err
	}

	// Update exchange state
	if firstIsThis {
		ex.FirstTokenBalance += c.Quant
		ex.SecondTokenBalance -= receive
	} else {
		ex.SecondTokenBalance += c.Quant
		ex.FirstTokenBalance -= receive
	}
	if err := rawdb.WriteExchange(ctx.DB, ex); err != nil {
		return nil, err
	}
	return &Result{ContractRet: 1}, nil
}
