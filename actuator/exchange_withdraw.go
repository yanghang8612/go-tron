package actuator

import (
	"bytes"
	"errors"
	"math"
	"math/big"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// ExchangeWithdrawActuator handles ExchangeWithdrawContract (type 43).
// Mirrors java-tron's ExchangeWithdrawActuator.
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
// Mirrors ExchangeWithdrawActuator.validate() in java-tron.
func (a *ExchangeWithdrawActuator) Validate(ctx *Context) error {
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := tcommon.BytesToAddress(c.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	ex := rawdb.ReadExchange(ctx.DB, c.ExchangeId)
	if ex == nil {
		return errors.New("exchange not found")
	}
	// Only the exchange creator may withdraw (java line 172-174).
	if !bytes.Equal(c.OwnerAddress, ex.CreatorAddress) {
		return errors.New("account is not creator")
	}
	// Token must be one of the two in the pool (java 192-194).
	if !bytes.Equal(ex.FirstTokenId, c.TokenId) && !bytes.Equal(ex.SecondTokenId, c.TokenId) {
		return errors.New("token is not in exchange")
	}
	// withdraw quant > 0 (java 196-198).
	if c.Quant <= 0 {
		return errors.New("withdraw token quant must greater than zero")
	}
	// Exchange must be open (java 200-203).
	if ex.FirstTokenBalance == 0 || ex.SecondTokenBalance == 0 {
		return errors.New("Token balance in exchange is equal with 0,the exchange has been closed")
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

	// anotherTokenQuant := floor(otherBalance * tokenQuant / thisBalance)
	// (java uses BigDecimal.divideToIntegralValue which truncates toward zero).
	otherBig := new(big.Int).Mul(big.NewInt(otherBalance), big.NewInt(c.Quant))
	otherBig.Quo(otherBig, big.NewInt(thisBalance))
	if !otherBig.IsInt64() {
		return errors.New("withdraw another token quant overflows int64")
	}
	anotherTokenQuant := otherBig.Int64()

	// Exchange must have enough to pay out (java 211-213 / 229-231).
	if thisBalance < c.Quant || otherBalance < anotherTokenQuant {
		return errors.New("exchange balance is not enough")
	}

	// Derived amount must be positive (java 215-217 / 233-235).
	if anotherTokenQuant <= 0 {
		return errors.New("withdraw another token quant must greater than zero")
	}

	// Precision guard (java 219-224 / 237-242):
	//   precise := round(otherBalance * quant / thisBalance, 4 decimals, HALF_UP)
	//   remainder := precise - anotherTokenQuant
	//   if remainder / anotherTokenQuant > 0.0001 -> "Not precise enough"
	// Go's math.Round is half-away-from-zero; for these positive-integer-derived
	// operands the difference from Java's HALF_UP is negligible.
	precise := math.Round(float64(otherBalance)*float64(c.Quant)/float64(thisBalance)*10000) / 10000
	remainder := precise - float64(anotherTokenQuant)
	if remainder/float64(anotherTokenQuant) > 0.0001 {
		return errors.New("Not precise enough")
	}
	return nil
}

// Execute removes liquidity from an exchange and credits tokens to owner.
// Mirrors java-tron's ExchangeWithdrawActuator.execute().
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

	otherBig := new(big.Int).Mul(big.NewInt(otherBalance), big.NewInt(c.Quant))
	otherBig.Quo(otherBig, big.NewInt(thisBalance))
	otherQuant := otherBig.Int64()

	var otherTokenId []byte
	if firstIsThis {
		otherTokenId = ex.SecondTokenId
	} else {
		otherTokenId = ex.FirstTokenId
	}

	if err := transferToken(ctx, ownerAddr, c.TokenId, c.Quant); err != nil {
		return nil, err
	}
	if err := transferToken(ctx, ownerAddr, otherTokenId, otherQuant); err != nil {
		return nil, err
	}

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
