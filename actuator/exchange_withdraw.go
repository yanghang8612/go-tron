package actuator

import (
	"bytes"
	"errors"
	"math"
	"math/big"

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
	// Only the exchange creator may withdraw (java line 172-174).
	if !bytes.Equal(c.OwnerAddress, ex.CreatorAddress) {
		return errors.New("account is not creator")
	}
	// Token must be one of the two in the pool (java 192-194).
	if err := validateExchangeTokenID(ctx, c.TokenId, "token id"); err != nil {
		return err
	}
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
	if !exchangeWithdrawPreciseEnough(otherBalance, c.Quant, thisBalance, anotherTokenQuant, ctx.DynProps.AllowHardenExchangeCalculation()) {
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

	otherBig := new(big.Int).Mul(big.NewInt(otherBalance), big.NewInt(c.Quant))
	otherBig.Quo(otherBig, big.NewInt(thisBalance))
	if !otherBig.IsInt64() {
		return nil, errors.New("withdraw another token quant overflows int64")
	}
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

	harden := ctx.DynProps.AllowHardenExchangeCalculation()
	if firstIsThis {
		if ex.FirstTokenBalance, err = exchangeSub(ex.FirstTokenBalance, c.Quant, harden); err != nil {
			return nil, err
		}
		if ex.SecondTokenBalance, err = exchangeSub(ex.SecondTokenBalance, otherQuant, harden); err != nil {
			return nil, err
		}
	} else {
		if ex.SecondTokenBalance, err = exchangeSub(ex.SecondTokenBalance, c.Quant, harden); err != nil {
			return nil, err
		}
		if ex.FirstTokenBalance, err = exchangeSub(ex.FirstTokenBalance, otherQuant, harden); err != nil {
			return nil, err
		}
	}
	if err := writeExchangeForCurrentFork(ctx, ex); err != nil {
		return nil, err
	}
	return &Result{ExchangeWithdrawAnotherAmount: otherQuant, ContractRet: 1}, nil
}

func exchangeWithdrawPreciseEnough(otherBalance, quant, thisBalance, anotherTokenQuant int64, harden bool) bool {
	if !harden {
		precise := math.Round(float64(otherBalance)*float64(quant)/float64(thisBalance)*10000) / 10000
		remainder := precise - float64(anotherTokenQuant)
		return remainder/float64(anotherTokenQuant) <= 0.0001
	}

	numerator := new(big.Int).Mul(big.NewInt(otherBalance), big.NewInt(quant))
	precise := roundRatScaleHalfUp(new(big.Rat).SetFrac(numerator, big.NewInt(thisBalance)), 4)
	remainder := new(big.Rat).Sub(precise, big.NewRat(anotherTokenQuant, 1))
	threshold := new(big.Rat).Mul(big.NewRat(anotherTokenQuant, 1), big.NewRat(1, 10_000))
	return remainder.Cmp(threshold) <= 0
}
