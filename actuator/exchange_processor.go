package actuator

import (
	"errors"
	"math"
	"math/big"
	"strconv"

	"github.com/tronprotocol/go-tron/internal/math/strictmath"
)

// exchangeProcessor implements the Bancor connector-weight pricing used by
// java-tron's ExchangeProcessor (chainbase/.../ExchangeProcessor.java).
//
// The state variable `supply` is mutated across the two-step exchange
// (sell-side increases supply, buy-side decreases it), and must NOT be
// shared across transactions.
type exchangeProcessor struct {
	supply        int64
	useStrictMath bool
}

// exchangeProcessorSupply matches java-tron ExchangeCapsule.supply (1e18).
const exchangeProcessorSupply int64 = 1_000_000_000_000_000_000

// newExchangeProcessor returns a fresh processor initialized to the canonical
// supply. After proposal #87 (`allow_strict_math`) activates, callers must
// pass `useStrictMath=true` so the pow calls route through `strictmath.Pow`
// (java-tron `StrictMath.pow` parity).
func newExchangeProcessor(useStrictMath bool) *exchangeProcessor {
	return &exchangeProcessor{supply: exchangeProcessorSupply, useStrictMath: useStrictMath}
}

// pow routes between math.Pow and strictmath.Pow per the gate.
func (p *exchangeProcessor) pow(a, b float64) float64 {
	if p.useStrictMath {
		return strictmath.Pow(a, b)
	}
	return math.Pow(a, b)
}

// exchangeToSupply mints "supply" tokens from a sale of `quant` of the sell token.
// Mirrors ExchangeProcessor.exchangeToSupply — note the integer truncation of
// `(long) issuedSupply`, which Go's int64(float64) conversion reproduces
// (truncation toward zero).
func (p *exchangeProcessor) exchangeToSupply(balance, quant int64) int64 {
	newBalance := balance + quant
	issued := -float64(p.supply) * (1.0 - p.pow(1.0+float64(quant)/float64(newBalance), 0.0005))
	out := int64(issued)
	p.supply += out
	return out
}

// exchangeFromSupply burns supply tokens to redeem the buy-side token.
// Mirrors ExchangeProcessor.exchangeFromSupply.
func (p *exchangeProcessor) exchangeFromSupply(balance, supplyQuant int64) int64 {
	p.supply -= supplyQuant
	exchangeBalance := float64(balance) * (p.pow(1.0+float64(supplyQuant)/float64(p.supply), 2000.0) - 1.0)
	return int64(exchangeBalance)
}

// exchange computes the buy-side payout for selling `sellTokenQuant` of the
// sell token into a pool with `sellTokenBalance` and `buyTokenBalance`.
// Mirrors ExchangeProcessor.exchange (chainbase/.../ExchangeProcessor.java).
func (p *exchangeProcessor) exchange(sellTokenBalance, buyTokenBalance, sellTokenQuant int64) int64 {
	relay := p.exchangeToSupply(sellTokenBalance, sellTokenQuant)
	return p.exchangeFromSupply(buyTokenBalance, relay)
}

func exchangeQuote(sellTokenBalance, buyTokenBalance, sellTokenQuant int64, useStrictMath, harden bool) (int64, error) {
	if harden {
		return safeExchange(sellTokenBalance, buyTokenBalance, sellTokenQuant)
	}
	return newExchangeProcessor(useStrictMath).exchange(sellTokenBalance, buyTokenBalance, sellTokenQuant), nil
}

func safeExchange(sellTokenBalance, buyTokenBalance, sellTokenQuant int64) (int64, error) {
	relay, err := safeExchangeToSupply(sellTokenBalance, sellTokenQuant)
	if err != nil {
		return 0, err
	}
	return safeExchangeFromSupply(buyTokenBalance, relay)
}

func safeExchangeToSupply(balance, quant int64) (*big.Rat, error) {
	newBalance, ok := checkedAddInt64(balance, quant)
	if !ok {
		return nil, errors.New("exchange balance overflows int64")
	}
	if newBalance == 0 {
		return nil, errors.New("exchange balance division by zero")
	}

	div := roundRatScaleHalfUp(big.NewRat(quant, newBalance), 18)
	base := new(big.Rat).Add(big.NewRat(1, 1), div)
	powRat, err := ratFromFloat64Decimal(strictmath.Pow(ratToFloat64(base), 0.0005))
	if err != nil {
		return nil, err
	}
	oneMinusPow := new(big.Rat).Sub(big.NewRat(1, 1), powRat)
	issued := new(big.Rat).Mul(big.NewRat(-exchangeProcessorSupply, 1), oneMinusPow)
	return truncRatScale0Down(issued), nil
}

func safeExchangeFromSupply(balance int64, supplyQuant *big.Rat) (int64, error) {
	div := roundRatScaleHalfUp(new(big.Rat).Quo(supplyQuant, big.NewRat(exchangeProcessorSupply, 1)), 18)
	base := new(big.Rat).Add(big.NewRat(1, 1), div)
	powRat, err := ratFromFloat64Decimal(strictmath.Pow(ratToFloat64(base), 2000.0))
	if err != nil {
		return 0, err
	}
	exchangeBalance := new(big.Rat).Mul(big.NewRat(balance, 1), new(big.Rat).Sub(powRat, big.NewRat(1, 1)))
	return ratToInt64Trunc(truncRatScale0Down(exchangeBalance))
}

func roundRatScaleHalfUp(x *big.Rat, scale int) *big.Rat {
	pow10 := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	scaled := new(big.Rat).Mul(x, new(big.Rat).SetInt(pow10))
	num := new(big.Int).Set(scaled.Num())
	den := new(big.Int).Set(scaled.Denom())
	q, rem := new(big.Int), new(big.Int)
	q.QuoRem(num, den, rem)
	if rem.Sign() != 0 {
		doubleRem := new(big.Int).Mul(new(big.Int).Abs(rem), big.NewInt(2))
		if doubleRem.Cmp(den) >= 0 {
			if scaled.Sign() >= 0 {
				q.Add(q, big.NewInt(1))
			} else {
				q.Sub(q, big.NewInt(1))
			}
		}
	}
	return new(big.Rat).SetFrac(q, pow10)
}

func truncRatScale0Down(x *big.Rat) *big.Rat {
	q := new(big.Int).Quo(x.Num(), x.Denom())
	return new(big.Rat).SetInt(q)
}

func ratToInt64Trunc(x *big.Rat) (int64, error) {
	q := new(big.Int).Quo(x.Num(), x.Denom())
	if !q.IsInt64() {
		return 0, errors.New("exchange result overflows int64")
	}
	return q.Int64(), nil
}

func ratToFloat64(x *big.Rat) float64 {
	f, _ := new(big.Float).SetPrec(64).SetRat(x).Float64()
	return f
}

func ratFromFloat64Decimal(v float64) (*big.Rat, error) {
	r, ok := new(big.Rat).SetString(strconv.FormatFloat(v, 'g', -1, 64))
	if !ok {
		return nil, errors.New("failed to convert exchange float result")
	}
	return r, nil
}

func exchangeAdd(balance, delta int64, harden bool) (int64, error) {
	if !harden {
		return balance + delta, nil
	}
	next, ok := checkedAddInt64(balance, delta)
	if !ok {
		return 0, errors.New("exchange balance overflows int64")
	}
	return next, nil
}

func exchangeSub(balance, delta int64, harden bool) (int64, error) {
	if !harden {
		return balance - delta, nil
	}
	next, ok := checkedAddInt64(balance, -delta)
	if !ok {
		return 0, errors.New("exchange balance overflows int64")
	}
	if next < 0 {
		return 0, errors.New("exchange balance must be >=0 after transaction")
	}
	return next, nil
}
