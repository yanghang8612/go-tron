package actuator

import "math"

// exchangeProcessor implements the Bancor connector-weight pricing used by
// java-tron's ExchangeProcessor (chainbase/.../ExchangeProcessor.java).
//
// The state variable `supply` is mutated across the two-step exchange
// (sell-side increases supply, buy-side decreases it), and must NOT be
// shared across transactions.
type exchangeProcessor struct {
	supply int64
}

// exchangeProcessorSupply matches java-tron ExchangeCapsule.supply (1e18).
const exchangeProcessorSupply int64 = 1_000_000_000_000_000_000

// newExchangeProcessor returns a fresh processor initialized to the canonical supply.
func newExchangeProcessor() *exchangeProcessor {
	return &exchangeProcessor{supply: exchangeProcessorSupply}
}

// exchangeToSupply mints "supply" tokens from a sale of `quant` of the sell token.
// Mirrors ExchangeProcessor.exchangeToSupply — note the integer truncation of
// `(long) issuedSupply`, which Go's int64(float64) conversion reproduces
// (truncation toward zero).
func (p *exchangeProcessor) exchangeToSupply(balance, quant int64) int64 {
	newBalance := balance + quant
	issued := -float64(p.supply) * (1.0 - math.Pow(1.0+float64(quant)/float64(newBalance), 0.0005))
	out := int64(issued)
	p.supply += out
	return out
}

// exchangeFromSupply burns supply tokens to redeem the buy-side token.
// Mirrors ExchangeProcessor.exchangeFromSupply.
func (p *exchangeProcessor) exchangeFromSupply(balance, supplyQuant int64) int64 {
	p.supply -= supplyQuant
	exchangeBalance := float64(balance) * (math.Pow(1.0+float64(supplyQuant)/float64(p.supply), 2000.0) - 1.0)
	return int64(exchangeBalance)
}

// exchange computes the buy-side payout for selling `sellTokenQuant` of the
// sell token into a pool with `sellTokenBalance` and `buyTokenBalance`.
// Mirrors ExchangeProcessor.exchange (chainbase/.../ExchangeProcessor.java).
func (p *exchangeProcessor) exchange(sellTokenBalance, buyTokenBalance, sellTokenQuant int64) int64 {
	relay := p.exchangeToSupply(sellTokenBalance, sellTokenQuant)
	return p.exchangeFromSupply(buyTokenBalance, relay)
}
