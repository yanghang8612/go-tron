package actuator

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// MarketCancelOrderActuator handles market cancel order transactions (contract type 53).
type MarketCancelOrderActuator struct{}

func (a *MarketCancelOrderActuator) getContract(ctx *Context) (*contractpb.MarketCancelOrderContract, error) {
	return decodedContract[*contractpb.MarketCancelOrderContract](ctx, "MarketCancelOrderContract")
}

// Validate checks all preconditions for a MarketCancelOrder transaction.
func (a *MarketCancelOrderActuator) Validate(ctx *Context) error {
	if !forks.IsActive(forks.AllowMarketTransaction, ctx.BlockNumber, ctx.DynProps) {
		return errors.New("market transactions not yet enabled")
	}
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}

	// 1. Owner exists
	ownerAddr, err := checkedAddress(c.OwnerAddress, "ownerAddress")
	if err != nil {
		return err
	}
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}

	// 2. OrderId not empty
	if len(c.OrderId) == 0 {
		return errors.New("order ID must not be empty")
	}

	// 3. Order exists in DB
	order := ctx.State.ReadMarketOrder(c.OrderId)
	if order == nil {
		return errors.New("order does not exist")
	}

	// 4. OwnerAddress matches
	if !bytes.Equal(order.OwnerAddress, c.OwnerAddress) {
		return errors.New("owner address does not match order owner")
	}

	// 5. Order state is ACTIVE
	if order.State != corepb.MarketOrder_ACTIVE {
		return errors.New("order is not ACTIVE")
	}

	// 6. Sufficient balance for cancel fee (may be zero, but still checked)
	fee := ctx.DynProps.MarketCancelFee()
	if ctx.State.GetBalance(ownerAddr) < fee {
		return errors.New("insufficient balance for market cancel fee")
	}

	return nil
}

// Execute performs the market cancel order operation.
func (a *MarketCancelOrderActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}

	ownerAddr, err := checkedAddress(c.OwnerAddress, "ownerAddress")
	if err != nil {
		return nil, err
	}
	fee := ctx.DynProps.MarketCancelFee()
	if err := burnFee(ctx, ownerAddr, fee); err != nil {
		return nil, err
	}

	// Step 1: Read order from DB
	order := ctx.State.ReadMarketOrder(c.OrderId)
	if order == nil {
		return nil, errors.New("order does not exist")
	}

	// Step 2: Return remaining sell tokens to owner
	if order.SellTokenQuantityRemain > 0 {
		if err := transferToken(ctx, ownerAddr, order.SellTokenId, order.SellTokenQuantityRemain); err != nil {
			return nil, fmt.Errorf("return sell tokens: %w", err)
		}
	}

	// Step 3: Remove from order book
	pk := rawdb.PriceKey(order.SellTokenQuantity, order.BuyTokenQuantity)
	if err := removeOrderFromBook(ctx.State, order, pk); err != nil {
		return nil, err
	}

	// Step 4: Update order state
	order.SellTokenQuantityRemain = 0
	order.State = corepb.MarketOrder_CANCELED

	// Step 5: Write order back
	if err := ctx.State.WriteMarketOrder(c.OrderId, order); err != nil {
		return nil, err
	}

	// Step 6: Decrement account order Count
	if err := removeMarketAccountOrder(ctx, c.OwnerAddress, c.OrderId); err != nil {
		return nil, err
	}

	return &Result{Fee: fee, ContractRet: 1}, nil
}

// removeOrderFromBook unlinks the order from the linked list in the order book.
// If the list becomes empty, it deletes the order book entry and removes the
// price from the price list. Market state is rooted (SystemMarket KV), so reads
// and writes go through the rooted StateDB.
func removeOrderFromBook(st *state.StateDB, order *corepb.MarketOrder, pk [16]byte) error {
	ob := st.ReadMarketOrderBook(order.SellTokenId, order.BuyTokenId, pk)
	if ob == nil {
		// Already absent — nothing to do
		return nil
	}

	prevID := bytes.Clone(order.Prev)
	nextID := bytes.Clone(order.Next)

	// Update prev's Next pointer
	if len(prevID) > 0 {
		prev := st.ReadMarketOrder(prevID)
		if prev != nil {
			prev.Next = nextID
			if err := st.WriteMarketOrder(prevID, prev); err != nil {
				return err
			}
		}
	}

	// Update next's Prev pointer
	if len(nextID) > 0 {
		next := st.ReadMarketOrder(nextID)
		if next != nil {
			next.Prev = prevID
			if err := st.WriteMarketOrder(nextID, next); err != nil {
				return err
			}
		}
	}

	// Update head/tail if this was the head or tail
	isHead := bytes.Equal(ob.Head, order.OrderId)
	isTail := bytes.Equal(ob.Tail, order.OrderId)

	if isHead {
		ob.Head = nextID
	}
	if isTail {
		ob.Tail = prevID
	}

	// Check if list is now empty
	if len(ob.Head) == 0 {
		// Delete the order book entry
		if err := st.DeleteMarketOrderBook(order.SellTokenId, order.BuyTokenId, pk); err != nil {
			return err
		}

		// Remove price from price list
		pl := st.ReadMarketPriceList(order.SellTokenId, order.BuyTokenId)
		var remaining []*corepb.MarketPrice
		for _, p := range pl.Prices {
			if rawdb.PriceKey(p.SellTokenQuantity, p.BuyTokenQuantity) != pk {
				remaining = append(remaining, p)
			}
		}
		pl.Prices = remaining
		if err := st.WriteMarketPriceList(order.SellTokenId, order.BuyTokenId, pl); err != nil {
			return err
		}
		count := st.ReadMarketPairPriceCount(order.SellTokenId, order.BuyTokenId)
		if count <= 1 {
			if err := st.DeleteMarketPairPriceCount(order.SellTokenId, order.BuyTokenId); err != nil {
				return err
			}
		} else {
			if err := st.WriteMarketPairPriceCount(order.SellTokenId, order.BuyTokenId, count-1); err != nil {
				return err
			}
		}
	} else {
		// Write updated order book list back
		if err := st.WriteMarketOrderBook(order.SellTokenId, order.BuyTokenId, pk, ob); err != nil {
			return err
		}
	}

	return nil
}
