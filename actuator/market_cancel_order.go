package actuator

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// MarketCancelOrderActuator handles market cancel order transactions (contract type 53).
type MarketCancelOrderActuator struct{}

func (a *MarketCancelOrderActuator) getContract(ctx *Context) (*contractpb.MarketCancelOrderContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.MarketCancelOrderContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal MarketCancelOrderContract")
	}
	return c, nil
}

// Validate checks all preconditions for a MarketCancelOrder transaction.
func (a *MarketCancelOrderActuator) Validate(ctx *Context) error {
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}

	// 1. Owner exists
	ownerAddr := tcommon.BytesToAddress(c.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}

	// 2. OrderId not empty
	if len(c.OrderId) == 0 {
		return errors.New("order ID must not be empty")
	}

	// 3. Order exists in DB
	order := rawdb.ReadMarketOrder(ctx.DB, c.OrderId)
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

	return nil
}

// Execute performs the market cancel order operation.
func (a *MarketCancelOrderActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}

	ownerAddr := tcommon.BytesToAddress(c.OwnerAddress)

	// Step 1: Read order from DB
	order := rawdb.ReadMarketOrder(ctx.DB, c.OrderId)
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
	if err := removeOrderFromBook(ctx.DB, order, pk); err != nil {
		return nil, err
	}

	// Step 4: Update order state
	order.SellTokenQuantityReturn = order.SellTokenQuantityRemain
	order.SellTokenQuantityRemain = 0
	order.State = corepb.MarketOrder_CANCELED

	// Step 5: Write order back
	if err := rawdb.WriteMarketOrder(ctx.DB, c.OrderId, order); err != nil {
		return nil, err
	}

	// Step 6: Decrement account order Count
	mao := rawdb.ReadMarketAccountOrder(ctx.DB, c.OwnerAddress)
	if mao.Count > 0 {
		mao.Count--
	}
	if err := rawdb.WriteMarketAccountOrder(ctx.DB, c.OwnerAddress, mao); err != nil {
		return nil, err
	}

	return &Result{ContractRet: 1}, nil
}

// removeOrderFromBook unlinks the order from the linked list in the order book.
// If the list becomes empty, it deletes the order book entry and removes the price from the price list.
func removeOrderFromBook(db ethdb.KeyValueStore, order *corepb.MarketOrder, pk [16]byte) error {
	ob := rawdb.ReadMarketOrderBook(db, order.SellTokenId, order.BuyTokenId, pk)
	if ob == nil {
		// Already absent — nothing to do
		return nil
	}

	prevID := bytes.Clone(order.Prev)
	nextID := bytes.Clone(order.Next)

	// Update prev's Next pointer
	if len(prevID) > 0 {
		prev := rawdb.ReadMarketOrder(db, prevID)
		if prev != nil {
			prev.Next = nextID
			if err := rawdb.WriteMarketOrder(db, prevID, prev); err != nil {
				return err
			}
		}
	}

	// Update next's Prev pointer
	if len(nextID) > 0 {
		next := rawdb.ReadMarketOrder(db, nextID)
		if next != nil {
			next.Prev = prevID
			if err := rawdb.WriteMarketOrder(db, nextID, next); err != nil {
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
		if err := rawdb.DeleteMarketOrderBook(db, order.SellTokenId, order.BuyTokenId, pk); err != nil {
			return err
		}

		// Remove price from price list
		pl := rawdb.ReadMarketPriceList(db, order.SellTokenId, order.BuyTokenId)
		var remaining []*corepb.MarketPrice
		for _, p := range pl.Prices {
			if rawdb.PriceKey(p.SellTokenQuantity, p.BuyTokenQuantity) != pk {
				remaining = append(remaining, p)
			}
		}
		pl.Prices = remaining
		if err := rawdb.WriteMarketPriceList(db, order.SellTokenId, order.BuyTokenId, pl); err != nil {
			return err
		}
	} else {
		// Write updated order book list back
		if err := rawdb.WriteMarketOrderBook(db, order.SellTokenId, order.BuyTokenId, pk, ob); err != nil {
			return err
		}
	}

	return nil
}
