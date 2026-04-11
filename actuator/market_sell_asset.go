package actuator

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strconv"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// MarketSellAssetActuator handles market sell asset transactions (contract type 52).
type MarketSellAssetActuator struct{}

func (a *MarketSellAssetActuator) getContract(ctx *Context) (*contractpb.MarketSellAssetContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.MarketSellAssetContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal MarketSellAssetContract")
	}
	return c, nil
}

// Validate checks all preconditions for a MarketSellAsset transaction.
func (a *MarketSellAssetActuator) Validate(ctx *Context) error {
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}

	ownerAddr := tcommon.BytesToAddress(c.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if c.SellTokenQuantity <= 0 {
		return errors.New("sell token quantity must be positive")
	}
	if c.BuyTokenQuantity <= 0 {
		return errors.New("buy token quantity must be positive")
	}
	if bytes.Equal(c.SellTokenId, c.BuyTokenId) {
		return errors.New("sell token and buy token must be different")
	}

	// Check owner has sufficient balance of sell token
	if err := checkTokenBalance(ctx, ownerAddr, c.SellTokenId, c.SellTokenQuantity); err != nil {
		return err
	}
	return nil
}

// Execute performs the market sell asset operation.
func (a *MarketSellAssetActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}

	ownerAddr := tcommon.BytesToAddress(c.OwnerAddress)

	// Step 1: Deduct sell tokens from owner (put in escrow)
	if err := deductToken(ctx, ownerAddr, c.SellTokenId, c.SellTokenQuantity); err != nil {
		return nil, err
	}

	// Step 2: Generate order ID
	txHash := ctx.Tx.Hash()
	input := make([]byte, 0, 21+32)
	input = append(input, ownerAddr[:]...)
	input = append(input, txHash[:]...)
	orderID := generateOrderID(input)

	// Step 3: Create MarketOrder proto
	order := &corepb.MarketOrder{
		OrderId:                 orderID,
		OwnerAddress:            c.OwnerAddress,
		CreateTime:              ctx.BlockTime,
		SellTokenId:             c.SellTokenId,
		SellTokenQuantity:       c.SellTokenQuantity,
		BuyTokenId:              c.BuyTokenId,
		BuyTokenQuantity:        c.BuyTokenQuantity,
		SellTokenQuantityRemain: c.SellTokenQuantity,
		SellTokenQuantityReturn: 0,
		State:                   corepb.MarketOrder_ACTIVE,
	}

	// Step 4: Run matching engine
	totalBuyReceived, err := matchOrder(ctx, order)
	if err != nil {
		return nil, err
	}

	// Step 5: Credit incoming owner with what was received during matching
	if totalBuyReceived > 0 {
		if err := transferToken(ctx, ownerAddr, c.BuyTokenId, totalBuyReceived); err != nil {
			return nil, fmt.Errorf("credit buy tokens: %w", err)
		}
	}

	// Step 6: If remaining > 0, add to order book; else mark INACTIVE
	if order.SellTokenQuantityRemain > 0 {
		order.State = corepb.MarketOrder_ACTIVE
		if err := addOrderToBook(ctx, order); err != nil {
			return nil, err
		}
	} else {
		order.State = corepb.MarketOrder_INACTIVE
	}

	// Step 7: Write the order
	if err := rawdb.WriteMarketOrder(ctx.DB, orderID, order); err != nil {
		return nil, err
	}

	// Step 8: Update MarketAccountOrder
	mao := rawdb.ReadMarketAccountOrder(ctx.DB, c.OwnerAddress)
	mao.Orders = append(mao.Orders, orderID)
	mao.Count++
	mao.TotalCount++
	if err := rawdb.WriteMarketAccountOrder(ctx.DB, c.OwnerAddress, mao); err != nil {
		return nil, err
	}

	return &Result{ContractRet: 1}, nil
}

// generateOrderID creates a unique order ID by hashing owner address + tx hash.
func generateOrderID(input []byte) []byte {
	h := tcommon.Keccak256(input)
	return h[:]
}

// checkTokenBalance verifies the owner has at least `amount` of the given tokenID.
func checkTokenBalance(ctx *Context, ownerAddr tcommon.Address, tokenID []byte, amount int64) error {
	if bytes.Equal(tokenID, []byte("_")) {
		if ctx.State.GetBalance(ownerAddr) < amount {
			return errors.New("insufficient TRX balance")
		}
		return nil
	}
	tid, err := strconv.ParseInt(string(tokenID), 10, 64)
	if err != nil {
		return errors.New("invalid TRC10 token ID")
	}
	if ctx.State.GetTRC10Balance(ownerAddr, tid) < amount {
		return errors.New("insufficient TRC10 balance")
	}
	return nil
}

// transferToken credits amount of tokenID to addr.
func transferToken(ctx *Context, addr tcommon.Address, tokenID []byte, amount int64) error {
	if bytes.Equal(tokenID, []byte("_")) {
		ctx.State.AddBalance(addr, amount)
		return nil
	}
	tid, err := strconv.ParseInt(string(tokenID), 10, 64)
	if err != nil {
		return errors.New("invalid TRC10 token ID")
	}
	ctx.State.AddTRC10Balance(addr, tid, amount)
	return nil
}

// deductToken deducts amount of tokenID from addr.
func deductToken(ctx *Context, addr tcommon.Address, tokenID []byte, amount int64) error {
	if bytes.Equal(tokenID, []byte("_")) {
		return ctx.State.SubBalance(addr, amount)
	}
	tid, err := strconv.ParseInt(string(tokenID), 10, 64)
	if err != nil {
		return errors.New("invalid TRC10 token ID")
	}
	return ctx.State.SubTRC10Balance(addr, tid, amount)
}

// matchOrder runs the matching engine for the incoming order.
// It returns the total amount of buy tokens received by the incoming order owner.
func matchOrder(ctx *Context, incoming *corepb.MarketOrder) (int64, error) {
	// Get opposite price list: what's selling what we want to buy, for what we're selling
	// Opposite: sellTokenId = incoming.BuyTokenId, buyTokenId = incoming.SellTokenId
	oppPL := rawdb.ReadMarketPriceList(ctx.DB, incoming.BuyTokenId, incoming.SellTokenId)
	if oppPL == nil || len(oppPL.Prices) == 0 {
		return 0, nil
	}

	// Filter compatible prices.
	// incoming wants: sell SellToken, get BuyToken at ratio BuyTokenQuantity/SellTokenQuantity
	// existing (opp) order: sells BuyToken (our incoming buy), gets SellToken (our incoming sell)
	//   at ratio SellTokenQuantity/BuyTokenQuantity
	// Compatible means: oppSell * inSell >= inBuy * oppBuy (using big.Int to avoid overflow)
	// where:
	//   oppSell = opp.SellTokenQuantity (they sell the token we want)
	//   oppBuy  = opp.BuyTokenQuantity  (they want the token we're selling)
	//   inSell  = incoming.SellTokenQuantity
	//   inBuy   = incoming.BuyTokenQuantity
	inSell := big.NewInt(incoming.SellTokenQuantity)
	inBuy := big.NewInt(incoming.BuyTokenQuantity)

	type compatPrice struct {
		price    *corepb.MarketPrice
		ratioNum *big.Int // oppSell
		ratioDen *big.Int // oppBuy
	}
	var compatible []compatPrice

	for _, p := range oppPL.Prices {
		oppSell := big.NewInt(p.SellTokenQuantity)
		oppBuy := big.NewInt(p.BuyTokenQuantity)
		// oppSell * inSell >= inBuy * oppBuy
		lhs := new(big.Int).Mul(oppSell, inSell)
		rhs := new(big.Int).Mul(inBuy, oppBuy)
		if lhs.Cmp(rhs) >= 0 {
			compatible = append(compatible, compatPrice{
				price:    p,
				ratioNum: oppSell,
				ratioDen: oppBuy,
			})
		}
	}

	if len(compatible) == 0 {
		return 0, nil
	}

	// Sort compatible prices descending by oppSell/oppBuy ratio (best price for us first)
	sort.Slice(compatible, func(i, j int) bool {
		// i > j means i's ratio is larger
		// oppSell_i / oppBuy_i > oppSell_j / oppBuy_j
		// => oppSell_i * oppBuy_j > oppSell_j * oppBuy_i
		lhs := new(big.Int).Mul(compatible[i].ratioNum, compatible[j].ratioDen)
		rhs := new(big.Int).Mul(compatible[j].ratioNum, compatible[i].ratioDen)
		return lhs.Cmp(rhs) > 0
	})

	var totalBuyReceived int64
	// Track which prices were exhausted so we can remove them from the price list
	exhaustedPrices := make(map[[16]byte]bool)

	for _, cp := range compatible {
		if incoming.SellTokenQuantityRemain <= 0 {
			break
		}

		pk := rawdb.PriceKey(cp.price.SellTokenQuantity, cp.price.BuyTokenQuantity)
		ob := rawdb.ReadMarketOrderBook(ctx.DB, incoming.BuyTokenId, incoming.SellTokenId, pk)
		if ob == nil || len(ob.Head) == 0 {
			// Price entry with no orders — mark exhausted
			exhaustedPrices[pk] = true
			continue
		}

		// Walk linked list at this price
		currentID := bytes.Clone(ob.Head)
		for len(currentID) > 0 && incoming.SellTokenQuantityRemain > 0 {
			existing := rawdb.ReadMarketOrder(ctx.DB, currentID)
			if existing == nil {
				break
			}

			nextID := bytes.Clone(existing.Next)

			// Determine fill amounts
			// existing.SellTokenQuantity is their sell (= our buy token)
			// incoming.SellTokenQuantity is our sell (= their buy token)
			// Price ratio: they get BuyTokenQuantity (our sell token) per SellTokenQuantity (our buy token) of theirs
			// i.e., fillSell (we give) = fillBuy * cp.price.BuyTokenQuantity / cp.price.SellTokenQuantity

			var fillBuy, fillSell int64 // fillBuy: how much of existing's sell token we get; fillSell: how much we give

			// Use big.Int for all fill calculations to avoid int64 overflow
			pSell := big.NewInt(cp.price.SellTokenQuantity)
			pBuy := big.NewInt(cp.price.BuyTokenQuantity)
			bRemain := big.NewInt(incoming.SellTokenQuantityRemain)
			bExist := big.NewInt(existing.SellTokenQuantityRemain)

			// Check: existingRemain <= incomingRemain * pSell / pBuy
			threshold := new(big.Int).Mul(bRemain, pSell)
			threshold.Div(threshold, pBuy)

			if bExist.Cmp(threshold) <= 0 {
				// Full fill of existing order
				fillBuy = existing.SellTokenQuantityRemain
				// We need to give: fillBuy * pBuy / pSell
				fillSell = new(big.Int).Mul(big.NewInt(fillBuy), pBuy).Div(
					new(big.Int).Mul(big.NewInt(fillBuy), pBuy), pSell).Int64()
			} else {
				// Partial fill: we give all our remaining sell tokens
				fillSell = incoming.SellTokenQuantityRemain
				// We get: fillSell * pSell / pBuy
				fillBuy = new(big.Int).Mul(big.NewInt(fillSell), pSell).Div(
					new(big.Int).Mul(big.NewInt(fillSell), pSell), pBuy).Int64()
			}

			// Make sure we don't give more than we have
			if fillSell > incoming.SellTokenQuantityRemain {
				fillSell = incoming.SellTokenQuantityRemain
				fillBuy = new(big.Int).Mul(big.NewInt(fillSell), pSell).Div(
					new(big.Int).Mul(big.NewInt(fillSell), pSell), pBuy).Int64()
			}

			// Transfer fillSell (our sell token) to the existing order's owner
			existingOwner := tcommon.BytesToAddress(existing.OwnerAddress)
			if err := transferToken(ctx, existingOwner, incoming.SellTokenId, fillSell); err != nil {
				return 0, err
			}

			// Update existing order's remaining
			existing.SellTokenQuantityRemain -= fillBuy
			incoming.SellTokenQuantityRemain -= fillSell
			totalBuyReceived += fillBuy

			if existing.SellTokenQuantityRemain <= 0 {
				// Fully consumed: mark INACTIVE
				existing.State = corepb.MarketOrder_INACTIVE
				existing.Next = nil
				existing.Prev = nil

				// Remove from linked list: next becomes new head
				if len(nextID) > 0 {
					nextOrder := rawdb.ReadMarketOrder(ctx.DB, nextID)
					if nextOrder != nil {
						nextOrder.Prev = nil
						if err := rawdb.WriteMarketOrder(ctx.DB, nextID, nextOrder); err != nil {
							return 0, err
						}
					}
					ob.Head = nextID
				} else {
					// No more orders at this price
					ob.Head = nil
					ob.Tail = nil
					exhaustedPrices[pk] = true
				}
			}

			// Write updated existing order
			if err := rawdb.WriteMarketOrder(ctx.DB, currentID, existing); err != nil {
				return 0, err
			}

			currentID = nextID
		}

		// Update order book for this price
		if exhaustedPrices[pk] {
			if err := rawdb.DeleteMarketOrderBook(ctx.DB, incoming.BuyTokenId, incoming.SellTokenId, pk); err != nil {
				return 0, err
			}
		} else {
			if err := rawdb.WriteMarketOrderBook(ctx.DB, incoming.BuyTokenId, incoming.SellTokenId, pk, ob); err != nil {
				return 0, err
			}
		}
	}

	// Clean up exhausted prices from the opposite price list
	if len(exhaustedPrices) > 0 {
		var remaining []*corepb.MarketPrice
		for _, p := range oppPL.Prices {
			pk := rawdb.PriceKey(p.SellTokenQuantity, p.BuyTokenQuantity)
			if !exhaustedPrices[pk] {
				remaining = append(remaining, p)
			}
		}
		oppPL.Prices = remaining
		if err := rawdb.WriteMarketPriceList(ctx.DB, incoming.BuyTokenId, incoming.SellTokenId, oppPL); err != nil {
			return 0, err
		}
	}

	return totalBuyReceived, nil
}

// addOrderToBook adds an order to the price list and linked list in the order book.
func addOrderToBook(ctx *Context, order *corepb.MarketOrder) error {
	pk := rawdb.PriceKey(order.SellTokenQuantity, order.BuyTokenQuantity)

	// Update price list: add this price if not present
	pl := rawdb.ReadMarketPriceList(ctx.DB, order.SellTokenId, order.BuyTokenId)
	found := false
	for _, p := range pl.Prices {
		if rawdb.PriceKey(p.SellTokenQuantity, p.BuyTokenQuantity) == pk {
			found = true
			break
		}
	}
	if !found {
		pl.Prices = append(pl.Prices, &corepb.MarketPrice{
			SellTokenQuantity: order.SellTokenQuantity,
			BuyTokenQuantity:  order.BuyTokenQuantity,
		})
		if err := rawdb.WriteMarketPriceList(ctx.DB, order.SellTokenId, order.BuyTokenId, pl); err != nil {
			return err
		}
	}

	// Update linked list at this price key
	ob := rawdb.ReadMarketOrderBook(ctx.DB, order.SellTokenId, order.BuyTokenId, pk)
	if ob == nil {
		// First order at this price
		ob = &corepb.MarketOrderIdList{
			Head: order.OrderId,
			Tail: order.OrderId,
		}
		order.Prev = nil
		order.Next = nil
	} else {
		// Append as new tail
		prevTailID := bytes.Clone(ob.Tail)
		if len(prevTailID) > 0 {
			prevTail := rawdb.ReadMarketOrder(ctx.DB, prevTailID)
			if prevTail != nil {
				prevTail.Next = order.OrderId
				if err := rawdb.WriteMarketOrder(ctx.DB, prevTailID, prevTail); err != nil {
					return err
				}
			}
		}
		order.Prev = prevTailID
		order.Next = nil
		ob.Tail = order.OrderId
	}

	return rawdb.WriteMarketOrderBook(ctx.DB, order.SellTokenId, order.BuyTokenId, pk, ob)
}
