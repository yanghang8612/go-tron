package actuator

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// MarketSellAssetActuator handles market sell asset transactions (contract type 52).
type MarketSellAssetActuator struct{}

const (
	maxMarketActiveOrderNum = 100
	maxMarketMatchNum       = 20
)

func (a *MarketSellAssetActuator) getContract(ctx *Context) (*contractpb.MarketSellAssetContract, error) {
	return decodedContract[*contractpb.MarketSellAssetContract](ctx, "MarketSellAssetContract")
}

// Validate checks all preconditions for a MarketSellAsset transaction.
func (a *MarketSellAssetActuator) Validate(ctx *Context) error {
	if !forks.IsActive(forks.AllowMarketTransaction, ctx.BlockNumber, ctx.DynProps) {
		return errors.New("market transactions not yet enabled")
	}
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
	if !validMarketTokenID(c.SellTokenId) {
		return errors.New("sellTokenId is not a valid number")
	}
	if !validMarketTokenID(c.BuyTokenId) {
		return errors.New("buyTokenId is not a valid number")
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
	quantityLimit := ctx.DynProps.MarketQuantityLimit()
	if c.SellTokenQuantity > quantityLimit || c.BuyTokenQuantity > quantityLimit {
		return fmt.Errorf("token quantity must less than %d", quantityLimit)
	}
	if mao := ctx.State.ReadMarketAccountOrder(c.OwnerAddress); mao != nil && mao.Count >= maxMarketActiveOrderNum {
		return errors.New("maximum number of active market orders exceeded")
	}

	fee := ctx.DynProps.MarketSellFee()
	if bytes.Equal(c.SellTokenId, []byte("_")) {
		required, ok := checkedAddInt64(c.SellTokenQuantity, fee)
		if !ok || ctx.State.GetBalance(ownerAddr) < required {
			return errors.New("insufficient balance for market sell fee and sell quantity")
		}
	} else {
		if ctx.State.GetBalance(ownerAddr) < fee {
			return errors.New("insufficient balance for market sell fee")
		}
		if err := checkMarketTokenExists(ctx, c.SellTokenId, "No sellTokenId !"); err != nil {
			return err
		}
		if err := checkTokenBalance(ctx, ownerAddr, c.SellTokenId, c.SellTokenQuantity); err != nil {
			return err
		}
	}
	if !bytes.Equal(c.BuyTokenId, []byte("_")) {
		if err := checkMarketTokenExists(ctx, c.BuyTokenId, "No buyTokenId !"); err != nil {
			return err
		}
	}
	return nil
}

// Execute performs the market sell asset operation.
func (a *MarketSellAssetActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}

	ownerAddr, err := checkedAddress(c.OwnerAddress, "ownerAddress")
	if err != nil {
		return nil, err
	}
	fee := ctx.DynProps.MarketSellFee()
	if err := burnFee(ctx, ownerAddr, fee); err != nil {
		return nil, err
	}

	// Step 1: Deduct sell tokens from owner (put in escrow)
	if err := deductToken(ctx, ownerAddr, c.SellTokenId, c.SellTokenQuantity); err != nil {
		return nil, err
	}

	// Step 2: Generate order ID
	mao := ctx.State.ReadMarketAccountOrder(c.OwnerAddress)
	orderID := generateOrderID(c.OwnerAddress, c.SellTokenId, c.BuyTokenId, mao.TotalCount)

	// Step 3: Create MarketOrder proto
	order := &corepb.MarketOrder{
		OrderId:                 orderID,
		OwnerAddress:            c.OwnerAddress,
		CreateTime:              ctx.DynProps.LatestBlockHeaderTimestamp(),
		SellTokenId:             c.SellTokenId,
		SellTokenQuantity:       c.SellTokenQuantity,
		BuyTokenId:              c.BuyTokenId,
		BuyTokenQuantity:        c.BuyTokenQuantity,
		SellTokenQuantityRemain: c.SellTokenQuantity,
		SellTokenQuantityReturn: 0,
		State:                   corepb.MarketOrder_ACTIVE,
	}

	// Step 4: Run matching engine
	totalBuyReceived, orderDetails, err := matchOrder(ctx, order)
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
	if err := ctx.State.WriteMarketOrder(orderID, order); err != nil {
		return nil, err
	}

	// Step 8: Update MarketAccountOrder
	mao.TotalCount++
	if order.State == corepb.MarketOrder_ACTIVE {
		mao.Orders = append(mao.Orders, orderID)
		mao.Count++
	}
	if err := ctx.State.WriteMarketAccountOrder(c.OwnerAddress, mao); err != nil {
		return nil, err
	}

	return &Result{Fee: fee, OrderID: orderID, OrderDetails: orderDetails, ContractRet: 1}, nil
}

// generateOrderID mirrors java-tron's MarketUtils.calculateOrderId:
// owner || sellTokenId padded to 19 bytes || buyTokenId padded to 19 bytes || totalCount.
func generateOrderID(owner, sellTokenID, buyTokenID []byte, totalCount int64) []byte {
	const tokenIDLength = 19
	var count [8]byte
	binary.BigEndian.PutUint64(count[:], uint64(totalCount))
	input := make([]byte, len(owner)+tokenIDLength+tokenIDLength+len(count))
	copy(input, owner)
	copy(input[len(owner):], sellTokenID)
	copy(input[len(owner)+tokenIDLength:], buyTokenID)
	copy(input[len(owner)+tokenIDLength+tokenIDLength:], count[:])
	h := tcommon.Keccak256(input)
	return h[:]
}

func validMarketTokenID(tokenID []byte) bool {
	if bytes.Equal(tokenID, []byte("_")) {
		return true
	}
	if len(tokenID) == 0 {
		return false
	}
	for _, c := range tokenID {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// checkTokenBalance verifies the owner has at least `amount` of the given tokenID.
func checkTokenBalance(ctx *Context, ownerAddr tcommon.Address, tokenID []byte, amount int64) error {
	if bytes.Equal(tokenID, []byte("_")) {
		if ctx.State.GetBalance(ownerAddr) < amount {
			return errors.New("insufficient TRX balance")
		}
		return nil
	}
	tid, err := resolveAssetNameOrID(ctx, tokenID)
	if err != nil {
		return errors.New("invalid TRC10 token ID")
	}
	if ctx.State.GetTRC10BalanceFinal(ownerAddr, tokenID, tid, ctx.DynProps.AllowSameTokenName()) < amount {
		return errors.New("insufficient TRC10 balance")
	}
	return nil
}

func checkMarketTokenExists(ctx *Context, tokenID []byte, msg string) error {
	if _, err := resolveAsset(ctx, tokenID); err != nil {
		return errors.New(msg)
	}
	return nil
}

// transferToken credits amount of tokenID to addr.
func transferToken(ctx *Context, addr tcommon.Address, tokenID []byte, amount int64) error {
	if bytes.Equal(tokenID, []byte("_")) {
		if ctx.State.GetBalance(addr) > math.MaxInt64-amount {
			return errors.New("TRX balance overflows int64")
		}
		ctx.State.AddBalance(addr, amount)
		return nil
	}
	tid, err := resolveAssetNameOrID(ctx, tokenID)
	if err != nil {
		return errors.New("invalid TRC10 token ID")
	}
	if ctx.State.GetTRC10BalanceFinal(addr, tokenID, tid, ctx.DynProps.AllowSameTokenName()) > math.MaxInt64-amount {
		return errors.New("TRC10 balance overflows int64")
	}
	ctx.State.AddTRC10BalanceFinal(addr, tokenID, tid, amount, ctx.DynProps.AllowSameTokenName())
	return nil
}

// deductToken deducts amount of tokenID from addr.
func deductToken(ctx *Context, addr tcommon.Address, tokenID []byte, amount int64) error {
	if bytes.Equal(tokenID, []byte("_")) {
		return ctx.State.SubBalance(addr, amount)
	}
	tid, err := resolveAssetNameOrID(ctx, tokenID)
	if err != nil {
		return errors.New("invalid TRC10 token ID")
	}
	return ctx.State.SubTRC10BalanceFinal(addr, tokenID, tid, amount, ctx.DynProps.AllowSameTokenName())
}

// matchOrder runs the matching engine for the incoming order.
// It returns the total amount of buy tokens received by the incoming order owner
// and the java-tron-compatible MarketOrderDetail receipt entries.
func matchOrder(ctx *Context, incoming *corepb.MarketOrder) (int64, []*corepb.MarketOrderDetail, error) {
	// Get opposite price list: what's selling what we want to buy, for what we're selling
	// Opposite: sellTokenId = incoming.BuyTokenId, buyTokenId = incoming.SellTokenId
	oppPL := ctx.State.ReadMarketPriceList(incoming.BuyTokenId, incoming.SellTokenId)
	if oppPL == nil || len(oppPL.Prices) == 0 {
		return 0, nil, nil
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
		return 0, nil, nil
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
	var details []*corepb.MarketOrderDetail
	// Track which prices were exhausted so we can remove them from the price list
	exhaustedPrices := make(map[[16]byte]bool)
	matchOrderCount := 0

	for _, cp := range compatible {
		if incoming.SellTokenQuantityRemain <= 0 {
			break
		}

		pk := rawdb.PriceKey(cp.price.SellTokenQuantity, cp.price.BuyTokenQuantity)
		ob := ctx.State.ReadMarketOrderBook(incoming.BuyTokenId, incoming.SellTokenId, pk)
		if ob == nil || len(ob.Head) == 0 {
			// Price entry with no orders — mark exhausted
			exhaustedPrices[pk] = true
			continue
		}

		// Walk linked list at this price
		currentID := bytes.Clone(ob.Head)
		for len(currentID) > 0 && incoming.SellTokenQuantityRemain > 0 {
			existing := ctx.State.ReadMarketOrder(currentID)
			if existing == nil {
				break
			}

			nextID := bytes.Clone(existing.Next)
			matchOrderCount++
			if matchOrderCount > maxMarketMatchNum {
				return 0, nil, fmt.Errorf("Too many matches. MAX_MATCH_NUM = %d", maxMarketMatchNum)
			}

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

			// Java first calculates how much the taker can buy at the maker price.
			takerBuyRemain := new(big.Int).Mul(bRemain, pSell)
			takerBuyRemain.Div(takerBuyRemain, pBuy)
			if takerBuyRemain.Sign() == 0 {
				if err := returnOrderSellRemain(ctx, incoming); err != nil {
					return 0, nil, err
				}
				incoming.State = corepb.MarketOrder_INACTIVE
				break
			}

			switch takerBuyRemain.Cmp(bExist) {
			case 0:
				// taker == maker
				fillBuy = existing.SellTokenQuantityRemain
				fillSell = new(big.Int).Mul(big.NewInt(fillBuy), pBuy).Div(
					new(big.Int).Mul(big.NewInt(fillBuy), pBuy), pSell).Int64()
			case -1:
				// taker < maker: taker is fully consumed.
				fillSell = incoming.SellTokenQuantityRemain
				fillBuy = takerBuyRemain.Int64()
			default:
				// taker > maker: maker is fully consumed.
				fillBuy = existing.SellTokenQuantityRemain
				fillSell = new(big.Int).Mul(big.NewInt(fillBuy), pBuy).Div(
					new(big.Int).Mul(big.NewInt(fillBuy), pBuy), pSell).Int64()
				if fillSell == 0 {
					if err := returnOrderSellRemain(ctx, existing); err != nil {
						return 0, nil, err
					}
					existing.State = corepb.MarketOrder_INACTIVE
					if err := deactivateMarketOrderHead(ctx, existing, ob, nextID, exhaustedPrices, pk); err != nil {
						return 0, nil, err
					}
					if err := ctx.State.WriteMarketOrder(currentID, existing); err != nil {
						return 0, nil, err
					}
					currentID = nextID
					continue
				}
			}

			// Transfer fillSell (our sell token) to the existing order's owner
			existingOwner := tcommon.BytesToAddress(existing.OwnerAddress)
			if err := transferToken(ctx, existingOwner, incoming.SellTokenId, fillSell); err != nil {
				return 0, nil, err
			}

			// Update existing order's remaining
			existing.SellTokenQuantityRemain -= fillBuy
			incoming.SellTokenQuantityRemain -= fillSell
			totalBuyReceived += fillBuy
			details = append(details, &corepb.MarketOrderDetail{
				MakerOrderId:     existing.OrderId,
				TakerOrderId:     incoming.OrderId,
				FillSellQuantity: fillSell,
				FillBuyQuantity:  fillBuy,
			})

			if existing.SellTokenQuantityRemain <= 0 {
				if err := deactivateMarketOrderHead(ctx, existing, ob, nextID, exhaustedPrices, pk); err != nil {
					return 0, nil, err
				}
			}

			// Write updated existing order
			if err := ctx.State.WriteMarketOrder(currentID, existing); err != nil {
				return 0, nil, err
			}

			currentID = nextID
		}

		// Update order book for this price
		if exhaustedPrices[pk] {
			if err := ctx.State.DeleteMarketOrderBook(incoming.BuyTokenId, incoming.SellTokenId, pk); err != nil {
				return 0, nil, err
			}
			if err := decrementMarketPairPriceCount(ctx, incoming.BuyTokenId, incoming.SellTokenId); err != nil {
				return 0, nil, err
			}
		} else {
			if err := ctx.State.WriteMarketOrderBook(incoming.BuyTokenId, incoming.SellTokenId, pk, ob); err != nil {
				return 0, nil, err
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
		if err := ctx.State.WriteMarketPriceList(incoming.BuyTokenId, incoming.SellTokenId, oppPL); err != nil {
			return 0, nil, err
		}
	}

	return totalBuyReceived, details, nil
}

func returnOrderSellRemain(ctx *Context, order *corepb.MarketOrder) error {
	remain := order.SellTokenQuantityRemain
	if remain <= 0 {
		order.SellTokenQuantityRemain = 0
		return nil
	}
	owner := tcommon.BytesToAddress(order.OwnerAddress)
	if err := transferToken(ctx, owner, order.SellTokenId, remain); err != nil {
		return err
	}
	order.SellTokenQuantityReturn = remain
	order.SellTokenQuantityRemain = 0
	return nil
}

func deactivateMarketOrderHead(ctx *Context, order *corepb.MarketOrder, ob *corepb.MarketOrderIdList, nextID []byte, exhaustedPrices map[[16]byte]bool, pk [16]byte) error {
	order.State = corepb.MarketOrder_INACTIVE
	order.Next = nil
	order.Prev = nil
	if err := removeMarketAccountOrder(ctx, order.OwnerAddress, order.OrderId); err != nil {
		return err
	}

	if len(nextID) > 0 {
		nextOrder := ctx.State.ReadMarketOrder(nextID)
		if nextOrder != nil {
			nextOrder.Prev = nil
			if err := ctx.State.WriteMarketOrder(nextID, nextOrder); err != nil {
				return err
			}
		}
		ob.Head = nextID
	} else {
		ob.Head = nil
		ob.Tail = nil
		exhaustedPrices[pk] = true
	}
	return nil
}

func removeMarketAccountOrder(ctx *Context, owner []byte, orderID []byte) error {
	mao := ctx.State.ReadMarketAccountOrder(owner)
	for i, id := range mao.Orders {
		if bytes.Equal(id, orderID) {
			mao.Orders = append(mao.Orders[:i], mao.Orders[i+1:]...)
			break
		}
	}
	if mao.Count > 0 {
		mao.Count--
	}
	return ctx.State.WriteMarketAccountOrder(owner, mao)
}

func decrementMarketPairPriceCount(ctx *Context, sellTokenID, buyTokenID []byte) error {
	count := ctx.State.ReadMarketPairPriceCount(sellTokenID, buyTokenID)
	if count <= 1 {
		return ctx.State.DeleteMarketPairPriceCount(sellTokenID, buyTokenID)
	}
	return ctx.State.WriteMarketPairPriceCount(sellTokenID, buyTokenID, count-1)
}

// addOrderToBook adds an order to the price list and linked list in the order book.
func addOrderToBook(ctx *Context, order *corepb.MarketOrder) error {
	pk := rawdb.PriceKey(order.SellTokenQuantity, order.BuyTokenQuantity)

	// Update price list: add this price if not present
	pl := ctx.State.ReadMarketPriceList(order.SellTokenId, order.BuyTokenId)
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
		if err := ctx.State.WriteMarketPriceList(order.SellTokenId, order.BuyTokenId, pl); err != nil {
			return err
		}
		if err := ctx.State.IncrMarketPairPriceCount(order.SellTokenId, order.BuyTokenId, 1); err != nil {
			return err
		}
	}

	// Update linked list at this price key
	ob := ctx.State.ReadMarketOrderBook(order.SellTokenId, order.BuyTokenId, pk)
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
			prevTail := ctx.State.ReadMarketOrder(prevTailID)
			if prevTail != nil {
				prevTail.Next = order.OrderId
				if err := ctx.State.WriteMarketOrder(prevTailID, prevTail); err != nil {
					return err
				}
			}
		}
		order.Prev = prevTailID
		order.Next = nil
		ob.Tail = order.OrderId
	}

	return ctx.State.WriteMarketOrderBook(order.SellTokenId, order.BuyTokenId, pk, ob)
}
