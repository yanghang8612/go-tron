package state

import (
	"encoding/binary"
	"fmt"
	"math/big"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

const (
	defaultStatePrefetchQueue = 1024
	maxStatePrefetchWorkers   = 8
)

// PrefetchKind identifies a deterministic latest-domain read that can be
// warmed before a future transaction reaches Validate/Execute.
type PrefetchKind uint8

const (
	PrefetchAccountLatest PrefetchKind = iota + 1
	PrefetchAccountKVLatest
	PrefetchContractStorage
	PrefetchContractCode
	PrefetchContractOriginAccount
	PrefetchExchangeTokenAssets
	PrefetchMarketMatchOrders
	PrefetchOwnerIssuedAssetRows
)

const (
	marketMatchPriceLevelPrefetchLimit = 20
	marketMatchOrderPrefetchLimit      = 20
)

// PrefetchKey describes one read-only latest-domain warmup. It intentionally
// targets raw latest-domain rows instead of StateDB's object maps: StateDB's
// maps are still single-writer structures, while the underlying KV readers and
// blockbuffer are safe to read concurrently.
type PrefetchKey struct {
	Kind          PrefetchKind
	Owner         tcommon.Address
	Domain        kvdomains.KVDomain
	Key           []byte
	Slot          tcommon.Hash
	Generation    uint64
	HasGeneration bool
}

func AccountPrefetchKey(owner tcommon.Address) PrefetchKey {
	return PrefetchKey{Kind: PrefetchAccountLatest, Owner: owner}
}

func AccountKVPrefetchKey(owner tcommon.Address, domain kvdomains.KVDomain, key []byte) PrefetchKey {
	return PrefetchKey{
		Kind:   PrefetchAccountKVLatest,
		Owner:  owner,
		Domain: domain,
		Key:    append([]byte(nil), key...),
	}
}

func AccountKVGenerationPrefetchKey(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, key []byte) PrefetchKey {
	k := AccountKVPrefetchKey(owner, domain, key)
	k.Generation = generation
	k.HasGeneration = true
	return k
}

func ContractMetadataPrefetchKey(owner tcommon.Address) PrefetchKey {
	return AccountKVPrefetchKey(owner, kvdomains.ContractMetadata, contractMetaKVKey)
}

func ContractCodePrefetchKey(owner tcommon.Address) PrefetchKey {
	return PrefetchKey{Kind: PrefetchContractCode, Owner: owner}
}

func ContractOriginAccountPrefetchKey(contract tcommon.Address) PrefetchKey {
	return PrefetchKey{Kind: PrefetchContractOriginAccount, Owner: contract}
}

func ContractStoragePrefetchKey(owner tcommon.Address, slot tcommon.Hash) PrefetchKey {
	return PrefetchKey{Kind: PrefetchContractStorage, Owner: owner, Slot: slot}
}

func ContractStorageGenerationPrefetchKey(owner tcommon.Address, generation uint64, slot tcommon.Hash) PrefetchKey {
	k := ContractStoragePrefetchKey(owner, slot)
	k.Generation = generation
	k.HasGeneration = true
	return k
}

func WitnessCapsulePrefetchKey(addr tcommon.Address) PrefetchKey {
	return AccountKVPrefetchKey(addr, kvdomains.WitnessCapsule, rawdb.WitnessCapsuleStateKey(addr))
}

func WitnessBrokeragePrefetchKey(addr tcommon.Address) PrefetchKey {
	return AccountKVPrefetchKey(addr, kvdomains.WitnessCapsule, rawdb.WitnessBrokerageStateKey(addr))
}

func WitnessIndexPrefetchKey() PrefetchKey {
	return AccountKVPrefetchKey(tcommon.SystemAccountAddress, kvdomains.SystemWitnessSchedule, witnessScheduleIndexKey)
}

func ProposalPrefetchKey(id int64) PrefetchKey {
	return AccountKVPrefetchKey(tcommon.SystemAccountAddress, kvdomains.SystemProposal, proposalStoreKey(id))
}

func ProposalIndexPrefetchKey() PrefetchKey {
	return AccountKVPrefetchKey(tcommon.SystemAccountAddress, kvdomains.SystemProposal, proposalStoreIndexKey)
}

func PendingVotesPrefetchKey(voter tcommon.Address) PrefetchKey {
	return AccountKVPrefetchKey(tcommon.SystemAccountAddress, kvdomains.WitnessVoteState, votesStoreKey(voter))
}

func PendingVotesIndexPrefetchKey() PrefetchKey {
	return AccountKVPrefetchKey(tcommon.SystemAccountAddress, kvdomains.WitnessVoteState, votesStoreIndexKey)
}

func RewardBeginCyclePrefetchKey(voter tcommon.Address) PrefetchKey {
	return AccountKVPrefetchKey(tcommon.SystemAccountAddress, kvdomains.SystemReward, rawdb.BeginCycleStateKey(voter.Bytes()))
}

func RewardEndCyclePrefetchKey(voter tcommon.Address) PrefetchKey {
	return AccountKVPrefetchKey(tcommon.SystemAccountAddress, kvdomains.SystemReward, rawdb.EndCycleStateKey(voter.Bytes()))
}

func ExchangeTokenAssetsPrefetchKey(exchangeID int64) PrefetchKey {
	if exchangeID <= 0 {
		return PrefetchKey{}
	}
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(exchangeID))
	return PrefetchKey{Kind: PrefetchExchangeTokenAssets, Owner: tcommon.SystemAccountAddress, Key: key}
}

func MarketMatchOrdersPrefetchKey(sellTokenID, buyTokenID []byte, sellQty, buyQty int64) PrefetchKey {
	if len(sellTokenID) == 0 || len(buyTokenID) == 0 || sellQty <= 0 || buyQty <= 0 {
		return PrefetchKey{}
	}
	key := make([]byte, 0, 24+len(sellTokenID)+len(buyTokenID))
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(sellQty))
	key = append(key, buf[:]...)
	binary.BigEndian.PutUint64(buf[:], uint64(buyQty))
	key = append(key, buf[:]...)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(sellTokenID)))
	key = append(key, lenBuf[:]...)
	key = append(key, sellTokenID...)
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(buyTokenID)))
	key = append(key, lenBuf[:]...)
	key = append(key, buyTokenID...)
	return PrefetchKey{Kind: PrefetchMarketMatchOrders, Owner: tcommon.SystemAccountAddress, Key: key}
}

func OwnerIssuedAssetRowsPrefetchKey(owner tcommon.Address) PrefetchKey {
	return PrefetchKey{Kind: PrefetchOwnerIssuedAssetRows, Owner: owner}
}

type StatePrefetcherConfig struct {
	Workers int
	Queue   int
}

type StatePrefetcherStats struct {
	Enqueued  uint64
	Dropped   uint64
	Processed uint64
	Hits      uint64
	Misses    uint64
	Errors    uint64
}

// StatePrefetcher warms deterministic latest-domain reads through the backing
// KV reader. It is a first, race-safe prefetch layer: it warms Pebble/blockbuffer
// caches without mutating StateDB's in-memory account or storage maps.
type StatePrefetcher struct {
	db      ethdb.KeyValueReader
	workers int
	workCh  chan PrefetchKey

	mu        sync.RWMutex
	stopped   bool
	seen      map[string]struct{}
	startOnce sync.Once
	stopOnce  sync.Once
	wg        sync.WaitGroup

	enqueued  atomic.Uint64
	dropped   atomic.Uint64
	processed atomic.Uint64
	hits      atomic.Uint64
	misses    atomic.Uint64
	errors    atomic.Uint64
}

func DefaultStatePrefetchWorkers() int {
	workers := runtime.GOMAXPROCS(0) / 2
	if workers < 1 {
		workers = 1
	}
	if workers > maxStatePrefetchWorkers {
		workers = maxStatePrefetchWorkers
	}
	return workers
}

func NewStatePrefetcher(db ethdb.KeyValueReader, cfg StatePrefetcherConfig) *StatePrefetcher {
	if db == nil {
		return nil
	}
	workers := cfg.Workers
	if workers <= 0 {
		workers = DefaultStatePrefetchWorkers()
	}
	queue := cfg.Queue
	if queue <= 0 {
		queue = defaultStatePrefetchQueue
	}
	return &StatePrefetcher{
		db:      db,
		workers: workers,
		workCh:  make(chan PrefetchKey, queue),
		seen:    make(map[string]struct{}),
	}
}

func (p *StatePrefetcher) Start() {
	if p == nil {
		return
	}
	p.startOnce.Do(func() {
		for i := 0; i < p.workers; i++ {
			p.wg.Add(1)
			go p.worker()
		}
	})
}

func (p *StatePrefetcher) Stop() {
	if p == nil {
		return
	}
	p.stopOnce.Do(func() {
		p.mu.Lock()
		p.stopped = true
		close(p.workCh)
		p.mu.Unlock()
	})
	p.wg.Wait()
}

// Enqueue adds as many keys as fit without blocking. It returns the number of
// accepted keys so callers can account for intentionally dropped warmups.
func (p *StatePrefetcher) Enqueue(keys []PrefetchKey) int {
	if p == nil || len(keys) == 0 {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		p.dropped.Add(uint64(len(keys)))
		return 0
	}
	var accepted int
	for _, key := range keys {
		key = normalizePrefetchKey(key)
		id := prefetchKeyFingerprint(key)
		if _, ok := p.seen[id]; ok {
			continue
		}
		select {
		case p.workCh <- key:
			p.seen[id] = struct{}{}
			accepted++
			p.enqueued.Add(1)
		default:
			p.dropped.Add(1)
		}
	}
	return accepted
}

func (p *StatePrefetcher) Stats() StatePrefetcherStats {
	if p == nil {
		return StatePrefetcherStats{}
	}
	return StatePrefetcherStats{
		Enqueued:  p.enqueued.Load(),
		Dropped:   p.dropped.Load(),
		Processed: p.processed.Load(),
		Hits:      p.hits.Load(),
		Misses:    p.misses.Load(),
		Errors:    p.errors.Load(),
	}
}

func (p *StatePrefetcher) worker() {
	defer p.wg.Done()
	for key := range p.workCh {
		hit, err := prefetchLatest(p.db, key)
		p.processed.Add(1)
		if err != nil {
			p.errors.Add(1)
			continue
		}
		if hit {
			p.hits.Add(1)
		} else {
			p.misses.Add(1)
		}
	}
}

func normalizePrefetchKey(key PrefetchKey) PrefetchKey {
	if len(key.Key) > 0 {
		key.Key = append([]byte(nil), key.Key...)
	}
	return key
}

func prefetchKeyFingerprint(key PrefetchKey) string {
	return fmt.Sprintf("%d:%x:%04x:%x:%x:%d:%t",
		key.Kind,
		key.Owner[:],
		uint16(key.Domain),
		key.Key,
		key.Slot[:],
		key.Generation,
		key.HasGeneration,
	)
}

func prefetchLatest(db ethdb.KeyValueReader, key PrefetchKey) (bool, error) {
	if db == nil {
		return false, nil
	}
	switch key.Kind {
	case PrefetchAccountLatest:
		_, ok, err := rawdb.ReadStateAccountLatestNoCopy(db, key.Owner)
		return ok, err
	case PrefetchAccountKVLatest:
		if !kvdomains.IsRegistered(key.Domain) {
			return false, fmt.Errorf("state prefetch: unregistered domain %#04x", uint16(key.Domain))
		}
		generation, ok, err := prefetchGeneration(db, key)
		if err != nil || !ok {
			return false, err
		}
		_, ok, err = rawdb.ReadStateKVLatestNoCopy(db, key.Owner, generation, key.Domain, key.Key)
		return ok, err
	case PrefetchContractStorage:
		generation, ok, err := prefetchGeneration(db, key)
		if err != nil || !ok {
			return false, err
		}
		meta, err := prefetchContractMetadata(db, key.Owner, generation)
		if err != nil {
			return false, err
		}
		rowKey := javaStorageRowKey(key.Owner, key.Slot, meta)
		_, ok, err = rawdb.ReadStateKVLatestNoCopy(db, key.Owner, generation, kvdomains.ContractStorage, rowKey.Bytes())
		return ok, err
	case PrefetchContractCode:
		return prefetchContractCode(db, key.Owner)
	case PrefetchContractOriginAccount:
		return prefetchContractOriginAccount(db, key)
	case PrefetchExchangeTokenAssets:
		return prefetchExchangeTokenAssets(db, key)
	case PrefetchMarketMatchOrders:
		return prefetchMarketMatchOrders(db, key)
	case PrefetchOwnerIssuedAssetRows:
		return prefetchOwnerIssuedAssetRows(db, key.Owner)
	default:
		return false, fmt.Errorf("state prefetch: unknown kind %d", key.Kind)
	}
}

func prefetchGeneration(db ethdb.KeyValueReader, key PrefetchKey) (uint64, bool, error) {
	if key.HasGeneration {
		return key.Generation, true, nil
	}
	generation, ok, err := rawdb.ReadStateKVGeneration(db, key.Owner)
	if err != nil || ok {
		return generation, ok, err
	}
	return 0, true, nil
}

func prefetchContractMetadata(db ethdb.KeyValueReader, owner tcommon.Address, generation uint64) (*contractpb.SmartContract, error) {
	data, ok, err := rawdb.ReadStateKVLatestNoCopy(db, owner, generation, kvdomains.ContractMetadata, contractMetaKVKey)
	if err != nil || !ok || len(data) == 0 {
		return nil, err
	}
	var meta contractpb.SmartContract
	if err := proto.Unmarshal(data, &meta); err != nil {
		return nil, nil
	}
	return &meta, nil
}

func prefetchContractCode(db ethdb.KeyValueReader, owner tcommon.Address) (bool, error) {
	data, ok, err := rawdb.ReadStateAccountLatestNoCopy(db, owner)
	if err != nil || !ok || len(data) == 0 {
		return false, err
	}
	account, err := DecodeStateAccountV2(data)
	if err != nil || account.CodeHash == (tcommon.Hash{}) {
		return false, nil
	}
	_, ok, err = rawdb.ReadStateCodeStrict(db, account.CodeHash)
	return ok, err
}

func prefetchContractOriginAccount(db ethdb.KeyValueReader, key PrefetchKey) (bool, error) {
	generation, ok, err := prefetchGeneration(db, key)
	if err != nil || !ok {
		return false, err
	}
	meta, err := prefetchContractMetadata(db, key.Owner, generation)
	if err != nil || meta == nil {
		return false, err
	}
	origin, ok := prefetchTRONAddress(meta.GetOriginAddress())
	if !ok {
		return false, nil
	}
	_, ok, err = rawdb.ReadStateAccountLatestNoCopy(db, origin)
	return ok, err
}

func prefetchExchangeTokenAssets(db ethdb.KeyValueReader, key PrefetchKey) (bool, error) {
	if len(key.Key) != 8 {
		return false, nil
	}
	exchangeID := int64(binary.BigEndian.Uint64(key.Key))
	if exchangeID <= 0 {
		return false, nil
	}
	generation, ok, err := prefetchGeneration(db, PrefetchKey{Owner: tcommon.SystemAccountAddress})
	if err != nil || !ok {
		return false, err
	}
	seenAssets := make(map[string]struct{})
	hit := false
	for _, rowKey := range [][]byte{
		exchangeKVKey(exchangeKVDiscriminatorV2, exchangeID),
		exchangeKVKey(exchangeKVDiscriminatorV1, exchangeID),
	} {
		data, ok, err := rawdb.ReadStateKVLatestNoCopy(db, tcommon.SystemAccountAddress, generation, kvdomains.SystemExchange, rowKey)
		if err != nil {
			return hit, err
		}
		if !ok || len(data) == 0 {
			continue
		}
		hit = true
		var ex corepb.Exchange
		if err := proto.Unmarshal(data, &ex); err != nil {
			continue
		}
		if err := prefetchTRC10AssetRows(db, generation, ex.GetFirstTokenId(), seenAssets); err != nil {
			return hit, err
		}
		if err := prefetchTRC10AssetRows(db, generation, ex.GetSecondTokenId(), seenAssets); err != nil {
			return hit, err
		}
	}
	return hit, nil
}

func prefetchTRC10AssetRows(db ethdb.KeyValueReader, generation uint64, token []byte, seen map[string]struct{}) error {
	if len(token) == 0 || (len(token) == 1 && token[0] == '_') {
		return nil
	}
	rows := [][]byte{
		assetBytesKey(assetLegacyTag, token),
		assetBytesKey(assetNameIndexTag, token),
	}
	if tokenID, err := strconv.ParseInt(string(token), 10, 64); err == nil {
		rows = append(rows, assetIDKey(assetV2Tag, tokenID))
	}
	for _, row := range rows {
		if seen != nil {
			id := string(row)
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
		}
		if _, _, err := rawdb.ReadStateKVLatestNoCopy(db, tcommon.SystemAccountAddress, generation, kvdomains.SystemAsset, row); err != nil {
			return err
		}
	}
	return nil
}

func prefetchOwnerIssuedAssetRows(db ethdb.KeyValueReader, owner tcommon.Address) (bool, error) {
	data, ok, err := rawdb.ReadStateAccountLatestNoCopy(db, owner)
	if err != nil || !ok || len(data) == 0 {
		return false, err
	}
	envelope, err := DecodeStateAccountV2(data)
	if err != nil || len(envelope.AccountProto) == 0 {
		return false, nil
	}
	var account corepb.Account
	if err := proto.Unmarshal(envelope.AccountProto, &account); err != nil {
		return false, nil
	}
	generation, ok, err := prefetchGeneration(db, PrefetchKey{Owner: tcommon.SystemAccountAddress})
	if err != nil || !ok {
		return true, err
	}
	seen := make(map[string]struct{})
	if err := prefetchTRC10AssetRows(db, generation, account.GetAssetIssued_ID(), seen); err != nil {
		return true, err
	}
	if err := prefetchTRC10AssetRows(db, generation, account.GetAssetIssuedName(), seen); err != nil {
		return true, err
	}
	return true, nil
}

func prefetchMarketMatchOrders(db ethdb.KeyValueReader, key PrefetchKey) (bool, error) {
	sellToken, buyToken, sellQty, buyQty, ok := decodeMarketMatchOrdersPrefetchKey(key.Key)
	if !ok {
		return false, nil
	}
	generation, ok, err := prefetchGeneration(db, PrefetchKey{Owner: tcommon.SystemAccountAddress})
	if err != nil || !ok {
		return false, err
	}

	priceListData, ok, err := rawdb.ReadStateKVLatestNoCopy(db, tcommon.SystemAccountAddress, generation, kvdomains.SystemMarket, marketPriceListKVKey(buyToken, sellToken))
	if err != nil || !ok || len(priceListData) == 0 {
		return false, err
	}
	hit := true
	var priceList corepb.MarketPriceList
	if err := proto.Unmarshal(priceListData, &priceList); err != nil {
		return hit, nil
	}

	compatible := marketCompatiblePrices(priceList.GetPrices(), sellQty, buyQty)
	if len(compatible) == 0 {
		return hit, nil
	}
	seenOrders := make(map[string]struct{})
	prefetchedOrders := 0
	for priceIndex, price := range compatible {
		if prefetchedOrders >= marketMatchOrderPrefetchLimit {
			break
		}
		if priceIndex >= marketMatchPriceLevelPrefetchLimit {
			break
		}
		pk := rawdb.PriceKey(price.GetSellTokenQuantity(), price.GetBuyTokenQuantity())
		orderBookData, ok, err := rawdb.ReadStateKVLatestNoCopy(db, tcommon.SystemAccountAddress, generation, kvdomains.SystemMarket, marketOrderBookKVKey(buyToken, sellToken, pk))
		if err != nil {
			return hit, err
		}
		if !ok || len(orderBookData) == 0 {
			continue
		}
		var orderBook corepb.MarketOrderIdList
		if err := proto.Unmarshal(orderBookData, &orderBook); err != nil {
			continue
		}
		orderID := append([]byte(nil), orderBook.GetHead()...)
		for len(orderID) > 0 && prefetchedOrders < marketMatchOrderPrefetchLimit {
			orderKey := string(orderID)
			if _, ok := seenOrders[orderKey]; ok {
				break
			}
			seenOrders[orderKey] = struct{}{}
			orderData, ok, err := rawdb.ReadStateKVLatestNoCopy(db, tcommon.SystemAccountAddress, generation, kvdomains.SystemMarket, marketOrderKVKey(orderID))
			if err != nil {
				return hit, err
			}
			if !ok || len(orderData) == 0 {
				break
			}
			prefetchedOrders++
			var order corepb.MarketOrder
			if err := proto.Unmarshal(orderData, &order); err != nil {
				break
			}
			orderID = append([]byte(nil), order.GetNext()...)
		}
	}
	return hit, nil
}

func decodeMarketMatchOrdersPrefetchKey(key []byte) (sellTokenID, buyTokenID []byte, sellQty, buyQty int64, ok bool) {
	if len(key) < 24 {
		return nil, nil, 0, 0, false
	}
	sellQty = int64(binary.BigEndian.Uint64(key[:8]))
	buyQty = int64(binary.BigEndian.Uint64(key[8:16]))
	if sellQty <= 0 || buyQty <= 0 {
		return nil, nil, 0, 0, false
	}
	pos := 16
	sellLen := int(binary.BigEndian.Uint32(key[pos : pos+4]))
	pos += 4
	if sellLen == 0 || len(key) < pos+sellLen+4 {
		return nil, nil, 0, 0, false
	}
	sellTokenID = key[pos : pos+sellLen]
	pos += sellLen
	buyLen := int(binary.BigEndian.Uint32(key[pos : pos+4]))
	pos += 4
	if buyLen == 0 || len(key) != pos+buyLen {
		return nil, nil, 0, 0, false
	}
	buyTokenID = key[pos : pos+buyLen]
	return sellTokenID, buyTokenID, sellQty, buyQty, true
}

func marketCompatiblePrices(prices []*corepb.MarketPrice, sellQty, buyQty int64) []*corepb.MarketPrice {
	if sellQty <= 0 || buyQty <= 0 || len(prices) == 0 {
		return nil
	}
	inSell := big.NewInt(sellQty)
	inBuy := big.NewInt(buyQty)
	type compatiblePrice struct {
		price    *corepb.MarketPrice
		ratioNum *big.Int
		ratioDen *big.Int
	}
	compatible := make([]compatiblePrice, 0, len(prices))
	for _, price := range prices {
		if price == nil || price.GetSellTokenQuantity() <= 0 || price.GetBuyTokenQuantity() <= 0 {
			continue
		}
		oppSell := big.NewInt(price.GetSellTokenQuantity())
		oppBuy := big.NewInt(price.GetBuyTokenQuantity())
		lhs := new(big.Int).Mul(oppSell, inSell)
		rhs := new(big.Int).Mul(inBuy, oppBuy)
		if lhs.Cmp(rhs) >= 0 {
			compatible = append(compatible, compatiblePrice{
				price:    price,
				ratioNum: oppSell,
				ratioDen: oppBuy,
			})
		}
	}
	sort.Slice(compatible, func(i, j int) bool {
		lhs := new(big.Int).Mul(compatible[i].ratioNum, compatible[j].ratioDen)
		rhs := new(big.Int).Mul(compatible[j].ratioNum, compatible[i].ratioDen)
		return lhs.Cmp(rhs) > 0
	})
	out := make([]*corepb.MarketPrice, 0, len(compatible))
	for _, price := range compatible {
		out = append(out, price.price)
	}
	return out
}

func prefetchTRONAddress(raw []byte) (tcommon.Address, bool) {
	if len(raw) != tcommon.AddressLength {
		return tcommon.Address{}, false
	}
	addr := tcommon.BytesToAddress(raw)
	if !addr.ValidPrefix() {
		return tcommon.Address{}, false
	}
	return addr, true
}
