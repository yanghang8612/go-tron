package state

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
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
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.stopped {
		p.dropped.Add(uint64(len(keys)))
		return 0
	}
	var accepted int
	for _, key := range keys {
		select {
		case p.workCh <- normalizePrefetchKey(key):
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

func prefetchLatest(db ethdb.KeyValueReader, key PrefetchKey) (bool, error) {
	if db == nil {
		return false, nil
	}
	switch key.Kind {
	case PrefetchAccountLatest:
		_, ok, err := rawdb.ReadStateAccountLatest(db, key.Owner)
		return ok, err
	case PrefetchAccountKVLatest:
		if !kvdomains.IsRegistered(key.Domain) {
			return false, fmt.Errorf("state prefetch: unregistered domain %#04x", uint16(key.Domain))
		}
		generation, ok, err := prefetchGeneration(db, key)
		if err != nil || !ok {
			return false, err
		}
		_, ok, err = rawdb.ReadStateKVLatest(db, key.Owner, generation, key.Domain, key.Key)
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
		_, ok, err = rawdb.ReadStateKVLatest(db, key.Owner, generation, kvdomains.ContractStorage, rowKey.Bytes())
		return ok, err
	case PrefetchContractCode:
		return prefetchContractCode(db, key.Owner)
	case PrefetchContractOriginAccount:
		return prefetchContractOriginAccount(db, key)
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
	data, ok, err := rawdb.ReadStateKVLatest(db, owner, generation, kvdomains.ContractMetadata, contractMetaKVKey)
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
	data, ok, err := rawdb.ReadStateAccountLatest(db, owner)
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
	_, ok, err = rawdb.ReadStateAccountLatest(db, origin)
	return ok, err
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
