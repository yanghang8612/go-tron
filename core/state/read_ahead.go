package state

import (
	"errors"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/metrics"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/types"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

const (
	defaultStateReadAheadQueueBlocks = 64
	defaultStateReadAheadQueueBytes  = 16 << 20
	maxTransferAssetPrefetchRows     = 256
	maxStateReadAheadWorkers         = 4
	maxStateReadAheadPlanRows        = 4096
)

var (
	stateReadAheadEnqueuedBlocksCounter = metrics.NewRegisteredCounter("core/state_read_ahead/enqueued/blocks", nil)
	stateReadAheadEnqueuedBytesCounter  = metrics.NewRegisteredCounter("core/state_read_ahead/enqueued/bytes", nil)
	stateReadAheadQueuedBytesGauge      = metrics.NewRegisteredGauge("core/state_read_ahead/queue/bytes", nil)
	stateReadAheadDroppedBlocksCounter  = metrics.NewRegisteredCounter("core/state_read_ahead/dropped/blocks", nil)
	stateReadAheadDroppedBytesCounter   = metrics.NewRegisteredCounter("core/state_read_ahead/dropped/bytes", nil)
	stateReadAheadProcessedCounter      = metrics.NewRegisteredCounter("core/state_read_ahead/processed/blocks", nil)
	stateReadAheadStaleCounter          = metrics.NewRegisteredCounter("core/state_read_ahead/stale/blocks", nil)
	stateReadAheadRowsCounter           = metrics.NewRegisteredCounter("core/state_read_ahead/rows", nil)
	stateReadAheadPresentCounter        = metrics.NewRegisteredCounter("core/state_read_ahead/present", nil)
	stateReadAheadMissingCounter        = metrics.NewRegisteredCounter("core/state_read_ahead/missing", nil)
	stateReadAheadErrorsCounter         = metrics.NewRegisteredCounter("core/state_read_ahead/errors", nil)
	stateReadAheadBeforeApplyCounter    = metrics.NewRegisteredCounter("core/state_read_ahead/completed_before_apply/blocks", nil)
	stateReadAheadAfterApplyCounter     = metrics.NewRegisteredCounter("core/state_read_ahead/completed_after_apply/blocks", nil)
	stateReadAheadAssetCapBlocksCounter = metrics.NewRegisteredCounter("core/state_read_ahead/asset/cap/blocks", nil)
	stateReadAheadAssetCappedCounter    = metrics.NewRegisteredCounter("core/state_read_ahead/asset/cap/rejected_attempts", nil)
	stateReadAheadAssetLegacyCounter    = metrics.NewRegisteredCounter("core/state_read_ahead/asset/legacy/rows", nil)
	stateReadAheadAssetV2Counter        = metrics.NewRegisteredCounter("core/state_read_ahead/asset/v2/rows", nil)
	stateReadAheadAssetBalanceCounter   = metrics.NewRegisteredCounter("core/state_read_ahead/asset/balance/rows", nil)
	stateReadAheadAssetPlanFullCounter  = metrics.NewRegisteredCounter("core/state_read_ahead/asset/plan_full/rows", nil)
)

// StateReadAheadConfig bounds the non-consensus bulk-sync warmup pipeline.
type StateReadAheadConfig struct {
	Workers     int
	QueueBlocks int
	QueueBytes  int64
}

// StateReadAheadStats is a point-in-time view of one session's warmup work.
// Asset row counts describe planned reads, not cache admission or later hits.
// AssetCappedAttempts counts rejected hint attempts, including repeats of a
// key rejected earlier in the same block.
type StateReadAheadStats struct {
	EnqueuedBlocks       uint64
	EnqueuedBytes        uint64
	QueuedBytes          int64
	DroppedBlocks        uint64
	DroppedBytes         uint64
	ProcessedBlocks      uint64
	StaleBlocks          uint64
	Rows                 uint64
	Present              uint64
	Missing              uint64
	Errors               uint64
	CompletedBeforeApply uint64
	CompletedAfterApply  uint64
	AssetCapBlocks       uint64
	AssetCappedAttempts  uint64
	AssetLegacyRows      uint64
	AssetV2Rows          uint64
	AssetBalanceRows     uint64
	AssetPlanFullRows    uint64
}

type stateReadAheadJob struct {
	block *types.Block
	bytes int64
	epoch uint64
}

// StateReadAhead overlaps deterministic latest-state reads from decoded future
// blocks with ordered canonical execution. It never validates or mutates state.
type StateReadAhead struct {
	db         ethdb.KeyValueReader
	workers    int
	queueBytes int64
	work       chan stateReadAheadJob

	mu        sync.RWMutex
	stopped   bool
	startOnce sync.Once
	closeOnce sync.Once
	workersWG sync.WaitGroup
	pendingWG sync.WaitGroup

	epoch         atomic.Uint64
	applyingBlock atomic.Uint64
	queuedBytes   atomic.Int64

	enqueuedBlocks       atomic.Uint64
	enqueuedBytes        atomic.Uint64
	droppedBlocks        atomic.Uint64
	droppedBytes         atomic.Uint64
	processed            atomic.Uint64
	stale                atomic.Uint64
	rows                 atomic.Uint64
	present              atomic.Uint64
	missing              atomic.Uint64
	errors               atomic.Uint64
	completedBeforeApply atomic.Uint64
	completedAfterApply  atomic.Uint64
	assetCapBlocks       atomic.Uint64
	assetCappedAttempts  atomic.Uint64
	assetLegacyRows      atomic.Uint64
	assetV2Rows          atomic.Uint64
	assetBalanceRows     atomic.Uint64
	assetPlanFullRows    atomic.Uint64
}

// NewStateReadAhead builds a bounded session-level read-ahead pipeline.
func NewStateReadAhead(db ethdb.KeyValueReader, cfg StateReadAheadConfig) *StateReadAhead {
	if db == nil {
		return nil
	}
	workers := cfg.Workers
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0) / 4
		if workers < 1 {
			workers = 1
		}
	}
	if workers > maxStateReadAheadWorkers {
		workers = maxStateReadAheadWorkers
	}
	queueBlocks := cfg.QueueBlocks
	if queueBlocks <= 0 {
		queueBlocks = defaultStateReadAheadQueueBlocks
	}
	queueBytes := cfg.QueueBytes
	if queueBytes <= 0 {
		queueBytes = defaultStateReadAheadQueueBytes
	}
	return &StateReadAhead{
		db:         db,
		workers:    workers,
		queueBytes: queueBytes,
		work:       make(chan stateReadAheadJob, queueBlocks),
	}
}

// Start launches the configured workers once.
func (p *StateReadAhead) Start() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return
	}
	p.startOnce.Do(func() {
		for range p.workers {
			p.workersWG.Add(1)
			go p.worker()
		}
	})
}

// EnqueueBlocks submits blocks when their retained wire sizes are unavailable.
func (p *StateReadAhead) EnqueueBlocks(blocks []*types.Block) int {
	if p == nil || len(blocks) == 0 {
		return 0
	}
	p.Start()
	accepted := 0
	for _, block := range blocks {
		if p.enqueueBlock(block, 0) {
			accepted++
		}
	}
	return accepted
}

// EnqueueBlock submits one decoded block. encodedBytes should be the retained
// wire payload length when the caller already owns it; zero falls back to a
// protobuf size walk for non-downloader callers.
func (p *StateReadAhead) EnqueueBlock(block *types.Block, encodedBytes int) bool {
	if p == nil {
		return false
	}
	p.Start()
	return p.enqueueBlock(block, int64(encodedBytes))
}

func (p *StateReadAhead) enqueueBlock(block *types.Block, bytes int64) bool {
	if block == nil || block.Proto() == nil {
		return false
	}
	if bytes <= 0 {
		bytes = int64(proto.Size(block.Proto()))
	}
	if bytes < 1 {
		bytes = 1
	}
	if !p.reserveBytes(bytes) {
		p.recordDrop(bytes)
		return false
	}

	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.stopped {
		p.releaseBytes(bytes)
		p.recordDrop(bytes)
		return false
	}
	job := stateReadAheadJob{block: block, bytes: bytes, epoch: p.epoch.Load()}
	p.pendingWG.Add(1)
	select {
	case p.work <- job:
		p.enqueuedBlocks.Add(1)
		p.enqueuedBytes.Add(uint64(bytes))
		stateReadAheadEnqueuedBlocksCounter.Inc(1)
		stateReadAheadEnqueuedBytesCounter.Inc(bytes)
		return true
	default:
		p.pendingWG.Done()
		p.releaseBytes(bytes)
		p.recordDrop(bytes)
		return false
	}
}

func (p *StateReadAhead) reserveBytes(bytes int64) bool {
	if bytes > p.queueBytes {
		return false
	}
	for {
		current := p.queuedBytes.Load()
		if current+bytes > p.queueBytes {
			return false
		}
		if p.queuedBytes.CompareAndSwap(current, current+bytes) {
			stateReadAheadQueuedBytesGauge.Update(current + bytes)
			return true
		}
	}
}

func (p *StateReadAhead) releaseBytes(bytes int64) {
	stateReadAheadQueuedBytesGauge.Update(p.queuedBytes.Add(-bytes))
}

func (p *StateReadAhead) recordDrop(bytes int64) {
	p.droppedBlocks.Add(1)
	p.droppedBytes.Add(uint64(bytes))
	stateReadAheadDroppedBlocksCounter.Inc(1)
	stateReadAheadDroppedBytesCounter.Inc(bytes)
}

// Reset invalidates queued and in-flight work from the previous canonical fork.
func (p *StateReadAhead) Reset() {
	if p != nil {
		p.epoch.Add(1)
		p.applyingBlock.Store(0)
	}
}

// MarkApplying records when canonical execution begins a block. The canonical
// executor calls this and Reset serially. Workers use the height only for
// timing diagnostics; it never waits for or consumes warmup data.
func (p *StateReadAhead) MarkApplying(blockNumber uint64) {
	if p != nil {
		p.applyingBlock.Store(blockNumber)
	}
}

// Wait joins every block accepted before the call.
func (p *StateReadAhead) Wait() {
	if p != nil {
		p.pendingWG.Wait()
	}
}

// Close invalidates pending work, drains workers, and prevents new submissions.
func (p *StateReadAhead) Close() {
	if p == nil {
		return
	}
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.stopped = true
		p.epoch.Add(1)
		close(p.work)
		p.mu.Unlock()
	})
	p.workersWG.Wait()
}

// Stats returns lock-free cumulative counters for this pipeline.
func (p *StateReadAhead) Stats() StateReadAheadStats {
	if p == nil {
		return StateReadAheadStats{}
	}
	return StateReadAheadStats{
		EnqueuedBlocks:       p.enqueuedBlocks.Load(),
		EnqueuedBytes:        p.enqueuedBytes.Load(),
		QueuedBytes:          p.queuedBytes.Load(),
		DroppedBlocks:        p.droppedBlocks.Load(),
		DroppedBytes:         p.droppedBytes.Load(),
		ProcessedBlocks:      p.processed.Load(),
		StaleBlocks:          p.stale.Load(),
		Rows:                 p.rows.Load(),
		Present:              p.present.Load(),
		Missing:              p.missing.Load(),
		Errors:               p.errors.Load(),
		CompletedBeforeApply: p.completedBeforeApply.Load(),
		CompletedAfterApply:  p.completedAfterApply.Load(),
		AssetCapBlocks:       p.assetCapBlocks.Load(),
		AssetCappedAttempts:  p.assetCappedAttempts.Load(),
		AssetLegacyRows:      p.assetLegacyRows.Load(),
		AssetV2Rows:          p.assetV2Rows.Load(),
		AssetBalanceRows:     p.assetBalanceRows.Load(),
		AssetPlanFullRows:    p.assetPlanFullRows.Load(),
	}
}

func (p *StateReadAhead) worker() {
	defer p.workersWG.Done()
	for job := range p.work {
		p.releaseBytes(job.bytes)
		if job.epoch != p.epoch.Load() {
			p.stale.Add(1)
			stateReadAheadStaleCounter.Inc(1)
			p.pendingWG.Done()
			continue
		}
		if p.warmBlock(job) {
			if job.epoch != p.epoch.Load() {
				p.stale.Add(1)
				stateReadAheadStaleCounter.Inc(1)
				p.pendingWG.Done()
				continue
			}
			p.processed.Add(1)
			stateReadAheadProcessedCounter.Inc(1)
			if job.block.Number() <= p.applyingBlock.Load() {
				p.completedAfterApply.Add(1)
				stateReadAheadAfterApplyCounter.Inc(1)
			} else {
				p.completedBeforeApply.Add(1)
				stateReadAheadBeforeApplyCounter.Inc(1)
			}
		} else {
			p.stale.Add(1)
			stateReadAheadStaleCounter.Inc(1)
		}
		p.pendingWG.Done()
	}
}

type stateReadAheadTarget struct {
	address     tcommon.Address
	contract    bool
	owner       bool
	permissions []int32
}

type stateReadAheadTransferAsset struct {
	owner     tcommon.Address
	to        tcommon.Address
	assetName []byte
}

type stateReadAheadKVTarget struct {
	address    tcommon.Address
	generation uint64
	domain     kvdomains.KVDomain
	key        string
}

func (p *StateReadAhead) warmBlock(job stateReadAheadJob) bool {
	targets, assetTransfers := stateReadAheadTargets(job.block)
	assetAccounts := make(map[tcommon.Address]struct{}, len(assetTransfers)*2+1)
	if len(assetTransfers) > 0 {
		assetAccounts[tcommon.SystemAccountAddress] = struct{}{}
		for _, transfer := range assetTransfers {
			assetAccounts[transfer.owner] = struct{}{}
			assetAccounts[transfer.to] = struct{}{}
		}
	}
	// Stage one resolves account generations. Stage two groups all predictable
	// subrows into a deduplicated physical read plan. Hints may observe an old
	// generation, but canonical access still reopens the current envelope.
	accounts := rawdb.NewStatePrefetchPlan(maxStateReadAheadPlanRows)
	byID := make(map[int]stateReadAheadTarget, len(targets))
	for _, target := range targets {
		if id := accounts.AddAccount(target.address); id >= 0 {
			byID[id] = target
		}
	}
	generations := make(map[tcommon.Address]uint64, len(targets))
	rows := rawdb.NewStatePrefetchPlan(maxStateReadAheadPlanRows)
	stale := errors.New("state read-ahead epoch changed")
	err := accounts.Execute(p.db, func(id int, encoded []byte, ok bool, err error) error {
		if job.epoch != p.epoch.Load() {
			return stale
		}
		p.recordRow(ok, err)
		target := byID[id]
		_, assetAccount := assetAccounts[target.address]
		if err != nil || !ok || (!target.contract && !target.owner && len(target.permissions) == 0 && !assetAccount) {
			return nil
		}
		envelope, err := splitStateAccountV3(encoded)
		if err != nil {
			p.recordError()
			return nil
		}
		generation := envelope.accountKVGeneration
		generations[target.address] = generation
		for _, permission := range target.permissions {
			var key []byte
			switch permission {
			case 0:
				key = accountOwnerPermissionKey
			case 1:
				key = accountWitnessPermissionKey
			default:
				if permission < 2 {
					continue
				}
				key = accountActivePermissionKey(permission)
			}
			rows.AddKV(target.address, generation, kvdomains.AccountPermissionAux, key)
		}
		if target.owner {
			rows.AddKV(target.address, generation, kvdomains.AccountFrozenBandwidthAux, accountFrozenBandwidthKey(0))
			rows.AddKV(target.address, generation, kvdomains.AccountResourceAux, accountResourceKey)
		}
		if target.contract {
			rows.AddKV(target.address, generation, kvdomains.ContractMetadata, contractMetaKVKey)
			rows.AddCode(tcommon.BytesToHash(envelope.codeHash))
		}
		return nil
	})
	if errors.Is(err, stale) {
		return false
	}
	if err != nil {
		p.recordError()
	}
	// Prioritize the balance rows used by Validate across the whole block before
	// bandwidth, operation time, and metadata rows. Numeric names may mean a
	// literal pre-fork name or a post-fork V2 ID, so retain both balance forms;
	// putting all V2 balances first ensures a dense modern block does not spend
	// its bounded asset-row budget on less useful ancillary or legacy rows.
	seenCapacity := len(assetTransfers) * 8
	if seenCapacity > maxTransferAssetPrefetchRows {
		seenCapacity = maxTransferAssetPrefetchRows
	}
	seenKV := make(map[stateReadAheadKVTarget]struct{}, seenCapacity)
	var cappedAttempts uint64
	var legacyRows, v2Rows, balanceRows, planFullRows uint64
	prefetchKV := func(address tcommon.Address, domain kvdomains.KVDomain, key []byte, v2, balance bool) {
		generation, ok := generations[address]
		if !ok {
			return
		}
		target := stateReadAheadKVTarget{address: address, generation: generation, domain: domain, key: string(key)}
		if _, exists := seenKV[target]; exists {
			return
		}
		if len(seenKV) >= maxTransferAssetPrefetchRows {
			// Rejected hints are not retained; repeated rejected keys count
			// again. This keeps the tracking set bounded at the asset-row limit.
			cappedAttempts++
			return
		}
		seenKV[target] = struct{}{}
		if rows.AddKV(address, generation, domain, key) < 0 {
			planFullRows++
			return
		}
		if v2 {
			v2Rows++
		} else {
			legacyRows++
		}
		if balance {
			balanceRows++
		}
	}
	for _, transfer := range assetTransfers {
		tokenID, err := strconv.ParseInt(string(transfer.assetName), 10, 64)
		if err != nil {
			continue
		}
		tokenKey := []byte(strconv.FormatInt(tokenID, 10))
		prefetchKV(transfer.owner, kvdomains.AccountAssetV2, tokenKey, true, true)
		prefetchKV(transfer.to, kvdomains.AccountAssetV2, tokenKey, true, true)
	}
	for _, transfer := range assetTransfers {
		prefetchKV(transfer.owner, kvdomains.AccountAsset, transfer.assetName, false, true)
		prefetchKV(transfer.to, kvdomains.AccountAsset, transfer.assetName, false, true)
	}
	for _, transfer := range assetTransfers {
		legacyMeta := assetBytesKey(assetLegacyTag, transfer.assetName)
		prefetchKV(tcommon.SystemAccountAddress, kvdomains.SystemAsset, legacyMeta, false, false)
		prefetchKV(tcommon.SystemAccountAddress, kvdomains.SystemAsset, assetBandwidthKey(legacyMeta), false, false)
		prefetchKV(transfer.owner, kvdomains.AccountFreeAssetNetUsage, transfer.assetName, false, false)
		prefetchKV(transfer.owner, kvdomains.AccountAssetOperationTime, transfer.assetName, false, false)

		tokenID, err := strconv.ParseInt(string(transfer.assetName), 10, 64)
		if err != nil {
			continue
		}
		v2Meta := assetIDKey(assetV2Tag, tokenID)
		prefetchKV(tcommon.SystemAccountAddress, kvdomains.SystemAsset, v2Meta, true, false)
		prefetchKV(tcommon.SystemAccountAddress, kvdomains.SystemAsset, assetBandwidthKey(v2Meta), true, false)
		tokenKey := []byte(strconv.FormatInt(tokenID, 10))
		prefetchKV(transfer.owner, kvdomains.AccountFreeAssetNetUsageV2, tokenKey, true, false)
		prefetchKV(transfer.owner, kvdomains.AccountAssetOperationTimeV2, tokenKey, true, false)
	}
	if cappedAttempts > 0 {
		p.assetCapBlocks.Add(1)
		p.assetCappedAttempts.Add(cappedAttempts)
		stateReadAheadAssetCapBlocksCounter.Inc(1)
		stateReadAheadAssetCappedCounter.Inc(int64(cappedAttempts))
	}
	if legacyRows > 0 {
		p.assetLegacyRows.Add(legacyRows)
		stateReadAheadAssetLegacyCounter.Inc(int64(legacyRows))
	}
	if v2Rows > 0 {
		p.assetV2Rows.Add(v2Rows)
		stateReadAheadAssetV2Counter.Inc(int64(v2Rows))
	}
	if balanceRows > 0 {
		p.assetBalanceRows.Add(balanceRows)
		stateReadAheadAssetBalanceCounter.Inc(int64(balanceRows))
	}
	if planFullRows > 0 {
		p.assetPlanFullRows.Add(planFullRows)
		stateReadAheadAssetPlanFullCounter.Inc(int64(planFullRows))
	}
	err = rows.Execute(p.db, func(_ int, _ []byte, present bool, err error) error {
		if job.epoch != p.epoch.Load() {
			return stale
		}
		p.recordRow(present, err)
		return nil
	})
	if errors.Is(err, stale) {
		return false
	}
	if err != nil {
		p.recordError()
	}
	return job.epoch == p.epoch.Load()
}

func (p *StateReadAhead) recordRow(present bool, err error) {
	p.rows.Add(1)
	stateReadAheadRowsCounter.Inc(1)
	if err != nil {
		p.recordError()
		return
	}
	if present {
		p.present.Add(1)
		stateReadAheadPresentCounter.Inc(1)
		return
	}
	p.missing.Add(1)
	stateReadAheadMissingCounter.Inc(1)
}

func (p *StateReadAhead) recordError() {
	p.errors.Add(1)
	stateReadAheadErrorsCounter.Inc(1)
}

func stateReadAheadTargets(block *types.Block) ([]stateReadAheadTarget, []stateReadAheadTransferAsset) {
	if block == nil {
		return nil, nil
	}
	transactions := block.Transactions()
	index := make(map[tcommon.Address]int)
	targets := make([]stateReadAheadTarget, 0, len(transactions)*2+1)
	assetTransfers := make([]stateReadAheadTransferAsset, 0)
	add := func(raw []byte, contract bool) {
		if len(raw) != tcommon.AddressLength {
			return
		}
		address := tcommon.BytesToAddress(raw)
		if !address.ValidPrefix() {
			return
		}
		if i, ok := index[address]; ok {
			targets[i].contract = targets[i].contract || contract
			return
		}
		index[address] = len(targets)
		targets = append(targets, stateReadAheadTarget{address: address, contract: contract})
	}

	witness := block.WitnessAddress()
	add(witness[:], false)
	if i, ok := index[witness]; ok {
		targets[i].permissions = append(targets[i].permissions, 1)
	}
	for _, tx := range transactions {
		message, err := tx.DecodedContract()
		if err != nil || message == nil {
			continue
		}
		if owner, _, err := tx.ContractOwnerAddress(); err == nil {
			add(owner, false)
			if i, ok := index[tcommon.BytesToAddress(owner)]; ok {
				targets[i].owner = true
				permission := tx.Contract().GetPermissionId()
				seen := false
				for _, id := range targets[i].permissions {
					if id == permission {
						seen = true
						break
					}
				}
				if !seen {
					targets[i].permissions = append(targets[i].permissions, permission)
				}
			}
		}
		if value, ok := message.(interface{ GetToAddress() []byte }); ok {
			add(value.GetToAddress(), false)
		}
		if value, ok := message.(interface{ GetContractAddress() []byte }); ok {
			add(value.GetContractAddress(), true)
		}
		if value, ok := message.(interface{ GetReceiverAddress() []byte }); ok {
			add(value.GetReceiverAddress(), false)
		}
		if value, ok := message.(interface{ GetAccountAddress() []byte }); ok {
			add(value.GetAccountAddress(), false)
		}
		if value, ok := message.(interface{ GetTransparentFromAddress() []byte }); ok {
			add(value.GetTransparentFromAddress(), false)
		}
		if value, ok := message.(interface{ GetTransparentToAddress() []byte }); ok {
			add(value.GetTransparentToAddress(), false)
		}
		if transfer, ok := message.(*contractpb.TransferAssetContract); ok && len(transfer.OwnerAddress) == tcommon.AddressLength && len(transfer.ToAddress) == tcommon.AddressLength {
			owner := tcommon.BytesToAddress(transfer.OwnerAddress)
			to := tcommon.BytesToAddress(transfer.ToAddress)
			if owner.ValidPrefix() && to.ValidPrefix() {
				assetTransfers = append(assetTransfers, stateReadAheadTransferAsset{owner: owner, to: to, assetName: transfer.AssetName})
				add(tcommon.SystemAccountAddress[:], false)
			}
		}
	}
	return targets, assetTransfers
}
