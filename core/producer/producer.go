package producer

import (
	"crypto/ecdsa"
	"fmt"
	"sync"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	tcommon "github.com/tronprotocol/go-tron/common"
	gtronlog "github.com/tronprotocol/go-tron/common/log"
	"github.com/tronprotocol/go-tron/consensus/dpos"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/txpool"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/crypto"
	"github.com/tronprotocol/go-tron/params"
)

var log = gtronlog.NewModule("core/producer")

// Producer drives block production on a DPoS schedule.
type Producer struct {
	chain       *core.BlockChain
	pool        *txpool.TxPool
	engine      *dpos.DPoS
	witnessKey  *ecdsa.PrivateKey
	witnessAddr tcommon.Address

	lastProducedSlot int64
	loggedWitnessErr bool

	// Warning aggregation state is owned by the producer loop goroutine. It
	// changes logging only; every 500ms production retry still runs unchanged.
	lowParticipationActive          bool
	lowParticipationSince           time.Time
	lowParticipationLastSlot        int64
	lowParticipationSkippedSlots    uint64
	lowParticipationRetrySuppressed uint64
	lowParticipationLastSummary     time.Time
	productionFailureActive         bool
	productionFailureSince          time.Time
	productionFailureLastSlot       int64
	productionFailureSlots          uint64
	productionFailureSuppressed     uint64
	productionFailureLastSummary    time.Time

	quit chan struct{}
	wg   sync.WaitGroup

	// BlockCallback is called after a new block is produced and inserted.
	// Used by the P2P layer to broadcast the block to peers.
	BlockCallback func(block *types.Block)
}

const producerWarningSummaryInterval = 10 * time.Minute

func New(chain *core.BlockChain, pool *txpool.TxPool, engine *dpos.DPoS, witnessKey *ecdsa.PrivateKey) *Producer {
	return &Producer{
		chain:       chain,
		pool:        pool,
		engine:      engine,
		witnessKey:  witnessKey,
		witnessAddr: crypto.PubkeyToAddress(&witnessKey.PublicKey),
		quit:        make(chan struct{}),
	}
}

func (p *Producer) Start() error {
	p.wg.Add(1)
	go p.loop()
	log.Info("Block producer started",
		"witness", fmt.Sprintf("%x", p.witnessAddr[:6]))
	return nil
}

func (p *Producer) Stop() error {
	close(p.quit)
	p.wg.Wait()
	log.Info("Block producer stopped")
	return nil
}

func (p *Producer) loop() {
	defer p.wg.Done()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.tryProduceBlock()
		case <-p.quit:
			return
		}
	}
}

func (p *Producer) tryProduceBlock() {
	now := time.Now().UnixMilli()
	genesisTime := p.chain.GenesisTimestamp()
	interval := int64(params.BlockProducedInterval)

	// Align to slot boundary relative to genesis
	slotTimestamp := ((now-genesisTime)/interval)*interval + genesisTime

	// Check duplicate production
	currentSlot := dpos.AbsoluteSlot(slotTimestamp, genesisTime)
	if currentSlot <= p.lastProducedSlot {
		return
	}

	// Check if this is our slot
	headSlot := productionSlotForTimestamp(p.chain, slotTimestamp)
	if headSlot <= 0 {
		return
	}

	scheduled, err := p.engine.GetScheduledWitness(headSlot)
	if err != nil {
		if !p.loggedWitnessErr {
			log.Warn("Cannot get scheduled witness", "slot", currentSlot, "headSlot", headSlot, "err", err)
			p.loggedWitnessErr = true
		}
		return
	}
	if p.loggedWitnessErr {
		log.Info("Scheduled witness lookup recovered", "slot", currentSlot, "headSlot", headSlot)
		p.loggedWitnessErr = false
	}
	if scheduled != p.witnessAddr {
		return
	}

	// LOW_PARTICIPATION gate: skip the slot when the rolling
	// BLOCK_FILLED_SLOTS rate has dropped below the threshold. Mirrors
	// java-tron consensus/dpos/StateManager.java:54-59 invoked from
	// DposTask.produceBlock (DposTask.java:89-92).
	if skip, rate := shouldSkipLowParticipation(p.chain); skip {
		p.reportLowParticipation(currentSlot, rate, time.Now())
		return
	} else {
		p.reportLowParticipationRecovery(currentSlot, rate, time.Now())
	}

	produceStart := time.Now()
	if err := p.produceBlock(p.witnessAddr, slotTimestamp); err != nil {
		p.reportProduceFailure(currentSlot, slotTimestamp, time.Since(produceStart), err, time.Now())
		return
	}
	p.reportProduceRecovery(currentSlot, time.Now())

	p.lastProducedSlot = currentSlot
}

func (p *Producer) reportLowParticipation(slot, rate int64, now time.Time) {
	if p == nil {
		return
	}
	if slot == p.lowParticipationLastSlot {
		p.lowParticipationRetrySuppressed++
		return
	}
	p.lowParticipationLastSlot = slot
	p.lowParticipationSkippedSlots++
	if !p.lowParticipationActive {
		p.lowParticipationActive = true
		p.lowParticipationSince = now
		p.lowParticipationLastSummary = now
		p.lowParticipationSkippedSlots = 0
		log.Warn("Block production paused by low participation",
			"slot", slot,
			"ratePct", rate,
			"thresholdPct", params.MinParticipationRate)
		return
	}
	if now.Sub(p.lowParticipationLastSummary) < producerWarningSummaryInterval {
		return
	}
	log.Warn("Low participation continues",
		"slot", slot,
		"ratePct", rate,
		"thresholdPct", params.MinParticipationRate,
		"skippedSlotsSinceLastReport", p.lowParticipationSkippedSlots,
		"retryAttemptsSuppressed", p.lowParticipationRetrySuppressed,
		"stateDuration", now.Sub(p.lowParticipationSince).Round(time.Second))
	p.lowParticipationSkippedSlots = 0
	p.lowParticipationRetrySuppressed = 0
	p.lowParticipationLastSummary = now
}

func (p *Producer) reportLowParticipationRecovery(slot, rate int64, now time.Time) {
	if p == nil || !p.lowParticipationActive {
		return
	}
	log.Info("Block production resumed after low participation",
		"slot", slot,
		"ratePct", rate,
		"thresholdPct", params.MinParticipationRate,
		"stateDuration", now.Sub(p.lowParticipationSince).Round(time.Second),
		"skippedSlotsSinceLastReport", p.lowParticipationSkippedSlots,
		"retryAttemptsSuppressed", p.lowParticipationRetrySuppressed)
	p.lowParticipationActive = false
	p.lowParticipationLastSlot = 0
	p.lowParticipationSkippedSlots = 0
	p.lowParticipationRetrySuppressed = 0
}

func (p *Producer) reportProduceFailure(slot, slotTimestampMs int64, elapsed time.Duration, err error, now time.Time) {
	if p == nil {
		return
	}
	if slot == p.productionFailureLastSlot {
		p.productionFailureSuppressed++
		return
	}
	p.productionFailureLastSlot = slot
	p.productionFailureSlots++
	if !p.productionFailureActive {
		p.productionFailureActive = true
		p.productionFailureSince = now
		p.productionFailureLastSummary = now
		p.productionFailureSlots = 0
		log.Warn("Block production failing",
			"slot", slot,
			"slotTimestampMs", slotTimestampMs,
			"elapsed", ethcommon.PrettyDuration(elapsed),
			"err", err)
		return
	}
	if now.Sub(p.productionFailureLastSummary) < producerWarningSummaryInterval {
		return
	}
	log.Warn("Block production failures continue",
		"slot", slot,
		"slotTimestampMs", slotTimestampMs,
		"failedSlotsSinceLastReport", p.productionFailureSlots,
		"retryAttemptsSuppressed", p.productionFailureSuppressed,
		"stateDuration", now.Sub(p.productionFailureSince).Round(time.Second),
		"sampleErr", err)
	p.productionFailureSlots = 0
	p.productionFailureSuppressed = 0
	p.productionFailureLastSummary = now
}

func (p *Producer) reportProduceRecovery(slot int64, now time.Time) {
	if p == nil || !p.productionFailureActive {
		return
	}
	log.Info("Block production recovered",
		"slot", slot,
		"stateDuration", now.Sub(p.productionFailureSince).Round(time.Second),
		"failedSlotsSinceLastReport", p.productionFailureSlots,
		"retryAttemptsSuppressed", p.productionFailureSuppressed)
	p.productionFailureActive = false
	p.productionFailureSlots = 0
	p.productionFailureSuppressed = 0
}

func productionSlotForTimestamp(chain *core.BlockChain, timestamp int64) int64 {
	head := chain.CurrentBlock()
	wasMaintenance := chain.DynProps().StateFlag() == 1
	return dpos.SlotForTime(timestamp, head.Timestamp(), chain.GenesisTimestamp(),
		wasMaintenance, params.MaintenanceSkipSlots)
}

// shouldSkipLowParticipation reports whether the network's recent block-fill
// rate is below params.MinParticipationRate. Returns the observed rate so the
// caller can log it. Comparison is strict less-than to match java-tron
// StateManager.java:56 (`participation < minParticipationRate`).
func shouldSkipLowParticipation(chain *core.BlockChain) (bool, int64) {
	rate := chain.DynProps().CalculateFilledSlotsCount()
	return rate < int64(params.MinParticipationRate), rate
}

func (p *Producer) produceBlock(witnessAddr tcommon.Address, timestamp int64) error {
	produceStart := time.Now()
	result, err := core.BuildBlock(p.chain, p.pool, witnessAddr, timestamp)
	if err != nil {
		return err
	}
	block := result.Block

	// Evict transactions that failed validation
	if len(result.FailedTxIDs) > 0 {
		p.pool.RemoveBatch(result.FailedTxIDs)
		log.Debug("Evicted invalid transactions from pool",
			"count", len(result.FailedTxIDs))
	}

	if err := core.SignBlock(block, p.witnessKey); err != nil {
		return err
	}

	if err := p.chain.InsertBlock(block); err != nil {
		return err
	}

	var hashes []tcommon.Hash
	for _, tx := range block.Transactions() {
		hashes = append(hashes, tx.Hash())
	}
	if len(hashes) > 0 {
		p.pool.RemoveBatch(hashes)
	}

	log.Info("Block produced",
		"number", block.Number(),
		"hash", block.Hash(),
		"txs", len(block.Transactions()),
		"witness", fmt.Sprintf("%x", witnessAddr[:6]),
		"slot", dpos.AbsoluteSlot(timestamp, p.chain.GenesisTimestamp()),
		"slotTimestampMs", timestamp,
		"elapsed", ethcommon.PrettyDuration(time.Since(produceStart)))

	if p.BlockCallback != nil {
		p.BlockCallback(block)
	}
	return nil
}
