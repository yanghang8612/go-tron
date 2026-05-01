package producer

import (
	"crypto/ecdsa"
	"log"
	"sync"
	"time"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/consensus/dpos"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/txpool"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/crypto"
	"github.com/tronprotocol/go-tron/params"
)

// Producer drives block production on a DPoS schedule.
type Producer struct {
	chain       *core.BlockChain
	pool        *txpool.TxPool
	engine      *dpos.DPoS
	witnessKey  *ecdsa.PrivateKey
	witnessAddr tcommon.Address

	lastProducedSlot int64
	loggedWitnessErr bool
	quit             chan struct{}
	wg               sync.WaitGroup

	// BlockCallback is called after a new block is produced and inserted.
	// Used by the P2P layer to broadcast the block to peers.
	BlockCallback func(block *types.Block)
}

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
	log.Printf("Block producer started (witness=%x)", p.witnessAddr[:6])
	return nil
}

func (p *Producer) Stop() error {
	close(p.quit)
	p.wg.Wait()
	log.Println("Block producer stopped")
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
	slotTimestamp := ((now - genesisTime) / interval) * interval + genesisTime

	// Check duplicate production
	currentSlot := dpos.AbsoluteSlot(slotTimestamp, genesisTime)
	if currentSlot <= p.lastProducedSlot {
		return
	}

	// Check if this is our slot
	head := p.chain.CurrentBlock()
	headSlot := dpos.SlotForTime(slotTimestamp, head.Timestamp(), genesisTime,
		p.engine.IsInMaintenance(head.Timestamp()), params.MaintenanceSkipSlots)
	if headSlot <= 0 {
		return
	}

	scheduled, err := p.engine.GetScheduledWitness(headSlot)
	if err != nil {
		if !p.loggedWitnessErr {
			log.Printf("Cannot get scheduled witness: %v", err)
			p.loggedWitnessErr = true
		}
		return
	}
	if scheduled != p.witnessAddr {
		return
	}

	// LOW_PARTICIPATION gate: skip the slot when the rolling
	// BLOCK_FILLED_SLOTS rate has dropped below the threshold. Mirrors
	// java-tron consensus/dpos/StateManager.java:54-59 invoked from
	// DposTask.produceBlock (DposTask.java:89-92).
	if skip, rate := shouldSkipLowParticipation(p.chain); skip {
		log.Printf("LOW_PARTICIPATION rate=%d threshold=%d, skipping slot",
			rate, params.MinParticipationRate)
		return
	}

	if err := p.produceBlock(p.witnessAddr, slotTimestamp); err != nil {
		log.Printf("Failed to produce block: %v", err)
		return
	}

	p.lastProducedSlot = currentSlot
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
	result, err := core.BuildBlock(p.chain, p.pool, witnessAddr, timestamp)
	if err != nil {
		return err
	}
	block := result.Block

	// Evict transactions that failed validation
	if len(result.FailedTxIDs) > 0 {
		p.pool.RemoveBatch(result.FailedTxIDs)
		log.Printf("Evicted %d invalid transactions from pool", len(result.FailedTxIDs))
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

	log.Printf("Produced block #%d at timestamp %d (%d txs)",
		block.Number(), block.Timestamp(), len(block.Transactions()))

	if p.BlockCallback != nil {
		p.BlockCallback(block)
	}
	return nil
}
