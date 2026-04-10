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
	quit             chan struct{}
	wg               sync.WaitGroup
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
		return
	}
	if scheduled != p.witnessAddr {
		return
	}

	if err := p.produceBlock(p.witnessAddr, slotTimestamp); err != nil {
		log.Printf("Failed to produce block: %v", err)
		return
	}

	p.lastProducedSlot = currentSlot
}

func (p *Producer) produceBlock(witnessAddr tcommon.Address, timestamp int64) error {
	block, err := core.BuildBlock(p.chain, p.pool, witnessAddr, timestamp)
	if err != nil {
		return err
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
	return nil
}
