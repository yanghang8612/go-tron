package metricsapi

import (
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
)

const defaultCollectionInterval = 3 * time.Second

// NodeSources contains concurrency-safe snapshots of the node's core state.
// Function fields keep this package independent of the chain, P2P and sync
// implementations and make the collection loop cheap to test.
type NodeSources struct {
	HeadBlock           func() uint64
	SolidifiedBlock     func() int64
	ConnectedPeers      func() int
	HandshakedPeers     func() int
	PendingTransactions func() int
	Syncing             func() bool
	SyncRemainingBlocks func() (int64, bool)
}

// NodeCollector periodically publishes the most useful operator-level node
// gauges. It implements node.Lifecycle.
type NodeCollector struct {
	sources  NodeSources
	interval time.Duration

	headBlock       *metrics.Gauge
	solidifiedBlock *metrics.Gauge
	solidifiedLag   *metrics.Gauge
	connectedPeers  *metrics.Gauge
	handshakedPeers *metrics.Gauge
	pendingTxs      *metrics.Gauge
	syncing         *metrics.Gauge
	syncRemaining   *metrics.Gauge

	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

// NewNodeCollector constructs a collector backed by the process-wide registry.
func NewNodeCollector(sources NodeSources) *NodeCollector {
	return newNodeCollector(sources, metrics.DefaultRegistry, defaultCollectionInterval)
}

func newNodeCollector(sources NodeSources, registry metrics.Registry, interval time.Duration) *NodeCollector {
	return &NodeCollector{
		sources:         sources,
		interval:        interval,
		headBlock:       metrics.NewRegisteredGauge("chain/head/block", registry),
		solidifiedBlock: metrics.NewRegisteredGauge("chain/solidified/block", registry),
		solidifiedLag:   metrics.NewRegisteredGauge("chain/solidified/lag", registry),
		connectedPeers:  metrics.NewRegisteredGauge("p2p/peers/connected", registry),
		handshakedPeers: metrics.NewRegisteredGauge("p2p/peers/handshaked", registry),
		pendingTxs:      metrics.NewRegisteredGauge("txpool/pending", registry),
		syncing:         metrics.NewRegisteredGauge("sync/active", registry),
		syncRemaining:   metrics.NewRegisteredGauge("sync/remaining/blocks", registry),
		stop:            make(chan struct{}),
		done:            make(chan struct{}),
	}
}

// Start publishes an initial snapshot and begins periodic collection.
func (c *NodeCollector) Start() error {
	c.collect()
	go func() {
		defer close(c.done)
		ticker := time.NewTicker(c.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				c.collect()
			case <-c.stop:
				return
			}
		}
	}()
	return nil
}

// Stop ends periodic collection.
func (c *NodeCollector) Stop() error {
	c.stopOnce.Do(func() { close(c.stop) })
	<-c.done
	return nil
}

func (c *NodeCollector) collect() {
	var head int64
	if c.sources.HeadBlock != nil {
		head = clampUint64(c.sources.HeadBlock())
		c.headBlock.Update(head)
	}
	if c.sources.SolidifiedBlock != nil {
		solidified := c.sources.SolidifiedBlock()
		if solidified < 0 {
			solidified = 0
		}
		c.solidifiedBlock.Update(solidified)
		lag := head - solidified
		if lag < 0 {
			lag = 0
		}
		c.solidifiedLag.Update(lag)
	}
	if c.sources.ConnectedPeers != nil {
		c.connectedPeers.Update(int64(c.sources.ConnectedPeers()))
	}
	if c.sources.HandshakedPeers != nil {
		c.handshakedPeers.Update(int64(c.sources.HandshakedPeers()))
	}
	if c.sources.PendingTransactions != nil {
		c.pendingTxs.Update(int64(c.sources.PendingTransactions()))
	}
	if c.sources.Syncing != nil && c.sources.Syncing() {
		c.syncing.Update(1)
	} else {
		c.syncing.Update(0)
	}
	remaining := int64(0)
	if c.sources.SyncRemainingBlocks != nil {
		if value, ok := c.sources.SyncRemainingBlocks(); ok && value > 0 {
			remaining = value
		}
	}
	c.syncRemaining.Update(remaining)
}

func clampUint64(value uint64) int64 {
	const maxInt64 = int64(^uint64(0) >> 1)
	if value > uint64(maxInt64) {
		return maxInt64
	}
	return int64(value)
}
