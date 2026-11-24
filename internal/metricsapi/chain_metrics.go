package metricsapi

import (
	"slices"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/crypto"
)

// ChainMetrics owns the Java-compatible metric families for one node.
type ChainMetrics struct {
	registry            *prometheus.Registry
	transactionsInBlock *prometheus.HistogramVec
	witnessSetChanges   *prometheus.CounterVec
}

func NewChainMetrics() *ChainMetrics {
	m := &ChainMetrics{
		registry: prometheus.NewRegistry(),
		transactionsInBlock: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "tron:block_transaction_count",
			Help:    "Distribution of transaction counts per block.",
			Buckets: []float64{0, 20, 50, 80, 100, 120, 140, 160, 180, 200, 230, 260, 300, 500, 2000, 5000, 10000},
		}, []string{"miner"}),
		witnessSetChanges: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tron:sr_set_change_total",
			Help: "Number of changes to the active witness set.",
		}, []string{"action", "witness"}),
	}
	m.registry.MustRegister(m.transactionsInBlock, m.witnessSetChanges)
	return m
}

func (m *ChainMetrics) ObserveBlock(block *types.Block) {
	miner := crypto.AddressToBase58(block.WitnessAddress())
	m.transactionsInBlock.WithLabelValues(miner).Observe(float64(len(block.Proto().GetTransactions())))
}

func (m *ChainMetrics) ObserveWitnessSet(previous, current []common.Address) {
	for _, witness := range current {
		if !slices.Contains(previous, witness) {
			m.witnessSetChanges.WithLabelValues("add", crypto.AddressToBase58(witness)).Inc()
		}
	}
	for _, witness := range previous {
		if !slices.Contains(current, witness) {
			m.witnessSetChanges.WithLabelValues("remove", crypto.AddressToBase58(witness)).Inc()
		}
	}
}
