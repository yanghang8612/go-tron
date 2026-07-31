package metricsapi

import (
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
)

func TestNodeCollectorPublishesSnapshot(t *testing.T) {
	registry := metrics.NewRegistry()
	collector := newNodeCollector(NodeSources{
		HeadBlock:           func() uint64 { return 100 },
		SolidifiedBlock:     func() int64 { return 92 },
		ConnectedPeers:      func() int { return 9 },
		HandshakedPeers:     func() int { return 7 },
		PendingTransactions: func() int { return 12 },
		Syncing:             func() bool { return true },
		SyncRemainingBlocks: func() (int64, bool) { return 55, true },
	}, registry, time.Hour)

	if err := collector.Start(); err != nil {
		t.Fatalf("start node collector: %v", err)
	}
	if err := collector.Stop(); err != nil {
		t.Fatalf("stop node collector: %v", err)
	}

	wants := map[string]int64{
		"chain/head/block":       100,
		"chain/solidified/block": 92,
		"chain/solidified/lag":   8,
		"p2p/peers/connected":    9,
		"p2p/peers/handshaked":   7,
		"txpool/pending":         12,
		"sync/active":            1,
		"sync/remaining/blocks":  55,
	}
	for name, want := range wants {
		gauge, ok := registry.Get(name).(*metrics.Gauge)
		if !ok {
			t.Fatalf("metric %q is not a gauge", name)
		}
		if got := gauge.Snapshot().Value(); got != want {
			t.Errorf("metric %q = %d, want %d", name, got, want)
		}
	}
}

func TestClampUint64(t *testing.T) {
	if got := clampUint64(^uint64(0)); got != int64(^uint64(0)>>1) {
		t.Fatalf("clampUint64(max) = %d", got)
	}
}
