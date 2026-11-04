package metricsapi

import (
	"github.com/ethereum/go-ethereum/metrics"
	"testing"
	"time"
)

func TestNodeCollectorRefreshMaintenanceInExistingCollection(t *testing.T) {
	calls := 0
	c := newNodeCollector(NodeSources{RefreshMaintenanceMetrics: func() { calls++ }}, metrics.NewRegistry(), time.Hour)
	c.collect()
	c.collect()
	if calls != 2 {
		t.Fatalf("refresh calls %d", calls)
	}
}
