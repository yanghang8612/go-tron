package blockbuffer

import (
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// These process-cumulative counters cover only completed pooled foreground
// base fallbacks. Overlay/base-cache hits and cursor prefetch are excluded.
// They measure logical results, not L6 probes, SST reads, or device IO.
var (
	pooledForegroundStorageReads = newPooledForegroundBaseMetrics("contract_storage")
	pooledForegroundAssetReads   = newPooledForegroundBaseMetrics("account_asset_v2")
)

type pooledForegroundBaseMetrics struct {
	calls, present, notFound, errors *metrics.Counter
}

func newPooledForegroundBaseMetrics(domain string) *pooledForegroundBaseMetrics {
	prefix := "blockbuffer/pooled_foreground_base/state_kv_latest/" + domain + "/"
	return &pooledForegroundBaseMetrics{
		calls:    metrics.NewRegisteredCounter(prefix+"calls", nil),
		present:  metrics.NewRegisteredCounter(prefix+"present", nil),
		notFound: metrics.NewRegisteredCounter(prefix+"not_found", nil),
		errors:   metrics.NewRegisteredCounter(prefix+"errors", nil),
	}
}

// Only the metrics pointer survives the synchronous read and pool return; no
// caller or pooled key bytes are retained by instrumentation.
func pooledForegroundBaseMetricsForKey(key []byte) *pooledForegroundBaseMetrics {
	domain, ok := rawdb.ClassifyStateKVLatestReadDomain(key)
	if !ok {
		return nil
	}
	switch domain {
	case kvdomains.ContractStorage:
		return pooledForegroundStorageReads
	case kvdomains.AccountAssetV2:
		return pooledForegroundAssetReads
	default:
		return nil
	}
}

func (m *pooledForegroundBaseMetrics) recordCompleted(err error) {
	if m == nil {
		return
	}
	// Keep this after the original key-pool return. Panics are not completed
	// calls; adding no defer/recover preserves existing ownership and panic order.
	// Separate atomic counters do not form an atomic snapshot during a read.
	m.calls.Inc(1)
	switch err {
	case nil:
		m.present.Inc(1) // Empty values are still present at this layer.
	case ErrNotFound:
		m.notFound.Inc(1) // Already normalized by the read helper.
	default:
		m.errors.Inc(1)
	}
}
