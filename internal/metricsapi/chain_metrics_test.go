package metricsapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/crypto"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestChainMetricsEndpoint(t *testing.T) {
	registry := metrics.NewRegistry()
	gauge := metrics.NewRegisteredGauge("gtron/test/value", registry)
	gauge.Update(42)

	m := NewChainMetrics()
	miner := common.Address{0x41, 1}
	added := common.Address{0x41, 2}
	removed := common.Address{0x41, 3}
	m.ObserveBlock(types.NewBlockFromPB(&corepb.Block{
		BlockHeader:  &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{WitnessAddress: miner[:]}},
		Transactions: []*corepb.Transaction{{}, {}},
	}))
	m.ObserveWitnessSet([]common.Address{miner, removed}, []common.Address{miner, added})
	m.ObserveWitnessSet([]common.Address{miner, added}, []common.Address{added, miner})

	handler := newServer("127.0.0.1:0", registry, m.registry).httpServer.Handler
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := res.Body.String()
	for _, want := range []string{
		"gtron_test_value 42",
		"# TYPE tron:block_transaction_count histogram",
		`tron:block_transaction_count_bucket{miner="` + crypto.AddressToBase58(miner) + `",le="0"} 0`,
		`tron:block_transaction_count_bucket{miner="` + crypto.AddressToBase58(miner) + `",le="20"} 1`,
		`tron:block_transaction_count_bucket{miner="` + crypto.AddressToBase58(miner) + `",le="10000"} 1`,
		`tron:sr_set_change_total{action="add",witness="` + crypto.AddressToBase58(added) + `"} 1`,
		`tron:sr_set_change_total{action="remove",witness="` + crypto.AddressToBase58(removed) + `"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in %s", want, body)
		}
	}
	if res.Code != http.StatusOK || strings.Count(body, "tron:sr_set_change_total{") != 2 {
		t.Fatalf("status %d, body %s", res.Code, body)
	}
}
