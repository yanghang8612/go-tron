package jsonrpc_test

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/tronprotocol/go-tron/internal/jsonrpc"
	"github.com/tronprotocol/go-tron/internal/rpc"
)

func TestGtronAPI_FreezerStatusFramework(t *testing.T) {
	min, max := uint64(5), uint64(11)
	be := newFreezeBackend()
	be.freezerStatus = &jsonrpc.FreezerStatus{
		Available:   true,
		HasFrozen:   true,
		FrozenMin:   &min,
		FrozenMax:   &max,
		TableCounts: map[string]uint64{"bodies": 12},
	}

	srv := rpc.NewServer()
	if err := srv.RegisterName("gtron", jsonrpc.NewGtronAPI(be)); err != nil {
		t.Fatalf("RegisterName: %v", err)
	}
	t.Cleanup(srv.Stop)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	result, errObj := postRPC(t, ts.URL, `{"jsonrpc":"2.0","id":1,"method":"gtron_freezerStatus","params":[]}`)
	if errObj != nil {
		t.Fatalf("unexpected JSON-RPC error: %+v", errObj)
	}
	var got struct {
		Available   bool              `json:"available"`
		HasFrozen   bool              `json:"hasFrozen"`
		FrozenMin   uint64            `json:"frozenMin"`
		FrozenMax   uint64            `json:"frozenMax"`
		TableCounts map[string]uint64 `json:"tableCounts"`
	}
	if err := json.Unmarshal(result, &got); err != nil {
		t.Fatalf("decode result %s: %v", result, err)
	}
	if !got.Available || !got.HasFrozen || got.FrozenMin != 5 || got.FrozenMax != 11 || got.TableCounts["bodies"] != 12 {
		t.Fatalf("gtron_freezerStatus = %+v, want bounds 5..11 and bodies=12", got)
	}
}
