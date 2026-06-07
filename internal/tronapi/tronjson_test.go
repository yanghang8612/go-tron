package tronapi

import (
	"encoding/json"
	"testing"

	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// java-tron's JsonFormat serializes proto map fields as their underlying
// repeated MapEntry messages — an array of {key, value} objects, not a JSON
// object. assetV2 is the field the cross-impl flow test caught.
func TestMarshalTronJSON_MapAsKeyValueArray(t *testing.T) {
	acc := &corepb.Account{
		AssetV2: map[string]int64{"1000002": 200, "1000001": 100},
	}
	b, err := marshalTronJSON(acc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	arr, ok := out["assetV2"].([]any)
	if !ok {
		t.Fatalf("assetV2: want []any, got %T (%s)", out["assetV2"], b)
	}
	if len(arr) != 2 {
		t.Fatalf("assetV2: want 2 entries, got %d", len(arr))
	}
	// Entries are sorted by key.
	first := arr[0].(map[string]any)
	if first["key"] != "1000001" || first["value"].(float64) != 100 {
		t.Fatalf("entry 0: want {1000001,100}, got %v", first)
	}
	second := arr[1].(map[string]any)
	if second["key"] != "1000002" || second["value"].(float64) != 200 {
		t.Fatalf("entry 1: want {1000002,200}, got %v", second)
	}
}

// The diagnostic ResourceReceipt fields (gtron-only, non-consensus) must be
// reachable through gettransactioninfobyid — that is the whole point of adding
// them. marshalTronJSON is reflection-based, so they surface automatically
// under the snake_case proto names when non-zero; this pins that contract so a
// future switch to hand-built serialization can't silently drop them.
func TestMarshalTronJSON_DiagnosticReceiptFieldsVisible(t *testing.T) {
	info := &corepb.TransactionInfo{
		Receipt: &corepb.ResourceReceipt{
			OwnerBalance:                5_000_000,
			OwnerFreeNetLeft:            400,
			OwnerFrozenNetLeft:          700,
			OwnerNetLastConsumeTime:     111,
			OwnerFreeNetLastConsumeTime: 222,
			OwnerFrozenForNet:           1_000_000,
			OwnerFrozenForEnergy:        2_000_000,
			OriginEnergyWindow:          28_800,
			CallerEnergyWindow:          14_400,
		},
	}
	b, err := marshalTronJSON(info)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	receipt, ok := out["receipt"].(map[string]any)
	if !ok {
		t.Fatalf("receipt: want object, got %T (%s)", out["receipt"], b)
	}
	want := map[string]float64{
		"owner_balance":                    5_000_000,
		"owner_free_net_left":              400,
		"owner_frozen_net_left":            700,
		"owner_net_last_consume_time":      111,
		"owner_free_net_last_consume_time": 222,
		"owner_frozen_for_net":             1_000_000,
		"owner_frozen_for_energy":          2_000_000,
		"origin_energy_window":             28_800,
		"caller_energy_window":             14_400,
	}
	for key, val := range want {
		got, present := receipt[key]
		if !present {
			t.Errorf("receipt[%q] missing from JSON: %s", key, b)
			continue
		}
		if got.(float64) != val {
			t.Errorf("receipt[%q] = %v, want %v", key, got, val)
		}
	}
}

// java-tron's Util.convertOutput decodes Account.asset_issued_ID from its raw
// bytes to a UTF-8 string; every other bytes field stays hex-encoded.
func TestMarshalTronJSON_AssetIssuedIDDecoded(t *testing.T) {
	acc := &corepb.Account{
		AssetIssued_ID:  []byte("1000001"),
		AssetIssuedName: []byte("MYTOKEN"),
		Address:         []byte{0x41, 0x02, 0x03},
	}
	b, err := marshalTronJSON(acc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out["asset_issued_ID"] != "1000001" {
		t.Fatalf("asset_issued_ID: want \"1000001\", got %v", out["asset_issued_ID"])
	}
	// asset_issued_name and address stay hex.
	if out["asset_issued_name"] != "4d59544f4b454e" {
		t.Fatalf("asset_issued_name: want hex, got %v", out["asset_issued_name"])
	}
	if out["address"] != "410203" {
		t.Fatalf("address: want hex, got %v", out["address"])
	}
}
