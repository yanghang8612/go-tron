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
