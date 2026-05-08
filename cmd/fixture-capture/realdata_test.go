package main

import (
	"os"
	"testing"

	"google.golang.org/protobuf/proto"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// TestUnmarshalTronJSON_RealMainnetSR runs the converter against a real
// wallet/getaccount response captured from mainnet. Skipped when the cached
// payload is missing — capture it once via:
//
//	curl -sS -x socks5h://127.0.0.1:1088 -X POST \
//	    http://3.12.206.71:8088/wallet/getaccount \
//	    -d '{"address":"4170df4a99c7e3e249b290b43e11eb2368afd59899"}' > /tmp/sr_account.json
func TestUnmarshalTronJSON_RealMainnetSR(t *testing.T) {
	body, err := os.ReadFile("/tmp/sr_account.json")
	if err != nil {
		t.Skip("cached SR payload missing; see test comment for capture command")
	}
	var got corepb.Account
	if err := unmarshalTronJSON(body, &got); err != nil {
		t.Fatalf("unmarshal real mainnet payload: %v", err)
	}
	if len(got.Address) != 21 {
		t.Errorf("address: want 21 bytes, got %d", len(got.Address))
	}
	if got.Balance <= 0 {
		t.Errorf("balance not parsed: %d", got.Balance)
	}
	if !got.IsWitness {
		t.Errorf("is_witness=true expected for SR")
	}
	if got.OwnerPermission == nil || len(got.OwnerPermission.Keys) == 0 {
		t.Errorf("owner_permission lost: %+v", got.OwnerPermission)
	}
	if got.AccountResource == nil {
		t.Errorf("account_resource lost")
	}
	// java-tron returns proto maps in the [{key,value}] array form;
	// confirm at least one entry of assetV2 made it through.
	if len(got.AssetV2) > 0 {
		var sawNonZero bool
		for _, v := range got.AssetV2 {
			if v > 0 {
				sawNonZero = true
				break
			}
		}
		if !sawNonZero {
			t.Errorf("assetV2 map decoded but every entry is zero")
		}
	}

	// Marshallable — the bytes are what we'll base64 into snapshot.json.
	if _, err := proto.Marshal(&got); err != nil {
		t.Fatalf("marshal: %v", err)
	}
}

// TestUnmarshalTronJSON_RealMainnetBlock validates the Block + Transaction
// + Any path against a real wallet/getblockbynum response. Skipped when the
// cached payload is missing — capture once via:
//
//	curl -sS -x socks5h://127.0.0.1:1088 -X POST \
//	    http://3.12.206.71:8088/wallet/getblockbynum \
//	    -d '{"num":82495317}' > /tmp/sr_block.json
func TestUnmarshalTronJSON_RealMainnetBlock(t *testing.T) {
	body, err := os.ReadFile("/tmp/sr_block.json")
	if err != nil {
		t.Skip("cached block payload missing")
	}
	var blk corepb.Block
	if err := unmarshalTronJSON(body, &blk); err != nil {
		t.Fatalf("unmarshal block: %v", err)
	}
	if blk.BlockHeader == nil || blk.BlockHeader.RawData == nil {
		t.Fatal("block_header.raw_data missing")
	}
	if blk.BlockHeader.RawData.Number == 0 {
		t.Errorf("block number not parsed: %d", blk.BlockHeader.RawData.Number)
	}
	if len(blk.Transactions) == 0 {
		t.Fatal("transactions list empty")
	}

	// First tx: Any-encoded TransferContract should round-trip back to
	// real proto bytes that unpack to the inner contract.
	tx0 := blk.Transactions[0]
	if tx0.RawData == nil || len(tx0.RawData.Contract) == 0 {
		t.Fatal("first tx has no contract")
	}
	c0 := tx0.RawData.Contract[0]
	if c0.Parameter == nil || len(c0.Parameter.Value) == 0 {
		t.Fatal("contract.parameter.value bytes empty (Any not populated)")
	}

	// Decode the inner TransferContract to confirm Any.value carries proto bytes.
	var inner contractpb.TransferContract
	if err := proto.Unmarshal(c0.Parameter.Value, &inner); err != nil {
		t.Fatalf("Any.value did not survive as proto bytes: %v", err)
	}
	if len(inner.OwnerAddress) != 21 || len(inner.ToAddress) != 21 {
		t.Errorf("inner addresses malformed: owner=%dB to=%dB", len(inner.OwnerAddress), len(inner.ToAddress))
	}

	// Block must serialize back into proto bytes (this is what we'll write
	// into blocks.bin).
	if _, err := proto.Marshal(&blk); err != nil {
		t.Fatalf("marshal block: %v", err)
	}
}
