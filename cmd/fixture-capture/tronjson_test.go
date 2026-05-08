package main

import (
	"bytes"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// TestUnmarshalTronJSON_Account exercises the conversion path on a
// hand-written java-tron-style JSON. Covers: bytes-as-hex, int64-as-number,
// nested message, list of messages, enum-as-string-name.
func TestUnmarshalTronJSON_Account(t *testing.T) {
	// Sample shape closely matching wallet/getaccount on a real SR. Bytes
	// fields are hex (no 0x prefix) per java-tron's JsonFormat.
	jsonBody := []byte(`{
        "address": "4170df4a99c7e3e249b290b43e11eb2368afd59899",
        "account_name": "4e616e73656e5f6169",
        "balance": 445454911,
        "create_time": 1725344406000,
        "is_witness": true,
        "type": "AssetIssue",
        "frozenV2": [
            {},
            {"type": "ENERGY"},
            {"type": "TRON_POWER", "amount": 1000000}
        ],
        "votes": [
            {"vote_address": "4170df4a99c7e3e249b290b43e11eb2368afd59899", "vote_count": 100}
        ],
        "asset": {"hello": 5}
    }`)

	var got corepb.Account
	if err := unmarshalTronJSON(jsonBody, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if !bytes.Equal(got.Address, []byte{0x41, 0x70, 0xdf, 0x4a, 0x99, 0xc7, 0xe3, 0xe2, 0x49, 0xb2, 0x90, 0xb4, 0x3e, 0x11, 0xeb, 0x23, 0x68, 0xaf, 0xd5, 0x98, 0x99}) {
		t.Errorf("address bytes lost: %x", got.Address)
	}
	if string(got.AccountName) != "Nansen_ai" {
		t.Errorf("account_name: got %q, want Nansen_ai", got.AccountName)
	}
	if got.Balance != 445454911 {
		t.Errorf("balance: got %d, want 445454911", got.Balance)
	}
	if got.CreateTime != 1725344406000 {
		t.Errorf("create_time: got %d", got.CreateTime)
	}
	if !got.IsWitness {
		t.Errorf("is_witness lost")
	}
	if got.Type != corepb.AccountType_AssetIssue {
		t.Errorf("type: got %v, want AssetIssue", got.Type)
	}
	if len(got.FrozenV2) != 3 {
		t.Fatalf("frozenV2 len: got %d, want 3", len(got.FrozenV2))
	}
	// FreezeV2 type field is the proto enum value ResourceCode.
	if got.FrozenV2[0].Type != corepb.ResourceCode_BANDWIDTH /* zero default */ {
		t.Errorf("frozenV2[0].type: got %v, want BANDWIDTH", got.FrozenV2[0].Type)
	}
	if got.FrozenV2[1].Type != corepb.ResourceCode_ENERGY {
		t.Errorf("frozenV2[1].type: got %v", got.FrozenV2[1].Type)
	}
	if got.FrozenV2[2].Amount != 1000000 {
		t.Errorf("frozenV2[2].amount: got %d", got.FrozenV2[2].Amount)
	}
	if len(got.Votes) != 1 || got.Votes[0].VoteCount != 100 {
		t.Errorf("votes lost: %+v", got.Votes)
	}
	if v, ok := got.Asset["hello"]; !ok || v != 5 {
		t.Errorf("asset map lost: %+v", got.Asset)
	}

	// Round-trip via proto.Marshal: bytes must be deterministic enough for
	// downstream digest pinning.
	if _, err := proto.Marshal(&got); err != nil {
		t.Fatalf("marshal back: %v", err)
	}
}

// TestUnmarshalTronJSON_Witness verifies the listwitnesses-style JSON shape
// translates to corepb.Witness. This is the proto we serialize and write to
// the snapshot's witnesses[] list.
func TestUnmarshalTronJSON_Witness(t *testing.T) {
	body := []byte(`{
        "address": "4170df4a99c7e3e249b290b43e11eb2368afd59899",
        "voteCount": 71400000,
        "url": "http://nansen.ai/v1",
        "totalProduced": 12345,
        "totalMissed": 6,
        "isJobs": true,
        "latestBlockNum": 82495318,
        "latestSlotNum": 27498438
    }`)
	var w corepb.Witness
	if err := unmarshalTronJSON(body, &w); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if w.VoteCount != 71400000 {
		t.Errorf("voteCount: %d", w.VoteCount)
	}
	if !w.IsJobs {
		t.Errorf("isJobs lost")
	}
	if w.LatestBlockNum != 82495318 || w.LatestSlotNum != 27498438 {
		t.Errorf("latest block/slot lost: %d / %d", w.LatestBlockNum, w.LatestSlotNum)
	}
	if !strings.Contains(w.Url, "nansen") {
		t.Errorf("url lost: %q", w.Url)
	}
}

// TestUnmarshalTronJSON_TolerateUnknownFields ensures the converter doesn't
// fail when java-tron returns a field we haven't yet learned about — this is
// the forward-compat property that lets capture survive java-tron upgrades.
func TestUnmarshalTronJSON_TolerateUnknownFields(t *testing.T) {
	body := []byte(`{
        "address": "4170df4a99c7e3e249b290b43e11eb2368afd59899",
        "balance": 100,
        "thisFieldDoesNotExist": {"nested": "garbage"},
        "another_unknown": [1,2,3]
    }`)
	var got corepb.Account
	if err := unmarshalTronJSON(body, &got); err != nil {
		t.Fatalf("unknown fields must be tolerated: %v", err)
	}
	if got.Balance != 100 {
		t.Errorf("balance lost when unknowns present: %d", got.Balance)
	}
}
