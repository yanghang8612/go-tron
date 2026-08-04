package jsonrpc

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
)

func TestParseLogFilterJSONResolvesSharedFilterShape(t *testing.T) {
	raw := json.RawMessage(`{
		"fromBlock":"latest",
		"toBlock":"0x2",
		"address":[
			"0x1111111111111111111111111111111111111111",
			"0x2222222222222222222222222222222222222222"
		],
		"topics":[
			[
				"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
			],
			null,
			"0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
		]
	}`)

	filter, err := parseLogFilterJSON(raw, func() uint64 { return 123 })
	if err != nil {
		t.Fatalf("parseLogFilterJSON: %v", err)
	}
	if filter.FromBlock == nil || *filter.FromBlock != 123 {
		t.Fatalf("FromBlock = %v, want resolved latest 123", filter.FromBlock)
	}
	if filter.ToBlock == nil || *filter.ToBlock != 2 {
		t.Fatalf("ToBlock = %v, want 2", filter.ToBlock)
	}
	if filter.BlockHash != nil {
		t.Fatalf("BlockHash = %x, want nil", *filter.BlockHash)
	}
	wantAddress := func(body string) common.Address {
		var id common.AccountID
		copy(id[:], common.FromHex(body))
		return id.Address(common.AddressPrefixMainnet)
	}
	if len(filter.Addresses) != 2 ||
		filter.Addresses[0] != wantAddress("0x1111111111111111111111111111111111111111") ||
		filter.Addresses[1] != wantAddress("0x2222222222222222222222222222222222222222") {
		t.Fatalf("Addresses = %+v, want parsed address array", filter.Addresses)
	}
	if len(filter.Topics) != 3 ||
		len(filter.Topics[0]) != 2 ||
		filter.Topics[0][0] != common.BytesToHash(common.FromHex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")) ||
		filter.Topics[0][1] != common.BytesToHash(common.FromHex("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")) ||
		len(filter.Topics[1]) != 0 ||
		len(filter.Topics[2]) != 1 ||
		filter.Topics[2][0] != common.BytesToHash(common.FromHex("0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")) {
		t.Fatalf("Topics = %+v, want OR/wildcard/single topic shape", filter.Topics)
	}
}

func TestParseLogFilterJSONBlockHashOverridesRange(t *testing.T) {
	raw := json.RawMessage(`{
		"blockHash":"0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
		"fromBlock":"0x1",
		"toBlock":"latest"
	}`)

	filter, err := parseLogFilterJSON(raw, func() uint64 {
		t.Fatal("latest resolver should not run when blockHash is present")
		return 0
	})
	if err != nil {
		t.Fatalf("parseLogFilterJSON: %v", err)
	}
	want := common.BytesToHash(common.FromHex("0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"))
	if filter.BlockHash == nil || *filter.BlockHash != want {
		t.Fatalf("BlockHash = %v, want %x", filter.BlockHash, want)
	}
	if filter.FromBlock != nil || filter.ToBlock != nil {
		t.Fatalf("range = %v..%v, want nil range when blockHash is present", filter.FromBlock, filter.ToBlock)
	}
}

func TestParseLogFilterObjectKeepsLatestSentinelForSubscriptions(t *testing.T) {
	filter, err := parseLogFilterObject(json.RawMessage(`{"fromBlock":"latest","toBlock":"pending"}`))
	if err != nil {
		t.Fatalf("parseLogFilterObject: %v", err)
	}
	if filter.FromBlock == nil || *filter.FromBlock != ^uint64(0) {
		t.Fatalf("FromBlock = %v, want latest sentinel", filter.FromBlock)
	}
	if filter.ToBlock == nil || *filter.ToBlock != ^uint64(0) {
		t.Fatalf("ToBlock = %v, want pending/latest sentinel", filter.ToBlock)
	}
}

func TestParseLogFilterJSONWrapsBlockErrors(t *testing.T) {
	_, err := parseLogFilterJSON(json.RawMessage(`{"fromBlock":"nope"}`), func() uint64 { return 1 })
	if err == nil || !strings.Contains(err.Error(), "invalid fromBlock") {
		t.Fatalf("parseLogFilterJSON invalid fromBlock err = %v, want context", err)
	}
}

func TestParseLogFilterJSONRejectsInvalidAddresses(t *testing.T) {
	for _, raw := range []string{
		`{"address":"0x1234"}`,
		`{"address":"0x41aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`,
		`{"address":["0x1111111111111111111111111111111111111111","nope"]}`,
	} {
		if _, err := parseLogFilterJSON(json.RawMessage(raw), func() uint64 { return 1 }); err == nil {
			t.Fatalf("parseLogFilterJSON(%s) accepted invalid address", raw)
		}
	}
}
