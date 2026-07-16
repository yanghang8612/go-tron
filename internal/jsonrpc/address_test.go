package jsonrpc

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
)

func TestParseCompatibleAddress(t *testing.T) {
	body := "a0b0c0d0e0f000102030405060708090a0b0c0d0"
	want := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, mustDecodeHex(t, body)...))

	for _, input := range []string{"0x" + body, "0x41" + body} {
		got, err := parseCompatibleAddress(input)
		if err != nil {
			t.Fatalf("parseCompatibleAddress(%q): %v", input, err)
		}
		if got != want {
			t.Fatalf("parseCompatibleAddress(%q) = %x, want %x", input, got, want)
		}
	}
}

func TestParseCompatibleAddressJavaOddNibblePadding(t *testing.T) {
	input := "1" + strings.Repeat("23", 19) // 39 nibbles => leading zero => 20 bytes
	got, err := parseCompatibleAddress(input)
	if err != nil {
		t.Fatalf("parseCompatibleAddress: %v", err)
	}
	if got[0] != common.AddressPrefixMainnet || got[1] != 0x01 || got[2] != 0x23 {
		t.Fatalf("odd-nibble address padded incorrectly: %x", got)
	}
}

func TestParseCompatibleAddressRejectsInvalidInputs(t *testing.T) {
	for _, input := range []string{
		"0xzz",
		"0x" + strings.Repeat("11", 19),
		"0x42" + strings.Repeat("11", 20),
		"0x41" + strings.Repeat("11", 21),
	} {
		_, err := parseCompatibleAddress(input)
		if err == nil {
			t.Fatalf("parseCompatibleAddress(%q) unexpectedly succeeded", input)
		}
		coded, ok := err.(interface{ ErrorCode() int })
		if !ok || coded.ErrorCode() != codeInvalidParams {
			t.Fatalf("parseCompatibleAddress(%q) error = %T %v, want -32602", input, err, err)
		}
	}
}

func TestParseLogAddressUsesEthereumShapeOnly(t *testing.T) {
	body := strings.Repeat("12", common.AccountIDLength)
	got, err := parseLogAddress("0x" + body)
	if err != nil {
		t.Fatalf("parseLogAddress: %v", err)
	}
	if got[0] != common.AddressPrefixMainnet || got.AccountID() != common.BytesToAddress(mustDecodeHex(t, body)).AccountID() {
		t.Fatalf("parseLogAddress returned unexpected address: %x", got)
	}
	if _, err := parseLogAddress("0x41" + body); err == nil {
		t.Fatal("parseLogAddress accepted a 21-byte TRON address")
	}
}

func TestParseFilterAddressesReportsInvalidIndex(t *testing.T) {
	raw, _ := json.Marshal([]string{strings.Repeat("11", 20), "0x41" + strings.Repeat("22", 20)})
	_, err := parseFilterAddresses(raw)
	if err == nil || !strings.Contains(err.Error(), "index 1") {
		t.Fatalf("parseFilterAddresses error = %v, want invalid index 1", err)
	}
}

func mustDecodeHex(t *testing.T, input string) []byte {
	t.Helper()
	decoded, err := decodeAddressHex(input)
	if err != nil {
		t.Fatalf("decode %q: %v", input, err)
	}
	return decoded
}
