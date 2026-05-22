package actuator

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
)

func TestValidAddressBytesRejectsNonMainnetPrefix(t *testing.T) {
	mk := func(prefix byte) []byte {
		b := make([]byte, common.AddressLength)
		b[0] = prefix
		for i := 1; i < common.AddressLength; i++ {
			b[i] = byte(i)
		}
		return b
	}
	if !validAddressBytes(mk(common.AddressPrefixMainnet)) {
		t.Fatal("0x41 address must be valid")
	}
	// 0xa0 (legacy testnet) and any other prefix are rejected, so no two
	// distinct addresses can collapse onto one 20-byte AccountID trie key.
	for _, p := range []byte{0xa0, 0x00, 0x42} {
		if validAddressBytes(mk(p)) {
			t.Fatalf("prefix %#x must be rejected", p)
		}
	}
	if validAddressBytes(make([]byte, common.AddressLength-1)) {
		t.Fatal("wrong-length address must be rejected")
	}
}
