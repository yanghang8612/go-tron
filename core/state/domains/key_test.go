package domains

import (
	"bytes"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestEncodeLatestKeyUsesAccountID(t *testing.T) {
	owner := testAddress(0x11)
	key := EncodeLatestKey(owner, 7, kvdomains.ContractStorage, []byte("slot"))
	if len(key) != LatestKeyHeaderLen+4 {
		t.Fatalf("latest key len = %d", len(key))
	}
	if key[0] == common.AddressPrefixMainnet {
		t.Fatal("latest key must use owner AccountID, not the 21-byte address prefix")
	}

	gotOwner, generation, domain, logical, ok := DecodeLatestKey(key)
	if !ok {
		t.Fatal("DecodeLatestKey failed")
	}
	if gotOwner != owner.AccountID() {
		t.Fatalf("owner = %x, want %x", gotOwner, owner.AccountID())
	}
	if generation != 7 {
		t.Fatalf("generation = %d, want 7", generation)
	}
	if domain != kvdomains.ContractStorage {
		t.Fatalf("domain = %x, want %x", uint16(domain), uint16(kvdomains.ContractStorage))
	}
	if !bytes.Equal(logical, []byte("slot")) {
		t.Fatalf("logical key = %q", logical)
	}
}

func TestDecodeLatestKeyRejectsShortInput(t *testing.T) {
	if _, _, _, _, ok := DecodeLatestKey([]byte("short")); ok {
		t.Fatal("short latest key decoded successfully")
	}
}

func testAddress(tail byte) common.Address {
	var addr common.Address
	addr[0] = common.AddressPrefixMainnet
	addr[20] = tail
	return addr
}
