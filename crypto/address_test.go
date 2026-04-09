package crypto

import (
	"encoding/hex"
	"testing"

	"github.com/tronprotocol/go-tron/common"
)

func TestPubkeyToAddress(t *testing.T) {
	key, _ := GenerateKey()
	addr := PubkeyToAddress(&key.PublicKey)
	if len(addr) != common.AddressLength {
		t.Fatalf("expected %d bytes, got %d", common.AddressLength, len(addr))
	}
	if addr[0] != common.AddressPrefixMainnet {
		t.Fatalf("expected prefix 0x41, got 0x%x", addr[0])
	}
}

func TestAddressFromKnownKey(t *testing.T) {
	privHex := "da146374a75310b9666e834ee4ad0866d6f4035967bfc76217c5a495fff9f0d0"
	privBytes, _ := hex.DecodeString(privHex)
	key, err := BytesToPrivateKey(privBytes)
	if err != nil {
		t.Fatal(err)
	}
	addr := PubkeyToAddress(&key.PublicKey)
	if addr[0] != 0x41 {
		t.Fatalf("expected 0x41 prefix, got 0x%x", addr[0])
	}
	addr2 := PubkeyToAddress(&key.PublicKey)
	if addr != addr2 {
		t.Fatal("address derivation is not deterministic")
	}
}

func TestBase58CheckEncodeDecode(t *testing.T) {
	addrHex := "41a614f803b6fd780986a42c78ec9c7f77e6ded13c"
	addrBytes, _ := hex.DecodeString(addrHex)
	addr := common.BytesToAddress(addrBytes)
	encoded := AddressToBase58(addr)
	if encoded == "" {
		t.Fatal("base58 encoding returned empty")
	}
	decoded, err := Base58ToAddress(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != addr {
		t.Fatalf("round trip failed: got %s, want %s", decoded.Hex(), addr.Hex())
	}
}
