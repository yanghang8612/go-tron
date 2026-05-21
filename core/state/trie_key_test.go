package state

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/crypto"
	tcommon "github.com/tronprotocol/go-tron/common"
)

func TestTrieKeyUsesAccountID(t *testing.T) {
	var addr tcommon.Address
	addr[0] = tcommon.AddressPrefixMainnet
	for i := 1; i < tcommon.AddressLength; i++ {
		addr[i] = byte(i)
	}
	got := trieKey(addr)
	want := crypto.Keccak256(addr.AccountID().Bytes())
	if !bytes.Equal(got, want) {
		t.Fatalf("trieKey = %x, want Keccak256(AccountID) = %x", got, want)
	}
	old := crypto.Keccak256(addr.Bytes())
	if bytes.Equal(got, old) {
		t.Fatal("trieKey must no longer hash the 21-byte address")
	}
}

func TestTrieKeyIgnoresPrefix(t *testing.T) {
	var a, b tcommon.Address
	a[0], b[0] = tcommon.AddressPrefixMainnet, tcommon.AddressPrefixTestnet
	for i := 1; i < tcommon.AddressLength; i++ {
		a[i], b[i] = byte(i), byte(i)
	}
	if !bytes.Equal(trieKey(a), trieKey(b)) {
		t.Fatal("trieKey must depend only on the 20-byte AccountID")
	}
}
