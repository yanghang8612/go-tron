package common

import (
	"bytes"
	"testing"
)

func TestSystemAccountID(t *testing.T) {
	id := SystemAccountID
	for i := 0; i < AccountIDLength-1; i++ {
		if id[i] != 0xff {
			t.Fatalf("byte %d = %#x, want 0xff", i, id[i])
		}
	}
	if id[AccountIDLength-1] != 0xfe {
		t.Fatalf("last byte = %#x, want 0xfe", id[AccountIDLength-1])
	}
}

func TestSystemAccountAddress(t *testing.T) {
	addr := SystemAccountAddress
	if addr[0] != AddressPrefixMainnet {
		t.Fatalf("prefix = %#x, want 0x41", addr[0])
	}
	if !bytes.Equal(addr.AccountID().Bytes(), SystemAccountID.Bytes()) {
		t.Fatal("SystemAccountAddress.AccountID() must equal SystemAccountID")
	}
}

func TestIsSystemAccount(t *testing.T) {
	if !IsSystemAccount(SystemAccountAddress) {
		t.Fatal("SystemAccountAddress must be a system account")
	}
	var other Address
	other[0] = AddressPrefixMainnet
	other[1] = 0x11
	if IsSystemAccount(other) {
		t.Fatal("ordinary address must not be a system account")
	}
}
