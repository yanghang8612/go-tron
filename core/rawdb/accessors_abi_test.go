package rawdb

import (
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestContractABI_RoundTrip(t *testing.T) {
	db := memorydb.New()
	addr := make([]byte, 21)
	addr[0] = 0x41
	addr[20] = 0xAB

	if HasContractABI(db, addr) {
		t.Fatal("expected absent before write")
	}

	abi := &contractpb.SmartContract_ABI{
		Entrys: []*contractpb.SmartContract_ABI_Entry{
			{
				Type:   contractpb.SmartContract_ABI_Entry_Function,
				Name:   "transfer",
				Inputs: []*contractpb.SmartContract_ABI_Entry_Param{{Name: "to", Type: "address"}},
			},
		},
	}

	if err := WriteContractABI(db, addr, abi); err != nil {
		t.Fatalf("WriteContractABI: %v", err)
	}
	if !HasContractABI(db, addr) {
		t.Fatal("expected present after write")
	}

	got := ReadContractABI(db, addr)
	if got == nil {
		t.Fatal("ReadContractABI returned nil")
	}
	if len(got.Entrys) != 1 || got.Entrys[0].Name != "transfer" {
		t.Errorf("ABI mismatch: got %v", got)
	}
}

func TestContractABI_Absent(t *testing.T) {
	db := memorydb.New()
	addr := make([]byte, 21)
	if got := ReadContractABI(db, addr); got != nil {
		t.Fatalf("expected nil for absent key, got %v", got)
	}
}

func TestContractABI_Delete(t *testing.T) {
	db := memorydb.New()
	addr := make([]byte, 21)
	addr[20] = 0x01

	if err := WriteContractABI(db, addr, &contractpb.SmartContract_ABI{}); err != nil {
		t.Fatal(err)
	}
	if err := DeleteContractABI(db, addr); err != nil {
		t.Fatal(err)
	}
	if HasContractABI(db, addr) {
		t.Fatal("expected absent after delete")
	}
}

func TestContractABI_MultipleContracts(t *testing.T) {
	db := memorydb.New()
	addrs := [][]byte{{0x41, 0x01}, {0x41, 0x02}, {0x41, 0x03}}
	for i, addr := range addrs {
		abi := &contractpb.SmartContract_ABI{
			Entrys: []*contractpb.SmartContract_ABI_Entry{
				{Name: string([]byte{byte('a' + i)})},
			},
		}
		if err := WriteContractABI(db, addr, abi); err != nil {
			t.Fatalf("addr %d: %v", i, err)
		}
	}
	for i, addr := range addrs {
		got := ReadContractABI(db, addr)
		if got == nil {
			t.Fatalf("addr %d: nil ABI", i)
		}
		want := string([]byte{byte('a' + i)})
		if len(got.Entrys) != 1 || got.Entrys[0].Name != want {
			t.Errorf("addr %d: expected entry name %s, got %v", i, want, got.Entrys)
		}
	}
}
