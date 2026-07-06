package rawdb

import (
	"errors"
	"strings"
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
	if got, ok, err := ReadContractABIStrict(db, addr); got != nil || ok || err != nil {
		t.Fatalf("ReadContractABIStrict absent = %v/%v/%v, want nil/false/nil", got, ok, err)
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

func TestContractABIStrictSurfacesStorageErrors(t *testing.T) {
	db := memorydb.New()
	addr := make([]byte, 21)
	addr[0] = 0x41
	addr[20] = 0x44
	if err := WriteContractABI(db, addr, &contractpb.SmartContract_ABI{
		Entrys: []*contractpb.SmartContract_ABI_Entry{{Name: "transfer"}},
	}); err != nil {
		t.Fatal(err)
	}

	if got, ok, err := ReadContractABIStrict(failingStateDomainReader{reader: db, hasErr: errors.New("has boom")}, addr); err == nil || ok || got != nil || !strings.Contains(err.Error(), "presence") {
		t.Fatalf("ReadContractABIStrict has error = %v/%v/%v, want presence error", got, ok, err)
	}
	if got, ok, err := ReadContractABIStrict(failingStateDomainReader{reader: db, getErr: errors.New("get boom")}, addr); err == nil || ok || got != nil || !strings.Contains(err.Error(), "get boom") {
		t.Fatalf("ReadContractABIStrict get error = %v/%v/%v, want get error", got, ok, err)
	}
	if ReadContractABI(failingStateDomainReader{reader: db, getErr: errors.New("get boom")}, addr) != nil {
		t.Fatal("legacy ReadContractABI should keep nil default on storage error")
	}
}

func TestContractABIStrictSurfacesCorruptPayload(t *testing.T) {
	db := memorydb.New()
	addr := make([]byte, 21)
	addr[0] = 0x41
	addr[20] = 0x45
	if err := db.Put(abiKey(addr), []byte{0xff}); err != nil {
		t.Fatal(err)
	}
	if got := ReadContractABI(db, addr); got != nil {
		t.Fatalf("legacy ReadContractABI corrupt payload = %v, want nil", got)
	}
	got, ok, err := ReadContractABIStrict(db, addr)
	if err == nil || !ok || got != nil || !strings.Contains(err.Error(), "decode contract abi") {
		t.Fatalf("ReadContractABIStrict corrupt payload = %v/%v/%v, want decode error", got, ok, err)
	}
}
