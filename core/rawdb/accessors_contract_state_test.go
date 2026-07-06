package rawdb

import (
	"errors"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
)

func TestContractStateStrictRoundTripAndAbsent(t *testing.T) {
	db := memorydb.New()
	addr := tcommon.Address{0x41, 0x33}

	if got := ReadContractState(db, addr); got != nil {
		t.Fatalf("ReadContractState absent = %v, want nil", got)
	}
	if got, ok, err := ReadContractStateStrict(db, addr); got != nil || ok || err != nil {
		t.Fatalf("ReadContractStateStrict absent = %v/%v/%v, want nil/false/nil", got, ok, err)
	}

	cs := types.NewContractState(7)
	cs.SetEnergyFactor(300)
	cs.AddEnergyUsage(900)
	if err := WriteContractState(db, addr, cs); err != nil {
		t.Fatal(err)
	}
	got, ok, err := ReadContractStateStrict(db, addr)
	if err != nil || !ok || got == nil {
		t.Fatalf("ReadContractStateStrict = %v/%v/%v, want state/true/nil", got, ok, err)
	}
	if got.UpdateCycle() != 7 || got.EnergyFactor() != 300 || got.EnergyUsage() != 900 {
		t.Fatalf("contract state = cycle:%d factor:%d usage:%d", got.UpdateCycle(), got.EnergyFactor(), got.EnergyUsage())
	}
}

func TestContractStateStrictSurfacesStorageErrors(t *testing.T) {
	db := memorydb.New()
	addr := tcommon.Address{0x41, 0x34}
	cs := types.NewContractState(8)
	if err := WriteContractState(db, addr, cs); err != nil {
		t.Fatal(err)
	}

	if got, ok, err := ReadContractStateStrict(failingStateDomainReader{reader: db, hasErr: errors.New("has boom")}, addr); err == nil || ok || got != nil || !strings.Contains(err.Error(), "presence") {
		t.Fatalf("ReadContractStateStrict has error = %v/%v/%v, want presence error", got, ok, err)
	}
	if got, ok, err := ReadContractStateStrict(failingStateDomainReader{reader: db, getErr: errors.New("get boom")}, addr); err == nil || ok || got != nil || !strings.Contains(err.Error(), "get boom") {
		t.Fatalf("ReadContractStateStrict get error = %v/%v/%v, want get error", got, ok, err)
	}
	if ReadContractState(failingStateDomainReader{reader: db, getErr: errors.New("get boom")}, addr) != nil {
		t.Fatal("legacy ReadContractState should keep nil default on storage error")
	}
}

func TestContractStateStrictSurfacesCorruptPayload(t *testing.T) {
	db := memorydb.New()
	addr := tcommon.Address{0x41, 0x35}
	if err := db.Put(contractStateKey(addr.Bytes()), []byte{0xff}); err != nil {
		t.Fatal(err)
	}
	if got := ReadContractState(db, addr); got != nil {
		t.Fatalf("legacy ReadContractState corrupt payload = %v, want nil", got)
	}
	got, ok, err := ReadContractStateStrict(db, addr)
	if err == nil || !ok || got != nil || !strings.Contains(err.Error(), "decode contract state") {
		t.Fatalf("ReadContractStateStrict corrupt payload = %v/%v/%v, want decode error", got, ok, err)
	}
}
