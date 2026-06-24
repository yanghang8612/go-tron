package tracers_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"github.com/tronprotocol/go-tron/vm"
	"github.com/tronprotocol/go-tron/vm/tracers"
)

func TestFileLoggerFromEnvNilWhenUnset(t *testing.T) {
	t.Setenv("GTRON_TVM_TRACE", "")
	if fl := tracers.FileLoggerFromEnv(); fl != nil {
		t.Fatal("FileLoggerFromEnv must return nil when GTRON_TVM_TRACE is unset")
	}
}

func TestFileLoggerWritesOpcodeTrace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trace.txt")
	t.Setenv("GTRON_TVM_TRACE", path)
	fl := tracers.FileLoggerFromEnv()
	if fl == nil {
		t.Fatal("FileLoggerFromEnv must return a logger when GTRON_TVM_TRACE is set")
	}

	diskdb := ethrawdb.NewMemoryDatabase()
	sdb, err := state.New(tcommon.Hash{}, state.NewDatabase(diskdb))
	if err != nil {
		t.Fatal(err)
	}
	owner := tcommon.Address{0x41, 0x01}
	addr := tcommon.Address{0x41, 0x02}
	sdb.CreateAccount(owner, corepb.AccountType_Normal)
	sdb.CreateAccount(addr, corepb.AccountType_Contract)
	sdb.SetContract(addr, &contractpb.SmartContract{ContractAddress: addr.Bytes()})
	sdb.SetCode(addr, []byte{byte(vm.PUSH1), 0x03, byte(vm.PUSH1), 0x04, byte(vm.ADD), byte(vm.STOP)})

	evm := vm.NewTVM(sdb, nil, owner, 1, 1000, tcommon.Address{}, 1, vm.TVMConfig{Tracer: fl})
	if _, _, err := evm.Call(owner, addr, nil, 1_000_000, 0); err != nil {
		t.Fatalf("call: %v", err)
	}
	if err := fl.Flush("unit-test-reason"); err != nil {
		t.Fatalf("flush: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read trace file: %v", err)
	}
	s := string(data)
	if !strings.Contains(s, "unit-test-reason") {
		t.Fatalf("trace file missing reason banner:\n%s", s)
	}
	if !strings.Contains(s, "ADD") {
		t.Fatalf("trace file missing ADD opcode:\n%s", s)
	}
	if !strings.Contains(s, "pc=") {
		t.Fatalf("trace file missing pc markers:\n%s", s)
	}
}
