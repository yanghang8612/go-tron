package vm

import (
	"errors"
	"testing"

	"github.com/holiman/uint256"
	tcommon "github.com/tronprotocol/go-tron/common"
)

// java-tron CreateContractSuicideTest pins this behavior to ALLOW_MULTI_SIGN:
// before the proposal, a successful internal CREATE whose constructor returns
// empty runtime code crashes DepositImpl.commitCodeCache with a message-less
// NullPointerException (UNKNOWN); after the proposal, CREATE completes.
func TestInternalCreateEmptyCodeGatedByMultiSign(t *testing.T) {
	beneficiary := tcommon.Address{0x41, 0x77}
	initCode := append([]byte{byte(PUSH20)}, beneficiary[1:]...)
	initCode = append(initCode, byte(SELFDESTRUCT))

	for _, tc := range []struct {
		name    string
		cfg     TVMConfig
		wantErr bool
	}{
		{name: "before_allow_multi_sign", cfg: TVMConfig{}, wantErr: true},
		{name: "after_allow_multi_sign", cfg: TVMConfig{MultiSign: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tvm, sdb, _ := newTestTVMForCreate(t, tc.cfg, nil)
			parent := tcommon.Address{0x41, 0x11}
			sdb.CreateAccount(parent, 0)

			memory := newMemory()
			memory.set(0, uint64(len(initCode)), initCode)
			stack := newStack()
			stack.push(uint256.NewInt(uint64(len(initCode))))
			stack.push(uint256.NewInt(0))
			stack.push(uint256.NewInt(0))
			contract := NewContract(parent, parent, 0, 1_000_000)

			_, err := opCreate(nil, tvm.interpreter, contract, memory, stack)
			if tc.wantErr {
				if !errors.Is(err, ErrLegacyCreateEmptyCode) {
					t.Fatalf("opCreate error: got %v, want ErrLegacyCreateEmptyCode", err)
				}
				if stack.len() != 0 {
					t.Fatalf("CREATE wrapper exception must not push a result, stack len=%d", stack.len())
				}
				if got := err.Error(); got != "Unknown Exception" {
					t.Fatalf("runtime message: got %q, want %q", got, "Unknown Exception")
				}
				if len(tvm.InternalTransactions) != 2 {
					t.Fatalf("internal transactions: got %d, want create+suicide", len(tvm.InternalTransactions))
				}
				for i, it := range tvm.InternalTransactions {
					if !it.Rejected {
						t.Fatalf("internal transaction %d must be rejected", i)
					}
				}
				return
			}

			if err != nil {
				t.Fatalf("opCreate after ALLOW_MULTI_SIGN: %v", err)
			}
			if stack.len() != 1 {
				t.Fatalf("successful CREATE stack len: got %d, want 1", stack.len())
			}
			created := uint256ToAddress(stack.peek())
			if created == (tcommon.Address{}) {
				t.Fatal("successful CREATE must push the created address")
			}
			if !sdb.HasSelfDestructed(created) {
				t.Fatal("constructor SELFDESTRUCT marker missing")
			}
		})
	}
}
