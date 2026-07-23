package vm

import (
	"errors"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func overflowingMessageCallCode(op OpCode, target tcommon.Address) []byte {
	code := []byte{
		byte(PUSH1), 0x00, // out size
		byte(PUSH1), 0x00, // out offset
		byte(PUSH1), 0x00, // in size
		byte(PUSH1), 0x00, // in offset
		byte(PUSH32),
	}
	for i := 0; i < 32; i++ {
		code = append(code, 0xff)
	}
	code = append(code, byte(PUSH20))
	code = append(code, target[1:]...)
	code = append(code,
		byte(PUSH3), 0x0f, 0x42, 0x40, // one million message-call energy
		byte(op),
		byte(STOP),
	)
	return code
}

func runOverflowingMessageCall(t *testing.T, cfg TVMConfig, op OpCode, target tcommon.Address) (uint64, error) {
	t.Helper()
	tvm, sdb, _ := newTestTVMForCreate(t, cfg, nil)
	owner := tcommon.Address{0x41, 0x01}
	contractAddr := tcommon.Address{0x41, 0x02}
	sdb.CreateAccount(owner, corepb.AccountType_Normal)
	sdb.CreateAccount(contractAddr, corepb.AccountType_Contract)
	if getPrecompile(target, cfg, tvm.GenesisHash) == nil {
		sdb.CreateAccount(target, corepb.AccountType_Normal)
	}
	sdb.SetCode(contractAddr, overflowingMessageCallCode(op, target))

	_, left, err := tvm.Call(owner, contractAddr, nil, 100_000_000, 0)
	return left, err
}

// TestMainnet5780856EndowmentOutOfRangeBeforeConstantinople pins tx
// 0c746b0749b416e27314b8cbc9f243fead9969604288338a8051b94da7af3663.
// Casino.get() underflowed address(this).balance - 10 TRX into a uint256 that
// BigInteger.longValueExact could not represent. Before Constantinople the raw
// ArithmeticException escaped as UNKNOWN / "BigInteger out of long range" and
// consumed the full transaction energy.
func TestMainnet5780856EndowmentOutOfRangeBeforeConstantinople(t *testing.T) {
	target := tcommon.Address{0x41, 0x03}
	left, err := runOverflowingMessageCall(t, TVMConfig{}, CALL, target)
	if !errors.Is(err, ErrLegacyEndowmentOutOfRange) {
		t.Fatalf("Call error: got %v, want ErrLegacyEndowmentOutOfRange", err)
	}
	if got := err.Error(); got != "BigInteger out of long range" {
		t.Fatalf("runtime message: got %q, want %q", got, "BigInteger out of long range")
	}
	if left != 0 {
		t.Fatalf("remaining energy: got %d, want 0", left)
	}
}

func TestMessageCallEndowmentOutOfRangeProposalTransitions(t *testing.T) {
	target := tcommon.Address{0x41, 0x03}
	for _, op := range []OpCode{CALL, CALLCODE} {
		t.Run(op.String(), func(t *testing.T) {
			left, err := runOverflowingMessageCall(t, TVMConfig{Constantinople: true}, op, target)
			if !errors.Is(err, ErrEndowmentOutOfRange) {
				t.Fatalf("Call error: got %v, want ErrEndowmentOutOfRange", err)
			}
			if got := err.Error(); got != "endowment out of long range" {
				t.Fatalf("runtime message: got %q, want %q", got, "endowment out of long range")
			}
			if left == 0 {
				t.Fatal("Constantinople TransferException must preserve remaining energy")
			}
		})
	}
}

func TestPrecompileEndowmentOutOfRangeRemainsLegacyArithmeticError(t *testing.T) {
	precompile := addrFromUint(0x02)
	left, err := runOverflowingMessageCall(t, TVMConfig{Constantinople: true}, CALL, precompile)
	if !errors.Is(err, ErrLegacyEndowmentOutOfRange) {
		t.Fatalf("Call error: got %v, want ErrLegacyEndowmentOutOfRange", err)
	}
	if left != 0 {
		t.Fatalf("remaining energy: got %d, want 0", left)
	}
}

func TestCallTokenEndowmentOutOfRangeGatedByConstantinople(t *testing.T) {
	const tokenID = int64(1_000_002)
	target := tcommon.Address{0x41, 0x22}
	for _, tc := range []struct {
		name           string
		constantinople bool
		want           error
	}{
		{name: "before-constantinople", want: ErrLegacyEndowmentOutOfRange},
		{name: "after-constantinople", constantinople: true, want: ErrEndowmentOutOfRange},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tvm, sdb, _ := newTestTVMForCreate(t, TVMConfig{
				TransferTrc10:  true,
				Constantinople: tc.constantinople,
			}, nil)
			caller := tcommon.Address{0x41, 0x11}
			sdb.CreateAccount(caller, corepb.AccountType_Contract)
			sdb.CreateAccount(target, corepb.AccountType_Normal)
			sdb.AddTRC10Balance(caller, tokenID, 10)

			code := []byte{
				byte(PUSH1), 0x00, byte(PUSH1), 0x00,
				byte(PUSH1), 0x00, byte(PUSH1), 0x00,
				byte(PUSH3), 0x0f, 0x42, 0x42, // token ID
				byte(PUSH32),
			}
			for i := 0; i < 32; i++ {
				code = append(code, 0xff)
			}
			code = append(code, byte(PUSH20))
			code = append(code, target[1:]...)
			code = append(code, byte(PUSH2), 0x27, 0x10, byte(CALLTOKEN), byte(STOP))
			contract := NewContract(caller, caller, 0, 100_000)
			contract.SetCode(caller, code)

			if _, err := tvm.interpreter.Run(contract); !errors.Is(err, tc.want) {
				t.Fatalf("Run error: got %v, want %v", err, tc.want)
			}
		})
	}
}
