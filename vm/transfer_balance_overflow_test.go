package vm

import (
	"errors"
	"math"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestTRXRecipientBalanceOverflowGatedByConstantinople(t *testing.T) {
	for _, tc := range []struct {
		name           string
		constantinople bool
		want           error
		wantMessage    string
		wantLeft       uint64
	}{
		{
			name:        "before-constantinople",
			want:        ErrValidateForSmartContract,
			wantMessage: "validateForSmartContract failure",
		},
		{
			name:           "after-constantinople",
			constantinople: true,
			want:           ErrTransferFailed,
			wantMessage:    "transfer trx failed: long overflow",
			wantLeft:       100_000,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tvm, sdb, _ := newTestTVMForCreate(t, TVMConfig{Constantinople: tc.constantinople}, nil)
			caller := tcommon.Address{0x41, 0x11}
			dest := tcommon.Address{0x41, 0x22}
			sdb.CreateAccount(caller, corepb.AccountType_Contract)
			sdb.CreateAccount(dest, corepb.AccountType_Normal)
			sdb.AddBalance(caller, 10)
			sdb.AddBalance(dest, math.MaxInt64)

			_, left, err := tvm.Call(caller, dest, nil, 100_000, 1)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Call error: got %v, want %v", err, tc.want)
			}
			if got := err.Error(); got != tc.wantMessage {
				t.Fatalf("runtime message: got %q, want %q", got, tc.wantMessage)
			}
			if left != tc.wantLeft {
				t.Fatalf("remaining energy: got %d, want %d", left, tc.wantLeft)
			}
			if got := sdb.GetBalance(caller); got != 10 {
				t.Fatalf("caller balance changed: got %d, want 10", got)
			}
			if got := sdb.GetBalance(dest); got != math.MaxInt64 {
				t.Fatalf("destination balance changed: got %d, want %d", got, int64(math.MaxInt64))
			}
		})
	}
}

func TestTRC10RecipientBalanceOverflowGatedByConstantinople(t *testing.T) {
	const tokenID = int64(1_000_002)
	for _, tc := range []struct {
		name           string
		constantinople bool
		want           error
		wantMessage    string
		wantLeft       uint64
	}{
		{
			name:        "before-constantinople",
			want:        ErrValidateForSmartContract,
			wantMessage: "validateForSmartContract failure",
		},
		{
			name:           "after-constantinople",
			constantinople: true,
			want:           ErrTokenTransferFailed,
			wantMessage:    "transfer trc10 failed: long overflow",
			wantLeft:       100_000,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tvm, sdb, _ := newTestTVMForCreate(t, TVMConfig{
				TransferTrc10:  true,
				Constantinople: tc.constantinople,
			}, nil)
			caller := tcommon.Address{0x41, 0x11}
			dest := tcommon.Address{0x41, 0x22}
			sdb.CreateAccount(caller, corepb.AccountType_Contract)
			sdb.CreateAccount(dest, corepb.AccountType_Normal)
			sdb.AddTRC10Balance(caller, tokenID, 10)
			sdb.AddTRC10Balance(dest, tokenID, math.MaxInt64)

			_, left, err := tvm.CallToken(caller, dest, nil, 100_000, 0, tokenID, 1)
			if !errors.Is(err, tc.want) {
				t.Fatalf("CallToken error: got %v, want %v", err, tc.want)
			}
			if got := err.Error(); got != tc.wantMessage {
				t.Fatalf("runtime message: got %q, want %q", got, tc.wantMessage)
			}
			if left != tc.wantLeft {
				t.Fatalf("remaining energy: got %d, want %d", left, tc.wantLeft)
			}
			if got := sdb.GetTRC10Balance(caller, tokenID); got != 10 {
				t.Fatalf("caller token balance changed: got %d, want 10", got)
			}
			if got := sdb.GetTRC10Balance(dest, tokenID); got != math.MaxInt64 {
				t.Fatalf("destination token balance changed: got %d, want %d", got, int64(math.MaxInt64))
			}
		})
	}
}
