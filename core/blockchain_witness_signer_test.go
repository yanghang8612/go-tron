package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// TestHeaderParentChainReader_WitnessPermissionSigner pins the state-read wiring
// that block signature validation uses under AllowMultiSign: the parent-state
// account's witness-permission first key (java-tron
// AccountCapsule.getWitnessPermissionAddress), with safe fallbacks to the
// witness address when the state or account is unavailable, or the witness has
// not delegated block signing. This is the read side of the Nile 45,490,765
// fix, where SR 417d6fd4 delegated block signing to 415624c1.
func TestHeaderParentChainReader_WitnessPermissionSigner(t *testing.T) {
	wAddr := tcommon.Address{0x41, 0x7d, 0x6f, 0xd4}
	deleg := tcommon.Address{0x41, 0x56, 0x24, 0xc1}

	// nil state ⇒ witness address (non-delegating fallback).
	if got := (headerParentChainReader{state: nil}).WitnessPermissionSigner(wAddr); got != wAddr {
		t.Fatalf("nil state: want %x, got %x", wAddr, got)
	}

	db := ethrawdb.NewMemoryDatabase()
	statedb, err := state.New(tcommon.Hash{}, state.NewDatabase(db))
	if err != nil {
		t.Fatalf("state.New: %v", err)
	}
	r := headerParentChainReader{state: statedb}

	// Account absent from state ⇒ witness address.
	if got := r.WitnessPermissionSigner(wAddr); got != wAddr {
		t.Fatalf("absent account: want %x, got %x", wAddr, got)
	}

	// Account present without a witness permission ⇒ witness address.
	statedb.CreateAccount(wAddr, corepb.AccountType_Normal)
	if got := r.WitnessPermissionSigner(wAddr); got != wAddr {
		t.Fatalf("no witness permission: want %x, got %x", wAddr, got)
	}

	// Account delegating block signing via its witness permission ⇒ deleg key.
	statedb.SetPermissions(wAddr, nil, &corepb.Permission{
		Type:      corepb.Permission_Witness,
		Threshold: 1,
		Keys:      []*corepb.Key{{Address: deleg.Bytes(), Weight: 1}},
	}, nil)
	if got := r.WitnessPermissionSigner(wAddr); got != deleg {
		t.Fatalf("delegated witness permission: want %x, got %x", deleg, got)
	}
}
