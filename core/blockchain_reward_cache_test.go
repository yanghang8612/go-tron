package core

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestRewardAccountCachePrunesNonCurrentAddresses(t *testing.T) {
	statedb := newTestStateDB(t)
	staleAddr := tcommon.BytesToAddress([]byte{0x41, 0x01})
	currentAddr := tcommon.BytesToAddress([]byte{0x41, 0x02})
	staleAcc := statedb.CreateAccount(staleAddr, corepb.AccountType_Normal)
	currentAcc := statedb.CreateAccount(currentAddr, corepb.AccountType_Normal)
	currentAcc.SetAllowance(22)

	bc := &BlockChain{
		rewardAcctCache: map[tcommon.Address]*state.AccountSnapshot{
			staleAddr: {Account: staleAcc},
		},
		rewardAcctSeen:  make(map[tcommon.Address]struct{}),
		rewardAcctAddrs: make([]tcommon.Address, 0, 2),
	}

	addrs := bc.rewardAccountAddresses(currentAddr, nil)
	bc.updateRewardAccountCache(statedb, addrs)

	if _, ok := bc.rewardAcctCache[staleAddr]; ok {
		t.Fatal("stale reward account survived cache update")
	}
	if got := bc.rewardAcctCache[currentAddr]; got == nil || got.Account == nil || got.Account.Allowance() != 22 {
		t.Fatalf("current reward account cache: got %+v, want allowance 22", got)
	}
}
