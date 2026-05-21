package core

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestRewardAccountCachePrunesNonCurrentAddresses(t *testing.T) {
	statedb := newTestStateDB(t)
	staleAddr := tcommon.BytesToAddress([]byte{0x41, 0x01})
	currentAddr := tcommon.BytesToAddress([]byte{0x41, 0x02})
	currentAcc := statedb.CreateAccount(currentAddr, corepb.AccountType_Normal)
	currentAcc.SetAllowance(22)

	bc := &BlockChain{
		rewardAcctCache: map[tcommon.Address]*types.Account{
			staleAddr: types.NewAccount(staleAddr, corepb.AccountType_Normal),
		},
		rewardAcctSeen:  make(map[tcommon.Address]struct{}),
		rewardAcctAddrs: make([]tcommon.Address, 0, 2),
	}

	addrs := bc.rewardAccountAddresses(currentAddr, nil)
	bc.updateRewardAccountCache(statedb, addrs)

	if _, ok := bc.rewardAcctCache[staleAddr]; ok {
		t.Fatal("stale reward account survived cache update")
	}
	if got := bc.rewardAcctCache[currentAddr]; got == nil || got.Allowance() != 22 {
		t.Fatalf("current reward account cache: got %+v, want allowance 22", got)
	}
}
