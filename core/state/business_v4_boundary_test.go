package state

import (
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/statecodec"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"testing"
)

func TestV4GenericRewardWriteRejectsFullAccountBeforeCreatingOwner(t *testing.T) {
	for _, final := range []bool{false, true} {
		s := newTestStateDB(t)
		owner := common.SystemAccountAddress
		key := rawdb.CycleAccountVoteStateKey(3, testAddr(0x91).Bytes())
		old, err := statecodec.Marshal(&corepb.Account{Balance: 1})
		if err != nil {
			t.Fatal(err)
		}
		if final {
			err = s.SetAccountKVFinal(owner, kvdomains.SystemReward, key, old)
		} else {
			err = s.SetAccountKV(owner, kvdomains.SystemReward, key, old)
		}
		if err == nil || s.Exist(owner) {
			t.Fatalf("final=%v: accepted full Account or created owner: %v", final, err)
		}
	}
}
