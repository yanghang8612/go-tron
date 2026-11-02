package core

import (
	"fmt"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// Consensus charging uses the official-genesis V1 shape. Generic APIs must
// still diagnose a noncanonical row after that cache has been materialized.
func TestChargeStakedNetConsensusFrozenShape(t *testing.T) {
	for _, post70 := range []bool{false, true} {
		t.Run(fmt.Sprint(post70), func(t *testing.T) {
			database := state.NewDatabase(ethrawdb.NewMemoryDatabase())
			t.Cleanup(func() { database.Close() })
			s, e := state.New(tcommon.Hash(ethtypes.EmptyRootHash), database)
			if e != nil {
				t.Fatal(e)
			}
			a := testProcessorAddr(0xc5)
			s.CreateAccount(a, corepb.AccountType_Normal)
			s.FreezeV1Bandwidth(a, 1_000_000, 200)
			s.AddFreezeV2(a, corepb.ResourceCode_BANDWIDTH, 1_000_000)
			if e := s.SetAccountKV(a, kvdomains.AccountFrozenBandwidthAux, []byte{0, 0, 0, 1}, []byte("malformed compatibility row")); e != nil {
				t.Fatal(e)
			}
			root, e := s.Commit()
			if e != nil {
				t.Fatal(e)
			}
			s, e = state.New(root, database)
			if e != nil {
				t.Fatal(e)
			}
			dp := state.NewDynamicProperties()
			dp.SetTotalNetWeight(2)
			if post70 {
				dp.Set("unfreeze_delay_days", 14)
			}
			var rec state.TransactionAccessRecorder
			rec.Reset(1)
			s.SetTransactionAccessRecorder(&rec)
			if !chargeStakedNet(s, dp, a, 100, 1) {
				t.Fatalf("consensus charge failed: %v", s.Error())
			}
			s.SetTransactionAccessRecorder(nil)
			if s.GetNetUsage(a) != 100 || s.GetLatestConsumeTime(a) != 1 {
				t.Fatal("settlement fields differ")
			}
			foundV2 := false
			for _, r := range rec.CaptureReadSet().Reads {
				if r.Key.Kind != state.TransactionAccessAccountKV || r.Key.Address != a {
					continue
				}
				if r.Key.KVDomain == kvdomains.AccountFrozenBandwidthAux && r.Key.LogicalKey == string([]byte{0, 0, 0, 1}) {
					t.Fatal("consensus charged compatibility row dependency")
				}
				if r.Key.KVDomain == kvdomains.AccountFrozenV2Aux {
					foundV2 = true
				}
			}
			if foundV2 != post70 {
				t.Fatalf("V2 dependency=%t post70=%t", foundV2, post70)
			}
			if _, e = s.GetAccountFrozenBandwidth(a); e == nil || s.Error() == nil {
				t.Fatal("generic upgrade hid malformed row1")
			}
		})
	}
}
