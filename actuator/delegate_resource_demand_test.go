package actuator

import (
	tcommon "github.com/tronprotocol/go-tron/common"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestDelegateDemandReceiverTypePreservesValidationPosition(t *testing.T) {
	for _, kind := range []string{"normal", "contract", "deleted", "missing"} {
		t.Run(kind, func(t *testing.T) {
			owner, receiver := tcommon.Address{0x41, 0xd1}, tcommon.Address{0x41, 0xd2}
			c := &contractpb.DelegateResourceContract{OwnerAddress: owner.Bytes(), ReceiverAddress: receiver.Bytes(), Resource: corepb.ResourceCode_BANDWIDTH, Balance: 1_000_000}
			ctx := newTestContext(t, corepb.Transaction_Contract_DelegateResourceContract, c, 0)
			ctx.DynProps.SetAllowDelegateResource(true)
			ctx.DynProps.SetUnfreezeDelayDays(14)
			ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
			ctx.State.AddFreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 10_000_000)
			if kind != "missing" {
				typ := corepb.AccountType_Normal
				if kind == "contract" {
					typ = corepb.AccountType_Contract
				}
				ctx.State.CreateAccount(receiver, typ)
				// Narrow type reads intentionally omit unrelated corrupt auxiliaries.
				if err := ctx.State.SetAccountKV(receiver, kvdomains.AccountVotesAux, []byte{0, 0, 0, 0}, []byte{0xff}); err != nil {
					t.Fatal(err)
				}
				if kind == "deleted" {
					ctx.State.DeleteAccount(receiver)
				}
			}
			err := (&DelegateResourceActuator{}).Validate(ctx)
			switch kind {
			case "normal":
				if err != nil {
					t.Fatal(err)
				}
			case "contract":
				if err == nil || !strings.Contains(err.Error(), "contract addresses") {
					t.Fatal(err)
				}
			default:
				if err == nil || err.Error() != "receiver account does not exist" {
					t.Fatal(err)
				}
			}
			// Earlier validation failures continue to win over receiver type checks.
			c.Balance = 1
			bad := newTestContext(t, corepb.Transaction_Contract_DelegateResourceContract, c, 0)
			bad.State, bad.DynProps = ctx.State, ctx.DynProps
			if err := (&DelegateResourceActuator{}).Validate(bad); err == nil || !strings.Contains(err.Error(), "1 TRX") {
				t.Fatal(err)
			}
		})
	}
}
