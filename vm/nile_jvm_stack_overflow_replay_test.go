package vm

// Faithful runtime replay of Nile block 67,802,215 tx
// f53c6e98ddad99c24647a410ad89c02ff2485b6f3f69d1bb5498d9ae6f84d920.
// Contract A calls B (LOG1), then C with recursion count 63. At the bottom C
// executes legacy CREATE2 at Program.MAX_DEPTH; its constructor calls C again,
// crossing the Java equality-only depth guard and eventually raising the JVM's
// StackOverflowError. The canonical receipt is JVM_STACK_OVER_FLOW and spends
// the full 10,000,000-energy budget.

import (
	"encoding/hex"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func mustDecodeHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestNileLegacyCreate2JVMStackOverflowReplay(t *testing.T) {
	owner := hexAddr(t, "41ce96d2ad2a41146b98897287947dcfd723d100ea")
	a := hexAddr(t, "41ca113f15b47f921b68af664b31ae14c4b33b3cb7")
	b := hexAddr(t, "413bf29b1655aeee1898119fc0b92f5614ba802caf")
	c := hexAddr(t, "41fecd032d235b70954460d0fe660ffe3547871961")

	tvm := newTestEVMWithConfig(t, TVMConfig{Constantinople: true, Istanbul: true})
	tvm.BlockNumber = 67_802_215
	tvm.TrustTransactionRet = true
	tvm.ExpectedContractRet = corepb.Transaction_Result_JVM_STACK_OVER_FLOW
	tvm.RootTxID = tcommon.HexToHash("f53c6e98ddad99c24647a410ad89c02ff2485b6f3f69d1bb5498d9ae6f84d920")
	tvm.StateDB.CreateAccount(owner, corepb.AccountType_Normal)
	for addr, code := range map[[21]byte]string{
		a: "603f60005260006000600060006000733bf29b1655aeee1898119fc0b92f5614ba802caf5af1506000600060206000600073fecd032d235b70954460d0fe660ffe35478719615af100",
		b: "600160006000a100",
		c: "361560345760003580156022576001900360005260006000602060006000305af1005b601260436000396000601260006000f5005b60006000600060006000305af10060006000600060006000335af160006000f3",
	} {
		tvm.StateDB.CreateAccount(addr, corepb.AccountType_Contract)
		tvm.StateDB.SetCode(addr, mustDecodeHex(t, code))
	}

	const energyLimit = 10_000_000
	_, left, err := tvm.Call(owner, a, nil, energyLimit, 0)
	if err != ErrJVMStackOverflow {
		t.Fatalf("contractRet mismatch: got %v, want ErrJVMStackOverflow", err)
	}
	if left != 0 {
		t.Fatalf("energy mismatch: used %d, want full %d", energyLimit-left, energyLimit)
	}
	if got := len(tvm.InternalTransactions); got != 2 {
		t.Fatalf("internal transaction count: got %d, want canonical 2", got)
	}
	for i, it := range tvm.InternalTransactions {
		if !it.Rejected {
			t.Errorf("internal transaction %d not rejected", i)
		}
	}
	if got := len(tvm.Logs); got != 1 {
		t.Fatalf("log count: got %d, want canonical 1", got)
	}
}
