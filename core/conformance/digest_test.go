package conformance

import (
	"encoding/json"
	"strings"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// newDigestFixture builds a minimal StateDB + DP + two addrs for digest tests.
func newDigestFixture(t *testing.T) (*state.StateDB, ethdb.KeyValueStore, *state.DynamicProperties, []tcommon.Address) {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb, err := state.New(tcommon.Hash(ethtypes.EmptyRootHash), state.NewDatabase(diskdb))
	if err != nil {
		t.Fatal(err)
	}
	dp := state.NewDynamicProperties()
	sdb.SetDynamicProperties(dp)
	dp.Set("energy_fee", 100)

	a1, _ := ParseAddress("41" + strings.Repeat("a", 40))
	a2, _ := ParseAddress("41" + strings.Repeat("b", 40))
	sdb.CreateAccount(a1, corepb.AccountType_Normal)
	sdb.AddBalance(a1, 1000)
	sdb.CreateAccount(a2, corepb.AccountType_Contract)
	sdb.AddBalance(a2, 2000)
	sdb.SetCode(a2, []byte{0x60, 0x01, 0x00})

	return sdb, diskdb, dp, []tcommon.Address{a1, a2}
}

func TestDigestB_Deterministic(t *testing.T) {
	sdb, db, dp, addrs := newDigestFixture(t)
	d1 := DigestB(sdb, db, addrs, dp)
	d2 := DigestB(sdb, db, addrs, dp)
	if d1 != d2 {
		t.Fatalf("digest not deterministic: %x vs %x", d1, d2)
	}
}

func TestDigestB_AddrOrderInvariant(t *testing.T) {
	sdb, db, dp, addrs := newDigestFixture(t)
	rev := []tcommon.Address{addrs[1], addrs[0]}
	if DigestB(sdb, db, addrs, dp) != DigestB(sdb, db, rev, dp) {
		t.Fatal("digest must be invariant to addrs order")
	}
}

func TestDigestB_DetectsBalanceChange(t *testing.T) {
	sdb, db, dp, addrs := newDigestFixture(t)
	d0 := DigestB(sdb, db, addrs, dp)
	sdb.AddBalance(addrs[0], 1)
	d1 := DigestB(sdb, db, addrs, dp)
	if d0 == d1 {
		t.Fatal("digest must change when balance changes")
	}
}

func TestDigestB_DetectsDPChange(t *testing.T) {
	sdb, db, dp, addrs := newDigestFixture(t)
	d0 := DigestB(sdb, db, addrs, dp)
	dp.Set("energy_fee", 200)
	d1 := DigestB(sdb, db, addrs, dp)
	if d0 == d1 {
		t.Fatal("digest must change when DP changes")
	}
}

func TestDigestB_DetectsContractStateChange(t *testing.T) {
	sdb, db, dp, addrs := newDigestFixture(t)
	d0 := DigestB(sdb, db, addrs, dp)

	cs := types.NewContractState(5)
	cs.SetEnergyFactor(1234)
	if err := sdb.WriteContractState(addrs[1], cs); err != nil {
		t.Fatal(err)
	}
	d1 := DigestB(sdb, db, addrs, dp)
	if d0 == d1 {
		t.Fatal("digest must change when ContractState changes")
	}
}

func TestDigestB_DetectsCodeChange(t *testing.T) {
	sdb, db, dp, addrs := newDigestFixture(t)
	d0 := DigestB(sdb, db, addrs, dp)
	sdb.SetCode(addrs[1], []byte{0xFF})
	d1 := DigestB(sdb, db, addrs, dp)
	if d0 == d1 {
		t.Fatal("digest must change when code changes")
	}
}

func TestDigestB_DetectsWitnessChange(t *testing.T) {
	sdb, db, dp, addrs := newDigestFixture(t)
	// Make addrs[0] a witness with initial counters.
	w := types.NewWitness(addrs[0], "http://example.com")
	if err := sdb.SetWitnessCapsule(w); err != nil {
		t.Fatal(err)
	}
	d0 := DigestB(sdb, db, addrs, dp)

	// Bump TotalProduced; digest must change.
	w.SetTotalProduced(w.TotalProduced() + 1)
	if err := sdb.SetWitnessCapsule(w); err != nil {
		t.Fatal(err)
	}
	d1 := DigestB(sdb, db, addrs, dp)
	if d0 == d1 {
		t.Fatal("digest must change when witness TotalProduced changes")
	}
}

func TestDigestB_DetectsDPStringChange(t *testing.T) {
	sdb, db, dp, addrs := newDigestFixture(t)
	d0 := DigestB(sdb, db, addrs, dp)
	dp.SetEnergyPriceHistory("0:100,1234567890:200")
	d1 := DigestB(sdb, db, addrs, dp)
	if d0 == d1 {
		t.Fatal("digest must change when DP string-typed value changes")
	}
}

func TestDigestB_DetectsBlockFilledSlotsChange(t *testing.T) {
	sdb, db, dp, addrs := newDigestFixture(t)
	d0 := DigestB(sdb, db, addrs, dp)
	// One application of the ring must produce a different digest.
	dp.ApplyBlockToFilledSlots(true)
	d1 := DigestB(sdb, db, addrs, dp)
	if d0 == d1 {
		t.Fatal("digest must change when block_filled_slots ring changes")
	}
}

func TestDigestC_WitnessAndDPStrings(t *testing.T) {
	sdb, db, dp, addrs := newDigestFixture(t)
	w := types.NewWitness(addrs[0], "http://example.com")
	w.SetTotalProduced(5)
	w.SetTotalMissed(1)
	w.SetLatestBlockNum(123)
	if err := sdb.SetWitnessCapsule(w); err != nil {
		t.Fatal(err)
	}

	raw := DigestC(sdb, db, addrs, dp)
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("not valid JSON: %v\n%s", err, raw)
	}
	dpStrings, ok := m["dpStrings"].(map[string]any)
	if !ok {
		t.Fatalf("dpStrings field missing or wrong type: %s", raw)
	}
	if _, ok := dpStrings["energy_price_history"]; !ok {
		t.Errorf("dpStrings missing energy_price_history: %s", raw)
	}
	if _, ok := dpStrings["block_filled_slots"]; !ok {
		t.Errorf("dpStrings missing block_filled_slots (hex-encoded): %s", raw)
	}

	accs, _ := m["accounts"].(map[string]any)
	a0Hex := strings.Repeat("a", 40) // matches the addr fixture format
	addrKey := "41" + a0Hex
	a0Entry, ok := accs[addrKey].(map[string]any)
	if !ok {
		t.Fatalf("addr[0] entry missing: %s", raw)
	}
	witnessEntry, ok := a0Entry["witness"].(map[string]any)
	if !ok {
		t.Fatalf("witness sub-entry missing for addr[0]: %s", raw)
	}
	if got := witnessEntry["totalProduced"].(float64); got != 5 {
		t.Errorf("witness.totalProduced: want 5, got %v", got)
	}
	if got := witnessEntry["latestBlockNum"].(float64); got != 123 {
		t.Errorf("witness.latestBlockNum: want 123, got %v", got)
	}
}

func TestDigestC_IsValidJSON(t *testing.T) {
	sdb, db, dp, addrs := newDigestFixture(t)
	raw := DigestC(sdb, db, addrs, dp)
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("not valid JSON: %v\n%s", err, raw)
	}
	accs, ok := m["accounts"].(map[string]any)
	if !ok {
		t.Fatalf("accounts field missing: %s", raw)
	}
	if len(accs) != 2 {
		t.Fatalf("want 2 accounts, got %d", len(accs))
	}
	if _, ok := m["dp"]; !ok {
		t.Fatalf("dp field missing: %s", raw)
	}
}
