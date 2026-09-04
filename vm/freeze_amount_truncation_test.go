package vm

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/holiman/uint256"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// newFreezeV1TVM builds a pre-Stake-2.0 Freeze TVM (allow_tvm_freeze active,
// StakingV2 off) so the legacy FREEZE opcode actually mutates state (java
// OperationActions.freezeAction runs program.freeze when !allowTvmFreezeV2).
func newFreezeV1TVM(t *testing.T) (*TVM, *state.StateDB, *state.DynamicProperties) {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)
	statedb, err := state.New(tcommon.Hash{}, sdb)
	if err != nil {
		t.Fatal(err)
	}
	dp := state.NewDynamicProperties()
	dp.SetLatestBlockHeaderTimestamp(1_000_000)
	dp.SetTotalEnergyWeight(0)
	statedb.SetDynamicProperties(dp)
	tvm := NewTVM(statedb, dp, tcommon.Address{}, 1, 1_000_000, tcommon.Address{}, 1,
		TVMConfig{Freeze: true})
	tvm.SetDB(diskdb)
	return tvm, statedb, dp
}

func freezeAmtAddr(last byte) tcommon.Address {
	var a tcommon.Address
	a[0] = 0x41
	a[20] = last
	return a
}

// callFreeze drives opFreeze. Stack pop order is resourceType, amount, receiver,
// so push receiver, amount, resource (resource on top).
func callFreeze(t *testing.T, tvm *TVM, owner, receiver tcommon.Address, amount *uint256.Int, resource int64) uint64 {
	t.Helper()
	stack := newStack()
	recv := addressWord(receiver)
	stack.push(&recv)
	stack.push(amount)
	stack.push(uint256.NewInt(uint64(resource)))
	contract := NewContract(owner, owner, 0, 5_000_000)
	if _, err := opFreeze(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("opFreeze error: %v", err)
	}
	ret := stack.pop()
	return ret.Uint64()
}

func callUnfreezeV1(t *testing.T, tvm *TVM, owner, receiver tcommon.Address, resource int64) uint64 {
	t.Helper()
	stack := newStack()
	recv := addressWord(receiver)
	stack.push(&recv)
	stack.push(uint256.NewInt(uint64(resource)))
	contract := NewContract(owner, owner, 0, 5_000_000)
	if _, err := opUnfreeze(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("opUnfreeze error: %v", err)
	}
	result := stack.pop()
	return result.Uint64()
}

// UnfreezeBalanceProcessor validates self BANDWIDTH by row count and expiry,
// not by the frozen amount. Zero and malformed negative expired rows therefore
// still commit: Java removes them, applies the wrapping signed sum to balance
// and total weight, backfills that value into the internal transaction, and
// returns true.
func TestUnfreezeV1BandwidthNonPositiveExpiredRowsStillSucceed(t *testing.T) {
	for _, tc := range []struct {
		name          string
		frozen        int64
		wantBalance   int64
		wantWeight    int64
		wantStoredVal int64
	}{
		{name: "zero", frozen: 0, wantBalance: 10 * tvmTRXPrecision, wantWeight: 0, wantStoredVal: 0},
		{name: "negative", frozen: -3 * tvmTRXPrecision, wantBalance: 7 * tvmTRXPrecision, wantWeight: 3, wantStoredVal: -3 * tvmTRXPrecision},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tvm, statedb, dp := newFreezeV1TVM(t)
			root := featuredRoot()
			tvm.RootTxID = root
			owner := freezeAmtAddr(0x20)
			statedb.CreateAccount(owner, corepb.AccountType_Normal)
			statedb.AddBalance(owner, 10*tvmTRXPrecision)
			statedb.FreezeV1Bandwidth(owner, tc.frozen, dp.LatestBlockHeaderTimestamp()-1)

			if got := callUnfreezeV1(t, tvm, owner, owner, 0); got != 1 {
				t.Fatalf("unfreeze result: got %d, want 1", got)
			}
			if got := statedb.GetBalance(owner); got != tc.wantBalance {
				t.Fatalf("balance: got %d, want %d", got, tc.wantBalance)
			}
			if got := dp.TotalNetWeight(); got != tc.wantWeight {
				t.Fatalf("total net weight: got %d, want %d", got, tc.wantWeight)
			}
			if count, err := statedb.GetFreezeV1BandwidthCount(owner); err != nil || count != 0 {
				t.Fatalf("frozen row count: got %d, err=%v, want 0", count, err)
			}
			if len(tvm.InternalTransactions) != 1 {
				t.Fatalf("internal transactions: got %d, want 1", len(tvm.InternalTransactions))
			}
			assertFeaturedInternal(t, tvm.InternalTransactions[0], root, owner, owner[:],
				"unfreezeForBandwidth", 0, tc.wantStoredVal, 1, false, "")
		})
	}
}

// TestFreezeAmountTruncationPushesZero is the A gate for the legacy FREEZE amount.
// frozenBalance = 2^64 + 5*TRX has low-64-bits == 5*TRX, so the old
// int64(amountWord.Uint64()) truncated it to a VALID 5-TRX freeze and mutated
// state (SubBalance, FreezeV1, weight). java reads frozenBalance.sValue()
// .longValueExact() (Program.java:1935), which throws ArithmeticException for an
// out-of-int64 word -> the freeze is rejected (push 0) with zero state change.
// This case distinguishes truncation (freezes 5 TRX) from longValueExact (push 0).
func TestFreezeAmountTruncationPushesZero(t *testing.T) {
	tvm, statedb, dp := newFreezeV1TVM(t)
	owner := freezeAmtAddr(0x01)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	startBalance := int64(100) * tvmTRXPrecision
	statedb.AddBalance(owner, startBalance)
	weightBefore := dp.TotalEnergyWeight()

	// 2^64 + 5*TRX: low 64 bits == 5_000_000 (old truncation -> valid freeze),
	// bytesOccupied == 9 (java longValueExact throws -> reject).
	amount := new(uint256.Int).Add(pow2(64), uint256.NewInt(uint64(5*tvmTRXPrecision)))

	if got := callFreeze(t, tvm, owner, owner, amount, 1 /*ENERGY*/); got != 0 {
		t.Fatalf("freeze with out-of-int64 amount must push 0 (java longValueExact throws): got %d", got)
	}
	if got := statedb.GetBalance(owner); got != startBalance {
		t.Fatalf("balance mutated by rejected freeze: got %d, want %d", got, startBalance)
	}
	if got := dp.TotalEnergyWeight(); got != weightBefore {
		t.Fatalf("energy weight mutated by rejected freeze: got %d, want %d", got, weightBefore)
	}
}

// TestFreezeAmountNormalUnchanged guards the ordinary in-range freeze still works.
func TestFreezeAmountNormalUnchanged(t *testing.T) {
	tvm, statedb, dp := newFreezeV1TVM(t)
	owner := freezeAmtAddr(0x02)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.AddBalance(owner, 100*tvmTRXPrecision)

	if got := callFreeze(t, tvm, owner, owner, uint256.NewInt(uint64(10*tvmTRXPrecision)), 1); got != 1 {
		t.Fatalf("normal 10-TRX freeze: got %d, want 1", got)
	}
	if got := statedb.GetBalance(owner); got != 90*tvmTRXPrecision {
		t.Fatalf("balance after freeze: got %d, want %d", got, 90*tvmTRXPrecision)
	}
	if got := dp.TotalEnergyWeight(); got != 10 {
		t.Fatalf("energy weight after 10-TRX freeze: got %d, want 10", got)
	}
}

// FreezeBalanceProcessor uses AccountCapsule.setFrozenForEnergy: balance is
// accumulated, but expiry is replaced with this operation's calculated value
// even if an imported/older slot carries a later timestamp.
func TestFreezeV1EnergyOverwritesLaterExpiry(t *testing.T) {
	tvm, statedb, dp := newFreezeV1TVM(t)
	owner := freezeAmtAddr(0x0f)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.AddBalance(owner, 100*tvmTRXPrecision)
	wantExpiry := dp.LatestBlockHeaderTimestamp() + 3*86_400_000
	statedb.FreezeV1Energy(owner, 5*tvmTRXPrecision, wantExpiry+86_400_000)

	if got := callFreeze(t, tvm, owner, owner, uint256.NewInt(uint64(tvmTRXPrecision)), 1); got != 1 {
		t.Fatalf("freeze result: got %d, want 1", got)
	}
	account := statedb.GetAccount(owner)
	if got := account.FrozenEnergyAmount(); got != 6*tvmTRXPrecision {
		t.Fatalf("frozen energy: got %d, want %d", got, 6*tvmTRXPrecision)
	}
	if got := account.FrozenEnergyExpireTime(); got != wantExpiry {
		t.Fatalf("energy expiry: got %d, want %d", got, wantExpiry)
	}
}

// java FreezeBalanceProcessor rejects an owner whose legacy bandwidth Frozen
// repeated field has any cardinality other than zero or one, even when the
// requested resource is ENERGY. The old Go path skipped that validation and
// proceeded to debit balance and mutate stake state.
func TestFreezeV1RejectsMultipleBandwidthFrozenEntries(t *testing.T) {
	tvm, statedb, dp := newFreezeV1TVM(t)
	owner := freezeAmtAddr(0x0c)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	startBalance := int64(100) * tvmTRXPrecision
	statedb.AddBalance(owner, startBalance)

	// Seed the otherwise-impossible legacy/imported shape after materializing
	// the split V1 field, so the VM observes exactly two protobuf entries.
	statedb.FreezeV1Bandwidth(owner, tvmTRXPrecision, 11)
	account := statedb.GetAccount(owner)
	account.Proto().Frozen = append([]*corepb.Account_Frozen(nil), account.Proto().Frozen...)
	account.Proto().Frozen = append(account.Proto().Frozen, &corepb.Account_Frozen{
		FrozenBalance: 2 * tvmTRXPrecision,
		ExpireTime:    22,
	})
	wantFrozen := len(account.Proto().Frozen)
	weightBefore := dp.TotalEnergyWeight()

	if got := callFreeze(t, tvm, owner, owner, uint256.NewInt(uint64(tvmTRXPrecision)), 1 /* ENERGY */); got != 0 {
		t.Fatalf("freeze with two V1 bandwidth rows: got %d, want rejection 0", got)
	}
	if got := statedb.GetBalance(owner); got != startBalance {
		t.Fatalf("rejected freeze mutated balance: got %d, want %d", got, startBalance)
	}
	if got := len(statedb.GetAccount(owner).Proto().Frozen); got != wantFrozen {
		t.Fatalf("rejected freeze mutated frozen rows: got %d, want %d", got, wantFrozen)
	}
	if got := dp.TotalEnergyWeight(); got != weightBefore {
		t.Fatalf("rejected freeze mutated energy weight: got %d, want %d", got, weightBefore)
	}
	if got := tvm.InternalTransactions; len(got) != 1 || !got[0].Rejected {
		t.Fatalf("featured internal transaction = %+v, want one rejected record", got)
	}
}

func TestFreezeV1ContractReceiverRejectsWithoutBalanceTrace(t *testing.T) {
	tvm, statedb, _ := newFreezeV1TVM(t)
	owner := freezeAmtAddr(0x0d)
	receiver := freezeAmtAddr(0x0e)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.CreateAccount(receiver, corepb.AccountType_Contract)
	startBalance := int64(100) * tvmTRXPrecision
	statedb.AddBalance(owner, startBalance)

	txID := []byte{0xaa}
	statedb.BeginBalanceTrace(1, []byte{0xbb}, 1_000_000)
	statedb.BeginBalanceTraceTransaction(txID, "TriggerSmartContract")
	if got := callFreeze(t, tvm, owner, receiver, uint256.NewInt(uint64(tvmTRXPrecision)), 1); got != 0 {
		t.Fatalf("freeze to contract receiver: got %d, want rejection 0", got)
	}
	statedb.EndBalanceTraceTransaction("")

	if got := statedb.GetBalance(owner); got != startBalance {
		t.Fatalf("rejected freeze mutated balance: got %d, want %d", got, startBalance)
	}
	if trace := statedb.CopyLastBalanceTraceTransaction(txID); trace != nil {
		t.Fatalf("receiver validation leaked debit/refund trace: %+v", trace.Operation)
	}
}

func TestFreezeV1InsufficientBalanceDoesNotCreateReceiver(t *testing.T) {
	tvm, statedb, _ := newFreezeV1TVM(t)
	owner := freezeAmtAddr(0x0f)
	receiver := freezeAmtAddr(0x10)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.AddBalance(owner, tvmTRXPrecision-1)

	if got := callFreeze(t, tvm, owner, receiver, uint256.NewInt(uint64(tvmTRXPrecision)), 1); got != 0 {
		t.Fatalf("underfunded delegated freeze: got %d, want rejection 0", got)
	}
	if statedb.AccountExists(receiver) {
		t.Fatal("underfunded freeze created the delegated receiver before balance validation")
	}
}

// newFreezeV2AmountTVM builds a Stake-2.0 TVM for the FREEZEBALANCEV2 /
// UNFREEZEBALANCEV2 opcodes.
// TestFreezeBalanceV2RejectsHighByteResourceType pins the V2 resource-type parse
// to java Program.parseResourceCodeV2 (sValue().byteValueExact()): a word whose low
// 32 bits are 0/1/2 but whose HIGH bytes are set does NOT fit a signed byte, so java
// returns UNRECOGNIZED and the freeze is rejected (push 0, no state change). go
// previously truncated to int32(low-32) and wrongly committed a BANDWIDTH freeze.
func TestFreezeBalanceV2RejectsHighByteResourceType(t *testing.T) {
	tvm, statedb, _ := newFreezeV2AmountTVM(t)
	owner := stakeAddr(0x81)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.AddBalance(owner, 1000*tvmTRXPrecision)

	// Helper-level: 2^32 (low-32 = 0) must NOT decode to BANDWIDTH.
	bad := new(uint256.Int).Lsh(uint256.NewInt(1), 32)
	if validTVMStakeV2Resource(tvm, tvmResourceV2FromWord(bad)) {
		t.Fatal("tvmResourceV2FromWord accepted a high-byte word (should be UNRECOGNIZED)")
	}
	if got := tvmResourceV2FromWord(uint256.NewInt(2)); got != corepb.ResourceCode_TRON_POWER {
		t.Fatalf("clean type 2: got %v, want TRON_POWER", got)
	}

	// Opcode-level: freeze with the malformed resourceType word → push 0, no state change.
	stack := newStack()
	stack.push(uint256.NewInt(uint64(100 * tvmTRXPrecision))) // amount
	stack.push(bad)                                           // resourceType (popped first)
	contract := NewContract(owner, owner, 0, 5_000_000)
	if _, err := opFreezeBalanceV2(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("opFreezeBalanceV2: %v", err)
	}
	if ret := stack.pop(); ret.Uint64() != 0 {
		t.Fatalf("malformed resourceType: pushed %d, want 0 (byteValueExact rejects)", ret.Uint64())
	}
	if got := statedb.GetFrozenV2Amount(owner, corepb.ResourceCode_BANDWIDTH); got != 0 {
		t.Fatalf("freeze committed on rejected resourceType: frozen BW = %d, want 0", got)
	}
	if got := statedb.GetBalance(owner); got != 1000*tvmTRXPrecision {
		t.Fatalf("balance mutated on rejected resourceType: got %d, want %d", got, 1000*tvmTRXPrecision)
	}
	// Sanity: a clean BANDWIDTH(0) freeze still succeeds.
	if ret := callFreezeV2(t, tvm, owner, uint256.NewInt(uint64(100*tvmTRXPrecision)), corepb.ResourceCode_BANDWIDTH); ret != 1 {
		t.Fatalf("clean BANDWIDTH freeze: got %d, want 1", ret)
	}
}

func newFreezeV2AmountTVM(t *testing.T) (*TVM, *state.StateDB, *state.DynamicProperties) {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)
	statedb, err := state.New(tcommon.Hash{}, sdb)
	if err != nil {
		t.Fatal(err)
	}
	dp := state.NewDynamicProperties()
	dp.SetUnfreezeDelayDays(14)
	dp.SetLatestBlockHeaderTimestamp(1_000_000)
	statedb.SetDynamicProperties(dp)
	tvm := NewTVM(statedb, dp, tcommon.Address{}, 1, 1_000_000, tcommon.Address{}, 1,
		TVMConfig{StakingV2: true})
	tvm.SetDB(diskdb)
	return tvm, statedb, dp
}

func callFreezeV2(t *testing.T, tvm *TVM, owner tcommon.Address, amount *uint256.Int, resource corepb.ResourceCode) uint64 {
	t.Helper()
	stack := newStack()
	stack.push(amount)
	stack.push(uint256.NewInt(uint64(resource))) // resource on top: java pops resourceType first
	contract := NewContract(owner, owner, 0, 5_000_000)
	if _, err := opFreezeBalanceV2(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("opFreezeBalanceV2 error: %v", err)
	}
	ret := stack.pop()
	return ret.Uint64()
}

func callUnfreezeV2(t *testing.T, tvm *TVM, owner tcommon.Address, amount *uint256.Int, resource corepb.ResourceCode) uint64 {
	t.Helper()
	stack := newStack()
	stack.push(amount)
	stack.push(uint256.NewInt(uint64(resource))) // resource on top: java pops resourceType first
	contract := NewContract(owner, owner, 0, 5_000_000)
	if _, err := opUnfreezeBalanceV2(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("opUnfreezeBalanceV2 error: %v", err)
	}
	ret := stack.pop()
	return ret.Uint64()
}

// TestFreezeBalanceV2AmountTruncationPushesZero is the A gate for FREEZEBALANCEV2.
// amount = 2^64 + 7*TRX truncates (low 64 bits) to a valid 7-TRX freeze; java
// reads frozenBalance.sValue().longValueExact() (Program.java:2029) which throws
// -> push 0, no state change. Distinguishes truncation (freezes) from clamp.
func TestFreezeBalanceV2AmountTruncationPushesZero(t *testing.T) {
	tvm, statedb, dp := newFreezeV2AmountTVM(t)
	owner := freezeAmtAddr(0x03)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	startBalance := int64(100) * tvmTRXPrecision
	statedb.AddBalance(owner, startBalance)
	weightBefore := dp.TotalEnergyWeight()

	amount := new(uint256.Int).Add(pow2(64), uint256.NewInt(uint64(7*tvmTRXPrecision)))

	if got := callFreezeV2(t, tvm, owner, amount, corepb.ResourceCode_ENERGY); got != 0 {
		t.Fatalf("freezeV2 with out-of-int64 amount must push 0: got %d", got)
	}
	if got := statedb.GetBalance(owner); got != startBalance {
		t.Fatalf("balance mutated by rejected freezeV2: got %d, want %d", got, startBalance)
	}
	if got := statedb.GetFrozenV2Amount(owner, corepb.ResourceCode_ENERGY); got != 0 {
		t.Fatalf("frozenV2 created by rejected freezeV2: got %d", got)
	}
	if got := dp.TotalEnergyWeight(); got != weightBefore {
		t.Fatalf("energy weight mutated by rejected freezeV2: got %d", got)
	}
}

func TestFreezeBalanceV2InsufficientBalanceDoesNotInitializeOldTronPower(t *testing.T) {
	tvm, statedb, dp := newFreezeV2AmountTVM(t)
	dp.SetAllowNewResourceModel(true)
	owner := freezeAmtAddr(0x0d)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.AddBalance(owner, tvmTRXPrecision)
	// Give initialization an observable non-zero legacy snapshot. Java must not
	// execute it because validation fails on the requested balance first.
	statedb.FreezeV1Energy(owner, 5*tvmTRXPrecision, 2_000_000)

	if got := callFreezeV2(t, tvm, owner, uint256.NewInt(uint64(2*tvmTRXPrecision)), corepb.ResourceCode_ENERGY); got != 0 {
		t.Fatalf("insufficient-balance freezeV2: got %d, want 0", got)
	}
	account := statedb.GetAccount(owner)
	if got := account.OldTronPower(); got != 0 {
		t.Fatalf("rejected freezeV2 initialized old_tron_power: got %d, want 0", got)
	}
	if got := statedb.GetBalance(owner); got != tvmTRXPrecision {
		t.Fatalf("rejected freezeV2 mutated balance: got %d", got)
	}
	if got := statedb.GetFrozenV2Amount(owner, corepb.ResourceCode_ENERGY); got != 0 {
		t.Fatalf("rejected freezeV2 created V2 stake: got %d", got)
	}
	if got := tvm.InternalTransactions; len(got) != 1 || !got[0].Rejected {
		t.Fatalf("featured internal transaction = %+v, want one rejected record", got)
	}
}

// TestUnfreezeBalanceV2AmountTruncationPushesZero is the A gate for
// UNFREEZEBALANCEV2. With 50 TRX frozen, an unfreeze amount of 2^64 + 7*TRX
// truncates (low 64 bits) to a valid 7-TRX unfreeze; java reads
// unfreezeBalance.sValue().longValueExact() (Program.java:2059) which throws ->
// push 0, no state change.
func TestUnfreezeBalanceV2AmountTruncationPushesZero(t *testing.T) {
	tvm, statedb, _ := newFreezeV2AmountTVM(t)
	owner := freezeAmtAddr(0x04)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.AddFreezeV2(owner, corepb.ResourceCode_ENERGY, 50*tvmTRXPrecision)
	frozenBefore := statedb.GetFrozenV2Amount(owner, corepb.ResourceCode_ENERGY)
	balBefore := statedb.GetBalance(owner)

	amount := new(uint256.Int).Add(pow2(64), uint256.NewInt(uint64(7*tvmTRXPrecision)))

	if got := callUnfreezeV2(t, tvm, owner, amount, corepb.ResourceCode_ENERGY); got != 0 {
		t.Fatalf("unfreezeV2 with out-of-int64 amount must push 0: got %d", got)
	}
	if got := statedb.GetFrozenV2Amount(owner, corepb.ResourceCode_ENERGY); got != frozenBefore {
		t.Fatalf("frozenV2 mutated by rejected unfreezeV2: got %d, want %d", got, frozenBefore)
	}
	if got := statedb.GetBalance(owner); got != balBefore {
		t.Fatalf("balance mutated by rejected unfreezeV2: got %d, want %d", got, balBefore)
	}
	if got := statedb.UnfreezeV2Count(owner); got != 0 {
		t.Fatalf("unfreeze entry created by rejected unfreezeV2: got %d", got)
	}
}

// TestFreezeBalanceV2AmountNormalUnchanged guards the in-range freezeV2 path.
func TestFreezeBalanceV2AmountNormalUnchanged(t *testing.T) {
	tvm, statedb, dp := newFreezeV2AmountTVM(t)
	owner := freezeAmtAddr(0x05)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.AddBalance(owner, 100*tvmTRXPrecision)

	if got := callFreezeV2(t, tvm, owner, uint256.NewInt(uint64(20*tvmTRXPrecision)), corepb.ResourceCode_ENERGY); got != 1 {
		t.Fatalf("normal 20-TRX freezeV2: got %d, want 1", got)
	}
	if got := statedb.GetFrozenV2Amount(owner, corepb.ResourceCode_ENERGY); got != 20*tvmTRXPrecision {
		t.Fatalf("frozenV2 after 20-TRX freeze: got %d, want %d", got, 20*tvmTRXPrecision)
	}
	if got := dp.TotalEnergyWeight(); got != 20 {
		t.Fatalf("energy weight after 20-TRX freezeV2: got %d, want 20", got)
	}
}
