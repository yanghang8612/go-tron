package vm

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/holiman/uint256"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	troncrypto "github.com/tronprotocol/go-tron/crypto"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// These tests lock java-tron's Program.java featured-internal-transaction
// behavior. go-tron retains the complete ProgramResult list in TransactionInfo,
// so omitting native-op records makes otherwise-correct serial and speculative
// executions publish incomplete, non-java-compatible receipts.

func featuredRoot() tcommon.Hash {
	return tcommon.HexToHash("0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20")
}

func popFeaturedResult(stack *Stack) uint64 {
	result := stack.pop()
	return result.Uint64()
}

func assertFeaturedInternal(t *testing.T, tx *corepb.InternalTransaction, root tcommon.Hash,
	caller tcommon.Address, receiver []byte, note string, initialValue, storedValue int64,
	nonce uint64, rejected bool, extra string) {
	t.Helper()
	if tx == nil {
		t.Fatal("internal transaction is nil")
	}
	if got := string(tx.Note); got != note {
		t.Fatalf("note: got %q, want %q", got, note)
	}
	if !bytes.Equal(tx.CallerAddress, caller[:]) {
		t.Fatalf("caller: got %x, want %x", tx.CallerAddress, caller)
	}
	if !bytes.Equal(tx.TransferToAddress, receiver) {
		t.Fatalf("receiver: got %x, want %x", tx.TransferToAddress, receiver)
	}
	if len(tx.CallValueInfo) != 1 || tx.CallValueInfo[0] == nil {
		t.Fatalf("call values: got %+v, want one TRX value", tx.CallValueInfo)
	}
	if got := tx.CallValueInfo[0].CallValue; got != storedValue {
		t.Fatalf("stored value: got %d, want %d", got, storedValue)
	}
	if tx.CallValueInfo[0].TokenId != "" {
		t.Fatalf("featured TRX token id: got %q, want empty", tx.CallValueInfo[0].TokenId)
	}
	if tx.Rejected != rejected {
		t.Fatalf("rejected: got %v, want %v", tx.Rejected, rejected)
	}
	if tx.Extra != extra {
		t.Fatalf("extra: got %q, want %q", tx.Extra, extra)
	}
	wantHash := expectedInternalTxHash(root, receiver, nil, initialValue, nonce)
	if !bytes.Equal(tx.Hash, wantHash[:]) {
		t.Fatalf("hash: got %x, want %x (initial value %d, nonce %d)", tx.Hash, wantHash, initialValue, nonce)
	}
}

func TestFeaturedInternalLegacyFreezeAndUnfreeze(t *testing.T) {
	tvm, statedb, dp := newNonceTVM(t, TVMConfig{Freeze: true})
	root := featuredRoot()
	tvm.RootTxID = root
	owner := nonceAddr(0x31)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.AddBalance(owner, 100*tvmTRXPrecision)

	const amount = int64(10 * tvmTRXPrecision)
	if got := callFreeze(t, tvm, owner, owner, uint256.NewInt(uint64(amount)), 1); got != 1 {
		t.Fatalf("freeze result: got %d, want 1", got)
	}
	if len(tvm.InternalTransactions) != 1 {
		t.Fatalf("freeze internal transactions: got %d, want 1", len(tvm.InternalTransactions))
	}
	assertFeaturedInternal(t, tvm.InternalTransactions[0], root, owner, owner[:],
		"freezeForEnergy", amount, amount, 1, false, "")

	// The V1 freeze helper uses the Java default three-day duration.
	dp.SetLatestBlockHeaderTimestamp(1_000_000 + 3*86_400_000)
	stack := newStack()
	receiver := addressWord(owner)
	stack.push(&receiver)
	stack.push(uint256.NewInt(1))
	contract := NewContract(owner, owner, 0, 1_000_000)
	if _, err := opUnfreeze(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("opUnfreeze: %v", err)
	}
	if got := popFeaturedResult(stack); got != 1 {
		t.Fatalf("unfreeze result: got %d, want 1", got)
	}
	if len(tvm.InternalTransactions) != 2 {
		t.Fatalf("freeze+unfreeze internal transactions: got %d, want 2", len(tvm.InternalTransactions))
	}
	// Java hashes this record while its value is zero, then setValue(amount)
	// after successful execution without changing the cached hash.
	assertFeaturedInternal(t, tvm.InternalTransactions[1], root, owner, owner[:],
		"unfreezeForEnergy", 0, amount, 2, false, "")
}

func TestFeaturedInternalStakeV2Operations(t *testing.T) {
	t.Run("freeze", func(t *testing.T) {
		tvm, statedb, _ := newNonceTVM(t, TVMConfig{StakingV2: true})
		root := featuredRoot()
		tvm.RootTxID = root
		owner := nonceAddr(0x32)
		statedb.CreateAccount(owner, corepb.AccountType_Normal)
		statedb.AddBalance(owner, 100*tvmTRXPrecision)
		const amount = int64(12 * tvmTRXPrecision)
		if got := callFreezeV2(t, tvm, owner, uint256.NewInt(uint64(amount)), corepb.ResourceCode_ENERGY); got != 1 {
			t.Fatalf("freeze V2 result: got %d, want 1", got)
		}
		if len(tvm.InternalTransactions) != 1 {
			t.Fatalf("internal transactions: got %d, want 1", len(tvm.InternalTransactions))
		}
		assertFeaturedInternal(t, tvm.InternalTransactions[0], root, owner, owner[:],
			"freezeBalanceV2ForEnergy", amount, amount, 1, false, "")
	})

	t.Run("unfreeze_with_expired_withdrawal", func(t *testing.T) {
		tvm, statedb, dp := newNonceTVM(t, TVMConfig{StakingV2: true})
		root := featuredRoot()
		tvm.RootTxID = root
		owner := nonceAddr(0x33)
		statedb.CreateAccount(owner, corepb.AccountType_Normal)
		statedb.AddFreezeV2(owner, corepb.ResourceCode_ENERGY, 50*tvmTRXPrecision)
		const expired = int64(7 * tvmTRXPrecision)
		statedb.AddUnfreezeV2(owner, corepb.ResourceCode_ENERGY, expired, dp.LatestBlockHeaderTimestamp()-1)
		const amount = int64(10 * tvmTRXPrecision)
		if got := callUnfreezeV2(t, tvm, owner, uint256.NewInt(uint64(amount)), corepb.ResourceCode_ENERGY); got != 1 {
			t.Fatalf("unfreeze V2 result: got %d, want 1", got)
		}
		if len(tvm.InternalTransactions) != 2 {
			t.Fatalf("internal transactions: got %d, want main+expired withdrawal", len(tvm.InternalTransactions))
		}
		assertFeaturedInternal(t, tvm.InternalTransactions[0], root, owner, owner[:],
			"unfreezeBalanceV2ForEnergy", amount, amount, 1, false, "")
		assertFeaturedInternal(t, tvm.InternalTransactions[1], root, owner, owner[:],
			"withdrawExpireUnfreezeWhileUnfreezing", expired, expired, 2, false, "")
	})

	t.Run("withdraw_expired", func(t *testing.T) {
		tvm, statedb, dp := newNonceTVM(t, TVMConfig{StakingV2: true})
		root := featuredRoot()
		tvm.RootTxID = root
		owner := nonceAddr(0x34)
		statedb.CreateAccount(owner, corepb.AccountType_Normal)
		const expired = int64(8 * tvmTRXPrecision)
		statedb.AddUnfreezeV2(owner, corepb.ResourceCode_ENERGY, expired, dp.LatestBlockHeaderTimestamp())
		stack := newStack()
		contract := NewContract(owner, owner, 0, 1_000_000)
		if _, err := opWithdrawExpireUnfreeze(nil, tvm.interpreter, contract, nil, stack); err != nil {
			t.Fatalf("opWithdrawExpireUnfreeze: %v", err)
		}
		if got := popFeaturedResult(stack); got != uint64(expired) {
			t.Fatalf("withdraw result: got %d, want %d", got, expired)
		}
		assertFeaturedInternal(t, tvm.InternalTransactions[0], root, owner, owner[:],
			"withdrawExpireUnfreeze", 0, expired, 1, false, "")
	})

	t.Run("cancel_with_expired_withdrawal", func(t *testing.T) {
		tvm, statedb, dp := newNonceTVM(t, TVMConfig{StakingV2: true})
		root := featuredRoot()
		tvm.RootTxID = root
		owner := nonceAddr(0x35)
		statedb.CreateAccount(owner, corepb.AccountType_Normal)
		const expired = int64(9 * tvmTRXPrecision)
		statedb.AddUnfreezeV2(owner, corepb.ResourceCode_ENERGY, expired, dp.LatestBlockHeaderTimestamp())
		statedb.AddUnfreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 4*tvmTRXPrecision, dp.LatestBlockHeaderTimestamp()+1)
		stack := newStack()
		contract := NewContract(owner, owner, 0, 1_000_000)
		if _, err := opCancelAllUnfreezeV2(nil, tvm.interpreter, contract, nil, stack); err != nil {
			t.Fatalf("opCancelAllUnfreezeV2: %v", err)
		}
		if got := popFeaturedResult(stack); got != 1 {
			t.Fatalf("cancel result: got %d, want 1", got)
		}
		if len(tvm.InternalTransactions) != 2 {
			t.Fatalf("internal transactions: got %d, want cancel+expired withdrawal", len(tvm.InternalTransactions))
		}
		assertFeaturedInternal(t, tvm.InternalTransactions[0], root, owner, owner[:],
			"cancelAllUnfreezeV2", 0, 0, 1, false, `{"BANDWIDTH":4000000,"ENERGY":0,"TRON_POWER":0}`)
		assertFeaturedInternal(t, tvm.InternalTransactions[1], root, owner, owner[:],
			"withdrawExpireUnfreezeWhileCanceling", expired, expired, 2, false, "")
	})
}

func TestFeaturedInternalDelegateAndUndelegate(t *testing.T) {
	tvm, statedb, _ := newNonceTVM(t, TVMConfig{StakingV2: true})
	root := featuredRoot()
	tvm.RootTxID = root
	owner := nonceAddr(0x36)
	receiver := nonceAddr(0x37)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.CreateAccount(receiver, corepb.AccountType_Normal)
	statedb.AddFreezeV2(owner, corepb.ResourceCode_ENERGY, 100*tvmTRXPrecision)

	const delegated = int64(40 * tvmTRXPrecision)
	if got := callDelegateResource(t, tvm, owner, receiver, corepb.ResourceCode_ENERGY, delegated); got != 1 {
		t.Fatalf("delegate result: got %d, want 1", got)
	}
	assertFeaturedInternal(t, tvm.InternalTransactions[0], root, owner, receiver[:],
		"delegateResourceOfEnergy", delegated, delegated, 1, false, "")

	const undelegated = int64(15 * tvmTRXPrecision)
	if got := callUnDelegateResource(t, tvm, owner, receiver, corepb.ResourceCode_ENERGY, undelegated); got != 1 {
		t.Fatalf("undelegate result: got %d, want 1", got)
	}
	assertFeaturedInternal(t, tvm.InternalTransactions[1], root, owner, receiver[:],
		"unDelegateResourceOfEnergy", undelegated, undelegated, 2, false, "")
}

func TestFeaturedInternalVoteWitness(t *testing.T) {
	t.Run("success_preserves_input_order_and_zero_vote_in_extra", func(t *testing.T) {
		tvm, statedb, _ := newNonceTVM(t, TVMConfig{Vote: true, EnergyAdjustment: true})
		root := featuredRoot()
		tvm.RootTxID = root
		caller := nonceAddr(0x38)
		w1 := nonceAddr(0x39)
		w2 := nonceAddr(0x3a)
		statedb.CreateAccount(caller, corepb.AccountType_Normal)
		statedb.FreezeV1Bandwidth(caller, 100*tvmTRXPrecision, tvm.Timestamp+1)
		statedb.PutWitness(w1, "w1")
		statedb.PutWitness(w2, "w2")
		mem := newMemory()
		mem.set32(0, uint256.NewInt(2))
		witnessWordAt(mem, 32, w1)
		witnessWordAt(mem, 64, w2)
		mem.set32(96, uint256.NewInt(2))
		mem.set32(128, uint256.NewInt(4))
		mem.set32(160, uint256.NewInt(0))
		got, err := callVoteWitness(t, tvm, caller, mem,
			uint256.NewInt(0), uint256.NewInt(2), uint256.NewInt(96), uint256.NewInt(2), 1_000_000)
		if err != nil {
			t.Fatalf("voteWitness: %v", err)
		}
		if got != 1 {
			t.Fatalf("vote result: got %d, want 1", got)
		}
		extra := `{"votes":[{"vote_address":"` + troncrypto.AddressToBase58(w1) +
			`","vote_count":4},{"vote_address":"` + troncrypto.AddressToBase58(w2) +
			`","vote_count":0}]}`
		assertFeaturedInternal(t, tvm.InternalTransactions[0], root, caller, nil,
			"voteWitness", 0, 0, 1, false, extra)
	})

	t.Run("processor_failure_rejects_after_setting_extra", func(t *testing.T) {
		tvm, statedb, _ := newNonceTVM(t, TVMConfig{Vote: true, EnergyAdjustment: true})
		root := featuredRoot()
		tvm.RootTxID = root
		caller := nonceAddr(0x3b)
		witness := nonceAddr(0x3c)
		statedb.CreateAccount(caller, corepb.AccountType_Normal)
		statedb.PutWitness(witness, "w")
		mem := newMemory()
		mem.set32(0, uint256.NewInt(1))
		witnessWordAt(mem, 32, witness)
		mem.set32(64, uint256.NewInt(1))
		mem.set32(96, uint256.NewInt(4))
		got, err := callVoteWitness(t, tvm, caller, mem,
			uint256.NewInt(0), uint256.NewInt(1), uint256.NewInt(64), uint256.NewInt(1), 1_000_000)
		if err != nil {
			t.Fatalf("voteWitness: %v", err)
		}
		if got != 0 {
			t.Fatalf("vote result: got %d, want 0", got)
		}
		extra := `{"votes":[{"vote_address":"` + troncrypto.AddressToBase58(witness) + `","vote_count":4}]}`
		assertFeaturedInternal(t, tvm.InternalTransactions[0], root, caller, nil,
			"voteWitness", 0, 0, 1, true, extra)
	})

	t.Run("array_count_mismatch_is_java_unrejected_quirk", func(t *testing.T) {
		tvm, _, _ := newNonceTVM(t, TVMConfig{Vote: true, EnergyAdjustment: true})
		root := featuredRoot()
		tvm.RootTxID = root
		caller := nonceAddr(0x3d)
		mem := newMemory()
		mem.set32(0, uint256.NewInt(1))
		mem.set32(64, uint256.NewInt(0))
		got, err := callVoteWitness(t, tvm, caller, mem,
			uint256.NewInt(0), uint256.NewInt(1), uint256.NewInt(64), uint256.NewInt(0), 1_000_000)
		if err != nil {
			t.Fatalf("voteWitness: %v", err)
		}
		if got != 0 {
			t.Fatalf("vote result: got %d, want 0", got)
		}
		assertFeaturedInternal(t, tvm.InternalTransactions[0], root, caller, nil,
			"voteWitness", 0, 0, 1, false, "")
	})
}

func TestFeaturedInternalWithdrawReward(t *testing.T) {
	t.Run("success_backfills_value_after_hash", func(t *testing.T) {
		tvm, statedb, _ := newNonceTVM(t, TVMConfig{Vote: true})
		root := featuredRoot()
		tvm.RootTxID = root
		owner := nonceAddr(0x3e)
		statedb.CreateAccount(owner, corepb.AccountType_Normal)
		statedb.SetAllowance(owner, 50)
		stack := newStack()
		contract := NewContract(owner, owner, 0, 100_000)
		if _, err := opWithdrawReward(nil, tvm.interpreter, contract, nil, stack); err != nil {
			t.Fatalf("opWithdrawReward: %v", err)
		}
		if got := popFeaturedResult(stack); got != 50 {
			t.Fatalf("withdraw reward: got %d, want 50", got)
		}
		assertFeaturedInternal(t, tvm.InternalTransactions[0], root, owner, owner[:],
			"withdrawReward", 0, 50, 1, false, "")
	})

	t.Run("genesis_witness_failure_rejects", func(t *testing.T) {
		tvm, statedb, _ := newNonceTVM(t, TVMConfig{Vote: true})
		root := featuredRoot()
		tvm.RootTxID = root
		owner := nonceAddr(0x3f)
		statedb.CreateAccount(owner, corepb.AccountType_Normal)
		rawdb.WriteGenesisWitnesses(tvm.DB, []rawdb.GenesisWitness{{Address: owner, VoteCount: 1}})
		stack := newStack()
		contract := NewContract(owner, owner, 0, 100_000)
		if _, err := opWithdrawReward(nil, tvm.interpreter, contract, nil, stack); err != nil {
			t.Fatalf("opWithdrawReward: %v", err)
		}
		if got := popFeaturedResult(stack); got != 0 {
			t.Fatalf("withdraw reward: got %d, want 0", got)
		}
		assertFeaturedInternal(t, tvm.InternalTransactions[0], root, owner, owner[:],
			"withdrawReward", 0, 0, 1, true, "")
	})

	t.Run("nonpositive_allowance_returns_zero", func(t *testing.T) {
		tvm, statedb, _ := newNonceTVM(t, TVMConfig{Vote: true})
		root := featuredRoot()
		tvm.RootTxID = root
		owner := nonceAddr(0x52)
		statedb.CreateAccount(owner, corepb.AccountType_Normal)
		statedb.SetAllowance(owner, -7)
		stack := newStack()
		contract := NewContract(owner, owner, 0, 100_000)
		if _, err := opWithdrawReward(nil, tvm.interpreter, contract, nil, stack); err != nil {
			t.Fatalf("opWithdrawReward: %v", err)
		}
		if got := popFeaturedResult(stack); got != 0 {
			t.Fatalf("withdraw reward: got %d, want 0", got)
		}
		assertFeaturedInternal(t, tvm.InternalTransactions[0], root, owner, owner[:],
			"withdrawReward", 0, 0, 1, false, "")
	})

	t.Run("negative_allowance_balance_underflow_rejects", func(t *testing.T) {
		tvm, statedb, _ := newNonceTVM(t, TVMConfig{Vote: true})
		root := featuredRoot()
		tvm.RootTxID = root
		owner := nonceAddr(0x53)
		statedb.CreateAccount(owner, corepb.AccountType_Normal)
		statedb.AddBalance(owner, math.MinInt64)
		statedb.SetAllowance(owner, -1)
		stack := newStack()
		contract := NewContract(owner, owner, 0, 100_000)
		if _, err := opWithdrawReward(nil, tvm.interpreter, contract, nil, stack); err != nil {
			t.Fatalf("opWithdrawReward: %v", err)
		}
		if got := popFeaturedResult(stack); got != 0 {
			t.Fatalf("withdraw reward: got %d, want 0", got)
		}
		assertFeaturedInternal(t, tvm.InternalTransactions[0], root, owner, owner[:],
			"withdrawReward", 0, 0, 1, true, "")
		if got := statedb.GetBalance(owner); got != math.MinInt64 {
			t.Fatalf("balance: got %d, want %d", got, int64(math.MinInt64))
		}
		if got := statedb.GetAllowance(owner); got != -1 {
			t.Fatalf("allowance: got %d, want -1", got)
		}
	})
}

func TestFeaturedInternalValidationFailuresReject(t *testing.T) {
	t.Run("legacy_freeze_exact_range", func(t *testing.T) {
		tvm, statedb, _ := newNonceTVM(t, TVMConfig{Freeze: true})
		root := featuredRoot()
		tvm.RootTxID = root
		owner := nonceAddr(0x40)
		statedb.CreateAccount(owner, corepb.AccountType_Normal)
		statedb.AddBalance(owner, 100*tvmTRXPrecision)
		amount := new(uint256.Int).Add(pow2(64), uint256.NewInt(uint64(5*tvmTRXPrecision)))
		if got := callFreeze(t, tvm, owner, owner, amount, 1); got != 0 {
			t.Fatalf("freeze result: got %d, want 0", got)
		}
		// InternalTransaction is created with DataWord.longValue (low signed
		// 64 bits) before longValueExact rejects the actual 256-bit amount.
		assertFeaturedInternal(t, tvm.InternalTransactions[0], root, owner, owner[:],
			"freezeForEnergy", 5*tvmTRXPrecision, 5*tvmTRXPrecision, 1, true, "")
	})

	t.Run("legacy_unfreeze_missing", func(t *testing.T) {
		tvm, statedb, _ := newNonceTVM(t, TVMConfig{Freeze: true})
		root := featuredRoot()
		tvm.RootTxID = root
		owner := nonceAddr(0x41)
		statedb.CreateAccount(owner, corepb.AccountType_Normal)
		stack := newStack()
		receiver := addressWord(owner)
		stack.push(&receiver)
		stack.push(uint256.NewInt(1))
		contract := NewContract(owner, owner, 0, 1_000_000)
		if _, err := opUnfreeze(nil, tvm.interpreter, contract, nil, stack); err != nil {
			t.Fatalf("opUnfreeze: %v", err)
		}
		if got := popFeaturedResult(stack); got != 0 {
			t.Fatalf("unfreeze result: got %d, want 0", got)
		}
		assertFeaturedInternal(t, tvm.InternalTransactions[0], root, owner, owner[:],
			"unfreezeForEnergy", 0, 0, 1, true, "")
	})

	t.Run("freeze_v2_invalid_resource", func(t *testing.T) {
		tvm, statedb, _ := newNonceTVM(t, TVMConfig{StakingV2: true})
		root := featuredRoot()
		tvm.RootTxID = root
		owner := nonceAddr(0x42)
		statedb.CreateAccount(owner, corepb.AccountType_Normal)
		statedb.AddBalance(owner, 100*tvmTRXPrecision)
		if got := callFreezeV2(t, tvm, owner, uint256.NewInt(uint64(5*tvmTRXPrecision)), corepb.ResourceCode(3)); got != 0 {
			t.Fatalf("freeze V2 result: got %d, want 0", got)
		}
		assertFeaturedInternal(t, tvm.InternalTransactions[0], root, owner, owner[:],
			"freezeBalanceV2ForUnknownType", 5*tvmTRXPrecision, 5*tvmTRXPrecision, 1, true, "")
	})

	t.Run("unfreeze_v2_insufficient_frozen", func(t *testing.T) {
		tvm, statedb, _ := newNonceTVM(t, TVMConfig{StakingV2: true})
		root := featuredRoot()
		tvm.RootTxID = root
		owner := nonceAddr(0x43)
		statedb.CreateAccount(owner, corepb.AccountType_Normal)
		const amount = int64(5 * tvmTRXPrecision)
		if got := callUnfreezeV2(t, tvm, owner, uint256.NewInt(uint64(amount)), corepb.ResourceCode_ENERGY); got != 0 {
			t.Fatalf("unfreeze V2 result: got %d, want 0", got)
		}
		assertFeaturedInternal(t, tvm.InternalTransactions[0], root, owner, owner[:],
			"unfreezeBalanceV2ForEnergy", amount, amount, 1, true, "")
	})

	t.Run("withdraw_expired_balance_overflow", func(t *testing.T) {
		tvm, statedb, dp := newNonceTVM(t, TVMConfig{StakingV2: true})
		root := featuredRoot()
		tvm.RootTxID = root
		owner := nonceAddr(0x4b)
		statedb.CreateAccount(owner, corepb.AccountType_Normal)
		statedb.AddBalance(owner, math.MaxInt64-5)
		statedb.AddUnfreezeV2(owner, corepb.ResourceCode_ENERGY, 10, dp.LatestBlockHeaderTimestamp())
		stack := newStack()
		contract := NewContract(owner, owner, 0, 1_000_000)
		if _, err := opWithdrawExpireUnfreeze(nil, tvm.interpreter, contract, nil, stack); err != nil {
			t.Fatalf("opWithdrawExpireUnfreeze: %v", err)
		}
		if got := popFeaturedResult(stack); got != 0 {
			t.Fatalf("withdraw result: got %d, want 0", got)
		}
		if got := statedb.GetBalance(owner); got != math.MaxInt64-5 {
			t.Fatalf("overflow failure changed balance: got %d", got)
		}
		if got := statedb.UnfreezeV2Count(owner); got != 1 {
			t.Fatalf("overflow failure removed queue entries: got %d, want 1", got)
		}
		assertFeaturedInternal(t, tvm.InternalTransactions[0], root, owner, owner[:],
			"withdrawExpireUnfreeze", 0, 0, 1, true, "")
	})

	t.Run("delegate_missing_receiver", func(t *testing.T) {
		tvm, statedb, _ := newNonceTVM(t, TVMConfig{StakingV2: true})
		root := featuredRoot()
		tvm.RootTxID = root
		owner := nonceAddr(0x44)
		receiver := nonceAddr(0x45)
		statedb.CreateAccount(owner, corepb.AccountType_Normal)
		statedb.AddFreezeV2(owner, corepb.ResourceCode_ENERGY, 100*tvmTRXPrecision)
		const amount = int64(5 * tvmTRXPrecision)
		if got := callDelegateResource(t, tvm, owner, receiver, corepb.ResourceCode_ENERGY, amount); got != 0 {
			t.Fatalf("delegate result: got %d, want 0", got)
		}
		assertFeaturedInternal(t, tvm.InternalTransactions[0], root, owner, receiver[:],
			"delegateResourceOfEnergy", amount, amount, 1, true, "")
	})

	t.Run("delegate_feature_disabled", func(t *testing.T) {
		tvm, statedb, dp := newNonceTVM(t, TVMConfig{StakingV2: true})
		root := featuredRoot()
		tvm.RootTxID = root
		dp.SetAllowDelegateResource(false)
		owner := nonceAddr(0x4c)
		receiver := nonceAddr(0x4d)
		statedb.CreateAccount(owner, corepb.AccountType_Normal)
		statedb.CreateAccount(receiver, corepb.AccountType_Normal)
		statedb.AddFreezeV2(owner, corepb.ResourceCode_ENERGY, 100*tvmTRXPrecision)
		const amount = int64(5 * tvmTRXPrecision)
		if got := callDelegateResource(t, tvm, owner, receiver, corepb.ResourceCode_ENERGY, amount); got != 0 {
			t.Fatalf("delegate result: got %d, want 0", got)
		}
		assertFeaturedInternal(t, tvm.InternalTransactions[0], root, owner, receiver[:],
			"delegateResourceOfEnergy", amount, amount, 1, true, "")
	})

	t.Run("undelegate_missing_pair", func(t *testing.T) {
		tvm, statedb, _ := newNonceTVM(t, TVMConfig{StakingV2: true})
		root := featuredRoot()
		tvm.RootTxID = root
		owner := nonceAddr(0x46)
		receiver := nonceAddr(0x47)
		statedb.CreateAccount(owner, corepb.AccountType_Normal)
		statedb.CreateAccount(receiver, corepb.AccountType_Normal)
		const amount = int64(5 * tvmTRXPrecision)
		if got := callUnDelegateResource(t, tvm, owner, receiver, corepb.ResourceCode_ENERGY, amount); got != 0 {
			t.Fatalf("undelegate result: got %d, want 0", got)
		}
		assertFeaturedInternal(t, tvm.InternalTransactions[0], root, owner, receiver[:],
			"unDelegateResourceOfEnergy", amount, amount, 1, true, "")
	})
}

func TestWithdrawExpireUnfreezeZeroTotalPreservesExpiredRows(t *testing.T) {
	tvm, statedb, dp := newNonceTVM(t, TVMConfig{StakingV2: true})
	owner := nonceAddr(0x5d)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.AddUnfreezeV2(owner, corepb.ResourceCode_ENERGY, 0, dp.LatestBlockHeaderTimestamp())

	stack := newStack()
	contract := NewContract(owner, owner, 0, 1_000_000)
	if _, err := opWithdrawExpireUnfreeze(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("opWithdrawExpireUnfreeze: %v", err)
	}
	if got := popFeaturedResult(stack); got != 0 {
		t.Fatalf("withdraw result: got %d, want 0", got)
	}
	if got := statedb.UnfreezeV2Count(owner); got != 1 {
		t.Fatalf("zero-total expired queue count: got %d, want retained 1", got)
	}
	if got := tvm.InternalTransactions; len(got) != 1 || got[0].Rejected || got[0].CallValueInfo[0].CallValue != 0 {
		t.Fatalf("featured internal transaction = %+v, want one accepted zero-value record", got)
	}
}

func TestFeaturedInternalFreezeGatesBeforeRecord(t *testing.T) {
	t.Run("stake_v2_disables_legacy_program_call", func(t *testing.T) {
		tvm, statedb, _ := newNonceTVM(t, TVMConfig{Freeze: true, StakingV2: true})
		owner := nonceAddr(0x48)
		statedb.CreateAccount(owner, corepb.AccountType_Normal)
		statedb.AddBalance(owner, 100*tvmTRXPrecision)
		if got := callFreeze(t, tvm, owner, owner, uint256.NewInt(uint64(5*tvmTRXPrecision)), 1); got != 0 {
			t.Fatalf("freeze result: got %d, want 0", got)
		}
		if tvm.Nonce != 0 || len(tvm.InternalTransactions) != 0 {
			t.Fatalf("Stake-V2-gated FREEZE entered Program.freeze: nonce=%d internal=%d", tvm.Nonce, len(tvm.InternalTransactions))
		}
	})

	t.Run("out_of_energy_precedes_program_call", func(t *testing.T) {
		tvm, statedb, _ := newNonceTVM(t, TVMConfig{Freeze: true})
		owner := nonceAddr(0x49)
		receiver := nonceAddr(0x4a) // deliberately absent: NEW_ACCT_CALL surcharge
		statedb.CreateAccount(owner, corepb.AccountType_Normal)
		statedb.AddBalance(owner, 100*tvmTRXPrecision)
		stack := newStack()
		receiverWord := addressWord(receiver)
		stack.push(&receiverWord)
		stack.push(uint256.NewInt(uint64(5 * tvmTRXPrecision)))
		stack.push(uint256.NewInt(1))
		contract := NewContract(owner, owner, 0, 1)
		if _, err := opFreeze(nil, tvm.interpreter, contract, nil, stack); err != ErrOutOfEnergy {
			t.Fatalf("opFreeze error: got %v, want ErrOutOfEnergy", err)
		}
		if tvm.Nonce != 0 || len(tvm.InternalTransactions) != 0 {
			t.Fatalf("energy failure entered Program.freeze: nonce=%d internal=%d", tvm.Nonce, len(tvm.InternalTransactions))
		}
	})
}

func TestFeaturedInternalRuntimeFrameCommitAndRevert(t *testing.T) {
	const amount = int64(6 * tvmTRXPrecision)
	freezeCode := func(revert bool) []byte {
		code := []byte{byte(PUSH8)}
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], uint64(amount))
		code = append(code, encoded[:]...)
		code = append(code,
			byte(PUSH1), byte(corepb.ResourceCode_ENERGY),
			byte(FREEZEBALANCEV2),
		)
		if revert {
			code = append(code, byte(PUSH1), 0, byte(PUSH1), 0, byte(REVERT))
		} else {
			code = append(code, byte(STOP))
		}
		return code
	}

	for _, tc := range []struct {
		name     string
		revert   bool
		wantErr  error
		rejected bool
	}{
		{name: "commit"},
		{name: "outer_revert", revert: true, wantErr: ErrExecutionReverted, rejected: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tvm, statedb, _ := newNonceTVM(t, TVMConfig{StakingV2: true})
			root := featuredRoot()
			tvm.RootTxID = root
			caller := nonceAddr(0x50)
			contractAddr := nonceAddr(0x51)
			statedb.CreateAccount(caller, corepb.AccountType_Normal)
			statedb.CreateAccount(contractAddr, corepb.AccountType_Contract)
			statedb.AddBalance(contractAddr, 20*tvmTRXPrecision)
			statedb.SetCode(contractAddr, freezeCode(tc.revert))

			_, _, err := tvm.Call(caller, contractAddr, nil, 2_000_000, 0)
			if err != tc.wantErr {
				t.Fatalf("Call error: got %v, want %v", err, tc.wantErr)
			}
			if len(tvm.InternalTransactions) != 1 {
				t.Fatalf("internal transactions: got %d, want 1", len(tvm.InternalTransactions))
			}
			assertFeaturedInternal(t, tvm.InternalTransactions[0], root, contractAddr, contractAddr[:],
				"freezeBalanceV2ForEnergy", amount, amount, 1, tc.rejected, "")
			if tc.revert {
				if got := statedb.GetBalance(contractAddr); got != 20*tvmTRXPrecision {
					t.Fatalf("reverted balance: got %d, want %d", got, 20*tvmTRXPrecision)
				}
				if got := statedb.GetFrozenV2Amount(contractAddr, corepb.ResourceCode_ENERGY); got != 0 {
					t.Fatalf("reverted frozen balance: got %d, want 0", got)
				}
			} else {
				if got := statedb.GetBalance(contractAddr); got != 14*tvmTRXPrecision {
					t.Fatalf("committed balance: got %d, want %d", got, 14*tvmTRXPrecision)
				}
				if got := statedb.GetFrozenV2Amount(contractAddr, corepb.ResourceCode_ENERGY); got != amount {
					t.Fatalf("committed frozen balance: got %d, want %d", got, amount)
				}
			}
		})
	}
}
