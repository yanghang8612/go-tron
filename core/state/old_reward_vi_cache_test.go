package state

import (
	"math/big"
	"sync/atomic"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
)

type countingRewardKeyValueReader struct {
	ethdb.Database
	gets atomic.Int64
}

func (r *countingRewardKeyValueReader) Get(key []byte) ([]byte, error) {
	r.gets.Add(1)
	return r.Database.Get(key)
}

func TestOldRewardVICacheMatchesUncachedAndInvalidates(t *testing.T) {
	disk := ethrawdb.NewMemoryDatabase()
	witness := tcommon.BytesToAddress([]byte{0x41, 0x09})

	seedDB := NewDatabase(disk)
	seed, err := New(tcommon.Hash{}, seedDB)
	if err != nil {
		t.Fatal(err)
	}
	// Populate cycles 1..8 (leave cycle 3 reward=0 to cover sparse zero-reward skip).
	for cycle := int64(1); cycle <= 8; cycle++ {
		reward := int64(cycle * 100)
		if cycle == 3 {
			reward = 0
		}
		if err := seed.WriteCycleReward(cycle, witness.Bytes(), reward); err != nil {
			t.Fatal(err)
		}
		if err := seed.WriteCycleVote(cycle, witness.Bytes(), 7+cycle); err != nil {
			t.Fatal(err)
		}
	}
	root, err := seed.Commit()
	if err != nil {
		t.Fatal(err)
	}

	countedDisk := &countingRewardKeyValueReader{Database: disk}
	sdb := NewDatabase(countedDisk)
	uncached, err := New(root, sdb)
	if err != nil {
		t.Fatal(err)
	}
	cached, err := New(root, sdb)
	if err != nil {
		t.Fatal(err)
	}
	cache := NewOldRewardVICache()
	cached.SetOldRewardVICache(cache)

	// Guard: currentCycle <= end+1 must refuse caching and fall back.
	if _, ok := cached.OldRewardVISum(witness, 1, 9, 10); ok {
		t.Fatal("expected cache refusal when currentCycle <= end+1")
	}

	// Guard: transactionAccess != nil must refuse caching.
	var rec TransactionAccessRecorder
	cached.SetTransactionAccessRecorder(&rec)
	if _, ok := cached.OldRewardVISum(witness, 1, 9, 20); ok {
		t.Fatal("expected cache refusal with transactionAccess active")
	}
	cached.SetTransactionAccessRecorder(nil)

	referenceSum := func(begin, end int64) *big.Int {
		sum := new(big.Int)
		for cycle := begin; cycle < end; cycle++ {
			r := uncached.ReadCycleReward(cycle, witness.Bytes())
			v := uncached.ReadCycleVote(cycle, witness.Bytes())
			if r == 0 || v == 0 {
				continue
			}
			delta := new(big.Int).Mul(big.NewInt(r), oldRewardDecimalOfVI)
			delta.Quo(delta, big.NewInt(v))
			sum.Add(sum, delta)
		}
		return sum
	}

	// First cached call populates [1, 6); subsequent subrange [2, 5) hits with 0 DB reads.
	got1, ok := cached.OldRewardVISum(witness, 1, 6, 20)
	if !ok {
		t.Fatal("expected first OldRewardVISum to populate cache")
	}
	if want1 := referenceSum(1, 6); got1.Cmp(want1) != 0 {
		t.Fatalf("[1,6) mismatch: got %s, want %s", got1, want1)
	}

	beforeGets := countedDisk.gets.Load()
	gotSub, ok := cached.OldRewardVISum(witness, 2, 5, 20)
	if !ok {
		t.Fatal("expected cached subrange hit")
	}
	if afterGets := countedDisk.gets.Load(); afterGets != beforeGets {
		t.Fatalf("subrange hit read durable disk: before=%d after=%d", beforeGets, afterGets)
	}
	if wantSub := referenceSum(2, 5); gotSub.Cmp(wantSub) != 0 {
		t.Fatalf("[2,5) mismatch: got %s, want %s", gotSub, wantSub)
	}

	// Extending to [1, 9) only fills missing cycles [6, 9) and serves second call with 0 reads.
	got2, ok := cached.OldRewardVISum(witness, 1, 9, 20)
	if !ok {
		t.Fatal("expected extended range [1,9) to succeed")
	}
	if want2 := referenceSum(1, 9); got2.Cmp(want2) != 0 {
		t.Fatalf("[1,9) mismatch: got %s, want %s", got2, want2)
	}
	beforeFull := countedDisk.gets.Load()
	got3, ok := cached.OldRewardVISum(witness, 1, 9, 20)
	if !ok || got3.Cmp(got2) != 0 {
		t.Fatalf("repeat [1,9) mismatch: ok=%v got=%s want=%s", ok, got3, got2)
	}
	if afterFull := countedDisk.gets.Load(); afterFull != beforeFull {
		t.Fatalf("repeat [1,9) read durable disk: before=%d after=%d", beforeFull, afterFull)
	}

	// Dirty SystemReward row in StateDB must bypass cache.
	if err := cached.WriteCycleReward(4, witness.Bytes(), 9999); err != nil {
		t.Fatal(err)
	}
	if _, ok := cached.OldRewardVISum(witness, 1, 9, 20); ok {
		t.Fatal("expected cache bypass while SystemReward has dirty entries")
	}
	// Commit mutation and verify invalidated cache recomputes exact new value.
	newRoot, err := cached.Commit()
	if err != nil {
		t.Fatal(err)
	}
	uncached2, err := New(newRoot, sdb)
	if err != nil {
		t.Fatal(err)
	}
	cached2, err := New(newRoot, sdb)
	if err != nil {
		t.Fatal(err)
	}
	cached2.SetOldRewardVICache(cache)
	gotAfterWrite, ok := cached2.OldRewardVISum(witness, 1, 9, 20)
	if !ok {
		t.Fatal("expected cache repopulation after commit")
	}
	wantAfterWrite := new(big.Int)
	for cycle := int64(1); cycle < 9; cycle++ {
		r := uncached2.ReadCycleReward(cycle, witness.Bytes())
		v := uncached2.ReadCycleVote(cycle, witness.Bytes())
		if r == 0 || v == 0 {
			continue
		}
		delta := new(big.Int).Mul(big.NewInt(r), oldRewardDecimalOfVI)
		delta.Quo(delta, big.NewInt(v))
		wantAfterWrite.Add(wantAfterWrite, delta)
	}
	if gotAfterWrite.Cmp(wantAfterWrite) != 0 {
		t.Fatalf("post-write mismatch: got %s, want %s", gotAfterWrite, wantAfterWrite)
	}
}
