package rawdb

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
)

func TestRewardVi_RoundTrip(t *testing.T) {
	db := memorydb.New()
	addr := make([]byte, 21)
	addr[20] = 0xAB

	// VI values can be large (reward * 10^18 / voteCount)
	vi := new(big.Int).Mul(big.NewInt(1_000_000), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))

	WriteRewardVi(db, 10, addr, vi)
	got := ReadRewardVi(db, 10, addr)
	if got.Cmp(vi) != 0 {
		t.Errorf("ReadRewardVi: got %v want %v", got, vi)
	}
}

func TestRewardVi_Absent(t *testing.T) {
	db := memorydb.New()
	addr := make([]byte, 21)
	got := ReadRewardVi(db, 99, addr)
	if got == nil || got.Sign() != 0 {
		t.Errorf("expected zero big.Int for absent key, got %v", got)
	}
}

func TestRewardVi_ZeroNotStored(t *testing.T) {
	db := memorydb.New()
	addr := make([]byte, 21)
	WriteRewardVi(db, 1, addr, new(big.Int))
	// Zero VI should not be stored (mirrors java-tron "Zero vi will not be record")
	got := ReadRewardVi(db, 1, addr)
	if got.Sign() != 0 {
		t.Errorf("expected zero for unwritten entry, got %v", got)
	}
}

func TestRewardVi_Delete(t *testing.T) {
	db := memorydb.New()
	addr := make([]byte, 21)
	addr[0] = 0x41
	vi := big.NewInt(12345)
	WriteRewardVi(db, 5, addr, vi)
	if err := DeleteRewardVi(db, 5, addr); err != nil {
		t.Fatal(err)
	}
	if got := ReadRewardVi(db, 5, addr); got.Sign() != 0 {
		t.Errorf("expected zero after delete, got %v", got)
	}
}

func TestRewardVi_MultiCycleMultiWitness(t *testing.T) {
	db := memorydb.New()
	addrs := [][]byte{make([]byte, 21), make([]byte, 21), make([]byte, 21)}
	addrs[0][20] = 0x01
	addrs[1][20] = 0x02
	addrs[2][20] = 0x03

	for cycle := int64(1); cycle <= 3; cycle++ {
		for wi, addr := range addrs {
			vi := big.NewInt(cycle*100 + int64(wi+1))
			WriteRewardVi(db, cycle, addr, vi)
		}
	}
	for cycle := int64(1); cycle <= 3; cycle++ {
		for wi, addr := range addrs {
			want := big.NewInt(cycle*100 + int64(wi+1))
			got := ReadRewardVi(db, cycle, addr)
			if got.Cmp(want) != 0 {
				t.Errorf("cycle %d witness %d: got %v want %v", cycle, wi, got, want)
			}
		}
	}
}

func TestRewardViIsDone(t *testing.T) {
	db := memorydb.New()
	if IsRewardViDone(db) {
		t.Fatal("expected not done before write")
	}
	WriteRewardViIsDone(db)
	if !IsRewardViDone(db) {
		t.Fatal("expected done after write")
	}
}
