package state

import (
	"bytes"
	"math/big"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestRewardStoreDefaults(t *testing.T) {
	statedb := newTestStateDB(t)
	addr := testAddr(0x44).Bytes()

	if got := statedb.ReadCycleReward(3, addr); got != 0 {
		t.Fatalf("default cycle reward: got %d, want 0", got)
	}
	if got := statedb.ReadCycleVote(3, addr); got != rawdb.RewardRemark {
		t.Fatalf("default cycle vote: got %d, want %d", got, rawdb.RewardRemark)
	}
	if got := statedb.ReadWitnessVI(3, addr); got.Sign() != 0 {
		t.Fatalf("default witness VI: got %s, want 0", got.String())
	}
	if got := statedb.ReadCycleBrokerage(3, addr); got != rawdb.DefaultBrokerage {
		t.Fatalf("default brokerage: got %d, want %d", got, rawdb.DefaultBrokerage)
	}
	if got := statedb.ReadCycleAccountVote(3, addr); got != nil {
		t.Fatalf("default account vote: got %x, want nil", got)
	}
	if got := statedb.ReadBeginCycle(addr); got != 0 {
		t.Fatalf("default begin cycle: got %d, want 0", got)
	}
	if got := statedb.ReadEndCycle(addr); got != rawdb.RewardRemark {
		t.Fatalf("default end cycle: got %d, want %d", got, rawdb.RewardRemark)
	}
}

func TestRewardStoreRoundTripAcrossRoot(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	db := NewDatabase(diskdb)
	statedb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		t.Fatal(err)
	}

	addr := testAddr(0x45).Bytes()
	decimalOfVI := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	vi := new(big.Int).Mul(big.NewInt(9), decimalOfVI)
	snap := []byte{0x01, 0x02, 0x03}

	if err := statedb.WriteCycleReward(8, addr, 123); err != nil {
		t.Fatal(err)
	}
	if err := statedb.AddCycleReward(8, addr, 7); err != nil {
		t.Fatal(err)
	}
	if err := statedb.WriteCycleVote(8, addr, 456); err != nil {
		t.Fatal(err)
	}
	if err := statedb.WriteWitnessVI(8, addr, vi); err != nil {
		t.Fatal(err)
	}
	if err := statedb.WriteCycleBrokerage(8, addr, 33); err != nil {
		t.Fatal(err)
	}
	if err := statedb.WriteCycleAccountVote(8, addr, snap); err != nil {
		t.Fatal(err)
	}
	if err := statedb.WriteBeginCycle(addr, 8); err != nil {
		t.Fatal(err)
	}
	if err := statedb.WriteEndCycle(addr, 9); err != nil {
		t.Fatal(err)
	}

	root, err := statedb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := New(root, db)
	if err != nil {
		t.Fatal(err)
	}

	if got := reopened.ReadCycleReward(8, addr); got != 130 {
		t.Fatalf("cycle reward: got %d, want 130", got)
	}
	if got := reopened.ReadCycleVote(8, addr); got != 456 {
		t.Fatalf("cycle vote: got %d, want 456", got)
	}
	if got := reopened.ReadWitnessVI(8, addr); got.Cmp(vi) != 0 {
		t.Fatalf("witness VI: got %s, want %s", got.String(), vi.String())
	}
	if got := reopened.ReadCycleBrokerage(8, addr); got != 33 {
		t.Fatalf("brokerage: got %d, want 33", got)
	}
	if got := reopened.ReadCycleAccountVote(8, addr); !bytes.Equal(got, snap) {
		t.Fatalf("account vote: got %x, want %x", got, snap)
	}
	if got := reopened.ReadBeginCycle(addr); got != 8 {
		t.Fatalf("begin cycle: got %d, want 8", got)
	}
	if got := reopened.ReadEndCycle(addr); got != 9 {
		t.Fatalf("end cycle: got %d, want 9", got)
	}
}

func TestRewardStoreAddCycleRewardsBatch(t *testing.T) {
	statedb := newTestStateDB(t)
	addr1 := testAddr(0x46)
	addr2 := testAddr(0x47)

	if err := statedb.WriteCycleReward(8, addr1.Bytes(), 10); err != nil {
		t.Fatal(err)
	}
	if err := statedb.AddCycleRewards(8, map[tcommon.Address]int64{
		addr1: 5,
		addr2: 7,
	}); err != nil {
		t.Fatal(err)
	}
	root, err := statedb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := New(root, statedb.db)
	if err != nil {
		t.Fatal(err)
	}
	if got := reopened.ReadCycleReward(8, addr1.Bytes()); got != 15 {
		t.Fatalf("addr1 reward = %d, want 15", got)
	}
	if got := reopened.ReadCycleReward(8, addr2.Bytes()); got != 7 {
		t.Fatalf("addr2 reward = %d, want 7", got)
	}
}
