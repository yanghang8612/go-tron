package rawdb

import (
	"errors"
	"strings"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/common"
)

func TestCycleRewardPendingRoundTrip(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	addr1 := common.BytesToAddress([]byte{0x41, 0x01})
	addr2 := common.BytesToAddress([]byte{0x41, 0x02})

	if _, _, ok, err := ReadCycleRewardPending(db); err != nil || ok {
		t.Fatalf("empty pending: ok=%v err=%v", ok, err)
	}
	if err := WriteCycleRewardPending(db, 7, map[common.Address]int64{
		addr2: 20,
		addr1: 10,
	}); err != nil {
		t.Fatal(err)
	}

	cycle, rewards, ok, err := ReadCycleRewardPending(db)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || cycle != 7 {
		t.Fatalf("pending header: cycle=%d ok=%v, want 7/true", cycle, ok)
	}
	if rewards[addr1] != 10 || rewards[addr2] != 20 {
		t.Fatalf("pending rewards = %#v, want addr1=10 addr2=20", rewards)
	}

	if err := WriteCycleRewardPending(db, 8, map[common.Address]int64{addr1: 0}); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, err := ReadCycleRewardPending(db); err != nil || ok {
		t.Fatalf("deleted pending: ok=%v err=%v", ok, err)
	}
}

func TestCycleRewardPendingSurfacesStorageErrors(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	addr := common.BytesToAddress([]byte{0x41, 0x01})
	if err := WriteCycleRewardPending(db, 7, map[common.Address]int64{addr: 10}); err != nil {
		t.Fatalf("write pending: %v", err)
	}

	if _, _, ok, err := ReadCycleRewardPending(failingStateDomainReader{reader: db, hasErr: errors.New("has boom")}); err == nil || ok || !strings.Contains(err.Error(), "presence") {
		t.Fatalf("has error ok=%v err=%v, want presence error", ok, err)
	}
	if _, _, ok, err := ReadCycleRewardPending(failingStateDomainReader{reader: db, getErr: errors.New("get boom")}); err == nil || ok || !strings.Contains(err.Error(), "get boom") {
		t.Fatalf("get error ok=%v err=%v, want get error", ok, err)
	}
}

func TestCycleRewardPendingRejectsEmptyPersistedValue(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	if err := db.Put(cycleRewardPendingKey, nil); err != nil {
		t.Fatalf("write empty pending: %v", err)
	}
	if _, _, ok, err := ReadCycleRewardPending(db); err == nil || ok || !strings.Contains(err.Error(), "short value") {
		t.Fatalf("empty pending ok=%v err=%v, want short value error", ok, err)
	}
}
