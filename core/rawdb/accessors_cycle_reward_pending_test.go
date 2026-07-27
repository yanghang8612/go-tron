package rawdb

import (
	"bytes"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/common"
)

type cycleRewardPendingOwnedWriterProbe struct {
	putCalled bool
	key       string
	value     []byte
}

func (p *cycleRewardPendingOwnedWriterProbe) Put([]byte, []byte) error {
	p.putCalled = true
	return nil
}

func (*cycleRewardPendingOwnedWriterProbe) Delete([]byte) error { return nil }

func (p *cycleRewardPendingOwnedWriterProbe) PutStringOwnedValue(key string, value []byte) error {
	p.key = key
	p.value = value
	return nil
}

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

func TestWriteCycleRewardPendingEntriesTransfersEncodedValue(t *testing.T) {
	addr1 := common.BytesToAddress([]byte{0x41, 0x01})
	addr2 := common.BytesToAddress([]byte{0x41, 0x02})
	entries := []CycleRewardPendingEntry{
		{Address: addr2, Amount: 20},
		{Address: addr1, Amount: 10},
	}
	probe := new(cycleRewardPendingOwnedWriterProbe)
	if err := WriteCycleRewardPendingEntries(probe, 7, entries); err != nil {
		t.Fatal(err)
	}
	if probe.putCalled {
		t.Fatal("pending reward used defensive Put instead of owned string write")
	}
	if probe.key != cycleRewardPendingKeyString {
		t.Fatalf("pending reward key = %q, want %q", probe.key, cycleRewardPendingKeyString)
	}

	db := ethrawdb.NewMemoryDatabase()
	if err := db.Put(cycleRewardPendingKey, probe.value); err != nil {
		t.Fatal(err)
	}
	cycle, rewards, ok, err := ReadCycleRewardPending(db)
	if err != nil || !ok || cycle != 7 || rewards[addr1] != 10 || rewards[addr2] != 20 {
		t.Fatalf("decoded transferred value: cycle=%d rewards=%v ok=%v err=%v", cycle, rewards, ok, err)
	}

	wantProbe := new(cycleRewardPendingOwnedWriterProbe)
	if err := WriteCycleRewardPending(wantProbe, 7, map[common.Address]int64{addr1: 10, addr2: 20}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(probe.value, wantProbe.value) {
		t.Fatalf("entry encoding = %x, map encoding = %x", probe.value, wantProbe.value)
	}
}

var benchmarkCycleRewardPendingValue []byte

func TestWriteCycleRewardPendingEntriesAllocatesOnlyEncodedValue(t *testing.T) {
	entries := make([]CycleRewardPendingEntry, 27)
	for i := range entries {
		entries[i] = CycleRewardPendingEntry{
			Address: common.BytesToAddress([]byte{byte(27 - i)}),
			Amount:  int64(i + 1),
		}
	}
	probe := new(cycleRewardPendingOwnedWriterProbe)
	allocs := testing.AllocsPerRun(100, func() {
		if err := WriteCycleRewardPendingEntries(probe, 7, entries); err != nil {
			panic(err)
		}
		benchmarkCycleRewardPendingValue = probe.value
	})
	if allocs != 1 {
		t.Fatalf("entry write allocations = %v, want 1 encoded value", allocs)
	}
}
