package state

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/big"
	"strings"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

var benchmarkCycleReward int64

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

func TestReadCycleRewardDirtyScalarDoesNotAllocate(t *testing.T) {
	statedb := newTestStateDB(t)
	addr := testAddr(0x49).Bytes()
	if err := statedb.WriteCycleReward(7, addr, 123); err != nil {
		t.Fatal(err)
	}
	read := func() {
		benchmarkCycleReward = statedb.ReadCycleReward(7, addr)
	}
	read()
	if benchmarkCycleReward != 123 {
		t.Fatalf("reward=%d want=123", benchmarkCycleReward)
	}
	if allocs := testing.AllocsPerRun(1000, read); allocs != 0 {
		t.Fatalf("dirty scalar reward read allocated %.2f objects, want zero", allocs)
	}
}

func BenchmarkReadCycleRewardDirtyScalar(b *testing.B) {
	statedb := newTestStateDB(b)
	addr := testAddr(0x4a).Bytes()
	if err := statedb.WriteCycleReward(7, addr, 123); err != nil {
		b.Fatal(err)
	}
	b.Run("allocating-owned-baseline", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			key := rawdb.CycleRewardStateKey(7, addr)
			if raw, ok := statedb.readSystemReward(key); ok && len(raw) == 8 {
				benchmarkCycleReward = int64(binary.BigEndian.Uint64(raw))
			}
		}
	})
	b.Run("scratch-borrowed", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			benchmarkCycleReward = statedb.ReadCycleReward(7, addr)
		}
	})
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
	accountVote := reopened.ReadCycleAccountVote(8, addr)
	if !bytes.Equal(accountVote, snap) {
		t.Fatalf("account vote: got %x, want %x", accountVote, snap)
	}
	accountVote[0] ^= 0xff
	if got := reopened.ReadCycleAccountVote(8, addr); !bytes.Equal(got, snap) {
		t.Fatalf("account vote read exposed internal storage: got %x want=%x", got, snap)
	}
	if got := reopened.ReadBeginCycle(addr); got != 8 {
		t.Fatalf("begin cycle: got %d, want 8", got)
	}
	if got := reopened.ReadEndCycle(addr); got != 9 {
		t.Fatalf("end cycle: got %d, want 9", got)
	}

	if got, ok, err := reopened.ReadCycleRewardStrict(8, addr); err != nil || !ok || got != 130 {
		t.Fatalf("strict cycle reward = (%d,%v,%v), want (130,true,nil)", got, ok, err)
	}
	if got, ok, err := reopened.ReadCycleVoteStrict(8, addr); err != nil || !ok || got != 456 {
		t.Fatalf("strict cycle vote = (%d,%v,%v), want (456,true,nil)", got, ok, err)
	}
	if got, ok, err := reopened.ReadWitnessVIStrict(8, addr); err != nil || !ok || got.Cmp(vi) != 0 {
		t.Fatalf("strict witness VI = (%s,%v,%v), want (%s,true,nil)", got, ok, err, vi)
	}
	if got, ok, err := reopened.ReadCycleBrokerageStrict(8, addr); err != nil || !ok || got != 33 {
		t.Fatalf("strict brokerage = (%d,%v,%v), want (33,true,nil)", got, ok, err)
	}
	if got, ok, err := reopened.ReadCycleAccountVoteStrict(8, addr); err != nil || !ok || !bytes.Equal(got, snap) {
		t.Fatalf("strict account vote = (%x,%v,%v), want (%x,true,nil)", got, ok, err, snap)
	}
	if got, ok, err := reopened.ReadBeginCycleStrict(addr); err != nil || !ok || got != 8 {
		t.Fatalf("strict begin cycle = (%d,%v,%v), want (8,true,nil)", got, ok, err)
	}
	if got, ok, err := reopened.ReadEndCycleStrict(addr); err != nil || !ok || got != 9 {
		t.Fatalf("strict end cycle = (%d,%v,%v), want (9,true,nil)", got, ok, err)
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

func TestRewardStoreStrictBatchSurfacesMalformedReward(t *testing.T) {
	statedb := newTestStateDB(t)
	addr := testAddr(0x4c)

	if err := statedb.SystemKVPut(kvdomains.SystemReward, rawdb.CycleRewardStateKey(8, addr.Bytes()), []byte{0x01, 0x02}); err != nil {
		t.Fatalf("write malformed cycle reward: %v", err)
	}
	if got, err := statedb.ReadCycleRewardsStrict(8, []tcommon.Address{addr}); err == nil {
		t.Fatalf("ReadCycleRewardsStrict malformed error = nil, got %v", got)
	} else if !strings.Contains(err.Error(), "decode cycle reward") || !strings.Contains(err.Error(), "length 2, want 8") {
		t.Fatalf("ReadCycleRewardsStrict malformed error = %v, want decode length context", err)
	}
	if got := statedb.ReadCycleRewards(8, []tcommon.Address{addr}); len(got) != 0 {
		t.Fatalf("legacy ReadCycleRewards malformed = %v, want empty map", got)
	}
}

func TestRewardStoreStrictSurfacesMalformedRows(t *testing.T) {
	tests := []struct {
		name     string
		key      func([]byte) []byte
		write    []byte
		strictOK func(*StateDB, []byte) error
		legacyOK func(*StateDB, []byte) bool
	}{
		{
			name:  "cycle reward",
			key:   func(addr []byte) []byte { return rawdb.CycleRewardStateKey(3, addr) },
			write: []byte{0x01},
			strictOK: func(s *StateDB, addr []byte) error {
				_, ok, err := s.ReadCycleRewardStrict(3, addr)
				if err == nil || ok {
					return fmt.Errorf("err=%v ok=%v", err, ok)
				}
				return nil
			},
			legacyOK: func(s *StateDB, addr []byte) bool { return s.ReadCycleReward(3, addr) == 0 },
		},
		{
			name:  "cycle vote",
			key:   func(addr []byte) []byte { return rawdb.CycleVoteStateKey(3, addr) },
			write: []byte{0x01},
			strictOK: func(s *StateDB, addr []byte) error {
				_, ok, err := s.ReadCycleVoteStrict(3, addr)
				if err == nil || ok {
					return fmt.Errorf("err=%v ok=%v", err, ok)
				}
				return nil
			},
			legacyOK: func(s *StateDB, addr []byte) bool { return s.ReadCycleVote(3, addr) == rawdb.RewardRemark },
		},
		{
			name:  "cycle brokerage",
			key:   func(addr []byte) []byte { return rawdb.CycleBrokerageStateKey(3, addr) },
			write: []byte{0x01, 0x02, 0x03},
			strictOK: func(s *StateDB, addr []byte) error {
				_, ok, err := s.ReadCycleBrokerageStrict(3, addr)
				if err == nil || ok {
					return fmt.Errorf("err=%v ok=%v", err, ok)
				}
				return nil
			},
			legacyOK: func(s *StateDB, addr []byte) bool { return s.ReadCycleBrokerage(3, addr) == rawdb.DefaultBrokerage },
		},
		{
			name:  "begin cycle",
			key:   rawdb.BeginCycleStateKey,
			write: []byte{0x01},
			strictOK: func(s *StateDB, addr []byte) error {
				_, ok, err := s.ReadBeginCycleStrict(addr)
				if err == nil || ok {
					return fmt.Errorf("err=%v ok=%v", err, ok)
				}
				return nil
			},
			legacyOK: func(s *StateDB, addr []byte) bool { return s.ReadBeginCycle(addr) == 0 },
		},
		{
			name:  "end cycle",
			key:   rawdb.EndCycleStateKey,
			write: []byte{0x01},
			strictOK: func(s *StateDB, addr []byte) error {
				_, ok, err := s.ReadEndCycleStrict(addr)
				if err == nil || ok {
					return fmt.Errorf("err=%v ok=%v", err, ok)
				}
				return nil
			},
			legacyOK: func(s *StateDB, addr []byte) bool { return s.ReadEndCycle(addr) == rawdb.RewardRemark },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			statedb := newTestStateDB(t)
			addr := testAddr(0x4d).Bytes()
			if err := statedb.SystemKVPut(kvdomains.SystemReward, tt.key(addr), tt.write); err != nil {
				t.Fatalf("write malformed %s: %v", tt.name, err)
			}
			if err := tt.strictOK(statedb, addr); err != nil {
				t.Fatalf("%s strict malformed err = %v, want non-nil decode error", tt.name, err)
			}
			if !tt.legacyOK(statedb, addr) {
				t.Fatalf("%s legacy reader did not preserve default behavior", tt.name)
			}
		})
	}
}

func TestRewardStoreAddCycleRewardsFinalStacksDirtyValues(t *testing.T) {
	statedb := newTestStateDB(t)
	addr := testAddr(0x48)

	if err := statedb.AddCycleRewardsFinal(9, map[tcommon.Address]int64{addr: 5}); err != nil {
		t.Fatal(err)
	}
	if err := statedb.AddCycleRewardsFinal(9, map[tcommon.Address]int64{addr: 7}); err != nil {
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
	if got := reopened.ReadCycleReward(9, addr.Bytes()); got != 12 {
		t.Fatalf("reward = %d, want 12", got)
	}
}

func TestCycleBrokerageAtDefaultsMissingHistory(t *testing.T) {
	f := newHistoryFixture(t)
	addr := testAddr(0x49)

	f.applyBlock(tcommon.Hash{0x01}, func(*StateDB) {})

	got, err := f.reader().CycleBrokerageAt(9, addr.Bytes(), 1)
	if err != nil {
		t.Fatalf("CycleBrokerageAt missing history error = %v", err)
	}
	if got != int64(rawdb.DefaultBrokerage) {
		t.Fatalf("CycleBrokerageAt missing history = %d, want %d", got, rawdb.DefaultBrokerage)
	}
}

func TestCycleBrokerageAtSurfacesMalformedLength(t *testing.T) {
	f := newHistoryFixture(t)
	addr := testAddr(0x4a)

	f.applyBlock(tcommon.Hash{0x01}, func(s *StateDB) {
		if err := s.SystemKVPut(kvdomains.SystemReward, rawdb.CycleBrokerageStateKey(9, addr.Bytes()), []byte{0x01, 0x02, 0x03}); err != nil {
			t.Fatalf("write malformed brokerage history: %v", err)
		}
	})
	f.applyBlock(tcommon.Hash{0x02}, func(*StateDB) {})

	got, err := f.reader().CycleBrokerageAt(9, addr.Bytes(), 1)
	if err == nil {
		t.Fatal("CycleBrokerageAt malformed history error = nil")
	}
	if got != 0 {
		t.Fatalf("CycleBrokerageAt malformed history = %d, want 0", got)
	}
	if !strings.Contains(err.Error(), "decode cycle brokerage at block 1") || !strings.Contains(err.Error(), "length 3, want 4") {
		t.Fatalf("CycleBrokerageAt malformed history error = %v, want decode length context", err)
	}
}
