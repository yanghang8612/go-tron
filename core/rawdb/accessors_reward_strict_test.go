package rawdb

import (
	"errors"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
)

func TestRewardStrictReadersRoundTripAndAbsentDefaults(t *testing.T) {
	db := NewMemoryDatabase()
	addr := rewardStrictTestAddr()

	if got, ok, err := ReadCycleRewardStrict(db, 3, addr); err != nil || ok || got != 0 {
		t.Fatalf("missing cycle reward: got=%d ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := ReadCycleVoteStrict(db, 3, addr); err != nil || ok || got != RewardRemark {
		t.Fatalf("missing cycle vote: got=%d ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := ReadWitnessVIStrict(db, 3, addr); err != nil || ok || got.Sign() != 0 {
		t.Fatalf("missing witness vi: got=%v ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := ReadCycleBrokerageStrict(db, 3, addr); err != nil || ok || got != DefaultBrokerage {
		t.Fatalf("missing cycle brokerage: got=%d ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := ReadCycleAccountVoteStrict(db, 3, addr); err != nil || ok || got != nil {
		t.Fatalf("missing account vote: got=%x ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := ReadBeginCycleStrict(db, addr); err != nil || ok || got != 0 {
		t.Fatalf("missing begin cycle: got=%d ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := ReadEndCycleStrict(db, addr); err != nil || ok || got != RewardRemark {
		t.Fatalf("missing end cycle: got=%d ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := ReadRewardViStrict(db, 3, addr); err != nil || ok || got.Sign() != 0 {
		t.Fatalf("missing reward vi: got=%v ok=%v err=%v", got, ok, err)
	}
	if ok, err := IsRewardViDoneStrict(db); err != nil || ok {
		t.Fatalf("missing reward vi done: ok=%v err=%v", ok, err)
	}

	vi := new(big.Int).Mul(big.NewInt(123), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
	WriteCycleReward(db, 3, addr, 11)
	WriteCycleVote(db, 3, addr, 22)
	WriteWitnessVI(db, 3, addr, vi)
	WriteCycleBrokerage(db, 3, addr, 33)
	WriteCycleAccountVote(db, 3, addr, []byte{0x0a, 0x01})
	WriteBeginCycle(db, addr, 4)
	WriteEndCycle(db, addr, 5)
	WriteRewardVi(db, 3, addr, vi)
	WriteRewardViIsDone(db)

	if got, ok, err := ReadCycleRewardStrict(db, 3, addr); err != nil || !ok || got != 11 {
		t.Fatalf("cycle reward: got=%d ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := ReadCycleVoteStrict(db, 3, addr); err != nil || !ok || got != 22 {
		t.Fatalf("cycle vote: got=%d ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := ReadWitnessVIStrict(db, 3, addr); err != nil || !ok || got.Cmp(vi) != 0 {
		t.Fatalf("witness vi: got=%v ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := ReadCycleBrokerageStrict(db, 3, addr); err != nil || !ok || got != 33 {
		t.Fatalf("cycle brokerage: got=%d ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := ReadCycleAccountVoteStrict(db, 3, addr); err != nil || !ok || string(got) != string([]byte{0x0a, 0x01}) {
		t.Fatalf("account vote: got=%x ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := ReadBeginCycleStrict(db, addr); err != nil || !ok || got != 4 {
		t.Fatalf("begin cycle: got=%d ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := ReadEndCycleStrict(db, addr); err != nil || !ok || got != 5 {
		t.Fatalf("end cycle: got=%d ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := ReadRewardViStrict(db, 3, addr); err != nil || !ok || got.Cmp(vi) != 0 {
		t.Fatalf("reward vi: got=%v ok=%v err=%v", got, ok, err)
	}
	if ok, err := IsRewardViDoneStrict(db); err != nil || !ok {
		t.Fatalf("reward vi done: ok=%v err=%v", ok, err)
	}
}

func TestRewardStrictReadersSurfaceStorageErrors(t *testing.T) {
	db := NewMemoryDatabase()
	addr := rewardStrictTestAddr()
	vi := big.NewInt(12345)

	WriteCycleReward(db, 3, addr, 11)
	WriteCycleVote(db, 3, addr, 22)
	WriteWitnessVI(db, 3, addr, vi)
	WriteCycleBrokerage(db, 3, addr, 33)
	WriteCycleAccountVote(db, 3, addr, []byte{0x0a, 0x01})
	WriteBeginCycle(db, addr, 4)
	WriteEndCycle(db, addr, 5)
	WriteRewardVi(db, 3, addr, vi)
	WriteRewardViIsDone(db)

	readers := []struct {
		name string
		read func(ethdb.KeyValueReader) (bool, error)
	}{
		{
			name: "cycle reward",
			read: func(r ethdb.KeyValueReader) (bool, error) {
				_, ok, err := ReadCycleRewardStrict(r, 3, addr)
				return ok, err
			},
		},
		{
			name: "cycle vote",
			read: func(r ethdb.KeyValueReader) (bool, error) {
				_, ok, err := ReadCycleVoteStrict(r, 3, addr)
				return ok, err
			},
		},
		{
			name: "witness vi",
			read: func(r ethdb.KeyValueReader) (bool, error) {
				_, ok, err := ReadWitnessVIStrict(r, 3, addr)
				return ok, err
			},
		},
		{
			name: "cycle brokerage",
			read: func(r ethdb.KeyValueReader) (bool, error) {
				_, ok, err := ReadCycleBrokerageStrict(r, 3, addr)
				return ok, err
			},
		},
		{
			name: "account vote",
			read: func(r ethdb.KeyValueReader) (bool, error) {
				_, ok, err := ReadCycleAccountVoteStrict(r, 3, addr)
				return ok, err
			},
		},
		{
			name: "begin cycle",
			read: func(r ethdb.KeyValueReader) (bool, error) {
				_, ok, err := ReadBeginCycleStrict(r, addr)
				return ok, err
			},
		},
		{
			name: "end cycle",
			read: func(r ethdb.KeyValueReader) (bool, error) {
				_, ok, err := ReadEndCycleStrict(r, addr)
				return ok, err
			},
		},
		{
			name: "reward vi",
			read: func(r ethdb.KeyValueReader) (bool, error) {
				_, ok, err := ReadRewardViStrict(r, 3, addr)
				return ok, err
			},
		},
		{
			name: "reward vi done",
			read: func(r ethdb.KeyValueReader) (bool, error) {
				return IsRewardViDoneStrict(r)
			},
		},
	}

	for _, tc := range readers {
		t.Run(tc.name+"/has", func(t *testing.T) {
			ok, err := tc.read(failingStateDomainReader{reader: db, hasErr: errors.New("has boom")})
			if err == nil || ok || !strings.Contains(err.Error(), "presence") {
				t.Fatalf("has error: ok=%v err=%v", ok, err)
			}
		})
		if tc.name == "reward vi done" {
			continue
		}
		t.Run(tc.name+"/get", func(t *testing.T) {
			ok, err := tc.read(failingStateDomainReader{reader: db, getErr: errors.New("get boom")})
			if err == nil || ok || !strings.Contains(err.Error(), "get boom") {
				t.Fatalf("get error: ok=%v err=%v", ok, err)
			}
		})
	}
}

func TestRewardStrictReadersRejectMalformedFixedWidthValues(t *testing.T) {
	db := NewMemoryDatabase()
	addr := rewardStrictTestAddr()

	malformed := []struct {
		name      string
		key       []byte
		read      func() (bool, error)
		legacyOK  func() bool
		wantError string
	}{
		{
			name: "cycle reward",
			key:  CycleRewardStateKey(3, addr),
			read: func() (bool, error) {
				_, ok, err := ReadCycleRewardStrict(db, 3, addr)
				return ok, err
			},
			legacyOK:  func() bool { return ReadCycleReward(db, 3, addr) == 0 },
			wantError: "length 1, want 8",
		},
		{
			name: "cycle vote",
			key:  CycleVoteStateKey(3, addr),
			read: func() (bool, error) {
				_, ok, err := ReadCycleVoteStrict(db, 3, addr)
				return ok, err
			},
			legacyOK:  func() bool { return ReadCycleVote(db, 3, addr) == RewardRemark },
			wantError: "length 1, want 8",
		},
		{
			name: "cycle brokerage",
			key:  CycleBrokerageStateKey(3, addr),
			read: func() (bool, error) {
				_, ok, err := ReadCycleBrokerageStrict(db, 3, addr)
				return ok, err
			},
			legacyOK:  func() bool { return ReadCycleBrokerage(db, 3, addr) == DefaultBrokerage },
			wantError: "length 1, want 4",
		},
		{
			name: "begin cycle",
			key:  BeginCycleStateKey(addr),
			read: func() (bool, error) {
				_, ok, err := ReadBeginCycleStrict(db, addr)
				return ok, err
			},
			legacyOK:  func() bool { return ReadBeginCycle(db, addr) == 0 },
			wantError: "length 1, want 8",
		},
		{
			name: "end cycle",
			key:  EndCycleStateKey(addr),
			read: func() (bool, error) {
				_, ok, err := ReadEndCycleStrict(db, addr)
				return ok, err
			},
			legacyOK:  func() bool { return ReadEndCycle(db, addr) == RewardRemark },
			wantError: "length 1, want 8",
		},
	}

	for _, tc := range malformed {
		t.Run(tc.name, func(t *testing.T) {
			if err := db.Put(tc.key, []byte{0x01}); err != nil {
				t.Fatalf("put malformed: %v", err)
			}
			ok, err := tc.read()
			if err == nil || ok || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("strict malformed read: ok=%v err=%v", ok, err)
			}
			if !tc.legacyOK() {
				t.Fatalf("legacy reader did not preserve its default for malformed data")
			}
			if err := db.Delete(tc.key); err != nil {
				t.Fatalf("delete malformed: %v", err)
			}
		})
	}
}

func TestRewardLegacyReadersKeepDefaultsOnStorageErrors(t *testing.T) {
	db := NewMemoryDatabase()
	addr := rewardStrictTestAddr()
	vi := big.NewInt(12345)

	WriteCycleReward(db, 3, addr, 11)
	WriteCycleVote(db, 3, addr, 22)
	WriteWitnessVI(db, 3, addr, vi)
	WriteCycleBrokerage(db, 3, addr, 33)
	WriteCycleAccountVote(db, 3, addr, []byte{0x0a, 0x01})
	WriteBeginCycle(db, addr, 4)
	WriteEndCycle(db, addr, 5)
	WriteRewardVi(db, 3, addr, vi)
	WriteRewardViIsDone(db)

	readers := []struct {
		reader       ethdb.KeyValueReader
		wantDoneBool bool
	}{
		{
			reader:       failingStateDomainReader{reader: db, hasErr: errors.New("has boom")},
			wantDoneBool: false,
		},
		{
			reader:       failingStateDomainReader{reader: db, getErr: errors.New("get boom")},
			wantDoneBool: true,
		},
	}
	for _, failing := range readers {
		reader := failing.reader
		if got := ReadCycleReward(reader, 3, addr); got != 0 {
			t.Fatalf("cycle reward default on error: got %d", got)
		}
		if got := ReadCycleVote(reader, 3, addr); got != RewardRemark {
			t.Fatalf("cycle vote default on error: got %d", got)
		}
		if got := ReadWitnessVI(reader, 3, addr); got.Sign() != 0 {
			t.Fatalf("witness vi default on error: got %v", got)
		}
		if got := ReadCycleBrokerage(reader, 3, addr); got != DefaultBrokerage {
			t.Fatalf("cycle brokerage default on error: got %d", got)
		}
		if got := ReadCycleAccountVote(reader, 3, addr); got != nil {
			t.Fatalf("account vote default on error: got %x", got)
		}
		if got := ReadBeginCycle(reader, addr); got != 0 {
			t.Fatalf("begin cycle default on error: got %d", got)
		}
		if got := ReadEndCycle(reader, addr); got != RewardRemark {
			t.Fatalf("end cycle default on error: got %d", got)
		}
		if got := ReadRewardVi(reader, 3, addr); got.Sign() != 0 {
			t.Fatalf("reward vi default on error: got %v", got)
		}
		if got := IsRewardViDone(reader); got != failing.wantDoneBool {
			t.Fatalf("reward vi done on error: got %v want %v", got, failing.wantDoneBool)
		}
	}
}

func rewardStrictTestAddr() []byte {
	addr := make([]byte, 21)
	addr[0] = 0x41
	addr[20] = 0xab
	return addr
}
