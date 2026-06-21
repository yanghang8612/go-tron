package state

import (
	"strings"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestWitnessBrokerageAnchorAndFlatLatest(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := tcommon.Address{0x41, 0x01}

	sdb.GetOrCreateAccount(addr)
	if got := sdb.ReadWitnessBrokerage(addr); got != int64(rawdb.DefaultBrokerage) {
		t.Fatalf("default brokerage = %d, want %d", got, rawdb.DefaultBrokerage)
	}
	if err := sdb.WriteWitnessBrokerage(addr, 45); err != nil {
		t.Fatal(err)
	}
	root1, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	atR1, err := New(root1, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if got := atR1.ReadWitnessBrokerage(addr); got != 45 {
		t.Fatalf("root1 brokerage = %d, want 45", got)
	}

	if err := atR1.WriteWitnessBrokerage(addr, 70); err != nil {
		t.Fatal(err)
	}
	root2, err := atR1.Commit()
	if err != nil {
		t.Fatal(err)
	}

	root1Open, err := New(root1, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if got := root1Open.ReadWitnessBrokerage(addr); got != 70 {
		t.Fatalf("root1-open latest brokerage = %d, want 70", got)
	}
	atR2, err := New(root2, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if got := atR2.ReadWitnessBrokerage(addr); got != 70 {
		t.Fatalf("root2 brokerage = %d, want 70", got)
	}
}

func TestWitnessHistoryAtSurfacesCorruptPayloads(t *testing.T) {
	f := newHistoryFixture(t)
	witness := tcommon.Address{0x41, 0x72}
	voter := tcommon.Address{0x41, 0x73}
	corruptProto := []byte{0x80}

	f.applyBlock(tcommon.Hash{0x01}, func(s *StateDB) {
		if err := s.SetAccountKV(witness, kvdomains.WitnessCapsule, rawdb.WitnessCapsuleStateKey(witness), corruptProto); err != nil {
			t.Fatalf("write corrupt witness capsule: %v", err)
		}
		if err := s.SystemKVPut(kvdomains.WitnessVoteState, votesStoreKey(voter), corruptProto); err != nil {
			t.Fatalf("write corrupt votes: %v", err)
		}
	})
	f.applyBlock(tcommon.Hash{0x02}, func(*StateDB) {})

	gotWitness, err := f.reader().WitnessAt(witness, 1)
	if err == nil {
		t.Fatal("WitnessAt corrupt payload error = nil")
	}
	if gotWitness != nil {
		t.Fatalf("WitnessAt corrupt payload witness = %+v, want nil", gotWitness)
	}
	if !strings.Contains(err.Error(), "decode witness at block 1") {
		t.Fatalf("WitnessAt corrupt payload error = %v, want decode witness context", err)
	}

	gotVotes, err := f.reader().VotesAt(voter, 1)
	if err == nil {
		t.Fatal("VotesAt corrupt payload error = nil")
	}
	if gotVotes != nil {
		t.Fatalf("VotesAt corrupt payload votes = %+v, want nil", gotVotes)
	}
	if !strings.Contains(err.Error(), "decode votes at block 1") {
		t.Fatalf("VotesAt corrupt payload error = %v, want decode votes context", err)
	}
}

func TestWitnessHistoryAtSurfacesCorruptIndexes(t *testing.T) {
	f := newHistoryFixture(t)
	truncated := []byte{0, 0, 0, 2, 0x41}

	f.applyBlock(tcommon.Hash{0x01}, func(s *StateDB) {
		if err := s.SystemKVPut(kvdomains.SystemWitnessSchedule, witnessScheduleIndexKey, truncated); err != nil {
			t.Fatalf("write corrupt witness index: %v", err)
		}
		if err := s.SystemKVPut(kvdomains.SystemWitnessSchedule, witnessScheduleActiveKey, truncated); err != nil {
			t.Fatalf("write corrupt active witnesses: %v", err)
		}
		if err := s.SystemKVPut(kvdomains.WitnessVoteState, votesStoreIndexKey, truncated); err != nil {
			t.Fatalf("write corrupt votes index: %v", err)
		}
	})
	f.applyBlock(tcommon.Hash{0x02}, func(*StateDB) {})

	if got, err := f.reader().WitnessIndexAt(1); err == nil || got != nil || !strings.Contains(err.Error(), "decode witness index at block 1") {
		t.Fatalf("WitnessIndexAt corrupt index = %v err=%v, want nil decode error", got, err)
	}
	if got, err := f.reader().ActiveWitnessesAt(1); err == nil || got != nil || !strings.Contains(err.Error(), "decode active witness list at block 1") {
		t.Fatalf("ActiveWitnessesAt corrupt index = %v err=%v, want nil decode error", got, err)
	}
	if got, err := f.reader().VotesIndexAt(1); err == nil || got != nil || !strings.Contains(err.Error(), "decode votes index at block 1") {
		t.Fatalf("VotesIndexAt corrupt index = %v err=%v, want nil decode error", got, err)
	}
	if got, ok, err := f.reader().PendingVoteDeltasAt(1); err == nil || ok || got != nil || !strings.Contains(err.Error(), "decode votes index at block 1") {
		t.Fatalf("PendingVoteDeltasAt corrupt index = %v ok=%v err=%v, want nil false decode error", got, ok, err)
	}
}
